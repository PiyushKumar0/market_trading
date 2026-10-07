"""Paper broker construction and the market-data bridge (plan Q4.1).

Construction runs once at boot, before the scheduler arms: seed the id counters, close stranded
orders, restore the GTT book, mark session prep due. A failed step leaves the runtime degraded:
nothing is forwarded and prep never completes, so paper stays deaf until a restart, and that
restart's prep covers the unobserved time.

The bridge forwards in-session ticks and finalized bars to the PaperBroker only while this
session's prep has completed and none is due or running (plan §1.7).

Session prep and the Reconciler (Q4.7) plug in through :meth:`PaperRuntime.attach`. A reconcile pass
follows the first forwarded tick of each held paper symbol after every prep, and runs once a session
from close - 5 min.
"""

from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Awaitable, Callable, Iterator, Sequence
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.config import PaperSettings
from engine.core.contracts import PAPER_ORDER_UPDATE_TOPIC
from engine.core.eventbus import EventBus
from engine.core.log import get_logger
from engine.core.scope import HELD_STATES_SQL, scope_sql
from engine.core.types import Bar, Session, Tick
from engine.learning.exit_sim import Held, SimBar, simulate
from engine.notify import catalog
from engine.notify.catalog import CatalogMessage
from engine.oms.exits import CorpActionsFn, ExitManager, PreExMarkFn
from engine.oms.manager import OrderGuard, OrderManager, PaperOrderBlocked
from engine.oms.positions import PositionBook
from engine.oms.prep import BarsFn, Replayed, ReplayFn, SessionPrep, Walk
from engine.oms.protection import ProtectionManager
from engine.oms.reconcile import Reconciler
from engine.oms.state import CloseReason, OrderRole, OrderState, modify_rejected, transition
from engine.oms.store import OrderStore
from engine.paper.broker import GTT_ID_BASE, ORDER_ID_BASE, PaperBroker
from engine.paper.fill_model import FillModelConfig, load_fill_model
from engine.risk.gate import TERMINAL_ORDER_STATES_SQL
from engine.strategy.scanners.hi52 import UNADJUSTED_KINDS

if TYPE_CHECKING:
    from engine.marketdata.backfill import BackfillJob
    from engine.marketdata.store import MarketStore

_log = get_logger("engine.ops.paper_runtime")

TICK_TOPIC = "tick"
BAR_TOPIC = "bar.1m"

#: More unobserved session time than this before a tick runs session prep first (plan §1.7).
GAP_TOLERANCE = timedelta(minutes=5)
#: A backstop only: prep keeps its own 120 s deadline and voids what it did not finish (Q4.7).
PREP_BACKSTOP_S = 180.0

#: ``prep(since, until)``: catch up the unobserved window (``since`` is None if never observed).
PrepFn = Callable[[datetime | None, datetime], Awaitable[None]]
#: ``alert(key, message)``: the PAPER_ALERT send; ``key`` is ``construct`` or ``prep``.
PaperAlert = Callable[[str, str], Awaitable[None]]
#: One ``paper_tick`` step; each runs guarded, so one failing step never starves the others.
TickStep = Callable[[], Awaitable[Any]]

PAPER_TICK_S = 30
#: The close reconcile (Q4.7) runs once a session from this long before the close.
CLOSE_RECONCILE_LEAD = timedelta(minutes=5)
#: How far back a stored mark is looked for; none found prices a void at ``avg_entry``.
PRE_EX_LOOKBACK = timedelta(days=10)
#: Bounds the restart mark seeding that precedes session prep.
SEED_MARKS_S = 30.0
#: ``last_mark_fn(symbol, at)``: the last stored price at or before ``at`` and its timestamp.
LastMarkFn = Callable[[str, datetime], Awaitable[tuple[Decimal, datetime] | None]]


class PrepState(StrEnum):
    DUE = "due"
    RUNNING = "running"
    DONE = "done"


#: Legal edges that close an order a restart stranded; MODIFY_PENDING is first resolved by fill.
STRANDED_EXIT: dict[OrderState, OrderState] = {
    OrderState.DRAFT: OrderState.REJECTED,
    OrderState.VALIDATED: OrderState.REJECTED,
    OrderState.SUBMITTED: OrderState.CANCELLED,
    OrderState.CANCEL_PENDING: OrderState.CANCELLED,
    OrderState.ACKED: OrderState.LAPSED,
    OrderState.PARTIALLY_FILLED: OrderState.LAPSED,
}
_RESTART_PAYLOAD = {"platform_intent": "restart_close", "reject_reason": "stranded by an engine restart"}


async def no_prep(since: datetime | None, until: datetime) -> None:
    """Session prep until Q4.7 replaces it."""


async def no_step() -> None:
    """A ``paper_tick`` step not yet wired."""


async def log_paper_alert(key: str, message: str) -> None:
    """The log-only alert: a bare PaperRuntime's default."""
    _log.critical("paper_alert", key=key, message=message)


async def no_notify(msg: CatalogMessage) -> None:
    """The owner sink of a composition built without one."""


_ENTRY_FILLS = (
    "SELECT o.order_id, o.filled_qty, p.symbol, p.avg_entry FROM orders o "
    "JOIN positions p ON p.position_id = o.position_id "
    f"WHERE {scope_sql('paper', 'o')} AND o.role = 'entry' AND o.state IN {TERMINAL_ORDER_STATES_SQL} "
    "AND o.filled_qty > 0 "
    f"AND {scope_sql('paper', 'p', has_origin=True)}"
)
_CLOSES = (
    "SELECT l.position_id, l.qty, l.exit_px, l.net_pnl, l.close_reason, p.symbol, p.close_basis "
    "FROM learning_ledger l JOIN positions p ON p.position_id = l.position_id "
    f"WHERE {scope_sql('paper', 'l')} AND l.close_reason != 'void' "
    f"AND {scope_sql('paper', 'p', has_origin=True)}"
)


class PaperNotifier:
    """Owner messages from the paper path (plan Q4.10), through the owner sink and never the real alert
    sink: PAPER_ALERT for protection and construction failures, FILL for entry completions and closes.
    Halts, reconcile mismatches, voids and late corporate actions are only logged.

    Fills and closes are found by scanning the paper tables, since closes also happen with no broker
    order (session prep). What already exists at boot counts as announced; the journal's dedupe_key
    drops anything a restart would announce twice."""

    def __init__(self, conn: sqlite3.Connection, clock: Clock, notify: Callable[[CatalogMessage], Awaitable[None]]):
        self._conn = conn
        self._clock = clock
        self._notify = notify
        try:
            self._announced = {m.dedupe_key for m in self._fills()}
        except Exception:
            _log.exception("paper_announced_unread")
            self._announced = set()

    async def alert(self, key: str, message: str) -> None:
        """The ``PaperAlert``: ``key`` is ``<position_id>:<attempt>``, ``construct`` or ``prep``."""
        _log.critical("paper_alert", key=key, message=message)
        if key == "prep":
            return
        if key == "construct":
            key = f"construct:{self._clock.now().isoformat()}"
        await self._send(catalog.paper_alert(key, message))

    async def tick(self) -> None:
        for msg in self._fills():
            if msg.dedupe_key not in self._announced:
                self._announced.add(msg.dedupe_key)
                await self._send(msg)

    def _fills(self) -> list[CatalogMessage]:
        entries = [
            catalog.paper_entry_filled(
                order_id=r["order_id"], symbol=r["symbol"], qty=int(r["filled_qty"]), avg_price=Decimal(r["avg_entry"])
            )
            for r in self._conn.execute(_ENTRY_FILLS)
        ]
        closes = [
            catalog.paper_position_closed(
                position_id=r["position_id"], symbol=r["symbol"], qty=int(r["qty"]), exit_price=Decimal(r["exit_px"]),
                net_pnl=Decimal(r["net_pnl"]), reason=r["close_reason"], basis=r["close_basis"] or "fill",
            )
            for r in self._conn.execute(_CLOSES)
        ]
        return entries + closes

    async def _send(self, msg: CatalogMessage) -> None:
        try:
            await self._notify(msg)
        except Exception:
            _log.exception("paper_notify_failed", dedupe_key=msg.dedupe_key)


def seeded_counters(conn: sqlite3.Connection) -> tuple[int, int]:
    """``(order_seq, gtt_seq)`` past every paper id ever stored, whatever its state (Q4.2a)."""
    order_max = conn.execute(
        f"SELECT MAX(CAST(broker_order_id AS INTEGER)) FROM orders WHERE {scope_sql('paper')}"
    ).fetchone()[0]
    gtt_max = conn.execute(f"SELECT MAX(gtt_id) FROM gtts WHERE {scope_sql('paper')}").fetchone()[0]

    def seq(max_id: int | None, base: int) -> int:
        return 0 if max_id is None else max(int(max_id) - base, 0)

    return seq(order_max, ORDER_ID_BASE), seq(gtt_max, GTT_ID_BASE)


def close_stranded(store: OrderStore, at: datetime) -> int:
    """Close every non-terminal paper order through legal edges; raises if any could not be."""
    closed = failed = 0
    for order in store.open_orders():
        if not order.is_paper:
            continue
        try:
            if order.state is OrderState.MODIFY_PENDING:
                after, event = modify_rejected(order, payload=dict(_RESTART_PAYLOAD), at=at)
                store.record(order, after, event)
                order = after
            after, event = transition(order, STRANDED_EXIT[order.state], payload=dict(_RESTART_PAYLOAD), at=at)
            store.record(order, after, event)
            closed += 1
        except Exception:
            _log.exception("paper_stranded_order_not_closed", order_id=order.order_id, state=order.state)
            failed += 1
    if failed:
        raise RuntimeError(f"{failed} stranded paper orders could not be closed")
    return closed


def active_gtt_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """ACTIVE paper GTTs of held paper positions, as plain dicts for ``PaperBroker.restore``."""
    rows = conn.execute(
        "SELECT g.* FROM gtts g JOIN positions p ON p.position_id = g.position_id "
        f"WHERE {scope_sql('paper', 'g')} AND UPPER(g.state) = 'ACTIVE' "
        f"AND {scope_sql('paper', 'p', has_origin=True)} AND p.{HELD_STATES_SQL['paper']} "
        "ORDER BY g.gtt_id"
    ).fetchall()
    return [dict(r) for r in rows]


def held_paper_symbols(conn: sqlite3.Connection) -> set[str]:
    return {r[0] for r in conn.execute(
        f"SELECT symbol FROM positions WHERE {scope_sql('paper', has_origin=True)} AND {HELD_STATES_SQL['paper']}"
    )}


def load_last_observed(conn: sqlite3.Connection) -> datetime | None:
    row = conn.execute("SELECT last_observed_at FROM paper_state WHERE id = 1").fetchone()
    if row is None or row[0] is None:
        return None
    at = datetime.fromisoformat(row[0])
    if at.tzinfo is None:
        raise ValueError(f"paper_state.last_observed_at {row[0]!r} is naive")
    return at


def store_corp_actions_fn(store: MarketStore) -> CorpActionsFn:
    """The ExitManager's ``corp_actions_fn``: ``corp_actions`` read through the store, never the
    ``calendar.ex_dates`` stub, which always returns ``[]``."""

    async def corp_actions(ex_from: date, ex_to: date | None) -> list[dict[str, Any]]:
        rows = await store.arun(store.get_corp_actions, ex_from=ex_from, ex_to=ex_to)
        return [r for r in rows if r["kind"] in UNADJUSTED_KINDS]

    return corp_actions


async def _last_bar(store: MarketStore, symbol: str, end: datetime) -> Bar | None:
    """The last stored 1m bar before ``end``."""
    bars = await store.arun(store.get_bars_1m, symbol, end - PRE_EX_LOOKBACK, end)
    return bars[-1] if bars else None


def store_pre_ex_mark_fn(store: MarketStore) -> PreExMarkFn:
    """The ExitManager's ``pre_ex_mark_fn``: the close of the last stored 1m bar before the ex-date."""

    async def pre_ex_mark(symbol: str, ex_date: date) -> Decimal | None:
        bar = await _last_bar(store, symbol, datetime.combine(ex_date, time(0), tzinfo=IST))
        return None if bar is None else bar.close

    return pre_ex_mark


def store_last_mark_fn(store: MarketStore) -> LastMarkFn:
    """The runtime's ``last_mark_fn``: the close of the last stored 1m bar whose minute starts at or
    before ``at``, stamped with that minute."""

    async def last_mark(symbol: str, at: datetime) -> tuple[Decimal, datetime] | None:
        bar = await _last_bar(store, symbol, at.replace(second=0, microsecond=0) + _MINUTE)
        return None if bar is None else (bar.close, bar.ts_minute)

    return last_mark


def session_segments(calendar: NSECalendar, frm: datetime, to: datetime) -> Iterator[tuple[datetime, datetime]]:
    """The session minutes of ``[frm, to)``, one segment per session."""
    d, last = frm.astimezone(IST).date(), to.astimezone(IST).date()
    while d <= last:
        session = calendar.session(d)
        if session is not None and max(frm, session.open) < min(to, session.close):
            yield max(frm, session.open), min(to, session.close)
        d += timedelta(days=1)


def backfill_bars_fn(backfill: BackfillJob | None, store: MarketStore, calendar: NSECalendar) -> BarsFn:
    """Session prep's ``bars_fn``: ``warmup_gap`` once per session segment (its coverage check assumes a
    within-session range), then the stored bars. A symbol any segment failed to fetch gets None."""

    async def bars(symbols: Sequence[str], frm: datetime, to: datetime) -> dict[str, list[Bar] | None]:
        if backfill is None:
            raise RuntimeError("no Kite session to backfill the unobserved window from")
        failed: set[str] = set()
        for seg_from, seg_to in session_segments(calendar, frm, to):
            report = await backfill.warmup_gap(symbols, seg_from, seg_to)
            failed |= {span.symbol for span in report.failed}
        return {s: None if s in failed else await store.aget_bars_1m(s, frm, to) for s in symbols}

    return bars


_MINUTE = timedelta(minutes=1)
_REASONS = {"stop": CloseReason.STOP, "target": CloseReason.TARGET, "time": CloseReason.TIME_STOP}


def exit_sim_replay(bars: Sequence[Bar], walk: Walk) -> Replayed | None:
    """Session prep's ``replay_fn``: ``exit_sim`` from an open position, stamped with its exit bar's time."""
    # A Held start walks bars starting after it; one minute back admits the minute holding ``observed``.
    start = walk.observed - _MINUTE
    stream = [SimBar(b.ts_minute, b.open, b.high, b.low, b.close) for b in bars if start < b.ts_minute < walk.end]
    out = simulate(stream, Held(walk.avg_entry, start), stop=walk.stop, target=walk.target,
                   horizon=walk.exit_session, end=walk.end, cost_pct=Decimal(0))
    if out.status != "exit":
        return None
    day = [b for b in stream if b.d == out.exit_d]
    if out.reason == "time":
        at = day[-1].at + _MINUTE
    else:
        at = next(b.at for b in day if (walk.stop is not None and b.low <= walk.stop)
                  or (walk.target is not None and b.high >= walk.target))
    return Replayed(out.exit_px, _REASONS[out.reason], at)


class PaperRuntime:
    """Builds the PaperBroker, bridges market data to it, and owns the session-prep gate.

    ``prep_ready()`` is the gate for paper entries and ``paper_tick``; :meth:`guard` is the order
    guard to install on the OrderManager (the broker already has it).
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        clock: Clock,
        calendar: NSECalendar,
        bus: EventBus,
        settings: PaperSettings,
        *,
        capital_base_fn: Callable[[], Decimal],
        tick_size_fn: Callable[[str], Decimal],
        order_guard: OrderGuard,
        alert: PaperAlert = log_paper_alert,
        prep: PrepFn = no_prep,
        fill_model_fn: Callable[[], FillModelConfig] = load_fill_model,
        bars_fn: BarsFn | None = None,
        replay_fn: ReplayFn | None = None,
        corp_actions_fn: CorpActionsFn | None = None,
        last_mark_fn: LastMarkFn | None = None,
    ) -> None:
        self._bars_fn = bars_fn
        self._last_mark_fn = last_mark_fn
        self._orders: OrderManager | None = None
        self._replay_fn = replay_fn
        self._corp_actions_fn = corp_actions_fn
        self.session_prep: SessionPrep | None = None
        self.reconciler: Reconciler | None = None
        self._reconcile: TickStep = no_step
        self._unreconciled: set[str] = set()
        self._reconcile_tasks: set[asyncio.Task[None]] = set()
        self._conn = conn
        self._clock = clock
        self._calendar = calendar
        self._bus = bus
        self._settings = settings
        self._capital_base_fn = capital_base_fn
        self._tick_size_fn = tick_size_fn
        self._order_guard = order_guard
        self._alert = alert
        self._prep = prep
        self._fill_model_fn = fill_model_fn
        self.broker: PaperBroker | None = None
        self.failures: list[str] = []
        self._state = PrepState.DUE
        self._ready_for: date | None = None
        self._last_observed: datetime | None = None
        self._prep_task: asyncio.Task[None] | None = None
        self._session: tuple[date, Session | None] | None = None
        self._marks: dict[str, tuple[Decimal, datetime]] = {}
        self.set_tick_steps()
        self._reconciled_for: date | None = None

    # ---------------------------------------------------------------- construction
    async def start(self) -> None:
        """Construct synchronously, then subscribe the bridge, or alert once if degraded."""
        self._construct()
        if self.degraded:
            await self._send_alert(
                "construct", f"paper construction failed ({', '.join(self.failures)}); paper is off until a restart"
            )
            return
        self._bus.subscribe(TICK_TOPIC, self._tick_event)
        self._bus.subscribe(BAR_TOPIC, self._bar_event)

    async def stop(self) -> None:
        self._bus.unsubscribe(TICK_TOPIC, self._tick_event)
        self._bus.unsubscribe(BAR_TOPIC, self._bar_event)
        tasks = [t for t in (self._prep_task, *self._reconcile_tasks) if t is not None]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    def attach(
        self, *, orders: OrderManager, book: PositionBook, protection: ProtectionManager, exits: ExitManager,
        pre_ex_mark_fn: PreExMarkFn,
    ) -> tuple[SessionPrep, Reconciler]:
        """Plug in session prep and the reconcile passes over the managers built on :attr:`broker`."""
        if self.broker is None or self._bars_fn is None or self._replay_fn is None or self._corp_actions_fn is None:
            raise RuntimeError("session prep needs the broker and the injected bars, replay and corp-actions fns")
        self.session_prep = SessionPrep(
            self._conn, book=book, exit_fn=exits.exit_position, bars_fn=self._bars_fn, replay_fn=self._replay_fn,
            corp_actions_fn=self._corp_actions_fn, pre_ex_mark_fn=pre_ex_mark_fn, mark_fn=self.mark,
        )
        self.reconciler = Reconciler(self._conn, self._clock, self.broker, orders=orders, protection=protection)
        self._orders = orders
        self._prep = self.session_prep.run
        self._reconcile = self.reconciler.run
        return self.session_prep, self.reconciler

    def _construct(self) -> None:
        seqs = self._step("seed_counters", lambda: seeded_counters(self._conn))
        if seqs is not None:
            self.broker = self._step("build_broker", lambda: self._build_broker(*seqs))
        self._step("close_stranded", lambda: close_stranded(OrderStore(self._conn), self._clock.now()))
        if self.broker is not None:
            broker = self.broker
            self._step("restore_gtts", lambda: broker.restore(active_gtt_rows(self._conn)))
        self._last_observed = self._step("load_last_observed", lambda: load_last_observed(self._conn))
        self._state = PrepState.DUE
        _log.info("paper_runtime_constructed", failures=self.failures, last_observed_at=self._last_observed)

    def _step(self, name: str, fn: Callable[[], Any]) -> Any:
        try:
            return fn()
        except Exception:
            _log.exception("paper_construct_step_failed", step=name)
            self.failures.append(name)
            return None

    def _build_broker(self, order_seq: int, gtt_seq: int) -> PaperBroker:
        return PaperBroker(
            self._clock,
            self._bus.publish,
            self._fill_model_fn(),
            self._tick_size_fn,
            self._settings.seed,
            order_guard=self.guard,
            available_margin=self._capital_base_fn(),
            order_seq=order_seq,
            gtt_seq=gtt_seq,
            topic=PAPER_ORDER_UPDATE_TOPIC,
        )

    # ---------------------------------------------------------------- gate
    @property
    def degraded(self) -> bool:
        return bool(self.failures) or self.broker is None

    @property
    def prep_state(self) -> PrepState:
        return self._state

    @property
    def last_observed_at(self) -> datetime | None:
        return self._last_observed

    def prep_ready(self) -> bool:
        """This session's prep has completed and none is due or running."""
        return (
            not self.degraded
            and self._state is PrepState.DONE
            and self._ready_for == self._clock.now().date()
        )

    def guard(self, intent: str) -> None:
        if intent == "entry" and not self.prep_ready():
            raise PaperOrderBlocked("paper session prep has not completed")
        self._order_guard(intent)

    def mark(self, symbol: str) -> Decimal | None:
        """The last price forwarded to the broker, or the stored one prep seeded: paper equity never
        marks ahead of session prep."""
        mark = self._marks.get(symbol)
        return mark[0] if mark is not None else None

    def persist_observed(self) -> None:
        if self._last_observed is not None:
            self._conn.execute(
                "UPDATE paper_state SET last_observed_at = ? WHERE id = 1", (self._last_observed.isoformat(),)
            )

    # ---------------------------------------------------------------- paper_tick
    def set_tick_steps(
        self,
        *,
        protection: TickStep = no_step,
        exits: TickStep = no_step,
        equity: TickStep = no_step,
    ) -> None:
        """Wire the managers built on :attr:`broker`: ProtectionManager.tick, ExitManager.tick and the
        Q4.8 equity snapshot and halts. The close reconcile comes with :meth:`attach`."""
        self._steps = (("protection", protection), ("exits", exits), ("equity", equity),
                       ("entries", self._cancel_blocked_entries))

    async def paper_tick(self) -> None:
        """The 30 s ``paper_tick`` job. Does nothing until this session's prep has completed; the
        exit steps gate themselves to the session."""
        if not self.prep_ready():
            return
        for name, step in self._steps:
            await self._run_step(name, step)
        now = self._clock.now()
        session = self._current_session(now)
        if session is not None and now >= session.close - CLOSE_RECONCILE_LEAD and self._reconciled_for != now.date():
            self._reconciled_for = now.date()
            await self._run_step("close_reconcile", self._reconcile)
        try:
            self.persist_observed()
        except Exception:
            _log.exception("paper_last_observed_not_persisted")

    async def _run_step(self, name: str, step: TickStep) -> None:
        try:
            await step()
        except Exception:
            _log.exception("paper_tick_step_failed", step=name)

    async def _cancel_blocked_entries(self) -> None:
        """Cancel every resting paper entry while the order guard refuses entries (paper off or the
        effective state not NORMAL; an unreadable state refuses too). A partial fill keeps its part."""
        if self._orders is None:
            return
        try:
            self._order_guard("entry")
            return
        except Exception as exc:
            blocked = str(exc)
        for order in OrderStore(self._conn).open_orders():
            if order.is_paper and order.role is OrderRole.ENTRY and order.broker_order_id is not None:
                _log.warning("paper_entry_cancel_blocked", order_id=order.order_id, blocked=blocked)
                await self._orders.cancel(order.broker_order_id, reason="entries_blocked")

    # ---------------------------------------------------------------- bridge
    async def _tick_event(self, tick: Tick) -> None:
        self.on_tick(tick)

    async def _bar_event(self, bar: Bar) -> None:
        self.on_bar(bar)

    def on_tick(self, tick: Tick) -> None:
        """Forward an in-session tick, or start prep and drop it. No await: ticks stay ordered."""
        broker = self.broker
        session = self._current_session(tick.exchange_ts)
        if broker is None or self.failures or session is None:
            return
        if self._state is PrepState.DONE and self._prep_needed(tick.exchange_ts, session):
            self._state = PrepState.DUE
        if self._state is PrepState.DUE:
            self._start_prep(session, max(self._clock.now(), tick.exchange_ts))
        if self._state is not PrepState.DONE:
            return
        mark = self._marks.get(tick.tradingsymbol)
        if mark is None or tick.exchange_ts >= mark[1]:
            self._marks[tick.tradingsymbol] = (tick.ltp, tick.exchange_ts)
        broker.on_tick(tick)
        if self._last_observed is None or tick.exchange_ts > self._last_observed:
            self._last_observed = tick.exchange_ts
        if tick.tradingsymbol in self._unreconciled:
            self._unreconciled.discard(tick.tradingsymbol)
            task = asyncio.get_running_loop().create_task(self._run_step("reconcile", self._reconcile))
            self._reconcile_tasks.add(task)
            task.add_done_callback(self._reconcile_tasks.discard)

    def on_bar(self, bar: Bar) -> None:
        if self.prep_ready() and self.broker is not None and self._current_session(bar.ts_minute) is not None:
            self.broker.on_bar(bar)

    def _current_session(self, ts: datetime) -> Session | None:
        """Today's session if ``ts`` falls inside it; stale and out-of-hours stamps are not."""
        today = self._clock.now().date()
        if self._session is None or self._session[0] != today:
            self._session = (today, self._calendar.session(today))
        session = self._session[1]
        return session if session is not None and session.open <= ts <= session.close else None

    def _prep_needed(self, ts: datetime, session: Session) -> bool:
        return (
            self._ready_for != date.fromisoformat(session.date_ist)
            or self._last_observed is None
            or ts - self._last_observed > GAP_TOLERANCE
        )

    def _start_prep(self, session: Session, until: datetime) -> None:
        self._state = PrepState.RUNNING
        self._prep_task = asyncio.get_running_loop().create_task(
            self._run_prep(date.fromisoformat(session.date_ist), self._last_observed, until),
            name="paper-session-prep",
        )

    async def _run_prep(self, session_date: date, since: datetime | None, until: datetime) -> None:
        _log.info("paper_prep_started", since=since, until=until)
        await self._seed_marks(since)
        try:
            async with asyncio.timeout(PREP_BACKSTOP_S):
                await self._prep(since, until)
            failed = False
        except Exception:
            _log.exception("paper_prep_failed", since=since, until=until)
            failed = True
        # Advanced even after a failure: prep voids what it could not finish, and a window left
        # open would re-trigger prep on every tick.
        self._last_observed = until if self._last_observed is None else max(self._last_observed, until)
        self._ready_for = session_date
        try:
            self._unreconciled = held_paper_symbols(self._conn)
        except Exception:
            _log.exception("paper_held_symbols_unread")
        self._state = PrepState.DONE
        try:
            self.persist_observed()
        except Exception:
            _log.exception("paper_last_observed_not_persisted")
        _log.info("paper_prep_done", failed=failed, last_observed_at=self._last_observed)
        if failed:
            await self._send_alert("prep", f"paper session prep failed for ({since}, {until}]; forwarding resumed")

    async def _seed_marks(self, observed: datetime | None) -> None:
        """Mark each held paper symbol at its last stored bar up to ``observed`` unless a newer mark is
        carried: prep voids and the first equity snapshot after a restart price at the last mark."""
        if observed is None or self._last_mark_fn is None:
            return
        try:
            async with asyncio.timeout(SEED_MARKS_S):
                for symbol in held_paper_symbols(self._conn):
                    found = await self._last_mark_fn(symbol, observed)
                    mark = self._marks.get(symbol)
                    if found is not None and (mark is None or found[1] > mark[1]):
                        self._marks[symbol] = found
        except Exception:
            _log.exception("paper_marks_not_seeded", observed=observed)

    async def _send_alert(self, key: str, message: str) -> None:
        try:
            await self._alert(key, message)
        except Exception:
            _log.exception("paper_alert_failed", key=key, message=message)
