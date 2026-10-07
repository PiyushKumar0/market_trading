"""Paper broker construction and the market-data bridge (plan Q4.1).

Construction runs once at boot, before the scheduler arms: seed the id counters, close stranded
orders, restore the GTT book, mark session prep due. A failed step leaves the runtime degraded:
nothing is forwarded and prep never completes, so paper stays deaf until a restart, and that
restart's prep covers the unobserved time.

The bridge forwards in-session ticks and finalized bars to the PaperBroker only while this
session's prep has completed and none is due or running (plan §1.7).
"""

from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Awaitable, Callable
from datetime import date, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Any

from engine.core.calendar import NSECalendar
from engine.core.clock import Clock
from engine.core.config import PaperSettings
from engine.core.contracts import PAPER_ORDER_UPDATE_TOPIC
from engine.core.eventbus import EventBus
from engine.core.log import get_logger
from engine.core.scope import HELD_STATES_SQL, scope_sql
from engine.core.types import Bar, Session, Tick
from engine.oms.manager import OrderGuard, PaperOrderBlocked
from engine.oms.state import OrderState, modify_rejected, transition
from engine.oms.store import OrderStore
from engine.paper.broker import GTT_ID_BASE, ORDER_ID_BASE, PaperBroker
from engine.paper.fill_model import FillModelConfig, load_fill_model

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


async def log_paper_alert(key: str, message: str) -> None:
    """PAPER_ALERT placeholder until Q4.10 adds the catalog kind; never the real alert sink."""
    _log.critical("paper_alert", key=key, message=message)


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


def load_last_observed(conn: sqlite3.Connection) -> datetime | None:
    row = conn.execute("SELECT last_observed_at FROM paper_state WHERE id = 1").fetchone()
    if row is None or row[0] is None:
        return None
    at = datetime.fromisoformat(row[0])
    if at.tzinfo is None:
        raise ValueError(f"paper_state.last_observed_at {row[0]!r} is naive")
    return at


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
    ) -> None:
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
        if self._prep_task is not None:
            self._prep_task.cancel()
            await asyncio.gather(self._prep_task, return_exceptions=True)

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

    def persist_observed(self) -> None:
        if self._last_observed is not None:
            self._conn.execute(
                "UPDATE paper_state SET last_observed_at = ? WHERE id = 1", (self._last_observed.isoformat(),)
            )

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
        broker.on_tick(tick)
        if self._last_observed is None or tick.exchange_ts > self._last_observed:
            self._last_observed = tick.exchange_ts

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
        self._state = PrepState.DONE
        try:
            self.persist_observed()
        except Exception:
            _log.exception("paper_last_observed_not_persisted")
        _log.info("paper_prep_done", failed=failed, last_observed_at=self._last_observed)
        if failed:
            await self._send_alert("prep", f"paper session prep failed for ({since}, {until}]; forwarding resumed")

    async def _send_alert(self, key: str, message: str) -> None:
        try:
            await self._alert(key, message)
        except Exception:
            _log.exception("paper_alert_failed", key=key, message=message)
