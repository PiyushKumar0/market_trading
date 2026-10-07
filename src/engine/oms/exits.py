"""Paper ExitManager (plan Q4.6): the one exit routine, the D2 time exit and corporate actions.

Every live exit cause (time, equity-floor flatten, PROTECTION_FAILED, overdue, residual) goes through
:meth:`ExitManager.exit_position`; nothing else flattens paper, and a real kill or halt never does.
Exits are placed only in session; outside it, or while a fired leg is not yet in the OMS, the request
waits for the next in-session :meth:`ExitManager.tick`. SQLite runs on the event-loop thread and no
write spans an ``await``.
"""

from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

from engine.core.clock import Clock
from engine.core.config import PaperSettings
from engine.core.db import transaction
from engine.core.log import get_logger
from engine.core.scope import HELD_STATES_SQL, scope_sql
from engine.core.types import Session
from engine.oms.manager import OrderManager
from engine.oms.positions import LEG_REASON, AddSessionsFn, HoldFn, PositionBook, SessionOfFn, leg_of
from engine.oms.state import TERMINAL_ORDER_STATES, CloseReason, PositionState
from engine.paper.broker import TERMINAL_STATUSES, PaperBroker

_log = get_logger("engine.oms.exits")

_PAPER = scope_sql("paper")
_PAPER_POSITION = scope_sql("paper", has_origin=True)
_HELD = f"{_PAPER_POSITION} AND {HELD_STATES_SQL['paper']}"
_WORKING = "state NOT IN ({})".format(", ".join(f"'{s.value}'" for s in sorted(TERMINAL_ORDER_STATES)))
_PAISA = Decimal("0.01")
_PRICE = Decimal("0.0001")

#: Open positions are re-checked for an ex-date this often in session (plan Q4.6).
CORP_RECHECK = timedelta(minutes=15)
#: An in-session exit still waiting on an unapplied leg this long is logged as stuck (Q4.7 reconcile adopts it).
STUCK_DEFERRAL = timedelta(minutes=5)

#: ``corp_actions_fn(ex_from, ex_to)``: the UNADJUSTED_KINDS corporate actions with ``ex_from <= ex_date``
#: and, unless ``ex_to`` is None, ``ex_date <= ex_to``; rows carry ``symbol`` and ``ex_date``.
CorpActionsFn = Callable[[date, date | None], Awaitable[Sequence[Mapping[str, Any]]]]
#: ``pre_ex_mark_fn(symbol, ex_date)``: the last mark carried before ``ex_date``; None when unknown.
PreExMarkFn = Callable[[str, date], Awaitable[Decimal | None]]
SessionFn = Callable[[date], Session | None]


@dataclass
class ExitCounters:
    """Since boot, for ``/paper status``."""

    voids: int = 0
    late_corp_actions: int = 0
    calendar_raises: int = 0
    overdue: int = 0


@dataclass
class _Deferred:
    reason: CloseReason
    basis: str
    why: str
    since: datetime
    alarmed: bool = False


async def cancel_working_entries(conn: sqlite3.Connection, orders: OrderManager, pid: str, reason: str) -> None:
    """A resting entry must not buy into a position on its way out."""
    for r in conn.execute(
        f"SELECT broker_order_id FROM orders WHERE position_id = ? AND role = 'entry' AND {_PAPER} "
        f"AND {_WORKING} AND broker_order_id IS NOT NULL",
        (pid,),
    ).fetchall():
        await orders.cancel(r["broker_order_id"], reason=reason)


def _ex_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    return value if isinstance(value, date) else date.fromisoformat(str(value))


class ExitManager:
    """Seams: :meth:`exit_position` is the ProtectionManager's ``exit_fn``; :meth:`tick` runs from
    ``paper_tick``; :meth:`flatten_all` is Q4.8's; :meth:`entry_refusal` is Q4.9's."""

    def __init__(
        self,
        conn: sqlite3.Connection,
        clock: Clock,
        broker: PaperBroker,
        *,
        orders: OrderManager,
        book: PositionBook,
        session_fn: SessionFn,
        session_of: SessionOfFn,
        add_sessions: AddSessionsFn,
        hold_fn: HoldFn,
        corp_actions_fn: CorpActionsFn,
        pre_ex_mark_fn: PreExMarkFn,
        settings: PaperSettings,
    ) -> None:
        if not isinstance(broker, PaperBroker):
            raise TypeError(f"ExitManager takes a PaperBroker only, got {type(broker).__name__} (D8)")
        self._conn = conn
        self._clock = clock
        self._broker = broker
        self._orders = orders
        self._book = book
        self._session_fn = session_fn
        self._session_of = session_of
        self._add_sessions = add_sessions
        self._hold_fn = hold_fn
        self._corp_actions = corp_actions_fn
        self._pre_ex_mark = pre_ex_mark_fn
        self._close_buffer = timedelta(minutes=settings.exit_minutes_before_close)
        self._lock = asyncio.Lock()
        self._deferred: dict[str, _Deferred] = {}
        self._corp_checked_at: datetime | None = None
        self._raised: set[tuple[str, date]] = set()
        self.counters = ExitCounters()

    # ---------------------------------------------------------------- the routine
    async def exit_position(self, position_id: str, reason: CloseReason, basis: str) -> None:
        async with self._lock:
            await self._exit(position_id, reason, basis)

    async def flatten_all(self, basis: str = "equity_floor") -> int:
        """Run the exit routine for every held paper position; returns how many were held."""
        async with self._lock:
            held = self._held()
            for row in held:
                await self._guarded(self._exit, row["position_id"], CloseReason.RISK_FLATTEN, basis)
        _log.warning("paper_flatten_all", basis=basis, positions=len(held))
        return len(held)

    async def _exit(self, pid: str, reason: CloseReason, basis: str) -> None:
        if self._held_row(pid) is None:
            self._deferred.pop(pid, None)
            return
        if self._working_sells(pid):
            self._deferred.pop(pid, None)
            return
        if self._in_session(self._clock.now()) is None:
            self._defer(pid, reason, basis, "out_of_session")
            return
        gtt_ids = self._gtt_ids(pid)
        live = [g for g in await self._broker.gtts() if g["id"] in gtt_ids]
        book = await self._broker.orders()
        legs = [(leg_of(o.get("tag")), o) for o in book]
        working_leg = next((leg for leg, o in legs if leg is not None and leg[0] in gtt_ids
                            and o["status"] not in TERMINAL_STATUSES), None)
        if working_leg is not None:
            self._deferred.pop(pid, None)
            self._book.mark_pending_exit(pid, LEG_REASON.get(working_leg[1], CloseReason.STOP))
            _log.info("paper_exit_handed_to_leg", position_id=pid, gtt_id=working_leg[0], reason=reason.value)
            await cancel_working_entries(self._conn, self._orders, pid, "exit_handed_to_leg")
            return
        known = self._broker_ids(pid)
        if any(leg is not None and leg[0] in gtt_ids and o["order_id"] not in known for leg, o in legs):
            # A spent leg's fill is not in the OMS yet: an exit sized now would oversell.
            self._defer(pid, reason, basis, "leg_unapplied")
            return
        self._deferred.pop(pid, None)
        # PENDING_EXIT first: verify_all never re-arms a position whose GTT is about to go.
        self._book.mark_pending_exit(pid, reason, basis)
        await self._delete_active(pid, [g["id"] for g in live if g["status"] == "active"])
        await cancel_working_entries(self._conn, self._orders, pid, "exit_routine")
        row = self._held_row(pid)
        if row is None:
            return
        order = await self._orders.submit_exit(pid, int(row["qty"]))
        _log.info("paper_exit_sent", position_id=pid, reason=reason.value, basis=basis, qty=row["qty"],
                  order_id=order.order_id, state=order.state.value)

    async def _delete_active(self, pid: str, gtt_ids: list[int]) -> None:
        if not gtt_ids:
            return
        for gtt_id in gtt_ids:
            try:
                await self._broker.delete_gtt(gtt_id)
            except Exception as exc:
                _log.warning("paper_exit_gtt_delete_refused", position_id=pid, gtt_id=gtt_id, error=repr(exc))
        armed = {g["id"] for g in await self._broker.gtts() if g["status"] == "active"}
        now = self._clock.now().isoformat()
        for gtt_id in set(gtt_ids) - armed:
            self._conn.execute(
                f"UPDATE gtts SET state = 'deleted', last_verified_at = ? WHERE gtt_id = ? AND {_PAPER}", (now, gtt_id)
            )
        if armed & set(gtt_ids):
            raise RuntimeError(f"{pid}: GTT {sorted(armed & set(gtt_ids))} still armed; no exit sent")

    def _defer(self, pid: str, reason: CloseReason, basis: str, why: str) -> None:
        now = self._clock.now()
        deferred = self._deferred.get(pid)
        if deferred is None or deferred.why != why:
            self._deferred[pid] = _Deferred(reason, basis, why, now)
            _log.info("paper_exit_deferred", position_id=pid, reason=reason.value, basis=basis, why=why)
        elif why != "out_of_session" and now - deferred.since >= STUCK_DEFERRAL and not deferred.alarmed:
            deferred.alarmed = True
            _log.error("paper_exit_deferral_stuck", position_id=pid, since=deferred.since.isoformat(), why=why)

    # ---------------------------------------------------------------- tick
    async def tick(self) -> None:
        async with self._lock:
            now = self._clock.now()
            session = self._in_session(now)
            held = self._held()
            ids = {r["position_id"] for r in held}
            for pid in set(self._deferred) - ids:
                del self._deferred[pid]
            if session is not None:
                today = date.fromisoformat(session.date_ist)
                self._raised = {k for k in self._raised if k[1] == today}
                for pid, deferred in list(self._deferred.items()):
                    await self._guarded(self._exit, pid, deferred.reason, deferred.basis)
                for row in held:
                    if row["state"] == PositionState.OPEN.value and row["position_id"] not in self._deferred:
                        await self._guarded(self._time_exit, row, session, today, now)
                if self._corp_checked_at is None or now - self._corp_checked_at >= CORP_RECHECK:
                    self._corp_checked_at = now
                    try:
                        await self._corp_recheck(today, now)
                    except Exception:
                        self._corp_checked_at = None
                        _log.exception("paper_corp_recheck_failed")
            try:
                await self._sweep_closed_gtts()
            except Exception:
                _log.exception("paper_closed_gtt_sweep_failed")

    async def _time_exit(self, row: sqlite3.Row, session: Session, today: date, now: datetime) -> None:
        pid = row["position_id"]
        exit_session = self._exit_session(row)
        if exit_session is None:
            return
        if exit_session < today:
            self.counters.overdue += 1
            _log.warning("paper_time_exit_overdue", position_id=pid, exit_session=exit_session.isoformat())
            await self._exit(pid, CloseReason.TIME_STOP, "time_overdue")
        elif exit_session == today and now >= session.close - self._close_buffer:
            await self._exit(pid, CloseReason.TIME_STOP, "time")

    def _exit_session(self, row: sqlite3.Row) -> date | None:
        """The stored exit session, else recomputed from the hold and persisted; None = still pending."""
        if row["exit_session"] is not None:
            return date.fromisoformat(row["exit_session"])
        pid = row["position_id"]
        computed = self.exit_session_for(
            row["strategy_id"], row["style"], self._session_of(datetime.fromisoformat(row["opened_at"])), key=pid
        )
        if computed is not None:
            self._conn.execute(
                f"UPDATE positions SET exit_session = ? WHERE position_id = ? AND {_HELD} AND exit_session IS NULL",
                (computed.isoformat(), pid),
            )
            _log.info("paper_exit_session_computed", position_id=pid, exit_session=computed.isoformat())
        return computed

    def exit_session_for(self, strategy_id: str, style: str, entry_session: date, *, key: str) -> date | None:
        """Session N of the hold, ``entry_session`` being 1; None while the calendar cannot say (the
        raise is logged and counted once per ``key`` a day; Q0.4 alerts) or the hold is unknown."""
        try:
            n = self._hold_fn(strategy_id, style)
            if n is not None:
                return self._add_sessions(entry_session, n - 1)
        except ValueError as exc:
            today = self._clock.now().date()
            if (key, today) not in self._raised:
                self._raised.add((key, today))
                self.counters.calendar_raises += 1
                _log.warning("paper_exit_session_calendar_raise", key=key, error=str(exc))
            return None
        _log.error("paper_hold_unknown", key=key, strategy_id=strategy_id, style=style)
        return None

    # ---------------------------------------------------------------- corporate actions
    async def entry_refusal(self, symbol: str, strategy_id: str, style: str, at: datetime) -> str | None:
        """Why a paper entry in ``symbol`` at ``at`` must be refused, or None: an unadjusted ex-date in
        (entry session, exit session], or any later one while the exit session is pending. A
        ``corp_actions_fn`` failure propagates: the caller refuses."""
        entry = self._session_of(at)
        exit_session = self.exit_session_for(strategy_id, style, entry, key=f"entry:{symbol}")
        rows = await self._corp_actions(entry + timedelta(days=1), exit_session)
        hits = sorted(_ex_date(r["ex_date"]) for r in rows if r["symbol"] == symbol)
        if not hits:
            return None
        hold = exit_session.isoformat() if exit_session is not None else "pending"
        return f"unadjusted corporate action on {hits[0].isoformat()} inside the hold ({entry.isoformat()}, {hold}]"

    async def _corp_recheck(self, today: date, now: datetime) -> None:
        """A held position voids at the pre-ex mark of the first ex-date in (entry session, today],
        however late it was found; one that closed today across today's ex-date is re-labelled VOID.
        A failed void re-arms the check for the next tick."""
        def entry(row: sqlite3.Row) -> date:
            return self._session_of(datetime.fromisoformat(row["opened_at"]))

        held = self._held()
        entered = {r["position_id"]: entry(r) for r in held}
        ex_dates: dict[str, set[date]] = {}
        for r in await self._corp_actions(min(entered.values(), default=today), today):
            ex_dates.setdefault(r["symbol"], set()).add(_ex_date(r["ex_date"]))

        failed = False
        for row in held:
            pid = row["position_id"]
            ex = min((d for d in ex_dates.get(row["symbol"], ()) if entered[pid] < d <= today), default=None)
            if ex is None:
                continue
            try:
                mark = await self._pre_ex_mark(row["symbol"], ex)
                await self._book.void(pid, mark, now, "corp_action_late")
            except Exception:
                failed = True
                _log.exception("paper_late_void_failed", position_id=pid)
                continue
            self._late_find(row, "void", mark, ex)
        for row in self._conn.execute(
            f"SELECT * FROM positions WHERE {_PAPER_POSITION} AND state = 'CLOSED' AND substr(closed_at, 1, 10) = ? "
            f"AND close_reason IS NOT 'void'",
            (today.isoformat(),),
        ).fetchall():
            if today in ex_dates.get(row["symbol"], ()) and entry(row) < today:
                mark = await self._pre_ex_mark(row["symbol"], today)
                self._relabel_void(row, mark)
                self._late_find(row, "relabel", mark, today)
        if failed:
            self._corp_checked_at = None

    def _late_find(self, row: sqlite3.Row, action: str, mark: Decimal | None, ex: date) -> None:
        """Halts latched before the late find stay latched: the owner judges a ``/paper reset``."""
        self.counters.late_corp_actions += 1
        self.counters.voids += 1
        halts = [r["cause"] for r in self._conn.execute(
            "SELECT cause FROM paper_halts WHERE cleared_at IS NULL ORDER BY set_at")]
        _log.warning("paper_corp_action_late", position_id=row["position_id"], symbol=row["symbol"], action=action,
                     ex_date=ex.isoformat(), mark=None if mark is None else str(mark), halts=halts)

    def _relabel_void(self, row: sqlite3.Row, mark: Decimal | None) -> None:
        """Re-price a closed position at ``mark`` (``avg_entry`` when unknown), cost-free, as VOID."""
        pid = row["position_id"]
        avg = Decimal(str(row["avg_entry"]))
        price = mark if mark is not None else avg
        qty = int(row["qty"])
        gross = (qty * (price - avg)).quantize(_PAISA, rounding=ROUND_HALF_UP)
        with transaction(self._conn):
            cur = self._conn.execute(
                f"UPDATE positions SET realized_pnl = ?, costs = '0.00', close_reason = 'void', "
                f"close_basis = 'corp_action_relabel' WHERE position_id = ? AND {_PAPER_POSITION} "
                f"AND state = 'CLOSED' AND close_reason IS NOT 'void'",
                (str(gross), pid),
            )
            if cur.rowcount != 1:
                raise LookupError(f"{pid} is not a closed paper position to re-label")
            self._conn.execute(
                f"UPDATE learning_ledger SET exit_px = ?, costs = '0.00', gross_pnl = ?, net_pnl = ?, "
                f"close_reason = 'void', outcome_label = 'void' WHERE position_id = ? AND {_PAPER}",
                (str(price.quantize(_PRICE)), str(gross), str(gross), pid),
            )
        _log.info("paper_position_relabelled_void", position_id=pid, was=row["close_reason"], gross=str(gross))

    # ---------------------------------------------------------------- step 6
    async def _sweep_closed_gtts(self) -> None:
        """No ACTIVE GTT outlives its position's close."""
        orphans = {
            r["gtt_id"]: r["position_id"]
            for r in self._conn.execute(
                f"SELECT g.gtt_id, g.position_id FROM gtts g JOIN positions p ON p.position_id = g.position_id "
                f"WHERE {scope_sql('paper', 'g')} AND {scope_sql('paper', 'p', has_origin=True)} "
                f"AND p.state = 'CLOSED'"
            )
        }
        for g in await self._broker.gtts():
            if g["status"] == "active" and g["id"] in orphans:
                _log.error("paper_gtt_active_after_close", gtt_id=g["id"], position_id=orphans[g["id"]])
                await self._delete_active(orphans[g["id"]], [g["id"]])

    # ---------------------------------------------------------------- rows
    def _in_session(self, now: datetime) -> Session | None:
        session = self._session_fn(now.date())
        return session if session is not None and session.open <= now < session.close else None

    def _held(self) -> list[sqlite3.Row]:
        return self._conn.execute(f"SELECT * FROM positions WHERE {_HELD} ORDER BY opened_at").fetchall()

    def _held_row(self, pid: str) -> sqlite3.Row | None:
        return self._conn.execute(f"SELECT * FROM positions WHERE position_id = ? AND {_HELD}", (pid,)).fetchone()

    def _working_sells(self, pid: str) -> list[sqlite3.Row]:
        return self._conn.execute(
            f"SELECT order_id FROM orders WHERE position_id = ? AND side = 'SELL' AND {_PAPER} AND {_WORKING}", (pid,)
        ).fetchall()

    def _gtt_ids(self, pid: str) -> set[int]:
        return {r["gtt_id"] for r in self._conn.execute(
            f"SELECT gtt_id FROM gtts WHERE position_id = ? AND {_PAPER}", (pid,))}

    def _broker_ids(self, pid: str) -> set[str]:
        return {r["broker_order_id"] for r in self._conn.execute(
            f"SELECT broker_order_id FROM orders WHERE position_id = ? AND broker_order_id IS NOT NULL AND {_PAPER}",
            (pid,))}

    async def _guarded(self, fn: Callable[..., Awaitable[None]], *args: Any) -> None:
        try:
            await fn(*args)
        except Exception:
            _log.exception("paper_exit_step_failed", step=fn.__name__, args=repr(args[:1]))
