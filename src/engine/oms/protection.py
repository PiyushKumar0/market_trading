"""Paper ProtectionManager (plan Q4.5): one GTT per open paper position, and what follows its trigger.

Per held paper position:

* ACTIVE: an active GTT for the open qty (``protection_state`` PROTECTED).
* EXIT_WORKING: the GTT triggered and its leg works; the position is PENDING_EXIT.
* PROTECTION_FAILED: a ``failed`` GTT, or re-placements or exit retries exhausted; PAPER_ALERT goes out.
* closed.

Every exit goes through the injected Q4.6 exit routine, only in session and only once the broker book
is settled into the OMS; otherwise it queues for the next :meth:`ProtectionManager.tick`.
SQLite runs on the event-loop thread and no write spans an ``await``.
"""

from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Protocol

from engine.core.clock import Clock
from engine.core.config import PaperSettings
from engine.core.log import get_logger
from engine.core.scope import HELD_STATES_SQL, scope_sql
from engine.core.types import Session
from engine.oms.manager import OrderManager
from engine.oms.positions import LEG_REASON, PositionBook, leg_of
from engine.oms.state import (
    TERMINAL_ORDER_STATES,
    CloseReason,
    OrderEvent,
    OrderRole,
    OrderState,
    PlatformOrder,
    PositionState,
    ProtectionState,
)
from engine.paper.broker import TERMINAL_STATUSES, PaperBroker

_log = get_logger("engine.oms.protection")

_PAPER = scope_sql("paper")
_HELD = f"{scope_sql('paper', has_origin=True)} AND {HELD_STATES_SQL['paper']}"
_WORKING = "state NOT IN ({})".format(", ".join(f"'{s.value}'" for s in sorted(TERMINAL_ORDER_STATES)))
_ENDED = frozenset({OrderState.CANCELLED, OrderState.REJECTED, OrderState.LAPSED})

#: GTT re-placements, and exit retries per session, before PROTECTION_FAILED.
MAX_RETRIES = 3


class ExitFn(Protocol):
    """The Q4.6 exit routine. Sends nothing while the position has a working exit; marks it PENDING_EXIT."""

    def __call__(self, position_id: str, reason: CloseReason, basis: str) -> Awaitable[None]: ...


#: ``notify(key, message)``: PAPER_ALERT, never the real alert sink; ``key`` is ``<position_id>:<attempt>``.
PaperNotify = Callable[[str, str], Awaitable[None]]
SessionFn = Callable[[date], Session | None]
LtpFn = Callable[[str], Decimal | None]
RoundTickFn = Callable[[str, Decimal], Decimal]


@dataclass(frozen=True)
class _Queued:
    reason: CloseReason
    basis: str
    after: date | None = None


def _dec(value: Any) -> Decimal | None:
    return None if value is None or value == "" else Decimal(str(value))


class ProtectionManager:
    """Seams: :meth:`on_fill` / :meth:`on_transition` are the OrderManager's; :meth:`tick` runs from
    ``paper_tick``; :meth:`verify_all` from each reconcile pass."""

    def __init__(
        self,
        conn: sqlite3.Connection,
        clock: Clock,
        broker: PaperBroker,
        *,
        orders: OrderManager,
        book: PositionBook,
        exit_fn: ExitFn,
        notify: PaperNotify,
        session_fn: SessionFn,
        ltp_fn: LtpFn,
        round_tick_fn: RoundTickFn,
        gtt_limit_offset_pct: Decimal,
        settings: PaperSettings,
    ) -> None:
        if not isinstance(broker, PaperBroker):
            raise TypeError(f"ProtectionManager takes a PaperBroker only, got {type(broker).__name__} (D8)")
        self._conn = conn
        self._clock = clock
        self._broker = broker
        self._orders = orders
        self._book = book
        self._exit_fn = exit_fn
        self._notify = notify
        self._session_fn = session_fn
        self._ltp_fn = ltp_fn
        self._round_tick = round_tick_fn
        self._offset = Decimal(str(gtt_limit_offset_pct)) / 100
        self._leg_timeout = timedelta(minutes=settings.exit_working_timeout_min)
        self._close_buffer = timedelta(minutes=settings.exit_minutes_before_close)
        self._lock = asyncio.Lock()
        self._repairs: dict[str, int] = {}
        self._exit_failures: dict[str, tuple[date, int]] = {}
        self._alerts: dict[str, int] = {}
        self._queued: dict[str, _Queued] = {}

    # ---------------------------------------------------------------- seams
    async def on_fill(self, order: PlatformOrder, qty: int, price: Decimal, at: datetime) -> None:
        """The position accretes first, then protection follows the new qty."""
        try:
            self._book.on_fill(order, qty, price, at)
        finally:
            async with self._lock:
                await self._verify_all()

    async def on_transition(self, order: PlatformOrder, event: OrderEvent) -> None:
        if not order.is_paper or order.position_id is None or order.side != "SELL":
            return
        async with self._lock:
            if event.to_state in TERMINAL_ORDER_STATES:
                await self._sell_ended(order, event.to_state)
            elif order.role is OrderRole.GTT_LEG:
                await self._verify_all()

    async def tick(self) -> None:
        async with self._lock:
            now = self._clock.now()
            session = self._session(now)
            held = self._held()
            for pid in set(self._queued) - {r["position_id"] for r in held}:
                del self._queued[pid]
            if session is not None:
                today = date.fromisoformat(session.date_ist)
                for pid, queued in list(self._queued.items()):
                    if queued.after is None or queued.after < today:
                        del self._queued[pid]
                        await self._exit(pid, queued.reason, queued.basis)
            for row in held:
                if row["state"] == PositionState.PENDING_EXIT.value:
                    await self._guarded(self._watch_exit, row, session, now)
            await self._verify_all()

    async def verify_all(self) -> None:
        async with self._lock:
            await self._verify_all()

    # ---------------------------------------------------------------- protection
    async def _verify_all(self) -> None:
        held = self._held()
        ids = {r["position_id"] for r in held}
        for counters in (self._repairs, self._exit_failures, self._alerts):
            for pid in set(counters) - ids:
                del counters[pid]
        book = {g["id"]: g for g in await self._broker.gtts()}
        for row in held:
            if row["state"] == PositionState.OPEN.value:
                await self._guarded(self._protect, row, book)

    async def _protect(self, row: sqlite3.Row, book: dict[int, dict[str, Any]]) -> None:
        pid = row["position_id"]
        if pid in self._queued:
            return
        gtt = self._conn.execute(
            f"SELECT * FROM gtts WHERE position_id = ? AND {_PAPER} ORDER BY gtt_id DESC LIMIT 1", (pid,)
        ).fetchone()
        live = book.get(gtt["gtt_id"]) if gtt is not None else None
        status = None if live is None else live["status"]
        if status == "triggered":
            await self._triggered(row, gtt["gtt_id"])
            return
        if status == "failed":
            self._gtt_state(gtt["gtt_id"], "failed")
            await self._fail(pid, f"GTT {gtt['gtt_id']} failed at the broker", exit_now=True)
            return
        if status == "active" and live["orders"][0]["quantity"] == row["qty"]:
            self._gtt_state(gtt["gtt_id"], "active")
            self._protection(pid, ProtectionState.PROTECTED)
            return
        if gtt is not None and status is None and gtt["state"] == "active":
            _log.error("paper_gtt_missing", position_id=pid, gtt_id=gtt["gtt_id"])
            self._gtt_state(gtt["gtt_id"], "missing")
            self._repairs[pid] = self._repairs.get(pid, 0) + 1
        stop, avg, ltp = _dec(row["stop"]), _dec(row["avg_entry"]), self._ltp_fn(row["symbol"])
        if stop is None or avg is None:
            await self._fail(pid, "position has no stop or entry price", exit_now=True)
            return
        if avg <= stop or (ltp is not None and ltp <= stop):
            _log.warning("paper_stop_gap_through", position_id=pid, stop=str(stop), avg=str(avg), ltp=str(ltp))
            await self._exit(pid, CloseReason.STOP, "stop_gap_through")
            return
        if self._repairs.get(pid, 0) > MAX_RETRIES:
            await self._fail(pid, f"GTT lost or refused {self._repairs[pid]} times", exit_now=True)
            return
        try:
            if status == "active":
                await self._modify(row, gtt["gtt_id"], stop, avg)
            else:
                await self._place(row, stop, avg)
        except Exception:
            self._repairs[pid] = self._repairs.get(pid, 0) + 1
            _log.exception("paper_gtt_arm_failed", position_id=pid, repairs=self._repairs[pid])
            if self._repairs[pid] > MAX_RETRIES:
                await self._fail(pid, f"GTT lost or refused {self._repairs[pid]} times", exit_now=True)

    def _request(self, row: sqlite3.Row, stop: Decimal, avg: Decimal) -> tuple[dict[str, Any], Decimal, Decimal | None]:
        """trigger = stop, limit = stop less the offset on the tick; ``last_price`` = ``avg_entry``, so a
        stop below and a target above the entry keep their directions."""
        target = _dec(row["target"])
        if target is not None and target <= avg:
            _log.warning("paper_target_leg_dropped", position_id=row["position_id"], target=str(target), avg=str(avg))
            target = None
        limit = self._round_tick(row["symbol"], stop * (1 - self._offset))
        legs = [(stop, limit)] + ([] if target is None else [(target, target)])
        request = {
            "tradingsymbol": row["symbol"],
            "exchange": "NSE",
            "last_price": avg,
            "trigger_values": [trigger for trigger, _ in legs],
            "orders": [
                {"transaction_type": "SELL", "quantity": row["qty"], "product": row["product"],
                 "order_type": "LIMIT", "price": price}
                for _, price in legs
            ],
        }
        return request, limit, target

    async def _place(self, row: sqlite3.Row, stop: Decimal, avg: Decimal) -> None:
        request, limit, target = self._request(row, stop, avg)
        gtt_id = await self._broker.place_gtt(request)
        now = self._clock.now().isoformat()
        self._conn.execute(
            "INSERT INTO gtts (gtt_id, position_id, state, trigger_low, trigger_high, last_verified_at, is_paper, "
            "symbol, side, product, qty, stop_limit, target_limit, last_price, created_at) "
            "VALUES (?, ?, 'active', ?, ?, ?, 1, ?, 'SELL', ?, ?, ?, ?, ?, ?)",
            (gtt_id, row["position_id"], str(stop), None if target is None else str(target), now, row["symbol"],
             row["product"], row["qty"], str(limit), None if target is None else str(target), str(avg), now),
        )
        self._protection(row["position_id"], ProtectionState.PROTECTED)
        _log.info("paper_gtt_placed", position_id=row["position_id"], gtt_id=gtt_id, qty=row["qty"],
                  stop=str(stop), limit=str(limit), target=str(target))

    async def _modify(self, row: sqlite3.Row, gtt_id: int, stop: Decimal, avg: Decimal) -> None:
        request, _limit, target = self._request(row, stop, avg)
        await self._broker.modify_gtt(gtt_id, request)
        self._conn.execute(
            f"UPDATE gtts SET qty = ?, last_price = ?, trigger_high = ?, target_limit = ?, last_verified_at = ? "
            f"WHERE gtt_id = ? AND {_PAPER}",
            (row["qty"], str(avg), None if target is None else str(target), None if target is None else str(target),
             self._clock.now().isoformat(), gtt_id),
        )
        self._protection(row["position_id"], ProtectionState.PROTECTED)
        _log.info("paper_gtt_modified", position_id=row["position_id"], gtt_id=gtt_id, qty=row["qty"])

    async def _triggered(self, row: sqlite3.Row, gtt_id: int) -> None:
        """Never re-place and never a second exit while the leg works; a spent GTT hands the open
        qty to the exit routine."""
        pid = row["position_id"]
        self._gtt_state(gtt_id, "triggered")
        legs = [o for o in await self._broker.orders() if (leg_of(o.get("tag")) or (None,))[0] == gtt_id]
        working = [o for o in legs if o["status"] not in TERMINAL_STATUSES]
        await self._cancel_entries(pid)
        if not working:
            await self._exit(pid, CloseReason.GTT_FAILURE_EXIT, "gtt_spent")
            return
        index = leg_of(working[0]["tag"])[1]
        self._book.mark_pending_exit(pid, LEG_REASON.get(index, CloseReason.STOP))
        _log.info("paper_exit_working", position_id=pid, gtt_id=gtt_id, leg=index)

    async def _cancel_entries(self, pid: str) -> None:
        rows = self._conn.execute(
            f"SELECT broker_order_id FROM orders WHERE position_id = ? AND role = 'entry' AND {_PAPER} "
            f"AND {_WORKING} AND broker_order_id IS NOT NULL",
            (pid,),
        ).fetchall()
        for r in rows:
            await self._orders.cancel(r["broker_order_id"], reason="gtt_triggered")

    # ---------------------------------------------------------------- exits
    async def _sell_ended(self, order: PlatformOrder, ended: OrderState) -> None:
        """Whenever an exit or a leg ends with qty still open, the position re-enters the exit routine;
        an exit the platform did not cancel that ended short counts as a failure."""
        pid = order.position_id
        row = self._held_row(pid)
        if row is None or self._working_sells(pid):
            return
        if order.role is OrderRole.GTT_LEG:
            leg = leg_of((order.raw_broker_payload or {}).get("tag"))
            if ended is OrderState.FILLED:
                await self._exit(pid, LEG_REASON.get(leg[1] if leg else 0, CloseReason.STOP), "leg_residual")
            else:
                await self._exit(pid, CloseReason.GTT_FAILURE_EXIT, f"leg_{ended.value.lower()}")
        elif ended is OrderState.FILLED:
            await self._exit(pid, *self._exit_reason(row))
        elif ended in _ENDED and not self._orders.cancel_intended(order.order_id):
            await self._exit_failed(pid, *self._exit_reason(row))

    async def _watch_exit(self, row: sqlite3.Row, session: Session | None, now: datetime) -> None:
        pid = row["position_id"]
        if pid in self._queued:
            return
        working = self._working_sells(pid)
        if not working:
            if await self._settled(pid):
                await self._exit_failed(pid, *self._exit_reason(row), stalled=True)
            return
        if session is None:
            return
        for order in working:
            created = order["created_at"]
            overdue = created is not None and now - datetime.fromisoformat(created) >= self._leg_timeout
            if order["role"] == OrderRole.GTT_LEG.value and (overdue or now >= session.close - self._close_buffer):
                if await self._orders.cancel(order["broker_order_id"], reason="leg_timeout"):
                    _log.info("paper_leg_cancelled", position_id=pid, broker_order_id=order["broker_order_id"])

    async def _exit(self, pid: str, reason: CloseReason, basis: str) -> None:
        if self._session(self._clock.now()) is None or not await self._settled(pid):
            self._queued.setdefault(pid, _Queued(reason, basis))
            return
        try:
            await self._exit_fn(pid, reason, basis)
        except Exception:
            _log.exception("paper_exit_routine_failed", position_id=pid, reason=reason.value, basis=basis)
            await self._exit_failed(pid, reason, basis)

    async def _exit_failed(self, pid: str, reason: CloseReason, basis: str, *, stalled: bool = False) -> None:
        today = self._clock.now().date()
        day, failures = self._exit_failures.get(pid, (today, 0))
        failures = (failures if day == today else 0) + 1
        self._exit_failures[pid] = (today, failures)
        _log.warning("paper_exit_failed", position_id=pid, failures=failures, stalled=stalled)
        if failures <= MAX_RETRIES:
            await self._exit(pid, reason, basis)
            return
        self._queued[pid] = _Queued(reason, basis, after=today)
        await self._fail(pid, f"exit failed {failures} times today; retried next session", exit_now=False)

    async def _fail(self, pid: str, why: str, *, exit_now: bool) -> None:
        self._protection(pid, ProtectionState.PROTECTION_FAILED)
        attempt = self._alerts[pid] = self._alerts.get(pid, 0) + 1
        _log.error("paper_protection_failed", position_id=pid, why=why)
        try:
            await self._notify(f"{pid}:{attempt}", f"paper protection failed for {pid}: {why}")
        except Exception:
            _log.exception("paper_alert_failed", position_id=pid)
        if exit_now:
            await self._exit(pid, CloseReason.GTT_FAILURE_EXIT, "protection_failed")

    def _exit_reason(self, row: sqlite3.Row) -> tuple[CloseReason, str]:
        reason = row["close_reason"]
        return CloseReason(reason) if reason else CloseReason.GTT_FAILURE_EXIT, row["close_basis"] or "fill"

    async def _settled(self, pid: str) -> bool:
        """Every broker order of the position is in the OMS with all its fills applied: an exit sized
        before a queued leg or fill lands would oversell."""
        gtt_ids = {r["gtt_id"] for r in self._conn.execute(
            f"SELECT gtt_id FROM gtts WHERE position_id = ? AND {_PAPER}", (pid,))}
        applied = {r["broker_order_id"]: int(r["filled_qty"] or 0) for r in self._conn.execute(
            f"SELECT broker_order_id, filled_qty FROM orders WHERE position_id = ? AND broker_order_id IS NOT NULL "
            f"AND {_PAPER}", (pid,))}
        for o in await self._broker.orders():
            leg = leg_of(o.get("tag"))
            if o["order_id"] in applied:
                if int(o["filled_quantity"]) > applied[o["order_id"]]:
                    return False
            elif leg is not None and leg[0] in gtt_ids:
                return False
        return True

    # ---------------------------------------------------------------- rows
    def _session(self, now: datetime) -> Session | None:
        session = self._session_fn(now.date())
        return session if session is not None and session.open <= now < session.close else None

    def _held(self) -> list[sqlite3.Row]:
        return self._conn.execute(f"SELECT * FROM positions WHERE {_HELD} ORDER BY opened_at").fetchall()

    def _held_row(self, pid: str) -> sqlite3.Row | None:
        return self._conn.execute(f"SELECT * FROM positions WHERE position_id = ? AND {_HELD}", (pid,)).fetchone()

    def _working_sells(self, pid: str) -> list[sqlite3.Row]:
        return self._conn.execute(
            f"SELECT * FROM orders WHERE position_id = ? AND side = 'SELL' AND {_PAPER} AND {_WORKING}", (pid,)
        ).fetchall()

    def _protection(self, pid: str, state: ProtectionState) -> None:
        self._conn.execute(
            f"UPDATE positions SET protection_state = ? WHERE position_id = ? AND {_HELD} "
            f"AND protection_state IS NOT ?",
            (state.value, pid, state.value),
        )

    def _gtt_state(self, gtt_id: int, state: str) -> None:
        self._conn.execute(
            f"UPDATE gtts SET state = ?, last_verified_at = ? WHERE gtt_id = ? AND {_PAPER}",
            (state, self._clock.now().isoformat(), gtt_id),
        )

    async def _guarded(self, fn: Callable[..., Awaitable[None]], row: sqlite3.Row, *args: Any) -> None:
        try:
            await fn(row, *args)
        except Exception:
            _log.exception("paper_protection_step_failed", step=fn.__name__, position_id=row["position_id"])
