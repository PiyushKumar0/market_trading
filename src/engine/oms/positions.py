"""Paper PositionBook (plan Q4.4): paper position rows and their learning-ledger rows.

* Paper only (D8): every insert carries ``is_paper=1`` (positions also ``origin='platform'``) and every
  UPDATE is guarded by the paper scope, so a real row is never touched.
* ``qty`` is the open quantity while held and the entry quantity once CLOSED. ``realized_pnl`` is gross
  and accrues per exit fill; ``costs`` are charged once, at close, on the entry notional.
* SQLite runs on the event-loop thread and no write spans an ``await``.
"""

from __future__ import annotations

import re
import sqlite3
from collections.abc import Callable
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

from ulid import ULID

from engine.core.clock import IST, Clock
from engine.core.contracts import EnterAction
from engine.core.db import transaction
from engine.core.log import get_logger
from engine.core.scope import HELD_STATES_SQL, scope_sql
from engine.oms.state import (
    ALLOWED_TRANSITIONS,
    TERMINAL_ORDER_STATES,
    CloseReason,
    OrderRole,
    OrderState,
    PlatformOrder,
    PositionState,
    ProtectionState,
    transition,
)
from engine.oms.store import OrderStore
from engine.paper.broker import TERMINAL_STATUSES, PaperBroker, PaperOrderError

_log = get_logger("engine.oms.positions")

_PAPER = scope_sql("paper")
_PAPER_POSITION = scope_sql("paper", has_origin=True)
_HELD = f"{_PAPER_POSITION} AND {HELD_STATES_SQL['paper']}"
_WORKING = "state NOT IN ({})".format(", ".join(f"'{s.value}'" for s in sorted(TERMINAL_ORDER_STATES)))
#: Leg index -> reason; leg 0 is the stop (``gtts.trigger_low``), leg 1 the target (Q4.2 restore order).
_LEG_REASON = {0: CloseReason.STOP, 1: CloseReason.TARGET}
_LEG_TAG = re.compile(r"gtt:(\d+):(\d+)")
_PRICE = Decimal("0.0001")
_PAISA = Decimal("0.01")

HoldFn = Callable[[str, str], int | None]
SessionOfFn = Callable[[datetime], date]
AddSessionsFn = Callable[[date, int], date]
RoundTripFn = Callable[[Decimal, str], Decimal]


class PositionNotHeld(LookupError):
    """No held paper position has that id."""


class Oversell(ValueError):
    """The sell exceeds the open quantity net of working sells: paper CNC never goes short."""


class CloseDeferred(RuntimeError):
    """A bookkeeping close booked nothing: something survived the cancel, or the broker reports a fill
    the OMS has not applied yet. The cancels already sent stand, so a later retry books cleanly."""


def _dec(value: Any) -> Decimal:
    return Decimal(0) if value is None or value == "" else Decimal(str(value))


def _money(value: Decimal) -> Decimal:
    return value.quantize(_PAISA, rounding=ROUND_HALF_UP)


def _iso(at: datetime) -> str:
    if at.tzinfo is None:
        raise ValueError(f"naive timestamp {at!r}")
    return at.astimezone(IST).isoformat()


def _leg(tag: Any) -> tuple[int, int] | None:
    m = _LEG_TAG.fullmatch(str(tag or ""))
    return None if m is None else (int(m.group(1)), int(m.group(2)))


class PositionBook:
    """Seams:

    * :meth:`on_fill` is the OrderManager's ``on_fill``.
    * :meth:`check_sell` runs immediately before ``OrderManager.submit_exit``, with no ``await`` between.
    * :meth:`mark_pending_exit` names the close an exit order's fill will book.
    * :meth:`close_bookkeeping` / :meth:`void` close without a broker order.
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        clock: Clock,
        broker: PaperBroker,
        *,
        hold_fn: HoldFn,
        session_of: SessionOfFn,
        add_sessions: AddSessionsFn,
        round_trip_fn: RoundTripFn,
    ) -> None:
        if not isinstance(broker, PaperBroker):
            raise TypeError(f"PositionBook takes a PaperBroker only, got {type(broker).__name__} (D8)")
        self._conn = conn
        self._clock = clock
        self._broker = broker
        self._hold_fn = hold_fn
        self._session_of = session_of
        self._add_sessions = add_sessions
        self._round_trip = round_trip_fn
        self._store = OrderStore(conn)

    # ---------------------------------------------------------------- fills
    def on_fill(self, order: PlatformOrder, qty: int, price: Decimal, at: datetime) -> None:
        if not order.is_paper or order.position_id is None:
            _log.error("paper_fill_outside_paper_scope", order_id=order.order_id)
        elif order.role is OrderRole.ENTRY and order.side == "BUY":
            self._entry_fill(order, qty, price, at)
        elif order.role in (OrderRole.EXIT, OrderRole.GTT_LEG) and order.side == "SELL":
            self._exit_fill(order, qty, price, at)
        else:
            _log.error("paper_fill_unhandled", order_id=order.order_id, role=order.role, side=order.side)

    def _entry_fill(self, order: PlatformOrder, qty: int, price: Decimal, at: datetime) -> None:
        position_id = order.position_id
        row = self._held(position_id)
        if row is None:
            if self._conn.execute(
                f"SELECT 1 FROM positions WHERE position_id = ? AND {_PAPER_POSITION}", (position_id,)
            ).fetchone():
                _log.error("paper_entry_fill_on_closed_position", order_id=order.order_id, qty=qty)
            else:
                self._open(order, qty, price, at)
            return
        held = int(row["qty"])
        avg = (_dec(row["avg_entry"]) * held + price * qty) / (held + qty)
        self._update(position_id, "qty = ?, avg_entry = ?", held + qty, str(avg.quantize(_PRICE)))

    def _open(self, order: PlatformOrder, qty: int, price: Decimal, at: datetime) -> None:
        proposal = self._proposal(order.proposal_id)
        exit_session = self._exit_session(proposal, at)
        self._conn.execute(
            "INSERT INTO positions (position_id, symbol, side, style, product, qty, avg_entry, stop, target, "
            "state, protection_state, is_paper, origin, opened_at, strategy_id, exit_session) "
            "VALUES (?, ?, 'BUY', ?, ?, ?, ?, ?, ?, ?, ?, 1, 'platform', ?, ?, ?)",
            (
                order.position_id, proposal.tradingsymbol, proposal.style, order.product, qty,
                str(price.quantize(_PRICE)), str(proposal.stop_price),
                None if proposal.target_price is None else str(proposal.target_price),
                PositionState.OPEN.value, ProtectionState.PROTECTION_PENDING.value, _iso(at),
                proposal.strategy_id, None if exit_session is None else exit_session.isoformat(),
            ),
        )
        _log.info("paper_position_opened", position_id=order.position_id, symbol=proposal.tradingsymbol,
                  qty=qty, exit_session=exit_session)

    def _exit_session(self, proposal: EnterAction, at: datetime) -> date | None:
        """Session N of the hold, the first fill's session being 1; None = pending. A failure here never
        costs the fill its position row."""
        try:
            n = self._hold_fn(proposal.strategy_id, proposal.style)
            if n is not None:
                return self._add_sessions(self._session_of(at), n - 1)
        except Exception as exc:
            _log.info("paper_exit_session_pending", proposal_id=proposal.proposal_id, error=repr(exc))
            return None
        _log.error("paper_hold_unknown", strategy_id=proposal.strategy_id, style=proposal.style)
        return None

    def _exit_fill(self, order: PlatformOrder, qty: int, price: Decimal, at: datetime) -> None:
        row = self._held(order.position_id)
        if row is None:
            _log.error("paper_exit_fill_without_held_position", order_id=order.order_id, qty=qty)
            return
        held = int(row["qty"])
        if qty > held:
            _log.error("paper_oversold_fill", order_id=order.order_id, qty=qty, held=held)
        sold = min(qty, held)
        realized = _dec(row["realized_pnl"]) + sold * (price - _dec(row["avg_entry"]))
        if sold < held:
            self._update(order.position_id, "qty = ?, realized_pnl = ?", held - sold, str(realized))
            return
        reason = row["close_reason"]
        if reason is None and order.role is OrderRole.GTT_LEG:
            leg = _leg((order.raw_broker_payload or {}).get("tag"))
            reason = _LEG_REASON.get(leg[1]) if leg else None
        if reason is None:
            _log.warning("paper_close_reason_unknown", order_id=order.order_id)
        self._close(row, realized, at, CloseReason(reason or CloseReason.EXTERNAL_UNKNOWN),
                    row["close_basis"] or "fill")

    # ---------------------------------------------------------------- exits
    def check_sell(self, position_id: str, qty: int) -> None:
        row = self._held(position_id)
        if row is None:
            raise PositionNotHeld(position_id)
        working = self._conn.execute(
            f"SELECT COALESCE(SUM(qty - filled_qty), 0) AS n FROM orders "
            f"WHERE position_id = ? AND side = 'SELL' AND {_PAPER} AND {_WORKING}",
            (position_id,),
        ).fetchone()["n"]
        free = int(row["qty"]) - int(working)
        if not 0 < qty <= free:
            raise Oversell(f"{position_id}: sell {qty} against {row['qty']} open, {working} already working")

    def mark_pending_exit(self, position_id: str, reason: CloseReason, basis: str = "fill") -> None:
        """The latest call names the close."""
        self._update(position_id, "state = ?, close_reason = ?, close_basis = ?",
                     PositionState.PENDING_EXIT.value, reason.value, basis)

    async def close_bookkeeping(
        self, position_id: str, price: Decimal, at: datetime, reason: CloseReason, basis: str
    ) -> None:
        """Book the open qty at ``price`` without a broker order, once the position's ACTIVE GTTs are
        deleted and its working orders (a fired leg included) cancelled, both verified at the broker."""
        if price <= 0:
            raise ValueError(f"close price {price} for {position_id}")
        if self._held(position_id) is None:
            raise PositionNotHeld(position_id)
        await self._clear_broker(position_id)
        row = self._held(position_id)
        if row is None:
            raise PositionNotHeld(position_id)
        realized = _dec(row["realized_pnl"]) + int(row["qty"]) * (price - _dec(row["avg_entry"]))
        self._close(row, realized, at, reason, basis)

    async def void(self, position_id: str, mark: Decimal | None, at: datetime, basis: str) -> None:
        """VOID at ``mark``: the paper ExposureTracker's ``mark_price(symbol)`` at the void, i.e. the mark
        its open MTM last carried (session prep voids before any tick is forwarded); None takes the
        tracker's own fallback, ``avg_entry``. A void charges no costs, so paper equity does not move."""
        row = self._held(position_id)
        if row is None:
            raise PositionNotHeld(position_id)
        price = mark if mark is not None else _dec(row["avg_entry"])
        await self.close_bookkeeping(position_id, price, at, CloseReason.VOID, basis)

    async def _clear_broker(self, position_id: str) -> None:
        gtt_ids = {
            r["gtt_id"]
            for r in self._conn.execute(f"SELECT gtt_id FROM gtts WHERE position_id = ? AND {_PAPER}", (position_id,))
        }
        known = self._broker_ids(position_id)

        def ours(book_row: dict[str, Any]) -> bool:
            leg = _leg(book_row.get("tag"))
            return book_row["order_id"] in known or (leg is not None and leg[0] in gtt_ids)

        async def armed() -> set[int]:
            return {g["id"] for g in await self._broker.gtts() if g["id"] in gtt_ids and g["status"] == "active"}

        deleted = await armed()
        for gtt_id in deleted:
            try:
                await self._broker.delete_gtt(gtt_id)
            except PaperOrderError as exc:
                _log.warning("paper_bookkeeping_delete_refused", gtt_id=gtt_id, error=str(exc))
        for book_row in await self._broker.orders():
            if ours(book_row) and book_row["status"] not in TERMINAL_STATUSES:
                await self._cancel(book_row["order_id"])

        book = [o for o in await self._broker.orders() if ours(o)]
        still_armed = await armed()
        now = _iso(self._clock.now())
        for gtt_id in deleted - still_armed:
            self._conn.execute(
                f"UPDATE gtts SET state = 'deleted', last_verified_at = ? WHERE gtt_id = ? AND {_PAPER}",
                (now, gtt_id),
            )
        working = [o["order_id"] for o in book if o["status"] not in TERMINAL_STATUSES]
        if working or still_armed:
            raise CloseDeferred(f"{position_id}: still working {working}, still armed {sorted(still_armed)}")
        applied = self._broker_ids(position_id)
        unapplied = [o["order_id"] for o in book if int(o["filled_quantity"]) > applied.get(o["order_id"], 0)]
        if unapplied:
            raise CloseDeferred(f"{position_id}: fills on {unapplied} not yet applied by the OMS")

    def _broker_ids(self, position_id: str) -> dict[str, int]:
        """Broker order id -> OMS ``filled_qty`` for the position's paper orders."""
        rows = self._conn.execute(
            f"SELECT broker_order_id, filled_qty FROM orders "
            f"WHERE position_id = ? AND broker_order_id IS NOT NULL AND {_PAPER}",
            (position_id,),
        ).fetchall()
        return {r["broker_order_id"]: int(r["filled_qty"] or 0) for r in rows}

    async def _cancel(self, broker_order_id: str) -> None:
        order = self._store.by_broker_id(broker_order_id)
        if order is not None and order.is_paper and OrderState.CANCEL_PENDING in ALLOWED_TRANSITIONS[order.state]:
            after, event = transition(
                order, OrderState.CANCEL_PENDING,
                payload={"platform_intent": "cancel", "reason": "bookkeeping_close"}, at=self._clock.now(),
            )
            self._store.record(order, after, event)
        try:
            await self._broker.cancel_order(broker_order_id)
        except PaperOrderError as exc:
            _log.warning("paper_bookkeeping_cancel_refused", broker_order_id=broker_order_id, error=str(exc))

    # ---------------------------------------------------------------- close
    def _close(self, row: sqlite3.Row, realized: Decimal, at: datetime, reason: CloseReason, basis: str) -> None:
        position_id = row["position_id"]
        entry = self._conn.execute(
            f"SELECT proposal_id, verdict_id, filled_qty FROM orders "
            f"WHERE position_id = ? AND role = 'entry' AND {_PAPER}",
            (position_id,),
        ).fetchone()
        if entry is None:
            raise LookupError(f"{position_id} has no paper entry order")
        proposal = self._proposal(entry["proposal_id"])
        qty = int(entry["filled_qty"])
        avg = _dec(row["avg_entry"])
        gross = _money(realized)
        void = reason is CloseReason.VOID
        costs = Decimal("0.00") if void else _money(self._round_trip(avg * qty, row["product"]))
        net = gross - costs
        outcome = "void" if void else "win" if net > 0 else "loss" if net < 0 else "scratch"
        opened = datetime.fromisoformat(row["opened_at"])
        closed_at = _iso(at)
        with transaction(self._conn):
            self._update(
                position_id,
                "state = ?, qty = ?, realized_pnl = ?, costs = ?, close_reason = ?, close_basis = ?, closed_at = ?",
                PositionState.CLOSED.value, qty, str(gross), str(costs), reason.value, basis, closed_at,
            )
            self._conn.execute(
                "INSERT INTO learning_ledger (entry_id, position_id, rec_id, is_paper, strategy_id, "
                "features_snapshot_id, thesis, confidence, agent_id, proposal_id, verdict_id, entry_px, exit_px, "
                "qty, costs, gross_pnl, net_pnl, holding_minutes, close_reason, outcome_label, created_at, "
                "closed_at) VALUES (?, ?, NULL, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    str(ULID()), position_id, proposal.strategy_id, proposal.features_snapshot_id,
                    proposal.thesis, proposal.confidence, proposal.agent_id, entry["proposal_id"],
                    entry["verdict_id"], str(avg), str((avg + gross / qty).quantize(_PRICE)), qty, str(costs),
                    str(gross), str(net), int((at - opened).total_seconds() // 60), reason.value, outcome,
                    row["opened_at"], closed_at,
                ),
            )
        _log.info("paper_position_closed", position_id=position_id, reason=reason.value, basis=basis,
                  gross=str(gross), costs=str(costs), net=str(net), outcome=outcome)

    # ---------------------------------------------------------------- rows
    def _held(self, position_id: str) -> sqlite3.Row | None:
        return self._conn.execute(
            f"SELECT * FROM positions WHERE position_id = ? AND {_HELD}", (position_id,)
        ).fetchone()

    def _update(self, position_id: str, assignments: str, *params: Any) -> None:
        cur = self._conn.execute(
            f"UPDATE positions SET {assignments} WHERE position_id = ? AND {_HELD}", (*params, position_id)
        )
        if cur.rowcount != 1:
            raise PositionNotHeld(position_id)

    def _proposal(self, proposal_id: str | None) -> EnterAction:
        row = self._conn.execute("SELECT payload FROM proposals WHERE proposal_id = ?", (proposal_id,)).fetchone()
        if row is None:
            raise LookupError(f"no proposal {proposal_id}")
        return EnterAction.model_validate_json(row["payload"])
