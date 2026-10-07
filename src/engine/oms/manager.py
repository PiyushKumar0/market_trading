"""Paper OrderManager (plan Q4.3): submits paper orders and applies paper postbacks.

* Paper only (D8): the broker must be a ``PaperBroker`` publishing on ``PAPER_ORDER_UPDATE_TOPIC``.
* SQLite runs on the event-loop thread and no write spans an ``await``; callbacks run after commit.
* One consumer task applies every postback in arrival order and awaits its callbacks before the next,
  so a callback never sees a later fill's state and two fills never race into two protections.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import re
import sqlite3
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from ulid import ULID

from engine.core.clock import Clock
from engine.core.contracts import PAPER_ORDER_UPDATE_TOPIC, EnterAction, GateVerdict, OrderUpdateFrame
from engine.core.enums import RiskState
from engine.core.eventbus import EventBus
from engine.core.log import get_logger
from engine.core.scope import HELD_STATES_SQL, scope_sql
from engine.oms.correlation import PendingCorrelation
from engine.oms.state import (
    ALLOWED_TRANSITIONS,
    OrderEvent,
    OrderRole,
    OrderState,
    PlatformOrder,
    correlate,
    transition,
)
from engine.oms.store import OrderStore
from engine.oms.updates import NOOP_FLAGS, BrokerUpdate, apply_update, parse_postback
from engine.paper.broker import TERMINAL_STATUSES, PaperBroker, PaperOrderError

_log = get_logger("engine.oms.manager")

_LEG_TAG = re.compile(r"gtt:(\d+):(\d+)")
_PRICE_QUANTUM = Decimal("0.0001")
_PENDING_MAX_AGE_S = 300.0

OrderGuard = Callable[[str], None]
OnFill = Callable[[PlatformOrder, int, Decimal, datetime], Awaitable[None] | None]
OnTransition = Callable[[PlatformOrder, OrderEvent], Awaitable[None] | None]
#: ``sell_check(position_id, qty)`` raises unless the SELL fits the position (``PositionBook.check_sell``).
SellCheck = Callable[[str, int], None]


class SubmitRefused(ValueError):
    """Outside paper scope or an unusable proposal, verdict or position; nothing was persisted."""


class PaperOrderBlocked(RuntimeError):
    """The paper order guard refused the call."""


def paper_order_guard(
    paper_enabled_fn: Callable[[], bool], effective_state_fn: Callable[[], RiskState]
) -> OrderGuard:
    """An entry needs paper enabled and the effective paper state NORMAL; ``risk_reducing`` always
    passes. Install the same guard on the PaperBroker and the OrderManager."""

    def guard(intent: str) -> None:
        if intent == "risk_reducing":
            return
        if intent != "entry":
            raise PaperOrderBlocked(f"unknown order intent {intent!r}")
        if not paper_enabled_fn():
            raise PaperOrderBlocked("paper is off")
        state = effective_state_fn()
        if state != RiskState.NORMAL:
            raise PaperOrderBlocked(f"effective paper state is {state}")

    return guard


@dataclass(frozen=True)
class _Frame:
    data: dict[str, Any]
    at: datetime


@dataclass(frozen=True)
class _Resolve:
    order_id: str
    broker_order_id: str


@dataclass(frozen=True)
class _Applied:
    order: PlatformOrder
    event: OrderEvent
    fill_qty: int
    fill_price: Decimal | None
    fill_at: datetime


def _new_id() -> str:
    return str(ULID())


def _request(order: PlatformOrder, symbol: str) -> dict[str, Any]:
    return {
        "variety": "regular",
        "exchange": "NSE",
        "tradingsymbol": symbol,
        "transaction_type": order.side,
        "order_type": "MARKET" if order.price is None else "LIMIT",
        "product": order.product,
        "quantity": order.qty,
        "price": order.price,
    }


def _fill_price(before: PlatformOrder, upd: BrokerUpdate) -> Decimal | None:
    """This fill's own price, from the broker's cumulative average before and after it."""
    avg = upd.average_price
    if avg is None or avg <= 0:
        return None
    if before.filled_qty == 0:
        return avg.quantize(_PRICE_QUANTUM)
    raw = before.raw_broker_payload or {}
    prev_avg = raw.get("average_price")
    if prev_avg is None or raw.get("filled_quantity") != before.filled_qty:
        _log.warning("paper_fill_price_from_cumulative_average", order_id=before.order_id)
        return avg.quantize(_PRICE_QUANTUM)
    value = avg * upd.filled_qty - Decimal(str(prev_avg)) * before.filled_qty
    return (value / (upd.filled_qty - before.filled_qty)).quantize(_PRICE_QUANTUM)


async def _call(name: str, fn: Callable[..., Awaitable[None] | None], *args: Any) -> None:
    try:
        result = fn(*args)
        if inspect.isawaitable(result):
            await result
    except Exception:
        _log.exception("paper_order_callback_failed", callback=name)


class OrderManager:
    """Paper entry and exit submission, and the one postback consumer.

    Seams:

    * ``order_guard``: the guard also installed on ``broker`` (both from :func:`paper_order_guard`).
    * ``sell_check``: run by :meth:`submit_exit` before the order exists.
    * ``on_fill(order, qty, price, at)``: each fill; ``order`` is committed, ``price`` is this fill's.
    * ``on_transition(order, event)``: each committed postback transition, after its ``on_fill``.
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        clock: Clock,
        broker: PaperBroker,
        *,
        order_guard: OrderGuard,
        sell_check: SellCheck,
        on_fill: OnFill,
        on_transition: OnTransition | None = None,
    ) -> None:
        if not isinstance(broker, PaperBroker):
            raise TypeError(f"OrderManager takes a PaperBroker only, got {type(broker).__name__} (D8)")
        if broker.topic != PAPER_ORDER_UPDATE_TOPIC:
            raise ValueError(f"the paper broker publishes on {broker.topic!r}, not {PAPER_ORDER_UPDATE_TOPIC!r}")
        self._conn = conn
        self._clock = clock
        self._broker = broker
        self._guard = order_guard
        self._sell_check = sell_check
        self._on_fill = on_fill
        self._on_transition = on_transition
        self._store = OrderStore(conn)
        self._pending = PendingCorrelation()
        self._queue: asyncio.Queue[_Frame | _Resolve] = asyncio.Queue()
        self._bus: EventBus | None = None
        self._task: asyncio.Task[None] | None = None

    # ---------------------------------------------------------------- lifecycle
    def start(self, bus: EventBus) -> None:
        if self._task is not None:
            raise RuntimeError("OrderManager already started")
        bus.subscribe(PAPER_ORDER_UPDATE_TOPIC, self._on_frame)
        self._bus = bus
        self._task = asyncio.get_running_loop().create_task(self._consume(), name="paper-order-consumer")

    async def stop(self) -> None:
        if self._bus is not None:
            self._bus.unsubscribe(PAPER_ORDER_UPDATE_TOPIC, self._on_frame)
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        self._bus = self._task = None

    async def join(self) -> None:
        """Wait until every queued postback has been applied and its callbacks have returned.
        Never await this from a callback: it would wait on itself."""
        await self._queue.join()

    # ---------------------------------------------------------------- submission
    async def submit(self, proposal: EnterAction, verdict: GateVerdict) -> PlatformOrder:
        """Place the paper CNC entry for an approved paper verdict. A repeat for the same verdict
        returns the existing order without a broker call."""
        self._require_started()
        qty = self._entry_qty(proposal, verdict)
        now = self._clock.now()
        order = PlatformOrder(
            order_id=_new_id(),
            position_id=_new_id(),
            proposal_id=proposal.proposal_id,
            verdict_id=verdict.verdict_id,
            role=OrderRole.ENTRY,
            is_paper=True,
            state=OrderState.DRAFT,
            product="CNC",
            side="BUY",
            qty=qty,
            price=proposal.entry_price if proposal.entry_type == "LIMIT" else None,
            created_at=now,
            updated_at=now,
        )
        try:
            self._store.insert(order)
        except sqlite3.IntegrityError:
            existing = self._entry_order(verdict.verdict_id)
            if existing is None:
                raise
            _log.info("paper_entry_duplicate_submit", verdict_id=verdict.verdict_id, order_id=existing.order_id)
            return existing
        return await self._send(order, proposal.tradingsymbol)

    async def submit_exit(self, position_id: str, qty: int, *, price: Decimal | None = None) -> PlatformOrder:
        """Place a paper CNC SELL against a held paper position: MARKET, or LIMIT at ``price``. Whatever
        ``sell_check`` raises (an oversell) propagates with nothing persisted."""
        self._require_started()
        row = self._conn.execute(
            f"SELECT symbol, product FROM positions WHERE position_id = ? "
            f"AND {scope_sql('paper', has_origin=True)} AND {HELD_STATES_SQL['paper']}",
            (position_id,),
        ).fetchone()
        if row is None or row["product"] != "CNC":
            raise SubmitRefused(f"{position_id} is not a held paper CNC position")
        if qty <= 0 or (price is not None and price <= 0):
            raise SubmitRefused(f"exit qty {qty} / price {price} for {position_id}")
        self._sell_check(position_id, qty)
        now = self._clock.now()
        order = PlatformOrder(
            order_id=_new_id(),
            position_id=position_id,
            role=OrderRole.EXIT,
            is_paper=True,
            state=OrderState.DRAFT,
            product="CNC",
            side="SELL",
            qty=qty,
            price=price,
            created_at=now,
            updated_at=now,
        )
        self._store.insert(order)
        return await self._send(order, row["symbol"])

    async def cancel(self, broker_order_id: str, *, reason: str) -> bool:
        """Cancel a paper order at the broker, its OMS row (if any yet) first moved to CANCEL_PENDING
        with the platform's intent. True once the broker reports the order terminal, which includes
        filled or rejected before the cancel landed; the OMS row follows on its postback."""
        order = self._store.by_broker_id(broker_order_id)
        if order is not None:
            if not order.is_paper:
                raise SubmitRefused(f"broker order {broker_order_id} is a real order")
            if OrderState.CANCEL_PENDING in ALLOWED_TRANSITIONS[order.state]:
                self._move(order, OrderState.CANCEL_PENDING, {"platform_intent": "cancel", "reason": reason})
        try:
            await self._broker.cancel_order(broker_order_id)
        except PaperOrderError as exc:
            _log.warning("paper_cancel_refused", broker_order_id=broker_order_id, reason=reason, error=str(exc))
        status = next((o["status"] for o in await self._broker.orders() if o["order_id"] == broker_order_id), None)
        if status not in TERMINAL_STATUSES:
            _log.error("paper_cancel_unverified", broker_order_id=broker_order_id, reason=reason, status=status)
            return False
        return True

    def cancel_intended(self, order_id: str) -> bool:
        """Whether :meth:`cancel` ever recorded a platform cancel intent on this order."""
        return any(e.payload.get("platform_intent") == "cancel" for e in self._store.events(order_id))

    def _entry_qty(self, proposal: EnterAction, verdict: GateVerdict) -> int:
        if not isinstance(proposal, EnterAction):
            raise SubmitRefused(f"paper submits entries only, got {type(proposal).__name__}")
        if proposal.style == "intraday":
            raise SubmitRefused("MIS is out of paper scope (plan §1.3)")
        if proposal.side != "BUY":
            raise SubmitRefused("paper CNC entries are BUY only")
        if proposal.entry_type == "LIMIT" and (proposal.entry_price is None or proposal.entry_price <= 0):
            raise SubmitRefused("a LIMIT entry needs a positive entry_price")
        if verdict.verdict not in ("approve", "shrink") or verdict.proposal_id != proposal.proposal_id:
            raise SubmitRefused(f"verdict {verdict.verdict_id} does not approve {proposal.proposal_id}")
        qty = verdict.approved_qty
        if qty is None or not 0 < qty <= proposal.quantity:
            raise SubmitRefused(f"approved qty {qty} for proposal qty {proposal.quantity}")
        paper_verdict = self._conn.execute(
            f"SELECT 1 FROM verdicts WHERE verdict_id = ? AND {scope_sql('paper')}", (verdict.verdict_id,)
        ).fetchone()
        if paper_verdict is None:
            raise SubmitRefused(f"verdict {verdict.verdict_id} is not a persisted paper verdict")
        return qty

    def _entry_order(self, verdict_id: str) -> PlatformOrder | None:
        row = self._conn.execute(
            "SELECT order_id FROM orders WHERE verdict_id = ? AND role = 'entry'", (verdict_id,)
        ).fetchone()
        return None if row is None else self._store.get(row["order_id"])

    def _require_started(self) -> None:
        if self._task is None:
            raise RuntimeError("OrderManager is not started: its postbacks would never be applied")

    async def _send(self, order: PlatformOrder, symbol: str) -> PlatformOrder:
        intent = "entry" if order.role is OrderRole.ENTRY else "risk_reducing"
        try:
            self._guard(intent)
        except Exception as exc:
            _log.warning("paper_order_abandoned", order_id=order.order_id, reason=str(exc))
            abandon = {"platform_intent": "abandon", "reject_reason": str(exc)}
            return self._move(order, OrderState.REJECTED, abandon)
        order = self._move(order, OrderState.VALIDATED, {"platform_intent": "validate", "intent": intent})
        order = self._move(order, OrderState.SUBMITTED, {"platform_intent": "submit", "intent": intent})
        try:
            broker_order_id = await self._broker.place_order(_request(order, symbol), intent=intent)
        except Exception as exc:
            _log.warning("paper_order_refused", order_id=order.order_id, error=repr(exc))
            refused = {"platform_intent": "place", "reject_reason": f"{type(exc).__name__}: {exc}"}
            return self._move(self._reload(order), OrderState.REJECTED, refused)
        current = self._reload(order)
        bound = correlate(current, broker_order_id)
        event = OrderEvent(
            from_state=current.state,
            to_state=current.state,
            payload={"platform_intent": "correlate", "broker_order_id": broker_order_id},
            at=self._clock.now(),
        )
        self._store.record(current, bound, event)
        if self._pending.pending(broker_order_id):
            self._queue.put_nowait(_Resolve(bound.order_id, broker_order_id))
        return bound

    def _move(self, order: PlatformOrder, to: OrderState, payload: dict[str, Any]) -> PlatformOrder:
        after, event = transition(order, to, payload=payload, at=self._clock.now())
        self._store.record(order, after, event)
        return after

    def _reload(self, order: PlatformOrder) -> PlatformOrder:
        current = self._store.get(order.order_id)
        if current is None:
            raise LookupError(f"order {order.order_id} vanished")
        return current

    # ---------------------------------------------------------------- postbacks
    async def _on_frame(self, frame: OrderUpdateFrame) -> None:
        self.redeliver(frame.data)

    def redeliver(self, data: dict[str, Any]) -> None:
        """Queue a postback-shaped broker row behind the frames already queued: how the Reconciler (R5)
        re-applies a lost postback or adopts an unseen leg."""
        self._queue.put_nowait(_Frame(dict(data), self._clock.now()))

    async def _consume(self) -> None:
        while True:
            item = await self._queue.get()
            try:
                try:
                    updates = self._updates_for(item)
                except Exception:
                    _log.exception("paper_postback_unusable", item=repr(item))
                    updates = []
                for upd, at in updates:
                    try:
                        applied = self._apply(upd, at)
                    except Exception:
                        _log.exception("paper_postback_apply_failed", broker_order_id=upd.broker_order_id)
                        continue
                    if applied is not None:
                        await self._dispatch(applied)
            finally:
                self._queue.task_done()

    def _updates_for(self, item: _Frame | _Resolve) -> list[tuple[BrokerUpdate, datetime]]:
        """The updates to apply for ``item``, oldest first: any buffered ones, then the frame's own."""
        if isinstance(item, _Resolve):
            return self._buffered(item.order_id, item.broker_order_id)
        self._pending.sweep(item.at, _PENDING_MAX_AGE_S)
        upd = parse_postback(item.data)
        order = self._store.by_broker_id(upd.broker_order_id)
        if order is None:
            leg = _LEG_TAG.fullmatch(upd.tag or "")
            if leg is None:
                self._pending.hold(upd.broker_order_id, item.data, at=item.at)
                return []
            order = self._adopt(upd, int(leg.group(1)), item.at)
            if order is None:
                return []
        elif not order.is_paper:
            _log.error("paper_postback_matches_real_order", order_id=order.order_id)
            return []
        return [*self._buffered(order.order_id, upd.broker_order_id), (upd, item.at)]

    def _buffered(self, order_id: str, broker_order_id: str) -> list[tuple[BrokerUpdate, datetime]]:
        return [(parse_postback(e.data), e.at) for e in self._pending.resolve(order_id, broker_order_id)]

    def _adopt(self, upd: BrokerUpdate, gtt_id: int, at: datetime) -> PlatformOrder | None:
        """Insert the ``gtt_leg`` order for a fired paper GTT, on that GTT's position."""
        row = self._conn.execute(
            f"SELECT position_id FROM gtts WHERE gtt_id = ? AND {scope_sql('paper')}", (gtt_id,)
        ).fetchone()
        if row is None or row["position_id"] is None:
            _log.error("paper_gtt_leg_unadoptable", gtt_id=gtt_id, broker_order_id=upd.broker_order_id)
            return None
        leg = PlatformOrder(
            order_id=_new_id(),
            broker_order_id=upd.broker_order_id,
            position_id=row["position_id"],
            role=OrderRole.GTT_LEG,
            is_paper=True,
            state=OrderState.SUBMITTED,
            product=upd.product,
            side=upd.transaction_type,
            qty=upd.qty,
            price=upd.price,
            created_at=at,
            updated_at=at,
        )
        self._store.insert(leg)
        _log.info("paper_gtt_leg_adopted", gtt_id=gtt_id, order_id=leg.order_id, position_id=leg.position_id)
        return leg

    def _apply(self, upd: BrokerUpdate, at: datetime) -> _Applied | None:
        before = self._store.by_broker_id(upd.broker_order_id)
        if before is None:
            raise LookupError(f"no order for broker order {upd.broker_order_id}")
        after, event = apply_update(before, upd, at=at)
        if event is None:
            return None
        if any(event.payload.get(flag) for flag in NOOP_FLAGS):
            self._store.record_noop(before.order_id, event)
            return None
        self._store.record(before, after, event)
        qty = after.filled_qty - before.filled_qty
        price = _fill_price(before, upd) if qty > 0 else None
        return _Applied(after, event, qty, price, upd.broker_ts or at)

    async def _dispatch(self, applied: _Applied) -> None:
        if applied.fill_qty > 0:
            if applied.fill_price is None:
                _log.error("paper_fill_without_price", order_id=applied.order.order_id, qty=applied.fill_qty)
            else:
                await _call("on_fill", self._on_fill, applied.order, applied.fill_qty, applied.fill_price, applied.fill_at)
        if self._on_transition is not None:
            await _call("on_transition", self._on_transition, applied.order, applied.event)
