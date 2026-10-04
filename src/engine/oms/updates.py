"""The ONE broker-postback parser, shared by live and paper routing (WO-P3-1, 2026-09-10; §3.5.1).

``data`` here is the **verbatim Kite order postback** — the dict carried by
:class:`engine.core.contracts.OrderUpdateFrame` ``.data``, which the mt-ticker child forwards
untouched from ``KiteTicker.on_order_update`` (A3). PaperBroker (WO-P3-2) publishes the same
shape on the same topic, so live and paper share this parser and there is exactly one place where
broker vocabulary is translated into platform states. Divergence between the two paths is therefore
structurally impossible, which is what makes the §8.5 paper soak evidence about the live path.

**Kite fields read here (pinned; the parser reads NOTHING else):** ``order_id``, ``status``,
``filled_quantity``, ``pending_quantity``, ``cancelled_quantity``, ``average_price``,
``status_message`` (+ ``status_message_raw`` on the reject path), ``order_timestamp`` /
``exchange_update_timestamp``, ``tradingsymbol``, ``transaction_type``, ``product``, ``order_type``,
``quantity``, ``price``, ``trigger_price``, ``variety``, ``tag``.

**Status mapping (§3.5.1).** ``OPEN``/``TRIGGER PENDING`` -> ACKED; ``COMPLETE`` -> FILLED;
``CANCELLED`` -> CANCELLED; ``REJECTED`` -> REJECTED; ``OPEN`` with ``filled_quantity > 0`` and
either a smaller ``quantity`` or **no usable ``quantity`` at all** -> PARTIALLY_FILLED (a garbled or
missing quantity must not turn a real fill into a plain ack — 2026-09-10 review). The ``*PENDING``
validation/modify/cancel intermediates carry NO state change — they are recorded as an event payload
only. An unrecognised status raises :class:`UnknownBrokerStatus`: the platform never guesses what the
broker meant.

**Caller sequence (ratified 2026-09-10).** For each update, in this order:

1. :func:`~engine.oms.state.correlate` the platform order with the broker order id once
   ``place_order()`` returns it (or once the pending-correlation buffer resolves it, A3). Postbacks
   that arrive first live in :class:`~engine.oms.correlation.PendingCorrelation` until then, and
   :meth:`~engine.oms.correlation.PendingCorrelation.resolve` hands back the buffered ENTRIES so
   each one is applied with its own arrival time.
2. :meth:`~engine.oms.store.OrderStore.record` the correlation as a same-state event
   (``{"platform_intent": "correlate", "broker_order_id": ...}``) — persist before any side effect
   (R8), and a correlation the platform cannot explain later is worse than a redundant audit row.
3. :func:`apply_update`, then :meth:`~engine.oms.store.OrderStore.record` the resulting transition —
   or :meth:`~engine.oms.store.OrderStore.record_noop` when the event is an annotated no-op (which
   is the only way a postback against a TERMINAL row gets audited, §3.5.1).

:func:`apply_update` itself never assigns the broker id and never mutates on a no-op: it REFUSES an
update whose ``broker_order_id`` is not already the order's (:class:`CorrelationMismatch`).
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from engine.core.clock import IST
from engine.oms.state import (
    TERMINAL_ORDER_STATES,
    CorrelationMismatch,
    IllegalTransition,
    OrderEvent,
    OrderState,
    PlatformOrder,
    transition,
)


class UnknownBrokerStatus(RuntimeError):
    """A Kite order status this parser does not know. Never guessed — the caller alerts (§3.5.1)."""


#: Kite order statuses that map to a platform state. ``CANCELLED AMO`` is Kite's AMO variant of a
#: cancel; both are the same platform terminal state.
KITE_STATUS_MAP: dict[str, OrderState] = {
    "OPEN": OrderState.ACKED,
    "TRIGGER PENDING": OrderState.ACKED,       # a resting SL/SL-M leg is ACKED, not a fill (A12)
    "COMPLETE": OrderState.FILLED,
    "CANCELLED": OrderState.CANCELLED,
    "CANCELLED AMO": OrderState.CANCELLED,
    "REJECTED": OrderState.REJECTED,
}

#: Broker-side intermediates: real postbacks, but no platform state change (§3.5.1). Recorded as an
#: event payload so the audit chain shows every frame the broker sent (R8).
KITE_INTERMEDIATE_STATUSES: frozenset[str] = frozenset(
    {
        "PUT ORDER REQ RECEIVED",
        "VALIDATION PENDING",
        "OPEN PENDING",
        "MODIFY VALIDATION PENDING",
        "MODIFY PENDING",
        "CANCEL PENDING",
        "AMO REQ RECEIVED",
    }
)

#: Targets that report FILL PROGRESS; a report behind our own progress is stale, not illegal.
_FILL_PROGRESS_TARGETS: frozenset[OrderState] = frozenset(
    {OrderState.ACKED, OrderState.PARTIALLY_FILLED, OrderState.FILLED}
)
_PROGRESS_RANK: dict[OrderState, int] = {
    OrderState.ACKED: 1,
    OrderState.PARTIALLY_FILLED: 2,
    OrderState.FILLED: 3,
}
#: Targets that merely re-report the order as WORKING. Reaching one of these does not clear a
#: platform intent unless the fill count actually moved (§3.5.1 cancel-intent rule).
_WORKING_TARGETS: frozenset[OrderState] = frozenset(
    {OrderState.ACKED, OrderState.PARTIALLY_FILLED}
)

#: Payload flags that mark an event as a NO-OP: the order is returned UNCHANGED and the event is
#: audited through :meth:`engine.oms.store.OrderStore.record_noop` (the only door onto a terminal
#: row). Published as a set so a caller — and the §9.2 property test — can recognise a no-op without
#: enumerating flags it does not know about; ``duplicate_reason`` carries the why for all three.
NOOP_FLAGS: frozenset[str] = frozenset({"duplicate", "intermediate", "illegal_transition"})


def _postdates_intent(upd: BrokerUpdate, order: PlatformOrder) -> bool:
    """Is this broker frame from AFTER the platform raised its current intent (§3.5.1)?

    Compared at the BROKER's clock resolution, which is whole seconds: Kite stamps
    ``order_timestamp``/``exchange_update_timestamp`` to the second (PaperBroker matches it, see
    ``engine.paper.broker``), while ``updated_at`` is the platform's sub-second clock. A literal
    ``broker_ts > updated_at`` would therefore reject the ack of a modify the broker applied within
    the SAME second — which is the normal case, postbacks land in ~100-300 ms — stranding the order
    in MODIFY_PENDING. That breaks §3.2.8's SL-M qty ladder on the very next accretion (MODIFY_PENDING
    has no outgoing modify edge), i.e. it converts a stale-frame guard into an R3 protection hazard.
    The rule is therefore "the frame does not PREDATE the second the intent was raised".

    A frame with no timestamp at all is treated as stale: Kite always sends one, so its absence marks
    a malformed frame, and the payload is still recorded either way. (``updated_at`` is None only on
    an order that has never transitioned, which cannot be in a ``*_PENDING`` state — defensive.)
    """
    if upd.broker_ts is None or order.updated_at is None:
        return False
    return upd.broker_ts >= order.updated_at.replace(microsecond=0)


def _broker_fields(order: PlatformOrder, upd: BrokerUpdate) -> dict[str, Any] | None:
    """Order parameters the postback reports differently from the row (§3.5.1 modify path).

    Zero prices are ignored: Kite sends ``price``/``trigger_price`` as ``0`` to mean "not applicable"
    on MARKET and non-SL orders, and a 0 must never overwrite the limit/trigger the platform actually
    placed. A **non-positive quantity** is rejected on the same grounds and for the reason
    :func:`~engine.oms.state.amend` rejects one (2026-09-10 round-3 review): an order for zero shares
    cannot exist, so a 0/negative ``quantity`` is filler or garble, never the broker confirming an
    amend — writing it would leave the row claiming an order for nothing while the real one rests.
    """
    fields: dict[str, Any] = {}
    if upd.qty is not None and upd.qty > 0 and upd.qty != order.qty:
        fields["qty"] = upd.qty
    if upd.price is not None and upd.price > 0 and upd.price != order.price:
        fields["price"] = upd.price
    if upd.trigger_price is not None and upd.trigger_price > 0 and upd.trigger_price != order.trigger_price:
        fields["trigger_price"] = upd.trigger_price
    return fields or None


class BrokerUpdate(BaseModel):
    """A parsed broker postback. ``raw`` keeps the verbatim dict — persisted with the event (R8)."""

    model_config = ConfigDict(frozen=True)

    broker_order_id: str
    status: str                              # normalised Kite status (upper, whitespace-collapsed)
    target_state: OrderState | None          # None for the *PENDING intermediates
    filled_qty: int = Field(ge=0)
    pending_qty: int | None = None
    cancelled_qty: int | None = None
    qty: int | None = None
    average_price: Decimal | None = None
    price: Decimal | None = None
    trigger_price: Decimal | None = None
    status_message: str | None = None
    broker_ts: datetime | None = None        # tz-aware IST; exchange_update_timestamp preferred
    tradingsymbol: str | None = None
    transaction_type: str | None = None
    product: str | None = None
    order_type: str | None = None
    variety: str | None = None
    tag: str | None = None
    raw: dict[str, Any] = Field(default_factory=dict)


def _int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _decimal(value: Any) -> Decimal | None:
    """Kite sends prices as JSON numbers; go through ``str`` so no binary-float artifact is inherited
    into a price (§3.2 money convention)."""
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _timestamp(value: Any) -> datetime | None:
    """Kite order timestamps are exchange-local (IST) and arrive naive — as a ``datetime`` from
    pykiteconnect, or as a ``"YYYY-MM-DD HH:MM:SS"`` string over the postback wire. Both are stamped
    IST here; the platform never holds a naive datetime (§3.2)."""
    if value is None or value == "":
        return None
    ts: datetime | None = None
    if isinstance(value, datetime):
        ts = value
    elif isinstance(value, str):
        try:
            ts = datetime.fromisoformat(value)
        except ValueError:
            return None
    if ts is None:
        return None
    return ts.replace(tzinfo=IST) if ts.tzinfo is None else ts.astimezone(IST)


def parse_postback(data: dict[str, Any]) -> BrokerUpdate:
    """Parse a verbatim Kite order postback into a :class:`BrokerUpdate` (the ONE parser, §3.5.1).

    Raises
    ------
    ValueError
        The postback carries no ``order_id`` — uncorrelatable, so it must not be silently accepted.
    UnknownBrokerStatus
        An unrecognised ``status``. The platform never guesses a state from an unknown broker word.
    """
    broker_order_id = data.get("order_id")
    if broker_order_id is None or str(broker_order_id).strip() == "":
        raise ValueError("broker postback without order_id is uncorrelatable (A3, §3.5.1)")

    status = " ".join(str(data.get("status", "")).upper().split())
    qty = _int(data.get("quantity"))
    filled = _int(data.get("filled_quantity")) or 0

    if status in KITE_INTERMEDIATE_STATUSES:
        target: OrderState | None = None
    elif status in KITE_STATUS_MAP:
        target = KITE_STATUS_MAP[status]
        # §3.5.1: a working order that has filled something is PARTIALLY_FILLED, not merely ACKED.
        # Only OPEN qualifies — a TRIGGER PENDING leg has not entered the book yet. A MISSING or
        # garbled `quantity` (qty is None) still yields PARTIALLY_FILLED: before the 2026-09-10 fix
        # such a frame was read as a plain ack and then swallowed as a same-state duplicate, so a
        # real fill went unseen and the position stayed unprotected (R3).
        if status == "OPEN" and filled > 0 and (qty is None or filled < qty):
            target = OrderState.PARTIALLY_FILLED
    else:
        raise UnknownBrokerStatus(f"unknown Kite order status {status!r} (order_id={broker_order_id})")

    return BrokerUpdate(
        broker_order_id=str(broker_order_id),
        status=status,
        target_state=target,
        filled_qty=filled,
        pending_qty=_int(data.get("pending_quantity")),
        cancelled_qty=_int(data.get("cancelled_quantity")),
        qty=qty,
        average_price=_decimal(data.get("average_price")),
        price=_decimal(data.get("price")),
        trigger_price=_decimal(data.get("trigger_price")),
        status_message=data.get("status_message") or data.get("status_message_raw") or None,
        broker_ts=_timestamp(data.get("exchange_update_timestamp")) or _timestamp(data.get("order_timestamp")),
        tradingsymbol=data.get("tradingsymbol"),
        transaction_type=data.get("transaction_type"),
        product=data.get("product"),
        order_type=data.get("order_type"),
        variety=data.get("variety"),
        tag=data.get("tag"),
        raw=dict(data),
    )


def apply_update(
    order: PlatformOrder, upd: BrokerUpdate, *, at: datetime
) -> tuple[PlatformOrder, OrderEvent | None]:
    """Compose parse + :func:`~engine.oms.state.transition` for one broker update.

    **Idempotent by construction** — the WebSocket can replay, and reconciliation re-reads the same
    order (A3, §9.2). Three classes of update produce NO transition and an event whose
    ``to_state == from_state``:

    * the order is already terminal (terminal absorbs everything);
    * the update repeats the current state (except a PARTIALLY_FILLED whose ``filled_quantity`` has
      grown — fills accrete across postbacks, §3.2.8);
    * the update reports fill progress BEHIND our own (an out-of-order replay).

    Those events are annotated ``duplicate=True`` (with a ``duplicate_reason``); the ``*PENDING``
    intermediates are annotated ``intermediate=True``. Everything else applies the §3.5.1 edge.

    **This function never raises for a broker frame** (2026-09-10 round-3 review). Two mechanisms get
    it there:

    * the target is re-derived from the FILL COUNT before any gate — a working-state frame that
      reports fills is PARTIALLY_FILLED, or FILLED once the fill reaches the quantity — because
      Kite's status WORD lies about the book on a triggered SL leg ("TRIGGER PENDING" while filling)
      and on the completing frame ("OPEN" with ``filled_quantity == quantity``), and both words map
      to ACKED, which has no self-edge;
    * any residual shape the table still cannot absorb becomes an annotated no-op
      (``illegal_transition=True``, with ``attempted_to_state`` and the verbatim payload) rather than
      an exception, because raising here aborts the postback loop and loses every later frame in the
      batch. The anomaly is recorded for the Reconciler (R5), never swallowed.
      :func:`~engine.oms.state.transition` itself still raises: the platform's OWN moves are bugs.

    Every no-op event also carries ``fields_reported`` when the broker's quantity/price/trigger
    disagree with the row — the row is not rewritten (purity), but the disagreement is evidence.
    :data:`NOOP_FLAGS` is the set of flags that mark an event as one of these.

    Two intent guards make the no-op set larger than "same state" (ratified 2026-09-10):

    * **``cancel_pending_held``** — an OPEN / TRIGGER PENDING frame against a CANCEL_PENDING order is
      a no-op, whatever its timestamp. The Reconciler's 5-minute re-read returns exactly that for an
      in-flight cancel; clearing the cancel intent on it would let §3.2.8's square-off send a market
      exit alongside a protective order it believes cancelled (double exit into a short). Only a REAL
      fill (``filled_quantity`` strictly grown), a cancel outcome, or a reject leaves CANCEL_PENDING.
    * **``out_of_order`` on a modify ack** — EVERY move out of MODIFY_PENDING into a working state
      (ACKED **or** PARTIALLY_FILLED) requires a broker frame that does not predate the modify
      (:func:`_postdates_intent`, compared at the broker's whole-second resolution). A replayed OPEN
      frame from before the modify would otherwise read as the modify's acknowledgement — and on an
      order that already has fills it parses as PARTIALLY_FILLED, which an ACKED-only gate let
      through, reverting the amended fields (2026-09-10 round-3 review). A FILL during a modify is
      never gated: it already happened.

    ``filled_qty`` is passed as ``max(update, current)``: a CANCELLED/REJECTED postback that reports
    ``filled_quantity`` differently from what we already recorded must never shrink the filled
    portion of a partially-filled entry (§3.5.1 residual-cancel conservation). The postback's own
    ``quantity``/``price``/``trigger_price`` are carried onto the row when they differ (the broker is
    authoritative about the live order's parameters, and a modify's ack is how an amended quantity is
    confirmed) — recorded on the event as ``fields_applied``.

    Purity: on a no-op the order passed in is returned UNCHANGED (same object). Correlation is not
    done here — see the module docstring's caller sequence.

    The return type is ``OrderEvent | None`` per the WO-P3-1 surface; today an event is ALWAYS
    produced, because R8 requires every broker payload to be recorded. ``None`` is reserved for a
    future caller-side filter (e.g. the reconciler suppressing its own re-reads) and callers should
    handle it rather than assume non-None.
    """
    if order.broker_order_id is None or upd.broker_order_id != order.broker_order_id:
        raise CorrelationMismatch(
            f"update for broker order {upd.broker_order_id} routed to {order.order_id} "
            f"(broker_order_id={order.broker_order_id}) — correlate() first, or buffer it (A3)"
        )
    current = order
    payload = dict(upd.raw)

    def _noop_event(annotations: dict[str, Any]) -> OrderEvent:
        body: dict[str, Any] = {**payload, **annotations, "broker_status": upd.status}
        reported = _broker_fields(current, upd)
        if reported:
            # A no-op does NOT rewrite the row — that purity is what makes a replay safe — but the
            # broker reporting parameters that differ from ours is evidence, and it used to be
            # dropped on the floor (2026-09-10 round-3 review, minor). A modify the broker applied
            # while our frame was gated, or a quantity we never sent, surfaces here and nowhere else;
            # the Reconciler (R5) is what resolves the disagreement.
            body["fields_reported"] = reported
        return OrderEvent(from_state=current.state, to_state=current.state, payload=body, at=at)

    def _noop(flag: str, reason: str) -> tuple[PlatformOrder, OrderEvent]:
        return current, _noop_event({flag: True, "duplicate_reason": reason})

    target = upd.target_state
    if target is None:
        return current, _noop_event({"intermediate": True})
    if current.state in TERMINAL_ORDER_STATES:
        return _noop("duplicate", "terminal")
    if target in _FILL_PROGRESS_TARGETS and upd.filled_qty < current.filled_qty:
        return _noop("duplicate", "filled_qty_regression")

    grew = upd.filled_qty > current.filled_qty

    # The STATUS WORD is not the whole truth about the book, so the target is re-derived from the
    # FILL COUNT — the one fact a frame cannot garble (2026-09-10 round-3 review, major). Kite
    # reports "TRIGGER PENDING" for a triggered SL leg that is already filling, and "OPEN" with
    # filled_quantity == quantity for the frame that completes an order; both map to ACKED, which has
    # no self-edge, so such a fill hit a non-existent edge and raised out of the postback loop.
    # Re-derived BEFORE the intent gates so those gates judge the real move, not the word.
    if target in _WORKING_TARGETS and upd.filled_qty > 0:
        # The broker's own quantity wins when it reports a usable one (a modify's ack confirms the
        # amended size in the same frame); the row's is the fallback, and neither being known leaves
        # the order PARTIALLY_FILLED — completion is never assumed.
        eff_qty = upd.qty if upd.qty is not None and upd.qty > 0 else current.qty
        target = (
            OrderState.FILLED
            if eff_qty is not None and upd.filled_qty >= eff_qty
            else OrderState.PARTIALLY_FILLED
        )

    if current.state is OrderState.CANCEL_PENDING and target in _WORKING_TARGETS and not grew:
        return _noop("duplicate", "cancel_pending_held")
    if (
        current.state is OrderState.MODIFY_PENDING
        and target in _WORKING_TARGETS
        and not (grew or _postdates_intent(upd, current))
    ):
        # EVERY move out of a modify intent into a working state is gated, not just the ACKED one
        # (2026-09-10 round-3 review, major): a PARTIALLY_FILLED order's stale OPEN re-report parses
        # as PARTIALLY_FILLED, walked past an ACKED-only gate, cleared the intent AND carried the
        # pre-amend price/qty back onto the row — the platform then believed a modify had been
        # acknowledged that the broker never applied. A real fill (`grew`) is never gated: it
        # already happened.
        return _noop("duplicate", "out_of_order")

    if target is current.state:
        # The accretion exemption is about the FILL COUNT, not the target state: a frame whose
        # filled_quantity has grown is a real fill even when the status word repeats (a postback
        # with a missing `quantity` reports ACKED-shaped frames as fills accrete). §3.2.8 needs it:
        # the protective SL-M is qty-modified upward on each accretion.
        if not grew:
            return _noop("duplicate", "same_state")
    else:
        here, there = _PROGRESS_RANK.get(current.state), _PROGRESS_RANK.get(target)
        if here is not None and there is not None and there < here:
            # Unreachable for a frame that carries a fill (the re-derivation above lifts its target
            # to at least the row's progress) and for a zero-fill frame against a filled row (the
            # regression check above returns first) — kept as the table's own statement of the
            # rank rule, never as a fill filter (2026-09-10 round-3 review).
            return _noop("duplicate", "out_of_order")

    # Two decisions, split (2026-09-10 round-3 review, major on both lenses): a frame that carries
    # a real fill always LANDS it (the state moves even out of MODIFY_PENDING), but a frame that
    # PREDATES a modify intent never rewrites the amended parameters — that was the §3.2.8
    # protective-SL revert, arriving through the fill door instead of the ack door. The stale
    # parameters ride the event as `fields_reported` for the Reconciler (R5) instead.
    fields = _broker_fields(current, upd)
    annotations: dict[str, Any] | None = None
    if fields and current.state is OrderState.MODIFY_PENDING and not _postdates_intent(upd, current):
        annotations = {"fields_reported": fields}
        fields = None
    try:
        return transition(
            current,
            target,
            payload=payload,
            at=at,
            filled_qty=max(upd.filled_qty, current.filled_qty),
            fields=fields,
            annotations=annotations,
        )
    except IllegalTransition as exc:
        # apply_update NEVER raises for a broker frame (2026-09-10 round-3 review, major). A residual
        # shape the table cannot absorb — a postback correlated onto a row that was never submitted,
        # say — is a platform/correlation anomaly, and raising it here aborts the postback loop and
        # loses every LATER frame in the batch, which is a far worse outcome than an unexplained row.
        # It is recorded instead: annotated, with the verbatim payload, for the Reconciler (R5).
        # :func:`~engine.oms.state.transition` still raises — the OMS's OWN moves are bugs, not
        # broker reality, and §3.5.1 wants those surfaced.
        return current, _noop_event(
            {
                "illegal_transition": True,
                "duplicate_reason": "illegal_transition",
                "attempted_to_state": target.value,
                "illegal_transition_detail": str(exc),
            }
        )
