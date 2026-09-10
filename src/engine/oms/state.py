"""Order/position state vocabularies + the §3.5.1 transition table as a pure function (WO-P3-1).

Built 2026-09-10 as the foundation tranche of Phase 3 (§8.4 addendum). Pure Tier 3: this module
imports ``core`` only — no broker, no store, no ``engine.intelligence`` (R1, guarded by
``tests/unit/test_import_graph.py``). Nothing here places an order or is reachable from the
RECOMMEND pipeline; it is data + one pure function.

**What is pinned here and why**

* The enums mirror the §4.2 column comments EXACTLY: ``OrderState`` is the §3.5.1 diagram's state
  set, ``PositionState``/``ProtectionState``/``CloseReason`` the §3.5.2 ones, and ``OrderRole`` the
  ``orders.role`` CHECK list in ``0001_initial.sql`` (a test parses the SQL and asserts they match,
  so the enum cannot drift from the schema).
* :data:`DIAGRAM_TRANSITIONS` is the §3.5.1 diagram, edge for edge.
* :data:`BROKER_REALITY_TRANSITIONS` is the **separately enumerated** set of edges the diagram's
  happy-path chain elides but a live broker produces. It is kept apart from the diagram set so the
  additions stay auditable (and so a reviewer can see exactly what was added beyond the plan), and
  both are written out LITERALLY rather than derived, because a derived table can only be tested
  against its own derivation (2026-09-10 review finding). The rule behind it: the diagram describes
  the edges the PLATFORM issues; the broker, by contrast, can report the order's true book state
  (ACKED / PARTIALLY_FILLED / FILLED / CANCELLED / REJECTED) from any non-terminal state — it does
  not know about our local CANCEL_PENDING / MODIFY_PENDING intents, and a fill, cancel or reject can
  land while one is in flight. Backwards fill-progress edges are excluded (a PARTIALLY_FILLED order
  never returns to ACKED).
* **A cancel intent is cleared ONLY by a cancel outcome** (ratified 2026-09-10, §8.4 WO-P3-1):
  CANCEL_PENDING -> ACKED is NOT an edge, so a replayed or stale OPEN/TRIGGER PENDING frame — which
  is exactly what the Reconciler's 5-minute re-read returns for an in-flight cancel — can never flip
  the row back to working. Flipping it back would steer the §3.2.8 square-off coordination into the
  double-exit hazard (market exit sent alongside a protective order believed cancelled). The only
  ways out of CANCEL_PENDING are CANCELLED, REJECTED, a REAL fill (FILLED, or PARTIALLY_FILLED when
  ``filled_qty`` grows — gated in :func:`engine.oms.updates.apply_update`), and the explicit
  :func:`cancel_rejected` door, which carries the broker's rejection payload.
* :data:`MODIFICATION_CAP` = 20 is the A2 self-cap (broker allows 25);
  :data:`MODIFY_REPLACE_THRESHOLD` = 18 is where §3.2.8 policy switches to cancel-and-replace. The
  machine still permits modifications 19 and 20 so an in-flight protection ladder is never bricked
  mid-way; only the 21st raises.
* :func:`transition` **absorbs nothing silently** — an edge outside the table raises
  :class:`IllegalTransition` so the caller alerts (§3.5.1 rejection-storm / reconciliation paths).
  Terminal states (FILLED/CANCELLED/REJECTED/LAPSED) have no outgoing edges at all.
* **A fill that already happened always lands** (ratified 2026-09-10). A fill quantity that
  disagrees with the row's ``qty`` — a COMPLETE short of it, or an overfill beyond it — is RECORDED
  and flagged ``qty_mismatch`` on the event payload for the Reconciler (R5), never refused. Refusing
  it left the platform believing a closed position was still open, which is the R3 hazard the
  quantity-modified protective SL-M walks into. Only a fill-count *regression* still raises: that is
  a platform bug, and nothing is lost by refusing it.
* :func:`amend` is how a modify carries its new qty/price/trigger onto the row (the frozen-model
  discipline is preserved — it returns a new instance), and it records the values it REPLACED on its
  event payload as ``prior_fields``, because a frozen row keeps only the new ones.
  :func:`modify_rejected` reads them back when the broker refuses the modify (added 2026-09-10,
  round 3): MODIFY_PENDING has no outgoing modify edge, so a refusal without a door strands the
  order and breaks §3.2.8's SL-M quantity ladder on the next accretion. The A2 counter stays spent —
  the self-cap counts modify CALLS, and refunding on a refusal makes a rejection storm unbounded.
* :func:`correlate` is the explicit step that binds a broker order id, so
  :func:`engine.oms.updates.apply_update` stays pure. It refuses a TERMINAL order: the store never
  writes a terminal row again, so binding an id there is a fact that can never be persisted.
* ``filled_qty`` is preserved by default across every transition, which is what makes
  PARTIALLY_FILLED -> CANCEL_PENDING -> CANCELLED (and -> LAPSED) conserve the filled portion
  (§3.5.1, §9.2 quantity conservation): the filled part is a protected Position, the residual a
  recorded cancel, never an orphan.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class OrderState(StrEnum):
    """Order lifecycle states (§3.5.1; ``orders.state``). Declaration order is the diagram's order."""

    DRAFT = "DRAFT"
    VALIDATED = "VALIDATED"
    SUBMITTED = "SUBMITTED"
    ACKED = "ACKED"                        # broker OPEN / TRIGGER PENDING
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCEL_PENDING = "CANCEL_PENDING"
    CANCELLED = "CANCELLED"
    MODIFY_PENDING = "MODIFY_PENDING"
    LAPSED = "LAPSED"                      # validity expired; filled_qty preserved
    REJECTED = "REJECTED"                  # broker reject; reason captured (A8/A9)


#: The four states from which nothing may move (§3.5.1 "Terminal:").
TERMINAL_ORDER_STATES: frozenset[OrderState] = frozenset(
    {OrderState.FILLED, OrderState.CANCELLED, OrderState.REJECTED, OrderState.LAPSED}
)


class PositionState(StrEnum):
    """Position lifecycle (§3.5.2; ``positions.state``)."""

    PENDING_ENTRY = "PENDING_ENTRY"
    OPEN = "OPEN"
    PENDING_EXIT = "PENDING_EXIT"
    CLOSED = "CLOSED"
    DISCARDED = "DISCARDED"                # entry terminal with zero fills => no position row


class ProtectionState(StrEnum):
    """Protection lifecycle, orthogonal to :class:`PositionState` (§3.5.2; ``positions.protection_state``).

    ``PROTECTION_FAILED`` is an emergency state: ProtectionManager market-exits rather than leave a
    leveraged position unprotected (R3). Owned by WO-P3-6; the vocabulary lands here.
    """

    PROTECTED = "PROTECTED"
    PROTECTION_PENDING = "PROTECTION_PENDING"
    PROTECTION_FAILED = "PROTECTION_FAILED"


class CloseReason(StrEnum):
    """The §3.5.2 CLOSED-reason list, verbatim and in plan order (``positions.close_reason``)."""

    TARGET = "target"
    STOP = "stop"
    SQUARE_OFF = "square_off"
    SQUARE_OFF_OVERDUE_STARTUP = "square_off_overdue_startup"   # §2.6 startup catch-up
    MANUAL_OWNER = "manual_owner"
    BROKER_RMS = "broker_rms"                                   # A8
    BROKER_SQUAREOFF_OFFLINE = "broker_squareoff_offline"       # §2.6 broker 15:25 while offline
    KILL_SWITCH = "kill_switch"
    LLM_EXIT = "llm_exit"
    TIME_STOP = "time_stop"
    GTT_FAILURE_EXIT = "gtt_failure_exit"                       # A12
    AUCTION_SETTLED = "auction_settled"                         # C8
    EXTERNAL_UNKNOWN = "external_unknown"


class OrderRole(StrEnum):
    """``orders.role`` — kept identical to the CHECK constraint in ``0001_initial.sql`` (asserted)."""

    ENTRY = "entry"
    PROTECTIVE_SL = "protective_sl"
    TARGET = "target"
    SQUAREOFF = "squareoff"
    EXIT = "exit"
    GTT_LEG = "gtt_leg"


#: A2 self-cap on broker modifications per order (broker allows 25). The 21st raises.
MODIFICATION_CAP = 20
#: §3.2.8 policy switch: past this count the OMS cancels-and-replaces instead of modifying.
MODIFY_REPLACE_THRESHOLD = 18


class IllegalTransition(RuntimeError):
    """An edge outside the §3.5.1 table. Never absorbed — the caller alerts (§3.5.1)."""


class ModificationCapExceeded(RuntimeError):
    """The 21st modification on one order (A2 self-cap 20; §3.2.8 replaces past 18)."""


class FillQuantityError(ValueError):
    """A fill quantity that cannot be true: a regression, or a "partial fill" of nothing.

    A fill that merely DISAGREES with the row (short COMPLETE, overfill) is not an error — it is
    recorded and flagged ``qty_mismatch`` for the Reconciler (R5), because a fill that already
    happened must always land (ratified 2026-09-10).
    """


class CorrelationMismatch(RuntimeError):
    """A broker order id that does not belong to this order — or is not known yet (A3, §3.5.1).

    Raised by :func:`correlate` (re-pointing a correlated order at another broker id) and by
    :func:`engine.oms.updates.apply_update` (an update routed to an order whose broker id is unset or
    different). An uncorrelated order's updates belong in
    :class:`~engine.oms.correlation.PendingCorrelation`, never in the state machine.
    """


class PlatformOrder(BaseModel):
    """One ``orders`` row (§4.2), as a model. Frozen — :func:`transition` returns a NEW instance.

    Field set and nullability mirror the table exactly so persistence is mechanical (see
    ``engine.oms.store``). Money is ``Decimal`` (never float — §3.2 convention); timestamps are
    tz-aware IST and validated as such (a naive datetime is a bug, §9.1).
    """

    model_config = ConfigDict(frozen=True)

    order_id: str                                    # platform ULID (§3.2 convention 6)
    broker_order_id: str | None = None               # None until place_order() returns / correlates (A3)
    position_id: str | None = None
    proposal_id: str | None = None                   # the R1/R8 provenance chain
    verdict_id: str | None = None
    role: OrderRole
    is_paper: bool = False                           # §3.5.3 routing; R9 live-vs-paper joins
    state: OrderState
    product: Literal["MIS", "CNC"]
    side: Literal["BUY", "SELL"] | None = None
    qty: int | None = None
    filled_qty: int = Field(default=0, ge=0)
    price: Decimal | None = None
    trigger_price: Decimal | None = None
    modifications: int = Field(default=0, ge=0)
    reject_reason: str | None = None
    raw_broker_payload: dict[str, Any] | None = None  # R8: the payload that drove the last transition
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @field_validator("created_at", "updated_at")
    @classmethod
    def _tz_aware(cls, v: datetime | None) -> datetime | None:
        if v is not None and v.tzinfo is None:
            raise ValueError("order timestamps must be tz-aware IST (naive datetimes are a bug, §3.2)")
        return v


class OrderEvent(BaseModel):
    """One ``order_events`` row (§4.2): the audit record of a transition attempt (R8).

    A NO-OP update is an event ANNOTATED with one of :data:`engine.oms.updates.NOOP_FLAGS` in its
    payload — a duplicate/out-of-order postback (``duplicate=True``), a broker ``*PENDING``
    intermediate (``intermediate=True``) or a table refusal recorded rather than raised
    (``illegal_transition=True``). ``from_state == to_state`` alone is NOT the no-op contract
    (2026-09-10 round-3 review): the PARTIALLY_FILLED accretion self-edge is a REAL transition that
    moves ``filled_qty`` and belongs in :meth:`~engine.oms.store.OrderStore.record`, and the
    ProtectionManager acts on it (§3.2.8). No-ops are recorded, never dropped: an unrecorded postback
    is an unexplained broker action.
    """

    model_config = ConfigDict(frozen=True)

    from_state: OrderState
    to_state: OrderState
    payload: dict[str, Any] = Field(default_factory=dict)
    at: datetime

    @field_validator("at")
    @classmethod
    def _tz_aware(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("OrderEvent.at must be tz-aware IST (§3.2)")
        return v


# ---------------------------------------------------------------- the §3.5.1 transition table as data
_S = OrderState

#: The §3.5.1 diagram, edge for edge (platform-issued lifecycle).
DIAGRAM_TRANSITIONS: dict[OrderState, frozenset[OrderState]] = {
    _S.DRAFT: frozenset({_S.VALIDATED}),
    _S.VALIDATED: frozenset({_S.SUBMITTED}),
    _S.SUBMITTED: frozenset({_S.ACKED, _S.CANCEL_PENDING, _S.REJECTED}),
    _S.ACKED: frozenset({_S.PARTIALLY_FILLED, _S.CANCEL_PENDING, _S.MODIFY_PENDING, _S.LAPSED}),
    _S.PARTIALLY_FILLED: frozenset({_S.FILLED, _S.CANCEL_PENDING, _S.MODIFY_PENDING, _S.LAPSED}),
    _S.CANCEL_PENDING: frozenset({_S.CANCELLED}),
    _S.MODIFY_PENDING: frozenset({_S.ACKED, _S.PARTIALLY_FILLED}),
    _S.FILLED: frozenset(),
    _S.CANCELLED: frozenset(),
    _S.REJECTED: frozenset(),
    _S.LAPSED: frozenset(),
}

#: Edges beyond the literal diagram, written out edge by edge so every addition is auditable at a
#: glance (a derived table can only be tested against its own derivation — 2026-09-10 review).
#: Each entry is a state a LIVE BROKER can report the order to be in that the diagram's happy-path
#: chain elides. Read with the exclusions, which are the load-bearing part:
#:
#: * SUBMITTED -> {PARTIALLY_FILLED, FILLED, CANCELLED}: a dropped socket frame or a reconciliation
#:   re-read can reveal a book state we never saw the ACK for — recording the fill beats raising.
#: * ACKED / MODIFY_PENDING -> {FILLED, CANCELLED, REJECTED}: a fill, an exchange cancel or a reject
#:   can land while a platform-local intent is in flight; the broker knows nothing of our intents.
#: * PARTIALLY_FILLED -> PARTIALLY_FILLED (self-edge) is load-bearing: fills accrete across
#:   successive postbacks and the protective SL-M is qty-modified upward as they do (§3.2.8).
#: * CANCEL_PENDING -> {PARTIALLY_FILLED, FILLED, REJECTED} and NOT ACKED: §3.2.8's race — "the
#:   cancel is REJECTED because the protective order already triggered/filled in the gap" — is real,
#:   so a fill still lands; but a mere OPEN/TRIGGER PENDING re-report must NEVER clear the cancel
#:   intent. The PARTIALLY_FILLED edge is additionally gated on filled_qty GROWTH in
#:   ``updates.apply_update``: only a real fill, never a re-report, leaves CANCEL_PENDING.
#: * No backwards fill-progress edge anywhere (PARTIALLY_FILLED -> ACKED, FILLED -> anything).
#: * No self-edge except PARTIALLY_FILLED: a self-report is a duplicate, not a transition.
BROKER_REALITY_TRANSITIONS: dict[OrderState, frozenset[OrderState]] = {
    _S.SUBMITTED: frozenset({_S.PARTIALLY_FILLED, _S.FILLED, _S.CANCELLED}),
    _S.ACKED: frozenset({_S.FILLED, _S.CANCELLED, _S.REJECTED}),
    _S.PARTIALLY_FILLED: frozenset({_S.PARTIALLY_FILLED, _S.CANCELLED, _S.REJECTED}),
    _S.CANCEL_PENDING: frozenset({_S.PARTIALLY_FILLED, _S.FILLED, _S.REJECTED}),
    _S.MODIFY_PENDING: frozenset({_S.FILLED, _S.CANCELLED, _S.REJECTED}),
}

#: The table the machine enforces for POSTBACK-driven and platform-issued transitions.
ALLOWED_TRANSITIONS: dict[OrderState, frozenset[OrderState]] = {
    state: DIAGRAM_TRANSITIONS[state] | BROKER_REALITY_TRANSITIONS.get(state, frozenset())
    for state in OrderState
}

#: The ONE door back out of a cancel intent into a working state, walked only by
#: :func:`cancel_rejected` — the broker explicitly refused our cancel. Deliberately NOT part of
#: :data:`ALLOWED_TRANSITIONS`: keeping it separate is what makes a stale OPEN frame structurally
#: incapable of clearing a cancel intent (a postback can only reach the table above).
CANCEL_REJECTED_TRANSITIONS: dict[OrderState, frozenset[OrderState]] = {
    _S.CANCEL_PENDING: frozenset({_S.ACKED, _S.PARTIALLY_FILLED}),
}

#: Columns an amend / a broker postback may carry onto the order row (nothing else is ever written
#: from a payload — a postback cannot rewrite provenance, role, or the platform's own ids).
AMENDABLE_FIELDS: frozenset[str] = frozenset({"qty", "price", "trigger_price"})


def _reject_reason(payload: dict[str, Any]) -> str | None:
    """Broker reject reason from a postback (Kite field names, A8/A9)."""
    for key in ("status_message", "reject_reason", "status_message_raw"):
        value = payload.get(key)
        if value:
            return str(value)
    return None


def _is_broker_payload(payload: dict[str, Any]) -> bool:
    """True for a broker postback (carries Kite's own ``status``/``order_id``), false for a platform
    intent record. Only a broker payload is written to ``orders.raw_broker_payload`` — a platform
    intent must not overwrite the last thing the BROKER said about the order (R8)."""
    return bool(payload) and ("status" in payload or "order_id" in payload)


def _apply_edge(
    order: PlatformOrder,
    to_state: OrderState,
    *,
    table: dict[OrderState, frozenset[OrderState]],
    payload: dict[str, Any],
    at: datetime,
    filled_qty: int | None,
    modifications_delta: int,
    fields: dict[str, Any] | None,
    annotations: dict[str, Any] | None = None,
) -> tuple[PlatformOrder, OrderEvent]:
    """The one implementation behind :func:`transition` and :func:`cancel_rejected`.

    ``table`` decides which edges exist, which is how the cancel-rejected door stays separate from
    the postback-reachable table (see :data:`CANCEL_REJECTED_TRANSITIONS`).
    """
    frm = order.state
    if frm in TERMINAL_ORDER_STATES:
        raise IllegalTransition(f"{order.order_id}: {frm} is terminal; refused move to {to_state} (§3.5.1)")
    if to_state not in table.get(frm, frozenset()):
        raise IllegalTransition(f"{order.order_id}: {frm} -> {to_state} is not a §3.5.1 edge")

    new_fields = dict(fields or {})
    unknown = set(new_fields) - AMENDABLE_FIELDS
    if unknown:
        raise ValueError(f"{order.order_id}: {sorted(unknown)} are not amendable columns")
    # The row's quantity for THIS transition is whatever the same update also carries: a modify's
    # ack reports the fill and the new quantity in one frame, and validating the fill against the
    # pre-modify quantity is exactly the false disagreement the 2026-09-10 fix removes (§3.2.8).
    eff_qty = new_fields.get("qty", order.qty)

    new_filled = order.filled_qty if filled_qty is None else int(filled_qty)
    # The FILLED completion is evaluated BEFORE the regression check (2026-09-10 round-3 review): a
    # row whose qty sits below what is already filled — the overfill the Reconciler has yet to
    # resolve, or a quantity amended down — otherwise passed the check on the OLD value and then
    # completed DOWN to `qty`, silently deleting filled shares. Completion is now a candidate the
    # regression check judges like any other.
    if to_state is OrderState.FILLED and filled_qty is None and eff_qty is not None:
        new_filled = eff_qty
    if new_filled < order.filled_qty:
        raise FillQuantityError(
            f"{order.order_id}: filled_qty {order.filled_qty} -> {new_filled} regresses (§9.2)"
        )
    if to_state is OrderState.PARTIALLY_FILLED and new_filled <= 0:
        raise FillQuantityError(
            f"{order.order_id}: PARTIALLY_FILLED needs a fill, got {new_filled}/{eff_qty}"
        )

    # A quantity DISAGREEMENT is recorded and flagged, never refused: the fill already happened, and
    # the Reconciler (R5) is what resolves who is right (ratified 2026-09-10).
    mismatch = eff_qty is not None and (
        new_filled > eff_qty
        or (to_state is OrderState.FILLED and new_filled != eff_qty)
        or (to_state is OrderState.PARTIALLY_FILLED and new_filled >= eff_qty)
    )

    if modifications_delta < 0:
        raise ValueError("modifications_delta must be >= 0 (the A2 counter never decreases)")
    mods = order.modifications + modifications_delta
    if mods > MODIFICATION_CAP:
        raise ModificationCapExceeded(
            f"{order.order_id}: modification {mods} exceeds the self-cap {MODIFICATION_CAP} "
            f"(A2; cancel-and-replace past {MODIFY_REPLACE_THRESHOLD}, §3.2.8)"
        )

    updates: dict[str, Any] = {
        "state": to_state,
        "filled_qty": new_filled,
        "modifications": mods,
        "updated_at": at,
        **new_fields,
    }
    if to_state is OrderState.REJECTED:
        updates["reject_reason"] = _reject_reason(payload) or order.reject_reason
    if _is_broker_payload(payload):
        # Verbatim: the platform's annotations go on the EVENT, never into what the broker said (R8).
        updates["raw_broker_payload"] = payload

    event_payload: dict[str, Any] = {**payload, **(annotations or {})}
    if new_fields:
        event_payload["fields_applied"] = new_fields
    if mismatch:
        event_payload["qty_mismatch"] = True
        event_payload["qty_mismatch_detail"] = {
            "filled_qty": new_filled,
            "order_qty": eff_qty,
            "to_state": to_state.value,
        }

    return order.model_copy(update=updates), OrderEvent(
        from_state=frm, to_state=to_state, payload=event_payload, at=at
    )


def transition(
    order: PlatformOrder,
    to_state: OrderState,
    *,
    payload: dict[str, Any],
    at: datetime,
    filled_qty: int | None = None,
    modifications_delta: int = 0,
    fields: dict[str, Any] | None = None,
    annotations: dict[str, Any] | None = None,
) -> tuple[PlatformOrder, OrderEvent]:
    """Apply one §3.5.1 edge. Pure: returns a NEW order plus the event to persist.

    ``annotations`` are platform notes merged onto the EVENT payload only (never the row, never the
    verbatim broker payload, R8) — e.g. ``fields_reported`` for broker parameters a stale frame
    carried but must not write (2026-09-10 round-3 review).

    Parameters
    ----------
    payload:
        What drove the transition — a verbatim broker postback, or a platform intent record. Stored
        on the event either way (R8); written to ``raw_broker_payload`` only if it is a broker one.
    filled_qty:
        The new cumulative filled quantity. ``None`` PRESERVES the current value — which is what
        makes the residual-cancel and lapse paths conserve the filled portion (§3.5.1/§9.2). A
        transition to FILLED with ``None`` completes to ``qty``.
    modifications_delta:
        1 when the platform ISSUES a modify (the A2 counter counts platform modify calls, not
        postbacks). The 21st raises :class:`ModificationCapExceeded`.
    fields:
        New order parameters this same update carries onto the row — a subset of
        :data:`AMENDABLE_FIELDS`. Used by :func:`amend` (the platform's own modify) and by
        :func:`engine.oms.updates.apply_update` (the broker reporting the order's live parameters).
        Recorded on the event as ``fields_applied``.

    Raises
    ------
    IllegalTransition
        The edge is not in :data:`ALLOWED_TRANSITIONS` (terminal states have none). Nothing is
        absorbed silently — §3.5.1 wants the anomaly surfaced, not swallowed.
    FillQuantityError
        A fill-quantity regression, or a PARTIALLY_FILLED reporting no fill at all. A fill that
        merely disagrees with ``qty`` is flagged ``qty_mismatch`` on the event, NOT raised.
    """
    return _apply_edge(
        order,
        to_state,
        table=ALLOWED_TRANSITIONS,
        payload=payload,
        at=at,
        filled_qty=filled_qty,
        modifications_delta=modifications_delta,
        fields=fields,
        annotations=annotations,
    )


def cancel_rejected(
    order: PlatformOrder, *, payload: dict[str, Any], at: datetime
) -> tuple[PlatformOrder, OrderEvent]:
    """The broker REFUSED our cancel: return the order to its pre-cancel fill state (§3.5.1).

    This is the ONLY way a cancel intent is cleared other than a cancel outcome or a real fill — the
    postback-reachable table has no CANCEL_PENDING -> ACKED edge, precisely so that a replayed or
    stale OPEN frame cannot do this implicitly (ratified 2026-09-10). ``payload`` is the broker's
    verbatim rejection (kept on the row as ``raw_broker_payload``, R8) so §3.2.8 can tell the two
    rejection reasons apart: "already triggered/filled in the gap" (treat the protective order as the
    exit, never send the market order) versus an ordinary refusal.

    The destination is the order's pre-cancel FILL state: PARTIALLY_FILLED if anything is filled,
    ACKED otherwise — the cancel request never changed the fill count (§9.2 conservation).
    """
    if order.state is not OrderState.CANCEL_PENDING:
        raise IllegalTransition(
            f"{order.order_id}: cancel_rejected applies to a CANCEL_PENDING order, not {order.state}"
        )
    to_state = OrderState.PARTIALLY_FILLED if order.filled_qty > 0 else OrderState.ACKED
    return _apply_edge(
        order,
        to_state,
        table=CANCEL_REJECTED_TRANSITIONS,
        payload=payload,
        at=at,
        filled_qty=None,
        modifications_delta=0,
        fields=None,
        annotations={"cancel_rejected": True},
    )


def amend(
    order: PlatformOrder,
    *,
    qty: int | None = None,
    price: Decimal | None = None,
    trigger_price: Decimal | None = None,
    at: datetime,
) -> tuple[PlatformOrder, OrderEvent]:
    """Issue a modify that CARRIES its new parameters (§3.5.1 MODIFY_PENDING edge, A2 counter +1).

    §3.2.8's protective SL-M is qty-modified upward as fills accrete; before this existed the row
    kept the pre-modify quantity, and the eventual FILLED postback for the enlarged order was
    refused as a quantity disagreement — a fill that had already happened, rejected (2026-09-10
    review, major). The new values land on the row at the moment the modify is ISSUED, which is also
    what the ProtectionManager's retry ladder reads back.

    Raises
    ------
    ValueError
        No new parameter was given (an amend that changes nothing still spends A2 budget), or a
        non-positive quantity.
    FillQuantityError
        A quantity below what is already filled — that order cannot exist.
    IllegalTransition / ModificationCapExceeded
        As :func:`transition` (MODIFY_PENDING is reachable only from ACKED / PARTIALLY_FILLED).
    """
    fields: dict[str, Any] = {}
    if qty is not None:
        if qty <= 0:
            raise ValueError(f"{order.order_id}: amend qty must be positive, got {qty}")
        if qty < order.filled_qty:
            raise FillQuantityError(
                f"{order.order_id}: amend qty {qty} is below filled_qty {order.filled_qty} (§9.2)"
            )
        fields["qty"] = qty
    if price is not None:
        fields["price"] = price
    if trigger_price is not None:
        fields["trigger_price"] = trigger_price
    if not fields:
        raise ValueError(
            f"{order.order_id}: amend needs at least one of qty/price/trigger_price "
            f"(a no-change modify still spends the A2 budget)"
        )
    # The frozen row keeps only the NEW values, so the pre-amend ones are recorded on the amend's own
    # audit row (2026-09-10 round-3 review). That is where :func:`modify_rejected` reads them back
    # from when the broker refuses the modify — without them a refusal would leave the row claiming
    # parameters the broker never accepted, which is exactly the §3.2.8 SL-M hazard in reverse.
    prior_fields = {name: getattr(order, name) for name in fields}
    return transition(
        order,
        OrderState.MODIFY_PENDING,
        payload={"platform_intent": "amend", "prior_fields": prior_fields},
        at=at,
        modifications_delta=1,
        fields=fields,
    )


def modify_rejected(
    order: PlatformOrder,
    *,
    payload: dict[str, Any],
    at: datetime,
    prior_fields: dict[str, Any] | None = None,
) -> tuple[PlatformOrder, OrderEvent]:
    """The broker REFUSED our modify: return the order to its pre-modify state (§3.5.1, A2).

    The mirror of :func:`cancel_rejected`, added 2026-09-10 (round-3 review). MODIFY_PENDING has no
    outgoing modify edge, so an order stranded there breaks §3.2.8's SL-M quantity ladder on the very
    next fill accretion — a refusal needs an explicit door just as much as a cancel's does.

    The destination is the order's FILL state: PARTIALLY_FILLED if anything is filled, ACKED
    otherwise (a refused modify never changed the fill count, §9.2 conservation).

    ``prior_fields`` restores the qty/price/trigger the row carried BEFORE the amend — the broker
    never applied the new ones, so leaving them on the row would have the platform (and the
    ProtectionManager's retry ladder, which reads the row back) believing in an order that does not
    exist. :func:`amend` records exactly this dict on its event payload as ``prior_fields``, so the
    caller reads it from the persisted ``order_events`` row (R8); omit it and the state moves alone,
    which is the honest outcome when the amend's audit row cannot be found — the platform never
    invents values it did not hold.

    **The A2 counter stays spent.** The modification was issued and the broker processed it; the
    self-cap of 20 counts platform modify CALLS (§3.2.8), not successes, and refunding the budget on
    a refusal is how a rejection storm turns into an unbounded modify loop.
    """
    if order.state is not OrderState.MODIFY_PENDING:
        raise IllegalTransition(
            f"{order.order_id}: modify_rejected applies to a MODIFY_PENDING order, not {order.state}"
        )
    to_state = OrderState.PARTIALLY_FILLED if order.filled_qty > 0 else OrderState.ACKED
    return _apply_edge(
        order,
        to_state,
        table=ALLOWED_TRANSITIONS,
        payload=payload,
        at=at,
        filled_qty=None,
        modifications_delta=0,
        fields=dict(prior_fields) if prior_fields else None,
        annotations={"modify_rejected": True},
    )


def correlate(order: PlatformOrder, broker_order_id: str) -> PlatformOrder:
    """Bind the broker's order id to a platform order — the explicit step before any update applies.

    Split out of ``apply_update`` on 2026-09-10 so the state machine is genuinely pure on a no-op:
    correlation is a persisted fact about the order (the A3 postback-before-response race resolves
    here), not a side effect of parsing a postback. Idempotent — re-correlating with the same id
    returns the SAME object, so a retried place-response is free.

    The caller sequence is ``correlate`` -> ``OrderStore.record`` -> ``apply_update`` for each
    buffered update; see :mod:`engine.oms.updates`.
    """
    bid = str(broker_order_id).strip()
    if not bid:
        raise ValueError(f"{order.order_id}: refusing to correlate against an empty broker order id")
    if order.state in TERMINAL_ORDER_STATES:
        # Correlation is persisted by OrderStore.record, which refuses a terminal row (§3.5.1) — so
        # binding an id here would produce an in-memory fact that can never be written, and the
        # caller sequence correlate -> record would fail one step later with the id already bound
        # (2026-09-10 round-3 review). A broker id arriving for a dead order is a correlation bug;
        # the frame itself still belongs in record_noop, which IS allowed on a terminal row.
        raise IllegalTransition(
            f"{order.order_id}: refusing to correlate broker order {bid} onto a {order.state} order "
            f"— a terminal row is never written again (§3.5.1)"
        )
    if order.broker_order_id is None:
        return order.model_copy(update={"broker_order_id": bid})
    if order.broker_order_id != bid:
        raise CorrelationMismatch(
            f"{order.order_id}: already correlated to broker order {order.broker_order_id}; "
            f"refusing to re-point it at {bid} (A3)"
        )
    return order
