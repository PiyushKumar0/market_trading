"""OMS state-machine property tests (WO-P3-1, 2026-09-10; plan §9.2).

Random sequences of broker postbacks (acks, partial fills, rejects, cancels, modify storms,
duplicates, out-of-order replays — the WebSocket can replay, A3) are driven through the ONE parser
(:func:`engine.oms.updates.parse_postback`) and the pure transition function. The properties are the
§9.2 invariants that must hold in EVERY reachable state:

- no transition out of a terminal state;
- **the platform-intent invariant (ratified 2026-09-10):** a non-terminal order never regresses along
  the platform's intent chain — a cancel intent is cleared only by a cancel outcome, by a REAL fill,
  or by the explicit :func:`~engine.oms.state.cancel_rejected` door, and a modify intent only by a
  REAL fill, by a broker frame that does not predate the intent, or by the explicit
  :func:`~engine.oms.state.modify_rejected` door. A replayed OPEN frame clears neither — and the
  modify half of that holds for BOTH working destinations (ACKED and PARTIALLY_FILLED), which is the
  round-3 finding: an order with fills has its stale re-report parsed as PARTIALLY_FILLED, so an
  ACKED-only gate let it clear the intent and revert the amended fields;
- **a broker frame never raises:** every postback yields an order plus a recordable event, whether it
  transitions or lands as one of the :data:`~engine.oms.updates.NOOP_FLAGS`-annotated no-ops;
- ``filled_qty`` monotone non-decreasing and equal, at every step, to the high-water fill an APPLIED
  postback reported (an independent model of the fill count, not a restatement of the machine's);
- the modification counter never exceeds 20 (A2);
- exactly one recorded event per applied transition (and never a silently dropped update);
- an annotated no-op is exactly pure: the returned order **is** the one passed in;
- a residual cancel preserves the fill count snapshotted when the cancel intent was raised;
- PendingCorrelation never drops an update: held == pending + resolved + evicted + expired (A3), and
  an order that starts UNCORRELATED buffers its postbacks until :func:`correlate` resolves the id.

The §9.2 R3 protection invariant belongs to ProtectionManager (WO-P3-6) and is not asserted here.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from engine.core.clock import IST
from engine.oms.correlation import PendingCorrelation
from engine.oms.state import (
    MODIFICATION_CAP,
    TERMINAL_ORDER_STATES,
    IllegalTransition,
    ModificationCapExceeded,
    OrderRole,
    OrderState,
    PlatformOrder,
    amend,
    cancel_rejected,
    correlate,
    modify_rejected,
    transition,
)
from engine.oms.updates import NOOP_FLAGS, apply_update, parse_postback

AT = datetime(2026, 9, 10, 10, 5, tzinfo=IST)

#: Broker statuses the generator can emit, plus the platform-issued intents and the replay ops.
#:
#: WEIGHTED (repeat count = weight): terminal-producing ops are rare on purpose. Uniform sampling
#: killed the order within the first few steps of most examples — measured with ``hypothesis.event``
#: statistics, ~20% of ALL applied steps were SUBMITTED -> CANCELLED/FILLED/REJECTED and the deep
#: states (MODIFY_PENDING especially) were never reached at all, so the modify and cancel-intent
#: invariants were being asserted over sequences that could not exercise them.
OPS = st.sampled_from(
    [
        *["ack"] * 4,
        *["partial"] * 4,
        *["modify_issue"] * 3,
        *["amend_issue"] * 3,
        *["modify_done"] * 3,
        *["cancel_issue"] * 3,
        *["cancel_reject"] * 3,
        *["modify_reject"] * 3,
        *["duplicate"] * 3,
        *["out_of_order"] * 3,
        *["correlate"] * 3,
        *["intermediate"] * 2,
        "complete",
        "reject",
        "broker_cancel",
        "lapse",
    ]
)

CI = settings(
    max_examples=200,
    deadline=None,                       # CI boxes stall; the work here is pure-python and tiny
    suppress_health_check=[HealthCheck.too_slow],
)

BROKER_ORDER_ID = "251009000123456"


def _postback(qty: int, *, status: str, filled: int, ts: datetime) -> dict:
    """A verbatim-shaped Kite order postback (``OrderUpdateFrame.data`` field names, A3).

    ``ts`` is the BROKER clock (``exchange_update_timestamp``), which is what gates the
    MODIFY_PENDING -> ACKED edge — a replayed frame carries its original, older timestamp.
    """
    return {
        "order_id": BROKER_ORDER_ID,
        "status": status,
        "tradingsymbol": "TATAMOTORS",
        "transaction_type": "BUY",
        "product": "MIS",
        "order_type": "LIMIT",
        "variety": "regular",
        "quantity": qty,
        "filled_quantity": filled,
        "pending_quantity": qty - filled,
        "cancelled_quantity": 0,
        "price": 100.0,
        "trigger_price": 0,
        "average_price": 100.0,
        "status_message": "RMS rejected" if status == "REJECTED" else None,
        "order_timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "exchange_update_timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "tag": "mt",
    }


def _fresh(qty: int, *, correlated: bool) -> PlatformOrder:
    return PlatformOrder(
        order_id="01JABCDEFGHJKMNPQRSTVWXYZ0",
        broker_order_id=BROKER_ORDER_ID if correlated else None,
        role=OrderRole.ENTRY,
        state=OrderState.SUBMITTED,
        product="MIS",
        side="BUY",
        qty=qty,
        filled_qty=0,
        created_at=AT,
        updated_at=AT,
    )


@given(
    ops=st.lists(st.tuples(OPS, st.integers(min_value=1, max_value=97)), min_size=1, max_size=60),
    qty=st.integers(min_value=2, max_value=500),
    start_correlated=st.booleans(),
)
@CI
def test_state_machine_invariants_hold_for_any_update_sequence(
    ops, qty: int, start_correlated: bool
) -> None:
    order = _fresh(qty, correlated=start_correlated)
    buffer = PendingCorrelation(max_entries=64)
    history: list[dict] = []
    stats = {
        "events": 0,        # every recorded OrderEvent
        "applied": 0,       # events NOT annotated as a no-op == claimed transitions
        "mutations": 0,     # steps that actually changed the order
        "buffered": 0,      # postbacks held because the broker id was not known yet (A3)
        "expected_filled": 0,   # high-water fill an APPLIED postback reported (independent model)
        "cancel_snapshot": None,    # filled_qty when a cancel intent was first raised
        "fills_after_cancel": 0,    # fills that landed after that snapshot (a real fill still lands)
    }
    # What the CURRENT modify intent replaced, read from the amend's own event payload — exactly how
    # the OrderManager recovers it from the persisted order_events row when the broker refuses (R8).
    pending_prior: dict | None = None

    def apply_postback(cur: PlatformOrder, data: dict, at: datetime) -> PlatformOrder:
        """Apply one postback and assert every §9.2 invariant the step must preserve."""
        before = cur
        prev_state = cur.state
        prev_filled = cur.filled_qty
        upd = parse_postback(data)
        new, ev = apply_update(cur, upd, at=at)

        # Never silently dropped: EVERY update yields exactly one recordable event (R8).
        assert ev is not None
        assert ev.at == at
        assert ev.from_state is prev_state
        assert ev.to_state is new.state
        stats["events"] += 1

        # A no-op is ANNOTATED, not merely from_state == to_state: a PARTIALLY_FILLED accretion is a
        # real transition on the self-edge, and the ProtectionManager must act on it (SL-M
        # qty-modified upward as fills accrete, §3.2.8) — so the flag is the contract. NOOP_FLAGS is
        # the published set, so a flag added later (illegal_transition was, in round 3) cannot slip
        # past this branch and be asserted as a transition.
        # The generator only emits legal shapes against legal states, so a table refusal surfacing as
        # the round-3 `illegal_transition` no-op is a TABLE bug, not broker reality — assert it never
        # happens here; the swallow itself is pinned by a unit test (2026-09-10 round-3 review).
        assert not ev.payload.get("illegal_transition"), ev.payload.get("illegal_transition_detail")
        if any(ev.payload.get(flag) for flag in NOOP_FLAGS):
            assert new is before, "an annotated no-op must return the SAME order object (purity)"
        else:
            assert new != before, "a claimed transition must change the order"
            stats["applied"] += 1
            stats["expected_filled"] = max(stats["expected_filled"], upd.filled_qty)

        # ---- the platform-intent invariant (§9.2, ratified 2026-09-10)
        if prev_state is OrderState.CANCEL_PENDING:
            assert new.state is not OrderState.ACKED, (
                "a postback must never clear a cancel intent — the §3.2.8 square-off would then "
                "send a market exit alongside a protective order it believes is cancelled"
            )
            assert new.state is not OrderState.MODIFY_PENDING
            if new.state is OrderState.PARTIALLY_FILLED:
                assert new.filled_qty > prev_filled, "only a REAL fill leaves CANCEL_PENDING"
        if prev_state is OrderState.MODIFY_PENDING and new.state in (
            OrderState.ACKED,
            OrderState.PARTIALLY_FILLED,
        ):
            # BOTH working destinations are gated (round-3 finding): an order carrying fills has its
            # stale OPEN re-report parsed as PARTIALLY_FILLED, which an ACKED-only gate waved through
            # — clearing the intent AND reverting the amended qty/price. Compared at the BROKER's
            # whole-second resolution (see updates._postdates_intent); a REAL fill is never gated.
            assert new.filled_qty > prev_filled or (
                upd.broker_ts is not None
                and upd.broker_ts >= before.updated_at.replace(microsecond=0)
            ), "a modify intent is cleared only by a real fill or a frame that does not predate it"

        # ---- fill accounting, checked against the independent model
        assert new.filled_qty >= prev_filled, "filled_qty must be monotone non-decreasing (§9.2)"
        assert new.filled_qty <= qty, "filled_qty must never exceed the order's quantity (§9.2)"
        assert new.filled_qty == stats["expected_filled"], (
            "the row's fill count must equal the high-water fill an applied postback reported"
        )
        if stats["cancel_snapshot"] is not None and new.filled_qty > prev_filled:
            stats["fills_after_cancel"] = new.filled_qty
        if new is not before:
            stats["mutations"] += 1
        return new

    at = AT
    for op, n in ops:
        at = at + timedelta(milliseconds=n)
        before = order
        prev_state = order.state
        prev_mods = order.modifications
        data: dict | None = None

        if op == "ack":
            data = _postback(qty, status="OPEN", filled=order.filled_qty, ts=at)
        elif op == "partial":
            data = _postback(qty, status="OPEN", filled=min(order.filled_qty + n, qty - 1), ts=at)
        elif op == "complete":
            data = _postback(qty, status="COMPLETE", filled=qty, ts=at)
        elif op == "reject":
            data = _postback(qty, status="REJECTED", filled=order.filled_qty, ts=at)
        elif op == "broker_cancel":
            data = _postback(qty, status="CANCELLED", filled=order.filled_qty, ts=at)
        elif op == "intermediate":
            data = _postback(qty, status="VALIDATION PENDING", filled=order.filled_qty, ts=at)
        elif op == "duplicate":
            data = history[-1] if history else None       # the socket replays the LAST frame verbatim
        elif op == "out_of_order":
            data = history[n % len(history)] if history else None

        if data is not None:
            history.append(data)
            if order.broker_order_id is None:
                # A3: the postback beat place_order()'s return. Buffered, never dropped, never
                # applied to an order whose broker id the platform does not yet know.
                buffer.hold(BROKER_ORDER_ID, data, at=at)
                stats["buffered"] += 1
                continue
            order = apply_postback(order, data, at)
            if op == "ack" and prev_state is OrderState.CANCEL_PENDING:
                # An OPEN frame against a CANCEL intent is a no-op whatever its clock says — unlike
                # a modify ack, which a broker frame that postdates the modify legitimately clears.
                assert order is before
        elif op == "correlate":
            if order.broker_order_id is None and order.state in TERMINAL_ORDER_STATES:
                # 2026-09-10 round-3: a broker id can arrive after the platform lapsed/rejected the
                # order; the state layer REFUSES it (the store cannot persist it) and the buffered
                # frames are audited through record_noop instead, never applied — pinned here.
                with pytest.raises(IllegalTransition):
                    correlate(order, BROKER_ORDER_ID)
                assert order.broker_order_id is None
            elif order.broker_order_id is None:
                order = correlate(order, BROKER_ORDER_ID)
                assert order.state is prev_state, "correlation is not a transition"
                for entry in buffer.resolve(order.order_id, BROKER_ORDER_ID):
                    order = apply_postback(order, entry.data, entry.at)
        else:
            # Platform-issued intents; the OrderManager only issues them from a legal state.
            try:
                if op == "modify_issue" and order.state in (
                    OrderState.ACKED,
                    OrderState.PARTIALLY_FILLED,
                ):
                    order, ev = transition(
                        order, OrderState.MODIFY_PENDING, payload={"intent": "modify"},
                        at=at, modifications_delta=1,
                    )
                    pending_prior = None          # a bare modify carried no new parameters
                elif op == "amend_issue" and order.state in (
                    OrderState.ACKED,
                    OrderState.PARTIALLY_FILLED,
                ):
                    # §3.2.8: the protective SL-M is qty-modified as fills accrete; the amend carries
                    # the new quantity onto the row (never below what is already filled).
                    order, ev = amend(order, qty=max(order.filled_qty, (n % qty) + 1), at=at)
                    pending_prior = ev.payload["prior_fields"]
                elif op == "modify_done" and order.state is OrderState.MODIFY_PENDING:
                    back = (
                        OrderState.PARTIALLY_FILLED if order.filled_qty > 0 else OrderState.ACKED
                    )
                    order, ev = transition(order, back, payload={"intent": "modify_done"}, at=at)
                elif op == "cancel_issue" and order.state in (
                    OrderState.SUBMITTED,
                    OrderState.ACKED,
                    OrderState.PARTIALLY_FILLED,
                ):
                    order, ev = transition(
                        order, OrderState.CANCEL_PENDING, payload={"intent": "cancel"}, at=at
                    )
                elif op == "cancel_reject" and order.state is OrderState.CANCEL_PENDING:
                    order, ev = cancel_rejected(
                        order, payload={"status": "REJECTED", "status_message": "already traded"},
                        at=at,
                    )
                elif op == "modify_reject" and order.state is OrderState.MODIFY_PENDING:
                    # The broker refused the modify: the row goes back to what it held before the
                    # amend, and the A2 budget stays spent (the call was made).
                    order, ev = modify_rejected(
                        order, payload={"status": "REJECTED", "status_message": "modify failed"},
                        at=at, prior_fields=pending_prior,
                    )
                    assert order.modifications == prev_mods, "a refused modify never refunds A2 budget"
                    if pending_prior is not None:
                        for name, value in pending_prior.items():
                            assert getattr(order, name) == value, (
                                "a refused modify must not leave the amended parameters on the row"
                            )
                    pending_prior = None
                elif op == "lapse" and order.state in (
                    OrderState.ACKED,
                    OrderState.PARTIALLY_FILLED,
                ):
                    order, ev = transition(order, OrderState.LAPSED, payload={"intent": "lapse"}, at=at)
                else:
                    continue
            except ModificationCapExceeded:
                # The ONLY sanctioned refusal of a legal-state intent (A2); the counter must be AT the cap.
                assert order.modifications == MODIFICATION_CAP
                continue
            except IllegalTransition as exc:      # pragma: no cover - a table bug, surfaced loudly
                raise AssertionError(f"legal-state intent {op!r} from {order.state} refused: {exc}") from exc
            assert ev.from_state is prev_state and ev.to_state is order.state
            stats["events"] += 1
            stats["applied"] += 1
            stats["mutations"] += 1
            if order.state is OrderState.CANCEL_PENDING and stats["cancel_snapshot"] is None:
                # Snapshot the fill count the moment the cancel intent is raised (§9.2 conservation).
                stats["cancel_snapshot"] = order.filled_qty

        # ---- invariants, checked after EVERY step
        assert order.modifications <= MODIFICATION_CAP, "self-cap 20 (A2)"
        assert order.modifications >= prev_mods
        if prev_state in TERMINAL_ORDER_STATES:
            assert order is before, "no transition out of a terminal state (§9.2)"

    # Exactly one event per applied transition, and no order mutation without one (R8/§9.2).
    assert stats["applied"] == stats["mutations"]
    assert stats["events"] >= stats["applied"]

    # Nothing buffered was dropped (A3): every held update is still pending, or was resolved/evicted.
    acc = buffer.accounting()
    assert acc.held == stats["buffered"]
    assert acc.held == acc.pending + acc.resolved + acc.evicted + acc.expired

    # Residual cancel / lapse never leaks the filled portion (§3.5.1, §9.2 quantity conservation):
    # the terminal fill count is the one snapshotted when the cancel intent was raised, unless a
    # REAL fill landed in the gap between the cancel request and its outcome (§3.2.8).
    if stats["cancel_snapshot"] is not None and order.state in (
        OrderState.CANCELLED,
        OrderState.LAPSED,
    ):
        assert order.filled_qty == max(stats["cancel_snapshot"], stats["fills_after_cancel"])
    if order.state is OrderState.FILLED:
        assert order.filled_qty == qty


@given(
    ops=st.lists(
        st.tuples(
            st.sampled_from(["hold", "resolve", "sweep"]),
            st.integers(min_value=0, max_value=5),
            st.integers(min_value=1, max_value=400),
        ),
        min_size=1,
        max_size=60,
    ),
    max_entries=st.integers(min_value=1, max_value=6),
)
@CI
def test_pending_correlation_never_drops_an_update(ops, max_entries: int) -> None:
    pc = PendingCorrelation(max_entries=max_entries)
    at = AT
    evicted_seen = 0
    expired_seen = 0
    resolved_seen = 0
    held = 0

    for op, key, n in ops:
        at = at + timedelta(seconds=n)
        bid = f"25100900{key:06d}"
        if op == "hold":
            evicted = pc.hold(bid, _postback(10, status="OPEN", filled=0, ts=at), at=at)
            held += 1
            evicted_seen += len(evicted)
        elif op == "resolve":
            taken = pc.resolve(f"01JORDER{key}", bid)
            # Entries, not bare payloads: the caller needs each update's ARRIVAL time (§3.5.1 audit).
            assert all(e.broker_order_id == bid and e.at.tzinfo is not None for e in taken)
            assert [e.seq for e in taken] == sorted(e.seq for e in taken)
            resolved_seen += len(taken)
        else:
            expired_seen += len(pc.sweep(at, max_age_s=n))

    acc = pc.accounting()
    assert acc.held == held
    assert acc.resolved == resolved_seen
    assert acc.evicted == evicted_seen
    assert acc.expired == expired_seen
    # The conservation law: nothing is ever silently dropped (A3 — a dropped update is a lost order).
    assert acc.held == acc.pending + acc.resolved + acc.evicted + acc.expired
    assert acc.pending <= max_entries
