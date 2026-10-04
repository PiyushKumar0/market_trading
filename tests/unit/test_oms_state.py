"""OMS order state machine unit tests (WO-P3-1, 2026-09-10; plan §3.5.1/§3.5.2, §9.1).

Covers every edge of the §3.5.1 transition table (plus a forbidden sample per state), `filled_qty`
preservation across the residual-cancel / lapse paths, the A2 modification cap (20) and the
cancel-and-replace threshold (18), and reject-reason capture.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from engine.core.clock import IST
from engine.core.config import repo_root
from engine.oms.correlation import PendingCorrelation
from engine.oms.state import (
    ALLOWED_TRANSITIONS,
    BROKER_REALITY_TRANSITIONS,
    CANCEL_REJECTED_TRANSITIONS,
    DIAGRAM_TRANSITIONS,
    MODIFICATION_CAP,
    MODIFY_REPLACE_THRESHOLD,
    TERMINAL_ORDER_STATES,
    CloseReason,
    CorrelationMismatch,
    FillQuantityError,
    IllegalTransition,
    ModificationCapExceeded,
    OrderRole,
    OrderState,
    PlatformOrder,
    PositionState,
    ProtectionState,
    amend,
    cancel_rejected,
    correlate,
    modify_rejected,
    transition,
)
from engine.oms.updates import (
    KITE_INTERMEDIATE_STATUSES,
    UnknownBrokerStatus,
    apply_update,
    parse_postback,
)

AT = datetime(2026, 9, 10, 10, 5, tzinfo=IST)
IST_OFFSET = timedelta(hours=5, minutes=30)
QTY = 100


def make_order(state: OrderState, **kw) -> PlatformOrder:
    base = {
        "order_id": "01JABCDEFGHJKMNPQRSTVWXYZ0",
        "role": OrderRole.ENTRY,
        "state": state,
        "product": "MIS",
        "side": "BUY",
        "qty": QTY,
        "filled_qty": 0,
        "price": Decimal("101.50"),
        "created_at": AT,
        "updated_at": AT,
    }
    base.update(kw)
    return PlatformOrder(**base)


# ----------------------------------------------------------------- enums pinned to the plan / schema
def test_terminal_states_are_the_four_plan_states() -> None:
    assert TERMINAL_ORDER_STATES == frozenset(
        {OrderState.FILLED, OrderState.CANCELLED, OrderState.REJECTED, OrderState.LAPSED}
    )


def test_order_state_members_match_plan_3_5_1() -> None:
    assert [s.value for s in OrderState] == [
        "DRAFT",
        "VALIDATED",
        "SUBMITTED",
        "ACKED",
        "PARTIALLY_FILLED",
        "FILLED",
        "CANCEL_PENDING",
        "CANCELLED",
        "MODIFY_PENDING",
        "LAPSED",
        "REJECTED",
    ]


def test_position_and_protection_states_match_plan_3_5_2() -> None:
    assert [s.value for s in PositionState] == [
        "PENDING_ENTRY",
        "OPEN",
        "PENDING_EXIT",
        "CLOSED",
        "DISCARDED",
    ]
    assert [s.value for s in ProtectionState] == [
        "PROTECTED",
        "PROTECTION_PENDING",
        "PROTECTION_FAILED",
    ]


def test_close_reasons_are_the_3_5_2_list_verbatim() -> None:
    assert [r.value for r in CloseReason] == [
        "target",
        "stop",
        "square_off",
        "square_off_overdue_startup",
        "manual_owner",
        "broker_rms",
        "broker_squareoff_offline",
        "kill_switch",
        "llm_exit",
        "time_stop",
        "gtt_failure_exit",
        "auction_settled",
        "external_unknown",
    ]


def test_order_roles_match_the_orders_role_check_constraint() -> None:
    # The enum must not drift from the schema: parse the CHECK list out of the migration itself.
    sql = (Path(repo_root()) / "src/engine/core/migrations/0001_initial.sql").read_text(encoding="utf-8")
    m = re.search(r"role\s+TEXT NOT NULL CHECK \(role IN\s*\(([^)]*)\)", sql, re.S)
    assert m, "orders.role CHECK constraint not found in 0001_initial.sql"
    schema_roles = re.findall(r"'([a-z_]+)'", m.group(1))
    assert [r.value for r in OrderRole] == schema_roles


# ----------------------------------------------------------------- the transition table
def _seed_filled(frm: OrderState) -> int:
    return 40 if frm is OrderState.PARTIALLY_FILLED else 0


def _fill_arg(frm: OrderState, to: OrderState) -> int | None:
    if to is OrderState.PARTIALLY_FILLED:
        return max(_seed_filled(frm) + 10, 50)
    return None


ALL_EDGES = sorted(
    (frm, to) for frm, tos in ALLOWED_TRANSITIONS.items() for to in tos
)


@pytest.mark.parametrize(("frm", "to"), ALL_EDGES, ids=[f"{f}->{t}" for f, t in ALL_EDGES])
def test_every_allowed_edge_is_traversable(frm: OrderState, to: OrderState) -> None:
    order = make_order(frm, filled_qty=_seed_filled(frm))
    new, ev = transition(
        order,
        to,
        payload={"status": "X"},
        at=AT,
        filled_qty=_fill_arg(frm, to),
        modifications_delta=1 if to is OrderState.MODIFY_PENDING else 0,
    )
    assert new.state is to
    assert new.updated_at == AT
    assert ev.from_state is frm and ev.to_state is to and ev.at == AT


_S = OrderState

#: The §3.5.1 diagram TRANSCRIBED BY HAND from the plan, edge for edge. Compared for LITERAL EQUALITY
#: against the shipped table (2026-09-10 review finding: a derived/subset assertion is tautological —
#: it passes for any widening of the table). Anything added to, or dropped from, the machine has to be
#: re-transcribed here from the plan, so an unreviewed edge cannot reach the OMS.
EXPECTED_DIAGRAM_TRANSITIONS = {
    _S.DRAFT: {_S.VALIDATED},
    _S.VALIDATED: {_S.SUBMITTED},
    _S.SUBMITTED: {_S.ACKED, _S.CANCEL_PENDING, _S.REJECTED},
    _S.ACKED: {_S.PARTIALLY_FILLED, _S.CANCEL_PENDING, _S.MODIFY_PENDING, _S.LAPSED},
    _S.PARTIALLY_FILLED: {_S.FILLED, _S.CANCEL_PENDING, _S.MODIFY_PENDING, _S.LAPSED},
    _S.CANCEL_PENDING: {_S.CANCELLED},
    _S.MODIFY_PENDING: {_S.ACKED, _S.PARTIALLY_FILLED},
    _S.FILLED: set(),
    _S.CANCELLED: set(),
    _S.REJECTED: set(),
    _S.LAPSED: set(),
}

#: The broker-reality additions, transcribed from the ratified §8.4 WO-P3-1 decision (2026-09-10).
#: Load-bearing exclusion: CANCEL_PENDING -> ACKED is ABSENT — a cancel intent is cleared only by
#: CANCELLED or by the explicit :func:`cancel_rejected` door, never by a replayed/stale OPEN frame
#: (the Reconciler's 5-minute re-read returns OPEN for an in-flight cancel; flipping the row back
#: steers the §3.2.8 square-off coordination into the double-exit hazard).
EXPECTED_BROKER_REALITY_TRANSITIONS = {
    _S.SUBMITTED: {_S.PARTIALLY_FILLED, _S.FILLED, _S.CANCELLED},
    _S.ACKED: {_S.FILLED, _S.CANCELLED, _S.REJECTED},
    _S.PARTIALLY_FILLED: {_S.PARTIALLY_FILLED, _S.CANCELLED, _S.REJECTED},
    _S.CANCEL_PENDING: {_S.PARTIALLY_FILLED, _S.FILLED, _S.REJECTED},
    _S.MODIFY_PENDING: {_S.FILLED, _S.CANCELLED, _S.REJECTED},
}


def test_diagram_transitions_equal_the_plan_diagram_literally() -> None:
    assert {frm: set(tos) for frm, tos in DIAGRAM_TRANSITIONS.items()} == EXPECTED_DIAGRAM_TRANSITIONS


def test_broker_reality_transitions_equal_the_ratified_set_literally() -> None:
    assert {
        frm: set(tos) for frm, tos in BROKER_REALITY_TRANSITIONS.items()
    } == EXPECTED_BROKER_REALITY_TRANSITIONS


def test_allowed_table_is_exactly_the_union_of_the_two_transcribed_tables() -> None:
    expected = {
        s: EXPECTED_DIAGRAM_TRANSITIONS[s] | EXPECTED_BROKER_REALITY_TRANSITIONS.get(s, set())
        for s in OrderState
    }
    assert {frm: set(tos) for frm, tos in ALLOWED_TRANSITIONS.items()} == expected


def test_broker_reality_edges_are_disjoint_from_the_diagram() -> None:
    # The additions beyond the literal §3.5.1 diagram are enumerated separately so they stay auditable.
    for frm, tos in BROKER_REALITY_TRANSITIONS.items():
        assert not (tos & DIAGRAM_TRANSITIONS.get(frm, frozenset()))


def test_cancel_pending_is_never_cleared_by_a_working_state_edge() -> None:
    # The whole point of the 2026-09-10 ratification: no postback-driven edge leaves CANCEL_PENDING
    # for a working state except a REAL fill (guarded on growth in updates.apply_update).
    assert OrderState.ACKED not in ALLOWED_TRANSITIONS[OrderState.CANCEL_PENDING]
    assert OrderState.MODIFY_PENDING not in ALLOWED_TRANSITIONS[OrderState.CANCEL_PENDING]


def test_cancel_rejected_table_is_the_only_door_back_to_a_working_state() -> None:
    assert {frm: set(tos) for frm, tos in CANCEL_REJECTED_TRANSITIONS.items()} == {
        _S.CANCEL_PENDING: {_S.ACKED, _S.PARTIALLY_FILLED}
    }


def test_terminal_states_have_no_outgoing_edges() -> None:
    for s in TERMINAL_ORDER_STATES:
        assert ALLOWED_TRANSITIONS[s] == frozenset()


FORBIDDEN_SAMPLES = [
    (OrderState.DRAFT, OrderState.SUBMITTED),           # must be VALIDATED first (R1 gate chain)
    (OrderState.VALIDATED, OrderState.ACKED),           # no ack without a submit
    (OrderState.SUBMITTED, OrderState.MODIFY_PENDING),  # nothing to modify before an ack
    (OrderState.ACKED, OrderState.ACKED),               # self-edge: a duplicate, not a transition
    (OrderState.PARTIALLY_FILLED, OrderState.ACKED),    # fill progress never regresses
    (OrderState.CANCEL_PENDING, OrderState.ACKED),      # a cancel intent is never cleared by an OPEN
    (OrderState.CANCEL_PENDING, OrderState.MODIFY_PENDING),
    (OrderState.MODIFY_PENDING, OrderState.CANCEL_PENDING),
    (OrderState.FILLED, OrderState.CANCEL_PENDING),     # terminal
    (OrderState.CANCELLED, OrderState.ACKED),           # terminal
    (OrderState.REJECTED, OrderState.SUBMITTED),        # terminal
    (OrderState.LAPSED, OrderState.ACKED),              # terminal
]


@pytest.mark.parametrize(
    ("frm", "to"), FORBIDDEN_SAMPLES, ids=[f"{f}->{t}" for f, t in FORBIDDEN_SAMPLES]
)
def test_forbidden_edge_raises(frm: OrderState, to: OrderState) -> None:
    order = make_order(frm, filled_qty=_seed_filled(frm))
    with pytest.raises(IllegalTransition):
        transition(order, to, payload={}, at=AT)


def test_every_state_has_a_forbidden_sample() -> None:
    assert {f for f, _ in FORBIDDEN_SAMPLES} == set(OrderState)


# ----------------------------------------------------------------- filled_qty conservation (§9.2)
def test_residual_cancel_preserves_filled_qty() -> None:
    order = make_order(OrderState.PARTIALLY_FILLED, filled_qty=40)
    pending, _ = transition(order, OrderState.CANCEL_PENDING, payload={}, at=AT)
    assert pending.filled_qty == 40
    cancelled, _ = transition(pending, OrderState.CANCELLED, payload={}, at=AT)
    assert cancelled.filled_qty == 40
    assert cancelled.state is OrderState.CANCELLED


def test_lapse_of_a_partial_preserves_filled_qty() -> None:
    order = make_order(OrderState.PARTIALLY_FILLED, filled_qty=40)
    lapsed, _ = transition(order, OrderState.LAPSED, payload={}, at=AT)
    assert lapsed.filled_qty == 40


def test_modify_of_a_partial_preserves_filled_qty() -> None:
    order = make_order(OrderState.PARTIALLY_FILLED, filled_qty=40)
    pending, _ = transition(order, OrderState.MODIFY_PENDING, payload={}, at=AT, modifications_delta=1)
    assert pending.filled_qty == 40
    back, _ = transition(pending, OrderState.PARTIALLY_FILLED, payload={}, at=AT, filled_qty=55)
    assert back.filled_qty == 55


def test_fill_completion_defaults_to_full_qty() -> None:
    order = make_order(OrderState.PARTIALLY_FILLED, filled_qty=40)
    filled, _ = transition(order, OrderState.FILLED, payload={}, at=AT)
    assert filled.filled_qty == QTY


def test_filled_qty_regression_raises() -> None:
    order = make_order(OrderState.PARTIALLY_FILLED, filled_qty=40)
    with pytest.raises(FillQuantityError):
        transition(order, OrderState.PARTIALLY_FILLED, payload={}, at=AT, filled_qty=30)


def test_a_fill_completion_never_shrinks_the_recorded_fill_count() -> None:
    # 2026-09-10 round-3 review (minor): a row whose qty sits BELOW what is already filled (the
    # overfill case the Reconciler has yet to resolve, or a quantity the broker amended down) used to
    # complete to `qty` AFTER the regression check had passed — deleting 10 filled shares from the
    # audit chain. The completion is now evaluated first, so the regression check sees it.
    order = make_order(OrderState.PARTIALLY_FILLED, filled_qty=40, qty=30)
    with pytest.raises(FillQuantityError):
        transition(order, OrderState.FILLED, payload={}, at=AT)


def test_partial_of_zero_is_refused() -> None:
    # Nothing has happened, so nothing is lost by refusing: a "partial fill" of zero is a parser bug.
    order = make_order(OrderState.ACKED)
    with pytest.raises(FillQuantityError):
        transition(order, OrderState.PARTIALLY_FILLED, payload={}, at=AT, filled_qty=0)


def test_partial_that_completes_the_qty_is_recorded_and_flagged_never_raised() -> None:
    order = make_order(OrderState.ACKED)
    done, ev = transition(order, OrderState.PARTIALLY_FILLED, payload={}, at=AT, filled_qty=QTY)
    assert done.state is OrderState.PARTIALLY_FILLED and done.filled_qty == QTY
    assert ev.payload["qty_mismatch"] is True
    assert ev.payload["qty_mismatch_detail"]["order_qty"] == QTY


def test_filled_short_of_recorded_qty_is_recorded_and_flagged_never_raised() -> None:
    # R5: a fill that already happened must always land; the Reconciler resolves the disagreement.
    order = make_order(OrderState.PARTIALLY_FILLED, filled_qty=40)
    filled, ev = transition(order, OrderState.FILLED, payload={}, at=AT, filled_qty=90)
    assert filled.state is OrderState.FILLED and filled.filled_qty == 90
    assert ev.payload["qty_mismatch"] is True
    assert ev.payload["qty_mismatch_detail"] == {"filled_qty": 90, "order_qty": QTY, "to_state": "FILLED"}


def test_overfill_beyond_recorded_qty_is_recorded_and_flagged_never_raised() -> None:
    order = make_order(OrderState.PARTIALLY_FILLED, filled_qty=40)
    filled, ev = transition(order, OrderState.FILLED, payload={}, at=AT, filled_qty=QTY + 25)
    assert filled.filled_qty == QTY + 25
    assert ev.payload["qty_mismatch"] is True


def test_a_matching_fill_carries_no_mismatch_flag() -> None:
    order = make_order(OrderState.PARTIALLY_FILLED, filled_qty=40)
    _, ev = transition(order, OrderState.FILLED, payload={}, at=AT, filled_qty=QTY)
    assert "qty_mismatch" not in ev.payload


# ----------------------------------------------------------------- modification cap (A2)
def test_modification_cap_is_twenty_and_threshold_eighteen() -> None:
    assert MODIFICATION_CAP == 20
    assert MODIFY_REPLACE_THRESHOLD == 18
    assert MODIFY_REPLACE_THRESHOLD < MODIFICATION_CAP


def test_twenty_modifications_allowed_twenty_first_raises() -> None:
    order = make_order(OrderState.ACKED)
    for i in range(MODIFICATION_CAP):
        pending, _ = transition(order, OrderState.MODIFY_PENDING, payload={}, at=AT, modifications_delta=1)
        assert pending.modifications == i + 1
        order, _ = transition(pending, OrderState.ACKED, payload={}, at=AT)
    assert order.modifications == MODIFICATION_CAP
    with pytest.raises(ModificationCapExceeded):
        transition(order, OrderState.MODIFY_PENDING, payload={}, at=AT, modifications_delta=1)


def test_replace_threshold_reached_before_the_cap() -> None:
    order = make_order(OrderState.ACKED, modifications=MODIFY_REPLACE_THRESHOLD)
    # At the threshold the OMS policy switches to cancel-and-replace; the machine itself still allows
    # the remaining two modifications so an in-flight ladder is never bricked mid-way.
    assert order.modifications >= MODIFY_REPLACE_THRESHOLD
    pending, _ = transition(order, OrderState.MODIFY_PENDING, payload={}, at=AT, modifications_delta=1)
    assert pending.modifications == MODIFY_REPLACE_THRESHOLD + 1


def test_negative_modifications_delta_rejected() -> None:
    order = make_order(OrderState.ACKED, modifications=3)
    with pytest.raises(ValueError):
        transition(order, OrderState.MODIFY_PENDING, payload={}, at=AT, modifications_delta=-1)


# ----------------------------------------------------------------- reject reason capture (A8/A9)
def test_reject_captures_broker_status_message() -> None:
    order = make_order(OrderState.SUBMITTED)
    payload = {"status": "REJECTED", "status_message": "Insufficient margin (RMS:Margin Exceeds)"}
    rejected, ev = transition(order, OrderState.REJECTED, payload=payload, at=AT)
    assert rejected.state is OrderState.REJECTED
    assert rejected.reject_reason == "Insufficient margin (RMS:Margin Exceeds)"
    assert rejected.raw_broker_payload == payload
    assert ev.payload == payload


def test_reject_falls_back_to_status_message_raw() -> None:
    order = make_order(OrderState.SUBMITTED)
    rejected, _ = transition(
        order,
        OrderState.REJECTED,
        payload={"status": "REJECTED", "status_message_raw": "17070 : ORDER IS BLOCKED"},
        at=AT,
    )
    assert rejected.reject_reason == "17070 : ORDER IS BLOCKED"


def test_non_broker_payload_does_not_overwrite_raw_broker_payload() -> None:
    order = make_order(OrderState.ACKED, raw_broker_payload={"status": "OPEN"})
    pending, _ = transition(order, OrderState.CANCEL_PENDING, payload={"intent": "window_shrink"}, at=AT)
    assert pending.raw_broker_payload == {"status": "OPEN"}


def test_platform_order_rejects_naive_timestamps() -> None:
    with pytest.raises(ValueError):
        make_order(OrderState.DRAFT, created_at=datetime(2026, 9, 10, 10, 5))


# ----------------------------------------------------------------- the ONE postback parser (§3.5.1)
def postback(**kw) -> dict:
    """A Kite order postback as carried by ``OrderUpdateFrame.data`` (verbatim broker field names)."""
    data = {
        "order_id": "251009000123456",
        "status": "OPEN",
        "tradingsymbol": "TATAMOTORS",
        "transaction_type": "BUY",
        "product": "MIS",
        "order_type": "LIMIT",
        "variety": "regular",
        "quantity": QTY,
        "filled_quantity": 0,
        "pending_quantity": QTY,
        "cancelled_quantity": 0,
        "price": 101.5,
        "trigger_price": 0,
        "average_price": 0,
        "status_message": None,
        "order_timestamp": "2026-09-10 10:05:01",
        "exchange_update_timestamp": "2026-09-10 10:05:02",
        "tag": "mt",
    }
    data.update(kw)
    return data


@pytest.mark.parametrize(
    ("status", "filled", "expected"),
    [
        ("OPEN", 0, OrderState.ACKED),
        ("TRIGGER PENDING", 0, OrderState.ACKED),
        ("OPEN", 40, OrderState.PARTIALLY_FILLED),
        ("OPEN", QTY, OrderState.ACKED),          # not strictly between 0 and quantity
        ("COMPLETE", QTY, OrderState.FILLED),
        ("CANCELLED", 0, OrderState.CANCELLED),
        ("REJECTED", 0, OrderState.REJECTED),
    ],
)
def test_parse_postback_status_mapping(status: str, filled: int, expected: OrderState) -> None:
    upd = parse_postback(postback(status=status, filled_quantity=filled))
    assert upd.target_state is expected
    assert upd.filled_qty == filled
    assert upd.broker_order_id == "251009000123456"


@pytest.mark.parametrize("status", sorted(KITE_INTERMEDIATE_STATUSES))
def test_intermediate_statuses_carry_no_state_change(status: str) -> None:
    upd = parse_postback(postback(status=status))
    assert upd.target_state is None


def test_unknown_status_raises_never_guesses() -> None:
    with pytest.raises(UnknownBrokerStatus):
        parse_postback(postback(status="SOMETHING NEW"))


def test_postback_timestamps_are_tz_aware_ist() -> None:
    upd = parse_postback(postback())
    assert upd.broker_ts is not None
    assert upd.broker_ts.tzinfo is not None
    assert upd.broker_ts.utcoffset() == IST_OFFSET
    # exchange_update_timestamp wins over order_timestamp
    assert upd.broker_ts.second == 2


def test_postback_without_order_id_rejected() -> None:
    data = postback()
    del data["order_id"]
    with pytest.raises(ValueError):
        parse_postback(data)


def test_apply_update_acks_then_fills() -> None:
    order = correlate(make_order(OrderState.SUBMITTED, broker_order_id=None), "251009000123456")
    acked, ev = apply_update(order, parse_postback(postback(status="OPEN")), at=AT)
    assert acked.state is OrderState.ACKED
    assert acked.broker_order_id == "251009000123456"
    assert ev is not None and ev.to_state is OrderState.ACKED

    part, _ = apply_update(acked, parse_postback(postback(filled_quantity=40)), at=AT)
    assert part.state is OrderState.PARTIALLY_FILLED and part.filled_qty == 40

    more, _ = apply_update(part, parse_postback(postback(filled_quantity=70)), at=AT)
    assert more.state is OrderState.PARTIALLY_FILLED and more.filled_qty == 70

    done, _ = apply_update(more, parse_postback(postback(status="COMPLETE", filled_quantity=QTY)), at=AT)
    assert done.state is OrderState.FILLED and done.filled_qty == QTY


def test_apply_update_duplicate_is_idempotent() -> None:
    order = make_order(OrderState.ACKED, broker_order_id="251009000123456")
    same, ev = apply_update(order, parse_postback(postback(status="OPEN")), at=AT)
    assert same.state is OrderState.ACKED
    assert ev is not None and ev.from_state is ev.to_state
    assert ev.payload["duplicate"] is True


def test_apply_update_out_of_order_ack_after_partial_is_absorbed() -> None:
    order = make_order(OrderState.PARTIALLY_FILLED, filled_qty=40, broker_order_id="251009000123456")
    same, ev = apply_update(order, parse_postback(postback(status="OPEN", filled_quantity=0)), at=AT)
    assert same.state is OrderState.PARTIALLY_FILLED and same.filled_qty == 40
    assert ev is not None and ev.payload["duplicate"] is True


def test_apply_update_on_terminal_order_never_transitions() -> None:
    order = make_order(OrderState.FILLED, filled_qty=QTY, broker_order_id="251009000123456")
    same, ev = apply_update(order, parse_postback(postback(status="CANCELLED")), at=AT)
    assert same.state is OrderState.FILLED
    assert ev is not None and ev.from_state is ev.to_state and ev.payload["duplicate"] is True


def test_apply_update_cancel_of_a_partial_preserves_filled_qty() -> None:
    order = make_order(OrderState.PARTIALLY_FILLED, filled_qty=40, broker_order_id="251009000123456")
    pending, _ = transition(order, OrderState.CANCEL_PENDING, payload={}, at=AT)
    # Kite reports the cancel with filled_quantity/cancelled_quantity split; filled must not regress.
    upd = parse_postback(postback(status="CANCELLED", filled_quantity=40, cancelled_quantity=60))
    cancelled, ev = apply_update(pending, upd, at=AT)
    assert cancelled.state is OrderState.CANCELLED and cancelled.filled_qty == 40
    assert ev is not None and ev.to_state is OrderState.CANCELLED


def test_apply_update_intermediate_records_event_without_state_change() -> None:
    order = make_order(OrderState.SUBMITTED, broker_order_id="251009000123456")
    same, ev = apply_update(order, parse_postback(postback(status="VALIDATION PENDING")), at=AT)
    assert same.state is OrderState.SUBMITTED
    assert ev is not None and ev.from_state is ev.to_state
    assert ev.payload["intermediate"] is True
    assert ev.payload["broker_status"] == "VALIDATION PENDING"


def test_apply_update_rejects_a_foreign_broker_id() -> None:
    order = make_order(OrderState.ACKED, broker_order_id="999")
    with pytest.raises(CorrelationMismatch):
        apply_update(order, parse_postback(postback(status="COMPLETE", filled_quantity=QTY)), at=AT)


def test_apply_update_refuses_an_uncorrelated_order() -> None:
    # apply_update is pure and correlation is an explicit prior step (2026-09-10 review): an update
    # for an order whose broker id is unknown belongs in PendingCorrelation, not in the machine.
    order = make_order(OrderState.SUBMITTED, broker_order_id=None)
    with pytest.raises(CorrelationMismatch):
        apply_update(order, parse_postback(postback(status="OPEN")), at=AT)


def test_apply_update_captures_reject_reason() -> None:
    order = make_order(OrderState.SUBMITTED, broker_order_id="251009000123456")
    upd = parse_postback(postback(status="REJECTED", status_message="RMS:Blocked for TATAMOTORS"))
    rejected, _ = apply_update(order, upd, at=AT)
    assert rejected.state is OrderState.REJECTED
    assert rejected.reject_reason == "RMS:Blocked for TATAMOTORS"


# ----------------------------------------------------------------- pending correlation (A3, §3.5.1)
def test_pending_correlation_buffers_then_resolves_in_arrival_order() -> None:
    pc = PendingCorrelation(max_entries=8)
    for i in range(3):
        assert pc.hold("251009000123456", postback(filled_quantity=i), at=AT) == []
    pc.hold("OTHER", postback(order_id="OTHER"), at=AT)
    got = pc.resolve("01JORDER", "251009000123456")
    # Entries, not bare payloads: the caller must pass each update's ARRIVAL time into apply_update,
    # so the audit row carries when the platform saw it, not when it got around to applying it.
    assert [e.data["filled_quantity"] for e in got] == [0, 1, 2]
    assert [e.at for e in got] == [AT, AT, AT]
    assert [e.seq for e in got] == [1, 2, 3]
    assert pc.resolve("01JORDER", "251009000123456") == []      # drained
    acc = pc.accounting()
    assert acc.held == 4 and acc.resolved == 3 and acc.pending == 1


def test_pending_correlation_evicts_oldest_and_returns_them() -> None:
    pc = PendingCorrelation(max_entries=2)
    pc.hold("A", postback(order_id="A"), at=AT)
    pc.hold("B", postback(order_id="B"), at=AT)
    evicted = pc.hold("C", postback(order_id="C"), at=AT)
    assert [e.broker_order_id for e in evicted] == ["A"]
    acc = pc.accounting()
    assert acc.held == 3 and acc.evicted == 1 and acc.pending == 2


def test_pending_correlation_sweep_returns_stale_entries() -> None:
    pc = PendingCorrelation(max_entries=8)
    pc.hold("A", postback(order_id="A"), at=AT)
    later = AT + timedelta(seconds=120)
    assert pc.sweep(AT + timedelta(seconds=5), max_age_s=60) == []
    stale = pc.sweep(later, max_age_s=60)
    assert [e.broker_order_id for e in stale] == ["A"]
    acc = pc.accounting()
    assert acc.held == 1 and acc.expired == 1 and acc.pending == 0


# ------------------------------------------------- cancel intent is cleared ONLY by a cancel outcome
# 2026-09-10 review finding (major): a replayed/stale OPEN frame moved CANCEL_PENDING back to ACKED,
# which would let the §3.2.8 square-off coordination send a market exit alongside a protective order
# that is in fact still resting (or already triggered) — the double-exit-into-a-short hazard.
def test_stale_open_postback_never_clears_a_cancel_intent() -> None:
    order = make_order(OrderState.CANCEL_PENDING, broker_order_id="251009000123456")
    same, ev = apply_update(order, parse_postback(postback(status="OPEN")), at=AT)
    assert same == order                                   # untouched, not merely same-state
    assert ev is not None and ev.payload["duplicate"] is True
    assert ev.payload["duplicate_reason"] == "cancel_pending_held"


def test_stale_trigger_pending_postback_never_clears_a_cancel_intent() -> None:
    order = make_order(OrderState.CANCEL_PENDING, role=OrderRole.PROTECTIVE_SL,
                       broker_order_id="251009000123456")
    same, ev = apply_update(order, parse_postback(postback(status="TRIGGER PENDING")), at=AT)
    assert same == order
    assert ev is not None and ev.payload["duplicate_reason"] == "cancel_pending_held"


def test_cancel_outcome_still_applies_from_cancel_pending() -> None:
    order = make_order(OrderState.CANCEL_PENDING, filled_qty=40, broker_order_id="251009000123456")
    cancelled, _ = apply_update(
        order,
        parse_postback(postback(status="CANCELLED", filled_quantity=40, cancelled_quantity=60)),
        at=AT,
    )
    assert cancelled.state is OrderState.CANCELLED and cancelled.filled_qty == 40


def test_a_fill_that_already_happened_still_lands_from_cancel_pending() -> None:
    # §3.2.8: "the cancel is REJECTED because the protective order already triggered/filled in the gap".
    order = make_order(OrderState.CANCEL_PENDING, filled_qty=40, broker_order_id="251009000123456")
    filled, _ = apply_update(
        order, parse_postback(postback(status="COMPLETE", filled_quantity=QTY)), at=AT
    )
    assert filled.state is OrderState.FILLED and filled.filled_qty == QTY


def test_partial_fill_leaves_cancel_pending_only_when_filled_qty_grows() -> None:
    order = make_order(OrderState.CANCEL_PENDING, filled_qty=40, broker_order_id="251009000123456")
    grown, _ = apply_update(order, parse_postback(postback(filled_quantity=70)), at=AT)
    assert grown.state is OrderState.PARTIALLY_FILLED and grown.filled_qty == 70

    flat, ev = apply_update(order, parse_postback(postback(filled_quantity=40)), at=AT)
    assert flat == order                                   # same fill count => a replay, not a fill
    assert ev is not None and ev.payload["duplicate_reason"] == "cancel_pending_held"


def test_cancel_rejected_returns_an_unfilled_order_to_acked() -> None:
    order = make_order(OrderState.CANCEL_PENDING, broker_order_id="251009000123456")
    payload = {"status": "REJECTED", "status_message": "Order cannot be cancelled: already traded"}
    back, ev = cancel_rejected(order, payload=payload, at=AT + timedelta(seconds=3))
    assert back.state is OrderState.ACKED
    assert back.filled_qty == 0
    assert back.updated_at == AT + timedelta(seconds=3)
    assert back.raw_broker_payload == payload              # the broker's rejection is kept (R8)
    assert ev.from_state is OrderState.CANCEL_PENDING and ev.to_state is OrderState.ACKED
    assert ev.payload["cancel_rejected"] is True


def test_cancel_rejected_returns_a_partial_to_its_pre_cancel_fill_state() -> None:
    order = make_order(OrderState.CANCEL_PENDING, filled_qty=40, broker_order_id="251009000123456")
    back, ev = cancel_rejected(order, payload={"status": "REJECTED"}, at=AT)
    assert back.state is OrderState.PARTIALLY_FILLED and back.filled_qty == 40
    assert ev.to_state is OrderState.PARTIALLY_FILLED


@pytest.mark.parametrize(
    "state",
    [OrderState.ACKED, OrderState.PARTIALLY_FILLED, OrderState.MODIFY_PENDING, OrderState.CANCELLED],
)
def test_cancel_rejected_only_applies_to_a_cancel_intent(state: OrderState) -> None:
    order = make_order(state, filled_qty=40 if state is OrderState.PARTIALLY_FILLED else 0)
    with pytest.raises(IllegalTransition):
        cancel_rejected(order, payload={"status": "REJECTED"}, at=AT)


# ------------------------------------------------- the broker REFUSED our modify (mirrors the above)
def test_amend_records_the_pre_amend_values_on_its_event() -> None:
    # The frozen row keeps only the NEW values, so the old ones have to live somewhere the platform
    # can read them back: the amend's own audit row (R8). modify_rejected restores from there.
    order = make_order(OrderState.ACKED, role=OrderRole.PROTECTIVE_SL, side="SELL", qty=40,
                       price=None, trigger_price=Decimal("98.50"), broker_order_id="251009000999999")
    _, ev = amend(order, qty=70, trigger_price=Decimal("99.10"), at=AT)
    assert ev.payload["prior_fields"] == {"qty": 40, "trigger_price": Decimal("98.50")}
    assert ev.payload["fields_applied"] == {"qty": 70, "trigger_price": Decimal("99.10")}


def test_modify_rejected_restores_the_pre_amend_fields_and_keeps_the_a2_spend() -> None:
    order = make_order(OrderState.ACKED, role=OrderRole.PROTECTIVE_SL, side="SELL", qty=40,
                       price=None, trigger_price=Decimal("98.50"), broker_order_id="251009000999999")
    pending, ev = amend(order, qty=70, trigger_price=Decimal("99.10"), at=AT)

    payload = {"status": "REJECTED", "status_message": "Order modification failed"}
    back, ev2 = modify_rejected(pending, payload=payload, at=AT + timedelta(seconds=2),
                                prior_fields=ev.payload["prior_fields"])
    assert back.state is OrderState.ACKED                    # the pre-modify fill state
    assert back.qty == 40 and back.trigger_price == Decimal("98.50")   # the broker never applied it
    assert back.modifications == 1                           # the A2 budget stays SPENT (A2)
    assert back.updated_at == AT + timedelta(seconds=2)
    assert back.raw_broker_payload == payload                # the broker's refusal is kept (R8)
    assert ev2.from_state is OrderState.MODIFY_PENDING and ev2.to_state is OrderState.ACKED
    assert ev2.payload["modify_rejected"] is True
    assert ev2.payload["fields_applied"] == {"qty": 40, "trigger_price": Decimal("98.50")}


def test_modify_rejected_returns_a_partial_to_its_pre_modify_fill_state() -> None:
    order = make_order(OrderState.PARTIALLY_FILLED, filled_qty=40, broker_order_id="251009000123456")
    pending, ev = amend(order, price=Decimal("103.00"), at=AT)
    back, _ = modify_rejected(pending, payload={"status": "REJECTED"}, at=AT,
                              prior_fields=ev.payload["prior_fields"])
    assert back.state is OrderState.PARTIALLY_FILLED and back.filled_qty == 40
    assert back.price == Decimal("101.50")


def test_modify_rejected_without_prior_fields_moves_the_state_only() -> None:
    # The caller could not find the amend's audit row: the state must still leave MODIFY_PENDING (it
    # has no outgoing modify edge, so a stranded order breaks the §3.2.8 SL-M ladder), and the row
    # keeps what it has rather than inventing values the platform never held.
    order = make_order(OrderState.ACKED, qty=40, broker_order_id="251009000123456")
    pending, _ = amend(order, qty=70, at=AT)
    back, ev = modify_rejected(pending, payload={"status": "REJECTED"}, at=AT)
    assert back.state is OrderState.ACKED and back.qty == 70
    assert "fields_applied" not in ev.payload


@pytest.mark.parametrize(
    "state",
    [OrderState.ACKED, OrderState.PARTIALLY_FILLED, OrderState.CANCEL_PENDING, OrderState.FILLED],
)
def test_modify_rejected_only_applies_to_a_modify_intent(state: OrderState) -> None:
    order = make_order(state, filled_qty=QTY if state is OrderState.FILLED
                       else (40 if state is OrderState.PARTIALLY_FILLED else 0))
    with pytest.raises(IllegalTransition):
        modify_rejected(order, payload={"status": "REJECTED"}, at=AT)


# ------------------------------------------------- MODIFY_PENDING -> ACKED needs a NEWER broker clock
def test_modify_ack_applies_when_the_broker_timestamp_is_newer() -> None:
    order = make_order(OrderState.MODIFY_PENDING, modifications=1,
                       broker_order_id="251009000123456", updated_at=AT)
    # exchange_update_timestamp in the fixture is 10:05:02; order.updated_at is 10:05:00.
    acked, ev = apply_update(order, parse_postback(postback(status="OPEN")), at=AT)
    assert acked.state is OrderState.ACKED
    assert ev is not None and ev.to_state is OrderState.ACKED


@pytest.mark.parametrize(
    ("exchange_ts", "case"),
    [
        ("2026-09-10 10:05:01", "one second before the modify"),
        ("2026-09-10 09:20:00", "a much older replayed frame"),
        ("", "no broker timestamp at all — a malformed frame is treated as stale"),
    ],
)
def test_modify_ack_is_a_noop_when_the_frame_predates_the_modify(exchange_ts: str, case: str) -> None:
    # A replayed OPEN frame from BEFORE the modify must not be read as the modify's acknowledgement,
    # or the order looks amended when the broker never applied it (§3.5.1 duplicate/out-of-order).
    order = make_order(OrderState.MODIFY_PENDING, modifications=1,
                       broker_order_id="251009000123456",
                       updated_at=datetime(2026, 9, 10, 10, 5, 2, 400000, tzinfo=IST))
    data = postback(status="OPEN", exchange_update_timestamp=exchange_ts, order_timestamp=exchange_ts)
    same, ev = apply_update(order, parse_postback(data), at=AT)
    assert same is order, case
    assert ev is not None and ev.payload["duplicate_reason"] == "out_of_order"


def test_modify_ack_within_the_same_second_still_applies() -> None:
    """The broker clock is whole-second; the platform clock is not. Compare at the BROKER's resolution.

    A literal ``broker_ts > updated_at`` drops the ack of a modify applied inside the same second —
    the normal case at ~100-300 ms postback latency — stranding the order in MODIFY_PENDING, which
    has no outgoing modify edge. §3.2.8's SL-M qty ladder then breaks on the next fill accretion:
    the stale-frame guard would have become an R3 protection hazard. Probed with the §9.2 property
    generator, which never once reached a cleared modify intent under the literal comparison.
    """
    order = make_order(OrderState.MODIFY_PENDING, modifications=1, qty=40,
                       broker_order_id="251009000123456",
                       updated_at=datetime(2026, 9, 10, 10, 5, 2, 400000, tzinfo=IST))
    data = postback(status="OPEN", quantity=70, pending_quantity=70,
                    exchange_update_timestamp="2026-09-10 10:05:02",
                    order_timestamp="2026-09-10 10:05:02")
    acked, ev = apply_update(order, parse_postback(data), at=AT)
    assert acked.state is OrderState.ACKED and acked.qty == 70
    assert ev is not None and ev.to_state is OrderState.ACKED


def test_a_fill_during_a_modify_applies_regardless_of_the_broker_clock() -> None:
    # Only the ACK is clock-gated; a fill that already happened must always land (R5).
    order = make_order(OrderState.MODIFY_PENDING, modifications=1,
                       broker_order_id="251009000123456",
                       updated_at=datetime(2026, 9, 10, 10, 9, tzinfo=IST))
    filled, _ = apply_update(
        order, parse_postback(postback(status="COMPLETE", filled_quantity=QTY)), at=AT
    )
    assert filled.state is OrderState.FILLED and filled.filled_qty == QTY


def test_stale_partial_frame_never_clears_a_modify_intent_or_reverts_the_amend() -> None:
    """2026-09-10 round-3 review (major): the clock gate covered only MODIFY_PENDING -> ACKED.

    A PARTIALLY_FILLED order under a modify intent gets its stale OPEN re-report parsed as
    PARTIALLY_FILLED (fills are already on the row), which walked straight past the gate: the intent
    was cleared and ``_broker_fields`` carried the PRE-amend price back onto the row, so the platform
    believed a modify had been acknowledged that the broker never applied. Every MODIFY_PENDING ->
    working-state move is now gated on a real fill or a frame that does not predate the intent.
    """
    order = make_order(OrderState.PARTIALLY_FILLED, filled_qty=40,
                       broker_order_id="251009000123456",
                       updated_at=datetime(2026, 9, 10, 10, 5, 2, 400000, tzinfo=IST))
    pending, _ = amend(order, price=Decimal("103.00"), at=datetime(2026, 9, 10, 10, 5, 3, tzinfo=IST))
    assert pending.state is OrderState.MODIFY_PENDING and pending.price == Decimal("103.00")

    stale = postback(status="OPEN", filled_quantity=40, price=101.5,
                     exchange_update_timestamp="2026-09-10 10:05:01",
                     order_timestamp="2026-09-10 10:05:01")
    same, ev = apply_update(pending, parse_postback(stale), at=AT)
    assert same is pending                                  # untouched: the amended price survives
    assert same.price == Decimal("103.00")
    assert ev is not None and ev.payload["duplicate_reason"] == "out_of_order"
    # the disagreement is still evidence for the Reconciler, even though the row is not rewritten
    assert ev.payload["fields_reported"] == {"price": Decimal("101.5")}

    real = postback(status="OPEN", filled_quantity=40, price=103.0,
                    exchange_update_timestamp="2026-09-10 10:05:04",
                    order_timestamp="2026-09-10 10:05:04")
    back, ev2 = apply_update(pending, parse_postback(real), at=AT)
    assert back.state is OrderState.PARTIALLY_FILLED and back.price == Decimal("103.00")
    assert ev2 is not None and "duplicate" not in ev2.payload


def test_a_stale_but_grown_frame_lands_the_fill_without_reverting_the_amend() -> None:
    """2026-09-10 round-3 review (major, both lenses): the `grew` exemption let a PRE-amend frame
    that carries a real fill clear MODIFY_PENDING *and* write its stale trigger back onto the row —
    the §3.2.8 protective-SL revert, through the fill door instead of the ack door. The two decisions
    are now split: the fill lands and the state moves (it already happened), but a frame that
    predates the intent never rewrites the amended fields — they ride the event as evidence."""
    order = make_order(OrderState.PARTIALLY_FILLED, filled_qty=5, order_type="SL-M",
                       broker_order_id="251009000123456",
                       trigger_price=Decimal("100.00"),
                       updated_at=datetime(2026, 9, 10, 10, 5, 0, tzinfo=IST))
    pending, _ = amend(order, trigger_price=Decimal("102.00"),
                       at=datetime(2026, 9, 10, 10, 5, 3, 500000, tzinfo=IST))
    assert pending.state is OrderState.MODIFY_PENDING and pending.trigger_price == Decimal("102.00")

    stale_fill = postback(status="OPEN", filled_quantity=7, trigger_price=100.0,
                          exchange_update_timestamp="2026-09-10 10:05:02",
                          order_timestamp="2026-09-10 10:05:02")
    after, ev = apply_update(pending, parse_postback(stale_fill), at=AT)
    assert after.state is OrderState.PARTIALLY_FILLED and after.filled_qty == 7   # the fill lands
    assert after.trigger_price == Decimal("102.00")                              # the amend survives
    assert ev is not None and "fields_applied" not in ev.payload
    assert ev.payload["fields_reported"] == {"trigger_price": Decimal("100.0")}

    # control: the same frame stamped AFTER the amend does carry the broker's fields onto the row
    fresh_fill = postback(status="OPEN", filled_quantity=7, trigger_price=102.0,
                          exchange_update_timestamp="2026-09-10 10:05:04",
                          order_timestamp="2026-09-10 10:05:04")
    after2, ev2 = apply_update(pending, parse_postback(fresh_fill), at=AT)
    assert after2.filled_qty == 7 and after2.trigger_price == Decimal("102.00")
    assert ev2 is not None and "fields_reported" not in ev2.payload


def test_a_partial_fill_during_a_modify_applies_however_old_the_frame_is() -> None:
    # Only a re-report is gated; a fill that already happened is never held back (R5).
    order = make_order(OrderState.MODIFY_PENDING, modifications=1, filled_qty=40,
                       broker_order_id="251009000123456",
                       updated_at=datetime(2026, 9, 10, 10, 9, tzinfo=IST))
    grown, ev = apply_update(order, parse_postback(postback(filled_quantity=70)), at=AT)
    assert grown.state is OrderState.PARTIALLY_FILLED and grown.filled_qty == 70
    assert ev is not None and "duplicate" not in ev.payload


# ------------------------------------------------- a fill is read from the FILL COUNT, not the word
# 2026-09-10 round-3 review (major): Kite reports "TRIGGER PENDING" for a triggered SL leg that is
# already filling, and "OPEN" with filled_quantity == quantity for the frame that completes an order.
# Both map to ACKED, which has no self-edge — so the fill hit a non-existent edge and apply_update
# raised IllegalTransition into the postback loop, losing every later frame in the batch.
def test_trigger_pending_frame_carrying_a_fill_is_promoted_to_a_partial_fill() -> None:
    order = make_order(OrderState.ACKED, role=OrderRole.PROTECTIVE_SL, side="SELL",
                       trigger_price=Decimal("98.50"), broker_order_id="251009000123456")
    upd = parse_postback(postback(status="TRIGGER PENDING", filled_quantity=40))
    assert upd.target_state is OrderState.ACKED             # the parser maps the STATUS WORD
    part, ev = apply_update(order, upd, at=AT)
    assert part.state is OrderState.PARTIALLY_FILLED and part.filled_qty == 40
    assert ev is not None and "duplicate" not in ev.payload


def test_open_frame_reporting_a_complete_fill_is_promoted_to_filled() -> None:
    order = make_order(OrderState.ACKED, broker_order_id="251009000123456")
    upd = parse_postback(postback(status="OPEN", filled_quantity=QTY))
    assert upd.target_state is OrderState.ACKED
    filled, ev = apply_update(order, upd, at=AT)
    assert filled.state is OrderState.FILLED and filled.filled_qty == QTY
    assert ev is not None and "duplicate" not in ev.payload


def test_a_grown_fill_is_never_discarded_as_a_rank_regression() -> None:
    # The rank check (ACKED ranks below PARTIALLY_FILLED) must never swallow a frame whose fill count
    # actually moved: a fill that already happened always lands.
    order = make_order(OrderState.PARTIALLY_FILLED, filled_qty=40, broker_order_id="251009000123456")
    grown, ev = apply_update(
        order, parse_postback(postback(status="TRIGGER PENDING", filled_quantity=70)), at=AT
    )
    assert grown.state is OrderState.PARTIALLY_FILLED and grown.filled_qty == 70
    assert ev is not None and "duplicate" not in ev.payload


def test_apply_update_never_raises_an_illegal_transition_for_a_broker_frame() -> None:
    # A residual shape the table cannot absorb (here: a postback correlated to a row that was never
    # submitted) is an anomaly to RECORD, not to raise — raising aborts the postback loop and loses
    # every later frame. The event is a no-op carrying the raw payload, so record_noop accepts it.
    order = make_order(OrderState.DRAFT, broker_order_id="251009000123456")
    same, ev = apply_update(order, parse_postback(postback(status="OPEN")), at=AT)
    assert same is order
    assert ev is not None and ev.from_state is ev.to_state is OrderState.DRAFT
    assert ev.payload["illegal_transition"] is True
    assert ev.payload["attempted_to_state"] == "ACKED"
    assert ev.payload["status"] == "OPEN"                   # the verbatim broker payload (R8)


def test_same_state_postback_records_the_fields_the_broker_reported() -> None:
    # A no-op does not rewrite the row (purity), but a broker reporting different parameters is
    # evidence the Reconciler needs — it used to be dropped on the floor.
    order = make_order(OrderState.ACKED, broker_order_id="251009000123456")
    upd = parse_postback(postback(status="OPEN", quantity=70, pending_quantity=70, price=102.25))
    same, ev = apply_update(order, upd, at=AT)
    assert same is order
    assert ev is not None and ev.payload["duplicate_reason"] == "same_state"
    assert ev.payload["fields_reported"] == {"qty": 70, "price": Decimal("102.25")}


def test_a_zero_quantity_postback_never_overwrites_the_row_quantity() -> None:
    # `quantity: 0` is as much a filler as `price: 0`; writing it would leave an order for nothing.
    order = make_order(OrderState.MODIFY_PENDING, modifications=1, qty=40,
                       broker_order_id="251009000123456", updated_at=AT)
    acked, ev = apply_update(
        order, parse_postback(postback(status="OPEN", quantity=0, pending_quantity=0)), at=AT
    )
    assert acked.state is OrderState.ACKED and acked.qty == 40
    assert ev is not None and "fields_applied" not in ev.payload


# ------------------------------------------------- amend: a modify carries its new parameters (R3)
def test_amend_stages_the_new_qty_on_the_modify_pending_edge() -> None:
    order = make_order(OrderState.ACKED, role=OrderRole.PROTECTIVE_SL, qty=40, side="SELL",
                       trigger_price=Decimal("98.50"), broker_order_id="251009000123456")
    pending, ev = amend(order, qty=70, at=AT)
    assert pending.state is OrderState.MODIFY_PENDING
    assert pending.qty == 70
    assert pending.modifications == 1
    assert ev.payload["platform_intent"] == "amend"
    assert ev.payload["fields_applied"] == {"qty": 70}


def test_amend_carries_price_and_trigger_too() -> None:
    order = make_order(OrderState.ACKED, broker_order_id="251009000123456")
    pending, ev = amend(order, price=Decimal("102.25"), trigger_price=Decimal("99.75"), at=AT)
    assert pending.price == Decimal("102.25") and pending.trigger_price == Decimal("99.75")
    assert ev.payload["fields_applied"] == {
        "price": Decimal("102.25"),
        "trigger_price": Decimal("99.75"),
    }


def test_amend_without_any_new_parameter_is_refused() -> None:
    order = make_order(OrderState.ACKED, broker_order_id="251009000123456")
    with pytest.raises(ValueError):
        amend(order, at=AT)


def test_amend_below_the_filled_qty_is_refused() -> None:
    order = make_order(OrderState.PARTIALLY_FILLED, filled_qty=40, broker_order_id="251009000123456")
    with pytest.raises(FillQuantityError):
        amend(order, qty=30, at=AT)


def test_amend_counts_against_the_a2_budget_and_stops_at_the_cap() -> None:
    order = make_order(OrderState.ACKED, modifications=MODIFICATION_CAP)
    with pytest.raises(ModificationCapExceeded):
        amend(order, qty=50, at=AT)


def test_protective_sl_modified_upward_then_fills_end_to_end() -> None:
    """§3.2.8 R3 path: the SL-M is sized to the first partial, qty-modified upward as fills accrete.

    Before the 2026-09-10 fix the modify could not carry the new qty, so the FILLED postback for the
    enlarged protective order raised FillQuantityError against the stale row qty — a fill that had
    already happened was refused, leaving the platform believing a closed position was still open.
    """
    sl = correlate(
        make_order(OrderState.ACKED, role=OrderRole.PROTECTIVE_SL, side="SELL", qty=40,
                   price=None, trigger_price=Decimal("98.50")),
        "251009000999999",
    )
    pending, _ = amend(sl, qty=70, at=AT)          # entry accretes 40 -> 70; the SL-M follows
    assert pending.qty == 70

    modify_ack = postback(order_id="251009000999999", status="TRIGGER PENDING", quantity=70,
                          filled_quantity=0, pending_quantity=70,
                          exchange_update_timestamp="2026-09-10 10:05:05")
    acked, _ = apply_update(pending, parse_postback(modify_ack), at=AT + timedelta(seconds=5))
    assert acked.state is OrderState.ACKED and acked.qty == 70

    fill = postback(order_id="251009000999999", status="COMPLETE", quantity=70, filled_quantity=70,
                    pending_quantity=0, exchange_update_timestamp="2026-09-10 10:06:00")
    filled, ev = apply_update(acked, parse_postback(fill), at=AT + timedelta(seconds=60))
    assert filled.state is OrderState.FILLED and filled.filled_qty == 70
    assert ev is not None and "qty_mismatch" not in ev.payload      # no bogus disagreement


def test_protective_sl_modified_down_then_fills_end_to_end() -> None:
    # The mirror case: a partial exit shrinks the protective order (still >= filled_qty).
    sl = make_order(OrderState.ACKED, role=OrderRole.PROTECTIVE_SL, side="SELL", qty=100,
                    trigger_price=Decimal("98.50"), broker_order_id="251009000999999")
    pending, _ = amend(sl, qty=60, trigger_price=Decimal("99.10"), at=AT)
    assert pending.qty == 60 and pending.trigger_price == Decimal("99.10")

    ack = postback(order_id="251009000999999", status="TRIGGER PENDING", quantity=60,
                   filled_quantity=0, pending_quantity=60, trigger_price=99.1,
                   exchange_update_timestamp="2026-09-10 10:05:05")
    acked, _ = apply_update(pending, parse_postback(ack), at=AT + timedelta(seconds=5))
    assert acked.state is OrderState.ACKED and acked.qty == 60

    fill = postback(order_id="251009000999999", status="COMPLETE", quantity=60, filled_quantity=60,
                    pending_quantity=0, exchange_update_timestamp="2026-09-10 10:06:00")
    filled, ev = apply_update(acked, parse_postback(fill), at=AT + timedelta(seconds=60))
    assert filled.state is OrderState.FILLED and filled.filled_qty == 60
    assert ev is not None and "qty_mismatch" not in ev.payload


def test_apply_update_carries_a_changed_quantity_onto_the_row() -> None:
    order = make_order(OrderState.MODIFY_PENDING, modifications=1, qty=40,
                       broker_order_id="251009000123456", updated_at=AT)
    acked, ev = apply_update(order, parse_postback(postback(quantity=70, pending_quantity=70)), at=AT)
    assert acked.qty == 70
    assert ev is not None and ev.payload["fields_applied"] == {"qty": 70}


def test_apply_update_never_manufactures_a_zero_price_from_a_market_order_postback() -> None:
    # Kite reports price/trigger_price 0 as "not applicable" on MARKET/non-SL orders; a 0 must never
    # overwrite the real limit/trigger the platform placed.
    order = make_order(OrderState.SUBMITTED, price=Decimal("101.50"),
                       trigger_price=Decimal("98.50"), broker_order_id="251009000123456")
    acked, ev = apply_update(order, parse_postback(postback(price=0, trigger_price=0)), at=AT)
    assert acked.price == Decimal("101.50") and acked.trigger_price == Decimal("98.50")
    assert ev is not None and "fields_applied" not in ev.payload


def test_fill_disagreeing_with_the_row_is_recorded_and_flagged_never_raised() -> None:
    order = make_order(OrderState.ACKED, qty=100, broker_order_id="251009000123456")
    # A COMPLETE for 70 against a row that says 100, on a postback that omits `quantity`.
    data = postback(status="COMPLETE", filled_quantity=70)
    del data["quantity"]
    filled, ev = apply_update(order, parse_postback(data), at=AT)
    assert filled.state is OrderState.FILLED and filled.filled_qty == 70
    assert ev is not None and ev.payload["qty_mismatch"] is True


# ------------------------------------------------- garbled/missing quantity on a partial (§3.5.1)
def test_partial_with_a_missing_quantity_is_still_a_partial_fill() -> None:
    # 2026-09-10 review finding: an OPEN postback carrying filled_quantity but no `quantity` was
    # parsed as a plain ACK and then swallowed as a same-state duplicate — a real fill went unseen.
    data = postback(filled_quantity=40)
    del data["quantity"]
    upd = parse_postback(data)
    assert upd.target_state is OrderState.PARTIALLY_FILLED
    assert upd.qty is None

    order = make_order(OrderState.ACKED, broker_order_id="251009000123456")
    part, ev = apply_update(order, upd, at=AT)
    assert part.state is OrderState.PARTIALLY_FILLED and part.filled_qty == 40
    assert ev is not None and "duplicate" not in ev.payload


def test_partial_with_a_garbled_quantity_is_still_a_partial_fill() -> None:
    upd = parse_postback(postback(filled_quantity=40, quantity="n/a"))
    assert upd.target_state is OrderState.PARTIALLY_FILLED and upd.qty is None


def test_accretion_is_never_swallowed_as_a_duplicate_whatever_the_target() -> None:
    # The accretion exemption applies to ANY target whose filled_quantity has grown, not only to a
    # PARTIALLY_FILLED self-edge (a quantity-less OPEN reports ACKED-shaped frames as fills accrete).
    order = make_order(OrderState.PARTIALLY_FILLED, filled_qty=40, broker_order_id="251009000123456")
    data = postback(filled_quantity=55)
    del data["quantity"]
    grown, ev = apply_update(order, parse_postback(data), at=AT)
    assert grown.filled_qty == 55
    assert ev is not None and "duplicate" not in ev.payload


# ------------------------------------------------- correlate: an explicit step, never a side effect
def test_correlate_assigns_the_broker_id_and_is_idempotent() -> None:
    order = make_order(OrderState.SUBMITTED, broker_order_id=None)
    correlated = correlate(order, "251009000123456")
    assert correlated.broker_order_id == "251009000123456"
    assert correlated.state is OrderState.SUBMITTED           # correlation is not a transition
    assert correlate(correlated, "251009000123456") is correlated


def test_correlate_refuses_to_re_point_an_order_at_another_broker_id() -> None:
    order = make_order(OrderState.ACKED, broker_order_id="251009000123456")
    with pytest.raises(CorrelationMismatch):
        correlate(order, "251009000999999")


@pytest.mark.parametrize("state", sorted(TERMINAL_ORDER_STATES), ids=lambda s: str(s))
def test_correlate_refuses_a_terminal_order(state: OrderState) -> None:
    # 2026-09-10 round-3 review: correlation is persisted by OrderStore.record, which refuses a
    # terminal row — so binding a broker id to one produces an in-memory fact that can never be
    # written. It is a correlation bug (a stray id matched to a dead order); surface it here.
    order = make_order(state, filled_qty=QTY if state is OrderState.FILLED else 0,
                       broker_order_id=None)
    with pytest.raises(IllegalTransition):
        correlate(order, "251009000123456")


def test_correlate_refuses_an_empty_broker_id() -> None:
    with pytest.raises(ValueError):
        correlate(make_order(OrderState.SUBMITTED, broker_order_id=None), "  ")


def test_apply_update_is_pure_on_a_noop() -> None:
    order = make_order(OrderState.ACKED, broker_order_id="251009000123456")
    same, _ = apply_update(order, parse_postback(postback(status="OPEN")), at=AT)
    assert same is order            # not merely equal: nothing is copied on a no-op


def test_buffered_updates_are_applied_with_their_arrival_times() -> None:
    """The A3 end-to-end sequence: buffer -> correlate -> apply, each with its OWN arrival time."""
    pc = PendingCorrelation(max_entries=8)
    pc.hold("251009000123456", postback(status="OPEN"), at=AT)
    pc.hold("251009000123456", postback(filled_quantity=40), at=AT + timedelta(seconds=1))

    order = correlate(make_order(OrderState.SUBMITTED, broker_order_id=None), "251009000123456")
    ats = []
    for entry in pc.resolve(order.order_id, "251009000123456"):
        order, ev = apply_update(order, parse_postback(entry.data), at=entry.at)
        ats.append(ev.at)
    assert order.state is OrderState.PARTIALLY_FILLED and order.filled_qty == 40
    assert ats == [AT, AT + timedelta(seconds=1)]
