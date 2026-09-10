"""OrderStore unit tests (WO-P3-1, 2026-09-10; plan §3.5.1 persistence, §4.2 schema, R8).

The load-bearing property under test is **persist-before-side-effect**: :meth:`OrderStore.record`
commits the new order row AND its ``order_events`` row in ONE transaction, so a caller that only acts
after ``record`` returns can never act on an unpersisted state (a crash in the gap leaves the DB
consistent). Proven with a SECOND connection to the same file (only committed data is visible there)
and with a mid-record failure asserting neither half landed.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from decimal import Decimal

import pytest

from engine.core.clock import IST
from engine.core.db import connect
from engine.oms.state import (
    OrderEvent,
    OrderRole,
    OrderState,
    PlatformOrder,
    transition,
)
from engine.oms.store import OrderStore, StaleOrderWrite, TerminalOrderWrite

AT = datetime(2026, 9, 10, 10, 5, tzinfo=IST)
QTY = 100


def make_order(state: OrderState = OrderState.DRAFT, **kw) -> PlatformOrder:
    base = {
        "order_id": "01JABCDEFGHJKMNPQRSTVWXYZ0",
        "broker_order_id": None,
        "position_id": "01JPOSITION00000000000000",
        "proposal_id": "01JPROPOSAL00000000000000",
        "verdict_id": "01JVERDICT000000000000000",
        "role": OrderRole.ENTRY,
        "is_paper": True,
        "state": state,
        "product": "MIS",
        "side": "BUY",
        "qty": QTY,
        "filled_qty": 0,
        "price": Decimal("101.55"),
        "trigger_price": Decimal("99.05"),
        "modifications": 0,
        "reject_reason": None,
        "raw_broker_payload": None,
        "created_at": AT,
        "updated_at": AT,
    }
    base.update(kw)
    return PlatformOrder(**base)


@pytest.fixture
def store(conn: sqlite3.Connection) -> OrderStore:
    return OrderStore(conn)


# ----------------------------------------------------------------- round trip
def test_insert_and_get_round_trip_preserves_every_column(store: OrderStore) -> None:
    order = make_order(OrderState.VALIDATED, raw_broker_payload={"status": "OPEN", "n": 1})
    store.insert(order)
    got = store.get(order.order_id)
    assert got == order                       # Decimals, tz-aware IST datetimes and JSON all survive
    assert got is not None
    assert isinstance(got.price, Decimal) and got.price == Decimal("101.55")
    assert got.created_at.tzinfo is not None and got.created_at == AT
    assert got.is_paper is True


def test_get_missing_returns_none(store: OrderStore) -> None:
    assert store.get("nope") is None


def test_insert_is_not_an_upsert(store: OrderStore) -> None:
    order = make_order()
    store.insert(order)
    with pytest.raises(sqlite3.IntegrityError):
        store.insert(order)


def test_record_persists_order_and_event_atomically(store: OrderStore) -> None:
    order = make_order(OrderState.SUBMITTED)
    store.insert(order)
    after, ev = transition(order, OrderState.ACKED, payload={"status": "OPEN"}, at=AT)
    after = after.model_copy(update={"broker_order_id": "251009000123456"})
    store.record(order, after, ev)

    got = store.get(order.order_id)
    assert got is not None and got.state is OrderState.ACKED
    assert got.broker_order_id == "251009000123456"
    events = store.events(order.order_id)
    assert len(events) == 1
    assert events[0].from_state is OrderState.SUBMITTED
    assert events[0].to_state is OrderState.ACKED
    assert events[0].payload == {"status": "OPEN"}
    assert events[0].at == AT


def test_events_are_returned_in_arrival_order(store: OrderStore) -> None:
    order = make_order(OrderState.SUBMITTED)
    store.insert(order)
    cur = order
    for to, at in [
        (OrderState.ACKED, AT),
        (OrderState.PARTIALLY_FILLED, AT + timedelta(seconds=1)),
        (OrderState.FILLED, AT + timedelta(seconds=2)),
    ]:
        nxt, ev = transition(
            cur, to, payload={"status": to}, at=at, filled_qty=40 if to is OrderState.PARTIALLY_FILLED else None
        )
        store.record(cur, nxt, ev)
        cur = nxt
    assert [e.to_state for e in store.events(order.order_id)] == [
        OrderState.ACKED,
        OrderState.PARTIALLY_FILLED,
        OrderState.FILLED,
    ]


# ----------------------------------------------------------------- persist BEFORE side effect (R8)
def test_record_is_committed_before_it_returns(store: OrderStore, conn, db_path: str) -> None:
    """A second connection sees the new state the instant ``record`` returns — the caller may act."""
    order = make_order(OrderState.SUBMITTED)
    store.insert(order)
    after, ev = transition(order, OrderState.ACKED, payload={"status": "OPEN"}, at=AT)

    other = connect(db_path)
    try:
        before_row = other.execute(
            "SELECT state FROM orders WHERE order_id=?", (order.order_id,)
        ).fetchone()
        assert before_row["state"] == OrderState.SUBMITTED.value

        store.record(order, after, ev)

        # The side effect (the broker call) would happen HERE; the durable record already exists.
        row = other.execute("SELECT state FROM orders WHERE order_id=?", (order.order_id,)).fetchone()
        assert row["state"] == OrderState.ACKED.value
        n = other.execute(
            "SELECT COUNT(*) c FROM order_events WHERE order_id=?", (order.order_id,)
        ).fetchone()["c"]
        assert n == 1
    finally:
        other.close()


class _FailOnEventInsert:
    """Connection proxy that fails the order_events INSERT — the second half of ``record``.

    ``sqlite3.Connection.execute`` is a read-only C attribute (monkeypatching it raises), so the
    injection goes through a proxy; ROLLBACK still reaches the real connection.
    """

    def __init__(self, real: sqlite3.Connection) -> None:
        self._real = real
        self.failures = 0

    def execute(self, sql, *args, **kw):
        if sql.lstrip().upper().startswith("INSERT INTO ORDER_EVENTS"):
            self.failures += 1
            raise sqlite3.OperationalError("disk I/O error (injected)")
        return self._real.execute(sql, *args, **kw)


def test_record_rolls_back_both_halves_on_failure(store: OrderStore, conn) -> None:
    order = make_order(OrderState.SUBMITTED)
    store.insert(order)
    after, ev = transition(order, OrderState.ACKED, payload={"status": "OPEN"}, at=AT)

    flaky = _FailOnEventInsert(conn)
    with pytest.raises(sqlite3.OperationalError):
        OrderStore(flaky).record(order, after, ev)

    assert flaky.failures == 1
    got = store.get(order.order_id)
    assert got is not None and got.state is OrderState.SUBMITTED    # order row rolled back too
    assert store.events(order.order_id) == []


# ----------------------------------------------------------------- never backwards; terminal is final
def test_terminal_order_rejects_further_records(store: OrderStore) -> None:
    order = make_order(OrderState.PARTIALLY_FILLED, filled_qty=40)
    store.insert(order)
    filled, ev = transition(order, OrderState.FILLED, payload={"status": "COMPLETE"}, at=AT)
    store.record(order, filled, ev)

    # Any later record against the persisted terminal row is refused (never re-opened, R8/§3.5.1).
    ghost = filled.model_copy(update={"state": OrderState.CANCEL_PENDING})
    with pytest.raises(TerminalOrderWrite):
        store.record(filled, ghost, OrderEvent(
            from_state=OrderState.FILLED, to_state=OrderState.CANCEL_PENDING, payload={}, at=AT
        ))
    assert store.get(order.order_id).state is OrderState.FILLED
    assert len(store.events(order.order_id)) == 1


def test_record_noop_audits_a_postback_against_a_terminal_row(store: OrderStore) -> None:
    """§3.5.1 promises EVERY postback is recorded; ``record`` refuses terminal rows, so no-ops need
    their own door (2026-09-10 review finding: a replayed postback on a FILLED order was unrecordable
    and the audit chain lost the frame — an unrecorded broker action is an unexplained one, R8)."""
    order = make_order(OrderState.PARTIALLY_FILLED, filled_qty=40)
    store.insert(order)
    filled, ev = transition(order, OrderState.FILLED, payload={"status": "COMPLETE"}, at=AT)
    store.record(order, filled, ev)

    replay = OrderEvent(
        from_state=OrderState.FILLED,
        to_state=OrderState.FILLED,
        payload={"status": "COMPLETE", "duplicate": True, "duplicate_reason": "terminal"},
        at=AT + timedelta(seconds=5),
    )
    store.record_noop(order.order_id, replay)

    events = store.events(order.order_id)
    assert len(events) == 2
    assert events[1].payload["duplicate_reason"] == "terminal"
    got = store.get(order.order_id)
    assert got is not None and got.state is OrderState.FILLED and got.updated_at == AT   # row untouched


def test_record_noop_works_on_a_live_row_too(store: OrderStore) -> None:
    order = make_order(OrderState.ACKED)
    store.insert(order)
    store.record_noop(order.order_id, OrderEvent(
        from_state=OrderState.ACKED, to_state=OrderState.ACKED,
        payload={"intermediate": True}, at=AT,
    ))
    assert len(store.events(order.order_id)) == 1
    assert store.get(order.order_id).state is OrderState.ACKED


def test_record_noop_refuses_an_unannotated_self_edge(store: OrderStore) -> None:
    """2026-09-10 round-3 review (major): a no-op is an event ANNOTATED with a NOOP flag, not merely
    one whose states match — the PARTIALLY_FILLED accretion self-edge is a REAL transition (§3.2.8),
    and routing it here would audit a clean event while the fill never reached the orders row."""
    order = make_order(OrderState.PARTIALLY_FILLED, filled_qty=40)
    store.insert(order)
    accretion, ev = transition(
        order, OrderState.PARTIALLY_FILLED, payload={"status": "OPEN"}, at=AT, filled_qty=75,
    )
    assert ev.from_state is ev.to_state and not any(k in ev.payload for k in ("duplicate", "intermediate"))

    with pytest.raises(ValueError, match="record\\(\\)"):
        store.record_noop(order.order_id, ev)
    assert store.get(order.order_id).filled_qty == 40      # nothing was written by the refusal
    store.record(order, accretion, ev)                      # the right door still works
    assert store.get(order.order_id).filled_qty == 75


def test_record_noop_refuses_an_event_that_claims_a_transition(store: OrderStore) -> None:
    order = make_order(OrderState.ACKED)
    store.insert(order)
    with pytest.raises(ValueError):
        store.record_noop(order.order_id, OrderEvent(
            from_state=OrderState.ACKED, to_state=OrderState.FILLED, payload={}, at=AT,
        ))
    assert store.events(order.order_id) == []


def test_record_noop_before_insert_raises(store: OrderStore) -> None:
    with pytest.raises(LookupError):
        store.record_noop("01JNOSUCH", OrderEvent(
            from_state=OrderState.ACKED, to_state=OrderState.ACKED,
            payload={"duplicate": True, "duplicate_reason": "same_state"}, at=AT,
        ))


def test_record_against_a_stale_read_is_refused(store: OrderStore) -> None:
    order = make_order(OrderState.SUBMITTED)
    store.insert(order)
    acked, ev1 = transition(order, OrderState.ACKED, payload={"status": "OPEN"}, at=AT)
    store.record(order, acked, ev1)

    # A second writer still holding the pre-ack snapshot must not clobber the persisted row.
    stale_after, ev2 = transition(order, OrderState.CANCEL_PENDING, payload={}, at=AT)
    with pytest.raises(StaleOrderWrite):
        store.record(order, stale_after, ev2)
    assert store.get(order.order_id).state is OrderState.ACKED


def test_record_before_insert_raises(store: OrderStore) -> None:
    order = make_order(OrderState.SUBMITTED)
    after, ev = transition(order, OrderState.ACKED, payload={}, at=AT)
    with pytest.raises(LookupError):
        store.record(order, after, ev)


# ----------------------------------------------------------------- lookups
def test_by_broker_id(store: OrderStore) -> None:
    a = make_order(OrderState.ACKED, order_id="01JA", broker_order_id="251009000111111")
    b = make_order(OrderState.ACKED, order_id="01JB", broker_order_id="251009000222222")
    store.insert(a)
    store.insert(b)
    assert store.by_broker_id("251009000222222") == b
    assert store.by_broker_id("nosuch") is None


def test_by_broker_id_ignores_unset_broker_ids(store: OrderStore) -> None:
    store.insert(make_order(OrderState.SUBMITTED, order_id="01JA", broker_order_id=None))
    assert store.by_broker_id(None) is None       # never correlate on a NULL broker id (A3)


def test_open_orders_excludes_terminal_states(store: OrderStore) -> None:
    live = [OrderState.SUBMITTED, OrderState.ACKED, OrderState.PARTIALLY_FILLED,
            OrderState.CANCEL_PENDING, OrderState.MODIFY_PENDING]
    dead = [OrderState.FILLED, OrderState.CANCELLED, OrderState.REJECTED, OrderState.LAPSED]
    for i, st in enumerate(live + dead):
        store.insert(make_order(
            st,
            order_id=f"01J{i:022d}",
            filled_qty=40 if st is OrderState.PARTIALLY_FILLED else (QTY if st is OrderState.FILLED else 0),
            created_at=AT + timedelta(seconds=i),
        ))
    got = store.open_orders()
    assert [o.state for o in got] == live         # ordered by created_at, terminal excluded
