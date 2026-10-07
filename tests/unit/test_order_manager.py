"""Paper OrderManager (plan Q4.3): submission, the paper guard, the one postback consumer, leg adoption."""

from __future__ import annotations

import asyncio
import datetime as dt
from dataclasses import dataclass, field
from decimal import Decimal

import pytest

from engine.core.clock import IST, Clock
from engine.core.contracts import (
    ORDER_UPDATE_TOPIC,
    PAPER_ORDER_UPDATE_TOPIC,
    EnterAction,
    GateVerdict,
    OrderUpdateFrame,
)
from engine.core.enums import Mode, RiskState
from engine.core.types import Bar, Tick
from engine.oms.manager import OrderManager, PaperOrderBlocked, SubmitRefused, paper_order_guard
from engine.oms.state import OrderRole, OrderState
from engine.oms.store import OrderStore
from engine.paper.broker import PaperBroker
from engine.paper.fill_model import FillModelConfig

SYM = "TCS"
S = OrderState


def at(h: int, m: int, s: int = 0) -> dt.datetime:
    return dt.datetime(2026, 6, 17, h, m, s, tzinfo=IST)


def tick(ts: dt.datetime, ltp: str, cum: int = 0) -> Tick:
    px = Decimal(ltp)
    return Tick(
        instrument_token=1, tradingsymbol=SYM, ltp=px, volume_traded=cum, exchange_ts=ts,
        bid=px - Decimal("0.05"), ask=px + Decimal("0.05"),
    )


class _Now:
    def __init__(self) -> None:
        self.value = at(10, 0)

    def __call__(self) -> dt.datetime:
        return self.value


@dataclass
class _Flags:
    enabled: bool = True
    state: RiskState = RiskState.NORMAL


@dataclass
class _Recorder:
    store: OrderStore
    fills: list[tuple] = field(default_factory=list)
    transitions: list[tuple] = field(default_factory=list)

    async def on_fill(self, order, qty, price, when) -> None:
        persisted = self.store.get(order.order_id)
        assert (persisted.state, persisted.filled_qty) == (order.state, order.filled_qty)
        self.fills.append((order, qty, price, when))

    def on_transition(self, order, event) -> None:
        assert self.store.get(order.order_id).state is order.state
        self.transitions.append((order.role, event.from_state, event.to_state))


@dataclass
class _Rig:
    broker: PaperBroker
    mgr: OrderManager
    rec: _Recorder
    now: _Now


@pytest.fixture
def flags() -> _Flags:
    return _Flags()


@pytest.fixture
async def make_rig(conn, bus, flags):
    managers: list[OrderManager] = []

    def build(*, rejection_rate: float = 0.0, on_fill=None) -> _Rig:
        now = _Now()
        guard = paper_order_guard(lambda: flags.enabled, lambda: flags.state)
        broker = PaperBroker(
            Clock(time_source=now), bus.publish, FillModelConfig(), lambda _: Decimal("0.05"), 7,
            rejection_rate=rejection_rate, order_guard=guard, topic=PAPER_ORDER_UPDATE_TOPIC,
        )
        rec = _Recorder(OrderStore(conn))
        mgr = OrderManager(
            conn, Clock(time_source=now), broker,
            order_guard=guard, on_fill=on_fill or rec.on_fill, on_transition=rec.on_transition,
        )
        mgr.start(bus)
        managers.append(mgr)
        return _Rig(broker, mgr, rec, now)

    yield build
    for mgr in managers:
        await mgr.stop()


@pytest.fixture
async def rig(make_rig) -> _Rig:
    return make_rig()


async def settle(bus, mgr: OrderManager) -> None:
    for _ in range(3):
        await bus.drain(1.0)
        await mgr.join()


def seed_verdict(conn, *, qty: int = 10, paper: bool = True, n: int = 1) -> tuple[EnterAction, GateVerdict]:
    proposal = EnterAction(
        action="enter", proposal_id=f"P{n}", thesis="paper order manager test thesis", confidence=0.6,
        tradingsymbol=SYM, exchange="NSE", side="BUY", style="swing", entry_type="MARKET",
        stop_price=Decimal("95"), quantity=qty, signal_id="s", strategy_id="hi52", features_snapshot_id="f",
    )
    verdict = GateVerdict(
        verdict_id=f"V{n}", proposal_id=proposal.proposal_id, verdict="approve", original_qty=qty,
        approved_qty=qty, checks=[], mode=Mode.RECOMMEND, risk_state=RiskState.NORMAL,
        degrade_tier="DG0", evaluated_at=at(10, 0),
    )
    conn.execute(
        "INSERT INTO proposals (proposal_id, agent_id, action, payload, inputs_digest, created_at) "
        "VALUES (?, 'a', 'enter', ?, 'd', ?)",
        (proposal.proposal_id, proposal.model_dump_json(), at(10, 0).isoformat()),
    )
    conn.execute(
        "INSERT INTO verdicts (verdict_id, proposal_id, verdict, payload, evaluated_at, is_paper) "
        "VALUES (?, ?, 'approve', ?, ?, ?)",
        (verdict.verdict_id, proposal.proposal_id, verdict.model_dump_json(), at(10, 0).isoformat(), int(paper)),
    )
    return proposal, verdict


def seed_position(conn, *, paper: bool = True, state: str = "OPEN") -> str:
    conn.execute(
        "INSERT INTO positions (position_id, symbol, side, product, qty, avg_entry, stop, state, "
        "protection_state, is_paper, origin, opened_at) VALUES ('POS1', ?, 'BUY', 'CNC', 10, '100', '95', ?, "
        "'PROTECTED', ?, ?, ?)",
        (SYM, state, int(paper), "platform" if paper else "recommended", at(9, 30).isoformat()),
    )
    return "POS1"


def transitions(conn, order_id: str) -> list[tuple[OrderState, OrderState]]:
    return [(e.from_state, e.to_state) for e in OrderStore(conn).events(order_id)]


def count_orders(conn) -> int:
    return conn.execute("SELECT count(*) FROM orders").fetchone()[0]


ENTRY_TO_FILLED = [(S.DRAFT, S.VALIDATED), (S.VALIDATED, S.SUBMITTED), (S.SUBMITTED, S.SUBMITTED),
                   (S.SUBMITTED, S.ACKED), (S.ACKED, S.FILLED)]


# ------------------------------------------------------------------------------------- construction
@pytest.mark.parametrize("make_broker, error", [
    (lambda clock: object(), TypeError),
    (lambda clock: PaperBroker(clock, lambda *_: None, FillModelConfig(), lambda _: Decimal("0.05"), 1), ValueError),
], ids=["not-a-paper-broker", "paper-broker-on-the-real-topic"])
def test_ctor_takes_only_a_paper_broker_on_the_paper_topic(conn, clock, make_broker, error) -> None:
    with pytest.raises(error):
        OrderManager(conn, clock, make_broker(clock), order_guard=lambda _: None, on_fill=lambda *_: None)


@pytest.mark.parametrize("intent, enabled, state, blocked", [
    ("entry", True, RiskState.NORMAL, False),
    ("entry", False, RiskState.NORMAL, True),
    ("entry", True, RiskState.FROZEN, True),
    ("entry", True, RiskState.CLOSE_ONLY, True),
    ("entry", True, RiskState.KILLED, True),
    ("risk_reducing", False, RiskState.KILLED, False),
    ("modify", True, RiskState.NORMAL, True),
])
def test_paper_order_guard(intent, enabled, state, blocked) -> None:
    guard = paper_order_guard(lambda: enabled, lambda: state)
    if blocked:
        with pytest.raises(PaperOrderBlocked):
            guard(intent)
    else:
        guard(intent)


# ------------------------------------------------------------------------------------- submission
@pytest.mark.parametrize("paper, change", [
    (True, lambda p, v: (p.model_copy(update={"style": "intraday"}), v)),
    (True, lambda p, v: (p.model_copy(update={"side": "SELL"}), v)),
    (True, lambda p, v: (p.model_copy(update={"entry_type": "LIMIT", "entry_price": None}), v)),
    (True, lambda p, v: (p, v.model_copy(update={"verdict": "reject"}))),
    (True, lambda p, v: (p, v.model_copy(update={"proposal_id": "P9"}))),
    (True, lambda p, v: (p, v.model_copy(update={"approved_qty": 0}))),
    (True, lambda p, v: (p, v.model_copy(update={"approved_qty": 11}))),
    (False, lambda p, v: (p, v)),
], ids=["mis", "short", "limit-without-price", "rejected", "other-proposal", "zero-qty", "enlarged",
        "real-verdict"])
async def test_submit_refuses_outside_paper_scope_and_persists_nothing(conn, rig, paper, change) -> None:
    proposal, verdict = change(*seed_verdict(conn, paper=paper))
    with pytest.raises(SubmitRefused):
        await rig.mgr.submit(proposal, verdict)
    assert count_orders(conn) == 0
    assert await rig.broker.orders() == []


async def test_entry_persists_before_the_broker_call_and_reports_the_committed_fill(
    conn, bus, rig, monkeypatch
) -> None:
    seen: list[tuple] = []
    place = rig.broker.place_order

    async def spy(req, intent):
        row = conn.execute("SELECT * FROM orders").fetchone()
        seen.append((row["state"], row["broker_order_id"], row["is_paper"], row["role"], row["verdict_id"], intent))
        return await place(req, intent)

    monkeypatch.setattr(rig.broker, "place_order", spy)
    order = await rig.mgr.submit(*seed_verdict(conn))
    rig.now.value = at(10, 0, 1)
    rig.broker.on_tick(tick(at(10, 0, 1), "100.00"))
    await settle(bus, rig.mgr)

    assert seen == [("SUBMITTED", None, 1, "entry", "V1", "entry")]
    assert transitions(conn, order.order_id) == ENTRY_TO_FILLED
    [(filled, qty, price, when)] = rig.rec.fills
    assert (filled.state, filled.filled_qty, qty, filled.position_id) == (S.FILLED, 10, 10, order.position_id)
    assert price == filled.raw_broker_payload["average_price"] and when == at(10, 0, 1)
    assert rig.rec.transitions == [(OrderRole.ENTRY, S.SUBMITTED, S.ACKED), (OrderRole.ENTRY, S.ACKED, S.FILLED)]


@pytest.mark.parametrize("postbacks_first", [True, False], ids=["postbacks-first", "response-first"])
async def test_postbacks_and_the_place_response_in_either_order(conn, bus, rig, monkeypatch, postbacks_first) -> None:
    place = rig.broker.place_order

    def fill() -> None:
        rig.now.value = at(10, 0, 1)
        rig.broker.on_tick(tick(at(10, 0, 1), "100.00"))

    async def slow_response(req, intent):
        broker_order_id = await place(req, intent)
        fill()
        await settle(bus, rig.mgr)
        row = conn.execute("SELECT state, broker_order_id FROM orders").fetchone()
        assert (row["state"], row["broker_order_id"]) == ("SUBMITTED", None)
        return broker_order_id

    if postbacks_first:
        monkeypatch.setattr(rig.broker, "place_order", slow_response)
    order = await rig.mgr.submit(*seed_verdict(conn))
    if not postbacks_first:
        fill()
    await settle(bus, rig.mgr)

    assert transitions(conn, order.order_id) == ENTRY_TO_FILLED
    assert [(o.filled_qty, qty) for o, qty, _, _ in rig.rec.fills] == [(10, 10)]


async def test_back_to_back_partial_fills_with_an_awaiting_callback_build_one_protection(
    conn, bus, make_rig
) -> None:
    gtt_qty: dict[str, int] = {}
    placed: list[str] = []
    calls: list[tuple[int, int]] = []
    prices: list[Decimal] = []

    async def on_fill(order, qty, price, when) -> None:
        exists = order.order_id in gtt_qty
        for _ in range(3):
            await asyncio.sleep(0)
        if not exists:
            placed.append(order.order_id)
        gtt_qty[order.order_id] = order.filled_qty
        calls.append((order.filled_qty, qty))
        prices.append(price)

    rig = make_rig(on_fill=on_fill)
    order = await rig.mgr.submit(*seed_verdict(conn, qty=100))
    rig.broker.on_bar(Bar(symbol=SYM, ts_minute=at(10, 0), open=Decimal(100), high=Decimal(100),
                          low=Decimal(100), close=Decimal(100), volume=300))
    rig.broker.on_tick(tick(at(10, 1), "100.00", cum=5000))
    rig.broker.on_tick(tick(at(10, 2), "100.00", cum=5000))
    await settle(bus, rig.mgr)

    assert calls == [(30, 30), (60, 30)]
    assert placed == [order.order_id] and gtt_qty == {order.order_id: 60}
    average = Decimal(OrderStore(conn).get(order.order_id).raw_broker_payload["average_price"])
    assert abs(sum(prices) * 30 - average * 60) < Decimal("0.01")


async def test_a_repeat_submit_for_the_same_verdict_returns_the_first_order(conn, rig) -> None:
    proposal, verdict = seed_verdict(conn)
    first = await rig.mgr.submit(proposal, verdict)
    again = await rig.mgr.submit(proposal, verdict)
    assert again.order_id == first.order_id
    assert count_orders(conn) == 1 and len(await rig.broker.orders()) == 1


async def test_an_injected_rejection_ends_the_entry_rejected_with_no_fill(conn, bus, make_rig) -> None:
    rig = make_rig(rejection_rate=1.0)
    order = await rig.mgr.submit(*seed_verdict(conn))
    await settle(bus, rig.mgr)
    rejected = OrderStore(conn).get(order.order_id)
    assert rejected.state is S.REJECTED and "injected" in rejected.reject_reason
    assert rig.rec.fills == []
    assert rig.rec.transitions == [(OrderRole.ENTRY, S.SUBMITTED, S.REJECTED)]


async def test_a_blocked_entry_is_abandoned_before_the_broker_while_exits_still_go(conn, bus, rig, flags) -> None:
    flags.state = RiskState.CLOSE_ONLY
    entry = await rig.mgr.submit(*seed_verdict(conn))
    exit_order = await rig.mgr.submit_exit(seed_position(conn), 10)
    await settle(bus, rig.mgr)

    assert entry.state is S.REJECTED and "CLOSE_ONLY" in entry.reject_reason
    assert transitions(conn, entry.order_id) == [(S.DRAFT, S.REJECTED)]
    with pytest.raises(PaperOrderBlocked):
        await rig.broker.place_order({}, intent="entry")
    [booked] = await rig.broker.orders()
    assert booked["order_id"] == exit_order.broker_order_id and booked["transaction_type"] == "SELL"
    exit_row = OrderStore(conn).get(exit_order.order_id)
    assert (exit_row.role, exit_row.is_paper, exit_row.state, exit_row.position_id) == (
        OrderRole.EXIT, True, S.ACKED, "POS1")


@pytest.mark.parametrize("paper, state", [(False, "OPEN"), (True, "CLOSED")], ids=["real", "closed"])
async def test_an_exit_needs_a_held_paper_position(conn, rig, paper, state) -> None:
    with pytest.raises(SubmitRefused):
        await rig.mgr.submit_exit(seed_position(conn, paper=paper, state=state), 10)
    assert count_orders(conn) == 0


# ------------------------------------------------------------------------------------- postbacks
async def test_a_fired_gtt_leg_is_adopted_onto_its_position(conn, bus, rig) -> None:
    position_id = seed_position(conn)
    gtt_id = await rig.broker.place_gtt({
        "tradingsymbol": SYM, "last_price": Decimal(100), "trigger_values": [Decimal(95)],
        "orders": [{"transaction_type": "SELL", "quantity": 10, "product": "CNC", "price": Decimal(95)}],
    })
    conn.execute(
        "INSERT INTO gtts (gtt_id, position_id, state, trigger_low, is_paper, symbol, side, product, qty) "
        "VALUES (?, ?, 'active', '95', 1, ?, 'SELL', 'CNC', 10)",
        (gtt_id, position_id, SYM),
    )
    rig.broker.on_tick(tick(at(10, 0, 1), "94.00"))
    await settle(bus, rig.mgr)
    rig.now.value = at(10, 0, 3)
    rig.broker.on_tick(tick(at(10, 0, 3), "96.00"))
    await settle(bus, rig.mgr)

    [(leg_id,)] = conn.execute("SELECT order_id FROM orders").fetchall()
    leg = OrderStore(conn).get(leg_id)
    assert (leg.role, leg.position_id, leg.is_paper, leg.side, leg.qty, leg.state) == (
        OrderRole.GTT_LEG, position_id, True, "SELL", 10, S.FILLED)
    assert transitions(conn, leg_id) == [(S.SUBMITTED, S.ACKED), (S.ACKED, S.FILLED)]
    assert [(o.role, qty) for o, qty, _, _ in rig.rec.fills] == [(OrderRole.GTT_LEG, 10)]
    assert rig.mgr._pending.accounting().held == 0


@pytest.mark.parametrize("tag, held", [(None, 1), ("gtt:123:0", 0)], ids=["untagged", "tag-of-unknown-gtt"])
async def test_an_unknown_frame_is_held_only_when_untagged(conn, bus, rig, tag, held) -> None:
    frame = {"order_id": "900000000000999", "status": "OPEN", "filled_quantity": 0, "quantity": 10,
             "transaction_type": "SELL", "product": "CNC", "tag": tag}
    bus.publish(PAPER_ORDER_UPDATE_TOPIC, OrderUpdateFrame(data=frame))
    await settle(bus, rig.mgr)
    assert count_orders(conn) == 0
    assert rig.mgr._pending.accounting().held == held


async def test_a_real_order_update_never_reaches_paper(conn, bus, rig) -> None:
    order = await rig.mgr.submit(*seed_verdict(conn))
    await settle(bus, rig.mgr)
    bus.publish(ORDER_UPDATE_TOPIC, OrderUpdateFrame(data={
        "order_id": order.broker_order_id, "status": "CANCELLED", "filled_quantity": 0, "quantity": 10}))
    await settle(bus, rig.mgr)
    assert OrderStore(conn).get(order.order_id).state is S.ACKED
