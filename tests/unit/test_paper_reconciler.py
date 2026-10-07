"""Paper Reconciler (plan Q4.7; R5): lost postbacks, unseen legs, orphans, protection, isolation."""

from __future__ import annotations

import inspect
from decimal import Decimal

import pytest

from engine.core.clock import Clock
from engine.core.contracts import PAPER_ORDER_UPDATE_TOPIC
from engine.oms.reconcile import Reconciler
from engine.ops import lifecycle
from engine.paper.broker import PaperBroker
from engine.paper.fill_model import FillModelConfig
from tests.unit.test_exit_manager import real_rows
from tests.unit.test_protection_manager import SYM, TICK, Now, at, orders, position, seed
from tests.unit.test_session_prep import Stack, bar, flat, stacks

STRAY_ORDER = {"variety": "regular", "exchange": "NSE", "tradingsymbol": SYM, "transaction_type": "BUY",
               "order_type": "LIMIT", "price": Decimal("50"), "product": "CNC", "quantity": 1}
STRAY_GTT = {"tradingsymbol": SYM, "last_price": "100", "trigger_values": ["80"],
             "orders": [{"transaction_type": "SELL", "quantity": 1, "product": "CNC", "order_type": "LIMIT",
                         "price": "79"}]}


class Drop:
    """The broker's publish; while ``on``, every postback is lost."""

    def __init__(self, bus) -> None:
        self.bus, self.on = bus, False

    def __call__(self, topic, event) -> None:
        if not self.on:
            self.bus.publish(topic, event)


@pytest.fixture
async def make_stack(conn, bus):
    async with stacks(conn, bus) as build:
        yield build


@pytest.fixture
async def stack(make_stack) -> Stack:
    return make_stack()


@pytest.fixture
async def lossy(bus, make_stack) -> tuple[Stack, Drop]:
    drop, now = Drop(bus), Now()
    broker = PaperBroker(Clock(time_source=now), drop, FillModelConfig(), lambda _: TICK, 7, rejection_rate=0.0,
                         topic=PAPER_ORDER_UPDATE_TOPIC)
    return make_stack(broker=broker, now=now), drop


async def reconcile(stack: Stack) -> None:
    await stack.reconciler.run()
    await stack.settle()


def counts(stack: Stack) -> tuple[int, int, int, int]:
    c = stack.reconciler.counters
    return c.mismatches, c.redelivered, c.orphans_cancelled, c.gtts


def test_ctor_takes_only_a_paper_broker(conn, clock) -> None:
    with pytest.raises(TypeError, match="PaperBroker only"):
        Reconciler(conn, clock, object(), orders=None, protection=None)


async def test_a_lost_postback_is_re_applied_once_and_the_position_protected(conn, lossy) -> None:
    stack, drop = lossy
    drop.on = True
    await stack.mgr.submit(*seed(conn))
    await stack.px(at(10, 0, 1), "100.00")
    [entry] = orders(conn, "entry")
    assert entry["state"] == "SUBMITTED" and conn.execute("SELECT COUNT(*) FROM positions").fetchone()[0] == 0

    drop.on = False
    for _ in range(2):
        await reconcile(stack)
    [entry] = orders(conn, "entry")
    pos = position(conn, entry["position_id"])
    assert (entry["state"], pos["state"], pos["protection_state"]) == ("FILLED", "OPEN", "PROTECTED")
    assert [g["status"] for g in await stack.broker.gtts()] == ["active"]
    assert counts(stack) == (1, 1, 0, 0)


async def test_an_unseen_leg_is_adopted_and_its_pending_exit_respected(conn, lossy) -> None:
    stack, drop = lossy
    pid = await stack.enter()
    drop.on = True
    await stack.px(at(10, 5), "90.00")
    assert orders(conn, "gtt_leg") == []

    drop.on = False
    await reconcile(stack)
    [leg] = orders(conn, "gtt_leg")
    assert (leg["state"], leg["position_id"], position(conn, pid)["state"]) == ("ACKED", pid, "PENDING_EXIT")
    assert orders(conn, "exit") == [] and conn.execute("SELECT COUNT(*) FROM gtts").fetchone()[0] == 1
    assert counts(stack) == (2, 1, 0, 1)


async def test_orphans_are_cancelled_and_an_unprotected_position_is_handed_to_protection(conn, stack) -> None:
    pid = await stack.enter()
    [lost] = await stack.broker.gtts()
    await stack.broker.delete_gtt(lost["id"])
    stray_gtt = await stack.broker.place_gtt(STRAY_GTT)
    stray = await stack.broker.place_order(STRAY_ORDER, intent="risk_reducing")
    await stack.settle()

    await reconcile(stack)
    [stray_row] = [o for o in await stack.broker.orders() if o["order_id"] == stray]
    [armed] = await stack.broker.gtts()
    assert stray_row["status"] == "CANCELLED" and armed["id"] not in (lost["id"], stray_gtt)
    assert position(conn, pid)["protection_state"] == "PROTECTED"
    assert counts(stack) == (3, 0, 1, 2)


async def test_a_raising_pass_is_logged_and_counted_never_raised_and_no_lifecycle_hook(stack, monkeypatch) -> None:
    async def broken() -> list:
        raise RuntimeError("book unreadable")

    monkeypatch.setattr(stack.broker, "orders", broken)
    await stack.reconciler.run()
    assert (stack.reconciler.counters.passes, stack.reconciler.counters.failures) == (1, 1)
    source = inspect.getsource(lifecycle)
    assert "Reconciler" not in source and "SessionPrep" not in source


async def test_prep_and_reconcile_leave_a_same_symbol_real_row_untouched(conn, stack) -> None:
    conn.execute("INSERT INTO positions (position_id, symbol, side, product, qty, avg_entry, stop, state, is_paper, "
                 "origin, opened_at) VALUES ('REAL', ?, 'BUY', 'CNC', 10, '100', '95', 'OPEN', 0, 'recommended', ?)",
                 (SYM, at(9, 30).isoformat()))
    conn.execute("INSERT INTO gtts (gtt_id, position_id, state, trigger_low) VALUES (1, 'REAL', 'active', '95')")
    conn.execute("INSERT INTO orders (order_id, broker_order_id, position_id, role, is_paper, state, product, side, "
                 "qty) VALUES ('RO', '250000000000001', 'REAL', 'exit', 0, 'ACKED', 'CNC', 'SELL', 10)")
    before = real_rows(conn)
    pid = await stack.enter()
    await stack.broker.place_order(STRAY_ORDER, intent="risk_reducing")
    stack.bars[SYM] = flat(at(10, 0), 30) + [bar(at(10, 30), low="94")]

    await stack.prep.run(at(10, 0, 1), at(11, 0))
    await reconcile(stack)
    assert position(conn, pid)["state"] == "CLOSED" and stack.reconciler.counters.orphans_cancelled == 1
    assert real_rows(conn) == before
