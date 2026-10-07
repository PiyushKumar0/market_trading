"""The paper autopilot as the composition root builds it (plan Q4.1-Q4.9)."""

from __future__ import annotations

import datetime as dt
import logging
from decimal import Decimal
from pathlib import Path

import pytest
import yaml

from engine.core.calendar import NSECalendar
from engine.core.clock import Clock
from engine.core.config import config_dir, load_settings
from engine.core.contracts import PAPER_ORDER_UPDATE_TOPIC
from engine.core.enums import RiskState
from engine.ops import main as opsmain
from engine.ops import paper_runtime as runtime_mod
from engine.ops.paper_control import PaperControl
from engine.risk.gate import GateContextBuilder
from engine.risk.limits import LimitTable
from engine.strategy.cost_model import CostModel
from tests.unit.test_protection_manager import SYM, Now, at, round_tick, seed, tick

LIMITS = LimitTable.model_validate(
    yaml.safe_load((Path(__file__).resolve().parents[2] / "config" / "limits.yaml").read_text(encoding="utf-8"))
)
TOPICS = ("tick", "bar.1m", PAPER_ORDER_UPDATE_TOPIC)


class Limits:
    def __init__(self, broken: bool = False) -> None:
        self.broken = broken

    def load(self) -> LimitTable:
        if self.broken:
            raise RuntimeError("limits unverified")
        return LIMITS


class Store:
    """The MarketStore surface paper reads: corporate actions and 1m bars."""

    def __init__(self, corp: list[dict] | None = None, broken: bool = False) -> None:
        self.corp = corp or []
        self.broken = broken

    async def arun(self, fn, /, *args, **kwargs):
        return fn(*args, **kwargs)

    def get_corp_actions(self, *, ex_from, ex_to):
        if self.broken:
            raise RuntimeError("store stalled")
        return [r for r in self.corp if ex_from <= r["ex_date"] and (ex_to is None or r["ex_date"] <= ex_to)]

    def get_bars_1m(self, symbol, start, end):
        return []


@pytest.fixture
def now() -> Now:
    return Now()


@pytest.fixture
async def compose(conn, bus, now):
    clock = Clock(time_source=now)
    calendar = NSECalendar(config_dir() / "calendar", clock, strict=False)
    control = PaperControl(conn, clock)
    stacks: list[opsmain.PaperStack] = []

    async def build(*, store: Store | None = None, limits: Limits | None = None, real=lambda: RiskState.NORMAL,
                    notify=opsmain.no_notify):
        limits = limits or Limits()
        stack = await opsmain._compose_paper(
            conn, clock, calendar, bus, load_settings(), control,
            real_risk_state=real, limits=limits, tick_size_fn=lambda _s: Decimal("0.05"),
            round_tick_fn=round_tick, cost_model=CostModel.from_config(), hold_fn=lambda *_: 20,
            store=store or Store(), backfill=None,
            ctx_builder_fn=lambda tracker: GateContextBuilder(
                limits, tracker, None, None, calendar, clock, None, None, conn=conn, scope="paper"),
            notify=notify,
        )
        if stack is not None:
            stacks.append(stack)
        return stack

    yield build
    for stack in stacks:
        await stack.stop()


async def settle(bus, stack) -> None:
    for _ in range(4):
        await bus.drain(1.0)
        await stack.orders.join()


def subscribers(bus) -> dict[str, int]:
    return {t: len(bus._subscribers.get(t, [])) for t in TOPICS}


async def prepped(bus, now, stack) -> None:
    stack.control.set_enabled(True, "owner")
    bus.publish("tick", tick(at(10, 0), "100"))           # the session's first tick runs prep
    await settle(bus, stack)
    assert stack.entries_open()


async def test_every_manager_is_built_once_and_wired_end_to_end(conn, bus, now, compose, monkeypatch):
    built: dict[str, int] = {}

    def counted(module, name: str) -> None:
        cls = getattr(module, name)

        def make(*args, **kwargs):
            built[name] = built.get(name, 0) + 1
            return cls(*args, **kwargs)

        monkeypatch.setattr(module, name, make)

    for name in ("PaperRuntime", "OrderManager", "PositionBook", "ExitManager", "ProtectionManager", "PaperRisk"):
        counted(opsmain, name)
    for name in ("SessionPrep", "Reconciler"):
        counted(runtime_mod, name)

    stack = await compose()
    assert stack is not None and stack.ctx_builder._scope == "paper"
    assert set(built.values()) == {1} and len(built) == 8
    assert subscribers(bus) == dict.fromkeys(TOPICS, 1)
    assert [name for name, _ in stack.runtime._steps] == ["protection", "exits", "equity", "entries"]

    await prepped(bus, now, stack)
    proposal, verdict = seed(conn)
    order = await stack.submit(proposal, verdict)
    assert order is not None and order.is_paper
    now.value = at(10, 0, 1)
    bus.publish("tick", tick(now.value, "100"))           # bridge -> broker fill -> OMS -> book -> GTT
    await settle(bus, stack)

    pos = conn.execute("SELECT * FROM positions WHERE position_id = ?", (order.position_id,)).fetchone()
    assert (pos["symbol"], pos["state"], pos["is_paper"], pos["origin"]) == (SYM, "OPEN", 1, "platform")
    gtts = conn.execute("SELECT state, is_paper FROM gtts WHERE position_id = ?", (order.position_id,)).fetchall()
    assert [tuple(g) for g in gtts] == [("active", 1)]

    await stack.runtime.paper_tick()                       # the equity step is PaperRisk.tick
    assert conn.execute("SELECT COUNT(*) FROM paper_equity_snapshots").fetchone()[0] == 1
    assert stack.status()["open_positions"] == 1
    assert stack.counters() == {"reconcile_mismatches": 0, "voids": 0, "late_corp_actions": 0}


async def test_the_position_book_is_given_the_full_round_trip_and_the_spread_free_fees(compose, monkeypatch):
    seen: dict = {}
    cls = opsmain.PositionBook
    monkeypatch.setattr(opsmain, "PositionBook", lambda *a, **kw: seen.update(kw) or cls(*a, **kw))
    await compose()

    full = CostModel.from_config().round_trip(Decimal("20000"), "CNC")
    assert seen["round_trip_fn"](Decimal("20000"), "CNC") == full.total_cost
    assert seen["fees_fn"](Decimal("20000"), "CNC") == full.total_cost - full.components["spread"]
    assert full.components["spread"] > 0


@pytest.mark.parametrize("store", [Store(corp=[{"symbol": SYM, "ex_date": dt.date(2026, 6, 25), "kind": "bonus"}]),
                                   Store(broken=True)], ids=["ex-date in the hold", "unreadable"])
async def test_a_corporate_action_refusal_places_no_paper_order(conn, bus, now, compose, store):
    stack = await compose(store=store)
    await prepped(bus, now, stack)

    assert await stack.submit(*seed(conn)) is None
    assert conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0] == 0


async def test_paper_entries_wait_for_the_switch_and_session_prep(conn, bus, now, compose):
    stack = await compose()
    stack.control.set_enabled(True, "owner")
    assert not stack.entries_open()                        # no prep yet this session
    await prepped(bus, now, stack)
    stack.control.set_enabled(False, "owner")
    assert not stack.entries_open()


@pytest.mark.parametrize("block", ["paper off", "real kill", "paper halt"])
async def test_a_resting_paper_entry_is_cancelled_once_entries_are_blocked(conn, bus, now, compose, block):
    real = [RiskState.NORMAL]
    stack = await compose(real=lambda: real[0])
    await prepped(bus, now, stack)
    order = await stack.submit(*seed(conn, limit="99"))
    await settle(bus, stack)

    if block == "paper off":
        stack.control.set_enabled(False, "owner")
    elif block == "real kill":
        real[0] = RiskState.KILLED
    else:
        conn.execute("INSERT INTO paper_halts (cause, rung, set_at) VALUES ('daily_loss_hard', 'CLOSE_ONLY', ?)",
                     (now.value.isoformat(),))
    await stack.runtime.paper_tick()
    await settle(bus, stack)
    now.value = at(10, 0, 30)
    bus.publish("tick", tick(now.value, "98"))
    await settle(bus, stack)

    entry = conn.execute("SELECT state, filled_qty FROM orders WHERE order_id = ?", (order.order_id,)).fetchone()
    assert tuple(entry) == ("CANCELLED", 0)
    assert conn.execute("SELECT COUNT(*) FROM positions WHERE state = 'OPEN'").fetchone()[0] == 0


async def test_a_fill_applied_since_the_last_paper_tick_is_announced_on_stop(conn, bus, now, compose):
    sent = []

    async def sink(msg) -> None:
        sent.append(msg)

    stack = await compose(notify=sink)
    await prepped(bus, now, stack)
    order = await stack.submit(*seed(conn))
    now.value = at(10, 0, 1)
    bus.publish("tick", tick(now.value, "100"))
    await settle(bus, stack)

    await stack.stop()
    assert [m.dedupe_key for m in sent] == [f"paper_fill:{order.order_id}"]


@pytest.mark.parametrize("failure", ["manager raises", "runtime degraded"])
async def test_a_construction_failure_leaves_paper_off_and_never_raises(
    conn, bus, compose, monkeypatch, caplog, failure
):
    if failure == "manager raises":
        def broken(*_args, **_kwargs):
            raise RuntimeError("protection broke")

        monkeypatch.setattr(opsmain, "ProtectionManager", broken)
    with caplog.at_level(logging.CRITICAL, logger="engine.ops.paper_runtime"):
        stack = await compose(limits=Limits(broken=failure == "runtime degraded"))

    assert stack is None
    assert subscribers(bus) == dict.fromkeys(TOPICS, 0)   # no bridge, no OMS consumer
    assert [r.key for r in caplog.records if r.getMessage() == "paper_alert"] == ["construct"]
