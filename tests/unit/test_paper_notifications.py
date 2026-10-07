"""Paper owner notifications (plan Q4.10): FILL and PAPER_ALERT messages, dedupe, the paper-only sink."""

from __future__ import annotations

import ast
import inspect
import logging
from decimal import Decimal

import pytest

from engine.core.calendar import NSECalendar
from engine.core.clock import Clock
from engine.core.config import config_dir, load_settings
from engine.core.enums import RiskState
from engine.notify import catalog
from engine.notify.catalog import CatalogMessage, MessageKind
from engine.ops import main as opsmain
from engine.ops.paper_control import PaperControl
from engine.ops.paper_runtime import PaperNotifier
from engine.risk.gate import GateContextBuilder
from engine.strategy.cost_model import CostModel
from tests.unit.test_paper_composition import LIMITS, Limits, Store
from tests.unit.test_protection_manager import Rig, at, orders, position, round_tick, seed, volume


@pytest.fixture
async def rig(conn, bus):
    r = Rig(conn, bus)
    yield r
    await r.mgr.stop()


class Sink:
    def __init__(self, failing: bool = False) -> None:
        self.sent: list[CatalogMessage] = []
        self.failing = failing

    async def __call__(self, msg: CatalogMessage) -> None:
        if self.failing:
            raise RuntimeError("telegram down")
        self.sent.append(msg)


def notifier(rig: Rig, sink: Sink) -> PaperNotifier:
    return PaperNotifier(rig.conn, Clock(time_source=rig.now), sink)


# ----------------------------------------------------------------------------------------- catalog
def test_paper_fills_reuse_the_non_critical_fill_kind_with_a_paper_title_and_flag() -> None:
    entry = catalog.paper_entry_filled(order_id="O1", symbol="TCS", qty=10, avg_price=Decimal("100.05"))
    close = catalog.paper_position_closed(
        position_id="POS1", symbol="TCS", qty=10, exit_price=Decimal("94.50"), net_pnl=Decimal("-58.35"),
        reason="stop", basis="fill",
    )

    assert entry.render() == "ℹ️ PAPER fill: TCS\nPaper entry filled: 10 TCS at an average of ₹100.05."
    assert close.render() == (
        "ℹ️ PAPER close: TCS\nPaper position closed: 10 TCS out at ₹94.50, net -₹58.35. Reason stop, basis fill."
    )
    for msg in (entry, close):
        assert (msg.kind, msg.severity, msg.data["is_paper"]) == (MessageKind.FILL, "info", True)
    assert (entry.dedupe_key, close.dedupe_key) == ("paper_fill:O1", "paper_close:POS1")
    assert entry.data["avg_price"] == "100.05" and close.data["net_pnl"] == "-58.35"


def test_paper_alert_is_critical_and_keyed_on_position_and_attempt() -> None:
    msg = catalog.paper_alert("POS1:2", "paper protection failed for POS1: no gtt")

    assert (msg.kind, msg.severity, msg.dedupe_key) == (MessageKind.PAPER_ALERT, "critical", "paper_alert:POS1:2")
    assert msg.render() == "🚨 PAPER alert\npaper protection failed for POS1: no gtt"


# ----------------------------------------------------------------------------------------- fills
async def test_an_entry_completion_and_a_close_are_each_sent_once(conn, rig) -> None:
    sink = Sink()
    paper = notifier(rig, sink)
    pid = await rig.enter()

    await paper.tick()
    await paper.tick()
    [fill] = sink.sent
    order_id = conn.execute("SELECT order_id FROM orders WHERE role = 'entry'").fetchone()[0]
    assert (fill.dedupe_key, fill.data["qty"], fill.data["avg_price"]) == (
        f"paper_fill:{order_id}", 10, position(conn, pid)["avg_entry"])

    await rig.px(at(10, 5), "94.50")
    await rig.px(at(10, 6), "94.50")
    await paper.tick()
    await paper.tick()
    close = sink.sent[1]
    ledger = conn.execute("SELECT exit_px, net_pnl FROM learning_ledger").fetchone()
    assert len(sink.sent) == 2 and close.dedupe_key == f"paper_close:{pid}"
    assert (close.data["exit_price"], close.data["net_pnl"], close.data["reason"], close.data["basis"]) == (
        ledger["exit_px"], ledger["net_pnl"], "stop", "fill")


async def test_a_partial_entry_fill_ended_by_a_cancel_is_sent_with_its_filled_qty(conn, rig) -> None:
    sink = Sink()
    paper = notifier(rig, sink)
    await rig.mgr.submit(*seed(conn, qty=20, limit="100"))
    volume(rig, 10)
    await rig.px(at(10, 1), "99.90", cum=5000)
    [entry] = orders(conn, "entry")
    await rig.mgr.cancel(entry["broker_order_id"], reason="test")
    await rig.settle()

    await paper.tick()
    assert orders(conn, "entry")[0]["state"] == "CANCELLED"
    assert [(m.dedupe_key, m.data["qty"]) for m in sink.sent] == [(f"paper_fill:{entry['order_id']}", 10)]


async def test_what_exists_at_boot_is_not_announced_again(conn, rig) -> None:
    pid = await rig.enter()
    await rig.px(at(10, 5), "94.50")
    await rig.px(at(10, 6), "94.50")
    sink = Sink()

    await notifier(rig, sink).tick()

    assert position(conn, pid)["state"] == "CLOSED" and sink.sent == []


async def test_a_void_is_not_sent(conn, rig) -> None:
    sink = Sink()
    paper = notifier(rig, sink)
    pid = await rig.enter()

    await rig.book.void(pid, Decimal("99"), at(10, 5), "corp_action_late")
    await paper.tick()

    assert [m.dedupe_key.split(":")[0] for m in sink.sent] == ["paper_fill"]


async def test_a_failing_sink_never_raises_and_is_not_retried_every_tick(conn, rig) -> None:
    sink = Sink(failing=True)
    paper = notifier(rig, sink)
    await rig.enter()

    await paper.tick()
    sink.failing = False
    await paper.tick()

    assert sink.sent == []


# ----------------------------------------------------------------------------------------- alerts
async def test_protection_and_construction_failures_are_critical_and_prep_is_only_logged(
    conn, rig, caplog
) -> None:
    sink = Sink()
    paper = notifier(rig, sink)
    with caplog.at_level(logging.CRITICAL, logger="engine.ops.paper_runtime"):
        await paper.alert("POS1:1", "protection failed")
        await paper.alert("construct", "paper construction failed")
        rig.now.value = at(10, 1)
        await notifier(rig, sink).alert("construct", "paper construction failed")
        await paper.alert("prep", "prep failed")

    keys = [m.dedupe_key for m in sink.sent]
    assert [m.severity for m in sink.sent] == ["critical"] * 3
    assert keys[0] == "paper_alert:POS1:1"
    assert keys[1].startswith("paper_alert:construct:") and keys[1] != keys[2]
    assert [r.key for r in caplog.records if r.getMessage() == "paper_alert"] == ["POS1:1", "construct", "construct", "prep"]


async def test_a_composition_failure_sends_one_paper_alert_through_the_owner_sink(conn, bus, monkeypatch) -> None:
    def broken(*_args, **_kwargs):
        raise RuntimeError("protection broke")

    monkeypatch.setattr(opsmain, "ProtectionManager", broken)
    clock = Clock(time_source=lambda: at(10, 0))
    sink = Sink()

    stack = await opsmain._compose_paper(
        conn, clock, NSECalendar(config_dir() / "calendar", clock, strict=False), bus, load_settings(),
        PaperControl(conn, clock), real_risk_state=lambda: RiskState.NORMAL, limits=Limits(),
        tick_size_fn=lambda _s: Decimal("0.05"), round_tick_fn=round_tick, cost_model=CostModel.from_config(),
        hold_fn=lambda *_: 20, store=Store(), backfill=None,
        ctx_builder_fn=lambda tracker: GateContextBuilder(LIMITS, tracker, None, None, None, clock, None, None,
                                                          conn=conn, scope="paper"),
        notify=sink,
    )

    assert stack is None
    [msg] = sink.sent
    assert (msg.kind, msg.severity) == (MessageKind.PAPER_ALERT, "critical")
    assert msg.dedupe_key.startswith("paper_alert:construct:")


def test_the_notifier_exists_only_inside_the_flagged_composition() -> None:
    module = ast.parse(inspect.getsource(opsmain))
    compose = next(n for n in module.body if isinstance(n, ast.AsyncFunctionDef) and n.name == "_compose_paper")
    inside = {id(n) for n in ast.walk(compose)}
    built = [n for n in ast.walk(module) if isinstance(n, ast.Call) and ast.unparse(n.func) == "PaperNotifier"]

    assert len(built) == 1 and id(built[0]) in inside
