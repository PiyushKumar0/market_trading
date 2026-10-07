"""Paper construction, the market-data bridge and the session-prep gate (plan Q4.1)."""

from __future__ import annotations

import asyncio
import datetime as dt
import shutil
from decimal import Decimal

import pytest

from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.config import PaperSettings, config_dir
from engine.core.contracts import ORDER_UPDATE_TOPIC, PAPER_ORDER_UPDATE_TOPIC, OrderUpdateFrame
from engine.core.enums import Actor, RiskState
from engine.core.protected_store import ProtectedStore
from engine.core.types import Bar, OwnerConfirmation, Tick
from engine.oms.manager import OrderManager, PaperOrderBlocked, paper_order_guard
from engine.oms.state import OrderState
from engine.oms.store import OrderStore
from engine.ops.paper_control import PaperControl
from engine.ops.paper_runtime import PaperRuntime, PrepState
from engine.paper.broker import GTT_ID_BASE, ORDER_ID_BASE
from engine.paper.fill_model import FillModelConfig, load_fill_model
from engine.risk.limits import LimitsEngine
from tests.unit.test_order_manager import seed_verdict

D = dt.date(2026, 6, 17)
S = OrderState
ENTRY = {
    "variety": "regular", "exchange": "NSE", "tradingsymbol": "TCS", "transaction_type": "BUY",
    "order_type": "MARKET", "product": "CNC", "quantity": 1,
}


def at(h: int, m: int, s: int = 0, d: dt.date = D) -> dt.datetime:
    return dt.datetime.combine(d, dt.time(h, m, s), tzinfo=IST)


def tick(ts: dt.datetime, ltp: str = "100") -> Tick:
    px = Decimal(ltp)
    return Tick(instrument_token=1, tradingsymbol="TCS", ltp=px, volume_traded=1000, exchange_ts=ts,
                bid=px - Decimal("0.05"), ask=px + Decimal("0.05"))


def bar(minute: dt.datetime) -> Bar:
    px = Decimal("100")
    return Bar(symbol="TCS", ts_minute=minute, open=px, high=px, low=px, close=px, volume=500)


class _Now:
    def __init__(self) -> None:
        self.value = at(10, 0)

    def __call__(self) -> dt.datetime:
        return self.value


class _Prep:
    def __init__(self) -> None:
        self.calls: list[tuple[dt.datetime | None, dt.datetime]] = []
        self.gate: asyncio.Event | None = None
        self.raises = False

    async def __call__(self, since: dt.datetime | None, until: dt.datetime) -> None:
        self.calls.append((since, until))
        if self.gate is not None:
            await self.gate.wait()
        if self.raises:
            raise RuntimeError("prep broke")


@pytest.fixture
def now() -> _Now:
    return _Now()


@pytest.fixture
def clock(now) -> Clock:
    return Clock(time_source=now)


@pytest.fixture
def prep() -> _Prep:
    return _Prep()


@pytest.fixture
def alerts() -> list[tuple[str, str]]:
    return []


@pytest.fixture
async def make_runtime(conn, clock, bus, prep, alerts):
    runtimes: list[PaperRuntime] = []
    calendar = NSECalendar(config_dir() / "calendar", clock, strict=False)

    async def alert(key: str, message: str) -> None:
        alerts.append((key, message))

    async def build(**overrides) -> PaperRuntime:
        kwargs = {
            "capital_base_fn": lambda: Decimal("40000"),
            "tick_size_fn": lambda _symbol: Decimal("0.05"),
            "order_guard": paper_order_guard(lambda: True, lambda: RiskState.NORMAL),
            "alert": alert,
            "prep": prep,
            "fill_model_fn": FillModelConfig,
            **overrides,
        }
        PaperControl(conn, clock)
        runtime = PaperRuntime(conn, clock, calendar, bus, PaperSettings(), **kwargs)
        await runtime.start()
        runtimes.append(runtime)
        return runtime

    yield build
    for runtime in runtimes:
        await runtime.stop()


async def settle(bus) -> None:
    for _ in range(3):
        await bus.drain(1.0)
        await asyncio.sleep(0)


async def prepped(bus, runtime: PaperRuntime) -> PaperRuntime:
    bus.publish("tick", tick(at(10, 0)))
    await settle(bus)
    assert runtime.prep_ready()
    return runtime


def collector(sink: list):
    async def collect(event) -> None:
        sink.append(event)

    return collect


def spy(monkeypatch, runtime: PaperRuntime) -> list:
    seen: list = []
    monkeypatch.setattr(runtime.broker, "on_tick", seen.append)
    monkeypatch.setattr(runtime.broker, "on_bar", seen.append)
    return seen


def insert_order(conn, order_id: str, state: str, *, filled: int = 0, broker_id: int | None = None,
                 paper: bool = True) -> None:
    conn.execute(
        "INSERT INTO orders (order_id, broker_order_id, role, is_paper, state, product, side, qty, "
        "filled_qty, created_at, updated_at) VALUES (?, ?, 'entry', ?, ?, 'CNC', 'BUY', 10, ?, ?, ?)",
        (order_id, None if broker_id is None else str(broker_id), int(paper), state, filled,
         at(9, 30).isoformat(), at(9, 30).isoformat()),
    )


def insert_position(conn, position_id: str, state: str = "OPEN", *, paper: bool = True) -> None:
    conn.execute(
        "INSERT INTO positions (position_id, symbol, side, product, qty, avg_entry, stop, state, is_paper, "
        "origin, opened_at) VALUES (?, 'TCS', 'BUY', 'CNC', 10, '100', '95', ?, ?, ?, ?)",
        (position_id, state, int(paper), "platform" if paper else "recommended", at(9, 30).isoformat()),
    )


def insert_gtt(conn, gtt_id: int, position_id: str | None, state: str = "active", *, paper: bool = True,
               trigger_low: str | None = "95", last_price: str | None = "100") -> None:
    conn.execute(
        "INSERT INTO gtts (gtt_id, position_id, state, trigger_low, trigger_high, is_paper, symbol, side, "
        "product, qty, stop_limit, target_limit, last_price, created_at) "
        "VALUES (?, ?, ?, ?, '110', ?, 'TCS', 'SELL', 'CNC', 10, '94.05', '110', ?, ?)",
        (gtt_id, position_id, state, trigger_low, int(paper), last_price, at(9, 30).isoformat()),
    )


# ---------------------------------------------------------------- construction
@pytest.mark.parametrize("stored", [False, True])
async def test_counters_seed_past_every_stored_paper_id_even_terminal_ones(conn, make_runtime, stored) -> None:
    if stored:
        insert_order(conn, "A", "FILLED", filled=10, broker_id=ORDER_ID_BASE + 7)
        insert_order(conn, "B", "CANCELLED", broker_id=ORDER_ID_BASE + 3)
        insert_order(conn, "R", "REJECTED", broker_id=ORDER_ID_BASE + 99, paper=False)
        insert_gtt(conn, GTT_ID_BASE + 5, None, "deleted")
        insert_gtt(conn, GTT_ID_BASE + 4, None, "triggered")
        insert_gtt(conn, GTT_ID_BASE + 50, None, "active", paper=False)
    runtime = await make_runtime()

    order_id = await runtime.broker.place_order(ENTRY, intent="risk_reducing")
    gtt_id = await runtime.broker.place_gtt({
        "tradingsymbol": "TCS", "last_price": "100", "trigger_values": ["95"],
        "orders": [{"transaction_type": "SELL", "quantity": 1, "product": "CNC", "order_type": "LIMIT", "price": "94"}],
    })
    assert int(order_id) == ORDER_ID_BASE + (8 if stored else 1)
    assert gtt_id == GTT_ID_BASE + (6 if stored else 1)
    assert not runtime.degraded


@pytest.mark.parametrize(("state", "filled", "path"), [
    (S.DRAFT, 0, [S.REJECTED]),
    (S.VALIDATED, 0, [S.REJECTED]),
    (S.SUBMITTED, 0, [S.CANCELLED]),
    (S.CANCEL_PENDING, 4, [S.CANCELLED]),
    (S.ACKED, 0, [S.LAPSED]),
    (S.PARTIALLY_FILLED, 4, [S.LAPSED]),
    (S.MODIFY_PENDING, 0, [S.ACKED, S.LAPSED]),
    (S.MODIFY_PENDING, 4, [S.PARTIALLY_FILLED, S.LAPSED]),
])
async def test_every_stranded_state_reaches_a_terminal_state_legally(conn, make_runtime, state, filled, path) -> None:
    insert_order(conn, "PAPER", state, filled=filled)
    insert_order(conn, "REAL", state, filled=filled, paper=False)
    runtime = await make_runtime()

    store = OrderStore(conn)
    closed = store.get("PAPER")
    assert (closed.state, closed.filled_qty) == (path[-1], filled)
    assert [(e.from_state, e.to_state) for e in store.events("PAPER")] == list(zip([state, *path], path, strict=False))
    assert store.get("REAL").state is state and store.events("REAL") == []
    assert not runtime.degraded


async def test_restore_books_only_the_active_gtts_of_held_paper_positions(conn, make_runtime) -> None:
    insert_position(conn, "OPEN")
    insert_position(conn, "CLOSED", "CLOSED")
    insert_position(conn, "REAL", paper=False)
    insert_gtt(conn, GTT_ID_BASE + 1, "OPEN")
    insert_gtt(conn, GTT_ID_BASE + 2, "OPEN", "triggered")
    insert_gtt(conn, GTT_ID_BASE + 3, "CLOSED")
    insert_gtt(conn, GTT_ID_BASE + 4, "REAL", paper=False)
    runtime = await make_runtime()

    assert [g["id"] for g in await runtime.broker.gtts()] == [GTT_ID_BASE + 1]


def _raise() -> Decimal:
    raise RuntimeError("limits unverified")


@pytest.mark.parametrize(("setup", "overrides", "step"), [
    (None, {"capital_base_fn": _raise}, "build_broker"),
    (lambda conn: (insert_position(conn, "P"), insert_gtt(conn, GTT_ID_BASE + 1, "P", last_price=None)), {},
     "restore_gtts"),
    (lambda conn: (insert_position(conn, "P"), insert_gtt(conn, GTT_ID_BASE + 1, "P", trigger_low=None)), {},
     "restore_gtts"),
    (lambda conn: conn.execute("INSERT INTO paper_state (id, last_observed_at) VALUES (1, '2026-06-16T15:00:00')"),
     {}, "load_last_observed"),
])
async def test_a_failed_construction_step_degrades_alerts_once_and_forwards_nothing(
    conn, bus, make_runtime, prep, alerts, setup, overrides, step
) -> None:
    if setup is not None:
        setup(conn)
    runtime = await make_runtime(**overrides)

    assert runtime.failures == [step] and runtime.degraded
    assert len(alerts) == 1 and alerts[0][0] == "construct" and step in alerts[0][1]
    bus.publish("tick", tick(at(10, 0)))
    await settle(bus)
    assert prep.calls == [] and runtime.last_observed_at is None and not runtime.prep_ready()
    with pytest.raises(PaperOrderBlocked):
        runtime.guard("entry")


# ---------------------------------------------------------------- gate and bridge
async def test_nothing_trades_and_the_gate_stays_shut_while_prep_is_due_or_running(
    bus, make_runtime, prep, monkeypatch
) -> None:
    prep.gate = asyncio.Event()
    runtime = await make_runtime()
    seen = spy(monkeypatch, runtime)

    for state in (PrepState.DUE, PrepState.RUNNING):
        assert runtime.prep_state is state and not runtime.prep_ready()
        with pytest.raises(PaperOrderBlocked):
            await runtime.broker.place_order(ENTRY)
        runtime.guard("risk_reducing")
        bus.publish("tick", tick(at(10, 0)))
        bus.publish("bar.1m", bar(at(9, 59)))
        await settle(bus)
    assert seen == [] and len(prep.calls) == 1

    prep.gate.set()
    await settle(bus)
    assert runtime.prep_state is PrepState.DONE and runtime.prep_ready()
    runtime.guard("entry")
    events = [tick(at(10, 0, 30)), bar(at(10, 0))]
    bus.publish("tick", events[0])
    bus.publish("bar.1m", events[1])
    await settle(bus)
    assert seen == events


async def test_an_older_snapshot_tick_never_moves_last_observed_back(bus, make_runtime, now, monkeypatch) -> None:
    runtime = await prepped(bus, await make_runtime())
    seen = spy(monkeypatch, runtime)
    now.value = at(10, 2, 30)
    for ts in (at(10, 2), at(9, 58)):
        bus.publish("tick", tick(ts))
        await settle(bus)
    assert [t.exchange_ts for t in seen] == [at(10, 2), at(9, 58)]
    assert runtime.last_observed_at == at(10, 2)


async def test_prep_runs_on_each_session_start_and_after_a_gap_and_nothing_else(
    conn, bus, clock, make_runtime, prep, now
) -> None:
    yesterday, tomorrow = D - dt.timedelta(days=1), D + dt.timedelta(days=1)
    PaperControl(conn, clock)
    conn.execute("UPDATE paper_state SET last_observed_at = ?", (at(15, 20, d=yesterday).isoformat(),))
    runtime = await make_runtime()

    for wall, ts in (
        (at(9, 10), at(9, 10)),                                     # pre-open
        (at(9, 12), at(15, 29, d=yesterday)),                       # stale snapshot of the last session
        (at(9, 15, 5), at(9, 15, 1)),                               # first in-session tick: prep
        (at(9, 20), at(9, 20)),                                     # 4m55s after the prep window
        (at(9, 26), at(9, 26)),                                     # 6 min unobserved: prep
        (at(15, 31), at(15, 31)),                                   # post-close
        (at(9, 15, 3, d=tomorrow), at(9, 15, 2, d=tomorrow)),       # the next session: prep
    ):
        now.value = wall
        bus.publish("tick", tick(ts))
        await settle(bus)

    assert prep.calls == [
        (at(15, 20, d=yesterday), at(9, 15, 5)),
        (at(9, 20), at(9, 26)),
        (at(9, 26), at(9, 15, 3, d=tomorrow)),
    ]
    stored = conn.execute("SELECT last_observed_at FROM paper_state").fetchone()[0]
    assert stored == runtime.last_observed_at.isoformat() == at(9, 15, 3, d=tomorrow).isoformat()


async def test_a_failed_prep_alerts_and_forwarding_resumes(bus, make_runtime, prep, alerts, monkeypatch) -> None:
    prep.raises = True
    runtime = await prepped(bus, await make_runtime())
    seen = spy(monkeypatch, runtime)
    bus.publish("tick", tick(at(10, 0, 30)))
    await settle(bus)
    assert [key for key, _ in alerts] == ["prep"] and len(seen) == 1


async def test_paper_frames_never_reach_order_update_and_a_real_frame_changes_no_paper_row(
    conn, bus, clock, make_runtime
) -> None:
    runtime = await prepped(bus, await make_runtime())
    frames: dict[str, list] = {ORDER_UPDATE_TOPIC: [], PAPER_ORDER_UPDATE_TOPIC: []}
    for topic, sink in frames.items():
        bus.subscribe(topic, collector(sink))
    manager = OrderManager(conn, clock, runtime.broker, order_guard=runtime.guard, on_fill=lambda *_: None)
    manager.start(bus)
    try:
        order = await manager.submit(*seed_verdict(conn))
        await settle(bus)
        await manager.join()
        assert frames[ORDER_UPDATE_TOPIC] == [] and frames[PAPER_ORDER_UPDATE_TOPIC]

        def paper_rows() -> list:
            return [tuple(r) for r in conn.execute(
                "SELECT o.*, (SELECT COUNT(*) FROM order_events e WHERE e.order_id = o.order_id) "
                "FROM orders o WHERE o.is_paper = 1"
            )]

        before = paper_rows()
        bus.publish(ORDER_UPDATE_TOPIC, OrderUpdateFrame(data={
            "order_id": order.broker_order_id, "status": "CANCELLED", "filled_quantity": 0,
            "tradingsymbol": "TCS", "transaction_type": "BUY", "product": "CNC", "quantity": 10,
        }))
        await settle(bus)
        await manager.join()
        assert paper_rows() == before
    finally:
        await manager.stop()


async def test_a_raising_paper_handler_leaves_tick_and_bar_consumers_running(bus, make_runtime, monkeypatch) -> None:
    runtime = await prepped(bus, await make_runtime())
    consumed: list = []
    broker_calls: list = []

    def broken(event) -> None:
        broker_calls.append(event)
        raise RuntimeError("paper broker broke")

    bus.subscribe("tick", collector(consumed))
    bus.subscribe("bar.1m", collector(consumed))
    monkeypatch.setattr(runtime.broker, "on_tick", broken)
    monkeypatch.setattr(runtime.broker, "on_bar", broken)
    events = [tick(at(10, 1)), bar(at(10, 0)), tick(at(10, 1, 30))]
    for event in events:
        bus.publish("tick" if isinstance(event, Tick) else "bar.1m", event)
        await settle(bus)
    assert consumed == events and broker_calls == events


async def test_the_broker_is_built_from_the_real_limits_capital_base_and_fill_model(
    conn, clock, tmp_path, make_runtime
) -> None:
    cfg = tmp_path / "cfg"
    cfg.mkdir()
    shutil.copyfile(config_dir() / "limits.yaml", cfg / "limits.yaml")
    store = ProtectedStore(cfg, conn, clock)
    store.register_initial("limits.yaml", OwnerConfirmation(actor=Actor.OWNER, confirmed=True, note="test"))
    limits = LimitsEngine(store)
    runtime = await make_runtime(
        capital_base_fn=lambda: limits.load().capital_base_inr, fill_model_fn=load_fill_model
    )
    margins = await runtime.broker.margins()
    assert margins["equity"]["available"]["cash"] == Decimal("40000")
    assert not runtime.degraded
