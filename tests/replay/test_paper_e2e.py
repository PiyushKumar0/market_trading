"""Plan Q4.12: one replayed day drives proposal -> paper gate -> OMS -> fill -> GTT -> exit through the
composition root's own factory (``ops.main._compose_paper``), beside the real recommendation path.

Prints reach the paper bridge through its synchronous entry points (``PaperRuntime.on_tick``/``on_bar``,
which its bus handlers wrap): a bus hop would batch them at the harness's yields, behind the clock.
"""

from __future__ import annotations

import asyncio
import json
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from engine.broker.kite_client import KiteClient
from engine.core.calendar import NSECalendar
from engine.core.clock import IST
from engine.core.config import config_dir, load_settings
from engine.core.contracts import ORDER_UPDATE_TOPIC, PAPER_ORDER_UPDATE_TOPIC
from engine.core.db import connect
from engine.core.enums import Mode, RiskState
from engine.core.eventbus import EventBus
from engine.core.migrations import apply_migrations
from engine.core.scope import scope_sql
from engine.core.types import Bar, Tick, TradeWindow
from engine.marketdata.backfill import BackfillJob
from engine.notify.catalog import CatalogMessage, MessageKind
from engine.oms.state import CloseReason
from engine.ops import main as opsmain
from engine.ops.holds import build_hold_fn
from engine.ops.paper_control import PaperControl
from engine.ops.paper_runtime import PaperRuntime
from engine.ops.pipeline import RecommendationBook, RecommendationPipeline
from engine.paper import replay as replay_mod
from engine.paper.replay import ReplayHarness, ReplayReport, mask_ulids
from engine.risk.exposure import ExposureTracker
from engine.risk.gate import TERMINAL_ORDER_STATES_SQL, GateContextBuilder, RiskGate
from engine.strategy.cost_model import CostModel
from engine.strategy.types import RawLevels, SignalCandidate, round_to_tick
from tests.replay.fixtures.synthetic_day import PRE_OPEN_TICKS, SECONDS_IN_MINUTE, SYMBOLS, SyntheticDay
from tests.unit.test_paper_composition import Limits
from tests.unit.test_reco_pipeline import FakeAssembler, FakeGovernor, FakeHarness, agent_defs

SYMBOL = "SYNA"
#: Stream prints between the harness's yields. Async work (session prep, the bus, the OMS consumer)
#: runs only at a yield, so one print per yield keeps it a loop turn behind the broker, as live; at the
#: shipped 2,000 the OMS would lag by hours of stream and every prep would look like a fresh gap.
SLICE = 1
#: R3: prints within which a protection lapse must heal. The OMS applies a postback two loop turns
#: after the broker publishes it (the bus task, then the consumer).
R3_TICKS = 3
PROPOSE_AT = time(10, 0)
QTY = 100
#: Read off the fixture's SYNA walk (98.71 at 10:00:01): neither level is touched while the entry fills
#: over the next two minutes under the participation cap; the stop is first touched at 10:09:41.
STOP, TARGET = Decimal("98.06"), Decimal("102.00")
GOLDEN = "2c87549928a000f871550f0804cba9327903be501673a490dc0f975b3ac72954"
WRITE = re.compile(r"^\s*(INSERT|UPDATE|DELETE|REPLACE)\b", re.IGNORECASE)
GUARDED = re.compile(r"^\s*(INSERT(\s+OR\s+\w+)?\s+INTO|REPLACE\s+INTO|UPDATE(\s+OR\s+\w+)?|DELETE\s+FROM)\s+"
                     r"\W?(mode_state|kill_state|risk_state_causes)\b", re.IGNORECASE)
PAPER_TABLES = ("orders", "positions", "gtts", "paper_equity_snapshots", "paper_halts")
#: Logs every write that leaves a real (``is_paper=0``) order, position or GTT row, however briefly.
REAL_ROW_WATCH = ["CREATE TEMP TABLE real_rows (tbl TEXT)", *(
    f"CREATE TEMP TRIGGER real_{t}_{op} AFTER {op} ON main.{t} WHEN COALESCE(NEW.is_paper, 0) = 0 "
    f"BEGIN INSERT INTO real_rows VALUES ('{t}'); END"
    for t in ("orders", "positions", "gtts") for op in ("INSERT", "UPDATE")
)]


# ------------------------------------------------------------------ doubles
class KiteDouble(KiteClient):
    """The real broker client, reachable where production holds it (the backfill). Every order and
    GTT method records and raises; the paper path swallows exceptions, so the test reads ``calls``."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    def _refuse(self, name: str) -> None:
        self.calls.append(name)
        raise AssertionError(f"KiteClient.{name} called from the replay (D8)")

    async def place_order(self, *a: Any, **k: Any) -> str: self._refuse("place_order")
    async def modify_order(self, *a: Any, **k: Any) -> str: self._refuse("modify_order")
    async def cancel_order(self, *a: Any, **k: Any) -> str: self._refuse("cancel_order")
    async def orders(self) -> list: self._refuse("orders")
    async def positions(self) -> Any: self._refuse("positions")
    async def holdings(self) -> list: self._refuse("holdings")
    async def margins(self) -> Any: self._refuse("margins")
    async def place_gtt(self, *a: Any, **k: Any) -> int: self._refuse("place_gtt")
    async def modify_gtt(self, *a: Any, **k: Any) -> int: self._refuse("modify_gtt")
    async def delete_gtt(self, *a: Any, **k: Any) -> None: self._refuse("delete_gtt")
    async def gtts(self) -> list: self._refuse("gtts")

    async def historical(self, *a: Any, **k: Any) -> list:
        return []


class Market:
    """The MarketStore reads the gates, the pipeline and paper make: SYMBOLS in today's universe, one
    sector, no corporate actions, earnings, surveillance or history."""

    async def arun(self, fn: Callable[..., Any], /, *args: Any, **kwargs: Any) -> Any:
        return fn(*args, **kwargs)

    def get_universe_daily(self, d: Any) -> list[dict]:
        return [{"symbol": s, "included": True} for s in SYMBOLS]

    def get_sector_map(self, as_of: Any = None) -> list[dict]:
        return [{"symbol": s, "sector": "ENERGY"} for s in SYMBOLS]

    def get_instruments_daily(self, *a: Any, **k: Any) -> list: return []
    def get_earnings_calendar(self, *a: Any, **k: Any) -> list: return []
    def get_corp_actions(self, *a: Any, **k: Any) -> list: return []
    def get_bars_1m(self, *a: Any, **k: Any) -> list: return []
    def get_bars_1d(self, *a: Any, **k: Any) -> list: return []


class Instruments:
    def is_fno(self, symbol: str) -> bool:
        return False

    def round_to_tick(self, symbol: str, price: Decimal) -> Decimal:
        return round_to_tick(price)


class ModeDouble:
    def mode(self) -> Mode:
        return Mode.RECOMMEND

    def risk_state(self) -> RiskState:
        return RiskState.NORMAL

    def get_trade_window(self) -> TradeWindow:
        return TradeWindow(start=time(10, 0), end=time(10, 30))


class Notes:
    def __init__(self) -> None:
        self.messages: list[CatalogMessage] = []

    async def __call__(self, message: CatalogMessage) -> None:
        self.messages.append(message)


WARM = SimpleNamespace(ready=True, blockers=[], ready_for=lambda *_: True)


class Feed:
    """The real side's tick and bar consumers: the mark and tick age the gates read, and the prints seen."""

    def __init__(self, clock: Any) -> None:
        self._clock = clock
        self.marks: dict[str, Tick] = {}
        self.prints: list[tuple[str, str, str]] = []
        self.bars: list[Bar] = []

    def on_tick(self, tick: Tick) -> None:
        self.marks[tick.tradingsymbol] = tick
        self.prints.append((tick.tradingsymbol, tick.exchange_ts.isoformat(), str(tick.ltp)))

    def on_bar(self, bar: Bar) -> None:
        self.bars.append(bar)

    def ltp(self, symbol: str) -> Decimal | None:
        tick = self.marks.get(symbol)
        return None if tick is None else tick.ltp

    def age(self, symbol: str) -> float | None:
        # The archive has no index prints: the index is as fresh as the newest print.
        ticks = list(self.marks.values()) if symbol == opsmain.INDEX_SYMBOL else [self.marks.get(symbol)]
        stamps = [t.exchange_ts for t in ticks if t is not None]
        return (self._clock.now() - max(stamps)).total_seconds() if stamps else None


_R3_SQL = (
    "SELECT p.position_id, p.state, p.protection_state, "
    "EXISTS (SELECT 1 FROM gtts g WHERE g.position_id = p.position_id AND "
    f"{scope_sql('paper', 'g')} AND g.state = 'active' AND g.qty = p.qty) AS armed, "
    "EXISTS (SELECT 1 FROM orders x WHERE x.position_id = p.position_id AND "
    f"{scope_sql('paper', 'x')} AND x.side = 'SELL' AND x.state NOT IN {TERMINAL_ORDER_STATES_SQL}) AS exiting "
    "FROM orders o JOIN positions p ON p.position_id = o.position_id "
    f"WHERE o.broker_order_id = ? AND {scope_sql('paper', 'o')} AND {scope_sql('paper', 'p', has_origin=True)}"
)


class R3:
    """Plan Q4.5's property, judged on every forwarded print from a paper entry's first broker fill until
    its position closes: the position has an ACTIVE GTT for its qty, or is PENDING_EXIT with a working
    exit, or is PROTECTION_FAILED with its PAPER_ALERT sent. A lapse must heal within R3_TICKS prints."""

    def __init__(self, conn: Any, alerts: Notes) -> None:
        self._conn = conn
        self._alerts = alerts
        self.prints = 0
        self.watched: set[str] = set()
        self.lapsed_at: dict[str, int] = {}
        self.judged: dict[str, int] = {}
        self.lapses = 0
        self.violations: list[tuple[int, str, dict | None]] = []

    def on_publish(self, topic: str, event: Any) -> None:
        data = getattr(event, "data", {})
        if topic == PAPER_ORDER_UPDATE_TOPIC and data.get("transaction_type") == "BUY" and data.get("filled_quantity"):
            self.watched.add(data["order_id"])

    def check(self) -> None:
        self.prints += 1
        for broker_id in sorted(self.watched):
            row = self._conn.execute(_R3_SQL, (broker_id,)).fetchone()
            if row is not None and row["state"] == "CLOSED":
                self.watched.discard(broker_id)
                continue
            if row is not None and (row["armed"] or (row["state"] == "PENDING_EXIT" and row["exiting"])
                                    or (row["protection_state"] == "PROTECTION_FAILED" and self._alerted(row))):
                self.lapsed_at.pop(broker_id, None)
                self.judged[row["state"]] = self.judged.get(row["state"], 0) + 1
                continue
            if broker_id not in self.lapsed_at:
                self.lapsed_at[broker_id] = self.prints
                self.lapses += 1
            if self.prints - self.lapsed_at[broker_id] >= R3_TICKS:
                self.violations.append((self.prints, broker_id, None if row is None else dict(row)))

    def _alerted(self, row: Any) -> bool:
        return any(m.kind == MessageKind.PAPER_ALERT and m.data["key"].startswith(row["position_id"])
                   for m in self._alerts.messages)


class Bridge:
    """The harness's broker seam: each print reaches the real feed first, then the paper bridge, then R3."""

    def __init__(self, feed: Feed, runtime: PaperRuntime | None, r3: R3) -> None:
        self._feed = feed
        self._runtime = runtime
        self._r3 = r3
        self.topic = runtime.broker.topic if runtime is not None else ORDER_UPDATE_TOPIC

    def on_tick(self, tick: Tick) -> None:
        self._feed.on_tick(tick)
        if self._runtime is not None:
            self._runtime.on_tick(tick)
            self._r3.check()

    def on_bar(self, bar: Bar) -> None:
        self._feed.on_bar(bar)
        if self._runtime is not None:
            self._runtime.on_bar(bar)

    async def orders(self) -> list:
        return [] if self._runtime is None else await self._runtime.broker.orders()


class TapBus(EventBus):
    """The engine bus, handing every publish to the harness collector and R3 first."""

    def __init__(self, *taps: Callable[[str, Any], None]) -> None:
        super().__init__()
        self._taps = taps
        self.topics: set[str] = set()

    def publish(self, topic: str, event: Any) -> None:
        self.topics.add(topic)
        for tap in self._taps:
            tap(topic, event)
        super().publish(topic, event)


# ------------------------------------------------------------------ one replayed day
@dataclass
class Outcome:
    report: ReplayReport
    bars: list[Bar]
    frames_after_settle: int
    prints: list[tuple[str, str, str]]
    feed_bars: list[Bar]
    real: str
    rows: dict[str, list[dict]]
    real_rows: list[str]
    paper_messages: list[CatalogMessage]
    kite_calls: list[str]
    writes: list[str]
    r3: R3
    subscribers: dict[str, int]
    topics: set[str]


def _propose_ordinal(day: SyntheticDay) -> int:
    """The stream ordinal right after SYMBOL's first print in the PROPOSE_AT minute: the fixture prints
    each symbol every 10 s, past the gate's 5 s tick-age limit, so the proposal must follow a print.
    Counts pre-open prints, then whole session minutes (the gap minute is later in the day)."""
    minutes = PROPOSE_AT.hour * 60 + PROPOSE_AT.minute - (9 * 60 + 15)
    n = len(SYMBOLS)
    return PRE_OPEN_TICKS * n + minutes * len(SECONDS_IN_MINUTE) * n + SYMBOLS.index(SYMBOL) + 1


def _rows(conn: Any, sql: str) -> list[dict]:
    return [dict(r) for r in conn.execute(sql)]


async def _replay(day: SyntheticDay, scratch: Path, *, enabled: bool) -> Outcome:
    harness = ReplayHarness(day.root, scratch)
    clock = harness.clock
    conn = connect(str(scratch / "state.db"))
    apply_migrations(conn)
    for sql in REAL_ROW_WATCH:
        conn.execute(sql)
    writes: list[str] = []
    conn.set_trace_callback(lambda sql: writes.append(sql) if WRITE.match(sql) else None)
    try:
        shipped = load_settings()
        paper_settings = shipped.paper.model_copy(update={"subsystem_enabled": enabled})
        settings = shipped.model_copy(update={"paper": paper_settings})
        limits = Limits()
        table = limits.load()
        calendar = NSECalendar(config_dir() / "calendar", clock, strict=False, sqlite_conn=conn)
        paper_notes, real_notes = Notes(), Notes()
        r3 = R3(conn, paper_notes)
        bus = TapBus(harness.publish, r3.on_publish)
        mode, kill, market = ModeDouble(), SimpleNamespace(is_killed=lambda: False), Market()
        kite, feed = KiteDouble(), Feed(clock)
        cost_model = CostModel.from_config()
        hold_fn = build_hold_fn(settings, limits)
        round_tick = opsmain._round_tick_fn(Instruments())
        control = PaperControl(conn, clock)

        def builder(exposure: ExposureTracker, scope: str) -> GateContextBuilder:
            return GateContextBuilder(
                limits, exposure, Instruments(), market, calendar, clock, mode, kill,
                ltp_fn=feed.ltp, tick_age_fn=feed.age, warmup_status_fn=lambda: WARM,
                clock_skew_ok_fn=lambda: True, degrade_tier_fn=lambda: "DG0",
                conn=conn, index_symbol=opsmain.INDEX_SYMBOL, scope=scope,
            )

        paper = None
        if settings.paper.subsystem_enabled:          # the composition root's own gate
            paper = await opsmain._compose_paper(
                conn, clock, calendar, bus, settings, control,
                real_risk_state=mode.risk_state, limits=limits, tick_size_fn=lambda _s: Decimal("0.05"),
                round_tick_fn=round_tick, cost_model=cost_model, hold_fn=hold_fn, store=market,
                backfill=BackfillJob(harness.store, kite, clock, settings, conn, lambda _s: None),
                ctx_builder_fn=lambda tracker: builder(tracker, "paper"), notify=paper_notes,
            )
            assert paper is not None
            control.set_enabled(True, "owner", via="test")
        exposure = ExposureTracker(conn, clock, table.capital_base_inr, mark_price=feed.ltp)
        analyst = FakeHarness()
        pipeline = RecommendationPipeline(
            FakeAssembler(), analyst, agent_defs(),
            RiskGate(limits, cost_model, clock,
                     strategy_expected_edge_pct=opsmain._strategy_expected_edge_pct(settings),
                     no_edge_shadow_strategies=opsmain.NO_EDGE_SHADOW_STRATEGIES),
            builder(exposure, "real"),
            RecommendationBook(conn, clock, cost_model,
                               overnight_gap_mult_fn=lambda: table.limits.per_trade_risk.overnight_gap_mult,
                               calendar=calendar, veto_window_sessions=settings.recommend.veto_window_sessions),
            mode, kill, FakeGovernor(), exposure, limits, real_notes, clock, calendar, conn, market,
            ltp_fn=feed.ltp, warmup_status_fn=lambda: WARM, hold_fn=hold_fn, round_tick_fn=round_tick,
            recommend=settings.recommend, strategy_edge_pct=opsmain._strategy_expected_edge_pct(settings),
            paper_ctx_builder=paper.ctx_builder if paper is not None else None,
            paper_submit=paper.submit if paper is not None else None,
            paper_enabled_fn=paper.entries_open if paper is not None else None,
        )

        async def propose() -> None:
            mark = feed.marks[SYMBOL]
            assert mark.exchange_ts == clock.now(), "the proposal must follow a fresh print"
            analyst.queued.append({
                "action": "enter", "thesis": "Twenty-session breakout holding above the level on volume.",
                "confidence": 0.7, "tradingsymbol": SYMBOL, "exchange": "NSE", "side": "BUY", "style": "swing",
                "entry_type": "MARKET", "stop_price": str(STOP), "target_price": str(TARGET), "quantity": QTY,
                "signal_id": "01SIGNAL", "strategy_id": "brk20", "features_snapshot_id": "01SNAP",
            })
            await pipeline.on_signal_candidate(SignalCandidate(
                signal_id="01SIGNAL", strategy_id="brk20", symbol=SYMBOL, side="BUY", style="swing",
                raw_levels=RawLevels(entry=mark.ltp, stop=STOP, target=TARGET), score=0.8,
                features_snapshot_id="01SNAP",
            ))
            assert await pipeline.drain_forward_queue()

        subscribers = {t: len(bus._subscribers.get(t, [])) for t in ("tick", "bar.1m", PAPER_ORDER_UPDATE_TOPIC)}
        harness.attach_broker(Bridge(feed, paper.runtime if paper is not None else None, r3))
        harness.at_tick(_propose_ordinal(day), propose)
        if paper is not None:
            at = datetime.combine(day.day, time(9, 15), tzinfo=IST)
            while at.time() < time(15, 30):           # the 30 s ``paper_tick`` job
                harness.schedule(at, paper.runtime.paper_tick)
                at += timedelta(seconds=30)
        try:
            report = await harness.run([day.day])
            for _ in range(4):
                await bus.drain(1.0)
                if paper is not None:
                    await paper.orders.join()
        finally:
            if paper is not None:
                await paper.stop()
        real = {
            "verdicts": _rows(conn, "SELECT verdict, payload FROM verdicts WHERE is_paper = 0"),
            "recommendations": _rows(conn, "SELECT payload FROM recommendations"),
            "ledger": _rows(conn, f"SELECT * FROM learning_ledger WHERE {scope_sql('real')}"),
            "messages": [m.render() for m in real_notes.messages],
        }
        return Outcome(
            report=report, bars=harness.bars(day.day), frames_after_settle=len(harness.postbacks()),
            prints=feed.prints, feed_bars=feed.bars,
            real=mask_ulids(json.dumps(real, sort_keys=True, default=str)),
            rows={t: _rows(conn, f"SELECT * FROM {t}") for t in (*PAPER_TABLES, "verdicts", "learning_ledger")},
            real_rows=[r[0] for r in conn.execute("SELECT tbl FROM real_rows")],
            paper_messages=paper_notes.messages, kite_calls=kite.calls, writes=writes, r3=r3,
            subscribers=subscribers, topics=bus.topics,
        )
    finally:
        conn.set_trace_callback(None)
        harness.close()
        conn.close()


@pytest.fixture(scope="module")
def runs(synthetic_day: SyntheticDay, tmp_path_factory: pytest.TempPathFactory) -> dict[str, Outcome]:
    out: dict[str, Outcome] = {}
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(replay_mod, "_YIELD_EVERY_TICKS", SLICE)
        for name, enabled in (("on", True), ("again", True), ("off", False)):
            out[name] = asyncio.run(_replay(synthetic_day, tmp_path_factory.mktemp(name), enabled=enabled))
    return out


# ------------------------------------------------------------------ the day, paper on
def test_a_proposal_is_traded_end_to_end_on_paper(runs: dict[str, Outcome]) -> None:
    on = runs["on"]
    rows = on.rows
    [real_v, paper_v] = sorted(rows["verdicts"], key=lambda v: v["is_paper"])
    assert (real_v["proposal_id"], real_v["is_paper"]) == (paper_v["proposal_id"], 0)
    assert paper_v["is_paper"] == 1 and paper_v["verdict"] in ("approve", "shrink")

    orders = {o["role"]: o for o in rows["orders"]}
    assert sorted(orders) == ["entry", "gtt_leg"] and len(rows["orders"]) == 2
    entry, leg = orders["entry"], orders["gtt_leg"]
    assert (entry["verdict_id"], entry["state"], entry["filled_qty"]) == (paper_v["verdict_id"], "FILLED", QTY)
    [position] = rows["positions"]
    [gtt] = rows["gtts"]
    assert (position["position_id"], gtt["position_id"], leg["position_id"]) == (entry["position_id"],) * 3
    assert (position["is_paper"], position["origin"], position["state"]) == (1, "platform", "CLOSED")
    assert position["close_reason"] in (CloseReason.STOP, CloseReason.TARGET) and leg["state"] == "FILLED"
    assert gtt["is_paper"] == 1 and gtt["state"] == "triggered"
    [ledger] = [r for r in rows["learning_ledger"] if r["is_paper"]]
    assert (ledger["rec_id"], ledger["proposal_id"], ledger["verdict_id"], ledger["strategy_id"]) == (
        None, paper_v["proposal_id"], paper_v["verdict_id"], "brk20")

    assert [(m.kind, m.data["is_paper"]) for m in on.paper_messages] == [(MessageKind.FILL, True)] * 2
    assert on.frames_after_settle == on.report.postbacks > 0      # the digest covers every frame


def test_r3_holds_on_every_print_after_a_fill(runs: dict[str, Outcome]) -> None:
    r3 = runs["on"].r3
    assert r3.violations == []
    assert r3.judged.get("OPEN", 0) > 0                           # not vacuous: judged while armed


def test_no_real_order_and_no_real_state_write(runs: dict[str, Outcome]) -> None:
    on = runs["on"]
    assert on.kite_calls == []
    assert on.real_rows == []
    assert [w for w in on.writes if GUARDED.match(w)] == []
    assert any(" INTO orders" in w for w in on.writes)            # the trace sees paper writes
    assert PAPER_ORDER_UPDATE_TOPIC in on.topics and ORDER_UPDATE_TOPIC not in on.topics
    assert on.subscribers == {"tick": 1, "bar.1m": 1, PAPER_ORDER_UPDATE_TOPIC: 1}


def test_the_golden_digest_is_byte_identical_and_pinned(runs: dict[str, Outcome]) -> None:
    assert runs["on"].report.digest == runs["again"].report.digest == GOLDEN


# ------------------------------------------------------------------ paper off (the shipped setting)
def test_shipped_settings_keep_paper_off_and_change_nothing_real(runs: dict[str, Outcome]) -> None:
    on, off = runs["on"], runs["off"]
    assert {t: off.rows[t] for t in PAPER_TABLES} == dict.fromkeys(PAPER_TABLES, [])
    assert [v["is_paper"] for v in off.rows["verdicts"]] == [0]
    assert off.subscribers == {"tick": 0, "bar.1m": 0, PAPER_ORDER_UPDATE_TOPIC: 0}
    assert off.report.postbacks == 0 and off.paper_messages == []
    # The real recommendation, and what the real tick and bar consumers saw, do not depend on paper.
    assert '"recommendations": [{"payload"' in off.real
    assert off.real == on.real
    assert off.prints == on.prints
    assert off.bars == on.bars == on.feed_bars == off.feed_bars
