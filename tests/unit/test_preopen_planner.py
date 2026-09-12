"""PreopenPlannerJob (§5.3, plan lines 1155-1163): governor gate, deterministic inputs, persistence.

A fake governor and a fake harness (recording the real ``ContextAssembler``-built context) stand in
for the SDK boundary — same style as ``test_agent_harness.py``'s fakes, but here the harness itself
is faked so these tests exercise the JOB's own logic (input assembly, gating, persistence) against a
real ``ContextAssembler``/``MarketStore``/SQLite ``conn`` (conftest's migrated temp db + frozen clock).
"""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from typing import Any

import pytest

from engine.core.calendar import NSECalendar
from engine.core.config import config_dir
from engine.core.enums import DegradeTier
from engine.intelligence.agents import preopen
from engine.intelligence.context import ContextAssembler
from engine.intelligence.governor import InvokeDecision
from engine.intelligence.harness import AgentResult, load_agent_defs
from engine.intelligence.schemas import DayPlan, DayPlanFocus
from engine.marketdata.store import DailyBar, MarketStore
from engine.ops.jobs import AdvisoryOutcome
from engine.ops.preopen_planner import _SOLD_OUTSIDE_HEADING, CALL_CLASS, PreopenPlannerJob

# conftest's frozen clock: Wed 2026-06-17 10:05 IST — a real trading day.
TODAY = date(2026, 6, 17)
PRIOR = date(2026, 6, 16)     # the prior trading day (Tue)
PRIOR2 = date(2026, 6, 15)    # the trading day before THAT (Mon)

AGENTS_CFG: dict[str, Any] = {
    "agents": {
        "preopen_planner": {
            "model": "sonnet-4.6",
            "shape": "single_shot",
            "tools_enabled": False,
            "max_output_tokens": 2000,
            "timeout_s": 60,
        }
    }
}


# --------------------------------------------------------------------------- fixtures
@pytest.fixture
def store(tmp_path, clock):
    s = MarketStore(tmp_path / "market.duckdb", tmp_path / "parquet", clock)
    s.open()
    yield s
    s.close()


@pytest.fixture
def calendar(clock) -> NSECalendar:
    return NSECalendar(config_dir() / "calendar", clock, strict=False)


@pytest.fixture
def assembler(store, conn, clock, calendar) -> ContextAssembler:
    return ContextAssembler(store, conn, clock, calendar)


@pytest.fixture
def agent_defs():
    return load_agent_defs(AGENTS_CFG)


# --------------------------------------------------------------------------- fakes
class FakeGovernor:
    """Records every ``can_invoke`` call; answers a fixed decision (§5.6 admission, faked)."""

    def __init__(self, allowed: bool = True, reason: str | None = None) -> None:
        self.allowed = allowed
        self.reason = reason
        self.calls: list[tuple[str, str]] = []

    def can_invoke(self, agent_id: str, call_class: str = "schedule") -> InvokeDecision:
        self.calls.append((agent_id, call_class))
        tier = DegradeTier.DG0 if self.allowed else DegradeTier.DG4
        return InvokeDecision(allowed=self.allowed, tier=tier, reason=self.reason)


class FakeHarness:
    """Stands in for ``AgentHarness.run_single_shot``: records the REAL assembled context it was
    given (so movers/digest/etc. rendering can be asserted) and returns a scripted ``AgentResult``."""

    def __init__(self, result: AgentResult) -> None:
        self.result = result
        self.calls: list[dict[str, Any]] = []

    async def run_single_shot(self, agent_def, context, validate, *, json_schema=None, call_class=None):
        self.calls.append(
            {"agent_def": agent_def, "context": context, "validate": validate, "call_class": call_class}
        )
        return self.result


# --------------------------------------------------------------------------- helpers
def _plan(**overrides: Any) -> DayPlan:
    base: dict[str, Any] = dict(
        regime="trending up, breadth positive", focus=[], catalyst_focus=[], warnings=[], no_trade_today=False
    )
    base.update(overrides)
    return DayPlan(**base)


def _job(store, conn, assembler, harness, agent_defs, governor, clock, calendar, **kw) -> PreopenPlannerJob:
    return PreopenPlannerJob(store, conn, assembler, harness, agent_defs, governor, clock, calendar, **kw)


def _seed_universe(store: MarketStore, d: date, symbols: list[str]) -> None:
    store.upsert_universe_daily(
        [
            {
                "d": d, "symbol": s, "included": True, "mis_candidate": False,
                "exclusion_reasons": None, "median_traded_value": None,
            }
            for s in symbols
        ]
    )


def _seed_bars(store: MarketStore, d: date, closes: dict[str, str]) -> None:
    store.upsert_bars_1d(
        [
            DailyBar(symbol=s, d=d, open=Decimal(c), high=Decimal(c), low=Decimal(c), close=Decimal(c), volume=10_000)
            for s, c in closes.items()
        ]
    )


# --------------------------------------------------------------------------- persistence round-trip
async def test_persisted_row_round_trips_to_dayplan(store, conn, assembler, agent_defs, clock, calendar):
    plan = _plan(
        regime="chop, breadth flat",
        focus=[DayPlanFocus(symbol="RELIANCE", bias="long", levels="above 1400", why="breakout setup")],
        warnings=["monthly expiry today"],
    )
    harness = FakeHarness(AgentResult.Ok(call_id="c1", payload=plan))
    governor = FakeGovernor(allowed=True)
    job = _job(store, conn, assembler, harness, agent_defs, governor, clock, calendar)

    ok = await job.run(TODAY)

    assert ok is AdvisoryOutcome.RAN
    row = conn.execute("SELECT payload, created_at FROM day_plans WHERE d = ?", (TODAY.isoformat(),)).fetchone()
    assert row is not None
    assert row["created_at"]
    assert DayPlan.model_validate_json(row["payload"]) == plan


# --------------------------------------------------------------------------- governor gate
async def test_governor_blocked_skips_harness_and_persists_nothing(store, conn, assembler, agent_defs, clock, calendar):
    harness = FakeHarness(AgentResult.Ok(call_id="c1", payload=_plan()))
    governor = FakeGovernor(allowed=False, reason="DG4_zero_sdk_calls")
    job = _job(store, conn, assembler, harness, agent_defs, governor, clock, calendar)

    ok = await job.run(TODAY)

    assert ok is AdvisoryOutcome.BLOCKED          # WO-14 (c): a correct outcome, not a failure
    assert harness.calls == []                                        # planner death never calls the SDK
    assert governor.calls == [(preopen.AGENT_ID, CALL_CLASS)]
    row = conn.execute("SELECT 1 FROM day_plans WHERE d = ?", (TODAY.isoformat(),)).fetchone()
    assert row is None


# --------------------------------------------------------------------------- harness failure
async def test_harness_failure_returns_failed_and_persists_nothing(store, conn, assembler, agent_defs, clock, calendar):
    harness = FakeHarness(AgentResult.Failed("schema_invalid", "not valid JSON", call_id="c2"))
    governor = FakeGovernor(allowed=True)
    alerts: list[tuple[str, str]] = []

    async def notify(severity: str, message: str) -> None:
        alerts.append((severity, message))

    job = _job(store, conn, assembler, harness, agent_defs, governor, clock, calendar, notify=notify)

    ok = await job.run(TODAY)

    assert ok is AdvisoryOutcome.FAILED           # WO-14 (c): retryable, unlike a governor block
    assert len(harness.calls) == 1                                     # the call WAS attempted
    row = conn.execute("SELECT 1 FROM day_plans WHERE d = ?", (TODAY.isoformat(),)).fetchone()
    assert row is None
    assert len(alerts) == 1
    assert alerts[0][0] == "error"
    assert "schema_invalid" in alerts[0][1]


async def test_harness_failure_with_no_notify_still_returns_failed(store, conn, assembler, agent_defs, clock, calendar):
    """``notify`` is optional (§5.3 step 3) — its absence must not raise."""
    harness = FakeHarness(AgentResult.Failed("timeout", "no complete response within 60s", call_id="c3"))
    governor = FakeGovernor(allowed=True)
    job = _job(store, conn, assembler, harness, agent_defs, governor, clock, calendar)

    assert await job.run(TODAY) is AdvisoryOutcome.FAILED


# --------------------------------------------------------------------------- movers ordering
async def test_movers_ranked_by_absolute_return_descending(store, conn, assembler, agent_defs, clock, calendar):
    """Prior trading day (Tue 06-16) vs the one before it (Mon 06-15), never TODAY itself (unopened)."""
    _seed_universe(store, PRIOR, ["AAA", "BBB", "CCC", "DDD"])
    _seed_bars(store, PRIOR2, {"AAA": "100.00", "BBB": "100.00", "CCC": "100.00", "DDD": "100.00"})
    _seed_bars(store, PRIOR, {"AAA": "104.10", "BBB": "91.70", "CCC": "101.00", "DDD": "100.50"})
    harness = FakeHarness(AgentResult.Ok(call_id="c4", payload=_plan()))
    governor = FakeGovernor(allowed=True)
    job = _job(store, conn, assembler, harness, agent_defs, governor, clock, calendar)

    ok = await job.run(TODAY)

    assert ok is AdvisoryOutcome.RAN
    volatile = harness.calls[0]["context"].volatile_block
    start = volatile.index("overnight movers (bhavcopy):")
    end = volatile.index("gap scan vs prior close:")
    movers_text = volatile[start:end]
    # Hand-checked: |BBB -8.30%| > |AAA +4.10%| > |CCC +1.00%| > |DDD +0.50%|.
    assert "  - BBB -8.30%" in movers_text
    assert "  - AAA +4.10%" in movers_text
    assert "  - CCC +1.00%" in movers_text
    assert "  - DDD +0.50%" in movers_text
    assert (
        movers_text.index("BBB -8.30%")
        < movers_text.index("AAA +4.10%")
        < movers_text.index("CCC +1.00%")
        < movers_text.index("DDD +0.50%")
    )


# ------------------------------------------------------- the digest rail, measured (2026-09-04)
async def test_a_railed_market_digest_reaches_the_planner_with_its_measure(
    store, conn, assembler, agent_defs, clock, calendar
):
    """2026-09-03: the planner's digest lines rendered a bare ``market: -1.000`` (no WO-22 measure),
    read it as "pinned at its floor", and wrote a risk-off day plan that every intraday verdict then
    cited — from raw -9.48 across 759 clusters, i.e. neutral flow. The planner gets the same measured
    rail line the analyst context already carries; an unrailed sector line is unchanged."""
    as_of = clock.now()
    store.upsert_sentiment_agg([
        {"scope": "market", "scope_key": "market", "as_of": as_of, "value": -1.0,
         "raw_sum": -9.48, "n_clusters": 759},
        {"scope": "sector", "scope_key": "BANK", "as_of": as_of, "value": 0.088,
         "raw_sum": 0.088, "n_clusters": 3},
    ])
    harness = FakeHarness(AgentResult.Ok(call_id="c9", payload=_plan()))
    job = _job(store, conn, assembler, harness, agent_defs, FakeGovernor(allowed=True), clock, calendar)

    assert await job.run(TODAY) is AdvisoryOutcome.RAN

    volatile = harness.calls[0]["context"].volatile_block
    assert (
        "  - market: -1.000 (clipped SUM saturated: raw -9.48 across 759 clusters, "
        "mean -0.012 per cluster — read as net negative headline flow, not extremity)"
    ) in volatile
    assert "  - sector BANK: +0.088\n" in volatile


# --------------------------------------------------------------------------- fail-to-zero rendering
async def test_empty_tables_render_unavailable_without_raising(store, conn, assembler, agent_defs, clock, calendar):
    """D7: every deterministic input on a completely empty store/conn degrades to text, never raises."""
    harness = FakeHarness(AgentResult.Ok(call_id="c5", payload=_plan()))
    governor = FakeGovernor(allowed=True)
    job = _job(store, conn, assembler, harness, agent_defs, governor, clock, calendar)

    ok = await job.run(TODAY)

    assert ok is AdvisoryOutcome.RAN
    volatile = harness.calls[0]["context"].volatile_block
    # Foundational-data gaps (never even ran) => explicit "unavailable".
    assert "overnight movers (bhavcopy):\n  - unavailable" in volatile
    assert "catalyst digest:\n  - unavailable" in volatile
    assert "surveillance changes:\n  - unavailable" in volatile
    # Real, legitimate zeros (ran, found nothing) => "none", not "unavailable".
    assert "catalyst watchlist (binding levels are the scanner's): none" in volatile
    assert "earnings today: none" in volatile
    assert "open positions and overnight risk: none" in volatile
    assert "prior session's post-mortem (HISTORY, may describe already-fixed issues): none" in volatile
    # The health line renders even on an empty store (never-run is honest, not an outage claim).
    assert "platform health (current, authoritative): LLM self-test never-run" in volatile
    # The A14 gap-scan decision: never silently "none".
    assert "gap scan vs prior close:\n  - not computed: no trustworthy pre-open price source (A14" in volatile


# ------------------------------------------------- context provenance (2026-07-30 no_trade incident)
async def test_context_filters_platform_bookkeeping_and_dates_the_review(
    store, conn, assembler, agent_defs, clock, calendar
):
    """2026-07-30: the planner escalated yesterday's post-mortem into a present-tense platform
    outage (no_trade_today=true) and read our own watchlist_cap rows as a mass exchange
    surveillance action. Pins: (a) only surveillance_* exclusion reasons reach the context;
    (b) the review summary carries its session date and the HISTORY label."""
    store.upsert_universe_daily(
        [
            {"d": TODAY, "symbol": "CAPPED", "included": False, "mis_candidate": False,
             "exclusion_reasons": ["watchlist_cap"], "median_traded_value": None},
            {"d": TODAY, "symbol": "FLAGGED", "included": False, "mis_candidate": False,
             "exclusion_reasons": ["surveillance_asm"], "median_traded_value": None},
            {"d": TODAY, "symbol": "OK1", "included": True, "mis_candidate": False,
             "exclusion_reasons": None, "median_traded_value": None},
        ]
    )
    conn.execute(
        "INSERT INTO nightly_reviews (d, payload, created_at) VALUES (?, ?, ?)",
        ("2026-06-16", json.dumps({"summary": "25 consecutive analyst failures burned budget"}),
         "2026-06-16T21:00:00+05:30"),
    )
    harness = FakeHarness(AgentResult.Ok(call_id="c9", payload=_plan()))
    job = _job(store, conn, assembler, harness, agent_defs, FakeGovernor(allowed=True), clock, calendar)

    assert await job.run(TODAY) is AdvisoryOutcome.RAN
    volatile = harness.calls[0]["context"].volatile_block
    assert "FLAGGED: surveillance_asm" in volatile
    assert "CAPPED" not in volatile                       # platform bookkeeping never reaches the model
    assert "watchlist_cap" not in volatile
    assert ("prior session's post-mortem (HISTORY, may describe already-fixed issues): "
            "[review of the 2026-06-16 session] 25 consecutive analyst failures") in volatile


# --------------------------------------------------------------------------- idempotent run-latest (§2.6)
async def test_rerun_same_day_replaces_the_row(store, conn, assembler, agent_defs, clock, calendar):
    governor = FakeGovernor(allowed=True)
    harness1 = FakeHarness(AgentResult.Ok(call_id="c6", payload=_plan(regime="first run")))
    job1 = _job(store, conn, assembler, harness1, agent_defs, governor, clock, calendar)
    assert await job1.run(TODAY) is AdvisoryOutcome.RAN

    harness2 = FakeHarness(AgentResult.Ok(call_id="c7", payload=_plan(regime="second run replaces the first")))
    job2 = _job(store, conn, assembler, harness2, agent_defs, governor, clock, calendar)
    assert await job2.run(TODAY) is AdvisoryOutcome.RAN

    rows = conn.execute("SELECT payload FROM day_plans WHERE d = ?", (TODAY.isoformat(),)).fetchall()
    assert len(rows) == 1
    assert DayPlan.model_validate_json(rows[0]["payload"]).regime == "second run replaces the first"


# --------------------------------------------------------------------------- construction guard
def test_construction_rejects_a_roster_missing_the_planner(store, conn, assembler, clock, calendar):
    harness = FakeHarness(AgentResult.Ok(call_id="c8", payload=_plan()))
    governor = FakeGovernor(allowed=True)
    with pytest.raises(KeyError):
        _job(store, conn, assembler, harness, {}, governor, clock, calendar)


# ------------------------------------------------- WO-D2: positions the owner sold outside the ledger
# 2026-09-11: HDFCAMC and HINDZINC were sold on 08-26 and stayed OPEN until 09-11. Eleven consecutive
# day plans therefore reasoned about the "overnight risk" of two positions that did not exist, while
# the one thing that would have ended it -- a /closed reply -- was never put in front of the owner
# where he reads the morning plan.
def _position(conn, position_id: str, symbol: str, qty: int = 7, *, stop: str = "95") -> None:
    conn.execute(
        "INSERT INTO positions (position_id, symbol, side, style, product, qty, avg_entry, stop, "
        "state, origin, opened_at) VALUES (?, ?, 'BUY', 'swing', 'CNC', ?, '100', ?, 'OPEN', "
        "'recommended', '2026-06-10T10:00:00+05:30')",
        (position_id, symbol, qty, stop),
    )
    conn.commit()


def _observe(conn, position_id: str, d: date, *, tracked: int = 7, held: int = 0) -> None:
    conn.execute(
        "INSERT INTO holdings_observations (position_id, d, tracked_qty, held_qty, observed_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (position_id, d.isoformat(), tracked, held, f"{d.isoformat()}T10:05:00+05:30"),
    )
    conn.commit()


def _ledger(conn, entry_id: str, position_id: str, rec_id: str) -> None:
    conn.execute(
        "INSERT INTO learning_ledger (entry_id, position_id, rec_id, entry_px, created_at) "
        "VALUES (?, ?, ?, '100', '2026-06-10T10:00:00+05:30')",
        (entry_id, position_id, rec_id),
    )
    conn.commit()


async def _volatile(store, conn, assembler, agent_defs, clock, calendar) -> str:
    harness = FakeHarness(AgentResult.Ok(call_id="cD2", payload=_plan()))
    job = _job(store, conn, assembler, harness, agent_defs, FakeGovernor(allowed=True), clock, calendar)
    assert await job.run(TODAY) is AdvisoryOutcome.RAN
    return harness.calls[0]["context"].volatile_block


async def test_sold_outside_the_ledger_gets_its_own_block_and_leaves_the_risk_line(
    store, conn, assembler, agent_defs, clock, calendar
):
    """The whole fix: the sold position is OUT of the overnight-risk line the planner reasons over,
    and IN a block that names the exact reply that ends it. The genuinely held position is untouched."""
    _position(conn, "pos-held", "RELIANCE", 3, stop="90")
    _position(conn, "pos-sold", "HDFCAMC", 7)
    _ledger(conn, "led-1", "pos-sold", "REC-ENTRY-1")
    _observe(conn, "pos-sold", PRIOR)
    _observe(conn, "pos-sold", TODAY)

    volatile = await _volatile(store, conn, assembler, agent_defs, clock, calendar)

    lines = volatile.split("\n")
    risk_line = next(
        ln for ln in lines if ln.startswith("open positions and overnight risk:")
    )
    assert risk_line == "open positions and overnight risk: RELIANCE BUY 3 @ 100 (CNC, stop 90)"
    assert "HDFCAMC" not in risk_line
    assert (
        "HDFCAMC BUY 7 @ 100 (CNC) - SOLD OUTSIDE THE LEDGER (broker holdings show 0 on 2 "
        "sessions): reply /closed REC-ENTRY-1 <price>"
    ) in volatile

    # The assembler renders the whole summary as ONE prompt item, so a per-line marker alone leaves
    # the sold position as an unlabelled continuation line UNDER "open positions and overnight risk:"
    # - which is precisely the section it has to be out of, and which a planner can still fold into
    # its risk paragraph. A heading of its own is what separates the two groups.
    heading_at = lines.index(_SOLD_OUTSIDE_HEADING)
    assert heading_at == lines.index(risk_line) + 1              # immediately after the held block
    assert lines[heading_at + 1].startswith("HDFCAMC")           # ... and before the sold ones
    assert "NOT overnight risk" not in risk_line
    assert "excluded from the open positions and overnight risk above" in _SOLD_OUTSIDE_HEADING


async def test_the_only_open_position_being_sold_leaves_the_risk_line_empty(
    store, conn, assembler, agent_defs, clock, calendar
):
    """The live 09-11 shape: every tracked position had been sold. The risk line must then read
    "none" -- a plan that lists them as open risk is what produced eleven wrong mornings."""
    _position(conn, "pos-sold", "HINDZINC", 5)
    _observe(conn, "pos-sold", PRIOR, tracked=5)
    _observe(conn, "pos-sold", TODAY, tracked=5)

    volatile = await _volatile(store, conn, assembler, agent_defs, clock, calendar)

    assert "open positions and overnight risk: none" in volatile
    assert "HINDZINC BUY 5 @ 100 (CNC) - SOLD OUTSIDE THE LEDGER" in volatile


async def test_one_short_observation_day_keeps_the_position_on_the_risk_line(
    store, conn, assembler, agent_defs, clock, calendar
):
    """Same two-day rule as the position-event screen: a single short reading is not evidence of a
    sale, and dropping a live position off the overnight-risk line is the expensive error."""
    _position(conn, "pos-maybe", "TITAN", 4)
    _observe(conn, "pos-maybe", TODAY, tracked=4)

    volatile = await _volatile(store, conn, assembler, agent_defs, clock, calendar)

    assert "open positions and overnight risk: TITAN BUY 4 @ 100 (CNC, stop 95)" in volatile
    assert "SOLD OUTSIDE THE LEDGER" not in volatile


async def test_a_partial_holding_states_what_the_broker_actually_showed(
    store, conn, assembler, agent_defs, clock, calendar
):
    """Held 3 of 7 on both days is still an unreported exit, but the block says 3 rather than 0 --
    the plan must not assert a quantity the journal never observed."""
    _position(conn, "pos-part", "HDFCAMC", 7)
    _ledger(conn, "led-2", "pos-part", "REC-ENTRY-2")
    _observe(conn, "pos-part", PRIOR, held=3)
    _observe(conn, "pos-part", TODAY, held=3)

    volatile = await _volatile(store, conn, assembler, agent_defs, clock, calendar)

    assert "broker holdings show 3 on 2 sessions" in volatile
    # ... and the heading has to stay true of THIS line too. Those three shares are real overnight
    # exposure: the heading may void the TRACKED size (7) and must point the planner at the quantity
    # the line states, never tell it the name carries no exposure at all.
    assert "Never treat the tracked size as exposure" in _SOLD_OUTSIDE_HEADING
    assert "the quantity the broker actually showed, stated on each line" in _SOLD_OUTSIDE_HEADING


async def test_a_sold_position_with_no_ledger_row_still_gets_an_actionable_line(
    store, conn, assembler, agent_defs, clock, calendar
):
    """No ledger row => no rec id to type. The block says so and names the position id, exactly as
    the §3.6 alert does: an explicit un-actionable line beats a silent omission."""
    _position(conn, "pos-orphan", "HINDZINC", 5)
    _observe(conn, "pos-orphan", PRIOR, tracked=5)
    _observe(conn, "pos-orphan", TODAY, tracked=5)

    volatile = await _volatile(store, conn, assembler, agent_defs, clock, calendar)

    assert "no learning-ledger row for position pos-orphan" in volatile
