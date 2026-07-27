"""PreopenPlannerJob (§5.3, plan lines 1155-1163): governor gate, deterministic inputs, persistence.

A fake governor and a fake harness (recording the real ``ContextAssembler``-built context) stand in
for the SDK boundary — same style as ``test_agent_harness.py``'s fakes, but here the harness itself
is faked so these tests exercise the JOB's own logic (input assembly, gating, persistence) against a
real ``ContextAssembler``/``MarketStore``/SQLite ``conn`` (conftest's migrated temp db + frozen clock).
"""

from __future__ import annotations

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
from engine.ops.preopen_planner import CALL_CLASS, PreopenPlannerJob

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

    assert ok is True
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

    assert ok is False
    assert harness.calls == []                                        # planner death never calls the SDK
    assert governor.calls == [(preopen.AGENT_ID, CALL_CLASS)]
    row = conn.execute("SELECT 1 FROM day_plans WHERE d = ?", (TODAY.isoformat(),)).fetchone()
    assert row is None


# --------------------------------------------------------------------------- harness failure
async def test_harness_failure_returns_false_and_persists_nothing(store, conn, assembler, agent_defs, clock, calendar):
    harness = FakeHarness(AgentResult.Failed("schema_invalid", "not valid JSON", call_id="c2"))
    governor = FakeGovernor(allowed=True)
    alerts: list[tuple[str, str]] = []

    async def notify(severity: str, message: str) -> None:
        alerts.append((severity, message))

    job = _job(store, conn, assembler, harness, agent_defs, governor, clock, calendar, notify=notify)

    ok = await job.run(TODAY)

    assert ok is False
    assert len(harness.calls) == 1                                     # the call WAS attempted
    row = conn.execute("SELECT 1 FROM day_plans WHERE d = ?", (TODAY.isoformat(),)).fetchone()
    assert row is None
    assert len(alerts) == 1
    assert alerts[0][0] == "error"
    assert "schema_invalid" in alerts[0][1]


async def test_harness_failure_with_no_notify_still_returns_false(store, conn, assembler, agent_defs, clock, calendar):
    """``notify`` is optional (§5.3 step 3) — its absence must not raise."""
    harness = FakeHarness(AgentResult.Failed("timeout", "no complete response within 60s", call_id="c3"))
    governor = FakeGovernor(allowed=True)
    job = _job(store, conn, assembler, harness, agent_defs, governor, clock, calendar)

    assert await job.run(TODAY) is False


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

    assert ok is True
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


# --------------------------------------------------------------------------- fail-to-zero rendering
async def test_empty_tables_render_unavailable_without_raising(store, conn, assembler, agent_defs, clock, calendar):
    """D7: every deterministic input on a completely empty store/conn degrades to text, never raises."""
    harness = FakeHarness(AgentResult.Ok(call_id="c5", payload=_plan()))
    governor = FakeGovernor(allowed=True)
    job = _job(store, conn, assembler, harness, agent_defs, governor, clock, calendar)

    ok = await job.run(TODAY)

    assert ok is True
    volatile = harness.calls[0]["context"].volatile_block
    # Foundational-data gaps (never even ran) => explicit "unavailable".
    assert "overnight movers (bhavcopy):\n  - unavailable" in volatile
    assert "catalyst digest:\n  - unavailable" in volatile
    assert "surveillance changes:\n  - unavailable" in volatile
    # Real, legitimate zeros (ran, found nothing) => "none", not "unavailable".
    assert "catalyst watchlist (binding levels are the scanner's): none" in volatile
    assert "earnings today: none" in volatile
    assert "open positions and overnight risk: none" in volatile
    assert "yesterday's review: none" in volatile
    # The A14 gap-scan decision: never silently "none".
    assert "gap scan vs prior close:\n  - not computed: no trustworthy pre-open price source (A14" in volatile


# --------------------------------------------------------------------------- idempotent run-latest (§2.6)
async def test_rerun_same_day_replaces_the_row(store, conn, assembler, agent_defs, clock, calendar):
    governor = FakeGovernor(allowed=True)
    harness1 = FakeHarness(AgentResult.Ok(call_id="c6", payload=_plan(regime="first run")))
    job1 = _job(store, conn, assembler, harness1, agent_defs, governor, clock, calendar)
    assert await job1.run(TODAY) is True

    harness2 = FakeHarness(AgentResult.Ok(call_id="c7", payload=_plan(regime="second run replaces the first")))
    job2 = _job(store, conn, assembler, harness2, agent_defs, governor, clock, calendar)
    assert await job2.run(TODAY) is True

    rows = conn.execute("SELECT payload FROM day_plans WHERE d = ?", (TODAY.isoformat(),)).fetchall()
    assert len(rows) == 1
    assert DayPlan.model_validate_json(rows[0]["payload"]).regime == "second run replaces the first"


# --------------------------------------------------------------------------- construction guard
def test_construction_rejects_a_roster_missing_the_planner(store, conn, assembler, clock, calendar):
    harness = FakeHarness(AgentResult.Ok(call_id="c8", payload=_plan()))
    governor = FakeGovernor(allowed=True)
    with pytest.raises(KeyError):
        _job(store, conn, assembler, harness, {}, governor, clock, calendar)
