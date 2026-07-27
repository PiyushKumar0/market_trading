"""Token-budget governor (D6, §5.6/§11.2): pricing, trading-day pace, the degrade ladder, and the
call-admission predicates consumers read.

Every dollar figure here is produced through the real :meth:`BudgetGovernor.record` path. Exact
threshold amounts are reached with ``haiku-4.5`` input tokens because that rate is $1/MTok — one input
token is exactly one micro-dollar, so an arbitrary Decimal amount lands on the ledger EXACTLY and the
``>`` vs ``>=`` boundary behaviour is testable rather than approximated.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import pytest

from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.config import config_dir, load_yaml
from engine.core.enums import DegradeTier
from engine.intelligence.events import TOPIC_BUDGET_STATE, BudgetStateChanged
from engine.intelligence.governor import BudgetGovernor, InvokeDecision, TokenUsage

# conftest's FIXED_NOW: Wed 2026-06-17. June 2026 has 22 weekdays, one of them (Fri 2026-06-26,
# Muharram) a holiday => 21 trading days; 13 of them have elapsed through the 17th (R6).
JUN_TOTAL_TRADING_DAYS = 21
JUN_ELAPSED_THROUGH_17 = 13


# --------------------------------------------------------------------------- fixtures / helpers
@pytest.fixture
def calendar(clock) -> NSECalendar:
    return NSECalendar(config_dir() / "calendar", clock, strict=False)


@pytest.fixture
def real_cfg() -> dict:
    return load_yaml(config_dir() / "agents.yaml")


@pytest.fixture
def gov(conn, clock, calendar, real_cfg, bus) -> BudgetGovernor:
    return BudgetGovernor(conn, clock, calendar, real_cfg, bus=bus)


def isolated_cfg(real_cfg: dict, credit: str = "105") -> dict:
    """Real config with a $105 credit and one huge allocation.

    $105 makes the pace thresholds land on exact decimals at the fixed clock ($105 × 13/21 = $65, so
    DG1 = $71.50 and DG2 = $81.25 exactly), and the oversized allocation keeps the per-agent DG1 rule
    from firing so a pace-boundary test measures only the pace rule.
    """
    cfg = dict(real_cfg)
    cfg["llm"] = {**real_cfg["llm"], "monthly_credit_usd": credit}
    cfg["budget_allocations_usd"] = {"intraday_analyst": 200}
    return cfg


async def spend(gov: BudgetGovernor, usd: str, agent: str = "weekly_researcher", at=None) -> None:
    """Book exactly ``usd`` to ``agent`` (haiku input = $1/MTok => 1 token == 1 micro-dollar)."""
    micro = Decimal(usd) * 1_000_000
    tokens = int(micro)
    assert Decimal(tokens) == micro, f"{usd} is not a whole number of micro-dollars"
    await gov.record(agent, "haiku-4.5", TokenUsage(in_tokens=tokens, out_tokens=0), at=at)


async def _collect(sink: list, event) -> None:
    sink.append(event)


# --------------------------------------------------------------------------- pricing (D4/§11.2)
async def test_price_miss_matches_worksheet_footnote(gov):
    # §11.2 †: intraday analyst on a cache MISS — 7k cacheable prefix written at 1.25× input, 3k
    # remaining input, 1k output, sonnet-4.6 at $3/$15 per MTok.
    cost = gov.price("sonnet-4.6", TokenUsage(in_tokens=3000, out_tokens=1000, cache_write=7000))
    assert cost == Decimal("0.05025")             # 7000×3.75 + 3000×3 + 1000×15 micro-dollars
    assert cost.quantize(Decimal("0.001")) == Decimal("0.050")   # the published §11.2 figure


async def test_price_hit_matches_worksheet_footnote(gov):
    # §11.2 ‡: same call on a cache HIT — the 7k prefix is read at 0.1× input instead.
    cost = gov.price("sonnet-4.6", TokenUsage(in_tokens=3000, out_tokens=1000, cache_read=7000))
    assert cost == Decimal("0.0261")
    assert cost.quantize(Decimal("0.001")) == Decimal("0.026")


async def test_price_uses_per_model_rates(gov):
    usage = TokenUsage(in_tokens=1_000_000, out_tokens=0)
    assert gov.price("haiku-4.5", usage) == Decimal("1")
    assert gov.price("sonnet-4.6", usage) == Decimal("3")
    assert gov.price("opus-4.8", usage) == Decimal("5")


async def test_unpriced_model_fails_loud(gov):
    # A model missing from the D4 table would otherwise bill as $0 and blind the ladder all month.
    with pytest.raises(ValueError, match="no D4 pricing"):
        gov.price("sonnet-9.9", TokenUsage(in_tokens=1, out_tokens=1))


async def test_record_appends_priced_ledger_row(gov, conn, clock):
    await gov.record(
        "intraday_analyst", "sonnet-4.6", TokenUsage(in_tokens=3000, out_tokens=1000, cache_write=7000)
    )
    row = conn.execute("SELECT * FROM budget_ledger").fetchone()
    assert row["agent_id"] == "intraday_analyst"
    assert row["model"] == "sonnet-4.6"
    assert row["cache_write"] == 7000
    assert Decimal(row["cost_usd"]) == Decimal("0.05025")   # TEXT column, decimal-as-string (§8.1)
    assert row["month"] == "2026-06"
    assert row["at"] == clock.now().isoformat()


async def test_month_and_agent_spend_sum_the_ledger(gov):
    await spend(gov, "1.5", agent="intraday_analyst")
    await spend(gov, "2.25", agent="intraday_analyst")
    await spend(gov, "0.75", agent="news_analyst")
    assert gov.agent_spend("intraday_analyst") == Decimal("3.75")
    assert gov.agent_spend("news_analyst") == Decimal("0.75")
    assert gov.month_spend() == Decimal("4.5")
    assert gov.month_spend("2026-05") == Decimal(0)


# --------------------------------------------------------------------------- pace (R6)
async def test_pro_rata_counts_trading_days_only(gov, calendar):
    assert calendar.is_trading_day(date(2026, 6, 20)) is False   # Saturday
    assert calendar.is_trading_day(date(2026, 6, 26)) is False   # Muharram holiday
    assert gov.trading_days("2026-06") == (JUN_ELAPSED_THROUGH_17, JUN_TOTAL_TRADING_DAYS)
    # Calendar days (17/30) would put the month at $56.67; trading days put it at $61.90 (§11.2).
    assert gov.pro_rata_to_date() == Decimal(100) * Decimal(13) / Decimal(21)


async def test_pro_rata_is_zero_without_a_calendar(conn, clock, real_cfg, tmp_path):
    bare = NSECalendar(tmp_path / "no_calendar", clock, strict=False)
    gov = BudgetGovernor(conn, clock, bare, real_cfg)
    assert gov.trading_days() == (0, 0)
    assert gov.pro_rata_to_date() == Decimal(0)   # unmeasurable pace is never "over pace"


# --------------------------------------------------------------------------- ladder boundaries
async def test_dg1_pace_boundary_is_strict(conn, clock, calendar, real_cfg):
    gov = BudgetGovernor(conn, clock, calendar, isolated_cfg(real_cfg))
    assert gov.pro_rata_to_date() == Decimal(65)

    await spend(gov, "71.50", agent="intraday_analyst")          # exactly 110% of pace
    assert gov.degrade_tier() == DegradeTier.DG0
    await spend(gov, "0.000001", agent="intraday_analyst")       # one micro-dollar past it
    assert gov.degrade_tier() == DegradeTier.DG1


async def test_dg2_pace_boundary_is_strict(conn, clock, calendar, real_cfg):
    gov = BudgetGovernor(conn, clock, calendar, isolated_cfg(real_cfg))
    await spend(gov, "81.25", agent="intraday_analyst")          # exactly 125% of pace
    assert gov.degrade_tier() == DegradeTier.DG1
    await spend(gov, "0.000001", agent="intraday_analyst")
    assert gov.degrade_tier() == DegradeTier.DG2


async def test_dg1_agent_allocation_boundary_is_strict(gov):
    await spend(gov, "35.70", agent="intraday_analyst")          # exactly 85% of the $42 allocation
    assert gov.degrade_tier() == DegradeTier.DG0
    await spend(gov, "0.000001", agent="intraday_analyst")
    assert gov.degrade_tier() == DegradeTier.DG1


async def test_dg2_global_boundary_is_strict(conn, calendar, real_cfg):
    # Fixed clock moved to Tue 2026-06-23 (17 of 21 trading days elapsed) so 125% of pace is $101.19
    # and the global-85% rule, not the pace rule, is the binding one at $85.
    clock = Clock(time_source=lambda: datetime(2026, 6, 23, 10, 5, tzinfo=IST))
    cal = NSECalendar(config_dir() / "calendar", clock, strict=False)
    gov = BudgetGovernor(conn, clock, cal, real_cfg)
    assert gov.trading_days() == (17, 21)

    # 85% of EVERY allocation sums to exactly 85% of the credit, so no agent trips the DG1 rule either.
    for agent, usd in [
        ("intraday_analyst", "35.70"), ("nightly_reviewer", "17.00"), ("weekly_researcher", "10.20"),
        ("news_analyst", "11.90"), ("preopen_planner", "4.25"), ("reserve", "5.95"),
    ]:
        await spend(gov, usd, agent=agent)
    assert gov.month_spend() == Decimal("85.00")
    assert gov.degrade_tier() == DegradeTier.DG0

    await spend(gov, "0.000001", agent="intraday_analyst")
    assert gov.degrade_tier() == DegradeTier.DG2


async def test_dg3_and_dg4_boundaries_are_inclusive(gov):
    await spend(gov, "94.999999")
    assert gov.degrade_tier() == DegradeTier.DG2
    await spend(gov, "0.000001")                                 # exactly $95.00 => DG3 (>=)
    assert gov.degrade_tier() == DegradeTier.DG3
    await spend(gov, "4.999999")
    assert gov.degrade_tier() == DegradeTier.DG3
    await spend(gov, "0.000001")                                 # exactly $100.00 => DG4 (>=)
    assert gov.degrade_tier() == DegradeTier.DG4


async def test_billing_error_trips_dg4_from_dg0(gov):
    await spend(gov, "1.00")
    assert gov.degrade_tier() == DegradeTier.DG0
    gov.note_billing_error("credit_exhausted_http_402")
    assert gov.degrade_tier() == DegradeTier.DG4


# --------------------------------------------------------------------------- call admission
async def test_heartbeats_are_off_at_dg2_but_not_dg1(gov):
    await spend(gov, "36.00")                                    # weekly researcher > 85% of $12 => DG1
    assert gov.degrade_tier() == DegradeTier.DG1
    assert gov.can_invoke("intraday_analyst", "heartbeat").allowed is True

    await spend(gov, "44.00")                                    # $80 > 125% of pace ($77.38) => DG2
    assert gov.degrade_tier() == DegradeTier.DG2
    decision = gov.can_invoke("intraday_analyst", "heartbeat")
    assert decision == InvokeDecision(allowed=False, tier=DegradeTier.DG2, reason="DG2_heartbeats_off")
    # Only the heartbeat class is cut at DG2 — signal/position-event calls continue (§5.6).
    assert gov.can_invoke("intraday_analyst", "signal_candidate").allowed is True


async def test_dg3_blocks_intraday_class_agents_only(gov):
    # All $96 booked to the weekly researcher so the three agents under test are nowhere near their
    # own allocations — the block must come from the tier, not from exhaustion.
    await spend(gov, "96.00")
    assert gov.degrade_tier() == DegradeTier.DG3
    for agent in ("intraday_analyst", "news_analyst", "preopen_planner"):
        decision = gov.can_invoke(agent)
        assert decision.allowed is False
        assert decision.reason == "DG3_intraday_llm_off"
    assert gov.can_invoke("nightly_reviewer").allowed is True


async def test_dg4_blocks_every_agent(gov):
    await spend(gov, "100.00")
    assert gov.can_invoke("nightly_reviewer").reason == "DG4_zero_sdk_calls"
    assert gov.can_invoke("intraday_analyst", "heartbeat").allowed is False


async def test_allocation_exhaustion_blocks_only_that_agent(gov):
    await spend(gov, "5.00", agent="preopen_planner")            # the whole $5 allocation
    # 100% of an allocation is necessarily >85% of it, so the ladder is at DG1 — but the block on this
    # one agent is the allocation rule, and every other agent is unaffected.
    assert gov.degrade_tier() == DegradeTier.DG1
    assert gov.can_invoke("preopen_planner").reason == "agent_allocation_exhausted"
    assert gov.can_invoke("intraday_analyst").allowed is True


async def test_allocation_exhaustion_blocks_even_at_dg0(conn, clock, calendar, real_cfg):
    # Same rule with the DG1 agent trip lifted to 100%: the tier is DG0 and the agent is still blocked.
    cfg = dict(real_cfg)
    cfg["degrade_ladder"] = {**real_cfg["degrade_ladder"], "DG1": {"pro_rata_pct": 110, "agent_alloc_pct": 100}}
    gov = BudgetGovernor(conn, clock, calendar, cfg)
    await spend(gov, "5.00", agent="preopen_planner")
    assert gov.degrade_tier() == DegradeTier.DG0
    assert gov.can_invoke("preopen_planner") == InvokeDecision(
        allowed=False, tier=DegradeTier.DG0, reason="agent_allocation_exhausted"
    )


# --------------------------------------------------------------------------- behaviour knobs (§5.6)
async def test_behaviour_knobs_per_tier(gov):
    assert (gov.heartbeat_interval_min(), gov.prescreen_forward_cap(), gov.news_batch_cadence_min()) == (
        20, 6, 30,
    )
    await spend(gov, "36.00")                                    # => DG1
    assert (gov.heartbeat_interval_min(), gov.prescreen_forward_cap(), gov.news_batch_cadence_min()) == (
        45, 4, 30,
    )
    await spend(gov, "44.00")                                    # => DG2
    assert (gov.heartbeat_interval_min(), gov.prescreen_forward_cap(), gov.news_batch_cadence_min()) == (
        None, 4, 60,
    )
    await spend(gov, "16.00")                                    # => DG3
    assert gov.degrade_tier() == DegradeTier.DG3
    assert (gov.heartbeat_interval_min(), gov.prescreen_forward_cap(), gov.news_batch_cadence_min()) == (
        None, 4, 60,
    )


async def test_full_grade_knobs_come_from_agents_yaml(conn, clock, calendar, real_cfg):
    cfg = dict(real_cfg)
    cfg["agents"] = {
        **real_cfg["agents"],
        "intraday_analyst": {**real_cfg["agents"]["intraday_analyst"], "heartbeat_min": 25, "prescreen_cap_per_day": 9},
    }
    gov = BudgetGovernor(conn, clock, calendar, cfg)
    assert gov.heartbeat_interval_min() == 25
    assert gov.prescreen_forward_cap() == 9


async def test_from_config_loads_agents_yaml(conn, clock, calendar):
    gov = BudgetGovernor.from_config(conn, clock, calendar)
    assert gov.price("sonnet-4.6", TokenUsage(in_tokens=1_000_000, out_tokens=0)) == Decimal("3")
    assert gov.degrade_tier() == DegradeTier.DG0
    await spend(gov, "100.00")
    assert gov.degrade_tier() == DegradeTier.DG4                 # the shipped $100 credit (D3)


# --------------------------------------------------------------------------- bus (§3.2.1, R8)
async def test_tier_change_publishes_exactly_once(conn, clock, calendar, real_cfg, bus):
    seen: list[BudgetStateChanged] = []

    async def handler(event):
        seen.append(event)

    bus.subscribe(TOPIC_BUDGET_STATE, handler)
    gov = BudgetGovernor(conn, clock, calendar, isolated_cfg(real_cfg), bus=bus)

    await spend(gov, "60.00", agent="intraday_analyst")          # still DG0
    assert seen == []
    await spend(gov, "12.00", agent="intraday_analyst")          # $72 > $71.50 => DG1
    assert len(seen) == 1
    await spend(gov, "1.00", agent="intraday_analyst")           # still DG1 — no second event
    assert len(seen) == 1
    await spend(gov, "10.00", agent="intraday_analyst")          # $83 > $81.25 => DG2
    assert len(seen) == 2

    assert (seen[0].old_tier, seen[0].new_tier) == (DegradeTier.DG0, DegradeTier.DG1)
    assert (seen[1].old_tier, seen[1].new_tier) == (DegradeTier.DG1, DegradeTier.DG2)
    assert seen[1].month_spend_usd == Decimal("83")
    assert seen[1].at == clock.now()


async def test_raise_billing_error_publishes(conn, clock, calendar, real_cfg, bus):
    seen: list[BudgetStateChanged] = []
    bus.subscribe(TOPIC_BUDGET_STATE, lambda e: _collect(seen, e))
    gov = BudgetGovernor(conn, clock, calendar, real_cfg, bus=bus)
    await spend(gov, "1.00")
    assert await gov.raise_billing_error("http_402") == DegradeTier.DG4
    assert [(e.old_tier, e.new_tier) for e in seen] == [(DegradeTier.DG0, DegradeTier.DG4)]


# --------------------------------------------------------------------------- month rollover
async def test_month_rollover_resets_the_tier(conn, calendar, real_cfg, bus):
    now = [datetime(2026, 6, 17, 10, 5, tzinfo=IST)]
    clock = Clock(time_source=lambda: now[0])
    cal = NSECalendar(config_dir() / "calendar", clock, strict=False)
    seen: list[BudgetStateChanged] = []
    bus.subscribe(TOPIC_BUDGET_STATE, lambda e: _collect(seen, e))
    gov = BudgetGovernor(conn, clock, cal, real_cfg, bus=bus)

    await spend(gov, "100.00")
    gov.note_billing_error("credit_exhausted")
    assert gov.degrade_tier() == DegradeTier.DG4
    seen.clear()

    now[0] = datetime(2026, 7, 15, 10, 5, tzinfo=IST)
    assert gov.degrade_tier() == DegradeTier.DG0     # new month's ledger is empty; the DG4 latch cleared
    assert gov.month_spend() == Decimal(0)
    assert gov.month_spend("2026-06") == Decimal("100.00")   # June's history is untouched

    await spend(gov, "0.50")
    assert gov.degrade_tier() == DegradeTier.DG0
    # The recovery is a tier CHANGE and must alert like any other (R8).
    assert [(e.old_tier, e.new_tier) for e in seen] == [(DegradeTier.DG4, DegradeTier.DG0)]


async def test_record_keys_the_month_from_its_own_timestamp(gov, conn, clock):
    await spend(gov, "1.00", at=datetime(2026, 5, 29, 23, 59, tzinfo=IST))
    months = [r["month"] for r in conn.execute("SELECT month FROM budget_ledger").fetchall()]
    assert months == ["2026-05"]
    assert gov.month_spend() == Decimal(0)           # June (the clock's month) is still clean
    assert gov.month_spend("2026-05") == Decimal("1.00")
