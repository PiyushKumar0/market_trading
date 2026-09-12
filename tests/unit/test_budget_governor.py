"""Token-budget governor (D6, §5.6/§11.2): the WEEKLY quota window, pricing, trading-session pace, the
degrade ladder, and the call-admission predicates consumers read.

Every dollar figure here is produced through the real :meth:`BudgetGovernor.record` path. Exact
threshold amounts are reached with ``haiku-4.5`` input tokens because that rate is $1/MTok — one input
token is exactly one micro-dollar, so an arbitrary Decimal amount lands on the ledger EXACTLY and the
``>`` vs ``>=`` boundary behaviour is testable rather than approximated.

The 2026 calendar makes two windows worth naming, and both are used below:
  * ``2026-06-11`` — an ordinary week: Thu(½) Fri Mon Tue Wed Thu(½) = 5.0 sessions;
  * ``2026-01-15`` — the reset Thursday is itself a market holiday (Maharashtra election): the window
    still opens at 14:00 that day (the reset is wall-clock) but contributes 0 sessions => 4.5.
"""

from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal

import pytest

from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.config import config_dir, load_yaml
from engine.core.enums import DegradeTier
from engine.intelligence.events import TOPIC_BUDGET_STATE, BudgetStateChanged
from engine.intelligence.governor import (
    BudgetGovernor,
    InvokeDecision,
    TokenUsage,
    WindowAnchor,
    _window_key,
)

# conftest's FIXED_NOW: Wed 2026-06-17 10:05 IST, inside the window that opened Thu 2026-06-11 14:00.
# 4.5 of its 5.0 trading sessions have elapsed (Thu½ + Fri + Mon + Tue + Wed), so at a $100 credit the
# pace is $90 — DG1 needs >$99 and DG2-by-pace >$112.50, both ABOVE the $85 global rule. Pace-boundary
# tests therefore run on the Friday clock below, where the pace is low enough for DG1 to be reachable.
JUN_WINDOW = "2026-06-11"
JUN_SESSIONS_TOTAL = Decimal("5.0")
JUN_SESSIONS_THROUGH_WED = Decimal("4.5")

#: Fri 2026-06-12 10:05: 1.5 of 5.0 sessions elapsed => pace $30 at a $100 credit, so the DG1 pace trip
#: is exactly $33.00 and the DG2 pace trip exactly $37.50 — both well under the $85 global rule.
FRI_NOW = datetime(2026, 6, 12, 10, 5, tzinfo=IST)

ANCHOR = WindowAnchor(3, time(14, 0))           # Thursday (Mon=0) 14:00 IST


# --------------------------------------------------------------------------- fixtures / helpers
@pytest.fixture
def calendar(clock) -> NSECalendar:
    return NSECalendar(config_dir() / "calendar", clock, strict=False)


#: PINNED worked-example config (the §5.6 numbers as designed). The exact-boundary tests pin their
#: arithmetic against THIS, deliberately NOT against config/agents.yaml — the live file is owner-tunable
#: at will (the credit moved 100→120→550→200 across three rebalances and broke the old file-coupled
#: suite), and a worked example that moves with owner edits proves nothing. ``prescreen_cap_per_day``
#: DOES mirror the live 48 on purpose: the proportional degraded cap is only meaningful against a base
#: big enough for the fraction to matter.
PINNED_CFG: dict = {
    "llm": {
        "weekly_credit_usd": 100,
        "quota_window": {"reset_weekday": "thursday", "reset_time_ist": "14:00"},
    },
    "model_pricing_usd_per_mtok": {
        "haiku-4.5": {"input": 1, "output": 5},
        "sonnet-4.6": {"input": 3, "output": 15},
        "opus-4.8": {"input": 5, "output": 25},
    },
    "budget_allocations_usd": {
        "intraday_analyst": 42, "nightly_reviewer": 20, "weekly_researcher": 12,
        "news_analyst": 14, "preopen_planner": 5, "reserve": 7,
    },
    "degrade_ladder": {
        "DG1": {"pro_rata_pct": 110, "agent_alloc_pct": 85, "degraded_forward_cap_frac": 0.67},
        "DG2": {"pro_rata_pct": 125, "global_alloc_pct": 85},
        "DG3": {"global_alloc_pct": 95},
    },
    "agents": {"intraday_analyst": {"heartbeat_min": 20, "prescreen_cap_per_day": 48}},
}


@pytest.fixture
def real_cfg() -> dict:
    return load_yaml(config_dir() / "agents.yaml")


@pytest.fixture
def gov(conn, clock, calendar, bus) -> BudgetGovernor:
    return BudgetGovernor(conn, clock, calendar, PINNED_CFG, bus=bus)


def governor_at(conn, when: datetime, cfg: dict | None = None, bus=None) -> BudgetGovernor:
    """A governor whose clock is frozen at ``when`` (its own calendar, so is_trading_day agrees)."""
    clock = Clock(time_source=lambda: when)
    cal = NSECalendar(config_dir() / "calendar", clock, strict=False)
    return BudgetGovernor(conn, clock, cal, cfg if cfg is not None else PINNED_CFG, bus=bus)


@pytest.fixture
def fri_gov(conn, bus) -> BudgetGovernor:
    return governor_at(conn, FRI_NOW, bus=bus)


def test_real_agents_yaml_loads_and_prices(conn, clock, calendar, real_cfg) -> None:
    """Schema smoke against the LIVE owner-tunable file: it must construct and price — no amount
    assertions (owner edits must never fail the suite; the math is pinned above)."""
    g = BudgetGovernor(conn, clock, calendar, real_cfg)
    assert g.credit() > 0
    assert g.allocations()
    assert g.price("haiku-4.5", TokenUsage(in_tokens=1_000_000, out_tokens=0)) > 0


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
    # A model missing from the D4 table would otherwise bill as $0 and blind the ladder all window.
    with pytest.raises(ValueError, match="no D4 pricing"):
        gov.price("sonnet-9.9", TokenUsage(in_tokens=1, out_tokens=1))


# --------------------------------------------------------------------------- config contract
async def test_config_without_weekly_credit_fails_loud(conn, clock, calendar):
    """A config still carrying the retired monthly key must not silently run a weekly ladder against a
    monthly number (or a default) — it would simply never trip."""
    stale = {**PINNED_CFG, "llm": {"monthly_credit_usd": 550}}
    with pytest.raises(ValueError, match="weekly_credit_usd"):
        BudgetGovernor(conn, clock, calendar, stale)


async def test_live_agents_yaml_has_retired_the_monthly_key(real_cfg):
    assert "weekly_credit_usd" in real_cfg["llm"]
    assert "monthly_credit_usd" not in real_cfg["llm"]


# --------------------------------------------------------------------------- window key (§5.6)
@pytest.mark.parametrize(
    "at,expected",
    [
        (datetime(2026, 9, 9, 23, 59, tzinfo=IST), "2026-09-03"),    # Wed night — window that ends
        (datetime(2026, 9, 10, 13, 59, tzinfo=IST), "2026-09-03"),   # reset-Thursday MORNING: still old
        (datetime(2026, 9, 10, 14, 0, tzinfo=IST), "2026-09-10"),    # the reset instant opens the new one
        (datetime(2026, 9, 10, 14, 1, tzinfo=IST), "2026-09-10"),
        (datetime(2026, 9, 13, 12, 0, tzinfo=IST), "2026-09-10"),    # Sunday — mid-window
    ],
)
def test_window_key_splits_on_the_thursday_1400_reset(at, expected):
    assert _window_key(at, ANCHOR) == expected


def test_window_key_is_wall_clock_not_calendar_aware():
    """Thu 2026-01-15 is an NSE holiday; the SUBSCRIPTION still refills at 14:00 that day, so the key
    must roll anyway. Only the pace denominator is allowed to know about holidays."""
    assert _window_key(datetime(2026, 1, 15, 13, 59, tzinfo=IST), ANCHOR) == "2026-01-08"
    assert _window_key(datetime(2026, 1, 15, 14, 0, tzinfo=IST), ANCHOR) == "2026-01-15"


def test_window_key_normalises_a_non_ist_stamp():
    # 2026-09-10 08:29 UTC == 13:59 IST — still the OLD window. Keying off the raw UTC weekday/hour
    # would put it in the new one.
    from datetime import UTC

    assert _window_key(datetime(2026, 9, 10, 8, 29, tzinfo=UTC), ANCHOR) == "2026-09-03"


def test_window_key_rejects_a_naive_timestamp(gov):
    """``astimezone`` on a naive value reads it as SYSTEM-local, so on a non-IST host the same instant
    would key a different week here than through ``record`` — which refuses it. One contract, enforced
    at the arithmetic."""
    with pytest.raises(ValueError, match="tz-aware"):
        _window_key(datetime(2026, 9, 10, 13, 59), ANCHOR)
    with pytest.raises(ValueError, match="tz-aware"):
        gov.window_key(datetime(2026, 9, 10, 13, 59))


@pytest.mark.parametrize(
    "at",
    [
        datetime(2026, 9, 9, 23, 59, tzinfo=IST),
        datetime(2026, 9, 10, 13, 59, 59, 999999, tzinfo=IST),
        datetime(2026, 9, 10, 14, 0, tzinfo=IST),
        datetime(2026, 9, 13, 12, 0, tzinfo=IST),
        datetime(2026, 1, 15, 14, 0, tzinfo=IST),
    ],
)
def test_window_bounds_round_trips_the_key(gov, at):
    start, end = gov.window_bounds(_window_key(at, ANCHOR))
    assert start <= at < end
    assert (end - start).days == 7
    assert start.tzinfo is not None and end.tzinfo is not None
    assert start.timetz().hour == 14 and start.date().weekday() == 3
    assert _window_key(start, ANCHOR) == _window_key(end - (end - start) / 2, ANCHOR)


def test_window_bounds_rejects_an_off_weekday_key(gov):
    # A key from anywhere but ``_window_key`` would define a week no ledger row is ever assigned to.
    with pytest.raises(ValueError, match="Wednesday"):
        gov.window_bounds("2026-09-09")


# --------------------------------------------------------------------------- ledger
async def test_record_appends_priced_ledger_row(gov, conn, clock):
    await gov.record(
        "intraday_analyst", "sonnet-4.6", TokenUsage(in_tokens=3000, out_tokens=1000, cache_write=7000)
    )
    row = conn.execute("SELECT * FROM budget_ledger").fetchone()
    assert row["agent_id"] == "intraday_analyst"
    assert row["model"] == "sonnet-4.6"
    assert row["cache_write"] == 7000
    assert Decimal(row["cost_usd"]) == Decimal("0.05025")   # TEXT column, decimal-as-string (§8.1)
    assert row["month"] == "2026-06"                       # history only — the ladder ranges on `at`
    assert row["at"] == clock.now().isoformat()


async def test_record_rejects_a_naive_timestamp(gov):
    # ``at`` is range-scanned as an ISO string; a naive stamp sorts outside every window and its spend
    # would silently vanish from the ladder.
    with pytest.raises(ValueError, match="tz-aware"):
        await gov.record(
            "intraday_analyst", "haiku-4.5", TokenUsage(in_tokens=1, out_tokens=0),
            at=datetime(2026, 6, 17, 10, 5),
        )


async def test_window_and_agent_spend_sum_the_ledger(gov):
    await spend(gov, "1.5", agent="intraday_analyst")
    await spend(gov, "2.25", agent="intraday_analyst")
    await spend(gov, "0.75", agent="news_analyst")
    assert gov.agent_spend("intraday_analyst") == Decimal("3.75")
    assert gov.agent_spend("news_analyst") == Decimal("0.75")
    assert gov.window_spend() == Decimal("4.5")
    assert gov.window_spend("2026-06-04") == Decimal(0)     # the previous window is untouched


async def test_window_agents_covers_spend_with_no_allocation(gov):
    """The owner surfaces split the window total per agent; an agent that billed but was never
    budgeted (``sdk_smoke`` is already in the live ledger) would otherwise sit inside the headline and
    in no row, so the /budget table would not add up to the figure printed above it."""
    await spend(gov, "1.25", agent="intraday_analyst")
    await spend(gov, "0.20", agent="sdk_smoke")
    assert gov.window_agents() == ["intraday_analyst", "sdk_smoke"]
    assert "sdk_smoke" not in gov.allocations()
    covered = sum((gov.agent_spend(a) for a in gov.window_agents()), Decimal(0))
    assert covered == gov.window_spend() == Decimal("1.45")
    assert gov.window_agents("2026-06-04") == []            # scoped to its window like every reader


async def test_spend_is_bounded_by_the_reset_instants(gov, conn):
    """Half-open [start, end): a call at 13:59:59.999999 belongs to the window that ENDS, one at
    exactly 14:00:00 to the window that opens."""
    await spend(gov, "1.00", at=datetime(2026, 6, 11, 13, 59, 59, 999999, tzinfo=IST))
    await spend(gov, "2.00", at=datetime(2026, 6, 11, 14, 0, tzinfo=IST))
    await spend(gov, "4.00", at=datetime(2026, 6, 18, 13, 59, 59, 999999, tzinfo=IST))
    await spend(gov, "8.00", at=datetime(2026, 6, 18, 14, 0, tzinfo=IST))
    assert gov.window_spend("2026-06-04") == Decimal("1.00")
    assert gov.window_spend(JUN_WINDOW) == Decimal("6.00")
    assert gov.window_spend("2026-06-18") == Decimal("8.00")


async def test_record_keys_the_window_from_its_own_timestamp(gov, conn):
    await spend(gov, "1.00", at=datetime(2026, 6, 10, 23, 59, tzinfo=IST))
    assert gov.window_spend() == Decimal(0)                  # the live window is still clean
    assert gov.window_spend("2026-06-04") == Decimal("1.00")
    assert [r["month"] for r in conn.execute("SELECT month FROM budget_ledger")] == ["2026-06"]


# --------------------------------------------------------------------------- pace (R6, sessions)
async def test_boundary_thursdays_are_half_sessions(gov):
    assert gov.trading_sessions(JUN_WINDOW) == (JUN_SESSIONS_THROUGH_WED, JUN_SESSIONS_TOTAL)
    # Calendar days would put Wed 06-17 at 6/7 of the window; sessions put it at 4.5/5 (§5.6).
    assert gov.pro_rata_to_date() == Decimal(100) * Decimal("4.5") / Decimal("5.0") == Decimal(90)


async def test_pace_on_a_normal_week_at_the_monday_mark(conn):
    g = governor_at(conn, datetime(2026, 6, 15, 10, 5, tzinfo=IST))
    # Thu½ + Fri + Mon = 2.5 of 5.0.
    assert g.trading_sessions() == (Decimal("2.5"), JUN_SESSIONS_TOTAL)
    assert g.pro_rata_to_date() == Decimal(50)


async def test_pace_never_exceeds_the_credit_on_the_closing_thursday_morning(conn):
    g = governor_at(conn, datetime(2026, 6, 18, 13, 59, tzinfo=IST))
    assert g.window_key() == JUN_WINDOW                        # Thursday morning is still the old week
    assert g.trading_sessions() == (JUN_SESSIONS_TOTAL, JUN_SESSIONS_TOTAL)
    assert g.pro_rata_to_date() == Decimal(100)


async def test_pace_on_a_week_whose_reset_thursday_is_a_holiday(conn):
    """Thu 2026-01-15 is an NSE holiday. The window still OPENS at 14:00 that day (wall-clock reset),
    but the day contributes 0 sessions, so the denominator is 4.5 rather than 5.0."""
    g = governor_at(conn, datetime(2026, 1, 19, 10, 5, tzinfo=IST))   # Mon of that window
    assert g.window_key() == "2026-01-15"
    # 01-15 holiday 0 + Fri 1 + Mon 1 elapsed; total adds Tue + Wed + Thu 01-22 (½).
    assert g.trading_sessions() == (Decimal("2.0"), Decimal("4.5"))
    assert g.pro_rata_to_date() == Decimal(100) * Decimal("2.0") / Decimal("4.5")


async def test_pro_rata_is_none_without_a_calendar(conn, clock, real_cfg, tmp_path):
    bare = NSECalendar(tmp_path / "no_calendar", clock, strict=False)
    gov = BudgetGovernor(conn, clock, bare, real_cfg)
    assert gov.trading_sessions() == (Decimal(0), Decimal(0))
    # None, NOT Decimal(0): a zero here is indistinguishable from "nothing should have been spent
    # yet" and every consumer would read an unmeasurable pace as a 100%-over-pace one.
    assert gov.pro_rata_to_date() is None


async def test_pro_rata_is_none_for_a_year_with_no_calendar_file(conn):
    """R6 against the REAL calendar directory rather than an empty one: ``config/calendar`` ships
    2024/2025/2026 only, so a governor clocked in 2027 has no sessions at all — the same unmeasurable
    state, reached the way it will actually be reached (the engine outliving its calendar files)."""
    g = governor_at(conn, datetime(2027, 1, 14, 20, 0, tzinfo=IST))   # a Thursday, no 2027.yaml
    assert g.trading_sessions() == (Decimal(0), Decimal(0))
    assert g.pro_rata_to_date() is None
    await spend(g, "1.00", agent="news_analyst")
    assert g.degrade_tier() == DegradeTier.DG0          # never over-pace on an unmeasurable pace
    assert g.can_invoke("intraday_analyst", "heartbeat").allowed is True


async def test_unmeasurable_pace_never_trips_a_pace_rung_without_a_calendar(conn, clock, tmp_path):
    """A pace of 0 is UNMEASURABLE, not "nothing should have been spent yet": `spend > 1.25 × 0` is
    `any spend at all`, so comparing against it would put a whole quota week at DG2 (heartbeats off,
    news batching coarsened) on one micro-dollar."""
    bare = NSECalendar(tmp_path / "no_calendar", clock, strict=False)
    gov = BudgetGovernor(conn, clock, bare, PINNED_CFG)
    await spend(gov, "0.30", agent="nightly_reviewer")
    assert gov.pro_rata_to_date() is None
    assert gov.degrade_tier() == DegradeTier.DG0
    assert gov.heartbeat_interval_min() == 20
    assert gov.news_batch_cadence_min() == 30
    assert gov.can_invoke("intraday_analyst", "heartbeat").allowed is True
    # The ABSOLUTE rungs still bind with no calendar — only the pace comparison is skipped.
    await spend(gov, "84.70", agent="nightly_reviewer")
    assert gov.degrade_tier() == DegradeTier.DG2


async def test_first_minute_of_a_holiday_opened_window_stays_dg0(conn):
    """The earliest instant the new window can be billed in: 14:01 IST on a reset Thursday that is an
    NSE holiday. Zero sessions have elapsed, so the pace is unmeasurable and a whole dollar — 1% of
    the credit, one minute in — must still read DG0 rather than "infinitely over pace"."""
    g = governor_at(conn, datetime(2026, 1, 15, 14, 1, tzinfo=IST))
    assert g.window_key() == "2026-01-15"
    assert g.trading_sessions() == (Decimal(0), Decimal("4.5"))
    assert g.pro_rata_to_date() is None
    await spend(g, "1.00", agent="news_analyst")
    assert g.degrade_tier() == DegradeTier.DG0
    assert g.heartbeat_interval_min() == 20
    assert g.news_batch_cadence_min() == 30
    assert g.can_invoke("intraday_analyst", "heartbeat").allowed is True


async def test_holiday_reset_thursday_evening_has_no_elapsed_session_and_stays_dg0(conn):
    """Thu 2026-01-15 is an NSE holiday, and it is also a reset Thursday: the window OPENS at 14:00 on
    a zero-session day, so ELAPSED is 0 while the denominator is 4.5. The news analyst's 19:00-22:00
    sweep is not window-gated and bills that same evening — it must not trip DG2 on its first call."""
    g = governor_at(conn, datetime(2026, 1, 15, 20, 0, tzinfo=IST))
    assert g.window_key() == "2026-01-15"
    assert g.trading_sessions() == (Decimal(0), Decimal("4.5"))
    assert g.pro_rata_to_date() is None
    await spend(g, "0.10", agent="news_analyst")
    assert g.degrade_tier() == DegradeTier.DG0
    assert g.heartbeat_interval_min() == 20
    assert g.prescreen_forward_cap() == 48
    assert g.news_batch_cadence_min() == 30
    assert g.can_invoke("intraday_analyst", "heartbeat").allowed is True


async def test_pace_rungs_resume_once_the_first_session_elapses(conn):
    """The skip is scoped to the unmeasurable case only: the Friday of that same holiday-opened window
    has 1.0 of 4.5 sessions elapsed, so the pace is real again and the DG1 rung binds."""
    g = governor_at(conn, datetime(2026, 1, 16, 20, 0, tzinfo=IST))
    assert g.trading_sessions() == (Decimal("1.0"), Decimal("4.5"))
    pace = Decimal(100) * Decimal(1) / Decimal("4.5")       # $22.222...
    assert g.pro_rata_to_date() == pace
    await spend(g, "24.44")                                 # just under 110% of pace ($24.4444…)
    assert g.degrade_tier() == DegradeTier.DG0
    await spend(g, "0.50")
    assert g.degrade_tier() == DegradeTier.DG1


# --------------------------------------------------------------------------- ladder boundaries
async def test_dg1_trips_on_pace_only(fri_gov):
    """The per-agent test was removed from the platform tier (2026-09-12): $33 booked entirely to an
    agent 175% over its own $12 allocation leaves the platform at DG0 until the PACE is exceeded."""
    assert fri_gov.pro_rata_to_date() == Decimal(30)
    await spend(fri_gov, "33.00")                            # exactly 110% of pace
    assert fri_gov.agent_degraded("weekly_researcher") is True
    assert fri_gov.degrade_tier() == DegradeTier.DG0
    await spend(fri_gov, "0.000001")                         # one micro-dollar past it
    assert fri_gov.degrade_tier() == DegradeTier.DG1


async def test_dg2_pace_boundary_is_strict(fri_gov):
    await spend(fri_gov, "37.50")                            # exactly 125% of pace
    assert fri_gov.degrade_tier() == DegradeTier.DG1
    await spend(fri_gov, "0.000001")
    assert fri_gov.degrade_tier() == DegradeTier.DG2


async def test_dg2_global_boundary_is_inclusive(gov):
    # At the Wednesday clock the pace trips sit at $99/$112.50, so the $85 global rule is the binding
    # one. Inclusive, matching DG3/DG4 — every global-percentage rung is `>=` (2026-09-12).
    await spend(gov, "84.999999")
    assert gov.degrade_tier() == DegradeTier.DG0
    await spend(gov, "0.000001")                             # exactly $85.00 => DG2
    assert gov.degrade_tier() == DegradeTier.DG2


async def test_dg3_and_dg4_boundaries_are_inclusive(gov):
    await spend(gov, "94.999999")
    assert gov.degrade_tier() == DegradeTier.DG2
    await spend(gov, "0.000001")                             # exactly $95.00 => DG3 (>=)
    assert gov.degrade_tier() == DegradeTier.DG3
    await spend(gov, "4.999999")
    assert gov.degrade_tier() == DegradeTier.DG3
    await spend(gov, "0.000001")                             # exactly $100.00 => DG4 (>=)
    assert gov.degrade_tier() == DegradeTier.DG4


async def test_billing_error_trips_dg4_from_dg0(gov):
    await spend(gov, "1.00")
    assert gov.degrade_tier() == DegradeTier.DG0
    gov.note_billing_error("credit_exhausted_http_402")
    assert gov.degrade_tier() == DegradeTier.DG4


# --------------------------------------------------------------------------- per-agent decoupling
async def test_agent_over_its_share_degrades_only_its_own_knobs(gov):
    """The 08-28/09-09 failure, inverted: an agent at 90% of its own allocation must throttle ITSELF
    and leave the platform (and every other agent's cadence) alone."""
    await spend(gov, "37.80", agent="intraday_analyst")      # 90% of the $42 allocation
    assert gov.degrade_tier() == DegradeTier.DG0             # platform untouched
    assert gov.agent_degraded("intraday_analyst") is True
    assert gov.prescreen_forward_cap() == 32                 # floor(48 × 0.67), not the old flat 4
    assert gov.heartbeat_interval_min() == 45
    assert gov.news_batch_cadence_min() == 30                # the news analyst is inside its share
    assert gov.can_invoke("intraday_analyst").allowed is True

    await spend(gov, "12.60", agent="news_analyst")          # 90% of the $14 allocation
    assert gov.agent_degraded("news_analyst") is True
    assert gov.news_batch_cadence_min() == 60
    assert gov.degrade_tier() == DegradeTier.DG0             # $50.40 total — still nowhere near a rung


async def test_agent_degraded_is_strict_and_unallocated_agents_are_never_degraded(gov):
    await spend(gov, "35.70", agent="intraday_analyst")      # exactly 85% of $42
    assert gov.agent_degraded("intraday_analyst") is False
    assert gov.prescreen_forward_cap() == 48
    await spend(gov, "0.000001", agent="intraday_analyst")
    assert gov.agent_degraded("intraday_analyst") is True
    # Nothing was budgeted to an unknown agent, so there is nothing for it to exceed.
    assert gov.agent_degraded("no_such_agent") is False


# --------------------------------------------------------------------------- call admission
async def test_heartbeats_are_off_at_dg2_but_not_dg1(fri_gov):
    await spend(fri_gov, "34.00")                            # $34 > $33 pace trip => DG1
    assert fri_gov.degrade_tier() == DegradeTier.DG1
    assert fri_gov.can_invoke("intraday_analyst", "heartbeat").allowed is True

    await spend(fri_gov, "4.00")                             # $38 > $37.50 => DG2
    assert fri_gov.degrade_tier() == DegradeTier.DG2
    decision = fri_gov.can_invoke("intraday_analyst", "heartbeat")
    assert decision == InvokeDecision(allowed=False, tier=DegradeTier.DG2, reason="DG2_heartbeats_off")
    # Only the heartbeat class is cut at DG2 — signal/position-event calls continue (§5.6).
    assert fri_gov.can_invoke("intraday_analyst", "signal_candidate").allowed is True


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


async def test_allocation_exhaustion_blocks_only_that_agent_at_100pct(gov):
    """The hard stop stays at 100% of the allocation and stays per-agent: the platform is DG0, every
    other agent is unaffected, and only the exhausted one is refused."""
    await spend(gov, "4.999999", agent="preopen_planner")
    assert gov.can_invoke("preopen_planner").allowed is True   # degraded, not exhausted
    assert gov.agent_degraded("preopen_planner") is True
    await spend(gov, "0.000001", agent="preopen_planner")      # the whole $5 allocation
    assert gov.degrade_tier() == DegradeTier.DG0
    assert gov.can_invoke("preopen_planner").reason == "agent_allocation_exhausted"
    assert gov.can_invoke("intraday_analyst").allowed is True


async def test_allocation_exhaustion_blocks_even_with_the_degrade_test_lifted(conn, clock, calendar):
    # agent_alloc_pct raised to 100: the agent is not "degraded" and the tier is DG0, and the hard
    # allocation stop still refuses it.
    cfg = dict(PINNED_CFG)
    cfg["degrade_ladder"] = {
        **PINNED_CFG["degrade_ladder"],
        "DG1": {"pro_rata_pct": 110, "agent_alloc_pct": 100, "degraded_forward_cap_frac": 0.67},
    }
    gov = BudgetGovernor(conn, clock, calendar, cfg)
    await spend(gov, "5.00", agent="preopen_planner")
    assert gov.degrade_tier() == DegradeTier.DG0
    assert gov.agent_degraded("preopen_planner") is False
    assert gov.can_invoke("preopen_planner") == InvokeDecision(
        allowed=False, tier=DegradeTier.DG0, reason="agent_allocation_exhausted"
    )


# --------------------------------------------------------------------------- behaviour knobs (§5.6)
async def test_behaviour_knobs_per_tier(fri_gov):
    knobs = lambda g: (  # noqa: E731 - three knobs read as one tuple per rung
        g.heartbeat_interval_min(), g.prescreen_forward_cap(), g.news_batch_cadence_min()
    )
    assert knobs(fri_gov) == (20, 48, 30)
    await spend(fri_gov, "34.00")                            # => DG1
    assert knobs(fri_gov) == (45, 32, 30)
    await spend(fri_gov, "4.00")                             # => DG2
    assert knobs(fri_gov) == (None, 32, 60)
    await spend(fri_gov, "58.00")                            # => DG3
    assert fri_gov.degrade_tier() == DegradeTier.DG3
    assert knobs(fri_gov) == (None, 32, 60)


async def test_degraded_forward_cap_is_proportional_and_floored(conn, clock, calendar):
    # (0, …, 0): an owner-set base of 0 means "forward nothing" — the max(1, …) floor must not re-open
    # it. config/settings.yaml parks max_per_strategy_day.orb at 0, so a zero cap is a live idiom.
    for base, frac, expected in [(48, 0.67, 32), (6, 0.67, 4), (1, 0.67, 1), (48, 0.5, 24), (0, 0.67, 0)]:
        cfg = dict(PINNED_CFG)
        cfg["agents"] = {"intraday_analyst": {"heartbeat_min": 20, "prescreen_cap_per_day": base}}
        cfg["degrade_ladder"] = {
            **PINNED_CFG["degrade_ladder"],
            "DG1": {"pro_rata_pct": 110, "agent_alloc_pct": 85, "degraded_forward_cap_frac": frac},
        }
        g = BudgetGovernor(conn, clock, calendar, cfg)
        assert g.prescreen_forward_cap() == base             # DG0, analyst inside its share
        await spend(g, "37.80", agent="intraday_analyst")
        assert g.prescreen_forward_cap() == expected, (base, frac)
        conn.execute("DELETE FROM budget_ledger")


@pytest.mark.parametrize("frac", [67, 6.7, 0, -0.5, 1.5, "two thirds"])
async def test_degraded_forward_cap_frac_out_of_range_fails_loud(conn, clock, calendar, frac):
    """It is the one ladder knob a wrong value makes the ladder spend MORE on: ``67`` (read as a
    percentage, like the pro_rata_pct/agent_alloc_pct keys beside it) would forward 48×67 a day."""
    cfg = {**PINNED_CFG, "degrade_ladder": {
        **PINNED_CFG["degrade_ladder"],
        "DG1": {"pro_rata_pct": 110, "agent_alloc_pct": 85, "degraded_forward_cap_frac": frac},
    }}
    with pytest.raises(ValueError, match="degraded_forward_cap_frac"):
        BudgetGovernor(conn, clock, calendar, cfg)


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
    # Amount-AGNOSTIC against the live owner-tunable file (credit is the owner's knob, D3/O6):
    # whatever the shipped credit is, spending exactly that much in one window must reach DG4.
    gov = BudgetGovernor.from_config(conn, clock, calendar)
    # Fully config-agnostic (owner tunes models AND rates): every priced model must price > 0.
    priced = load_yaml(config_dir() / "agents.yaml")["model_pricing_usd_per_mtok"]
    for name in priced:
        assert gov.price(name, TokenUsage(in_tokens=1_000_000, out_tokens=0)) > 0
    assert gov.degrade_tier() == DegradeTier.DG0
    await spend(gov, str(gov.credit()))
    assert gov.degrade_tier() == DegradeTier.DG4


# --------------------------------------------------------------------------- bus (§3.2.1, R8)
async def test_tier_change_publishes_exactly_once(conn, bus):
    seen: list[BudgetStateChanged] = []

    async def handler(event):
        seen.append(event)

    bus.subscribe(TOPIC_BUDGET_STATE, handler)
    gov = governor_at(conn, FRI_NOW, bus=bus)

    await spend(gov, "30.00", agent="intraday_analyst")      # still DG0 (pace trip is $33)
    assert seen == []
    await spend(gov, "4.00", agent="intraday_analyst")       # $34 > $33 => DG1
    assert len(seen) == 1
    await spend(gov, "1.00", agent="intraday_analyst")       # still DG1 — no second event
    assert len(seen) == 1
    await spend(gov, "3.00", agent="intraday_analyst")       # $38 > $37.50 => DG2
    assert len(seen) == 2

    assert (seen[0].old_tier, seen[0].new_tier) == (DegradeTier.DG0, DegradeTier.DG1)
    assert (seen[1].old_tier, seen[1].new_tier) == (DegradeTier.DG1, DegradeTier.DG2)
    assert seen[1].window_spend_usd == Decimal("38")
    assert seen[1].window_key == JUN_WINDOW
    assert seen[1].at == FRI_NOW


async def test_raise_billing_error_publishes(conn, clock, calendar, real_cfg, bus):
    seen: list[BudgetStateChanged] = []
    bus.subscribe(TOPIC_BUDGET_STATE, lambda e: _collect(seen, e))
    gov = BudgetGovernor(conn, clock, calendar, real_cfg, bus=bus)
    await spend(gov, "1.00")
    assert await gov.raise_billing_error("http_402") == DegradeTier.DG4
    assert [(e.old_tier, e.new_tier) for e in seen] == [(DegradeTier.DG0, DegradeTier.DG4)]
    assert seen[0].window_key == JUN_WINDOW


# --------------------------------------------------------------------------- window rollover
async def test_window_rollover_resets_the_tier_and_clears_the_dg4_latch(conn, bus):
    """The whole point of keying DG4 on the window: the quota refills at Thursday 14:00, so the latch
    must clear THEN, with no owner action."""
    now = [datetime(2026, 6, 17, 10, 5, tzinfo=IST)]
    clock = Clock(time_source=lambda: now[0])
    cal = NSECalendar(config_dir() / "calendar", clock, strict=False)
    seen: list[BudgetStateChanged] = []
    bus.subscribe(TOPIC_BUDGET_STATE, lambda e: _collect(seen, e))
    gov = BudgetGovernor(conn, clock, cal, PINNED_CFG, bus=bus)

    await spend(gov, "100.00")
    gov.note_billing_error("credit_exhausted")
    assert gov.degrade_tier() == DegradeTier.DG4
    seen.clear()

    now[0] = datetime(2026, 6, 18, 13, 59, tzinfo=IST)       # Thursday MORNING — still the old window
    assert gov.window_key() == JUN_WINDOW
    assert gov.degrade_tier() == DegradeTier.DG4

    now[0] = datetime(2026, 6, 18, 14, 0, tzinfo=IST)        # the reset instant
    assert gov.window_key() == "2026-06-18"
    assert gov.degrade_tier() == DegradeTier.DG0             # empty ledger in the new window; latch gone
    assert gov.window_spend() == Decimal(0)
    assert gov.window_spend(JUN_WINDOW) == Decimal("100.00")  # last week's history is untouched

    await spend(gov, "0.50")
    assert gov.degrade_tier() == DegradeTier.DG0
    # The recovery is a tier CHANGE and must alert like any other (R8).
    assert [(e.old_tier, e.new_tier) for e in seen] == [(DegradeTier.DG4, DegradeTier.DG0)]
    assert seen[0].window_key == "2026-06-18"


async def test_dg4_latch_is_keyed_on_its_own_window(conn):
    """The latch must not leak backwards onto a window the owner queries for history."""
    gov = governor_at(conn, datetime(2026, 6, 17, 10, 5, tzinfo=IST))
    gov.note_billing_error("credit_exhausted")
    assert gov.degrade_tier() == DegradeTier.DG4
    assert gov.degrade_tier("2026-06-04") == DegradeTier.DG0


def test_window_key_and_bounds_agree_across_a_whole_year(gov):
    """Every day of 2026 must land in a window whose bounds contain it — no gap, no overlap."""
    d = date(2026, 1, 1)
    while d < date(2027, 1, 1):
        for t in (time(0, 0), time(13, 59), time(14, 0), time(23, 59)):
            at = datetime.combine(d, t, tzinfo=IST)
            start, end = gov.window_bounds(_window_key(at, ANCHOR))
            assert start <= at < end, at
        d = date.fromordinal(d.toordinal() + 1)
