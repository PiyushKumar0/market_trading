"""``scripts/g2_evidence.py`` C5 block — it must measure the SAME quota window the governor does.

The script is deliberately stdlib-only (its module docstring: no engine import, so the G2 evidence
generator can never perturb the live engine it reports on), which means it REIMPLEMENTS the governor's
window arithmetic instead of importing it. An unpinned reimplementation is drift waiting to happen —
and the failure mode is not cosmetic: the C5 row is a Phase-2 gate verdict, and keying it a period off
is exactly what printed the false "~85% of allocation" signal that the weekly re-scope exists to
retire. So this pins the two implementations against each other across a whole year of instants, and
pins the C5 block itself to the window containing ``--to``.
"""

from __future__ import annotations

import importlib.util
from datetime import UTC, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.config import config_dir, load_yaml
from engine.intelligence.governor import (
    BudgetGovernor,
    TokenUsage,
    WindowAnchor,
    _window_bounds,
    _window_key,
)

REPO = Path(__file__).resolve().parents[2]
ANCHOR = WindowAnchor(3, time(14, 0))           # Thursday (Mon=0) 14:00 IST — agents.yaml's default


@pytest.fixture(scope="module")
def g2():
    """Load the script by path: ``scripts/`` is not a package and must not become one."""
    spec = importlib.util.spec_from_file_location("g2_evidence", REPO / "scripts" / "g2_evidence.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_script_window_matches_the_governor_across_a_whole_year(g2):
    """Every 5 hours through 2026 — 1,752 instants, so every weekday, both sides of the Thursday
    14:00 reset, and the DST-free +05:30 offset are all walked."""
    at = datetime(2026, 1, 1, 0, 0, tzinfo=IST)
    end = datetime(2027, 1, 1, 0, 0, tzinfo=IST)
    while at < end:
        key, start, stop = g2.quota_window(at, "thursday", "14:00")
        assert key == _window_key(at, ANCHOR), at
        assert (start, stop) == _window_bounds(key, ANCHOR), at
        at += timedelta(hours=5)


def test_script_window_rejects_a_naive_stamp_like_the_governor_does(g2):
    """``astimezone`` on a naive value reads it as SYSTEM-local, so on a non-IST host the same instant
    would land in a different week here than in the ledger the numbers come from."""
    with pytest.raises(ValueError, match="tz-aware"):
        g2.quota_window(datetime(2026, 9, 10, 13, 59), "thursday", "14:00")


def test_script_window_normalises_a_non_ist_stamp(g2):
    """Thu 18:00 IST is 12:30 UTC — still Thursday, but BEFORE 14:00. Read in UTC the same instant
    would key the previous week; normalising to IST first is what makes the two agree."""
    ist = datetime(2026, 9, 10, 18, 0, tzinfo=IST)
    assert g2.quota_window(ist, "thursday", "14:00")[0] == "2026-09-10"
    assert g2.quota_window(ist.astimezone(UTC), "thursday", "14:00")[0] == "2026-09-10"
    assert _window_key(ist.astimezone(UTC), ANCHOR) == "2026-09-10"


async def test_criterion_budget_covers_only_the_quota_week_containing_at(conn, g2):
    """Rows on both sides of a 14:00 reset, plus one a week later: the C5 block must sum ONLY the
    window that contains its instant. Half-open — a call made exactly at 14:00 is in the NEW week."""
    cfg = load_yaml(config_dir() / "agents.yaml")

    def gov_at(when: datetime) -> BudgetGovernor:
        clock = Clock(time_source=lambda: when)
        return BudgetGovernor(conn, clock, NSECalendar(config_dir() / "calendar", clock, strict=False), cfg)

    # haiku-4.5 input is $1/MTok => 1 input token is exactly 1 micro-dollar.
    async def book(agent: str, usd: str, when: datetime) -> None:
        await gov_at(when).record(
            agent, "haiku-4.5", TokenUsage(in_tokens=int(Decimal(usd) * 1_000_000), out_tokens=0), at=when
        )

    await book("news_analyst", "9.00", datetime(2026, 9, 10, 13, 59, 59, tzinfo=IST))   # previous week
    await book("news_analyst", "2.00", datetime(2026, 9, 10, 14, 0, tzinfo=IST))        # the reset itself
    await book("intraday_analyst", "3.50", datetime(2026, 9, 12, 11, 0, tzinfo=IST))
    await book("sdk_smoke", "0.25", datetime(2026, 9, 15, 9, 0, tzinfo=IST))
    await book("news_analyst", "40.00", datetime(2026, 9, 17, 14, 0, tzinfo=IST))       # the next week

    bud = g2.criterion_budget(conn, datetime(2026, 9, 12, 11, 40, tzinfo=IST))

    assert bud["window"] == "2026-09-10"
    assert (bud["window_start"], bud["window_end"]) == (
        "2026-09-10T14:00:00+05:30", "2026-09-17T14:00:00+05:30"
    )
    assert bud["total_usd"] == pytest.approx(5.75)                      # 2.00 + 3.50 + 0.25
    assert bud["per_agent"]["news_analyst"]["spend_usd"] == pytest.approx(2.00)
    assert bud["per_agent"]["intraday_analyst"]["spend_usd"] == pytest.approx(3.50)
    # An agent that billed with no allocation still gets a row, with no percentage to divide by.
    assert bud["per_agent"]["sdk_smoke"]["alloc_usd"] is None
    assert bud["per_agent"]["sdk_smoke"]["pct_of_alloc"] is None
    # The denominator is the WEEKLY allocation from the same file the engine reads — one period on
    # both sides of the ratio is the whole point of the C5 fix.
    alloc = cfg["budget_allocations_usd"]["news_analyst"]
    assert bud["per_agent"]["news_analyst"]["alloc_usd"] == pytest.approx(float(alloc))
    assert bud["per_agent"]["news_analyst"]["pct_of_alloc"] == pytest.approx(2.00 / float(alloc) * 100)


async def test_criterion_budget_agrees_with_the_governor_on_the_same_ledger(conn, g2):
    """The report and the ladder must not disagree about what this week cost."""
    when = datetime(2026, 9, 12, 11, 40, tzinfo=IST)
    clock = Clock(time_source=lambda: when)
    gov = BudgetGovernor(
        conn, clock, NSECalendar(config_dir() / "calendar", clock, strict=False),
        load_yaml(config_dir() / "agents.yaml"),
    )
    await gov.record("news_analyst", "haiku-4.5", TokenUsage(in_tokens=7_250_000, out_tokens=0), at=when)

    bud = g2.criterion_budget(conn, when)
    assert bud["window"] == gov.window_key(when)
    assert bud["total_usd"] == pytest.approx(float(gov.window_spend()))
    assert bud["weekly_credit_usd"] == pytest.approx(float(gov.credit()))
