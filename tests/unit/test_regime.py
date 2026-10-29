"""The one NIFTY 50 regime predicate (rsi2's index leg, C2's rule, the brk20 sweep gate)."""

from __future__ import annotations

import math
from datetime import date, timedelta

import pandas as pd
import pytest

from engine.core.calendar import NSECalendar
from engine.core.config import config_dir
from engine.strategy.regime import index_regime, session_regime

D = date(2026, 6, 16)
RISING = [1000.0 + i for i in range(70)]


@pytest.fixture
def live_cal(clock) -> NSECalendar:
    return NSECalendar(config_dir() / "calendar", clock, strict=True, include_special=False)


def _frame(closes: list[float], last: date) -> pd.DataFrame:
    """Weekday bars, then one bar dated ``last`` (which may be a Sunday muhurat session)."""
    days = [*pd.bdate_range(end=last - timedelta(days=1), periods=len(closes) - 1), pd.Timestamp(last)]
    return pd.DataFrame({"close": closes}, index=pd.DatetimeIndex(days))


def test_up_when_close_above_rising_sma():
    r = index_regime(RISING, D)
    assert r.up and r.blocked_leg is None
    assert (r.close, r.sma, r.sma_then, r.last_d) == (1069.0, 1044.5, 1024.5, D)


def test_blocked_leg_close_below_sma50():
    r = index_regime([*RISING[:-1], 1000.0], D)
    assert not r.up and r.blocked_leg == "close_below_sma50"


def test_close_equal_sma_is_not_up():
    r = index_regime([*RISING[:-1], 1044.0], D)                  # close == SMA50 == 1044.0, SMA rising
    assert r.close == r.sma and r.rising
    assert not r.up and r.blocked_leg == "close_below_sma50"


def test_flat_sma_is_not_up():
    r = index_regime([100.0 + (i % 20) for i in range(80)], D)   # SMA50 equal 20 sessions apart
    assert r.close > r.sma and not r.up and r.blocked_leg == "sma50_not_rising"


def test_insufficient_history_is_none():
    assert index_regime(RISING[:69], D) is None
    assert index_regime([], D) is None


def test_nan_is_none():
    assert index_regime([*RISING[:-1], math.nan], D) is None
    assert index_regime([*RISING[:30], math.nan, *RISING[31:]], D) is None   # inside the SMA window


def test_session_regime_reads_the_previous_session_not_yesterday(live_cal):
    r, reason = session_regime(_frame(RISING, date(2026, 6, 19)), live_cal, date(2026, 6, 22))   # Fri, Mon
    assert reason is None and r.up and r.last_d == date(2026, 6, 19)


def test_session_regime_bar_older_than_the_previous_session_is_missing(live_cal):
    assert session_regime(_frame(RISING, date(2026, 6, 18)), live_cal, date(2026, 6, 22)) == (
        None, "index_bar_missing")


def test_session_regime_counts_a_completed_session_the_calendar_treats_as_closed(live_cal):
    muhurat = date(2026, 11, 8)                                    # Sunday; prev(Mon 11-09) is Fri 11-06
    r, reason = session_regime(_frame(RISING, muhurat), live_cal, date(2026, 11, 9))
    assert reason is None and r.last_d == muhurat
