"""The one NIFTY 50 regime predicate (rsi2's index leg, C2's rule, the brk20 sweep gate)."""

from __future__ import annotations

import math
from datetime import date

from engine.strategy.regime import index_regime

D = date(2026, 6, 16)
RISING = [1000.0 + i for i in range(70)]


def test_up_when_close_above_rising_sma():
    r = index_regime(RISING, D)
    assert r.up and r.blocked_leg is None
    assert (r.close, r.sma, r.sma_then, r.last_d) == (1069.0, 1044.5, 1024.5, D)


def test_blocked_leg_close_below_sma50():
    r = index_regime([*RISING[:-1], 1000.0], D)
    assert not r.up and r.blocked_leg == "close_below_sma50"


def test_close_equal_sma_is_not_up():
    r = index_regime([1000.0] * 70, D)
    assert not r.up and r.blocked_leg == "close_below_sma50"


def test_flat_sma_is_not_up():
    r = index_regime([100.0 + (i % 20) for i in range(80)], D)   # SMA50 equal 20 sessions apart
    assert r.close > r.sma and not r.up and r.blocked_leg == "sma50_not_rising"


def test_insufficient_history_is_none():
    assert index_regime(RISING[:69], D) is None


def test_nan_is_none():
    assert index_regime([*RISING[:-1], math.nan], D) is None
    assert index_regime([*RISING[:30], math.nan, *RISING[31:]], D) is None   # inside the SMA window
