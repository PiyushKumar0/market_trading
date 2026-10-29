"""The NIFTY 50 index regime: close > SMA50 > SMA50 twenty sessions earlier, all strict.

ONE predicate for rsi2's uptrend leg and the brk20 sweep's C2 gate. ``scripts/backtest_regime_filter.py``
keeps its own copy (a completed registered study, byte-identical); ``tests/offline/test_research_filters.py``
pins the two to agree.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from engine.strategy.indicators import sma

INDEX_SYMBOL = "NIFTY 50"
INDEX_SMA = 50
RISING_LOOKBACK = 20


@dataclass(frozen=True)
class IndexRegime:
    """The regime over completed closes through ``last_d`` (None where the caller holds undated closes)."""

    close: float
    sma: float
    sma_then: float
    last_d: date | None

    @property
    def up(self) -> bool:
        return self.close > self.sma > self.sma_then

    @property
    def blocked_leg(self) -> str | None:
        """The first failing leg in evaluation order; None when up."""
        if not self.close > self.sma:
            return "close_below_sma50"
        if not self.sma > self.sma_then:
            return "sma50_not_rising"
        return None


def index_regime(closes: Sequence[float | Decimal], last_d: date | None) -> IndexRegime | None:
    """None below ``INDEX_SMA + RISING_LOOKBACK`` closes or on a NaN: the regime is undefined, never guessed."""
    if len(closes) < INDEX_SMA + RISING_LOOKBACK:
        return None
    s = sma(closes, INDEX_SMA)
    r = IndexRegime(float(closes[-1]), float(s.iloc[-1]), float(s.iloc[-1 - RISING_LOOKBACK]), last_d)
    if math.isnan(r.close) or math.isnan(r.sma) or math.isnan(r.sma_then):
        return None
    return r


__all__ = ["INDEX_SMA", "INDEX_SYMBOL", "RISING_LOOKBACK", "IndexRegime", "index_regime"]
