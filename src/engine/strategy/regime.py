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
from typing import TYPE_CHECKING

from engine.strategy.indicators import sma

if TYPE_CHECKING:  # pragma: no cover - typing only
    import pandas as pd

    from engine.core.calendar import NSECalendar

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
    def blocked_leg(self) -> str | None:
        """The first failing leg in evaluation order; None when up."""
        if not self.close > self.sma:
            return "close_below_sma50"
        if not self.rising:
            return "sma50_not_rising"
        return None

    @property
    def rising(self) -> bool:
        return self.sma > self.sma_then

    @property
    def up(self) -> bool:
        return self.blocked_leg is None


def index_regime(closes: Sequence[float | Decimal], last_d: date | None) -> IndexRegime | None:
    """None below ``INDEX_SMA + RISING_LOOKBACK`` closes or on a NaN: the regime is undefined, never guessed."""
    if len(closes) < INDEX_SMA + RISING_LOOKBACK:
        return None
    s = sma(closes, INDEX_SMA)
    r = IndexRegime(float(closes[-1]), float(s.iloc[-1]), float(s.iloc[-1 - RISING_LOOKBACK]), last_d)
    if math.isnan(r.sma) or math.isnan(r.sma_then):   # a NaN close is inside the SMA50 window
        return None
    return r


def session_regime(
    frame: pd.DataFrame, calendar: NSECalendar, today: date
) -> tuple[IndexRegime | None, str | None]:
    """The regime from the index's ``get_bars_1d_frame`` read through the day before ``today``, and
    why it is None.

    The last bar must be no older than ``calendar.previous_trading_day(today)`` (C2: no bar dated y is
    undefined); a later bar before today, a session the live calendar treats as closed (muhurat), is a
    completed session and counts. Reason: None | ``"index_bar_missing"`` | ``"undefined"``."""
    if not len(frame) or frame.index[-1].date() < calendar.previous_trading_day(today):
        return None, "index_bar_missing"
    r = index_regime(frame["close"].tolist(), frame.index[-1].date())
    return r, None if r is not None else "undefined"


__all__ = ["INDEX_SMA", "INDEX_SYMBOL", "RISING_LOOKBACK", "IndexRegime", "index_regime", "session_regime"]
