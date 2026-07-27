"""§6.1 ``trend`` — 20/50 EMA cross with ADX filter (position, CNC, long-only).

PINNED rule sketch (§6.1 row 3): 20/50 EMA cross with ADX > ``adx_min``; trail =
``trail_atr_mult × ATR(14, 1d)``; max hold 120 td (the §7.1 ``max_holding`` position cap —
gate-enforced, not a scanner output).

Documented choices where the sketch is silent:

* **Cross = golden cross only** (EMA20 crossing ABOVE EMA50): ``ema20[t−1] ≤ ema50[t−1]`` and
  ``ema20[t] > ema50[t]``. The bearish cross would be a CNC short — not tradeable (C5/§1.4.9), so
  it is not emitted. Long-only, like every CNC baseline at introduction.
* **EMA series = completed daily closes + today's provisional close** (the scanned 1m bar's close),
  so the cross is actionable the day it forms; ``t−1`` is the same series without the provisional
  point. ADX(14) and ATR(14, 1d) are computed on COMPLETED daily bars only — today's daily
  high/low/close do not exist intraday (a 1m bar is not a day bar); the trigger's trend-strength and
  trail distance come from finished sessions.
* **Warm-up**: ≥ 60 completed dailies required (EMA50 needs ~3×period ≈ 150 for textbook-exact
  values but is seeded deterministically from the series start — see ``indicators`` module contract;
  ADX(14) itself needs 28 rows; the 60 floor keeps early-history EMA seeding noise out of live
  signals; ``warmup_ready``/§2.6 backfill supplies 1–2 y of dailies in practice).
* **ADX boundary is strict**: ``adx > adx_min``, not ``>=``.
* **Score** = ``(adx − adx_min) / (50 − adx_min)`` clamped to [0, 1] — ADX 50 (a very strong trend)
  or higher scores 1.0. Informational only.
"""

from __future__ import annotations

import math

from engine.core.types import Bar
from engine.strategy.indicators import ema, wilder_adx, wilder_atr
from engine.strategy.scanners.base import Scanner, register
from engine.strategy.types import ScanContext, SignalCandidate, round_to_tick

_EMA_FAST = 20         # §6.1: 20/50 EMA cross
_EMA_SLOW = 50
_ATR_PERIOD = 14       # §6.1: ATR(14, 1d)
_ADX_PERIOD = 14
_MIN_DAILIES = 60      # warm-up floor (see module docstring)
_ADX_SCORE_CEIL = 50.0


@register
class TrendScanner(Scanner):
    strategy_id = "trend"
    style = "position"

    DEFAULT_PARAMS = {           # §6.3 envelope defaults
        "adx_min": 20,
        "trail_atr_mult": 2.5,
    }

    def scan(self, bar: Bar, ctx: ScanContext) -> list[SignalCandidate]:
        dailies = ctx.daily_bars
        if len(dailies) < _MIN_DAILIES:
            return []
        p = self.params

        closes = [float(d.close) for d in dailies] + [float(bar.close)]
        fast = ema(closes, _EMA_FAST)
        slow = ema(closes, _EMA_SLOW)
        crossed = fast.iloc[-2] <= slow.iloc[-2] and fast.iloc[-1] > slow.iloc[-1]
        if not crossed:
            return []

        highs = [d.high for d in dailies]
        lows = [d.low for d in dailies]
        dcloses = [d.close for d in dailies]
        adx = float(wilder_adx(highs, lows, dcloses, _ADX_PERIOD).iloc[-1])
        if math.isnan(adx) or not adx > p["adx_min"]:
            return []
        atr = float(wilder_atr(highs, lows, dcloses, _ATR_PERIOD).iloc[-1])
        if math.isnan(atr) or atr <= 0.0:
            return []

        entry = bar.close
        stop = round_to_tick(float(entry) - p["trail_atr_mult"] * atr)  # initial trail level
        denom = max(_ADX_SCORE_CEIL - p["adx_min"], 1.0)
        return [
            self._candidate(
                bar=bar,
                ctx=ctx,
                side="BUY",
                entry=entry,
                stop=stop,
                target=None,     # trail-managed; no fixed target in the §6.1 sketch
                score=(adx - p["adx_min"]) / denom,
            )
        ]
