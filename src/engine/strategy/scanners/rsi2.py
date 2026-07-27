"""§6.1 ``rsi2`` — RSI(2) mean reversion with regime filter (swing, CNC, long-only).

PINNED rule sketch (§6.1 row 2): RSI(2) < ``rsi_entry`` on stocks above their 200-DMA in an
uptrending index; exit RSI(2) > ``rsi_exit`` or ``max_hold_days``; stop = ``stop_pct``
(gap-adjustment of the risk-at-stop is the gate's R2 job — ``raw_levels.stop`` is the plain
percentage level).

Documented choices where the sketch is silent:

* **Uptrending index** (task-pinned definition): index close > its 50-DMA AND the 50-DMA is rising
  over 20 sessions (``sma50[t] > sma50[t−20]``, strict — a flat index is NOT an uptrend). Evaluated
  on COMPLETED index sessions (``ctx.index_daily_closes``): the regime is a slow filter, deliberately
  not recomputed from a live index tick.
* **Daily series = completed daily closes + today's provisional close** (the scanned 1m bar's close):
  RSI(2) and the 200-DMA both include today's provisional value, so the signal reflects the price at
  which the entry would actually happen. Needs 199 completed dailies (200-DMA) — fewer ⇒ fail to
  zero (warm-up, §7.1 ``warmup_ready``).
* **Long-only**: mean-reversion buys the dip; CNC cannot short overnight (C5). No SELL variant.
* **Exit levels are informational** — ``rsi_exit`` / ``max_hold_days`` ride along in ``self.params``
  for the position manager / Tier-1 context; the pinned §3.2.5 candidate carries no exit-rule fields,
  so ``raw_levels.target`` is ``None`` (never a synthetic invention).
* **Score** = ``(rsi_entry − rsi) / rsi_entry`` clamped to [0, 1] — deeper oversold ⇒ stronger.
"""

from __future__ import annotations

import math
from decimal import Decimal

from engine.core.types import Bar
from engine.strategy.indicators import sma, wilder_rsi
from engine.strategy.scanners.base import Scanner, register
from engine.strategy.types import ScanContext, SignalCandidate, round_to_tick

_RSI_PERIOD = 2        # §6.1: RSI(2)
_STOCK_DMA = 200       # §6.1: stock above 200-DMA
_INDEX_DMA = 50        # task-pinned uptrend definition
_RISING_LOOKBACK = 20  # 50-DMA rising over 20 sessions


@register
class Rsi2Scanner(Scanner):
    strategy_id = "rsi2"
    style = "swing"

    DEFAULT_PARAMS = {           # §6.3 envelope defaults
        "rsi_entry": 10,
        "rsi_exit": 65,          # informational exit rule (position manager), not a raw_level
        "stop_pct": 4.0,
        "max_hold_days": 10,     # informational exit rule
    }

    def scan(self, bar: Bar, ctx: ScanContext) -> list[SignalCandidate]:
        p = self.params

        # ---- index regime filter (completed sessions only).
        idx = ctx.index_daily_closes
        if len(idx) < _INDEX_DMA + _RISING_LOOKBACK:
            return []
        sma50 = sma(idx, _INDEX_DMA)
        now50, then50 = float(sma50.iloc[-1]), float(sma50.iloc[-1 - _RISING_LOOKBACK])
        if math.isnan(now50) or math.isnan(then50):
            return []
        if not (float(idx[-1]) > now50 and now50 > then50):
            return []

        # ---- stock series: completed dailies + today's provisional close.
        closes = [float(d.close) for d in ctx.daily_bars] + [float(bar.close)]
        sma200 = float(sma(closes, _STOCK_DMA).iloc[-1])
        if math.isnan(sma200) or not closes[-1] > sma200:
            return []
        rsi = float(wilder_rsi(closes, _RSI_PERIOD).iloc[-1])
        if math.isnan(rsi) or not rsi < p["rsi_entry"]:
            return []

        entry = bar.close
        stop = round_to_tick(entry * (Decimal(1) - Decimal(str(p["stop_pct"])) / Decimal(100)))
        return [
            self._candidate(
                bar=bar,
                ctx=ctx,
                side="BUY",
                entry=entry,
                stop=stop,
                target=None,     # exit is RSI(2) > rsi_exit or max_hold_days — informational (§6.1)
                score=(p["rsi_entry"] - rsi) / p["rsi_entry"],
            )
        ]
