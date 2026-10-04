"""§6.1 ``rsi2`` — RSI(2) mean reversion with regime filter (swing, CNC, long-only).

PINNED rule sketch (§6.1 row 2): RSI(2) < ``rsi_entry`` on stocks above their 200-DMA in an
uptrending index; exit RSI(2) > ``rsi_exit`` or ``max_hold_days``; stop = ``stop_pct``
(gap-adjustment of the risk-at-stop is the gate's R2 job — ``raw_levels.stop`` is the plain
percentage level).

Documented choices where the sketch is silent:

* **Uptrending index** (task-pinned definition): index close > its 50-DMA AND the 50-DMA is rising
  over 20 sessions (``sma50[t] > sma50[t−20]``, strict — a flat index is NOT an uptrend). Evaluated
  on COMPLETED index sessions (``ctx.index_daily_closes``): the regime is a slow filter, deliberately
  not recomputed from a live index tick. A shut leg logs ``rsi2_regime_blocked`` once per session
  (:meth:`Rsi2Scanner._log_regime_blocked`) — silence and a shut filter used to look the same.
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
from collections.abc import Mapping
from datetime import date
from decimal import Decimal

from engine.core.log import get_logger
from engine.core.types import Bar
from engine.strategy.indicators import sma, wilder_rsi
from engine.strategy.scanners.base import Scanner, register
from engine.strategy.types import PendingSetup, ScanContext, SignalCandidate, round_to_tick

_log = get_logger("engine.strategy.scanners.rsi2")

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

    def __init__(self, params: Mapping[str, float] | None = None) -> None:
        super().__init__(params)
        #: Session date of the last ``rsi2_regime_blocked`` line. The ONLY mutable state on this
        #: scanner and it is log-only: :meth:`scan` returns the same candidates with or without it,
        #: so the §9.6 purity contract in ``scanners/base.py`` still holds for the outputs. The guard
        #: is per instance because the integrator builds one scanner per process, and `scan` runs on
        #: every 1m bar of every watchlist symbol — ~80,000 lines a session without it.
        self._regime_logged_on: date | None = None

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
        index_close = float(idx[-1])
        if not (index_close > now50 and now50 > then50):
            self._log_regime_blocked(bar, index_close, now50, then50)
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

    def _log_regime_blocked(self, bar: Bar, index_close: float, now50: float, then50: float) -> None:
        """One INFO per SESSION naming why the index regime leg refused (2026-09-12).

        NIFTY 50 has been under its 50-DMA every session since 08-27 and rsi2 has produced nothing
        since 08-28 with no log line anywhere saying which of the two legs was shut — "the filter is
        working" and "the scanner is broken" looked identical from the outside for two weeks.

        Session date = ``bar.ts_minute.date()`` (bars are IST-aware), never a Clock: the same rule
        the pre-screen uses for "today", so a §9.6 replay logs once per replayed session too.
        """
        session = bar.ts_minute.date()
        if self._regime_logged_on == session:
            return
        self._regime_logged_on = session
        _log.info(
            "rsi2_regime_blocked", d=session.isoformat(),
            index_close=round(index_close, 2), sma50=round(now50, 2), rising=now50 > then50,
            # First failing leg in the condition's own evaluation order; `rising` still discloses the
            # second when both are shut.
            blocked_leg="close_below_sma50" if not index_close > now50 else "sma50_not_rising",
            reason="index regime filter shut — rsi2 originates nothing today (§6.1 uptrend leg)",
        )

    def pending(self, bar: Bar, ctx: ScanContext) -> list[PendingSetup]:
        """The dip price at which RSI(2) would cross under ``rsi_entry`` today (§3.2.5 sweep).

        RSI(2) over ``completed closes + [P]`` is strictly increasing in ``P``, so the arm level is
        found by bisection on today's provisional close. The level is only reported while it stays
        ABOVE the 200-DMA it would produce — a dip deep enough to break the DMA filter cannot arm
        this strategy today. Regime filter and warm-up gates match :meth:`scan` exactly.
        """
        p = self.params

        idx = ctx.index_daily_closes
        if len(idx) < _INDEX_DMA + _RISING_LOOKBACK:
            return []
        sma50 = sma(idx, _INDEX_DMA)
        now50, then50 = float(sma50.iloc[-1]), float(sma50.iloc[-1 - _RISING_LOOKBACK])
        if math.isnan(now50) or math.isnan(then50):
            return []
        if not (float(idx[-1]) > now50 and now50 > then50):
            return []

        completed = [float(d.close) for d in ctx.daily_bars]
        if len(completed) + 1 < _STOCK_DMA:
            return []
        close_now = float(bar.close)

        def rsi_at(price: float) -> float:
            return float(wilder_rsi([*completed, price], _RSI_PERIOD).iloc[-1])

        current = rsi_at(close_now)
        if math.isnan(current) or current < p["rsi_entry"]:
            return []  # already armed — scan()'s live-signal territory (or NaN: fail to zero)

        # Bisect the largest P with RSI(2) < rsi_entry in (0, close_now).
        lo, hi = close_now * 0.01, close_now
        if not rsi_at(lo) < p["rsi_entry"]:
            return []  # even a 99% collapse would not tip it (degenerate history) — nothing to arm
        for _ in range(50):
            mid = (lo + hi) / 2.0
            if rsi_at(mid) < p["rsi_entry"]:
                lo = mid
            else:
                hi = mid
        trigger = lo

        # The dip must still clear the 200-DMA computed WITH the dip value, or the DMA filter breaks
        # before the RSI condition can arm.
        sma200_at = float(sma([*completed, trigger], _STOCK_DMA).iloc[-1])
        if math.isnan(sma200_at) or not trigger > sma200_at:
            return []

        return [
            PendingSetup(
                strategy_id=self.strategy_id, symbol=bar.symbol, side="BUY", style=self.style,
                trigger_price=round_to_tick(trigger), arms_when="below", last_price=bar.close,
                stop_price=round_to_tick(trigger * (1.0 - p["stop_pct"] / 100.0)),
                target_price=None,
                exit_rule=f"exit on RSI(2) > {p['rsi_exit']:g} or after {p['max_hold_days']:g} sessions",
                condition=(
                    f"daily close at/below the level tips RSI(2) under {p['rsi_entry']:g} "
                    "while holding the 200-DMA; index uptrend filter currently PASSING"
                ),
            )
        ]
