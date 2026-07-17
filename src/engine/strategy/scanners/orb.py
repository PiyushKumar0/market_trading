"""§6.1 ``orb`` — opening-range breakout (intraday, MIS).

PINNED rule sketch (§6.1 row 1, **v2 2026-07-12**): range = first ``orb_minutes`` after session open
(09:15 regular; ``ctx.session_open`` is shortened-session aware), **with the A14 auction open folded
into the range seed (09:15) bar** — the pre-open auction print can sit outside that bar's
tick-derived high/low, so the seed bar's effective range is
``[min(low, auction_open), max(high, auction_open)]``. Entry on a 1m close STRICTLY beyond the range
with ``volume ≥ vol_mult × median(volume over the 20 session bars immediately BEFORE the trigger
bar)``; stop anchored at the **opposite range edge**: risk = ``stop_range_frac × (entry − range_low)``
for BUY (mirror ``× (range_high − entry)`` for SELL); target = ``rr_target × risk`` where risk = the
(unrounded) stop distance; suppressed on ``flagged_instrument_days`` (volume breakouts on
bulk/block-deal days are untrustworthy, §4.4 job 9).

v2 note: v1 sized the stop as ``stop_atr_mult × ATR(14, 1m)`` — a noise-scale unit (~0.14% of price
median) below the ₹20k MIS cost floor (round-trip ≈ 40–90% of the stop), which made negative net
expectancy structural (2026-07-12 CPCV 0/15). The far range edge is the structural invalidation
level: a breakout that trades back through it is dead. Sub-cost-floor candidates (risk < 2×
round-trip breakeven, C3) are the §7.1 cost gate's suppression, NOT re-checked here.

Documented choices where the sketch is silent:

* **Entry timestamp** = the trigger bar's CLOSE (``ts_minute + 1min``) — the moment the signal is
  actionable. It must lie inside 09:30–14:30 (plan base window, both ends inclusive) INTERSECTED
  with the owner trade window from ``ctx.trade_window`` (§7.1 — already session-clamped by
  ``NSECalendar.trade_window``). Missing window context ⇒ fail to zero. The MIS square-off buffer /
  ``min_residual_window`` residual check is the gate's job (§7.1), not repeated here.
* **Volume median excludes the trigger bar** (its own breakout volume must not inflate the baseline)
  and needs 20 completed prior session bars (the §7.1 ``warmup_ready`` gate guarantees this
  lookback live).
* **Both directions are emitted** (BUY above the range, SELL below): the sketch says "beyond range".
  Whether SELL (MIS short) candidates are tradeable is downstream policy (§1.4.9 shorts gate / risk
  gate) — the baseline records both for §6.1 attribution.
* **Score** = ``min(1, volume_ratio / (2 × vol_mult))`` — 0.5 exactly at the volume threshold, 1.0
  at twice the threshold. Informational only (§3.2.5).
"""

from __future__ import annotations

import math
from datetime import datetime, time, timedelta

from engine.core.types import Bar
from engine.strategy.indicators import rolling_median_volume
from engine.strategy.scanners.base import Scanner, register
from engine.strategy.types import ScanContext, Side, SignalCandidate, round_to_tick

#: Plan-pinned base entry window (§6.1: "entries 09:30–14:30"), intersected with the owner window.
BASE_ENTRY_START = time(9, 30)
BASE_ENTRY_END = time(14, 30)

_MEDIAN_WINDOW = 20   # §6.1: "20-bar median"


@register
class OrbScanner(Scanner):
    strategy_id = "orb"
    style = "intraday"

    DEFAULT_PARAMS = {           # §6.3 envelope defaults
        "orb_minutes": 30,
        "vol_mult": 1.5,
        "stop_range_frac": 1.0,  # v2 2026-07-12: stop at the opposite range edge (replaces stop_atr_mult)
        "rr_target": 1.5,
    }

    def scan(self, bar: Bar, ctx: ScanContext) -> list[SignalCandidate]:
        if ctx.flagged:
            return []  # bulk/block-deal day — volume breakout suppressed (§6.1)
        if ctx.session_open is None or ctx.trade_window is None:
            return []  # fail to zero on missing context (warm-up / provider gap)
        bars = ctx.intraday_bars
        if not bars or bars[-1].ts_minute != bar.ts_minute:
            return []

        p = self.params
        range_end = ctx.session_open + timedelta(minutes=int(p["orb_minutes"]))
        if bar.ts_minute < range_end:
            return []  # opening range not complete yet

        # ---- entry-time window: 09:30–14:30 ∩ owner trade window, on the bar-close instant.
        entry_dt = bar.ts_minute + timedelta(minutes=1)
        day, tz = bar.ts_minute.date(), bar.ts_minute.tzinfo
        lo_edge = max(datetime.combine(day, BASE_ENTRY_START, tzinfo=tz), ctx.trade_window[0])
        hi_edge = min(datetime.combine(day, BASE_ENTRY_END, tzinfo=tz), ctx.trade_window[1])
        if not (lo_edge <= entry_dt <= hi_edge):
            return []

        # ---- opening range, auction-open-seeded (A14).
        range_bars = [b for b in bars if ctx.session_open <= b.ts_minute < range_end]
        if not range_bars:
            return []
        range_high = -math.inf
        range_low = math.inf
        for b in range_bars:
            hi, lo = float(b.high), float(b.low)
            if b.ts_minute == ctx.session_open and b.auction_open is not None:
                hi = max(hi, float(b.auction_open))
                lo = min(lo, float(b.auction_open))
            range_high = max(range_high, hi)
            range_low = min(range_low, lo)

        close_f = float(bar.close)
        side: Side
        if close_f > range_high:
            side = "BUY"
        elif close_f < range_low:
            side = "SELL"
        else:
            return []

        # ---- volume confirmation vs the 20 bars immediately before the trigger.
        prior = bars[:-1]
        if len(prior) < _MEDIAN_WINDOW:
            return []
        med = float(rolling_median_volume([b.volume for b in prior], _MEDIAN_WINDOW).iloc[-1])
        if math.isnan(med) or med <= 0.0:
            return []
        vol_ratio = float(bar.volume) / med
        if vol_ratio < p["vol_mult"]:
            return []

        # ---- levels anchored at the OPPOSITE range edge (v2 2026-07-12): the far edge is the
        # structural invalidation level. Sub-cost-floor risk (C3) is the §7.1 cost gate's job.
        if side == "BUY":
            risk = p["stop_range_frac"] * (close_f - range_low)
        else:
            risk = p["stop_range_frac"] * (range_high - close_f)
        if not math.isfinite(risk) or risk <= 0.0:
            return []  # degenerate range (paranoia: a strict breakout implies positive distance)
        sign = 1.0 if side == "BUY" else -1.0
        stop = round_to_tick(close_f - sign * risk)
        target = round_to_tick(close_f + sign * p["rr_target"] * risk)

        return [
            self._candidate(
                bar=bar,
                ctx=ctx,
                side=side,
                entry=bar.close,
                stop=stop,
                target=target,
                score=vol_ratio / (2.0 * p["vol_mult"]),
            )
        ]
