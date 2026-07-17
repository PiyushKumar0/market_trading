"""§6.1 ``mom`` — 4-week cross-sectional momentum (swing, CNC, long-only).

PINNED rule sketch (§6.1 row 4): 4-week momentum rank within the universe, top ``top_n``, hold
``rebalance_days``, ex-date aware (A12). ``top_n`` defaults to 2 (C4: few, larger positions — never
₹2,000 × 10 scrips).

Inputs arrive pre-computed on the context (the provider computes ``indicators.momentum`` over the
same ``bars_1d`` history for EVERY universe symbol — a per-symbol scanner cannot see the cross
section): ``ctx.momentum_by_symbol`` (symbol → 4-week fractional return; NaN = insufficient
history ⇒ unrankable), ``ctx.mom_sessions_since_rebalance`` (trading sessions since the last
rebalance; ``None`` ⇒ never ⇒ due now), ``ctx.upcoming_ex_dates`` (this symbol's known upcoming
corp-action ex-dates, A12).

Documented choices where the sketch is silent:

* **Rebalance cadence** is denominated in TRADING SESSIONS (§6.3: ``rebalance_days`` upper bound 20
  = the §7.1 swing holding cap in td). Due when ``sessions_since >= rebalance_days`` or never
  rebalanced. The provider/position-manager owns updating the last-rebalance marker after acting.
* **Ex-date skip horizon (A12)**: skip the symbol when any upcoming ex-date falls within
  ``ceil(rebalance_days × 7/5)`` CALENDAR days of the bar's day (inclusive) — the trading-session
  hold horizon mapped conservatively (rounded up) onto the calendar, since the scanner is
  deliberately calendar-free. Past ex-dates are ignored.
* **Ranking**: ``indicators.cross_sectional_rank`` — descending momentum, deterministic alphabetical
  tie-break (§9.6); rank 1 = strongest; candidate iff rank ≤ ``top_n``.
* **Levels**: entry = bar close; stop/target ``None`` — the book is rebalance-driven (§6.1); the
  gate's R2 sizing applies its own gap-adjusted risk model.
* **Score** = ``(top_n − rank + 1) / top_n`` — rank 1 scores 1.0, the last admitted rank scores
  ``1/top_n``.
"""

from __future__ import annotations

import math
from datetime import timedelta

from engine.core.types import Bar
from engine.strategy.indicators import cross_sectional_rank
from engine.strategy.scanners.base import Scanner, register
from engine.strategy.types import ScanContext, SignalCandidate


@register
class MomentumScanner(Scanner):
    strategy_id = "mom"
    style = "swing"

    DEFAULT_PARAMS = {           # §6.3 envelope defaults
        "top_n": 2,
        "rebalance_days": 15,
    }

    def scan(self, bar: Bar, ctx: ScanContext) -> list[SignalCandidate]:
        p = self.params
        top_n = int(p["top_n"])
        rebalance_days = int(p["rebalance_days"])

        since = ctx.mom_sessions_since_rebalance
        if since is not None and since < rebalance_days:
            return []  # not due — the book holds until the rebalance horizon

        if bar.symbol not in ctx.momentum_by_symbol:
            return []
        rank = cross_sectional_rank(ctx.momentum_by_symbol).get(bar.symbol)
        if rank is None or rank > top_n:
            return []  # NaN momentum is unrankable; below top_n doesn't trade

        # ---- A12: skip when an ex-date lands inside the hold horizon.
        today = bar.ts_minute.date()
        horizon = today + timedelta(days=math.ceil(rebalance_days * 7 / 5))
        if any(today <= xd <= horizon for xd in ctx.upcoming_ex_dates):
            return []

        return [
            self._candidate(
                bar=bar,
                ctx=ctx,
                side="BUY",
                entry=bar.close,
                stop=None,       # rebalance-driven book — no fixed stop in the §6.1 sketch
                target=None,
                score=(top_n - rank + 1) / top_n,
            )
        ]
