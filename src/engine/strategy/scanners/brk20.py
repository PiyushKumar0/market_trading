"""``brk20`` — 20-day-high daily-close breakout over the FULL eligible universe (swing, long-only).

Owner-directed 2026-08-04 (plan-amended, §6.1 addendum) after the BPCL 2026-08-03 miss: the
intraday watchlist cap makes sub-cap symbols structurally invisible to the per-bar scanners, so
this rule scans COMPLETED daily bars (bhavcopy-final ``bars_1d``) for every ELIGIBLE universe
symbol — watchlist membership is irrelevant. It is deliberately NOT a :class:`Scanner` subclass:
the §3.2.5 scanner protocol is (1m bar, ScanContext)-driven and only watched symbols ever produce
bars. This module is a pure batch rule; the composition root runs it from the window-open//scan_now
sweep and the §5.3 pre-open planner context, and admits its candidates through
``SignalPreScreen.admit`` so the §3.2.5 dedupe/caps bind exactly as for bar-driven candidates.

PINNED rule (long-only — NSE cash equities cannot be shorted overnight, so a 20d-LOW leg cannot
produce an actionable swing recommendation):

* Let ``y`` be the last COMPLETED session and ``H20`` = max high of the ``lookback_days`` sessions
  strictly before ``y``. Fire iff ``close(y) > H20`` (daily-close breakout).
* FRESH-CROSS only: the previous session must NOT already have been a breakout by the same rule
  (``close(y-1) <= H20'`` for its own window) — a price riding above the band re-fires nothing.
  Same-day refire is separately impossible (prescreen (symbol, strategy) day-dedupe).
* VOLUME confirmation: ``volume(y) >= vol_mult × mean(volume of the lookback window)`` — the §6.1
  posture; the platform's backtests found unconfirmed momentum entries net-negative at retail
  costs. (BPCL 2026-08-03 broke out at 0.80× average volume — this rule would REFUSE it; that is
  a deliberate default, owner-tunable via the §6.3 envelope.)
* A12 ex-date skip: any known ex-date within ``ex_skip_days`` CALENDAR days of ``today``.
* Levels: entry = ``close(y)``; stop = ``H20`` (the broken level is the structural invalidation);
  target = entry + ``rr_target`` × (entry − stop). Sub-cost-floor risk is the §7.1 cost gate's
  job (C3), not this rule's.
* Score = ``min(1.0, 0.5 + 5 × (close(y)/H20 − 1))`` — +2% breakout margin ⇒ 0.6, ≥+10% ⇒ 1.0.

Evidence caveat rides along (§6.1): this rule ORIGINATES candidates for Tier-1 judgement and the
owner's decision; it carries no presumption of positive expectancy.
"""

from __future__ import annotations

import math
from datetime import date, timedelta
from typing import Mapping, NamedTuple, Sequence

from ulid import ULID

from engine.strategy.types import RawLevels, SignalCandidate, round_to_tick

STRATEGY_ID = "brk20"

DEFAULT_PARAMS: dict[str, float] = {   # §6.3 envelope defaults
    "lookback_days": 20,
    "vol_mult": 1.2,
    "rr_target": 2.0,
    "ex_skip_days": 10,
}


class DailyRow(NamedTuple):
    """One completed session, ascending order — the float analytical read path (bars_1d_frame)."""

    high: float
    close: float
    volume: float


def scan_daily(
    symbol: str,
    rows: Sequence[DailyRow],
    *,
    today: date,
    upcoming_ex_dates: Sequence[date] = (),
    params: Mapping[str, float] | None = None,
) -> SignalCandidate | None:
    """Apply the pinned brk20 rule to one symbol's ascending completed-session history.

    ``rows[-1]`` must be the last COMPLETED session (never today's forming bar). Fails to None on
    thin history, non-breakout, stale (non-fresh) breakout, unconfirmed volume, ex-date proximity,
    or degenerate levels — never raises on ordinary data (§3.2.5 fail-to-zero posture).
    """
    p = {**DEFAULT_PARAMS, **(params or {})}
    lookback = int(p["lookback_days"])
    if lookback < 2 or len(rows) < lookback + 2:
        return None  # need y, its window, and one more session for the fresh-cross test

    y = rows[-1]
    window = rows[-(lookback + 1):-1]
    h20 = max(r.high for r in window)
    if not (math.isfinite(y.close) and math.isfinite(h20)) or h20 <= 0.0 or y.close <= h20:
        return None

    # ---- fresh cross: yesterday-1 must not already have been above ITS OWN band.
    prev = rows[-2]
    prev_window = rows[-(lookback + 2):-2]
    if prev.close > max(r.high for r in prev_window):
        return None

    # ---- volume confirmation vs the window mean.
    vols = [r.volume for r in window]
    avg_vol = sum(vols) / len(vols)
    if not math.isfinite(avg_vol) or avg_vol <= 0.0 or y.volume < p["vol_mult"] * avg_vol:
        return None

    # ---- A12: skip when a known ex-date lands inside the calendar horizon.
    horizon = today + timedelta(days=int(p["ex_skip_days"]))
    if any(today <= xd <= horizon for xd in upcoming_ex_dates):
        return None

    entry = round_to_tick(y.close)
    stop = round_to_tick(h20)
    if entry <= stop:
        return None  # tick-rounding degenerate — no structural risk distance
    target = round_to_tick(entry + int(p["rr_target"]) * (entry - stop))
    score = min(1.0, 0.5 + 5.0 * (y.close / h20 - 1.0))
    return SignalCandidate(
        signal_id=str(ULID()),
        strategy_id=STRATEGY_ID,
        symbol=symbol,
        side="BUY",
        style="swing",
        raw_levels=RawLevels(entry=entry, stop=stop, target=target),
        score=score,
    )


def sweep_daily(
    histories: Mapping[str, Sequence[DailyRow]],
    *,
    today: date,
    ex_dates_by_symbol: Mapping[str, Sequence[date]] | None = None,
    params: Mapping[str, float] | None = None,
) -> list[SignalCandidate]:
    """Run :func:`scan_daily` over every symbol; deterministic order (§9.6): score desc, symbol asc."""
    ex_map = ex_dates_by_symbol or {}
    out: list[SignalCandidate] = []
    for symbol in sorted(histories):
        cand = scan_daily(
            symbol,
            histories[symbol],
            today=today,
            upcoming_ex_dates=ex_map.get(symbol, ()),
            params=params,
        )
        if cand is not None:
            out.append(cand)
    out.sort(key=lambda c: (-c.score, c.symbol))
    return out
