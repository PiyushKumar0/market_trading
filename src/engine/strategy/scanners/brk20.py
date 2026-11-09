"""``brk20`` — 20-day-high daily-close breakout over the FULL eligible universe (swing, long-only).

The intraday watchlist cap makes sub-cap symbols structurally invisible to the per-bar scanners, so
this rule scans COMPLETED daily bars (bhavcopy-final ``bars_1d``) for every ELIGIBLE universe
symbol — watchlist membership is irrelevant. It is deliberately NOT a :class:`Scanner` subclass
(that protocol is (1m bar, ScanContext)-driven and only watched symbols produce bars). A pure batch
rule: the composition root runs it from the window-open//scan_now sweep and the pre-open planner
context, and admits its candidates through ``SignalPreScreen.admit`` so the dedupe/caps bind as for
bar-driven candidates.

PINNED rule (long-only — NSE cash equities cannot be shorted overnight, so a 20d-LOW leg cannot
produce an actionable swing recommendation):

* Let ``y`` be the last COMPLETED session and ``H20`` = max high of the ``lookback_days`` sessions
  strictly before ``y``. Fire iff ``close(y) > H20`` (daily-close breakout).
* FRESH-CROSS only: the previous session must NOT already have been a breakout by the same rule
  (``close(y-1) <= H20'`` for its own window) — a price riding above the band re-fires nothing.
  Same-day refire is separately impossible (prescreen (symbol, strategy) day-dedupe).
* VOLUME confirmation: ``volume(y) >= vol_mult × mean(volume of the lookback window)`` — unconfirmed
  momentum entries are net-negative at retail costs. A deliberate default, a frozen code constant
  (``envelope.yaml`` has no ``brk20`` rows).
* Ex-date skip: any known ex-date within ``ex_skip_days`` CALENDAR days of ``today``.
* Levels — LIMIT-AT-LEVEL. ``raw_levels.entry`` is the BROKEN LEVEL ``H20`` itself, never
  ``close(y)``: the level is the only price still actionable when the candidate is read
  (``close(y)`` is a yesterday-print; sizing off it computes risk from an unobtainable price). The
  plan keeps the rule's OWN risk unit — ``R = close(y) − H20``, the breakout extension — TRANSLATED
  down onto the level, so risk unit, R:R and score are unaffected by the anchor::

      entry  = H20                                  (the retest of the broken level)
      stop   = H20 − R = 2·H20 − close(y)           (the breakout failed by as much as it succeeded)
      target = entry + rr_target × (entry − stop)   (unchanged formula, now off the level)

  Rounding order is pinned: ``entry``/``stop`` are tick-rounded FIRST and ``target`` is derived from
  the rounded pair, so the shipped levels are coherent (``stop < entry < target``, the gate's
  ``levels_coherent`` shape). A breakout margin that vanishes under tick rounding leaves
  ``entry <= stop`` ⇒ no candidate. Sub-cost-floor risk is the cost gate's job, not this rule's.
* Score = ``min(1.0, 0.5 + 5 × (close(y)/H20 − 1))`` — +2% breakout margin ⇒ 0.6, ≥+10% ⇒ 1.0.
  Still scored off ``close(y)/H20``: the breakout MARGIN is the strength signal.
* STOP-GEOMETRY FLOOR. A MARGINAL breakout ships a tiny ``R`` (a few ticks, a fraction of a percent
  of entry). A swing stop thinner than the symbol's ROUTINE overnight gap is noise-level: taken out
  at the next open with high probability while paying the full round trip, and per-trade risk
  sizing INVERTS on it (notional ≈ risk_budget ÷ (overnight_gap_mult × stop%), so the thinnest
  stops request the largest notionals). The ``entry_sanity_band`` is a backstop, not a fix — it
  rejects unfillable ENTRIES, never an unsurvivable STOP. So: ``gaps = |open_t/close_{t−1} − 1|``
  over the last ``gap_lookback_sessions`` completed session-pairs ending at ``y`` (pairs with a
  non-finite or ≤0 member skipped); fewer than ``gap_min_sessions`` valid gaps ⇒ NO candidate
  (``floor_unavailable``: a symbol whose overnight behaviour cannot be established does not ship a
  multi-night plan), and a tick-exact ``R`` below ``gap_floor_mult × median(gaps) × entry`` ⇒ NO
  candidate (``gap_floor``; equality passes). VETO, never widen: widening would re-shape the pinned
  geometry into a different trade (fail-to-zero).

Evidence caveat: this rule ORIGINATES candidates for Tier-1 judgement and the owner's decision; it
carries no presumption of positive expectancy.
"""

from __future__ import annotations

import math
from collections.abc import Collection, Mapping, Sequence
from datetime import date, timedelta
from decimal import Decimal
from statistics import median
from typing import NamedTuple

from ulid import ULID

from engine.strategy.types import RawLevels, SignalCandidate, round_to_tick

STRATEGY_ID = "brk20"

DEFAULT_PARAMS: dict[str, float] = {   # frozen rule defaults; no envelope.yaml rows
    "lookback_days": 20,
    "vol_mult": 1.2,
    "rr_target": 2.0,
    "ex_skip_days": 10,
}

# Stop-geometry floor knobs — a SEPARATE mapping from DEFAULT_PARAMS on purpose: that dict is the
# future learnable envelope; these are owner-only and NEVER learnable (a learner shrinking the floor
# re-opens the degeneracy it closes), and the learning path cannot address a key it has no dict
# entry for. `gap_floor_mult` = 2.0 is a DESIGN constant, not fitted: one TYPICAL night must not
# consume more than half the risk unit (≈p75–p85 of |gap| under fat-tailed gap distributions).
FLOOR_PARAMS: dict[str, float] = {
    "gap_floor_mult": 2.0,
    "gap_lookback_sessions": 20,
    "gap_min_sessions": 10,
}

# Veto classes surfaced through the optional accumulator; named so the sweep call sites read the
# keys scan_daily writes.
VETO_GAP_FLOOR = "gap_floor"
VETO_FLOOR_UNAVAILABLE = "floor_unavailable"
# A structural corp action (bonus/split/rights/demerger) inside the window leaves the stored series
# in two units — the caller computes the set (``hi52.unadjusted_history``) and the sweep skips them.
VETO_UNADJUSTED_HISTORY = "unadjusted_history"


class DailyRow(NamedTuple):
    """One completed session, ascending order — the float analytical read path (bars_1d_frame).

    Field order is APPEND-ONLY and not OHLC-ordered: ``open`` is a REQUIRED last field so a stale
    construction fails LOUDLY rather than defaulting into a gap statistic that would veto candidates.
    """

    high: float
    close: float
    volume: float
    open: float


def _bump(counts: dict[str, int] | None, key: str) -> None:
    """Record one veto on the CALLER's accumulator. ``None`` keeps :func:`scan_daily` pure: the seam
    must never become module state (order-dependent counts, non-reproducible replay)."""
    if counts is not None:
        counts[key] = counts.get(key, 0) + 1


def _gap_floor_frac(rows: Sequence[DailyRow], *, lookback: int, min_sessions: int, mult: float) -> float | None:
    """Stop-geometry floor as a FRACTION of price: ``mult × median(|open_t/close_{t−1} − 1|)``, or ``None``
    when fewer than ``min_sessions`` valid pairs exist in the window (``floor_unavailable``).

    The window is the LAST ``lookback`` pairs ENDING AT ``rows[-1]``, so the floor tracks the symbol's
    CURRENT overnight behaviour. No-lookahead is structural (only completed sessions ≤ y are
    passed). Pairs with a non-finite or ≤0 member are skipped, never zero-filled: zero-filling would
    drag the median DOWN, loosening the floor on the dirtiest data.
    """
    pairs = min(lookback, len(rows) - 1)
    if pairs < min_sessions:
        return None                                   # not enough pairs to ever reach the quorum
    window = rows[-(pairs + 1):]
    gaps = [
        abs(cur.open / prev.close - 1.0)
        for prev, cur in zip(window, window[1:])
        if math.isfinite(prev.close) and math.isfinite(cur.open) and prev.close > 0.0 and cur.open > 0.0
    ]
    if len(gaps) < min_sessions:
        return None
    return mult * median(gaps)


def scan_daily(
    symbol: str,
    rows: Sequence[DailyRow],
    *,
    today: date,
    upcoming_ex_dates: Sequence[date] = (),
    params: Mapping[str, float] | None = None,
    veto_counts: dict[str, int] | None = None,
) -> SignalCandidate | None:
    """Apply the pinned brk20 rule to one symbol's ascending completed-session history.

    ``rows[-1]`` must be the last COMPLETED session (never today's forming bar). Fails to None on
    thin history, non-breakout, stale (non-fresh) breakout, unconfirmed volume, ex-date proximity,
    degenerate levels, or the stop-geometry floor — never raises on ordinary data (fail-to-zero
    posture).

    ``veto_counts``, when supplied, is incremented in place with the stop-geometry-floor veto classes
    (:data:`VETO_GAP_FLOOR` / :data:`VETO_FLOOR_UNAVAILABLE`) so the sweep's call sites can log
    them; the other refusals above are the rule's ordinary silence and are not counted.
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

    # ---- skip when a known ex-date lands inside the calendar horizon.
    horizon = today + timedelta(days=int(p["ex_skip_days"]))
    if any(today <= xd <= horizon for xd in upcoming_ex_dates):
        return None

    # ---- LIMIT-AT-LEVEL plan: anchor on the broken level, keep the rule's own risk unit.
    entry = round_to_tick(h20)                       # the actionable trigger, never close(y)
    risk = round_to_tick(y.close) - entry            # R = breakout extension (tick-exact)
    stop = round_to_tick(entry - risk)
    if entry <= stop:
        return None  # tick-rounding degenerate — no structural risk distance

    # ---- Stop-geometry floor, PINNED last: after every other refusal and the tick-exact R, so the
    #      veto counters count SHIPPABLE plans the floor stopped, not every thin-history symbol (and
    #      a plan with no risk distance at all is not a gap-floor refusal). The floor comes from
    #      RAW gap stats (threshold not tick-rounded) and is compared against the tick-exact R;
    #      equality passes.
    floor_frac = _gap_floor_frac(
        rows,
        lookback=int(FLOOR_PARAMS["gap_lookback_sessions"]),
        min_sessions=int(FLOOR_PARAMS["gap_min_sessions"]),
        mult=float(FLOOR_PARAMS["gap_floor_mult"]),
    )
    if floor_frac is None:
        _bump(veto_counts, VETO_FLOOR_UNAVAILABLE)
        return None
    if risk < Decimal(str(floor_frac)) * entry:
        _bump(veto_counts, VETO_GAP_FLOOR)
        return None

    target = round_to_tick(entry + Decimal(str(p["rr_target"])) * (entry - stop))
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
    unadjusted_symbols: Collection[str] = (),
    veto_counts: dict[str, int] | None = None,
) -> list[SignalCandidate]:
    """Run :func:`scan_daily` over every symbol; deterministic order: score desc, symbol asc.

    ``unadjusted_symbols`` sit out (:data:`VETO_UNADJUSTED_HISTORY`); ``veto_counts`` is threaded
    through and owned by the caller."""
    ex_map = ex_dates_by_symbol or {}
    out: list[SignalCandidate] = []
    for symbol in sorted(histories):
        if symbol in unadjusted_symbols:
            _bump(veto_counts, VETO_UNADJUSTED_HISTORY)
            continue
        cand = scan_daily(
            symbol,
            histories[symbol],
            today=today,
            upcoming_ex_dates=ex_map.get(symbol, ()),
            params=params,
            veto_counts=veto_counts,
        )
        if cand is not None:
            out.append(cand)
    out.sort(key=lambda c: (-c.score, c.symbol))
    return out
