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
* Levels — LIMIT-AT-LEVEL (WO-4, 2026-08-13). ``raw_levels.entry`` is the BROKEN LEVEL ``H20``
  itself, never ``close(y)``: the level is the only price still actionable when the candidate is
  read (``close(y)`` is a yesterday-print that nobody can transact at, and sizing off it computes
  risk from a price better than obtainable). The trade plan keeps the rule's OWN risk unit —
  ``R = close(y) − H20``, the breakout extension, exactly the distance the previous scheme used as
  ``entry − stop`` — and is TRANSLATED down onto the level, so the geometry (risk unit, R:R,
  score) is unchanged and only the anchor moves::

      entry  = H20                                  (the retest of the broken level)
      stop   = H20 − R = 2·H20 − close(y)           (the breakout failed by as much as it succeeded)
      target = entry + rr_target × (entry − stop)   (unchanged formula, now off the level)

  Rounding order is pinned: ``entry``/``stop`` are tick-rounded FIRST and ``target`` is derived
  from the rounded pair, so the shipped levels are internally coherent (``stop < entry < target``,
  the §7.1 ``levels_coherent`` shape) rather than coherent-before-rounding only. A breakout margin
  that vanishes under tick rounding leaves ``entry <= stop`` ⇒ no candidate. Sub-cost-floor risk is
  still the §7.1 cost gate's job (C3), not this rule's.
* Score = ``min(1.0, 0.5 + 5 × (close(y)/H20 − 1))`` — +2% breakout margin ⇒ 0.6, ≥+10% ⇒ 1.0.
  Deliberately still scored off ``close(y)/H20``: the breakout MARGIN is the strength signal, and
  it is unaffected by where the trade plan is anchored.
* STOP-GEOMETRY FLOOR (WO-19, 2026-08-19 — the IDEA/LENSKART degeneracy). A MARGINAL breakout ships
  a tiny ``R``: live IDEA 2026-08-17 (₹0.10, two ticks) and 2026-08-19 MFSL 0.61% / MCX 0.54% /
  LENSKART 0.17% of entry — three of that day's four candidates. A swing stop thinner than the
  symbol's ROUTINE overnight gap is noise-level: it is taken out at the very next open with high
  probability while paying the full round trip, and §7.1 sizing INVERTS on it (notional ≈
  risk_budget ÷ (2.5 × stop%), so the thinnest stops request the largest notionals). The §7.1
  ``entry_sanity_band`` is a backstop, not a fix — it rejects unfillable ENTRIES at evaluation
  time, never an unsurvivable STOP. So: ``gaps = |open_t/close_{t−1} − 1|`` over the last
  ``gap_lookback_sessions`` completed session-pairs ending at ``y`` (pairs with a non-finite or ≤0
  member skipped); fewer than ``gap_min_sessions`` valid gaps ⇒ NO candidate
  (``floor_unavailable`` — a symbol whose overnight behaviour cannot be established does not ship
  a multi-night plan; the conservative young-listing posture), and a tick-exact ``R`` below
  ``gap_floor_mult × median(gaps) × entry`` ⇒ NO candidate (``gap_floor``; equality passes).
  VETO, never widen: widening would re-shape the geometry the rule was pinned with and convert a
  marginal breakout into a different trade (§3.2.5 fail-to-zero).

Evidence caveat rides along (§6.1): this rule ORIGINATES candidates for Tier-1 judgement and the
owner's decision; it carries no presumption of positive expectancy.
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

DEFAULT_PARAMS: dict[str, float] = {   # §6.3 envelope defaults
    "lookback_days": 20,
    "vol_mult": 1.2,
    "rr_target": 2.0,
    "ex_skip_days": 10,
}

# WO-19 stop-geometry floor knobs — a SEPARATE mapping from DEFAULT_PARAMS on purpose. That dict is
# the §6.3 learnable envelope; these are owner-only and NEVER learnable, because a learner shrinking
# the floor re-opens exactly the degeneracy the floor exists to close. Keeping them out of the
# envelope's dict makes the never-learnability structural rather than conventional — the learning
# path cannot address a key it has no dict entry for. `gap_floor_mult` = 2.0 is a DESIGN constant,
# deliberately NOT fitted: one TYPICAL night must not be able to consume more than half the risk
# unit (≈p75–p85 of |gap| under fat-tailed gap distributions).
FLOOR_PARAMS: dict[str, float] = {
    "gap_floor_mult": 2.0,
    "gap_lookback_sessions": 20,
    "gap_min_sessions": 10,
}

# Veto classes surfaced through the optional accumulator (§6.1 observability: a silent veto class is
# undiagnosable). Named constants so the sweep call sites read the same keys scan_daily writes.
VETO_GAP_FLOOR = "gap_floor"
VETO_FLOOR_UNAVAILABLE = "floor_unavailable"
# 2026-09-09: a structural corp action (bonus/split/rights/demerger) inside the window leaves the
# stored series in two units — the caller computes the set (``hi52.unadjusted_history`` over
# ``corp_actions``) and the sweep skips those symbols; the count keeps the tape readable.
VETO_UNADJUSTED_HISTORY = "unadjusted_history"


class DailyRow(NamedTuple):
    """One completed session, ascending order — the float analytical read path (bars_1d_frame).

    Field order is APPEND-ONLY and deliberately not OHLC-ordered: ``open`` joined last (WO-19) as a
    REQUIRED field so every pre-existing construction — positional or keyword — fails LOUDLY at its
    call site rather than silently defaulting into a gap statistic that would veto real candidates.
    """

    high: float
    close: float
    volume: float
    open: float


def _bump(counts: dict[str, int] | None, key: str) -> None:
    """Record one veto on the CALLER's accumulator. ``None`` — the default — keeps :func:`scan_daily`
    a pure function of its arguments: the observability seam must never become module state, which
    would make the sweep's counts order-dependent and the §9.6 replay non-reproducible."""
    if counts is not None:
        counts[key] = counts.get(key, 0) + 1


def _gap_floor_frac(rows: Sequence[DailyRow], *, lookback: int, min_sessions: int, mult: float) -> float | None:
    """WO-19 floor as a FRACTION of price: ``mult × median(|open_t/close_{t−1} − 1|)``, or ``None``
    when fewer than ``min_sessions`` valid pairs exist in the window (``floor_unavailable``).

    The window is the LAST ``lookback`` pairs ENDING AT ``rows[-1]`` — not all history: the floor
    must track the symbol's CURRENT overnight behaviour, so a gap from six months ago neither
    raises nor lowers it. No-lookahead is structural (the caller only ever passes completed
    sessions ≤ y). Pairs with a non-finite or ≤0 member are skipped, never zero-filled: a missing
    open is unknown gap behaviour, and zero-filling would drag the median DOWN — loosening the
    floor on exactly the dirtiest data.
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
    degenerate levels, or the WO-19 stop-geometry floor — never raises on ordinary data (§3.2.5
    fail-to-zero posture).

    ``veto_counts``, when supplied, is incremented in place with the WO-19 veto classes
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

    # ---- A12: skip when a known ex-date lands inside the calendar horizon.
    horizon = today + timedelta(days=int(p["ex_skip_days"]))
    if any(today <= xd <= horizon for xd in upcoming_ex_dates):
        return None

    # ---- LIMIT-AT-LEVEL plan (WO-4): anchor on the broken level, keep the rule's own risk unit.
    entry = round_to_tick(h20)                       # the actionable trigger, never close(y)
    risk = round_to_tick(y.close) - entry            # R = breakout extension (tick-exact)
    stop = round_to_tick(entry - risk)
    if entry <= stop:
        return None  # tick-rounding degenerate — no structural risk distance

    # ---- WO-19 stop-geometry floor. Ordering is PINNED here, last: after every other refusal and
    #      after the tick-exact R exists. The median is the rule's only O(lookback) statistic, so
    #      nothing pays for it on a non-breakout; more importantly the veto counters must count
    #      SHIPPABLE plans the floor stopped, not every universe symbol with a thin history — a
    #      counter that fires on non-candidates measures the universe, not the veto. The
    #      tick-degenerate guard stays AHEAD of it for the same reason: a plan with no risk
    #      distance at all is not a gap-floor refusal. The floor comes from RAW gap stats (no tick
    #      rounding of the threshold) and is compared against the tick-exact R; equality passes.
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
    unadjusted_symbols: Collection[str] = (),
    veto_counts: dict[str, int] | None = None,
) -> list[SignalCandidate]:
    """Run :func:`scan_daily` over every symbol; deterministic order (§9.6): score desc, symbol asc.

    ``unadjusted_symbols`` sit out (:data:`VETO_UNADJUSTED_HISTORY`); ``veto_counts`` is threaded
    straight through — the caller owns the accumulator and reads the WO-19 counts off it after
    the sweep returns (§6.1 observability)."""
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
