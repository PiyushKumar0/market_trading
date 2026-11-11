"""``hi52`` — 52-week-high-proximity continuation, v2, DEMOTED to shadow.

The v2 backtest was CPCV-promotable but its edge lived in the INDEX cell of a survivorship-tainted
population proxy, so it ran as a forward test under a written kill rule —
``scripts/hi52_forward_verdict.py`` (n >= 20 signals AND (median net at T+20 <= 0 OR T+20 hit rate
< 50%)) — and that rule read DEMOTE. The id is now in ``ops.main.NO_EDGE_SHADOW_STRATEGIES`` and
``settings.yaml`` carries no ``hi52.expected_edge_pct``, so the gate rejects every candidate while the
signals are still journalled and scored. Promoting it again is a new registration under the research
bar. This module touches neither gate; it only originates.

**v2 is the rule**: the three registered signal-time filters (smooth approach, no gap day, eligible
population) are GATING. Their thresholds are frozen in :data:`DEFAULT_PARAMS`, owner-only — never
learnable, never retuned without a new pre-registration.

**Architecture — a pure batch rule over the CORE (NIFTY 500) eligible subset, unlike ``brk20``,
which scans the full eligible set** (the full-market backtest measured the edge as index-class only).
It is deliberately NOT a :class:`~engine.strategy.scanners.base.Scanner`: that protocol is driven by
(1m bar, ScanContext) and only watched symbols produce bars, while a swing-horizon 52wk-proximity
read has nothing per-bar to key off. This module scans COMPLETED daily bars (bhavcopy-final
``bars_1d``) for every CORE-eligible symbol. The composition root runs it from the
window-open/``scan_now`` sweep and admits its candidates through ``SignalPreScreen.admit`` so the
dedupe/caps bind as for bar-driven candidates.

**Thesis.** George & Hwang (2004): proximity to the 52-week high anchors investors, so a FRESH
crossing into the high-proximity band keeps drifting over T+5..T+20. Frog-in-the-Pan (Da, Gurun &
Warachka): GRADUAL information diffusion is under-reacted to more than a single jump, so a smooth
climb should drift further than a spiky one. The path-smoothness read GATES, through the same
:func:`diagnostics_for` the study reads; surviving candidates' values ride the composition root's
``hi52_sweep`` log line.

PINNED rule (long-only — NSE cash equities cannot be shorted overnight):

* Let ``y`` be the last COMPLETED session. Fewer than ``min_sessions`` rows ⇒ no candidate.
  ``hi`` = max high of the last ``min(lookback_sessions, len(rows))`` sessions INCLUDING ``y``.
  ``prox = close(y) / hi``.
* Fire iff ``prox >= proximity_min`` AND FRESH-CROSS — the PREVIOUS session's own ``prox``, over its
  own window ending at ``y-1``, was ``< proximity_min`` (a stock parked above the band does not
  re-fire daily) — AND VOLUME confirmation, ``volume(y) >= vol_mult × mean(volume of the 20
  sessions immediately before y)`` (``vol_mult = 1.0``; never skipped, ``min_sessions`` guarantees
  20 prior rows) — AND no known ex-date within ``ex_skip_days`` CALENDAR days of ``today`` — AND the
  v2 filters below.
* v2 filters (thresholds fixed from full-sample medians BEFORE the run that measured them, so they
  are knowable at signal time): SMOOTH APPROACH — ``up_day_frac >= smooth_up_day_frac_min`` (0.55)
  AND ``max_day_move <= smooth_max_day_move`` (0.07) over the 20 completed sessions ending at ``y``,
  both from :func:`diagnostics_for` so the live gate and the study share ONE definition; NO GAP
  DAY — ``|close(y)/close(y-1) - 1| <= gap_day_max`` (0.05), rounded to the same 4 dp
  ``max_day_move`` is (when the trigger day is the window's biggest mover the two are the SAME
  number and must read identically). Counted as :data:`VETO_SMOOTH` / :data:`VETO_GAP`, evaluated
  in this order and FIRST-MATCH, so the counts partition the v2 rejects exactly as
  ``scripts/backtest_hi52.py`` tallies them (a trigger move above ``smooth_max_day_move`` books as
  ``smooth``, never ``gap``). A diagnostic that cannot be computed is a REFUSAL, never a pass. A
  filter NEUTRALIZED to an infinite ceiling (:data:`V1_PARAMS`, the backtest's v1 path only) is not
  in the registration and refuses nothing.
* The third v2 filter — population = the CORE eligible subset — is NOT enforced here: the
  composition root feeds ``MarketStore.get_core_eligible_symbols``, and re-deriving membership here
  would be a second, drifting definition.
* The ex-date skip mirrors ``brk20``'s calendar-horizon check, with one deliberate difference: it is
  COUNTED here as :data:`VETO_EX_DATE_SKIP`, where ``brk20`` counts only its stop-geometry-floor
  vetoes. A new forward-test strategy's veto classes must be observable from day one.
* Unadjusted-history veto: a symbol with a :data:`UNADJUSTED_KINDS` ex-date inside the lookback is
  not read at all (counted as :data:`VETO_UNADJUSTED_HISTORY`) — stored ``bars_1d`` is never
  re-adjusted across an ex-date, so the window holds bars in two units until it rolls past. The
  composition root supplies the set from ``corp_actions`` (:func:`unadjusted_history`).
* Levels: ``entry = round_to_tick(close(y))``; there is no "broken level" to retest. ``stop =
  round_to_tick(entry × (1 − stop_pct/100))``, default **6%**, a disaster stop sized like ``cat``'s
  (``per_trade_risk.overnight_gap_mult`` applies to a swing entry). ``target = None``: the drift was
  MEASURED over a fixed horizon, never predicted to a level, and no target is invented; with no
  registered edge (``settings.hi52.expected_edge_pct`` is absent) the gate's cost check rejects it.
  Degenerate tick-rounding (``stop >= entry``) emits nothing, as for ``brk20``/``cat``.
* Score = ``prox``, clamped to ``[0, 1]``. The scale does not agree with any other scanner's and is
  not meant to — cross-strategy STANDING is a per-strategy quantile at the analyst forward slot
  (``pipeline._quantile_band``). The one raw cross-strategy comparison is ``prescreen._rank`` inside
  one batch admission, where the per-strategy sub-caps, not the sort, bound a strategy's share of
  the day; the ordering consequence is handled at the composition root
  (``ops.main._publication_order``). Rescaling is NOT free: it moves this rule's quantile history.
* Exit is TIME, not price, as for ``cat``: :data:`DEFAULT_PARAMS` ``hold_sessions`` is the hold the
  hold map (``ops.holds``) gives hi52, and the time exit
  (``RecommendationPipeline.time_exit_check``) is the exit path; no exit code lives here.
* Diagnostics: ``up_day_frac`` = fraction of the last 20 sessions with ``close > prev close``;
  ``max_day_move`` = the largest single-session ``|close/prev − 1|`` over the same 20. **Not
  attached to** :class:`~engine.strategy.types.SignalCandidate` (its docstring pins EXACTLY its
  field set; no scanner attaches metadata); exposed through the standalone :func:`diagnostics_for`,
  computable on the rows :func:`scan_daily` scans. :func:`scan_daily` calls it only AFTER every v1
  test passes, so the cost is paid per CANDIDATE, not per symbol.

Evidence caveat: the edge was measured on a population labelled by CURRENT index membership applied
backwards (survivorship-tainted), so a live candidate carries the registered edge for the gate's
arithmetic and no presumption of expectancy for the analyst's judgement.
"""

from __future__ import annotations

import math
from collections.abc import Collection, Iterable, Mapping, Sequence
from datetime import date, timedelta
from decimal import Decimal
from typing import Any, NamedTuple

from ulid import ULID

from engine.strategy.scanners.brk20 import DailyRow
from engine.strategy.types import RawLevels, SignalCandidate, round_to_tick

STRATEGY_ID = "hi52"

DEFAULT_PARAMS: dict[str, float] = {   # parameter-envelope-shaped, frozen for the forward-test window
    "proximity_min": 0.95,
    "lookback_sessions": 252,
    "min_sessions": 126,
    "vol_mult": 1.0,
    "ex_skip_days": 10,
    "hold_sessions": 20,          # the live hold, read by engine.ops.holds; not read below
    "stop_pct": 6.0,
    # v2 signal-time filters. OWNER-ONLY and NEVER learnable: these three numbers DEFINE the
    # measured v2 population, so moving one invalidates the backtest and restarts the forward
    # clock. A change is a NEW pre-registration, never a tuning.
    "smooth_up_day_frac_min": 0.55,
    "smooth_max_day_move": 0.07,
    "gap_day_max": 0.05,
}

#: The v2 filter keys, as one set: what :data:`V1_PARAMS` neutralizes and what the owner-only rule
#: above applies to. Named so a caller never has to re-list the three literals.
V2_FILTER_PARAMS: frozenset[str] = frozenset(
    {"smooth_up_day_frac_min", "smooth_max_day_move", "gap_day_max"}
)

#: The v1 parameter set: :data:`DEFAULT_PARAMS` with the v2 filters NEUTRALIZED (a floor nothing can
#: fail, ceilings nothing finite can exceed) rather than removed — :func:`scan_daily` merges over
#: :data:`DEFAULT_PARAMS`, so a deleted key would come back at its gating value.
#:
#: ONE caller: ``scripts/backtest_hi52.py``'s ``--registration v1`` path, which reproduces the v1
#: numbers on the rule as registered then. Not a live operating mode. The neutralization is EXACT,
#: including the fail-closed refusals: an infinite ceiling turns its filter off outright, so a row v1
#: emitted is not dropped as a non-computable gap (see :func:`scan_daily`); no live params set one.
V1_PARAMS: dict[str, float] = {
    **DEFAULT_PARAMS,
    "smooth_up_day_frac_min": 0.0,
    "smooth_max_day_move": math.inf,
    "gap_day_max": math.inf,
}

#: Veto class surfaced through the optional accumulator (observability). See the module
#: docstring for why hi52 counts this where brk20 does not.
VETO_EX_DATE_SKIP = "ex_date_skip"

#: v2 filter vetoes, counted FIRST-MATCH in the registration's order so the two partition the v2
#: rejects, as ``scripts/backtest_hi52.py`` counts. Read :data:`VETO_GAP` as "trigger moves in the
#: half-open band (``gap_day_max``, ``smooth_max_day_move``], plus any non-computable gap": a bigger
#: move already failed the smooth ceiling and books as :data:`VETO_SMOOTH`.
VETO_SMOOTH = "smooth"
VETO_GAP = "gap"

#: Corp-action kinds that RESCALE the price series: after such an ex-date every earlier bar is in a
#: different unit. Neither ``bars_1d`` source re-adjusts STORED history (Kite candles are adjusted at
#: fetch time but the series is extended session by session; bhavcopy rows are raw), so any symbol
#: with one inside its lookback carries phantom pre-ex highs until the window rolls past.
UNADJUSTED_KINDS = frozenset({"bonus", "split", "rights", "demerger"})
VETO_UNADJUSTED_HISTORY = "unadjusted_history"


def unadjusted_history(corp_rows: Iterable[Mapping[str, Any]]) -> frozenset[str]:
    """Symbols with a structural action among ``corp_rows`` (``MarketStore.get_corp_actions`` rows
    for the lookback window). Pure; the caller picks the window."""
    return frozenset(r["symbol"] for r in corp_rows if r["kind"] in UNADJUSTED_KINDS)


class Diagnostics(NamedTuple):
    """Frog-in-the-Pan path-smoothness read on the SAME rows :func:`scan_daily` gates on.

    ``up_day_frac``/``max_day_move`` ARE the v2 smooth filter's inputs (the module docstring's PINNED
    rule); ``prox``/``high_52wk`` are reporting fields."""

    prox: float
    high_52wk: float
    up_day_frac: float
    max_day_move: float


def _bump(counts: dict[str, int] | None, key: str) -> None:
    """Record one veto on the CALLER's accumulator (own copy of ``brk20._bump``, deliberately not
    imported). ``None`` keeps :func:`scan_daily` a pure function of its arguments."""
    if counts is not None:
        counts[key] = counts.get(key, 0) + 1


def _proximity(rows: Sequence[DailyRow], *, lookback: int) -> tuple[float, float] | None:
    """``(prox, hi)`` for the window of ``min(lookback, len(rows))`` rows ENDING AT ``rows[-1]``, or
    ``None`` on empty input or non-finite/non-positive ``close``/``hi``.

    Shared by the live proximity check, the fresh-cross check (called on ``rows[:-1]``) and
    :func:`diagnostics_for`.
    """
    if not rows:
        return None
    window = rows[-min(lookback, len(rows)):]
    close_y = rows[-1].close
    hi = max(r.high for r in window)
    if not (math.isfinite(close_y) and math.isfinite(hi)) or hi <= 0.0:
        return None
    return close_y / hi, hi


def diagnostics_for(
    rows: Sequence[DailyRow], *, params: Mapping[str, float] | None = None
) -> Diagnostics | None:
    """Compute the Frog-in-the-Pan diagnostics for ``rows`` standalone (:class:`SignalCandidate` has
    no field to carry them; see the module docstring). ``None`` on degenerate input; never raises.

    The ONE definition of ``up_day_frac``/``max_day_move``: :func:`scan_daily`'s smooth filter,
    ``scripts/backtest_hi52.py``'s ``signal_diag`` and the journalled diagnostics all call THIS.
    """
    p = {**DEFAULT_PARAMS, **(params or {})}
    lookback = int(p["lookback_sessions"])
    if lookback < 1:
        return None
    prox_hi = _proximity(rows, lookback=lookback)
    if prox_hi is None:
        return None
    prox, hi = prox_hi

    tail = rows[-21:] if len(rows) >= 21 else rows
    pairs = list(zip(tail, tail[1:]))
    if not pairs:
        return Diagnostics(prox=round(prox, 4), high_52wk=hi, up_day_frac=0.0, max_day_move=0.0)

    up_days = sum(1 for prev, cur in pairs if cur.close > prev.close)
    moves = [
        abs(cur.close / prev.close - 1.0)
        for prev, cur in pairs
        if math.isfinite(prev.close) and math.isfinite(cur.close) and prev.close != 0.0
    ]
    max_move = max(moves) if moves else 0.0
    return Diagnostics(
        prox=round(prox, 4),
        high_52wk=hi,
        up_day_frac=round(up_days / len(pairs), 2),
        max_day_move=round(max_move, 4),
    )


def scan_daily(
    symbol: str,
    rows: Sequence[DailyRow],
    *,
    today: date,
    upcoming_ex_dates: Sequence[date] = (),
    params: Mapping[str, float] | None = None,
    veto_counts: dict[str, int] | None = None,
) -> SignalCandidate | None:
    """Apply the pinned hi52 rule to one symbol's ascending completed-session history.

    ``rows[-1]`` must be the last COMPLETED session (never today's forming bar). Fails to ``None`` on
    thin history, sub-threshold proximity, a stale (non-fresh) crossing, unconfirmed volume, ex-date
    proximity, a v2 filter refusal, or degenerate tick-rounded levels — never raises on ordinary OR
    malformed data (fail-to-zero posture).

    ``veto_counts``, when supplied, is incremented in place with :data:`VETO_EX_DATE_SKIP`,
    :data:`VETO_SMOOTH` or :data:`VETO_GAP`; the other refusals above are this rule's ordinary
    silence and are not counted (see the module docstring for why the ex-date class alone is counted
    here).
    """
    p = {**DEFAULT_PARAMS, **(params or {})}
    min_sessions = int(p["min_sessions"])
    lookback = int(p["lookback_sessions"])
    if min_sessions < 2 or lookback < 1 or len(rows) < min_sessions:
        return None

    proximity_min = float(p["proximity_min"])

    live = _proximity(rows, lookback=lookback)
    if live is None:
        return None
    prox, _hi = live
    if prox < proximity_min:
        return None

    # ---- fresh cross: yesterday's OWN window (ending at y-1) must not already have cleared the band.
    prev = _proximity(rows[:-1], lookback=lookback)
    if prev is None:
        return None
    prev_prox, _prev_hi = prev
    if prev_prox >= proximity_min:
        return None

    # ---- volume confirmation vs the mean of the 20 sessions immediately before y.
    vol_window = rows[:-1][-20:]
    vols = [r.volume for r in vol_window]
    avg_vol = sum(vols) / len(vols)
    y_volume = rows[-1].volume
    vol_mult = float(p["vol_mult"])
    if not (math.isfinite(y_volume) and math.isfinite(avg_vol)) or avg_vol < 0.0 or y_volume < vol_mult * avg_vol:
        return None

    # ---- ex-date skip (mirrors brk20's calendar horizon); COUNTED here (see module docstring).
    horizon = today + timedelta(days=int(p["ex_skip_days"]))
    if any(today <= xd <= horizon for xd in upcoming_ex_dates):
        _bump(veto_counts, VETO_EX_DATE_SKIP)
        return None

    # ---- v2 signal-time filters, AFTER every v1 test (matches `backtest_hi52.v2_admits`): a
    #      candidate the ex-date horizon refused never reaches these counters. Smooth first, then
    #      gap, first-match (see VETO_GAP's note).
    diag = diagnostics_for(rows, params=p)
    if diag is None or not (
        math.isfinite(diag.up_day_frac)
        and math.isfinite(diag.max_day_move)
        and diag.up_day_frac >= float(p["smooth_up_day_frac_min"])
        and diag.max_day_move <= float(p["smooth_max_day_move"])
    ):
        _bump(veto_counts, VETO_SMOOTH)
        return None
    # The trigger session's OWN move, rounded to the 4 dp `diagnostics_for` uses. rows[-2] exists
    # (min_sessions >= 2) and a zero/non-finite prior close cannot divide.
    # An INFINITE `gap_day_max` turns the filter OFF rather than making it unfailable: otherwise a
    # symbol whose close(y-1) is 0/NaN (a suspended session) would be dropped and booked VETO_GAP on
    # a v1 re-run, changing its population. The LIVE default (0.05) is finite and still fails closed.
    gap_max = float(p["gap_day_max"])
    prev_close = rows[-2].close
    gap_move = (
        round(abs(rows[-1].close / prev_close - 1.0), 4)
        if math.isfinite(prev_close) and prev_close > 0.0
        else math.nan
    )
    if math.isfinite(gap_max) and not (math.isfinite(gap_move) and gap_move <= gap_max):
        _bump(veto_counts, VETO_GAP)
        return None

    # ---- levels: entry is the trigger session's OWN close (no "broken level" to retest here);
    #      disaster stop, no fabricated target — identical reasoning to `cat`.
    stop_pct = Decimal(str(p["stop_pct"]))
    if stop_pct <= 0 or stop_pct >= 100:
        return None  # a non-positive or total stop is not a risk distance
    entry = round_to_tick(rows[-1].close)
    stop = round_to_tick(entry * (Decimal(1) - stop_pct / Decimal(100)))
    if stop >= entry:
        return None  # tick-rounding degenerate — no structural risk distance

    score = min(1.0, max(0.0, prox))
    return SignalCandidate(
        signal_id=str(ULID()),
        strategy_id=STRATEGY_ID,
        symbol=symbol,
        side="BUY",
        style="swing",
        raw_levels=RawLevels(entry=entry, stop=stop, target=None),
        score=score,
    )


def sweep_daily(
    histories: Mapping[str, Sequence[DailyRow]],
    *,
    today: date,
    ex_dates_by_symbol: Mapping[str, Sequence[date]] | None = None,
    unadjusted_symbols: Collection[str] = (),
    params: Mapping[str, float] | None = None,
    veto_counts: dict[str, int] | None = None,
) -> list[SignalCandidate]:
    """Run :func:`scan_daily` over every symbol; deterministic order: score desc, symbol asc.

    ``unadjusted_symbols`` (see :func:`unadjusted_history`) are not read at all and each is counted
    as :data:`VETO_UNADJUSTED_HISTORY`. ``veto_counts`` is threaded through; the caller owns it."""
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
