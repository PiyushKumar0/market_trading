"""``ins`` — insider net-BUY swing leg.

The platform's first evidence-first origination rule, from the filings event study: the only
recorded edge that survived the corrected-mechanics re-check (T+10 **+0.7297%** / T+20 **+1.5797%**
net at next-open fills with full spread-inclusive costs).

**Architecture — a pure batch rule, like ``brk20``, deliberately NOT a :class:`Scanner`.** Filings
arrive EOD, so there is nothing per-bar to scan. The EOD ``ins_crossings`` job
(:mod:`engine.datafeeds.ins_crossings`) computes the day's NEW crossings with the shared, validated
crossing function and persists them as ``ins_pending`` rows; the next session's
window-open//``scan_now`` sweep turns those rows into candidates HERE and admits them through
``SignalPreScreen.admit`` so the dedupe/caps bind identically (no cap bypass).

This module holds NO crossing logic: it is the pure crossing-row → :class:`SignalCandidate`
translation.

PINNED rule (long-only — NSE cash equities cannot be shorted overnight):

* **Event:** a symbol's trailing-10-session open-market insider net-BUY value crosses ≥
  ``ins.threshold_inr`` (₹1,00,00,000), anchored on the **disclosure broadcast timestamp** (never the
  transaction date), with the re-arm hysteresis. Owner-fixed and **NOT learnable**: the threshold
  defines the validated event population. Computed upstream by
  :func:`engine.datafeeds.insider_crossings.insider_cluster_events` — the ONE definition the
  corrected-mechanics study runs.
* **Style/side:** swing (CNC), BUY.

WHICH PRICE ANCHORS WHAT (read this before touching the levels)
---------------------------------------------------------------
The validated fill is the **next-session OPEN** after the crossing session, which is not a committed,
replayable number when the EOD job runs or the morning sweep admits the row. Three prices are in play
and only one is the entry:

1. ``reference_close`` — the CROSSING SESSION's close, journalled by the EOD job: the freshest
   COMMITTED price at admission, and what ``raw_levels.entry`` is computed from. A **pre-open
   reference for the next-session-open fill**, not a limit the market owes us.
2. The live LTP — the analyst sees it on the payload's price line, and the gate sizes off
   ``max(entry, LTP)`` for a BUY, so a market that ran away from the reference shrinks the size
   rather than flattering it. Nothing here needs to chase it.
3. The actual fill — the owner's, in RECOMMEND. The edge is a SLOW (2-4 week) drift, so minutes of
   open-slippage against the reference are immaterial; that is why a stale-by-one-session anchor is
   acceptable here and would not be for ``orb``.

Levels, pre-registered:

* ``entry`` = ``round_to_tick(reference_close)`` — the pre-open reference above.
* ``stop``  = ``round_to_tick(entry x (1 - stop_pct/100))``, default **5%**. A **disaster stop and a
  deliberate deviation from the stopless validated design**: per-trade risk sizing needs a risk
  distance. Wide enough that the T+10/T+20 drift capture is rarely interrupted. NOT the rule's exit.
* ``target`` = ``None``. The validated T+20 median (+1.21%) is INFORMATION, never an order level; a
  synthetic target would put a fabricated number into the gate's edge math. The cost gate's edge
  input is the configured per-strategy expected edge (``ins.expected_edge_pct`` = the validated T+20
  net +1.58%).
* **Exit is TIME**, not price: hold ``ins.hold_sessions`` (20) sessions — the validated T+20 horizon
  — and the time exit (``RecommendationPipeline.time_exit_check``) IS the exit path. No exit code
  lives here. The T+10 variant is recorded evidence, not a rule.

Rounding order is pinned (as for ``brk20``): ``entry``/``stop`` are tick-rounded and the coherence
test is applied AFTER rounding, so a stop that collapses onto the entry emits nothing rather than
shipping ``stop >= entry`` into the risk gate's ``levels_coherent`` check.

Score = ``min(1.0, max(0.0, 0.5 + 0.5 x log10(trailing_value / threshold)))`` — a bare crossing (1x
the threshold) scores 0.5 and a 10x cluster scores 1.0: the strength signal is the crossing's
MAGNITUDE in orders of magnitude above the floor. Scores are only compared WITHIN a strategy.

EVIDENCE CAVEATS THAT RIDE ALONG (binding):

* **Live-reachability.** Live crossings come from the BSE fresh feed and NSE's PIT V2.0 route. The
  live-reachable event population is NOT proven identical to the backtested one. The
  ``ins_crossings`` job logs fresh-feed row counts every run: sustained zero-rows is STARVATION, not
  absence of signal.
* The CPCV pass is **boundary-exact** (60.0% of folds against a 60% bar — one fold from failure).
* The survivorship / index-membership bound is uncorrectable with stored data.

``ins`` is the best-evidenced leg the platform has, not a proven money-printer. It ships
RECOMMEND-only; live-AUTO enablement follows the ``cat`` precedent — owner approval as new strategy
logic plus soak evidence (the owner promotion gate).
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from datetime import date
from decimal import Decimal
from typing import NamedTuple

from ulid import ULID

from engine.strategy.types import RawLevels, SignalCandidate, round_to_tick

STRATEGY_ID = "ins"

#: Owner knobs (``settings.yaml`` ``ins:`` block), NOT parameter-envelope rows: static owner
#: settings (``stop_pct`` [4-8] and ``hold_sessions`` [10-20] are the future envelope ranges).
#: ``threshold_inr`` is owner-FIXED and never learnable: it defines the validated event population.
DEFAULT_PARAMS: dict[str, float] = {
    "stop_pct": 5.0,             # disaster stop; deliberate deviation from the stopless validated design
    "hold_sessions": 20,         # = the swing max_holding cap = the validated T+20 horizon
    "threshold_inr": 10_000_000.0,   # ₹1cr — the score denominator; the EVENT floor lives in the job
}


class Crossing(NamedTuple):
    """One persisted ``ins_pending`` row — a crossing the EOD job already found and journalled.

    ``reference_close`` is the CROSSING SESSION's close (see the module docstring's "which price
    anchors what"). Journalled at EOD rather than re-read at admission, so the same pending row always
    produces the same candidate (replay determinism).
    """

    symbol: str
    crossing_session: date
    trailing_value: Decimal
    contributing_filings_n: int
    reference_close: Decimal


def scan_crossing(
    crossing: Crossing, *, params: Mapping[str, float] | None = None
) -> SignalCandidate | None:
    """Translate ONE journalled crossing into a :class:`SignalCandidate`.

    Fails to ``None`` on a non-positive/non-finite reference price or on tick-rounding degeneracy
    (a stop that does not survive rounding as strictly below the entry) — never raises on ordinary
    data (fail-to-zero posture). No crossing logic runs here: the event was already decided by
    the shared validated function upstream.
    """
    p = {**DEFAULT_PARAMS, **(params or {})}
    ref = crossing.reference_close
    if not isinstance(ref, Decimal):
        ref = Decimal(str(ref))
    if not ref.is_finite() or ref <= 0:
        return None

    stop_pct = Decimal(str(p["stop_pct"]))
    if stop_pct <= 0 or stop_pct >= 100:
        return None   # a non-positive or total stop is not a risk distance

    # ---- levels: entry anchored on the pre-open reference, stop a fixed % below it, NO target.
    entry = round_to_tick(ref)
    stop = round_to_tick(entry * (Decimal(1) - stop_pct / Decimal(100)))
    if stop >= entry:
        return None   # tick-rounding degenerate (a sub-tick stop distance) — no structural risk

    # ---- score: orders of magnitude above the threshold floor (see the module docstring).
    threshold = Decimal(str(p["threshold_inr"]))
    value = crossing.trailing_value
    if not isinstance(value, Decimal):
        value = Decimal(str(value))
    if threshold <= 0 or value <= 0:
        score = 0.5   # unscoreable magnitude ⇒ the neutral bare-crossing score, never a guess
    else:
        score = min(1.0, max(0.0, 0.5 + 0.5 * math.log10(float(value / threshold))))

    return SignalCandidate(
        signal_id=str(ULID()),
        strategy_id=STRATEGY_ID,
        symbol=crossing.symbol,
        side="BUY",
        style="swing",
        raw_levels=RawLevels(entry=entry, stop=stop, target=None),
        score=score,
    )


def sweep_crossings(
    crossings: Sequence[Crossing], *, params: Mapping[str, float] | None = None
) -> list[SignalCandidate]:
    """Run :func:`scan_crossing` over every pending row; deterministic order: score desc,
    symbol asc — matching ``brk20.sweep_daily`` so the ranked-admission path sees one convention."""
    out: list[SignalCandidate] = []
    for crossing in sorted(crossings, key=lambda c: c.symbol):
        cand = scan_crossing(crossing, params=params)
        if cand is not None:
            out.append(cand)
    out.sort(key=lambda c: (-c.score, c.symbol))
    return out
