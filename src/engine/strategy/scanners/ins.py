"""``ins`` — insider net-BUY swing leg (§6.1 addendum, owner-directed 2026-08-17).

The platform's FIRST evidence-first origination rule. Promoted from the §2.8.4 filings evidence
because it is the only recorded edge that survived the corrected-mechanics re-check (WO-16,
2026-08-14: T+10 **+0.7297%** / T+20 **+1.5797%** net at next-open fills with full spread-inclusive
costs; CPCV +0.0359%/day with the median passing split 4x the WO-3 margin floor).

**Architecture — a pure batch rule, like ``brk20``, deliberately NOT a :class:`Scanner`.** Filings
arrive EOD, so there is nothing per-bar to scan and the §3.2.5 (1m bar, ScanContext) protocol does
not fit. The EOD ``ins_crossings`` job (:mod:`engine.datafeeds.ins_crossings`) computes the day's NEW
crossings with the shared, validated crossing function and persists them as ``ins_pending`` rows; the
next session's window-open//``scan_now`` sweep turns those rows into candidates HERE and admits them
through ``SignalPreScreen.admit`` so the §3.2.5 dedupe/caps bind identically (no cap bypass).

This module holds NO crossing logic of its own — it never re-derives an event. It is the pure
crossing-row → :class:`SignalCandidate` translation, and nothing else.

PINNED rule (long-only — NSE cash equities cannot be shorted overnight):

* **Event:** a symbol's trailing-10-session open-market insider net-BUY value crosses ≥
  ``ins.threshold_inr`` (₹1,00,00,000), anchored on the **disclosure broadcast timestamp** (never the
  transaction date), with the §2.8.4 re-arm hysteresis. Owner-fixed and **NOT learnable**: the
  threshold defines the validated event population, and moving it re-opens multiplicity.
  Computed upstream by :func:`engine.datafeeds.insider_crossings.insider_cluster_events` — the ONE
  definition the WO-16 study runs.
* **Style/side:** swing (CNC), BUY.

WHICH PRICE ANCHORS WHAT (read this before touching the levels)
---------------------------------------------------------------
The validated fill is the **next-session OPEN** after the crossing session. That price does not exist
when the EOD job runs, and it still does not exist as a committed, replayable number when the morning
sweep admits the row. So three distinct prices are in play and only one of them is the entry:

1. ``reference_close`` — the CROSSING SESSION's close, journalled by the EOD job. This is the
   freshest COMMITTED price available at admission time (at 09:30+ it is literally yesterday's
   close), and it is what ``raw_levels.entry`` is computed from. It is a **pre-open reference for
   the next-session-open fill**, not a limit the market owes us.
2. The live LTP — the analyst sees it on the payload's price line, and the §7.1 gate's WO-4 sizing
   reference already sizes off ``max(entry, LTP)`` for a BUY, so a market that has run away from the
   reference shrinks the size rather than flattering it. Nothing here needs to chase it.
3. The actual fill — the owner's, in RECOMMEND. WO-16's ``close_t`` A/B showed this edge is SLOW
   (a 2-4 week drift), so minutes of open-slippage against the reference are immaterial; that is
   exactly why a stale-by-one-session anchor is acceptable here and would not be for ``orb``.

Levels, pre-registered in the plan:

* ``entry`` = ``round_to_tick(reference_close)`` — the pre-open reference above.
* ``stop``  = ``round_to_tick(entry x (1 - stop_pct/100))``, default **6%**. This is a **disaster
  stop and a deliberate deviation from the stopless validated design**: §7.1 sizing needs a risk
  distance, so one has to exist. It is set wide enough that the T+10/T+20 drift capture is rarely
  interrupted; its interference is measurable in the ledger and reviewable. It is NOT the rule's
  exit.
* ``target`` = ``None``. The rule has no price target — the validated T+20 median (+1.21%) is
  INFORMATION, never an order level, and inventing one would put a fabricated number into the
  gate's edge math. The C3 edge input comes from the configured per-strategy expected edge
  (``ins.expected_edge_pct`` = the validated T+20 net +1.58%), not from a synthetic target.
* **Exit is TIME**, not price: hold to the §7.1 ``max_holding`` swing cap of 20 trading days —
  exactly the validated T+20 horizon — so the EXISTING max-holding machinery IS the exit path
  (``RecommendationPipeline.check_aged_positions``). No exit code lives here or anywhere else for
  ``ins``. The T+10 variant remains recorded evidence, not a rule.

Rounding order is pinned (as for ``brk20``): ``entry``/``stop`` are tick-rounded and the coherence
test is applied AFTER rounding, so a stop that collapses onto the entry under tick rounding emits
nothing rather than shipping ``stop >= entry`` into the §7.1 ``levels_coherent`` check.

Score = ``min(1.0, max(0.0, 0.5 + 0.5 x log10(trailing_value / threshold)))`` — a bare crossing (1x
the threshold) scores 0.5 and a 10x cluster scores 1.0, so the strength signal is the crossing's
MAGNITUDE in orders of magnitude above the floor. Scores are only ever compared WITHIN a strategy
(the §5.2(a) funnel ranks by per-strategy score quantile), so this scale needs to agree with no other
scanner's.

EVIDENCE CAVEATS THAT RIDE ALONG (WO-16, binding — plan §6.1):

* **Live-reachability.** Live crossings are computable only from the BSE fresh feed (live since
  2026-07-19, ~13-18 in-universe rows/day); the NSE PIT feed's ~70-day content embargo makes it
  historical-only. The live-reachable event population is therefore NOT proven identical to the
  backtested one. The ``ins_crossings`` job logs fresh-feed row counts every run: sustained zero-rows
  is STARVATION, not absence of signal.
* The CPCV pass is **boundary-exact** (60.0% of folds against a 60% bar — one fold from failure).
* The survivorship / index-membership bound is uncorrectable with stored data.

``ins`` is the best-evidenced leg the platform has. It is not a proven money-printer, and it ships
RECOMMEND-only (Phase 2 is structurally so); Phase-4 live-AUTO enablement follows the ``cat``
precedent — owner approval as new strategy logic plus soak evidence (§8.6).
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

#: Owner knobs (``settings.yaml`` ``ins:`` block), NOT §6.3 envelope rows in Phase 2 — the plan pins
#: ``stop_pct`` [4-8] and ``hold_sessions`` [10-20] as FUTURE envelope rows and keeps them static
#: owner settings until the learning phase needs them (no protected-store churn before then).
#: ``threshold_inr`` is owner-FIXED and never learnable at all: it defines the validated event
#: population (§6.1 — moving it re-opens multiplicity).
DEFAULT_PARAMS: dict[str, float] = {
    "stop_pct": 6.0,             # disaster stop; deliberate deviation from the stopless validated design
    "hold_sessions": 20,         # = the §7.1 swing max_holding cap = the validated T+20 horizon
    "threshold_inr": 10_000_000.0,   # ₹1cr — the score denominator; the EVENT floor lives in the job
}


class Crossing(NamedTuple):
    """One persisted ``ins_pending`` row — a crossing the EOD job already found and journalled.

    ``reference_close`` is the CROSSING SESSION's close (see the module docstring's "which price
    anchors what"): the freshest committed price at admission time, and the pre-open reference for
    the validated next-session-open fill. It is deliberately journalled at EOD rather than re-read at
    admission, so the same pending row always produces the same candidate (§9.6 replay determinism).
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
    data (§3.2.5 fail-to-zero posture). No crossing logic runs here: the event was already decided by
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
    """Run :func:`scan_crossing` over every pending row; deterministic order (§9.6): score desc,
    symbol asc — matching ``brk20.sweep_daily`` so the ranked-admission path sees one convention."""
    out: list[SignalCandidate] = []
    for crossing in sorted(crossings, key=lambda c: c.symbol):
        cand = scan_crossing(crossing, params=params)
        if cand is not None:
            out.append(cand)
    out.sort(key=lambda c: (-c.score, c.symbol))
    return out
