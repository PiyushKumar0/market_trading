"""``cat_reversal`` SHADOW — story-level directional REVERSAL as a catalyst (§2.7, 2026-08-27).

A SEPARATE shadow rule, not a variant of ``cat``. The event it originates on is not "good news
arrived" but "**this story's own earlier bearish claim was reversed**" — a resolved-uncertainty
relief setup, which is a different economic mechanism from post-event drift and therefore a different
experiment with its own clock, its own thresholds and its own signal population.

THE LIVE CASE THAT MOTIVATED IT (HINDZINC, 2026-08-26/27)
---------------------------------------------------------
An official denial refuted the platform's own prior bearish reading of a story, and all the watchlist
did with it was flip ``short``→``long`` — the fact that the long reading was a REFUTATION was thrown
away. The incident is told in full, with the numbers, at ``news_pipeline._watchlist_rows``'s
best-cluster ranking comment; the recency-decay fix there is what makes the flip happen at all, and
this rule is what notices that a flip HAPPENED.

**What this rule can and cannot capture — read before judging the horizon.** The denial-day move
itself is NOT reachable and was never the target. The digest runs pre-open (~08:35) on day *d* over
clusters from day *d−1*, and the fill convention is the next session's open, so the denial day's own
repricing is already history before a candidate can exist. What is left to measure is the RESIDUAL
drift after the relief repricing — which is precisely why the horizons below are shorter than
``cat`` v2's and why "the stock jumped 5% on the day" is evidence that the event matters, not
evidence of this rule's expectancy.

RELATIONSHIP TO ``cat`` v2 — SEPARATE, AND DELIBERATELY OVERLAPPING
-------------------------------------------------------------------
``cat`` v2's shadow window opened 2026-08-18 with pre-registered, frozen thresholds. Nothing here
touches THEM: this module never imports ``cat``'s params, never reads ``cat.stop_pct`` (it has its
OWN — a future tuning of one must not silently move the other), and adds no condition to ``cat``'s
event definition. Its validation CLOCK, however, is no longer the one opened 2026-08-18: the
recency-decay best-cluster selection shipped alongside this rule can widen ``cat``'s originating
population mid-window, which restarts the clock under WO-18's own pre-registration (§2.7, 2026-08-28
amendment). That restart is the decay change's, not this rule's.

The consequence, stated plainly because it is a real cost: a reversal row satisfies ``cat`` v2's
event too (``originating`` ∧ ``long`` ∧ age ≤ 1), so ONE reversal story yields TWO candidates — one
per strategy — competing for the SAME ``catalyst_guard.max_catalyst_entries_day`` budget of 2. That
is the intended trade (§2.7: share the cap, never add a second one), and it is affordable only
because neither strategy can reach RECOMMEND: what a doubled story spends is analyst calls and
prescreen slots, not exposure. Narrowing ``cat`` to exclude reversal rows would be the alternative,
and it is refused — it would change ``cat``'s in-flight event definition and restart its clock.

PINNED rule (long-only — NSE cash equities cannot be shorted overnight):

* **Event:** a ``catalyst_watchlist`` row with ``grade='originating'`` ∧ ``direction='long'`` ∧
  ``event_age_sessions <= 1`` ∧ ``reversal_of IS NOT NULL``. The first three are ``cat`` v2's event
  verbatim; the fourth is this rule's own. **The grading conditions are NOT relaxed for reversal
  candidates** — a reversal row cleared the identical ``originating_conditions`` AND-list (story-level
  domain corroboration, sentiment floor, event-type whitelist, novelty, universe, surveillance,
  results-day ban) as every other originating row. "It reversed something" is an ADDITIONAL
  requirement, never a substitute for one.
* **One candidate per story:** the age bound IS the single-shot semantics, exactly as ``cat`` — an
  age-2+ re-grade never re-originates.
* **Style/side:** swing (CNC), BUY.

``reversal_of`` is computed deterministically in the digest (``news_pipeline.reversal_source``): the
same ``(symbol, event_type)`` story contains an EARLIER cluster that NAMED the symbol (direct entity
resolution, never sector/theme fan-out), cleared the §2.7 inclusion floor on its own undecayed
materiality, and carried the opposite (short) direction under the same ``sentiment_min_long``
classification. No LLM judges "is this a denial of that", and no cross-day DB
lookup is involved — it is a pure function of the corpus and the run's ``ran_at`` (§9.1).

LEVELS — ``ins``/``cat`` semantics, unchanged
----------------------------------------------
The validated-fill convention is the NEXT-SESSION OPEN, which is not a committed, replayable number
at sweep time. So, identically to ``ins`` and ``cat``:

* ``entry`` = ``round_to_tick(reference_close)`` — the PRIOR SESSION's bhavcopy-final close, the
  freshest committed price at admission, read from ``bars_1d`` by the caller. A pre-open REFERENCE
  for a next-open fill, never a limit the market owes us.
* ``stop``  = ``round_to_tick(entry x (1 - stop_pct/100))``, default **5%**, from this strategy's OWN
  ``cat_reversal.stop_pct``. A **disaster stop**: §7.1 sizing needs a risk distance, so one must
  exist, and the §7.1 ``per_trade_risk.overnight_gap_mult`` (2.5x) arithmetic applies to a swing
  entry (the ``ins`` 2026-08-17 correction). It is NOT the rule's exit.
* ``target`` = ``None``. The rule has no price target and will not invent one.
* **Exit is TIME**: the existing §7.1 ``max_holding`` swing path
  (``RecommendationPipeline.check_aged_positions``). No exit code lives here.

Rounding order is pinned as for ``brk20``/``ins``/``cat``: ``entry``/``stop`` are tick-rounded and
coherence is checked AFTER rounding, so a stop that collapses onto the entry emits nothing rather
than shipping ``stop >= entry`` into the §7.1 ``levels_coherent`` check.

Score = the reversal cluster's **weighted materiality**, clamped to [0, 1] — the digest's own
strength measure for the cluster that won the row. Scores are compared only WITHIN a strategy (the
§5.2(a) quantile funnel), so this scale need agree with no other scanner's.

WHY THIS SHIPS WITHOUT AN EDGE, AND WHY THAT IS ENFORCED RATHER THAN ASSUMED
----------------------------------------------------------------------------
There is deliberately **no ``cat_reversal.expected_edge_pct``** — no validated edge exists, and
measuring one is the entire point of the shadow. Inventing a number to feed the §7.1 C3 gate would
put a fabricated value into the gate's arithmetic, the exact failure ``ins`` documents against.

That absence is NOT what keeps the rule out of RECOMMEND. ``cat_reversal`` is registered in
``ops.main.NO_EDGE_SHADOW_STRATEGIES``, so C3 rejects it unconditionally, before ``target_price`` is
even read — see ``risk/gate.py`` :data:`~engine.risk.gate._SHADOW_NO_EDGE` for why the property is
declared there rather than inferred from a missing ``expected_edge_pct``. The funnel still journals
the signal at prescreen admission (that IS the validation population) and the analyst still writes
its thesis (a validation covariate); nothing reaches RECOMMEND. Wiring an edge, and with it RECOMMEND
eligibility, is the §8.6 owner gate, contingent on the verdict below.

VERDICT CRITERIA, PRE-REGISTERED NOW (before any signal accumulates — binding, and frozen for the
whole shadow window exactly as ``cat``'s are; moving one re-opens multiplicity and restarts the clock)

* **Horizons: T+5 and T+10 net drift**, at next-session-open fills with spread-inclusive CNC costs and
  the WO-3 margin floor. Reasoning, since these deliberately differ from ``cat`` v2's T+10/T+20:
  - **T+5** is the mechanism's own horizon. A denial/refutation resolves a KNOWN uncertainty, and
    resolved-uncertainty repricing is fast — the residual this rule can actually reach (see the
    docstring's second section) is days, not the 2-4 weeks of fundamental post-event drift. Measuring
    a fast mechanism only at slow horizons would dilute a real effect toward zero.
  - **T+10** exists so the two shadows share ONE common horizon. Without it there is no way to answer
    the question that actually matters — "is the reversal subset better than plain ``cat``?" — because
    the studies would have no comparable measurement.
  - **T+20 is deliberately NOT pre-registered.** There is no mechanism story for a relief rally still
    paying at four weeks, and every additional horizon is another multiplicity test on a population
    this small.
* **Population: ≥60 sessions AND ≥15 admitted signals** before the study is run. Both bars are higher
  in sessions and lower in count than ``cat``'s (≥30 / ≥20) for one reason: reversal rows are a strict
  SUBSET of originating rows, so the arrival rate is necessarily lower than ``cat``'s measured
  ~0.3-0.6/session, and a session bar that assumed ``cat``'s rate would close the study on a handful
  of events.
* **Verdict:** net ≤ the cost floor at BOTH horizons ⇒ ``cat_reversal`` origination is RETIRED and the
  reversal flag stays context/audit-only. Clears ⇒ §8.6 owner review, which is also where an
  ``expected_edge_pct`` (and thus RECOMMEND eligibility) would be wired.
* **Starvation:** < 1 admitted signal per 20 sessions, sustained across the first 60 sessions, is a
  STARVATION finding about the EVENT's rarity (or the news corpus), not about the rule's expectancy.
  It triggers an early owner check-in — widen the corpus, or retire the rule unmeasured — rather than
  silent accumulation toward a population bar that may be years away. This is the honest risk in the
  design and it is pre-registered rather than discovered later.

The signal population is the funnel's own journal: ``prescreen_day_slots`` rows with
``strategy_id='cat_reversal'`` (written by ``ops.pipeline._journal_slot`` at admission), which is the
same mechanism ``cat`` v2's population uses and is keyed by ``strategy_id`` — so the two studies read
disjoint row sets and can never mix.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from decimal import Decimal
from typing import NamedTuple

from engine.strategy.scanners import _shadow_catalyst
from engine.strategy.types import SignalCandidate

STRATEGY_ID = "cat_reversal"

#: The grade / direction the rule originates on — the DIGEST's verdicts (§2.7 step 5(ii)), re-checked
#: here only so a caller that fetches the table unfiltered cannot widen the rule.
ORIGINATING_GRADE = "originating"
LONG_DIRECTION = "long"

#: Maximum event age, in completed TRADING SESSIONS, at which a row may originate. **Not a knob and
#: never an envelope row** — this bound IS the single-shot semantics: an age-2+ re-grade of the same
#: story would enter it twice and double-count it in the shadow's signal population.
MAX_EVENT_AGE_SESSIONS = 1

#: Owner knobs (``settings.yaml`` ``cat_reversal:`` block), NOT §6.3 envelope rows, and frozen for the
#: whole shadow window. Deliberately a SEPARATE block from ``cat``'s: these are two experiments, and a
#: future tuning of one must never silently move the other.
DEFAULT_PARAMS: dict[str, float] = {
    "stop_pct": 5.0,             # disaster stop; the §7.1 overnight_gap_mult arithmetic (ins reasoning)
    "hold_sessions": 10,         # the intended exit if the §8.6 gate ever opens = the longer
                                 # pre-registered horizon (T+10). INERT during the shadow: nothing is
                                 # ever held, because C3 rejects every candidate by construction.
}


class WatchlistRow(NamedTuple):
    """One ``catalyst_watchlist`` row (§2.7 step 5(ii)), plus the price the caller looked up.

    Structurally ``cat.WatchlistRow`` plus ``reversal_of``, and deliberately its own type rather than
    a shared one: the two rules' event definitions are pre-registered separately and must be free to
    diverge without a shared tuple forcing a lockstep edit to the other's in-flight shadow.

    ``reference_close`` is the PRIOR SESSION's bhavcopy-final close from ``bars_1d``, read at sweep
    time by the caller; ``None`` when the symbol has no prior daily bar at all — a thin/absent-history
    case that must cost this candidate and nothing else.

    ``reversal_of`` is the digest's ``reversal_of`` column: the cluster_id of the earlier
    opposite-direction cluster this story reversed, or ``None`` for an ordinary row.

    ``entry_id`` becomes the candidate's ``catalyst_ref``: the §6.5 audit link back to the exact
    graded row — and through its ``cluster_refs``, to the headlines behind it.
    """

    entry_id: str
    symbol: str
    grade: str
    direction: str | None
    event_age_sessions: int | None
    materiality: float | None
    reversal_of: str | None
    reference_close: Decimal | None


def is_eligible(row: WatchlistRow) -> bool:
    """Does this watchlist row satisfy the rule's EVENT (grade ∧ direction ∧ age ∧ reversal)?

    Split out from :func:`sweep_watchlist` so the caller can COUNT eligible rows for its
    starvation-visibility line using the same predicate the sweep applies — a second, drifting copy of
    the filter in the composition root is exactly how "no signals" stops being diagnosable. For this
    rule that visibility matters more than for ``cat``: a pre-registered starvation criterion is only
    checkable if the reversal count is observable next to the originating count.

    ``reversal_of`` is required to be a NON-EMPTY string, not merely non-``None``: a round-trip through
    a store that renders SQL NULL as ``''`` must not be readable as "this reversed something".

    The grade/direction/age prefix is ``cat``'s event verbatim and is evaluated by the shared
    ``_shadow_catalyst.is_originating`` against THIS module's constants; the reversal clause below is
    the one condition that makes this a different rule.
    """
    return (
        _shadow_catalyst.is_originating(
            row,
            grade=ORIGINATING_GRADE,
            direction=LONG_DIRECTION,
            max_age_sessions=MAX_EVENT_AGE_SESSIONS,
        )
        and bool(row.reversal_of)
    )


def scan_entry(
    row: WatchlistRow, *, params: Mapping[str, float] | None = None
) -> SignalCandidate | None:
    """Translate ONE eligible watchlist row into a :class:`SignalCandidate`.

    Fails to ``None`` on a missing/non-positive/non-finite reference price or on tick-rounding
    degeneracy (a stop that does not survive rounding as strictly below the entry) — never raises on
    ordinary data (§3.2.5 fail-to-zero posture). Assumes eligibility: :func:`sweep_watchlist` applies
    :func:`is_eligible` first, and grading itself happened upstream in the digest.

    The arithmetic itself is ``_shadow_catalyst.build_candidate``, shared with ``cat``; what stays
    here is this rule's own identity and its own ``stop_pct`` (see :data:`DEFAULT_PARAMS` — the two
    rules' knobs are deliberately separate blocks and one must never move the other).
    """
    p = {**DEFAULT_PARAMS, **(params or {})}
    return _shadow_catalyst.build_candidate(
        row, strategy_id=STRATEGY_ID, stop_pct=p["stop_pct"]
    )


def sweep_watchlist(
    rows: Sequence[WatchlistRow], *, params: Mapping[str, float] | None = None
) -> list[SignalCandidate]:
    """Filter today's watchlist rows to the rule's event, then translate each; deterministic order
    (§9.6): score desc, symbol asc — matching ``cat``/``ins``/``brk20`` so the ranked-admission path
    sees one convention.

    The eligibility filter lives HERE rather than in the caller's query because for this rule the
    filter IS the event definition — single-shot semantics and the reversal requirement included — and
    it has to be unit-testable without a database. The caller may still narrow its fetch
    (``grade='originating'`` is cheap at the store); this pass is the authority, so a widened fetch can
    never widen the rule.
    """
    return _shadow_catalyst.sweep(
        rows,
        is_eligible=is_eligible,
        translate=lambda r: scan_entry(r, params=params),
    )
