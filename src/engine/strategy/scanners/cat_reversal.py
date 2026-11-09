"""``cat_reversal`` SHADOW — story-level directional REVERSAL as a catalyst.

A SEPARATE shadow rule, not a variant of ``cat``. The event is "**this story's own earlier bearish
claim was reversed**" (e.g. an official denial refuting a prior short reading) — a
resolved-uncertainty relief setup, a different mechanism from post-event drift and so a different
experiment with its own clock, thresholds and signal population.

**Horizon caveat.** The reversal-day move itself is NOT reachable: the digest runs pre-open (~08:35)
on day *d* over clusters from day *d−1* and the fill is the next session's open, so the repricing is
history before a candidate exists. What is measurable is the RESIDUAL drift — hence horizons shorter
than ``cat`` v2's.

RELATIONSHIP TO ``cat`` v2 — SEPARATE, AND DELIBERATELY OVERLAPPING
-------------------------------------------------------------------
This module never imports ``cat``'s params, never reads ``cat.stop_pct`` (it has its OWN; tuning one
must not move the other) and adds no condition to ``cat``'s event definition.

A reversal row satisfies ``cat`` v2's event too (``originating`` ∧ ``long`` ∧ age ≤ 1), so ONE
reversal story yields TWO candidates — one per strategy — competing for the SAME
``catalyst_guard.max_catalyst_entries_day`` budget of 2. That is the intended trade (share the cap,
never add a second one) and is affordable only because neither strategy can reach RECOMMEND: a
doubled story spends analyst calls and prescreen slots, not exposure. Narrowing ``cat`` to exclude
reversal rows is refused — it would change ``cat``'s in-flight event definition.

PINNED rule (long-only — NSE cash equities cannot be shorted overnight):

* **Event:** a ``catalyst_watchlist`` row with ``grade='originating'`` ∧ ``direction='long'`` ∧
  ``event_age_sessions <= 1`` ∧ ``reversal_of IS NOT NULL``. The first three are ``cat`` v2's event
  verbatim. **The grading conditions are NOT relaxed for reversal candidates** — the row cleared the
  identical ``originating_conditions`` AND-list as every originating row; "it reversed something" is
  an ADDITIONAL requirement, never a substitute.
* **One candidate per story:** the age bound IS the single-shot semantics, as ``cat``.
* **Style/side:** swing (CNC), BUY.

``reversal_of`` is computed deterministically in the digest (``news_pipeline.reversal_source``): the
same ``(symbol, event_type)`` story contains an EARLIER cluster that NAMED the symbol (direct entity
resolution, never sector/theme fan-out), cleared the inclusion floor on its own undecayed
materiality, and carried the opposite (short) direction under the same ``sentiment_min_long``
classification. No LLM judges it; it is a pure function of the corpus and the run's ``ran_at``.

LEVELS — ``ins``/``cat`` semantics, unchanged
----------------------------------------------
The validated fill is the NEXT-SESSION OPEN, not a committed number at sweep time. So:

* ``entry`` = ``round_to_tick(reference_close)`` — the PRIOR SESSION's bhavcopy-final close, read
  from ``bars_1d`` by the caller. A pre-open REFERENCE, never a limit the market owes us.
* ``stop``  = ``round_to_tick(entry x (1 - stop_pct/100))``, default **5%**, from this strategy's OWN
  ``cat_reversal.stop_pct``. A **disaster stop**: per-trade risk sizing needs a risk distance, and
  the ``per_trade_risk.overnight_gap_mult`` arithmetic applies to a swing entry. NOT the rule's exit.
* ``target`` = ``None``. The rule will not invent a price target.
* **Exit is TIME**: hold ``cat_reversal.hold_sessions`` sessions; the time exit
  (``RecommendationPipeline.time_exit_check``) is the exit path. No exit code lives here.

``entry``/``stop`` are tick-rounded and coherence is checked AFTER rounding, so a stop that collapses
onto the entry emits nothing rather than shipping ``stop >= entry`` into the gate's
``levels_coherent`` check (as for ``brk20``/``ins``/``cat``).

Score = the reversal cluster's **weighted materiality**, clamped to [0, 1]. Scores are compared only
WITHIN a strategy (the quantile funnel), so the scale need agree with no other scanner's.

WHY THIS SHIPS WITHOUT AN EDGE, AND WHY THAT IS ENFORCED RATHER THAN ASSUMED
----------------------------------------------------------------------------
There is deliberately **no ``cat_reversal.expected_edge_pct``** — no validated edge exists, and
inventing a number would put a fabricated value into the gate's arithmetic (the failure ``ins``
documents against).

That absence is NOT what keeps the rule out of RECOMMEND. ``cat_reversal`` is registered in
``ops.main.NO_EDGE_SHADOW_STRATEGIES``, so the cost gate rejects it unconditionally, before
``target_price`` is even read — see ``risk/gate.py`` :data:`~engine.risk.gate._SHADOW_NO_EDGE` for why
the property is declared there rather than inferred from a missing ``expected_edge_pct``. The funnel
still journals the signal at prescreen admission (that IS the validation population) and the analyst
still writes its thesis; nothing reaches RECOMMEND. Wiring an edge is the owner promotion gate,
contingent on the verdict below.

VERDICT CRITERIA, PRE-REGISTERED (binding and frozen for the whole shadow window; moving one
re-opens multiplicity and restarts the clock)

* **Horizons: T+5 and T+10 net drift**, at next-session-open fills with spread-inclusive CNC costs and
  the margin floor. These deliberately differ from ``cat`` v2's T+10/T+20:
  - **T+5** is the mechanism's own horizon: resolved-uncertainty repricing is fast, so the residual
    this rule can reach is days, not weeks. Measuring it only at slow horizons dilutes a real effect.
  - **T+10** is the common horizon the two shadows share, the only way to answer "is the reversal
    subset better than plain ``cat``?".
  - **T+20 is deliberately NOT pre-registered.** There is no mechanism story for a relief rally still
    paying at four weeks, and every extra horizon is another multiplicity test on a small population.
* **Population: ≥60 sessions AND ≥15 admitted signals** before the study is run. Reversal rows are a
  strict SUBSET of originating rows, so the arrival rate is lower than ``cat``'s (bars ≥30 / ≥20);
  a session bar assuming ``cat``'s rate would close the study on a handful of events.
* **Verdict:** net ≤ the cost floor at BOTH horizons ⇒ ``cat_reversal`` origination is RETIRED and the
  reversal flag stays context/audit-only. Clears ⇒ owner review, which is also where an
  ``expected_edge_pct`` (and thus RECOMMEND eligibility) would be wired.
* **Starvation:** < 1 admitted signal per 20 sessions, sustained across the first 60 sessions, is a
  STARVATION finding about the EVENT's rarity (or the news corpus), not the rule's expectancy; it
  triggers an early owner check-in (widen the corpus, or retire the rule unmeasured).

The signal population is the funnel's own journal: ``prescreen_day_slots`` rows with
``strategy_id='cat_reversal'`` (written by ``ops.pipeline._journal_slot``), keyed by ``strategy_id``
so this study and ``cat``'s read disjoint row sets.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from decimal import Decimal
from typing import NamedTuple

from engine.strategy.scanners import _shadow_catalyst
from engine.strategy.types import SignalCandidate

STRATEGY_ID = "cat_reversal"

#: The grade / direction the rule originates on — the DIGEST's verdicts, re-checked
#: here only so a caller that fetches the table unfiltered cannot widen the rule.
ORIGINATING_GRADE = "originating"
LONG_DIRECTION = "long"

#: Maximum event age, in completed TRADING SESSIONS, at which a row may originate. **Not a knob and
#: never an envelope row** — this bound IS the single-shot semantics: an age-2+ re-grade would enter
#: the story twice.
MAX_EVENT_AGE_SESSIONS = 1

#: Owner knobs (``settings.yaml`` ``cat_reversal:`` block), NOT envelope rows, frozen for the shadow
#: window. A SEPARATE block from ``cat``'s: tuning one must never move the other.
DEFAULT_PARAMS: dict[str, float] = {
    "stop_pct": 5.0,             # disaster stop; the overnight_gap_mult arithmetic
    "hold_sessions": 10,         # the longer registered horizon (T+10); INERT during the shadow:
                                 # the cost gate rejects every candidate, so nothing is held
}


class WatchlistRow(NamedTuple):
    """One ``catalyst_watchlist`` row, plus the price the caller looked up.

    Structurally ``cat.WatchlistRow`` plus ``reversal_of``, deliberately its own type so the two
    rules' separately pre-registered event definitions can diverge.

    ``reference_close`` is the PRIOR SESSION's bhavcopy-final close from ``bars_1d``; ``None`` when
    the symbol has no prior daily bar, which must cost this candidate and nothing else.

    ``reversal_of`` is the digest's ``reversal_of`` column: the cluster_id of the earlier
    opposite-direction cluster this story reversed, or ``None`` for an ordinary row.

    ``entry_id`` becomes the candidate's ``catalyst_ref``: the audit link back to the exact
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

    Split out so the caller can COUNT eligible rows for its starvation-visibility line with the
    same predicate the sweep applies (the starvation criterion is only checkable if the reversal
    count is observable next to the originating count).

    ``reversal_of`` must be a NON-EMPTY string: a store that renders SQL NULL as ``''`` must not
    read as "this reversed something".

    The grade/direction/age prefix is ``cat``'s event, evaluated by the shared
    ``_shadow_catalyst.is_originating`` against THIS module's constants.
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
    ordinary data (fail-to-zero posture). Assumes eligibility: :func:`sweep_watchlist` applies
    :func:`is_eligible` first, and grading itself happened upstream in the digest.

    The arithmetic is ``_shadow_catalyst.build_candidate``, shared with ``cat``; this rule keeps its
    own identity and ``stop_pct`` (:data:`DEFAULT_PARAMS`).
    """
    p = {**DEFAULT_PARAMS, **(params or {})}
    return _shadow_catalyst.build_candidate(
        row, strategy_id=STRATEGY_ID, stop_pct=p["stop_pct"]
    )


def sweep_watchlist(
    rows: Sequence[WatchlistRow], *, params: Mapping[str, float] | None = None
) -> list[SignalCandidate]:
    """Filter today's watchlist rows to the rule's event, then translate each; deterministic order:
    score desc, symbol asc — as ``cat``/``ins``/``brk20``.

    The eligibility filter lives HERE, not in the caller's query: it IS the event definition and must
    be unit-testable without a database. The caller may narrow its fetch; this pass is the authority,
    so a widened fetch can never widen the rule.
    """
    return _shadow_catalyst.sweep(
        rows,
        is_eligible=is_eligible,
        translate=lambda r: scan_entry(r, params=params),
    )
