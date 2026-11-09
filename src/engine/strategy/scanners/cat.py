"""``cat`` v2 SHADOW — news-catalyst swing leg (owner-directed 2026-08-18).

The platform's news→origination carve-out, a SHADOW rule: the drift the news layer is meant to
capture is a 2-4-WEEK phenomenon, which an intraday trigger cannot express (the retired v1 intraday
confirmation was refuted on proxies).

**Architecture — a pure batch rule, like ``brk20``/``ins``, deliberately NOT a :class:`Scanner`.**
The grade is decided pre-open (~08:35) by ``CatalystDigestJob``, so there is nothing per-bar to scan.
The digest persists the day's ``catalyst_watchlist``; the window-open//``scan_now`` sweep turns
today's ``originating`` rows into candidates HERE and admits them through ``SignalPreScreen.admit``
so the dedupe/caps — and the ``catalyst_guard.max_catalyst_entries_day`` cap the pre-screen owns —
bind identically (no bypass).

This module holds NO news logic: it never re-grades a row or touches a cluster. It is the pure
watchlist-row → :class:`SignalCandidate` translation plus the eligibility filter defining the event.

PINNED rule (long-only — NSE cash equities cannot be shorted overnight):

* **Event:** a ``catalyst_watchlist`` row with ``grade='originating'`` ∧ ``direction='long'`` ∧
  ``event_age_sessions <= 1``. **One candidate per story:** the age bound IS the single-shot
  semantics — an age-2+ re-grade never re-originates (the rows still reappear as context). The
  grading conditions (materiality, sentiment floor, event-type whitelist, story-level domain
  corroboration, novelty, universe/surveillance) are the digest's, applied upstream against the
  HASH-VERIFIED ``catalyst_guard``; nothing here re-derives them, and thresholds are FROZEN for the
  whole shadow window (moving one re-opens multiplicity and restarts the clock).
* **Style/side:** swing (CNC), BUY.

WHICH PRICE ANCHORS WHAT (read this before touching the levels)
---------------------------------------------------------------
Identical in shape to ``ins``: the validated fill is the NEXT-SESSION OPEN, which is not a committed,
replayable number when the morning sweep runs. Three prices are in play and only one is the entry:

1. ``reference_close`` — the PRIOR SESSION's bhavcopy-final close, read from ``bars_1d`` by the
   caller: the freshest COMMITTED price at admission, and what ``raw_levels.entry`` is computed
   from. A **pre-open reference for the next-session-open fill**, never a limit the market owes us.
2. The live LTP — the gate sizes a BUY off ``max(entry, LTP)``, so a stock that gapped away from
   the reference shrinks the size instead of flattering it.
3. The actual fill — in the shadow there ISN'T one. The horizon under measurement is T+10/T+20, so
   minutes of open slippage against the reference are immaterial; that is why a stale-by-one-session
   anchor is acceptable here and would not be for ``orb``.

Levels, pre-registered:

* ``entry`` = ``round_to_tick(reference_close)`` — the pre-open reference above.
* ``stop``  = ``round_to_tick(entry x (1 - stop_pct/100))``, default **5%**. A **disaster stop**:
  per-trade risk sizing needs a risk distance. 5 because the ``per_trade_risk.overnight_gap_mult``
  arithmetic applies to a swing entry (2.5x when chosen, 2.0x since owner decision 2026-09-24).
  It is NOT the rule's exit.
* ``target`` = ``None``. The rule has no price target; an invented level would put a fabricated
  number into the gate's edge math.
* **Exit is TIME**, not price: hold ``cat.hold_sessions`` sessions, and the time exit
  (``RecommendationPipeline.time_exit_check``) IS the exit path. No exit code lives here.

Rounding order is pinned (as for ``brk20``/``ins``): ``entry``/``stop`` are tick-rounded and the
coherence test is applied AFTER rounding, so a stop that collapses onto the entry emits nothing
rather than shipping ``stop >= entry`` into the gate's ``levels_coherent`` check.

Score = the row's **weighted materiality**, clamped to [0, 1] — the digest's strength measure
(already multiplied by ``cat.fanout_weight`` for a fanned-out sector/theme catalyst, i.e. the number
the grade decision used). Scores are only compared WITHIN a strategy (per-strategy score quantile).

WHY THIS SHIPS WITHOUT AN EDGE (the shadow's defining property)
---------------------------------------------------------------
There is deliberately **no ``cat.expected_edge_pct``** anywhere — not in ``settings.yaml``, not in
the gate's ``strategy_expected_edge_pct`` map. With no target and no registered edge the cost-gate
check has no edge basis and **fail-closed-rejects every ``cat`` candidate**: the funnel journals the
signal (prescreen admission IS the validation population), the analyst still writes its thesis (a
validation covariate, <=2 calls/day inside the existing budget), and nothing reaches RECOMMEND.
Inventing an edge number would put a fabricated value into the gate's arithmetic (the failure ``ins``
documents against). Wiring an edge is the owner promotion gate, contingent on the shadow verdict.

VERDICT CRITERIA, PRE-REGISTERED (binding):

* >=30 sessions AND >=20 admitted signals, then a T+10/T+20 net-drift study at next-session-open
  fills with spread-inclusive CNC costs and the margin floor.
* Net <= the cost floor at BOTH horizons ⇒ ``cat`` origination is RETIRED and the watchlist stays
  context-only permanently.
* Sustained < 0.2 signals/session for 3 consecutive weeks is a STARVATION finding (the news feeds,
  not the rule) and triggers an early owner check-in; the caller's per-sweep visibility line makes
  it observable.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from decimal import Decimal
from typing import NamedTuple

from engine.strategy.scanners import _shadow_catalyst
from engine.strategy.types import SignalCandidate

STRATEGY_ID = "cat"

#: The grade / direction the rule originates on. Both are the DIGEST's verdicts,
#: re-checked here only so a caller that fetches the table unfiltered cannot widen the rule.
ORIGINATING_GRADE = "originating"
LONG_DIRECTION = "long"

#: Maximum event age, in completed TRADING SESSIONS, at which a row may originate. **Not a knob and
#: never an envelope row**: this bound IS the single-shot semantics — an age-2+ re-grade would enter
#: the same story twice.
MAX_EVENT_AGE_SESSIONS = 1

#: Owner knobs (``settings.yaml`` ``cat:`` block), NOT envelope rows — frozen for the shadow window.
DEFAULT_PARAMS: dict[str, float] = {
    "stop_pct": 5.0,             # disaster stop; the overnight_gap_mult arithmetic
    "hold_sessions": 20,         # = the swing max_holding cap — the EXISTING machinery is the exit
}


class WatchlistRow(NamedTuple):
    """One ``catalyst_watchlist`` row, plus the price the caller looked up.

    Everything except ``reference_close`` is a column the digest wrote; ``reference_close`` is the
    PRIOR SESSION's bhavcopy-final close from ``bars_1d`` (see the module docstring's "which price
    anchors what"), ``None`` when the symbol has no prior daily bar — which must cost this
    candidate and nothing else.

    ``entry_id`` becomes the candidate's ``catalyst_ref``: the audit link back to the exact
    graded row — and through its ``cluster_refs``, to the headlines behind it.
    """

    entry_id: str
    symbol: str
    grade: str
    direction: str | None
    event_age_sessions: int | None
    materiality: float | None
    reference_close: Decimal | None


def is_eligible(row: WatchlistRow) -> bool:
    """Does this watchlist row satisfy the rule's EVENT (grade ∧ direction ∧ age)?

    Split out so the caller can COUNT age-eligible rows for its starvation-visibility line with the
    same predicate the sweep applies.

    ``_shadow_catalyst.is_originating`` holds the mechanics and this module the bounds, so
    ``cat_reversal`` (which adds a fourth condition) can share the prefix without reaching into
    this module's constants.
    """
    return _shadow_catalyst.is_originating(
        row,
        grade=ORIGINATING_GRADE,
        direction=LONG_DIRECTION,
        max_age_sessions=MAX_EVENT_AGE_SESSIONS,
    )


def scan_entry(
    row: WatchlistRow, *, params: Mapping[str, float] | None = None
) -> SignalCandidate | None:
    """Translate ONE eligible watchlist row into a :class:`SignalCandidate`.

    Fails to ``None`` on a missing/non-positive/non-finite reference price or on tick-rounding
    degeneracy (a stop that does not survive rounding as strictly below the entry) — never raises on
    ordinary data (fail-to-zero posture). Assumes eligibility: :func:`sweep_watchlist` applies
    :func:`is_eligible` first, and grading itself happened upstream in the digest.

    The arithmetic is ``_shadow_catalyst.build_candidate``, shared with ``cat_reversal``; ``cat``
    keeps its own identity and ``stop_pct`` (:data:`DEFAULT_PARAMS`).
    """
    p = {**DEFAULT_PARAMS, **(params or {})}
    return _shadow_catalyst.build_candidate(
        row, strategy_id=STRATEGY_ID, stop_pct=p["stop_pct"]
    )


def sweep_watchlist(
    rows: Sequence[WatchlistRow], *, params: Mapping[str, float] | None = None
) -> list[SignalCandidate]:
    """Filter today's watchlist rows to the rule's event, then translate each; deterministic order:
    score desc, symbol asc — as ``ins.sweep_crossings``/``brk20.sweep_daily``.

    The eligibility filter lives HERE, not in the caller's query: it IS the event definition and must
    be unit-testable without a database. The caller may narrow its fetch; this pass is the
    authority, so a widened fetch can never widen the rule.
    """
    return _shadow_catalyst.sweep(
        rows,
        is_eligible=is_eligible,
        translate=lambda r: scan_entry(r, params=params),
    )
