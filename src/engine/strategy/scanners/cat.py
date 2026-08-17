"""``cat`` v2 SHADOW — news-catalyst swing leg (§2.7 amendment, owner-directed 2026-08-18).

The platform's news→origination carve-out, restarted as a SHADOW rule. The §6.1 v1 design (an
intraday +1% price/volume confirmation on an ``originating`` watchlist row) was RETIRED by the
2026-08-18 owner review (WO-18): the confirmation mechanic was refuted 3x on proxies (net
−0.17..−0.44%), catalyst-conditioning made intraday ORB worse, and the drift the news layer is
supposed to capture is a 2-4-WEEK phenomenon that an intraday trigger cannot express. What justifies
restarting the clock at all is the measured originating-grade flow since the 2026-08-05 corroboration
amendment — 3 distinct stories in 9 sessions, **100% non-earnings** event types (rating_change /
order_win / m_and_a), exactly the classes the three recorded refutations (all earnings-anchored)
never tested.

**Architecture — a pure batch rule, like ``brk20``/``ins``, deliberately NOT a :class:`Scanner`.**
The grade is decided EOD-adjacent by ``CatalystDigestJob`` (~08:35), so there is nothing per-bar to
scan and the §3.2.5 (1m bar, ScanContext) protocol does not fit. The digest persists the day's
``catalyst_watchlist``; the window-open//``scan_now`` sweep turns today's ``originating`` rows into
candidates HERE and admits them through ``SignalPreScreen.admit`` so the §3.2.5 dedupe/caps — and the
``catalyst_guard.max_catalyst_entries_day`` cap the pre-screen owns — bind identically (no bypass).

This module holds NO news logic of its own — it never re-grades a row, never reads the watchlist,
never touches a cluster. It is the pure watchlist-row → :class:`SignalCandidate` translation plus the
eligibility filter that defines the rule's event, and nothing else.

PINNED rule (long-only — NSE cash equities cannot be shorted overnight):

* **Event:** a ``catalyst_watchlist`` row with ``grade='originating'`` ∧ ``direction='long'`` ∧
  ``event_age_sessions <= 1``. **One candidate per story:** the age bound IS the single-shot
  semantics — an age-2+ re-grade of the same story never re-originates (it supersedes §2.7 step 5's
  "reappears until the event ages out", which described the retired confirmation design; the rows
  themselves still reappear as context/advisory). The grading conditions (materiality, sentiment
  floor, event-type whitelist, story-level domain corroboration, novelty, universe/surveillance) are
  the digest's, applied upstream against the HASH-VERIFIED ``catalyst_guard``; nothing here re-derives
  them, and thresholds are FROZEN for the whole shadow window (moving one re-opens multiplicity and
  restarts the clock — the ``ins`` lesson).
* **Style/side:** swing (CNC), BUY.

WHICH PRICE ANCHORS WHAT (read this before touching the levels)
---------------------------------------------------------------
Identical in shape to ``ins``, and for the same reason: the validated-fill convention is the
NEXT-SESSION OPEN, which does not exist as a committed, replayable number when the morning sweep
runs. Three prices are in play and only one of them is the entry:

1. ``reference_close`` — the PRIOR SESSION's bhavcopy-final close, read from ``bars_1d`` by the
   caller. At 09:30+ this is literally yesterday's close: the freshest COMMITTED price at admission
   time, and what ``raw_levels.entry`` is computed from. A **pre-open reference for the
   next-session-open fill**, never a limit the market owes us.
2. The live LTP — the §7.1 gate's WO-4 sizing reference already sizes a BUY off ``max(entry, LTP)``,
   so a stock that gapped away from the reference shrinks the size instead of flattering it.
3. The actual fill — which in the shadow there ISN'T one (see the C3 note below). The horizon under
   measurement is T+10/T+20, so minutes of open slippage against the reference are immaterial; that
   is exactly why a stale-by-one-session anchor is acceptable here and would not be for ``orb``.

Levels, pre-registered in the plan:

* ``entry`` = ``round_to_tick(reference_close)`` — the pre-open reference above.
* ``stop``  = ``round_to_tick(entry x (1 - stop_pct/100))``, default **5%**. A **disaster stop**:
  §7.1 sizing needs a risk distance, so one has to exist. 5 rather than the ``ins`` 6 because the
  §7.1 ``per_trade_risk.overnight_gap_mult`` (2.5x) arithmetic applies to a swing entry — the same
  correction made to ``ins`` on 2026-08-17. It is NOT the rule's exit.
* ``target`` = ``None``. The rule has no price target. The v1 ATR-anchored ``rr_target`` band belonged
  to the retired confirmation design; inventing a level to replace it would put a fabricated number
  into the gate's edge math.
* **Exit is TIME**, not price: the §7.1 ``max_holding`` swing cap of 20 trading days, so the EXISTING
  max-holding machinery IS the exit path (``RecommendationPipeline.check_aged_positions``). No exit
  code lives here or anywhere else for ``cat``.

Rounding order is pinned (as for ``brk20``/``ins``): ``entry``/``stop`` are tick-rounded and the
coherence test is applied AFTER rounding, so a stop that collapses onto the entry under tick rounding
emits nothing rather than shipping ``stop >= entry`` into the §7.1 ``levels_coherent`` check.

Score = the row's **weighted materiality**, clamped to [0, 1] — the digest's own strength measure
(already multiplied by ``cat.fanout_weight`` for a fanned-out sector/theme catalyst, i.e. exactly the
number the grade decision used). Scores are only ever compared WITHIN a strategy (the §5.2(a) funnel
ranks by per-strategy score quantile), so this scale needs to agree with no other scanner's.

WHY THIS SHIPS WITHOUT AN EDGE (the shadow's defining property)
---------------------------------------------------------------
There is deliberately **no ``cat.expected_edge_pct``** anywhere — not in ``settings.yaml``, not in
the gate's ``strategy_expected_edge_pct`` map. ``cat`` has no validated edge; measuring one is the
entire point of the shadow. With no target and no registered edge the §7.1 C3 cost check has no edge
basis and **fail-closed-rejects every ``cat`` candidate**: the funnel journals the signal (prescreen
admission IS the validation population), the analyst still writes its thesis (a validation covariate,
<=2 calls/day inside the existing budget), and nothing reaches RECOMMEND. Inventing an edge number to
pass the gate would put a fabricated value into the gate's arithmetic — the exact failure ``ins``
documents against. Wiring an edge, and with it RECOMMEND eligibility, is the §8.6 owner gate,
contingent on the shadow verdict.

VERDICT CRITERIA, PRE-REGISTERED (WO-18 — binding):

* >=30 sessions AND >=20 admitted signals, then a T+10/T+20 net-drift study at next-session-open
  fills with spread-inclusive CNC costs and the WO-3 margin floor.
* Net <= the cost floor at BOTH horizons ⇒ ``cat`` origination is RETIRED and the watchlist stays
  context-only permanently.
* Sustained < 0.2 signals/session for 3 consecutive weeks is a STARVATION finding (the news feeds,
  not the rule) and triggers an early owner check-in rather than silent accumulation — which is what
  the caller's per-sweep visibility line exists to make observable.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from decimal import Decimal, InvalidOperation
from typing import NamedTuple

from ulid import ULID

from engine.strategy.types import RawLevels, SignalCandidate, round_to_tick

STRATEGY_ID = "cat"

#: The grade / direction the rule originates on. Both are the DIGEST's verdicts (§2.7 step 5(ii)),
#: re-checked here only so a caller that fetches the table unfiltered cannot widen the rule.
ORIGINATING_GRADE = "originating"
LONG_DIRECTION = "long"

#: Maximum event age, in completed TRADING SESSIONS, at which a row may originate. **Not a knob and
#: never an envelope row**: this bound IS the single-shot semantics (WO-18) — an age-2+ re-grade of
#: the same story reappears on the watchlist as context, and letting it originate again would enter
#: the same story twice and double-count it in the shadow's signal population.
MAX_EVENT_AGE_SESSIONS = 1

#: Owner knobs (``settings.yaml`` ``cat:`` block), NOT §6.3 envelope rows — and frozen for the whole
#: shadow window (moving a `cat` threshold mid-window re-opens multiplicity and restarts the clock,
#: WO-18). The v1 envelope rows (``confirm_move_pct``/``confirm_vol_mult``/``stop_atr_mult``/
#: ``rr_target``) belong to the retired confirmation design and are read by nothing here.
DEFAULT_PARAMS: dict[str, float] = {
    "stop_pct": 5.0,             # disaster stop; the §7.1 overnight_gap_mult arithmetic (ins reasoning)
    "hold_sessions": 20,         # = the §7.1 swing max_holding cap — the EXISTING machinery is the exit
}


class WatchlistRow(NamedTuple):
    """One ``catalyst_watchlist`` row (§2.7 step 5(ii)), plus the price the caller looked up.

    Everything except ``reference_close`` is a column the digest wrote; ``reference_close`` is the
    PRIOR SESSION's bhavcopy-final close from ``bars_1d``, read at sweep time by the caller (see the
    module docstring's "which price anchors what"). It is ``None`` when the symbol has no prior daily
    bar at all — a thin/absent-history case that must cost this candidate and nothing else.

    ``entry_id`` becomes the candidate's ``catalyst_ref``: the §6.5 audit link back to the exact
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

    Split out from :func:`sweep_watchlist` so the caller can COUNT age-eligible rows for its
    starvation-visibility line using the same predicate the sweep applies — a second, drifting copy
    of the filter in the composition root is exactly how "no signals" stops being diagnosable.
    """
    age = row.event_age_sessions
    return (
        row.grade == ORIGINATING_GRADE
        and row.direction == LONG_DIRECTION
        and age is not None
        and 0 <= int(age) <= MAX_EVENT_AGE_SESSIONS
    )


def scan_entry(
    row: WatchlistRow, *, params: Mapping[str, float] | None = None
) -> SignalCandidate | None:
    """Translate ONE eligible watchlist row into a :class:`SignalCandidate`.

    Fails to ``None`` on a missing/non-positive/non-finite reference price or on tick-rounding
    degeneracy (a stop that does not survive rounding as strictly below the entry) — never raises on
    ordinary data (§3.2.5 fail-to-zero posture). Assumes eligibility: :func:`sweep_watchlist` applies
    :func:`is_eligible` first, and grading itself happened upstream in the digest.
    """
    p = {**DEFAULT_PARAMS, **(params or {})}
    ref = row.reference_close
    if ref is None:
        return None                 # no prior daily bar — no committed price to anchor on
    if not isinstance(ref, Decimal):
        try:
            ref = Decimal(str(ref))
        except (InvalidOperation, ValueError, TypeError):
            return None
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

    # ---- score: the digest's weighted materiality, clamped into the contract's [0, 1]. A row with
    # NO materiality still originates — the GRADE is the origination decision and it was already
    # made — but it ranks at the floor: an unmeasured magnitude may never flatter the funnel.
    materiality = row.materiality
    try:
        score = 0.0 if materiality is None else min(1.0, max(0.0, float(materiality)))
    except (ValueError, TypeError):
        score = 0.0
    if score != score:                      # NaN — the one float that survives min/max unchanged
        score = 0.0

    return SignalCandidate(
        signal_id=str(ULID()),
        strategy_id=STRATEGY_ID,
        symbol=row.symbol,
        side="BUY",
        style="swing",
        raw_levels=RawLevels(entry=entry, stop=stop, target=None),
        score=score,
        catalyst_ref=row.entry_id,
    )


def sweep_watchlist(
    rows: Sequence[WatchlistRow], *, params: Mapping[str, float] | None = None
) -> list[SignalCandidate]:
    """Filter today's watchlist rows to the rule's event, then translate each; deterministic order
    (§9.6): score desc, symbol asc — matching ``ins.sweep_crossings``/``brk20.sweep_daily`` so the
    ranked-admission path sees one convention.

    The eligibility filter lives HERE rather than in the caller's query (where ``ins`` puts its
    ``consumed = 0`` predicate) because for ``cat`` the filter IS the rule's event definition —
    single-shot semantics included — and it has to be unit-testable without a database. The caller
    may still narrow its fetch (``grade='originating'`` is a cheap store-level filter); this pass is
    the authority, so a widened fetch can never widen the rule.
    """
    out: list[SignalCandidate] = []
    for row in sorted((r for r in rows if is_eligible(r)), key=lambda r: r.symbol):
        cand = scan_entry(row, params=params)
        if cand is not None:
            out.append(cand)
    out.sort(key=lambda c: (-c.score, c.symbol))
    return out
