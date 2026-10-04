"""Mechanical translation shared by the two watchlist-driven shadow rules (``cat``, ``cat_reversal``).

WHAT LIVES HERE, AND WHAT DELIBERATELY DOES NOT. Both news shadows turn a ``catalyst_watchlist`` row
into a :class:`SignalCandidate` through the identical arithmetic — Decimal coercion of the reference
price, the ``stop_pct`` sanity bound, tick-rounding, the post-rounding ``stop >= entry`` degeneracy
check, the NaN-safe score clamp, and the candidate construction itself. That arithmetic was copied
byte-for-byte between the two modules and is extracted here so a fix to it (a new degenerate-price
case, a rounding-order correction) lands once instead of drifting into one rule and not the other.

What is NOT shared, and must never be: each rule's ``STRATEGY_ID``, ``DEFAULT_PARAMS``, thresholds,
event definition, journal keying and pre-registered verdict criteria. The two are SEPARATE
experiments with separate clocks — ``cat``'s shadow window opened 2026-08-18 and ``cat_reversal``'s
2026-08-27 — and a future tuning of one must not silently move the other. So every rule-specific
value arrives here as an ARGUMENT from the caller; this module reads no config, owns no default, and
knows nothing about which rule it is serving beyond the ``strategy_id`` string it is handed.

Neither rule is a :class:`~engine.strategy.scanners.base.Scanner`: they are EOD-batch rules with no
per-bar condition (see this package's ``__init__`` docstring), so the per-bar ``scan``/``ScanContext``
protocol does not fit and nothing here inherits from it. This is a plain function module by design.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from decimal import Decimal, InvalidOperation
from typing import Protocol

from ulid import ULID

from engine.strategy.types import RawLevels, SignalCandidate, round_to_tick


class CatalystRow(Protocol):
    """The columns of a watchlist row this module reads — the two rules' COMMON shape.

    Structural, not inherited: ``cat.WatchlistRow`` and ``cat_reversal.WatchlistRow`` stay their own
    :class:`NamedTuple` types (``cat_reversal``'s carries an extra ``reversal_of``) precisely so the
    two event definitions can diverge without a shared tuple forcing a lockstep edit to the other's
    in-flight shadow. This protocol describes what the SHARED arithmetic needs and nothing more.
    """

    @property
    def symbol(self) -> str: ...
    @property
    def entry_id(self) -> str: ...
    @property
    def grade(self) -> str: ...
    @property
    def direction(self) -> str | None: ...
    @property
    def event_age_sessions(self) -> int | None: ...
    @property
    def materiality(self) -> float | None: ...
    @property
    def reference_close(self) -> Decimal | None: ...


def is_originating(
    row: CatalystRow, *, grade: str, direction: str, max_age_sessions: int
) -> bool:
    """The event prefix both rules share: grade ∧ direction ∧ a resolvable age within bound.

    ``cat``'s event is exactly this; ``cat_reversal``'s is this AND a non-empty ``reversal_of``. The
    bounds are the CALLER's constants, so neither rule can widen the other by editing its own.
    """
    age = row.event_age_sessions
    return (
        row.grade == grade
        and row.direction == direction
        and age is not None
        and 0 <= int(age) <= max_age_sessions
    )


def build_candidate(
    row: CatalystRow, *, strategy_id: str, stop_pct: float
) -> SignalCandidate | None:
    """Translate one ELIGIBLE watchlist row into a candidate, or ``None`` if it cannot be priced.

    Fails to ``None`` on a missing/non-positive/non-finite reference price, on a ``stop_pct`` that is
    not a risk distance, or on tick-rounding degeneracy — never raises on ordinary data (§3.2.5
    fail-to-zero posture). Eligibility is the caller's: it has already applied its own event filter.

    Levels are pinned identically for both rules (and for ``brk20``/``ins``): ``entry`` is the
    tick-rounded pre-open reference, ``stop`` sits ``stop_pct`` below it, ``target`` is ``None``
    because neither rule has one, and coherence is checked AFTER rounding so a stop that collapses
    onto the entry emits nothing rather than shipping ``stop >= entry`` into §7.1 ``levels_coherent``.
    """
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

    stop_distance = Decimal(str(stop_pct))
    if stop_distance <= 0 or stop_distance >= 100:
        return None   # a non-positive or total stop is not a risk distance

    # ---- levels: entry anchored on the pre-open reference, stop a fixed % below it, NO target.
    entry = round_to_tick(ref)
    stop = round_to_tick(entry * (Decimal(1) - stop_distance / Decimal(100)))
    if stop >= entry:
        return None   # tick-rounding degenerate (a sub-tick stop distance) — no structural risk

    # ---- score: the winning cluster's weighted materiality, clamped into the contract's [0, 1]. A
    # row with NO materiality still originates — the GRADE is the origination decision and it was
    # made upstream — but it ranks at the floor: an unmeasured magnitude may never flatter the funnel.
    materiality = row.materiality
    try:
        score = 0.0 if materiality is None else min(1.0, max(0.0, float(materiality)))
    except (ValueError, TypeError):
        score = 0.0
    if score != score:
        # NaN belt: with THIS argument order, max(0.0, nan) happens to return 0.0 (CPython keeps
        # the first arg when comparisons are False), so this check is currently unreachable — but
        # max(nan, 0.0) would return nan, so the guard is what survives an innocent argument swap
        # (2026-09-02 review: the previous comment claimed NaN always passes min/max, which is
        # exactly backwards for this ordering and would mislead a refactor).
        score = 0.0

    return SignalCandidate(
        signal_id=str(ULID()),
        strategy_id=strategy_id,
        symbol=row.symbol,
        side="BUY",
        style="swing",
        raw_levels=RawLevels(entry=entry, stop=stop, target=None),
        score=score,
        catalyst_ref=row.entry_id,
    )


def sweep[RowT: CatalystRow](
    rows: Sequence[RowT],
    *,
    is_eligible: Callable[[RowT], bool],
    translate: Callable[[RowT], SignalCandidate | None],
) -> list[SignalCandidate]:
    """Filter rows to the caller's event, translate each, and order deterministically (§9.6).

    Order is score desc, symbol asc — matching ``ins.sweep_crossings``/``brk20.sweep_daily`` so the
    ranked-admission path sees one convention. The intermediate symbol-ascending pass keeps the
    ULID-minting order a pure function of the input set, so a replay of the same rows mints the same
    candidates in the same sequence.
    """
    out: list[SignalCandidate] = []
    for row in sorted((r for r in rows if is_eligible(r)), key=lambda r: r.symbol):
        cand = translate(row)
        if cand is not None:
            out.append(cand)
    out.sort(key=lambda c: (-c.score, c.symbol))
    return out
