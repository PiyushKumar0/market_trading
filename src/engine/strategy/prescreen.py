"""§3.2.5 ``SignalPreScreen`` (D5) — the deterministic trigger that makes Tier-1 event-driven.

``on_bar(bar) -> list[SignalCandidate]`` (plan-PINNED signature) runs every enabled scanner over the
bar, dedupes on (symbol, strategy, day), applies per-day candidate caps, and publishes each accepted
candidate on the canonical ``"signal.candidate"`` topic. Scanners are pure (no I/O — see
``scanners.base``); everything they need for the bar is assembled once by the injected
``context_provider`` (the seam to ``MarketStore``/``FeatureEngine``/calendar — DuckDB reads happen
there, never inside a scanner).

Admission ORDER (WO-1, 2026-08-13): within one admission BATCH — one ``_scan(bar)`` call, one
:meth:`admit` call — candidates are admitted **score-descending**, not in scanner/arrival order, so a
binding cap keeps the batch's best rather than its first. ``SignalCandidate.score`` was previously
computed, logged and read by no decision at all (audit finding F1). Ties keep emission order
(``sorted`` is stable), so §9.6 replay determinism is unchanged, and the returned list is in the same
ranked order — the caller publishes best-first, which is what the §5.2(a) forward cap then sees.
``admission_mode="arrival"`` is the rollback flag to the pre-WO-1 behaviour. Cross-BATCH FORWARD
ordering is still not this class's job — the forward queue in ``engine.ops.pipeline`` owns which
admitted candidate the analyst sees next, and in what order. Cross-batch ADMISSION is, since
2026-08-27; see the displacement section below for why the two had to be split.

Cap displacement (2026-08-27)
-----------------------------
THE BUG. The per-strategy day cap was "whoever showed up first in a 90-second burst wins the whole
day". Live 2026-08-27: all 7 of ``orb``'s admission slots were charged between 09:46:06 and 09:47:05,
and TATACONSUM (score 0.80, 12:28) and KALYANKJIL (**score 1.0**, 12:49) were then refused outright
with ``prescreen_cap_suppressed cap="strategy_day"`` — zero chance regardless of quality, three hours
later, with no trace anywhere but the raw log line. A 12:49 ``orb`` fire is not an edge case: the
scanner's entry window is 09:30–14:30 and only its RANGE is fixed to the first 30 minutes, so ``orb``
is designed to fire all day into a cap that is designed to be gone by 09:47.

WHY IT COULD NOT BE FIXED IN THE PIPELINE. The forward queue already ranks across time
(``_forward_key``), but a candidate refused HERE never reaches it — the pipeline never sees the
symbol at all. Only this class knows the cap is full, so only this class can decide that a later
arrival deserves the slot more than an incumbent.

THE RULE. When a cap would refuse a NEW pair, look for a same-strategy pair that is charged, has
NEVER BEEN EVALUATED — or was evaluated and DECLINED (:meth:`decline`, 2026-09-04) — carries no
``catalyst_ref``, and scores at least ``displacement_margin`` BELOW the arrival. Evict the worst such
pair (lowest score, then earliest admission — a total order) and give the arrival its slot.
Everything else refuses exactly as before. A declined incumbent's eviction is a NEW analyst call,
so it is charged to the day-level displacement budget (and bounded by the forward cap), not to the
per-strategy churn budget in point 2 — on 2026-09-03 five declined evaluations held all five
``orb`` slots from 11:30 to the close with that budget already spent.

WHAT MAKES THIS SAFE, in the four places it could have gone wrong:

1. *An evaluated candidate is permanent.* :meth:`claim_slot` is the pipeline's atomic compare-and-set
   at ``_take_forward_slot`` — the single instant an analyst call is committed. It runs under THIS
   lock, so a pair is either claimed (and thereafter undisplaceable) or displaced, never both. The
   loser is told: a displaced pair's ``claim_slot`` returns ``False`` and the pipeline skips it, so a
   freed slot can never be spent twice. This is the same load-bearing invariant :meth:`rearm` and the
   TTL refund rest on (2026-07-29, 2026-08-27) and it is not weakened here.
2. *The spam bound survives.* This is the first decrement path ``_charged`` has ever had — the
   trade-window gate's docstring below still says one "would erode the spam bound", and for a REFUND
   that remains true. A displacement is not a refund: it is a SWAP, and the charged count is
   invariant across it, so the number of candidates that can ever reach the analyst is still exactly
   the cap. What a swap does inflate is PUBLICATIONS, so displacements carry TWO derived budgets, one
   per binding cap — a strategy's own ``max_per_strategy_day``, and ``max_candidates_per_day`` for
   the day as a whole. Both are needed: displacement also fires on a full DAY cap, so a per-strategy
   budget alone would let N strategies each spend theirs against the one day cap. Together they bound
   admissions at ≤ 2× cap per strategy (``orb``: ≤ 14 — 0 while orb is parked, 2026-09-12; the bound
   is 2× whatever the cap says) AND ≤ 2× the day cap overall, with no new owner knob to get out of
   sync with the caps they are derived from.
3. *The §2.7 ref set only grows.* A candidate carrying a ``catalyst_ref`` is never evicted, so
   ``_catalyst_refs`` never shrinks and the anti-manipulation cap cannot be churned. An arrival that
   carries one still faces that cap on its own after winning the displacement.
4. *No cross-strategy comparison.* The victim is always the same strategy as the arrival, so only
   scores that WO-1 already blesses as comparable ("comparable inside a strategy, not across them")
   are ever compared — even when it is the aggregate day cap that is binding.

Determinism (§9.6) is analysed in :meth:`_displacement_scan_locked`; the short version is that a
pure bar-stream replay is byte-identical, and the live coupling is the pre-existing
``hydrate``/``rearm`` carve-out, not a new one.

LIMIT — RESOLVED 2026-09-02: ``orb``'s score used to SATURATE (a large share of live fires at
exactly 1.0), so against seven incumbents all at 1.0 nothing displaced anything and this fix was
bounded by it — observed again live on 09-01 (PERSISTENT/HCLTECH/INFY at 1.0 refused) and 09-02
(IDEA/VMM/OIL at 1.0 refused). Two owner-directed changes the same day close it: the orb score is
now a saturation-free squash (``scanners/orb.py`` — scores discriminate, so the margin means
something), and the cap-release schedule below stops the window-open burst from spending the whole
day's sub-cap in its first minute.

Cap release schedule (owner-directed 2026-09-02; INERT for ``orb`` while it is parked, 2026-09-12)
--------------------------------------------------------------------------------------------------
``cap_release_schedule`` (constructor / ``strategy.prescreen`` config) lists CUMULATIVE per-strategy
sub-cap tranches by IST time-of-day, e.g. ``orb: {"10:00": 3, "11:30": 5, "13:00": 7}``: at most 3
orb pairs may charge before 11:30, 5 before 13:00, 7 after — so midday/afternoon fires are
guaranteed a contested seat instead of racing a burst that is over by the window's first minute
(the third recurrence of the 2026-08-27 lockout, three sessions running, was the trigger). The
effective cap is ``min(max_per_strategy_day, released tranche)`` — a schedule can only ever HOLD
BACK capacity, never add any, so every spam bound and displacement budget derived from the flat cap
is untouched. The release time is the candidate's BAR time (``_scan`` threads ``bar.ts_minute``
through; the batch :meth:`admit` path carries no bar and keeps flat caps), never a Clock — §9.6
replay determinism holds by construction. Unscheduled strategies and an absent config are byte-for-
byte the prior behaviour; tranche values must be non-decreasing (a shrinking tranche would demand
the cap decrement path the spam bound forbids). The tranche instant is the candidate's ENTRY time
(``ts_minute + 1m`` — the file-wide boundary convention), and the per-strategy displacement budget
derives from the RELEASED tranche, not the flat cap (both 2026-09-02 review findings). The ``orb``
schedule shipped in ``settings.yaml`` is INERT while ``orb`` is parked — ``min(0, tranche) = 0``, and
the park gate refuses before ``_cap_for`` is ever consulted — and is kept for the re-enable only.

Known, accepted limits of the pair of fixes (2026-09-02 review, documented rather than redesigned):
an incumbent scoring ≥ (1 − margin) — under orb's squash, a ≥27×-median-volume print — can never be
displaced, because no arrival on a sub-1.0 scale can clear an absolute margin above it; that band
held MOST live fires under the old clamp and now holds only genuinely extraordinary ones, which
arguably deserve their seat. And on a special session whose window opens after the last release
time (muhurat ~13:45), the schedule is inert and the full flat cap applies from the first bar — a
45-minute session cannot be meaningfully staggered.

Determinism (§9.6): the pre-screen takes NO Clock — "today" is ``bar.ts_minute.date()``, so a replay
of the same bar stream reproduces the same dedupe/cap decisions byte-for-byte (modulo the minted
``signal_id`` ULIDs). Per-day state (seen set, counters) resets when the bar date changes. The
trade-window gate below preserves this: it reads ``ScanContext.trade_window`` and compares it against
``bar.ts_minute``, never a Clock — the same bar-derived shape ``OrbScanner.scan`` already uses.

Trade-window gate (2026-08-18)
------------------------------
A candidate whose bar-close instant falls OUTSIDE the owner trade window is refused here, before it
can charge anything. The caps bind on unique (symbol, strategy) pairs and are deliberately NEVER
refunded — "the spam bound counts attempts, not outcomes" (:meth:`rearm`) — so a publication that the
pipeline is structurally guaranteed to drop as ``signal_candidate_out_of_window`` used to spend a day
slot permanently on a candidate nothing could ever act upon. Observed live 2026-08-18: 20/20 day
slots were spent by 10:10:49 (15 of them before the window opened at all), after which every further
candidate was refused for the rest of the session — 2,549 suppressions, 47 distinct ``orb`` pairs at
score >= 0.99, and zero proposals.

This gate PREVENTS the charge; it deliberately does not refund one. Refunding would require a
decrement path for ``_charged``, which does not exist anywhere by design — adding one would erode the
same spam bound. The pipeline's own window check (``engine.ops.pipeline.on_signal_candidate``) is
unchanged and remains the authoritative gate: it reads the live Clock, this reads bar time, and at a
window edge they can disagree by up to one minute. That is intended defence in depth — this gate
exists to protect the day's BUDGET, not to decide tradability.

Park gate (owner-directed 2026-09-12)
-------------------------------------
A per-strategy ``max_per_strategy_day`` of **0** parks that strategy: it admits nothing, at any hour,
and — like the window gate — charges nothing. It is the FIRST gate on the accept spine (ahead of the
window, which is per pair; the park is per strategy), so every raw fire of a parked strategy lands in
exactly one counter, ``suppressed_disabled``, instead of splitting across ``suppressed_window`` and
``suppressed_cap`` — a parked strategy and a starved one must not read the same in the funnel. The
sole exception is a pair already in ``_seen`` (a mid-session park after :meth:`hydrate` restored the
morning's publications): those fall through to ``suppressed_dedupe``, because their slot was spent
before the park existed. One ``prescreen_strategy_disabled`` INFO per strategy per day carries the
fact; the counter carries the volume. :meth:`sweep` also drops a parked strategy's
:meth:`Scanner.pending` arm levels — they can never arm, and that message is owner-facing.
``default: 0`` stays a ValueError (see :meth:`_parse_caps`): parking every strategy at once is a
kill-switch decision, not a cap edit. ``orb`` is parked; the re-enable is a plan §8.6 decision.

Async surface (§3.2 convention 4): ``handle_bar`` is the ``"bar.1m"`` bus handler — it offloads the
pandas-heavy scan to a worker thread (the loop is never blocked, §2.2) and awaits publication.
Sync/replay callers use ``on_bar`` directly. The integrator wires exactly ONE of the two paths
(``bus.subscribe("bar.1m", prescreen.handle_bar)`` in ``engine.ops``).

``cat`` catalyst seam (§3.2.5/§2.7, wired 2026-08-18 with the WO-18 v2 shadow): candidates carrying
a ``catalyst_ref`` must ADDITIONALLY respect ``catalyst_guard.max_catalyst_entries_day`` — enforced
HERE, before publication, never inside ``RiskGate`` (GateContext stays news-free, §2.4 item 4). The
value arrives through ``catalyst_cap_fn``, which the composition root binds to the HASH-VERIFIED
``limits.yaml`` block (``ProtectedStore.load_verified`` via ``LimitsEngine.catalyst_guard``) — the
anti-manipulation surface is read at the ENFORCEMENT site, exactly as ``CatalystDigestJob`` reads it,
and never accepted as a plain constructor number. Unwired or unverifiable ⇒ catalyst candidates are
REFUSED, never admitted uncapped (D7: the news carve-out fails to LESS activity). ``cat`` is a batch
rule and is deliberately absent from ``SCANNER_REGISTRY``; it reaches this class through
:meth:`admit`, so the guard sits on the shared accept spine and binds both paths.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime, time, timedelta
from typing import Any, NamedTuple

from pydantic import BaseModel

from engine.core.eventbus import EventBus
from engine.core.log import get_logger
from engine.core.types import Bar
from engine.strategy.scanners.base import Scanner
from engine.strategy.types import PendingSetup, ScanContext, SignalCandidate

_log = get_logger("engine.strategy.prescreen")

#: Canonical EventBus topic (§3.2.1).
SIGNAL_CANDIDATE_TOPIC = "signal.candidate"

#: Assembles a :class:`ScanContext` for one bar (MarketStore/calendar/FeatureEngine reads live here).
ContextProvider = Callable[[Bar], ScanContext]

#: Publication-admission orders (WO-1). ``ranked`` = score-descending within an admission batch;
#: ``arrival`` = the pre-WO-1 scanner/emission order, kept ONLY as the config rollback.
ADMISSION_MODES = ("ranked", "arrival")

#: Key in a ``max_per_strategy_day`` MAPPING that binds every strategy without its own entry. Not a
#: strategy id (no scanner may ever be called "default") — it is the structural ceiling that makes
#: the ≤40%-of-the-day guarantee hold for strategies added after the config was written.
DEFAULT_CAP_KEY = "default"

#: FALLBACK for ``displacement_margin`` — how much better a later arrival must score to take a full
#: cap's slot from an unevaluated incumbent of the same strategy (2026-08-27, module docstring).
#: The SHIPPED value is ``strategy.prescreen.displacement_margin`` in ``config/settings.yaml``, an
#: owner knob (§6.3) alongside ``max_per_strategy_day``, and the reasoning for 0.10 — plus the
#: orb-saturation caveat that bounds what any margin can do — lives there with it. This constant
#: exists so a directly-constructed pre-screen (tests, replay, backtest) behaves like the shipped
#: engine without having to load settings; it is deliberately the same number.
DEFAULT_DISPLACEMENT_MARGIN = 0.10

#: Scores are compared in integer BASIS POINTS, never as raw float arithmetic (2026-08-27). This is
#: not defensive noise: ``0.90 - 0.80`` is ``0.09999999999999998`` in IEEE-754, so a float margin
#: test silently refuses a displacement that is exactly at the boundary — and refuses it for some
#: pairs of scores and not others, which is precisely the kind of "deterministic but arbitrary"
#: behaviour §9.1/§9.6 exist to keep out of policy decisions. Scanner scores are clamped to [0, 1]
#: and none carries four significant decimals, so 1e-4 resolution loses nothing real.
SCORE_BASIS = 10_000


def score_bp(score: float) -> int:
    """A [0, 1] score as an integer basis-point count — the only form displacement compares."""
    return int(round(float(score) * SCORE_BASIS))


def hydrated_catalyst_ref(symbol: str) -> str:
    """The stand-in ``catalyst_ref`` :meth:`SignalPreScreen.hydrate` charges for ``symbol``.

    The day-slot journal records (symbol, strategy) pairs and carries no ref column, so a restart
    cannot read back the refs the §2.7 guard actually spent. It CAN read which of those pairs
    belonged to a catalyst strategy, and the twins one story projects share their symbol as well as
    their ref (one watchlist row per symbol per day) — so distinct symbols reconstruct the distinct-
    ref count. See :meth:`SignalPreScreen.hydrate` for the direction this is wrong in.
    """
    return f"hydrated:{symbol}"


class _DisplacementScan(NamedTuple):
    """One pass over ``_admitted``: who may be evicted, and how many could have been.

    ``victim`` is the eviction decision; ``displaceable`` is the refusal diagnostic — ``None`` when
    displacement is disabled entirely, which is NOT the same reading as ``0`` ("every incumbent has
    really been evaluated"). They are returned together because they are two readings of the same
    walk — see :meth:`SignalPreScreen._displacement_scan_locked`.
    """

    victim: tuple[str, str] | None
    displaceable: int | None


def bar_in_trade_window(bar: Bar, window: tuple[datetime, datetime] | None) -> bool:
    """Is ``bar``'s CLOSE instant inside the owner trade window? (§7.1, 2026-08-18)

    Bar-derived and Clock-free, so §9.6 replay determinism is preserved. The instant tested is
    ``ts_minute + 1m`` — the moment an entry off this bar could actually be placed — which is exactly
    the ``entry_dt`` convention :meth:`~engine.strategy.scanners.orb.OrbScanner.scan` already uses, so
    the pre-screen and orb agree on what "in the window" means down to the minute.

    ``None`` (not a trading day, or an unreadable window) is FALSE: no window ⇒ nothing may originate,
    the same fail-to-zero direction every other §2.7/§7.1 guard takes.
    """
    if window is None:
        return False
    entry_dt = bar.ts_minute + timedelta(minutes=1)
    return window[0] <= entry_dt <= window[1]


class SignalPreScreen:
    """Run enabled scanners per bar; dedupe; cap; publish (§3.2.5).

    Parameters
    ----------
    scanners:
        The enabled scanner instances (``scanners.build_enabled_scanners``). Order is preserved as
        the TIE-BREAK only: since WO-1 a binding cap keeps the batch's highest-scored candidates,
        and equal scores fall back to this scanner/emission order (deterministically).
    context_provider:
        Builds the per-bar :class:`ScanContext`.
    bus:
        Optional EventBus; accepted candidates are published on ``"signal.candidate"``.
    max_candidates_per_day:
        Total accepted candidates per day, all strategies/symbols combined [settings tunable —
        integrator: ``strategy.prescreen.max_candidates_per_day``]. Keeps a wild day from spamming
        Tier-1 (D5 exists to make LLM calls event-driven AND bounded).
    max_per_strategy_day:
        Per-strategy daily sub-cap [settings tunable — ``strategy.prescreen.max_per_strategy_day``].
        ``None`` = no per-strategy cap; an ``int`` = the same cap for every strategy; a MAPPING
        ``{strategy_id: cap}`` = per-strategy values, with the reserved key ``"default"`` binding
        every strategy that has no line of its own (WO-1 (iii): owner knob, NOT learner-movable —
        §6.3). The mapping form is what structurally prevents one strategy taking the whole day.
        A per-strategy ``0`` PARKS that strategy: its candidates are refused before any slot is
        charged, counted under ``suppressed_disabled`` and logged once a day as
        ``prescreen_strategy_disabled`` (2026-09-12, ``orb``). ``default: 0`` is a ValueError.
    admission_mode:
        ``"ranked"`` (default) = score-descending admission within a batch; ``"arrival"`` = the
        pre-WO-1 order. The rollback knob named in WO-1's risk note, nothing else.
    catalyst_cap_fn:
        Reads ``catalyst_guard.max_catalyst_entries_day`` from the hash-verified ``limits.yaml`` at
        the enforcement site (§2.4 item 1). Binds ONLY candidates with a ``catalyst_ref`` — the §2.7
        news→origination carve-out. ``None``, or a call that raises (an unverifiable protected
        store), refuses every catalyst candidate: an anti-manipulation surface that cannot be read
        must not degrade into "no cap". Price baselines are untouched either way (E5).
    raw_counts_loader:
        ``day -> {strategy_id: raw fires}`` reader for the persisted WO-9 raw counters
        (``funnel_raw_counts``, 2026-08-21), called on every day ROLL — which includes the first
        roll of a fresh process. The composition root binds it to the state DB; replay/backtest
        paths leave it ``None`` and stay pure, so §9.6 bar-stream determinism is untouched. It is
        the READ half of a pair: whatever hydrates here is what
        :meth:`~engine.ops.pipeline.RecommendationPipeline._flush_funnel_raw` later writes back as
        the day's absolute total, so wiring the flush WITHOUT this loader would have each restart
        overwrite the day's persisted count with the new process's smaller one.
    displacement_margin:
        How much better a later candidate must score to take a full cap's admission slot from an
        unevaluated incumbent of the same strategy [settings tunable —
        ``strategy.prescreen.displacement_margin``; owner knob, NOT learner-movable (§6.3), for the
        same reason ``max_per_strategy_day`` is not: it decides which candidates get evaluated at
        all]. ``None`` DISABLES displacement, restoring the pre-2026-08-27 behaviour where the first
        candidates to fire keep the strategy's slots for the whole session — the rollback flag, the
        way ``admission_mode`` is WO-1's. The shipped 0.10 and the reasoning behind it live in
        ``config/settings.yaml``; :data:`DEFAULT_DISPLACEMENT_MARGIN` mirrors it for direct
        construction. See the cap-displacement section of the module docstring for the mechanism.
    """

    def __init__(
        self,
        scanners: Sequence[Scanner],
        context_provider: ContextProvider,
        bus: EventBus | None = None,
        *,
        max_candidates_per_day: int = 20,
        max_per_strategy_day: int | Mapping[str, int] | None = None,
        admission_mode: str = "ranked",
        catalyst_cap_fn: Callable[[], int] | None = None,
        raw_counts_loader: Callable[[date], Mapping[str, int]] | None = None,
        displacement_margin: float | None = DEFAULT_DISPLACEMENT_MARGIN,
        cap_release_schedule: Mapping[str, Mapping[str, int]] | None = None,
    ) -> None:
        if max_candidates_per_day < 1:
            raise ValueError("max_candidates_per_day must be >= 1")
        if displacement_margin is not None and not 0.0 < float(displacement_margin) <= 1.0:
            raise ValueError("displacement_margin must be in (0, 1], or None to disable")
        if admission_mode not in ADMISSION_MODES:
            raise ValueError(f"admission_mode must be one of {ADMISSION_MODES}, got {admission_mode!r}")
        default_cap, per_strategy = self._parse_caps(max_per_strategy_day)
        self._scanners = list(scanners)
        self._context_provider = context_provider
        self._bus = bus
        self._max_day = max_candidates_per_day
        self._max_strategy_day = default_cap                  # binds strategies without a line
        self._strategy_caps = per_strategy                    # explicit per-strategy overrides
        self._admission_mode = admission_mode
        self._catalyst_cap_fn = catalyst_cap_fn               # §7.1 catalyst_guard, read at use
        self._raw_counts_loader = raw_counts_loader           # WO-9 raw counters, restored on roll
        #: ``None`` disables displacement entirely — the pre-2026-08-27 "first 90 seconds win the
        #: day" behaviour, kept as the rollback knob the way ``admission_mode`` is for WO-1.
        self._displacement_margin = (
            None if displacement_margin is None else float(displacement_margin)
        )
        #: Cumulative per-strategy cap tranches by IST bar time-of-day (owner-directed 2026-09-02,
        #: after three consecutive sessions of window-open cap lockout — see the schedule section of
        #: the module docstring). ``{}`` = no schedule anywhere = flat caps, the exact prior shape.
        self._cap_schedule = self._parse_cap_schedule(cap_release_schedule)
        # Per-day state (reset on bar-date change). Lock: handle_bar offloads to worker threads.
        self._lock = threading.Lock()
        self._day: date | None = None
        self._seen: set[tuple[str, str]] = set()      # (symbol, strategy_id) already emitted today
        #: Pairs that have been CHARGED against the daily caps (2026-07-29 owner decision:
        #: a re-armed pair re-publishes within its already-paid quota — caps count unique pairs,
        #: not publications, or re-arm cycles would exhaust the day cap without one evaluation).
        self._charged: set[tuple[str, str]] = set()
        self._count_day = 0
        self._count_by_strategy: dict[str, int] = {}
        #: DISTINCT ``catalyst_ref`` values charged today — the
        #: ``catalyst_guard.max_catalyst_entries_day`` budget (§2.7), which the cap compares by
        #: ``len``. Deliberately keyed on the FIELD, not on ``strategy_id == "cat"``: the guard
        #: bounds news-originated ENTRIES, and no strategy id may be the thing that decides whether
        #: the news guard applies. A SET rather than a counter because one watchlist row projects
        #: into both a ``cat`` and a ``cat_reversal`` candidate sharing its ``entry_id`` as their
        #: ref — two experiments on ONE story, which must pay one of the day's two entries, not
        #: both. Restored across a restart by :meth:`hydrate`.
        self._catalyst_refs: set[str] = set()
        # ---------------------------------------------------------- cap displacement (2026-08-27)
        #: Every CHARGED pair -> (score at admission, admission sequence, carried a catalyst_ref).
        #: The displaceable population and the total order over it both come from here, and every
        #: field is a fact about the candidate stream alone — which is what keeps §9.6 replay exact.
        self._admitted: dict[tuple[str, str], tuple[float, int, bool]] = {}
        #: Monotonic admission counter — the final tie-break when two pairs share a score.
        self._admit_seq = 0
        #: Pairs whose slot is PERMANENT: an analyst call was committed for them (:meth:`claim_slot`),
        #: or they were restored by :meth:`hydrate` and this process cannot prove otherwise. Never
        #: displaceable. This set IS invariant #1 (2026-07-29 / 2026-08-27).
        self._evaluated: set[tuple[str, str]] = set()
        #: Evaluated pairs the analyst DECLINED (:meth:`decline`, 2026-09-04): still charged and seen,
        #: but displaceable again — a no_action verdict holds no position and so holds no slot.
        self._declined: set[tuple[str, str]] = set()
        #: Displacements SPENT per strategy today, budgeted against that strategy's own cap so the
        #: swap cannot become an unbounded publication treadmill (module docstring, point 2).
        self._displacements: dict[str, int] = {}
        #: Displacements spent across ALL strategies today, budgeted against ``max_candidates_per_day``
        #: — the second half of point 2. Displacement fires on a full DAY cap as well as a full
        #: per-strategy one, and it is also the only bound left when no per-strategy cap is
        #: configured at all (the replay/backtest construction, where ``_cap_for`` is ``None``).
        self._day_displacements = 0
        #: Every pair displaced today — read by :meth:`claim_slot` to refuse a slot the pipeline is
        #: still holding a queue pointer to. Kept for the whole day, unlike ``_displaced_pending``.
        self._displaced: set[tuple[str, str]] = set()
        #: Displacement notices not yet collected by the pipeline (:meth:`take_displaced`). The
        #: pipeline drains this on the EVENT LOOP, which is the only place ``_pending_forwards`` may
        #: be touched — admission itself runs in a scan worker thread and must never reach into it.
        self._displaced_pending: list[tuple[str, str]] = []
        #: WO-9 funnel counters, per strategy, for the CURRENT day. ``_raw_by_strategy`` alone is
        #: RESTART-PROOF (2026-08-21): it is seeded from ``funnel_raw_counts`` on every day roll and
        #: flushed back by the drain tick, because a raw count that resets is indistinguishable from
        #: "nothing fired" — the exact ambiguity WO-9 exists to remove, and it cost the nightly line
        #: 4 of the last 6 trade days. The rest stay process-scoped: they describe what THIS
        #: process's admission spine did, and the day-slot journal is their restart-proof record.
        self._raw_by_strategy: dict[str, int] = {}
        self._published_scores: dict[str, list[float]] = {}
        self._suppressed_cap: dict[str, int] = {}
        self._suppressed_dedupe: dict[str, int] = {}
        #: Candidates refused because the owner trade window was shut (2026-08-18). Distinct from
        #: ``_suppressed_cap``: these cost NOTHING — the whole point is that no slot was charged — so
        #: a large number here is healthy budget protection, where a large ``_suppressed_cap`` is
        #: starvation. Keeping them apart is what makes the WO-9 funnel line readable.
        self._suppressed_window: dict[str, int] = {}
        #: Strategies already logged as window-refused today (one INFO per strategy per day).
        self._window_logged: set[str] = set()
        #: Candidates refused because their strategy is PARKED — its own ``max_per_strategy_day``
        #: line is 0 (2026-09-12, ``orb``). Apart from ``_suppressed_cap`` for the same reason
        #: ``_suppressed_window`` is: no slot is charged, so this is a configured OFF switch, and a
        #: funnel that folded it into the cap counter would read a parked strategy as starvation.
        self._suppressed_disabled: dict[str, int] = {}
        #: Strategies already logged as parked today (one INFO per strategy per day).
        self._disabled_logged: set[str] = set()

    @staticmethod
    def _parse_caps(
        spec: int | Mapping[str, int] | None,
    ) -> tuple[int | None, dict[str, int]]:
        """Normalise the ``max_per_strategy_day`` scalar/mapping/None into (default, overrides).

        A PER-STRATEGY ``0`` is legal and means PARKED — admit nothing for that strategy today
        (2026-09-12, ``orb``). ``default: 0`` and the scalar form stay a ValueError: those bind every
        strategy without a line of its own, so a typo there would park the whole platform silently,
        and stopping origination platform-wide is a kill-switch decision, not a cap edit.
        """
        if spec is None:
            return None, {}
        if isinstance(spec, Mapping):
            default_cap: int | None = None
            caps: dict[str, int] = {}
            for key, value in spec.items():
                if value is None:                     # a blank YAML line means "no cap here"
                    continue
                cap = int(value)
                floor = 1 if str(key) == DEFAULT_CAP_KEY else 0
                if cap < floor:
                    raise ValueError(f"max_per_strategy_day[{key!r}] must be >= {floor}, got {cap}")
                if str(key) == DEFAULT_CAP_KEY:
                    default_cap = cap
                else:
                    caps[str(key)] = cap
            return default_cap, caps
        cap = int(spec)
        if cap < 1:
            raise ValueError("max_per_strategy_day must be >= 1 (or None)")
        return cap, {}

    @staticmethod
    def _parse_cap_schedule(
        spec: Mapping[str, Mapping[str, int]] | None,
    ) -> dict[str, list[tuple[time, int]]]:
        """Normalise ``cap_release_schedule`` into per-strategy (release_time, cumulative_cap)
        lists, sorted by time. Malformed input is a constructor error, never a silent behaviour:
        times must parse ``HH:MM``, values must be >= 1 and NON-DECREASING with time (a shrinking
        tranche would demand a decrement path the spam bound forbids)."""
        out: dict[str, list[tuple[time, int]]] = {}
        for strategy_id, entries in (spec or {}).items():
            parsed: list[tuple[time, int]] = []
            for key, value in entries.items():
                try:
                    hh, mm = str(key).split(":")
                    release = time(int(hh), int(mm))
                except ValueError as exc:
                    raise ValueError(
                        f"cap_release_schedule[{strategy_id!r}]: bad time key {key!r} (want HH:MM)"
                    ) from exc
                cap = int(value)
                if cap < 1:
                    raise ValueError(
                        f"cap_release_schedule[{strategy_id!r}][{key!r}] must be >= 1, got {cap}"
                    )
                parsed.append((release, cap))
            parsed.sort()
            if len({t for t, _ in parsed}) != len(parsed):
                # Two distinct string keys parsing to one instant ("09:00"/"9:00") would silently
                # keep only the later-sorted entry (2026-09-02 review) — a config typo must be loud.
                raise ValueError(
                    f"cap_release_schedule[{strategy_id!r}] has duplicate release times"
                )
            for (_, lo), (_, hi) in zip(parsed, parsed[1:]):
                if hi < lo:
                    raise ValueError(
                        f"cap_release_schedule[{strategy_id!r}] must be non-decreasing over time"
                    )
            if parsed:
                out[str(strategy_id)] = parsed
        return out

    def _cap_for(self, strategy_id: str, at: datetime | None = None) -> int | None:
        """The publication sub-cap binding ``strategy_id`` — its own line, else ``default``.

        With a ``cap_release_schedule`` line AND a bar-derived ``at`` (2026-09-02): the effective
        cap is ``min(flat cap, cumulative tranche at at.time())`` — a candidate earlier than the
        first release competes for the first tranche. ``at=None`` (the batch :meth:`admit` path,
        which carries no bar) and unscheduled strategies keep the flat cap exactly as before; the
        schedule can only ever HOLD BACK capacity, never add any. ``at`` is bar time, never a
        Clock — §9.6 replay determinism is preserved by construction."""
        flat = self._strategy_caps.get(strategy_id, self._max_strategy_day)
        schedule = self._cap_schedule.get(strategy_id)
        if schedule is None or at is None:
            return flat
        released = schedule[0][1]                       # pre-first-release ⇒ the opening tranche
        # ENTRY time, not bar-close time (2026-09-02 review): this file's own convention for every
        # time boundary is entry_dt = ts_minute + 1m — "the moment an entry off this bar could be
        # placed" (bar_in_trade_window, OrbScanner.scan). An 11:29-close bar enters AT 11:30 and
        # gets the 11:30 tranche.
        bar_tod = (at + timedelta(minutes=1)).time()    # naive IST clock time (bars are IST-aware)
        for release, cap in schedule:
            if release <= bar_tod:
                released = cap
        return released if flat is None else min(flat, released)

    def _catalyst_cap(self) -> int | None:
        """``catalyst_guard.max_catalyst_entries_day``, or ``None`` meaning REFUSE (§2.7/§2.4).

        Read per admission from the hash-verified protected store, so an owner change lands on the
        next :meth:`~engine.risk.limits.LimitsEngine.reload` without a restart. ``None`` is returned
        for an unwired reader, an unverifiable store (``IntegrityError``) and a nonsense value alike:
        all three mean the anti-manipulation surface could not be established, and the D7 direction
        for that is no news-originated entries at all — never an uncapped one.
        """
        if self._catalyst_cap_fn is None:
            return None
        try:
            cap = int(self._catalyst_cap_fn())
        except Exception as exc:  # noqa: BLE001 - an unreadable guard refuses; it never kills the scan
            _log.warning("catalyst_guard_unreadable", error=str(exc))
            return None
        return cap if cap >= 0 else None

    # ------------------------------------------------------------------ pinned sync surface
    def on_bar(self, bar: Bar) -> list[SignalCandidate]:
        """Scan one bar; return accepted candidates and publish them (plan-pinned surface, §3.2.5)."""
        accepted = self._scan(bar)
        if self._bus is not None:
            for cand in accepted:
                self._bus.publish(SIGNAL_CANDIDATE_TOPIC, cand)
        return accepted

    # ------------------------------------------------------------------ sweep addendum (2026-07-29)
    def seen_today(self) -> int:
        """Number of (symbol, strategy) day slots spent so far today — the sweep verdict's
        "already evaluated" count."""
        with self._lock:
            return len(self._seen)

    def rearm(self, symbol: str, strategy_id: str) -> bool:
        """Give ``(symbol, strategy)`` its once-per-day publication back (owner-directed 2026-07-29).

        For the pipeline's analyst INFRASTRUCTURE failures only (timeout/SDK death/schema battles):
        the condition was never actually evaluated, so consuming the day slot would silence a
        still-true setup for the rest of the day (observed 2026-07-29: six candidates burned by a
        broken analyst could not re-publish in the repaired window). An analyst that RAN and said
        no_action, or a gate rejection, is a real evaluation — those must NOT re-arm. Daily caps are
        deliberately not refunded (the spam bound counts attempts, not outcomes).

        The pair also leaves ``_evaluated`` (2026-08-28). :meth:`claim_slot` adds it at DISPATCH, one
        instant before the call this method exists to say never happened, so leaving it there made
        the pair permanently undisplaceable on the strength of an evaluation nobody performed — with
        the journal recording ``evaluated=0`` for the same pair. Safe by the same argument the rest
        of this method rests on: only a never-completed call ever re-arms."""
        with self._lock:
            key = (symbol, strategy_id)
            if key in self._seen:
                self._seen.discard(key)
                self._evaluated.discard(key)
                _log.info("prescreen_rearmed", symbol=symbol, strategy_id=strategy_id)
                return True
            return False

    # ------------------------------------------------------- cap displacement (2026-08-27)
    def claim_slot(self, symbol: str, strategy_id: str) -> bool:
        """Commit ``(symbol, strategy)``'s slot to a real evaluation — atomically (2026-08-27).

        The pipeline calls this from ``_take_forward_slot``, the one instant an analyst call is
        actually about to be spent, and honours the answer:

        * ``True``  — the pair still holds its slot, and is now marked EVALUATED. From here it can
          never be displaced, which is invariant #1 (an evaluated candidate never loses its slot).
        * ``False`` — the pair was DISPLACED while it sat in the forward queue and no longer holds a
          slot at all. The pipeline must skip it, or the slot handed to the better arrival would be
          spent twice and the cap would be breached by one analyst call.

        The compare-and-set runs under the same lock as the displacement decision, so the two orders
        are the only two possible: claim-then-displace (the pair is in ``_evaluated``, displacement
        passes it over) or displace-then-claim (the pair is gone from ``_charged``, this returns
        ``False``). There is no interleaving in which both succeed.

        FAIL-OPEN on anything unrecognised — a pair this process never charged, or one from before a
        day roll, is claimable. The two failure directions are not symmetric: refusing wrongly
        silently drops a legitimate analyst call, while allowing wrongly costs at most one extra
        call. Only an actual displacement, recorded in ``_displaced``, is ever refused.
        """
        with self._lock:
            key = (symbol, strategy_id)
            if key in self._displaced and key not in self._charged:
                _log.info("prescreen_slot_claim_refused", symbol=symbol, strategy_id=strategy_id,
                          reason="slot was displaced by a better candidate for this strategy")
                return False
            self._evaluated.add(key)
            return True

    def decline(self, symbol: str, strategy_id: str) -> bool:
        """The analyst RAN on ``(symbol, strategy)`` and said ``no_action`` (2026-09-04).

        2026-09-03: five declined ``orb`` evaluations held all five of the strategy's slots from
        09:52/11:30 to the close — ``displaceable=0`` on 3,984 refusals, UNITDSPR at 0.967 among
        them — because :meth:`claim_slot` makes a pair permanent and nothing ever un-made it. A
        declined evaluation holds no position, so it holds no slot either: the pair becomes a
        displacement victim again, under the same margin and order as an unevaluated incumbent. It
        stays SEEN — its once-per-day publication is spent; this is NOT :meth:`rearm`, which exists
        for calls that never happened, and the spam bound still counts attempts, not outcomes.
        Evicting it costs one more analyst call, which the day-level displacement budget and the
        forward cap bound (:meth:`_displacement_scan_locked`). Pairs restored by :meth:`hydrate` are
        not in ``_admitted`` and stay permanent — the documented restart carve-out, unchanged.
        """
        with self._lock:
            key = (symbol, strategy_id)
            if key not in self._admitted:
                return False
            self._declined.add(key)
            _log.info("prescreen_slot_declined", symbol=symbol, strategy_id=strategy_id)
            return True

    def take_displaced(self) -> list[tuple[str, str]]:
        """Pop the displacement notices the pipeline has not acted on yet (2026-08-27).

        Displacement is decided HERE, on a scan worker thread; its consequences — dropping the
        evicted entry from ``_pending_forwards`` and flipping its day-slot journal row back to
        ``evaluated=0`` — belong to the pipeline and must happen on the event loop. This hand-off is
        that seam, and it is deliberately a pull rather than a callback: a push from the scan thread
        would be reaching into loop-confined state, which is precisely the race this design exists
        to avoid.
        """
        with self._lock:
            pending, self._displaced_pending = self._displaced_pending, []
            return pending

    def hydrate(
        self,
        day: date,
        *,
        seen: Sequence[tuple[str, str]],
        charged: Sequence[tuple[str, str]],
        catalyst_strategies: Sequence[str] = (),
    ) -> None:
        """Restore ``day``'s dedupe/cap state from the persisted journal at boot (2026-08-04).

        The per-day state is process memory, so before this existed every restart reset BOTH bounds —
        observed 2026-08-04: ~54 publications against the 20/day cap across two mid-session restarts.
        ``charged`` = every (symbol, strategy) pair published today — caps count attempts, never
        refunded (2026-07-29). ``seen`` = pairs whose day slot is SPENT (evaluated, or deliberately
        refused: governor/forward-cap/unsizeable); a charged-but-unseen pair was lost in flight and
        may re-publish within its already-paid quota — exactly the :meth:`rearm` semantics. Boot-only
        (composition root, from ``prescreen_day_slots``): replay/backtest paths never call this, so
        §9.6 bar-stream determinism is untouched.

        Restored pairs are marked EVALUATED, i.e. undisplaceable (2026-08-27). The journal cannot
        prove a negative: ``evaluated``/``forwarded`` record what a PREVIOUS process did, and a
        restart cannot reconstruct whether the analyst got as far as reading one. Invariant #1 says
        an evaluated candidate never loses its slot, so the only safe direction across a boot is to
        assume it was. The cost is that displacement is inert for pre-restart pairs; the alternative
        risks handing a second candidate the slot of one that had already been judged.

        ``catalyst_strategies`` (2026-08-28) restores the §2.7 ``_catalyst_refs`` budget, which this
        used to leave empty — justified while ``max_per_strategy_day['cat']`` equalled the guard and
        so bounded the day on its own, and broken the moment ``cat_reversal: 2`` was added alongside
        it (post-restart bound 4 against a guard of 2). The journal has no ref column, but the pairs
        that could have charged one are exactly those whose ``strategy_id`` is in this set, and the
        twins one story projects share their SYMBOL as well as their ref (one watchlist row per
        symbol per day) — so distinct symbols among them reconstruct the distinct-ref count. That is
        the conservative reconstruction AVAILABLE, not an exact one: two different event_types on
        one symbol the same day are two refs read back as one, which is fail-OPEN by at most that
        difference. One story per symbol per day is the overwhelming case, and the alternative — a
        journal column migration — buys exactness for a case that has never occurred. Unwired ⇒ the
        pre-2026-08-28 behaviour (an empty budget), so replay/backtest paths are unaffected."""
        catalyst_ids = frozenset(catalyst_strategies)
        with self._lock:
            self._roll_day_locked(day)
            self._seen = set(seen)
            self._charged = set(charged) | self._seen
            self._evaluated = set(self._charged)
            self._declined = set()
            self._count_day = len(self._charged)
            counts: dict[str, int] = {}
            for _, strategy_id in self._charged:
                counts[strategy_id] = counts.get(strategy_id, 0) + 1
            self._count_by_strategy = counts
            self._catalyst_refs = {
                hydrated_catalyst_ref(symbol)
                for symbol, strategy_id in self._charged
                if strategy_id in catalyst_ids
            }

    def sweep(self, bars: Sequence[Bar]) -> tuple[list[SignalCandidate], list[PendingSetup]]:
        """Re-scan the LATEST bar of each symbol on demand (§3.2.5 sweep addendum, 2026-07-29).

        Each bar runs through the normal :meth:`_scan` path — dedupe and caps apply exactly as if
        the bar had just arrived, so a sweep can never double-publish or bypass a bound.
        PUBLICATION is the caller's job on the event loop (``await bus.apublish`` per candidate —
        the same split :meth:`handle_bar` uses), which keeps ``sweep`` loop-agnostic and safe to
        run in a worker thread. Additionally collects every scanner's :meth:`Scanner.pending` arm
        levels, skipping (symbol, strategy) pairs that already published today (their day slot is
        spent) and every PARKED strategy (2026-09-12). The caller turns the pair into the owner
        verdict message."""
        accepted: list[SignalCandidate] = []
        pending: list[PendingSetup] = []
        # A parked strategy's arm levels are not "not yet triggered" — they can NEVER trigger, since
        # `_admit_one_locked` refuses every candidate it produces. The `seen` skip below cannot cover
        # them: a parked pair is refused before `_seen.add`, so it is never seen and would keep
        # rendering under "would arm at:" in the one surface the owner reads intraday. Snapshotted
        # once (caps are constructor-fixed) under the lock that owns them.
        with self._lock:
            parked = {sid for sid, cap in self._strategy_caps.items() if cap == 0}
        for bar in bars:
            accepted.extend(self._scan(bar))
            ctx = self._context_provider(bar)
            with self._lock:
                seen = set(self._seen)
            for scanner in self._scanners:
                if scanner.strategy_id in parked or (bar.symbol, scanner.strategy_id) in seen:
                    continue
                try:
                    pending.extend(scanner.pending(bar, ctx))
                except Exception:  # noqa: BLE001 - one scanner's pending math never kills the sweep
                    _log.exception("pending_setup_failed", symbol=bar.symbol,
                                   strategy_id=scanner.strategy_id)
        return accepted, pending

    # ------------------------------------------------------------------ bus adapter
    async def handle_bar(self, event: BaseModel) -> None:
        """``"bar.1m"`` handler: scan off-loop (§2.2 heartbeat invariant), then await publication."""
        if not isinstance(event, Bar):
            return
        accepted = await asyncio.to_thread(self._scan, event)
        if self._bus is not None:
            for cand in accepted:
                await self._bus.apublish(SIGNAL_CANDIDATE_TOPIC, cand)

    def admit(
        self, cands: Sequence[SignalCandidate], day: date, *, in_window: bool = True
    ) -> list[SignalCandidate]:
        """Admit EXTERNALLY-scanned candidates through the same dedupe/caps/telemetry spine.

        The brk20 daily sweep (2026-08-04, owner-directed after the BPCL miss) scans completed
        ``bars_1d`` for the full eligible universe — no 1m bar exists to drive :meth:`on_bar`, but
        its candidates must face the identical §3.2.5 bounds (same-day (symbol, strategy) dedupe,
        the daily and per-strategy caps) or the sweep would be a cap bypass. ``day`` is the
        session date (the caller's clock); publication stays the caller's job, as in :meth:`sweep`.

        ``in_window`` is the batch-path counterpart of the bar-driven trade-window gate (module
        docstring). These candidates carry no bar, so the caller — which HAS a Clock — decides; the
        pre-screen stays Clock-free. It defaults to ``True`` so the parameter is purely additive:
        omitting it reproduces the pre-2026-08-18 behaviour exactly, and the pipeline's own window
        check still backstops every path. ``engine.ops.main.run_scan_sweep`` passes it explicitly.
        """
        with self._lock:
            self._roll_day_locked(day)
            return self._admit_batch_locked(cands, in_window=in_window)

    # ------------------------------------------------------------------ funnel telemetry (WO-9)
    def funnel_counters(self) -> dict[str, Any]:
        """Today's raw→published funnel, per strategy (WO-9 — the top of the EOD funnel line).

        ``raw`` is what the scanners PRODUCED; everything else is what the §3.2.5 bounds did with
        it. Without ``raw``, "the analyst never saw it" and "nothing fired" are indistinguishable
        after the fact — that ambiguity is exactly what made the 2026-08-11 funnel unreadable.
        """
        with self._lock:
            return {
                "d": self._day,
                "raw": dict(self._raw_by_strategy),
                "published": {k: len(v) for k, v in self._published_scores.items()},
                "published_scores": {k: list(v) for k, v in self._published_scores.items()},
                "suppressed_cap": dict(self._suppressed_cap),
                "suppressed_dedupe": dict(self._suppressed_dedupe),
                "suppressed_window": dict(self._suppressed_window),
                "suppressed_disabled": dict(self._suppressed_disabled),
            }

    def raw_counts(self, d: date) -> dict[str, int]:
        """Raw per-strategy candidate counts for ``d`` — ``{}`` when ``d`` is not the tracked day
        (the counters are per-day state; reporting yesterday's numbers as today's would be a lie).

        With ``raw_counts_loader`` wired these are the day's RUNNING TOTAL across every process that
        ran it (2026-08-21), which is what makes this safe to persist as an absolute value."""
        with self._lock:
            return dict(self._raw_by_strategy) if self._day == d else {}

    # ------------------------------------------------------------------ core
    def _roll_day_locked(self, day: date) -> None:
        if day != self._day:
            self._day = day
            self._seen.clear()
            self._charged.clear()
            self._count_day = 0
            self._count_by_strategy.clear()
            self._catalyst_refs.clear()
            self._admitted.clear()
            self._admit_seq = 0
            self._evaluated.clear()
            self._declined.clear()
            self._displacements.clear()
            self._day_displacements = 0
            self._displaced.clear()
            self._displaced_pending.clear()
            self._raw_by_strategy.clear()
            self._published_scores.clear()
            self._suppressed_cap.clear()
            self._suppressed_dedupe.clear()
            self._suppressed_window.clear()
            self._window_logged.clear()
            self._suppressed_disabled.clear()
            self._disabled_logged.clear()
            self._load_raw_counts_locked(day)

    def _load_raw_counts_locked(self, day: date) -> None:
        """Seed ``day``'s RAW counters from ``funnel_raw_counts`` (2026-08-21, lock held).

        The roll that matters is the FIRST one of a process: a mid-session restart used to reset the
        raw row to zero, and the 22:35 review then reported ``raw=None`` for the whole day even
        though the morning's scanners had fired hundreds of times. Loading the persisted total here
        makes the counter CONTINUE — the flush writes absolute values, so a hydrate/flush cycle is
        idempotent no matter how many times the engine bounces.

        A genuine roll onto a NEW day loads ``{}`` (no rows written for it yet) and the counters
        legitimately start at zero. Unwired ⇒ no-op, the pre-2026-08-21 behaviour. Never raises: a
        telemetry read must not be able to kill a scan (D7)."""
        if self._raw_counts_loader is None:
            return
        try:
            loaded = {str(k): int(v) for k, v in self._raw_counts_loader(day).items() if int(v) > 0}
        except Exception as exc:  # noqa: BLE001 - an unreadable counter costs telemetry, never a scan
            _log.warning("funnel_raw_hydrate_failed", d=day.isoformat(), error=str(exc))
            return
        if not loaded:
            return
        self._raw_by_strategy.update(loaded)
        _log.info("funnel_raw_hydrated", d=day.isoformat(), strategies=len(loaded),
                  fires=sum(loaded.values()))

    def _rank(self, cands: Sequence[SignalCandidate]) -> list[SignalCandidate]:
        """Order one admission batch (WO-1 (i)).

        THE ADMISSION RULE: within a batch, admit in DESCENDING ``score``; ties keep emission
        (scanner) order. ``sorted`` is stable, so the pre-WO-1 order survives as the tie-break and a
        §9.6 replay of the same bar stream still reproduces the same decisions byte-for-byte. Scores
        are only comparable WITHIN a strategy, which is fine here and only here: a batch competes
        for the shared daily cap, and the per-strategy sub-caps — not this sort — are what bound one
        strategy's share of the day. Cross-strategy comparison at the ANALYST slot uses a per-
        strategy quantile instead (``engine.ops.pipeline``); it must not be done with raw scores.
        """
        if self._admission_mode == "arrival":
            return list(cands)
        return sorted(cands, key=lambda c: -float(c.score))

    def _displacement_scan_locked(
        self, cand: SignalCandidate, batch_floor: int | None = None,
        at: datetime | None = None,
    ) -> _DisplacementScan:
        """The pair ``cand`` may evict to take a full cap's slot (or ``None``), and the diagnostic
        count of displaceable incumbents — from ONE pass over ``_admitted`` (2026-08-27).

        Both outputs come from the same walk because the refusal path needs both and the two
        predicates are nested: ``displaceable`` counts every incumbent that clears the base
        eligibility below, and ``victim`` is the worst of those that ALSO clears the batch floor and
        the margin. Counting was previously a second full scan of the same dict on the per-bar
        admission path, under this lock, purely to populate a log field.

        The count is deliberately the BASE predicate only — it answers "were there any candidates
        for displacement at all", which is what makes a refusal readable: ``0`` means every incumbent
        has really been evaluated (the cap is doing its job), while a non-zero count next to a
        refusal means the arrival simply was not ``displacement_margin`` better, or a displacement
        budget is spent. So it is still computed when a budget is exhausted — that is one of the
        cases the field has to distinguish. It is NOT computed when displacement is disabled
        outright: no victim can exist, so the walk would buy only a log field, and a ``0`` there
        would read as the "every incumbent was evaluated" diagnosis rather than "the mechanism is
        off". That case returns ``None`` and logs as null.

        PURE SELECTION — it mutates nothing, so a candidate that passes the cap check on a
        displacement and is then refused by a LATER check (the §2.7 catalyst cap) has not silently
        destroyed an incumbent. :meth:`_commit_displacement_locked` is the mutation, and it runs at
        the accept point once every other bound has already said yes.

        Eligibility, all four required:

        * same ``strategy_id`` — the only comparison WO-1 permits with raw scores, and the reason
          this stays legitimate even when it is the aggregate DAY cap that binds;
        * admitted in an EARLIER batch (``seq <= batch_floor``) — displacement decides cross-batch
          competition, ranking decides intra-batch competition, and the two must not overlap;
        * not in ``_evaluated`` — invariant #1, the whole point;
        * no ``catalyst_ref`` — keeps ``_catalyst_refs`` from ever shrinking (§2.7 anti-manipulation);
        * ``cand.score - incumbent >= _displacement_margin`` — "materially better", not "better by a
          rounding error" (``strategy.prescreen.displacement_margin`` in ``config/settings.yaml``,
          which carries the reasoning for the shipped 0.10).

        Among the eligible, the victim is the WORST: lowest score, ties broken by earliest admission
        sequence. Two pairs can share a score; they cannot share a sequence, so the order is TOTAL.
        Both the margin test and the ordering run in integer basis points (:func:`score_bp`) — float
        subtraction makes ``0.90 - 0.80`` land *under* a 0.10 margin, which would have made the
        policy boundary depend on IEEE-754 representation rather than on the rule.

        DETERMINISM (§9.6), worked through rather than asserted. The decision reads exactly five
        pieces of state: ``_admitted`` (score/seq/catalyst per charged pair), ``_evaluated``,
        ``_displacements``, ``_charged`` and the caps. Four of the five are written only by
        :meth:`_admit_one_locked` and :meth:`_commit_displacement_locked`, both of which are driven
        solely by the candidate stream in arrival order — no Clock is read anywhere on this path,
        the tie-break is total, and float comparison of identical inputs is exact. So a REPLAY of the
        same bar/candidate stream through this class reproduces every admission and displacement
        byte-for-byte, which is the §9.6 guarantee.

        The fifth, ``_evaluated``, is the honest exception and it is not a new one. It is also
        written by :meth:`claim_slot`, which the LIVE pipeline calls on its own 3-minute drain
        cadence — so under live operation the displacement outcome does depend on drain timing,
        governor state and the forward cap. That is exactly the shape of the carve-out :meth:`hydrate`
        and ``raw_counts_loader`` already document: those seams are wired by the composition root and
        left unwired by replay/backtest paths, which is what keeps the replay guarantee pure. A replay
        never calls :meth:`claim_slot`, so ``_evaluated`` stays empty and the decision is a pure
        function of the stream. A LIVE session was never byte-reproducible against a different day's
        drain timing, and this changes nothing about that.
        """
        margin = self._displacement_margin
        if margin is None:                # displacement OFF — nothing to walk for (docstring above)
            return _DisplacementScan(None, None)
        strategy_id = cand.strategy_id
        # Both derived budgets bind (module docstring, point 2). The per-strategy one is absent under
        # a capless construction, where the day-level one is the whole bound. TRANCHE-AWARE
        # (2026-09-02 review, blocking): the budget derives from the same effective cap the
        # admission check uses — a flat budget here let a busy early tranche burn the whole day's
        # displacement allowance, voiding the schedule's contested-seat guarantee for the afternoon
        # (the lockout this change exists to fix, relocated into the budget).
        budget = self._cap_for(strategy_id, at)
        day_open = self._day_displacements < self._max_day
        strategy_open = not (
            budget is not None and self._displacements.get(strategy_id, 0) >= budget
        )
        incoming_bp = score_bp(cand.score)
        margin_bp = score_bp(margin)
        displaceable = 0
        best: tuple[int, int, tuple[str, str]] | None = None
        for pair, (score, seq, has_catalyst) in self._admitted.items():
            if pair[1] != strategy_id or has_catalyst:
                continue
            declined = pair in self._declined
            if pair in self._evaluated and not declined:
                continue
            displaceable += 1
            # A declined incumbent's eviction is a NEW evaluation: the day budget and the forward
            # cap bound it; the per-strategy budget bounds pre-evaluation churn only (2026-09-04).
            if not day_open or not (declined or strategy_open):
                continue
            if batch_floor is not None and seq > batch_floor:
                continue                              # admitted by THIS batch — not up for eviction
            incumbent_bp = score_bp(score)
            if incoming_bp - incumbent_bp < margin_bp:
                continue
            if best is None or (incumbent_bp, seq) < (best[0], best[1]):
                best = (incumbent_bp, seq, pair)
        return _DisplacementScan(None if best is None else best[2], displaceable)

    def _commit_displacement_locked(
        self, victim: tuple[str, str], cand: SignalCandidate, at: datetime | None = None
    ) -> None:
        """Evict ``victim`` and hand its charge back to the caps so ``cand`` can take it.

        The SWAP that keeps the spam bound (module docstring, point 2): ``_count_day`` and
        ``_count_by_strategy`` drop by one here and are immediately re-charged by the caller, so the
        cap is never actually exceeded at any point — this is not a refund path in disguise. The
        pair leaves ``_seen`` too, so a still-true setup may legitimately re-publish later and
        compete for whatever slot is free then.

        Nothing is hidden: ``prescreen_slot_displaced`` carries both sides of the trade, and the
        pipeline is separately handed the pair (:meth:`take_displaced`) so the evicted candidate
        leaves the forward queue and its journal row reverts to ``evaluated=0`` — the same
        never-evaluated accounting the TTL refund uses (2026-08-27), because it is the same fact.
        """
        score, seq, _ = self._admitted.pop(victim)
        strategy_id = victim[1]
        declined = victim in self._declined
        self._seen.discard(victim)
        self._charged.discard(victim)
        self._evaluated.discard(victim)       # a declined victim WAS evaluated; its slot is gone now
        self._declined.discard(victim)
        self._count_day = max(0, self._count_day - 1)
        self._count_by_strategy[strategy_id] = max(
            0, self._count_by_strategy.get(strategy_id, 1) - 1
        )
        if not declined:                      # declined evictions are day-budget-only (2026-09-04)
            self._displacements[strategy_id] = self._displacements.get(strategy_id, 0) + 1
        self._day_displacements += 1
        self._displaced.add(victim)
        self._displaced_pending.append(victim)
        _log.info(
            "prescreen_slot_displaced", cap="strategy_day", strategy_id=strategy_id,
            symbol=cand.symbol, score=float(cand.score),
            displaced_symbol=victim[0], displaced_score=score, displaced_seq=seq,
            displaced_declined=declined,
            margin=self._displacement_margin,
            displacements_used=self._displacements.get(strategy_id, 0),
            # Tranche-aware, matching the suppression log's field (2026-09-02 review: the two log
            # sites reported flat vs tranche values under one field name).
            displacement_budget=self._cap_for(strategy_id, at),
            day_displacements_used=self._day_displacements,
            day_displacement_budget=self._max_day,
            reason=(
                "cap full; this candidate scores materially better than a declined incumbent"
                if declined else
                "cap full; this candidate scores materially better than an unevaluated incumbent"
            ),
        )

    def _admit_batch_locked(
        self, cands: Sequence[SignalCandidate], *, in_window: bool = True,
        at: datetime | None = None,
    ) -> list[SignalCandidate]:
        """Count the batch as raw, rank it, and run each candidate through the accept spine.

        ``raw`` counts the batch BEFORE the window gate: what the scanners produced is a fact about
        the scanners, and WO-9's starvation reading depends on it staying that (otherwise a shut
        window renders as "nothing fired").
        """
        for cand in cands:
            self._raw_by_strategy[cand.strategy_id] = (
                self._raw_by_strategy.get(cand.strategy_id, 0) + 1
            )
        # Displacement is a CROSS-BATCH mechanism and must not reach inside this one (2026-08-27).
        # Everything admitted from here on carries a sequence above this floor and is off limits as a
        # victim, for two reasons. Correctness: the accepted list returned below is what the caller
        # PUBLISHES, so a candidate admitted and then evicted by a later member of its own batch
        # would be published holding no slot. Design: WO-1 already decides intra-batch competition by
        # ranking, and under ``ranked`` a later candidate can never outscore an earlier one anyway —
        # only the ``arrival`` rollback could trip this, and a rollback flag that quietly changed
        # behaviour would not be one.
        batch_floor = self._admit_seq
        return [
            c for c in self._rank(cands)
            if self._admit_one_locked(c, in_window=in_window, batch_floor=batch_floor, at=at)
        ]

    def _admit_one_locked(
        self, cand: SignalCandidate, *, in_window: bool = True, batch_floor: int | None = None,
        at: datetime | None = None,
    ) -> bool:
        """The §3.2.5 accept spine for ONE candidate (lock held, day rolled): park, window, dedupe,
        caps, telemetry. Shared verbatim between the bar-driven path and :meth:`admit`."""
        key = (cand.symbol, cand.strategy_id)
        # PARKED gate, ahead of everything including the window (2026-09-12): a per-strategy cap of 0
        # says this strategy admits nothing today at any hour, which is a strictly stronger refusal
        # than "the window is shut" and, like it, charges no slot. Placing it first also makes the
        # funnel unambiguous — every raw fire of a parked strategy lands in exactly one counter
        # instead of splitting between `suppressed_window` and `suppressed_cap`. The cap arithmetic
        # below would refuse these too, but as `prescreen_cap_suppressed`: indistinguishable in the
        # funnel from a strategy that ran out of room, which is the reading this exists to prevent.
        # An ALREADY-SEEN pair is the one exception and falls through to the dedupe counter below:
        # on the day a park ships mid-session, `hydrate` restores pairs that published under the old
        # cap, and their re-fires cost the park nothing — the slot is spent and the analyst has
        # already looked. Counting them here would report the park as refusing work it never saw.
        if self._strategy_caps.get(cand.strategy_id) == 0 and key not in self._seen:
            self._suppressed_disabled[cand.strategy_id] = (
                self._suppressed_disabled.get(cand.strategy_id, 0) + 1
            )
            # Once per strategy per day, for the same reason `prescreen_out_of_window` is: orb alone
            # fired 883 times on 2026-08-18 and a per-candidate line would bury the log.
            if cand.strategy_id not in self._disabled_logged:
                self._disabled_logged.add(cand.strategy_id)
                _log.info(
                    "prescreen_strategy_disabled", symbol=cand.symbol,
                    strategy_id=cand.strategy_id, score=cand.score,
                    max_per_strategy_day=0,
                    reason="strategy parked by config (max_per_strategy_day 0) — no slot charged",
                )
            return False
        # Trade-window gate ahead of all PER-PAIR reasoning (2026-08-18, module docstring; the park
        # above is the only thing before it, and it is per STRATEGY): "the window is shut" is a
        # statement about the clock that precedes any per-pair reasoning, and refusing here is the
        # only point at which a doomed candidate can be stopped BEFORE it charges an unrefundable
        # day slot.
        if not in_window:
            self._suppressed_window[cand.strategy_id] = (
                self._suppressed_window.get(cand.strategy_id, 0) + 1
            )
            # Once per strategy per day: this fires on every bar of a shut window across the whole
            # watchlist (thousands of candidates on 2026-08-18), and a per-candidate line would bury
            # the log exactly like `prescreen_cap_suppressed` does. The per-strategy counter in
            # :meth:`funnel_counters` carries the volume; this line carries the fact that it is on.
            if cand.strategy_id not in self._window_logged:
                self._window_logged.add(cand.strategy_id)
                _log.info(
                    "prescreen_out_of_window", symbol=cand.symbol, strategy_id=cand.strategy_id,
                    score=cand.score,
                    reason="outside owner trade window — not charging a day slot (§7.1)",
                )
            return False
        if key in self._seen:
            # Same (symbol, strategy) already fired today — a breakout re-closing beyond
            # the range every minute must not re-trigger Tier-1 (D5 dedupe).
            self._suppressed_dedupe[cand.strategy_id] = (
                self._suppressed_dedupe.get(cand.strategy_id, 0) + 1
            )
            return False
        # Caps bind on UNIQUE pairs (2026-07-29): a re-armed pair re-publishes within
        # its already-paid quota; only a NEW pair can be suppressed by a full cap.
        charged = key in self._charged
        per_strategy = self._count_by_strategy.get(cand.strategy_id, 0)
        strategy_cap = self._cap_for(cand.strategy_id, at)
        # A binding cap is no longer the end of the conversation (2026-08-27, module docstring). If
        # this strategy is holding a slot on a candidate NOBODY HAS LOOKED AT that scores materially
        # worse, the slot moves. Selection only — nothing is evicted until every remaining bound
        # (including the §2.7 catalyst cap below) has also said yes.
        day_full = self._count_day >= self._max_day
        strategy_full = strategy_cap is not None and per_strategy >= strategy_cap
        cap_binding = not charged and (day_full or strategy_full)
        # One walk of `_admitted` yields both the eviction decision and the refusal diagnostic below.
        victim, displaceable = (
            self._displacement_scan_locked(cand, batch_floor, at)
            if cap_binding else _DisplacementScan(None, 0)
        )
        if not charged and victim is None and day_full:
            _log.info(
                "prescreen_cap_suppressed", cap="day", symbol=cand.symbol,
                strategy_id=cand.strategy_id, score=cand.score,
                max_candidates_per_day=self._max_day,
                displaceable=displaceable,
                day_displacements_used=self._day_displacements,
                day_displacement_budget=self._max_day,
            )
            self._suppressed_cap[cand.strategy_id] = self._suppressed_cap.get(cand.strategy_id, 0) + 1
            return False
        if not charged and victim is None and strategy_full:
            # Still a flat refusal — but the log now says WHY displacement did not save it, so
            # "the cap is full and every incumbent is better or already evaluated" is readable
            # apart from "the cap is full and the displacement budget is spent" (WO-9 funnel).
            _log.info(
                "prescreen_cap_suppressed", cap="strategy_day", symbol=cand.symbol,
                strategy_id=cand.strategy_id, score=cand.score,
                max_per_strategy_day=strategy_cap,
                displaceable=displaceable,
                displacements_used=self._displacements.get(cand.strategy_id, 0),
                displacement_budget=strategy_cap,
                day_displacements_used=self._day_displacements,
                day_displacement_budget=self._max_day,
            )
            self._suppressed_cap[cand.strategy_id] = self._suppressed_cap.get(cand.strategy_id, 0) + 1
            return False
        # §2.7 news carve-out: a candidate carrying a `catalyst_ref` is ADDITIONALLY bound by
        # catalyst_guard.max_catalyst_entries_day (§3.2.5/§7.1) — read here, at the enforcement site,
        # from the hash-verified limits.yaml; never evaluated in RiskGate (§2.4 item 4). The budget
        # counts DISTINCT REFS, not admissions: one story is one news event however many strategies
        # enter on it (`cat` and `cat_reversal` share the watchlist row's entry_id), and charging it
        # twice spent the whole day's news budget on one headline. A ref already charged — including
        # one reconstructed by :meth:`hydrate` for this SYMBOL across a restart — admits free; its
        # story has paid. The pair-level `charged` test still short-circuits a re-armed pair, which
        # is the same "already paid" fact one level down.
        already_charged_ref = False
        if cand.catalyst_ref is not None and not charged:
            already_charged_ref = (
                str(cand.catalyst_ref) in self._catalyst_refs
                or hydrated_catalyst_ref(cand.symbol) in self._catalyst_refs
            )
            catalyst_cap = self._catalyst_cap()
            if catalyst_cap is None or (
                not already_charged_ref and len(self._catalyst_refs) >= catalyst_cap
            ):
                _log.info(
                    "prescreen_cap_suppressed", cap="catalyst_day", symbol=cand.symbol,
                    strategy_id=cand.strategy_id, score=cand.score,
                    catalyst_ref=cand.catalyst_ref,
                    max_catalyst_entries_day=catalyst_cap,
                    catalyst_entries_used=len(self._catalyst_refs),
                )
                self._suppressed_cap[cand.strategy_id] = (
                    self._suppressed_cap.get(cand.strategy_id, 0) + 1
                )
                return False
        # Every bound has now said yes, so the eviction is safe to commit: the counters it frees are
        # re-charged three lines down, and the cap is never over-subscribed in between.
        if victim is not None:
            self._commit_displacement_locked(victim, cand, at)
        self._seen.add(key)
        if not charged:
            self._charged.add(key)
            self._count_day += 1
            # Re-read rather than reuse `per_strategy`: a displacement just decremented it.
            self._count_by_strategy[cand.strategy_id] = (
                self._count_by_strategy.get(cand.strategy_id, 0) + 1
            )
            self._admit_seq += 1
            self._admitted[key] = (
                float(cand.score), self._admit_seq, cand.catalyst_ref is not None
            )
            if cand.catalyst_ref is not None and not already_charged_ref:
                self._catalyst_refs.add(str(cand.catalyst_ref))
        elif key in self._admitted:
            # A re-armed pair re-publishing inside its paid quota (2026-07-29) arrives with FRESH
            # levels and a fresh score; `_admitted` held the one it was first charged at, so a later
            # arrival was measured against a number that no longer described anything. The sequence
            # advances too: this is a new publication, and its position in the total order should say
            # so. Both are facts about the candidate stream in arrival order, so §9.6 replay is
            # unchanged. The catalyst flag is NOT re-read — it is what keeps the pair out of the
            # victim pool, and §2.7 monotonicity is not a per-publication judgement.
            self._admit_seq += 1
            self._admitted[key] = (float(cand.score), self._admit_seq, self._admitted[key][2])
        self._published_scores.setdefault(cand.strategy_id, []).append(float(cand.score))
        _log.info(
            "signal_candidate", signal_id=cand.signal_id, strategy_id=cand.strategy_id,
            symbol=cand.symbol, side=cand.side, style=cand.style, score=cand.score,
            entry=str(cand.raw_levels.entry),
            stop=None if cand.raw_levels.stop is None else str(cand.raw_levels.stop),
            target=None if cand.raw_levels.target is None else str(cand.raw_levels.target),
        )
        return True

    def _scan(self, bar: Bar) -> list[SignalCandidate]:
        with self._lock:
            self._roll_day_locked(bar.ts_minute.date())
            ctx = self._context_provider(bar)
            # Collect the whole bar's batch BEFORE admitting any of it: ranking cannot pick the
            # best of a batch it is only shown one candidate at a time (WO-1 (i)).
            produced: list[SignalCandidate] = []
            for scanner in self._scanners:
                produced.extend(scanner.scan(bar, ctx))
            return self._admit_batch_locked(
                produced, in_window=bar_in_trade_window(bar, ctx.trade_window),
                # Bar time, never a Clock (§9.6) — drives the cap-release schedule (2026-09-02).
                at=bar.ts_minute,
            )
