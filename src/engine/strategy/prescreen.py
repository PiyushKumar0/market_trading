"""``SignalPreScreen`` — the deterministic trigger that makes Tier-1 event-driven.

``on_bar(bar) -> list[SignalCandidate]`` (pinned signature) runs every enabled scanner over the
bar, dedupes on (symbol, strategy, day), applies per-day candidate caps, and publishes each accepted
candidate on ``"signal.candidate"``. Scanners are pure (see ``scanners.base``); everything they need
is assembled once by the injected ``context_provider``, where the store reads happen.

Admission order: within one admission batch (one ``_scan`` call or one :meth:`admit` call)
candidates are admitted score-descending, so a binding cap keeps the batch's best rather than its
first. Ties keep emission order (``sorted`` is stable) and the returned list is in the same ranked
order. ``admission_mode="arrival"`` is the rollback to emission order. Cross-batch forward ordering
belongs to the forward queue in ``engine.ops.pipeline``; cross-batch admission is cap displacement.

Cap displacement
----------------
A candidate refused here never reaches the pipeline's forward queue, so only this class can let a
later, better arrival take a slot that an early burst filled.

The rule: when a cap would refuse a NEW pair, evict the worst same-strategy pair that is charged,
has never been evaluated (or was evaluated and DECLINED, :meth:`decline`), carries no
``catalyst_ref``, and scores at least ``displacement_margin`` below the arrival (lowest score, then
earliest admission — a total order). Otherwise refuse. A declined incumbent's eviction is a new
analyst call, so it is charged to the day-level displacement budget only, not the per-strategy one.

What keeps it safe:

1. *An evaluated candidate is permanent.* :meth:`claim_slot` is the pipeline's atomic
   compare-and-set at the instant an analyst call is committed. It runs under THIS lock, so a pair
   is either claimed (then undisplaceable) or displaced, never both; a displaced pair's
   ``claim_slot`` returns ``False`` and the pipeline skips it, so a freed slot is never spent twice.
   :meth:`rearm` and the TTL refund rest on the same invariant.
2. *The spam bound survives.* This is the only decrement path ``_charged`` has — the trade-window
   gate's docstring warns that a REFUND would erode the spam bound. A displacement is a SWAP: the
   charged count is invariant, so the candidates that can reach the analyst stay exactly the cap.
   A swap does inflate PUBLICATIONS, so displacements carry TWO derived budgets: the strategy's
   own ``max_per_strategy_day`` and ``max_candidates_per_day`` for the day. Both are needed because
   displacement also fires on a full DAY cap. Together they bound admissions at ≤ 2× cap per
   strategy and ≤ 2× the day cap, with no new owner knob to drift out of sync.
3. *The catalyst ref set only grows.* A candidate carrying a ``catalyst_ref`` is never evicted, so
   ``_catalyst_refs`` never shrinks and the anti-manipulation cap cannot be churned. An arrival
   carrying one still faces that cap after winning the displacement.
4. *No cross-strategy comparison.* The victim is always the arrival's own strategy, so only scores
   that are comparable inside a strategy are compared, even when the day cap binds.

Determinism is analysed in :meth:`_displacement_scan_locked`.

Cap release schedule
--------------------
``cap_release_schedule`` lists CUMULATIVE per-strategy sub-cap tranches by IST time-of-day, e.g.
``orb: {"10:00": 3, "11:30": 5, "13:00": 7}``: at most 3 orb pairs may charge before 11:30, 5 before
13:00, 7 after, so later fires keep a contested seat. The effective cap is ``min(max_per_strategy_day,
released tranche)``: a schedule only HOLDS BACK capacity, so every bound derived from the flat cap
is untouched. The release time is the candidate's ENTRY time (``bar.ts_minute + 1m``), never a
Clock; the batch :meth:`admit` path carries no bar and keeps flat caps. Tranche values must be
non-decreasing (a shrinking tranche needs the decrement path the spam bound forbids). The
per-strategy displacement budget derives from the RELEASED tranche. A special session opening after
the last release time (muhurat ~13:45) gets the flat cap from the first bar. The shipped ``orb``
schedule is inert while ``orb`` is parked.

An incumbent scoring ≥ (1 − margin) can never be displaced: no arrival can clear an absolute margin
above it.

Determinism: the pre-screen takes NO Clock — "today" is ``bar.ts_minute.date()``, so a replay of
the same bar stream reproduces the same dedupe/cap decisions byte-for-byte (modulo the minted
``signal_id`` ULIDs). Per-day state resets when the bar date changes.

Trade-window gate
-----------------
A candidate whose entry instant (``ts_minute + 1m``) falls OUTSIDE the owner trade window is
refused before it can charge anything. Caps bind on unique (symbol, strategy) pairs and are NEVER
refunded — "the spam bound counts attempts, not outcomes" (:meth:`rearm`) — so a candidate the
pipeline is guaranteed to drop as ``signal_candidate_out_of_window`` must not spend a day slot.

This gate PREVENTS the charge; it does not refund one. Refunding would need a decrement path for
``_charged``, which does not exist by design — adding one would erode the spam bound. The
pipeline's own window check (``engine.ops.pipeline.on_signal_candidate``) stays authoritative: it
reads the live Clock, this reads bar time, and at a window edge they can disagree by a minute.

Park gate
---------
A per-strategy ``max_per_strategy_day`` of **0** parks that strategy: it admits nothing and charges
nothing. It is the FIRST gate on the accept spine, so every raw fire of a parked strategy lands in
exactly one counter, ``suppressed_disabled``, and a parked strategy never reads as starved. A pair
already in ``_seen`` (a mid-session park after :meth:`hydrate`) falls through to
``suppressed_dedupe``. :meth:`sweep` also drops a parked strategy's :meth:`Scanner.pending` arm
levels, which can never arm and are owner-facing. ``default: 0`` stays a ValueError (see
:meth:`_parse_caps`). ``orb`` is parked; re-enabling is an owner decision.

Async surface: ``handle_bar`` is the ``"bar.1m"`` bus handler — it offloads the pandas-heavy scan
to a worker thread and awaits publication. Sync/replay callers use ``on_bar`` directly. The
integrator wires exactly ONE of the two paths.

``cat`` catalyst seam: candidates carrying a ``catalyst_ref`` must ADDITIONALLY respect
``catalyst_guard.max_catalyst_entries_day`` — enforced HERE, never inside ``RiskGate`` (GateContext
stays news-free). The value arrives through ``catalyst_cap_fn``, bound by the composition root to
the HASH-VERIFIED ``limits.yaml`` block, and is never accepted as a plain constructor number.
Unwired or unverifiable ⇒ catalyst candidates are REFUSED, never admitted uncapped. ``cat`` is a
batch rule absent from ``SCANNER_REGISTRY``; it reaches this class through :meth:`admit`, so the
guard sits on the shared accept spine and binds both paths.
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

#: Canonical EventBus topic.
SIGNAL_CANDIDATE_TOPIC = "signal.candidate"

#: Assembles a :class:`ScanContext` for one bar (MarketStore/calendar/FeatureEngine reads live here).
ContextProvider = Callable[[Bar], ScanContext]

#: Publication-admission orders: ``ranked`` = score-descending within a batch; ``arrival`` =
#: scanner/emission order, kept only as the config rollback.
ADMISSION_MODES = ("ranked", "arrival")

#: Key in a ``max_per_strategy_day`` MAPPING that binds every strategy without its own entry. Not a
#: strategy id — it is the structural ceiling that binds strategies added after the config was written.
DEFAULT_CAP_KEY = "default"

#: Fallback for ``displacement_margin``: how much better a later arrival must score to take a full
#: cap's slot. The shipped value and its reasoning live in ``strategy.prescreen.displacement_margin``
#: in ``config/settings.yaml``; this is the same number for directly-constructed pre-screens.
DEFAULT_DISPLACEMENT_MARGIN = 0.10

#: Scores are compared in integer BASIS POINTS, never as raw floats: ``0.90 - 0.80`` is
#: ``0.09999999999999998``, so a float margin test would refuse a boundary displacement for some
#: score pairs and not others. Scanner scores are in [0, 1] with fewer than four significant
#: decimals, so 1e-4 resolution loses nothing.
SCORE_BASIS = 10_000


def score_bp(score: float) -> int:
    """A [0, 1] score as an integer basis-point count — the only form displacement compares."""
    return int(round(float(score) * SCORE_BASIS))


def hydrated_catalyst_ref(symbol: str) -> str:
    """The stand-in ``catalyst_ref`` :meth:`SignalPreScreen.hydrate` charges for ``symbol``.

    The day-slot journal carries no ref column, but twins projected from one story share their
    symbol as well as their ref, so distinct symbols reconstruct the distinct-ref count. See
    :meth:`SignalPreScreen.hydrate` for the direction this is wrong in.
    """
    return f"hydrated:{symbol}"


class _DisplacementScan(NamedTuple):
    """One pass over ``_admitted``: who may be evicted, and how many could have been.

    ``displaceable`` is ``None`` when displacement is disabled, which is NOT the same reading as
    ``0`` ("every incumbent has really been evaluated").
    """

    victim: tuple[str, str] | None
    displaceable: int | None


def bar_in_trade_window(bar: Bar, window: tuple[datetime, datetime] | None) -> bool:
    """Is an entry off ``bar`` inside the owner trade window? Bar-derived, Clock-free.

    The instant tested is ``ts_minute + 1m`` (the ``entry_dt`` convention of
    :meth:`~engine.strategy.scanners.orb.OrbScanner.scan`). ``None`` (not a trading day, or an
    unreadable window) is FALSE: no window ⇒ nothing may originate.
    """
    if window is None:
        return False
    entry_dt = bar.ts_minute + timedelta(minutes=1)
    return window[0] <= entry_dt <= window[1]


class SignalPreScreen:
    """Run enabled scanners per bar; dedupe; cap; publish.

    Parameters
    ----------
    scanners:
        The enabled scanner instances. Order is the tie-break only: equal scores fall back to it.
    context_provider:
        Builds the per-bar :class:`ScanContext`.
    bus:
        Optional EventBus; accepted candidates are published on ``"signal.candidate"``.
    max_candidates_per_day:
        Total accepted candidates per day, all strategies combined
        [``strategy.prescreen.max_candidates_per_day``]. Keeps Tier-1 calls bounded.
    max_per_strategy_day:
        Per-strategy daily sub-cap [``strategy.prescreen.max_per_strategy_day``; owner knob, NOT
        learner-movable]. ``None`` = no per-strategy cap; an ``int`` = the same cap for every
        strategy; a MAPPING ``{strategy_id: cap}`` = per-strategy values, with the reserved key
        ``"default"`` binding every strategy without a line. A per-strategy ``0`` PARKS that
        strategy (see the module docstring). ``default: 0`` is a ValueError.
    admission_mode:
        ``"ranked"`` (default) or ``"arrival"``; the rollback knob, nothing else.
    catalyst_cap_fn:
        Reads ``catalyst_guard.max_catalyst_entries_day`` from the hash-verified ``limits.yaml``.
        Binds ONLY candidates with a ``catalyst_ref``. ``None``, or a call that raises (an
        unverifiable protected store), refuses every catalyst candidate: an unreadable
        anti-manipulation surface must not degrade into "no cap".
    raw_counts_loader:
        ``day -> {strategy_id: raw fires}`` reader for the persisted raw funnel counters, called on
        every day ROLL, including the first roll of a fresh process. Replay/backtest leave it
        ``None`` and stay pure. It is the READ half of a pair with
        :meth:`~engine.ops.pipeline.RecommendationPipeline._flush_funnel_raw`, which writes the
        day's absolute total back: wiring the flush WITHOUT this loader would let each restart
        overwrite the persisted count with the new process's smaller one.
    displacement_margin:
        How much better a later candidate must score to take a full cap's slot from an unevaluated
        incumbent of the same strategy [``strategy.prescreen.displacement_margin``; owner knob,
        NOT learner-movable]. ``None`` DISABLES displacement (first fires keep their slots all
        session) — the rollback flag. :data:`DEFAULT_DISPLACEMENT_MARGIN` mirrors the shipped value.
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
        self._catalyst_cap_fn = catalyst_cap_fn               # catalyst_guard, read at use
        self._raw_counts_loader = raw_counts_loader           # funnel raw counters, restored on roll
        #: ``None`` disables displacement entirely (the rollback knob).
        self._displacement_margin = (
            None if displacement_margin is None else float(displacement_margin)
        )
        #: Cumulative per-strategy cap tranches by IST time-of-day; ``{}`` = flat caps.
        self._cap_schedule = self._parse_cap_schedule(cap_release_schedule)
        # Per-day state (reset on bar-date change). Lock: handle_bar offloads to worker threads.
        self._lock = threading.Lock()
        self._day: date | None = None
        self._seen: set[tuple[str, str]] = set()      # (symbol, strategy_id) already emitted today
        #: Pairs CHARGED against the daily caps. Caps count unique pairs, not publications: a
        #: re-armed pair re-publishes within its already-paid quota.
        self._charged: set[tuple[str, str]] = set()
        self._count_day = 0
        self._count_by_strategy: dict[str, int] = {}
        #: DISTINCT ``catalyst_ref`` values charged today — the ``max_catalyst_entries_day`` budget,
        #: compared by ``len``. Keyed on the FIELD, not on ``strategy_id == "cat"``: no strategy id
        #: may decide whether the news guard applies. A SET because ``cat`` and ``cat_reversal``
        #: share one story's ``entry_id`` as ref and must pay one entry, not two. Restored by
        #: :meth:`hydrate`.
        self._catalyst_refs: set[str] = set()
        # ------------------------------------------------------------------ cap displacement
        #: Every CHARGED pair -> (score at admission, admission sequence, carried a catalyst_ref).
        #: All fields are facts about the candidate stream alone, which keeps replay exact.
        self._admitted: dict[tuple[str, str], tuple[float, int, bool]] = {}
        #: Monotonic admission counter — the final tie-break when two pairs share a score.
        self._admit_seq = 0
        #: Pairs whose slot is PERMANENT (never displaceable): an analyst call was committed
        #: (:meth:`claim_slot`), or :meth:`hydrate` restored them and this process cannot prove
        #: otherwise. This set IS displacement invariant #1.
        self._evaluated: set[tuple[str, str]] = set()
        #: Evaluated pairs the analyst DECLINED (:meth:`decline`): still charged and seen, but
        #: displaceable again — a no_action verdict holds no position and so holds no slot.
        self._declined: set[tuple[str, str]] = set()
        #: Displacements spent per strategy today, budgeted against that strategy's cap.
        self._displacements: dict[str, int] = {}
        #: Displacements spent across ALL strategies today, budgeted against
        #: ``max_candidates_per_day``; also the only bound when no per-strategy cap is configured.
        self._day_displacements = 0
        #: Every pair displaced today — read by :meth:`claim_slot` to refuse a slot the pipeline
        #: still holds a queue pointer to. Kept all day, unlike ``_displaced_pending``.
        self._displaced: set[tuple[str, str]] = set()
        #: Displacement notices not yet collected by the pipeline (:meth:`take_displaced`). The
        #: pipeline drains this on the EVENT LOOP; admission runs in a scan worker thread and must
        #: never touch loop-confined state.
        self._displaced_pending: list[tuple[str, str]] = []
        #: Funnel counters, per strategy, for the CURRENT day. Only ``_raw_by_strategy`` is
        #: restart-proof (seeded from ``funnel_raw_counts`` on every day roll, flushed back by the
        #: drain tick): a raw count that resets is indistinguishable from "nothing fired". The rest
        #: describe this process's admission spine; the day-slot journal is their durable record.
        self._raw_by_strategy: dict[str, int] = {}
        self._published_scores: dict[str, list[float]] = {}
        self._suppressed_cap: dict[str, int] = {}
        self._suppressed_dedupe: dict[str, int] = {}
        #: Candidates refused because the owner trade window was shut. Kept apart from
        #: ``_suppressed_cap``: no slot was charged, so a large count is budget protection, not
        #: starvation.
        self._suppressed_window: dict[str, int] = {}
        #: Strategies already logged as window-refused today (one INFO per strategy per day).
        self._window_logged: set[str] = set()
        #: Candidates refused because their strategy is PARKED (its ``max_per_strategy_day`` is 0).
        #: Kept apart from ``_suppressed_cap`` so a configured OFF switch does not read as starvation.
        self._suppressed_disabled: dict[str, int] = {}
        #: Strategies already logged as parked today (one INFO per strategy per day).
        self._disabled_logged: set[str] = set()

    @staticmethod
    def _parse_caps(
        spec: int | Mapping[str, int] | None,
    ) -> tuple[int | None, dict[str, int]]:
        """Normalise the ``max_per_strategy_day`` scalar/mapping/None into (default, overrides).

        A PER-STRATEGY ``0`` is legal and means PARKED. ``default: 0`` and the scalar form stay a
        ValueError: a typo there would silently park the whole platform, which is a kill-switch
        decision, not a cap edit.
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
        lists, sorted by time. Malformed input is a constructor error: times must parse ``HH:MM``,
        values must be >= 1 and NON-DECREASING (a shrinking tranche needs a decrement path)."""
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
                # "09:00" and "9:00" parse to one instant; a config typo must be loud.
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

        With a ``cap_release_schedule`` line AND a bar-time ``at``: ``min(flat cap, cumulative
        tranche released at the entry time)``. ``at=None`` (the batch :meth:`admit` path) and
        unscheduled strategies keep the flat cap; a schedule only holds capacity back."""
        flat = self._strategy_caps.get(strategy_id, self._max_strategy_day)
        schedule = self._cap_schedule.get(strategy_id)
        if schedule is None or at is None:
            return flat
        released = schedule[0][1]                       # pre-first-release ⇒ the opening tranche
        # Entry time (ts_minute + 1m): an 11:29 bar enters AT 11:30 and gets the 11:30 tranche.
        bar_tod = (at + timedelta(minutes=1)).time()    # naive IST clock time (bars are IST-aware)
        for release, cap in schedule:
            if release <= bar_tod:
                released = cap
        return released if flat is None else min(flat, released)

    def _catalyst_cap(self) -> int | None:
        """``catalyst_guard.max_catalyst_entries_day``, or ``None`` meaning REFUSE.

        Read per admission from the hash-verified protected store, so an owner change lands on the
        next :meth:`~engine.risk.limits.LimitsEngine.reload`. ``None`` for an unwired reader, an
        unverifiable store (``IntegrityError``) and a nonsense value alike: the surface could not be
        established, so no news-originated entries, never an uncapped one.
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
        """Scan one bar; return accepted candidates and publish them (pinned surface)."""
        accepted = self._scan(bar)
        if self._bus is not None:
            for cand in accepted:
                self._bus.publish(SIGNAL_CANDIDATE_TOPIC, cand)
        return accepted

    # ------------------------------------------------------------------ sweep
    def seen_today(self) -> int:
        """Number of (symbol, strategy) day slots spent so far today."""
        with self._lock:
            return len(self._seen)

    def rearm(self, symbol: str, strategy_id: str) -> bool:
        """Give ``(symbol, strategy)`` its once-per-day publication back.

        For the pipeline's analyst INFRASTRUCTURE failures only (timeout/SDK death/schema battles):
        the condition was never evaluated, so consuming the day slot would silence a still-true
        setup. An analyst that RAN and said no_action, or a gate rejection, is a real evaluation and
        must NOT re-arm. Daily caps are not refunded (the spam bound counts attempts, not outcomes).

        The pair also leaves ``_evaluated``: :meth:`claim_slot` adds it at dispatch, one instant
        before the call that never happened, and it must not stay undisplaceable on the strength of
        an evaluation nobody performed."""
        with self._lock:
            key = (symbol, strategy_id)
            if key in self._seen:
                self._seen.discard(key)
                self._evaluated.discard(key)
                _log.info("prescreen_rearmed", symbol=symbol, strategy_id=strategy_id)
                return True
            return False

    # ------------------------------------------------------------------ cap displacement
    def claim_slot(self, symbol: str, strategy_id: str) -> bool:
        """Commit ``(symbol, strategy)``'s slot to a real evaluation, atomically.

        The pipeline calls this from ``_take_forward_slot``, the instant an analyst call is about
        to be spent, and honours the answer:

        * ``True``  — the pair still holds its slot and is now EVALUATED, never displaceable.
        * ``False`` — the pair was DISPLACED while queued. The pipeline must skip it, or the slot
          handed to the better arrival would be spent twice.

        The compare-and-set shares the lock with the displacement decision, so exactly one of
        claim-then-displace and displace-then-claim happens.

        FAIL-OPEN on anything unrecognised (a pair this process never charged, or from before a day
        roll): wrongly refusing silently drops a legitimate analyst call, wrongly allowing costs at
        most one extra call. Only a recorded displacement (``_displaced``) is refused.
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
        """The analyst RAN on ``(symbol, strategy)`` and said ``no_action``.

        A declined evaluation holds no position, so it holds no slot: the pair becomes a
        displacement victim again, under the same margin and order as an unevaluated incumbent. It
        stays SEEN — its once-per-day publication is spent; this is NOT :meth:`rearm`, which is for
        calls that never happened. Evicting it costs one more analyst call, bounded by the day-level
        displacement budget and the forward cap. Pairs restored by :meth:`hydrate` are not in
        ``_admitted`` and stay permanent.
        """
        with self._lock:
            key = (symbol, strategy_id)
            if key not in self._admitted:
                return False
            self._declined.add(key)
            _log.info("prescreen_slot_declined", symbol=symbol, strategy_id=strategy_id)
            return True

    def take_displaced(self) -> list[tuple[str, str]]:
        """Pop the displacement notices the pipeline has not acted on yet.

        Displacement is decided HERE on a scan worker thread; dropping the evicted entry from
        ``_pending_forwards`` and flipping its journal row back to ``evaluated=0`` must happen on
        the event loop. A pull rather than a callback: a push from the scan thread would touch
        loop-confined state.
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
        """Restore ``day``'s dedupe/cap state from the persisted journal at boot.

        ``charged`` = every (symbol, strategy) pair published today. ``seen`` = pairs whose day slot
        is SPENT (evaluated, or deliberately refused: governor/forward-cap/unsizeable); a
        charged-but-unseen pair was lost in flight and may re-publish within its paid quota, as in
        :meth:`rearm`. Boot-only (composition root, from ``prescreen_day_slots``); replay/backtest
        never call this.

        Restored pairs are marked EVALUATED (undisplaceable): the journal cannot prove the analyst
        did not read one, and an evaluated candidate must never lose its slot, so the safe direction
        across a boot is to assume it was. Displacement is inert for pre-restart pairs.

        ``catalyst_strategies`` restores the ``_catalyst_refs`` budget. The journal has no ref
        column, but the pairs that could have charged one are those whose ``strategy_id`` is in this
        set, and twins of one story share their SYMBOL as well as their ref, so distinct symbols
        reconstruct the distinct-ref count. Not exact: two event_types on one symbol the same day
        read back as one ref, which is fail-OPEN by at most that difference. Unwired ⇒ an empty
        budget."""
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
        """Re-scan the LATEST bar of each symbol on demand.

        Each bar runs through :meth:`_scan`, so dedupe and caps apply as if it had just arrived.
        PUBLICATION is the caller's job on the event loop (``await bus.apublish`` per candidate), so
        ``sweep`` is safe to run in a worker thread. Also collects every scanner's
        :meth:`Scanner.pending` arm levels, skipping pairs that already published today and every
        PARKED strategy."""
        accepted: list[SignalCandidate] = []
        pending: list[PendingSetup] = []
        # A parked strategy's arm levels can NEVER trigger, and the `seen` skip below cannot cover
        # them (a parked pair is refused before `_seen.add`); they would render under "would arm at:"
        # in an owner-facing message. Caps are constructor-fixed, so one snapshot suffices.
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
        """``"bar.1m"`` handler: scan off-loop (heartbeat invariant), then await publication."""
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

        The brk20 daily sweep scans completed ``bars_1d`` — no 1m bar exists to drive :meth:`on_bar`
        — and its candidates must face the same dedupe and caps or it would be a cap bypass. ``day``
        is the session date (the caller's clock); publication stays the caller's job.

        ``in_window`` is the batch-path counterpart of the trade-window gate. These candidates carry
        no bar, so the caller, which HAS a Clock, decides; the pre-screen stays Clock-free. The
        pipeline's own window check still backstops every path.
        """
        with self._lock:
            self._roll_day_locked(day)
            return self._admit_batch_locked(cands, in_window=in_window)

    # ------------------------------------------------------------------ funnel telemetry
    def funnel_counters(self) -> dict[str, Any]:
        """Today's raw→published funnel, per strategy (the top of the EOD funnel line).

        ``raw`` is what the scanners PRODUCED; the rest is what the prescreen bounds did with it.
        Without ``raw``, "the analyst never saw it" and "nothing fired" are indistinguishable.
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
        ran it, which is what makes this safe to persist as an absolute value."""
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
        """Seed ``day``'s RAW counters from ``funnel_raw_counts`` (lock held).

        The roll that matters is the FIRST one of a process: loading the persisted total makes a
        restarted counter CONTINUE. The flush writes absolute values, so a hydrate/flush cycle is
        idempotent across any number of restarts. A roll onto a NEW day loads ``{}`` and starts at
        zero. Unwired ⇒ no-op. Never raises: a telemetry read must not kill a scan."""
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
        """Order one admission batch.

        Within a batch, admit in DESCENDING ``score``; ties keep emission order (``sorted`` is
        stable), so replay is unchanged. Scores are only comparable WITHIN a strategy, which is
        acceptable here: the per-strategy sub-caps, not this sort, bound one strategy's share of the
        day. Cross-strategy comparison at the analyst slot uses a per-strategy quantile
        (``engine.ops.pipeline``), never raw scores.
        """
        if self._admission_mode == "arrival":
            return list(cands)
        return sorted(cands, key=lambda c: -float(c.score))

    def _displacement_scan_locked(
        self, cand: SignalCandidate, batch_floor: int | None = None,
        at: datetime | None = None,
    ) -> _DisplacementScan:
        """The pair ``cand`` may evict to take a full cap's slot (or ``None``), and the diagnostic
        count of displaceable incumbents, from ONE pass over ``_admitted``.

        ``displaceable`` counts incumbents clearing the base eligibility below; ``victim`` is the
        worst of those that ALSO clear the batch floor and the margin. The count answers "were there
        any candidates at all": ``0`` on a refusal means every incumbent has really been evaluated,
        non-zero means the arrival was not ``displacement_margin`` better or a budget is spent — so
        it is still computed when a budget is exhausted. When displacement is disabled it is ``None``
        (not ``0``, which would read as "every incumbent was evaluated").

        PURE SELECTION — it mutates nothing, so a candidate refused by a LATER check (the catalyst
        cap) destroys no incumbent. :meth:`_commit_displacement_locked` is the mutation, run once
        every other bound has said yes.

        Eligibility, all required:

        * same ``strategy_id`` — the only comparison raw scores permit, so it stays legitimate even
          when the aggregate DAY cap binds;
        * admitted in an EARLIER batch (``seq <= batch_floor``) — displacement decides cross-batch
          competition, ranking decides intra-batch competition, and the two must not overlap;
        * not in ``_evaluated`` (unless declined) — displacement invariant #1;
        * no ``catalyst_ref`` — keeps ``_catalyst_refs`` from ever shrinking;
        * ``cand.score - incumbent >= _displacement_margin``, so "materially better".

        The victim is the WORST eligible: lowest score, ties by earliest admission sequence (pairs
        cannot share a sequence, so the order is TOTAL). Margin test and ordering run in integer
        basis points (:func:`score_bp`).

        DETERMINISM. The decision reads ``_admitted``, ``_evaluated``, ``_displacements``,
        ``_charged`` and the caps. All but ``_evaluated`` are written only by
        :meth:`_admit_one_locked` and :meth:`_commit_displacement_locked`, driven solely by the
        candidate stream in arrival order — no Clock, a total tie-break — so a replay of the same
        stream reproduces every admission and displacement byte-for-byte. ``_evaluated`` is also
        written by :meth:`claim_slot` on the live pipeline's drain cadence, so live outcomes depend
        on drain timing, governor state and the forward cap — the same carve-out as :meth:`hydrate`
        and ``raw_counts_loader``, left unwired in replay/backtest. A replay never calls
        :meth:`claim_slot`, so there the decision is a pure function of the stream.
        """
        margin = self._displacement_margin
        if margin is None:                # displacement OFF — nothing to walk for
            return _DisplacementScan(None, None)
        strategy_id = cand.strategy_id
        # Both derived budgets bind (module docstring, point 2); the per-strategy one is absent
        # under a capless construction, where the day-level one is the whole bound. TRANCHE-AWARE:
        # the budget derives from the same effective cap the admission check uses, or a busy early
        # tranche would burn the day's displacement allowance and void the afternoon's seats.
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
            # A declined incumbent's eviction is a NEW evaluation: the per-strategy budget bounds
            # pre-evaluation churn only.
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

        The SWAP that keeps the spam bound (module docstring, point 2): the counters drop by one
        here and the caller immediately re-charges them, so the cap is never exceeded — not a refund
        path in disguise. The pair leaves ``_seen`` too, so a still-true setup may re-publish later.
        The pipeline is handed the pair (:meth:`take_displaced`) so it leaves the forward queue and
        its journal row reverts to ``evaluated=0``, as the TTL refund does.
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
        if not declined:                      # declined evictions are day-budget-only
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

        ``raw`` counts the batch BEFORE the window gate, or a shut window would read as "nothing
        fired".
        """
        for cand in cands:
            self._raw_by_strategy[cand.strategy_id] = (
                self._raw_by_strategy.get(cand.strategy_id, 0) + 1
            )
        # Displacement is cross-batch only: everything admitted from here carries a sequence above
        # this floor and is off limits as a victim. The returned list is what the caller PUBLISHES,
        # so a candidate evicted by a later member of its own batch would publish holding no slot.
        batch_floor = self._admit_seq
        return [
            c for c in self._rank(cands)
            if self._admit_one_locked(c, in_window=in_window, batch_floor=batch_floor, at=at)
        ]

    def _admit_one_locked(
        self, cand: SignalCandidate, *, in_window: bool = True, batch_floor: int | None = None,
        at: datetime | None = None,
    ) -> bool:
        """The accept spine for ONE candidate (lock held, day rolled): park, window, dedupe,
        caps, telemetry. Shared verbatim between the bar-driven path and :meth:`admit`."""
        key = (cand.symbol, cand.strategy_id)
        # PARKED gate, ahead of everything including the window: every raw fire of a parked strategy
        # must land in exactly one counter, not split between `suppressed_window` and
        # `suppressed_cap` (the latter reads as a strategy that ran out of room). An ALREADY-SEEN
        # pair (restored by `hydrate` when a park ships mid-session) falls through to the dedupe
        # counter: its slot was spent before the park.
        if self._strategy_caps.get(cand.strategy_id) == 0 and key not in self._seen:
            self._suppressed_disabled[cand.strategy_id] = (
                self._suppressed_disabled.get(cand.strategy_id, 0) + 1
            )
            # Once per strategy per day; a per-candidate line would bury the log.
            if cand.strategy_id not in self._disabled_logged:
                self._disabled_logged.add(cand.strategy_id)
                _log.info(
                    "prescreen_strategy_disabled", symbol=cand.symbol,
                    strategy_id=cand.strategy_id, score=cand.score,
                    max_per_strategy_day=0,
                    reason="strategy parked by config (max_per_strategy_day 0) — no slot charged",
                )
            return False
        # Trade-window gate ahead of all per-pair reasoning: the only point where a doomed candidate
        # can be stopped BEFORE it charges an unrefundable day slot.
        if not in_window:
            self._suppressed_window[cand.strategy_id] = (
                self._suppressed_window.get(cand.strategy_id, 0) + 1
            )
            # Once per strategy per day; the counter carries the volume.
            if cand.strategy_id not in self._window_logged:
                self._window_logged.add(cand.strategy_id)
                _log.info(
                    "prescreen_out_of_window", symbol=cand.symbol, strategy_id=cand.strategy_id,
                    score=cand.score,
                    reason="outside owner trade window — not charging a day slot",
                )
            return False
        if key in self._seen:
            # A breakout re-closing beyond the range every minute must not re-trigger Tier-1.
            self._suppressed_dedupe[cand.strategy_id] = (
                self._suppressed_dedupe.get(cand.strategy_id, 0) + 1
            )
            return False
        # Caps bind on UNIQUE pairs: a re-armed pair re-publishes within its paid quota; only a NEW
        # pair can be suppressed by a full cap.
        charged = key in self._charged
        per_strategy = self._count_by_strategy.get(cand.strategy_id, 0)
        strategy_cap = self._cap_for(cand.strategy_id, at)
        # A binding cap may be displaced (module docstring). Selection only — nothing is evicted
        # until every remaining bound (including the catalyst cap below) has said yes.
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
            # `displaceable` says why displacement did not save it (all evaluated vs budget spent).
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
        # News carve-out: a candidate carrying a `catalyst_ref` is ADDITIONALLY bound by
        # catalyst_guard.max_catalyst_entries_day. The budget counts DISTINCT REFS, not admissions:
        # one story is one entry however many strategies enter on it. A ref already charged —
        # including one reconstructed by :meth:`hydrate` for this SYMBOL — admits free. The
        # pair-level `charged` test short-circuits a re-armed pair, the same "already paid" fact.
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
        # Every bound has said yes, so the eviction is safe to commit.
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
            # A re-armed pair re-publishing inside its paid quota carries a fresh score; refresh it
            # and advance the sequence (a new publication). The catalyst flag is NOT re-read: it
            # keeps the pair out of the victim pool.
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
            # Collect the whole batch BEFORE admitting any of it, so ranking sees all of it.
            produced: list[SignalCandidate] = []
            for scanner in self._scanners:
                produced.extend(scanner.scan(bar, ctx))
            return self._admit_batch_locked(
                produced, in_window=bar_in_trade_window(bar, ctx.trade_window),
                # Bar time, never a Clock — drives the cap-release schedule.
                at=bar.ts_minute,
            )
