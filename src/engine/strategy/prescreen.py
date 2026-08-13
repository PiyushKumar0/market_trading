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
``admission_mode="arrival"`` is the rollback flag to the pre-WO-1 behaviour. Cross-BATCH ordering is
NOT this class's job: a candidate firing at 10:05 cannot be compared against one that has not
happened yet — the forward queue in ``engine.ops.pipeline`` owns selection across the day.

Determinism (§9.6): the pre-screen takes NO Clock — "today" is ``bar.ts_minute.date()``, so a replay
of the same bar stream reproduces the same dedupe/cap decisions byte-for-byte (modulo the minted
``signal_id`` ULIDs). Per-day state (seen set, counters) resets when the bar date changes.

Async surface (§3.2 convention 4): ``handle_bar`` is the ``"bar.1m"`` bus handler — it offloads the
pandas-heavy scan to a worker thread (the loop is never blocked, §2.2) and awaits publication.
Sync/replay callers use ``on_bar`` directly. The integrator wires exactly ONE of the two paths
(``bus.subscribe("bar.1m", prescreen.handle_bar)`` in ``engine.ops``).

Phase-3 ``cat`` seam (§3.2.5/§2.7): the catalyst scanner registers as a peer in
``SCANNER_REGISTRY``; its candidates (``catalyst_ref`` set) must ADDITIONALLY respect
``catalyst_guard.max_catalyst_entries_day`` — enforced HERE, before publication (loaded via
``ProtectedStore.load_verified``, never inside ``RiskGate``: GateContext stays news-free, §2.4
item 4). TODO(Phase 3): add that guard check where candidates are admitted below.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable, Mapping, Sequence
from datetime import date
from typing import Any

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
    admission_mode:
        ``"ranked"`` (default) = score-descending admission within a batch; ``"arrival"`` = the
        pre-WO-1 order. The rollback knob named in WO-1's risk note, nothing else.
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
    ) -> None:
        if max_candidates_per_day < 1:
            raise ValueError("max_candidates_per_day must be >= 1")
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
        #: WO-9 funnel counters, per strategy, for the CURRENT day. Process-scoped by design: they
        #: describe what this process's scanners produced, and a restart legitimately starts a new
        #: observation window (the journal, not these, is the restart-proof record).
        self._raw_by_strategy: dict[str, int] = {}
        self._published_scores: dict[str, list[float]] = {}
        self._suppressed_cap: dict[str, int] = {}
        self._suppressed_dedupe: dict[str, int] = {}

    @staticmethod
    def _parse_caps(
        spec: int | Mapping[str, int] | None,
    ) -> tuple[int | None, dict[str, int]]:
        """Normalise the ``max_per_strategy_day`` scalar/mapping/None into (default, overrides)."""
        if spec is None:
            return None, {}
        if isinstance(spec, Mapping):
            default_cap: int | None = None
            caps: dict[str, int] = {}
            for key, value in spec.items():
                if value is None:                     # a blank YAML line means "no cap here"
                    continue
                cap = int(value)
                if cap < 1:
                    raise ValueError(f"max_per_strategy_day[{key!r}] must be >= 1, got {cap}")
                if str(key) == DEFAULT_CAP_KEY:
                    default_cap = cap
                else:
                    caps[str(key)] = cap
            return default_cap, caps
        cap = int(spec)
        if cap < 1:
            raise ValueError("max_per_strategy_day must be >= 1 (or None)")
        return cap, {}

    def _cap_for(self, strategy_id: str) -> int | None:
        """The publication sub-cap binding ``strategy_id`` — its own line, else ``default``."""
        return self._strategy_caps.get(strategy_id, self._max_strategy_day)

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
        deliberately not refunded (the spam bound counts attempts, not outcomes)."""
        with self._lock:
            key = (symbol, strategy_id)
            if key in self._seen:
                self._seen.discard(key)
                _log.info("prescreen_rearmed", symbol=symbol, strategy_id=strategy_id)
                return True
            return False

    def hydrate(
        self,
        day: date,
        *,
        seen: Sequence[tuple[str, str]],
        charged: Sequence[tuple[str, str]],
    ) -> None:
        """Restore ``day``'s dedupe/cap state from the persisted journal at boot (2026-08-04).

        The per-day state is process memory, so before this existed every restart reset BOTH bounds —
        observed 2026-08-04: ~54 publications against the 20/day cap across two mid-session restarts.
        ``charged`` = every (symbol, strategy) pair published today — caps count attempts, never
        refunded (2026-07-29). ``seen`` = pairs whose day slot is SPENT (evaluated, or deliberately
        refused: governor/forward-cap/unsizeable); a charged-but-unseen pair was lost in flight and
        may re-publish within its already-paid quota — exactly the :meth:`rearm` semantics. Boot-only
        (composition root, from ``prescreen_day_slots``): replay/backtest paths never call this, so
        §9.6 bar-stream determinism is untouched."""
        with self._lock:
            self._roll_day_locked(day)
            self._seen = set(seen)
            self._charged = set(charged) | self._seen
            self._count_day = len(self._charged)
            counts: dict[str, int] = {}
            for _, strategy_id in self._charged:
                counts[strategy_id] = counts.get(strategy_id, 0) + 1
            self._count_by_strategy = counts

    def sweep(self, bars: Sequence[Bar]) -> tuple[list[SignalCandidate], list[PendingSetup]]:
        """Re-scan the LATEST bar of each symbol on demand (§3.2.5 sweep addendum, 2026-07-29).

        Each bar runs through the normal :meth:`_scan` path — dedupe and caps apply exactly as if
        the bar had just arrived, so a sweep can never double-publish or bypass a bound.
        PUBLICATION is the caller's job on the event loop (``await bus.apublish`` per candidate —
        the same split :meth:`handle_bar` uses), which keeps ``sweep`` loop-agnostic and safe to
        run in a worker thread. Additionally collects every scanner's :meth:`Scanner.pending` arm
        levels, skipping (symbol, strategy) pairs that already published today (their day slot is
        spent). The caller turns the pair into the owner verdict message."""
        accepted: list[SignalCandidate] = []
        pending: list[PendingSetup] = []
        for bar in bars:
            accepted.extend(self._scan(bar))
            ctx = self._context_provider(bar)
            with self._lock:
                seen = set(self._seen)
            for scanner in self._scanners:
                if (bar.symbol, scanner.strategy_id) in seen:
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

    def admit(self, cands: Sequence[SignalCandidate], day: date) -> list[SignalCandidate]:
        """Admit EXTERNALLY-scanned candidates through the same dedupe/caps/telemetry spine.

        The brk20 daily sweep (2026-08-04, owner-directed after the BPCL miss) scans completed
        ``bars_1d`` for the full eligible universe — no 1m bar exists to drive :meth:`on_bar`, but
        its candidates must face the identical §3.2.5 bounds (same-day (symbol, strategy) dedupe,
        the daily and per-strategy caps) or the sweep would be a cap bypass. ``day`` is the
        session date (the caller's clock); publication stays the caller's job, as in :meth:`sweep`.
        """
        with self._lock:
            self._roll_day_locked(day)
            return self._admit_batch_locked(cands)

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
            }

    def raw_counts(self, d: date) -> dict[str, int]:
        """Raw per-strategy candidate counts for ``d`` — ``{}`` when ``d`` is not the tracked day
        (the counters are per-day state; reporting yesterday's numbers as today's would be a lie)."""
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
            self._raw_by_strategy.clear()
            self._published_scores.clear()
            self._suppressed_cap.clear()
            self._suppressed_dedupe.clear()

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

    def _admit_batch_locked(self, cands: Sequence[SignalCandidate]) -> list[SignalCandidate]:
        """Count the batch as raw, rank it, and run each candidate through the accept spine."""
        for cand in cands:
            self._raw_by_strategy[cand.strategy_id] = (
                self._raw_by_strategy.get(cand.strategy_id, 0) + 1
            )
        return [c for c in self._rank(cands) if self._admit_one_locked(c)]

    def _admit_one_locked(self, cand: SignalCandidate) -> bool:
        """The §3.2.5 accept spine for ONE candidate (lock held, day rolled): dedupe, caps,
        telemetry. Shared verbatim between the bar-driven path and :meth:`admit`."""
        key = (cand.symbol, cand.strategy_id)
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
        if not charged and self._count_day >= self._max_day:
            _log.info(
                "prescreen_cap_suppressed", cap="day", symbol=cand.symbol,
                strategy_id=cand.strategy_id, score=cand.score,
                max_candidates_per_day=self._max_day,
            )
            self._suppressed_cap[cand.strategy_id] = self._suppressed_cap.get(cand.strategy_id, 0) + 1
            return False
        per_strategy = self._count_by_strategy.get(cand.strategy_id, 0)
        strategy_cap = self._cap_for(cand.strategy_id)
        if not charged and strategy_cap is not None and per_strategy >= strategy_cap:
            _log.info(
                "prescreen_cap_suppressed", cap="strategy_day", symbol=cand.symbol,
                strategy_id=cand.strategy_id, score=cand.score,
                max_per_strategy_day=strategy_cap,
            )
            self._suppressed_cap[cand.strategy_id] = self._suppressed_cap.get(cand.strategy_id, 0) + 1
            return False
        # TODO(Phase 3): `cat` candidates (catalyst_ref set) are additionally capped by
        # catalyst_guard.max_catalyst_entries_day here (§3.2.5/§7.1), loaded via
        # ProtectedStore.load_verified — never evaluated in RiskGate (§2.4 item 4).
        self._seen.add(key)
        if not charged:
            self._charged.add(key)
            self._count_day += 1
            self._count_by_strategy[cand.strategy_id] = per_strategy + 1
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
            return self._admit_batch_locked(produced)
