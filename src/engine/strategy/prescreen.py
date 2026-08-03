"""§3.2.5 ``SignalPreScreen`` (D5) — the deterministic trigger that makes Tier-1 event-driven.

``on_bar(bar) -> list[SignalCandidate]`` (plan-PINNED signature) runs every enabled scanner over the
bar, dedupes on (symbol, strategy, day), applies per-day candidate caps, and publishes each accepted
candidate on the canonical ``"signal.candidate"`` topic. Scanners are pure (no I/O — see
``scanners.base``); everything they need for the bar is assembled once by the injected
``context_provider`` (the seam to ``MarketStore``/``FeatureEngine``/calendar — DuckDB reads happen
there, never inside a scanner).

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
from collections.abc import Callable, Sequence
from datetime import date

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


class SignalPreScreen:
    """Run enabled scanners per bar; dedupe; cap; publish (§3.2.5).

    Parameters
    ----------
    scanners:
        The enabled scanner instances (``scanners.build_enabled_scanners``). Order is preserved —
        under a binding daily cap, earlier scanners win deterministically.
    context_provider:
        Builds the per-bar :class:`ScanContext`.
    bus:
        Optional EventBus; accepted candidates are published on ``"signal.candidate"``.
    max_candidates_per_day:
        Total accepted candidates per day, all strategies/symbols combined [settings tunable —
        integrator: ``strategy.prescreen.max_candidates_per_day``]. Keeps a wild day from spamming
        Tier-1 (D5 exists to make LLM calls event-driven AND bounded).
    max_per_strategy_day:
        Optional per-strategy daily cap [settings tunable — ``strategy.prescreen.max_per_strategy_day``];
        ``None`` = no per-strategy cap.
    """

    def __init__(
        self,
        scanners: Sequence[Scanner],
        context_provider: ContextProvider,
        bus: EventBus | None = None,
        *,
        max_candidates_per_day: int = 20,
        max_per_strategy_day: int | None = None,
    ) -> None:
        if max_candidates_per_day < 1:
            raise ValueError("max_candidates_per_day must be >= 1")
        if max_per_strategy_day is not None and max_per_strategy_day < 1:
            raise ValueError("max_per_strategy_day must be >= 1 (or None)")
        self._scanners = list(scanners)
        self._context_provider = context_provider
        self._bus = bus
        self._max_day = max_candidates_per_day
        self._max_strategy_day = max_per_strategy_day
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
            return [c for c in cands if self._admit_one_locked(c)]

    # ------------------------------------------------------------------ core
    def _roll_day_locked(self, day: date) -> None:
        if day != self._day:
            self._day = day
            self._seen.clear()
            self._charged.clear()
            self._count_day = 0
            self._count_by_strategy.clear()

    def _admit_one_locked(self, cand: SignalCandidate) -> bool:
        """The §3.2.5 accept spine for ONE candidate (lock held, day rolled): dedupe, caps,
        telemetry. Shared verbatim between the bar-driven path and :meth:`admit`."""
        key = (cand.symbol, cand.strategy_id)
        if key in self._seen:
            # Same (symbol, strategy) already fired today — a breakout re-closing beyond
            # the range every minute must not re-trigger Tier-1 (D5 dedupe).
            return False
        # Caps bind on UNIQUE pairs (2026-07-29): a re-armed pair re-publishes within
        # its already-paid quota; only a NEW pair can be suppressed by a full cap.
        charged = key in self._charged
        if not charged and self._count_day >= self._max_day:
            _log.info(
                "prescreen_cap_suppressed", cap="day", symbol=cand.symbol,
                strategy_id=cand.strategy_id, max_candidates_per_day=self._max_day,
            )
            return False
        per_strategy = self._count_by_strategy.get(cand.strategy_id, 0)
        if (not charged and self._max_strategy_day is not None
                and per_strategy >= self._max_strategy_day):
            _log.info(
                "prescreen_cap_suppressed", cap="strategy_day", symbol=cand.symbol,
                strategy_id=cand.strategy_id, max_per_strategy_day=self._max_strategy_day,
            )
            return False
        # TODO(Phase 3): `cat` candidates (catalyst_ref set) are additionally capped by
        # catalyst_guard.max_catalyst_entries_day here (§3.2.5/§7.1), loaded via
        # ProtectedStore.load_verified — never evaluated in RiskGate (§2.4 item 4).
        self._seen.add(key)
        if not charged:
            self._charged.add(key)
            self._count_day += 1
            self._count_by_strategy[cand.strategy_id] = per_strategy + 1
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
            accepted: list[SignalCandidate] = []
            for scanner in self._scanners:
                for cand in scanner.scan(bar, ctx):
                    if self._admit_one_locked(cand):
                        accepted.append(cand)
            return accepted
