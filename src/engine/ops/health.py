"""Health monitor (§3.2.12, R2/R8/§2.6).

Watches feed staleness, clock skew, disk, the ticker subprocess, and SQLite WAL size; alerts the owner
via the injected callback (R8). Crucially it distinguishes three conditions that look alike but mean
different things (§2.6):

- **WARMING** — just started; the ticker is reconnecting and warm-up is backfilling. Feed-stale alarms
  are SUPPRESSED (a fresh start has no recent ticks yet — that is not an incident).
- **feed-lost-while-running** — a real incident: ticks were flowing and stopped (R2 stale-data guard).
- **intentionally-off** — the engine is deliberately stopped between active periods. NORMAL, no alarm.

Also hosts the watchdog hook: alert if an expected scheduled active-period start did not occur (§2.6/§10.4).
Phase 0 ships the framework + the real `check()`; richer alerting wiring lands with the OMS (Phase 3).
"""

from __future__ import annotations

import asyncio
import shutil
import sys
import threading
import traceback
from collections.abc import Awaitable, Callable
from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from engine.core.clock import Clock, ClockSkewUnavailable
from engine.core.config import Settings
from engine.core.log import get_logger
from engine.ops.process_memory import ProcessMemoryReader

_log = get_logger("engine.ops.health")

AlertCallback = Callable[[str, str], Awaitable[None]]   # (severity, message)

#: How long one ``store.aping()`` may take before the store is called STALLED (seconds). A healthy
#: ping is a lock acquisition plus ``SELECT 1`` — sub-millisecond — so 10 s is not a latency budget,
#: it is the line past which "slow" is no longer a plausible explanation (WO-24b-prime).
STORE_PING_TIMEOUT_S = 10.0

#: Stack-dump bounds. Deepest ``_STACK_FRAME_LIMIT`` frames per thread, and the whole dump is capped
#: at ``_STACK_DUMP_MAX_CHARS`` — a ~30-thread process in a deep call tree would otherwise emit a
#: megabyte-scale log line at exactly the moment the machine is already in trouble.
_STACK_FRAME_LIMIT = 40
_STACK_DUMP_MAX_CHARS = 60_000


def _swallow_probe_result(task: asyncio.Future) -> None:
    """Retrieve an abandoned probe's exception so it cannot surface as an unretrieved-exception
    warning on the loop. A timed-out probe is deliberately left running (that is the evidence), and
    it may fail minutes later with nobody waiting on it."""
    if not task.cancelled():
        task.exception()


class HealthReport(BaseModel):
    feed_state: str = "UNKNOWN"          # STOPPED | WARMING | HEALTHY | STALE (from TickerSupervisor)
    last_tick_age_s: float | None = None
    clock_skew_s: float | None = None
    clock_skew_ok: bool = True
    disk_free_gb: float | None = None
    wal_size_mb: float | None = None
    problems: list[str] = Field(default_factory=list)

    @property
    def healthy(self) -> bool:
        return not self.problems


class HealthMonitor:
    """Periodic health checks + state-aware alerting (§3.2.12)."""

    def __init__(
        self,
        clock: Clock,
        settings: Settings,
        *,
        ticker_supervisor: Any = None,     # duck-typed: must expose .health() -> FeedHealth
        alert: AlertCallback | None = None,
        max_skew_s: float | None = None,
        wal_warn_mb: float = 256.0,
        disk_warn_gb: float = 2.0,
        calendar: Any = None,              # duck-typed: must expose .session(date) -> session|None
        keep_awake: Any = None,            # duck-typed: must expose .update(session_open: bool)
        memory_reader: Any = None,         # duck-typed: must expose .read() -> ProcessMemory | None
        memory_log_every: int = 5,
        store: Any = None,                 # duck-typed: must expose .aping() -> Awaitable[bool]
        store_ping_timeout_s: float = STORE_PING_TIMEOUT_S,
    ) -> None:
        self._clock = clock
        self._settings = settings
        self._ticker = ticker_supervisor
        self._alert = alert
        self._max_skew_s = max_skew_s if max_skew_s is not None else settings.clock.max_skew_s
        self._wal_warn_mb = wal_warn_mb
        self._disk_warn_gb = disk_warn_gb
        # In-session OS keep-awake (2026-07-23 sleep/resume wedge): the health loop is the always-on
        # periodic tick, so it is where keep-awake is engaged (session open) / released (session close).
        self._calendar = calendar
        self._keep_awake = keep_awake
        # Process-memory telemetry (2026-08-17 unbounded-commit crisis, ~53 GB/18h, one snapshot, no
        # curve): sampled off the same always-on pulse. self-constructs a real ProcessMemoryReader when
        # not injected (no-op off-Windows) so this works out of the box without extra main.py wiring.
        self._memory_reader = memory_reader if memory_reader is not None else ProcessMemoryReader()
        # Cadence: the health pulse is 60s (settings.lifecycle.watchdog_poll_s); logging on every pulse
        # would add 1440 lines/day to an already-busy stream for a leak that took ~18h to reach crisis.
        # Every 5th pulse (~5 min) still resolves that timescale (~216 samples over 18h) at 1/5th the
        # volume. Pulse-counted, not wall-clock, so cadence is deterministic in tests.
        self._memory_log_every = memory_log_every
        self._pulse_count = 0
        # --- store stall watchdog (WO-24b-prime, after the 2026-08-21 14-minute freeze) ---
        # The health pulse kept beating through the entire freeze while every store-touching path
        # was wedged, which is exactly what makes it the right host for the probe: it is the one
        # timer proven to survive the failure it is watching for.
        self._store = store
        self._store_ping_timeout_s = float(store_ping_timeout_s)
        #: The single outstanding probe. At most ONE exists at any moment: a pulse that finds the
        #: previous one still pending does NOT start another (that is the no-pile-up rule), and the
        #: pending probe is itself the strongest evidence of a stall we could ask for.
        self._store_probe: asyncio.Future | None = None
        self._store_stall_count = 0                     # consecutive stalled pulses
        self._store_stall_since: datetime | None = None
        self._store_last_ok: datetime | None = None
        #: One stack dump per STALL EPISODE, not per pulse. Reset by the next successful probe, so a
        #: second, later freeze dumps again while a 14-minute one does not dump fourteen times.
        self._store_stacks_dumped = False

    async def check(self, *, check_skew: bool = True) -> HealthReport:
        report = HealthReport()

        # --- feed health (WARMING suppresses stale alarms; intentionally-off is not checked here) ---
        if self._ticker is not None:
            fh = self._ticker.health()
            report.feed_state = getattr(fh, "state", "UNKNOWN")
            report.last_tick_age_s = getattr(fh, "last_tick_age_s", None)
            if report.feed_state == "STALE" and self._session_open():
                # feed-lost-WHILE-RUNNING (R2) — a real incident. Out-of-session STALE is
                # definitional (no ticks exist at night) and spammed an alert per minute from
                # midnight 2026-07-31 after the date rollover — never an incident. Calendar-less
                # wiring returns False here, keeping a misconfigured deploy quiet, not noisy.
                report.problems.append("feed_stale")
            # WARMING / STOPPED raise no problem (§2.6: warming is expected, stopped is intentional-off).

        # --- clock skew (R6) ---
        if check_skew:
            try:
                skew = await self._clock.check_skew()
                report.clock_skew_s = skew.total_seconds()
                report.clock_skew_ok = report.clock_skew_s <= self._max_skew_s
                if not report.clock_skew_ok:
                    report.problems.append("clock_skew")
            except ClockSkewUnavailable:
                report.clock_skew_ok = False
                report.problems.append("clock_skew_unverifiable")  # conservative (R6)

        # --- disk free ---
        try:
            usage = shutil.disk_usage(str(self._settings.resolved_data_dir()))
            report.disk_free_gb = usage.free / (1024**3)
            if report.disk_free_gb < self._disk_warn_gb:
                report.problems.append("low_disk")
        except OSError:
            pass

        # --- SQLite WAL size ---
        wal = Path(str(self._settings.sqlite_path()) + "-wal")
        if wal.exists():
            report.wal_size_mb = wal.stat().st_size / (1024**2)
            if report.wal_size_mb > self._wal_warn_mb:
                report.problems.append("large_wal")  # checkpoint at EOD (§4.1)

        # --- in-session OS keep-awake (2026-07-23 sleep/resume wedge): while a trading session is open,
        #     keep the OS awake so it does not auto-sleep mid-session and freeze the tick feed; release
        #     at session close. No-op off-Windows / when disabled / without a calendar. ---
        if self._keep_awake is not None:
            self._keep_awake.update(self._session_open())

        # --- process-memory telemetry (2026-08-17 crisis: no time series existed to diagnose the leak)
        #     Pure telemetry, not a check: a read failure (already logged DEBUG inside the reader) or any
        #     unexpected error here must never raise or affect the health verdict. ---
        try:
            self._pulse_count += 1
            if self._pulse_count % self._memory_log_every == 0:
                mem = self._memory_reader.read()
                if mem is not None:
                    _log.info(
                        "process_memory",
                        private_bytes=mem.private_bytes,
                        working_set_bytes=mem.working_set_bytes,
                        peak_working_set_bytes=mem.peak_working_set_bytes,
                    )
        except Exception:  # noqa: BLE001 - telemetry must never affect the health verdict
            _log.debug("process_memory_log_failed")

        # --- store stall watchdog (WO-24b-prime): make the NEXT freeze diagnose itself. Pure
        #     instrumentation — it never contributes a problem, an alert or a verdict, and it can
        #     never raise into the pulse (the pulse surviving is the entire premise). ---
        try:
            await self._probe_store()
        except Exception:  # noqa: BLE001 - a watchdog that can kill its own host is not a watchdog
            _log.exception("store_watchdog_failed")

        if report.problems and self._alert is not None:
            await self._alert("warning", f"health problems: {report.problems}")
        _log.info("health_check", feed=report.feed_state, skew_ok=report.clock_skew_ok,
                  disk_free_gb=report.disk_free_gb, problems=report.problems)
        return report

    # ------------------------------------------------------------------ store stall watchdog
    async def _probe_store(self) -> None:
        """One liveness probe of the market store per pulse, single-flight (WO-24b-prime).

        Three outcomes, and the difference between them is the diagnosis:

        * **the previous probe is STILL PENDING** — no new probe is started. A ping that has not
          returned since the last pulse is the finding, and issuing a second one would just queue
          another thread behind the same seized resource.
        * **this probe times out** — the store is stalled. A timed-out probe is left RUNNING on
          purpose: when it eventually returns it proves how long the seizure lasted, and until then
          it keeps the pending branch above truthful.
        * **the probe RAISES** — the store answered, with an error. That is a different animal
          entirely (a closed connection, a DuckDB error) and is logged as such, not as a stall.
        """
        if self._store is None:
            return
        now = self._clock.now()
        probe = self._store_probe
        if probe is not None and not probe.done():
            self._note_store_stall(now, pending=True)
            return
        task = asyncio.ensure_future(self._store.aping())
        task.add_done_callback(_swallow_probe_result)
        self._store_probe = task
        try:
            # shield: a timeout must ABANDON the probe, never cancel it — a cancelled probe would
            # destroy the evidence that the next pulse's pending-branch reads.
            await asyncio.wait_for(asyncio.shield(task), self._store_ping_timeout_s)
        except TimeoutError:
            self._note_store_stall(now, pending=False)
            return
        except Exception as exc:  # noqa: BLE001 - answering with an error is not stalling
            self._store_probe = None
            _log.warning("store_ping_failed", error=str(exc), error_type=type(exc).__name__)
            return
        self._store_probe = None
        self._note_store_ok(now)

    def _note_store_stall(self, now: datetime, *, pending: bool) -> None:
        self._store_stall_count += 1
        if self._store_stall_since is None:
            self._store_stall_since = now
        _log.error(
            "store_stalled",
            consecutive=self._store_stall_count,
            seconds_since_last_success=self._seconds_since(self._store_last_ok, now),
            stalled_s=self._seconds_since(self._store_stall_since, now),
            probe_pending=pending,
            timeout_s=self._store_ping_timeout_s,
        )
        if not self._store_stacks_dumped:
            self._store_stacks_dumped = True
            self._dump_thread_stacks()

    def _note_store_ok(self, now: datetime) -> None:
        if self._store_stall_count:
            _log.info(
                "store_stall_recovered",
                stalled_s=self._seconds_since(self._store_stall_since, now),
                consecutive=self._store_stall_count,
            )
        self._store_stall_count = 0
        self._store_stall_since = None
        self._store_stacks_dumped = False          # the NEXT episode gets its own dump
        self._store_last_ok = now

    @staticmethod
    def _seconds_since(then: datetime | None, now: datetime) -> float | None:
        """Elapsed seconds, or None when there is no ``then`` — a store that has NEVER answered
        since boot reports None rather than a fabricated age."""
        return None if then is None else round((now - then).total_seconds(), 3)

    def _dump_thread_stacks(self) -> None:
        """Every thread's stack, once per stall episode, as ONE ``store_stall_stacks`` event.

        This is the line that would have ended the 2026-08-21 post-mortem in a minute instead of
        leaving two live hypotheses: whichever thread is holding the store's ``RLock`` (or sitting
        in a wedged socket read) is named here, with the call path that put it there. Bounded on
        both axes (see the module constants) so the dump cannot itself become the incident, and
        wrapped whole — a diagnostic that fails must degrade to a warning, never to an exception on
        the pulse.
        """
        try:
            names = {t.ident: t.name for t in threading.enumerate()}
            frames = sys._current_frames()
            stacks: dict[str, str] = {}
            budget = _STACK_DUMP_MAX_CHARS
            omitted = 0
            for ident, frame in frames.items():
                text = "".join(traceback.format_stack(frame, _STACK_FRAME_LIMIT)).strip()
                if len(text) > budget:
                    omitted += 1
                    continue
                stacks[f"{names.get(ident, 'unknown')}-{ident}"] = text
                budget -= len(text)
            if omitted:
                stacks["_omitted"] = f"{omitted} thread stack(s) dropped at the payload cap"
            _log.error("store_stall_stacks", threads=len(frames), dumped=len(stacks), stacks=stacks)
        except Exception:  # noqa: BLE001 - a failed diagnostic is a warning, never a raise
            _log.warning("store_stall_stacks_failed")

    def _session_open(self) -> bool:
        """True iff ``clock.now()`` is inside today's NSE continuous session (same calendar/clock the
        tick-silence guard uses). Without a calendar the loop cannot know it is in-session ⇒ False."""
        if self._calendar is None:
            return False
        now = self._clock.now()
        session = self._calendar.session(now.date())
        if session is None:  # holiday / weekend / unverified horizon (R6)
            return False
        return session.open <= now <= session.close

    async def watchdog_missed_start(self, expected: str) -> None:
        """Alert that an expected scheduled active-period start did not occur (§2.6/§10.4)."""
        msg = f"expected active-period start did not occur: {expected}; an open MIS rides to the broker 15:25 backstop"
        _log.warning("missed_active_period_start", expected=expected)
        if self._alert is not None:
            await self._alert("warning", msg)
