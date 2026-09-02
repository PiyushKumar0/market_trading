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
from collections.abc import Awaitable, Callable, Mapping
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from engine.core.clock import Clock, ClockSkewUnavailable
from engine.core.config import Settings
from engine.core.enums import Mode, RiskState
from engine.core.log import get_logger
from engine.ops.process_memory import ProcessMemoryReader

_log = get_logger("engine.ops.health")

AlertCallback = Callable[[str, str], Awaitable[None]]   # (severity, message)

#: How long one ``store.aping()`` may take before the store is called STALLED (seconds). A healthy
#: ping is a lock acquisition plus ``SELECT 1`` — sub-millisecond — so 10 s is not a latency budget,
#: it is the line past which "slow" is no longer a plausible explanation (WO-24b-prime).
STORE_PING_TIMEOUT_S = 10.0

#: How long an UNCHANGED problem set stays quiet between owner alerts (WO-25b, 2026-08-24). The pulse
#: is 60 s and used to alert on every one of them — 57 identical ``health problems: ['feed_stale']``
#: messages in a single morning, the bulk of the 203-deep notification backlog that then starved a
#: fresh owner ack. A CHANGE in the problem set still alerts immediately; this only governs repeats.
HEALTH_REPEAT_MIN = 30

#: Stack-dump bounds. Deepest ``_STACK_FRAME_LIMIT`` frames per thread, and the whole dump is capped
#: at ``_STACK_DUMP_MAX_CHARS`` — a ~30-thread process in a deep call tree would otherwise emit a
#: megabyte-scale log line at exactly the moment the machine is already in trouble.
_STACK_FRAME_LIMIT = 40
_STACK_DUMP_MAX_CHARS = 60_000

#: While a probe is pending, issue a FRESH one every Nth consecutive pending pulse (WO-26a,
#: 2026-08-25). Strict single-flight produced a watchdog that could freeze with the freeze: on 08-25
#: the first probe never got a worker thread and the pulse then reported ``probe_pending=true,
#: consecutive=264`` for 4 h 24 min — 264 log lines, all of them repeating the same one observation
#: made at 09:15. Re-probing every 5th pending pulse (~5 min) costs one thread per five minutes and
#: buys the one fact the old design could not produce: whether the store is STILL stuck NOW.
_STORE_REPROBE_EVERY = 5


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
    #: problem -> owner-facing detail line. Rides the ALERT TEXT so a page is diagnosable without a
    #: shell (2026-09-01 review); never part of the WO-25b episode identity, which stays the problem
    #: SET alone — evolving detail (minutes counting up) must not re-trigger an unchanged episode.
    problem_details: dict[str, str] = Field(default_factory=dict)

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
        mode_manager: Any = None,          # duck-typed: must expose .mode() and .risk_state()
        latch: Any = None,                 # duck-typed: must expose .active_causes() (detail only)
        funnel_probe: Callable[[], tuple[int, int, int | None]] | None = None,
        # funnel_probe: () -> today's (slots published, forward events, governor daily forward cap
        # or None when unreadable — None narrows the alarm to the unambiguous zero-forwarded shape)
        frozen_alarm_after_min: float = 30.0,
        funnel_zero_alarm_after_min: float = 120.0,
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
        #: The probe currently being watched. A pulse that finds it still pending normally starts no
        #: other one — the pending probe IS evidence — but every ``_STORE_REPROBE_EVERY``th such
        #: pulse it is ABANDONED for a fresh one, because evidence from 4 hours ago is not evidence
        #: about now (WO-26a).
        self._store_probe: asyncio.Future | None = None
        #: Probes abandoned that way and still unfinished. Kept referenced (a dropped pending future
        #: would surface as a loop warning) and reported as ``abandoned=`` — a rising count is the
        #: distinctive signature of "worker threads are going in and not coming out".
        self._store_abandoned: list[asyncio.Future] = []
        self._store_pending_pulses = 0                  # consecutive pulses that found one pending
        self._store_stall_count = 0                     # consecutive stalled pulses
        self._store_stall_since: datetime | None = None
        self._store_last_ok: datetime | None = None
        #: One stack dump per STALL EPISODE, not per pulse. Reset by the next successful probe, so a
        #: second, later freeze dumps again while a 14-minute one does not dump fourteen times.
        self._store_stacks_dumped = False
        # --- problem-set alert episodes (WO-25b) ---
        #: The problem set the owner was last alerted about (sorted tuple; () = "all clear"), and when.
        #: Together they make ``_alert_problems`` fire on a CHANGE, repeat at most every
        #: :data:`HEALTH_REPEAT_MIN` minutes, and announce recovery exactly once.
        self._problem_episode: tuple[str, ...] = ()
        self._problem_alerted_at: datetime | None = None
        # --- origination liveness (2026-09-01, after the catchup_safety_jobs latch incident) ---
        self._mode_manager = mode_manager
        self._latch = latch
        self._funnel_probe = funnel_probe
        self._frozen_alarm_after_min = float(frozen_alarm_after_min)
        self._funnel_zero_alarm_after_min = float(funnel_zero_alarm_after_min)
        #: First pulse that observed each alarm's condition — the episode clocks. Reset QUIETLY the
        #: moment the condition breaks (incl. out-of-session, the 2026-08-26 lag-watchdog lesson).
        #: Per-process: a restart restarts the clocks, which is correct — the boot just re-verified
        #: the gates.
        self._frozen_since: datetime | None = None
        self._funnel_zero_since: datetime | None = None
        #: (published-at-last-forward-progress, forwarded) — the funnel stall baseline. `forwarded`
        #: is day-cumulative and monotonic (pipeline upserts +1 per forward, never resets mid-day),
        #: so "zero forwarded" alone goes permanently mute after the day's FIRST forward (2026-09-01
        #: review, blocking): a stall is instead "published grew past the baseline while the forward
        #: count did not move". None ⇒ re-baseline on the next eligible pulse.
        self._funnel_baseline: tuple[int, int] | None = None

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

        # --- origination liveness (2026-09-01: the catchup_safety_jobs latch froze entries for two
        #     whole sessions with zero pages — the engine hummed, exits flowed, nothing said "you are
        #     not originating". These two problems make that silence impossible.) ---
        try:
            self._check_origination(report)
        except Exception:  # noqa: BLE001 - a liveness check must never break the pulse it rides
            _log.exception("origination_watch_failed")

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

        await self._alert_problems(report.problems, report.problem_details)
        _log.info("health_check", feed=report.feed_state, skew_ok=report.clock_skew_ok,
                  disk_free_gb=report.disk_free_gb, problems=report.problems)
        return report

    # ------------------------------------------------------------- origination liveness (2026-09-01)
    def _check_origination(self, report: HealthReport) -> None:
        """Two problems that were silent by construction until the 08-31/09-01 latch incident:

        * ``entries_frozen_in_session`` — an armed mode (RECOMMEND/AUTO) with ``risk_state != NORMAL``
          for ``frozen_alarm_after_min`` CONTIGUOUS in-session minutes. The grace exists because a
          morning boot legitimately holds a warm-up freeze for up to ~20 minutes; a freeze that
          outlives it is an incident whatever its cause (the active causes ride the log line so the
          page is diagnosable without a shell).
        * ``funnel_zero_in_session`` — NO FORWARD PROGRESS while eligible work keeps arriving:
          today's forward count unchanged for ``funnel_zero_alarm_after_min`` CONTIGUOUS in-session
          minutes while slots published GREW past the last-progress baseline, risk_state IS NORMAL,
          and forward capacity remains under the governor's daily cap (a spent cap is quiet by
          design, not by fault; an unreadable cap narrows the alarm to the unambiguous
          zero-forwarded-all-day shape). A frozen state is the first problem's page, not this
          one's — one incident, one problem. Catches a wedged forward/analyst path that a healthy
          risk state would otherwise hide, including one that wedges AFTER the day's first forward
          (the day-cumulative forward counter never returns to zero — 2026-09-01 review).

        Both clocks reset QUIETLY whenever their condition breaks — risk recovers, a forward lands,
        the session closes, the mode disarms (the 2026-08-26 lag-watchdog lesson: alarms evaluate
        only inside session hours). Unwired ``mode_manager`` ⇒ both checks are inert, matching the
        calendar-less feed_stale behaviour: a misconfigured deploy stays quiet, not noisy.
        """
        if self._mode_manager is None:
            return
        now = self._clock.now()
        armed = self._mode_manager.mode() in (Mode.RECOMMEND, Mode.AUTO) and self._session_open()
        state = self._mode_manager.risk_state()

        if armed and state != RiskState.NORMAL:
            if self._frozen_since is None:
                self._frozen_since = now
            frozen_min = (now - self._frozen_since).total_seconds() / 60.0
            if frozen_min >= self._frozen_alarm_after_min:
                report.problems.append("entries_frozen_in_session")
                causes: list[str] | None = None
                if self._latch is not None:
                    try:
                        causes = [c for c, _s, _d in self._latch.active_causes()]
                    except Exception:  # noqa: BLE001 - diagnostic detail, never the verdict
                        causes = None
                report.problem_details["entries_frozen_in_session"] = (
                    f"risk_state={state.value} for {round(frozen_min)}m, causes={causes}"
                )
                _log.error("entries_frozen_in_session", risk_state=state.value,
                           frozen_min=round(frozen_min, 1), causes=causes)
        else:
            self._frozen_since = None

        if self._funnel_probe is None:
            return
        if not (armed and state == RiskState.NORMAL):
            self._funnel_zero_since = None
            self._funnel_baseline = None       # re-baseline when the gate re-opens
            return
        published, forwarded, cap = self._funnel_probe()
        base = self._funnel_baseline
        rolled = base is not None and (published < base[0] or forwarded < base[1])
        if base is None or rolled or forwarded != base[1]:
            # (Re)baseline on first look, day rollover, or forward progress. The published-baseline
            # is what "new work since the last forward" is measured against: 0 while nothing has
            # forwarded today (the 08-31 shape — every published slot counts), else the published
            # count at progress time (a mid-day restart cannot know which older slots were already
            # served, so only NEW publications evidence a stall — conservative by construction).
            self._funnel_baseline = (published if forwarded > 0 else 0, forwarded)
            self._funnel_zero_since = None
            return
        # Same day, forward count static. A stall needs BOTH new work beyond the baseline AND
        # remaining capacity — a day whose §5.6 forward cap is spent goes quiet by design, and an
        # unreadable cap (None) narrows eligibility to the unambiguous zero-forwarded shape.
        # cap=None (unreadable) ⇒ only the unambiguous zero-forwarded shape is eligible; a KNOWN cap
        # is authoritative even at 0 (2026-09-02 review: a deliberate cap=0 full pause used to page
        # via the forwarded==0 short-circuit — a spent-or-zero cap is quiet by design).
        capacity_left = (cap is None and forwarded == 0) or (cap is not None and forwarded < cap)
        if published > self._funnel_baseline[0] and capacity_left:
            if self._funnel_zero_since is None:
                self._funnel_zero_since = now
            zero_min = (now - self._funnel_zero_since).total_seconds() / 60.0
            if zero_min >= self._funnel_zero_alarm_after_min:
                report.problems.append("funnel_zero_in_session")
                report.problem_details["funnel_zero_in_session"] = (
                    f"{published} slots published, forwarded stuck at {forwarded} "
                    f"for {round(zero_min)}m (cap {cap})"
                )
                _log.error("funnel_zero_in_session", published=published, forwarded=forwarded,
                           cap=cap, zero_min=round(zero_min, 1))
        else:
            self._funnel_zero_since = None

    # ------------------------------------------------------------------ problem-set episodes (WO-25b)
    async def _alert_problems(self, problems: list[str],
                              details: Mapping[str, str] | None = None) -> None:
        """Owner-alert the problem set as an EPISODE, not once per pulse (WO-25b, 2026-08-24).

        The pulse runs every 60 s and used to alert on every pulse a problem existed: 57 identical
        ``health problems: ['feed_stale']`` messages in one morning, which is most of what built the
        203-deep Telegram outbox that then starved a fresh owner ack. One persistent problem is one
        thing to know.

        The episode's identity is the problem SET (sorted, so a reordered list is not a "change"):

        * **set changes** — a new problem appears, or one clears while others remain: alert now. A
          change is news by definition, whatever the quiet window says.
        * **set unchanged** — silence until :data:`HEALTH_REPEAT_MIN` minutes have passed, then one
          reminder, and so on. A real outage keeps nagging; it just stops shouting.
        * **all clear** — announce recovery ONCE and close the episode, so the owner learns the
          incident ended without having to infer it from the alerts stopping.

        The check itself is untouched: ``report.problems`` is computed exactly as before and the
        ``health_check`` log line still fires every pulse. Only the owner-facing cadence changes.
        State is per-process (a restart re-announces, which is correct: it IS a new situation).
        """
        if self._alert is None:
            return
        current = tuple(sorted(problems))
        now = self._clock.now()
        if not current:
            if self._problem_episode:
                recovered, self._problem_episode = self._problem_episode, ()
                self._problem_alerted_at = None
                await self._alert("info", f"health recovered: all clear (was {list(recovered)})")
            return
        unchanged = current == self._problem_episode
        if (
            unchanged
            and self._problem_alerted_at is not None
            and (now - self._problem_alerted_at) < timedelta(minutes=HEALTH_REPEAT_MIN)
        ):
            return
        # Stamped BEFORE the await: an alert callback that raises must not leave the window open and
        # turn the next pulse into a repeat.
        self._problem_episode = current
        self._problem_alerted_at = now
        msg = f"health problems: {problems}"
        if details:
            # Detail rides the MESSAGE only (2026-09-01 review: a page must be diagnosable without a
            # shell) — episode identity above stays the problem set, so evolving detail (minutes
            # counting up) never re-triggers an unchanged episode; a reminder just reads fresher.
            extras = [f"{p}: {details[p]}" for p in problems if p in details]
            if extras:
                msg += " — " + "; ".join(extras)
        await self._alert("warning", msg)

    # ------------------------------------------------------------------ store stall watchdog
    async def _probe_store(self) -> None:
        """One liveness probe of the market store per pulse, single-flight (WO-24b-prime).

        Three outcomes, and the difference between them is the diagnosis:

        * **the previous probe is STILL PENDING** — normally no new probe is started. A ping that
          has not returned since the last pulse is the finding, and issuing one per pulse would
          queue a thread per minute behind the same seized resource. But every
          ``_STORE_REPROBE_EVERY``th consecutive pending pulse the old probe is ABANDONED and a
          fresh one issued (WO-26a): a probe pending since 09:15 says nothing about 13:39, and on
          2026-08-25 that is exactly what the pulse spent 4.4 h saying — 264 identical lines, no
          new information, while the actual fault (a starved thread pool, not a seized lock) was
          invisible because it could only have been seen by a probe that got a worker.
        * **this probe times out** — the store is stalled. A timed-out probe is left RUNNING on
          purpose: when it eventually returns it proves how long the seizure lasted, and until then
          it keeps the pending branch above truthful.
        * **the probe RAISES** — the store answered, with an error. That is a different animal
          entirely (a closed connection, a DuckDB error) and is logged as such, not as a stall.

        A fresh probe that SUCCEEDS while older ones still hang is its own diagnosis: the store is
        reachable and something is holding threads. That is ``store_probe_anomaly`` (see
        :meth:`_note_store_ok`) and it counts as recovered — the engine can work again, which is
        the question this watchdog exists to answer.
        """
        if self._store is None:
            return
        now = self._clock.now()
        probe = self._store_probe
        if probe is not None and not probe.done():
            self._store_pending_pulses += 1
            if self._store_pending_pulses % _STORE_REPROBE_EVERY != 0:
                self._note_store_stall(now, pending=True)
                return
            # Nth pending pulse: abandon (never cancel — cancelling destroys the evidence) and
            # re-probe, so the next line reports the store as it is NOW.
            self._store_abandoned.append(probe)
            self._store_probe = None
        task = asyncio.ensure_future(self._store.aping())
        task.add_done_callback(_swallow_probe_result)
        self._store_probe = task
        self._store_pending_pulses = 0
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
            abandoned=self._outstanding_abandoned(),
            timeout_s=self._store_ping_timeout_s,
        )
        if not self._store_stacks_dumped:
            self._store_stacks_dumped = True
            self._dump_thread_stacks()

    def _note_store_ok(self, now: datetime) -> None:
        abandoned = self._outstanding_abandoned()
        if abandoned:
            # A fresh ping answered while N earlier ones are STILL hanging. Both facts matter and
            # neither alone is the story: the store is usable again (so: recovered), and something
            # swallowed N worker threads that never came back (so: not fine). This is the line the
            # 08-25 morning could not produce.
            _log.info(
                "store_probe_anomaly",
                abandoned=abandoned,
                consecutive=self._store_stall_count,
                stalled_s=self._seconds_since(self._store_stall_since, now),
            )
        if self._store_stall_count:
            _log.info(
                "store_stall_recovered",
                stalled_s=self._seconds_since(self._store_stall_since, now),
                consecutive=self._store_stall_count,
                abandoned=abandoned,
            )
        self._store_stall_count = 0
        self._store_stall_since = None
        self._store_stacks_dumped = False          # the NEXT episode gets its own dump
        self._store_last_ok = now

    def _outstanding_abandoned(self) -> int:
        """How many abandoned probes have still not answered — pruned of the ones that came back,
        so this is a live count of threads currently swallowed, not a lifetime total."""
        self._store_abandoned = [p for p in self._store_abandoned if not p.done()]
        return len(self._store_abandoned)

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
