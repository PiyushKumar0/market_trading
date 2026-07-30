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

import shutil
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from engine.core.clock import Clock, ClockSkewUnavailable
from engine.core.config import Settings
from engine.core.log import get_logger

_log = get_logger("engine.ops.health")

AlertCallback = Callable[[str, str], Awaitable[None]]   # (severity, message)


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

        if report.problems and self._alert is not None:
            await self._alert("warning", f"health problems: {report.problems}")
        _log.info("health_check", feed=report.feed_state, skew_ok=report.clock_skew_ok,
                  disk_free_gb=report.disk_free_gb, problems=report.problems)
        return report

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
