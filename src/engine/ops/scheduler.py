"""Scheduler (§3.2.12, R6).

APScheduler ``AsyncIOScheduler`` (IST tz). EVERY trading job is wrapped by an ``NSECalendar`` guard so it
runs only on trading days (without the calendar ~15 days/year misbehave and the LLM loop burns credit on
closed markets, R6). APScheduler's own misfire/coalesce is a FAST-PATH only — the authoritative gap
mechanism after an engine-off span is the ``CatchUpRunner`` driven by ``job_runs`` watermarks (§2.6),
because APScheduler cannot replay jobs whose fire-time fell while the process was down.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.log import get_logger

_log = get_logger("engine.ops.scheduler")

JobFunc = Callable[[], Awaitable[None]]


class Scheduler:
    """Thin wrapper over AsyncIOScheduler with a trading-day calendar guard (R6)."""

    def __init__(self, clock: Clock, calendar: NSECalendar) -> None:
        self._clock = clock
        self._calendar = calendar
        self._sched = AsyncIOScheduler(timezone=IST)

    def start(self) -> None:
        self._sched.start()
        _log.info("scheduler_started")

    def is_running(self) -> bool:
        """Is the underlying APScheduler ACTUALLY running (WO-25c)?

        Deliberately reads ``AsyncIOScheduler.running`` rather than a flag this wrapper sets in
        :meth:`start`: the 2026-08-24 12:44:58 boot proved the failure mode is "``start()`` was never
        reached at all", and a boot-contract check that trusted our own bookkeeping could only ever
        confirm what the boot path already believed. The scheduler's own state is the ground truth —
        it is False before ``start()`` and True only once triggers can actually fire.

        Timing caveat (APScheduler 3.11 ``AsyncIOScheduler.shutdown`` is ``@run_in_event_loop``): the
        stop is DEFERRED to the loop, so this still answers True for the remainder of the turn in
        which :meth:`shutdown` was called and flips False on the next one. Irrelevant to the boot
        contract — the watchdog is retired before ``shutdown()`` is ever called — but it means this is
        an "is armed" probe, not a synchronous stop acknowledgement.
        """
        return bool(getattr(self._sched, "running", False))

    def shutdown(self) -> None:
        self._sched.shutdown(wait=False)
        _log.info("scheduler_stopped")

    def add_trading_day_job(self, func: JobFunc, *, hour: int, minute: int, job_id: str) -> None:
        """Schedule a daily job that runs ONLY on trading days (calendar-guarded, R6)."""
        self._sched.add_job(
            self._guarded(func, job_id),
            CronTrigger(hour=hour, minute=minute, timezone=IST),
            id=job_id,
            replace_existing=True,
            misfire_grace_time=300,   # fast-path only; CatchUpRunner is the authoritative gap mechanism (§2.6)
            coalesce=True,
        )
        _log.info("job_scheduled", job_id=job_id, at=f"{hour:02d}:{minute:02d}", guarded=True)

    def add_job(self, func: JobFunc, *, trigger: CronTrigger, job_id: str, guard: bool = False) -> None:
        """Schedule an arbitrary job. ``guard=True`` applies the trading-day calendar guard (R6)."""
        wrapped = self._guarded(func, job_id) if guard else func
        self._sched.add_job(wrapped, trigger, id=job_id, replace_existing=True,
                            misfire_grace_time=300, coalesce=True)
        _log.info("job_scheduled", job_id=job_id, guarded=guard)

    def remove_job(self, job_id: str) -> None:
        self._sched.remove_job(job_id)

    def _guarded(self, func: JobFunc, job_id: str) -> JobFunc:
        async def _runner() -> None:
            today = self._clock.today()
            if not self._calendar.is_trading_day(today):
                _log.info("job_skipped_non_trading_day", job_id=job_id, date=today.isoformat())
                return
            await func()

        return _runner
