"""Job registry + startup missed-job catch-up (§2.6 step 5 / §3.2.12 ``CatchUpRunner``).

The engine is up only during active periods (§2.6); APScheduler cannot replay jobs whose fire-time
fell while the process was down, so the **authoritative gap mechanism** is this module: per-job
``job_runs`` watermarks (§4.2) drive re-running every scheduled job whose fire-time fell in the
off-window and is still meaningful, in dependency order, classified per §2.6 step 5:

- **safety/deadline-critical** — instruments+tick-size (A10), surveillance (A8), earnings calendar,
  corp-action ex-date GTT adjustment (A12): must run **or verify fresh** before entries open; a
  failure ⇒ FROZEN-for-entries (via the injected risk-state setter) + ``DATA_FRESHNESS_FROZEN``
  alert. Risk-reducing actions are never gated (R3).
- **idempotent run-latest** — pre-open planner, sector map, universe, the news chain (never
  entry-blocking, §2.7), champion/challenger eval, backups: a single catch-up run covering the whole
  gap, recorded under the LATEST missed fire-day (run-latest-once, NOT per-day).
- **date-keyed backfill** — bhavcopy, deals, official-candle reconcile, daily bars, nightly
  reviewer: one run per missed trading day, ascending; a failure stops that job's replay (the
  watermark is preserved so the next startup resumes exactly there).

Phase-1 jobs are registered by the integrator (``engine.ops.main``) against :class:`JobRegistry`;
this module owns only the machinery. Job ids are the ``job_runs.job_id`` keys and must stay stable
across releases (they ARE the watermark identity).
"""

from __future__ import annotations

import sqlite3
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from enum import StrEnum

from pydantic import BaseModel, Field

from engine.core.calendar import NSECalendar
from engine.core.clock import Clock
from engine.core.db import transaction
from engine.core.log import get_logger
from engine.notify import catalog
from engine.notify.catalog import CatalogMessage

_log = get_logger("engine.ops.jobs")

#: Freeze seam — the lifecycle wires this to ``ModeManager.set_risk_state(FROZEN, reason, RISK_GATE)``.
FreezeFn = Callable[[str], Awaitable[None]]
NotifyFn = Callable[[CatalogMessage], Awaitable[None]]

#: Run signatures: date-keyed jobs receive the ``run_for`` trading day; the other classes take no args.
RunLatestFn = Callable[[], Awaitable[None]]
DateKeyedFn = Callable[[date], Awaitable[None]]

# Canonical §10.1/§4.4 job ids (watermark identities — never rename). The integrator registers the
# Phase-1 callables under these ids; the self-test freshness check keys off the safety subset.
JOB_INSTRUMENTS = "instruments"                  # §4.4 job 4 (A10/A8) — safety-critical
JOB_SURVEILLANCE = "surveillance"                # §4.4 job 5 (A8) — safety-critical
JOB_EARNINGS = "earnings_calendar"               # §4.4 job 8 (R2) — safety-critical
JOB_CORP_ACTIONS_GTT = "corp_actions_gtt_adjust"  # §2.6 step-5 ex-date GTT repair (A12) — safety-critical
JOB_PREOPEN_PLANNER = "preopen_planner"          # §5.3 — run-latest
JOB_SECTOR_MAP = "sector_map"                    # §4.4 job 13 — run-latest (weekly fire-day)
JOB_UNIVERSE = "universe_build"                  # §3.2.4 — run-latest
JOB_NEWS_CHAIN = "news_chain"                    # §4.4 jobs 10+14 chain — run-latest (never entry-blocking)
JOB_CHAMP_CHALL = "champion_challenger_eval"     # §6.4 — run-latest
JOB_BACKUP = "backup"                            # §10.5 — run-latest (watermark-driven)
JOB_BHAVCOPY = "bhavcopy"                        # §4.4 job 6 — date-keyed
JOB_CORP_ACTIONS = "corp_actions"                # §4.4 job 7 (A12 data feed) — run-latest (forward-looking)
JOB_DEALS = "deals"                              # §4.4 job 9 — date-keyed
JOB_FILINGS_PIT = "filings_pit"                  # §2.8 insider trades (NSE PIT, historical) — date-keyed
JOB_FILINGS_PIT_FRESH = "filings_pit_fresh"      # §2.8 stage-3 fresh insider (BSE, same-day) — date-keyed
JOB_FILINGS_RESULTS = "filings_results"          # §2.8 results + board-meeting dates — date-keyed
JOB_FILINGS_SHP = "filings_shp"                  # §2.8 SHP + pledge — run-latest
JOB_RECONCILE = "bar_reconcile"                  # §4.4 job 2 (A13) — date-keyed
JOB_DAILY_BARS = "daily_bars"                    # §4.4 job 3 — date-keyed
JOB_FEATURES = "features_daily"                  # §3.2.5/§6.2 nightly feature snapshot — date-keyed
JOB_NIGHTLY_REVIEW = "nightly_review"            # §5.5 — date-keyed


class JobClass(StrEnum):
    """§2.6 step-5 catch-up classification."""

    SAFETY_CRITICAL = "safety_critical"   # run/verify before entries open, else FROZEN-for-entries
    RUN_LATEST = "run_latest"             # single catch-up run covering the gap (run-latest-once)
    DATE_KEYED = "date_keyed"             # one run per missed trading day


@dataclass(frozen=True)
class JobSpec:
    """One schedulable/catch-up-eligible job (§3.2.12).

    ``run`` takes the ``run_for`` date for DATE_KEYED jobs and no arguments otherwise. ``order`` is
    the dependency order WITHIN the job's class (lower first; ties keep registration order) — e.g.
    the news chain's backfill → cluster → resolve → score → digest ordering, or instruments before
    surveillance. ``fire_day`` overrides the default NSE-trading-day fire predicate (R6) for jobs on
    a different cadence (e.g. the weekly Sunday sector map: ``lambda d: d.weekday() == 6``).
    """

    job_id: str
    job_class: JobClass
    at: time                                       # scheduled fire-time IST (§10.1)
    run: RunLatestFn | DateKeyedFn
    order: int = 100
    fire_day: Callable[[date], bool] | None = None


class JobRegistry:
    """The Phase-1 job inventory the integrator fills; consumed by scheduler re-arm + catch-up."""

    def __init__(self) -> None:
        self._specs: list[JobSpec] = []

    def register(self, spec: JobSpec) -> None:
        if any(s.job_id == spec.job_id for s in self._specs):
            raise ValueError(f"duplicate job_id {spec.job_id!r} — job ids are watermark identities")
        self._specs.append(spec)

    def specs(self, job_class: JobClass | None = None) -> list[JobSpec]:
        """Specs in dependency order (stable sort by ``order`` preserves registration order)."""
        picked = [s for s in self._specs if job_class is None or s.job_class == job_class]
        return sorted(picked, key=lambda s: s.order)

    def __len__(self) -> int:
        return len(self._specs)


class CatchUpResult(BaseModel):
    """What a §2.6 step-5 catch-up pass did (feeds the STARTUP_REPORT / CATCHUP_REPORT)."""

    jobs_caught_up: list[str] = Field(default_factory=list)   # "job_id:YYYY-MM-DD" entries
    jobs_failed: list[str] = Field(default_factory=list)
    frozen_reasons: list[str] = Field(default_factory=list)   # safety-critical failures (§2.6)
    off_duration_s: float = 0.0


class CatchUpRunner:
    """Watermark-driven missed-job catch-up (§2.6 step 5).

    The ``job_runs`` watermark machinery (``record_run``/``was_run``) is Phase-0-stable; the Phase-1
    addition is the registry-driven replay. ``registry=None`` keeps the runner a pure watermark store
    (the Phase-0 wiring in ``engine.ops.main`` builds it without a registry until the integrator
    registers the real jobs).

    Parameters
    ----------
    freeze:
        Async ``(reason) -> None`` risk seam — FROZEN-for-entries on a safety-critical failure.
    notify:
        Typed owner-notification sink (``CatalogMessage``) for CATCHUP_REPORT / DATA_FRESHNESS_FROZEN.
    max_lookback_days:
        Hard horizon (calendar days) for missed-fire-day scans — bounds a fresh install / ancient
        watermark so catch-up never enumerates years (the initial history backfill is its own §4.4
        job, not a catch-up). Spec-silent bound, resolved here; 30 days covers any plausible off-span.
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        clock: Clock,
        calendar: NSECalendar,
        registry: JobRegistry | None = None,
        *,
        freeze: FreezeFn | None = None,
        notify: NotifyFn | None = None,
        max_lookback_days: int = 30,
    ) -> None:
        self._conn = conn
        self._clock = clock
        self._calendar = calendar
        self._registry = registry
        self._freeze = freeze
        self._notify = notify
        self._max_lookback_days = int(max_lookback_days)

    # ------------------------------------------------------------------ watermarks (§4.2 job_runs)
    @property
    def has_registry(self) -> bool:
        return self._registry is not None and len(self._registry) > 0

    def record_run(self, job_id: str, run_for: date, status: str = "success") -> None:
        now = self._clock.now().isoformat()
        with transaction(self._conn):
            self._conn.execute(
                """
                INSERT INTO job_runs (job_id, run_for_date, last_success_at, last_attempt_at, status)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(job_id, run_for_date) DO UPDATE SET
                    last_success_at=CASE WHEN excluded.status='success' THEN excluded.last_success_at ELSE job_runs.last_success_at END,
                    last_attempt_at=excluded.last_attempt_at,
                    status=excluded.status
                """,
                (job_id, run_for.isoformat(), now if status == "success" else None, now, status),
            )

    def was_run(self, job_id: str, run_for: date) -> bool:
        row = self._conn.execute(
            "SELECT status FROM job_runs WHERE job_id=? AND run_for_date=?",
            (job_id, run_for.isoformat()),
        ).fetchone()
        return bool(row and row["status"] == "success")

    def last_success_date(self, job_id: str) -> date | None:
        row = self._conn.execute(
            "SELECT max(run_for_date) AS d FROM job_runs WHERE job_id=? AND status='success'",
            (job_id,),
        ).fetchone()
        return date.fromisoformat(row["d"]) if row and row["d"] else None

    def last_success_at(self, job_id: str) -> str | None:
        row = self._conn.execute(
            "SELECT max(last_success_at) AS at FROM job_runs WHERE job_id=? AND status='success'",
            (job_id,),
        ).fetchone()
        return row["at"] if row else None

    # ------------------------------------------------------------------ freshness (§3.2.12 self-test)
    def stale_safety_jobs(self, now: datetime | None = None) -> list[str]:
        """Safety-critical jobs whose fire-time passed today without a recorded success — the
        §3.2.12 data-freshness predicate ("today-dated instruments/surveillance/earnings present").
        Empty when no registry is wired (nothing to verify yet)."""
        if not self.has_registry:
            return []
        now = now or self._clock.now()
        today = now.date()
        stale: list[str] = []
        for spec in self._registry.specs(JobClass.SAFETY_CRITICAL):  # type: ignore[union-attr]
            if not self._fires_on(spec, today):
                continue
            if self._clock.combine(today, spec.at) > now:
                continue  # not yet due today — the armed scheduler will fire it
            if not self.was_run(spec.job_id, today):
                stale.append(spec.job_id)
        return stale

    # ------------------------------------------------------------------ the catch-up pass (§2.6 step 5)
    async def catch_up(self, *, off_since: datetime | None = None) -> CatchUpResult:
        """Replay every missed job over the off-window, by class then dependency order (§2.6)."""
        now = self._clock.now()
        result = CatchUpResult(
            off_duration_s=max(0.0, (now - off_since).total_seconds()) if off_since else 0.0
        )
        if not self.has_registry:
            _log.info("catch_up_no_registry", note="Phase-1 jobs registered by the integrator (§2.6)")
            return result

        for spec in self._registry.specs(JobClass.SAFETY_CRITICAL):  # type: ignore[union-attr]
            await self._run_safety_critical(spec, now, result)
        for spec in self._registry.specs(JobClass.RUN_LATEST):  # type: ignore[union-attr]
            await self._run_latest(spec, now, off_since, result)
        for spec in self._registry.specs(JobClass.DATE_KEYED):  # type: ignore[union-attr]
            await self._run_date_keyed(spec, now, off_since, result)

        _log.info(
            "catch_up_complete",
            caught_up=result.jobs_caught_up, failed=result.jobs_failed,
            frozen=result.frozen_reasons, off_duration_s=result.off_duration_s,
        )
        if self._notify is not None and (result.jobs_caught_up or result.jobs_failed):
            try:
                await self._notify(catalog.catchup_report(
                    off_duration_s=result.off_duration_s,
                    jobs_caught_up=result.jobs_caught_up,
                    jobs_failed=result.jobs_failed,
                ))
            except Exception:  # noqa: BLE001 - reporting must never fail the recovery
                _log.exception("catchup_report_notify_failed")
        return result

    # ------------------------------------------------------------------ per-class executors
    async def _run_safety_critical(self, spec: JobSpec, now: datetime, result: CatchUpResult) -> None:
        """Deadline job: only TODAY's freshness matters (§2.6 — 'run or verify before entries open').
        Already-recorded-today ⇒ verified fresh, nothing to do. Not yet due today ⇒ the re-armed
        scheduler fires it (and freshness is re-verified before entries by the lifecycle/self-test)."""
        today = now.date()
        if not self._fires_on(spec, today) or self._clock.combine(today, spec.at) > now:
            return
        if self.was_run(spec.job_id, today):
            return
        try:
            await spec.run()  # type: ignore[call-arg]
            self.record_run(spec.job_id, today)
            result.jobs_caught_up.append(f"{spec.job_id}:{today.isoformat()}")
        except Exception:  # noqa: BLE001 - a safety-critical failure freezes entries, never crashes boot
            _log.exception("safety_critical_catchup_failed", job_id=spec.job_id)
            self.record_run(spec.job_id, today, status="failed")
            result.jobs_failed.append(f"{spec.job_id}:{today.isoformat()}")
            reason = f"data_freshness:{spec.job_id}"
            result.frozen_reasons.append(reason)
            if self._freeze is not None:
                await self._freeze(reason)
            if self._notify is not None:
                await self._notify(catalog.data_freshness_frozen(
                    job_id=spec.job_id,
                    last_success=self.last_success_at(spec.job_id),
                    reason="safety-critical catch-up run failed (§2.6 step 5)",
                ))

    async def _run_latest(
        self, spec: JobSpec, now: datetime, off_since: datetime | None, result: CatchUpResult
    ) -> None:
        missed = self._missed_days(spec, now, off_since)
        if not missed:
            return
        target = missed[-1]  # single run-latest covering the whole gap; recorded under the latest day
        try:
            await spec.run()  # type: ignore[call-arg]
            self.record_run(spec.job_id, target)
            result.jobs_caught_up.append(f"{spec.job_id}:{target.isoformat()}")
        except Exception:  # noqa: BLE001 - run-latest jobs are never entry-blocking (§2.6/§2.7)
            _log.exception("run_latest_catchup_failed", job_id=spec.job_id)
            self.record_run(spec.job_id, target, status="failed")
            result.jobs_failed.append(f"{spec.job_id}:{target.isoformat()}")

    async def _run_date_keyed(
        self, spec: JobSpec, now: datetime, off_since: datetime | None, result: CatchUpResult
    ) -> None:
        for d in self._missed_days(spec, now, off_since):
            try:
                await spec.run(d)  # type: ignore[call-arg]
                self.record_run(spec.job_id, d)
                result.jobs_caught_up.append(f"{spec.job_id}:{d.isoformat()}")
            except Exception:  # noqa: BLE001 - stop this job's replay; watermark resumes it next startup
                _log.exception("date_keyed_catchup_failed", job_id=spec.job_id, run_for=d.isoformat())
                self.record_run(spec.job_id, d, status="failed")
                result.jobs_failed.append(f"{spec.job_id}:{d.isoformat()}")
                break

    # ------------------------------------------------------------------ missed-fire-day computation
    def _fires_on(self, spec: JobSpec, d: date) -> bool:
        if spec.fire_day is not None:
            return spec.fire_day(d)
        return self._calendar.is_trading_day(d)  # default: NSE trading days (R6)

    def _missed_days(self, spec: JobSpec, now: datetime, off_since: datetime | None) -> list[date]:
        """Fire-days in the scan window whose fire-time passed without a recorded success, ascending.

        Scan start: day after the last success watermark; a never-run job anchors at the off-window
        start (``off_since``) or today (fresh install — deep history is the backfill job's business,
        not catch-up's). Always clamped to ``max_lookback_days``.
        """
        today = now.date()
        last = self.last_success_date(spec.job_id)
        if last is not None:
            start = last + timedelta(days=1)
        elif off_since is not None:
            start = off_since.date()
        else:
            start = today
        start = max(start, today - timedelta(days=self._max_lookback_days))

        missed: list[date] = []
        d = start
        while d <= today:
            if self._fires_on(spec, d) and self._clock.combine(d, spec.at) <= now and not self.was_run(spec.job_id, d):
                missed.append(d)
            d += timedelta(days=1)
        return missed
