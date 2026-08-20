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

**Boot ordering (WO-15, 2026-08-13).** A pass is scoped (:class:`CatchUpScope`): the boot pass runs
LOAD-BEARING data steps only, while the ids the integrator declares ``deferred`` (the news chain →
digest → planner, plus tick compaction) fire as one-shots AFTER ``scheduler.start()`` through the
same machinery under ``DEFERRED`` — same code, same watermarks, new firing point. The 08-10 wedge
(an unbounded news chain inside boot) starved every scheduled job for 8 h *including* the sweep that
exists to self-heal; behind the armed scheduler the identical wedge costs only the digest. Passes
are single-flight so the post-arm one-shot and the 30-min sweep can never replay a job twice.
"""

from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Awaitable, Callable, Collection
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


def _job_result_ok(result: object) -> bool:
    """A job's return value participates in the watermark verdict (2026-08-13 fix): most jobs return
    ``None`` (unaffected, defaults True); an E5 job that degrades-without-raising (e.g. bhavcopy) can
    return a result object whose ``ok=False`` must now sink the watermark too.

    Deliberately NOT widened to bare ``bool`` returns (WO-14): the advisory LLM jobs return a
    tri-state (:class:`AdvisoryOutcome`) whose "governor blocked me" case is a CORRECT outcome, and
    a blanket ``bool``-is-the-verdict rule would turn every blocked run into a retry that spends.
    The composition-root wrappers translate that tri-state into :class:`AdvisoryRun` (``.ok``)."""
    return bool(getattr(result, "ok", True))


class AdvisoryOutcome(StrEnum):
    """How an advisory LLM job (§5.3 pre-open planner, §5.5 nightly reviewer) ended — WO-14 (c).

    These jobs are advisory-only and NEVER raise into the scheduler (§2.7: planner death blocks
    nothing), so before WO-14 their ``bool`` return was discarded and every run — blocked, failed or
    real — green-stamped its watermark. The tri-state distinguishes the two false cases:

    - ``RAN``     — the work was done and persisted ⇒ success watermark.
    - ``BLOCKED`` — the §5.6 budget governor declined the call. A CORRECT outcome, not a failure:
      success watermark, no retry (a retry loop here would burn LLM budget re-asking a governor that
      is saying no on purpose).
    - ``FAILED``  — harness/roster failure (the call was admitted and did not produce a usable
      result) ⇒ failed watermark, swept by the next §2.6 catch-up pass. The retry is itself governor
      -gated at execution (``can_invoke`` runs again), so the governor bounds the spend.
    """

    RAN = "ran"
    BLOCKED = "blocked"
    FAILED = "failed"


@dataclass(frozen=True)
class AdvisoryRun:
    """WO-14 (c) translation seam: an :class:`AdvisoryOutcome` in the ok-bearing shape the watermark
    machinery already understands (``_job_result_ok`` reads ``.ok``). Built by the composition-root
    wrappers — the jobs themselves stay pure tri-state, and ``_job_result_ok`` stays unwidened."""

    outcome: AdvisoryOutcome

    @property
    def ok(self) -> bool:
        """Only a harness failure sinks the watermark; a governor block is a correct outcome."""
        return self.outcome is not AdvisoryOutcome.FAILED

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
JOB_CATALYST_DIGEST = "catalyst_digest"          # §2.7 step 5 / §4.4 job 14 — run-latest (~08:35)
JOB_RECO_EXPIRE = "reco_expire"                  # §3.6 expired-unconfirmed → no_action — run-latest
JOB_TICK_COMPACT = "tick_compact"                # §4.3 tick-partition compaction (WO-7) — date-keyed
JOB_INS_CROSSINGS = "ins_crossings"              # §6.1 `ins` insider net-buy crossings — date-keyed


class JobClass(StrEnum):
    """§2.6 step-5 catch-up classification."""

    SAFETY_CRITICAL = "safety_critical"   # run/verify before entries open, else FROZEN-for-entries
    RUN_LATEST = "run_latest"             # single catch-up run covering the gap (run-latest-once)
    DATE_KEYED = "date_keyed"             # one run per missed trading day


class CatchUpScope(StrEnum):
    """Which slice of the registry a catch-up pass replays (WO-15 boot reordering).

    ``LOAD_BEARING`` is the DEFAULT so the boot path (``SessionLifecycle.startup`` → ``catch_up()``)
    gets the reordering without the lifecycle knowing about it: the never-load-bearing jobs the
    integrator declared ``deferred`` (news chain → digest → planner) are skipped at boot and fired as
    one-shots right after ``scheduler.start()`` under ``DEFERRED``. The periodic sweep asks for
    ``ALL`` — that is what re-runs a deferred job whose post-arm one-shot failed.
    """

    LOAD_BEARING = "load_bearing"   # everything EXCEPT the deferred set (boot + self-test remediation)
    DEFERRED = "deferred"           # ONLY the deferred set (the post-scheduler-arm one-shot)
    ALL = "all"                     # the whole registry (the 30-min sweep; also the rollback path)


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


#: Calendar days a date-keyed day may keep FAILING (measured from its first recorded failure —
#: ``job_runs.first_failed_at``, migration 0009) before catch-up marks it ``skipped`` (terminal).
#: A streak clock, never the day's calendar age: a cold boot after a long off-gap replays old dates
#: on their FIRST-ever attempt, and an age rule would abandon them on one burst of NSE 503s
#: (review-confirmed by execution, 2026-08-18). At the 30-min sweep cadence a 7-day streak means
#: hundreds of attempts while neighboring dates succeeded — a permanent upstream condition
#: (observed: NSE 503 on deals for exactly one date, five days running), not a transient one.
#: The 30-day ``max_lookback_days`` horizon already implied give-up — silently.
GIVE_UP_AFTER_DAYS = 7


class CatchUpResult(BaseModel):
    """What a §2.6 step-5 catch-up pass did (feeds the STARTUP_REPORT / CATCHUP_REPORT)."""

    jobs_caught_up: list[str] = Field(default_factory=list)   # "job_id:YYYY-MM-DD" entries
    jobs_failed: list[str] = Field(default_factory=list)
    frozen_reasons: list[str] = Field(default_factory=list)   # safety-critical failures (§2.6)
    off_duration_s: float = 0.0
    #: WO-15 (ii): this pass did nothing because another pass was already in flight (single-flight).
    skipped_in_flight: bool = False


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
    deferred:
        WO-15: job ids that are NEVER load-bearing for entries and therefore must not run inside the
        boot pass (they fire as one-shots after ``scheduler.start()`` instead). A ``LOAD_BEARING``
        pass skips them, a ``DEFERRED`` pass runs only them, and an ``ALL`` pass (the periodic sweep)
        runs everything — so a deferred job whose post-arm one-shot failed is still swept. Empty ⇒
        the pre-WO-15 behavior exactly (every scope is the whole registry).
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
        clear: FreezeFn | None = None,
        max_lookback_days: int = 30,
        deferred: Collection[str] = (),
    ) -> None:
        self._conn = conn
        self._clock = clock
        self._calendar = calendar
        self._registry = registry
        self._freeze = freeze
        self._notify = notify
        self._deferred = frozenset(deferred)
        #: WO-15 (ii) single-flight: the 30-min sweep must never race a still-running pass. Required
        #: by the reordering — the post-arm one-shot can now still be in flight when the first sweep
        #: fires (APScheduler's max_instances=1 only serializes sweep-vs-sweep). Constructed outside
        #: a running loop is safe on 3.10+ (asyncio.Lock no longer binds a loop at construction).
        self._pass_lock = asyncio.Lock()
        #: Mirror of ``freeze`` (2026-08-06): clears a job's ``data_freshness:<job>`` cause once the
        #: job is verified fresh — without it a pre-login failure latched FROZEN for the whole day
        #: even after the post-login catch-up succeeded (observed live: instruments, 2026-08-06).
        self._clear = clear
        self._max_lookback_days = int(max_lookback_days)
        #: Last failure set sent to the owner (2026-08-18). A stuck failure used to re-send the
        #: identical CATCHUP_REPORT on every 30-min sweep (15+/day observed); a report whose only
        #: content is an unchanged failure set says nothing new. Process state on purpose: a fresh
        #: boot re-sends once, which doubles as the "still broken after restart" signal.
        self._last_failed_alert: list[str] | None = None
        #: (job_id, run_for_date) pairs whose data_freshness FREEZE has already been announced to the
        #: owner (WO-23, IMPROVEMENT_SPEC §205 "fires per attempt"). ``was_run`` keeps a failed
        #: safety-critical job retryable, so without this every 30-min sweep re-sent the identical
        #: freeze alert. The FREEZE itself stays unconditional (idempotent state that must always
        #: hold) — only the notify is one-shot. Process state on purpose, exactly like
        #: ``_last_failed_alert``: a restart re-alerts once, which doubles as "still broken".
        self._freeze_notified: set[tuple[str, str]] = set()

    # ------------------------------------------------------------------ watermarks (§4.2 job_runs)
    @property
    def has_registry(self) -> bool:
        return self._registry is not None and len(self._registry) > 0

    def record_run(self, job_id: str, run_for: date, status: str = "success") -> None:
        """Upsert the day's watermark row. ``first_failed_at`` is the failing-STREAK clock
        (migration 0009): set on the first ``failed`` recording, preserved across repeat failures,
        cleared by success. The give-up decision reads it — never the day's calendar age."""
        now = self._clock.now().isoformat()
        with transaction(self._conn):
            self._conn.execute(
                """
                INSERT INTO job_runs (job_id, run_for_date, last_success_at, last_attempt_at, status, first_failed_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(job_id, run_for_date) DO UPDATE SET
                    last_success_at=CASE WHEN excluded.status='success' THEN excluded.last_success_at ELSE job_runs.last_success_at END,
                    last_attempt_at=excluded.last_attempt_at,
                    status=excluded.status,
                    first_failed_at=CASE
                        WHEN excluded.status='success' THEN NULL
                        WHEN excluded.status='failed' THEN COALESCE(job_runs.first_failed_at, excluded.first_failed_at)
                        ELSE job_runs.first_failed_at
                    END
                """,
                (job_id, run_for.isoformat(), now if status == "success" else None, now, status,
                 now if status == "failed" else None),
            )

    def was_run(self, job_id: str, run_for: date) -> bool:
        """Resolved for ``run_for`` — a success, or a terminal give-up (``skipped``): both mean
        catch-up has nothing left to do for the day. ``failed`` stays unresolved (retryable)."""
        row = self._conn.execute(
            "SELECT status FROM job_runs WHERE job_id=? AND run_for_date=?",
            (job_id, run_for.isoformat()),
        ).fetchone()
        return bool(row and row["status"] in ("success", "skipped"))

    def first_failed_date(self, job_id: str) -> date | None:
        """Oldest retryable (``failed``) day. With per-day continue (2026-08-18) the success
        watermark can advance PAST a failed day, so ``_missed_days`` must anchor its scan here or a
        transient failure would be silently abandoned the moment a later day succeeds."""
        row = self._conn.execute(
            "SELECT min(run_for_date) AS d FROM job_runs WHERE job_id=? AND status='failed'",
            (job_id,),
        ).fetchone()
        return date.fromisoformat(row["d"]) if row and row["d"] else None

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
    async def catch_up(
        self,
        *,
        off_since: datetime | None = None,
        scope: CatchUpScope = CatchUpScope.LOAD_BEARING,
        exclude: Collection[str] = (),
    ) -> CatchUpResult:
        """Replay every missed job in ``scope`` over the off-window, by class then dependency order.

        ``exclude`` (WO-21) drops named job ids from THIS pass only — a caller-side, per-pass veto
        with no watermark side effects, so the next pass that does not veto them replays them
        normally. It exists for the post-arm one-shot's in-session ``tick_compact`` gate
        (``engine.ops.main``); ``deferred`` is the standing set, this is the situational one.

        SINGLE-FLIGHT (WO-15 (ii)): a pass firing while another is still running is a logged no-op,
        never a second concurrent replay. Watermarks make a *later* pass a cheap no-op anyway, but
        they cannot make a CONCURRENT one safe — two passes would both see the same un-watermarked
        job and run it twice (the post-arm news chain vs. the 30-min sweep is exactly that race).
        The ``locked()`` check and the acquire below are not separated by an await point (an
        uncontended ``asyncio.Lock.acquire`` returns without yielding), so no pass can slip between.
        """
        if self._pass_lock.locked():
            _log.info(
                "catch_up_skipped_in_flight", scope=str(scope),
                note="single-flight (§2.6/WO-15): a catch-up pass is already running",
            )
            return CatchUpResult(skipped_in_flight=True)
        async with self._pass_lock:
            return await self._pass(off_since=off_since, scope=scope, exclude=exclude)

    async def _pass(
        self, *, off_since: datetime | None, scope: CatchUpScope, exclude: Collection[str] = ()
    ) -> CatchUpResult:
        now = self._clock.now()
        result = CatchUpResult(
            off_duration_s=max(0.0, (now - off_since).total_seconds()) if off_since else 0.0
        )
        if not self.has_registry:
            _log.info("catch_up_no_registry", note="Phase-1 jobs registered by the integrator (§2.6)")
            return result

        for spec in self._in_scope(JobClass.SAFETY_CRITICAL, scope, exclude):
            await self._run_safety_critical(spec, now, result)
        for spec in self._in_scope(JobClass.RUN_LATEST, scope, exclude):
            await self._run_latest(spec, now, off_since, result)
        for spec in self._in_scope(JobClass.DATE_KEYED, scope, exclude):
            await self._run_date_keyed(spec, now, off_since, result)

        _log.info(
            "catch_up_complete", scope=str(scope),
            caught_up=result.jobs_caught_up, failed=result.jobs_failed,
            frozen=result.frozen_reasons, off_duration_s=result.off_duration_s,
        )
        if self._notify is not None and self._report_is_news(result):
            try:
                await self._notify(catalog.catchup_report(
                    off_duration_s=result.off_duration_s,
                    jobs_caught_up=result.jobs_caught_up,
                    jobs_failed=result.jobs_failed,
                ))
                # Only a DELIVERED report suppresses the next one — a failed send must retry.
                self._last_failed_alert = list(result.jobs_failed)
            except Exception:  # noqa: BLE001 - reporting must never fail the recovery
                _log.exception("catchup_report_notify_failed")
        return result

    def _report_is_news(self, result: CatchUpResult) -> bool:
        """Owner-report suppression (2026-08-18): progress (caught-up) and freeze events always send;
        a pure failure report sends only when the failure set CHANGED since the last one sent —
        an unchanged stuck failure re-reported every 30-min sweep is noise, and the pass's own
        ``catch_up_complete`` log line keeps the full per-pass record either way."""
        if result.jobs_caught_up or result.frozen_reasons:
            return True
        return bool(result.jobs_failed) and result.jobs_failed != self._last_failed_alert

    def _in_scope(
        self, job_class: JobClass, scope: CatchUpScope, exclude: Collection[str] = ()
    ) -> list[JobSpec]:
        """The class's specs in dependency order, filtered by the WO-15 deferred set.

        ``exclude`` (WO-21) is applied FIRST and to every scope — a per-pass veto is unconditional
        by construction, otherwise an ``ALL`` sweep would quietly reinstate what the caller vetoed.
        """
        specs = self._registry.specs(job_class)  # type: ignore[union-attr]
        if exclude:
            specs = [s for s in specs if s.job_id not in exclude]
        if scope is CatchUpScope.ALL or not self._deferred:
            return specs
        if scope is CatchUpScope.DEFERRED:
            return [s for s in specs if s.job_id in self._deferred]
        return [s for s in specs if s.job_id not in self._deferred]

    async def _clear_freshness(self, job_id: str) -> None:
        """Clear ``data_freshness:<job_id>`` after the job is verified fresh (success or a today's
        success watermark). Idempotent — the latch's clear_cause is a no-op recompute on an inactive
        cause. A clear failure degrades to the old always-latched behavior, never fails the pass."""
        if self._clear is None:
            return
        try:
            await self._clear(f"data_freshness:{job_id}")
        except Exception:  # noqa: BLE001 - healing is best-effort; the freeze side must stay intact
            _log.exception("data_freshness_clear_failed", job_id=job_id)

    # ------------------------------------------------------------------ per-class executors
    async def _run_safety_critical(self, spec: JobSpec, now: datetime, result: CatchUpResult) -> None:
        """Deadline job: only TODAY's freshness matters (§2.6 — 'run or verify before entries open').
        Already-recorded-today ⇒ verified fresh, nothing to do. Not yet due today ⇒ the re-armed
        scheduler fires it (and freshness is re-verified before entries by the lifecycle/self-test)."""
        today = now.date()
        if not self._fires_on(spec, today) or self._clock.combine(today, spec.at) > now:
            return
        if self.was_run(spec.job_id, today):
            # Verified fresh — a PRIOR failure's latched cause is stale evidence; clear it so a
            # restart self-heals (2026-08-06: instruments failed pre-login, succeeded post-login,
            # and the latch held FROZEN all day because no path cleared on later success).
            await self._clear_freshness(spec.job_id)
            return
        try:
            outcome = await spec.run()  # type: ignore[call-arg]
            if not _job_result_ok(outcome):
                # degraded return = failure for the watermark; the job already alerted (E5)
                _log.warning("safety_critical_catchup_degraded", job_id=spec.job_id)
                await self._fail_safety_critical(spec, today, result)
                return
            self.record_run(spec.job_id, today)
            result.jobs_caught_up.append(f"{spec.job_id}:{today.isoformat()}")
            await self._clear_freshness(spec.job_id)
        except Exception:  # noqa: BLE001 - a safety-critical failure freezes entries, never crashes boot
            _log.exception("safety_critical_catchup_failed", job_id=spec.job_id)
            await self._fail_safety_critical(spec, today, result)

    async def _fail_safety_critical(self, spec: JobSpec, today: date, result: CatchUpResult) -> None:
        """Shared failure handling for the safety-critical path — an exception and a not-ok return
        are treated identically (record failed, freeze, notify).

        The freeze is unconditional (idempotent); the NOTIFY is once per (job, day) per process
        (WO-23) — a persistently failing job used to re-alert on every 30-min sweep.
        """
        self.record_run(spec.job_id, today, status="failed")
        result.jobs_failed.append(f"{spec.job_id}:{today.isoformat()}")
        reason = f"data_freshness:{spec.job_id}"
        result.frozen_reasons.append(reason)
        if self._freeze is not None:
            await self._freeze(reason)
        key = (spec.job_id, today.isoformat())
        if self._notify is not None and key not in self._freeze_notified:
            await self._notify(catalog.data_freshness_frozen(
                job_id=spec.job_id,
                last_success=self.last_success_at(spec.job_id),
                reason="safety-critical catch-up run failed (§2.6 step 5)",
            ))
            # Only a DELIVERED alert suppresses the next one (mirrors ``_last_failed_alert``): a
            # send that raised must still reach the owner on the following pass.
            self._freeze_notified.add(key)

    async def _run_latest(
        self, spec: JobSpec, now: datetime, off_since: datetime | None, result: CatchUpResult
    ) -> None:
        missed = self._missed_days(spec, now, off_since)
        if not missed:
            return
        target = missed[-1]  # single run-latest covering the whole gap; recorded under the latest day
        try:
            outcome = await spec.run()  # type: ignore[call-arg]
            if not _job_result_ok(outcome):
                # degraded return = failure for the watermark; the job already alerted (E5)
                _log.warning("run_latest_catchup_degraded", job_id=spec.job_id)
                self.record_run(spec.job_id, target, status="failed")
                result.jobs_failed.append(f"{spec.job_id}:{target.isoformat()}")
                return
            self.record_run(spec.job_id, target)
            result.jobs_caught_up.append(f"{spec.job_id}:{target.isoformat()}")
        except Exception:  # noqa: BLE001 - run-latest jobs are never entry-blocking (§2.6/§2.7)
            _log.exception("run_latest_catchup_failed", job_id=spec.job_id)
            self.record_run(spec.job_id, target, status="failed")
            result.jobs_failed.append(f"{spec.job_id}:{target.isoformat()}")

    async def _run_date_keyed(
        self, spec: JobSpec, now: datetime, off_since: datetime | None, result: CatchUpResult
    ) -> None:
        """One run per missed day, ascending. A failed day is recorded and the replay CONTINUES to
        later days (2026-08-18: the old first-failure ``break`` let one NSE-poisoned date —
        ``deals:2026-08-13``, 503 on every retry while adjacent dates fetched fine — block five
        days of later dates from ever being attempted). Dates are per-day independent by the
        DATE_KEYED contract; cross-date dependencies live in the jobs' own missing-data handling.
        A day whose failing STREAK (first recorded failure → now) exceeds
        :data:`GIVE_UP_AFTER_DAYS` is marked ``skipped`` (terminal — ``was_run`` treats it as done)
        instead of retrying forever; a still-``failed`` day that has drifted beyond the
        ``max_lookback_days`` scan horizon is resolved the same way (the horizon already meant
        give-up — silently, and it would otherwise pin ``first_failed_date`` forever)."""
        today = now.date()
        for stale in self._failed_dates_before(spec.job_id, today - timedelta(days=self._max_lookback_days)):
            self.record_run(spec.job_id, stale, status="skipped")
            _log.warning("date_keyed_gave_up", job_id=spec.job_id, run_for=stale.isoformat(),
                         reason="drifted beyond max_lookback_days while failing")
            result.jobs_failed.append(f"{spec.job_id}:{stale.isoformat()} (gave up)")
        for d in self._missed_days(spec, now, off_since, include_failed=True):
            try:
                outcome = await spec.run(d)  # type: ignore[call-arg]
                if not _job_result_ok(outcome):
                    # degraded return = failure for the watermark; the job already alerted (E5)
                    _log.warning("date_keyed_catchup_degraded", job_id=spec.job_id, run_for=d.isoformat())
                    self._record_date_keyed_failure(spec, d, now, result)
                    continue
                self.record_run(spec.job_id, d)
                result.jobs_caught_up.append(f"{spec.job_id}:{d.isoformat()}")
            except Exception:  # noqa: BLE001 - one day's failure never blocks the later days
                _log.exception("date_keyed_catchup_failed", job_id=spec.job_id, run_for=d.isoformat())
                self._record_date_keyed_failure(spec, d, now, result)

    def _record_date_keyed_failure(
        self, spec: JobSpec, d: date, now: datetime, result: CatchUpResult
    ) -> None:
        """Record a failed date-keyed day: retryable (``failed``) until its failing streak — first
        recorded failure through ``now``, NEVER the day's calendar age (a cold boot replays old
        dates on their first-ever attempt) — exceeds :data:`GIVE_UP_AFTER_DAYS`; then terminal
        (``skipped``). An upstream that still errors for one specific date after a week of retries,
        while adjacent dates succeed, is a permanent condition (observed: NSE deals 503 for one date
        across five days), and eternal 30-min retries would hammer it and alert forever. The marker
        is plain ``job_runs`` state, so a manual re-run can still overwrite it."""
        streak_days = self._failing_streak_days(spec.job_id, d, now)
        if streak_days > GIVE_UP_AFTER_DAYS:
            self.record_run(spec.job_id, d, status="skipped")
            _log.warning("date_keyed_gave_up", job_id=spec.job_id, run_for=d.isoformat(),
                         failing_days=streak_days)
            result.jobs_failed.append(f"{spec.job_id}:{d.isoformat()} (gave up)")
        else:
            self.record_run(spec.job_id, d, status="failed")
            result.jobs_failed.append(f"{spec.job_id}:{d.isoformat()}")

    def _failing_streak_days(self, job_id: str, run_for: date, now: datetime) -> int:
        """Whole days since ``run_for``'s first recorded failure (0 when none — first attempts and
        pre-migration rows start their streak on THIS failure, they never give up on it)."""
        row = self._conn.execute(
            "SELECT first_failed_at FROM job_runs WHERE job_id=? AND run_for_date=?",
            (job_id, run_for.isoformat()),
        ).fetchone()
        if not row or not row["first_failed_at"]:
            return 0
        return max(0, (now - datetime.fromisoformat(row["first_failed_at"])).days)

    def _failed_dates_before(self, job_id: str, horizon: date) -> list[date]:
        """Still-``failed`` days older than the scan horizon, ascending — unreachable by retry,
        so they must be resolved (skipped) rather than left pinning the failure anchor forever."""
        rows = self._conn.execute(
            "SELECT run_for_date FROM job_runs WHERE job_id=? AND status='failed' AND run_for_date<? "
            "ORDER BY run_for_date",
            (job_id, horizon.isoformat()),
        ).fetchall()
        return [date.fromisoformat(r["run_for_date"]) for r in rows]

    # ------------------------------------------------------------------ missed-fire-day computation
    def _fires_on(self, spec: JobSpec, d: date) -> bool:
        if spec.fire_day is not None:
            return spec.fire_day(d)
        return self._calendar.is_trading_day(d)  # default: NSE trading days (R6)

    def _missed_days(
        self, spec: JobSpec, now: datetime, off_since: datetime | None, *, include_failed: bool = False
    ) -> list[date]:
        """Fire-days in the scan window whose fire-time passed without being resolved, ascending.

        Scan start: day after the last success watermark; a never-run job anchors at the off-window
        start (``off_since``) or today (fresh install — deep history is the backfill job's business,
        not catch-up's). Always clamped to ``max_lookback_days``.

        ``include_failed`` (the DATE_KEYED caller): pull the start back to the oldest still-``failed``
        day. Since per-day continue (2026-08-18) the success watermark advances past a failed day, so
        without this anchor the day would silently fall out of the scan the moment a later day
        succeeds. Run-latest callers must NOT set it: their failed rows are recorded under gap-target
        dates and pulling the scan back would re-run an already-superseded gap forever.
        """
        today = now.date()
        last = self.last_success_date(spec.job_id)
        if last is not None:
            start = last + timedelta(days=1)
        elif off_since is not None:
            start = off_since.date()
        else:
            start = today
        if include_failed:
            failed = self.first_failed_date(spec.job_id)
            if failed is not None:
                start = min(start, failed)
        start = max(start, today - timedelta(days=self._max_lookback_days))

        missed: list[date] = []
        d = start
        while d <= today:
            if self._fires_on(spec, d) and self._clock.combine(d, spec.at) <= now and not self.was_run(spec.job_id, d):
                missed.append(d)
            d += timedelta(days=1)
        return missed
