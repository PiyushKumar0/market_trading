"""CatchUpRunner over a simulated multi-day off-gap (§2.6 step 5 / §4.2 job_runs watermarks).

Scenario: FIXED_NOW is Wed 2026-06-17 10:05 IST; the engine was last up Fri 2026-06-12 evening.
Missed trading days in the gap: Mon 15, Tue 16 (13/14 = weekend), plus today's already-due morning
jobs. Asserts the §2.6 step-5 classes: safety-critical run-or-verify TODAY (in dependency order,
before everything else), run-latest exactly ONCE for the whole gap, date-keyed once per missed
trading day ascending, watermarks respected on re-run, and the freeze/notify seams on failure.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, time

import pytest

from engine.core.calendar import NSECalendar
from engine.core.clock import IST
from engine.core.config import config_dir
from engine.ops.jobs import CatchUpRunner, CatchUpScope, JobClass, JobRegistry, JobSpec

OFF_SINCE = datetime(2026, 6, 12, 18, 30, tzinfo=IST)  # Fri evening — 3-trading-day gap to Wed 17th
FRI, MON, TUE, WED = date(2026, 6, 12), date(2026, 6, 15), date(2026, 6, 16), date(2026, 6, 17)


@pytest.fixture
def calendar(clock):
    return NSECalendar(config_dir() / "calendar", clock, strict=False)


def _spec_recorder(calls: list, job_id: str, job_class: JobClass, at: time, *, order=100, fire_day=None,
                   fail_on=None):
    """A JobSpec whose run() appends (job_id, run_for|None) to ``calls``; raises for ``fail_on``."""
    if job_class is JobClass.DATE_KEYED:
        async def run(d: date) -> None:
            if fail_on is not None and d == fail_on:
                raise RuntimeError(f"boom on {d}")
            calls.append((job_id, d))
    else:
        async def run() -> None:
            if fail_on == "always":
                raise RuntimeError("boom")
            calls.append((job_id, None))
    return JobSpec(job_id=job_id, job_class=job_class, at=at, run=run, order=order, fire_day=fire_day)


def _build_runner(conn, clock, calendar, registry, *, freeze=None, notify=None, clear=None):
    return CatchUpRunner(conn, clock, calendar, registry, freeze=freeze, notify=notify, clear=clear)


@pytest.mark.asyncio
async def test_three_day_gap_catch_up_by_class(conn, clock, calendar):
    calls: list = []
    reg = JobRegistry()
    # Safety-critical (deadline: today) — dependency order instruments BEFORE surveillance.
    reg.register(_spec_recorder(calls, "instruments", JobClass.SAFETY_CRITICAL, time(8, 15), order=10))
    reg.register(_spec_recorder(calls, "surveillance", JobClass.SAFETY_CRITICAL, time(8, 20), order=20))
    # Run-latest — one catch-up run covering the whole gap, recorded under the LATEST missed day.
    reg.register(_spec_recorder(calls, "universe_build", JobClass.RUN_LATEST, time(8, 45)))
    # Weekly fire-day (Sunday sector map) — the gap contains Sun 2026-06-14, so run-latest once.
    reg.register(_spec_recorder(calls, "sector_map", JobClass.RUN_LATEST, time(9, 0),
                                fire_day=lambda d: d.weekday() == 6))
    # Date-keyed — one run per missed trading day, ascending.
    reg.register(_spec_recorder(calls, "bhavcopy", JobClass.DATE_KEYED, time(18, 30)))

    runner = _build_runner(conn, clock, calendar, reg)
    runner.record_run("universe_build", FRI)
    runner.record_run("bhavcopy", FRI)

    result = await runner.catch_up(off_since=OFF_SINCE)

    # Date-keyed enumerated each missed trading day (Mon/Tue; Wed's 18:30 not yet due at 10:05).
    assert [(j, d) for j, d in calls if j == "bhavcopy"] == [("bhavcopy", MON), ("bhavcopy", TUE)]
    # Run-latest ran exactly ONCE despite three missed fire-days, recorded under the latest (Wed).
    assert [(j, d) for j, d in calls if j == "universe_build"] == [("universe_build", None)]
    assert runner.was_run("universe_build", WED) is True
    assert runner.was_run("universe_build", MON) is False  # never per-day for run-latest (§2.6)
    # Weekly job caught up once for the in-gap Sunday.
    assert [(j, d) for j, d in calls if j == "sector_map"] == [("sector_map", None)]
    assert runner.was_run("sector_map", date(2026, 6, 14)) is True
    # Safety-critical ran first, in dependency order, then run-latest, then date-keyed (§2.6 classes).
    job_order = [j for j, _ in calls]
    assert job_order[:2] == ["instruments", "surveillance"]
    assert job_order.index("universe_build") < job_order.index("bhavcopy")
    assert runner.was_run("instruments", WED) and runner.was_run("surveillance", WED)
    assert result.jobs_failed == [] and result.frozen_reasons == []
    assert result.off_duration_s == pytest.approx((clock.now() - OFF_SINCE).total_seconds())

    # Watermarks respected: a second pass replays NOTHING.
    calls.clear()
    result2 = await runner.catch_up(off_since=OFF_SINCE)
    assert calls == [] and result2.jobs_caught_up == []


@pytest.mark.asyncio
async def test_safety_critical_failure_freezes_and_alerts(conn, clock, calendar):
    calls: list = []
    frozen: list[str] = []
    sent: list = []

    async def freeze(reason: str) -> None:
        frozen.append(reason)

    async def notify(msg) -> None:
        sent.append(msg)

    reg = JobRegistry()
    reg.register(_spec_recorder(calls, "instruments", JobClass.SAFETY_CRITICAL, time(8, 15), fail_on="always"))
    runner = _build_runner(conn, clock, calendar, reg, freeze=freeze, notify=notify)

    result = await runner.catch_up(off_since=OFF_SINCE)
    assert result.frozen_reasons == ["data_freshness:instruments"]
    assert frozen == ["data_freshness:instruments"]
    kinds = [str(m.kind) for m in sent]
    assert "data_freshness_frozen" in kinds
    assert "catchup_report" in kinds        # the failure still reaches the owner report
    # The failed attempt is recorded (status != success) so the self-test still sees it stale.
    assert runner.was_run("instruments", WED) is False
    assert runner.stale_safety_jobs() == ["instruments"]


@pytest.mark.asyncio
async def test_safety_critical_success_clears_the_latched_freshness_cause(conn, clock, calendar):
    """2026-08-06 live bug: a pre-login catch-up failure latched ``data_freshness:instruments``;
    the post-login catch-up re-ran instruments SUCCESSFULLY but nothing cleared the cause — the
    platform stayed FROZEN all day on stale evidence. A success — or an already-verified-fresh
    watermark on the next boot — must clear the job's own cause (idempotent; a latch clear on an
    inactive cause is a no-op recompute)."""
    calls: list = []
    frozen: list[str] = []
    cleared: list[str] = []

    async def freeze(reason: str) -> None:
        frozen.append(reason)

    async def clear(reason: str) -> None:
        cleared.append(reason)

    reg = JobRegistry()
    reg.register(_spec_recorder(calls, "instruments", JobClass.SAFETY_CRITICAL, time(8, 15)))
    runner = _build_runner(conn, clock, calendar, reg, freeze=freeze, clear=clear)

    # Catch-up runs the job successfully ⇒ its (possibly latched) cause clears.
    await runner.catch_up(off_since=OFF_SINCE)
    assert cleared == ["data_freshness:instruments"]
    assert frozen == []

    # Restart shape: watermarked fresh today ⇒ cleared again — the boot self-heals a stale latch.
    cleared.clear()
    await runner.catch_up(off_since=OFF_SINCE)
    assert cleared == ["data_freshness:instruments"]
    assert frozen == []


@pytest.mark.asyncio
async def test_safety_critical_failure_never_clears(conn, clock, calendar):
    """The clear fires ONLY on verified freshness — a failing job keeps its cause latched."""
    cleared: list[str] = []

    async def clear(reason: str) -> None:
        cleared.append(reason)

    reg = JobRegistry()
    reg.register(_spec_recorder([], "instruments", JobClass.SAFETY_CRITICAL, time(8, 15), fail_on="always"))
    runner = _build_runner(conn, clock, calendar, reg, clear=clear)

    await runner.catch_up(off_since=OFF_SINCE)
    assert cleared == []


@pytest.mark.asyncio
async def test_date_keyed_failure_stops_replay_and_resumes(conn, clock, calendar):
    calls: list = []
    reg = JobRegistry()
    reg.register(_spec_recorder(calls, "bhavcopy", JobClass.DATE_KEYED, time(18, 30), fail_on=TUE))
    runner = _build_runner(conn, clock, calendar, reg)
    runner.record_run("bhavcopy", FRI)

    result = await runner.catch_up(off_since=OFF_SINCE)
    assert [(j, d) for j, d in calls] == [("bhavcopy", MON)]          # replay stopped at the failure
    assert result.jobs_failed == [f"bhavcopy:{TUE.isoformat()}"]
    assert runner.last_success_date("bhavcopy") == MON                # watermark preserved

    # Next startup resumes EXACTLY at the failed day (idempotent, §2.6).
    calls.clear()
    reg2 = JobRegistry()
    reg2.register(_spec_recorder(calls, "bhavcopy", JobClass.DATE_KEYED, time(18, 30)))
    runner2 = _build_runner(conn, clock, calendar, reg2)
    await runner2.catch_up(off_since=OFF_SINCE)
    assert [(j, d) for j, d in calls] == [("bhavcopy", TUE)]


class _NotOkResult:
    """A minimal stand-in for a job's ``ok``-bearing return (e.g. ``BhavcopyResult``)."""

    def __init__(self, ok: bool) -> None:
        self.ok = ok


def _spec_recorder_notok(calls: list, job_id: str, at: time, *, notok_on: date, order: int = 100):
    """A DATE_KEYED JobSpec whose run() appends (job_id, d) then returns ``ok=False`` for
    ``notok_on`` — the degrade-without-raise E5 shape (e.g. ``BhavcopyResult(ok=False)``)."""

    async def run(d: date) -> _NotOkResult:
        calls.append((job_id, d))
        return _NotOkResult(ok=(d != notok_on))

    return JobSpec(job_id=job_id, job_class=JobClass.DATE_KEYED, at=at, run=run, order=order)


@pytest.mark.asyncio
async def test_date_keyed_notok_return_stops_replay_and_resumes(conn, clock, calendar):
    """2026-08-12 live bug: a job that returns ``ok=False`` WITHOUT raising (bhavcopy's E5 shape —
    it degrades + alerts + returns, never raises) must be treated exactly like an exception: recorded
    failed, listed in jobs_failed, replay stopped. Before the fix this hit ``record_run`` with its
    default ``status='success'`` and the day was silently skipped by every future sweep."""
    calls: list = []
    reg = JobRegistry()
    reg.register(_spec_recorder_notok(calls, "bhavcopy", time(18, 30), notok_on=TUE))
    runner = _build_runner(conn, clock, calendar, reg)
    runner.record_run("bhavcopy", FRI)

    result = await runner.catch_up(off_since=OFF_SINCE)
    assert [(j, d) for j, d in calls] == [("bhavcopy", MON), ("bhavcopy", TUE)]  # both attempted
    assert result.jobs_failed == [f"bhavcopy:{TUE.isoformat()}"]
    assert runner.last_success_date("bhavcopy") == MON                 # watermark preserved at MON
    assert runner.was_run("bhavcopy", TUE) is False

    # Next startup resumes EXACTLY at the not-ok day — was_run() is False so it is NOT skipped.
    calls.clear()
    reg2 = JobRegistry()
    reg2.register(_spec_recorder(calls, "bhavcopy", JobClass.DATE_KEYED, time(18, 30)))
    runner2 = _build_runner(conn, clock, calendar, reg2)
    await runner2.catch_up(off_since=OFF_SINCE)
    assert [(j, d) for j, d in calls] == [("bhavcopy", TUE)]


@pytest.mark.asyncio
async def test_safety_critical_not_yet_due_today_is_skipped(conn, clock, calendar):
    """A safety job whose fire-time is later today is NOT force-run — the re-armed scheduler fires
    it; freshness is re-verified before entries (§2.6)."""
    calls: list = []
    reg = JobRegistry()
    reg.register(_spec_recorder(calls, "earnings_calendar", JobClass.SAFETY_CRITICAL, time(18, 0)))
    runner = _build_runner(conn, clock, calendar, reg)
    result = await runner.catch_up(off_since=OFF_SINCE)
    assert calls == [] and result.jobs_caught_up == []
    assert runner.stale_safety_jobs() == []   # not due yet ⇒ not stale


def test_stale_safety_jobs_predicate(conn, clock, calendar):
    """The §3.2.12 data-freshness predicate: due-today-but-unrecorded ⇒ stale; recorded ⇒ fresh."""
    async def run() -> None:  # pragma: no cover - never invoked here
        pass

    reg = JobRegistry()
    reg.register(JobSpec(job_id="instruments", job_class=JobClass.SAFETY_CRITICAL, at=time(8, 15), run=run))
    runner = _build_runner(conn, clock, calendar, reg)
    assert runner.stale_safety_jobs() == ["instruments"]
    runner.record_run("instruments", WED)
    assert runner.stale_safety_jobs() == []


def test_no_registry_is_pure_watermark_store(conn, clock, calendar):
    runner = CatchUpRunner(conn, clock, calendar)
    assert runner.has_registry is False
    assert runner.stale_safety_jobs() == []
    runner.record_run("bhavcopy", MON)
    assert runner.was_run("bhavcopy", MON) is True


# =========================================================================== WO-15: boot ordering
# The 2026-08-10 wedge: the news chain ran INSIDE lifecycle.startup, ahead of scheduler.start(), and
# an 8 h hang starved every scheduled job — including the 30-min sweep that exists to self-heal. The
# machinery half of the fix is here (the firing point itself lives in engine.ops.main): a pass is
# SCOPED, so boot replays load-bearing steps only, the deferred ids fire as a post-arm one-shot
# through this same code, and the sweep (ALL) is their retry path. Passes are single-flight.
DEFERRED = ("news_chain",)


def _wo15_registry(calls: list) -> JobRegistry:
    """universe_build (load-bearing) + news_chain (deferred) + bhavcopy (load-bearing, date-keyed)."""
    reg = JobRegistry()
    reg.register(_spec_recorder(calls, "universe_build", JobClass.RUN_LATEST, time(8, 45), order=10))
    reg.register(_spec_recorder(calls, "news_chain", JobClass.RUN_LATEST, time(8, 25), order=20))
    reg.register(_spec_recorder(calls, "bhavcopy", JobClass.DATE_KEYED, time(18, 30)))
    return reg


@pytest.mark.asyncio
async def test_boot_pass_runs_load_bearing_only_and_the_post_arm_pass_runs_the_deferred(conn, clock, calendar):
    calls: list = []
    runner = CatchUpRunner(conn, clock, calendar, _wo15_registry(calls), deferred=DEFERRED)

    boot = await runner.catch_up(off_since=OFF_SINCE)          # the lifecycle's call — default scope

    # Fri/Mon/Tue bhavcopy replays (Wed's 18:30 is not due at 10:05) — and no news_chain anywhere.
    assert [j for j, _ in calls] == ["universe_build", "bhavcopy", "bhavcopy", "bhavcopy"]
    assert runner.was_run("news_chain", WED) is False          # nothing green-stamped it either
    assert not any(e.startswith("news_chain") for e in boot.jobs_caught_up)

    calls.clear()
    post_arm = await runner.catch_up(scope=CatchUpScope.DEFERRED)

    assert [j for j, _ in calls] == ["news_chain"]              # ONLY the deferred set
    assert runner.was_run("news_chain", WED) is True            # same machinery, same watermark
    assert post_arm.jobs_caught_up == [f"news_chain:{WED.isoformat()}"]


@pytest.mark.asyncio
async def test_failed_post_arm_run_is_swept_by_the_all_scope_sweep(conn, clock, calendar):
    """A post-arm one-shot that fails must still be swept — which is exactly why the periodic sweep
    asks for ALL rather than the default scope (WO-15 (i): 'the catch-up machinery should still track
    their watermarks so a failed post-arm run is swept')."""
    calls: list = []
    reg = JobRegistry()
    reg.register(_spec_recorder(calls, "news_chain", JobClass.RUN_LATEST, time(8, 25), fail_on="always"))
    runner = CatchUpRunner(conn, clock, calendar, reg, deferred=DEFERRED)

    failed = await runner.catch_up(scope=CatchUpScope.DEFERRED)
    assert failed.jobs_failed == [f"news_chain:{WED.isoformat()}"]
    assert runner.was_run("news_chain", WED) is False

    # A default-scope pass would NOT retry it (that is the point of the deferred set)...
    calls.clear()
    reg2 = JobRegistry()
    reg2.register(_spec_recorder(calls, "news_chain", JobClass.RUN_LATEST, time(8, 25)))
    runner2 = CatchUpRunner(conn, clock, calendar, reg2, deferred=DEFERRED)
    await runner2.catch_up()
    assert calls == []
    # ...the sweep does.
    swept = await runner2.catch_up(scope=CatchUpScope.ALL)
    assert [j for j, _ in calls] == ["news_chain"]
    assert swept.jobs_caught_up == [f"news_chain:{WED.isoformat()}"]
    assert runner2.was_run("news_chain", WED) is True


@pytest.mark.asyncio
async def test_sweep_during_an_in_flight_pass_is_a_logged_no_op(conn, clock, calendar, caplog):
    """WO-15 (ii) single-flight: the reordering makes sweep-vs-post-arm concurrency reachable
    (APScheduler's max_instances=1 only serializes sweep-vs-sweep), and watermarks cannot save a
    CONCURRENT pass — both would see the same un-watermarked job and run it twice."""
    calls: list = []
    started, release = asyncio.Event(), asyncio.Event()

    async def slow_chain() -> None:
        started.set()
        await release.wait()
        calls.append(("news_chain", None))

    reg = JobRegistry()
    reg.register(JobSpec("news_chain", JobClass.RUN_LATEST, time(8, 25), slow_chain, order=20))
    reg.register(_spec_recorder(calls, "bhavcopy", JobClass.DATE_KEYED, time(18, 30)))
    runner = CatchUpRunner(conn, clock, calendar, reg, deferred=DEFERRED)

    in_flight = asyncio.create_task(runner.catch_up(scope=CatchUpScope.DEFERRED))
    await asyncio.wait_for(started.wait(), timeout=5)

    with caplog.at_level(logging.INFO, logger="engine.ops.jobs"):
        sweep = await runner.catch_up(scope=CatchUpScope.ALL)

    assert sweep.skipped_in_flight is True
    assert sweep.jobs_caught_up == [] and sweep.jobs_failed == []
    assert calls == []                       # not even the load-bearing half ran a second time
    assert [r for r in caplog.records if r.getMessage() == "catch_up_skipped_in_flight"]

    release.set()
    done = await asyncio.wait_for(in_flight, timeout=5)
    assert done.skipped_in_flight is False
    assert done.jobs_caught_up == [f"news_chain:{WED.isoformat()}"]

    # The lock is released, so the next sweep is a normal pass again.
    after = await runner.catch_up(scope=CatchUpScope.ALL)
    assert after.skipped_in_flight is False


@pytest.mark.asyncio
async def test_no_deferred_set_is_the_pre_wo15_behavior(conn, clock, calendar):
    """The rollback path (``DEFER_POST_ARM_JOBS = False`` ⇒ ``deferred=()``): every scope is the whole
    registry again, and the post-arm one-shot has nothing to do."""
    calls: list = []
    runner = CatchUpRunner(conn, clock, calendar, _wo15_registry(calls))     # deferred defaults to ()

    await runner.catch_up(off_since=OFF_SINCE)
    assert "news_chain" in [j for j, _ in calls]           # ran INSIDE the boot pass, as before

    calls.clear()
    post_arm = await runner.catch_up(scope=CatchUpScope.DEFERRED)
    assert calls == [] and post_arm.jobs_caught_up == []
