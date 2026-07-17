"""CatchUpRunner over a simulated multi-day off-gap (§2.6 step 5 / §4.2 job_runs watermarks).

Scenario: FIXED_NOW is Wed 2026-06-17 10:05 IST; the engine was last up Fri 2026-06-12 evening.
Missed trading days in the gap: Mon 15, Tue 16 (13/14 = weekend), plus today's already-due morning
jobs. Asserts the §2.6 step-5 classes: safety-critical run-or-verify TODAY (in dependency order,
before everything else), run-latest exactly ONCE for the whole gap, date-keyed once per missed
trading day ascending, watermarks respected on re-run, and the freeze/notify seams on failure.
"""

from __future__ import annotations

from datetime import date, datetime, time

import pytest

from engine.core.calendar import NSECalendar
from engine.core.clock import IST
from engine.core.config import config_dir
from engine.ops.jobs import CatchUpRunner, JobClass, JobRegistry, JobSpec

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


def _build_runner(conn, clock, calendar, registry, *, freeze=None, notify=None):
    return CatchUpRunner(conn, clock, calendar, registry, freeze=freeze, notify=notify)


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
