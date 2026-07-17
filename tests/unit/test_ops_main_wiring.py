"""Composition-root wiring (§3.2.12, §10.1/§4.4): the integrator's ``engine.ops.main`` seams.

``engine.ops.main`` is the only module allowed to import everything; it is not unit-tested elsewhere
(it spawns uvicorn + the ticker child and idles on a stop signal). These tests lock the two pieces the
integrator owns that ARE pure enough to assert without booting the whole engine:

  * ``build_job_registry`` — the single §10.1/§4.4 job inventory feeding BOTH the live Scheduler and
    the startup CatchUpRunner (same registry, §2.6): every Phase-1 job present, at its pinned fire-time,
    in the right §2.6 step-5 class + dependency order, with the weekly Sunday sector-map fire-day.
  * ``_scheduled_runner`` — the live-scheduler wrapper that records the ``job_runs`` watermark on every
    fire (so the CatchUpRunner never re-runs a job the scheduler already ran today) and marks a failed
    run so catch-up retries it, never crashing the loop.
"""

from __future__ import annotations

from datetime import date, time

import pytest

from engine.core.calendar import NSECalendar
from engine.core.config import config_dir, load_settings
from engine.ops import main as opsmain
from engine.ops.jobs import (
    JOB_BHAVCOPY,
    JOB_EARNINGS,
    JOB_INSTRUMENTS,
    JOB_SECTOR_MAP,
    JOB_UNIVERSE,
    CatchUpRunner,
    JobClass,
    JobSpec,
)
from engine.ops.main import (
    PHASE1_JOB_IDS,
    _arm_live_jobs,
    _arm_registry_jobs,
    _scheduled_runner,
    build_job_registry,
)
from engine.ops.scheduler import Scheduler


async def _noop() -> None:
    return None


async def _noop_dated(_d: date) -> None:
    return None


def _all_noop_fns() -> dict:
    """A runner for every Phase-1 job id (DATE_KEYED ids need the dated signature)."""
    date_keyed = {
        opsmain.JOB_RECONCILE, opsmain.JOB_BHAVCOPY, opsmain.JOB_DAILY_BARS,
        opsmain.JOB_DEALS, opsmain.JOB_FEATURES,
        opsmain.JOB_FILINGS_PIT, opsmain.JOB_FILINGS_RESULTS,   # §2.8 date-keyed
    }
    return {jid: (_noop_dated if jid in date_keyed else _noop) for jid in PHASE1_JOB_IDS}


@pytest.fixture
def calendar(clock):
    return NSECalendar(config_dir() / "calendar", clock, strict=False)


# --------------------------------------------------------------------------- registry structure


def test_registry_covers_every_phase1_job() -> None:
    reg = build_job_registry(load_settings(), _all_noop_fns())
    assert {s.job_id for s in reg.specs()} == set(PHASE1_JOB_IDS)
    assert len(reg) == len(PHASE1_JOB_IDS) == 16   # +3 §2.8 filings jobs


def test_registry_classes_and_fire_times_match_the_schedule() -> None:
    """§10.1: instruments 08:15, surveillance 08:20, universe 08:30, reconcile 15:50, bhavcopy 18:00,
    corp-actions 18:15, earnings 18:30, deals 18:45 — in their §2.6 step-5 classes."""
    settings = load_settings()
    by_id = {s.job_id: s for s in build_job_registry(settings, _all_noop_fns()).specs()}

    expected = {
        opsmain.JOB_INSTRUMENTS:  (JobClass.SAFETY_CRITICAL, time(8, 15)),
        opsmain.JOB_SURVEILLANCE: (JobClass.SAFETY_CRITICAL, time(8, 20)),
        opsmain.JOB_EARNINGS:     (JobClass.SAFETY_CRITICAL, time(18, 30)),
        opsmain.JOB_UNIVERSE:     (JobClass.RUN_LATEST,      time(8, 30)),
        opsmain.JOB_NEWS_CHAIN:   (JobClass.RUN_LATEST,      time(8, 25)),
        opsmain.JOB_CORP_ACTIONS: (JobClass.RUN_LATEST,      time(18, 15)),
        opsmain.JOB_SECTOR_MAP:   (JobClass.RUN_LATEST,      time(8, 30)),
        opsmain.JOB_BACKUP:       (JobClass.RUN_LATEST,      time(21, 0)),
        opsmain.JOB_RECONCILE:    (JobClass.DATE_KEYED,      time(15, 50)),
        opsmain.JOB_BHAVCOPY:     (JobClass.DATE_KEYED,      time(18, 0)),
        opsmain.JOB_DAILY_BARS:   (JobClass.DATE_KEYED,      time(18, 5)),
        opsmain.JOB_DEALS:        (JobClass.DATE_KEYED,      time(18, 45)),
        opsmain.JOB_FEATURES:     (JobClass.DATE_KEYED,      time(18, 50)),
        opsmain.JOB_FILINGS_PIT:     (JobClass.DATE_KEYED,   time(18, 35)),
        opsmain.JOB_FILINGS_RESULTS: (JobClass.DATE_KEYED,   time(18, 45)),
        opsmain.JOB_FILINGS_SHP:     (JobClass.RUN_LATEST,   time(18, 50)),
    }
    for jid, (cls, at) in expected.items():
        assert by_id[jid].job_class == cls, jid
        assert by_id[jid].at == at, jid


def test_instruments_runs_before_surveillance_in_dependency_order() -> None:
    """A10/A8: instruments dump (tick sizes) must precede surveillance within the safety-critical class."""
    reg = build_job_registry(load_settings(), _all_noop_fns())
    safety = [s.job_id for s in reg.specs(JobClass.SAFETY_CRITICAL)]
    assert safety.index(JOB_INSTRUMENTS) < safety.index(opsmain.JOB_SURVEILLANCE)


def test_sector_map_fires_only_on_sunday() -> None:
    """§4.4 job 13: the sector/theme map refresh is a weekly Sunday cadence, not a trading-day job."""
    reg = build_job_registry(load_settings(), _all_noop_fns())
    sector = next(s for s in reg.specs() if s.job_id == JOB_SECTOR_MAP)
    assert sector.fire_day is not None
    assert sector.fire_day(date(2026, 7, 12)) is True    # Sunday
    assert sector.fire_day(date(2026, 7, 13)) is False   # Monday
    # Every other job leaves fire_day defaulted (the calendar trading-day guard applies).
    assert all(s.fire_day is None for s in reg.specs() if s.job_id != JOB_SECTOR_MAP)


def test_missing_job_fn_is_a_loud_wiring_error() -> None:
    fns = _all_noop_fns()
    del fns[JOB_EARNINGS]
    with pytest.raises(KeyError):
        build_job_registry(load_settings(), fns)


# --------------------------------------------------------------------------- scheduled_runner watermark


@pytest.mark.asyncio
async def test_scheduled_runner_records_watermark_on_success(conn, clock, calendar) -> None:
    catch_up = CatchUpRunner(conn, clock, calendar)
    seen: list[date] = []

    async def run_it(d: date) -> None:
        seen.append(d)

    spec = JobSpec(JOB_BHAVCOPY, JobClass.DATE_KEYED, time(18, 0), run_it, order=1)
    await _scheduled_runner(spec, catch_up, clock)()

    assert seen == [clock.today()]                       # date-keyed runner got today's date
    assert catch_up.was_run(JOB_BHAVCOPY, clock.today())  # watermark recorded -> catch-up skips it


@pytest.mark.asyncio
async def test_scheduled_runner_marks_failure_without_crashing(conn, clock, calendar) -> None:
    catch_up = CatchUpRunner(conn, clock, calendar)

    async def boom() -> None:
        raise RuntimeError("job blew up")

    spec = JobSpec(JOB_UNIVERSE, JobClass.RUN_LATEST, time(8, 30), boom, order=1)
    await _scheduled_runner(spec, catch_up, clock)()      # must not raise into the scheduler loop

    # A failed run is NOT a success watermark, so the CatchUpRunner will retry it on next startup.
    assert catch_up.was_run(JOB_UNIVERSE, clock.today()) is False


# --------------------------------------------------------------------------- scheduler arming (same registry)


def test_same_registry_arms_every_job_on_the_live_scheduler(conn, clock, calendar) -> None:
    """§2.6/§10.1: the live Scheduler and the CatchUpRunner are fed the SAME JobRegistry — arming must
    register every Phase-1 job id on APScheduler (the sector map as a weekly Sunday cron, the rest as
    calendar-guarded trading-day jobs)."""
    settings = load_settings()
    reg = build_job_registry(settings, _all_noop_fns())
    catch_up = CatchUpRunner(conn, clock, calendar, reg)
    sched = Scheduler(clock, calendar)

    _arm_registry_jobs(sched, reg, catch_up, clock)

    armed = {j.id for j in sched._sched.get_jobs()}
    assert set(PHASE1_JOB_IDS) <= armed
    # The weekly sector-map job carries a day-of-week cron; the daily ones do not.
    sector_trigger = str(next(j for j in sched._sched.get_jobs() if j.id == JOB_SECTOR_MAP).trigger)
    assert "day_of_week='sun'" in sector_trigger


def test_live_interval_jobs_are_armed(clock, calendar) -> None:
    """The always-on interval jobs (not calendar-gated): coarse bar finalization, health, per-feed news."""
    settings = load_settings()
    sched = Scheduler(clock, calendar)

    async def _resolve_news(_hs) -> None:
        return None

    _arm_live_jobs(sched, settings, bar_builder=None, health=None,
                   news_ingest=None, resolve_news=_resolve_news)

    armed = {j.id for j in sched._sched.get_jobs()}
    assert {"bar_advance", "health_check", "news_poll_et", "news_poll_mc", "news_poll_gdelt"} <= armed
