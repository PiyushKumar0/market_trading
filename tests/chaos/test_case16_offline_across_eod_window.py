"""Chaos case 16 — Offline across the EOD-jobs window.

Failure injected: the engine is offline across the whole EOD-jobs window. Must hold: the next startup
catches up every missed EOD job idempotently via job_runs watermarks (date-keyed jobs once per missed
trading day, run-latest jobs as a single snapshot) and the startup report lists what was caught up.

Scenario (``tests/chaos/_lifecycle_rig.py``, job bodies recorded): Tue's session left every job
watermarked; process A runs Wed 2026-06-17 09:35 → clean stop 15:40, BEFORE the EOD window (reco-expiry
15:45 … tick compaction 22:30); process B boots Thu 08:05. The DuckDB ``market_backup`` export (a lane job
behind the real ``_market_backup`` start gate) does NOT run at the 08:05 boot but once, under Wed, in the
first sweep after 15:45. ``corp_actions`` runs RUN_LATEST in the registry; for a one-day gap that also
means "exactly once" (case 17 covers multi-day). The EOD safety-critical ``earnings_calendar`` is
replayed because the runner resolves the run that governs the next entries (``CatchUpRunner._governing_day``).

Unbuilt (skipped): the champion/challenger evaluation; ``JOB_CHAMP_CHALL`` is a reserved watermark id with
no registered job.
"""

from __future__ import annotations

from datetime import date, datetime, time

import pytest

from engine.core.clock import IST
from engine.core.enums import RiskState
from engine.notify.catalog import MessageKind
from engine.ops import main as opsmain
from engine.ops.jobs import (
    JOB_BACKUP,
    JOB_BHAVCOPY,
    JOB_CORP_ACTIONS,
    JOB_DEALS,
    JOB_EARNINGS,
    JOB_MARKET_BACKUP,
    JOB_NIGHTLY_REVIEW,
    JOB_RECONCILE,
    JOB_TICK_COMPACT,
    JobClass,
)
from tests.chaos._lifecycle_rig import EngineProcess, RigEnv, run_session

TUE, WED, THU = date(2026, 6, 16), date(2026, 6, 17), date(2026, 6, 18)
A_BOOT, A_STOP, B_BOOT = time(9, 35), time(15, 40), time(8, 5)   # A boots after the 09:30 time exit

#: The date-keyed jobs the drill-catalog row names.
PLAN_NAMED_DATE_KEYED = {JOB_RECONCILE, JOB_BHAVCOPY, JOB_DEALS, JOB_NIGHTLY_REVIEW}


def _t(d: date, hh: int, mm: int, ss: int = 0) -> datetime:
    return datetime(d.year, d.month, d.day, hh, mm, ss, tzinfo=IST)


def _at(d: date, tm: time) -> datetime:
    return datetime.combine(d, tm, tzinfo=IST)


def _missed_eod(proc: EngineProcess) -> tuple[set[str], set[str], set[str]]:
    """Jobs from the real registry whose fire-time is after the 15:40 stop: (date-keyed, run-latest, safety-critical) ids."""
    dk = {s.job_id for s in proc.registry.specs(JobClass.DATE_KEYED) if s.at > A_STOP and s.fire_day is None}
    rl = {s.job_id for s in proc.registry.specs(JobClass.RUN_LATEST) if s.at > A_STOP and s.fire_day is None}
    sc = {s.job_id for s in proc.registry.specs(JobClass.SAFETY_CRITICAL) if s.at > A_STOP}
    return dk, rl, sc


async def _offline_across_wed_eod(tmp_path, monkeypatch) -> tuple[RigEnv, EngineProcess, int, int]:
    """Tue evening session → Wed session (stops before the EOD window) → Thu 08:05 boot.
    Returns (env, process B, job-ledger index at B's boot, notification index at B's boot)."""
    env = RigEnv(tmp_path, monkeypatch, start=_t(TUE, 22, 45))
    await run_session(env, _t(TUE, 22, 45), _t(TUE, 22, 50))
    await run_session(env, _at(WED, A_BOOT), _at(WED, A_STOP))
    assert [c for c in env.calls() if c[1] == WED] == [], "precondition: no Wed date-keyed run yet"
    env.at(_at(THU, B_BOOT))
    calls_b, sent_b = len(env.job_calls), len(env.sent)
    b = EngineProcess(env)
    # Preconditions on the configured schedule: nothing fires inside A's uptime, nothing of Thu's is due at B's boot.
    specs = b.registry.specs()
    assert not [s.job_id for s in specs if A_BOOT < s.at <= A_STOP], "a job fires inside A's uptime"
    assert min(s.at for s in specs) > B_BOOT, "a Thu job is already due at B's boot"
    await b.boot()
    return env, b, calls_b, sent_b


async def test_case16_missed_eod_jobs_caught_up_once_idempotently(tmp_path, monkeypatch):
    env, b, calls_b, sent_b = await _offline_across_wed_eod(tmp_path, monkeypatch)
    report = b.report
    assert report is not None and report.crash_recovered is False
    calls = env.calls(since=calls_b)
    eod_date_keyed, eod_run_latest, eod_safety = _missed_eod(b)
    assert PLAN_NAMED_DATE_KEYED | {JOB_TICK_COMPACT} <= eod_date_keyed
    assert {JOB_BACKUP, JOB_CORP_ACTIONS, JOB_MARKET_BACKUP} <= eod_run_latest
    assert JOB_EARNINGS in eod_safety
    # The export never STARTS 06:00-15:45 on a trading day: the boot leaves it missed for the first lane pass after 15:45.
    assert JOB_MARKET_BACKUP not in {j for j, _ in calls}
    assert not b.catch_up.was_run(JOB_MARKET_BACKUP, WED)
    eod_run_latest -= {JOB_MARKET_BACKUP}

    # Date-keyed: one run per missed trading day. Run-latest and safety-critical: a single run, recorded under the missed fire-day.
    for job_id in sorted(eod_date_keyed):
        assert [d for j, d in calls if j == job_id] == [WED], f"{job_id}: {[c for c in calls if c[0] == job_id]}"
    for job_id in sorted(eod_run_latest | eod_safety):
        assert [j for j, _ in calls if j == job_id] == [job_id], job_id
        assert b.catch_up.was_run(job_id, WED)
    # Nothing that was not missed ran.
    assert {j for j, _ in calls} == eod_date_keyed | eod_run_latest | eod_safety
    assert report.jobs_failed == [] and report.frozen_reasons == []
    assert b.mode.risk_state() == RiskState.NORMAL
    assert b.catch_up.stale_safety_jobs() == []

    # The StartupReport lists the boot pass; the owner's CATCHUP_REPORTs list every pass (post-arm and lane included).
    everything = {f"{j}:{WED.isoformat()}" for j in eod_date_keyed | eod_run_latest | eod_safety}
    boot_pass = everything - {f"{j}:{WED.isoformat()}" for j in (*opsmain.POST_ARM_JOB_IDS, *opsmain.LANE_JOB_IDS)}
    assert set(report.jobs_caught_up) == boot_pass
    catchup_reports = [m for m in env.messages(since=sent_b) if m.kind == MessageKind.CATCHUP_REPORT]
    owner_listed = {entry for m in catchup_reports for entry in m.data["jobs_caught_up"]}
    assert owner_listed == everything
    assert all(m.data["jobs_failed"] == [] for m in catchup_reports)

    # Idempotent: the 30-min sweep replays nothing …
    env.at(_t(THU, 8, 6))
    before = len(env.job_calls)
    sweep = await b.sweep()
    assert sweep.jobs_caught_up == [] and env.calls(since=before) == []
    # … and neither does another restart.
    await b.stop()
    env.at(_t(THU, 8, 7))
    c = EngineProcess(env)
    report_c = await c.boot()
    assert report_c.jobs_caught_up == [] and env.calls(since=before) == []

    # The first sweep after 15:45 runs the deferred export: ONE run-latest snapshot under Wed, reported by the lane runner.
    env.at(_t(THU, 15, 46))
    sent_c = len(env.sent)
    await c.sweep()
    exports_since_b = [c_.at for c_ in env.job_calls[calls_b:] if c_.job_id == JOB_MARKET_BACKUP]
    assert exports_since_b == [_t(THU, 15, 46)]
    assert c.catch_up.was_run(JOB_MARKET_BACKUP, WED)
    lane_reports = [m.data["jobs_caught_up"] for m in env.messages(since=sent_c)
                    if m.kind == MessageKind.CATCHUP_REPORT
                    and any(e.startswith(f"{JOB_MARKET_BACKUP}:") for e in m.data["jobs_caught_up"])]
    assert lane_reports == [[f"{JOB_MARKET_BACKUP}:{WED.isoformat()}"]]
    # Idempotent too: the next sweep exports nothing.
    env.at(_t(THU, 16, 16))
    await c.sweep()
    assert [c_.job_id for c_ in env.job_calls[calls_b:]].count(JOB_MARKET_BACKUP) == 1
    await c.stop()


def test_case16_champion_challenger_eval_single_snapshot() -> None:
    """No champion/challenger job exists to catch up (``build_job_registry`` registers no runner)."""
    pytest.skip(
        "case 16 champ/chall clause: ChampionChallenger is unbuilt — JOB_CHAMP_CHALL "
        "(ops/jobs.py:132) is a reserved watermark id with no registered job"
    )
