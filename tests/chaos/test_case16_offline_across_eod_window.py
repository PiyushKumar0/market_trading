"""§9.4 chaos case 16 — Offline across the EOD-jobs window.

Plan row (IMPLEMENTATION_PLAN.md §9.4, case 16), "Must hold":

    "next startup catches up every missed EOD job idempotently via job_runs watermarks (§2.6/§4.2):
    date-keyed jobs (reconcile/bhavcopy/corp-actions/deals/nightly review) run once per missed trading
    day; champ/chall eval + backup are single run-latest snapshots; startup report lists what was
    caught up"

Scenario (``tests/chaos/_lifecycle_rig.py``: real lifecycle + CatchUpRunner over the real
``build_job_registry`` inventory, job BODIES recorded instead of hitting NSE/Kite/LLM): Tue's evening
session left every job watermarked; process A runs Wed 2026-06-17 09:05 → clean stop 15:40, i.e.
BEFORE the whole EOD window (reco-expiry 15:45, reconcile 15:50 … tick compaction 22:30). The PC
sleeps overnight; process B boots Thu 08:05 (before the 08:15 instruments fire).

Clauses covered:

* every missed EOD job caught up ONCE for Wed via its watermark — the date-keyed set (bar_reconcile,
  bhavcopy, daily_bars, deals, features_daily, filings_pit, filings_pit_fresh, filings_results,
  ins_crossings, nightly_review, and tick_compact through the compaction lane) with ``run_for`` =
  Wed; the run-latest EOD set (reco_expire, filings_shp, corp_actions, backup) exactly once — backup
  as ONE run-latest snapshot;
* idempotent — an immediate 30-min ``catchup_sweep`` pass (``_catchup_sweep_once``: ALL scope, then
  the compaction lane) and a further restart re-run nothing;
* "startup report lists what was caught up" — ``StartupReport.jobs_caught_up`` and the owner's
  ``CATCHUP_REPORT`` both carry every caught-up ``job:date``.

corp-actions: the §9.4 row lists it with the date-keyed jobs, but the §2.6 step-5 class list (the
detailed spec) does not, and the registry runs ``corp_actions`` RUN_LATEST (forward-looking A12 feed).
For a one-trading-day gap both classes mean "exactly once" — asserted as such (see case 17 for the
multi-day behaviour).

CD-3 (fixed 2026-09-24): the EOD-window SAFETY-CRITICAL job ``earnings_calendar`` (18:30; §2.6 lists
earnings among the EOD jobs AND among the "must run or verify before entries open, else FROZEN +
alert" set) was neither replayed nor flagged, because the runner only considered TODAY's fire-time.
It now resolves the run that governs the next entries (``CatchUpRunner._governing_day``);
``test_case16_missed_eod_jobs_caught_up_once_idempotently`` asserts the replay with the rest of the
missed EOD window.

Unbuilt (skipped, ``test_case16_champion_challenger_eval_single_snapshot``): the champion/challenger
evaluation (§6.4) — ``JOB_CHAMP_CHALL`` is a reserved watermark id only; no job is registered (the
learning layer is Phase 2+ and not implemented).
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
    JOB_NIGHTLY_REVIEW,
    JOB_RECONCILE,
    JOB_TICK_COMPACT,
    JobClass,
)
from tests.chaos._lifecycle_rig import EngineProcess, RigEnv, run_session

TUE, WED, THU = date(2026, 6, 16), date(2026, 6, 17), date(2026, 6, 18)
A_BOOT, A_STOP, B_BOOT = time(9, 5), time(15, 40), time(8, 5)

#: The date-keyed jobs the §9.4 row names (corp-actions: see the module docstring).
PLAN_NAMED_DATE_KEYED = {JOB_RECONCILE, JOB_BHAVCOPY, JOB_DEALS, JOB_NIGHTLY_REVIEW}


def _t(d: date, hh: int, mm: int, ss: int = 0) -> datetime:
    return datetime(d.year, d.month, d.day, hh, mm, ss, tzinfo=IST)


def _at(d: date, tm: time) -> datetime:
    return datetime.combine(d, tm, tzinfo=IST)


def _missed_eod(proc: EngineProcess) -> tuple[set[str], set[str], set[str]]:
    """Wed's missed EOD window, read off the REAL registry: every job whose fire-time is after the
    15:40 stop — (date-keyed ids, run-latest ids, safety-critical ids)."""
    dk = {s.job_id for s in proc.registry.specs(JobClass.DATE_KEYED) if s.at > A_STOP}
    rl = {s.job_id for s in proc.registry.specs(JobClass.RUN_LATEST) if s.at > A_STOP and s.fire_day is None}
    sc = {s.job_id for s in proc.registry.specs(JobClass.SAFETY_CRITICAL) if s.at > A_STOP}
    return dk, rl, sc


async def _offline_across_wed_eod(tmp_path, monkeypatch) -> tuple[RigEnv, EngineProcess, int, int]:
    """Tue evening session → Wed 09:05-15:40 session (stops BEFORE the EOD window) → Thu 08:05 boot.
    Returns (env, process B, job-ledger index at B's boot, notification index at B's boot)."""
    env = RigEnv(tmp_path, monkeypatch, start=_t(TUE, 22, 45))
    await run_session(env, _t(TUE, 22, 45), _t(TUE, 22, 50))
    await run_session(env, _at(WED, A_BOOT), _at(WED, A_STOP))
    assert [c for c in env.calls() if c[1] == WED] == [], "precondition: no Wed date-keyed run yet"
    env.at(_at(THU, B_BOOT))
    calls_b, sent_b = len(env.job_calls), len(env.sent)
    b = EngineProcess(env)
    # Scenario preconditions on the configured schedule (settings.yaml jobs.*): nothing fires inside
    # A's uptime (the rig fires no scheduled jobs there) and nothing of Thu's is due at B's boot.
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
    assert {JOB_BACKUP, JOB_CORP_ACTIONS} <= eod_run_latest
    assert JOB_EARNINGS in eod_safety

    # Date-keyed: exactly one run per missed trading day (Wed), run_for = Wed.
    for job_id in sorted(eod_date_keyed):
        assert [d for j, d in calls if j == job_id] == [WED], f"{job_id}: {[c for c in calls if c[0] == job_id]}"
    # Run-latest (incl. backup) and the EOD safety-critical jobs Thu's entries read (earnings,
    # CD-3): a single run, recorded under the missed fire-day.
    for job_id in sorted(eod_run_latest | eod_safety):
        assert [j for j, _ in calls if j == job_id] == [job_id], job_id
        assert b.catch_up.was_run(job_id, WED)
    # Nothing that was NOT missed ran (Wed's morning ran in A; nothing of Thu's is due yet).
    assert {j for j, _ in calls} == eod_date_keyed | eod_run_latest | eod_safety
    assert report.jobs_failed == [] and report.frozen_reasons == []
    assert b.mode.risk_state() == RiskState.NORMAL
    assert b.catch_up.stale_safety_jobs() == []

    # "startup report lists what was caught up": the boot pass (load-bearing scope) in the
    # StartupReport, and every pass — incl. the post-arm one-shot and the compaction lane — to the owner.
    everything = {f"{j}:{WED.isoformat()}" for j in eod_date_keyed | eod_run_latest | eod_safety}
    boot_pass = everything - {f"{j}:{WED.isoformat()}" for j in (*opsmain.POST_ARM_JOB_IDS, JOB_TICK_COMPACT)}
    assert set(report.jobs_caught_up) == boot_pass
    catchup_reports = [m for m in env.messages(since=sent_b) if m.kind == MessageKind.CATCHUP_REPORT]
    owner_listed = {entry for m in catchup_reports for entry in m.data["jobs_caught_up"]}
    assert owner_listed == everything
    assert all(m.data["jobs_failed"] == [] for m in catchup_reports)

    # Idempotent: the 30-min sweep (ALL scope, exactly as scheduled) replays nothing …
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
    await c.stop()


def test_case16_champion_challenger_eval_single_snapshot() -> None:
    """'champ/chall eval … single run-latest snapshot' — nothing to catch up: no champion/challenger
    job exists (``JOB_CHAMP_CHALL`` is a reserved id; ``build_job_registry`` registers no runner)."""
    pytest.skip(
        "case 16 champ/chall clause: not built in Phase 2 — §6.4 champion/challenger evaluation is "
        "unimplemented (engine.learning is empty; JOB_CHAMP_CHALL has no registered job)"
    )
