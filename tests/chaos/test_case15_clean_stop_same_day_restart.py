"""§9.4 chaos case 15 — Intentional clean stop + restart same day (§2.6).

Plan row (IMPLEMENTATION_PLAN.md §9.4, case 15), "Must hold":

    "clean stop honored (not auto-restarted as a crash); off-window raises no fault alarm; restart runs
    reconcile + catch-up cleanly; no spurious FEED_STALE/incident alerts; resumes correctly"

Scenario (one engine day, Wed 2026-06-17, composed like ``engine.ops.main.run`` — see
``tests/chaos/_lifecycle_rig.py``): the previous evening's session left every Tue job watermarked;
process A boots 08:00, the owner arms RECOMMEND, the scheduler fires instruments / surveillance /
news chain, then at 08:28 the owner runs ``nssm stop``. That stop delivers the console Ctrl-C MORE
THAN ONCE (the 2026-09-02..23 regression: every stop logged ``stop_forced`` 0.85-1.4 s after
``stop_requested``, mid shutdown-backup, so the clean stop was never committed and every next boot
was a "crash"). The repeats are delivered to the real counted stop handler (``_make_stop_handler``,
injectable monotonic) WHILE ``SessionLifecycle.shutdown`` is inside its backup hook. The engine is off
08:28-09:05 (the universe / digest / planner fire-times pass while it is down), the out-of-band
watchdog polls throughout, then process B restarts at 09:05.

Clauses covered here:

* clean stop honored — repeat signals inside the grace window never reach ``force_exit`` (so the
  process exits 0 ⇒ NSSM ``AppExit 0 Exit``, not the crash-restart path); ``STOPPED`` +
  ``last_clean_stop_at`` committed; the shutdown backup completed; exactly one ``ENGINE_STOPPED``;
  the next startup reports ``crash_recovered=False`` / ``prior_state=STOPPED`` with the off-duration
  measured from the clean stop.
* off-window raises no fault alarm — every watchdog poll across the gap sends nothing (no
  ``ENGINE_DOWN``, no ``SCHEDULED_START_MISSED``, no force-kill).
* restart runs catch-up cleanly — exactly the jobs whose fire-time fell in the off-window run, once
  each; the ones process A already ran are not re-run; nothing fails, nothing freezes.
* no spurious FEED_STALE/incident alerts — none in the owner's inbox across stop + restart, no
  critical alert, and the health pulse on the resumed (WARMING) feed reports no problem.
* resumes correctly — sticky RECOMMEND mode and NORMAL risk state carried over, heartbeat beating,
  every registry job re-armed on a running scheduler, ticker resumed into WARMING, watchdog silent.

Phase-3-gated (skipped): "restart runs RECONCILE" — the §2.6 step-2 reconcile-vs-broker hook
(Reconciler, WO-P3-6) does not exist yet; the lifecycle reports the step as deferred (asserted below
so the gap stays visible) and ``test_case15_restart_reconcile_vs_broker`` skips.
"""

from __future__ import annotations

import asyncio
import logging
import signal
from datetime import date, datetime, time

import pytest

from engine.core.clock import IST
from engine.core.enums import RiskState
from engine.notify.catalog import MessageKind
from engine.ops import main as opsmain
from engine.ops.health import HealthMonitor
from tests.chaos._lifecycle_rig import (
    INCIDENT_KINDS,
    EngineProcess,
    FakeMonotonic,
    FakeTicker,
    RigEnv,
    run_session,
)
from tests.chaos.conftest import PHASE3_GATED

TUE, WED = date(2026, 6, 16), date(2026, 6, 17)
A_BOOT, STOP_AT, B_BOOT = time(8, 0), time(8, 28), time(9, 5)


def _t(d: date, hh: int, mm: int, ss: int = 0) -> datetime:
    return datetime(d.year, d.month, d.day, hh, mm, ss, tzinfo=IST)


def _at(d: date, tm: time) -> datetime:
    return datetime.combine(d, tm, tzinfo=IST)


SIGBREAK = getattr(signal, "SIGBREAK", signal.SIGTERM)   # Windows console ctrl-break (NSSM stop)


async def test_case15_nssm_stop_with_repeat_signals_then_same_day_restart(tmp_path, monkeypatch, caplog):
    env = RigEnv(tmp_path, monkeypatch, start=_t(TUE, 22, 45))
    # Scenario premise on the configured lifecycle: every expected active-period start is covered by
    # A's 08:00 boot, so the 08:28-09:05 off-window is a genuine intentional off (no start is owed).
    assert all(s <= A_BOOT for s in env.settings.lifecycle.active_period_starts)
    # The previous evening's session: every Tue job ran (watermarked), then a clean stop.
    await run_session(env, _t(TUE, 22, 45), _t(TUE, 22, 50))
    assert env.lifecycle_row()["state"] == "STOPPED"

    # ------------------------------------------------------------------ process A: 08:00 morning boot
    env.at(_at(WED, A_BOOT))
    calls_a = len(env.job_calls)
    a = EngineProcess(env)
    daily = [s for s in a.registry.specs() if s.fire_day is None]
    fired_while_up = sorted((s for s in daily if A_BOOT < s.at < STOP_AT), key=lambda s: s.at)
    due_in_off_window = {s.job_id for s in daily if STOP_AT <= s.at <= B_BOOT}
    assert fired_while_up and due_in_off_window, "scenario needs fires on both sides of the stop"
    report_a = await a.boot()
    assert report_a.crash_recovered is False
    assert env.calls(since=calls_a) == []                      # nothing due yet at 08:00
    env.at(_t(WED, 8, 5))
    await a.arm_recommend()                                    # owner arms RECOMMEND (sticky, R10)
    for spec in fired_while_up:                                # (instruments, surveillance, news chain)
        env.at(_at(WED, spec.at))
        await a.fire_scheduled(spec.job_id)                    # the live scheduler's own fires
    assert env.calls(since=calls_a) == [(s.job_id, None) for s in fired_while_up]

    env.at(_at(WED, STOP_AT))
    await a.heartbeat_synced()
    assert env.watchdog_poll()["down_reason"] is None          # engine up + beating: silent

    # ------------------------------------------------------------------ 08:28 `nssm stop`
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()
    exit_codes: list[int] = []
    mono = FakeMonotonic()
    handler = opsmain._make_stop_handler(loop, stop_event, force_exit=exit_codes.append, monotonic=mono)

    handler(signal.SIGINT, None)                               # first Ctrl-C: graceful stop request
    await asyncio.sleep(0)                                     # call_soon_threadsafe lands
    assert stop_event.is_set()

    def nssm_repeats_mid_backup() -> None:
        # The observed NSSM shape: the same console Ctrl-C reaches the process again ~1 s later —
        # while shutdown is inside the backup hook — plus a ctrl-break from the console group.
        mono.t += 1.1
        handler(signal.SIGINT, None)
        mono.t += 0.3
        handler(SIGBREAK, None)

    a.during_backup = nssm_repeats_mid_backup
    stopped_before = env.kinds().count(str(MessageKind.ENGINE_STOPPED))
    with caplog.at_level(logging.INFO, logger="engine"):
        await a.stop()                                         # main.py teardown incl. lifecycle.shutdown

    assert exit_codes == [], "a repeat signal inside the grace window forced exit (exit 130 = crash path)"
    assert [r for r in caplog.records if r.getMessage() == "stop_forced"] == []
    assert len([r for r in caplog.records if r.getMessage() == "stop_signal_repeat_ignored"]) == 2
    row = env.lifecycle_row()
    assert row["state"] == "STOPPED"                           # the clean stop was COMMITTED
    assert row["last_clean_stop_at"] == _at(WED, STOP_AT).isoformat()
    assert a.backups_written, "the shutdown backup never completed"
    assert env.kinds().count(str(MessageKind.ENGINE_STOPPED)) == stopped_before + 1

    # ------------------------------------------------------------------ off window 08:28 → 09:05
    for hh, mm in ((8, 29), (8, 40), (8, 55), (9, 4)):
        env.at(_t(WED, hh, mm))
        summary = env.watchdog_poll()
        assert summary == {"down_reason": None, "engine_down_sent": False, "killed_pid": None,
                           "missed_start_sent": []}, f"fault alarm in the off window at {hh:02d}:{mm:02d}"
    assert env.pages == [] and env.force_kills == []

    # ------------------------------------------------------------------ process B: 09:05 same-day restart
    env.at(_at(WED, B_BOOT))
    sent_b, calls_b = len(env.sent), len(env.job_calls)
    ticker = FakeTicker()
    b = EngineProcess(env, ticker=ticker)
    report_b = await b.boot()

    # clean stop honored — not a crash
    assert report_b.crash_recovered is False
    assert report_b.prior_state == "STOPPED"
    assert report_b.off_duration_s == pytest.approx(   # measured from last_clean_stop_at
        (_at(WED, B_BOOT) - _at(WED, STOP_AT)).total_seconds()
    )
    assert not any(n.startswith("crash_recovered") for n in report_b.notes)

    # restart runs catch-up cleanly: exactly the off-window fire-times (universe, digest, planner),
    # once each; what A already ran (instruments, surveillance, news chain) is NOT re-run
    assert sorted(env.calls(since=calls_b)) == sorted((j, None) for j in due_in_off_window)
    boot_pass = sorted(f"{j}:{WED.isoformat()}" for j in due_in_off_window - set(opsmain.POST_ARM_JOB_IDS))
    assert sorted(report_b.jobs_caught_up) == boot_pass         # the load-bearing boot pass
    assert report_b.jobs_failed == [] and report_b.frozen_reasons == []
    # §2.6 step 2 reconcile is a Phase-3 hook: visibly deferred, never silently skipped.
    assert "reconcile" in report_b.deferred_steps

    # no spurious FEED_STALE / incident alerts
    msgs_b = env.messages(since=sent_b)
    kinds_b = [m.kind for m in msgs_b]
    assert not INCIDENT_KINDS & set(kinds_b), kinds_b
    assert env.alerts == []
    started = next(m for m in msgs_b if m.kind == MessageKind.ENGINE_STARTED)
    report_msg = next(m for m in msgs_b if m.kind == MessageKind.STARTUP_REPORT)
    assert kinds_b.index(MessageKind.ENGINE_STARTED) < kinds_b.index(MessageKind.STARTUP_REPORT)
    assert started.data["crash_recovered"] is False and started.severity == "info"
    assert report_msg.data["crash_recovered"] is False and report_msg.severity == "info"
    assert "crash-recovered" not in report_msg.body

    health = HealthMonitor(env.clock, env.settings, ticker_supervisor=ticker, alert=env.alert,
                           calendar=b.calendar, mode_manager=b.mode, latch=b.latch, disk_warn_gb=0.0)
    for hh, mm in ((9, 6), (9, 20)):                            # pre-open and in-session, feed WARMING
        env.at(_t(WED, hh, mm))
        hr = await health.check(check_skew=False)
        assert hr.feed_state == "WARMING" and hr.problems == [], hr.problems
    assert env.alerts == []

    # resumes correctly
    assert report_b.sticky_mode == "RECOMMEND" and b.mode.risk_state() == RiskState.NORMAL
    assert b.heartbeat.running and b.scheduler.is_running()
    assert {s.job_id for s in b.registry.specs()} <= b.armed_job_ids()
    assert env.lifecycle_row()["state"] == "RUNNING"
    await b.heartbeat_synced()
    assert env.watchdog_poll()["down_reason"] is None
    assert env.pages == []

    env.at(_t(WED, 9, 30))
    await b.stop()


def test_case15_restart_reconcile_vs_broker() -> None:
    """'restart runs reconcile' — §2.6 step 2 reconcile vs broker = truth (R5) needs the Phase-3
    Reconciler; ``SessionLifecycle`` carries it as an unwired ``reconcile_hook`` today."""
    pytest.skip(f"case 15 reconcile clause: {PHASE3_GATED}; missing: Reconciler (reconcile_hook)")
