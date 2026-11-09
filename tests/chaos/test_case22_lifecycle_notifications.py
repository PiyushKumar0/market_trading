"""Chaos case 22 — Lifecycle notifications: (a) clean planned stop, (b) crash mid-active-
period, (c) crash during intentional-off.

Must hold: (a) a clean stop sends exactly one ENGINE_STOPPED, the watchdog stays silent across the idle gap
and the restart sends ENGINE_STARTED then a clean STARTUP_REPORT; (b) kill -9 while intending to run makes
the watchdog send one ENGINE_DOWN within ``down_stale_s`` (edge-triggered), and the restart reports
``crash_recovered``; (c) a kill while intentionally off stays silent. Alerts are operational, not capital
emergencies.

``intend_to_run`` is ``engine_lifecycle.state``: ``RUNNING``/``STOPPING`` = intends to run, ``STOPPED`` =
intentional off (the watchdog's predicate is ``state != 'STOPPED'``). Every scenario composes the real
lifecycle + heartbeat thread + ``scripts/watchdog.py`` over one on-disk state.db
(``tests/chaos/_lifecycle_rig.py``). The NSSM restart policy lives in the service manager, not engine
code; the restart is modelled as the next boot of the same data dir.

Skipped: "capital protection unaffected" needs broker-resident SL-M/GTT orders, which RECOMMEND does not
place; the alerts' positions-protected wording is asserted here.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta

import pytest

from engine.core.clock import IST
from engine.notify.catalog import MessageKind
from tests.chaos._lifecycle_rig import INCIDENT_KINDS, EngineProcess, RigEnv, run_session
from tests.chaos.conftest import BROKER_RESIDENT_PROTECTION

TUE, WED, THU = date(2026, 6, 16), date(2026, 6, 17), date(2026, 6, 18)
SILENT = {"down_reason": None, "engine_down_sent": False, "killed_pid": None, "missed_start_sent": []}


def _t(d: date, hh: int, mm: int, ss: int = 0) -> datetime:
    return datetime(d.year, d.month, d.day, hh, mm, ss, tzinfo=IST)


async def _env_after_previous_evening(tmp_path, monkeypatch) -> RigEnv:
    env = RigEnv(tmp_path, monkeypatch, start=_t(TUE, 22, 45))
    # Every expected start is a morning start covered by the Wed boots below, so no missed-start page is owed.
    starts = env.settings.lifecycle.active_period_starts
    assert starts and all(s <= time(9, 0) for s in starts), f"scenario assumes morning starts, got {starts}"
    await run_session(env, _t(TUE, 22, 45), _t(TUE, 22, 50))   # watermarks + a committed clean stop
    return env


# --------------------------------------------------------------------------- (a) clean planned stop
async def test_case22a_clean_stop_one_engine_stopped_then_silent_idle_gap(tmp_path, monkeypatch):
    env = await _env_after_previous_evening(tmp_path, monkeypatch)
    env.at(_t(WED, 9, 0))
    a = EngineProcess(env)
    await a.boot()

    env.at(_t(WED, 15, 40))                                    # trade window done: planned stop
    sent_stop = len(env.sent)
    await a.stop()

    stopped = [m for m in env.messages(since=sent_stop) if m.kind == MessageKind.ENGINE_STOPPED]
    assert len(stopped) == 1, env.kinds(since=sent_stop)
    assert stopped[0].data["reason"] == "owner" and stopped[0].data["open_positions"] == 0
    assert "broker-protected" in stopped[0].body              # positions-protected status rides it
    assert stopped[0].severity == "info"                       # operational, not a capital emergency
    row = env.lifecycle_row()
    assert row["state"] == "STOPPED"                           # intend_to_run = false
    assert row["last_clean_stop_at"] == _t(WED, 15, 40).isoformat()

    # Idle gap: the watchdog polls every 20 min up to Thu's scheduled start, which fires inside start_grace_s.
    restart_at = datetime.combine(THU, min(env.settings.lifecycle.active_period_starts), tzinfo=IST)
    restart_at += timedelta(minutes=5)
    assert timedelta(minutes=5) < timedelta(seconds=env.settings.lifecycle.start_grace_s)
    t = _t(WED, 15, 41)
    while t < restart_at:
        env.at(t)
        assert env.watchdog_poll() == SILENT, f"false alarm in the idle gap at {t.isoformat()}"
        t += timedelta(minutes=20)
    assert env.pages == [] and env.force_kills == []

    # Restart ⇒ ENGINE_STARTED, then a clean STARTUP_REPORT (no crash notice).
    env.at(restart_at)
    sent_b = len(env.sent)
    b = EngineProcess(env)
    report = await b.boot()
    msgs = env.messages(since=sent_b)
    kinds = [m.kind for m in msgs]
    assert kinds.index(MessageKind.ENGINE_STARTED) < kinds.index(MessageKind.STARTUP_REPORT)
    assert kinds.count(MessageKind.ENGINE_STARTED) == 1 and kinds.count(MessageKind.STARTUP_REPORT) == 1
    started = msgs[kinds.index(MessageKind.ENGINE_STARTED)]
    startup = msgs[kinds.index(MessageKind.STARTUP_REPORT)]
    assert started.data["crash_recovered"] is False and "crash" not in started.title.lower()
    assert startup.data["crash_recovered"] is False and "crash-recovered" not in startup.body
    assert report.crash_recovered is False
    assert not INCIDENT_KINDS & set(kinds)
    env.at(restart_at + timedelta(minutes=1))
    await b.heartbeat_synced()
    assert env.watchdog_poll() == SILENT
    assert env.pages == []
    await b.stop()


# --------------------------------------------------------------------------- (b) kill -9 mid-period
async def test_case22b_kill9_pages_once_edge_triggered_then_crash_recovered_restart(tmp_path, monkeypatch):
    env = await _env_after_previous_evening(tmp_path, monkeypatch)
    env.at(_t(WED, 10, 0))
    a = EngineProcess(env)
    await a.boot()
    env.at(_t(WED, 10, 14, 30))
    await a.heartbeat_synced()
    assert env.watchdog_poll() == SILENT                       # up and beating

    env.at(_t(WED, 10, 15))
    await a.heartbeat_synced()                                 # last beat before death
    await a.kill_9()
    killed_at = env.clock.now()
    assert env.lifecycle_row()["state"] == "RUNNING"           # intend_to_run stays TRUE — no clean stop
    assert MessageKind.ENGINE_STOPPED not in [m.kind for m in env.messages()[-3:]]

    # ONE ENGINE_DOWN, then edge-triggered silence.
    summaries = []
    for secs in (30, 90, 150):
        env.at(killed_at + timedelta(seconds=secs))
        summaries.append(env.watchdog_poll())
    assert [s["down_reason"] for s in summaries] == ["crash", "crash", "crash"]   # condition persists…
    assert [s["engine_down_sent"] for s in summaries] == [True, False, False]     # …one page per outage
    assert len(env.pages) == 1
    page = env.pages[0]
    assert "ENGINE_DOWN(reason=crash)" in page.text and "broker-protected" in page.text
    assert (page.at - killed_at).total_seconds() <= env.settings.lifecycle.down_stale_s
    assert env.force_kills == []                               # a crash is already dead — nothing to kill

    # NSSM restart: ENGINE_STARTED + STARTUP_REPORT(crash_recovered).
    env.at(killed_at + timedelta(seconds=165))
    sent_b = len(env.sent)
    b = EngineProcess(env)
    report = await b.boot()
    msgs = env.messages(since=sent_b)
    kinds = [m.kind for m in msgs]
    assert kinds.index(MessageKind.ENGINE_STARTED) < kinds.index(MessageKind.STARTUP_REPORT)
    started = msgs[kinds.index(MessageKind.ENGINE_STARTED)]
    startup = msgs[kinds.index(MessageKind.STARTUP_REPORT)]
    assert started.data["crash_recovered"] is True
    assert startup.data["crash_recovered"] is True and startup.data["prior_state"] == "RUNNING"
    assert startup.body.startswith("⚠ crash-recovered (prior state RUNNING)")
    assert report.crash_recovered is True
    assert report.off_duration_s == pytest.approx(165)         # measured from the last heartbeat
    assert MessageKind.ENGINE_CRASHLOOP not in kinds           # one respawn is not a crash loop

    # The fresh boot heartbeat re-arms the alarm; while B is up the watchdog is silent again.
    env.at(killed_at + timedelta(seconds=210))
    await b.heartbeat_synced()
    assert env.watchdog_poll() == SILENT
    assert len(env.pages) == 1                                 # still exactly one page for outage #1

    # …and a SECOND outage is a new page (re-armed by B's heartbeat) — one, not one per poll.
    env.at(killed_at + timedelta(seconds=600))
    await b.heartbeat_synced()
    await b.kill_9()
    for secs in (630, 690):
        env.at(killed_at + timedelta(seconds=secs))
        env.watchdog_poll()
    assert len(env.pages) == 2 and "ENGINE_DOWN(reason=crash)" in env.pages[1].text


# --------------------------------------------------------------------------- (c) killed while intentionally off
async def test_case22c_killed_after_intentional_stop_stays_silent(tmp_path, monkeypatch):
    """STOPPED is committed, then the process is killed before it exits (e.g. hung in post-commit teardown
    and ``taskkill /F``'d). The watchdog must stay silent: it should be off."""
    env = await _env_after_previous_evening(tmp_path, monkeypatch)
    env.at(_t(WED, 9, 0))
    a = EngineProcess(env)
    await a.boot()

    env.at(_t(WED, 11, 0))
    sent_stop = len(env.sent)
    a.scheduler.shutdown()
    await a.lifecycle.shutdown()                               # STOPPED committed (point of no return)…
    assert env.lifecycle_row()["state"] == "STOPPED"
    await a.kill_9()                                           # …then killed before a clean exit
    assert [m.kind for m in env.messages(since=sent_stop)] == [MessageKind.ENGINE_STOPPED]

    t = _t(WED, 11, 1)
    while t < _t(WED, 23, 0):                                  # the rest of the day, stale beat + dead pid
        env.at(t)
        assert env.watchdog_poll() == SILENT, f"watchdog alarmed on an intentional off at {t.isoformat()}"
        t += timedelta(minutes=30)
    assert env.pages == [] and env.force_kills == []

    # The next boot treats it as the clean stop it was.
    env.at(_t(THU, 8, 5))
    b = EngineProcess(env)
    report = await b.boot()
    assert report.crash_recovered is False and report.prior_state == "STOPPED"
    await b.stop()


def test_case22_capital_protection_unaffected() -> None:
    """Real positions have no resting SL-M/GTT in RECOMMEND; paper protection across process death is the in-process GTT book plus the session-prep catch-up."""
    pytest.skip(f"case 22 capital-protection clause: {BROKER_RESIDENT_PROTECTION}")
