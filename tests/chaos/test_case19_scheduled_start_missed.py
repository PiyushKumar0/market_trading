"""§9.4 chaos case 19 — Scheduled active-period start FAILS to fire (PC asleep / task disabled / power
loss).

Plan row (IMPLEMENTATION_PLAN.md §9.4, case 19), "Must hold":

    "watchdog detects no readiness report at expected start (§10.3/§10.4) + alerts owner; an open MIS
    rides to the broker 15:25 backstop; on eventual manual start, full reconcile adopts the broker
    square-off"

Scenario: Tue 2026-06-16 the engine ran and was stopped cleanly after the session. On Wed 2026-06-17
(a trading day) the 08:00 wake task never fires. The real ``scripts/watchdog.py`` IO shell polls the
on-disk state.db and its private debounce file on its ~60 s cadence (``tests/chaos/_lifecycle_rig.py``;
only the Telegram sender and the OS process table are faked). The owner starts the engine by hand at
09:30.

Clauses covered:

* "watchdog detects no readiness … at expected start + alerts owner" — silent before the start and
  inside ``start_grace_s``; exactly ONE ``SCHEDULED_START_MISSED`` once the grace lapses; edge-triggered
  (the per-(day, start) debounce persisted in ``watchdog_state.json`` suppresses every later poll that
  day); silent again once the manual start lands (``started_at`` ≥ the expected start); the NEXT day's
  missed start is a new alert. The real calendar YAML keeps it silent on a weekend and on an NSE
  holiday (2026-06-26 Muharram) — no expectation, no alert.

"Readiness report" resolution: the shipped watchdog keys the check on ``engine_lifecycle.started_at``
(committed at §2.6 step 0, before recovery) — a boot that has started covers the period; a boot that
started but wedged is the separate ``ENGINE_DOWN(reason=wedged)`` path (§2.2).

Phase-3-gated (skipped, ``test_case19_open_mis_rides_backstop_and_reconcile_adopts``): "an open MIS
rides to the broker 15:25 backstop; on eventual manual start, full reconcile adopts the broker
square-off" — no platform MIS exists before AUTO(paper) routing (WO-P3-5) and the adopting Reconciler
(``broker_squareoff_offline``) is WO-P3-6.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta

import pytest

from engine.core.clock import IST
from engine.notify.catalog import MessageKind
from tests.chaos._lifecycle_rig import EngineProcess, RigEnv, run_session
from tests.chaos.conftest import PHASE3_GATED

TUE, WED, THU = date(2026, 6, 16), date(2026, 6, 17), date(2026, 6, 18)
SILENT = {"down_reason": None, "engine_down_sent": False, "killed_pid": None, "missed_start_sent": []}


def _t(d: date, hh: int, mm: int, ss: int = 0) -> datetime:
    return datetime(d.year, d.month, d.day, hh, mm, ss, tzinfo=IST)


def _start_and_grace(env: RigEnv) -> tuple[time, timedelta]:
    """The configured expected start + grace (settings.yaml lifecycle). The scenario models the single
    configured morning start; a second active-period start would owe its own alert."""
    starts = env.settings.lifecycle.active_period_starts
    assert len(starts) == 1, f"scenario models one configured start, got {starts}"
    return starts[0], timedelta(seconds=env.settings.lifecycle.start_grace_s)


def _key(d: date, start: time) -> str:
    return f"{d.isoformat()}T{start:%H:%M}"


async def test_case19_missed_start_alerts_once_then_manual_start_covers_it(tmp_path, monkeypatch):
    env = RigEnv(tmp_path, monkeypatch, start=_t(TUE, 9, 0))
    start, grace = _start_and_grace(env)
    await run_session(env, _t(TUE, 9, 0), _t(TUE, 15, 40))      # Tue ran, clean stop after the session
    assert env.lifecycle_row()["state"] == "STOPPED"
    s_wed = datetime.combine(WED, start, tzinfo=IST)

    # Wed: the wake task never fires. Before the start and inside the grace: silent.
    for t in (s_wed - timedelta(minutes=1), s_wed + grace / 3, s_wed + grace - timedelta(minutes=1)):
        env.at(t)
        assert env.watchdog_poll() == SILENT, f"early alert at {t.isoformat()}"
    assert env.pages == []

    # Grace lapsed with no start ⇒ ONE SCHEDULED_START_MISSED.
    env.at(s_wed + grace + timedelta(minutes=1))
    summary = env.watchdog_poll()
    assert summary["missed_start_sent"] == [_key(WED, start)]
    assert summary["down_reason"] is None                       # a missed start is not ENGINE_DOWN
    assert len(env.pages) == 1
    assert "SCHEDULED_START_MISSED" in env.pages[0].text and f"{start:%H:%M}" in env.pages[0].text

    # Edge-triggered: every later poll that day stays quiet (debounce survives across Task runs).
    for later in (2, 15, 45, 74):
        env.at(s_wed + grace + timedelta(minutes=later))
        assert env.watchdog_poll() == SILENT
    assert len(env.pages) == 1
    assert _key(WED, start) in env.watchdog_state_path.read_text(encoding="utf-8")

    # Manual start (75 min after the grace lapsed): a clean (not crash) boot; the watchdog is satisfied.
    env.at(s_wed + grace + timedelta(minutes=75))
    sent_b = len(env.sent)
    b = EngineProcess(env)
    report = await b.boot()
    assert report.crash_recovered is False
    assert MessageKind.STARTUP_REPORT in [m.kind for m in env.messages(since=sent_b)]
    env.at(s_wed + grace + timedelta(minutes=76))
    await b.heartbeat_synced()
    assert env.watchdog_poll() == SILENT
    env.at(_t(WED, 15, 40))
    await b.stop()

    # After the day's stop the covered start stays covered (started_at ≥ the start): no late alert.
    env.at(_t(WED, 16, 0))
    assert env.watchdog_poll() == SILENT

    # Thu: a new (day, start) key — missed again ⇒ alerted again, once.
    s_thu = datetime.combine(THU, start, tzinfo=IST)
    for later in (1, 25):
        env.at(s_thu + grace + timedelta(minutes=later))
        env.watchdog_poll()
    assert len(env.pages) == 2
    assert "SCHEDULED_START_MISSED" in env.pages[1].text and THU.isoformat() in env.pages[1].text


async def test_case19_no_expected_start_on_weekend_or_nse_holiday(tmp_path, monkeypatch):
    """No trading day ⇒ no active period expected ⇒ no alert, from the real config/calendar YAML."""
    fri_holiday, sat, sun = date(2026, 6, 26), date(2026, 6, 27), date(2026, 6, 28)   # 06-26 = Muharram
    mon = date(2026, 6, 29)
    env = RigEnv(tmp_path, monkeypatch, start=_t(date(2026, 6, 25), 9, 0))
    start, grace = _start_and_grace(env)
    await run_session(env, _t(date(2026, 6, 25), 9, 0), _t(date(2026, 6, 25), 15, 40))
    for d in (fri_holiday, sat, sun):
        s = datetime.combine(d, start, tzinfo=IST)
        for t in (s + grace + timedelta(minutes=1), s + timedelta(hours=2), s + timedelta(hours=6)):
            env.at(t)
            assert env.watchdog_poll() == SILENT, f"alert on non-trading day at {t.isoformat()}"
    assert env.pages == []
    # …and the first trading day after the gap is watched again.
    env.at(datetime.combine(mon, start, tzinfo=IST) + grace + timedelta(minutes=1))
    assert env.watchdog_poll()["missed_start_sent"] == [_key(mon, start)]


def test_case19_open_mis_rides_backstop_and_reconcile_adopts() -> None:
    """'an open MIS rides to the broker 15:25 backstop; on eventual manual start, full reconcile
    adopts the broker square-off' — needs a platform MIS and the adopting Reconciler."""
    pytest.skip(
        f"case 19 MIS/reconcile clauses: {PHASE3_GATED}; missing: AUTO(paper) MIS + Reconciler "
        "(broker_squareoff_offline adoption)"
    )
