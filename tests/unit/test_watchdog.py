"""scripts/watchdog.py decision logic (§2.2 pinned algorithm) — PURE, everything injected.

Covers: crash vs wedged vs STOPPING-not-a-crash vs STOPPED-silent, the catchup-grace wedge
suppression, debounce (one alert per outage) + retry-on-send-failure + re-arm-by-fresh-boot,
alert-FIRST-then-kill ordering, and the missed-scheduled-start check with its own per-(day,start)
debounce. No real process, network, or db is touched.
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import time, timedelta
from pathlib import Path

from tests.conftest import FIXED_NOW

_WATCHDOG_PATH = Path(__file__).resolve().parents[2] / "scripts" / "watchdog.py"
_spec = importlib.util.spec_from_file_location("mt_watchdog", _WATCHDOG_PATH)
wd = importlib.util.module_from_spec(_spec)
sys.modules["mt_watchdog"] = wd
_spec.loader.exec_module(wd)

NOW = FIXED_NOW                                   # Wed 2026-06-17 10:05 IST (a trading day)
CFG = wd.WatchdogConfig(
    down_stale_s=90.0, catchup_grace_s=900.0, start_grace_s=900.0,
    active_period_starts=(time(8, 0),),
)
ALIVE = lambda pid: True      # noqa: E731
DEAD = lambda pid: False      # noqa: E731
TRADING = lambda d: True      # noqa: E731
HOLIDAY = lambda d: False     # noqa: E731
# Frozen (immutable) default debounce — module-level singleton so it is not constructed in an
# argument default (ruff B008); the pure decision core never mutates it.
EMPTY_DEBOUNCE = wd.DebounceState()


def _snap(state, *, pid=4242, alive_ago_s=10, started_ago_s=7200):
    return wd.LifecycleSnapshot(
        state=state,
        pid=pid,
        last_alive_at=NOW - timedelta(seconds=alive_ago_s) if alive_ago_s is not None else None,
        started_at=NOW - timedelta(seconds=started_ago_s) if started_ago_s is not None else None,
    )


def _decide(snap, *, debounce=EMPTY_DEBOUNCE, pid_alive=ALIVE, trading=HOLIDAY, cfg=CFG):
    return wd.decide(snap, cfg, debounce, NOW, is_pid_alive=pid_alive, is_trading_day=trading)


# ------------------------------------------------------------------ (b) engine down: the four states
def test_running_pid_dead_is_crash():
    d = _decide(_snap("RUNNING", alive_ago_s=30), pid_alive=DEAD)
    assert d.down_reason == "crash"
    assert d.engine_down_alert is True
    assert d.kill_pid is None                     # nothing to kill — it is already dead
    assert d.since == NOW - timedelta(seconds=30)


def test_running_alive_stale_past_grace_is_wedged_and_killed():
    d = _decide(_snap("RUNNING", alive_ago_s=120, started_ago_s=7200))
    assert d.down_reason == "wedged"
    assert d.kill_pid == 4242                     # force-kill so NSSM restart takes over (§2.2)


def test_wedge_suppressed_inside_catchup_grace():
    """A legitimately busy multi-day catch-up right after boot must not be shot (§2.2:
    now − started_at must exceed catchup_grace_s before a wedge alarm)."""
    d = _decide(_snap("RUNNING", alive_ago_s=120, started_ago_s=600))
    assert d.down_reason is None


def test_running_alive_fresh_heartbeat_is_silent():
    d = _decide(_snap("RUNNING", alive_ago_s=10))
    assert d.down_reason is None


def test_stopping_pid_alive_is_not_a_crash():
    """STOPPING = intentional teardown in progress — even a stale heartbeat is not wedge-alarmed
    (the §2.2 wedge branch requires state==RUNNING)."""
    d = _decide(_snap("STOPPING", alive_ago_s=500))
    assert d.down_reason is None


def test_stopping_pid_dead_is_crash():
    """A crash DURING the shutdown guard leaves STOPPING ⇒ ENGINE_DOWN, never mislabelled clean."""
    d = _decide(_snap("STOPPING", alive_ago_s=500), pid_alive=DEAD)
    assert d.down_reason == "crash"


def test_stopped_is_silent_even_with_dead_pid_and_stale_beat():
    """Intentional off is NORMAL (§2.6) — chaos case 22(c): killed while intend_to_run false."""
    d = _decide(_snap("STOPPED", alive_ago_s=86400), pid_alive=DEAD)
    assert d.down_reason is None and d.engine_down_alert is False


def test_no_lifecycle_row_is_silent():
    d = _decide(wd.LifecycleSnapshot(), pid_alive=DEAD)
    assert d.down_reason is None


# ------------------------------------------------------------------ debounce + re-arm (edge-trigger)
def test_debounced_after_alert_no_repeat():
    """One alert per outage (chaos 22(b): no duplicate ENGINE_DOWN on the next poll)."""
    snap = _snap("RUNNING", alive_ago_s=300)
    stamped = wd.DebounceState(last_down_alert_at=NOW - timedelta(seconds=60))
    d = _decide(snap, debounce=stamped, pid_alive=DEAD)
    assert d.down_reason == "crash"
    assert d.engine_down_alert is False           # condition holds, alert already sent


def test_fresh_boot_heartbeat_rearms():
    """last_down_alert_at < last_alive_at (fresh boot) ⇒ armed again for the NEXT outage (§2.2)."""
    snap = _snap("RUNNING", alive_ago_s=10)       # rebooted engine wrote a fresh beat…
    stamped = wd.DebounceState(last_down_alert_at=NOW - timedelta(seconds=600))
    d = _decide(snap, debounce=stamped, pid_alive=DEAD)   # …then crashed again
    assert d.down_reason == "crash" and d.engine_down_alert is True


# ------------------------------------------------------------------ (a) missed scheduled start
def test_missed_start_on_trading_day():
    snap = wd.LifecycleSnapshot(state="STOPPED", started_at=NOW - timedelta(days=1))
    d = _decide(snap, trading=TRADING, pid_alive=DEAD)
    assert [s.strftime("%H:%M") for s in d.missed_starts] == ["08:00"]


def test_missed_start_silent_on_non_trading_day():
    snap = wd.LifecycleSnapshot(state="STOPPED", started_at=None)
    d = _decide(snap, trading=HOLIDAY, pid_alive=DEAD)
    assert d.missed_starts == ()


def test_missed_start_satisfied_by_actual_start():
    snap = _snap("STOPPED", started_ago_s=3600)   # started 09:05 ≥ the 08:00 fire-time
    d = _decide(snap, trading=TRADING, pid_alive=DEAD)
    assert d.missed_starts == ()


def test_missed_start_suppressed_while_engine_is_up():
    """An engine already running across the fire-time covers the active period — no false alarm
    (mirrors §2.2: ENGINE_STARTED is not re-emitted for a later period either)."""
    snap = wd.LifecycleSnapshot(state="RUNNING", pid=4242,
                                last_alive_at=NOW - timedelta(seconds=10),
                                started_at=NOW - timedelta(days=1))
    d = _decide(snap, trading=TRADING, pid_alive=ALIVE)
    assert d.missed_starts == ()


def test_missed_start_inside_grace_is_silent():
    cfg = wd.WatchdogConfig(down_stale_s=90, catchup_grace_s=900, start_grace_s=900,
                            active_period_starts=(time(9, 55),))   # 10:05 is inside 09:55+15m
    snap = wd.LifecycleSnapshot(state="STOPPED")
    d = _decide(snap, trading=TRADING, pid_alive=DEAD, cfg=cfg)
    assert d.missed_starts == ()


def test_missed_start_debounced_per_day_and_start():
    snap = wd.LifecycleSnapshot(state="STOPPED")
    already = wd.DebounceState(missed_start_alerted=("2026-06-17T08:00",))
    d = _decide(snap, debounce=already, trading=TRADING, pid_alive=DEAD)
    assert d.missed_starts == ()


# ------------------------------------------------------------------ run_tick: send-first, stamp-on-success
def _tick(snap, debounce, *, send_ok, pid_alive=ALIVE, trading=HOLIDAY):
    events: list = []

    def send(text: str) -> bool:
        events.append(("send", text))
        return send_ok

    def kill(pid: int) -> bool:
        events.append(("kill", pid))
        return True

    new_debounce, summary = wd.run_tick(
        snap, CFG, debounce, NOW,
        is_pid_alive=pid_alive, is_trading_day=trading, send=send, kill=kill,
    )
    return new_debounce, summary, events


def test_tick_send_failure_keeps_retrying_and_defers_kill():
    """Correlated outage (router down → wedged engine → Telegram unreachable): the debounce is NOT
    stamped on a failed send, so every tick retries — the one real-time alert is never lost (§2.2).
    The wedge kill waits for a confirmed send (alert-FIRST is the pinned order)."""
    snap = _snap("RUNNING", alive_ago_s=300, started_ago_s=7200)
    db0 = wd.DebounceState()
    db1, summary, events = _tick(snap, db0, send_ok=False)
    assert summary["down_reason"] == "wedged" and summary["engine_down_sent"] is False
    assert db1.last_down_alert_at is None                 # not stamped ⇒ retry next tick
    assert ("kill", 4242) not in events                   # remediation deferred behind the alert

    db2, summary2, events2 = _tick(snap, db1, send_ok=True)   # connectivity back
    assert summary2["engine_down_sent"] is True
    assert db2.last_down_alert_at == NOW                  # stamped ONLY on confirmed success
    assert events2 == [("send", events2[0][1]), ("kill", 4242)]  # send FIRST, then the kill
    assert "wedged" in events2[0][1]

    db3, summary3, events3 = _tick(snap, db2, send_ok=True)   # next tick: debounced
    assert summary3["engine_down_sent"] is False and events3 == []


def test_tick_crash_alert_mentions_reason_and_protection():
    snap = _snap("RUNNING", alive_ago_s=200)
    _db, summary, events = _tick(snap, wd.DebounceState(), send_ok=True, pid_alive=DEAD)
    assert summary["down_reason"] == "crash" and summary["killed_pid"] is None
    text = events[0][1]
    assert "ENGINE_DOWN(reason=crash)" in text and "broker-protected" in text


def test_tick_stopped_sends_nothing():
    snap = _snap("STOPPED", alive_ago_s=86400)
    _db, summary, events = _tick(snap, wd.DebounceState(), send_ok=True, pid_alive=DEAD)
    assert events == [] and summary["down_reason"] is None


def test_tick_missed_start_stamped_only_on_send_success():
    snap = wd.LifecycleSnapshot(state="STOPPED")
    db1, _s, _e = _tick(snap, wd.DebounceState(), send_ok=False, pid_alive=DEAD, trading=TRADING)
    assert db1.missed_start_alerted == ()                 # failed send ⇒ retry next tick
    db2, summary, events = _tick(snap, db1, send_ok=True, pid_alive=DEAD, trading=TRADING)
    assert db2.missed_start_alerted == ("2026-06-17T08:00",)
    assert summary["missed_start_sent"] == ["2026-06-17T08:00"]
    assert "SCHEDULED_START_MISSED" in events[0][1]
    db3, _s3, events3 = _tick(snap, db2, send_ok=True, pid_alive=DEAD, trading=TRADING)
    assert events3 == []                                  # debounced per (day, start)


# ------------------------------------------------------------------ debounce file round-trip
def test_debounce_state_roundtrip(tmp_path):
    path = tmp_path / "watchdog_state.json"
    st = wd.DebounceState(last_down_alert_at=NOW, missed_start_alerted=("2026-06-17T08:00",))
    wd.save_debounce(path, st)
    loaded = wd.load_debounce(path)
    assert loaded.last_down_alert_at == NOW
    assert loaded.missed_start_alerted == ("2026-06-17T08:00",)
    assert wd.load_debounce(tmp_path / "absent.json") == wd.DebounceState()
    (tmp_path / "corrupt.json").write_text("{not json", encoding="utf-8")
    assert wd.load_debounce(tmp_path / "corrupt.json") == wd.DebounceState()


def test_read_snapshot_missing_db_is_silent(tmp_path):
    """No state.db yet (fresh install) ⇒ empty snapshot ⇒ silent tick — and CRUCIALLY the read-only
    open must not have created the file (the watchdog is never a writer, §2.2)."""
    db = tmp_path / "state.db"
    snap = wd.read_snapshot(db)
    assert snap == wd.LifecycleSnapshot()
    assert not db.exists()
