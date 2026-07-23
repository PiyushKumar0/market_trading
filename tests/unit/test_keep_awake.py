"""In-session OS keep-awake (2026-07-23 13:41 sleep/resume wedge).

The keep-awake asserts Windows SetThreadExecutionState while the NSE session is open so the OS does not
auto-sleep mid-session and freeze the feed; it releases at session close. Engage/release are one-shot
transitions (no per-tick log spam) driven by the always-on health loop. These tests record the
(monkeypatched) ctypes call and drive the transitions off a fake calendar; they also confirm the new
ticker config keys load.
"""

from __future__ import annotations

import datetime as dt

import pytest

from engine.core.clock import IST, Clock
from engine.core.config import load_settings
from engine.ops.health import HealthMonitor
from engine.ops.keep_awake import ES_CONTINUOUS, ES_SYSTEM_REQUIRED, KeepAwake


def _at(h: int, m: int, s: int = 0) -> dt.datetime:
    return dt.datetime(2026, 6, 17, h, m, s, tzinfo=IST)   # a real 2026 trading day (conftest)


class _Now:
    def __init__(self, value: dt.datetime) -> None:
        self.value = value

    def __call__(self) -> dt.datetime:
        return self.value

    def set(self, value: dt.datetime) -> None:
        self.value = value


class _FakeSession:
    def __init__(self, open_dt: dt.datetime, close_dt: dt.datetime) -> None:
        self.open = open_dt
        self.close = close_dt


class _FakeCalendar:
    def __init__(self, session: _FakeSession | None) -> None:
        self._session = session

    def session(self, d: dt.date):  # noqa: ANN001 - test double
        return self._session


class _FakeClockCfg:
    max_skew_s = 2


class _FakeHealthSettings:
    """Minimal Settings stand-in for HealthMonitor.check (disk/WAL point at an isolated tmp dir)."""

    def __init__(self, tmp) -> None:  # noqa: ANN001 - tmp_path
        self._tmp = tmp
        self.clock = _FakeClockCfg()

    def resolved_data_dir(self):
        return self._tmp

    def sqlite_path(self):
        return self._tmp / "state.db"


# --------------------------------------------------------------------------- KeepAwake unit transitions
def test_keep_awake_engages_and_releases_once():
    """Engage on session-open transition, release on close; idempotent otherwise (no spam)."""
    calls: list[int] = []
    ka = KeepAwake(enabled=True, set_execution_state=lambda flags: calls.append(flags))
    ka._is_windows = True                                    # force the platform gate on (cross-platform CI)

    ka.update(session_open=True)                             # engage
    assert calls == [ES_CONTINUOUS | ES_SYSTEM_REQUIRED]

    ka.update(session_open=True)                             # already engaged ⇒ no new call, no spam
    assert len(calls) == 1

    ka.update(session_open=False)                            # release: clear with ES_CONTINUOUS alone
    assert calls == [ES_CONTINUOUS | ES_SYSTEM_REQUIRED, ES_CONTINUOUS]

    ka.update(session_open=False)                            # already released ⇒ no new call
    assert len(calls) == 2


def test_keep_awake_disabled_is_noop():
    """The owner opt-out (keep_awake_in_session: false) makes every call a no-op."""
    calls: list[int] = []
    ka = KeepAwake(enabled=False, set_execution_state=lambda flags: calls.append(flags))
    ka._is_windows = True
    ka.update(session_open=True)
    ka.update(session_open=False)
    assert calls == []


def test_keep_awake_never_asserts_display_required():
    """ES_DISPLAY_REQUIRED (0x2) must never be set — the screen is allowed to sleep."""
    calls: list[int] = []
    ka = KeepAwake(enabled=True, set_execution_state=lambda flags: calls.append(flags))
    ka._is_windows = True
    ka.update(session_open=True)
    assert all((flags & 0x00000002) == 0 for flags in calls)


# ------------------------------------------------------------------ calendar-driven via the health loop
@pytest.mark.asyncio
async def test_health_monitor_drives_keep_awake_from_calendar(tmp_path):
    """The health loop engages keep-awake in-session and releases it after the session closes."""
    now = _Now(_at(10, 5))                                   # inside the 09:15–15:30 session
    clock = Clock(time_source=now)
    calls: list[int] = []
    ka = KeepAwake(enabled=True, set_execution_state=lambda flags: calls.append(flags))
    ka._is_windows = True
    cal = _FakeCalendar(_FakeSession(_at(9, 15), _at(15, 30)))
    hm = HealthMonitor(clock, _FakeHealthSettings(tmp_path), calendar=cal, keep_awake=ka)

    await hm.check(check_skew=False)                         # in-session ⇒ engage
    assert calls == [ES_CONTINUOUS | ES_SYSTEM_REQUIRED]

    now.set(_at(16, 0))                                      # after close ⇒ release
    await hm.check(check_skew=False)
    assert calls[-1] == ES_CONTINUOUS


@pytest.mark.asyncio
async def test_health_monitor_no_keep_awake_off_session(tmp_path):
    """A holiday/weekend (calendar returns no session) never engages keep-awake."""
    clock = Clock(time_source=_Now(_at(10, 5)))
    calls: list[int] = []
    ka = KeepAwake(enabled=True, set_execution_state=lambda flags: calls.append(flags))
    ka._is_windows = True
    hm = HealthMonitor(clock, _FakeHealthSettings(tmp_path), calendar=_FakeCalendar(None), keep_awake=ka)
    await hm.check(check_skew=False)
    assert calls == []


# --------------------------------------------------------------------------- config keys load
def test_new_ticker_config_keys_load():
    """The new WARMING-wedge + keep-awake knobs load from config/settings.yaml with the seeded values."""
    s = load_settings()
    assert s.ticker.warming_timeout_s == 60
    assert s.ticker.warming_backoff_cap_s == 300
    assert s.ticker.max_wedge_respawns == 5
    assert s.ticker.keep_awake_in_session is True
