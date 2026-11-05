"""In-session OS keep-awake (2026-07-23 13:41 sleep/resume wedge).

The keep-awake asserts Windows SetThreadExecutionState while the NSE session is open so the OS does not
auto-sleep mid-session and freeze the feed; it releases at session close. Engage/release are one-shot
transitions (no per-tick log spam) driven by the always-on health loop. These tests record the
(monkeypatched) ctypes call and drive the transitions off a fake calendar; they also confirm the new
ticker config keys load.
"""

from __future__ import annotations

import datetime as dt
import logging
import sys

import pytest

from engine.core.clock import IST, Clock
from engine.core.config import load_settings
from engine.ops.health import HealthMonitor
from engine.ops.keep_awake import ES_CONTINUOUS, ES_SYSTEM_REQUIRED, KeepAwake

_ENGAGE = ES_CONTINUOUS | ES_SYSTEM_REQUIRED


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


# ------------------------------------------------------------------ lapse check (2026-11-05 diagnosis)
class _FakePower:
    """The two Win32 calls the lapse check uses, with both suspected lapse shapes.

    ``thread`` is this thread's execution state; ``registered`` is the request as the system-wide state
    counts it. It changes only when the thread's flags change, so re-asserting identical flags does not
    re-register a request the system has lost. ``set_state(0)`` is the pure query (verified on the live
    host 2026-11-05) and is not recorded in ``calls``.
    """

    def __init__(self) -> None:
        self.thread = ES_CONTINUOUS
        self.registered = False
        self.others = 0          # ES_* bits other processes hold
        self.refuse = False      # the system will not count this process's request at all
        self.blind = False       # the read cannot see this process's request
        self.query_fails = False  # set_state(0) returns NULL
        self.failing_reads: set[int] = set()   # 1-based system_state() calls that raise
        self.reads = 0
        self.calls: list[int] = []

    def set_state(self, flags: int) -> int:
        previous = self.thread | ES_CONTINUOUS
        if not flags:
            return 0 if self.query_fails else previous
        self.calls.append(flags)
        if flags & ES_CONTINUOUS and flags != self.thread:
            self.thread = flags
            self.registered = bool(flags & ES_SYSTEM_REQUIRED) and not self.refuse
        return previous

    def system_state(self) -> int:
        self.reads += 1
        if self.reads in self.failing_reads:
            raise OSError("CallNtPowerInformation(SystemExecutionState) failed: 0xc0000022")
        own = ES_SYSTEM_REQUIRED if self.registered and not self.blind else 0
        return own | self.others

    def drop_thread_state(self) -> None:
        """Lapse shape 1: this thread's flags were cleared."""
        self.thread = ES_CONTINUOUS
        self.registered = False

    def drop_registration(self) -> None:
        """Lapse shape 2: the flags are intact but the system no longer counts the request."""
        self.registered = False


def _engaged(power: _FakePower, **kwargs) -> KeepAwake:  # noqa: ANN003 - KeepAwake kwargs
    ka = KeepAwake(
        enabled=True, set_execution_state=power.set_state, get_system_state=power.system_state, **kwargs,
    )
    ka._is_windows = True
    ka.update(session_open=True)
    return ka


def _events(caplog, name: str) -> list[logging.LogRecord]:  # noqa: ANN001 - pytest caplog
    return [r for r in caplog.records if r.getMessage() == name]


def test_lapse_check_never_touches_a_request_that_holds(caplog):
    """Bit present ⇒ no Win32 call at all, no warning, and a keep_awake_state line every N pulses."""
    power = _FakePower()
    with caplog.at_level(logging.INFO, logger="engine.ops.keep_awake"):
        ka = _engaged(power, state_log_every=3)
        for _ in range(6):
            ka.update(session_open=True)

    assert power.calls == [_ENGAGE]
    (engaged,) = _events(caplog, "keep_awake_engaged")
    assert engaged.own_visible is True                       # the bit appeared with this process's assert
    assert engaged.system_state_before == "0x00000000" and engaged.system_state == "0x00000001"
    states = _events(caplog, "keep_awake_state")
    assert len(states) == 2 and states[0].thread_state == "0x80000001"
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


def test_own_lapse_is_caught_while_another_process_keeps_the_bit_present(caplog):
    """The system-wide bit alone would hide this lapse until the other holder lets go, and the PC can
    idle-sleep the moment that happens (11-03: sleep began the second the engine released)."""
    power = _FakePower()
    ka = _engaged(power)
    power.others = ES_SYSTEM_REQUIRED
    power.drop_thread_state()
    with caplog.at_level(logging.INFO, logger="engine.ops.keep_awake"):
        ka.update(session_open=True)

    assert power.calls == [_ENGAGE, _ENGAGE]                 # no clear: re-asserting restored this thread
    (lapsed,) = _events(caplog, "keep_awake_lapsed")
    assert lapsed.thread_state == "0x80000000" and lapsed.system_state == "0x00000001"
    assert lapsed.restored is True and power.thread == _ENGAGE


def test_lapse_with_thread_state_cleared_is_rearmed_by_reasserting(caplog):
    power = _FakePower()
    ka = _engaged(power)
    power.drop_thread_state()
    with caplog.at_level(logging.INFO, logger="engine.ops.keep_awake"):
        ka.update(session_open=True)                         # lapse found and repaired
        ka.update(session_open=True)                         # in force again ⇒ quiet

    assert power.calls == [_ENGAGE, _ENGAGE]                 # the same-flags re-assert sufficed: no clear
    (lapsed,) = _events(caplog, "keep_awake_lapsed")
    assert lapsed.levelname == "WARNING"
    assert lapsed.thread_state == "0x80000000"               # shows this thread's flags had been cleared
    assert lapsed.toggled is False and lapsed.restored is True
    assert power.system_state() & ES_SYSTEM_REQUIRED


def test_lapse_with_thread_state_intact_is_rearmed_by_clear_and_set(caplog):
    power = _FakePower()
    ka = _engaged(power)
    power.drop_registration()
    with caplog.at_level(logging.INFO, logger="engine.ops.keep_awake"):
        ka.update(session_open=True)

    assert power.calls == [_ENGAGE, _ENGAGE, ES_CONTINUOUS, _ENGAGE]
    (lapsed,) = _events(caplog, "keep_awake_lapsed")
    assert lapsed.thread_state == "0x80000001"               # flags intact: the system had lost the request
    assert lapsed.system_state == "0x00000000" and lapsed.system_state_after == "0x00000001"
    assert lapsed.toggled is True and lapsed.restored is True and lapsed.own_visible is True


def test_unknown_visibility_still_reregisters_a_lost_request(caplog):
    """Another requester held the bit at the engage, so whether the read sees this process stays unknown.
    The read is trusted anyway: refusing to re-register a lost request costs a certain idle sleep."""
    power = _FakePower()
    power.others = ES_SYSTEM_REQUIRED
    with caplog.at_level(logging.INFO, logger="engine.ops.keep_awake"):
        ka = _engaged(power)
        power.others = 0
        power.drop_registration()
        ka.update(session_open=True)

    assert power.calls == [_ENGAGE, _ENGAGE, ES_CONTINUOUS, _ENGAGE]
    (engaged,) = _events(caplog, "keep_awake_engaged")
    assert engaged.own_visible is None and engaged.system_state_before == "0x00000001"
    (lapsed,) = _events(caplog, "keep_awake_lapsed")
    assert lapsed.own_visible is None and lapsed.toggled is True and lapsed.restored is True


def test_unrepaired_lapse_warns_once_retries_each_pulse_and_logs_the_restore(caplog):
    power = _FakePower()
    ka = _engaged(power)
    power.refuse = True
    power.drop_registration()
    with caplog.at_level(logging.INFO, logger="engine.ops.keep_awake"):
        for _ in range(3):
            ka.update(session_open=True)                     # missing every pulse
        power.refuse = False
        ka.update(session_open=True)                         # this pulse's retry takes
        ka.update(session_open=False)

    assert len(power.calls) == 1 + 4 * 3 + 1                 # engage, 4 × (re-assert, clear, set), release
    (lapsed,) = _events(caplog, "keep_awake_lapsed")
    assert lapsed.restored is False
    (restored,) = _events(caplog, "keep_awake_restored")
    assert restored.lapse_pulses == 4
    (released,) = _events(caplog, "keep_awake_released")
    assert released.lapses == 1 and released.lapsed_at_release is False


def test_blind_read_falls_back_to_this_threads_flags(caplog):
    """If the read cannot see this process's request right after the engage, its missing bit proves
    nothing and clearing could drop a working lock: only this thread's flags are checked, and a lapse
    there is repaired by re-asserting alone."""
    power = _FakePower()
    power.blind = True
    with caplog.at_level(logging.INFO, logger="engine.ops.keep_awake"):
        ka = _engaged(power)
        for _ in range(3):
            ka.update(session_open=True)                     # system-wide bit missing: ignored
        power.drop_thread_state()
        ka.update(session_open=True)                         # this thread's flags cleared: repaired
        ka.update(session_open=False)

    assert power.calls == [_ENGAGE, _ENGAGE, ES_CONTINUOUS]  # engage, re-assert, release: never a clear-and-set
    assert len(_events(caplog, "keep_awake_probe_blind")) == 1
    assert _events(caplog, "keep_awake_engaged")[0].own_visible is False
    (lapsed,) = _events(caplog, "keep_awake_lapsed")
    assert lapsed.thread_state == "0x80000000" and lapsed.toggled is False and lapsed.restored is True


def test_each_engage_reverifies_the_read_and_resets_the_counts(caplog):
    power = _FakePower()
    power.blind = True
    with caplog.at_level(logging.INFO, logger="engine.ops.keep_awake"):
        ka = _engaged(power)                                 # session 1: the read is blind
        ka.update(session_open=False)
        power.blind = False
        ka.update(session_open=True)                         # session 2: re-verified, visible
        power.drop_registration()
        ka.update(session_open=True)
        ka.update(session_open=False)
        ka.update(session_open=True)                         # session 3: fresh counts
        ka.update(session_open=False)

    assert [r.own_visible for r in _events(caplog, "keep_awake_engaged")] == [False, True, True]
    assert [r.lapses for r in _events(caplog, "keep_awake_released")] == [0, 1, 0]


def test_failed_read_proves_nothing_and_touches_nothing():
    power = _FakePower()
    ka = _engaged(power)                                     # the engage made reads 1 and 2
    power.failing_reads = {3}
    ka.update(session_open=True)                             # must not raise
    assert power.calls == [_ENGAGE]


def test_failed_read_after_the_reassert_does_not_clear_the_request():
    """The clear-and-set needs evidence that the re-assert did not work; a failed read is none."""
    power = _FakePower()
    ka = _engaged(power)
    power.drop_registration()
    power.failing_reads = {4}                                # the pulse's read after its re-assert
    ka.update(session_open=True)
    assert ES_CONTINUOUS not in power.calls


def test_failed_thread_query_falls_back_to_the_system_read_quietly(caplog):
    """A NULL query would otherwise warn on every pulse; it only loses the thread view."""
    power = _FakePower()
    power.query_fails = True
    with caplog.at_level(logging.INFO, logger="engine.ops.keep_awake"):
        ka = _engaged(power)
        for _ in range(3):
            ka.update(session_open=True)
        power.drop_registration()
        ka.update(session_open=True)                         # still caught by the system-wide read

    assert not _events(caplog, "keep_awake_set_state_rejected")
    (lapsed,) = _events(caplog, "keep_awake_lapsed")
    assert lapsed.thread_state is None and lapsed.restored is True


def test_null_return_from_set_state_is_logged(caplog):
    """NULL is SetThreadExecutionState's failure return; the engage line alone would hide it."""
    ka = KeepAwake(enabled=True, set_execution_state=lambda flags: 0)
    ka._is_windows = True
    with caplog.at_level(logging.INFO, logger="engine.ops.keep_awake"):
        ka.update(session_open=True)
    (rejected,) = _events(caplog, "keep_awake_set_state_rejected")
    assert rejected.flags == hex(_ENGAGE)


@pytest.mark.skipif(sys.platform != "win32", reason="real Win32 power calls")
def test_real_win32_engage_is_visible_to_the_lapse_check(caplog):
    """The production pair (SetThreadExecutionState + CallNtPowerInformation). Holds the host awake for
    the few ms the test runs and always releases."""
    ka = KeepAwake(enabled=True)
    try:
        with caplog.at_level(logging.INFO, logger="engine.ops.keep_awake"):
            ka.update(session_open=True)
            ka.update(session_open=True)
        (engaged,) = _events(caplog, "keep_awake_engaged")
        assert engaged.own_visible is not False              # None when another process already held it
        assert int(engaged.system_state, 16) & ES_SYSTEM_REQUIRED
        assert not _events(caplog, "keep_awake_lapsed")
    finally:
        ka.update(session_open=False)


@pytest.mark.skipif(sys.platform != "win32", reason="real Win32 power calls")
def test_real_win32_cleared_thread_state_is_rearmed(caplog):
    """Clears this thread's flags behind KeepAwake's back, as a lapse would, and lets the next pulse
    repair it through the real calls. The thread query catches it even when another process (the live
    engine in session) keeps the system-wide bit present."""
    ka = KeepAwake(enabled=True)
    try:
        ka.update(session_open=True)
        set_state, system_state = ka._resolve_setter(), ka._resolve_reader()
        assert set_state is not None and system_state is not None
        set_state(ES_CONTINUOUS)
        with caplog.at_level(logging.INFO, logger="engine.ops.keep_awake"):
            ka.update(session_open=True)
        (lapsed,) = _events(caplog, "keep_awake_lapsed")
        assert lapsed.thread_state == "0x80000000" and lapsed.thread_state_after == "0x80000001"
        assert lapsed.toggled is False and lapsed.restored is True
        assert system_state() & ES_SYSTEM_REQUIRED
    finally:
        ka.update(session_open=False)


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


@pytest.mark.asyncio
async def test_health_pulse_survives_a_raising_keep_awake(tmp_path, caplog):
    """The pulse also hosts the store-stall watchdog, so a keep-awake bug must not end it."""

    class _Raising:
        def update(self, session_open: bool) -> None:
            raise RuntimeError("boom")

    clock = Clock(time_source=_Now(_at(10, 5)))
    cal = _FakeCalendar(_FakeSession(_at(9, 15), _at(15, 30)))
    hm = HealthMonitor(clock, _FakeHealthSettings(tmp_path), calendar=cal, keep_awake=_Raising())
    with caplog.at_level(logging.ERROR, logger="engine.ops.health"):
        await hm.check(check_skew=False)                     # must not raise
    assert _events(caplog, "keep_awake_update_failed")


# --------------------------------------------------------------------------- config keys load
def test_new_ticker_config_keys_load():
    """The new WARMING-wedge + keep-awake knobs load from config/settings.yaml with the seeded values."""
    s = load_settings()
    assert s.ticker.warming_timeout_s == 60
    assert s.ticker.warming_backoff_cap_s == 300
    assert s.ticker.max_wedge_respawns == 5
    assert s.ticker.keep_awake_in_session is True
