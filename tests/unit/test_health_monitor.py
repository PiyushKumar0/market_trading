"""HealthMonitor feed-staleness alerting (§3.2.12, R2) — session-aware since 2026-07-31.

The midnight-rollover incident: a STALE feed out of session is DEFINITIONAL (no ticks exist at
night), but ``check()`` flagged it unconditionally and alerted every minute from 00:32 once the
date rolled. ``feed_stale`` is a problem ONLY while the session is open (feed lost while running).
"""

from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.config import config_dir, load_settings
from engine.ops.health import HealthMonitor
from engine.ops.process_memory import ProcessMemory


class FakeTicker:
    def __init__(self, state: str) -> None:
        self._state = state

    def health(self):
        return SimpleNamespace(state=self._state, last_tick_age_s=12345.0)


def _monitor(state: str, at: datetime, *, calendar: bool = True):
    clock = Clock(time_source=lambda: at)
    alerts: list[tuple[str, str]] = []

    async def alert(severity: str, message: str) -> None:
        alerts.append((severity, message))

    cal = NSECalendar(config_dir() / "calendar", clock, strict=False) if calendar else None
    return HealthMonitor(clock, load_settings(), ticker_supervisor=FakeTicker(state),
                         alert=alert, calendar=cal), alerts


async def test_stale_in_session_is_an_incident():
    mon, alerts = _monitor("STALE", datetime(2026, 6, 17, 11, 0, tzinfo=IST))   # trading Wednesday
    report = await mon.check(check_skew=False)
    assert "feed_stale" in report.problems
    assert alerts and "feed_stale" in alerts[0][1]


async def test_stale_out_of_session_is_definitional_not_an_incident():
    # 00:32 the night after a trading day — the 2026-07-31 midnight-rollover alert spam.
    mon, alerts = _monitor("STALE", datetime(2026, 6, 17, 0, 32, tzinfo=IST))
    report = await mon.check(check_skew=False)
    assert "feed_stale" not in report.problems
    assert alerts == []
    # Post-close the same trading day: equally quiet.
    mon2, alerts2 = _monitor("STALE", datetime(2026, 6, 17, 18, 5, tzinfo=IST))
    report2 = await mon2.check(check_skew=False)
    assert "feed_stale" not in report2.problems and alerts2 == []


async def test_stale_without_calendar_stays_quiet():
    mon, alerts = _monitor("STALE", datetime(2026, 6, 17, 11, 0, tzinfo=IST), calendar=False)
    report = await mon.check(check_skew=False)
    assert "feed_stale" not in report.problems and alerts == []


async def test_healthy_and_warming_raise_nothing_in_session():
    for state in ("HEALTHY", "WARMING", "STOPPED"):
        mon, alerts = _monitor(state, datetime(2026, 6, 17, 11, 0, tzinfo=IST))
        report = await mon.check(check_skew=False)
        assert "feed_stale" not in report.problems, state
        assert alerts == [], state


# ------------------------------------------------------- process-memory telemetry (2026-08-17 crisis)
class _FakeMemoryReader:
    """Records call count and returns a fixed :class:`ProcessMemory`."""

    def __init__(self, mem: ProcessMemory | None) -> None:
        self._mem = mem
        self.calls = 0

    def read(self) -> ProcessMemory | None:
        self.calls += 1
        return self._mem


class _RaisingMemoryReader:
    """Simulates an injected reader that raises directly (not caught by ProcessMemoryReader itself) —
    exercises HealthMonitor's own defensive wrapper, not just the reader's internal try/except."""

    def read(self) -> ProcessMemory:
        raise RuntimeError("boom")


async def test_process_memory_logged_every_fifth_pulse(caplog):
    """The health pulse is 60s (settings.lifecycle.watchdog_poll_s); process_memory is sampled every
    5th pulse (~5 min), not every pulse — see HealthMonitor.__init__ for the cadence rationale."""
    mem = ProcessMemory(private_bytes=111_222_333, working_set_bytes=44_555_666, peak_working_set_bytes=77_888_999)
    reader = _FakeMemoryReader(mem)
    clock = Clock(time_source=lambda: datetime(2026, 6, 17, 11, 0, tzinfo=IST))
    mon = HealthMonitor(clock, load_settings(), memory_reader=reader)

    caplog.set_level("INFO", logger="engine.ops.health")
    for _ in range(4):
        await mon.check(check_skew=False)
    assert reader.calls == 0
    assert "process_memory" not in caplog.text

    await mon.check(check_skew=False)  # 5th pulse: fires
    assert reader.calls == 1
    assert "process_memory" in caplog.text


async def test_process_memory_log_line_carries_the_three_counter_fields(caplog):
    """The emitted line names exactly the fields the crisis needed: private_bytes (PagefileUsage),
    working_set_bytes, peak_working_set_bytes."""
    mem = ProcessMemory(private_bytes=1, working_set_bytes=2, peak_working_set_bytes=3)
    reader = _FakeMemoryReader(mem)
    clock = Clock(time_source=lambda: datetime(2026, 6, 17, 11, 0, tzinfo=IST))
    mon = HealthMonitor(clock, load_settings(), memory_reader=reader, memory_log_every=1)

    caplog.set_level("INFO", logger="engine.ops.health")
    await mon.check(check_skew=False)

    record = next(r for r in caplog.records if r.getMessage() == "process_memory")
    assert record.private_bytes == 1
    assert record.working_set_bytes == 2
    assert record.peak_working_set_bytes == 3


async def test_process_memory_reader_returning_none_logs_nothing(caplog):
    """A read failure inside the reader (already DEBUG-logged there) yields no INFO line at all."""
    reader = _FakeMemoryReader(None)
    clock = Clock(time_source=lambda: datetime(2026, 6, 17, 11, 0, tzinfo=IST))
    mon = HealthMonitor(clock, load_settings(), memory_reader=reader, memory_log_every=1)

    caplog.set_level("INFO", logger="engine.ops.health")
    await mon.check(check_skew=False)

    assert "process_memory" not in caplog.text


async def test_process_memory_reader_failure_does_not_block_health_check():
    """A reader failure must never raise and must never affect the health verdict (telemetry, not a
    check) — compared against a baseline run with no memory reader at all."""
    clock = Clock(time_source=lambda: datetime(2026, 6, 17, 11, 0, tzinfo=IST))
    baseline = HealthMonitor(clock, load_settings(), memory_log_every=1)
    baseline_report = await baseline.check(check_skew=False)

    mon = HealthMonitor(clock, load_settings(), memory_reader=_RaisingMemoryReader(), memory_log_every=1)
    report = await mon.check(check_skew=False)  # must not raise

    assert report.problems == baseline_report.problems
    assert report.healthy == baseline_report.healthy
