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
