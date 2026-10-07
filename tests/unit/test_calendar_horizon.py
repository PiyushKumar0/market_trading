"""Calendar horizon monitor (plan Q0.4), outbox dedupe and the new shipped settings."""

from __future__ import annotations

import asyncio
from datetime import date, time

import pytest
import yaml
from pydantic import ValidationError

from engine.core.calendar import NSECalendar
from engine.core.config import Settings, load_settings
from engine.notify import catalog
from engine.notify.catalog import MessageKind
from engine.ops.calendar_horizon import check_calendar_horizon, sessions_to_horizon
from tests.unit.test_notifications_journal import _bot

TODAY = date(2026, 6, 17)  # the conftest clock's day (Wednesday)


def _calendar(tmp_path, clock, through: str | None) -> NSECalendar:
    (tmp_path / "2026.yaml").write_text(
        yaml.safe_dump({"year": 2026, "verified": through is not None, "verified_through": through}),
        encoding="utf-8",
    )
    return NSECalendar(tmp_path, clock)


def _rows(conn) -> int:
    return conn.execute("SELECT COUNT(*) FROM notifications").fetchone()[0]


def test_shipped_settings_new_values():
    s = load_settings()
    assert s.clock.calendar_horizon_alert_sessions == 40
    r = s.recommend
    assert (r.gtt_limit_offset_pct, r.veto_window_sessions, r.protection_first_reminder_min) == (1.0, 3, 30)
    assert (r.reminder_window_start, r.reminder_window_end) == (time(8, 0), time(22, 0))


def test_recommend_typo_rejected():
    with pytest.raises(ValidationError):
        Settings(recommend={"veto_window_session": 3})


def test_dedupe_key_one_row_one_send(conn, clock):
    bot, wire = _bot(conn, clock)
    msg = catalog.calendar_horizon(date(2026, 7, 1), 10)
    assert msg.kind is MessageKind.CALENDAR_HORIZON and msg.severity == "warning"
    assert "config/calendar/2027.yaml" in msg.body
    asyncio.run(bot.send(msg))
    asyncio.run(bot.send(msg))
    assert _rows(conn) == 1 and len(wire.sent) == 1


def test_an_expired_failed_row_is_requeued_by_the_next_send(conn, clock):
    bot, wire = _bot(conn, clock)
    msg = catalog.calendar_horizon(date(2026, 7, 1), 10)
    asyncio.run(bot.send(msg))
    conn.execute("UPDATE notifications SET status='failed', attempts=5, last_error='boom'")
    asyncio.run(bot.send(msg))
    assert conn.execute("SELECT COUNT(*), status, attempts, last_error FROM notifications").fetchone()[:] == (
        1, "pending", 0, None,
    )


@pytest.mark.parametrize(("through", "expected"), [("2026-07-15", 20), (None, 0)])
def test_sessions_to_horizon(tmp_path, clock, through, expected):
    assert sessions_to_horizon(_calendar(tmp_path, clock, through), TODAY) == expected


def test_monitor_once_per_horizon(tmp_path, conn, clock):
    bot, wire = _bot(conn, clock)

    def boot(through):
        cal = _calendar(tmp_path, clock, through)
        asyncio.run(check_calendar_horizon(cal, clock, 40, bot.send))

    boot("2026-07-15")
    boot("2026-07-15")
    assert len(wire.sent) == 1
    boot("2026-07-16")
    assert len(wire.sent) == 2
    boot("2026-12-31")
    assert len(wire.sent) == 2
    boot(None)
    assert len(wire.sent) == 3 and _rows(conn) == 3


def test_monitor_swallows_send_failure(tmp_path, clock):
    async def boom(_msg):
        raise RuntimeError("down")

    asyncio.run(check_calendar_horizon(_calendar(tmp_path, clock, None), clock, 40, boom))
