from __future__ import annotations

from datetime import datetime

import pytest

from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.config import RecommendSettings, config_dir
from engine.notify.catalog import MessageKind
from engine.ops import protection_reminders as pr

WED_1000 = datetime(2026, 6, 17, 10, 0, tzinfo=IST)


def at(day: int, hh: int, mm: int = 0) -> datetime:
    return datetime(2026, 6, day, hh, mm, tzinfo=IST)


class Harness:
    def __init__(self, conn) -> None:
        self.conn = conn
        self.now = WED_1000
        self.sent: list = []
        self.clock = Clock(time_source=lambda: self.now)
        self.calendar = NSECalendar(config_dir() / "calendar", self.clock, strict=False)
        self.new_instance()

    def new_instance(self) -> None:
        async def notify(msg) -> None:
            self.sent.append(msg)

        self.reminders = pr.ProtectionReminders(
            self.conn, self.clock, self.calendar, notify, RecommendSettings())

    def add(self, pid="p1", opened=WED_1000, **cols) -> None:
        row = {"state": "OPEN", "origin": "recommended", "protection_reminders": 0,
               "owner_protected_at": None, **cols}
        self.conn.execute(
            "INSERT INTO positions (position_id, symbol, side, style, product, qty, avg_entry, "
            "state, is_paper, origin, opened_at, protection_reminders, owner_protected_at) "
            "VALUES (?, 'RELIANCE', 'BUY', 'swing', 'CNC', 5, '100', ?, 0, ?, ?, ?, ?)",
            (pid, row["state"], row["origin"], opened.isoformat(), row["protection_reminders"],
             row["owner_protected_at"]),
        )

    async def tick(self, when: datetime) -> int:
        self.now = when
        await self.reminders.tick()
        return len(self.sent)

    def counter(self, pid="p1") -> int:
        return self.conn.execute(
            "SELECT protection_reminders FROM positions WHERE position_id=?", (pid,)).fetchone()[0]


@pytest.fixture
def h(conn) -> Harness:
    return Harness(conn)


@pytest.mark.asyncio
async def test_two_reminders_then_silence_and_a_restart_neither_repeats_nor_skips(h):
    h.add()
    assert await h.tick(at(17, 10, 29)) == 0                    # before opened_at + 30 min
    assert await h.tick(at(17, 10, 31)) == 1
    h.new_instance()                                            # restart mid-sequence
    assert await h.tick(at(17, 15, 0)) == 1                     # same session: no repeat
    assert await h.tick(at(18, 9, 14)) == 1                     # next session not yet open
    assert await h.tick(at(18, 9, 16)) == 2
    assert await h.tick(at(18, 9, 21)) == 2 and await h.tick(at(19, 12, 0)) == 2
    assert h.counter() == 2

    msg = h.sent[0]
    assert msg.kind == MessageKind.PROTECTION_REMINDER and msg.severity == "critical"
    assert "2026-06-17 10:31 IST" in msg.body and "/protected RELIANCE" in msg.body


@pytest.mark.asyncio
async def test_late_night_taken_waits_for_the_window_then_follows_the_session_open(h):
    h.add(opened=at(17, 23, 0))
    assert await h.tick(at(17, 23, 40)) == 0
    assert await h.tick(at(18, 7, 59)) == 0
    assert await h.tick(at(18, 8, 0)) == 1
    assert await h.tick(at(18, 9, 16)) == 2


@pytest.mark.asyncio
async def test_a_reminder_missed_in_an_outage_goes_once(h):
    h.add()
    assert await h.tick(at(19, 10, 0)) == 1
    assert h.counter() == 2


@pytest.mark.asyncio
async def test_confirmation_before_the_first_reminder_silences_it(h):
    h.add(owner_protected_at=at(17, 10, 10).isoformat())
    assert await h.tick(at(17, 11, 0)) == 0 and await h.tick(at(18, 9, 30)) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("cols", [
    {"state": "CLOSED"},
    {"protection_reminders": 2},                                # grandfathered by migration 0015
    {"origin": "platform"},
])
async def test_positions_that_need_no_reminder_get_none(h, cols):
    h.add(**cols)
    assert await h.tick(at(18, 9, 30)) == 0


@pytest.mark.asyncio
async def test_a_position_the_journal_shows_at_zero_is_skipped(h, monkeypatch):
    h.add()
    monkeypatch.setattr(pr, "positions_missing_from_holdings",
                        lambda conn, d, require_zero: {"p1": object()})
    assert await h.tick(at(18, 9, 30)) == 0 and h.counter() == 0
