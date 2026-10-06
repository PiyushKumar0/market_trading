"""Protection reminders for owner-taken positions (plan Q1.9, D6).

The platform places no orders in RECOMMEND, so a taken position stays unprotected until the owner
confirms the stop order with ``/protected``. At most two reminders per position, tracked in
``positions.protection_reminders`` so a restart neither repeats nor skips one.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta

from engine.core.calendar import NSECalendar
from engine.core.clock import Clock
from engine.core.config import RecommendSettings
from engine.core.db import transaction
from engine.core.log import get_logger
from engine.notify import catalog
from engine.notify.catalog import CatalogMessage
from engine.ops.holdings_reconcile import positions_missing_from_holdings

_log = get_logger(__name__)

MAX_REMINDERS = 2


class ProtectionReminders:
    def __init__(
        self, conn: sqlite3.Connection, clock: Clock, calendar: NSECalendar,
        notify: Callable[[CatalogMessage], Awaitable[None]], settings: RecommendSettings,
    ) -> None:
        self._conn = conn
        self._clock = clock
        self._calendar = calendar
        self._notify = notify
        self._cfg = settings

    def _due(self, opened_at: datetime, now: datetime) -> int:
        """How many reminders are owed by ``now``: the second once the next session has opened."""
        next_open = self._calendar.session(self._calendar.next_trading_day(opened_at.date()))
        if next_open is not None and now >= next_open.open:
            return 2
        return int(now >= opened_at + timedelta(minutes=self._cfg.protection_first_reminder_min))

    async def tick(self) -> None:
        """Send what is owed. Outside the reminder window nothing is sent; the same reminders go at
        the window's start. A reminder missed entirely (outage) is sent once, not twice in a row."""
        now = self._clock.now()
        if not (self._cfg.reminder_window_start <= now.time() <= self._cfg.reminder_window_end):
            return
        rows = self._conn.execute(
            "SELECT position_id, symbol, qty, opened_at, protection_reminders FROM positions "
            "WHERE state='OPEN' AND origin='recommended' AND owner_protected_at IS NULL "
            "AND protection_reminders < ?", (MAX_REMINDERS,),
        ).fetchall()
        if not rows:
            return
        gone = positions_missing_from_holdings(self._conn, now.date(), require_zero=True)
        for row in rows:
            if row["position_id"] in gone:
                continue
            opened = datetime.fromisoformat(row["opened_at"])
            due = self._due(opened, now)
            if due <= row["protection_reminders"]:
                continue
            await self._notify(catalog.protection_reminder(
                symbol=row["symbol"], qty=int(row["qty"]), opened_at=opened.strftime("%d %b %H:%M"),
                as_of=now.strftime("%Y-%m-%d %H:%M IST"),
            ))
            with transaction(self._conn):
                self._conn.execute(
                    "UPDATE positions SET protection_reminders=? WHERE position_id=?",
                    (due, row["position_id"]),
                )
            _log.warning("protection_reminder_sent", position_id=row["position_id"],
                         symbol=row["symbol"], reminder=due)
