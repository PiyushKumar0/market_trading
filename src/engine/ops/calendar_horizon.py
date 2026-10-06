"""Calendar horizon monitor (plan Q0.4): the only alert for a verified calendar running out."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import date, timedelta

from engine.core.calendar import NSECalendar
from engine.core.clock import Clock
from engine.core.log import get_logger
from engine.notify.catalog import CatalogMessage, calendar_horizon

_log = get_logger("engine.ops.calendar_horizon")


def sessions_to_horizon(calendar: NSECalendar, today: date) -> int:
    """Trading days in (today, verified_horizon]; 0 when no horizon is verified."""
    horizon = calendar.verified_horizon()
    if horizon is None:
        return 0
    days = (horizon - today).days
    return sum(calendar.is_trading_day(today + timedelta(days=i)) for i in range(1, days + 1))


async def check_calendar_horizon(
    calendar: NSECalendar,
    clock: Clock,
    threshold: int,
    send: Callable[[CatalogMessage], Awaitable[None]],
) -> None:
    """Send CALENDAR_HORIZON when fewer than ``threshold`` verified sessions remain. Never raises;
    the message's dedupe key makes repeat boots/nights silent per horizon value."""
    try:
        left = sessions_to_horizon(calendar, clock.today())
        if left < threshold:
            await send(calendar_horizon(calendar.verified_horizon(), left))
    except Exception:  # noqa: BLE001 - a monitor must never take down boot or a job
        _log.exception("calendar_horizon_check_failed")
