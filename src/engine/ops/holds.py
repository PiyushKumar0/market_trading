"""Per-strategy hold horizons (plan §1.10, Q1.1) and the session arithmetic their exit dates use."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime, timedelta
from typing import Any

from engine.core.calendar import NSECalendar
from engine.core.clock import IST
from engine.core.config import Settings
from engine.core.log import get_logger
from engine.strategy.scanners import cat, cat_reversal, hi52, ins
from engine.strategy.scanners.rsi2 import Rsi2Scanner

_log = get_logger("engine.ops.holds")

HoldFn = Callable[[str, str], int | None]

#: Strategies whose exit is a declared time hold rather than a price target.
TIME_EXIT_STRATEGIES = frozenset(
    {hi52.STRATEGY_ID, ins.STRATEGY_ID, cat.STRATEGY_ID, cat_reversal.STRATEGY_ID,
     Rsi2Scanner.strategy_id}
)


def is_time_exit(strategy_id: str) -> bool:
    return strategy_id in TIME_EXIT_STRATEGIES


def build_hold_fn(settings: Settings, limits: Any) -> HoldFn:
    """``hold_fn(strategy_id, style)``: the declared hold capped at the style's §7.1 ``max_holding``
    (read from ``limits.load()`` per call), the cap itself for undeclared swing/position strategies,
    ``None`` for intraday."""
    declared = {
        ins.STRATEGY_ID: int(settings.ins.hold_sessions),
        cat.STRATEGY_ID: int(settings.cat.hold_sessions),
        cat_reversal.STRATEGY_ID: int(settings.cat_reversal.hold_sessions),
        hi52.STRATEGY_ID: int(hi52.DEFAULT_PARAMS["hold_sessions"]),
        Rsi2Scanner.strategy_id: int(Rsi2Scanner.DEFAULT_PARAMS["max_hold_days"]),
    }

    def hold_fn(strategy_id: str, style: str) -> int | None:
        if style == "intraday":
            return None
        cap = limits.load().limits.max_holding
        cap_n = int(cap.position_trading_days if style == "position" else cap.swing_trading_days)
        return min(declared.get(strategy_id, cap_n), cap_n)

    return hold_fn


def session_of(calendar: NSECalendar, ts: datetime) -> date:
    """The counted session ``ts`` belongs to: its own day if that is a counted session not yet
    closed at ``ts``, else the next counted session. Calendar ``ValueError`` propagates."""
    ts = ts.astimezone(IST)
    d = ts.date()
    s = calendar.session(d)
    if s is None or s.is_muhurat or ts >= s.close:
        d += timedelta(days=1)
    return calendar.add_sessions(d, 0)


def exit_session(calendar: NSECalendar, ts: datetime, n: int) -> date | None:
    """The ``n``-th session of a hold entered at ``ts`` (entry session = 1); ``None`` = pending
    (beyond the loaded calendars)."""
    try:
        return calendar.add_sessions(session_of(calendar, ts), n - 1)
    except ValueError:
        _log.info("exit_session_pending", entered_at=ts.isoformat(), hold_sessions=n)
        return None
