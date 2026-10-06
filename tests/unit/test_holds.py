"""Hold map and exit-session arithmetic (plan Q1.1, §1.10)."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pytest
import yaml

from engine.core.calendar import NSECalendar
from engine.core.clock import IST
from engine.core.config import load_settings
from engine.ops.holds import TIME_EXIT_STRATEGIES, build_hold_fn, exit_session, session_of
from engine.risk.limits import LimitTable

REPO = Path(__file__).resolve().parents[2]
SETTINGS = load_settings()


class _Limits:
    def __init__(self) -> None:
        self.table = LimitTable.model_validate(
            yaml.safe_load((REPO / "config" / "limits.yaml").read_text(encoding="utf-8"))
        )

    def load(self) -> LimitTable:
        return self.table


LIMITS = _Limits()
CAPS = {
    "swing": LIMITS.table.limits.max_holding.swing_trading_days,
    "position": LIMITS.table.limits.max_holding.position_trading_days,
}


@pytest.fixture
def cal(tmp_path, clock) -> NSECalendar:
    """2026 only, so the horizon the pending case runs off stays fixed."""
    (tmp_path / "2026.yaml").write_text(
        (REPO / "config" / "calendar" / "2026.yaml").read_text(encoding="utf-8"), encoding="utf-8"
    )
    return NSECalendar(tmp_path, clock)


@pytest.mark.parametrize(
    ("strategy_id", "style", "hold"),
    [
        ("hi52", "swing", 20),
        ("rsi2", "swing", 10),
        ("brk20", "swing", 20),
        ("mom", "swing", 20),
        ("trend", "position", 120),
        ("orb", "intraday", None),
        ("ins", "swing", SETTINGS.ins.hold_sessions),
        ("cat", "swing", SETTINGS.cat.hold_sessions),
        ("cat_reversal", "swing", SETTINGS.cat_reversal.hold_sessions),
    ],
)
def test_shipped_holds_never_exceed_the_style_cap(strategy_id, style, hold):
    got = build_hold_fn(SETTINGS, LIMITS)(strategy_id, style)
    assert got == hold
    assert got is None or got <= CAPS[style]


def test_a_declared_hold_above_the_cap_is_clamped():
    settings = SETTINGS.model_copy(
        update={"ins": SETTINGS.ins.model_copy(update={"hold_sessions": 500})}
    )
    assert build_hold_fn(settings, LIMITS)("ins", "swing") == CAPS["swing"]


def test_time_exit_strategies_are_the_declared_holds():
    assert TIME_EXIT_STRATEGIES == {"hi52", "ins", "cat", "cat_reversal", "rsi2"}


def _ist(*args: int) -> datetime:
    return datetime(*args, tzinfo=IST)


@pytest.mark.parametrize(
    ("ts", "session"),
    [
        (_ist(2026, 6, 17, 8, 0), date(2026, 6, 17)),        # before the open
        (_ist(2026, 6, 17, 15, 29), date(2026, 6, 17)),
        (_ist(2026, 6, 17, 15, 30), date(2026, 6, 18)),       # at the close
        (_ist(2026, 6, 17, 23, 26), date(2026, 6, 18)),       # a late-night /taken
        (_ist(2026, 6, 20, 10, 0), date(2026, 6, 22)),        # Saturday
        (_ist(2026, 6, 26, 10, 0), date(2026, 6, 29)),        # Muharram
        (_ist(2026, 11, 8, 9, 30), date(2026, 11, 9)),        # muhurat is not counted
        (_ist(2026, 6, 17, 10, 5).astimezone(UTC), date(2026, 6, 17)),
    ],
)
def test_session_of(cal, ts, session):
    assert session_of(cal, ts) == session


@pytest.mark.parametrize(
    ("ts", "n", "exit_on"),
    [
        (_ist(2026, 6, 17, 10, 5), 1, date(2026, 6, 17)),
        (_ist(2026, 6, 17, 10, 5), 20, date(2026, 7, 15)),    # skips Muharram 06-26
        (_ist(2026, 11, 5, 10, 0), 3, date(2026, 11, 9)),     # skips the 11-08 muhurat
        (_ist(2026, 11, 5, 10, 0), 4, date(2026, 11, 11)),    # and the 11-10 holiday
        (_ist(2026, 12, 28, 10, 0), 20, None),                # beyond the loaded calendars
    ],
)
def test_exit_session(cal, ts, n, exit_on):
    assert exit_session(cal, ts, n) == exit_on
