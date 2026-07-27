"""NSE calendar + IST clock (R6). Trading-day logic, sessions, fail-safe strict mode, trade window."""

from __future__ import annotations

from datetime import date, datetime, time

import pytest

from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.config import config_dir
from engine.core.types import TradeWindow

CAL_DIR = config_dir() / "calendar"

WED = date(2026, 6, 17)   # a regular trading day
FRI = date(2026, 6, 19)
SAT = date(2026, 6, 20)
SUN = date(2026, 6, 21)
MON = date(2026, 6, 22)
REPUBLIC_DAY = date(2026, 1, 26)


@pytest.fixture
def cal(clock) -> NSECalendar:
    return NSECalendar(CAL_DIR, clock, strict=False)


def test_clock_now_is_ist_aware(clock):
    now = clock.now()
    assert now.tzinfo is not None
    assert now.utcoffset() == datetime(2026, 6, 17, tzinfo=IST).utcoffset()


def test_clock_rejects_naive_source():
    bad = Clock(time_source=lambda: datetime(2026, 6, 17, 10, 0))  # naive
    with pytest.raises(ValueError):
        bad.now()


def test_trading_day_logic(cal):
    assert cal.is_trading_day(WED) is True
    assert cal.is_trading_day(SAT) is False
    assert cal.is_trading_day(SUN) is False
    assert cal.is_trading_day(REPUBLIC_DAY) is False  # holiday


def test_no_calendar_no_trading(cal):
    # No 2027 calendar file is loaded => not a trading day (fail-safe, R6).
    assert cal.is_trading_day(date(2027, 6, 17)) is False


def test_strict_mode_refuses_unverified(clock, tmp_path):
    # Strict mode refuses any date whose year-calendar is not verified: true ("no calendar, no
    # trading", R6) — tested against a synthetic draft calendar, not the shipped files (which are
    # verified since the B2 boot-log fix, runbooks/boot-log-investigation-2026-07-10.md).
    (tmp_path / "2026.yaml").write_text("year: 2026\nverified: false\nholidays: []\n", encoding="utf-8")
    assert NSECalendar(tmp_path, clock, strict=True).is_trading_day(WED) is False
    assert NSECalendar(tmp_path, clock, strict=False).is_trading_day(WED) is True  # dev mode tolerates


def test_strict_mode_refuses_beyond_verified_through(clock, tmp_path):
    (tmp_path / "2026.yaml").write_text(
        'year: 2026\nverified: true\nverified_through: "2026-05-31"\nholidays: []\n', encoding="utf-8"
    )
    strict = NSECalendar(tmp_path, clock, strict=True)
    assert strict.is_trading_day(date(2026, 5, 27)) is True   # Wed at/inside the verified horizon
    assert strict.is_trading_day(WED) is False                # Jun 17 > verified_through => refused


def test_shipped_calendars_are_strict_usable(clock):
    # The shipped 2024/2025/2026 calendars are verified with a full-year horizon so the 200-session
    # warmup lookback can enumerate history (boot-log fix B2). Guards the exact regression that froze
    # warmup with "calendar horizon < 200 sessions".
    strict = NSECalendar(CAL_DIR, clock, strict=True)
    assert strict.is_trading_day(WED) is True                 # 2026 mid-week, verified
    assert strict.is_trading_day(date(2025, 6, 18)) is True   # Wed, 2025 calendar present + verified
    assert strict.is_trading_day(date(2024, 6, 19)) is True   # Wed, 2024 calendar present + verified
    assert strict.verified_horizon() >= date(2026, 12, 31)


def test_session_times(cal):
    session = cal.session(WED)
    assert session is not None
    assert session.open == datetime(2026, 6, 17, 9, 15, tzinfo=IST)
    assert session.close == datetime(2026, 6, 17, 15, 30, tzinfo=IST)
    assert session.pre_open_start == datetime(2026, 6, 17, 9, 0, tzinfo=IST)
    assert cal.session(SAT) is None


def test_next_trading_day_skips_weekend(cal):
    assert cal.next_trading_day(FRI) == MON


def test_next_trading_day_skips_holiday(cal):
    # 2026-01-23 is a Friday; 24/25 weekend; 26 Republic Day => next trading day is 27 (Tue).
    assert cal.next_trading_day(date(2026, 1, 23)) == date(2026, 1, 27)


def test_trade_window_seed_clamped_to_session(clock):
    cal = NSECalendar(
        CAL_DIR, clock, strict=False,
        window_seed=TradeWindow(start=time(10, 0), end=time(10, 30), squareoff_buffer_min=5),
    )
    start, end = cal.trade_window(WED)
    assert start == datetime(2026, 6, 17, 10, 0, tzinfo=IST)
    assert end == datetime(2026, 6, 17, 10, 30, tzinfo=IST)


def test_trade_window_clamps_to_session_bounds(clock):
    # A window wider than the session is clamped to [open, close].
    cal = NSECalendar(
        CAL_DIR, clock, strict=False,
        window_seed=TradeWindow(start=time(8, 0), end=time(16, 0), squareoff_buffer_min=5),
    )
    start, end = cal.trade_window(WED)
    assert start == datetime(2026, 6, 17, 9, 15, tzinfo=IST)
    assert end == datetime(2026, 6, 17, 15, 30, tzinfo=IST)


def test_trade_window_reads_sticky_sqlite(clock, conn):
    conn.execute(
        "INSERT INTO trade_window_state (id, start_ist, end_ist, squareoff_buffer_min, set_by, changed_at)"
        " VALUES (1, '11:00', '11:20', 5, 'owner', '2026-06-17T09:00:00+05:30')"
    )
    cal = NSECalendar(CAL_DIR, clock, strict=False, sqlite_conn=conn)
    start, end = cal.trade_window(WED)
    assert start == datetime(2026, 6, 17, 11, 0, tzinfo=IST)
    assert end == datetime(2026, 6, 17, 11, 20, tzinfo=IST)


def test_trade_window_on_nontrading_day_raises(cal):
    with pytest.raises(ValueError):
        cal.trade_window(SAT)
