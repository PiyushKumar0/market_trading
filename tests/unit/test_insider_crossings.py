"""engine.datafeeds.insider_crossings — the live ``ins`` crossing rule (§2.8.2/§2.8.4): acq_mode
exclusions, the after-hours point-in-time entry session, and the trailing-10-session crossing + re-arm.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest

from engine.core.clock import IST
from engine.datafeeds.insider_crossings import (
    after_hours,
    entry_session_index,
    insider_buy_events,
    is_open_market_buy,
)


def _sessions(n: int, start: date = date(2026, 1, 5)) -> list[date]:
    return [start + timedelta(days=i) for i in range(n)]


def _bdt(d: date, hh: int, mm: int = 0) -> datetime:
    return datetime(d.year, d.month, d.day, hh, mm, tzinfo=IST)


# =========================================================================== acq_mode exclusions
def test_is_open_market_buy_taxonomy():
    # a plain market Buy is open-market; a blank/None mode on a Buy defaults to open-market
    assert is_open_market_buy("Buy", "Market Purchase") is True
    assert is_open_market_buy("Buy", "") is True
    assert is_open_market_buy("Buy", None) is True
    assert is_open_market_buy("buy", "market purchase") is True          # case-insensitive txn_type
    # a Sell is never a buy
    assert is_open_market_buy("Sell", "Market Purchase") is False
    # every §2.8.2 non-market mode is excluded (case-insensitive substring)
    for mode in ("ESOP", "ESOPs", "ESOP Allotment", "Gift", "Inter-se Transfer",
                 "Inter se Transfer", "Pledge Invocation", "Preferential Offer",
                 "Preferential Allotment", "Rights", "Bonus", "gift  "):
        assert is_open_market_buy("Buy", mode) is False, mode


def test_insider_events_exclude_non_market_even_if_huge():
    sess = _sessions(30)
    esop = [{"txn_type": "Buy", "acq_mode": "ESOP Allotment", "value": Decimal("10000000000"),
             "broadcast_dt": _bdt(sess[5], 10)}]
    assert insider_buy_events(sess, esop, 10_000_000) == []        # excluded ⇒ never crosses
    market = [{"txn_type": "Buy", "acq_mode": "Market Purchase", "value": Decimal("20000000"),
               "broadcast_dt": _bdt(sess[5], 10)}]
    assert insider_buy_events(sess, market, 10_000_000) == [5]     # included ⇒ crosses at its session


# =========================================================================== after-hours PIT entry
def test_entry_session_index_after_hours_and_gaps():
    # Thu, Fri, Mon, Tue (a weekend gap between index 1 and 2).
    sess = [date(2026, 1, 8), date(2026, 1, 9), date(2026, 1, 12), date(2026, 1, 13)]
    assert after_hours(_bdt(sess[0], 16)) is True
    assert after_hours(_bdt(sess[0], 15, 30)) is False             # 15:30 sharp is AT the close
    assert entry_session_index(sess, _bdt(sess[0], 10)) == 0       # before close ⇒ same session
    assert entry_session_index(sess, _bdt(sess[1], 16)) == 2       # after close ⇒ NEXT session (skips weekend)
    assert entry_session_index(sess, _bdt(date(2026, 1, 10), 10)) == 2   # Sat filing ⇒ next Mon
    assert entry_session_index(sess, _bdt(sess[3], 16)) is None    # after the last bar ⇒ unmeasurable


def test_insider_after_hours_shifts_entry_to_next_session():
    sess = _sessions(30)
    # one crossing filing broadcast AFTER close on session[5] ⇒ entry at session[6], not [5].
    filings = [{"txn_type": "Buy", "acq_mode": "Market", "value": Decimal("15000000"),
                "broadcast_dt": _bdt(sess[5], 16)}]
    assert insider_buy_events(sess, filings, 10_000_000) == [6]


# =========================================================================== trailing crossing + rearm
@pytest.mark.parametrize(
    ("third", "expected"),
    [(18, [6, 18]), (15, [6]), (16, [6, 16])],
    ids=["re-arm-then-cross", "session-6-still-in-window", "fresh-cross-after-re-arm"],
)
def test_insider_trailing_crossing_and_rearm(third, expected):
    sess = _sessions(30)
    filings = [
        {"txn_type": "Buy", "acq_mode": "Market", "value": Decimal("6000000"), "broadcast_dt": _bdt(sess[5], 10)},
        {"txn_type": "Buy", "acq_mode": "Market", "value": Decimal("6000000"), "broadcast_dt": _bdt(sess[6], 10)},
        {"txn_type": "Buy", "acq_mode": "Market", "value": Decimal("15000000"),
         "broadcast_dt": _bdt(sess[third], 10)},
    ]
    # First crossing at 6 (60L+60L=120L >= 100L), then disarmed while the trailing-10 sum stays >= thr.
    # Session 5 leaves the window at 15 (sum 60L: re-arm), session 6 at 16. A third filing AT 15 lands
    # while 6 is still inside (210L: still disarmed, no event); at 16 or later it is a fresh crossing.
    # 15 and 16 pin the window at exactly 10 sessions: 9 would fire at 15, 11 would not fire at 16.
    assert insider_buy_events(sess, filings, 10_000_000) == expected


def test_insider_single_big_filing_fires_once_only():
    sess = _sessions(30)
    filings = [{"txn_type": "Buy", "acq_mode": "Market", "value": Decimal("50000000"), "broadcast_dt": _bdt(sess[4], 10)}]
    # crosses at 4; stays in the trailing window for sessions 4..13 but must fire exactly once.
    assert insider_buy_events(sess, filings, 10_000_000) == [4]


def test_after_hours_treats_unknown_midnight_time_as_after_close():
    """A date-only broadcast parses to 00:00 — unknown time, NOT 'public before dawn' (WO-16).

    The feeds' date-only fallbacks and scripts/event_study.py's date-only earnings fallback both produce
    midnight; treating it as intraday would enter at that session's close on a filing that was in
    reality disseminated after hours (every real PIT row in the fixture is an evening broadcast).
    """
    sess = [date(2026, 1, 8), date(2026, 1, 9), date(2026, 1, 12)]
    assert after_hours(_bdt(sess[0], 0, 0)) is True
    assert after_hours(_bdt(sess[0], 0, 1)) is False               # a real 00:01 stamp is not the guard
    assert entry_session_index(sess, _bdt(sess[0], 0, 0)) == 1     # conservative: NEXT session
    filings = [{"txn_type": "Buy", "acq_mode": "Market", "value": Decimal("15000000"),
                "broadcast_dt": datetime(2026, 1, 8, tzinfo=IST)}]    # date-only ⇒ midnight
    assert insider_buy_events(sess, filings, 10_000_000) == [1]
