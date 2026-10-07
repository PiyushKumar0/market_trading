"""Equal-weight benchmark (plan Q2.2)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from engine.core.calendar import NSECalendar
from engine.core.config import config_dir
from engine.learning.benchmark import bench_pct, compute_ew_return
from engine.marketdata.store import DailyBar, MarketStore

MON, TUE, WED, THU = date(2026, 10, 5), date(2026, 10, 6), date(2026, 10, 7), date(2026, 10, 8)


@pytest.fixture
def cal(clock):
    return NSECalendar(config_dir() / "calendar", clock, strict=False)


@pytest.fixture
def store(tmp_path, clock):
    s = MarketStore(tmp_path / "market.duckdb", tmp_path / "parquet", clock).open()
    yield s
    s.close()


def _universe(store, d, symbols):
    store.upsert_universe_daily(
        [{"d": d, "symbol": s, "included": True, "exclusion_reasons": None,
          "median_traded_value": Decimal("1")} for s in symbols]
    )


def _closes(store, d, closes):
    store.upsert_bars_1d(
        [DailyBar(symbol=s, d=d, open=Decimal(c), high=Decimal(c), low=Decimal(c),
                  close=Decimal(c), volume=1, src="bhavcopy") for s, c in closes.items()]
    )


def test_excludes_split_and_outlier_and_averages_rest(store, cal, clock):
    _universe(store, WED, ["A", "B", "SPLIT", "JUMP", "NOBAR"])
    _closes(store, TUE, {"A": "100", "B": "100", "SPLIT": "100", "JUMP": "100"})
    _closes(store, WED, {"A": "102", "B": "100", "SPLIT": "50", "JUMP": "130"})
    store.upsert_corp_actions([{"symbol": "SPLIT", "ex_date": WED, "kind": "split", "ratio": None,
                                "amount": None, "source": "test", "recorded_at": clock.now()}])
    ret, n = compute_ew_return(store, cal, WED)
    assert n == 2
    assert ret == pytest.approx(0.01)


def test_missing_universe_row_uses_previous(store, cal):
    _universe(store, MON, ["A"])
    _closes(store, TUE, {"A": "100"})
    _closes(store, WED, {"A": "101"})
    assert compute_ew_return(store, cal, WED) == pytest.approx((0.01, 1))


def test_previous_session_skips_muhurat(store, cal):
    fri, mon = date(2026, 11, 6), date(2026, 11, 9)
    assert cal.previous_trading_day(mon) == date(2026, 11, 8)
    _universe(store, mon, ["A"])
    _closes(store, fri, {"A": "100"})
    _closes(store, mon, {"A": "102"})
    assert compute_ew_return(store, cal, mon) == pytest.approx((0.02, 1))


def test_no_universe_row_within_five_sessions_is_gap(store, cal):
    _universe(store, date(2026, 9, 21), ["A"])
    assert compute_ew_return(store, cal, WED) is None


def test_empty_universe_raises(store, cal):
    store.upsert_universe_daily(
        [{"d": WED, "symbol": "A", "included": False, "exclusion_reasons": ["illiquid"],
          "median_traded_value": Decimal("1")}]
    )
    with pytest.raises(ValueError):
        compute_ew_return(store, cal, WED)


@pytest.mark.parametrize(
    ("daily", "fill_d", "exit_d", "reason", "expected"),
    [
        ({TUE: 0.01, WED: 0.02}, MON, WED, "time", Decimal("3.020000")),
        ({TUE: 0.01, WED: 0.02}, MON, WED, "stop", Decimal("1.000000")),
        ({TUE: 0.01}, MON, WED, "target", Decimal("1.000000")),
        ({}, MON, TUE, "stop", Decimal("0")),
        ({}, TUE, TUE, "stop", Decimal("0")),
        ({TUE: 0.01}, MON, WED, "time", None),
        ({TUE: 0.01, WED: None}, MON, WED, "time", None),
        ({date(2026, 12, 31): 0.01}, date(2026, 12, 30), date(2026, 12, 31), "time", Decimal("1.000000")),
        ({}, date(2026, 12, 31), date(2026, 12, 31), "stop", Decimal("0")),
        ({date(2026, 12, 31): 0.01}, date(2026, 12, 30), date(2027, 1, 4), "time", None),
    ],
)
def test_bench_pct_window(cal, daily, fill_d, exit_d, reason, expected):
    assert bench_pct(daily, fill_d, exit_d, reason, cal) == expected
