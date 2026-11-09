"""scripts/macro_mood_common.py: the macro mood study's shared constants, timing rules and verdict. Synthetic only."""

from __future__ import annotations

import importlib.util
import json
import math
import sys
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
import numpy as np
import pandas as pd
import pytest

_ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("mt_macro_mood_common", _ROOT / "scripts" / "macro_mood_common.py")
mm = importlib.util.module_from_spec(_spec)
sys.modules["mt_macro_mood_common"] = mm
_spec.loader.exec_module(mm)

yahoo_daily, us_overnight, brent_change = mm.yahoo_daily, mm.us_overnight, mm.brent_change
trailing_pctl_signal, zscore_trailing = mm.trailing_pctl_signal, mm.zscore_trailing
index_trade, non_overlapping, verdict, TradeRow = mm.index_trade, mm.non_overlapping, mm.verdict, mm.TradeRow

from engine.learning.multiplicity import family_trials, load_trials, registration_alpha  # noqa: E402
from engine.learning.validate import fold_pass_min  # noqa: E402
from engine.strategy.cost_model import CostModel  # noqa: E402

NY = "America/New_York"
EST_GMTOFFSET = -18000   # Yahoo's meta.gmtoffset holds the CURRENT offset; in November that is EST


# ----------------------------------------------------------------------------- helpers
def write_chart(tmp_path: Path, *, tz: str, ts: list[int], close: list, opens: list | None = None,
                gmtoffset: int = EST_GMTOFFSET, name: str = "chart.json") -> Path:
    """A Yahoo v8 chart document as the fetch script stores it."""
    opens = opens if opens is not None else close
    quote = {"open": opens, "high": close, "low": close, "close": close, "volume": [0] * len(ts)}
    doc = {"chart": {"result": [{"meta": {"exchangeTimezoneName": tz, "gmtoffset": gmtoffset},
                                 "timestamp": ts, "indicators": {"quote": [quote]}}], "error": None}}
    p = tmp_path / name
    p.write_text(json.dumps(doc), encoding="utf-8")
    return p


def local_midnight_utc(day: str, tz: str = NY) -> int:
    return int(datetime.fromisoformat(day).replace(tzinfo=ZoneInfo(tz)).timestamp())


def sessions(n: int, start: date = date(2026, 1, 5)) -> list[date]:
    out, d = [], start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


def frame(*, opens: list[float], closes: list[float], days: list[date]) -> pd.DataFrame:
    return pd.DataFrame({"open": opens, "high": [max(o, c) for o, c in zip(opens, closes, strict=True)],
                         "low": [min(o, c) for o, c in zip(opens, closes, strict=True)], "close": closes},
                        index=pd.Index(days, name="date"))


def trades(n: int, *, net: float, gross: float | None = None, second_half_net: float | None = None,
           start: date = date(2010, 1, 4)) -> list[TradeRow]:
    """``n`` one-per-week trades (entry dates far enough apart that no CPCV purge empties a fold)."""
    days = sessions(5 * n, start)[::5]
    out = []
    for i, d in enumerate(days):
        nv = second_half_net if second_half_net is not None and i >= n // 2 else net
        out.append(TradeRow(signal_date=d, entry_date=d, exit_date=d, gross=gross if gross is not None else nv, net=nv))
    return out


# ----------------------------------------------------------------------------- brief tests
def test_bzf_bar_stamped_new_york_midnight_in_summer_is_its_own_session(tmp_path):
    # 2026-10-30 04:00 UTC = 00:00 EDT -> session 2026-10-30, not 10-29
    df = yahoo_daily(write_chart(tmp_path, tz="America/New_York", ts=[1793332800], close=[117.64]))
    assert df.index[0] == date(2026, 10, 30)


def test_dst_switch_weeks_keep_session_dates(tmp_path):
    # midnight-local stamps on 2026-03-06 (EST) and 2026-03-09 (EDT)
    df = yahoo_daily(write_chart(tmp_path, tz="America/New_York",
                                 ts=[local_midnight_utc("2026-03-06"), local_midnight_utc("2026-03-09")], close=[1, 2]))
    assert list(df.index) == [date(2026, 3, 6), date(2026, 3, 9)]


def test_us_overnight_compounds_us_sessions_between_nifty_sessions():
    nifty = [date(2026, 11, 3), date(2026, 11, 5)]          # 11-04 an Indian holiday in this fixture
    spx = pd.Series([100.0, 101.0, 99.99], index=[date(2026, 11, 2), date(2026, 11, 3), date(2026, 11, 4)])
    r = us_overnight(nifty, spx)
    assert r[date(2026, 11, 5)] == pytest.approx((99.99 / 100.0 - 1) * 100)   # u = 11-03 and 11-04


def test_us_overnight_nan_when_no_new_us_close():
    nifty = [date(2026, 11, 26), date(2026, 11, 27)]          # US Thanksgiving 11-26: no US session
    spx = pd.Series([100.0], index=[date(2026, 11, 25)])
    assert math.isnan(us_overnight(nifty, spx)[date(2026, 11, 27)])


def test_brent_change_uses_latest_settlement_before_d_and_flags_month_end():
    brent = pd.Series([113.88, 115.32, 114.88, 117.64, 104.77],
                      index=[date(2026, 10, 27), date(2026, 10, 28), date(2026, 10, 29), date(2026, 10, 30), date(2026, 11, 2)])
    out = brent_change([date(2026, 11, 2), date(2026, 11, 3)], brent, n=3)
    assert out.loc[date(2026, 11, 2), "pct"] == pytest.approx((117.64 / 113.88 - 1) * 100)   # 11-02 itself excluded
    assert bool(out.loc[date(2026, 11, 2), "roll_flag"]) is True                               # 10-30 = last weekday of Oct
    assert out.loc[date(2026, 11, 3), "pct"] == pytest.approx((104.77 / 115.32 - 1) * 100)


def test_trailing_pctl_signal_uses_only_strictly_earlier_values():
    x = pd.Series([1.0] * 250 + [1.0, 5.0])
    sig = trailing_pctl_signal(x, window=250, pctl=90)
    assert not sig.iloc[249] and not sig.iloc[250] and sig.iloc[251]


def test_non_overlapping_skips_signals_inside_hold():
    s = sessions(10)
    assert non_overlapping([s[0], s[2], s[5], s[6]], s, hold=5) == [s[0], s[5]]


def test_index_trade_counts_entry_session_as_one():
    px = frame(opens=[100.0, 102.0], closes=[101.0, 103.0], days=sessions(2))
    assert index_trade(px, sessions(2), sessions(2)[0], hold=1) == (sessions(2)[0], pytest.approx(1.0))
    assert index_trade(px, sessions(2), sessions(2)[0], hold=2) == (sessions(2)[1], pytest.approx(3.0))
    assert index_trade(px, sessions(2), sessions(2)[1], hold=2) is None


def test_verdict_labels():
    pop = [0.0] * 1000
    assert verdict(trades(29, net=2.0), pop, 5, 0.3)["label"] == "insufficient"
    assert verdict(trades(30, net=-0.1), pop, 5, 0.3)["label"] == "refuted"
    assert verdict(trades(40, gross=3.0, net=2.7), pop, 5, 0.3)["label"] == "supported"
    assert verdict(trades(40, gross=3.0, net=2.7, second_half_net=-1.0), pop, 5, 0.3)["label"] == "null"


# ----------------------------------------------------------------------------- registered constants
def test_registered_constants_are_the_spec_values():
    assert (mm.FAMILY, mm.N_TRIALS, mm.ALPHA, mm.FOLD_PASS_MIN, mm.MIN_TRADES, mm.PERM_SEED) == (
        "edge/macro_mood", 10, 0.005, 0.60, 30, 20261110)
    assert (mm.US_FALL_PCT, mm.US_RISE_PCT, mm.OIL_DROP_PCT, mm.OIL_SESSIONS) == (-2.0, 1.0, -5.0, 3)
    assert (mm.FII_SESSIONS, mm.FII_WINDOW, mm.FII_PCTL) == (3, 250, 90)
    assert (mm.BREADTH_MIN_PCT, mm.LIQUID_MIN_TV, mm.LIQUID_WINDOW) == (75.0, 1e8, 20)
    assert (mm.Z_WINDOW, mm.COMBINED_TOP) == (250, 0.10)
    assert mm.HOLD == {"us_rebound": 1, "us_follow": 5, "oil_drop": 5, "fii_short_cover": 5,
                       "breadth_thrust": 5, "combined": 5, "news_tone": 5, "policy": 10}
    assert mm.PRODUCT == {k: ("MIS" if k == "us_rebound" else "CNC") for k in mm.HOLD}
    assert mm.EXCLUDED == (date(2026, 9, 1), date(2026, 11, 9))
    assert mm.REGISTERED_FROM == date(2026, 11, 11)


def test_samples_are_the_spec_windows():
    seen_us = (date(2022, 7, 14), date(2026, 8, 31))
    assert mm.SAMPLES == {
        "us_rebound": {"confirm": (date(2008, 1, 1), date(2022, 7, 13)), "seen": seen_us},
        "us_follow": {"confirm": (date(2008, 1, 1), date(2022, 7, 13)), "seen": seen_us},
        "oil_drop": {"confirm": (date(2008, 1, 1), date(2026, 8, 31)), "seen": None},
        "fii_short_cover": {"confirm": (date(2012, 1, 2), date(2026, 8, 31)), "seen": None},
        "breadth_thrust": {"confirm": (date(2022, 7, 14), date(2026, 8, 31)), "seen": None},
        "combined": {"confirm": (None, date(2022, 7, 13)), "seen": seen_us},
        "news_tone": {"confirm": (date(2026, 11, 11), None), "seen": (date(2026, 7, 16), date(2026, 11, 10))},
        "policy": {"confirm": (date(2026, 11, 11), None), "seen": (date(2026, 7, 16), date(2026, 11, 10))},
    }


def test_alpha_and_fold_floor_match_the_family_ledger():
    """The constants equal what the shared multiplicity helpers give for the family's ledger count."""
    n = family_trials(load_trials(), mm.FAMILY)
    assert n == mm.N_TRIALS
    assert mm.ALPHA == pytest.approx(registration_alpha(n))
    assert mm.FOLD_PASS_MIN == fold_pass_min(n)


def test_in_window_and_excluded_bounds_are_inclusive():
    assert mm.in_window(date(2022, 7, 13), (None, date(2022, 7, 13)))
    assert not mm.in_window(date(2022, 7, 14), (None, date(2022, 7, 13)))
    assert mm.in_window(date(2030, 1, 1), (date(2026, 11, 11), None))
    assert mm.is_excluded(date(2026, 9, 1)) and mm.is_excluded(date(2026, 11, 9))
    assert not mm.is_excluded(date(2026, 8, 31)) and not mm.is_excluded(date(2026, 11, 10))


# ----------------------------------------------------------------------------- yahoo_daily
def test_yahoo_daily_november_switch_and_gmtoffset_is_ignored(tmp_path):
    # 2026-10-30 is EDT, 2026-11-02 is EST; a fixed EST gmtoffset would put the first a day early
    ts = [local_midnight_utc("2026-10-30"), local_midnight_utc("2026-11-02")]
    df = yahoo_daily(write_chart(tmp_path, tz=NY, ts=ts, close=[10.0, 11.0], gmtoffset=EST_GMTOFFSET))
    assert list(df.index) == [date(2026, 10, 30), date(2026, 11, 2)]


def test_yahoo_daily_kolkata_stamps(tmp_path):
    ts = [local_midnight_utc("2026-11-09", "Asia/Kolkata") + 9 * 3600 + 15 * 60]   # 09:15 IST bar stamp
    df = yahoo_daily(write_chart(tmp_path, tz="Asia/Kolkata", ts=ts, close=[24000.5], gmtoffset=19800))
    assert df.index[0] == date(2026, 11, 9)


def test_yahoo_daily_shape_drops_null_close_and_sorts(tmp_path):
    ts = [local_midnight_utc(d) for d in ("2026-03-04", "2026-03-02", "2026-03-03")]
    df = yahoo_daily(write_chart(tmp_path, tz=NY, ts=ts, close=[3, 1, None], opens=[3, 1, 2]))
    assert list(df.columns) == ["open", "high", "low", "close"]
    assert df.index.name == "date"
    assert list(df.index) == [date(2026, 3, 2), date(2026, 3, 4)]
    assert all(df.dtypes == np.float64)
    assert df.loc[date(2026, 3, 4), "close"] == 3.0


def test_yahoo_daily_duplicate_session_refuses(tmp_path):
    ts = [local_midnight_utc("2026-03-02"), local_midnight_utc("2026-03-02") + 3600]
    with pytest.raises(ValueError, match="duplicate"):
        yahoo_daily(write_chart(tmp_path, tz=NY, ts=ts, close=[1, 2]))


def test_yahoo_daily_error_document_refuses(tmp_path):
    p = tmp_path / "bad.json"
    p.write_text(json.dumps({"chart": {"result": None, "error": {"code": "Not Found"}}}), encoding="utf-8")
    with pytest.raises(ValueError):
        yahoo_daily(p)


# ----------------------------------------------------------------------------- us_overnight
def test_us_overnight_first_nifty_day_has_no_d_prev():
    nifty = [date(2026, 11, 3), date(2026, 11, 4)]
    spx = pd.Series([100.0, 101.0, 102.0], index=[date(2026, 10, 30), date(2026, 11, 2), date(2026, 11, 3)])
    r = us_overnight(nifty, spx)
    assert math.isnan(r[date(2026, 11, 3)])
    assert r[date(2026, 11, 4)] == pytest.approx((102.0 / 101.0 - 1) * 100)


def test_us_overnight_excludes_the_us_session_dated_d():
    nifty = [date(2026, 11, 3), date(2026, 11, 4)]
    spx = pd.Series([100.0, 101.0, 150.0], index=[date(2026, 11, 2), date(2026, 11, 3), date(2026, 11, 4)])
    assert us_overnight(nifty, spx)[date(2026, 11, 4)] == pytest.approx(1.0)


def test_us_overnight_nan_without_a_us_close_before_the_window():
    nifty = [date(2026, 11, 3), date(2026, 11, 4)]
    spx = pd.Series([101.0], index=[date(2026, 11, 3)])          # u = 11-03 has no previous US close
    assert math.isnan(us_overnight(nifty, spx)[date(2026, 11, 4)])


def test_us_overnight_rejects_unsorted_nifty_days():
    with pytest.raises(ValueError):
        us_overnight([date(2026, 11, 4), date(2026, 11, 3)], pd.Series([1.0], index=[date(2026, 11, 2)]))


# ----------------------------------------------------------------------------- brent_change
def test_brent_change_nan_without_n_earlier_sessions_and_no_flag_mid_month():
    brent = pd.Series([100.0, 90.0, 95.0, 94.0],
                      index=[date(2026, 10, 12), date(2026, 10, 13), date(2026, 10, 14), date(2026, 10, 15)])
    out = brent_change([date(2026, 10, 15), date(2026, 10, 16)], brent, n=3)
    assert math.isnan(out.loc[date(2026, 10, 15), "pct"])                 # only 3 sessions before 10-15
    assert out.loc[date(2026, 10, 16), "pct"] == pytest.approx(-6.0)
    assert bool(out.loc[date(2026, 10, 16), "roll_flag"]) is False


def test_brent_roll_flag_last_weekday_when_month_ends_on_a_weekend():
    # 2026-05-29 is a Friday and the last weekday of May (05-30/31 are a weekend)
    days = [date(2026, 5, 22), date(2026, 5, 25), date(2026, 5, 26), date(2026, 5, 27), date(2026, 5, 28),
            date(2026, 5, 29), date(2026, 6, 1), date(2026, 6, 2), date(2026, 6, 3)]
    brent = pd.Series([100.0 - i for i in range(len(days))], index=days)
    out = brent_change([date(2026, 5, 29), date(2026, 6, 1), date(2026, 6, 3), date(2026, 6, 4)], brent, n=3)
    assert bool(out.loc[date(2026, 5, 29), "roll_flag"]) is False         # 05-25..05-28
    assert bool(out.loc[date(2026, 6, 1), "roll_flag"]) is True           # 05-26..05-29
    assert bool(out.loc[date(2026, 6, 3), "roll_flag"]) is True           # 05-28..06-02
    assert bool(out.loc[date(2026, 6, 4), "roll_flag"]) is True           # 05-29..06-03: 05-29 compared
    assert out.loc[date(2026, 6, 4), "pct"] == pytest.approx((92.0 / 95.0 - 1) * 100)


# ----------------------------------------------------------------------------- trailing statistics
def test_trailing_pctl_signal_false_before_window_and_on_nan():
    x = pd.Series([0.0, 0.0, 9.0, np.nan, 9.0])
    sig = trailing_pctl_signal(x, window=2, pctl=50)
    assert list(sig) == [False, False, True, False, True]
    assert sig.dtype == bool


def test_trailing_pctl_signal_window_is_the_previous_values_skipping_gaps():
    # previous two VALUES of index 4 are 1.0 and 1.0 (the NaN is not a value), so 2.0 exceeds them
    x = pd.Series([5.0, 1.0, 1.0, np.nan, 2.0])
    assert list(trailing_pctl_signal(x, window=2, pctl=90)) == [False, False, False, False, True]


def test_trailing_pctl_signal_linear_percentile_boundary():
    # 87.5th of five values sits at rank 3.5: exact in binary, so equality is tested, not float noise
    x = pd.Series([0.0, 10.0, 20.0, 30.0, 40.0, 35.0, 37.6])
    sig = trailing_pctl_signal(x, window=5, pctl=87.5)
    assert not sig.iloc[5]                        # linear pctl of 0..40 is 35: equal is not "exceeds"
    assert sig.iloc[6]                            # previous five 10,20,30,40,35: linear 37.5 (not 'higher' 40)


def test_zscore_trailing_previous_values_sample_sd():
    x = pd.Series([1.0, 2.0, 3.0, 10.0])
    z = zscore_trailing(x, window=3)
    assert z.iloc[:3].isna().all()
    assert z.iloc[3] == pytest.approx((10.0 - 2.0) / 1.0)            # mean 2, sample SD 1


def test_zscore_trailing_zero_sd_is_nan_and_nan_input_stays_nan():
    x = pd.Series([0.1, 0.1, 0.1, 0.5, np.nan])
    z = zscore_trailing(x, window=3)
    assert math.isnan(z.iloc[3]) and math.isnan(z.iloc[4])


def test_trailing_stats_keep_the_input_index():
    idx = pd.Index(sessions(4))
    x = pd.Series([1.0, 2.0, 3.0, 4.0], index=idx)
    assert list(zscore_trailing(x, 2).index) == list(idx)
    assert list(trailing_pctl_signal(x, 2, 90).index) == list(idx)


# ----------------------------------------------------------------------------- trade construct
def test_non_overlapping_hold_one_keeps_consecutive_sessions_and_dedupes():
    s = sessions(5)
    assert non_overlapping([s[2], s[0], s[1], s[1]], s, hold=1) == [s[0], s[1], s[2]]


def test_non_overlapping_signal_on_exit_session_is_skipped():
    s = sessions(10)
    assert non_overlapping([s[0], s[4], s[5]], s, hold=5) == [s[0], s[5]]   # s[4] is the exit session


def test_non_overlapping_unknown_session_raises():
    with pytest.raises(ValueError):
        non_overlapping([date(2026, 1, 3)], sessions(5), hold=1)          # a Saturday


def test_index_trade_none_when_a_bar_is_missing_or_unusable():
    s = sessions(3)
    px = frame(opens=[100.0, 101.0, 102.0], closes=[100.0, 101.0, 102.0], days=s).drop(index=s[2])
    assert index_trade(px, s, s[0], hold=3) is None                  # exit session has no bar
    px2 = frame(opens=[np.nan, 101.0], closes=[100.0, 101.0], days=s[:2])
    assert index_trade(px2, s[:2], s[0], hold=1) is None


def test_index_trade_unknown_session_raises():
    s = sessions(2)
    with pytest.raises(ValueError):
        index_trade(frame(opens=[1.0, 1.0], closes=[1.0, 1.0], days=s), s, date(2026, 1, 3), hold=1)


def test_cost_pct_is_the_cost_models_round_trip_at_the_research_notional():
    cm = CostModel.from_config()
    assert mm.cost_pct("CNC") == pytest.approx(float(cm.breakeven_pct(Decimal("20000"), "CNC")))
    assert mm.cost_pct("MIS") == pytest.approx(float(cm.breakeven_pct(Decimal("20000"), "MIS")))
    assert isinstance(mm.cost_pct("CNC"), float)


# ----------------------------------------------------------------------------- verdict metrics
def test_verdict_reports_every_metric():
    rows = trades(40, gross=3.0, net=2.7, second_half_net=-1.0)
    v = verdict(rows, [0.0] * 1000, 5, 0.3)
    assert v["n"] == 40
    assert v["mean_net"] == pytest.approx(0.85) and v["median_net"] == pytest.approx(0.85)
    assert v["half1_mean"] == pytest.approx(2.7) and v["half2_mean"] == pytest.approx(-1.0)
    assert v["drop_top3_mean"] == pytest.approx((17 * 2.7 - 20 * 1.0) / 37)
    assert v["perm_p"] == 0.0
    assert 0.0 <= v["fold_pass"] <= 1.0
    assert isinstance(v["margin_floor_promotable"], bool)


def test_verdict_halves_split_by_entry_date_not_input_order():
    rows = trades(40, gross=3.0, net=2.7, second_half_net=-1.0)
    v = verdict(list(reversed(rows)), [0.0] * 1000, 5, 0.3)
    assert v["half1_mean"] == pytest.approx(2.7) and v["half2_mean"] == pytest.approx(-1.0)


def test_verdict_odd_n_puts_the_extra_trade_in_the_second_half():
    rows = trades(31, net=1.0)
    rows = [TradeRow(t.signal_date, t.entry_date, t.exit_date, t.gross, float(i)) for i, t in enumerate(rows)]
    v = verdict(rows, [0.0] * 1000, 5, 0.3)
    assert v["half1_mean"] == pytest.approx(sum(range(15)) / 15)
    assert v["half2_mean"] == pytest.approx(sum(range(15, 31)) / 16)


def test_verdict_permutation_failure_blocks_supported():
    pop = list(np.linspace(-1.0, 5.0, 1000))                       # median 2.0: a 3.0 sample median is common
    v = verdict(trades(40, gross=2.1, net=1.8), pop, 5, 0.3)
    assert v["perm_p"] >= mm.ALPHA
    assert v["label"] == "null"


def test_verdict_drop_top3_failure_blocks_supported():
    rows = trades(40, gross=3.0, net=-0.05)
    rows[:3] = [TradeRow(t.signal_date, t.entry_date, t.exit_date, 30.0, 30.0) for t in rows[:3]]
    rows[20:23] = [TradeRow(t.signal_date, t.entry_date, t.exit_date, 30.0, 30.0) for t in rows[20:23]]
    v = verdict(rows, [0.0] * 1000, 5, 0.3)
    assert v["mean_net"] > 0 and v["drop_top3_mean"] > 0
    rows2 = trades(40, gross=3.0, net=-0.05)
    rows2[:2] = [TradeRow(t.signal_date, t.entry_date, t.exit_date, 30.0, 30.0) for t in rows2[:2]]
    rows2[20] = TradeRow(rows2[20].signal_date, rows2[20].entry_date, rows2[20].exit_date, 30.0, 30.0)
    v2 = verdict(rows2, [0.0] * 1000, 5, 0.3)
    assert v2["mean_net"] > 0 and v2["drop_top3_mean"] < 0
    assert v2["label"] == "null"


def test_verdict_refuses_non_finite_returns():
    # a NaN population median never compares >= the sample's, so the p-value would read a spurious 0
    with pytest.raises(ValueError, match="non-finite"):
        verdict(trades(40, gross=3.0, net=2.7), [0.0] * 999 + [float("nan")], 5, 0.3)
    bad = trades(40, gross=3.0, net=2.7)
    bad[5] = TradeRow(bad[5].signal_date, bad[5].entry_date, bad[5].exit_date, 3.0, float("nan"))
    with pytest.raises(ValueError, match="non-finite"):
        verdict(bad, [0.0] * 1000, 5, 0.3)


def test_non_overlapping_rejects_unsorted_sessions():
    s = sessions(5)
    with pytest.raises(ValueError):
        non_overlapping([s[0]], [s[1], s[0], s[2]], hold=1)


def test_verdict_empty_is_insufficient_with_none_metrics():
    v = verdict([], [0.0] * 10, 5, 0.3)
    assert v["label"] == "insufficient" and v["n"] == 0
    assert v["mean_net"] is None and v["perm_p"] is None and v["fold_pass"] is None


# ----------------------------------------------------------------------------- liquid_universe
def _db(rows: list[tuple], ex: list[tuple] = ()) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    conn.execute('CREATE TABLE bars_1d (symbol TEXT NOT NULL, d DATE NOT NULL, "open" DECIMAL(12,2) NOT NULL, '
                 'high DECIMAL(12,2) NOT NULL, low DECIMAL(12,2) NOT NULL, "close" DECIMAL(12,2) NOT NULL, '
                 "volume BIGINT NOT NULL, src TEXT NOT NULL DEFAULT 'kite_official', PRIMARY KEY (symbol, d))")
    conn.execute("CREATE TABLE corp_actions (symbol TEXT NOT NULL, ex_date DATE NOT NULL, kind TEXT NOT NULL, "
                 "ratio TEXT, amount DECIMAL(12,2), source TEXT, recorded_at TIMESTAMPTZ, "
                 "PRIMARY KEY (symbol, ex_date, kind))")
    if rows:
        conn.executemany('INSERT INTO bars_1d (symbol, d, "open", high, low, "close", volume) '
                         "VALUES (?, ?, ?, ?, ?, ?, ?)", rows)
    if ex:
        conn.executemany("INSERT INTO corp_actions (symbol, ex_date, kind) VALUES (?, ?, ?)", ex)
    return conn


def _bars(sym: str, days: list[date], close: float, volume: int) -> list[tuple]:
    return [(sym, d, close, close, close, close, volume) for d in days]


def test_liquid_universe_window_threshold_etf_and_ex_date():
    s = sessions(25)
    rows = (_bars("LIQ", s, 100.0, 1_000_000)                     # 1e8 exactly: liquid
            + _bars("THIN", s, 100.0, 999_999)                    # just under
            + _bars("YOUNG", s[-19:], 1000.0, 1_000_000)          # 19 sessions to D-1: not liquid
            + _bars("NIFTYBEES", s, 100.0, 5_000_000)             # ETF by suffix
            + _bars("LISTETF", s, 100.0, 5_000_000)               # ETF by list (and suffix)
            + _bars("ONLIST", s, 100.0, 5_000_000)                # ETF by list only
            + _bars("SPLIT", s, 100.0, 5_000_000)                 # ex-date on D-1
            + _bars("NIFTY 50", s, 100.0, 5_000_000)              # an index row, not a stock
            + _bars("GONE", s[:-1], 100.0, 5_000_000))            # no bar on D-1
    conn = _db(rows, ex=[("SPLIT", s[-1], "split"), ("LIQ", s[-2], "dividend")])
    got = mm.liquid_universe(conn, s[-1], frozenset({"ONLIST"}))
    assert got == {"LIQ"}


def test_liquid_universe_uses_only_sessions_to_d_prev():
    s = sessions(40)
    rows = _bars("A", s[:20], 100.0, 1_000_000) + _bars("A", s[20:], 100.0, 1)   # turnover collapses after s[19]
    conn = _db(rows)
    assert mm.liquid_universe(conn, s[19], frozenset()) == {"A"}
    assert mm.liquid_universe(conn, s[20], frozenset()) == {"A"}               # median of 19 x 1e8 and 1 x 100
    assert mm.liquid_universe(conn, s[30], frozenset()) == set()


def test_liquid_universe_cached_per_connection():
    s = sessions(20)
    c1, c2 = _db(_bars("A", s, 100.0, 1_000_000)), _db(_bars("B", s, 100.0, 1_000_000))
    assert mm.liquid_universe(c1, s[-1], frozenset()) == {"A"}
    assert mm.liquid_universe(c2, s[-1], frozenset()) == {"B"}
    c1.execute("DELETE FROM bars_1d")
    assert mm.liquid_universe(c1, s[-1], frozenset()) == {"A"}                  # computed once per connection


def test_liquid_universe_returns_a_fresh_set():
    s = sessions(20)
    conn = _db(_bars("A", s, 100.0, 1_000_000))
    mm.liquid_universe(conn, s[-1], frozenset()).add("X")
    assert mm.liquid_universe(conn, s[-1], frozenset()) == {"A"}


def test_liquid_universe_missing_corp_actions_refuses():
    s = sessions(20)
    conn = duckdb.connect()
    conn.execute('CREATE TABLE bars_1d (symbol TEXT, d DATE, "close" DECIMAL(12,2), volume BIGINT)')
    conn.executemany("INSERT INTO bars_1d VALUES (?, ?, ?, ?)", [("A", d, 100.0, 1_000_000) for d in s])
    with pytest.raises(mm.Refusal):
        mm.liquid_universe(conn, s[-1], frozenset())


@pytest.mark.skipif(not mm.ETF_LIST.exists(), reason="the NSE ETF list is a local research artefact")
def test_etf_list_loads_the_registered_nse_file():
    etfs = mm.load_etf_symbols()
    assert "NIFTYBEES" in etfs
    assert len(etfs) > 50
