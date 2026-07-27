"""Hand-computed reference values for ``engine.strategy.indicators`` (§6.1/§6.2, §9.6).

Every expected number below is derived BY HAND from the smoothing conventions pinned in the
indicators module docstring (Wilder SMA-seeded recursions; EMA ``adjust=False`` seeded at the first
value) so live scanners and the vectorbt sweep verify against the same arithmetic.
"""

from __future__ import annotations

from decimal import Decimal

import numpy as np
import pandas as pd
import pytest

from engine.strategy.indicators import (
    as_float_series,
    cross_sectional_rank,
    ema,
    momentum,
    rolling_median_volume,
    sma,
    vwap,
    wilder_adx,
    wilder_atr,
    wilder_dmi,
    wilder_rsi,
)


# --------------------------------------------------------------------------- coercion
def test_as_float_series_accepts_decimals_and_series():
    s = as_float_series([Decimal("1.5"), 2, 2.5])
    assert s.dtype == "float64"
    assert list(s) == [1.5, 2.0, 2.5]
    s2 = as_float_series(pd.Series([1, 2]))
    assert s2.dtype == "float64"


# --------------------------------------------------------------------------- SMA / EMA
def test_sma_hand_computed():
    out = sma([2, 4, 8], 2)
    assert np.isnan(out.iloc[0])
    assert out.iloc[1] == pytest.approx(3.0)
    assert out.iloc[2] == pytest.approx(6.0)


def test_ema_hand_computed():
    # span=3 => alpha=1/2, seeded at the first value: 2, 3, 5.5
    out = ema([2, 4, 8], 3)
    assert out.iloc[0] == pytest.approx(2.0)
    assert out.iloc[1] == pytest.approx(3.0)
    assert out.iloc[2] == pytest.approx(5.5)


# --------------------------------------------------------------------------- Wilder RSI
def test_wilder_rsi_hand_computed():
    # closes 100,101,100,99,98; period 2. Seed at index 2: avg_gain=.5, avg_loss=.5 -> 50.
    # index 3: ag=.25, al=.75 -> 25. index 4: ag=.125, al=.875 -> 12.5.
    out = wilder_rsi([100, 101, 100, 99, 98], 2)
    assert np.isnan(out.iloc[0]) and np.isnan(out.iloc[1])
    assert out.iloc[2] == pytest.approx(50.0)
    assert out.iloc[3] == pytest.approx(25.0)
    assert out.iloc[4] == pytest.approx(12.5)


def test_wilder_rsi_edge_cases():
    # All gains, no losses -> 100; all flat -> neutral 50 (module contract).
    assert wilder_rsi([100, 101, 102, 103], 2).iloc[-1] == pytest.approx(100.0)
    assert wilder_rsi([100, 100, 100], 2).iloc[-1] == pytest.approx(50.0)
    # Too short: all NaN.
    assert wilder_rsi([100, 101], 2).isna().all()


# --------------------------------------------------------------------------- Wilder ATR
def test_wilder_atr_hand_computed():
    # TRs: 2 (first bar H-L), 2, 2, 3, 1 (see values). Seed ATR[2]=(2+2+2)/3=2;
    # ATR[3]=(2*2+3)/3=7/3; ATR[4]=(7/3*2+1)/3=17/9.
    high = [10, 11, 12, 14, 13]
    low = [8, 9, 10, 11, 12]
    close = [9, 10, 11, 13, 12.5]
    out = wilder_atr(high, low, close, 3)
    assert np.isnan(out.iloc[0]) and np.isnan(out.iloc[1])
    assert out.iloc[2] == pytest.approx(2.0)
    assert out.iloc[3] == pytest.approx(7 / 3)
    assert out.iloc[4] == pytest.approx(17 / 9)


def test_wilder_atr_too_short_is_all_nan():
    assert wilder_atr([10, 11], [8, 9], [9, 10], 3).isna().all()


# --------------------------------------------------------------------------- Wilder ADX / DMI
def test_wilder_dmi_pure_uptrend_hand_computed():
    # Bars stepping +1 every day: +DM=1, -DM=0, TR deltas all 1.5 (period 2).
    # +DI=100*1/1.5=66.67 from index 2; DX=100; ADX[3]=mean(DX[2:4])=100.
    high = [10, 11, 12, 13]
    low = [9, 10, 11, 12]
    close = [9.5, 10.5, 11.5, 12.5]
    out = wilder_dmi(high, low, close, 2)
    assert out["plus_di"].iloc[2] == pytest.approx(100 * 1 / 1.5)
    assert out["minus_di"].iloc[2] == pytest.approx(0.0)
    assert out["dx"].iloc[2] == pytest.approx(100.0)
    assert out["adx"].iloc[3] == pytest.approx(100.0)
    assert np.isnan(out["adx"].iloc[2])  # first ADX needs 2*period rows


def test_wilder_dmi_mixed_hand_computed():
    # Hand-worked: sm[2]: tr=1.5, +dm=.5, -dm=.5 -> DI 33.33/33.33 -> DX[2]=0.
    # i=3: tr=1.75, +dm=.75, -dm=.25 -> +DI=42.857, -DI=14.286 -> DX[3]=50; ADX[3]=mean(0,50)=25.
    high = [10, 11, 10.5, 11.5]
    low = [9, 10, 9, 10]
    close = [9.5, 10.5, 9.5, 11]
    out = wilder_dmi(high, low, close, 2)
    assert out["plus_di"].iloc[2] == pytest.approx(100 * 0.5 / 1.5)
    assert out["minus_di"].iloc[2] == pytest.approx(100 * 0.5 / 1.5)
    assert out["dx"].iloc[2] == pytest.approx(0.0)
    assert out["plus_di"].iloc[3] == pytest.approx(100 * 0.75 / 1.75)
    assert out["minus_di"].iloc[3] == pytest.approx(100 * 0.25 / 1.75)
    assert out["dx"].iloc[3] == pytest.approx(50.0)
    assert out["adx"].iloc[3] == pytest.approx(25.0)
    # wilder_adx is exactly the adx column.
    assert wilder_adx(high, low, close, 2).iloc[3] == pytest.approx(25.0)


def test_wilder_dmi_too_short_is_all_nan():
    out = wilder_dmi([10, 11], [9, 10], [9.5, 10.5], 2)
    assert out.isna().all().all()


# --------------------------------------------------------------------------- volume / momentum
def test_rolling_median_volume_hand_computed():
    out = rolling_median_volume([10, 20, 30, 100], 3)
    assert np.isnan(out.iloc[0]) and np.isnan(out.iloc[1])
    assert out.iloc[2] == pytest.approx(20.0)
    assert out.iloc[3] == pytest.approx(30.0)


def test_momentum_hand_computed():
    out = momentum([100, 110, 105, 121], weeks=1, sessions_per_week=2)  # k=2
    assert np.isnan(out.iloc[0]) and np.isnan(out.iloc[1])
    assert out.iloc[2] == pytest.approx(0.05)
    assert out.iloc[3] == pytest.approx(0.10)


def test_momentum_default_is_20_sessions():
    closes = list(range(1, 22))  # 21 values; mom[20] = 21/1 - 1 = 20
    out = momentum(closes)
    assert np.isnan(out.iloc[19])
    assert out.iloc[20] == pytest.approx(20.0)


def test_cross_sectional_rank_deterministic():
    ranks = cross_sectional_rank({"B": 0.3, "A": 0.1, "D": 0.1, "C": float("nan")})
    # Descending by value; alphabetical tie-break; NaN excluded; ranks sequential.
    assert ranks == {"B": 1, "A": 2, "D": 3}
    # Same inputs in a different dict order -> identical result (§9.6).
    assert cross_sectional_rank({"D": 0.1, "C": float("nan"), "A": 0.1, "B": 0.3}) == ranks


# --------------------------------------------------------------------------- VWAP
def test_vwap_hand_computed():
    # Typical prices 9 and 11; volumes 100/300 -> [9, (900+3300)/400 = 10.5].
    out = vwap([10, 12], [8, 10], [9, 11], [100, 300])
    assert out.iloc[0] == pytest.approx(9.0)
    assert out.iloc[1] == pytest.approx(10.5)


def test_vwap_zero_cumulative_volume_is_nan():
    out = vwap([10, 12], [8, 10], [9, 11], [0, 400])
    assert np.isnan(out.iloc[0])
    assert out.iloc[1] == pytest.approx(11.0)  # (11*400)/400


# --------------------------------------------------------------------------- determinism contract
def test_same_window_same_values_and_input_type_invariance():
    closes = [100, 101, 100, 99, 98, 99, 100, 101]
    a = wilder_rsi(closes, 2)
    b = wilder_rsi(pd.Series(closes), 2)
    pd.testing.assert_series_equal(a, b)
    pd.testing.assert_series_equal(wilder_rsi(closes, 2), a)  # rerun => identical (§9.6)
