"""Pure deterministic indicator functions (§6.1/§6.2) — shared by live scanners AND backtests.

Design contract (load-bearing for §9.6 determinism and the Phase-1 vectorbt sweep):

* **Pure functions, column-wise.** Every function maps input series → output ``pandas.Series``
  aligned 1:1 with the input (same index, same length; warm-up positions are ``NaN``). No state, no
  Clock, no I/O. The vectorbt parameter sweep wraps these exact functions per column
  (``frame.apply(lambda col: wilder_rsi(col, n))``), so live and backtest math cannot diverge.
* **Dual use.** Incremental (live): pass the last-N bars' values and read ``.iloc[-1]``; because
  every recursion here is seeded at a fixed position from the start of the input (never "wherever
  the stream happens to begin"), a scanner passing the same window always gets the same value.
  Vectorized (research): pass the full history once.
* **Floats, deliberately.** Indicators are statistics, not money — inputs are coerced to
  ``float64`` (Decimals via ``str``-free direct ``float()``: these feed comparisons and multipliers,
  never a ledger). Scanners convert final PRICE LEVELS back to exact ``Decimal`` with
  ``strategy.types.round_to_tick`` — no float ever becomes a persisted price (§3.2).

Smoothing conventions (documented so hand-computed test vectors are unambiguous):

* **Wilder smoothing** (RSI/ATR/ADX): seed = simple average of the first ``period`` raw values,
  then ``s[i] = (s[i-1]*(period-1) + x[i]) / period`` — Wilder's original recursion, NOT pandas
  ``ewm``. RSI edge cases: all-flat window (no gains, no losses) ⇒ 50.0 (neutral); losses zero with
  gains present ⇒ 100.0.
* **EMA**: ``alpha = 2/(period+1)``, seeded at the first value (``ema[0] = x[0]``) — identical to
  ``Series.ewm(span=period, adjust=False)``. Callers wanting TA-textbook SMA-seeded values must
  supply ≥ ~3×period warm-up bars (the §7.1 ``warmup_ready`` gate guarantees this live).
* **ADX**: ±DM/TR are Wilder-smoothed with seed = mean of elements 1..period (the first delta-based
  values) landing at index ``period``; DX exists from index ``period``; ADX seed = mean of
  DX[period .. 2*period-1] landing at index ``2*period-1``. First ADX therefore needs
  ``2*period`` input rows.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import numpy as np
import pandas as pd

__all__ = [
    "as_float_series",
    "sma",
    "ema",
    "wilder_rsi",
    "wilder_atr",
    "wilder_dmi",
    "wilder_adx",
    "rolling_median_volume",
    "momentum",
    "cross_sectional_rank",
    "vwap",
]


def as_float_series(values: Sequence[Any] | pd.Series) -> pd.Series:
    """Coerce a sequence of numbers (incl. ``Decimal``) or a Series to ``float64``."""
    if isinstance(values, pd.Series):
        return values.astype("float64")
    return pd.Series([float(v) for v in values], dtype="float64")


def _empty_like(s: pd.Series) -> pd.Series:
    return pd.Series(np.full(len(s), np.nan), index=s.index, dtype="float64")


# --------------------------------------------------------------------------- moving averages
def sma(values: Sequence[Any] | pd.Series, period: int) -> pd.Series:
    """Simple moving average; NaN until a full ``period`` window exists (the 200-DMA / 50-DMA)."""
    s = as_float_series(values)
    return s.rolling(period, min_periods=period).mean()


def ema(values: Sequence[Any] | pd.Series, period: int) -> pd.Series:
    """Exponential moving average, ``alpha = 2/(period+1)``, seeded at the first value."""
    s = as_float_series(values)
    return s.ewm(span=period, adjust=False).mean()


# --------------------------------------------------------------------------- Wilder RSI
def _rsi_value(avg_gain: float, avg_loss: float) -> float:
    if avg_loss == 0.0:
        return 50.0 if avg_gain == 0.0 else 100.0
    return 100.0 - 100.0 / (1.0 + avg_gain / avg_loss)


def wilder_rsi(closes: Sequence[Any] | pd.Series, period: int) -> pd.Series:
    """Wilder RSI (SMA-seeded, Wilder-smoothed). First value at index ``period``."""
    s = as_float_series(closes)
    out = _empty_like(s)
    x = s.to_numpy()
    n = len(x)
    if n <= period:
        return out
    delta = np.diff(x)
    gains = np.clip(delta, 0.0, None)
    losses = np.clip(-delta, 0.0, None)
    avg_gain = float(gains[:period].mean())
    avg_loss = float(losses[:period].mean())
    vals = out.to_numpy()
    vals[period] = _rsi_value(avg_gain, avg_loss)
    for i in range(period + 1, n):
        avg_gain = (avg_gain * (period - 1) + gains[i - 1]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i - 1]) / period
        vals[i] = _rsi_value(avg_gain, avg_loss)
    return pd.Series(vals, index=s.index, dtype="float64")


# --------------------------------------------------------------------------- Wilder ATR
def _true_range(high: np.ndarray, low: np.ndarray, close: np.ndarray) -> np.ndarray:
    """TR per bar; the first bar (no previous close) uses plain high−low."""
    tr = high - low
    if len(tr) > 1:
        prev_close = close[:-1]
        tr = np.concatenate((
            tr[:1],
            np.maximum.reduce([
                high[1:] - low[1:],
                np.abs(high[1:] - prev_close),
                np.abs(low[1:] - prev_close),
            ]),
        ))
    return tr


def wilder_atr(
    high: Sequence[Any] | pd.Series,
    low: Sequence[Any] | pd.Series,
    close: Sequence[Any] | pd.Series,
    period: int = 14,
) -> pd.Series:
    """Wilder ATR (SMA seed over the first ``period`` TRs, then Wilder recursion).

    Timeframe-agnostic — pass 1m bars for ATR(14,1m) (orb stops) or daily bars for ATR(14,1d)
    (trend trail / cat stops). First value at index ``period − 1``.
    """
    h, lo, c = as_float_series(high), as_float_series(low), as_float_series(close)
    out = _empty_like(c)
    n = len(c)
    if n < period:
        return out
    tr = _true_range(h.to_numpy(), lo.to_numpy(), c.to_numpy())
    vals = out.to_numpy()
    vals[period - 1] = float(tr[:period].mean())
    for i in range(period, n):
        vals[i] = (vals[i - 1] * (period - 1) + tr[i]) / period
    return pd.Series(vals, index=c.index, dtype="float64")


# --------------------------------------------------------------------------- Wilder ADX / DMI
def wilder_dmi(
    high: Sequence[Any] | pd.Series,
    low: Sequence[Any] | pd.Series,
    close: Sequence[Any] | pd.Series,
    period: int = 14,
) -> pd.DataFrame:
    """Wilder directional system: columns ``plus_di``, ``minus_di``, ``dx``, ``adx``.

    ±DI from index ``period``; ADX from index ``2*period − 1`` (see module docstring for the exact
    seeding convention the tests hand-compute against).
    """
    h, lo, c = as_float_series(high), as_float_series(low), as_float_series(close)
    n = len(c)
    nan_col = np.full(n, np.nan)
    frame = pd.DataFrame(
        {"plus_di": nan_col.copy(), "minus_di": nan_col.copy(), "dx": nan_col.copy(), "adx": nan_col.copy()},
        index=c.index,
    )
    if n <= period:
        return frame
    ha, la = h.to_numpy(), lo.to_numpy()
    up = ha[1:] - ha[:-1]
    down = la[:-1] - la[1:]
    pdm = np.where((up > down) & (up > 0.0), up, 0.0)          # aligned to bars 1..n-1
    mdm = np.where((down > up) & (down > 0.0), down, 0.0)
    tr = _true_range(ha, la, c.to_numpy())[1:]                 # deltas only, aligned with pdm/mdm

    def _smooth(raw: np.ndarray) -> np.ndarray:
        """Wilder-average smoothing of a delta-aligned array; result aligned to bars, from ``period``."""
        sm = np.full(n, np.nan)
        sm[period] = float(raw[:period].mean())
        for i in range(period + 1, n):
            sm[i] = (sm[i - 1] * (period - 1) + raw[i - 1]) / period
        return sm

    sm_tr, sm_pdm, sm_mdm = _smooth(tr), _smooth(pdm), _smooth(mdm)
    with np.errstate(invalid="ignore", divide="ignore"):
        plus_di = np.where(sm_tr > 0.0, 100.0 * sm_pdm / sm_tr, 0.0)
        minus_di = np.where(sm_tr > 0.0, 100.0 * sm_mdm / sm_tr, 0.0)
        di_sum = plus_di + minus_di
        dx = np.where(di_sum > 0.0, 100.0 * np.abs(plus_di - minus_di) / di_sum, 0.0)
    plus_di[:period] = np.nan
    minus_di[:period] = np.nan
    dx[:period] = np.nan

    adx = np.full(n, np.nan)
    first = 2 * period - 1
    if n > first:
        adx[first] = float(np.nanmean(dx[period : first + 1]))
        for i in range(first + 1, n):
            adx[i] = (adx[i - 1] * (period - 1) + dx[i]) / period

    frame["plus_di"], frame["minus_di"], frame["dx"], frame["adx"] = plus_di, minus_di, dx, adx
    return frame


def wilder_adx(
    high: Sequence[Any] | pd.Series,
    low: Sequence[Any] | pd.Series,
    close: Sequence[Any] | pd.Series,
    period: int = 14,
) -> pd.Series:
    """The ADX line of :func:`wilder_dmi` (trend-strength filter, §6.1 ``trend``)."""
    return wilder_dmi(high, low, close, period)["adx"]


# --------------------------------------------------------------------------- volume / momentum
def rolling_median_volume(volumes: Sequence[Any] | pd.Series, window: int = 20) -> pd.Series:
    """Rolling median volume over ``window`` bars; NaN until a full window exists (orb vol filter)."""
    s = as_float_series(volumes)
    return s.rolling(window, min_periods=window).median()


def momentum(
    closes: Sequence[Any] | pd.Series, weeks: int = 4, sessions_per_week: int = 5
) -> pd.Series:
    """N-week price momentum: fractional return vs ``weeks × sessions_per_week`` sessions ago.

    §6.1 ``mom`` uses 4 weeks = 20 trading sessions. NaN until the lookback exists.
    """
    s = as_float_series(closes)
    k = weeks * sessions_per_week
    return s / s.shift(k) - 1.0


def cross_sectional_rank(values: Mapping[str, float]) -> dict[str, int]:
    """Rank symbols by value DESCENDING: rank 1 = strongest. Deterministic (§9.6).

    NaN values are excluded (no rank ⇒ the symbol simply cannot make ``top_n``). Ties are broken
    alphabetically by symbol and ranks stay strictly sequential 1..n, so the same inputs always
    produce the same ranking — no dict-order or float-noise dependence.
    """
    clean = [(sym, float(v)) for sym, v in values.items() if not np.isnan(float(v))]
    ordered = sorted(clean, key=lambda kv: (-kv[1], kv[0]))
    return {sym: i + 1 for i, (sym, _) in enumerate(ordered)}


# --------------------------------------------------------------------------- VWAP
def vwap(
    high: Sequence[Any] | pd.Series,
    low: Sequence[Any] | pd.Series,
    close: Sequence[Any] | pd.Series,
    volume: Sequence[Any] | pd.Series,
) -> pd.Series:
    """Session VWAP from typical price ``(H+L+C)/3``, cumulative from the FIRST input bar.

    Callers pass exactly one session's bars (the caller owns session slicing — this function has no
    calendar). Positions with zero cumulative volume are NaN.
    """
    h, lo, c, v = (as_float_series(x) for x in (high, low, close, volume))
    typical = (h + lo + c) / 3.0
    cum_v = v.cumsum()
    cum_pv = (typical * v).cumsum()
    return (cum_pv / cum_v.where(cum_v > 0.0)).astype("float64")
