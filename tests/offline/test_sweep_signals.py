"""Sweep signal-builder correctness (pure pandas/numpy — no vectorbt).

The per-strategy signal builders are testable without the heavy backtest stack. Pins:

* the ``orb`` forced session-end square-off: a breakout open at the 14:30 entry-window end rides to the
  day's last bar, not truncated at 14:30;
* the range-anchored stop/target (risk = ``stop_range_frac × (entry − range_low)``, stamped as per-signal
  ``sl_stop``/``tp_stop`` fractions) and the cost floor skipping breakouts with risk/price < 4 × per-side fee;
* the ``rsi2`` ``max_hold_days`` scheduled time-exit OR-ed onto the RSI-exit;
* the next-bar-open shift (``_shift_to_next_bar`` / ``_shift_stop_frame``), including the session-aware
  drop that stops an intraday signal leaking into the next session;
* the ``mom`` ranking window excludes the traded session.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from engine.core.clock import IST
from engine.learning.sweep import (
    _Frames,
    _session_codes,
    _shift_stop_frame,
    _shift_to_next_bar,
    _signals_mom,
    _signals_orb,
    _signals_rsi2,
)

_SYM = "ZZ"
_DAY = "2024-06-03"                     # a Monday; a full 09:15–15:29 IST session
_BREAK_POS = 45                         # 09:15 + 45 min = 10:00 — well inside the 09:30–14:30 window


def _one_col(values: np.ndarray, index: pd.DatetimeIndex) -> pd.DataFrame:
    return pd.DataFrame({_SYM: values}, index=index)


def _breakout_session() -> _Frames:
    """One symbol, one full session: a 30-min opening range topping at 100.5, then a single
    volume-confirmed upside breakout to 102.0 at 10:00 that never revisits its stop/target."""
    idx = pd.date_range(f"{_DAY} 09:15", f"{_DAY} 15:29", freq="1min", tz=IST)
    n = len(idx)
    close = np.full(n, 100.0)          # flat everywhere: opening-range high = 100.0 + 0.5 = 100.5
    volume = np.full(n, 1000.0)
    close[_BREAK_POS] = 102.0          # breakout close clears the 100.5 range high
    volume[_BREAK_POS] = 5000.0        # 5.0× the 20-bar median 1000 ≥ vol_mult (1.5)
    high = close + 0.5
    low = close - 0.5
    return _Frames(
        close=_one_col(close, idx),
        high=_one_col(high, idx),
        low=_one_col(low, idx),
        open=_one_col(close.copy(), idx),
        volume=_one_col(volume, idx),
        intraday=True,
        auction_open=None,
    )


def test_orb_squares_off_at_session_end_not_entry_window_end():
    frames = _breakout_session()
    params = {"orb_minutes": 30.0, "vol_mult": 1.5, "stop_range_frac": 1.0, "rr_target": 1.5}

    sig = _signals_orb(frames, params)
    entries, exits = sig.entries[_SYM], sig.exits[_SYM]

    idx = frames.close.index
    ts_break = idx[_BREAK_POS]                                  # 10:00 entry
    ts_last = idx[-1]                                           # 15:29 session end
    ts_window_end = pd.Timestamp(f"{_DAY} 14:30", tz=IST)       # 14:30 entry-window end

    # exactly one entry, at the breakout bar
    assert int(entries.sum()) == 1
    assert bool(entries.loc[ts_break]) is True

    # the square-off lands on the session's LAST bar, never on the 14:30 entry-window end
    assert int(exits.sum()) == 1
    assert bool(exits.loc[ts_last]) is True
    assert bool(exits.loc[ts_window_end]) is False
    assert ts_last > ts_window_end                             # the session really does extend past 14:30

    # Stop/target anchor at the opposite opening-range edge (low 99.5), stamped at the entry bar:
    # risk = 1.0 × (102.0 − 99.5) = 2.5, so sl_stop = 2.5/102.0 and tp_stop = 1.5 × risk/price.
    assert sig.sl_stop is not None and sig.tp_stop is not None
    assert float(sig.sl_stop[_SYM].loc[ts_break]) == pytest.approx(2.5 / 102.0)
    assert float(sig.tp_stop[_SYM].loc[ts_break]) == pytest.approx(1.5 * 2.5 / 102.0)


def test_orb_cost_floor_skips_sub_breakeven_breakout():
    # A breakout is skipped when risk/price < 4 × per-side fee. Here risk/price = 2.5/102.0 ≈ 0.0245 and
    # the floor is 4 × 0.01 = 0.04, so there is no entry and no square-off.
    frames = _breakout_session()
    params = {"orb_minutes": 30.0, "vol_mult": 1.5, "stop_range_frac": 1.0, "rr_target": 1.5}
    sig = _signals_orb(frames, params, fee=0.01)
    assert int(sig.entries[_SYM].sum()) == 0
    assert int(sig.exits[_SYM].sum()) == 0


def test_orb_cost_floor_boundary_passes():
    # fee=0.005 ⇒ floor 0.02 < 0.0245 = risk/price, so the same breakout clears it.
    frames = _breakout_session()
    params = {"orb_minutes": 30.0, "vol_mult": 1.5, "stop_range_frac": 1.0, "rr_target": 1.5}
    sig = _signals_orb(frames, params, fee=0.005)
    entries = sig.entries[_SYM]
    assert int(entries.sum()) == 1
    assert bool(entries.loc[frames.close.index[_BREAK_POS]]) is True


def _rsi2_shock_frames() -> _Frames:
    """One symbol, ~260 daily bars: 255 gently rising days (RSI(2) ≈ 100, close well above SMA200), two
    consecutive −2% shocks that drive RSI(2) below the 10 entry threshold, then a flat tail (a zero-delta
    day halves avg-gain and avg-loss, so the sub-10 RSI holds). No index_closes ⇒ regime disabled."""
    n = 260
    idx = pd.bdate_range("2024-01-01", periods=n)          # naive daily DatetimeIndex — regime off
    close = np.empty(n, dtype="float64")
    for i in range(255):
        close[i] = 100.0 + 0.1 * i
    close[255] = close[254] * 0.98
    close[256] = close[255] * 0.98
    close[257:] = close[256]                               # flat tail
    high = close + 0.5
    low = close - 0.5
    return _Frames(
        close=_one_col(close, idx),
        high=_one_col(high, idx),
        low=_one_col(low, idx),
        open=_one_col(close.copy(), idx),
        volume=_one_col(np.full(n, 1000.0), idx),
        intraday=False,
        auction_open=None,
    )


def test_rsi2_max_hold_days_schedules_time_exit():
    frames = _rsi2_shock_frames()
    n = len(frames.close)
    base = {"rsi_entry": 10.0, "rsi_exit": 100.0, "stop_pct": 4.0}

    sig = _signals_rsi2(frames, {**base, "max_hold_days": 3.0})
    entry_positions = np.flatnonzero(sig.entries[_SYM].to_numpy())
    exits = sig.exits[_SYM].to_numpy()
    assert entry_positions.size >= 1                       # the oversold shocks fire RSI(2) < 10
    for p in entry_positions:                              # every entry signal schedules a time-exit
        if p + 3 < n:                                      # max_hold_days sessions later (clipped)
            assert bool(exits[p + 3]) is True

    # max_hold_days = 0 ⇒ no scheduled time-exit; rsi_exit=100 never fires, so exits are empty. Reusing
    # the frames is safe: frames.cache["rsi2"] holds only the RSI(2)/SMA(200) series max_hold does not touch.
    sig0 = _signals_rsi2(frames, {**base, "max_hold_days": 0.0})
    assert int(sig0.exits[_SYM].sum()) == 0
    assert int(sig0.entries[_SYM].sum()) == entry_positions.size   # entries unchanged (cache reused)


# --------------------------------------------------------------------------- next-bar-open shift
def test_shift_to_next_bar_moves_signals_forward_and_drops_the_last_one():
    """A signal on bar t acts on bar t+1; a signal on the last bar has no bar to fill into and is dropped, not filled at its own bar."""
    idx = pd.bdate_range("2024-01-01", periods=5)
    sig = pd.DataFrame({_SYM: [False, True, False, False, True]}, index=idx)

    out = _shift_to_next_bar(sig, session_codes=None)

    assert list(out[_SYM]) == [False, False, True, False, False]
    assert out.dtypes.iloc[0] is np.dtype(bool)      # still boolean — vectorbt needs a bool mask
    assert int(sig[_SYM].sum()) == 2 and int(out[_SYM].sum()) == 1   # the last-bar signal is dropped


def test_shift_to_next_bar_never_crosses_an_intraday_session_boundary():
    """Intraday (orb): a signal on a session's last bar must not fill on the next session's first bar."""
    idx = pd.DatetimeIndex(
        [
            pd.Timestamp("2024-06-03 15:28", tz=IST), pd.Timestamp("2024-06-03 15:29", tz=IST),
            pd.Timestamp("2024-06-04 09:15", tz=IST), pd.Timestamp("2024-06-04 09:16", tz=IST),
        ]
    )
    sig = pd.DataFrame({_SYM: [True, True, False, False]}, index=idx)
    codes = _session_codes(idx)
    assert list(codes) == [0, 0, 1, 1]

    out = _shift_to_next_bar(sig, session_codes=codes)

    assert list(out[_SYM]) == [False, True, False, False]   # 15:28 -> 15:29; 15:29 -> dropped


def test_shift_stop_frame_travels_with_its_entry_and_passes_scalars_through():
    """Per-signal sl/tp fractions are stamped on the signal bar, so they move with it."""
    idx = pd.bdate_range("2024-01-01", periods=4)
    stops = pd.DataFrame({_SYM: [np.nan, 0.025, np.nan, np.nan]}, index=idx)

    out = _shift_stop_frame(stops, session_codes=None)

    assert np.isnan(out[_SYM].iloc[1])
    assert float(out[_SYM].iloc[2]) == pytest.approx(0.025)     # same row as the shifted entry
    assert _shift_stop_frame(0.04, session_codes=None) == 0.04  # rsi2's constant stop_pct
    assert _shift_stop_frame(None, session_codes=None) is None


def _mom_frames() -> _Frames:
    """Two symbols over 24 daily bars: AAA leads on momentum through t=22, then a −20% close on t=23
    flips the ranking, so with top_n=1 the winner depends on whether the window includes session 23."""
    n = 24
    idx = pd.bdate_range("2024-01-01", periods=n)
    aaa = np.array([100.0 + 2.0 * i for i in range(n)])      # steady strong uptrend
    bbb = np.array([100.0 + 0.5 * i for i in range(n)])      # weaker uptrend
    aaa[-1] = aaa[-2] * 0.80                                 # AAA collapses on the LAST session
    close = pd.DataFrame({"AAA": aaa, "BBB": bbb}, index=idx)
    return _Frames(
        close=close,
        high=close * 1.01,
        low=close * 0.99,
        open=close.copy(),
        volume=pd.DataFrame(1000.0, index=idx, columns=close.columns),
        intraday=False,
        auction_open=None,
    )


def test_mom_ranking_window_excludes_the_traded_session():
    """The rank that fills on session t+1 is computed from closes through t, matching the live 'through d−1' window.

    The builder stamps the rebalance on row t and ``SweepRunner._portfolio`` shifts it to t+1. AAA's crash
    on row 23 must not influence the order filling on row 23, and flips only the order for the
    nonexistent row 24 (dropped)."""
    frames = _mom_frames()
    close = frames.close
    sig = _signals_mom(frames, {"top_n": 1.0, "rebalance_days": 1.0})

    mom = close / close.shift(20) - 1.0
    # Signal rows are pre-shift: signal row t fills at t+1's open.
    signal_row_for_last_fill = len(close) - 2               # fills on the final row
    assert bool(sig.entries["AAA"].iloc[signal_row_for_last_fill]) is True   # ranked on closes <= t
    assert bool(sig.entries["BBB"].iloc[signal_row_for_last_fill]) is False
    # The crash session (row 23) ranks BBB first, but that can only fill on a nonexistent row 24.
    assert float(mom["AAA"].iloc[-1]) < float(mom["BBB"].iloc[-1])
    assert bool(sig.entries["BBB"].iloc[-1]) is True
    shifted = _shift_to_next_bar(sig.entries, session_codes=None)
    assert bool(shifted["BBB"].iloc[-1]) is False           # nothing from the crash bar ever fills
    assert bool(shifted["AAA"].iloc[-1]) is True            # the pre-crash rank is what trades
