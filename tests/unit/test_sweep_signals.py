"""§6.1 sweep signal-builder correctness (pure pandas/numpy — no vectorbt).

``engine.learning.sweep`` keeps its module top pure (indicator math + stdlib), so the per-strategy
signal builders are testable without the heavy backtest stack. This file pins:

* the ``orb`` intraday baseline's **forced session-end square-off** — a breakout still open at the
  14:30 entry-window end must ride to the day's LAST bar (the MIS end-of-session square-off, §6.1
  "squared off by window end"), NOT be truncated at 14:30 (truncation biased the §6.1 CPCV reports);
* the §6.1 v2 (2026-07-12) **range-anchored stop/target** — risk = ``stop_range_frac × (entry −
  range_low)`` stamped as per-signal ``sl_stop``/``tp_stop`` fractions — and the **C3 cost floor**
  that skips breakouts whose risk cannot pay a round trip twice (risk/price < 4 × per-side fee);
* the ``rsi2`` **max_hold_days scheduled time-exit** (modelled 2026-07-12) OR-ed onto the RSI-exit;
* the WO-2 (2026-08-13) **next-bar-open shift** — the pure transform ``_shift_to_next_bar`` /
  ``_shift_stop_frame`` that turns "signal on bar t" into "order on bar t+1, filled at its open",
  including the session-aware drop that stops an intraday signal leaking into the next session;
* WO-2 (iv): the ``mom`` ranking window **excludes the traded session** once fills are next-open.
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

    # §6.1 v2: per-signal stop/target anchored at the OPPOSITE opening-range edge (range low 99.5),
    # stamped at the entry bar. On the 102.0 breakout: risk = stop_range_frac(1.0) × (102.0 − 99.5)
    # = 2.5, so sl_stop = risk/price = 2.5/102.0 and tp_stop = rr_target(1.5) × risk/price.
    assert sig.sl_stop is not None and sig.tp_stop is not None
    assert float(sig.sl_stop[_SYM].loc[ts_break]) == pytest.approx(2.5 / 102.0)
    assert float(sig.tp_stop[_SYM].loc[ts_break]) == pytest.approx(1.5 * 2.5 / 102.0)


def test_orb_cost_floor_skips_sub_breakeven_breakout():
    # C3 cost floor (§6.1 v2): a breakout is skipped when risk/price < 4 × per-side fee (= 2×
    # round-trip breakeven). Here risk/price = 2.5/102.0 ≈ 0.0245; with fee=0.01 the floor is
    # 4 × 0.01 = 0.04 > 0.0245, so the single breakout is suppressed — no entry AND no square-off.
    frames = _breakout_session()
    params = {"orb_minutes": 30.0, "vol_mult": 1.5, "stop_range_frac": 1.0, "rr_target": 1.5}
    sig = _signals_orb(frames, params, fee=0.01)
    assert int(sig.entries[_SYM].sum()) == 0
    assert int(sig.exits[_SYM].sum()) == 0


def test_orb_cost_floor_boundary_passes():
    # fee=0.005 ⇒ floor = 4 × 0.005 = 0.02 < 0.0245 = risk/price, so the SAME breakout clears the
    # floor: exactly one entry survives, at the breakout bar.
    frames = _breakout_session()
    params = {"orb_minutes": 30.0, "vol_mult": 1.5, "stop_range_frac": 1.0, "rr_target": 1.5}
    sig = _signals_orb(frames, params, fee=0.005)
    entries = sig.entries[_SYM]
    assert int(entries.sum()) == 1
    assert bool(entries.loc[frames.close.index[_BREAK_POS]]) is True


def _rsi2_shock_frames() -> _Frames:
    """One symbol, ~260 DAILY bars: 255 gently rising days (RSI(2) ≈ 100, close climbs well above its
    SMA200), then two consecutive −2% shocks that slam RSI(2) toward 0 (< the 10 entry threshold),
    then a flat tail (Wilder RSI(2) halves both avg-gain and avg-loss on a zero-delta day, so the
    ratio — and thus the sub-10 RSI — is held across the tail). No index_closes ⇒ regime disabled."""
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

    # max_hold_days = 0 ⇒ NO scheduled time-exit; rsi_exit=100 never fires (RSI ≤ 100 always), so the
    # exit frame is entirely empty. Reusing the SAME frames is safe: frames.cache["rsi2"] holds only
    # the period-FIXED RSI(2)/SMA(200) series (identical across both variants — max_hold does not
    # touch them); max_hold only gates the cheap scheduled-exit step downstream of that cache.
    sig0 = _signals_rsi2(frames, {**base, "max_hold_days": 0.0})
    assert int(sig0.exits[_SYM].sum()) == 0
    assert int(sig0.entries[_SYM].sum()) == entry_positions.size   # entries unchanged (cache reused)


# --------------------------------------------------------------------------- WO-2 next-bar-open shift
def test_shift_to_next_bar_moves_signals_forward_and_drops_the_last_one():
    """A signal computed on bar t must act on bar t+1; a signal on the LAST bar has no bar to fill
    into and is dropped (never silently filled at its own bar, the pre-WO-2 defect)."""
    idx = pd.bdate_range("2024-01-01", periods=5)
    sig = pd.DataFrame({_SYM: [False, True, False, False, True]}, index=idx)

    out = _shift_to_next_bar(sig, session_codes=None)

    assert list(out[_SYM]) == [False, False, True, False, False]
    assert out.dtypes.iloc[0] is np.dtype(bool)      # still boolean — vectorbt needs a bool mask
    assert int(sig[_SYM].sum()) == 2 and int(out[_SYM].sum()) == 1   # the last-bar signal is dropped


def test_shift_to_next_bar_never_crosses_an_intraday_session_boundary():
    """Intraday (orb): a signal on a session's LAST bar must not fill on the next session's first
    bar — the session gap is not a fillable next bar."""
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
    """Per-signal sl/tp FRACTIONS are stamped on the signal bar, so they must move with it."""
    idx = pd.bdate_range("2024-01-01", periods=4)
    stops = pd.DataFrame({_SYM: [np.nan, 0.025, np.nan, np.nan]}, index=idx)

    out = _shift_stop_frame(stops, session_codes=None)

    assert np.isnan(out[_SYM].iloc[1])
    assert float(out[_SYM].iloc[2]) == pytest.approx(0.025)     # same row as the shifted entry
    assert _shift_stop_frame(0.04, session_codes=None) == 0.04  # rsi2's constant stop_pct
    assert _shift_stop_frame(None, session_codes=None) is None


def _mom_frames() -> _Frames:
    """Two symbols over 24 daily bars. AAA leads on 4-week momentum through session t=22, then a
    single −20% close on t=23 flips the ranking. With top_n=1 the winner therefore differs
    depending on whether the ranking window includes session 23 or stops at 22."""
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
    """WO-2 (iv): the rank that FILLS on session t+1 is computed from closes through session t.

    The builder stamps the rebalance on row t and ``SweepRunner._portfolio`` shifts it to t+1, so in
    fill-row space the ranking input is ``momentum.shift(1)`` — the live 'through d−1' window
    (ScanContext builds momentum from bars through d−1; the scanner trades on d). Here AAA's crash
    lands on row 23: it must NOT influence the order that fills on row 23, and it MUST flip the
    order that would fill on row 24 (which does not exist ⇒ dropped)."""
    frames = _mom_frames()
    close = frames.close
    sig = _signals_mom(frames, {"top_n": 1.0, "rebalance_days": 1.0})

    mom = close / close.shift(20) - 1.0
    # signal rows are pre-shift; the row that FILLS at t+1 open is the signal row t.
    signal_row_for_last_fill = len(close) - 2               # fills on the final row
    assert bool(sig.entries["AAA"].iloc[signal_row_for_last_fill]) is True   # ranked on closes <= t
    assert bool(sig.entries["BBB"].iloc[signal_row_for_last_fill]) is False
    # the crash session itself (row 23) does rank BBB first — but that decision can only fill on a
    # row 24 that does not exist, so it is dropped, exactly as a live pre-open rank for d+1 would be.
    assert float(mom["AAA"].iloc[-1]) < float(mom["BBB"].iloc[-1])
    assert bool(sig.entries["BBB"].iloc[-1]) is True
    shifted = _shift_to_next_bar(sig.entries, session_codes=None)
    assert bool(shifted["BBB"].iloc[-1]) is False           # nothing from the crash bar ever fills
    assert bool(shifted["AAA"].iloc[-1]) is True            # the pre-crash rank is what trades
