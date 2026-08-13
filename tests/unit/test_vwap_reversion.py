"""WO-10 / WO-10b VWAP-reversion experiment harness — pinned on SYNTHETIC 1m fixtures only.

Nothing in this file opens ``data/market.duckdb`` (or any store): every test builds its own 1m bars in
memory and injects them through ``VwapReversionExperiment(loader=...)``. The pre-registration is what
is under test — if one of these changes, WO-10 has drifted:

* the **stretch trigger** fires only past the threshold;
* **fills are at the NEXT bar's OPEN, never the signal bar** — pinned so a same-bar fill would produce
  a different, detectably wrong number;
* each rung of the exit ladder: **VWAP touch**, **60-min timeout (exactly 60 minutes)**, **1.5 x
  Wilder ATR(14) on session-local 10-MINUTE bars** — every one filling at the bar AFTER its trigger
  bar. The stop test uses a fixture where the (wrong) 1m-ATR scale and the (pre-registered) 10m-ATR
  scale give DIFFERENT stop levels, and asserts the 10m one, so the wrong timescale is detectable;
* the **10m-ATR warm-up**: bars before it produce NO trade rather than an unstopped one, and the
  first possible entry FILL is 11:36 on a 09:15 session;
* the **10:00-14:00 entry-window boundary** (on the FILL, inclusive both ends);
* a signal on a **session's last usable bar is dropped**, never carried into the next session;
* the **abort path**: a fixture whose unconditioned reversion is negative after costs must produce an
  ABORTED report and never compute stage 2;
* the cost charge is the **full round trip INCLUDING spread**, on every trade.

And, for **WO-10b** (owner-directed 2026-08-14 — prior-session ATR seeding as a MODE, not a fork):

* the **seeding chain**: session 2's first bucket continues session 1's FINAL 10m-ATR value;
* the **overnight gap is excluded** — a fixture with a large gap where including the prev-close term
  would detectably change the ATR proves the first bucket's TR is high−low only;
* **fail-closed on day 1**: a symbol's first session has no prior ATR and keeps WO-10's warm-up;
* **default-off is byte-identical**: the same fixture run both ways produces identical trades,
  identical stage-1 base rate and identical grid stats with the flag off;
* the **split-reporting math** (10:00–11:35 vs 11:36–14:00, by entry-fill time) — reporting only.

FIXTURE CONVENTION. Bars are ``high = close + 0.25`` / ``low = close - 0.25``, so the typical price
``(H+L+C)/3`` is exactly the close (0.25 is binary-exact — no float drift in the hand-computed VWAP).
Baseline bars carry volume 1000 at close 100.0 ⇒ session VWAP = 100.0 exactly. Bars that move price
carry **volume 0** so they do not drag the cumulative VWAP: that pins VWAP at 100.0 for the whole
session and makes every expected number computable by hand.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from engine.core.clock import IST, Clock
from engine.learning.vwap_reversion import (
    ATR_PERIOD,
    ATR_RESAMPLE_MINUTES,
    ATR_WARMUP_MINUTES,
    BANNER,
    EXPERIMENT_ID,
    MORNING_SPLIT_END,
    REFERENCE_NOTIONAL,
    REL_VOL_MAX,
    SPLIT_MIDDAY,
    SPLIT_MORNING,
    STOP_ATR_MULT,
    STRETCH_GRID,
    TIMEOUT_MINUTES,
    ConfigAccumulator,
    ReversionConfig,
    SessionFeatures,
    Trade,
    VwapReversionExperiment,
    compute_session_features,
    cost_floor_pct_for,
    session_atr_10m,
    session_slices,
    simulate_session,
    simulate_symbol,
    split_of,
    write_report,
)
from engine.strategy.indicators import wilder_atr

_DAY = "2026-01-05"                 # a Monday; any date works (this module carries no calendar)
_SYM = "ZZ"
_BASE = 100.0                       # baseline close ⇒ session VWAP = 100.0 exactly
_HALF_RANGE = 0.25                  # high/low offsets: typical price == close, exactly
_BASE_VOL = 1000.0
_SESSION_BARS = 375                 # 09:15 .. 15:29 inclusive
_COST = 0.106                       # a stand-in round-trip floor %; the real one is pinned separately

#: 09:15 + 45 min = 10:00 — the pre-registered window opening. NOT reachable in practice: it is
#: dominated by the 10m-ATR warm-up below (that is a pre-registered consequence, pinned by
#: ``test_10m_atr_warmup_blocks_entries_and_dominates_the_1000_window_opening``).
_WINDOW_OPEN_POS = 45
#: 09:15 + 140 min = 11:35 — the first bar whose 10m ATR(14) exists ⇒ first SIGNAL bar; its fill is
#: 11:36. Bars before it are ineligible (no stop ⇒ no trade).
_ATR_READY_POS = ATR_WARMUP_MINUTES
#: 09:15 + 285 min = 14:00 — the last bar at which an entry may FILL.
_LAST_FILL_POS = 285
#: The standard signal bar for the mechanics fixtures: 12:34, comfortably past the 10m-ATR warm-up
#: and inside the entry window, with room for a 60-min hold before 15:29.
_SIGNAL_POS = 199

_CFG_UNCONDITIONED = ReversionConfig(stretch_min_pct=0.0, rel_vol_max=None)
_CFG_1PCT = ReversionConfig(stretch_min_pct=1.0, rel_vol_max=REL_VOL_MAX)


# --------------------------------------------------------------------------- fixture builders
def _session_frame(
    closes: np.ndarray,
    volumes: np.ndarray,
    *,
    opens: np.ndarray | None = None,
    day: str = _DAY,
) -> pd.DataFrame:
    """One session of 1m bars from close/volume arrays (see the module's FIXTURE CONVENTION)."""
    c = np.asarray(closes, dtype="float64")
    idx = pd.date_range(f"{day} 09:15", periods=len(c), freq="1min", tz=IST)
    return pd.DataFrame(
        {
            "open": c.copy() if opens is None else np.asarray(opens, dtype="float64"),
            "high": c + _HALF_RANGE,
            "low": c - _HALF_RANGE,
            "close": c,
            "volume": np.asarray(volumes, dtype="float64"),
        },
        index=idx,
    )


def _flat_session(n: int = _SESSION_BARS) -> tuple[np.ndarray, np.ndarray]:
    """``(closes, volumes)`` for a session pinned at VWAP 100.0 with no signal anywhere."""
    return np.full(n, _BASE), np.full(n, _BASE_VOL)


def _dip_session(
    *,
    signal_pos: int,
    dip_close: float,
    hold_until: int | None = None,
    hold_close: float | None = None,
    n: int = _SESSION_BARS,
) -> tuple[np.ndarray, np.ndarray]:
    """A session that steps DOWN to ``dip_close`` at ``signal_pos`` (volume 0 ⇒ VWAP stays 100.0).

    ``hold_until``/``hold_close`` keep price parked below VWAP after the dip, which is what lets the
    exit-ladder tests choose which rung fires.
    """
    closes, volumes = _flat_session(n)
    closes[signal_pos] = dip_close
    volumes[signal_pos] = 0.0
    if hold_until is not None:
        closes[signal_pos + 1 : hold_until + 1] = (
            dip_close if hold_close is None else hold_close
        )
        volumes[signal_pos + 1 : hold_until + 1] = 0.0
    return closes, volumes


def _atr10_at(frame: pd.DataFrame, pos: int) -> float:
    """The PRE-REGISTERED stop unit at ``pos``: Wilder ATR(14) on session-local 10-minute bars."""
    feats = compute_session_features(_SYM, frame)
    assert feats is not None
    return float(feats.atr_10m[pos])


def _atr_1m_at(frame: pd.DataFrame, pos: int, period: int = 10) -> float:
    """The REJECTED reading (ATR(10) on 1m bars) — computed only so tests can prove the two differ."""
    return float(wilder_atr(frame["high"], frame["low"], frame["close"], period).to_numpy()[pos])


def _trades(frame: pd.DataFrame, config: ReversionConfig = _CFG_1PCT, cost: float = _COST):
    feats = compute_session_features(_SYM, frame)
    assert feats is not None
    return simulate_session(feats, config, cost_floor_pct=cost)


# --------------------------------------------------------------------------- session VWAP (req. 3)
def test_vwap_is_session_cumulative_typical_price_volume() -> None:
    """VWAP = cum(typical x volume)/cum(volume) from the session's FIRST bar (indicators.vwap reused)."""
    closes = np.array([100.0, 102.0, 98.0, 101.0])
    volumes = np.array([1000.0, 2000.0, 3000.0, 4000.0])
    feats = compute_session_features(_SYM, _session_frame(closes, volumes))
    assert feats is not None

    typical = closes                                  # (H+L+C)/3 == close under the fixture convention
    expected = np.cumsum(typical * volumes) / np.cumsum(volumes)
    assert feats.vwap == pytest.approx(expected)
    # …and it is cumulative-from-first-bar, not bar-local: bar 0's VWAP is bar 0's typical price.
    assert feats.vwap[0] == pytest.approx(100.0)
    assert feats.vwap[-1] == pytest.approx((100e3 + 204e3 + 294e3 + 404e3) / 10_000.0)


def test_session_slices_split_on_ist_calendar_date() -> None:
    a = _session_frame(*_flat_session(10), day="2026-01-05")
    b = _session_frame(*_flat_session(7), day="2026-01-06")
    frame = pd.concat([a, b])
    assert session_slices(pd.DatetimeIndex(frame.index)) == [(0, 10), (10, 17)]


def test_out_of_session_bars_are_clamped_before_any_feature_is_computed() -> None:
    """A 09:07 pre-open row must not enter the session VWAP (design requirement 2: session-aware)."""
    frame = _session_frame(*_flat_session(30))
    stray = frame.iloc[[0]].copy()
    stray.index = pd.DatetimeIndex([pd.Timestamp(f"{_DAY} 09:07", tz=IST)])
    stray.iloc[0, stray.columns.get_loc("close")] = 50.0        # would wreck a VWAP that ingested it
    stray.iloc[0, stray.columns.get_loc("high")] = 50.25
    stray.iloc[0, stray.columns.get_loc("low")] = 49.75

    feats = compute_session_features(_SYM, pd.concat([stray, frame]))
    assert feats is not None
    assert feats.n == 30                                        # the 09:07 row is gone
    assert feats.ts[0] == pd.Timestamp(f"{_DAY} 09:15", tz=IST)
    assert feats.vwap == pytest.approx(np.full(30, _BASE))


# --------------------------------------------------------------------------- the stretch trigger
@pytest.mark.parametrize(
    ("threshold", "expect_trade"),
    [(0.0, True), (1.0, True), (1.4, True), (1.6, False), (2.0, False)],
)
def test_stretch_trigger_fires_only_past_the_threshold(threshold: float, expect_trade: bool) -> None:
    """A 1.5%-below-VWAP close is a signal for every threshold at or below it, and for none above."""
    closes, volumes = _dip_session(signal_pos=_SIGNAL_POS, dip_close=98.5, hold_until=340)
    frame = _session_frame(closes, volumes)
    feats = compute_session_features(_SYM, frame)
    assert feats is not None
    assert feats.stretch_pct[_SIGNAL_POS] == pytest.approx(1.5)    # (100 - 98.5)/100 x 100

    config = ReversionConfig(stretch_min_pct=threshold, rel_vol_max=REL_VOL_MAX)
    trades = _trades(frame, config)
    assert bool(trades) is expect_trade
    if expect_trade:
        assert trades[0].signal_ts == frame.index[_SIGNAL_POS]


def test_bars_at_or_above_vwap_are_never_signals() -> None:
    """Long-only reversion needs price BELOW VWAP; a flat/above session produces nothing, even
    unconditioned (stretch threshold 0.0)."""
    frame = _session_frame(*_flat_session())
    assert _trades(frame, _CFG_UNCONDITIONED) == []


def test_relative_volume_filter_uses_the_house_20_bar_median_and_gates_entries() -> None:
    """rel_vol = bar volume / median(previous 20 bars). A high-volume dip is filtered out at 1.5x;
    the SAME dip is a signal once the filter is off (stage 1). Single-bar dip so the fixture has
    exactly one candidate."""
    closes, volumes = _dip_session(signal_pos=_SIGNAL_POS, dip_close=98.5)
    volumes[_SIGNAL_POS] = 5.0 * _BASE_VOL                      # rel_vol = 5.0x -> filtered
    frame = _session_frame(closes, volumes)
    feats = compute_session_features(_SYM, frame)
    assert feats is not None
    assert feats.rel_vol[_SIGNAL_POS] == pytest.approx(5.0)
    assert feats.stretch_pct[_SIGNAL_POS] > 1.0                 # still past the 1.0% stretch threshold

    assert _trades(frame, _CFG_1PCT) == []                       # filter ON  -> rejected
    assert len(_trades(frame, _CFG_UNCONDITIONED)) == 1          # filter OFF -> the same bar signals


# --------------------------------------------------------------------------- next-bar-open fills (WO-2)
def test_entry_fills_at_next_bar_open_never_the_signal_bar() -> None:
    """THE fill pin: a same-bar fill would produce a different, detectably wrong number."""
    signal_pos = _SIGNAL_POS
    closes, volumes = _dip_session(signal_pos=signal_pos, dip_close=98.5, hold_until=340)
    opens = closes.copy()
    opens[signal_pos + 1] = 98.0             # the FILL price: deliberately != the signal bar's close
    frame = _session_frame(closes, volumes, opens=opens)

    trades = _trades(frame)
    assert trades
    tr = trades[0]
    assert tr.signal_ts == frame.index[signal_pos]
    assert tr.entry_ts == frame.index[signal_pos + 1]            # the NEXT bar…
    assert tr.entry_price == pytest.approx(98.0)                 # …at its OPEN
    assert tr.entry_price != pytest.approx(closes[signal_pos])   # not the signal bar's close

    # A same-bar-close fill (the WO-2 defect) would book a materially different trade: pin the gap so
    # this test cannot pass against a harness that quietly fills on the signal bar.
    wrong_gross = (tr.exit_price - closes[signal_pos]) / closes[signal_pos] * 100.0
    assert abs(wrong_gross - tr.gross_return_pct) > 0.4


def test_exit_fills_at_the_bar_after_its_trigger_bar() -> None:
    """Same rule on the way out: the trigger is observed on bar i, the fill is bar i+1's OPEN."""
    signal_pos, touch_pos = _SIGNAL_POS, _SIGNAL_POS + 5
    closes, volumes = _dip_session(signal_pos=signal_pos, dip_close=98.5, hold_until=touch_pos - 1)
    closes[touch_pos] = 100.5                       # high 100.75 >= VWAP 100 -> the trigger bar
    volumes[touch_pos] = 0.0
    opens = closes.copy()
    opens[touch_pos] = 99.9                         # NOT the fill (that would be a same-bar exit)…
    opens[touch_pos + 1] = 101.0                    # …this is
    frame = _session_frame(closes, volumes, opens=opens)

    trades = _trades(frame)
    assert trades
    tr = trades[0]
    assert tr.exit_reason == "vwap_touch"
    assert tr.exit_ts == frame.index[touch_pos + 1]
    assert tr.exit_price == pytest.approx(101.0)
    assert tr.exit_price != pytest.approx(opens[touch_pos])


# --------------------------------------------------------------------------- the exit ladder
def test_vwap_touch_exit() -> None:
    signal_pos, touch_pos = _SIGNAL_POS, _SIGNAL_POS + 10
    closes, volumes = _dip_session(signal_pos=signal_pos, dip_close=98.5, hold_until=touch_pos - 1)
    closes[touch_pos] = 99.8                        # high = 100.05 >= VWAP 100.0 -> touch
    volumes[touch_pos] = 0.0
    frame = _session_frame(closes, volumes)

    feats = compute_session_features(_SYM, frame)
    assert feats is not None
    assert feats.high[touch_pos] >= feats.vwap[touch_pos]
    assert feats.high[touch_pos - 1] < feats.vwap[touch_pos - 1]     # nothing touched earlier

    trades = _trades(frame)
    assert trades
    assert trades[0].exit_reason == "vwap_touch"
    assert trades[0].exit_ts == frame.index[touch_pos + 1]


def test_timeout_exit_is_exactly_60_minutes_after_the_entry_fill() -> None:
    signal_pos = _SIGNAL_POS
    closes, volumes = _dip_session(signal_pos=signal_pos, dip_close=98.5, hold_until=340)
    frame = _session_frame(closes, volumes)

    trades = _trades(frame)
    assert trades
    tr = trades[0]
    assert tr.exit_reason == "timeout"
    assert tr.exit_ts - tr.entry_ts == timedelta(minutes=TIMEOUT_MINUTES)
    assert tr.holding_minutes == pytest.approx(60.0)
    assert tr.exit_ts == frame.index[signal_pos + 1 + TIMEOUT_MINUTES]
    # …and it is the OPEN of that bar, not its close.
    assert tr.exit_price == pytest.approx(frame["open"].iloc[signal_pos + 1 + TIMEOUT_MINUTES])


# ------------------------------------------------- the stop: 1.5 x Wilder ATR(14) on 10-MINUTE bars
def _wide_bar_session(
    *, signal_pos: int = _SIGNAL_POS, half_width: float = 2.0, last_wide_pos: int = 180
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """A session where the 10-MINUTE and the 1-MINUTE ATR readings diverge by ~6x.

    One bar per 10-minute bucket (indices 0, 10, … ``last_wide_pos``) carries a WIDE high/low —
    ``close ± half_width`` — while its close stays at 100. Because the fixture's typical price is
    ``(H+L+C)/3`` and the widening is symmetric, typical price is still exactly the close, so the
    session VWAP is untouched at 100.0. Consequences:

    * every 10-minute bucket 0..18 has range ``2 x half_width`` ⇒ Wilder ATR(14) on the 10m series is
      exactly ``2 x half_width`` (4.0 by default) — the PRE-REGISTERED stop unit;
    * the 1m bars are almost all ``±_HALF_RANGE`` wide, so ATR(10) on 1m sits under 1.0 — the REJECTED
      reading. A stop placed on the wrong scale is therefore ~6x too tight and trivially detectable.

    Wide bars stop at ``last_wide_pos`` so the holding window after ``signal_pos`` contains none of
    them (a wide bar's high would otherwise trigger the VWAP-touch exit immediately).
    """
    closes, volumes = _flat_session()
    highs = closes + _HALF_RANGE
    lows = closes - _HALF_RANGE
    wide = np.arange(0, last_wide_pos + 1, ATR_RESAMPLE_MINUTES)
    highs[wide] = closes[wide] + half_width
    lows[wide] = closes[wide] - half_width
    closes[signal_pos] = 98.5                       # the dip: 1.5% below VWAP 100
    volumes[signal_pos] = 0.0
    highs[signal_pos] = closes[signal_pos] + _HALF_RANGE
    lows[signal_pos] = closes[signal_pos] - _HALF_RANGE
    closes[signal_pos + 1 :] = 98.5                 # park below VWAP; volume 0 keeps VWAP at 100
    volumes[signal_pos + 1 :] = 0.0
    highs[signal_pos + 1 :] = 98.5 + _HALF_RANGE
    lows[signal_pos + 1 :] = 98.5 - _HALF_RANGE
    return closes, volumes, highs, lows


def _explicit_frame(closes, volumes, highs, lows, *, day: str = _DAY) -> pd.DataFrame:
    idx = pd.date_range(f"{day} 09:15", periods=len(closes), freq="1min", tz=IST)
    return pd.DataFrame(
        {"open": closes.copy(), "high": highs, "low": lows, "close": closes, "volume": volumes},
        index=idx,
    )


def test_stop_uses_the_10_minute_atr_scale_not_the_1_minute_one() -> None:
    """PUNCH-LIST (c): the two ATR timescales give DIFFERENT stop levels here, and the harness must
    use the 10-minute one (manager ruling 2026-08-13). A bar that would stop out on the rejected
    1m scale must NOT stop this trade."""
    signal_pos = _SIGNAL_POS
    closes, volumes, highs, lows = _wide_bar_session()
    frame = _explicit_frame(closes, volumes, highs, lows)

    atr_10m = _atr10_at(frame, signal_pos)
    atr_1m = _atr_1m_at(frame, signal_pos)
    assert atr_10m == pytest.approx(4.0)            # every 10m bucket ranges 98.0..102.0 ⇒ TR = 4.0
    assert atr_1m < 1.0                             # 1-minute noise scale — the REJECTED reading
    assert atr_10m / atr_1m > 4.0                   # …and the two are far apart, so a mix-up shows up

    entry_price = 98.5                              # open of bar signal_pos+1 (parked)
    stop_10m = entry_price - STOP_ATR_MULT * atr_10m
    stop_1m = entry_price - STOP_ATR_MULT * atr_1m
    assert stop_10m < stop_1m - 3.0                 # the correct stop is FAR below the wrong one

    # A probe bar whose low sits strictly BETWEEN the two stops: it takes out the WRONG stop only.
    probe_pos, real_break_pos = signal_pos + 11, signal_pos + 21
    probe_close = (stop_10m + stop_1m) / 2.0
    closes[probe_pos] = probe_close
    highs[probe_pos] = probe_close + _HALF_RANGE
    lows[probe_pos] = probe_close - _HALF_RANGE
    assert lows[probe_pos] < stop_1m                # would stop out on the 1m scale…
    assert lows[probe_pos] > stop_10m               # …but not on the pre-registered 10m scale
    # …and a later bar that genuinely breaks the 10m stop.
    closes[real_break_pos] = closes[real_break_pos + 1] = stop_10m - 0.10
    highs[real_break_pos] = closes[real_break_pos] + _HALF_RANGE
    lows[real_break_pos] = closes[real_break_pos] - _HALF_RANGE

    trades = _trades(_explicit_frame(closes, volumes, highs, lows))
    assert trades
    tr = trades[0]
    assert tr.stop_price == pytest.approx(stop_10m)
    assert tr.exit_reason == "stop"
    # The exit is the REAL break, not the probe: a 1m-scale stop would have exited 10 bars earlier.
    assert tr.exit_ts == frame.index[real_break_pos + 1]
    assert tr.exit_ts > frame.index[probe_pos + 1]
    assert tr.gross_return_pct < 0.0


def test_stop_exit_fills_at_the_bar_after_the_breakdown_bar() -> None:
    """The stop rung's next-bar-open fill, on the standard fixture (10m ATR = 0.5 ⇒ stop = entry−0.75)."""
    signal_pos, break_pos = _SIGNAL_POS, _SIGNAL_POS + 8
    closes, volumes = _dip_session(signal_pos=signal_pos, dip_close=98.5, hold_until=340)
    atr = _atr10_at(_session_frame(closes, volumes), signal_pos)
    entry_price = float(closes[signal_pos + 1])
    stop_price = entry_price - STOP_ATR_MULT * atr
    assert atr == pytest.approx(0.5)                    # flat 10m buckets range 99.75..100.25
    assert stop_price == pytest.approx(98.5 - 0.75)

    # The breakdown bar takes the stop out; the NEXT bar (where the exit actually fills) opens there
    # too, so the realised loss is the next-bar OPEN, not the trigger bar's low.
    closes[break_pos] = closes[break_pos + 1] = stop_price - 0.10
    frame = _session_frame(closes, volumes)
    trades = _trades(frame)
    # Price stays parked below VWAP, so re-entries follow the stop-out; only the FIRST trade is under
    # test here (the no-overlap rule has its own test).
    assert trades
    tr = trades[0]
    assert tr.exit_reason == "stop"
    assert tr.stop_price == pytest.approx(stop_price)
    assert tr.exit_ts == frame.index[break_pos + 1]     # next-bar-open, like every other exit
    assert tr.gross_return_pct < 0.0


def test_10m_atr_is_availability_lagged_by_one_bucket_no_lookahead() -> None:
    """Bucket k's ATR may only be used from bucket k+1 — otherwise a bar would read its own bucket's
    later high. Also pins the exact warm-up boundary the entry window inherits."""
    closes, volumes = _flat_session()
    frame = _session_frame(closes, volumes)
    minute_of_day = np.asarray([t.hour * 60 + t.minute for t in frame.index], dtype=np.int64)
    atr, final = session_atr_10m(
        frame["high"].to_numpy(), frame["low"].to_numpy(), frame["close"].to_numpy(), minute_of_day
    )
    # ATR(14) needs 14 completed buckets; the 15th bucket is the first that may USE it.
    assert np.isnan(atr[: _ATR_READY_POS]).all()
    assert np.isfinite(atr[_ATR_READY_POS:]).all()
    assert _ATR_READY_POS == ATR_RESAMPLE_MINUTES * ATR_PERIOD == 140
    assert frame.index[_ATR_READY_POS].time() == datetime(2026, 1, 5, 11, 35).time()
    assert atr[_ATR_READY_POS] == pytest.approx(0.5)
    assert final == pytest.approx(0.5)          # the value WO-10b hands to the next session

    # Fewer than ATR_PERIOD complete buckets ⇒ no ATR at all ⇒ that session can never trade.
    short = frame.iloc[:100]
    minute_short = minute_of_day[:100]
    atr_short, final_short = session_atr_10m(
        short["high"].to_numpy(), short["low"].to_numpy(), short["close"].to_numpy(), minute_short
    )
    assert np.isnan(atr_short).all()
    assert final_short is None                  # …and nothing to seed the next session with


def test_10m_atr_warmup_blocks_entries_and_dominates_the_1000_window_opening() -> None:
    """Pre-registered consequence: no entry can fill between 10:00 and 11:35 — bars inside the 10m-ATR
    warm-up produce NO TRADE (never an unstopped one), so the EFFECTIVE window is 11:36-14:00."""
    # Price is stretched below VWAP continuously from 09:59 onward, so the STRETCH signal is present
    # through the whole pre-registered 10:00-14:00 window…
    closes, volumes = _dip_session(signal_pos=_WINDOW_OPEN_POS - 1, dip_close=98.5, hold_until=340)
    frame = _session_frame(closes, volumes)
    feats = compute_session_features(_SYM, frame)
    assert feats is not None
    assert frame.index[_WINDOW_OPEN_POS].time() == datetime(2026, 1, 5, 10, 0).time()
    assert feats.stretch_pct[_WINDOW_OPEN_POS - 1] == pytest.approx(1.5)   # the signal IS there…
    assert np.isnan(feats.atr_10m[: _ATR_READY_POS]).all()                 # …but no stop exists yet

    # …yet nothing can fill before 11:36: the warm-up, not the 10:00 opening, is the binding bound.
    # (Unconditioned config: a long parked stretch drives its own 20-bar volume median to zero, which
    # would disable the rel-vol filter's bars for an unrelated reason — the ATR is what is under test.)
    trades = _trades(frame, _CFG_UNCONDITIONED)
    assert trades
    assert min(tr.entry_ts for tr in trades).time() == datetime(2026, 1, 5, 11, 36).time()
    assert all(tr.entry_ts.time() >= datetime(2026, 1, 5, 11, 36).time() for tr in trades)

    # The exact boundary, on single-bar dips: 11:34 signal ⇒ nothing; 11:35 signal ⇒ fill at 11:36.
    before, before_vol = _dip_session(signal_pos=_ATR_READY_POS - 1, dip_close=98.5)
    assert _trades(_session_frame(before, before_vol)) == []

    at, at_vol = _dip_session(signal_pos=_ATR_READY_POS, dip_close=98.5)
    at_frame = _session_frame(at, at_vol)
    at_trades = _trades(at_frame)
    assert len(at_trades) == 1
    assert at_trades[0].signal_ts == at_frame.index[_ATR_READY_POS]
    assert at_trades[0].entry_ts.time() == datetime(2026, 1, 5, 11, 36).time()


def test_stop_wins_a_same_bar_tie_with_the_vwap_touch() -> None:
    """1m bars carry no intrabar path, so the ADVERSE event is assumed first (documented choice)."""
    signal_pos, both_pos = _SIGNAL_POS, _SIGNAL_POS + 6
    closes, volumes = _dip_session(signal_pos=signal_pos, dip_close=98.5, hold_until=340)
    frame_base = _session_frame(closes, volumes)
    stop_price = float(closes[signal_pos + 1]) - STOP_ATR_MULT * _atr10_at(frame_base, signal_pos)

    # One bar that spans BOTH levels: low below the stop and high at/above VWAP.
    frame = _session_frame(closes, volumes)
    frame.iloc[both_pos, frame.columns.get_loc("low")] = stop_price - 0.05
    frame.iloc[both_pos, frame.columns.get_loc("high")] = 100.5

    trades = _trades(frame)
    assert trades
    assert trades[0].exit_reason == "stop"


# --------------------------------------------------------------------------- the entry window
def _features_with_atr_everywhere(closes: np.ndarray, volumes: np.ndarray) -> SessionFeatures:
    """A ``SessionFeatures`` with the 10m ATR defined on EVERY bar.

    The pre-registered 10:00 window opening is dominated by the real 10m-ATR warm-up (11:35), so on a
    real fixture the 10:00 boundary can never be exercised — the ATR would decide the outcome, not the
    window. Overriding ``atr_10m`` isolates the WINDOW predicate so its constants stay pinned in their
    own right; the warm-up has its own end-to-end test above.
    """
    base = compute_session_features(_SYM, _session_frame(closes, volumes))
    assert base is not None
    return SessionFeatures(
        symbol=base.symbol, session=base.session, ts=base.ts, ts_i8=base.ts_i8,
        minute_of_day=base.minute_of_day, open=base.open, high=base.high, low=base.low,
        close=base.close, volume=base.volume, vwap=base.vwap,
        atr_10m=np.full(base.n, 0.5),          # defined everywhere: the window is the only gate left
        rel_vol=base.rel_vol, stretch_pct=base.stretch_pct,
    )


@pytest.mark.parametrize(
    ("fill_pos", "expect_trade", "why"),
    [
        (_WINDOW_OPEN_POS - 1, False, "fill 09:59 — before the window"),
        (_WINDOW_OPEN_POS, True, "fill 10:00 — the inclusive lower boundary"),
        (_LAST_FILL_POS, True, "fill 14:00 — the inclusive upper boundary"),
        (_LAST_FILL_POS + 1, False, "fill 14:01 — after the window"),
    ],
)
def test_entry_window_boundary_is_on_the_fill_inclusive(fill_pos: int, expect_trade: bool, why: str) -> None:
    # ONE dipping bar ⇒ exactly one candidate, so the only thing that can decide the outcome is
    # whether ITS fill bar falls inside the window.
    signal_pos = fill_pos - 1
    closes, volumes = _dip_session(signal_pos=signal_pos, dip_close=98.5)
    feats = _features_with_atr_everywhere(closes, volumes)
    expected_time = (datetime(2026, 1, 5, 9, 15) + timedelta(minutes=fill_pos)).time()
    assert feats.ts[fill_pos].time() == expected_time, why

    trades = simulate_session(feats, _CFG_1PCT, cost_floor_pct=_COST)
    assert bool(trades) is expect_trade, why
    if expect_trade:
        assert trades[0].entry_ts == feats.ts[fill_pos]


# --------------------------------------------------------------------------- session-last-bar drop
def test_signal_on_the_sessions_last_bar_is_dropped() -> None:
    """No bar left inside the session to fill into ⇒ the signal is dropped, not deferred."""
    signal_pos, n = 199, 270                    # 09:15 + 199 min = 12:34, inside the entry window
    closes, volumes = _dip_session(signal_pos=signal_pos, dip_close=98.5, n=n)
    full = _session_frame(closes, volumes)

    # Truncated so the dipping bar IS the session's last bar: bars 0..199 are byte-identical to the
    # control's, so the ONLY difference is whether a next bar exists to fill into.
    truncated = full.iloc[: signal_pos + 1]
    assert truncated.index[-1] == full.index[signal_pos]
    assert _trades(truncated) == []

    control = _trades(full)
    assert len(control) == 1
    assert control[0].signal_ts == full.index[signal_pos]
    assert control[0].entry_ts == full.index[signal_pos + 1]


def test_a_signal_never_fills_into_the_next_session() -> None:
    """Session-aware: session A's last bar must not fill at session B's first bar."""
    n = 200
    closes_a, volumes_a = _dip_session(signal_pos=n - 1, dip_close=98.5, n=n)
    day_a = _session_frame(closes_a, volumes_a, day="2026-01-05")
    day_b = _session_frame(*_flat_session(n), day="2026-01-06")
    frame = pd.concat([day_a, day_b])

    by_config, sessions = simulate_symbol(_SYM, frame, [_CFG_1PCT], cost_floor_pct=_COST)
    assert sessions == [date(2026, 1, 5), date(2026, 1, 6)]
    assert by_config[_CFG_1PCT] == []


def test_one_position_at_a_time_no_overlapping_entries() -> None:
    """Every bar of a long parked-below-VWAP stretch is an unconditioned signal; only non-overlapping
    trades may be booked (otherwise stage 1 would count ~240 'trades' per symbol-session)."""
    signal_pos = _SIGNAL_POS
    closes, volumes = _dip_session(signal_pos=signal_pos, dip_close=98.5, hold_until=_SESSION_BARS - 1)
    frame = _session_frame(closes, volumes)
    feats = compute_session_features(_SYM, frame)
    assert feats is not None
    below = int(
        ((feats.close < feats.vwap) & np.isfinite(feats.atr_10m) & (feats.atr_10m > 0)).sum()
    )
    assert below > 150                                  # scores of eligible bars…

    trades = simulate_session(feats, _CFG_UNCONDITIONED, cost_floor_pct=_COST)
    assert 1 <= len(trades) <= 5                        # …but only a handful of 60-min holds fit
    for earlier, later in zip(trades, trades[1:], strict=False):
        assert later.entry_ts > earlier.exit_ts         # strictly after the previous exit fill


# --------------------------------------------------------------------------- costs (requirement 4)
def test_full_round_trip_is_charged_on_every_trade() -> None:
    signal_pos = _SIGNAL_POS
    closes, volumes = _dip_session(signal_pos=signal_pos, dip_close=98.5, hold_until=340)
    trades = _trades(_session_frame(closes, volumes), cost=_COST)
    assert trades
    for tr in trades:
        assert tr.net_return_pct == pytest.approx(tr.gross_return_pct - _COST)


def test_cost_floor_is_the_wo2_surface_including_spread_at_20k_mis() -> None:
    """Requirement 4: ``breakeven_pct`` (fees + spread), NOT the spread-excluded fee anchor."""
    from engine.strategy.cost_model import CostModel

    cm = CostModel.from_config()
    floor = cost_floor_pct_for(cm)
    assert floor == pytest.approx(float(cm.breakeven_pct(REFERENCE_NOTIONAL, "MIS")))
    # Strictly dearer than the fees-only anchor ⇒ the measured spread really is in there.
    assert floor > float(cm.fee_breakeven_pct(REFERENCE_NOTIONAL, "MIS"))
    assert float(REFERENCE_NOTIONAL) == 20_000.0


# --------------------------------------------------------------------------- the two-stage run
class _FixedClock(Clock):
    def __init__(self, ts: datetime) -> None:
        super().__init__(time_source=lambda: ts)


def _clock() -> Clock:
    return _FixedClock(datetime(2026, 8, 13, 21, 30, tzinfo=IST))


def _splitter(n_obs: int) -> list[tuple[np.ndarray, np.ndarray]]:
    """Deterministic 4-block CPCV stand-in — keeps the promotion path exercised without skfolio."""
    if n_obs < 8:
        return []
    blocks = np.array_split(np.arange(n_obs), 4)
    return [
        (np.concatenate([b for j, b in enumerate(blocks) if j != i]), blocks[i])
        for i in range(4)
    ]


def _multi_session_loader(closes: np.ndarray, volumes: np.ndarray, *, n_sessions: int = 20):
    """A loader serving ``n_sessions`` identical synthetic sessions for one symbol, with a call count."""
    days = pd.bdate_range("2026-01-05", periods=n_sessions).strftime("%Y-%m-%d").tolist()
    frame = pd.concat([_session_frame(closes, volumes, day=d) for d in days])
    calls: list[str] = []

    def loader(symbol: str) -> pd.DataFrame | None:
        calls.append(symbol)
        return frame if symbol == _SYM else None

    return loader, calls


def test_stage1_abort_writes_an_aborted_report_and_never_computes_stage2(tmp_path) -> None:
    """A fixture whose unconditioned reversion is NEGATIVE after costs must stop the whole run."""
    # Price dips and parks: every trade times out flat, so gross == 0 and net == -cost < 0.
    closes, volumes = _dip_session(signal_pos=_SIGNAL_POS, dip_close=98.5, hold_until=340)
    loader, calls = _multi_session_loader(closes, volumes, n_sessions=20)

    exp = VwapReversionExperiment(
        loader=loader, cost_floor_pct=_COST, clock=_clock(), splitter=_splitter
    )
    report = exp.run([_SYM], start=date(2026, 1, 5), end=date(2026, 1, 30))

    assert report.status == "ABORTED_STAGE1"
    assert report.stage2_ran is False
    assert report.grid == []
    assert report.stage1 is not None
    assert report.stage1.aborted is True
    assert report.stage1.n_trades > 0
    assert report.stage1.base_rate_pct is not None and report.stage1.base_rate_pct <= 0.0
    assert report.stage1.base_rate_pct == pytest.approx(-_COST)
    assert "ABORTED" in (report.stage1.abort_reason or "")
    # Stage 2 would need a SECOND read pass; exactly one call per symbol proves it never happened.
    assert calls == [_SYM]

    artifacts = write_report(report, tmp_path)
    md = artifacts.markdown.read_text(encoding="utf-8")
    assert BANNER in md
    assert "ABORTED AT STAGE 1" in md
    assert "NOT RUN" in md                                   # the stage-2 section says so explicitly
    assert f"{report.stage1.base_rate_pct:+.5f}" in md        # the number itself is stated
    assert artifacts.json.exists()
    assert artifacts.markdown.name.startswith(f"{EXPERIMENT_ID}_")


def test_zero_unconditioned_trades_also_aborts_fail_closed(tmp_path) -> None:
    """No trades at all ⇒ no base rate to clear zero with ⇒ abort (fail closed), not a silent pass."""
    closes, volumes = _flat_session()                        # never below VWAP ⇒ never a signal
    loader, calls = _multi_session_loader(closes, volumes, n_sessions=12)

    exp = VwapReversionExperiment(
        loader=loader, cost_floor_pct=_COST, clock=_clock(), splitter=_splitter
    )
    report = exp.run([_SYM], start=date(2026, 1, 5), end=date(2026, 1, 20))

    assert report.status == "ABORTED_STAGE1"
    assert report.stage2_ran is False
    assert report.stage1 is not None
    assert report.stage1.n_trades == 0
    assert report.stage1.base_rate_pct is None
    assert "ZERO trades" in (report.stage1.abort_reason or "")
    assert calls == [_SYM]


def test_no_data_reports_no_data_and_measures_nothing() -> None:
    exp = VwapReversionExperiment(
        loader=lambda _sym: None, cost_floor_pct=_COST, clock=_clock(), splitter=_splitter
    )
    report = exp.run(["NOPE"], start=date(2026, 1, 5), end=date(2026, 1, 30))
    assert report.status == "NO_DATA"
    assert report.stage1 is None
    assert report.stage2_ran is False


def test_stage2_runs_the_3x1_grid_and_applies_the_house_promotion_rule(tmp_path) -> None:
    """Stage 1 clears zero ⇒ the pre-registered grid runs, each point validated by CPCV +
    fold_pass_min(N=3) + the WO-3 margin floor, with the MIS cost floor passed through."""
    signal_pos, touch_pos = _SIGNAL_POS, _SIGNAL_POS + 5
    closes, volumes = _dip_session(signal_pos=signal_pos, dip_close=98.5, hold_until=touch_pos - 1)
    closes[touch_pos] = 101.0                       # reverts THROUGH VWAP -> a clearly profitable exit
    volumes[touch_pos] = 0.0
    closes[touch_pos + 1 :] = 101.0                 # stays above VWAP -> no further signals
    volumes[touch_pos + 1 :] = 0.0
    loader, calls = _multi_session_loader(closes, volumes, n_sessions=20)

    exp = VwapReversionExperiment(
        loader=loader, cost_floor_pct=_COST, clock=_clock(), splitter=_splitter
    )
    report = exp.run([_SYM], start=date(2026, 1, 5), end=date(2026, 2, 2))

    assert report.status == "COMPLETED"
    assert report.stage2_ran is True
    assert report.stage1 is not None and report.stage1.base_rate_pct > 0.0
    assert calls == [_SYM, _SYM]                              # exactly two passes: stage 1, then stage 2

    # The grid is the STRETCH axis only — 3 points, N = 3, rel-vol fixed at 1.5x.
    assert report.trial_count_n == 3 == len(STRETCH_GRID)
    assert [g.stretch_min_pct for g in report.grid] == list(STRETCH_GRID)
    assert {g.rel_vol_max for g in report.grid} == {REL_VOL_MAX}

    g = report.grid[0]                                        # stretch >= 1.0% catches the 1.5% dip
    assert g.n_trades == 20                                   # one trade per synthetic session
    assert g.win_rate == pytest.approx(1.0)
    assert g.expectancy_pct == pytest.approx((101.0 - 98.5) / 98.5 * 100.0 - _COST)
    assert g.clears_2x_cost_floor is True
    v = g.validation
    assert v is not None
    assert v.strategy_id == EXPERIMENT_ID
    assert v.trial_count_n == 3
    assert v.fold_pass_min == pytest.approx(0.60)             # fold_pass_min(N=3) = 60%
    assert v.cost_floor_pct == pytest.approx(_COST)           # the MIS floor, not the CNC fallback
    assert v.margin_floor_pct_per_day == pytest.approx(_COST / 20.0)
    assert v.n_obs == 20
    assert len(v.cpcv) == 4 and v.cpcv_fold_pass_fraction == pytest.approx(1.0)
    assert g.promotable is True

    # stretch >= 2.0% never fires on a 1.5% dip: zero trades ⇒ a flat series ⇒ not promotable.
    assert report.grid[2].n_trades == 0
    assert report.grid[2].promotable is False

    artifacts = write_report(report, tmp_path)
    md = artifacts.markdown.read_text(encoding="utf-8")
    assert BANNER in md
    assert "STATUS: COMPLETED" in md
    assert "modelling_notes" in md
    assert "next_bar_open" in md
    for note_fragment in ("ABORT QUANTITY", "'THE 10-MIN ATR'", "OVERLAP"):
        assert note_fragment in md


def test_report_carries_every_required_block(tmp_path) -> None:
    """Design requirement 5: the report must carry the banner, stage-1 base rate, grid stats, CPCV +
    margin floor + promotable, and a modelling_notes block naming every pre-registered choice."""
    closes, volumes = _dip_session(signal_pos=_SIGNAL_POS, dip_close=98.5, hold_until=340)
    loader, _ = _multi_session_loader(closes, volumes, n_sessions=12)
    exp = VwapReversionExperiment(
        loader=loader, cost_floor_pct=_COST, clock=_clock(), splitter=_splitter
    )
    report = exp.run([_SYM], start=date(2026, 1, 5), end=date(2026, 1, 20))

    assert report.banner == BANNER
    assert report.fill_mechanics == "next_bar_open"
    assert report.product == "MIS"
    assert report.reference_notional == "20000"
    assert report.cost_floor_pct == pytest.approx(_COST)
    joined = " ".join(report.modelling_notes)
    for required in (
        "PRE-REGISTRATION",
        "TWO-STAGE",
        "NEXT-1m-BAR OPEN",
        "SESSION AWARENESS",
        "session-cumulative",
        "INCLUDING the measured spread",
        f"{TIMEOUT_MINUTES}-min timeout",
        f"ATR({ATR_PERIOD}) on session-local {ATR_RESAMPLE_MINUTES}m bars",
        "STOP SCALE (manager ruling 2026-08-13",
        "ORB death geometry",
        "EFFECTIVE entry window is 11:36",
        "10m-ATR WARM-UP",
        "long side only",
        "INTERPRETATIONS",
    ):
        assert required in joined, required

    artifacts = write_report(report, tmp_path)
    assert artifacts.json.exists() and artifacts.markdown.exists()
    import json

    payload = json.loads(artifacts.json.read_text(encoding="utf-8"))
    assert payload["banner"] == BANNER
    assert payload["stage1"]["base_rate_pct"] == pytest.approx(-_COST)
    assert payload["status"] == "ABORTED_STAGE1"
    # Default mode is WO-10; the report says so, and carries no (empty) split table.
    assert payload["experiment_variant"] == "WO-10"
    assert payload["seed_atr_prior_session"] is False
    assert payload["stage1"]["window_splits"] == []


# ======================================================================= WO-10b: prior-session ATR
# (owner-directed 2026-08-14 — a MODE of the shipped harness, not a fork)
def _two_session_frame(
    *, gap: float = 0.0, n: int = _SESSION_BARS, day1: str = "2026-01-05", day2: str = "2026-01-06"
) -> pd.DataFrame:
    """Two sessions for one symbol; session 2's prices are shifted by ``gap`` (the overnight gap).

    Both sessions are flat internally (every 10m bucket ranges exactly 0.5), so the ATR arithmetic is
    hand-computable and any contribution from the overnight gap stands out immediately.
    """
    c1, v1 = _flat_session(n)
    c2, v2 = _flat_session(n)
    c2 = c2 + gap
    return pd.concat([_session_frame(c1, v1, day=day1), _session_frame(c2, v2, day=day2)])


def _session_features_chain(frame: pd.DataFrame, *, seeded: bool) -> list[SessionFeatures]:
    """Walk a multi-session frame the way ``simulate_symbol`` does, returning each session's features."""
    out: list[SessionFeatures] = []
    seed: float | None = None
    for start, stop in session_slices(pd.DatetimeIndex(frame.index)):
        feats = compute_session_features(_SYM, frame.iloc[start:stop], atr_seed=seed if seeded else None)
        assert feats is not None
        out.append(feats)
        if seeded and feats.atr_10m_final is not None:
            seed = feats.atr_10m_final
    return out


def test_seeding_chain_continues_the_prior_sessions_final_atr() -> None:
    """WO-10b: session 2's FIRST bucket continues session 1's FINAL 10m-ATR value."""
    frame = _two_session_frame()
    day1, day2 = _session_features_chain(frame, seeded=True)

    seed = day1.atr_10m_final
    assert seed == pytest.approx(0.5)                     # flat buckets range 99.75..100.25

    # atr[0] = (seed x (period-1) + (high[0] - low[0])) / period, exactly as pre-registered.
    first_bucket_tr = 0.5                                 # bucket 0 spans 99.75..100.25
    expected = (seed * (ATR_PERIOD - 1) + first_bucket_tr) / ATR_PERIOD
    # The availability lag publishes bucket 0's ATR to bucket 1 (bar index 10 = 09:25), not bar 0.
    assert np.isnan(day2.atr_10m[:ATR_RESAMPLE_MINUTES]).all()
    assert day2.atr_10m[ATR_RESAMPLE_MINUTES] == pytest.approx(expected)
    assert day2.atr_10m[ATR_RESAMPLE_MINUTES] == pytest.approx(0.5)   # a flat chain stays at 0.5

    # …and unseeded, that same bar has NO ATR at all — the chain is what buys the morning window.
    _, day2_unseeded = _session_features_chain(frame, seeded=False)
    assert np.isnan(day2_unseeded.atr_10m[ATR_RESAMPLE_MINUTES])
    assert np.isnan(day2_unseeded.atr_10m[: _ATR_READY_POS]).all()


def test_overnight_gap_never_enters_the_stop_unit() -> None:
    """The first bucket's TR is high−low ONLY. A 12-point gap-down would, if the prev-close term were
    included, put ~12 into TR[0] and blow the seeded ATR up ~25x — this pins that it does not."""
    gap = -12.0
    frame = _two_session_frame(gap=gap)
    day1, day2 = _session_features_chain(frame, seeded=True)
    seed = day1.atr_10m_final
    assert seed == pytest.approx(0.5)

    first_bucket_hi, first_bucket_lo = 100.0 + gap + _HALF_RANGE, 100.0 + gap - _HALF_RANGE
    prev_session_close = 100.0

    correct_tr = first_bucket_hi - first_bucket_lo                       # 0.5 — gap EXCLUDED
    gap_tr = max(                                                       # what the rejected form gives
        correct_tr,
        abs(first_bucket_hi - prev_session_close),
        abs(first_bucket_lo - prev_session_close),
    )
    assert gap_tr > 11.0 and gap_tr / correct_tr > 20.0                 # the readings really do diverge

    correct = (seed * (ATR_PERIOD - 1) + correct_tr) / ATR_PERIOD
    with_gap = (seed * (ATR_PERIOD - 1) + gap_tr) / ATR_PERIOD
    observed = day2.atr_10m[ATR_RESAMPLE_MINUTES]
    assert observed == pytest.approx(correct)
    assert observed != pytest.approx(with_gap)
    assert with_gap / correct > 1.5                                     # detectably wrong, not a rounding nit

    # The gap must not leak anywhere later in the chain either.
    assert day2.atr_10m_final == pytest.approx(0.5)


def test_day_one_is_fail_closed_no_prior_atr_means_wo10_warmup() -> None:
    """A symbol's FIRST session has no prior ATR, so it keeps WO-10's 140-minute warm-up even in
    seeded mode — 'no trades until its own warm-up completes' (pre-registered)."""
    frame = _two_session_frame()
    day1, day2 = _session_features_chain(frame, seeded=True)

    assert np.isnan(day1.atr_10m[: _ATR_READY_POS]).all()      # day 1: unchanged from WO-10
    assert np.isfinite(day1.atr_10m[_ATR_READY_POS])
    assert np.isfinite(day2.atr_10m[ATR_RESAMPLE_MINUTES])     # day 2: seeded, ATR from 09:25

    # End to end: a 10:00 dip trades on day 2 but NOT on day 1.
    closes, volumes = _dip_session(signal_pos=_WINDOW_OPEN_POS - 1, dip_close=98.5)
    both = pd.concat([
        _session_frame(closes, volumes, day="2026-01-05"),
        _session_frame(closes, volumes, day="2026-01-06"),
    ])
    by_cfg, sessions = simulate_symbol(
        _SYM, both, [_CFG_1PCT], cost_floor_pct=_COST, seed_atr_prior_session=True
    )
    trades = by_cfg[_CFG_1PCT]
    assert sessions == [date(2026, 1, 5), date(2026, 1, 6)]
    assert [tr.session for tr in trades] == [date(2026, 1, 6)]         # day 1 produced nothing
    assert trades[0].entry_ts.time() == datetime(2026, 1, 5, 10, 0).time()


def test_seeded_mode_opens_the_registered_1000_window_from_day_two() -> None:
    """The point of WO-10b: with seeding, the effective entry window is the registered 10:00-14:00."""
    closes, volumes = _dip_session(signal_pos=_WINDOW_OPEN_POS - 1, dip_close=98.5)
    days = pd.bdate_range("2026-01-05", periods=4).strftime("%Y-%m-%d").tolist()
    frame = pd.concat([_session_frame(closes, volumes, day=d) for d in days])

    seeded, _ = simulate_symbol(
        _SYM, frame, [_CFG_1PCT], cost_floor_pct=_COST, seed_atr_prior_session=True
    )
    unseeded, _ = simulate_symbol(_SYM, frame, [_CFG_1PCT], cost_floor_pct=_COST)

    assert [tr.entry_ts.time() for tr in seeded[_CFG_1PCT]] == [
        datetime(2026, 1, 5, 10, 0).time()
    ] * 3                                                     # days 2-4 all fill at the registered 10:00
    assert unseeded[_CFG_1PCT] == []                          # WO-10 could never reach 10:00


def test_default_off_is_byte_identical_to_wo10() -> None:
    """PUNCH-LIST: run the existing fixture BOTH ways; with the flag off every number is unchanged."""
    closes, volumes = _dip_session(signal_pos=_SIGNAL_POS, dip_close=98.5, hold_until=340)
    days = pd.bdate_range("2026-01-05", periods=6).strftime("%Y-%m-%d").tolist()
    frame = pd.concat([_session_frame(closes, volumes, day=d) for d in days])

    off_a, sessions_a = simulate_symbol(_SYM, frame, [_CFG_1PCT], cost_floor_pct=_COST)
    off_b, sessions_b = simulate_symbol(
        _SYM, frame, [_CFG_1PCT], cost_floor_pct=_COST, seed_atr_prior_session=False
    )
    assert off_a[_CFG_1PCT] == off_b[_CFG_1PCT]               # explicit False == omitted, exactly
    assert sessions_a == sessions_b
    assert off_a[_CFG_1PCT]                                   # …and it is not a vacuous pass

    # The same via the experiment surface: identical stage-1 numbers, and the report says WO-10.
    loader, _ = _multi_session_loader(closes, volumes, n_sessions=12)
    default = VwapReversionExperiment(
        loader=loader, cost_floor_pct=_COST, clock=_clock(), splitter=_splitter
    ).run([_SYM], start=date(2026, 1, 5), end=date(2026, 1, 20))
    loader2, _ = _multi_session_loader(closes, volumes, n_sessions=12)
    explicit_off = VwapReversionExperiment(
        loader=loader2, cost_floor_pct=_COST, clock=_clock(), splitter=_splitter,
        seed_atr_prior_session=False,
    ).run([_SYM], start=date(2026, 1, 5), end=date(2026, 1, 20))

    assert default.experiment_variant == explicit_off.experiment_variant == "WO-10"
    assert default.stage1.model_dump() == explicit_off.stage1.model_dump()
    assert default.status == explicit_off.status

    # Seeding ON is a DIFFERENT measurement — otherwise the byte-identity above would be vacuous.
    # (On a fixture that only dips at 12:34 the two modes coincide by construction, so the contrast
    # uses an EARLY dip: the morning bars are exactly what seeding buys.)
    early_c, early_v = _dip_session(signal_pos=_WINDOW_OPEN_POS - 1, dip_close=98.5, hold_until=340)
    early = pd.concat([_session_frame(early_c, early_v, day=d) for d in days])
    seeded_trades, _ = simulate_symbol(
        _SYM, early, [_CFG_UNCONDITIONED], cost_floor_pct=_COST, seed_atr_prior_session=True
    )
    unseeded_trades, _ = simulate_symbol(
        _SYM, early, [_CFG_UNCONDITIONED], cost_floor_pct=_COST
    )
    assert len(seeded_trades[_CFG_UNCONDITIONED]) > len(unseeded_trades[_CFG_UNCONDITIONED])


# --------------------------------------------------------------- WO-10b split reporting (never a gate)
def test_split_of_boundary_is_1135_inclusive_on_the_entry_fill() -> None:
    def _ts(h: int, m: int) -> pd.Timestamp:
        return pd.Timestamp(f"{_DAY} {h:02d}:{m:02d}", tz=IST)

    assert MORNING_SPLIT_END == datetime(2026, 1, 5, 11, 35).time()
    assert split_of(_ts(10, 0)) == SPLIT_MORNING
    assert split_of(_ts(11, 35)) == SPLIT_MORNING          # inclusive upper edge of the morning
    assert split_of(_ts(11, 36)) == SPLIT_MIDDAY           # exactly WO-10's first reachable fill
    assert split_of(_ts(14, 0)) == SPLIT_MIDDAY


def test_window_split_statistics_are_computed_per_split() -> None:
    """The split math: counts, net expectancy and win rate are per-split; both splits always emitted."""
    acc = ConfigAccumulator(config=_CFG_1PCT)

    def _trade(hour: int, minute: int, net: float) -> Trade:
        ts = pd.Timestamp(f"{_DAY} {hour:02d}:{minute:02d}", tz=IST)
        return Trade(
            symbol=_SYM, session=date(2026, 1, 5), signal_ts=ts, entry_ts=ts, exit_ts=ts,
            entry_price=100.0, exit_price=100.0 + net, stop_price=99.0, exit_reason="timeout",
            holding_minutes=60.0, stretch_pct=1.5, rel_vol=1.0,
            gross_return_pct=net + _COST, net_return_pct=net,
        )

    acc.add_symbol_session(date(2026, 1, 5), [
        _trade(10, 30, +1.0), _trade(11, 35, -3.0),        # morning: mean -1.0, win rate 0.5
        _trade(11, 36, +2.0), _trade(13, 0, +4.0),         # midday:  mean +3.0, win rate 1.0
    ])

    morning, midday = acc.window_splits()
    assert morning.split == SPLIT_MORNING and midday.split == SPLIT_MIDDAY
    assert (morning.n_trades, midday.n_trades) == (2, 2)
    assert morning.net_expectancy_pct == pytest.approx(-1.0)
    assert midday.net_expectancy_pct == pytest.approx(+3.0)
    assert morning.win_rate == pytest.approx(0.5)
    assert midday.win_rate == pytest.approx(1.0)
    assert morning.gross_expectancy_pct == pytest.approx(-1.0 + _COST)
    # The OVERALL figure — the abort criterion — is the mean of all four, not of the split means.
    assert acc.expectancy_pct == pytest.approx(1.0)

    # Both splits are emitted even when one is empty, so a missing row can never be misread as zero.
    empty = ConfigAccumulator(config=_CFG_1PCT)
    empty.add_symbol_session(date(2026, 1, 5), [_trade(13, 0, +1.0)])
    m2, d2 = empty.window_splits()
    assert (m2.n_trades, m2.net_expectancy_pct, m2.win_rate) == (0, None, None)
    assert d2.n_trades == 1


def test_seeded_report_carries_the_split_table_and_the_abort_stays_on_the_overall_number(tmp_path) -> None:
    """Splits are REPORTING: the abort verdict must read the OVERALL base rate, not a split."""
    closes, volumes = _dip_session(signal_pos=_WINDOW_OPEN_POS - 1, dip_close=98.5, hold_until=340)
    loader, _ = _multi_session_loader(closes, volumes, n_sessions=14)
    report = VwapReversionExperiment(
        loader=loader, cost_floor_pct=_COST, clock=_clock(), splitter=_splitter,
        seed_atr_prior_session=True,
    ).run([_SYM], start=date(2026, 1, 5), end=date(2026, 1, 24))

    assert report.experiment_variant == "WO-10b"
    assert report.seed_atr_prior_session is True
    s = report.stage1
    assert s is not None
    splits = {sp.split: sp for sp in s.window_splits}
    assert set(splits) == {SPLIT_MORNING, SPLIT_MIDDAY}
    assert splits[SPLIT_MORNING].n_trades > 0                 # the morning window IS now measured
    # The overall count is the sum of the splits, and the abort verdict tracks the OVERALL number.
    assert sum(sp.n_trades for sp in s.window_splits) == s.n_trades
    assert s.aborted is (s.base_rate_pct is None or s.base_rate_pct <= 0.0)

    artifacts = write_report(report, tmp_path)
    md = artifacts.markdown.read_text(encoding="utf-8")
    assert "WO-10b experiment" in md
    assert "PRIOR-SESSION SEEDED (WO-10b)" in md
    assert "Entry-window splits (WO-10b) — REPORTING ONLY, never a gate" in md
    assert "unreachable under WO-10" in md
    joined = " ".join(report.modelling_notes)
    assert "PRIOR-SESSION ATR SEEDING (WO-10b, pre-registered verbatim)" in joined
    assert "the overnight gap must never enter the stop unit" in joined
    assert "THE ABORT CRITERION IS THE OVERALL NUMBER" in joined
