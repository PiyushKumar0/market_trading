"""Mini end-to-end sweep smoke test (needs_heavy_deps — vectorbt + skfolio).

Synthetic daily bars, a tiny grid (2 configs), one strategy: the sweep runs vectorbt, reports the trial
count N = grid cardinality, feeds ``returns_for`` into the ValidationPipeline, and the report cites N.

Also pins the fill mechanics (next-bar-open, not the signal bar's close, on a path where they differ
materially) and the sweep mechanics: intrabar daily stops, the half-spread on a stop exit, the
closed-trade ranking statistic, and the report stamp.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import numpy as np
import pandas as pd
import pytest

from engine.core.clock import IST, Clock

pytest.importorskip("vectorbt")
pytest.importorskip("skfolio")

from engine.learning import reports  # noqa: E402
from engine.learning.sweep import (  # noqa: E402
    EXPECTANCY_BASIS,
    MECHANICS_STAMP,
    REFERENCE_NOTIONAL_DEFAULT,
    STOP_EVALUATION,
    STOP_EXIT_PRICE,
    ParamSetStat,
    SweepRunner,
    _Frames,
    _hold_stats,
    _Signals,
    _trade_hold_bars,
    _trade_split_stats,
    build_param_grid,
)
from engine.learning.validate import ParamSet, ValidationPipeline  # noqa: E402
from engine.marketdata.store import DailyBar, MarketStore  # noqa: E402
from engine.strategy.cost_model import CostModel  # noqa: E402

pytestmark = pytest.mark.needs_heavy_deps

FIXED_NOW = datetime(2026, 6, 17, 18, 0, tzinfo=IST)


def _D(x: float) -> Decimal:
    return Decimal(str(round(x, 2)))


def _seed_daily(store: MarketStore) -> tuple:
    """Two symbols, ~140 business days, deterministic noisy walks that rotate in momentum ranking."""
    import numpy as np

    rng = np.random.RandomState(7)
    dates = pd.bdate_range("2024-01-01", periods=140)
    for sym, drift in (("AAA", 0.0009), ("BBB", 0.0003)):
        price = 100.0
        bars: list[DailyBar] = []
        for d in dates:
            ret = drift + float(rng.normal(0.0, 0.02))
            newc = max(1.0, price * (1.0 + ret))
            o, c = price, newc
            h = max(o, c) * 1.01
            lo = min(o, c) * 0.99
            bars.append(
                DailyBar(
                    symbol=sym, d=d.date(), open=_D(o), high=_D(h), low=_D(lo), close=_D(c),
                    volume=100_000 + int(rng.randint(0, 50_000)),
                )
            )
            price = newc
        store.upsert_bars_1d(bars)
    return dates[0].date(), dates[-1].date()


@pytest.fixture
def store(tmp_path) -> MarketStore:
    clock = Clock(time_source=lambda: FIXED_NOW)
    s = MarketStore(tmp_path / "market.duckdb", tmp_path / "parquet", clock).open()
    yield s
    s.close()


def test_build_param_grid_cardinality_is_trial_count():
    # coarse (2 points/param) + default unioned in. mom has top_n (1..3) and rebalance_days (10..20).
    grid = build_param_grid("mom", points=2)
    assert grid, "expected a non-empty grid"
    # top_n ∈ {1,2,3} (min,max,default all distinct integers); rebalance_days ∈ {10,15,20}
    top_ns = sorted({g["top_n"] for g in grid})
    reb = sorted({g["rebalance_days"] for g in grid})
    assert top_ns == [1.0, 2.0, 3.0]
    assert reb == [10.0, 15.0, 20.0]
    assert len(grid) == len(top_ns) * len(reb)  # cartesian product = trial count N


def test_mini_sweep_and_validate(store, tmp_path):
    start, end = _seed_daily(store)
    clock = Clock(time_source=lambda: FIXED_NOW)
    cost_model = CostModel.from_config()
    runner = SweepRunner(store, cost_model, clock)

    tiny_grid = [
        {"top_n": 1.0, "rebalance_days": 10.0},
        {"top_n": 2.0, "rebalance_days": 10.0},
    ]
    report = runner.run("mom", start, end, symbols=["AAA", "BBB"], param_grid=tiny_grid)

    assert report.trial_count_n == 2               # grid cardinality = the cited N
    assert len(report.stats) == 2
    assert report.product == "CNC"
    assert report.n_symbols == 2
    assert report.per_side_fee_pct > 0.0           # a real cost was modelled
    assert any(s.n_trades > 0 for s in report.stats), "expected at least one config to trade"

    # returns provider yields a validate-ready daily series
    series = runner.returns_for("mom", report.stats[0].params)
    assert isinstance(series, pd.Series)
    assert len(series) > 0

    # end-to-end validation citing N (real skfolio CPCV over the ~140-obs series)
    pipeline = ValidationPipeline(
        returns_provider=runner.returns_for, clock=clock, reports_dir=tmp_path / "reports"
    )
    best = report.best_params or report.stats[0].params
    result = pipeline.validate_sync(
        "mom", ParamSet(strategy_id="mom", params=best, trial_count_n=report.trial_count_n)
    )
    assert result.trial_count_n == 2
    assert result.fold_pass_min == 0.60            # N=2 ⇒ 60% bar
    assert isinstance(result.promotable, bool)
    assert result.n_obs == len(series)
    assert result.cpcv, "expected CPCV folds over ~140 daily observations"
    # a report artifact was written
    md = (tmp_path / "reports" / f"mom_{result.generated_at:%Y%m%dT%H%M%S}.md")
    assert md.exists()


def test_returns_for_requires_prior_run(store):
    clock = Clock(time_source=lambda: FIXED_NOW)
    runner = SweepRunner(store, CostModel.from_config(), clock)
    with pytest.raises(RuntimeError):
        runner.returns_for("mom", {"top_n": 1.0, "rebalance_days": 10.0})


# --------------------------------------------------------------------------- fill mechanics
_SYM = "ZZ"

#: Close is flat at 100, so a same-bar-close fill buys and sells at 100 (0% gross); the bar after the
#: entry signal opens at 90, so a next-bar-open fill buys at 90 and sells at 100 (+11.1% gross).
_FILL_CLOSE = [100.0, 100.0, 100.0, 100.0, 100.0, 100.0]
_FILL_OPEN = [100.0, 100.0, 90.0, 100.0, 100.0, 100.0]
_ENTRY_SIGNAL_ROW = 1        # ⇒ order on row 2, filled at its open (90.0)
_EXIT_SIGNAL_ROW = 3         # ⇒ order on row 4, filled at its open (100.0)


def _fill_frames() -> _Frames:
    idx = pd.bdate_range("2024-01-01", periods=len(_FILL_CLOSE))
    close = pd.DataFrame({_SYM: _FILL_CLOSE}, index=idx)
    return _Frames(
        close=close,
        high=close * 1.001,
        low=pd.DataFrame({_SYM: _FILL_OPEN}, index=idx) * 0.999,
        open=pd.DataFrame({_SYM: _FILL_OPEN}, index=idx),
        volume=pd.DataFrame({_SYM: [1000.0] * len(_FILL_CLOSE)}, index=idx),
        intraday=False,
        auction_open=None,
    )


def _fill_signals(frames: _Frames) -> _Signals:
    entries = pd.DataFrame(False, index=frames.close.index, columns=frames.close.columns)
    exits = entries.copy()
    entries.iloc[_ENTRY_SIGNAL_ROW, 0] = True
    exits.iloc[_EXIT_SIGNAL_ROW, 0] = True
    return _Signals(entries=entries, exits=exits)


def test_daily_sweep_fills_entries_and_exits_at_the_next_sessions_open():
    """The fill price is the next session's open, not the signal bar's close (which would score ~0% gross)."""
    clock = Clock(time_source=lambda: FIXED_NOW)
    cost_model = CostModel.from_config()
    runner = SweepRunner(None, cost_model, clock)          # _portfolio never touches the store
    frames = _fill_frames()
    fee = float(cost_model.fee_breakeven_pct(REFERENCE_NOTIONAL_DEFAULT, "CNC")) / 100.0 / 2.0

    pf = runner._portfolio(frames, _fill_signals(frames), fee)
    trades = pf.trades.records_readable
    assert len(trades) == 1
    trade = trades.iloc[0]

    half_spread = float(cost_model.half_spread_pct) / 100.0
    idx = frames.close.index
    # The order lands on the bar after the signal and pays that bar's open ± half the spread.
    assert pd.Timestamp(trade["Entry Timestamp"]) == idx[_ENTRY_SIGNAL_ROW + 1]
    assert pd.Timestamp(trade["Exit Timestamp"]) == idx[_EXIT_SIGNAL_ROW + 1]
    assert float(trade["Avg Entry Price"]) == pytest.approx(90.0 * (1.0 + half_spread))
    assert float(trade["Avg Exit Price"]) == pytest.approx(100.0 * (1.0 - half_spread))

    assert float(trade["Return"]) > 0.10


def test_next_open_fill_charges_fees_and_half_the_spread_on_every_leg():
    """Spread enters as vectorbt slippage, half the measured spread per leg, on top of the statutory fee."""
    clock = Clock(time_source=lambda: FIXED_NOW)
    cost_model = CostModel.from_config()
    runner = SweepRunner(None, cost_model, clock)
    frames = _fill_frames()
    fee = float(cost_model.fee_breakeven_pct(REFERENCE_NOTIONAL_DEFAULT, "CNC")) / 100.0 / 2.0

    pf = runner._portfolio(frames, _fill_signals(frames), fee)
    trade = pf.trades.records_readable.iloc[0]

    half_spread = float(cost_model.half_spread_pct) / 100.0
    entry_px, exit_px = float(trade["Avg Entry Price"]), float(trade["Avg Exit Price"])
    size = float(trade["Size"])
    # slippage is inside the fill prices; the fee is the separate ``Entry Fees``/``Exit Fees`` legs
    assert float(trade["Entry Fees"]) == pytest.approx(size * entry_px * fee)
    assert float(trade["Exit Fees"]) == pytest.approx(size * exit_px * fee)
    assert entry_px / 90.0 - 1.0 == pytest.approx(half_spread)     # bought half a spread above mid
    assert 1.0 - exit_px / 100.0 == pytest.approx(half_spread)     # sold half a spread below mid


def test_sizing_uses_the_reference_notional_the_fee_is_calibrated_at():
    """Per-symbol init_cash defaults to reference_notional (₹20,000), not ₹100,000."""
    clock = Clock(time_source=lambda: FIXED_NOW)
    runner = SweepRunner(None, CostModel.from_config(), clock)
    assert runner._init_cash == float(REFERENCE_NOTIONAL_DEFAULT) == 20_000.0

    frames = _fill_frames()
    pf = runner._portfolio(frames, _fill_signals(frames), 0.0)
    trade = pf.trades.records_readable.iloc[0]
    notional = float(trade["Size"]) * float(trade["Avg Entry Price"])
    assert notional == pytest.approx(20_000.0, rel=0.01)      # all-in at the reference size

    # The proportional cost surface makes the return series scale-invariant.
    big = SweepRunner(None, CostModel.from_config(), clock, init_cash=100_000.0)
    fee = 0.001
    r_small = runner._portfolio(frames, _fill_signals(frames), fee).returns()[_SYM].to_numpy()
    r_big = big._portfolio(frames, _fill_signals(frames), fee).returns()[_SYM].to_numpy()
    assert np.allclose(r_small, r_big)


# --------------------------------------------------------------- holding period + open trades
# The measured hold: bars between fill rows, NaN-safe stats, and the open/closed split of the trade
# records ``expectancy_pct`` is averaged over.


def test_hold_bars_counts_rows_between_the_entry_and_exit_FILLS():
    """The hold is exit row − entry row (weekends/holidays are not counted), between the fills, not the signals."""
    clock = Clock(time_source=lambda: FIXED_NOW)
    runner = SweepRunner(None, CostModel.from_config(), clock)
    frames = _fill_frames()

    trades = runner._portfolio(frames, _fill_signals(frames), 0.0).trades.records_readable
    bars = _trade_hold_bars(trades, frames.close.index)

    assert len(bars) == 1
    assert bars[0] == float(_EXIT_SIGNAL_ROW - _ENTRY_SIGNAL_ROW) == 2.0
    assert _hold_stats(bars) == (2.0, 2.0, 2.0)

    # Rows, not calendar days: a Friday entry and Monday exit are one bar apart.
    week = pd.bdate_range("2024-01-01", periods=10)                     # Mon 01-01 .. Fri 01-12
    weekend = pd.DataFrame({"Entry Timestamp": [week[4]], "Exit Timestamp": [week[5]]})   # Fri -> Mon
    assert _trade_hold_bars(weekend, week).tolist() == [1.0]


def test_an_open_trade_contributes_its_AGE_at_the_window_edge_not_a_realized_hold():
    """A position still open at the last bar has paid no exit leg: its duration is an age, reported beside the closed-only figures."""
    clock = Clock(time_source=lambda: FIXED_NOW)
    runner = SweepRunner(None, CostModel.from_config(), clock)
    frames = _fill_frames()
    entries = pd.DataFrame(False, index=frames.close.index, columns=frames.close.columns)
    entries.iloc[_ENTRY_SIGNAL_ROW, 0] = True
    never_exits = pd.DataFrame(False, index=frames.close.index, columns=frames.close.columns)

    trades = runner._portfolio(
        frames, _Signals(entries=entries, exits=never_exits), 0.0
    ).trades.records_readable

    assert len(trades) == 1
    assert trades["Status"].astype(str).str.lower().iloc[0] == "open"
    bars = _trade_hold_bars(trades, frames.close.index)
    # Entry fills on row _ENTRY_SIGNAL_ROW + 1; the open trade is marked at the last row.
    assert bars[0] == float(len(_FILL_CLOSE) - 1 - (_ENTRY_SIGNAL_ROW + 1))


def test_hold_stats_are_total_on_empty_and_nan_inputs():
    """A reporting path never fails a validation run."""
    assert _hold_stats(np.zeros(0, dtype="float64")) == (None, None, None)
    assert _hold_stats(np.array([np.nan, np.nan])) == (None, None, None)
    assert _hold_stats(np.array([np.nan, 4.0, 10.0]))[:2] == (7.0, 7.0)


def test_sweep_report_carries_the_holding_distribution_and_the_open_closed_split(store):
    """Every scored config records its own holding distribution + open/closed split in the JSON."""
    start, end = _seed_daily(store)
    clock = Clock(time_source=lambda: FIXED_NOW)
    runner = SweepRunner(store, CostModel.from_config(), clock)

    report = runner.run(
        "mom", start, end, symbols=["AAA", "BBB"],
        param_grid=[{"top_n": 1.0, "rebalance_days": 10.0}],
    )

    assert report.bar_unit == "session"                            # daily frames ⇒ bars ARE sessions
    assert report.population_is_survivorship_tainted_proxy is True  # no PIT membership is stored
    assert any("SURVIVORSHIP" in n for n in report.notes)
    assert any("STOPS ARE EVALUATED INTRABAR" in n for n in report.notes)
    stat = report.stats[0]
    assert stat.n_trades > 0
    assert stat.n_closed + stat.n_open == stat.n_trades
    assert stat.hold_bars_mean is not None and stat.hold_bars_mean >= 0.0
    assert stat.hold_bars_median is not None
    assert stat.hold_bars_p90 is not None
    if stat.n_closed:
        assert stat.expectancy_closed_pct is not None
        assert stat.hold_bars_median_closed is not None
    # The headline is the closed-trade mean; the all-trades mean rides beside it.
    tr = runner._backtest("mom", runner._frames["mom"], dict(stat.params), runner._fee["mom"])
    records = tr.trades.records_readable
    all_returns = records["Return"].astype(float)
    closed = records["Status"].astype(str).str.lower().to_numpy() == "closed"
    assert stat.expectancy_all_pct == pytest.approx(float(all_returns.mean() * 100.0))
    assert stat.expectancy_pct == stat.expectancy_closed_pct
    assert stat.expectancy_pct == pytest.approx(
        float(all_returns.to_numpy()[closed].mean() * 100.0)
    )
    # Win rate is reported on both populations, so a mark-to-market hit rate is never paired with a realized mean.
    assert stat.win_rate == pytest.approx(float((all_returns > 0.0).mean()))
    if stat.n_closed:
        assert stat.win_rate_closed == pytest.approx(
            float((all_returns.to_numpy()[closed] > 0.0).mean())
        )


def test_hold_stats_degrade_instead_of_taking_the_sweep_down():
    """A reporting-only read of vectorbt's column names must never fail a multi-minute sweep."""
    idx = pd.bdate_range("2024-01-01", periods=4)
    # An unanticipated records_readable shape (no Status column).
    trades = pd.DataFrame({"Return": [0.1], "Entry Timestamp": [idx[0]], "Exit Timestamp": [idx[2]]})
    n_closed, n_open, exp_closed, win_closed, hold_all, hold_closed = _trade_split_stats(
        trades, trades["Return"], idx
    )
    assert (n_closed, n_open, exp_closed, win_closed) == (0, 0, None, None)
    assert hold_all == hold_closed == (None, None, None)

    # a duplicated bar timestamp would make index.get_indexer raise
    dup = pd.DatetimeIndex([idx[0], idx[0], idx[1], idx[2]])
    ok = pd.DataFrame(
        {"Return": [0.1], "Entry Timestamp": [idx[0]], "Exit Timestamp": [idx[2]],
         "Status": ["Closed"]}
    )
    assert np.isnan(_trade_hold_bars(ok, dup)).all()
    assert _hold_stats(_trade_hold_bars(ok, dup)) == (None, None, None)


# ------------------------------------------------- sweep mechanics
# Intrabar daily stops, spread on stop exits, closed-trade ranking, and the mechanics stamp.

#: A path whose low breaches the stop on row 3 while every close stays at 100.
_STOP_LOW = [100.0, 100.0, 100.0, 90.0, 100.0, 100.0]
_SL_FRAC = 0.05          # 5% below the entry FILL (stop_entry_price='fillprice')


def _stop_frames(*, intrabar: bool) -> _Frames:
    """The next-open fill frame with one intrabar dip. ``intrabar=False`` gives vectorbt no real
    low (the close stands in), as a close-only daily sweep would."""
    idx = pd.bdate_range("2024-01-01", periods=len(_FILL_CLOSE))
    close = pd.DataFrame({_SYM: _FILL_CLOSE}, index=idx)
    low = pd.DataFrame({_SYM: _STOP_LOW}, index=idx) if intrabar else close.copy()
    high = close.copy()
    return _Frames(
        close=close,
        high=high,
        low=low,
        open=pd.DataFrame({_SYM: _FILL_CLOSE}, index=idx),
        volume=pd.DataFrame({_SYM: [1000.0] * len(_FILL_CLOSE)}, index=idx),
        intraday=False,
        auction_open=None,
    )


def _stop_signals(frames: _Frames) -> _Signals:
    entries = pd.DataFrame(False, index=frames.close.index, columns=frames.close.columns)
    entries.iloc[_ENTRY_SIGNAL_ROW, 0] = True
    stops = pd.DataFrame(np.nan, index=frames.close.index, columns=frames.close.columns)
    stops.iloc[_ENTRY_SIGNAL_ROW, 0] = _SL_FRAC          # travels with its entry to the fill row
    return _Signals(
        entries=entries,
        exits=pd.DataFrame(False, index=frames.close.index, columns=frames.close.columns),
        sl_stop=stops,
    )


def test_a_daily_stop_fires_on_an_INTRABAR_breach_not_only_on_a_close_through_it():
    """High/low go to vectorbt for daily frames, so a stop behaves like the live resting broker stop;
    with high=low=close the bar's 90.0 low is invisible and the position survives."""
    clock = Clock(time_source=lambda: FIXED_NOW)
    runner = SweepRunner(None, CostModel.from_config(), clock)

    frames = _stop_frames(intrabar=True)
    trades = runner._portfolio(frames, _stop_signals(frames), 0.0).trades.records_readable
    assert len(trades) == 1
    assert trades["Status"].astype(str).str.lower().iloc[0] == "closed"
    assert pd.Timestamp(trades["Exit Timestamp"].iloc[0]) == frames.close.index[3]

    close_only = _stop_frames(intrabar=False)
    survived = runner._portfolio(
        close_only, _stop_signals(close_only), 0.0
    ).trades.records_readable
    assert survived["Status"].astype(str).str.lower().iloc[0] == "open"


def _ohlc_frames(o: list[float], h: list[float], low: list[float], c: list[float]) -> _Frames:
    idx = pd.bdate_range("2024-01-01", periods=len(c))
    return _Frames(
        close=pd.DataFrame({_SYM: c}, index=idx),
        high=pd.DataFrame({_SYM: h}, index=idx),
        low=pd.DataFrame({_SYM: low}, index=idx),
        open=pd.DataFrame({_SYM: o}, index=idx),
        volume=pd.DataFrame({_SYM: [1000.0] * len(c)}, index=idx),
        intraday=False,
        auction_open=None,
    )


def test_a_daily_stop_fills_at_the_LEVEL_when_the_bar_closes_through_it_and_at_the_OPEN_on_a_gap():
    """vectorbt substitutes the close for any OHLC leg it is not handed and ``get_stop_price_nb`` tests the
    open before the low/high range, so the real open must be supplied: trading through the stop fills
    at the level (~95.0, not the 88.0 close), gapping through it fills at the gapped open."""
    clock = Clock(time_source=lambda: FIXED_NOW)
    cost_model = CostModel.from_config()
    runner = SweepRunner(None, cost_model, clock)
    half_spread = float(cost_model.half_spread_pct) / 100.0

    def _exit_price(frames: _Frames) -> tuple[float, float]:
        trade = runner._portfolio(frames, _stop_signals(frames), 0.0).trades.records_readable.iloc[0]
        entry_px = float(trade["Avg Entry Price"])
        return entry_px * (1.0 - _SL_FRAC), float(trade["Avg Exit Price"])

    # Row 3 opens above the stop and closes far through it: fills at the level.
    through = _ohlc_frames(
        [100.0, 100.0, 100.0, 99.0, 100.0, 100.0], [100.0, 100.0, 100.0, 99.0, 100.0, 100.0],
        [100.0, 100.0, 100.0, 88.0, 100.0, 100.0], [100.0, 100.0, 100.0, 88.0, 100.0, 100.0],
    )
    level, exit_px = _exit_price(through)
    assert exit_px == pytest.approx(level * (1.0 - half_spread))
    assert exit_px > 90.0                      # not the 88.0 close

    # Row 3 gaps open below the stop: fills at the open only.
    gap = _ohlc_frames(
        [100.0, 100.0, 100.0, 90.0, 100.0, 100.0], [100.0, 100.0, 100.0, 92.0, 100.0, 100.0],
        [100.0, 100.0, 100.0, 88.0, 100.0, 100.0], [100.0, 100.0, 100.0, 91.0, 100.0, 100.0],
    )
    level_gap, exit_gap = _exit_price(gap)
    assert exit_gap == pytest.approx(90.0 * (1.0 - half_spread))
    assert exit_gap < level_gap                # worse than the level, which is what a gap costs


def test_a_stop_exit_pays_the_same_half_spread_as_every_other_exit():
    """stop_exit_price='stopmarket' fills at the stop level with per-leg slippage; vectorbt's 'stoplimit'
    default zeroes slippage, so a stop exit would pay the fee but not the spread."""
    clock = Clock(time_source=lambda: FIXED_NOW)
    cost_model = CostModel.from_config()
    runner = SweepRunner(None, cost_model, clock)
    half_spread = float(cost_model.half_spread_pct) / 100.0
    assert half_spread > 0.0, "a zero measured spread would make this test vacuous"

    frames = _stop_frames(intrabar=True)
    trade = runner._portfolio(frames, _stop_signals(frames), 0.0).trades.records_readable.iloc[0]

    entry_px = float(trade["Avg Entry Price"])
    stop_level = entry_px * (1.0 - _SL_FRAC)            # anchored at the FILL, not a bar's close
    exit_px = float(trade["Avg Exit Price"])
    assert exit_px == pytest.approx(stop_level * (1.0 - half_spread))
    assert exit_px < stop_level                          # the zero-slippage default filled AT it
    assert stop_level - exit_px == pytest.approx(stop_level * half_spread)


def _rank_stat(label: float, *, closed: float | None, all_trades: float, n_closed: int = 5):
    return ParamSetStat(
        params={"axis": label},
        n_trades=n_closed + 5,
        win_rate=0.5,
        expectancy_pct=closed,
        total_return_pct=1.0,
        sharpe=0.5,
        max_drawdown_pct=2.0,
        n_closed=n_closed,
        n_open=5,
        expectancy_closed_pct=closed,
        expectancy_all_pct=all_trades,
    )


def test_the_grid_winner_is_ranked_on_closed_round_trips_not_on_open_marks():
    """Ranking is on the closed-trade mean: a lead from unrealized marks cannot win, and a config that
    closed nothing is not rankable (no fallback to its marks)."""
    marks_only = _rank_stat(1.0, closed=0.5, all_trades=9.0)     # leads only on open marks
    realized = _rank_stat(2.0, closed=2.0, all_trades=1.0)
    nothing_closed = _rank_stat(3.0, closed=None, all_trades=99.0, n_closed=0)

    assert SweepRunner._rank_best([marks_only, realized, nothing_closed]) == {"axis": 2.0}
    assert SweepRunner._rank_best([nothing_closed]) is None


def test_every_sweep_report_stamps_the_three_mechanics_settings(store):
    """The mechanics stamp appears in the report, JSON and notes."""
    start, end = _seed_daily(store)
    clock = Clock(time_source=lambda: FIXED_NOW)
    runner = SweepRunner(store, CostModel.from_config(), clock)

    report = runner.run(
        "mom", start, end, symbols=["AAA", "BBB"],
        param_grid=[{"top_n": 1.0, "rebalance_days": 10.0}],
    )

    assert report.mechanics == MECHANICS_STAMP
    for setting in (STOP_EVALUATION, STOP_EXIT_PRICE, EXPECTANCY_BASIS):
        assert setting in report.mechanics
    assert any(MECHANICS_STAMP in n for n in report.notes)
    md = reports.render_sweep_markdown(report)
    assert MECHANICS_STAMP in md
    assert "expectancy (CLOSED)" in md
    # A report built without the stamp says so in words.
    assert "PRE-2026-09-13" in reports.render_sweep_markdown(
        report.model_copy(update={"mechanics": ""})
    )
