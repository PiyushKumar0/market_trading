"""Mini end-to-end sweep smoke test (needs_heavy_deps — vectorbt + skfolio).

Synthetic daily bars, a TINY grid (2 configs), one strategy: proves the sweep runs vectorbt, reports
the trial count N = grid cardinality, feeds ``returns_for`` into the ValidationPipeline, and the
report cites N. Must run in seconds (tiny data + tiny grid).

Also home to the WO-2 (2026-08-13) **fill-mechanics regression test**: a synthetic price path where
same-bar-close and next-bar-open fills differ MATERIALLY, pinning that the sweep produces the
next-open number. That defect (``price=None`` ⇒ vectorbt's ``np.inf`` ⇒ the signal bar's own close)
invalidated every sweep/CPCV report generated before 2026-08-13.
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

from engine.learning.sweep import (  # noqa: E402
    REFERENCE_NOTIONAL_DEFAULT,
    SweepRunner,
    _Frames,
    _Signals,
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

    assert report.trial_count_n == 2               # grid cardinality = the §6.4 cited N
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
    assert result.fold_pass_min == 0.60            # N=2 ⇒ 60% bar (§9.1)
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


# --------------------------------------------------------------------------- WO-2 fill mechanics
_SYM = "ZZ"

#: A path built so the two mechanics disagree by ~11 percentage points on ONE trade.
#: close is flat at 100 everywhere, so a same-bar-CLOSE fill buys at 100 and sells at 100 (0% gross);
#: the next bar after the entry signal OPENS at 90, so a next-bar-OPEN fill buys at 90 and sells at
#: 100 (+11.1% gross). Nothing else about the frame differs.
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
    """WO-2 (i) regression pin: the fill price is the NEXT session's OPEN, not the signal bar's close.

    Same-bar-close (the pre-2026-08-13 mechanics) would score this trade ~0% gross and NEGATIVE net;
    next-bar-open scores ~+11% gross. The assertions below fail on the old mechanics.
    """
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
    # the order lands on the bar AFTER the signal, and pays that bar's OPEN (± half the spread)
    assert pd.Timestamp(trade["Entry Timestamp"]) == idx[_ENTRY_SIGNAL_ROW + 1]
    assert pd.Timestamp(trade["Exit Timestamp"]) == idx[_EXIT_SIGNAL_ROW + 1]
    assert float(trade["Avg Entry Price"]) == pytest.approx(90.0 * (1.0 + half_spread))
    assert float(trade["Avg Exit Price"]) == pytest.approx(100.0 * (1.0 - half_spread))
    # ...NOT the signal bar's close (100.0 both legs), which is what price=None resolves to
    assert float(trade["Avg Entry Price"]) != pytest.approx(100.0, abs=1e-6)

    # and the P&L difference is material, not a rounding artifact
    assert float(trade["Return"]) > 0.10
    same_bar_close_return = 0.0 - 2.0 * fee - 2.0 * half_spread   # what the old mechanics scored
    assert same_bar_close_return < 0.0
    assert float(trade["Return"]) - same_bar_close_return > 0.10


def test_next_open_fill_charges_fees_and_half_the_spread_on_every_leg():
    """WO-2 (ii): spread enters the sweep as vectorbt slippage — half the measured quoted spread per
    leg, so a round trip pays the full spread ON TOP of the statutory fee."""
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
    """WO-2 (iii): per-symbol init_cash defaults to reference_notional (₹20,000), not ₹100,000."""
    clock = Clock(time_source=lambda: FIXED_NOW)
    runner = SweepRunner(None, CostModel.from_config(), clock)
    assert runner._init_cash == float(REFERENCE_NOTIONAL_DEFAULT) == 20_000.0

    frames = _fill_frames()
    pf = runner._portfolio(frames, _fill_signals(frames), 0.0)
    trade = pf.trades.records_readable.iloc[0]
    notional = float(trade["Size"]) * float(trade["Avg Entry Price"])
    assert notional == pytest.approx(20_000.0, rel=0.01)      # all-in at the reference size

    # the proportional cost surface makes the RETURN series scale-invariant — what the old
    # 100k-vs-20k mismatch broke was the fee CONSTANT's calibration, not the arithmetic.
    big = SweepRunner(None, CostModel.from_config(), clock, init_cash=100_000.0)
    fee = 0.001
    r_small = runner._portfolio(frames, _fill_signals(frames), fee).returns()[_SYM].to_numpy()
    r_big = big._portfolio(frames, _fill_signals(frames), fee).returns()[_SYM].to_numpy()
    assert np.allclose(r_small, r_big)
