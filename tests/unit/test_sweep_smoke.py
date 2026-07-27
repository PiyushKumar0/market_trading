"""Mini end-to-end sweep smoke test (needs_heavy_deps — vectorbt + skfolio).

Synthetic daily bars, a TINY grid (2 configs), one strategy: proves the sweep runs vectorbt, reports
the trial count N = grid cardinality, feeds ``returns_for`` into the ValidationPipeline, and the
report cites N. Must run in seconds (tiny data + tiny grid).
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import pandas as pd
import pytest

from engine.core.clock import IST, Clock

pytest.importorskip("vectorbt")
pytest.importorskip("skfolio")

from engine.learning.sweep import SweepRunner, build_param_grid  # noqa: E402
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
