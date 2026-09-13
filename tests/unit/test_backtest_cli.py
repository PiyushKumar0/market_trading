"""scripts/backtest.py argument defaults (§6.1/§8.2 G1).

The ``rsi2`` baseline is pinned to the "above a rising 50-DMA index" regime filter (§6.1 row 2); the
live ``Rsi2Scanner`` always applies it. The backtest CLI must therefore validate that SAME rule by
DEFAULT — a ``--index-symbol`` that defaults to nothing silently disables the sweep's regime gate
(``SweepRunner(index_symbol=None)``), promoting params measured on an all-regime strategy the live
scanner never runs. These tests pin the canonical default and the explicit all-regime opt-out.
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import date, datetime
from pathlib import Path

import pytest

_BACKTEST_PATH = Path(__file__).resolve().parents[2] / "scripts" / "backtest.py"
_spec = importlib.util.spec_from_file_location("mt_backtest", _BACKTEST_PATH)
bt = importlib.util.module_from_spec(_spec)
sys.modules["mt_backtest"] = bt
_spec.loader.exec_module(bt)

from engine.learning.sweep import (  # noqa: E402 - after the loose-script shim above
    MECHANICS_STAMP,
    ParamSetStat,
    SweepReport,
)
from engine.learning.validate import MARGIN_FLOOR_DAYS  # noqa: E402

_BASE_ARGV = ["rsi2", "--from", "2024-01-01", "--to", "2025-12-31"]


def test_default_index_symbol_engages_the_rsi2_regime_filter():
    """Default invocation resolves a real reference index, so the sweep applies the pinned
    'rising 50-DMA index' gate (SweepRunner receives a non-None index_symbol)."""
    args = bt._build_parser().parse_args(_BASE_ARGV)
    assert args.index_symbol == "NIFTY 50"
    assert args.index_symbol == bt._DEFAULT_INDEX_SYMBOL
    assert (args.index_symbol or None) is not None            # main() forwards it (filter engaged)


def test_empty_index_symbol_is_the_explicit_all_regime_optout():
    """``--index-symbol ""`` is the deliberate opt-out: main() normalizes it to None so the sweep
    runs all-regime (disclosed in the report notes), never silently by default."""
    args = bt._build_parser().parse_args([*_BASE_ARGV, "--index-symbol", ""])
    assert args.index_symbol == ""
    assert (args.index_symbol or None) is None                # the all-regime opt-out


def test_explicit_index_symbol_override_is_honored():
    args = bt._build_parser().parse_args([*_BASE_ARGV, "--index-symbol", "NIFTY BANK"])
    assert args.index_symbol == "NIFTY BANK"


# ============================================================================ WO-3 adjacency wiring
#
# "for the DAILY strategies only (rsi2/trend/mom — NOT orb), also run the adjacent grid density
# (coarse<->medium) and pass adjacent_density + adjacent_winner (+ cost_floor_pct from the sweep)
# into ParamSet so the validate report's WinnerStability populates. orb stays single-density (flag
# reads not-assessed)." — the runner is mocked throughout (no real sweeps); these tests assert only
# WHAT _run_one calls and WHAT it hands to ParamSet.


def _fake_sweep(
    strategy_id: str, grid_density: str, best_params: dict[str, float] | None
) -> SweepReport:
    return SweepReport(
        strategy_id=strategy_id,
        product="MIS" if strategy_id == "orb" else "CNC",
        grid_density=grid_density,
        trial_count_n=4,
        n_symbols=1,
        symbols=["TCS"],
        data_start=date(2024, 1, 1),
        data_end=date(2024, 12, 31),
        reference_notional="20000",
        per_side_fee_pct=0.05,
        cost_floor_pct=0.12,
        stats=[],
        best_params=best_params,
        generated_at=datetime(2026, 8, 13, 10, 0),
    )


class _FakeRunner:
    """Records every ``.run()`` call — (strategy_id, grid_density) is the whole adjacency
    assertion surface; the returned report content is otherwise irrelevant."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def run(self, strategy_id, start, end, *, symbols, grid_density):  # noqa: ANN001, ANN201
        self.calls.append((strategy_id, grid_density))
        winner = {"x": 1.0} if grid_density == "coarse" else {"x": 2.0}
        return _fake_sweep(strategy_id, grid_density, winner)


class _FakeValidationReport:
    promotable = False
    expectancy_pct = None
    cpcv_fold_pass_fraction = None
    fold_pass_min = None
    margin_floor_pct_per_day = None
    margin_floor_days = None
    realized_hold = None            # R2 reporting block — absent on this stub
    realized_hold_cells = ()
    reasons = ["stub report — fake pipeline, no real validation"]


class _FakePipeline:
    """Records every ``ParamSet`` handed to ``validate_sync`` — what ``_run_one`` actually wired."""

    def __init__(self) -> None:
        self.received: list = []

    def validate_sync(self, strategy_id, params):  # noqa: ANN001, ANN201
        self.received.append(params)
        return _FakeValidationReport()


def _stub_report_writers(monkeypatch, tmp_path):
    """``_run_one`` writes both a sweep and a validation report to disk; stub both so the test
    exercises only the adjacency wiring, never the filesystem/schema of a real report."""
    stub = bt.reports.ReportArtifacts(markdown=tmp_path / "stub.md", json=tmp_path / "stub.json")
    monkeypatch.setattr(bt.reports, "write_sweep_report", lambda *a, **k: stub)
    monkeypatch.setattr(bt.reports, "write_report", lambda *a, **k: stub)


def _run_one(strategy_id: str, grid_density: str, *, run_adjacent: bool, tmp_path, monkeypatch):
    _stub_report_writers(monkeypatch, tmp_path)
    runner = _FakeRunner()
    pipeline = _FakePipeline()
    bt._run_one(
        strategy_id, runner, pipeline, tmp_path,
        start=date(2024, 1, 1), end=date(2024, 12, 31), symbols=["TCS"],
        grid_density=grid_density, run_adjacent=run_adjacent,
    )
    return runner, pipeline


def test_daily_baselines_excludes_orb():
    assert bt.DAILY_BASELINES == {"rsi2", "trend", "mom"}


def test_daily_strategy_runs_adjacent_density_and_wires_paramset(tmp_path, monkeypatch):
    runner, pipeline = _run_one("trend", "coarse", run_adjacent=True, tmp_path=tmp_path,
                                 monkeypatch=monkeypatch)

    assert runner.calls == [("trend", "coarse"), ("trend", "medium")]   # primary + THE adjacent call
    params = pipeline.received[-1]
    assert params.grid_density == "coarse"
    assert params.adjacent_density == "medium"
    assert params.adjacent_winner == {"x": 2.0}          # the adjacent (medium) sweep's best_params
    assert params.cost_floor_pct == 0.12                 # from the PRIMARY sweep


def test_orb_never_runs_an_adjacent_sweep(tmp_path, monkeypatch):
    runner, pipeline = _run_one("orb", "coarse", run_adjacent=True, tmp_path=tmp_path,
                                 monkeypatch=monkeypatch)

    assert runner.calls == [("orb", "coarse")]           # single-density, always — orb is excluded
    params = pipeline.received[-1]
    assert params.grid_density == "coarse"
    assert params.adjacent_density is None                # WinnerStability reads "not assessed"
    assert params.adjacent_winner is None
    assert params.cost_floor_pct == 0.12                  # still populated — unconditional on strategy


def test_no_adjacent_flag_skips_the_extra_sweep_for_a_daily_strategy(tmp_path, monkeypatch):
    runner, pipeline = _run_one("mom", "coarse", run_adjacent=False, tmp_path=tmp_path,
                                 monkeypatch=monkeypatch)

    assert runner.calls == [("mom", "coarse")]
    assert pipeline.received[-1].adjacent_density is None
    assert pipeline.received[-1].adjacent_winner is None


def test_fine_density_has_no_adjacency_wiring_even_for_a_daily_strategy(tmp_path, monkeypatch):
    # Only coarse<->medium is the wired pair (manager's scope); `fine` is a valid density with no
    # adjacency partner, so the daily strategy still runs single-density.
    runner, pipeline = _run_one("rsi2", "fine", run_adjacent=True, tmp_path=tmp_path,
                                 monkeypatch=monkeypatch)

    assert runner.calls == [("rsi2", "fine")]
    assert pipeline.received[-1].adjacent_density is None


def test_no_adjacent_cli_flag_defaults_false_and_parses():
    assert bt._build_parser().parse_args(_BASE_ARGV).no_adjacent is False
    assert bt._build_parser().parse_args([*_BASE_ARGV, "--no-adjacent"]).no_adjacent is True


# ====================================================================== R2 --margin-floor-days wiring
#
# The WO-3 margin floor is cost_floor / <days>. The 20 is the §7.1 SWING holding cap; a positional
# leg (trend, max_holding.position_trading_days = 120) is measured against a 6x-too-high bar at that
# denominator. The CLI now carries the denominator, and these tests pin (a) the default is still 20,
# so every pre-existing invocation is byte-identical, and (b) the value REACHES the
# ValidationPipeline that runs the promotion rule — a flag parsed but not threaded is the whole risk.


def test_margin_floor_days_defaults_to_the_wo3_constant():
    args = bt._build_parser().parse_args(_BASE_ARGV)
    assert args.margin_floor_days == MARGIN_FLOOR_DAYS == 20


def test_margin_floor_days_parses_the_positional_holding_cap():
    args = bt._build_parser().parse_args([*_BASE_ARGV, "--margin-floor-days", "120"])
    assert args.margin_floor_days == 120


@pytest.mark.parametrize("bad", ["0", "-5", "notanumber"])
def test_margin_floor_days_rejects_a_non_positive_denominator(bad):
    # Rejected at PARSE time: 0 would otherwise divide-by-zero inside margin_floor_pct_per_day
    # after a multi-minute sweep had already run.
    with pytest.raises(SystemExit):
        bt._build_parser().parse_args([*_BASE_ARGV, "--margin-floor-days", bad])


class _CapturingPipeline:
    """Stands in for ValidationPipeline in ``main()`` — records the kwargs it was constructed with."""

    def __init__(self, **kwargs) -> None:
        _CapturingPipeline.kwargs = kwargs


def _main_pipeline_kwargs(
    monkeypatch, tmp_path, extra_argv: list[str], *, strategy: str = "trend"
) -> dict:
    """Run ``main()`` with every I/O collaborator stubbed and return the ValidationPipeline kwargs.

    Nothing here touches DuckDB/SQLite/vectorbt: the assertion surface is the plumbing between
    ``--margin-floor-days`` and the pipeline that enforces the floor.
    """

    class _Store:
        @staticmethod
        def from_settings(settings, clock, **kw):        # noqa: ANN001, ANN205
            return _Store()

        def open(self):                                   # noqa: ANN201
            return self

        def close(self) -> None:
            pass

    class _Conn:
        def commit(self) -> None:
            pass

        def close(self) -> None:
            pass

    class _Settings:
        def sqlite_path(self):                            # noqa: ANN201
            return ":memory:"

        def resolved_data_dir(self) -> Path:
            return tmp_path

    class _CostModel:
        @staticmethod
        def from_config():                                # noqa: ANN205
            return _CostModel()

    class _Runner:
        def __init__(self, *a, **k) -> None:
            pass

        def returns_for(self, strategy_id, params):       # noqa: ANN001, ANN201
            raise AssertionError("no sweep should run in this test")

    monkeypatch.setattr(bt, "configure_logging", lambda *a, **k: None)
    monkeypatch.setattr(bt, "load_settings", lambda: _Settings())
    monkeypatch.setattr(bt, "MarketStore", _Store)
    monkeypatch.setattr(bt, "connect", lambda *a, **k: _Conn())
    monkeypatch.setattr(bt, "apply_migrations", lambda *a, **k: None)
    monkeypatch.setattr(bt, "CostModel", _CostModel)
    monkeypatch.setattr(bt, "SweepRunner", _Runner)
    monkeypatch.setattr(bt, "ValidationPipeline", _CapturingPipeline)
    monkeypatch.setattr(bt, "_run_one", lambda *a, **k: None)
    _CapturingPipeline.kwargs = {}
    argv = [
        strategy, "--from", "2024-01-01", "--to", "2025-12-31",
        "--symbols", "TCS", "--reports-dir", str(tmp_path), *extra_argv,
    ]
    assert bt.main(argv) == 0
    return _CapturingPipeline.kwargs


def test_margin_floor_days_reaches_the_validation_pipeline(tmp_path, monkeypatch):
    kwargs = _main_pipeline_kwargs(monkeypatch, tmp_path, ["--margin-floor-days", "120"])
    assert kwargs["margin_floor_days"] == 120


def test_margin_floor_days_default_leaves_existing_invocations_unchanged(tmp_path, monkeypatch):
    kwargs = _main_pipeline_kwargs(monkeypatch, tmp_path, [])
    assert kwargs["margin_floor_days"] == MARGIN_FLOOR_DAYS == 20


def test_all_run_with_a_non_default_denominator_warns(tmp_path, monkeypatch, capsys):
    """One denominator across `all` scales every leg to one leg's horizon — which can only LOOSEN
    the floor for the shorter-held ones. Not blocked (research CLI), but never silent."""
    kwargs = _main_pipeline_kwargs(
        monkeypatch, tmp_path, ["--margin-floor-days", "120"], strategy="all"
    )
    assert kwargs["margin_floor_days"] == 120
    assert "applies to EVERY strategy" in capsys.readouterr().err

    _main_pipeline_kwargs(monkeypatch, tmp_path, [], strategy="all")
    assert capsys.readouterr().err == ""          # the default denominator warns about nothing


# ============================================ R2 (2026-09-12): realized-hold + survivorship wiring
#
# The 2026-09-12 trend run was promoted against a floor spread over the 120-session §7.1 CAP while
# no artifact recorded that its median trade was held 33 sessions (where the same edge clears by
# 1.07x, not 3.89x), and no artifact said the 200-name universe is a present-day snapshot applied
# backwards. Both now travel from the sweep into the ParamSet the pipeline validates. Neither is an
# input to the promotion rule — these tests pin the PLUMBING; test_validation.py pins that the
# verdict does not move.


def _stat(strategy_params: dict[str, float], **kw) -> ParamSetStat:
    base = dict(
        # WO-M: expectancy_pct IS the closed-trade mean; the all-trades figure rides beside it, and
        # the win rate is reported on both populations so the pair never mixes bases.
        params=strategy_params, n_trades=303, win_rate=0.3729, expectancy_pct=1.841,
        total_return_pct=5.026, sharpe=0.98, max_drawdown_pct=3.24,
        n_closed=273, n_open=30, expectancy_closed_pct=1.841, expectancy_all_pct=3.626,
        win_rate_closed=0.4176,
        hold_bars_mean=48.64, hold_bars_median=33.0, hold_bars_p90=121.0,
        hold_bars_mean_closed=45.05, hold_bars_median_closed=31.0, hold_bars_p90_closed=111.2,
    )
    base.update(kw)
    return ParamSetStat(**base)


def _sweep_with_stats(strategy_id: str = "trend", **report_kw) -> SweepReport:
    winner = {"adx_min": 20.0, "trail_atr_mult": 4.0}
    kw = dict(
        strategy_id=strategy_id,
        product="MIS" if strategy_id == "orb" else "CNC",
        grid_density="coarse",
        trial_count_n=9,
        n_symbols=200,
        symbols=["TCS"],
        data_start=date(2024, 1, 1),
        data_end=date(2025, 12, 31),
        reference_notional="20000",
        per_side_fee_pct=0.1496,
        cost_floor_pct=0.3192,
        stats=[_stat(winner), _stat({"adx_min": 30.0, "trail_atr_mult": 1.5}, n_trades=0)],
        best_params=winner,
        mechanics=MECHANICS_STAMP,
        generated_at=datetime(2026, 9, 12, 1, 6, 53),
    )
    kw.update(report_kw)
    return SweepReport(**kw)


def test_sweep_stats_dict_carries_the_closed_only_expectancy_beside_the_headline():
    sweep = _sweep_with_stats()
    d = bt._sweep_stats_dict(sweep, sweep.best_params)
    # WO-M: every per-trade key NAMES its population. This dict is persisted verbatim into
    # param_sets.validation_report, so the two keys whose MEANING would otherwise have silently
    # changed on 2026-09-13 (all-trades -> closed-only) under an unchanged name are RETIRED, not
    # redefined: a query spanning both sides of the fix must never compare two statistics as one.
    assert "sweep_expectancy_pct" not in d
    assert "win_rate" not in d
    assert d["sweep_expectancy_closed_pct"] == 1.841   # CLOSED round trips only (the ranked one)
    assert d["sweep_expectancy_all_pct"] == 3.626      # ALL trades, incl. the 30 still open
    assert d["win_rate_closed"] == 0.4176 and d["win_rate_all"] == 0.3729
    assert d["n_trades"] == 303.0 and d["n_closed"] == 273.0


def test_realized_hold_maps_the_winning_configs_measured_sessions():
    sweep = _sweep_with_stats()
    hold = bt._realized_hold(sweep, sweep.best_params)
    assert hold is not None
    assert (hold.n_trades, hold.n_closed, hold.n_open) == (303, 273, 30)
    assert hold.median_sessions == 33.0 and hold.mean_sessions == 48.64
    assert hold.median_sessions_closed == 31.0 and hold.p90_sessions_closed == 111.2
    # RealizedHold's field meanings are unchanged by WO-M: _pct is ALL trades, _closed_pct closed.
    assert hold.expectancy_per_trade_pct == 3.626
    assert hold.expectancy_per_trade_closed_pct == 1.841


def test_realized_hold_is_withheld_for_an_intraday_sweep_whose_bars_are_not_sessions():
    """orb's hold_bars_* are 1-MINUTE bars. Labelling them sessions would make the re-based floor
    ~375x too strict and the headroom multiple meaningless, so the cells are simply not emitted."""
    sweep = _sweep_with_stats("orb", bar_unit="1m bar")
    assert sweep.bar_unit == "1m bar"
    assert bt._realized_hold(sweep, sweep.best_params) is None


def test_realized_hold_is_none_when_the_winning_config_scored_no_trades():
    sweep = _sweep_with_stats()
    assert bt._realized_hold(sweep, {"adx_min": 30.0, "trail_atr_mult": 1.5}) is None
    assert bt._realized_hold(sweep, {"not": 1.0}) is None       # config absent from the grid


def test_run_one_threads_the_hold_and_the_survivorship_flag_into_the_validated_paramset(
    tmp_path, monkeypatch
):
    _stub_report_writers(monkeypatch, tmp_path)

    class _Runner:
        def run(self, strategy_id, start, end, *, symbols, grid_density):  # noqa: ANN001, ANN201
            return _sweep_with_stats(strategy_id)

    pipeline = _FakePipeline()
    bt._run_one(
        "trend", _Runner(), pipeline, tmp_path,
        start=date(2024, 1, 1), end=date(2025, 12, 31), symbols=["TCS"],
        grid_density="coarse", run_adjacent=False,
    )
    ps = pipeline.received[0]
    assert ps.realized_hold is not None
    assert ps.realized_hold.median_sessions == 33.0
    # the platform stores no point-in-time index membership, so this can only be True today
    assert ps.population_is_survivorship_tainted_proxy is True
    # WO-M: the sweep's mechanics stamp reaches the validated ParamSet, so the verdict artifact
    # names the mechanics its returns were produced under instead of leaving it to the reader.
    assert ps.sweep_mechanics == MECHANICS_STAMP
    assert ps.params_are_grid_winner is True


def test_a_grid_that_ranked_nothing_validates_the_defaults_but_never_looks_like_a_winner(
    tmp_path, monkeypatch, capsys
):
    """WO-M item (iii) made an unrankable config drop OUT of the ranking, so a window shorter than
    the strategy's realized hold (or a vectorbt schema change that makes every open/closed split
    unreadable) can leave ``best_params`` None. The CLI still validates the Section 6.3 defaults —
    that is the long-standing fallback and nothing here gates on it — but a defaults verdict must be
    impossible to mistake for a swept one, on the console line AND in the persisted ParamSet."""
    _stub_report_writers(monkeypatch, tmp_path)

    class _NoWinnerRunner:
        def run(self, strategy_id, start, end, *, symbols, grid_density):  # noqa: ANN001, ANN201
            return _sweep_with_stats(strategy_id, best_params=None)

    pipeline = _FakePipeline()
    bt._run_one(
        "trend", _NoWinnerRunner(), pipeline, tmp_path,
        start=date(2026, 6, 1), end=date(2026, 9, 12), symbols=["TCS"],
        grid_density="coarse", run_adjacent=False,
    )
    ps = pipeline.received[0]
    assert ps.params_are_grid_winner is False
    assert ps.params == bt._default_params("trend")          # the envelope defaults, not a winner

    out = capsys.readouterr()
    assert "WARNING: the grid selected NO winner" in out.err
    assert "DEFAULTS(no grid winner)=" in out.out
    assert " best=" not in out.out                            # never the grid-winner rendering
