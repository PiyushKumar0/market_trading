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

_BACKTEST_PATH = Path(__file__).resolve().parents[2] / "scripts" / "backtest.py"
_spec = importlib.util.spec_from_file_location("mt_backtest", _BACKTEST_PATH)
bt = importlib.util.module_from_spec(_spec)
sys.modules["mt_backtest"] = bt
_spec.loader.exec_module(bt)

from engine.learning.sweep import SweepReport  # noqa: E402 - after the loose-script shim above

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
