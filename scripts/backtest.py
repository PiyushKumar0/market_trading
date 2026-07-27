#!/usr/bin/env python
"""Phase-1 backtest CLI (§8.2 G1) — sweep + validate + report for the four price baselines.

    python scripts/backtest.py <orb|rsi2|trend|mom|all> --from 2024-01-01 --to 2025-12-31 \
        [--grid-density coarse|medium|fine] [--symbols RELIANCE,TCS] [--index-symbol "NIFTY 50"] \
        [--reports-dir data/reports]

For each strategy it: (1) runs the vectorbt sweep over the §6.3 envelope grid — the sweep reports the
**trial count N** (every configuration evaluated, §6.4 step 1); (2) picks the best config by
cost-adjusted expectancy; (3) runs the ValidationPipeline (anchored walk-forward + skfolio CPCV on
cost-adjusted returns) citing that N; (4) writes the sweep + validation reports to ``data/reports``
and persists the candidate to SQLite ``param_sets``. Honest negative results are surfaced, never
massaged (C9): a negative expectancy / non-promotable verdict is a valid outcome and is printed.

This is a **standalone, blocking** tool — fine here. The in-engine nightly re-optimization path
(Phase 2 ``ChampionChallenger``) must run the identical logic **executor-offloaded**
(``ValidationPipeline.validate`` is already an ``asyncio.to_thread`` wrapper) so it never blocks the
event loop (§2.2). Exit codes: 0 = ran; 2 = no symbols/bars resolved (nothing to backtest).
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

_REPO_SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if _REPO_SRC not in sys.path:  # pragma: no cover - loose-script shim
    sys.path.insert(0, _REPO_SRC)

import engine  # noqa: E402,F401  native import-order guard (sklearn before numba/vectorbt/cvxpy)
from engine.core.clock import Clock  # noqa: E402
from engine.core.config import load_settings  # noqa: E402
from engine.core.db import connect  # noqa: E402
from engine.core.log import configure_logging, get_logger  # noqa: E402
from engine.core.migrations import apply_migrations  # noqa: E402
from engine.learning import reports  # noqa: E402
from engine.learning.sweep import PRICE_BASELINES, SweepRunner, load_envelope  # noqa: E402
from engine.learning.validate import ParamSet, ValidationPipeline  # noqa: E402
from engine.marketdata.store import MarketStore  # noqa: E402
from engine.strategy.cost_model import CostModel  # noqa: E402

_log = get_logger("scripts.backtest")

#: Canonical reference index for the ``rsi2`` regime filter (§6.1 "above rising 50-DMA index").
#: Mirrors ``engine.ops.main.INDEX_SYMBOL`` / the ``FeatureEngine`` default so the DEFAULT backtest
#: validates the SAME pinned rule the live ``Rsi2Scanner`` runs — never the all-regime variant. Pass
#: ``--index-symbol ""`` (empty) to deliberately disable the filter (all-regime; disclosed in notes).
_DEFAULT_INDEX_SYMBOL = "NIFTY 50"


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Phase-1 sweep + validate + report (§8.2).")
    parser.add_argument("strategy", choices=(*PRICE_BASELINES, "all"))
    parser.add_argument("--from", dest="start", required=True, type=_parse_date, help="YYYY-MM-DD")
    parser.add_argument("--to", dest="end", required=True, type=_parse_date, help="YYYY-MM-DD")
    parser.add_argument("--grid-density", choices=("coarse", "medium", "fine"), default="coarse")
    parser.add_argument("--symbols", default=None, help="comma-separated override of the universe")
    parser.add_argument(
        "--index-symbol",
        default=_DEFAULT_INDEX_SYMBOL,
        help=(
            "reference index for the rsi2 regime filter (default: "
            f"{_DEFAULT_INDEX_SYMBOL!r}); pass an empty string to disable the filter (all-regime)"
        ),
    )
    parser.add_argument("--reports-dir", default=None, help="default: <data_dir>/reports")
    return parser


def _default_params(strategy_id: str) -> dict[str, float]:
    """§6.3 envelope defaults (bare keys) for ``strategy_id`` — the champion baseline config."""
    prefix = strategy_id + "."
    env = load_envelope()
    return {k[len(prefix):]: float(v["default"]) for k, v in env.items() if k.startswith(prefix)}


def _resolve_symbols(store: MarketStore, end: date, *, lookback: int = 60) -> list[str]:
    """Most-recent ``universe_daily`` (included) symbols at or before ``end`` (up to ``lookback`` d)."""
    for i in range(lookback + 1):
        rows = store.get_universe_daily(end - timedelta(days=i), included_only=True)
        if rows:
            return [r["symbol"] for r in rows]
    return []


def _sweep_stats_dict(sweep, params: dict[str, float]) -> dict[str, float | None]:
    for s in sweep.stats:
        if s.params == params:
            return {
                "n_trades": float(s.n_trades),
                "win_rate": s.win_rate,
                "sweep_expectancy_pct": s.expectancy_pct,
                "sweep_total_return_pct": s.total_return_pct,
                "sweep_sharpe": s.sharpe,
                "sweep_max_drawdown_pct": s.max_drawdown_pct,
            }
    return {}


def _run_one(
    strategy_id: str,
    runner: SweepRunner,
    pipeline: ValidationPipeline,
    reports_dir: Path,
    *,
    start: date,
    end: date,
    symbols: list[str],
    grid_density: str,
) -> None:
    sweep = runner.run(strategy_id, start, end, symbols=symbols, grid_density=grid_density)
    sweep_art = reports.write_sweep_report(sweep, reports_dir)

    if sweep.n_symbols == 0:
        # Every requested symbol resolved to zero bars — the verdict below is vacuous, say so.
        print(
            f"[{strategy_id}] WARNING: 0 of {len(symbols)} requested symbols had any bars in "
            "the window -- check the --symbols value and the store coverage; the validation "
            "below ran on an EMPTY series",
            file=sys.stderr,
        )

    # Surface a silent data shortfall LOUDLY: a request for 2 years that resolves to 6 months of
    # bars (e.g. 1m history shallower than 1d, data.backfill_minute_years) changes what the
    # validation verdict means — the reports record the true span, but the operator must not have
    # to diff dates to notice. 7-day tolerance absorbs holidays/weekends at the window edges.
    _SPAN_TOLERANCE = timedelta(days=7)
    shortfall = []
    if sweep.data_start is not None and sweep.data_start - start > _SPAN_TOLERANCE:
        shortfall.append(f"bars start {sweep.data_start} vs requested {start}")
    if sweep.data_end is not None and end - sweep.data_end > _SPAN_TOLERANCE:
        shortfall.append(f"bars end {sweep.data_end} vs requested {end}")
    if shortfall:
        # ASCII only: this goes to a Windows console that may be cp1252 (em-dash prints as '?').
        print(
            f"[{strategy_id}] WARNING: resolved data span is narrower than requested: "
            + "; ".join(shortfall)
            + " (backfill more history? see scripts/backfill.py seed --minute-years/--daily-years)",
            file=sys.stderr,
        )

    best = sweep.best_params or _default_params(strategy_id)
    params = ParamSet(
        strategy_id=strategy_id,
        params=best,
        trial_count_n=sweep.trial_count_n,           # §6.4: the cited N = every config evaluated
        sweep_stats=_sweep_stats_dict(sweep, best),
    )
    report = pipeline.validate_sync(strategy_id, params)
    val_art = reports.write_report(report, reports_dir)

    verdict = "PROMOTABLE" if report.promotable else "NOT PROMOTABLE"
    exp = "—" if report.expectancy_pct is None else f"{report.expectancy_pct:+.4f}%/day"
    frac = (
        "n/a"
        if report.cpcv_fold_pass_fraction is None
        else f"{report.cpcv_fold_pass_fraction:.1%}"
    )
    bar = "n/a" if report.fold_pass_min is None else f"{report.fold_pass_min:.0%}"
    print(
        f"[{strategy_id}] N={sweep.trial_count_n} best={best}"
        f" expectancy={exp} CPCV_pass={frac}/{bar} -> {verdict}"
    )
    print(f"    sweep:  {sweep_art.markdown}")
    print(f"    report: {val_art.markdown}")
    if not report.promotable:
        for r in report.reasons:
            print(f"    reason: {r}")


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    args = _build_parser().parse_args(argv)
    # Empty ``--index-symbol ""`` is the explicit opt-out (all-regime); anything else (incl. the
    # canonical default) engages the pinned rsi2 regime filter.
    index_symbol = args.index_symbol or None

    settings = load_settings()
    clock = Clock()
    store = MarketStore.from_settings(settings, clock).open()
    conn = connect(settings.sqlite_path())
    apply_migrations(conn)
    reports_dir = (
        Path(args.reports_dir)
        if args.reports_dir
        else settings.resolved_data_dir() / "reports"
    )

    try:
        symbols = (
            [s.strip() for s in args.symbols.split(",") if s.strip()]
            if args.symbols
            else _resolve_symbols(store, args.end)
        )
        # Shell-expansion footgun: `--symbols $syms` under cmd.exe passes the LITERAL text `$syms`
        # (only PowerShell/bash expand it), so the backtest silently runs on zero real symbols.
        suspicious = [s for s in symbols if s.startswith(("$", "%")) or "%" in s]
        if suspicious:
            print(
                f"ERROR: --symbols contains unexpanded shell variable(s): {suspicious} -- "
                "run from PowerShell (where $syms expands) or pass the comma-separated list "
                "explicitly; nothing was backtested",
                file=sys.stderr,
            )
            return 2
        if not symbols:
            print(
                "no symbols resolved (empty universe_daily and no --symbols) — nothing to backtest",
                file=sys.stderr,
            )
            return 2

        cost_model = CostModel.from_config()
        runner = SweepRunner(store, cost_model, clock, index_symbol=index_symbol)
        pipeline = ValidationPipeline(
            returns_provider=runner.returns_for,
            clock=clock,
            conn=conn,
            reports_dir=reports_dir,
        )
        targets = list(PRICE_BASELINES) if args.strategy == "all" else [args.strategy]
        for strat in targets:
            _run_one(
                strat, runner, pipeline, reports_dir,
                start=args.start, end=args.end, symbols=symbols, grid_density=args.grid_density,
            )
        conn.commit()
    finally:
        conn.close()
        store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
