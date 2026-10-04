#!/usr/bin/env python
"""Phase-1 backtest CLI (§8.2 G1) — sweep + validate + report for the four price baselines.

    python scripts/backtest.py <orb|rsi2|trend|mom|all> --from 2024-01-01 --to 2025-12-31 \
        [--grid-density coarse|medium|fine] [--symbols RELIANCE,TCS] [--index-symbol "NIFTY 50"] \
        [--reports-dir data/reports] [--margin-floor-days 20]

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
from engine.learning.validate import (  # noqa: E402
    MARGIN_FLOOR_DAYS,
    ParamSet,
    RealizedHold,
    ValidationPipeline,
)
from engine.marketdata.store import MarketStore  # noqa: E402
from engine.strategy.cost_model import CostModel  # noqa: E402

_log = get_logger("scripts.backtest")

#: Canonical reference index for the ``rsi2`` regime filter (§6.1 "above rising 50-DMA index").
#: Mirrors ``engine.ops.main.INDEX_SYMBOL`` / the ``FeatureEngine`` default so the DEFAULT backtest
#: validates the SAME pinned rule the live ``Rsi2Scanner`` runs — never the all-regime variant. Pass
#: ``--index-symbol ""`` (empty) to deliberately disable the filter (all-regime; disclosed in notes).
_DEFAULT_INDEX_SYMBOL = "NIFTY 50"

#: WO-3 winner-stability adjacency (manager-decided wiring, 2026-08-13): DAILY strategies only —
#: ``orb`` is the intraday/1m-scale baseline and stays single-density (its ``ParamSet`` never gets
#: an ``adjacent_density``, so the validate report's ``WinnerStability`` reads "not assessed", by
#: design). ``PRICE_BASELINES`` minus ``orb``.
DAILY_BASELINES: frozenset[str] = frozenset(PRICE_BASELINES) - {"orb"}

#: The coarse<->medium pair the manager specified. ``fine`` has no adjacency wiring (out of scope
#: for this change) — a ``--grid-density fine`` run still validates, just without the extra sweep.
_ADJACENT_DENSITY: dict[str, str] = {"coarse": "medium", "medium": "coarse"}


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _positive_days(s: str) -> int:
    """argparse type for ``--margin-floor-days`` — the floor's denominator in SESSIONS.

    Rejected at parse time rather than deep inside ``margin_floor_pct_per_day`` (which raises on
    < 1): a zero would otherwise surface as a divide-by-zero traceback after a multi-minute sweep.
    """
    days = int(s)
    if days < 1:
        raise argparse.ArgumentTypeError(f"must be >= 1 session, got {days}")
    return days


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
    parser.add_argument(
        "--margin-floor-days",
        type=_positive_days,
        default=MARGIN_FLOOR_DAYS,
        help=(
            "sessions the WO-3 margin floor spreads ONE round trip over: the bar is "
            f"cost_floor/<days> per day (default: {MARGIN_FLOOR_DAYS}, the 7.1 swing holding cap). "
            "Set it to the leg's real holding cap -- e.g. 120 for the positional trend leg "
            "(limits.yaml max_holding.position_trading_days) -- or the floor overstates what the "
            "strategy must earn per day. The value used is printed in the validation report"
        ),
    )
    parser.add_argument(
        "--no-adjacent", action="store_true",
        help=(
            "skip the WO-3 adjacent-grid-density winner-stability sweep for daily strategies "
            "(rsi2/trend/mom); orb never runs one regardless of this flag"
        ),
    )
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


def _best_stat(sweep, params: dict[str, float]):
    """The ``ParamSetStat`` row for ``params`` in ``sweep`` (``None`` when the grid never scored it)."""
    return next((s for s in sweep.stats if s.params == params), None)


def _sweep_stats_dict(sweep, params: dict[str, float]) -> dict[str, float | None]:
    s = _best_stat(sweep, params)
    if s is None:
        return {}
    return {
        "n_trades": float(s.n_trades),
        "n_closed": float(s.n_closed),
        # WO-M (iii): every per-trade key here NAMES its population. This dict is persisted verbatim
        # into ``param_sets.validation_report``, where a query spans rows written on both sides of
        # 2026-09-13 — so the pre-fix keys ``win_rate`` and ``sweep_expectancy_pct``, whose meaning
        # would have silently changed from all-trades to closed-only under the same name, are RETIRED
        # rather than redefined. A query for a retired key returns the pre-fix rows only, which is
        # unambiguous; a redefined one would have compared two different statistics as if they were
        # one. The mechanics stamp lives in a sibling field a SQL/JSON comparison would not read.
        "win_rate_closed": s.win_rate_closed,
        "win_rate_all": s.win_rate,
        "sweep_expectancy_closed_pct": s.expectancy_closed_pct,
        "sweep_expectancy_all_pct": s.expectancy_all_pct,
        "sweep_total_return_pct": s.total_return_pct,
        "sweep_sharpe": s.sharpe,
        "sweep_max_drawdown_pct": s.max_drawdown_pct,
    }


def _realized_hold(sweep, params: dict[str, float]) -> RealizedHold | None:
    """R2 (2026-09-12): the winning config's MEASURED holding distribution, in SESSIONS.

    Returns ``None`` for an intraday sweep (``orb``), whose ``hold_bars_*`` are 1-minute bars — the
    WO-3 floor's denominator is sessions, and re-labelling 1m bars as sessions would produce a floor
    ~375× too strict and a headroom multiple to match. ``None`` also when the config scored no
    trades: there is no realized horizon to report, and the reporting cells are simply omitted.
    """
    s = _best_stat(sweep, params)
    if s is None or not s.n_trades or sweep.bar_unit != "session":
        return None
    return RealizedHold(
        n_trades=s.n_trades,
        n_closed=s.n_closed,
        n_open=s.n_open,
        # RealizedHold keeps its field meanings: ``_pct`` is the ALL-trades mean, ``_closed_pct``
        # the closed-only one. WO-M changed which of the two the sweep ranks on (the closed one),
        # not what either field holds — so the report can label both correctly.
        expectancy_per_trade_pct=s.expectancy_all_pct,
        expectancy_per_trade_closed_pct=s.expectancy_closed_pct,
        mean_sessions=s.hold_bars_mean,
        median_sessions=s.hold_bars_median,
        p90_sessions=s.hold_bars_p90,
        mean_sessions_closed=s.hold_bars_mean_closed,
        median_sessions_closed=s.hold_bars_median_closed,
        p90_sessions_closed=s.hold_bars_p90_closed,
    )


def _adjacent_winner(
    strategy_id: str,
    runner: SweepRunner,
    *,
    start: date,
    end: date,
    symbols: list[str],
    grid_density: str,
    run_adjacent: bool,
) -> tuple[str | None, dict[str, float] | None]:
    """WO-3 winner-stability input (manager-decided wiring): for a DAILY strategy, ALSO run the
    adjacent grid density (coarse<->medium) and return ``(adjacent_density, adjacent_winner)``.

    ``orb`` and any density outside the coarse/medium pair (e.g. ``fine``) return ``(None, None)`` —
    exactly the "not assessed" input :func:`engine.learning.validate.winner_stability` expects, so
    the validate report's ``WinnerStability`` flag reads "not assessed" rather than a fabricated
    comparison. This is a SECOND full sweep (real cost) — ``--no-adjacent`` skips it.
    """
    if not run_adjacent or strategy_id not in DAILY_BASELINES:
        return None, None
    adjacent_density = _ADJACENT_DENSITY.get(grid_density)
    if adjacent_density is None:
        return None, None
    adjacent_sweep = runner.run(
        strategy_id, start, end, symbols=symbols, grid_density=adjacent_density
    )
    return adjacent_density, adjacent_sweep.best_params


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
    run_adjacent: bool = True,
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

    # The grid can rank NOTHING: since WO-M item (iii) a config that closed no round trip inside the
    # window is excluded rather than ranked on its unrealized marks, so a window shorter than the
    # strategy's realized hold — or any vectorbt schema change that makes every open/closed split
    # unreadable — leaves ``best_params`` None. Falling back to the §6.3 defaults keeps the run
    # producing a verdict, but that verdict is about the DEFAULT config, not about a swept winner,
    # and the console line plus the artifact must both say so. ASCII only (cp1252 console).
    grid_selected_winner = sweep.best_params is not None
    best = sweep.best_params or _default_params(strategy_id)
    if not grid_selected_winner:
        print(
            f"[{strategy_id}] WARNING: the grid selected NO winner -- all {sweep.trial_count_n} "
            "configs closed no round trip inside the window (or their open/closed split was "
            "unreadable), so none was rankable. Validating the Section 6.3 envelope DEFAULTS "
            f"{best} instead; the verdict below is NOT a swept result and the report says so.",
            file=sys.stderr,
        )
    adjacent_density, adjacent_winner = _adjacent_winner(
        strategy_id, runner, start=start, end=end, symbols=symbols,
        grid_density=grid_density, run_adjacent=run_adjacent,
    )
    params = ParamSet(
        strategy_id=strategy_id,
        params=best,
        trial_count_n=sweep.trial_count_n,           # §6.4: the cited N = every config evaluated
        sweep_stats=_sweep_stats_dict(sweep, best),
        cost_floor_pct=sweep.cost_floor_pct,          # WO-3 margin floor: the sweep's OWN measured floor
        grid_density=sweep.grid_density,              # WO-3 winner-stability: this run's density
        adjacent_density=adjacent_density,            # None for orb / a non-adjacent density (WO-3)
        adjacent_winner=adjacent_winner,
        # R2 (2026-09-12), REPORTING ONLY — neither reaches the promotion rule:
        realized_hold=_realized_hold(sweep, best),    # the horizon the floor should be read against
        population_is_survivorship_tainted_proxy=(    # a present-day list applied backwards
            sweep.population_is_survivorship_tainted_proxy
        ),
        # WO-M (2026-09-13): the sweep's mechanics stamp travels into the validation artifact, so a
        # verdict is never read beside one produced under the pre-fix mechanics.
        sweep_mechanics=sweep.mechanics,
        # ... and so does whether these params were RANKED or merely defaulted to (see above).
        params_are_grid_winner=grid_selected_winner,
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
    floor = (
        "n/a"
        if report.margin_floor_pct_per_day is None
        else f"{report.margin_floor_pct_per_day:.5f}%/day over {report.margin_floor_days}d"
    )
    # The params label distinguishes a ranked winner from the defaults fallback ON THE VERDICT LINE
    # itself: this line is what gets pasted into a report, and the stderr warning above may not be.
    label = "best" if grid_selected_winner else "DEFAULTS(no grid winner)"
    print(
        f"[{strategy_id}] N={sweep.trial_count_n} {label}={best}"
        f" expectancy={exp} CPCV_pass={frac}/{bar} margin_floor={floor} -> {verdict}"
    )
    # R2: the verdict line above quotes the floor at the REGISTERED denominator. Print the measured
    # horizon and the floor re-based on it right underneath, so the operator cannot read a pass at a
    # 120-session cap as comfortable without seeing what the median trade was actually held for.
    hold = report.realized_hold
    if hold is not None and hold.median_sessions is not None and hold.mean_sessions is not None:
        print(
            f"    realized hold: median {hold.median_sessions:.1f} / mean "
            f"{hold.mean_sessions:.1f} sessions over {hold.n_trades} trades "
            f"({hold.n_open} still open)"
        )
        for c in report.realized_hold_cells:
            head = "" if c.headroom_x is None else f" ({c.headroom_x:.2f}x headroom)"
            print(
                f"      floor at {c.label}: {c.margin_floor_pct_per_day:.5f}%/day{head}"
            )
    # ASCII-safe: the stamp's separators are printed by the artifacts, not by this console line.
    print(f"    mechanics: {sweep.mechanics.replace(' · ', ' | ').replace(' — ', ' - ')}")
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
    # The denominator is PER-LEG (a leg's own holding cap); one value applied across `all` hands the
    # short-horizon baselines a floor scaled to somebody else's horizon, which only ever LOOSENS it.
    if args.strategy == "all" and args.margin_floor_days != MARGIN_FLOOR_DAYS:
        print(
            f"WARNING: --margin-floor-days {args.margin_floor_days} applies to EVERY strategy in "
            "this 'all' run, including the intraday/swing legs whose holding cap is shorter -- "
            "their margin floor is loosened accordingly. Run one strategy per horizon instead.",
            file=sys.stderr,
        )

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
            margin_floor_days=args.margin_floor_days,
        )
        targets = list(PRICE_BASELINES) if args.strategy == "all" else [args.strategy]
        for strat in targets:
            _run_one(
                strat, runner, pipeline, reports_dir,
                start=args.start, end=args.end, symbols=symbols, grid_density=args.grid_density,
                run_adjacent=not args.no_adjacent,
            )
        conn.commit()
    finally:
        conn.close()
        store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
