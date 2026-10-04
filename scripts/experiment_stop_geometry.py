#!/usr/bin/env python
"""WO-17 pre-registered diagnostic CLI — stop geometry / adverse-excursion recovery on intraday longs.

    python scripts/experiment_stop_geometry.py --from 2025-07-10 --to 2026-08-13 \
        --symbols RELIANCE,TCS [--reports-dir data/reports]

**C-CATEGORY: STOPS FOR OWNER REVIEW — no live wiring, promotion NOT sought.** This is a thin shell
around :class:`engine.learning.stop_geometry.StopGeometryExperiment`; all the logic (and the whole
pre-registration) lives in that module, which is where to read it. The CLI resolves the universe,
opens the market store READ-ONLY, runs the diagnostic and writes ONE report pair
(``data/reports/stop_geometry_<ts>.{json,md}``). It writes nothing to SQLite and — unlike the WO-10
experiment — never constructs a ``ValidationPipeline``: there is no CPCV, no margin floor, no
``param_sets`` row, so nothing downstream can promote this.

What it measures: the SAME orb entries under 10 fixed exit geometries — stop widths
``{1.0, 1.5, 2.5, 4.0} x ATR_10m`` plus NO-STOP, each with and without orb's own 1.5R target — and
the two diagnostics the owner's hypothesis lives on (DODGED-WINNER FRACTION and the MAE distribution
of eventual winners).

Exit codes: **0** = ran (including a "no entries" outcome, which is a valid measured result);
**2** = no symbols/bars resolved (nothing was measured).
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
from engine.core.log import configure_logging, get_logger  # noqa: E402
from engine.learning import stop_geometry as sg  # noqa: E402
from engine.marketdata.store import MarketStore  # noqa: E402
from engine.strategy.cost_model import CostModel  # noqa: E402

_log = get_logger("scripts.experiment_stop_geometry")


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="WO-17 pre-registered stop-geometry diagnostic (C-category; owner review only)."
    )
    parser.add_argument("--from", dest="start", required=True, type=_parse_date, help="YYYY-MM-DD")
    parser.add_argument("--to", dest="end", required=True, type=_parse_date, help="YYYY-MM-DD")
    parser.add_argument(
        "--symbols",
        default=None,
        help=(
            "comma-separated universe override — pass the HISTORICAL NIFTY200 list per "
            "runbooks/COMMANDS.md ($syms); omitted ⇒ today's universe_daily (correct for live, "
            "WRONG for history)"
        ),
    )
    parser.add_argument("--reports-dir", default=None, help="default: <data_dir>/reports")
    return parser


def _resolve_symbols(store: MarketStore, end: date, *, lookback: int = 60) -> list[str]:
    """Most-recent ``universe_daily`` (included) symbols at or before ``end`` — mirrors backtest.py."""
    for i in range(lookback + 1):
        rows = store.get_universe_daily(end - timedelta(days=i), included_only=True)
        if rows:
            return [r["symbol"] for r in rows]
    return []


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    args = _build_parser().parse_args(argv)

    settings = load_settings()
    clock = Clock()
    store = MarketStore.from_settings(settings, clock).open()
    reports_dir = (
        Path(args.reports_dir) if args.reports_dir else settings.resolved_data_dir() / "reports"
    )

    try:
        symbols = (
            [s.strip() for s in args.symbols.split(",") if s.strip()]
            if args.symbols
            else _resolve_symbols(store, args.end)
        )
        # Shell-expansion footgun (same guard backtest.py carries): `--symbols $syms` under cmd.exe
        # passes the LITERAL text, so the diagnostic would silently run on zero real symbols.
        suspicious = [s for s in symbols if s.startswith(("$", "%")) or "%" in s]
        if suspicious:
            print(
                f"ERROR: --symbols contains unexpanded shell variable(s): {suspicious} -- run from "
                "PowerShell (where $syms expands) or pass the comma-separated list explicitly; "
                "nothing was measured",
                file=sys.stderr,
            )
            return 2
        if not symbols:
            print(
                "no symbols resolved (empty universe_daily and no --symbols) — nothing to measure",
                file=sys.stderr,
            )
            return 2

        cost_model = CostModel.from_config()
        experiment = sg.StopGeometryExperiment.from_store(
            store, cost_model, clock, start=args.start, end=args.end
        )
        report = experiment.run(symbols, start=args.start, end=args.end)
        artifacts = sg.write_report(report, reports_dir)
    finally:
        store.close()

    _print_summary(report, artifacts)
    return 2 if report.status == "NO_DATA" else 0


def _print_summary(report: sg.StopGeometryReport, artifacts) -> None:
    """Console summary. ASCII only — this goes to a Windows console that may be cp1252."""
    print("")
    print("=" * 78)
    print(sg.BANNER)
    print("=" * 78)
    print(f"NULL HYPOTHESIS: {report.null_hypothesis}")
    print("")
    print(
        f"window {report.requested_start} -> {report.requested_end} | resolved "
        f"{report.data_start} -> {report.data_end} | {report.n_symbols} symbols, "
        f"{report.n_sessions} sessions"
    )
    print(
        f"common population: {report.n_entries} orb entries (every config ran on the SAME entries) | "
        f"orb params: " + ", ".join(f"{k}={v:g}" for k, v in sorted(report.orb_params.items()))
    )
    print(
        f"costs: {report.product} Rs{report.reference_notional}/trade, full round-trip "
        f"{report.cost_floor_pct:.4f}% (fees + spread) charged on EVERY trade | fills: "
        f"{report.fill_mechanics} | ATR prior-session-seeded: {report.seed_atr_prior_session}"
    )

    if report.status == "NO_DATA":
        print("")
        print("*** NO DATA -- no requested symbol had 1m bars in the window; nothing measured. ***")
    elif report.status == "NO_ENTRIES":
        print("")
        print("*** NO ENTRIES -- bars were read but orb signalled no bookable entry. ***")
    else:
        print("")
        print(f"THE {report.n_configs} PRE-REGISTERED CONFIGS:")
        print(
            f"  {'stop':<14} {'ladder':<20} {'trades':>6} {'win%':>6} {'net/trade':>11} "
            f"{'stops':>6} {'dodged':>7}"
        )
        for c in report.pre_registered_configs:
            stop_label, ladder_label = c.label.split(" / ")
            exp = "n/a" if c.expectancy_pct is None else f"{c.expectancy_pct:+.5f}%"
            win = "n/a" if c.win_rate is None else f"{c.win_rate:.1%}"
            dodged = "n/a" if c.dodged_winner_fraction is None else f"{c.dodged_winner_fraction:.1%}"
            print(
                f"  {stop_label:<14} {ladder_label:<20} {c.n_trades:>6} {win:>6} {exp:>11} "
                f"{c.n_stopped:>6} {dodged:>7}"
            )
        for c in report.configs:
            if c.pre_registered:
                continue
            exp = "n/a" if c.expectancy_pct is None else f"{c.expectancy_pct:+.5f}%"
            print(f"  [comparator, NOT pre-registered] {c.label}: net/trade={exp} stops={c.n_stopped}")

        print("")
        print("MAE OF EVENTUAL WINNERS (what dip depth winners survive):")
        for m in report.mae_distributions:
            if m.atr_mult is None or m.pct is None:
                print(f"  {m.label}: n=0")
                continue
            print(
                f"  {m.label}: n={m.n} | ATR mult p25/p50/p75/p90 = "
                f"{m.atr_mult['p25']:.2f}/{m.atr_mult['p50']:.2f}/{m.atr_mult['p75']:.2f}/"
                f"{m.atr_mult['p90']:.2f} | pct p25/p50/p75/p90 = "
                f"{m.pct['p25']:.3f}/{m.pct['p50']:.3f}/{m.pct['p75']:.3f}/{m.pct['p90']:.3f}"
            )

        print("")
        print("PLAIN-LANGUAGE ANSWER:")
        for line in report.plain_language_answer:
            print(f"  {line}" if line else "")

    print("")
    print(f"report: {artifacts.markdown}")
    print(f"        {artifacts.json}")
    print("")
    print(
        "C-category: STOPS HERE FOR OWNER REVIEW. Nothing was wired live; no param_sets row, no "
        "ValidationPipeline call, no promotion sought."
    )


if __name__ == "__main__":
    raise SystemExit(main())
