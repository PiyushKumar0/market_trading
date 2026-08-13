#!/usr/bin/env python
"""WO-10 pre-registered experiment CLI — intraday VWAP-deviation reversion at 15-60 min.

    python scripts/experiment_vwap_reversion.py --from 2025-07-10 --to 2026-08-12 \
        --symbols RELIANCE,TCS [--reports-dir data/reports]

**C-CATEGORY: STOPS FOR OWNER REVIEW — no live wiring.** This is a thin shell around
:class:`engine.learning.vwap_reversion.VwapReversionExperiment`; all the logic (and the whole
pre-registration) lives in that module, which is where to read it. The CLI resolves the universe,
opens the market store READ-ONLY, runs the two-stage experiment and writes ONE report pair
(``data/reports/vwap_reversion_<ts>.{json,md}``). It writes nothing to SQLite — no ``param_sets``
candidate row is created, so nothing downstream can promote this.

Two stages, per WO-10's abort criterion:

1. the UNCONDITIONED 15-60-min reversion base rate after costs over the full window (no stretch, no
   relative-volume condition). If it is ``<= 0`` the run writes an **ABORTED** report stating the
   number and stops — the stretch grid is never evaluated;
2. only if stage 1 clears zero: the pre-registered ``{1.0, 1.5, 2.0}`` stretch grid (that axis ONLY;
   N = 3), each point validated by the house CPCV + ``fold_pass_min`` + WO-3 margin-floor rule.

Exit codes: **0** = ran (a stage-1 ABORT is a valid pre-registered outcome and still exits 0);
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
from engine.learning import vwap_reversion as vr  # noqa: E402
from engine.marketdata.store import MarketStore  # noqa: E402
from engine.strategy.cost_model import CostModel  # noqa: E402

_log = get_logger("scripts.experiment_vwap_reversion")


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="WO-10 pre-registered VWAP-reversion experiment (C-category; owner review only)."
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
    parser.add_argument(
        "--seed-atr-prior-session",
        action="store_true",
        help=(
            "WO-10b mode: seed each session's 10m-ATR from the PRIOR session's final value (first "
            "bucket's TR = high-low only, so the overnight gap never enters the stop). Makes the "
            "10:00-11:35 morning window measurable — WO-10's unseeded warm-up could only reach "
            "11:36-14:00. Default OFF = WO-10 behavior, byte-identical. The report states which "
            "mode ran and, seeded, splits the stage-1 base rate 10:00-11:35 vs 11:36-14:00."
        ),
    )
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
        # passes the LITERAL text, so the experiment would silently run on zero real symbols.
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
        experiment = vr.VwapReversionExperiment.from_store(
            store, cost_model, clock, start=args.start, end=args.end,
            seed_atr_prior_session=args.seed_atr_prior_session,
        )
        report = experiment.run(symbols, start=args.start, end=args.end)
        artifacts = vr.write_report(report, reports_dir)
    finally:
        store.close()

    _print_summary(report, artifacts)
    return 2 if report.status == "NO_DATA" else 0


def _print_summary(report: vr.VwapReversionReport, artifacts) -> None:
    """Console summary. ASCII only — this goes to a Windows console that may be cp1252."""
    print("")
    print("=" * 78)
    print(vr.BANNER)
    print("=" * 78)
    print(
        f"window {report.requested_start} -> {report.requested_end} | resolved "
        f"{report.data_start} -> {report.data_end} | {report.n_symbols} symbols, "
        f"{report.n_sessions} sessions"
    )
    print(
        f"costs: {report.product} Rs{report.reference_notional}/trade, full round-trip "
        f"{report.cost_floor_pct:.4f}% (fees + spread) charged on EVERY trade | fills: "
        f"{report.fill_mechanics}"
    )
    mode = (
        "WO-10b: prior-session-seeded ATR, effective entry window 10:00-14:00 from day 2"
        if report.seed_atr_prior_session
        else "WO-10: unseeded ATR (140-min warm-up), effective entry window 11:36-14:00"
    )
    print(f"variant: {report.experiment_variant} | {mode}")

    if report.stage1 is not None:
        s = report.stage1
        rate = "n/a (no trades)" if s.base_rate_pct is None else f"{s.base_rate_pct:+.5f}%/trade"
        win = "n/a" if s.win_rate is None else f"{s.win_rate:.1%}"
        print("")
        print(f"STAGE 1 (UNCONDITIONED base rate after costs) = {rate}   [the abort criterion]")
        print(f"        trades={s.n_trades}  win_rate={win}  symbol-sessions={s.n_symbol_sessions}")
        for sp in s.window_splits:
            exp = "n/a" if sp.net_expectancy_pct is None else f"{sp.net_expectancy_pct:+.5f}%"
            swin = "n/a" if sp.win_rate is None else f"{sp.win_rate:.1%}"
            print(
                f"        split {sp.label}: trades={sp.n_trades}  win={swin}  net_exp/trade={exp}"
                "   [reporting only, never a gate]"
            )

    if report.status == "ABORTED_STAGE1":
        print("")
        print("*** ABORTED AT STAGE 1 -- the stretch grid was NOT evaluated. ***")
        print(f"    {report.stage1.abort_reason if report.stage1 else ''}")
    elif report.status == "NO_DATA":
        print("")
        print("*** NO DATA -- no requested symbol had 1m bars in the window; nothing measured. ***")
    else:
        print("")
        print(f"STAGE 2 grid (N={report.trial_count_n}, stretch axis only):")
        for g in report.grid:
            exp = "n/a" if g.expectancy_pct is None else f"{g.expectancy_pct:+.5f}%"
            win = "n/a" if g.win_rate is None else f"{g.win_rate:.1%}"
            verdict = "PROMOTABLE" if g.promotable else "NOT PROMOTABLE"
            print(
                f"  stretch>={g.stretch_min_pct:g}%  trades={g.n_trades}  win={win}  "
                f"net_exp/trade={exp}  -> {verdict}"
            )
            for reason in g.reasons:
                print(f"      reason: {reason}")

    print("")
    print(f"report: {artifacts.markdown}")
    print(f"        {artifacts.json}")
    print("")
    print("C-category: STOPS HERE FOR OWNER REVIEW. Nothing was wired live; no param_sets row written.")


if __name__ == "__main__":
    raise SystemExit(main())
