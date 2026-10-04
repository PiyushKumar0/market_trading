#!/usr/bin/env python
"""§6.4 validation artifact for the PINNED ``insider_net_buy`` rule (§2.8.2 / §2.8.4 stage 3, N=1).

    python scripts/validate_insider.py --from 2023-08-01 --to 2026-07-01 [--symbols RELIANCE,TCS] \
        [--reports-dir data/reports]

Unlike the price baselines (``scripts/backtest.py``), ``insider_net_buy`` was PRE-REGISTERED and passed
a single-pass event study (stage-2 verdict, plan §2.8.4) — there is NO sweep, so the cited trial count
**N = 1** and ``fold_pass_min(1) = 60%`` (§6.4 step 2). This script builds the rule's cost-adjusted
DAILY return series and runs it through the identical :class:`ValidationPipeline` the baselines use, so
the promotion verdict lands in the same SQLite ``param_sets`` audit trail + md/json report.

**Daily-return series (the §6.4 CPCV/walk-forward input):** equal-weight portfolio of OPEN insider
positions. Each ``insider_net_buy`` event's ``event_session`` T is the first session at whose CLOSE
the DISCLOSURE was public (``event_study.entry_session_index``, keyed on the exchange broadcast
timestamp — never the transaction dates) and it holds one unit through T+20 sessions (or ``--to``,
whichever is first). The portfolio's daily return on a session = the cross-sectional MEAN of the open
positions' daily returns that day, **0 on no-position days** (the same convention as ``SweepRunner``
returns, §6.4). A single CNC round trip is amortized as TWO legs — per-side fee =
``CostModel.breakeven_pct(₹20k, 'CNC') / 2`` — charged on the entry-day and exit-day returns (mirrors
how the sweep splits its legs). ``breakeven_pct`` is the FULL friction: statutory fees **plus** the
measured bid-ask spread (WO-2) — 0.3192% = 0.2992% + 0.0200% at ₹20k CNC, both legs, DP included.
Documented approximation: the round trip is modelled at the ₹20k reference notional (not per-symbol
size); it is subtracted at the two boundary sessions rather than spread, so the total cost drag per
event is exactly one round trip regardless of hold length.

**Entry fill (WO-16, 2026-08-14):** ``--entry-fill next_open`` (the DEFAULT) buys at ``open_(T+1)``
— the first price reachable by an order placed after the signal was knowable at close_T — so the
first held session earns ``close_(T+1)/open_(T+1) − 1`` and later sessions accrue close-to-close.
``--entry-fill close_t`` reproduces the pre-WO-16 convention (buy at close_T, every held session
close-to-close), which books the T→T+1 overnight gap no post-close order can capture; it is kept
only so the corrected run can be reported side by side with the recorded 2026-07-19 result.

Standalone/blocking offline research tool (fine here). Exit codes: 0 = ran; 2 = no symbols resolved.
"""

from __future__ import annotations

import argparse
import os
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from decimal import Decimal
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
from engine.datafeeds.filings_events import insider_net_buy  # noqa: E402
from engine.learning.validate import ParamSet, ValidationPipeline  # noqa: E402
from engine.marketdata.store import MarketStore  # noqa: E402
from engine.strategy.cost_model import CostModel  # noqa: E402

_log = get_logger("scripts.validate_insider")

STRATEGY_ID = "insider"
HOLD_SESSIONS = 20                      # §2.8.4 T+20 hold horizon (the leg's strongest net drift)
REFERENCE_NOTIONAL = Decimal("20000")   # the §6.3/§2.8.4 CNC reference notional for the cost model

#: WO-16 entry-fill conventions (see the module docstring); ``next_open`` is the corrected default.
ENTRY_FILL_NEXT_OPEN = "next_open"
ENTRY_FILL_CLOSE_T = "close_t"
ENTRY_FILLS = (ENTRY_FILL_NEXT_OPEN, ENTRY_FILL_CLOSE_T)
DEFAULT_ENTRY_FILL = ENTRY_FILL_NEXT_OPEN


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


# --------------------------------------------------------------------------- pure daily-return builder
def build_daily_return_series(
    events: list[dict],
    bars_by_symbol: dict[str, list[tuple[date, float, float]]],
    start: date,
    end: date,
    *,
    per_side_fee_pct: float,
    hold_sessions: int = HOLD_SESSIONS,
    entry_fill: str = DEFAULT_ENTRY_FILL,
) -> dict[date, float]:
    """Cost-adjusted DAILY portfolio return series (FRACTIONS), 0 on no-position days. PURE — no store,
    no pandas (the ``ValidationPipeline`` wraps the dict in a ``pd.Series``).

    ``bars_by_symbol[sym]`` is that symbol's ascending ``(date, open, close)`` bars. ``event_session``
    T is the session at whose close the disclosure was public; the position is then filled per
    ``entry_fill`` (WO-16):

    * ``next_open`` (default) — bought at ``open`` of the FIRST held session, so that session earns
      ``close/open − 1`` and every later held session accrues close-to-close;
    * ``close_t`` — bought at ``close_T`` (superseded), so every held session accrues close-to-close.

    Either way the position runs over T+1..T+``hold_sessions`` (truncated at ``end``).
    ``per_side_fee_pct`` (a percent, one CNC leg) is charged on the event's first and last held
    session — the exit leg lands on the same day as the entry leg when only one session is held
    (total drag = one round trip either way). The series spans the trading calendar in
    ``[start, end]`` (union of bar dates); a session with no open position is 0.0."""
    if entry_fill not in ENTRY_FILLS:
        raise ValueError(f"entry_fill must be one of {ENTRY_FILLS}, got {entry_fill!r}")
    per_side_frac = per_side_fee_pct / 100.0
    per_date: dict[date, list[float]] = defaultdict(list)

    for ev in events:
        sym = str(ev.get("symbol") or "").upper()
        bars = bars_by_symbol.get(sym)
        if not bars:
            continue
        dates = [d for d, _, _ in bars]
        opens = [o for _, o, _ in bars]
        closes = [c for _, _, c in bars]
        entry = ev.get("event_session")
        try:
            idx = dates.index(entry)
        except ValueError:
            continue  # no bar at the entry session for this symbol — un-actionable, skip
        ks = [
            k for k in range(1, hold_sessions + 1)
            if idx + k < len(closes) and dates[idx + k] <= end and closes[idx + k - 1] > 0
        ]
        if not ks:
            continue
        # WO-16: under next_open the FIRST held session is entered at its own open, not carried in
        # from the prior close — the T→T+1 gap is unreachable by an order placed after close_T.
        if entry_fill == ENTRY_FILL_NEXT_OPEN and opens[idx + ks[0]] <= 0:
            continue                      # no usable fill price — un-actionable, skip the event
        for k in ks:
            d_k = dates[idx + k]
            if entry_fill == ENTRY_FILL_NEXT_OPEN and k == ks[0]:
                ret = closes[idx + k] / opens[idx + k] - 1.0
            else:
                ret = closes[idx + k] / closes[idx + k - 1] - 1.0
            if k == ks[0]:
                ret -= per_side_frac      # entry leg
            if k == ks[-1]:
                ret -= per_side_frac      # exit leg (same day as entry when one session held)
            per_date[d_k].append(ret)

    calendar = sorted({d for bars in bars_by_symbol.values() for d, _, _ in bars if start <= d <= end})
    return {d: (sum(per_date[d]) / len(per_date[d]) if per_date.get(d) else 0.0) for d in calendar}


# --------------------------------------------------------------------------- driver
def _resolve_symbols(store: MarketStore, end: date, *, lookback: int = 60) -> list[str]:
    """Most-recent ``universe_daily`` (included) symbols at or before ``end`` (backtest.py convention)."""
    for i in range(lookback + 1):
        rows = store.get_universe_daily(end - timedelta(days=i), included_only=True)
        if rows:
            return [r["symbol"] for r in rows]
    return []


def _events_for(store: MarketStore, symbols: list[str], start: date, end: date, min_value_inr: int) -> tuple[
    list[dict], dict[str, list[tuple[date, float, float]]]
]:
    """Per-symbol ``insider_net_buy`` events (clustered against that symbol's own sessions, exactly like
    ``event_study``) + the symbol->bars map the return builder reads (``(date, open, close)``)."""
    events: list[dict] = []
    bars_by_symbol: dict[str, list[tuple[date, float, float]]] = {}
    for sym in symbols:
        bars = store.get_bars_1d(sym, start, end)
        if not bars:
            continue
        bars_by_symbol[sym] = [(b.d, float(b.open), float(b.close)) for b in bars]
        sessions = [b.d for b in bars]
        filings = store.get_insider_trades(symbol=sym)
        events.extend(insider_net_buy(filings, sessions, min_value_inr=min_value_inr))
    return events, bars_by_symbol


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    parser = argparse.ArgumentParser(description="§6.4 validation for the pinned insider_net_buy rule (N=1).")
    parser.add_argument("--from", dest="start", required=True, type=_parse_date, help="YYYY-MM-DD")
    parser.add_argument("--to", dest="end", required=True, type=_parse_date, help="YYYY-MM-DD")
    parser.add_argument("--symbols", default=None, help="comma-separated override of the universe")
    parser.add_argument("--reports-dir", default=None, help="default: <data_dir>/reports")
    parser.add_argument(
        "--entry-fill", dest="entry_fill", default=DEFAULT_ENTRY_FILL, choices=list(ENTRY_FILLS),
        help=(
            "WO-16 fill convention: 'next_open' (default) buys at open_(T+1); 'close_t' is the "
            "superseded pre-WO-16 close_T fill, kept for old-vs-new comparison"
        ),
    )
    args = parser.parse_args(argv)

    settings = load_settings()
    clock = Clock()
    store = MarketStore.from_settings(settings, clock).open()
    conn = connect(settings.sqlite_path())
    apply_migrations(conn)
    reports_dir = Path(args.reports_dir) if args.reports_dir else settings.resolved_data_dir() / "reports"

    try:
        symbols = (
            [s.strip() for s in args.symbols.split(",") if s.strip()]
            if args.symbols
            else _resolve_symbols(store, args.end)
        )
        # Shell-expansion footgun (backtest.py): `--symbols $syms` under cmd.exe passes the LITERAL
        # `$syms`, so the run silently uses zero real symbols. Reject the unexpanded form loudly.
        suspicious = [s for s in symbols if s.startswith(("$", "%")) or "%" in s]
        if suspicious:
            print(
                f"ERROR: --symbols contains unexpanded shell variable(s): {suspicious} -- run from "
                "PowerShell (where $syms expands) or pass the comma-separated list explicitly; nothing "
                "was validated",
                file=sys.stderr,
            )
            return 2
        if not symbols:
            print(
                "no symbols resolved (empty universe_daily and no --symbols) -- nothing to validate",
                file=sys.stderr,
            )
            return 2

        min_value_inr = settings.filings.insider_min_value_inr
        events, bars_by_symbol = _events_for(store, symbols, args.start, args.end, min_value_inr)
        cost_model = CostModel.from_config()
        # FULL round-trip friction: statutory fees + the measured bid-ask spread (WO-2). The SAME
        # number is handed to the WO-3 margin floor below, so the floor can never drift from the
        # costs these returns were actually charged.
        round_trip_pct = float(cost_model.breakeven_pct(REFERENCE_NOTIONAL, "CNC"))
        fees_pct = float(cost_model.fee_breakeven_pct(REFERENCE_NOTIONAL, "CNC"))
        spread_pct = float(cost_model.spread_pct)
        per_side_fee_pct = round_trip_pct / 2.0
        series = build_daily_return_series(
            events, bars_by_symbol, args.start, args.end,
            per_side_fee_pct=per_side_fee_pct, entry_fill=args.entry_fill,
        )
        n_events = len(events)
        n_position_days = sum(1 for v in series.values() if v != 0.0)

        pipeline = ValidationPipeline(
            returns_provider=lambda _sid, _params: series,  # N=1: no sweep, the series is fixed
            clock=clock,
            conn=conn,
            reports_dir=reports_dir,
        )
        params = ParamSet(
            strategy_id=STRATEGY_ID,
            params={
                "insider_min_value_inr": float(min_value_inr),
                "hold_sessions": float(HOLD_SESSIONS),
                # WO-16: the fill convention is part of the candidate's identity, not a footnote —
                # it lands in the report md and the param_sets audit row.
                "entry_fill_next_open": float(args.entry_fill == ENTRY_FILL_NEXT_OPEN),
            },
            trial_count_n=1,                          # PRE-REGISTERED rule, no sweep (§6.4: N=1)
            sweep_stats={
                "n_events": float(n_events),
                "n_position_days": float(n_position_days),
                "cost_round_trip_pct": round_trip_pct,
                "cost_fees_pct": fees_pct,
                "cost_spread_pct": spread_pct,
            },
            # WO-3 margin floor measured against the SAME friction the returns were charged.
            cost_floor_pct=round_trip_pct,
        )
        report = pipeline.validate_sync(STRATEGY_ID, params)
        conn.commit()

        verdict = "PROMOTABLE" if report.promotable else "NOT PROMOTABLE"
        exp = "n/a" if report.expectancy_pct is None else f"{report.expectancy_pct:+.4f}%/day"
        frac = "n/a" if report.cpcv_fold_pass_fraction is None else f"{report.cpcv_fold_pass_fraction:.1%}"
        bar = "n/a" if report.fold_pass_min is None else f"{report.fold_pass_min:.0%}"
        med = (
            "n/a" if report.cpcv_median_passing_expectancy_pct is None
            else f"{report.cpcv_median_passing_expectancy_pct:+.5f}%/day"
        )
        floor = (
            "n/a" if report.margin_floor_pct_per_day is None
            else f"{report.margin_floor_pct_per_day:.5f}%/day"
        )
        # ASCII only (Windows console may be cp1252).
        print(
            f"[insider] N=1 entry_fill={args.entry_fill} events={n_events} obs={report.n_obs} "
            f"position_days={n_position_days} expectancy={exp} CPCV_pass={frac}/{bar} "
            f"median_passing={med} margin_floor={floor} -> {verdict}"
        )
        print(
            f"    cost: round trip {round_trip_pct:.4f}% = {fees_pct:.4f}% fees + "
            f"{spread_pct:.4f}% spread (CNC, ref notional {REFERENCE_NOTIONAL}, both legs, DP incl.)"
        )
        _log.info("validate_insider_done", promotable=report.promotable, n_events=n_events)
        if not report.promotable:
            for r in report.reasons:
                print(f"    reason: {r}")
    finally:
        conn.close()
        store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
