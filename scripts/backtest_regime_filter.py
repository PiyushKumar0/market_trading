#!/usr/bin/env python
"""C2 regime-filter study (IMPLEMENTATION_PLAN §6.1 "C2 REGIME-FILTER STUDY — PRE-REGISTERED 2026-10-07").

Per strategy (brk20 V2-N5 fills, hi52 v2, ins), the arm (ii) construct (registered entry, T+20 time
exit, no stop) over the admitted events, split by the rsi2 NIFTY 50 regime at the signal session: ON
is the trial, the full population the REFERENCE; OFF and the September 2026 down-leg are diagnostics.
Family N = 1 per strategy. Evidence only; wires nothing. Exit codes: 0 ran, 2 refused.
``backtest_selection_filters.py`` (C3) reuses the measurement, report and CLI helpers below.
"""

from __future__ import annotations

import argparse
import sys
from bisect import bisect_right
from collections.abc import Callable
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import research_common as rc  # noqa: E402
from engine.learning.validate import fold_pass_min  # noqa: E402
from engine.strategy.cost_model import CostModel  # noqa: E402
from engine.strategy.indicators import sma  # noqa: E402
from engine.strategy.scanners.rsi2 import _INDEX_DMA, _RISING_LOOKBACK  # noqa: E402

STRATEGIES = ("brk20", "hi52", "ins")
INDEX_SYMBOL = "NIFTY 50"
FAMILY_N = 1
MIN_N = 30
T20 = rc.HORIZON - 1  # exit_sim counts sessions after the fill; T+20 counts the fill session as 1 (D2)
DOWN_LEG = (date(2026, 9, 1), date(2026, 9, 30))
RULE = f"close(y) > SMA{_INDEX_DMA}(y) AND SMA{_INDEX_DMA}(y) > SMA{_INDEX_DMA}(y-{_RISING_LOOKBACK})"
FULL, ON, OFF = "full_REFERENCE", "on", "off"
SUPPORTED, NOT_SUPPORTED, INSUFFICIENT = "supported", "not supported", "insufficient"

Pair = tuple[rc.Event, rc.Trade | None]  # None: void_ca


# ----------------------------------------------------------------------------- index regime
def index_series(conn, end: date):
    found = rc.bb.load_series(conn, rc.FULL_HISTORY_START, end, symbols=[INDEX_SYMBOL])
    if not found:
        raise rc.Refusal(f"bars_1d has no {INDEX_SYMBOL!r} rows")
    return next(iter(found.values()))


def closes_through(index, y: date) -> np.ndarray | None:
    """Index closes through session ``y``; ``None`` when the index has no bar dated ``y``."""
    k = bisect_right(index.dates, y)
    return index.close[:k] if k and index.dates[k - 1] == y else None


def regime_on(closes: np.ndarray | None) -> bool | None:
    """The rsi2 index leg over completed closes; ``None`` when undefined (rsi2's own history floor)."""
    if closes is None or len(closes) < _INDEX_DMA + _RISING_LOOKBACK:
        return None
    s = sma(closes, _INDEX_DMA)
    return bool(closes[-1] > s.iloc[-1] > s.iloc[-1 - _RISING_LOOKBACK])


# ----------------------------------------------------------------------------- shared with C3
def build_population(name: str, conn, db: Path, end: date, symbols, max_symbols) -> rc.Population:
    kw = {"symbols": symbols, "max_symbols": max_symbols}
    if name == "hi52":
        return rc.hi52_population(conn, db, end, **kw)
    return (rc.brk20_population if name == "brk20" else rc.ins_population)(conn, end, **kw)


def measure(pop: rc.Population, cost: Decimal) -> list[Pair]:
    """Arm (ii) once per admitted event; every cohort is a subset of this list."""
    pairs: list[Pair] = []
    for ev in pop.events:
        o = rc.walk(ev, stop=None, target=None, horizon=T20, cost_pct=cost,
                    ex_dates=pop.structural.get(ev.symbol, []))
        if o.status not in ("exit", "void_ca"):
            raise AssertionError(f"{ev.symbol} {ev.fill_date}: walk ended {o.status}")
        pairs.append((ev, None if o.status == "void_ca" else rc.Trade(
            ev.symbol, ev.signal_date, ev.fill_date, o.reason, float(o.gross_pct), float(o.net_pct))))
    return pairs


def stats(trades: list[rc.Trade]) -> dict[str, Any]:
    return rc.stats([t.gross for t in trades], [t.net for t in trades])


def report(groups: dict[str, list[Pair]], family_n: int, cost: float) -> dict[str, Any]:
    trades = {k: [t for _, t in v if t is not None] for k, v in groups.items()}
    return {
        "geometry": {k: rc.geometry(t, cost) for k, t in trades.items()},
        "cohorts": {k: {**stats(t), "events": len(groups[k]), "voids": len(groups[k]) - len(t)}
                    for k, t in trades.items()},
        "cpcv": {k: rc.cpcv(t, family_n, cost) for k, t in trades.items()},
        "held_out": {k: stats(rc.held_out(t)) for k, t in trades.items()},
    }


def population_block(pop: rc.Population) -> dict[str, Any]:
    return {
        "source": pop.source, "label": rc.SURVIVORSHIP_LABEL, "population_is_survivorship_tainted_proxy": True,
        "n_symbols": pop.n_symbols, "n_events": len(pop.events), "counts": pop.counts,
    }


def meta(script: str, registration: str, db: Path, end: date, symbols, max_symbols, family_n: int,
         cost_model: CostModel) -> dict[str, Any]:
    return {
        "script": script, "registration": registration,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "db": str(db), "window_end": str(end),
        "population_override": {"symbols": symbols, "max_symbols": max_symbols},
        "registered_population": not symbols and not max_symbols,
        "population_is_survivorship_tainted_proxy": True,
        "construct": "arm (ii): registered entry, T+20 time exit, no stop",
        "horizon_sessions_after_fill": T20,
        "family_n": family_n, "fold_pass_min": fold_pass_min(family_n), "min_n": MIN_N,
        "purge": rc.PURGE, "embargo": rc.EMBARGO,
        "cost_round_trip_pct": float(rc.cost_pct(cost_model)), "notional_inr": str(rc.NOTIONAL),
        "product": rc.PRODUCT, "index_symbol": INDEX_SYMBOL, "held_out_from": str(rc.HELD_OUT_FROM),
    }


# ----------------------------------------------------------------------------- C2
def decide(r: dict[str, Any]) -> str:
    on, off, full = (r["cohorts"][k] for k in (ON, OFF, FULL))
    if on["n"] < MIN_N or off["n"] < MIN_N:
        return INSUFFICIENT
    if r["cpcv"][ON]["promotable"] and on["median_net"] > max(0.0, full["median_net"]) and off["median_net"] <= 0:
        return SUPPORTED
    return NOT_SUPPORTED


def study(pop: rc.Population, index, cost_model: CostModel) -> dict[str, Any]:
    cost = rc.cost_pct(cost_model)
    pairs = measure(pop, cost)
    regime = [regime_on(closes_through(index, ev.signal_date)) for ev, _ in pairs]
    groups = {FULL: pairs, ON: [p for p, r in zip(pairs, regime, strict=True) if r],
              OFF: [p for p, r in zip(pairs, regime, strict=True) if r is False]}
    out = {"population": {**population_block(pop), "regime_undefined": regime.count(None)},
           **report(groups, FAMILY_N, float(cost))}
    out["decision"] = decide(out)
    out["down_leg_2026_09"] = {
        k: stats([t for ev, t in v if t is not None and DOWN_LEG[0] <= ev.signal_date <= DOWN_LEG[1]])
        for k, v in groups.items()
    }
    return out


def run(conn, db: Path, end: date, *, symbols=None, max_symbols=None, strategies=STRATEGIES) -> dict[str, Any]:
    cost_model = CostModel.from_config()
    index = index_series(conn, end)
    m = meta("scripts/backtest_regime_filter.py", "C2 regime filter, IMPLEMENTATION_PLAN 6.1, pre-registered 2026-10-07",
             db, end, symbols, max_symbols, FAMILY_N, cost_model)
    m |= {"rule": RULE, "down_leg": [str(d) for d in DOWN_LEG]}
    return {"meta": m, "strategies": {
        s: study(build_population(s, conn, db, end, symbols, max_symbols), index, cost_model) for s in strategies
    }}


# ----------------------------------------------------------------------------- rendering
def num(v: Any, fmt: str = "+.4f") -> str:
    return "-" if v is None else format(v, fmt)


def render_text(doc: dict[str, Any], title: str, extra: Callable[[dict[str, Any]], list[str]]) -> str:
    m = doc["meta"]
    out = [f"{title} ({m['registration']})", f"db {m['db']}  end {m['window_end']}  "
           f"N={m['family_n']} fold_pass_min={m['fold_pass_min']:.0%}  purge/embargo {m['purge']}/{m['embargo']}  "
           f"{m['construct']}",
           f"round trip {m['cost_round_trip_pct']:.4f}% {m['product']} at Rs {m['notional_inr']}", ""]
    if not m["registered_population"]:
        out += ["POPULATION OVERRIDDEN (--symbols/--max-symbols): smoke run, NOT the registered study", ""]
    out.append("GEOMETRY FIRST - median GROSS at T+20 vs one round trip (WO-3 margin floor per session)")
    for s, b in doc["strategies"].items():
        for k, g in b["geometry"].items():
            out.append(f"  {s:<6} {k:<16} n={g['n']:<6} median gross {num(g['median_gross_pct'])}%  "
                       f"cost {g['cost_floor_pct']:.4f}%  margin floor {num(g['margin_floor_pct_per_session'], '.5f')}"
                       f"%/session  GEOMETRY: {g['verdict']}")
    for s, b in doc["strategies"].items():
        p = b["population"]
        out += ["", f"[{s}] {p['source']}  [{p['label']}]",
                f"  symbols {p['n_symbols']}  admitted events {p['n_events']}  counts {p['counts']}",
                "  cohort            events  voids      n  hit%net  med gross   med net  mean net"]
        for k, c in b["cohorts"].items():
            hit = "-" if c["hit_rate_net"] is None else f"{c['hit_rate_net'] * 100:.1f}"
            out.append(f"  {k:<16} {c['events']:>7} {c['voids']:>6} {c['n']:>6} {hit:>8} {num(c['median_gross']):>10}"
                       f" {num(c['median_net']):>9} {num(c['mean_net']):>9}")
        out.append(f"  CPCV at T+20 (N={m['family_n']}):")
        for k, c in b["cpcv"].items():
            fp = "-" if c["fold_pass_fraction"] is None else f"{c['fold_pass_fraction'] * 100:.1f}%"
            out.append(f"    {k:<16} {c['cv_method']}  splits {c['n_splits']}  fold pass {fp}  "
                       f"promotable {c['promotable']}")
        out += extra(b)
        out.append("  held-out (signals >= " + m["held_out_from"] + ", diagnostic): " + ", ".join(
            f"{k} n={h['n']} med net {num(h['median_net'])}" for k, h in b["held_out"].items()))
    out += ["", "Every population is a " + rc.SURVIVORSHIP_LABEL + ".",
            "brk20 fills on TOUCH: a touch is not a guaranteed fill, so brk20 fill rates and nets are optimistic."]
    return rc.ascii_text("\n".join(out))


def _extra(b: dict[str, Any]) -> list[str]:
    leg = ", ".join(f"{k} n={c['n']} med net {num(c['median_net'])}" for k, c in b["down_leg_2026_09"].items())
    return [f"  regime undefined (no index bar on y or < {_INDEX_DMA + _RISING_LOOKBACK} index closes): "
            f"{b['population']['regime_undefined']} events, in neither cohort",
            f"  DECISION: {b['decision']}  (rule {RULE}; insufficient when ON or OFF n < {MIN_N})",
            f"  September 2026 down-leg (LOW POWER, decides nothing): {leg}"]


# ----------------------------------------------------------------------------- CLI
def cli(argv: list[str] | None, *, prog: str, title: str, run_fn: Callable[..., dict[str, Any]],
        render_fn: Callable[[dict[str, Any]], str], prefix: str) -> int:
    ap = argparse.ArgumentParser(prog=prog, description=title)
    ap.add_argument("--db", type=Path, required=True, help="research snapshot (data/research/market_<date>.duckdb)")
    ap.add_argument("--end", type=date.fromisoformat, default=date.today(), help="cap the window (YYYY-MM-DD)")
    ap.add_argument("--symbols", default=None, help="comma-separated subset (smoke runs only)")
    ap.add_argument("--max-symbols", type=int, default=None, help="cap symbols per population (smoke runs)")
    ap.add_argument("--out", type=Path, default=None, help="JSON path; the .md is written beside it")
    args = ap.parse_args(argv)
    try:
        if reason := rc.market_hours_refusal():
            raise rc.Refusal(reason)
        conn = rc.open_snapshot(args.db)
        try:
            symbols = [s.strip() for s in args.symbols.split(",") if s.strip()] if args.symbols else None
            doc = run_fn(conn, args.db, args.end, symbols=symbols, max_symbols=args.max_symbols)
        finally:
            conn.close()
    except rc.Refusal as exc:
        print(f"{prog}: REFUSED: {rc.ascii_text(str(exc))}", file=sys.stderr)
        return 2
    out = args.out or rc.default_out(args.db, prefix)
    text = render_fn(doc)
    md = rc.write_reports(doc, f"# {title}\n\n```\n{text}\n```\n", out)
    print(text)
    print(f"JSON -> {out}\nMD   -> {md}")
    return 0


TITLE = "C2 REGIME-FILTER STUDY"


def main(argv: list[str] | None = None) -> int:
    return cli(argv, prog="backtest_regime_filter", title=TITLE, run_fn=run,
               render_fn=lambda doc: render_text(doc, TITLE, _extra), prefix="regime_filter")


if __name__ == "__main__":
    raise SystemExit(main())
