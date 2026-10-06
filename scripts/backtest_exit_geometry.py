#!/usr/bin/env python
"""C1 exit-geometry study (IMPLEMENTATION_PLAN §6.1 "C1 EXIT-GEOMETRY STUDY — PRE-REGISTERED 2026-10-07").

Per strategy (brk20 V2-N5 fills, hi52 v2, ins), three arms walked through ``exit_sim`` from the
research fill: (i) the shipped stop/target re-anchored to the fill, (ii) time only (REFERENCE),
(iii) a ``fill - 2.5 x ATR14`` catastrophe stop. Family N = 2 per strategy. Evidence only; wires
nothing. Exit codes: 0 ran, 2 refused.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime
from decimal import Decimal
from functools import cache
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
import research_common as rc  # noqa: E402
from engine.core.config import load_settings  # noqa: E402
from engine.learning.validate import fold_pass_min  # noqa: E402
from engine.strategy.cost_model import CostModel  # noqa: E402
from engine.strategy.scanners import hi52  # noqa: E402
from engine.strategy.types import round_to_tick  # noqa: E402

ARM_SHIPPED = "i_shipped"
ARM_TIME = "ii_time_only_REFERENCE"
ARM_CATASTROPHE = "iii_catastrophe_2.5xATR14"
ARMS = (ARM_SHIPPED, ARM_TIME, ARM_CATASTROPHE)
TRIAL_ARMS = (ARM_SHIPPED, ARM_CATASTROPHE)
FAMILY_N = 2
CATASTROPHE_ATR_MULT = Decimal("2.5")
STRATEGIES = ("brk20", "hi52", "ins")

DECISION_HORIZON = "T+20"
# exit_sim horizon = sessions after the fill session; T+k counts the fill session as 1 (D2).
HORIZONS = {"T+5": 4, "T+10": 9, DECISION_HORIZON: rc.HORIZON - 1}
BRK20_REGISTERED = {"close(fill+20)": rc.HORIZON}

NEITHER = "neither"


@cache
def shipped_stop_pct(strategy: str) -> Decimal:
    pct = hi52.DEFAULT_PARAMS["stop_pct"] if strategy == "hi52" else load_settings().ins.stop_pct
    return Decimal(str(pct))


def arm_levels(arm: str, strategy: str, ev: rc.Event) -> tuple[Decimal | None, Decimal | None]:
    """``(stop, target)`` for one arm, anchored at the research fill."""
    fill = ev.fill_px
    if arm == ARM_TIME:
        return None, None
    if arm == ARM_CATASTROPHE:
        return round_to_tick(fill - CATASTROPHE_ATR_MULT * Decimal(str(ev.atr))), None
    if strategy == "brk20":
        lv = ev.levels
        return round_to_tick(fill * lv.stop / lv.entry), round_to_tick(fill * lv.target / lv.entry)
    return round_to_tick(fill * (1 - shipped_stop_pct(strategy) / 100)), None


def measure(pop: rc.Population, cost_model: CostModel, limits) -> dict[str, dict[str, Any]]:
    """``arm -> horizon label -> {"trades": [...], "voids": n}``."""
    cost = rc.cost_pct(cost_model)
    horizons = HORIZONS | (BRK20_REGISTERED if pop.strategy == "brk20" else {})
    out = {a: {h: {"trades": [], "voids": 0} for h in horizons} for a in ARMS}
    for ev in pop.events:
        ex = pop.structural.get(ev.symbol, [])
        for arm in ARMS:
            stop, target = arm_levels(arm, pop.strategy, ev)
            notional = rc.sized_notional(ev.fill_px, stop, limits)
            sized_cost = None if notional is None else rc.cost_pct(cost_model, notional)
            for label, h in horizons.items():
                o = rc.walk(ev, stop=stop, target=target, horizon=h, cost_pct=cost, ex_dates=ex)
                cell = out[arm][label]
                if o.status == "void_ca":
                    cell["voids"] += 1
                    continue
                if o.status != "exit":
                    raise AssertionError(f"{ev.symbol} {ev.fill_date}: walk ended {o.status}")
                cell["trades"].append(rc.Trade(
                    ev.symbol, ev.signal_date, ev.fill_date, o.reason, float(o.gross_pct), float(o.net_pct),
                    None if sized_cost is None else float(o.gross_pct - sized_cost),
                ))
    return out


def decide(cpcv: dict[str, dict], medians: dict[str, float | None]) -> dict[str, Any]:
    """Promotable iff CPCV passes at fold_pass_min(2) AND T+20 median net > 0; higher median wins."""
    promotable = {a: medians[a] for a in TRIAL_ARMS
                  if cpcv[a]["promotable"] and medians[a] is not None and medians[a] > 0}
    ranked = sorted(promotable.items(), key=lambda kv: -kv[1])
    if not ranked or (len(ranked) == 2 and ranked[0][1] == ranked[1][1]):
        outcome = NEITHER
    else:
        outcome = ranked[0][0]

    def gap(arm: str) -> float | None:
        a, b = medians[ARM_TIME], medians[arm]
        return None if a is None or b is None else round(a - b, 4)

    return {
        "promotable_arms": [a for a, _ in ranked],
        "outcome": outcome,
        "median_net_t20": medians,
        "cost_of_stop_pp": {"ii_minus_i": gap(ARM_SHIPPED), "ii_minus_iii": gap(ARM_CATASTROPHE)},
    }


def study(pop: rc.Population, cost_model: CostModel, limits) -> dict[str, Any]:
    cost = float(rc.cost_pct(cost_model))
    cells = measure(pop, cost_model, limits)
    t20 = {a: cells[a][DECISION_HORIZON]["trades"] for a in ARMS}
    cpcv = {a: rc.cpcv(t20[a], FAMILY_N, cost) for a in ARMS}
    arms = {
        a: {label: {**rc.trade_stats(c["trades"]), "voids": c["voids"],
                    "exits": {r: sum(t.reason == r for t in c["trades"]) for r in ("stop", "target", "time")}}
            for label, c in cells[a].items()}
        for a in ARMS
    }
    return {
        "population": {
            "source": pop.source, "label": rc.SURVIVORSHIP_LABEL,
            "population_is_survivorship_tainted_proxy": True,
            "n_symbols": pop.n_symbols, "n_events": len(pop.events), "counts": pop.counts,
        },
        "geometry": {a: rc.geometry(t20[a], cost) for a in ARMS},
        "arms": arms,
        "cpcv": cpcv,
        "decision": decide(cpcv, {a: arms[a][DECISION_HORIZON]["median_net"] for a in ARMS}),
        "held_out": {a: rc.trade_stats(rc.held_out(t20[a])) for a in ARMS},
    }


def run(conn, db: Path, end: date, *, symbols=None, max_symbols=None, strategies=STRATEGIES) -> dict[str, Any]:
    builders = {
        "brk20": lambda: rc.brk20_population(conn, end, symbols=symbols, max_symbols=max_symbols),
        "hi52": lambda: rc.hi52_population(conn, db, end, symbols=symbols, max_symbols=max_symbols),
        "ins": lambda: rc.ins_population(conn, end, symbols=symbols, max_symbols=max_symbols),
    }
    cost_model = CostModel.from_config()
    limits = rc.load_limits()
    lim = limits.limits
    return {
        "meta": {
            "script": "scripts/backtest_exit_geometry.py",
            "registration": "C1 exit geometry, IMPLEMENTATION_PLAN 6.1, pre-registered 2026-10-07",
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "db": str(db), "window_end": str(end),
            "population_override": {"symbols": symbols, "max_symbols": max_symbols},
            "registered_population": not symbols and not max_symbols,
            "population_is_survivorship_tainted_proxy": True,
            "arms": list(ARMS), "trial_arms": list(TRIAL_ARMS), "reference_arm": ARM_TIME,
            "family_n": FAMILY_N, "fold_pass_min": fold_pass_min(FAMILY_N),
            "purge": rc.PURGE, "embargo": rc.EMBARGO,
            "horizons_sessions_after_fill": HORIZONS, "brk20_registered_horizon": BRK20_REGISTERED,
            "shipped_stop_pct": {"hi52": str(shipped_stop_pct("hi52")), "ins": str(shipped_stop_pct("ins")),
                                 "brk20": "scanner stop/target re-anchored to the fill by fraction"},
            "cost_round_trip_pct": float(rc.cost_pct(cost_model)), "notional_inr": str(rc.NOTIONAL),
            "product": rc.PRODUCT,
            "sized_notional_diagnostic": {
                "capital_base_inr": str(limits.capital_base_inr),
                "swing_position_pct": lim.per_trade_risk.swing_position_pct,
                "overnight_gap_mult": lim.per_trade_risk.overnight_gap_mult,
                "cnc_notional_inr": str(lim.per_stock_exposure.cnc_notional_inr),
                "max_deployed_capital_inr": str(lim.capital_cap.max_deployed_capital_inr),
                "note": "daily-band leg not modelled; arm (ii) has no stop and cannot be sized",
            },
            "held_out_from": str(rc.HELD_OUT_FROM),
        },
        "strategies": {s: study(builders[s](), cost_model, limits) for s in strategies},
    }


# ----------------------------------------------------------------------------- rendering
def _num(v: Any, fmt: str = "+.4f") -> str:
    return "-" if v is None else format(v, fmt)


def render_text(doc: dict[str, Any]) -> str:
    m = doc["meta"]
    out = [f"C1 EXIT-GEOMETRY STUDY ({m['registration']})", f"db {m['db']}  end {m['window_end']}  "
           f"N={m['family_n']} fold_pass_min={m['fold_pass_min']:.0%}  purge/embargo {m['purge']}/{m['embargo']}",
           f"round trip {m['cost_round_trip_pct']:.4f}% {m['product']} at Rs {m['notional_inr']}", ""]
    if not m["registered_population"]:
        out += ["POPULATION OVERRIDDEN (--symbols/--max-symbols): smoke run, NOT the registered study", ""]
    out.append("GEOMETRY FIRST - median GROSS at T+20 vs one round trip (WO-3 margin floor per session)")
    for s, b in doc["strategies"].items():
        for a, g in b["geometry"].items():
            out.append(f"  {s:<6} {a:<26} n={g['n']:<6} median gross {_num(g['median_gross_pct'])}%  "
                       f"cost {g['cost_floor_pct']:.4f}%  margin floor {_num(g['margin_floor_pct_per_session'], '.5f')}"
                       f"%/session  GEOMETRY: {g['verdict']}")
    for s, b in doc["strategies"].items():
        p = b["population"]
        out += ["", f"[{s}] {p['source']}  [{p['label']}]",
                f"  symbols {p['n_symbols']}  admitted events {p['n_events']}  counts {p['counts']}",
                "  arm                        horizon         n  voids stop/tgt/time  hit%net  med gross   med net"
                "  med net@7.1"]
        for a, cells in b["arms"].items():
            for label, c in cells.items():
                e = c["exits"]
                hit = "-" if c["hit_rate_net"] is None else f"{c['hit_rate_net'] * 100:.1f}"
                out.append(f"  {a:<26} {label:<14} {c['n']:>5} {c['voids']:>6} {e['stop']:>4}/{e['target']}/"
                           f"{e['time']:<5} {hit:>7} {_num(c['median_gross']):>10} {_num(c['median_net']):>9}"
                           f" {_num(c['median_net_sized']):>11}")
        out.append(f"  CPCV at T+20 (N={m['family_n']}):")
        for a, c in b["cpcv"].items():
            fp = "-" if c["fold_pass_fraction"] is None else f"{c['fold_pass_fraction'] * 100:.1f}%"
            out.append(f"    {a:<26} {c['cv_method']}  splits {c['n_splits']}  fold pass {fp}  "
                       f"promotable {c['promotable']}")
        d = b["decision"]
        out.append(f"  DECISION: {d['outcome']}  (promotable: {', '.join(d['promotable_arms']) or 'none'}; "
                   f"cost of stop (ii)-(i) {_num(d['cost_of_stop_pp']['ii_minus_i'])} pp, "
                   f"(ii)-(iii) {_num(d['cost_of_stop_pp']['ii_minus_iii'])} pp)")
        out.append("  held-out (signals >= " + m["held_out_from"] + ", diagnostic): " + ", ".join(
            f"{a} n={h['n']} med net {_num(h['median_net'])}" for a, h in b["held_out"].items()))
    out += ["", "Every population is a " + rc.SURVIVORSHIP_LABEL + ".",
            "brk20 fills on TOUCH: a touch is not a guaranteed fill, so brk20 fill rates and nets are optimistic."]
    return rc.ascii_text("\n".join(out))


def render_markdown(doc: dict[str, Any]) -> str:
    return f"# C1 exit-geometry study\n\n```\n{render_text(doc)}\n```\n"


# ----------------------------------------------------------------------------- CLI
def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="backtest_exit_geometry", description=__doc__.splitlines()[0])
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
            doc = run(conn, args.db, args.end, symbols=symbols, max_symbols=args.max_symbols)
        finally:
            conn.close()
    except rc.Refusal as exc:
        print(f"backtest_exit_geometry: REFUSED: {rc.ascii_text(str(exc))}", file=sys.stderr)
        return 2
    out = args.out or rc.default_out(args.db, "exit_geometry")
    md = rc.write_reports(doc, render_markdown(doc), out)
    print(render_text(doc))
    print(f"JSON -> {out}\nMD   -> {md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
