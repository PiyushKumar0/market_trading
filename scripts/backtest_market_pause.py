#!/usr/bin/env python
"""Market-wide pause study (IMPLEMENTATION_PLAN §3 "Market-wide pause study", review §9 #1, D-J).

Per strategy (hi52, ins), C2's populations, arm (ii) construct and cost (``backtest_regime_filter``), split at the
signal session y: PAUSE if the core equal-weight return compounded over the 20 sessions ending at y is < 0,
otherwise TRADE. The EW series is ``research_common.core_ew``, a survivorship-tainted proxy. Family
``regime_filter/<strategy>`` = C2 plus this rule, N = 2. Evidence only; SUPPORTED goes to the owner and wires
nothing. Never run before the pre-registration is committed. Exit codes: 0 ran, 2 refused.
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
import research_common as rc  # noqa: E402
from engine.learning.benchmark import _MAX_ABS_RET  # noqa: E402
from engine.learning.multiplicity import permutation_pvalue, registration_alpha  # noqa: E402
from engine.strategy.cost_model import CostModel  # noqa: E402

rf = rc._load_script("backtest_regime_filter")

STRATEGIES = ("hi52", "ins")
FAMILY_N = 2
ALPHA = registration_alpha(FAMILY_N)
SEED = 20261029
DRAWS = 10_000
WINDOW = 20
TRADE, PAUSE = "trade", "pause"
RULE = f"PAUSE iff core EW return compounded over the {WINDOW} sessions ending at y < 0, else TRADE"
EW_RULE = (f"daily mean of close/prev_close - 1 over the current core list; |r| > {_MAX_ABS_RET:.0%} and "
           "structural corporate-action days excluded")


def ew20_table(daily: dict[date, float | None]) -> dict[date, float]:
    """``y -> EW compounded over the WINDOW sessions ending at y``; absent when undefined."""
    grid = list(daily)
    out = {}
    for i in range(WINDOW - 1, len(grid)):
        r = rc.compound(daily, grid[i - WINDOW + 1:i + 1])
        if r is not None:
            out[grid[i]] = r
    return out


def decide(r: dict[str, Any]) -> str:
    trade, pause, full = (r["cohorts"][k] for k in (TRADE, PAUSE, rf.FULL))
    if trade["n"] < rf.MIN_N or pause["n"] < rf.MIN_N:
        return rf.INSUFFICIENT
    if (r["cpcv"][TRADE]["promotable"] and trade["median_net"] > max(0.0, full["median_net"])
            and pause["median_net"] <= 0 and r["permutation"]["p"] < ALPHA):
        return rf.SUPPORTED
    return rf.NOT_SUPPORTED


def study(pop: rc.Population, ew20: dict[date, float], cost_model: CostModel) -> dict[str, Any]:
    cost = rc.cost_pct(cost_model)
    pairs = rf.measure(pop, cost)
    ew = [ew20.get(ev.signal_date) for ev, _ in pairs]
    groups = {rf.FULL: pairs, TRADE: [p for p, r in zip(pairs, ew, strict=True) if r is not None and r >= 0],
              PAUSE: [p for p, r in zip(pairs, ew, strict=True) if r is not None and r < 0]}
    out = {"family": f"regime_filter/{pop.strategy}",
           "population": {**rf.population_block(pop), "ew20_undefined": ew.count(None)},
           **rf.report(groups, FAMILY_N, float(cost))}
    trade, full = ([t.net for _, t in groups[k] if t is not None] for k in (TRADE, rf.FULL))
    out["permutation"] = {"p": permutation_pvalue(trade, full, draws=DRAWS, seed=SEED) if trade else None,
                          "draws": DRAWS, "seed": SEED, "alpha": ALPHA}
    out["decision"] = decide(out)
    return out


def run(conn, db: Path, end: date, *, symbols=None, max_symbols=None, strategies=STRATEGIES) -> dict[str, Any]:
    cost_model = CostModel.from_config()
    daily, source = rc.core_ew(conn, db, end)
    m = rf.meta("scripts/backtest_market_pause.py", "market-wide pause, IMPLEMENTATION_PLAN 3, pre-registered",
                db, end, symbols, max_symbols, FAMILY_N, cost_model)
    m |= {"rule": RULE, "alpha": ALPHA, "ew": {"rule": EW_RULE, "source": source, "label": rc.SURVIVORSHIP_LABEL,
                                               "sessions": len(daily)}}
    ew20 = ew20_table(daily)
    return {"meta": m, "strategies": {
        s: study(rf.build_population(s, conn, db, end, symbols, max_symbols), ew20, cost_model) for s in strategies
    }}


def _extra(b: dict[str, Any]) -> list[str]:
    p = b["permutation"]
    return [f"  EW20 undefined (y off the EW grid or a window gap): {b['population']['ew20_undefined']} events, "
            "in neither cohort",
            f"  permutation p {rf.num(p['p'], '.4f')} (TRADE median vs {p['draws']} FULL subsamples, seed {p['seed']}; "
            f"alpha {p['alpha']})",
            f"  DECISION {b['family']}: {b['decision']}  ({RULE}; insufficient when TRADE or PAUSE n < {rf.MIN_N})"]


TITLE = "MARKET-WIDE PAUSE STUDY"


def render(doc: dict[str, Any]) -> str:
    ew = doc["meta"]["ew"]
    return rf.render_text(doc, TITLE, _extra) + rc.ascii_text(
        f"\nEW series: {ew['rule']}; {ew['source']}, {ew['sessions']} sessions [{ew['label']}]")


def main(argv: list[str] | None = None) -> int:
    return rf.cli(argv, prog="backtest_market_pause", title=TITLE, run_fn=run, render_fn=render, prefix="market_pause")


if __name__ == "__main__":
    raise SystemExit(main())
