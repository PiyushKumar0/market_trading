#!/usr/bin/env python
"""C3 selection-filter study (IMPLEMENTATION_PLAN §6.1 "C3 SELECTION-FILTER STUDY — PRE-REGISTERED 2026-10-07").

Expect null. Per strategy (brk20 V2-N5 fills, hi52 v2), the arm (ii) construct over the admitted
events; each of five pinned filters, evaluated at the signal session y from bars through y, splits
the same events (pass vs all). Family N = 5 per strategy, deflated together; no combinations.
Evidence only; wires nothing. Exit codes: 0 ran, 2 refused.
"""

from __future__ import annotations

import sys
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import research_common as rc  # noqa: E402
from engine.strategy.cost_model import CostModel  # noqa: E402
from engine.strategy.indicators import wilder_atr  # noqa: E402

rf = rc._load_script("backtest_regime_filter")

STRATEGIES = ("brk20", "hi52")
FAMILY_N = 5


@dataclass(frozen=True)
class Bars:
    """One event's inputs, each ending at the signal session y; ``index`` is None without a NIFTY 50 bar on y."""

    close: np.ndarray
    high: np.ndarray
    low: np.ndarray
    volume: np.ndarray
    atr: np.ndarray
    index: np.ndarray | None


def bars_through(ev: rc.Event, atr: np.ndarray, index) -> Bars:
    s, k = ev.series, ev.signal_idx + 1
    return Bars(s.close[:k], s.high[:k], s.low[:k], s.volume[:k], atr[:k], rf.closes_through(index, ev.signal_date))


def _sma(x: np.ndarray, n: int, back: int = 0) -> float:
    end = len(x) - back
    return float(x[end - n:end].mean())


# ----------------------------------------------------------------------------- the five filters
# Each returns None when its inputs do not reach back far enough; None never passes.
def rs63(b: Bars) -> bool | None:
    """63-session return minus NIFTY 50's > 0."""
    if b.index is None or len(b.close) < 64 or len(b.index) < 64:
        return None
    return bool(b.close[-1] / b.close[-64] > b.index[-1] / b.index[-64])


def trend_template(b: Bars) -> bool | None:
    """close > SMA50 > SMA150 > SMA200, SMA200(y) > SMA200(y-20), close >= 1.25 x 252-session low,
    close >= 0.75 x 252-session high (daily lows and highs)."""
    c = b.close
    if len(c) < 252:
        return None
    s50, s150, s200 = _sma(c, 50), _sma(c, 150), _sma(c, 200)
    return bool(c[-1] > s50 > s150 > s200 and s200 > _sma(c, 200, 20)
                and c[-1] >= 1.25 * b.low[-252:].min() and c[-1] >= 0.75 * b.high[-252:].max())


def atr_contraction(b: Bars) -> bool | None:
    """ATR14(y) <= 0.8 x the median ATR14 over sessions y-60 .. y-1."""
    window = b.atr[-61:]
    if len(window) < 61 or not np.isfinite(window).all():
        return None
    return bool(window[-1] <= 0.8 * np.median(window[:-1]))


def stage2(b: Bars) -> bool | None:
    """close > SMA150 AND SMA150(y) > SMA150(y-20)."""
    c = b.close
    if len(c) < 170:
        return None
    return bool(c[-1] > _sma(c, 150) > _sma(c, 150, 20))


def acc_dist(b: Bars) -> bool | None:
    """Over the 25 sessions ending y, up-days minus down-days, each on volume above the prior session's, > 0."""
    if len(b.close) < 26:
        return None
    move, heavier = np.diff(b.close[-26:]), np.diff(b.volume[-26:]) > 0
    return bool(int((heavier & (move > 0)).sum()) - int((heavier & (move < 0)).sum()) > 0)


FILTERS: dict[str, Callable[[Bars], bool | None]] = {
    "rs63_vs_nifty50": rs63, "trend_template": trend_template, "atr_contraction": atr_contraction,
    "stage2": stage2, "acc_dist": acc_dist,
}


# ----------------------------------------------------------------------------- C3
def decide(r: dict[str, Any], name: str) -> str:
    c = r["cohorts"][name]
    if c["n"] < rf.MIN_N:
        return rf.INSUFFICIENT
    if r["cpcv"][name]["promotable"] and c["median_net"] > max(0.0, r["cohorts"][rf.FULL]["median_net"]):
        return rf.SUPPORTED
    return rf.NOT_SUPPORTED


def study(pop: rc.Population, index, cost_model: CostModel) -> dict[str, Any]:
    cost = rc.cost_pct(cost_model)
    pairs = rf.measure(pop, cost)
    atr = {sym: wilder_atr(s.high, s.low, s.close, rc.ATR_PERIOD).to_numpy()
           for sym, s in {ev.symbol: ev.series for ev in pop.events}.items()}
    verdicts = {name: [] for name in FILTERS}
    for ev, _ in pairs:
        b = bars_through(ev, atr[ev.symbol], index)
        for name, f in FILTERS.items():
            verdicts[name].append(f(b))
    groups = {rf.FULL: pairs} | {n: [p for p, ok in zip(pairs, v, strict=True) if ok] for n, v in verdicts.items()}
    out = {"population": {**rf.population_block(pop),
                          "filter_undefined": {n: v.count(None) for n, v in verdicts.items()}},
           **rf.report(groups, FAMILY_N, float(cost))}
    out["decision"] = {n: decide(out, n) for n in FILTERS}
    return out


def run(conn, db: Path, end: date, *, symbols=None, max_symbols=None, strategies=STRATEGIES) -> dict[str, Any]:
    cost_model = CostModel.from_config()
    index = rf.index_series(conn, end)
    m = rf.meta("scripts/backtest_selection_filters.py",
                "C3 selection filters, IMPLEMENTATION_PLAN 6.1, pre-registered 2026-10-07",
                db, end, symbols, max_symbols, FAMILY_N, cost_model)
    m["filters"] = {n: " ".join(f.__doc__.split()) for n, f in FILTERS.items()}
    return {"meta": m, "strategies": {
        s: study(rf.build_population(s, conn, db, end, symbols, max_symbols), index, cost_model) for s in strategies
    }}


def _extra(b: dict[str, Any]) -> list[str]:
    und = b["population"]["filter_undefined"]
    return [f"  DECISION {n:<16} {d:<14} (undefined, never passing: {und[n]})" for n, d in b["decision"].items()] + [
        f"  supported = pass cohort promotable at N={FAMILY_N} (deflated CPCV AND T+20 median net > 0) AND median "
        f"net > unfiltered; insufficient when pass n < {rf.MIN_N}"]


TITLE = "C3 SELECTION-FILTER STUDY"


def main(argv: list[str] | None = None) -> int:
    return rf.cli(argv, prog="backtest_selection_filters", title=TITLE, run_fn=run,
                  render_fn=lambda doc: rf.render_text(doc, TITLE, _extra), prefix="selection_filters")


if __name__ == "__main__":
    raise SystemExit(main())
