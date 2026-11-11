#!/usr/bin/env python
"""Pool widening study (``edge/pool_widening``, registered 2026-11-11): do the live swing legs hold beyond the core?

The registration entry in IMPLEMENTATION_PLAN.md is the spec of record; this docstring summarises it.

* **Stocks.** Every ``bars_1d`` symbol of the whole-market research copy except the index rows and ETFs (the hvol
  study's test: NSE's ETF list plus ``(BEES|ETF|IETF)$``). Historical bhavcopy files hold names that later delisted.
* **Bands (point in time).** On signal day ``y`` a stock's liquidity is its median ``close x volume`` over its own 20
  sessions strictly before ``y`` (exactly 20 required, the hvol convention). The stocks with a bar on ``y`` and a
  liquidity are ranked (descending, symbol ascending on ties) and banded by ``pool_widening_common.band_of``: C0 the
  500 most liquid (the control), W1 past rank 500 at >= Rs 5 cr, W2 Rs 2 cr to under Rs 5 cr.
* **Legs, as live.** brk20 at the pre-registered params, the 5-session limit fill, the live regime gate at ``y``, exit
  ``close(fill+20)``; ins (``insider_net_buy`` at the live Rs 1 cr floor), fill ``open(y+1)``, exit at T+20.
  Time-only exits, no stop. Signals from ``SIGNALS_FROM``; the registered helpers' vetoes; a fill session whose high
  equals its low (locked all day) is unfillable and dropped. hi52 is not a leg (see ``LEGS``).
* **Cost.** Statutory fees at Rs 20,000 CNC (``CostModel.fee_breakeven_pct``) plus the band's measured spread.
* **Benchmark.** Equal-weight mean gross ``open(entry) -> close(exit)`` of the stocks in the trade's band on ``y``
  with bars on both dates and no structural ex-date between them, the trade's own stock included.
* **Verdict per leg x widening band** (``FAMILY_N`` = 4 trials): seven criteria, all on unrounded values, a missing
  value failing. C0 is reported under the same table and is not a trial.

Read-only against a research snapshot; refuses the live store and market hours. Exit codes: 0 ran, 2 refused.
"""

from __future__ import annotations

import argparse
import math
import sys
import time
from bisect import bisect_left
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import research_common as rc  # noqa: E402,I001  imports engine first (native import-order guard)
import pool_widening_common as pw  # noqa: E402
from engine.core.clock import IST  # noqa: E402
from engine.core.config import load_settings, repo_root  # noqa: E402
from engine.learning.validate import fold_pass_min  # noqa: E402
from engine.strategy.cost_model import CostModel  # noqa: E402
from engine.strategy.scanners import hi52  # noqa: E402

bb, es = rc.bb, rc.es
hv = rc._load_script("backtest_hvol")
rf = rc._load_script("backtest_regime_filter")
Series = bb.Series

# =============================================================================== registered constants
REGISTERED_ON = "2026-11-11"
FAMILY = "edge/pool_widening"
FAMILY_N = 4
#: hi52 is not a leg: its full-market backtest (median net +0.10% against +1.71% on the index) was seen in 2026-09 on
#: the same bars, and its forward kill rule is demoting it on the core.
LEGS: tuple[str, ...] = ("brk20", "ins")
SIGNALS_FROM = date(2023, 8, 1)
#: exit_sim horizon = sessions after the fill session. T+20 counts the fill session as 1; brk20's registered exit is
#: close(fill+20).
EXIT_SESSIONS: dict[str, int] = {"brk20": rc.HORIZON, "ins": rc.HORIZON - 1}
PRODUCT = "CNC"
NOTIONAL = Decimal("20000")
#: Measured 2026-11-11 by ``pool_widening_spreads.py`` (eight rounds 13:07-15:08, bands as of 2026-11-09): per band
#: the median over names of each name's median quoted spread, rounded UP to 2 dp (the ``costs.yaml`` convention); the
#: 75th percentile is the diagnostic. Percent of price, one full spread per round trip. Unrounded medians: C0 0.0313,
#: W1 0.0629, W2 0.1042; p75: 0.0487, 0.0834, 0.1321.
BAND_SPREAD_PCT: dict[str, Decimal] = {
    pw.C0: Decimal("0.04"),
    pw.W1: Decimal("0.07"),
    pw.W2: Decimal("0.11"),
}
BAND_SPREAD_P75_PCT: dict[str, Decimal] = {
    pw.C0: Decimal("0.05"),
    pw.W1: Decimal("0.09"),
    pw.W2: Decimal("0.14"),
}
MIN_N = 100
CLUSTERED_T_MIN = 2.0
BOOTSTRAP_REPS = 2000
BOOTSTRAP_SEED = 20261111
CI_LEVEL = 0.95
TOP_TAIL_FRAC = 0.01
HELD_OUT_FROM = date(2026, 1, 1)
DEFAULT_ETF_LIST_REL = hv.DEFAULT_ETF_LIST_REL

WIDEN, REFUTED, INSUFFICIENT, NOT_SHOWN = "WIDEN", "REFUTED", "INSUFFICIENT", "NOT SHOWN"
BAND_CODE = {pw.C0: 0, pw.W1: 1, pw.W2: 2}
NO_BAND = -1


# =============================================================================== bands + benchmark
def asof_liquidity(s: Series) -> np.ndarray:
    """Per own session ``i``: median traded value over sessions ``i-20 .. i-1`` (NaN below 20 prior), traded value
    rounded to paise first. Identical to ``backtest_hvol.features(s).liquidity``."""
    tv = pd.Series(np.round(np.asarray(s.close, dtype="float64") * np.asarray(s.volume, dtype="float64"), 2))
    through = tv.rolling(pw.LIQ_SESSIONS, min_periods=pw.LIQ_SESSIONS).median().to_numpy()
    return np.concatenate([[np.nan], through[:-1]]) if len(through) else through


def band_row(liq: np.ndarray, valid: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """``(band code, rank)`` per column for one session. Columns are in symbol order, so a stable sort on descending
    liquidity breaks ties by symbol ascending. Columns not ``valid`` get ``NO_BAND`` and rank 0."""
    band = np.full(liq.shape, NO_BAND, dtype=np.int8)
    rank = np.zeros(liq.shape, dtype=np.int32)
    idx = np.flatnonzero(valid)
    if idx.size == 0:
        return band, rank
    order = idx[np.argsort(-liq[idx], kind="stable")]
    rank[order] = np.arange(1, order.size + 1)
    for j in order:
        b = pw.band_of(int(rank[j]), float(liq[j]))
        band[j] = NO_BAND if b is None else BAND_CODE[b]
    return band, rank


@dataclass
class Grid:
    """Every stock on the snapshot's union session grid: bands, prices and structural ex-date counts."""

    dates: list[date]
    pos: dict[date, int]
    symbols: list[str]
    col: dict[str, int]
    band: np.ndarray            # int8 [session, stock]
    rank: np.ndarray            # int32 [session, stock], 0 = unranked
    liquidity: np.ndarray       # as-of liquidity on own bar sessions, NaN elsewhere
    present: np.ndarray
    open: np.ndarray
    close: np.ndarray
    ex_lt: np.ndarray           # structural ex-dates strictly before each session
    ex_le: np.ndarray           # structural ex-dates on or before each session
    _bench: dict[tuple[int, int, int, int], tuple[float | None, int]] = field(default_factory=dict)

    @classmethod
    def build(cls, series: Mapping[str, Series], structural: Mapping[str, Sequence[date]]) -> Grid:
        dates = sorted({d for s in series.values() for d in s.dates})
        grid = np.array(dates, dtype="datetime64[D]")
        symbols = sorted(series)
        shape = (len(dates), len(symbols))
        o, c, liq = (np.full(shape, np.nan) for _ in range(3))
        present = np.zeros(shape, dtype=bool)
        ex_lt, ex_le = np.zeros(shape, dtype=np.int32), np.zeros(shape, dtype=np.int32)
        for j, sym in enumerate(symbols):
            s = series[sym]
            rows = np.searchsorted(grid, np.array(s.dates, dtype="datetime64[D]"))
            o[rows, j], c[rows, j], liq[rows, j] = s.open, s.close, asof_liquidity(s)
            present[rows, j] = True
            ex = structural.get(sym)
            if ex:
                exd = np.array(sorted(ex), dtype="datetime64[D]")
                ex_lt[:, j] = np.searchsorted(exd, grid, side="left")
                ex_le[:, j] = np.searchsorted(exd, grid, side="right")
        band = np.full(shape, NO_BAND, dtype=np.int8)
        rank = np.zeros(shape, dtype=np.int32)
        with np.errstate(invalid="ignore"):
            valid = present & np.isfinite(liq)
        for g in range(len(dates)):
            band[g], rank[g] = band_row(liq[g], valid[g])
        return cls(dates, {d: i for i, d in enumerate(dates)}, symbols, {s: j for j, s in enumerate(symbols)},
                   band, rank, liq, present, o, c, ex_lt, ex_le)

    def band_at(self, symbol: str, d: date) -> tuple[int, int, float]:
        """``(band code, rank, liquidity)`` of ``symbol`` on session ``d``."""
        g, j = self.pos[d], self.col[symbol]
        return int(self.band[g, j]), int(self.rank[g, j]), float(self.liquidity[g, j])

    def benchmark(self, code: int, y: date, entry: date, exit_: date) -> tuple[float | None, int]:
        """``(EW mean gross %, members)`` of band ``code`` on ``y`` over ``open(entry) -> close(exit)``."""
        key = (code, self.pos[y], self.pos[entry], self.pos[exit_])
        hit = self._bench.get(key)
        if hit is not None:
            return hit
        gy, ge, gx = key[1:]
        with np.errstate(invalid="ignore"):
            ok = ((self.band[gy] == code) & self.present[ge] & self.present[gx]
                  & ((self.ex_le[gx] - self.ex_lt[ge]) == 0) & (self.open[ge] > 0) & (self.close[gx] > 0))
        r = (self.close[gx][ok] / self.open[ge][ok] - 1.0) * 100.0
        out = (float(r.mean()) if r.size else None, int(r.size))
        self._bench[key] = out
        return out


# =============================================================================== leg populations
@dataclass(frozen=True)
class Candidate:
    """One admitted signal with its fill, before the walk."""

    leg: str
    event: rc.Event
    band: str
    rank: int
    liquidity: float


def _bump(counts: dict[str, int], key: str) -> None:
    counts[key] = counts.get(key, 0) + 1


def _in_window(d: date, counts: dict[str, int]) -> bool:
    if d < SIGNALS_FROM:
        _bump(counts, "before_window")
        return False
    _bump(counts, "signals_in_window")
    return True


def _gate(leg: str, sym: str, s: Series, i: int, grid: Grid, atr: np.ndarray,
          counts: dict[str, int]) -> tuple[str, int, float] | None:
    """Band and forward-window admission shared by the legs; ``None`` = dropped (counted)."""
    code, rank, liq = grid.band_at(sym, s.dates[i])
    if code == NO_BAND:
        _bump(counts, "no_band")
        return None
    if not rc._admitted(s, i, rc.SPAN[leg], atr, counts):
        return None
    return pw.BANDS[code], rank, liq


def _fillable(s: Series, fill_idx: int, counts: dict[str, int]) -> bool:
    if float(s.high[fill_idx]) == float(s.low[fill_idx]):
        _bump(counts, "locked_fill_session")
        return False
    return True


def brk20_candidates(series: Mapping[str, Series], grid: Grid, ex, structural, index) -> tuple[list[Candidate], dict]:
    counts: dict[str, int] = {}
    out: list[Candidate] = []
    params, floor = bb.PRE_REGISTERED_PARAMS, bb.PRE_REGISTERED_FLOOR_PARAMS
    for sym, s in series.items():
        atr = rc._atr(s)
        for sig in bb.signals_for(s, params, ex.get(sym, []), structural.get(sym, []),
                                  floor_params=floor, veto_counts=counts):
            if not _in_window(sig.signal_date, counts):
                continue
            regime = rf.regime_on(rf.closes_through(index, sig.signal_date))
            if regime is not True:
                _bump(counts, "regime_blocked" if regime is False else "regime_undefined")
                continue
            got = _gate("brk20", sym, s, sig.idx, grid, atr, counts)
            if got is None:
                continue
            fill = bb.fill_for(s, sig, bb.VARIANT_LIMIT_5)
            if fill is None:
                _bump(counts, "unfilled")
                continue
            if not _fillable(s, fill[0], counts):
                continue
            ev = rc.Event(sym, s, sig.idx, fill[0], rc.px(fill[1]), float(atr[sig.idx]))
            out.append(Candidate("brk20", ev, *got))
    return out, counts


def ins_candidates(conn, series: Mapping[str, Series], grid: Grid, structural) -> tuple[list[Candidate], dict]:
    store = rc.store_reader(conn)
    threshold = load_settings().filings.insider_min_value_inr
    counts: dict[str, int] = {}
    out: list[Candidate] = []
    for sym, s in series.items():
        w0 = int(np.searchsorted(np.array(s.dates, dtype="datetime64[D]"), np.datetime64(SIGNALS_FROM, "D")))
        if len(s) - w0 < es.VOL_WINDOW + max(es.HORIZONS) + 2:
            continue
        atr = rc._atr(s)
        for t in es.insider_buy_events(s.dates[w0:], store.get_insider_trades(symbol=sym), threshold):
            i = w0 + t
            if not _in_window(s.dates[i], counts):
                continue
            if i + 1 < len(s) and bb._unadjusted_at(structural.get(sym, []), s.dates[i + 1]):
                _bump(counts, hi52.VETO_UNADJUSTED_HISTORY)
                continue
            got = _gate("ins", sym, s, i, grid, atr, counts)
            if got is None or not _fillable(s, i + 1, counts):
                continue
            ev = rc.Event(sym, s, i, i + 1, rc.px(s.open[i + 1]), float(atr[i]))
            out.append(Candidate("ins", ev, *got))
    return out, counts


# =============================================================================== measurement
@dataclass(frozen=True)
class Trade:
    """One walked trade. Returns are PERCENT."""

    leg: str
    band: str
    symbol: str
    signal_date: date
    fill_date: date
    exit_date: date
    gross: float
    net: float
    benchmark: float | None
    bench_n: int
    excess: float | None
    rank: int
    liquidity: float
    locked_exit: bool


def band_cost(band: str, fees_pct: Decimal) -> Decimal:
    return fees_pct + BAND_SPREAD_PCT[band]


def walk_all(cands: Sequence[Candidate], grid: Grid, structural, fees_pct: Decimal,
             counts: dict[str, int]) -> list[Trade]:
    out: list[Trade] = []
    for c in cands:
        ev, h = c.event, EXIT_SESSIONS[c.leg]
        o = rc.walk(ev, stop=None, target=None, horizon=h, cost_pct=band_cost(c.band, fees_pct),
                    ex_dates=structural.get(ev.symbol, []))
        if o.status == "void_ca":
            _bump(counts, "void_ca")
            continue
        if o.status != "exit":
            raise AssertionError(f"{ev.symbol} {ev.fill_date}: walk ended {o.status}")
        x = ev.fill_idx + h
        exit_d = ev.series.dates[x]
        bench, n = grid.benchmark(BAND_CODE[c.band], ev.signal_date, ev.fill_date, exit_d)
        gross = float(o.gross_pct)
        out.append(Trade(c.leg, c.band, ev.symbol, ev.signal_date, ev.fill_date, exit_d, gross, float(o.net_pct),
                         bench, n, None if bench is None else gross - bench, c.rank, c.liquidity,
                         float(ev.series.high[x]) == float(ev.series.low[x])))
    return out


def _mean(xs: Sequence[float]) -> float | None:
    return float(np.mean(xs)) if len(xs) else None


def _median(xs: Sequence[float]) -> float | None:
    return float(np.median(xs)) if len(xs) else None


def _r(x: Any, nd: int = 6) -> Any:
    return round(float(x), nd) if isinstance(x, float) and math.isfinite(x) else x


def measure_cell(trades: Sequence[Trade], cost_pct: float) -> dict[str, Any]:
    """Every criterion input and descriptive statistic of one leg x band cell, UNROUNDED."""
    gross = [t.gross for t in trades]
    net = [t.net for t in trades]
    with_bench = [t for t in trades if t.excess is not None]
    excess = [t.excess for t in with_bench]
    boot_net = hv.clustered_bootstrap([t.signal_date for t in trades], {"net": net},
                                      reps=BOOTSTRAP_REPS, seed=BOOTSTRAP_SEED, level=CI_LEVEL)["net"]
    boot_exc = hv.clustered_bootstrap([t.signal_date for t in with_bench], {"excess": excess},
                                      reps=BOOTSTRAP_REPS, seed=BOOTSTRAP_SEED, level=CI_LEVEL)["excess"]
    rc_trades = [rc.Trade(t.symbol, t.signal_date, t.fill_date, "time", t.gross, t.net) for t in trades]
    cpcv = rc.cpcv(rc_trades, FAMILY_N, cost_pct)
    held = [t.excess for t in with_bench if t.signal_date >= HELD_OUT_FROM]
    return {
        "n": len(trades),
        "n_signal_dates": len({t.signal_date for t in trades}),
        "n_symbols": len({t.symbol for t in trades}),
        "cost_pct": cost_pct,
        "mean_gross": _mean(gross), "median_gross": _median(gross),
        "mean_net": _mean(net), "median_net": _median(net),
        "hit_rate_net": (sum(v > 0 for v in net) / len(net)) if net else None,
        "t_net": boot_net["t"], "net_ci": [boot_net["ci_lo"], boot_net["ci_hi"]],
        "n_with_benchmark": len(with_bench),
        "mean_benchmark": _mean([t.benchmark for t in with_bench]),
        "mean_benchmark_members": _mean([t.bench_n for t in with_bench]),
        "mean_excess": _mean(excess), "median_excess": _median(excess),
        "excess_ci_lo": boot_exc["ci_lo"], "excess_ci_hi": boot_exc["ci_hi"],
        "excess_degenerate": boot_exc["degenerate"], "t_excess": boot_exc["t"],
        "cpcv_promotable": bool(cpcv["promotable"]), "cpcv_fold_pass": cpcv["fold_pass_fraction"],
        "cpcv_method": cpcv["cv_method"], "cpcv_reasons": cpcv["reasons"],
        "top1_dropped_mean_net": hv.top_tail_dropped_mean(net, TOP_TAIL_FRAC),
        "held_out_n": len(held), "held_out_mean_excess": _mean(held),
        "locked_exit_sessions": sum(t.locked_exit for t in trades),
    }


def _gt(x: Any, thr: float) -> bool:
    return x is not None and not isinstance(x, bool) and math.isfinite(float(x)) and float(x) > thr


def verdict(m: Mapping[str, Any]) -> dict[str, Any]:
    """The registered seven criteria and the label. ``INSUFFICIENT`` below ``MIN_N``; ``WIDEN`` when all pass;
    ``REFUTED`` when mean excess <= 0; otherwise ``NOT SHOWN``."""
    lo, hi = m.get("excess_ci_lo"), m.get("excess_ci_hi")
    ci_excludes_0 = not m.get("excess_degenerate", True) and _gt(lo, 0.0) and hi is not None
    rows = [
        (1, f"n >= {MIN_N}", m.get("n") is not None and m["n"] >= MIN_N),
        (2, "median gross > the band's round-trip cost",
         _gt(m.get("median_gross"), float(m["cost_pct"]) if m.get("cost_pct") is not None else math.inf)),
        (3, f"mean net > 0 AND clustered t > {CLUSTERED_T_MIN:g}",
         _gt(m.get("mean_net"), 0.0) and _gt(m.get("t_net"), CLUSTERED_T_MIN)),
        (4, "mean excess > 0 AND its 95% clustered CI lies above 0", _gt(m.get("mean_excess"), 0.0) and ci_excludes_0),
        (5, f"CPCV promotable at N = {FAMILY_N}", m.get("cpcv_promotable") is True),
        (6, "mean net > 0 with the top 1% of trades by net removed", _gt(m.get("top1_dropped_mean_net"), 0.0)),
        (7, f"held-out slice (signals >= {HELD_OUT_FROM}): mean excess > 0", _gt(m.get("held_out_mean_excess"), 0.0)),
    ]
    out = [{"id": i, "criterion": c, "pass": bool(p)} for i, c, p in rows]
    n = m.get("n") or 0
    if n < MIN_N:
        label = INSUFFICIENT
    elif all(r["pass"] for r in out):
        label = WIDEN
    elif m.get("mean_excess") is not None and float(m["mean_excess"]) <= 0.0:
        label = REFUTED
    else:
        label = NOT_SHOWN
    return {"rows": out, "verdict": label}


def diagnostics(trades: Sequence[Trade], band: str, nifty500: set[str], total_market: set[str],
                inactive: set[str]) -> dict[str, Any]:
    by_year: dict[str, list[Trade]] = defaultdict(list)
    by_class: dict[str, list[Trade]] = defaultdict(list)
    for t in trades:
        by_year[str(t.signal_date.year)].append(t)
        by_class[hv.index_class(t.symbol, nifty500, total_market)].append(t)

    def brief(ts: Sequence[Trade]) -> dict[str, Any]:
        return {"n": len(ts), "mean_net": _r(_mean([t.net for t in ts])),
                "mean_excess": _r(_mean([t.excess for t in ts if t.excess is not None]))}

    mean_net = _mean([t.net for t in trades])
    p75_gap = float(BAND_SPREAD_P75_PCT[band] - BAND_SPREAD_PCT[band])
    return {
        "by_year": {k: brief(v) for k, v in sorted(by_year.items())},
        "by_current_index_class": {"label": rc.SURVIVORSHIP_LABEL, **{k: brief(v) for k, v in sorted(by_class.items())}},
        "inactive_now_share": _r(sum(t.symbol in inactive for t in trades) / len(trades)) if trades else None,
        "mean_net_at_p75_spread": None if mean_net is None else _r(mean_net - p75_gap),
    }


# =============================================================================== the study
def run_study(conn, *, db_path: Path, etf, symbols: Sequence[str] | None = None,
              max_symbols: int | None = None, counts_only: bool = False) -> dict[str, Any]:
    """The registered study. ``counts_only`` keeps funnels and per-cell trade counts and drops every return."""
    if set(BAND_SPREAD_PCT) != set(pw.BANDS) or set(BAND_SPREAD_P75_PCT) != set(pw.BANDS):
        raise rc.Refusal("band spreads are not recorded (BAND_SPREAD_PCT / BAND_SPREAD_P75_PCT)")
    t0 = time.monotonic()
    cm = CostModel.from_config()
    fees_pct = cm.fee_breakeven_pct(NOTIONAL, PRODUCT)
    series = bb.load_series(conn, rc.FULL_HISTORY_START, date.max, symbols=symbols, max_symbols=max_symbols)
    for name in hv.INDEX_ROWS:
        series.pop(name, None)
    series, etf_counts = hv.exclude_etfs(series, etf)
    if not series:
        raise rc.Refusal("no bars_1d rows for the requested population")
    ex, structural = bb.load_ex_dates(conn), bb.load_structural_ex_dates(conn)
    grid = Grid.build(series, structural)
    index = rf.index_series(conn, grid.dates[-1])
    nifty500, total_market, seeds = hv.load_membership()
    recent = set(grid.dates[-20:])
    inactive = {sym for sym, s in series.items() if s.dates[-1] not in recent}

    built = {
        "brk20": brk20_candidates(series, grid, ex, structural, index),
        "ins": ins_candidates(conn, series, grid, structural),
    }
    legs: dict[str, Any] = {}
    for leg in LEGS:
        cands, counts = built[leg]
        trades = walk_all(cands, grid, structural, fees_pct, counts)
        cells: dict[str, Any] = {}
        for band in pw.BANDS:
            ts = [t for t in trades if t.band == band]
            if counts_only:
                cells[band] = {"trial": band in pw.WIDENING_BANDS, "n": len(ts),
                               "n_signal_dates": len({t.signal_date for t in ts}),
                               "n_symbols": len({t.symbol for t in ts})}
                continue
            m = measure_cell(ts, float(band_cost(band, fees_pct)))
            cells[band] = {
                "trial": band in pw.WIDENING_BANDS,
                "measures": {k: _r(v) if isinstance(v, float) else v for k, v in m.items()},
                **verdict(m),
                "diagnostics": diagnostics(ts, band, nifty500, total_market, inactive),
            }
        legs[leg] = {"funnel": dict(sorted(counts.items())), "trades": len(trades), "cells": cells}

    registered = not symbols and not max_symbols
    g0 = bisect_left(grid.dates, SIGNALS_FROM)
    band_sizes = {b: (int(np.mean((grid.band[g0:] == BAND_CODE[b]).sum(axis=1))) if g0 < len(grid.dates) else None)
                  for b in pw.BANDS}
    return {
        "meta": {
            "script": "scripts/pool_widening_study.py",
            "registration": f"pool widening, registered {REGISTERED_ON}", "family": FAMILY,
            "generated_at": datetime.now(IST).isoformat(timespec="seconds"),
            "db": str(db_path), "window": {"bars_from": str(grid.dates[0]), "bars_to": str(grid.dates[-1]),
                                           "signals_from": str(SIGNALS_FROM)},
            "n_stocks": len(series), "registered_population": registered, "counts_only": counts_only,
            "population_override": {"symbols": list(symbols) if symbols else None, "max_symbols": max_symbols},
            "etf_exclusion": {"list_path": etf.path, "sha256": etf.sha256, "pattern": hv.ETF_PATTERN.pattern,
                              **etf_counts},
            "family_n": FAMILY_N, "fold_pass_min": fold_pass_min(FAMILY_N),
            "exit_sessions_after_fill": EXIT_SESSIONS, "product": PRODUCT, "notional_inr": str(NOTIONAL),
            "fees_pct": float(fees_pct), "band_spread_pct": {k: str(v) for k, v in BAND_SPREAD_PCT.items()},
            "band_spread_p75_pct": {k: str(v) for k, v in BAND_SPREAD_P75_PCT.items()},
            "cost_file_spread_pct": float(cm.spread_pct),
            "band_rule": {"core_rank": pw.CORE_RANK, "floor_w1_inr": pw.FLOOR_W1_INR, "floor_w2_inr": pw.FLOOR_W2_INR,
                          "liquidity_sessions": pw.LIQ_SESSIONS},
            "mean_band_sizes_from_signals_from": band_sizes,
            "constants": {"min_n": MIN_N, "clustered_t_min": CLUSTERED_T_MIN, "bootstrap_reps": BOOTSTRAP_REPS,
                          "bootstrap_seed": BOOTSTRAP_SEED, "ci_level": CI_LEVEL, "top_tail_frac": TOP_TAIL_FRAC,
                          "held_out_from": str(HELD_OUT_FROM)},
            "membership_seeds": seeds, "inactive_now_stocks": len(inactive),
            "corp_actions_coverage": bb.corp_actions_coverage(conn),
            "runtime_seconds": round(time.monotonic() - t0, 1),
        },
        "legs": legs,
        "notes": [
            "Bands are point in time, by liquidity alone: no current index membership, MIS eligibility or "
            "surveillance history. bars_1d holds names that later delisted.",
            "Known optimism: open fills assume the open is reachable (only all-day locks are caught); the "
            "official close is a VWAP no order fills at; names then under surveillance are included; spreads were "
            "measured on one afternoon and applied backwards; brk20's benchmark runs from open(fill session) while "
            "its fill is the limit level.",
            "C0 is the control and not a trial; the current-index-class split is a " + rc.SURVIVORSHIP_LABEL + ".",
        ] + ([] if registered else ["POPULATION OVERRIDDEN (--symbols/--max-symbols): smoke run, NOT the "
                                    "registered study; bands and benchmarks cover the loaded names only."]),
    }


# =============================================================================== rendering
def _fmt(v: Any, spec: str = "+.3f") -> str:
    return "-" if v is None else format(v, spec)


def render_text(doc: dict[str, Any]) -> str:
    m = doc["meta"]
    out = [f"POOL WIDENING STUDY ({m['registration']}, {m['family']}, N={m['family_n']}, "
           f"fold_pass_min {m['fold_pass_min']:.0%})",
           f"db {m['db']}  bars {m['window']['bars_from']} -> {m['window']['bars_to']}  signals from "
           f"{m['window']['signals_from']}  stocks {m['n_stocks']}",
           f"cost = fees {m['fees_pct']:.4f}% + band spread {m['band_spread_pct']} (cost file spread "
           f"{m['cost_file_spread_pct']}%)", ""]
    if not m["registered_population"]:
        out += ["POPULATION OVERRIDDEN: smoke run, NOT the registered study", ""]
    if m.get("counts_only"):
        out += [f"COUNTS ONLY (no returns). Mean band sizes from {m['window']['signals_from']}: "
                f"{m['mean_band_sizes_from_signals_from']}; ETF exclusion {m['etf_exclusion']}; "
                f"inactive now {m['inactive_now_stocks']}", ""]
        for leg, b in doc["legs"].items():
            out.append(f"[{leg}] trades {b['trades']}  funnel {b['funnel']}")
            out += [f"  {band:<21} n {c['n']:>5}  signal dates {c['n_signal_dates']:>4}  stocks {c['n_symbols']:>4}"
                    for band, c in b["cells"].items()]
        return rc.ascii_text("\n".join(out))
    hdr = ("  band                    n  dates  med gross  mean net  med net    t  mean exc  exc CI            "
           "CPCV  held-out exc  verdict")
    for leg, b in doc["legs"].items():
        out += [f"[{leg}] trades {b['trades']}  funnel {b['funnel']}", hdr]
        for band, c in b["cells"].items():
            x = c["measures"]
            ci = f"[{_fmt(x['excess_ci_lo'])}, {_fmt(x['excess_ci_hi'])}]"
            tag = c["verdict"] if c["trial"] else f"({c['verdict']}, control)"
            out.append(f"  {band:<21} {x['n']:>5} {x['n_signal_dates']:>6} {_fmt(x['median_gross']):>10} "
                       f"{_fmt(x['mean_net']):>9} {_fmt(x['median_net']):>8} {_fmt(x['t_net'], '.2f'):>4} "
                       f"{_fmt(x['mean_excess']):>9} {ci:<17} {str(x['cpcv_promotable']):>5} "
                       f"{_fmt(x['held_out_mean_excess']):>13}  {tag}")
            failed = [str(r["id"]) for r in c["rows"] if not r["pass"]]
            if failed:
                out.append(f"      failed criteria: {', '.join(failed)}")
        out.append("")
    out += ["Criteria: 1 n>=" + str(MIN_N) + "; 2 median gross > cost; 3 mean net > 0 and t > 2; 4 mean excess > 0 "
            "with CI above 0; 5 CPCV promotable; 6 top-1%-dropped mean net > 0; 7 held-out mean excess > 0."]
    out += doc["notes"]
    return rc.ascii_text("\n".join(out))


def render_markdown(doc: dict[str, Any]) -> str:
    return f"# Pool widening study\n\n```\n{render_text(doc)}\n```\n"


# =============================================================================== CLI
def main(argv: list[str] | None = None, *, now: datetime | None = None) -> int:
    ap = argparse.ArgumentParser(prog="pool_widening_study", description=__doc__.splitlines()[0])
    ap.add_argument("--db", type=Path, required=True, help="whole-market research snapshot")
    ap.add_argument("--etf-list", type=Path, default=repo_root() / DEFAULT_ETF_LIST_REL)
    ap.add_argument("--symbols", default=None, help="comma-separated subset (smoke runs only)")
    ap.add_argument("--max-symbols", type=int, default=None, help="cap the stocks loaded (smoke runs only)")
    ap.add_argument("--out", type=Path, default=None, help="JSON path; the .md is written beside it")
    ap.add_argument("--counts-only", action="store_true",
                    help="pipeline check: funnels and per-cell trade counts, no returns, no report written")
    args = ap.parse_args(argv)
    try:
        if reason := rc.market_hours_refusal(now):
            raise rc.Refusal(reason)
        etf = hv.load_etf_list(args.etf_list)
        conn = rc.open_snapshot(args.db)
        try:
            symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()] if args.symbols else None
            doc = run_study(conn, db_path=args.db, etf=etf, symbols=symbols, max_symbols=args.max_symbols,
                            counts_only=args.counts_only)
        finally:
            conn.close()
    except rc.Refusal as exc:
        print(f"pool_widening_study: REFUSED: {rc.ascii_text(str(exc))}", file=sys.stderr)
        return 2
    if args.counts_only:
        print(render_text(doc))
        return 0
    out = args.out or rc.default_out(args.db, "pool_widening")
    md = rc.write_reports(doc, render_markdown(doc), out)
    print(render_text(doc))
    print(f"JSON -> {out}\nMD   -> {md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
