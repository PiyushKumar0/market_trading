#!/usr/bin/env python
"""Swing timing study (``edge/swing_timing``, registered 2026-11-11): candle timing and brk20 add-ons on the live legs.

This docstring, the constants and the report's notes are the record of the registered rules. The harness is the pool
widening study's (``pool_widening_study.py``), imported and unchanged unless stated here.

* **Data and mechanics.** As the pool study: every non-ETF ``bars_1d`` stock of the research copy, delisted names in,
  signals from ``SIGNALS_FROM``; market sessions; brk20's regime gate at ``y``; the ins floor of Rs 1 cr; the AMIORG
  supplement; voids; late exits; fees at Rs 20,000 CNC plus the band's spread; the band's EW benchmark.
* **Populations, as live.** brk20 on bands C0 and W1 (band at ``y``), ins on C0. Other bands drop as
  ``out_of_population``. hi52 is not a leg.
* **Admission.** Every gate up to the fill, with one forward window for both legs: ``FILL_WINDOW_SESSIONS +
  rc.HORIZON`` market sessions after ``y`` (``short_forward_window``), so every rule of a leg reads one admitted set.
* **Live rule (the comparator).** brk20: the limit at the level over ``y+1 .. y+5``, exit ``close(fill+20)``; ins:
  ``open(y+1)``, exit at T+20. One position per stock per leg.
* **Candle entry.** The first own bar ``d`` on market sessions ``y .. y+4`` that completes a pattern of the set and
  whose stock trades on market session ``pos(d)+1``; buy at that bar's open. None: ``no_pattern`` (``no_fill_session``
  when a pattern completed but never with a next-session bar). Then the live rule's lock, exit-bar and usability
  checks and its hold from the fill. One position per stock per leg on this rule's own trades.
* **Candle exit.** On each kept, non-void live trade, the first own bar from the fill bar through the bar before
  the exit bar that completes a pattern of the set; sell at the open of the next own bar (late when that bar is not
  on the next market session). No such bar: the live exit stands. The benchmark of an open exit runs to the open of
  the exit session.
* **Add-on (brk20).** A live brk20 fill that one-position drops (``held_overlap``) and that fills before the exit
  session of the kept trade it follows; the first per kept trade. Walked as its own trade.
* **Random-timing controls.** ``CONTROL_DRAWS`` draws, seed ``CONTROL_SEED``; each draw moves only timing (entry:
  a uniform eligible day in ``y .. y+4``; exit: a uniform own bar from the fill bar through the bar before the exit
  bar, for the trades the candle rule exits only). A void draw drops that trade from that draw's mean excess.
* **Verdict** (``FAMILY_N`` = 7 trials): the pool study's seven criteria with criterion 2 = median net > 0 and CPCV
  at the cell's mean cost; the timing trials add the relative test (beats the live rule with the 95% CI of the
  difference above 0, and beats the control's 95th percentile). Unrounded values; a missing value fails.

Read-only against a research snapshot; refuses the live store and market hours. Exit codes: 0 ran, 2 refused.
"""

from __future__ import annotations

import argparse
import math
import sys
import time
from bisect import bisect_left
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any

import numpy as np

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
pws = rc._load_script("pool_widening_study")
Series = bb.Series
Candidate = pws.Candidate

# =============================================================================== registered constants
REGISTERED_ON = "2026-11-11"
FAMILY = "edge/swing_timing"
FAMILY_N = 7
LEGS: tuple[str, ...] = pws.LEGS
SIGNALS_FROM = pws.SIGNALS_FROM
EXIT_SESSIONS = pws.EXIT_SESSIONS
FILL_WINDOW_SESSIONS = pws.FILL_WINDOW_SESSIONS
#: Market sessions after ``y`` every admitted signal needs: the latest candle fill (``y+5``) plus the hold.
FORWARD_SESSIONS = FILL_WINDOW_SESSIONS + rc.HORIZON
#: The candle entry scans market sessions ``y .. y + ENTRY_SCAN_SESSIONS - 1``.
ENTRY_SCAN_SESSIONS = 5
POPULATION: dict[str, tuple[str, ...]] = {"brk20": (pw.C0, pw.W1), "ins": (pw.C0,)}
MIN_N = pws.MIN_N
CLUSTERED_T_MIN = pws.CLUSTERED_T_MIN
BOOTSTRAP_REPS = pws.BOOTSTRAP_REPS
BOOTSTRAP_SEED = 20261111
CI_LEVEL = pws.CI_LEVEL
TOP_TAIL_FRAC = pws.TOP_TAIL_FRAC
HELD_OUT_FROM = pws.HELD_OUT_FROM
CONTROL_DRAWS = 1000
CONTROL_SEED = 20261111
CONTROL_PCT = 95.0
#: The candle module's set sizes, as registered.
N_BULLISH, N_BEARISH, N_FIVE_ENTRY, N_FIVE_EXIT = 21, 17, 2, 3
DEFAULT_ETF_LIST_REL = hv.DEFAULT_ETF_LIST_REL

ADOPT, BETTER_NOT_ENOUGH = "ADOPT", "BETTER, NOT ENOUGH"
REFUTED, INSUFFICIENT, NOT_SHOWN = "REFUTED", "INSUFFICIENT", "NOT SHOWN"
LEDGER: dict[str, str] = {ADOPT: "supported", BETTER_NOT_ENOUGH: "null", NOT_SHOWN: "null", REFUTED: "refuted",
                          INSUFFICIENT: "insufficient"}

ADDON = "brk20_addon"
#: timing trial -> (kind, legs pooled, candle set)
TIMING_TRIALS: dict[str, tuple[str, tuple[str, ...], str]] = {
    "brk20_candle_entry": ("entry", ("brk20",), "bullish"),
    "ins_candle_entry": ("entry", ("ins",), "bullish"),
    "brk20_candle_exit": ("exit", ("brk20",), "bearish"),
    "ins_candle_exit": ("exit", ("ins",), "bearish"),
    "five_entry": ("entry", LEGS, "five_entry"),
    "five_exit": ("exit", LEGS, "five_exit"),
}
TRIALS: tuple[str, ...] = (ADDON, "brk20_candle_entry", "ins_candle_entry", "brk20_candle_exit", "ins_candle_exit",
                           "five_entry", "five_exit")
ENTRY_SETS, EXIT_SETS = ("bullish", "five_entry"), ("bearish", "five_exit")

_PCT_Q = Decimal("0.000001")
_HUNDRED = Decimal("100")

_bump = pws._bump


# =============================================================================== candle flags
class Patterns:
    """Per-stock completion flags from the candle module's ``detect``, packed one bit per rule per own bar and kept
    from ``keep_from`` on (no scan reads an earlier bar)."""

    def __init__(self, series: Mapping[str, Series], detector: Callable[..., Mapping[str, Any]],
                 names: Sequence[str], keep_from: date = SIGNALS_FROM) -> None:
        if len(names) > 64 or len(set(names)) != len(names):
            raise ValueError("pattern names must be distinct and at most 64")
        self._series, self._detector, self._names = series, detector, tuple(names)
        self._bit = {n: k for k, n in enumerate(self._names)}
        self._keep_from = keep_from
        self._masks: dict[str, tuple[int, np.ndarray]] = {}

    def _mask(self, sym: str) -> tuple[int, np.ndarray]:
        hit = self._masks.get(sym)
        if hit is None:
            s = self._series[sym]
            raw = self._detector(np.asarray(s.open, dtype="float64"), np.asarray(s.high, dtype="float64"),
                                 np.asarray(s.low, dtype="float64"), np.asarray(s.close, dtype="float64"),
                                 self._names)
            k0 = bisect_left(s.dates, self._keep_from)
            m = np.zeros(len(s) - k0, dtype=np.uint64)
            for n in self._names:
                f = np.asarray(raw[n], dtype=bool)
                if f.shape != (len(s),):
                    raise ValueError(f"{sym} {n}: {f.shape} flags for {len(s)} bars")
                m |= f[k0:].astype(np.uint64) << np.uint64(self._bit[n])
            hit = (k0, m)
            self._masks[sym] = hit
        return hit

    def _bits(self, sym: str, j: int) -> int:
        k0, m = self._mask(sym)
        if j < k0:
            raise IndexError(f"{sym}: bar {j} precedes the kept flags ({k0})")
        return int(m[j - k0])

    def hits(self, sym: str, j: int, names: Sequence[str]) -> tuple[str, ...]:
        """The rules of ``names`` completing on own bar ``j``, in ``names`` order."""
        b = self._bits(sym, j)
        return tuple(n for n in names if b >> self._bit[n] & 1) if b else ()


# =============================================================================== admission
@dataclass(frozen=True, eq=False)
class Admitted:
    """One signal past every gate up to the fill. ``got`` = (band, rank, liquidity) at ``y``; ``sig`` is brk20's."""

    leg: str
    symbol: str
    series: Series
    idx: int
    atr: np.ndarray
    got: tuple[str, int, float]
    sig: Any = None


@dataclass(frozen=True)
class Entry(Candidate):
    """A candle-entry candidate: the rules that completed on the chosen day."""

    patterns: tuple[str, ...] = ()


def _gate(leg: str, sym: str, s: Series, i: int, grid, atr: np.ndarray,
          counts: dict[str, int]) -> tuple[str, int, float] | None:
    """The pool study's band and forward-window admission with the leg's population and one forward window of
    ``FORWARD_SESSIONS`` market sessions for both legs; ``None`` = dropped (counted)."""
    code, rank, liq = grid.band_at(sym, s.dates[i])
    if code == pws.NO_BAND:
        _bump(counts, "no_band")
        return None
    band = pw.BANDS[code]
    if band not in POPULATION[leg]:
        _bump(counts, "out_of_population")
        return None
    if int(grid.own_pos[sym][i]) + FORWARD_SESSIONS >= len(grid.dates):
        _bump(counts, "short_forward_window")
        return None
    if not np.isfinite(atr[i]):
        _bump(counts, "no_atr14")
        return None
    return band, rank, liq


def brk20_admitted(series: Mapping[str, Series], grid, ex, structural, index) -> tuple[list[Admitted], dict]:
    """``pool_widening_study.brk20_candidates`` up to the fill."""
    counts: dict[str, int] = {}
    out: list[Admitted] = []
    params, floor = bb.PRE_REGISTERED_PARAMS, bb.PRE_REGISTERED_FLOOR_PARAMS
    for sym, s in series.items():
        atr = rc._atr(s)
        for sig in bb.signals_for(s, params, ex.get(sym, []), structural.get(sym, []),
                                  floor_params=floor, veto_counts=counts):
            if not pws._in_window(sig.signal_date, counts):
                continue
            regime = rf.regime_on(rf.closes_through(index, sig.signal_date))
            if regime is not True:
                _bump(counts, "regime_blocked" if regime is False else "regime_undefined")
                continue
            got = _gate("brk20", sym, s, sig.idx, grid, atr, counts)
            if got is not None:
                out.append(Admitted("brk20", sym, s, sig.idx, atr, got, sig))
    return out, counts


def ins_admitted(conn, series: Mapping[str, Series], grid, structural) -> tuple[list[Admitted], dict]:
    """``pool_widening_study.ins_candidates`` up to the fill (events from the stock's full history)."""
    store = rc.store_reader(conn)
    counts: dict[str, int] = {}
    out: list[Admitted] = []
    for sym, s in series.items():
        atr = rc._atr(s)
        for i in es.insider_buy_events(list(s.dates), store.get_insider_trades(symbol=sym), pws.INS_FLOOR_INR):
            if not pws._in_window(s.dates[i], counts):
                continue
            if i + 1 < len(s) and bb._unadjusted_at(structural.get(sym, []), s.dates[i + 1]):
                _bump(counts, hi52.VETO_UNADJUSTED_HISTORY)
                continue
            got = _gate("ins", sym, s, i, grid, atr, counts)
            if got is not None:
                out.append(Admitted("ins", sym, s, i, atr, got))
    return out, counts


# =============================================================================== rules
def live_candidate(a: Admitted, grid, counts: dict[str, int]) -> Candidate | None:
    """The pool study's live fill and ``_finish``: brk20's limit over ``y+1 .. y+5``; ins at ``open(y+1)``."""
    s, i = a.series, a.idx
    if a.leg == "brk20":
        fill = pws.limit_fill(grid, s, a.sig)
        if fill is None:
            _bump(counts, "unfilled")
            return None
        return pws._finish("brk20", a.symbol, s, i, fill[0], fill[1], a.atr, a.got, grid, counts)
    gp = grid.own_pos[a.symbol]
    if i + 1 >= len(s) or gp[i + 1] != gp[i] + 1:
        _bump(counts, "no_fill_session")
        return None
    return pws._finish("ins", a.symbol, s, i, i + 1, float(s.open[i + 1]), a.atr, a.got, grid, counts)


def entry_days(a: Admitted, grid) -> list[int]:
    """Own bars on market sessions ``y .. y+4`` whose stock trades on the next market session."""
    s, gp = a.series, grid.own_pos[a.symbol]
    last = int(gp[a.idx]) + ENTRY_SCAN_SESSIONS - 1
    out = []
    for d in range(a.idx, len(s)):
        if gp[d] > last:
            break
        if d + 1 < len(s) and gp[d + 1] == gp[d] + 1:
            out.append(d)
    return out


def _entry_at(a: Admitted, d: int, grid, counts: dict[str, int], patterns: tuple[str, ...] = ()) -> Entry | None:
    """Buy at the open of own bar ``d + 1``; the live rule's lock, exit-bar and usability checks."""
    s = a.series
    c = pws._finish(a.leg, a.symbol, s, a.idx, d + 1, float(s.open[d + 1]), a.atr, a.got, grid, counts)
    if c is None:
        return None
    return Entry(c.leg, c.event, c.band, c.rank, c.liquidity, exit_idx=c.exit_idx, exit_late=c.exit_late,
                 patterns=patterns)


def candle_entry(a: Admitted, grid, pat: Patterns, names: Sequence[str], counts: dict[str, int]) -> Entry | None:
    """The first own bar on sessions ``y .. y+4`` completing a rule of ``names`` with a next-session bar."""
    s, gp = a.series, grid.own_pos[a.symbol]
    last = int(gp[a.idx]) + ENTRY_SCAN_SESSIONS - 1
    seen = False
    for d in range(a.idx, len(s)):
        if gp[d] > last:
            break
        got = pat.hits(a.symbol, d, names)
        if not got:
            continue
        seen = True
        if d + 1 < len(s) and gp[d + 1] == gp[d] + 1:
            return _entry_at(a, d, grid, counts, got)
    _bump(counts, "no_fill_session" if seen else "no_pattern")
    return None


def addons(cands: Sequence[Candidate], kept: Sequence[Candidate], counts: dict[str, int]) -> list[Candidate]:
    """The live brk20 fills one-position dropped that fill before the exit session of the kept trade they follow;
    the first per kept trade. ``kept`` must be ``one_position_per_symbol(cands)``."""
    kept_ids = {id(c) for c in kept}
    last: dict[str, Candidate] = {}
    used: set[int] = set()
    out: list[Candidate] = []
    for c in sorted(cands, key=lambda c: (c.event.symbol, c.event.signal_date)):
        sym = c.event.symbol
        if id(c) in kept_ids:
            last[sym] = c
            continue
        k = last.get(sym)
        if k is None:
            raise AssertionError(f"{sym} {c.event.signal_date}: dropped with no kept trade before it")
        if c.event.fill_date >= k.exit_date:
            _bump(counts, "filled_after_exit")
        elif id(k) in used:
            _bump(counts, "extra_repeat")
        else:
            used.add(id(k))
            out.append(c)
    return out


def candle_exit_day(c: Candidate, pat: Patterns, names: Sequence[str]) -> tuple[int, tuple[str, ...]] | None:
    """``(own bar, rules)`` of the first completion from the fill bar through the bar before the exit bar."""
    for j in range(c.event.fill_idx, c.exit_idx):
        got = pat.hits(c.event.symbol, j, names)
        if got:
            return j, got
    return None


# =============================================================================== measurement
@dataclass(frozen=True, eq=False)
class STrade:
    """One walked trade. Returns are PERCENT. ``exit_day`` = market sessions from the fill session to the exit
    session; ``cand`` is the candidate it was walked from (the live one for an open exit)."""

    leg: str
    band: str
    symbol: str
    signal_date: date
    fill_date: date
    exit_date: date
    gross: float
    net: float
    cost: float
    benchmark: float | None
    bench_n: int
    excess: float | None
    exit_late: int
    locked_exit: bool
    exit_kind: str
    exit_day: int
    cand: Any = None
    patterns: tuple[str, ...] = ()


@dataclass
class OpenBenchmark:
    """``(EW mean gross %, members)`` of band ``code`` on ``y`` over ``open(entry) -> open(exit)``: the membership of
    ``Grid.benchmark`` (bar on both dates, no structural ex-date in [entry, exit], prices > 0)."""

    grid: Any
    _cache: dict[tuple[int, int, int, int], tuple[float | None, int]] = field(default_factory=dict)

    def __call__(self, code: int, y: date, entry: date, exit_: date) -> tuple[float | None, int]:
        g = self.grid
        key = (code, g.pos[y], g.pos[entry], g.pos[exit_])
        hit = self._cache.get(key)
        if hit is not None:
            return hit
        gy, ge, gx = key[1:]
        with np.errstate(invalid="ignore"):
            ok = ((g.band[gy] == code) & g.present[ge] & g.present[gx]
                  & ((g.ex_le[gx] - g.ex_lt[ge]) == 0) & (g.open[ge] > 0) & (g.open[gx] > 0))
        r = (g.open[gx][ok] / g.open[ge][ok] - 1.0) * 100.0
        out = (float(r.mean()) if r.size else None, int(r.size))
        self._cache[key] = out
        return out


def walk_close(c: Candidate, grid, structural, fees_pct: Decimal, counts: dict[str, int]) -> STrade | None:
    """``pool_widening_study.walk_all`` on one candidate; ``None`` = void (counted)."""
    got = pws.walk_all([c], grid, structural, fees_pct, counts)
    if not got:
        return None
    (t,) = got
    gp = grid.own_pos[t.symbol]
    return STrade(t.leg, t.band, t.symbol, t.signal_date, t.fill_date, t.exit_date, t.gross, t.net,
                  float(pws.band_cost(c.band, fees_pct)), t.benchmark, t.bench_n, t.excess, t.exit_late,
                  t.locked_exit, "close", int(gp[c.exit_idx] - gp[c.event.fill_idx]), c,
                  getattr(c, "patterns", ()))


def open_exit(c: Candidate, j: int, grid, structural, fees_pct: Decimal, obench: OpenBenchmark,
              patterns: tuple[str, ...] = ()) -> STrade | None:
    """Sell ``c`` at the open of own bar ``j + 1``: ``exit_sim``'s paise prices, gross quantization and
    ``net = gross - cost``. ``None`` = a structural ex-date in (fill session, exit session]."""
    ev, s = c.event, c.event.series
    x = j + 1
    if not ev.fill_idx <= j < c.exit_idx:
        raise ValueError(f"{ev.symbol}: open exit after bar {j} outside the hold {ev.fill_idx}..{c.exit_idx - 1}")
    f, xd = ev.fill_date, s.dates[x]
    if any(f < d <= xd for d in structural.get(ev.symbol, ())):
        return None
    cost = pws.band_cost(c.band, fees_pct)
    fill_px, exit_px = ev.fill_px, rc.px(float(s.open[x]))
    gross = ((exit_px - fill_px) / fill_px * _HUNDRED).quantize(_PCT_Q, rounding=ROUND_HALF_UP)
    bench, n = obench(pws.BAND_CODE[c.band], ev.signal_date, f, xd)
    gp = grid.own_pos[ev.symbol]
    g = float(gross)
    return STrade(c.leg, c.band, ev.symbol, ev.signal_date, f, xd, g, float(gross - cost), float(cost), bench, n,
                  None if bench is None else g - bench, int(gp[x] - gp[j] - 1),
                  float(s.high[x]) == float(s.low[x]), "open", int(gp[x] - gp[ev.fill_idx]), c, patterns)


def candle_exit(t: STrade, pat: Patterns, names: Sequence[str], grid, structural, fees_pct: Decimal,
                obench: OpenBenchmark, counts: dict[str, int]) -> STrade | None:
    """The live trade ``t`` under the candle exit: itself when no rule completes; ``None`` = void (counted)."""
    hit = candle_exit_day(t.cand, pat, names)
    if hit is None:
        return t
    _bump(counts, "early_exit")
    out = open_exit(t.cand, hit[0], grid, structural, fees_pct, obench, hit[1])
    if out is None:
        _bump(counts, "void_ca")
    elif out.exit_late:
        _bump(counts, "late_open_exit")
    return out


def _mean(xs: Sequence[float]) -> float | None:
    return float(np.mean(xs)) if len(xs) else None


def _median(xs: Sequence[float]) -> float | None:
    return float(np.median(xs)) if len(xs) else None


def measure_cell(trades: Sequence[STrade]) -> dict[str, Any]:
    """``pool_widening_study.measure_cell`` with each trade's own cost (the cell's cost is their mean) and CPCV at
    ``FAMILY_N``. UNROUNDED."""
    gross = [t.gross for t in trades]
    net = [t.net for t in trades]
    cost = _mean([t.cost for t in trades])
    with_bench = [t for t in trades if t.excess is not None]
    excess = [t.excess for t in with_bench]
    boot_net = hv.clustered_bootstrap([t.signal_date for t in trades], {"net": net},
                                      reps=BOOTSTRAP_REPS, seed=BOOTSTRAP_SEED, level=CI_LEVEL)["net"]
    boot_exc = hv.clustered_bootstrap([t.signal_date for t in with_bench], {"excess": excess},
                                      reps=BOOTSTRAP_REPS, seed=BOOTSTRAP_SEED, level=CI_LEVEL)["excess"]
    rc_trades = [rc.Trade(t.symbol, t.signal_date, t.fill_date, "time", t.gross, t.net) for t in trades]
    cpcv = rc.cpcv(rc_trades, FAMILY_N, 0.0 if cost is None else cost)
    held = [t.excess for t in with_bench if t.signal_date >= HELD_OUT_FROM]
    return {
        "n": len(trades),
        "n_signal_dates": len({t.signal_date for t in trades}),
        "n_symbols": len({t.symbol for t in trades}),
        "cost_pct": cost,
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
        "late_exits": sum(t.exit_late > 0 for t in trades),
        "late_exit_median_sessions": _median([t.exit_late for t in trades if t.exit_late > 0]),
    }


# =============================================================================== relative tests
def _ci(vals: np.ndarray) -> tuple[float | None, float | None]:
    vals = vals[np.isfinite(vals)]
    if not vals.size:
        return None, None
    tail = (1.0 - CI_LEVEL) / 2.0 * 100.0
    lo, hi = np.percentile(vals, [tail, 100.0 - tail])
    return float(lo), float(hi)


def joint_bootstrap(a: Sequence[STrade], b: Sequence[STrade], *, reps: int = BOOTSTRAP_REPS,
                    seed: int = BOOTSTRAP_SEED) -> dict[str, Any]:
    """CI of ``mean excess(a) - mean excess(b)``: resample the union of both cells' distinct signal dates with
    replacement; each cell's mean is over its trades on the drawn dates, with multiplicity. Trades without a
    benchmark are out of both means."""
    a = [t for t in a if t.excess is not None]
    b = [t for t in b if t.excess is not None]
    dates = sorted({t.signal_date for t in a} | {t.signal_date for t in b})
    out = {"diff": None, "ci_lo": None, "ci_hi": None, "n_dates": len(dates), "reps": reps, "seed": seed}
    if not a or not b:
        return out
    ix = {d: k for k, d in enumerate(dates)}

    def sums(ts: Sequence[STrade]) -> tuple[np.ndarray, np.ndarray]:
        k = np.array([ix[t.signal_date] for t in ts])
        return (np.bincount(k, weights=np.array([t.excess for t in ts], dtype="float64"), minlength=len(dates)),
                np.bincount(k, minlength=len(dates)).astype("float64"))

    (sa, ca), (sb, cb) = sums(a), sums(b)
    draws = np.random.default_rng(seed).integers(0, len(dates), size=(reps, len(dates)))
    with np.errstate(invalid="ignore", divide="ignore"):
        diff_b = sa[draws].sum(axis=1) / ca[draws].sum(axis=1) - sb[draws].sum(axis=1) / cb[draws].sum(axis=1)
    lo, hi = _ci(diff_b)
    out.update(diff=float(np.mean([t.excess for t in a]) - np.mean([t.excess for t in b])), ci_lo=lo, ci_hi=hi)
    return out


def paired_bootstrap(pairs: Sequence[tuple[STrade, STrade | None]]) -> dict[str, Any]:
    """Per-trade ``variant excess - live excess`` on the same trade, clustered by signal date."""
    ok = [(lv, v) for lv, v in pairs if v is not None and lv.excess is not None and v.excess is not None]
    b = hv.clustered_bootstrap([lv.signal_date for lv, _ in ok], {"d": [v.excess - lv.excess for lv, v in ok]},
                               reps=BOOTSTRAP_REPS, seed=BOOTSTRAP_SEED, level=CI_LEVEL)["d"]
    return {"diff": b["mean"], "ci_lo": b["ci_lo"], "ci_hi": b["ci_hi"], "n_pairs": len(ok),
            "n_dates": b["n_clusters"], "degenerate": b["degenerate"], "reps": BOOTSTRAP_REPS, "seed": BOOTSTRAP_SEED}


# =============================================================================== random-timing controls
def draw_means(fixed: Sequence[float | None], options: Sequence[np.ndarray], *, draws: int = CONTROL_DRAWS,
               seed: int = CONTROL_SEED) -> np.ndarray:
    """Per draw, the mean excess of ``fixed`` trades plus one uniform pick from each ``options`` row. NaN (void or
    no benchmark) drops that trade from that draw; a draw with nothing left is NaN."""
    fx = np.array([np.nan if v is None else v for v in fixed], dtype="float64")
    fin = np.isfinite(fx)
    fs, fc = float(fx[fin].sum()), int(fin.sum())
    if not options:
        return np.full(draws, fs / fc if fc else np.nan)
    highs = np.array([len(o) for o in options], dtype=np.int64)
    if (highs < 1).any():
        raise ValueError("a redrawn trade has no eligible day")
    m = np.full((len(options), int(highs.max())), np.nan)
    for r, o in enumerate(options):
        m[r, :len(o)] = o
    idx = np.random.default_rng(seed).integers(0, highs, size=(draws, len(options)))
    vals = m[np.arange(len(options)), idx]
    ok = np.isfinite(vals)
    with np.errstate(invalid="ignore", divide="ignore"):
        return (np.where(ok, vals, 0.0).sum(axis=1) + fs) / (ok.sum(axis=1) + fc)


def _excess_or_nan(t: STrade | None) -> float:
    return np.nan if t is None or t.excess is None else float(t.excess)


def entry_options(t: STrade, a: Admitted, grid, structural, fees_pct: Decimal) -> np.ndarray:
    """Excess of the trade on each eligible entry day (NaN when void or unbenchmarked). Eligible: a day of
    ``entry_days`` whose trade passes the live rule's lock, exit-bar and usability checks."""
    out = []
    for d in entry_days(a, grid):
        c = _entry_at(a, d, grid, {})
        if c is not None:
            out.append(_excess_or_nan(walk_close(c, grid, structural, fees_pct, {})))
    return np.array(out, dtype="float64")


def exit_options(t: STrade, grid, structural, fees_pct: Decimal, obench: OpenBenchmark) -> np.ndarray:
    """Excess of the live trade sold at the next own bar's open after each own bar from the fill bar through the
    bar before the exit bar."""
    c = t.cand
    return np.array([_excess_or_nan(open_exit(c, j, grid, structural, fees_pct, obench))
                     for j in range(c.event.fill_idx, c.exit_idx)], dtype="float64")


def control_summary(means: np.ndarray, variant_mean: float | None, n_redrawn: int) -> dict[str, Any]:
    fin = means[np.isfinite(means)]
    p95 = float(np.percentile(fin, CONTROL_PCT)) if fin.size else None
    below = (float(np.mean(fin < variant_mean)) if fin.size and variant_mean is not None else None)
    return {"draws": int(means.size), "seed": CONTROL_SEED, "finite_draws": int(fin.size), "n_redrawn": n_redrawn,
            "mean": _mean(fin), "p95": p95, "variant_share_of_draws_below": below}


def entry_control(trades: Sequence[STrade], lookup: Mapping[tuple[str, str, int], Admitted], grid, structural,
                  fees_pct: Decimal, variant_mean: float | None) -> dict[str, Any]:
    opts = [entry_options(t, lookup[(t.leg, t.symbol, t.cand.event.signal_idx)], grid, structural, fees_pct)
            for t in trades]
    return control_summary(draw_means([], opts), variant_mean, len(opts))


def exit_control(trades: Sequence[STrade], grid, structural, fees_pct: Decimal, obench: OpenBenchmark,
                 variant_mean: float | None) -> dict[str, Any]:
    moved = [t for t in trades if t.exit_kind == "open"]
    fixed = [t.excess for t in trades if t.exit_kind != "open"]
    opts = [exit_options(t, grid, structural, fees_pct, obench) for t in moved]
    return control_summary(draw_means(fixed, opts), variant_mean, len(opts))


# =============================================================================== verdict
def _gt(x: Any, thr: float) -> bool:
    return x is not None and not isinstance(x, bool) and math.isfinite(float(x)) and float(x) > thr


def absolute(m: Mapping[str, Any]) -> list[dict[str, Any]]:
    """The registered seven criteria on unrounded values; a missing value fails."""
    lo, hi = m.get("excess_ci_lo"), m.get("excess_ci_hi")
    ci_excludes_0 = not m.get("excess_degenerate", True) and _gt(lo, 0.0) and hi is not None
    rows = [
        (1, f"n >= {MIN_N}", m.get("n") is not None and m["n"] >= MIN_N),
        (2, "median net > 0", _gt(m.get("median_net"), 0.0)),
        (3, f"mean net > 0 AND clustered t > {CLUSTERED_T_MIN:g}",
         _gt(m.get("mean_net"), 0.0) and _gt(m.get("t_net"), CLUSTERED_T_MIN)),
        (4, "mean excess > 0 AND its 95% clustered CI lies above 0", _gt(m.get("mean_excess"), 0.0) and ci_excludes_0),
        (5, f"CPCV promotable at N = {FAMILY_N}", m.get("cpcv_promotable") is True),
        (6, "mean net > 0 with the top 1% of trades by net removed", _gt(m.get("top1_dropped_mean_net"), 0.0)),
        (7, f"held-out slice (signals >= {HELD_OUT_FROM}): mean excess > 0", _gt(m.get("held_out_mean_excess"), 0.0)),
    ]
    return [{"id": i, "criterion": c, "pass": bool(p)} for i, c, p in rows]


def relative(variant_mean: float | None, live_mean: float | None, ci: Mapping[str, Any],
             control: Mapping[str, Any]) -> dict[str, Any]:
    """Beats the live rule (mean excess above it, the 95% CI of the difference above 0) AND beats the control's
    95th percentile."""
    beats_live = variant_mean is not None and _gt(variant_mean - live_mean if live_mean is not None else None, 0.0)
    ci_above_0 = _gt(ci.get("ci_lo"), 0.0) and not ci.get("degenerate", False)
    p95 = control.get("p95")
    beats_control = variant_mean is not None and p95 is not None and _gt(variant_mean - p95, 0.0)
    return {"variant_mean_excess": variant_mean, "live_mean_excess": live_mean, "diff": ci.get("diff"),
            "ci_lo": ci.get("ci_lo"), "ci_hi": ci.get("ci_hi"), "control_p95": p95,
            "beats_live": bool(beats_live), "ci_above_0": bool(ci_above_0), "beats_control": bool(beats_control),
            "pass": bool(beats_live and ci_above_0 and beats_control)}


def addon_label(m: Mapping[str, Any], rows: Sequence[Mapping[str, Any]]) -> str:
    if (m.get("n") or 0) < MIN_N:
        return INSUFFICIENT
    if all(r["pass"] for r in rows):
        return ADOPT
    if m.get("mean_excess") is not None and float(m["mean_excess"]) <= 0.0:
        return REFUTED
    return NOT_SHOWN


def timing_label(m: Mapping[str, Any], rows: Sequence[Mapping[str, Any]], rel: Mapping[str, Any]) -> str:
    if (m.get("n") or 0) < MIN_N:
        return INSUFFICIENT
    if rel["pass"]:
        return ADOPT if all(r["pass"] for r in rows) else BETTER_NOT_ENOUGH
    v, lv = rel.get("variant_mean_excess"), rel.get("live_mean_excess")
    if v is not None and lv is not None and float(v) <= float(lv):
        return REFUTED
    return NOT_SHOWN


# =============================================================================== diagnostics
def _brief(ts: Sequence[STrade]) -> dict[str, Any]:
    return {"n": len(ts), "mean_net": _mean([t.net for t in ts]),
            "mean_excess": _mean([t.excess for t in ts if t.excess is not None])}


def by_year(ts: Sequence[STrade]) -> dict[str, Any]:
    out: dict[str, list[STrade]] = defaultdict(list)
    for t in ts:
        out[str(t.signal_date.year)].append(t)
    return {k: _brief(v) for k, v in sorted(out.items())}


def per_pattern(ts: Sequence[STrade], names: Sequence[str]) -> dict[str, Any]:
    """Per rule, the trades it triggered (first-day attribution; every rule completing that day counts)."""
    out: dict[str, list[STrade]] = {n: [] for n in names}
    for t in ts:
        for p in t.patterns:
            out[p].append(t)
    return {n: {"n": len(v), "mean_excess": _mean([t.excess for t in v if t.excess is not None])}
            for n, v in out.items()}


def exit_profile(ts: Sequence[STrade]) -> dict[str, Any]:
    early = [t for t in ts if t.exit_kind == "open"]
    return {"n": len(ts), "early_exits": len(early),
            "share_exited_early": (len(early) / len(ts)) if ts else None,
            "median_exit_day": _median([t.exit_day for t in ts]),
            "median_exit_day_early": _median([t.exit_day for t in early]),
            "late_open_exits": sum(t.exit_late > 0 for t in early)}


# =============================================================================== evaluation
def _rt(x: Any) -> Any:
    if isinstance(x, float):
        return pws._r(x)
    if isinstance(x, dict):
        return {k: _rt(v) for k, v in x.items()}
    if isinstance(x, list | tuple):
        return [_rt(v) for v in x]
    return x


def evaluate(admitted: Mapping[str, tuple[list[Admitted], dict[str, int]]], grid, structural, fees_pct: Decimal,
             pat: Patterns, sets: Mapping[str, tuple[str, ...]], *, counts_only: bool) -> dict[str, Any]:
    """Every rule, trial and diagnostic from the admitted signals. ``counts_only`` walks nothing and prices no
    exit: funnels and trade counts only (the void rule applied to dates alone)."""
    funnels: dict[str, dict[str, int]] = {}
    live_all: dict[str, list[Candidate]] = {}
    live_kept: dict[str, list[Candidate]] = {}
    for leg in LEGS:
        counts: dict[str, int] = {}
        cands = [c for a in admitted[leg][0] if (c := live_candidate(a, grid, counts)) is not None]
        live_all[leg], live_kept[leg] = cands, pws.one_position_per_symbol(cands, counts)
        funnels[f"live/{leg}"] = counts
    addon_counts: dict[str, int] = {}
    addon_cands = addons(live_all["brk20"], live_kept["brk20"], addon_counts)
    funnels["addon/brk20"] = addon_counts
    entry_kept: dict[tuple[str, str], list[Candidate]] = {}
    for st in ENTRY_SETS:
        for leg in LEGS:
            counts = {}
            cands = [e for a in admitted[leg][0]
                     if (e := candle_entry(a, grid, pat, sets[st], counts)) is not None]
            entry_kept[(leg, st)] = pws.one_position_per_symbol(cands, counts)
            funnels[f"entry_{st}/{leg}"] = counts
    out: dict[str, Any] = {"admission": {leg: dict(sorted(admitted[leg][1].items())) for leg in LEGS}}
    if counts_only:
        out.update(_counts(live_kept, addon_cands, entry_kept, funnels, grid, structural, pat, sets))
        return out

    obench = OpenBenchmark(grid)
    live = {leg: [t for c in live_kept[leg]
                  if (t := walk_close(c, grid, structural, fees_pct, funnels[f"live/{leg}"])) is not None]
            for leg in LEGS}
    addon_trades = [t for c in addon_cands
                    if (t := walk_close(c, grid, structural, fees_pct, addon_counts)) is not None]
    entries = {k: [t for c in v if (t := walk_close(c, grid, structural, fees_pct,
                                                    funnels[f"entry_{k[1]}/{k[0]}"])) is not None]
               for k, v in entry_kept.items()}
    exits: dict[tuple[str, str], list[tuple[STrade, STrade | None]]] = {}
    for st in EXIT_SETS:
        for leg in LEGS:
            counts = {}
            exits[(leg, st)] = [(t, candle_exit(t, pat, sets[st], grid, structural, fees_pct, obench, counts))
                                for t in live[leg]]
            funnels[f"exit_{st}/{leg}"] = counts
    lookup = {(a.leg, a.symbol, a.idx): a for leg in LEGS for a in admitted[leg][0]}

    trials: dict[str, Any] = {}
    m = measure_cell(addon_trades)
    rows = absolute(m)
    first = live["brk20"]
    label = addon_label(m, rows)
    trials[ADDON] = {"measures": _rt(m), "rows": rows, "verdict": label, "ledger": LEDGER[label],
                     "diagnostics": {"first_position_cell": _rt(_brief(first)),
                                     "addon_minus_first_position": _rt(joint_bootstrap(addon_trades, first)),
                                     "by_year": _rt(by_year(addon_trades)), "funnel": dict(sorted(addon_counts.items()))}}
    for name, (kind, legs, st) in TIMING_TRIALS.items():
        live_cell = [t for leg in legs for t in live[leg]]
        live_mean = _mean([t.excess for t in live_cell if t.excess is not None])
        if kind == "entry":
            var = [t for leg in legs for t in entries[(leg, st)]]
            m = measure_cell(var)
            ci = joint_bootstrap(var, live_cell)
            control = entry_control(var, lookup, grid, structural, fees_pct, m["mean_excess"])
            diag = {"no_pattern": sum(funnels[f"entry_{st}/{leg}"].get("no_pattern", 0) for leg in legs),
                    "no_fill_session": sum(funnels[f"entry_{st}/{leg}"].get("no_fill_session", 0) for leg in legs)}
        else:
            pairs = [p for leg in legs for p in exits[(leg, st)]]
            var = [v for _, v in pairs if v is not None]
            m = measure_cell(var)
            ci = paired_bootstrap(pairs)
            control = exit_control(var, grid, structural, fees_pct, obench, m["mean_excess"])
            diag = exit_profile(var)
        rows = absolute(m)
        rel = relative(m["mean_excess"], live_mean, ci, control)
        label = timing_label(m, rows, rel)
        trials[name] = {"kind": kind, "legs": list(legs), "candle_set": st, "measures": _rt(m), "rows": rows,
                        "relative": _rt({**rel, "bootstrap": ci}), "control": _rt(control), "verdict": label,
                        "ledger": LEDGER[label],
                        "diagnostics": _rt({**diag, "live_n": len(live_cell), "per_pattern": per_pattern(var, sets[st]),
                                            "by_year": by_year(var)})}
    live_cells = {}
    for leg in LEGS:
        lm = measure_cell(live[leg])
        live_cells[leg] = {"trial": False, "measures": _rt(lm), "rows": absolute(lm), "by_year": _rt(by_year(live[leg]))}
    out.update({"rules": {k: dict(sorted(v.items())) for k, v in funnels.items()}, "live_cells": live_cells,
                "trials": trials})
    return out


def _counts(live_kept, addon_cands, entry_kept, funnels, grid, structural, pat, sets) -> dict[str, Any]:
    """Counts-only: funnels and per-trial trade counts, no walk and no price of any exit."""
    def nonvoid(cs: Sequence[Candidate], counts: dict[str, int]) -> list[Candidate]:
        kept = [c for c in cs if not pws.voided(c, structural)]
        if len(kept) < len(cs):
            counts["void_ca"] = counts.get("void_ca", 0) + len(cs) - len(kept)
        return kept

    live = {leg: nonvoid(live_kept[leg], funnels[f"live/{leg}"]) for leg in LEGS}
    addon = nonvoid(addon_cands, funnels["addon/brk20"])
    entries = {k: nonvoid(v, funnels[f"entry_{k[1]}/{k[0]}"]) for k, v in entry_kept.items()}
    early: dict[tuple[str, str], tuple[int, int]] = {}
    for st in EXIT_SETS:
        for leg in LEGS:
            n_early = n_late = 0
            for c in live[leg]:
                hit = candle_exit_day(c, pat, sets[st])
                if hit is not None:
                    n_early += 1
                    gp = grid.own_pos[c.event.symbol]
                    n_late += int(gp[hit[0] + 1] - gp[hit[0]] - 1 > 0)
            early[(leg, st)] = (n_early, n_late)
    trials: dict[str, Any] = {ADDON: {"variant_n": len(addon), "first_position_n": len(live["brk20"]),
                                      **{k: funnels["addon/brk20"].get(k, 0)
                                         for k in ("filled_after_exit", "extra_repeat")}}}
    for name, (kind, legs, st) in TIMING_TRIALS.items():
        live_n = sum(len(live[leg]) for leg in legs)
        if kind == "entry":
            trials[name] = {"variant_n": sum(len(entries[(leg, st)]) for leg in legs), "live_n": live_n,
                            **{k: sum(funnels[f"entry_{st}/{leg}"].get(k, 0) for leg in legs)
                               for k in ("no_pattern", "no_fill_session", "held_overlap")}}
        else:
            trials[name] = {"variant_n": live_n, "live_n": live_n,
                            "early_exits": sum(early[(leg, st)][0] for leg in legs),
                            "late_open_exits": sum(early[(leg, st)][1] for leg in legs)}
    return {"rules": {k: dict(sorted(v.items())) for k, v in funnels.items()}, "trials": trials}


# =============================================================================== the study
def _candle_sets(cp) -> dict[str, tuple[str, ...]]:
    """The candle module's sets, checked against the registered sizes and nesting."""
    bull, bear = tuple(cp.BULLISH), tuple(cp.BEARISH)
    five_in, five_out = tuple(cp.FIVE_ENTRY), tuple(cp.FIVE_EXIT)
    if (len(set(bull)), len(set(bear)), len(set(five_in)), len(set(five_out))) != (
            N_BULLISH, N_BEARISH, N_FIVE_ENTRY, N_FIVE_EXIT):
        raise rc.Refusal(f"candle sets are not the registered {N_BULLISH}/{N_BEARISH}/{N_FIVE_ENTRY}/{N_FIVE_EXIT}: "
                         f"{len(bull)}/{len(bear)}/{len(five_in)}/{len(five_out)}")
    if not (set(five_in) <= set(bull) and set(five_out) <= set(bear)) or set(bull) & set(bear):
        raise rc.Refusal("candle sets: the five must sit inside their sets and no rule in both")
    return {"bullish": bull, "bearish": bear, "five_entry": five_in, "five_exit": five_out}


def _load_candles():
    try:
        return rc._load_script("candle_patterns")
    except (FileNotFoundError, ImportError) as exc:
        sys.modules.pop("mt_candle_patterns", None)
        raise rc.Refusal(f"scripts/candle_patterns.py is not loadable: {exc}") from exc


def run_study(conn, *, db_path: Path, etf, counts_only: bool = False,
              candles=None) -> dict[str, Any]:
    """The registered study. ``counts_only`` keeps funnels and trade counts and drops every return. ``candles``:
    the candle module (default ``scripts/candle_patterns.py``)."""
    if set(pws.BAND_SPREAD_PCT) != set(pw.BANDS):
        raise rc.Refusal("band spreads are not recorded (BAND_SPREAD_PCT)")
    st = load_settings()
    if not (st.ins.threshold_inr == st.filings.insider_min_value_inr == pws.INS_FLOOR_INR):
        raise rc.Refusal(f"the ins floor moved: ins.threshold_inr {st.ins.threshold_inr}, filings."
                         f"insider_min_value_inr {st.filings.insider_min_value_inr}, registered {pws.INS_FLOOR_INR}")
    cp = candles if candles is not None else _load_candles()
    sets = _candle_sets(cp)
    t0 = time.monotonic()
    cm = CostModel.from_config()
    fees_pct = cm.fee_breakeven_pct(pws.NOTIONAL, pws.PRODUCT)
    series = bb.load_series(conn, rc.FULL_HISTORY_START, date.max)
    for name in hv.INDEX_ROWS:
        series.pop(name, None)
    series, etf_counts = hv.exclude_etfs(series, etf)
    if not series:
        raise rc.Refusal("no bars_1d rows")
    ex, structural = bb.load_ex_dates(conn), bb.load_structural_ex_dates(conn)
    for sym, extra in pws.STRUCTURAL_SUPPLEMENT.items():
        for table in (ex, structural):
            table[sym] = sorted(set(table.get(sym, [])) | set(extra))
    grid = pws.Grid.build(series, structural)
    index = rf.index_series(conn, grid.dates[-1])
    admitted = {"brk20": brk20_admitted(series, grid, ex, structural, index),
                "ins": ins_admitted(conn, series, grid, structural)}
    names = tuple(dict.fromkeys(sets["bullish"] + sets["bearish"]))
    pat = Patterns(series, cp.detect, names)
    body = evaluate(admitted, grid, structural, fees_pct, pat, sets, counts_only=counts_only)
    return {
        "meta": {
            "script": "scripts/swing_timing_study.py",
            "registration": f"swing timing, registered {REGISTERED_ON}", "family": FAMILY, "trials": list(TRIALS),
            "generated_at": datetime.now(IST).isoformat(timespec="seconds"),
            "db": str(db_path), "window": {"bars_from": str(grid.dates[0]), "bars_to": str(grid.dates[-1]),
                                           "signals_from": str(SIGNALS_FROM)},
            "n_stocks": len(series), "counts_only": counts_only,
            "etf_exclusion": {"list_path": etf.path, "sha256": etf.sha256, "pattern": hv.ETF_PATTERN.pattern,
                              **etf_counts},
            "family_n": FAMILY_N, "fold_pass_min": fold_pass_min(FAMILY_N),
            "populations": {k: list(v) for k, v in POPULATION.items()},
            "forward_sessions_after_y": FORWARD_SESSIONS, "entry_scan_sessions": ENTRY_SCAN_SESSIONS,
            "fill_window_sessions": FILL_WINDOW_SESSIONS, "exit_sessions_after_fill": EXIT_SESSIONS,
            "product": pws.PRODUCT, "notional_inr": str(pws.NOTIONAL), "fees_pct": float(fees_pct),
            "band_spread_pct": {k: str(v) for k, v in pws.BAND_SPREAD_PCT.items()},
            "ins_floor_inr": pws.INS_FLOOR_INR,
            "structural_supplement": {k: [str(d) for d in v] for k, v in pws.STRUCTURAL_SUPPLEMENT.items()},
            "constants": {"min_n": MIN_N, "clustered_t_min": CLUSTERED_T_MIN, "bootstrap_reps": BOOTSTRAP_REPS,
                          "bootstrap_seed": BOOTSTRAP_SEED, "ci_level": CI_LEVEL, "top_tail_frac": TOP_TAIL_FRAC,
                          "held_out_from": str(HELD_OUT_FROM), "control_draws": CONTROL_DRAWS,
                          "control_seed": CONTROL_SEED, "control_percentile": CONTROL_PCT},
            "candle_module": {"path": str(getattr(cp, "__file__", "")), **{k: list(v) for k, v in sets.items()}},
            "corp_actions_coverage": bb.corp_actions_coverage(conn),
            "runtime_seconds": round(time.monotonic() - t0, 1),
        },
        **body,
        "notes": NOTES,
    }


NOTES: list[str] = [
    "Inherited from the pool study: bands are point in time by liquidity alone; open fills assume the open is "
    "reachable (only all-day locks are caught); the official close is a VWAP no order fills at; spreads were measured "
    "on one afternoon and applied backwards; brk20's benchmark runs from open(fill session) while its fill is the "
    "limit level; ins applies the helpers' unadjusted-history veto, which the live leg does not; a brk20 touch "
    "session locked all day is dropped; one position per stock is per leg, from kept trades.",
    "Sessions are market sessions; an exit bar the stock did not trade moves to its next own bar (late).",
    "Random-timing controls keep the variant's trades as they are: they are not re-filtered by one position per "
    "stock.",
    "An early (candle) exit does not free the stock for new signals: one position per stock is decided on the live "
    "rule's exits.",
    "The candle entry buys at the open after the pattern and can chase a move the live limit would not.",
    "An open exit assumes the open is reachable; a session locked all day is counted (locked_exit_sessions), not "
    "dropped.",
    "Live cells are the comparators and not trials; per-pattern, add-on, by-year and exit profiles decide nothing.",
]


# =============================================================================== rendering
def _fmt(v: Any, spec: str = "+.3f") -> str:
    return "-" if v is None else format(v, spec)


def render_text(doc: dict[str, Any]) -> str:
    m = doc["meta"]
    out = [f"SWING TIMING STUDY ({m['registration']}, {m['family']}, N={m['family_n']}, "
           f"fold_pass_min {m['fold_pass_min']:.0%})",
           f"db {m['db']}  bars {m['window']['bars_from']} -> {m['window']['bars_to']}  signals from "
           f"{m['window']['signals_from']}  stocks {m['n_stocks']}",
           f"cost = fees {m['fees_pct']:.4f}% + band spread {m['band_spread_pct']}", ""]
    out += [f"[admission {leg}] {f}" for leg, f in doc["admission"].items()]
    out += [f"[{rule}] {f}" for rule, f in doc["rules"].items()]
    out.append("")
    if m.get("counts_only"):
        out.append("COUNTS ONLY (no returns)")
        out += [f"  {name:<20} {c}" for name, c in doc["trials"].items()]
        return rc.ascii_text("\n".join(out))
    hdr = ("  trial                    n  med net  mean net     t  mean exc  live exc  diff CI             "
           "ctrl p95  CPCV  held-out  verdict")
    out.append(hdr)
    for name, t in doc["trials"].items():
        x, r = t["measures"], t.get("relative", {})
        ci = f"[{_fmt(r.get('ci_lo'))}, {_fmt(r.get('ci_hi'))}]" if r else "-"
        out.append(f"  {name:<20} {x['n']:>5} {_fmt(x['median_net']):>8} {_fmt(x['mean_net']):>9} "
                   f"{_fmt(x['t_net'], '.2f'):>5} {_fmt(x['mean_excess']):>9} {_fmt(r.get('live_mean_excess')):>9} "
                   f"{ci:<19} {_fmt(r.get('control_p95')):>9} {str(x['cpcv_promotable']):>5} "
                   f"{_fmt(x['held_out_mean_excess']):>9}  {t['verdict']} ({t['ledger']})")
        failed = [str(row["id"]) for row in t["rows"] if not row["pass"]]
        if failed:
            out.append(f"      failed criteria: {', '.join(failed)}")
        if r:
            out.append(f"      relative: beats live {r['beats_live']}, CI above 0 {r['ci_above_0']}, "
                       f"beats control {r['beats_control']}")
    out += ["", "Live cells (comparators, not trials):"]
    for leg, c in doc["live_cells"].items():
        x = c["measures"]
        failed = [str(row["id"]) for row in c["rows"] if not row["pass"]]
        out.append(f"  {leg:<6} n {x['n']:>5}  median net {_fmt(x['median_net'])}  mean excess "
                   f"{_fmt(x['mean_excess'])}  failed criteria: {', '.join(failed) or 'none'}")
    out += ["", "Criteria: 1 n>=" + str(MIN_N) + "; 2 median net > 0; 3 mean net > 0 and t > 2; 4 mean excess > 0 "
            "with CI above 0; 5 CPCV promotable at N=7; 6 top-1%-dropped mean net > 0; 7 held-out mean excess > 0.",
            "Diagnostics (per pattern, add-on beside the first position, by year, exit profiles) are in the JSON."]
    out += doc["notes"]
    return rc.ascii_text("\n".join(out))


def render_markdown(doc: dict[str, Any]) -> str:
    return f"# Swing timing study\n\n```\n{render_text(doc)}\n```\n"


# =============================================================================== CLI
def main(argv: list[str] | None = None, *, now: datetime | None = None) -> int:
    ap = argparse.ArgumentParser(prog="swing_timing_study", description=__doc__.splitlines()[0])
    ap.add_argument("--db", type=Path, required=True, help="whole-market research snapshot")
    ap.add_argument("--counts-only", action="store_true",
                    help="pipeline check: funnels and per-trial trade counts, no returns, no report written")
    args = ap.parse_args(argv)
    try:
        if reason := rc.market_hours_refusal(now):
            raise rc.Refusal(reason)
        etf = hv.load_etf_list(repo_root() / DEFAULT_ETF_LIST_REL)
        conn = rc.open_snapshot(args.db)
        try:
            doc = run_study(conn, db_path=args.db, etf=etf, counts_only=args.counts_only)
        finally:
            conn.close()
    except rc.Refusal as exc:
        print(f"swing_timing_study: REFUSED: {rc.ascii_text(str(exc))}", file=sys.stderr)
        return 2
    if args.counts_only:
        print(render_text(doc))
        return 0
    out = rc.default_out(args.db, "swing_timing")
    md = rc.write_reports(doc, render_markdown(doc), out)
    print(render_text(doc))
    print(f"JSON -> {out}\nMD   -> {md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
