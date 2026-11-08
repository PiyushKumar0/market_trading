#!/usr/bin/env python
"""Fibonacci retracement study (registered 2026-11-04).

F1: a limit at a retracement depth of the 60-session up-leg (``edge/fib_pullback``, N = 3), with the
non-Fibonacci neighbours as controls. F2: do completed pullbacks stop near the Fibonacci depths
(``price_structure/fib``, N = 1). F3: Fibonacci confluence on the selection-filter study's swing populations
(``selection_filter/<strategy>``). F4: brk20's limit at a Fibonacci retracement of the breakout leg against
V2-5 (``entry_mechanism/brk20``, N = 6). Arm (ii) construct and cost throughout (``backtest_regime_filter``).
Evidence only; SUPPORTED goes to the owner and wires nothing. Never run before the pre-registration is
committed. Exit codes: 0 ran, 2 refused.
"""

from __future__ import annotations

import math
import statistics
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any, NamedTuple

import numpy as np
from numpy.lib.stride_tricks import sliding_window_view

sys.path.insert(0, str(Path(__file__).resolve().parent))
import research_common as rc  # noqa: E402
from engine.learning.multiplicity import permutation_pvalue, registration_alpha  # noqa: E402
from engine.learning.validate import fold_pass_min  # noqa: E402
from engine.strategy.cost_model import CostModel  # noqa: E402
from engine.strategy.types import round_to_tick  # noqa: E402

rf = rc._load_script("backtest_regime_filter")
hv = rc._load_script("backtest_hvol")
bb = rc.bb

#: The registration date, set by the commit that registers the study; until then
#: every run is refused.
REGISTERED: date | None = date(2026, 11, 4)
SEED = 20261104
DRAWS = 10_000
BOOT_REPS = 2000
LEG_WINDOW = 60
LOW_WINDOW = LEG_WINDOW - 1          # L over h-59..h-1: the registered text, 59 sessions
MIN_LEG = 0.15
PULLBACK_MAX = 20
COMPLETE_MAX = 60
FIB = (0.382, 0.5, 0.618)
CONTROLS = (0.322, 0.44, 0.56, 0.678)
NEIGHBOURS = {0.382: (0.322, 0.44), 0.5: (0.44, 0.56), 0.618: (0.56, 0.678)}
DESCRIPTIVE = (0.236, 0.786)
DEPTHS = FIB + CONTROLS + DESCRIPTIVE
HALF, OUTER = 0.01, 0.03
RATIO_MIN = 1.10
F2_MIN_LEGS = 100
CONFLUENCE_TOL = 0.01
F4_WINDOW = bb.FILL_WINDOW[bb.VARIANT_V2_N5]
L20_LOOKBACK = int(bb.PRE_REGISTERED_PARAMS["lookback_days"])
T20 = rf.T20
#: Ratios are rounded to this many decimals before meeting a registered boundary, so a tick-exact 15%,
#: 1% or window edge is not decided by binary-float representation; far below one tick at any price.
DP = 9

F1_N, F2_N, F4_N = 3, 1, 6
F3_N = {"brk20": 6, "hi52": 6, "ins": 1}
F3_STRATEGIES = tuple(F3_N)
FAMILIES = {"edge/fib_pullback": F1_N, "price_structure/fib": F2_N,
            **{f"selection_filter/{s}": n for s, n in F3_N.items()}, "entry_mechanism/brk20": F4_N}
F1_ALPHA = registration_alpha(F1_N)

ANY_DAY = "any_pullback_day"
CONFLUENCE = "confluence"
BASE = "V2-5"
COMPLETED, SKIPPED, FAILED, INCOMPLETE, CENSORED = "completed", "skipped", "failed", "incomplete", "censored"
NOT_QUALIFIED = "not_qualified"
NO_WINNER = "none"
WINNER_TIE = "none (exact tie on the median net)"

LEG_RULE = (f"h = earliest highest high in y-{LEG_WINDOW - 1}..y with h >= {LEG_WINDOW}; L = lowest low in "
            f"h-{LOW_WINDOW}..h-1; (H-L)/L >= {MIN_LEG:.0%}; bars from L's session through the compared session "
            "usable; no structural ex-date after L's session up to and including the compared session; "
            "level = H - d(H-L) to the tick")
F1_RULE = (f"SUPPORTED iff n >= {rf.MIN_N}, CPCV-promotable at fold_pass_min({F1_N}), median net > 0, excess CI "
           f"lower bound > 0 and permutation p < 0.05/{F1_N} against {DRAWS} same-size subsamples of its two "
           f"neighbours' pooled trades (no p when the pool is smaller); INSUFFICIENT if n < {rf.MIN_N}")
F2_RULE = (f"SUPPORTED iff the pooled ratio >= {RATIO_MIN:.2f}, its 95% bootstrap CI lower bound (h-date clusters, "
           f"{BOOT_REPS} reps) > 1 and it exceeds the pooled placebo ratio; INSUFFICIENT if the three windows hold "
           f"< {F2_MIN_LEGS} legs")
F3_RULE = (f"SUPPORTED iff confluence n >= {rf.MIN_N}, CPCV-promotable at fold_pass_min(N), median net > max(0, "
           f"FULL median) and permutation p < 0.05/N against {DRAWS} same-size subsamples of FULL; "
           f"INSUFFICIENT if n < {rf.MIN_N}")
F4_RULE = (f"SUPPORTED iff n >= {rf.MIN_N}, CPCV-promotable at fold_pass_min({F4_N}) and median net > max(0, "
           f"{BASE} median net); the highest median among supported depths wins; INSUFFICIENT if n < {rf.MIN_N}")
TOUCH_CAVEAT = ("F1, F3's brk20 and F4 fill on TOUCH at min(open, level): a touch is not a guaranteed fill, so "
                "fill rates and nets are optimistic.")


def key(d: float) -> str:
    return f"d{d * 100:g}"


def _q(x: float) -> float:
    return round(x, DP)


# ----------------------------------------------------------------------------- the leg
class Leg(NamedTuple):
    h: int
    lo: int
    H: float
    L: float


@dataclass(frozen=True)
class Legs:
    """One series' leg geometry per session y, each entry read from bars through y alone; the bar and
    ex-date checks run to a compared session k >= y in ``leg_as_of``."""

    h: np.ndarray           # earliest index of the highest high in y-59..y; -1 for y < 59
    lo: np.ndarray          # earliest index of the lowest low in h-59..h-1; -1 when h < 60
    H: np.ndarray
    L: np.ndarray
    geom: np.ndarray        # h >= 60 and (H - L) / L >= MIN_LEG
    new_high: np.ndarray    # high(y) > every high in y-59..y-1
    bad: np.ndarray         # cumulative unusable bars: bars i..k usable iff bad[k + 1] == bad[i]
    ex_le: np.ndarray       # structural ex-dates on or before dates[i]


def legs_for(s, structural) -> Legs:
    n = len(s)
    with np.errstate(invalid="ignore"):
        hi_ok, lo_ok = np.isfinite(s.high) & (s.high > 0), np.isfinite(s.low) & (s.low > 0)
        good = (hi_ok & lo_ok & np.isfinite(s.open) & (s.open > 0) & np.isfinite(s.close) & (s.close > 0))
    hi_c, lo_c = np.where(hi_ok, s.high, -np.inf), np.where(lo_ok, s.low, np.inf)
    h = np.full(n, -1, dtype=np.int64)
    lo_of = np.full(n, -1, dtype=np.int64)          # indexed by h
    new_high = np.zeros(n, dtype=bool)
    if n >= LEG_WINDOW:
        h[LEG_WINDOW - 1:] = np.arange(n - LEG_WINDOW + 1) + sliding_window_view(hi_c, LEG_WINDOW).argmax(axis=1)
    if n > LOW_WINDOW:
        lo_of[LOW_WINDOW:] = (np.arange(n - LOW_WINDOW)
                              + sliding_window_view(lo_c[:n - 1], LOW_WINDOW).argmin(axis=1))
        new_high[LOW_WINDOW:] = hi_c[LOW_WINDOW:] > sliding_window_view(hi_c[:n - 1], LOW_WINDOW).max(axis=1)
    ok = h >= LEG_WINDOW
    hh = np.where(ok, h, 0)
    lo = np.where(ok, lo_of[hh], -1)
    top = np.where(ok, s.high[hh], np.nan)
    base = np.where(ok, s.low[np.where(ok, lo, 0)], np.nan)
    with np.errstate(invalid="ignore", divide="ignore"):
        ratio = np.round((top - base) / base, DP)
        geom = ok & (lo >= 0) & np.isfinite(ratio) & (base > 0) & (ratio >= MIN_LEG)
    ex = np.array(sorted(structural), dtype="datetime64[D]")
    ex_le = np.searchsorted(ex, np.array(s.dates, dtype="datetime64[D]"), side="right")
    bad = np.concatenate([[0], np.cumsum(~good)])
    return Legs(h, lo, top, base, geom, new_high, bad, ex_le)


def leg_as_of(legs: Legs, y: int, k: int) -> Leg | None:
    """The leg as of session ``y`` when it qualifies through the compared session ``k``; else ``None``."""
    if not 0 <= y <= k < len(legs.h) or not legs.geom[y]:
        return None
    lo = int(legs.lo[y])
    if legs.bad[k + 1] != legs.bad[lo] or legs.ex_le[k] != legs.ex_le[lo]:
        return None
    return Leg(int(legs.h[y]), lo, float(legs.H[y]), float(legs.L[y]))


def level(leg: Leg, d: float) -> float:
    return float(round_to_tick(leg.H - d * (leg.H - leg.L)))


# ----------------------------------------------------------------------------- F1 populations
@dataclass(frozen=True)
class Fill:
    event: rc.Event
    leg: Leg


def f1_events(sym: str, s, legs: Legs, atr: np.ndarray, counts: dict[float, dict[str, int]],
              depths=DEPTHS) -> dict[float, list[Fill]]:
    """Per depth, the admitted touches of the limit resting at the leg-as-of-(t-1) level on session t."""
    out: dict[float, list[Fill]] = {d: [] for d in depths}
    done: set[tuple[int, float]] = set()
    levels: dict[int, dict[float, float]] = {}
    for t in range(LEG_WINDOW + 1, len(s)):
        h = int(legs.h[t - 1])
        if t - h > PULLBACK_MAX:
            continue
        leg = leg_as_of(legs, t - 1, t)
        if leg is None:
            continue
        if h not in levels:
            levels[h] = {d: level(leg, d) for d in depths}
        prior = float(s.low[h + 1:t].min()) if t > h + 1 else math.inf
        low_t = float(s.low[t])
        for d in depths:
            x = levels[h][d]
            if (h, d) in done or prior <= x or low_t > x:
                continue
            done.add((h, d))
            c = counts.setdefault(d, {})
            rc.brk20._bump(c, "touches")
            if rc._admitted(s, t - 1, rc.HORIZON, atr, c):
                ev = rc.Event(sym, s, t - 1, t, rc.px(min(float(s.open[t]), x)), float(atr[t - 1]))
                out[d].append(Fill(ev, leg))
    return out


def pullback_day_events(sym: str, s, legs: Legs, atr: np.ndarray, counts: dict[str, int]) -> list[Fill]:
    """Entry at open(y+1) for every y with 0 <= y-h <= 19 and no low below L since h (descriptive)."""
    out: list[Fill] = []
    for y in range(LEG_WINDOW, len(s) - 1):
        h = int(legs.h[y])
        if not 0 <= y - h < PULLBACK_MAX:
            continue
        leg = leg_as_of(legs, y, y + 1)
        if leg is None or (y > h and float(s.low[h + 1:y + 1].min()) < leg.L):
            continue
        rc.brk20._bump(counts, "days")
        if rc._admitted(s, y, rc.HORIZON, atr, counts):
            out.append(Fill(rc.Event(sym, s, y, y + 1, rc.px(float(s.open[y + 1])), float(atr[y])), leg))
    return out


class Bench:
    """EW mean of ``close(exit)/open(fill) - 1`` (percent) over the universe names with bars on both sessions,
    positive prices and no structural ex-date in [fill, exit]: hvol's benchmark without its liquidity screen."""

    def __init__(self, series: dict[str, Any], structural: dict[str, list[date]]):
        dates = sorted({d for s in series.values() for d in s.dates})
        grid = np.array(dates, dtype="datetime64[D]")
        shape = (len(dates), len(series))
        self.pos = {d: i for i, d in enumerate(dates)}
        self.open, self.close = np.full(shape, np.nan), np.full(shape, np.nan)
        self.present = np.zeros(shape, dtype=bool)
        self.ex_lt, self.ex_le = np.zeros(shape, dtype=np.int32), np.zeros(shape, dtype=np.int32)
        self._cache: dict[tuple[int, int], tuple[float | None, int]] = {}
        for j, sym in enumerate(sorted(series)):
            s = series[sym]
            rows = np.searchsorted(grid, np.array(s.dates, dtype="datetime64[D]"))
            self.open[rows, j], self.close[rows, j], self.present[rows, j] = s.open, s.close, True
            if ex := structural.get(sym):
                exd = np.array(sorted(ex), dtype="datetime64[D]")
                self.ex_lt[:, j] = np.searchsorted(exd, grid, side="left")
                self.ex_le[:, j] = np.searchsorted(exd, grid, side="right")

    def __call__(self, fill_date: date, exit_date: date) -> tuple[float | None, int]:
        k = (self.pos[fill_date], self.pos[exit_date])
        if (hit := self._cache.get(k)) is not None:
            return hit
        ge, gx = k
        with np.errstate(invalid="ignore"):
            ok = (self.present[ge] & self.present[gx] & ((self.ex_le[gx] - self.ex_lt[ge]) == 0)
                  & (self.open[ge] > 0) & (self.close[gx] > 0))
        r = (self.close[gx][ok] / self.open[ge][ok] - 1.0) * 100.0
        out = self._cache[k] = (float(r.mean()) if r.size else None, int(r.size))
        return out


def benchmark_block(pairs: list[rf.Pair], bench: Bench) -> dict[str, Any]:
    dates, excess, bench_r, undefined = [], [], [], 0
    for ev, t in pairs:
        if t is None:
            continue
        b, _ = bench(ev.fill_date, ev.series.dates[ev.fill_idx + T20])
        if b is None:
            undefined += 1
            continue
        dates.append(ev.fill_date)
        excess.append(t.gross - b)
        bench_r.append(b)
    boot = hv.clustered_bootstrap(dates, {"excess": excess}, reps=BOOT_REPS, seed=SEED)["excess"]
    return {"n": len(excess), "undefined": undefined,
            "mean_benchmark": round(statistics.fmean(bench_r), 4) if bench_r else None,
            "mean_excess": round(statistics.fmean(excess), 4) if excess else None,
            "median_excess": round(statistics.median(excess), 4) if excess else None,
            "bootstrap": {**{k: boot[k] for k in ("mean", "ci_lo", "ci_hi", "n_clusters", "degenerate")},
                          "reps": BOOT_REPS, "seed": SEED}}


def classic_block(fills: list[Fill], cost: Decimal, structural: dict[str, list[date]]) -> dict[str, Any]:
    """Stop at L, target at H, the same T+20 cap (descriptive)."""
    reasons: Counter[str] = Counter()
    nets: list[float] = []
    for f in fills:
        ev = f.event
        o = rc.walk(ev, stop=rc.px(f.leg.L), target=rc.px(f.leg.H), horizon=T20, cost_pct=cost,
                    ex_dates=structural.get(ev.symbol, []))
        if o.status not in ("exit", "void_ca"):
            raise AssertionError(f"{ev.symbol} {ev.fill_date}: walk ended {o.status}")
        reasons[o.reason or o.status] += 1
        if o.status == "exit":
            nets.append(float(o.net_pct))
    return {"n": len(nets),
            "median_net": round(statistics.median(nets), 4) if nets else None,
            "mean_net": round(statistics.fmean(nets), 4) if nets else None,
            "hit_rate_net": round(sum(v > 0 for v in nets) / len(nets), 4) if nets else None,
            "exit_reasons": dict(sorted(reasons.items()))}


def permutation_block(sample: list[float], pool: list[float]) -> dict[str, Any]:
    p = permutation_pvalue(sample, pool, draws=DRAWS, seed=SEED) if 0 < len(sample) <= len(pool) else None
    return {"p": p, "n": len(sample), "pool_n": len(pool), "draws": DRAWS, "seed": SEED}


def decide_f1(r: dict[str, Any], k: str) -> str:
    c = r["cohorts"][k]
    if c["n"] < rf.MIN_N:
        return rf.INSUFFICIENT
    lo, p = r["benchmark"][k]["bootstrap"]["ci_lo"], r["permutation"][k]["p"]
    if (r["cpcv"][k]["promotable"] and c["median_net"] > 0 and lo is not None and lo > 0
            and p is not None and p < r["alpha"]):
        return rf.SUPPORTED
    return rf.NOT_SUPPORTED


def study_f1(series: dict[str, Any], legs: dict[str, Legs], structural: dict[str, list[date]], source: str,
             cost: Decimal) -> dict[str, Any]:
    fills: dict[str, list[Fill]] = {key(d): [] for d in DEPTHS} | {ANY_DAY: []}
    by_depth: dict[float, dict[str, int]] = {d: {} for d in DEPTHS}
    any_counts: dict[str, int] = {}
    for sym, s in series.items():
        atr = rc._atr(s)
        for d, got in f1_events(sym, s, legs[sym], atr, by_depth).items():
            fills[key(d)] += got
        fills[ANY_DAY] += pullback_day_events(sym, s, legs[sym], atr, any_counts)
    counts = {key(d): c for d, c in by_depth.items()} | {ANY_DAY: any_counts}
    pairs = {k: rf.measure(rc.Population(k, source, len(series), [f.event for f in v], counts[k], structural),
                           cost) for k, v in fills.items()}
    roles = ({key(d): "fib" for d in FIB} | {key(d): "control" for d in CONTROLS}
             | {key(d): "descriptive" for d in DESCRIPTIVE} | {ANY_DAY: "descriptive"})
    out = {"family": "edge/fib_pullback", "family_n": F1_N, "alpha": F1_ALPHA, "roles": roles,
           "population": {"source": source, "label": rc.SURVIVORSHIP_LABEL, "n_symbols": len(series),
                          "counts": counts},
           **rf.report(pairs, F1_N, float(cost))}
    bench = Bench(series, structural)
    out["benchmark"] = {k: benchmark_block(v, bench) for k, v in pairs.items()}
    nets = {k: [t.net for _, t in v if t is not None] for k, v in pairs.items()}
    out["permutation"] = {key(d): {**permutation_block(nets[key(d)], [x for a in NEIGHBOURS[d] for x in nets[key(a)]]),
                                   "pool": [key(a) for a in NEIGHBOURS[d]]} for d in FIB}
    out["classic"] = {key(d): classic_block(fills[key(d)], cost, structural) for d in FIB}
    out["decision"] = {key(d): decide_f1(out, key(d)) for d in FIB}
    return out


# ----------------------------------------------------------------------------- F2
class LegOutcome(NamedTuple):
    symbol: str
    h: int
    h_date: date
    kind: str
    r: float | None


def f2_legs(sym: str, s, legs: Legs, counts: dict[str, int]) -> list[LegOutcome]:
    """Every leg starting at a strict 60-session new high, scanned h+1..h+60 to completion or failure."""
    out: list[LegOutcome] = []
    n = len(s)
    for h in np.flatnonzero(legs.new_high & legs.geom).tolist():
        H, L = float(legs.H[h]), float(legs.L[h])
        hi, lo = s.high[h + 1:h + COMPLETE_MAX + 1], s.low[h + 1:h + COMPLETE_MAX + 1]
        up, dn = np.flatnonzero(hi > H), np.flatnonzero(lo < L)
        c = h + 1 + int(up[0]) if up.size else None
        f = h + 1 + int(dn[0]) if dn.size else None
        if c is not None and (f is None or f >= c):
            kind, k = (SKIPPED if c == h + 1 else COMPLETED), c
        elif f is not None:
            kind, k = FAILED, f
        elif hi.size < COMPLETE_MAX:
            kind, k = CENSORED, n - 1
        else:
            kind, k = INCOMPLETE, h + COMPLETE_MAX
        if leg_as_of(legs, h, k) is None:
            rc.brk20._bump(counts, NOT_QUALIFIED)
            continue
        rc.brk20._bump(counts, kind)
        r = _q((H - float(s.low[h + 1:c].min())) / (H - L)) if kind == COMPLETED else None
        out.append(LegOutcome(sym, h, s.dates[h], kind, r))
    return out


def windows(r: np.ndarray, ctr: float) -> tuple[np.ndarray, np.ndarray]:
    """``(in the +-1pp window, in the 1-3pp shoulders)`` masks, half-open intervals."""
    a, b, lo, hi = _q(ctr - HALF), _q(ctr + HALF), _q(ctr - OUTER), _q(ctr + OUTER)
    return (r >= a) & (r < b), ((r >= lo) & (r < a)) | ((r >= b) & (r < hi))


def _ratio(obs: float, expected: float) -> float | None:
    return obs / expected if expected > 0 else None


def centre_counts(r: np.ndarray, ctr: float) -> dict[str, Any]:
    w, sh = windows(r, ctr)
    obs, shoulders = int(w.sum()), int(sh.sum())
    return {"obs": obs, "shoulders": shoulders, "expected": shoulders / 2, "ratio": _ratio(obs, shoulders / 2)}


def pooled(r: np.ndarray, centres) -> dict[str, Any]:
    rows = [centre_counts(r, c) for c in centres]
    obs, expected = sum(x["obs"] for x in rows), sum(x["expected"] for x in rows)
    return {"obs": obs, "expected": expected, "ratio": _ratio(obs, expected)}


def _per_leg(r: np.ndarray, centres) -> tuple[np.ndarray, np.ndarray]:
    obs, exp = np.zeros(r.size), np.zeros(r.size)
    for c in centres:
        w, sh = windows(r, c)
        obs += w
        exp += sh / 2
    return obs, exp


def bootstrap_f2(h_dates: list[date], r: np.ndarray, *, reps: int = BOOT_REPS, seed: int = SEED) -> dict[str, Any]:
    """Pooled Fibonacci ratio over ``reps`` resamples of the distinct h dates with replacement; percentile 95% CI."""
    if not h_dates:
        return {"ci_lo": None, "ci_hi": None, "n_clusters": 0, "reps": reps, "seed": seed, "undefined_reps": reps}
    _, inv = np.unique(np.array([d.toordinal() for d in h_dates]), return_inverse=True)
    n_cl = int(inv.max()) + 1
    obs, exp = _per_leg(r, FIB)
    obs_c, exp_c = np.bincount(inv, obs, n_cl), np.bincount(inv, exp, n_cl)
    draws = np.random.default_rng(seed).integers(0, n_cl, size=(reps, n_cl))
    ob, ex = obs_c[draws].sum(axis=1), exp_c[draws].sum(axis=1)
    ratio = np.divide(ob, ex, out=np.full(reps, np.nan), where=ex > 0)
    ok = ratio[np.isfinite(ratio)]
    lo, hi = (float(v) for v in np.percentile(ok, [2.5, 97.5])) if ok.size else (None, None)
    return {"ci_lo": lo, "ci_hi": hi, "n_clusters": n_cl, "reps": reps, "seed": seed,
            "undefined_reps": int(reps - ok.size)}


def histogram(r: np.ndarray) -> list[int]:
    """1-pp bins over [0, 1]; r == 1.0 lands in the last bin."""
    bins = np.minimum(np.floor(np.round(r * 100, 6)).astype(np.int64), 99)
    return np.bincount(bins, minlength=100).tolist()


def decide_f2(r: dict[str, Any]) -> str:
    fib, placebo, lo = r["pooled"]["fib"], r["pooled"]["placebo"], r["bootstrap"]["ci_lo"]
    if fib["obs"] < F2_MIN_LEGS:
        return rf.INSUFFICIENT
    if (fib["ratio"] is not None and _q(fib["ratio"]) >= RATIO_MIN and lo is not None and lo > 1
            and placebo["ratio"] is not None and _q(fib["ratio"]) > _q(placebo["ratio"])):
        return rf.SUPPORTED
    return rf.NOT_SUPPORTED


def f2_report(outcomes: list[LegOutcome], counts: dict[str, int]) -> dict[str, Any]:
    done = [o for o in outcomes if o.kind == COMPLETED]
    r = np.array([o.r for o in done], dtype="float64")
    held = np.array([o.r for o in done if o.h_date >= rc.HELD_OUT_FROM], dtype="float64")
    out = {"family": "price_structure/fib", "family_n": F2_N,
           "counts": {k: counts.get(k, 0) for k in (COMPLETED, SKIPPED, FAILED, INCOMPLETE, CENSORED, NOT_QUALIFIED)},
           "centres": {key(c): centre_counts(r, c) for c in FIB + CONTROLS},
           "pooled": {"fib": pooled(r, FIB), "placebo": pooled(r, CONTROLS)},
           "bootstrap": bootstrap_f2([o.h_date for o in done], r),
           "held_out": {"n": int(held.size), "fib": pooled(held, FIB), "placebo": pooled(held, CONTROLS)},
           "histogram_1pp": histogram(r)}
    out["decision"] = decide_f2(out)
    return out


def study_f2(series: dict[str, Any], legs: dict[str, Legs]) -> dict[str, Any]:
    counts: dict[str, int] = {}
    outcomes = [o for sym, s in series.items() for o in f2_legs(sym, s, legs[sym], counts)]
    return f2_report(outcomes, counts)


# ----------------------------------------------------------------------------- F3
def confluent(ev: rc.Event, legs: Legs) -> bool | None:
    """Fill within 1% of a Fibonacci level of the leg as of the session before the fill; ``None`` when that
    leg does not qualify."""
    leg = leg_as_of(legs, ev.fill_idx - 1, ev.fill_idx)
    if leg is None:
        return None
    fill = float(ev.fill_px)
    return min(_q(abs(fill / level(leg, d) - 1)) for d in FIB) <= CONFLUENCE_TOL


def decide_f3(r: dict[str, Any]) -> str:
    c, full, p = r["cohorts"][CONFLUENCE], r["cohorts"][rf.FULL], r["permutation"]["p"]
    if c["n"] < rf.MIN_N:
        return rf.INSUFFICIENT
    if (r["cpcv"][CONFLUENCE]["promotable"] and c["median_net"] > max(0.0, full["median_net"])
            and p is not None and p < r["alpha"]):
        return rf.SUPPORTED
    return rf.NOT_SUPPORTED


def study_f3(pop: rc.Population, cost: Decimal) -> dict[str, Any]:
    n = F3_N[pop.strategy]
    pairs = rf.measure(pop, cost)
    legs: dict[str, Legs] = {}
    flags = []
    for ev, _ in pairs:
        if ev.symbol not in legs:
            legs[ev.symbol] = legs_for(ev.series, pop.structural.get(ev.symbol, []))
        flags.append(confluent(ev, legs[ev.symbol]))
    groups = {rf.FULL: pairs, CONFLUENCE: [p for p, f in zip(pairs, flags, strict=True) if f]}
    out = {"family": f"selection_filter/{pop.strategy}", "family_n": n, "alpha": registration_alpha(n),
           "population": {**rf.population_block(pop), "leg_undefined": flags.count(None)},
           **rf.report(groups, n, float(cost))}
    conf, full = ([t.net for _, t in groups[k] if t is not None] for k in (CONFLUENCE, rf.FULL))
    out["permutation"] = permutation_block(conf, full)
    out["decision"] = decide_f3(out)
    return out


# ----------------------------------------------------------------------------- F4
def fib_fill(d: float):
    """The limit at ``H_t - d(H_t - L20)`` on t in y+1..y+5: H_t the highest high over y..t-1, L20 the lowest
    low of the 20 sessions before y; touch fill at min(open, level)."""
    def fill(s, sig) -> tuple[int, float] | None:
        y = sig.idx
        if y < L20_LOOKBACK:
            return None
        l20, top = float(s.low[y - L20_LOOKBACK:y].min()), float(s.high[y])
        if not (math.isfinite(l20) and math.isfinite(top)):
            return None
        for t in range(y + 1, min(y + F4_WINDOW, len(s) - 1) + 1):
            if t > y + 1:
                top = max(top, float(s.high[t - 1]))
            x = float(round_to_tick(top - d * (top - l20)))
            if float(s.low[t]) <= x:
                return t, min(float(s.open[t]), x)
        return None
    return fill


def fill_rate(pop: rc.Population) -> float | None:
    total = len(pop.events) + pop.counts.get("unfilled", 0)
    return round(len(pop.events) / total, 4) if total else None


def matched(a: list[rf.Pair], b: list[rf.Pair]) -> dict[str, Any]:
    """The trades both cohorts measured (same symbol and signal date), descriptive."""
    na, nb = ({(ev.symbol, ev.signal_date): t.net for ev, t in v if t is not None} for v in (a, b))
    common = sorted(na.keys() & nb.keys())
    return {"n": len(common),
            "median_net": round(statistics.median(na[k] for k in common), 4) if common else None,
            f"median_net_{BASE}": round(statistics.median(nb[k] for k in common), 4) if common else None}


def decide_f4(r: dict[str, Any], k: str) -> str:
    c, base = r["cohorts"][k], r["cohorts"][BASE]["median_net"]
    if c["n"] < rf.MIN_N:
        return rf.INSUFFICIENT
    if r["cpcv"][k]["promotable"] and base is not None and c["median_net"] > max(0.0, base):
        return rf.SUPPORTED
    return rf.NOT_SUPPORTED


def winner(r: dict[str, Any]) -> str:
    meds = {k: r["cohorts"][k]["median_net"] for k, d in r["decision"].items() if d == rf.SUPPORTED}
    if not meds:
        return NO_WINNER
    best = max(meds.values())
    top = [k for k, v in meds.items() if v == best]
    return top[0] if len(top) == 1 else WINNER_TIE


def study_f4(base: rc.Population, by_depth: dict[float, rc.Population], cost: Decimal) -> dict[str, Any]:
    pops = {BASE: base} | {key(d): p for d, p in by_depth.items()}
    groups = {k: rf.measure(p, cost) for k, p in pops.items()}
    out = {"family": "entry_mechanism/brk20", "family_n": F4_N,
           "population": {k: {**rf.population_block(p), "fill_rate": fill_rate(p)} for k, p in pops.items()},
           **rf.report(groups, F4_N, float(cost))}
    out["matched"] = {key(d): matched(groups[key(d)], groups[BASE]) for d in by_depth}
    out["decision"] = {key(d): decide_f4(out, key(d)) for d in by_depth}
    out["winner"] = winner(out)
    return out


# ----------------------------------------------------------------------------- run
def universe(conn, end: date, symbols, max_symbols) -> tuple[dict[str, Any], str]:
    """brk20's registered universe, resolved as ``rc.brk20_population`` resolves it."""
    eligible = rc.store_reader(conn).get_universe_eligible_symbols(rc.BRK20_UNIVERSE_AS_OF)
    if not eligible:
        raise rc.Refusal(f"universe_daily has no rows on {rc.BRK20_UNIVERSE_AS_OF} (the brk20 registration's day)")
    return (rc._series(conn, rc._restrict(eligible, symbols), end, max_symbols),
            f"universe_daily eligible set as of {rc.BRK20_UNIVERSE_AS_OF}")


def run(conn, db: Path, end: date, *, symbols=None, max_symbols=None) -> dict[str, Any]:
    if REGISTERED is None:
        raise rc.Refusal("the Fibonacci retracement study is not registered (REGISTERED is None); "
                         "no run before IMPLEMENTATION_PLAN 3 commits it")
    cost_model = CostModel.from_config()
    cost = rc.cost_pct(cost_model)
    series, source = universe(conn, end, symbols, max_symbols)
    structural = bb.load_structural_ex_dates(conn)
    legs = {sym: legs_for(s, structural.get(sym, [])) for sym, s in series.items()}
    m = rf.meta("scripts/backtest_fib.py", f"Fibonacci retracement, IMPLEMENTATION_PLAN 3, pre-registered {REGISTERED}",
                db, end, symbols, max_symbols, F1_N, cost_model)
    m |= {"family_n": FAMILIES, "fold_pass_min": {f: fold_pass_min(n) for f, n in FAMILIES.items()},
          "alpha": {f: registration_alpha(n) for f, n in FAMILIES.items()},
          "seed": SEED, "draws": DRAWS, "boot_reps": BOOT_REPS, "leg_rule": LEG_RULE,
          "leg_window": LEG_WINDOW, "low_window": LOW_WINDOW, "min_leg": MIN_LEG, "pullback_max": PULLBACK_MAX,
          "complete_max": COMPLETE_MAX, "fib": FIB, "controls": CONTROLS,
          "neighbours": {key(d): [key(a) for a in v] for d, v in NEIGHBOURS.items()}, "descriptive": DESCRIPTIVE,
          "f2_half": HALF, "f2_outer": OUTER, "ratio_min": RATIO_MIN, "f2_min_legs": F2_MIN_LEGS,
          "confluence_tol": CONFLUENCE_TOL, "f4_window": F4_WINDOW, "l20_lookback": L20_LOOKBACK,
          "rules": {"f1": F1_RULE, "f2": F2_RULE, "f3": F3_RULE, "f4": F4_RULE}}
    f1 = study_f1(series, legs, structural, source, cost)
    f2 = study_f2(series, legs)
    pops = {s: rf.build_population(s, conn, db, end, symbols, max_symbols) for s in F3_STRATEGIES}
    by_depth = {d: rc.brk20_population(conn, end, symbols=symbols, max_symbols=max_symbols, fill=fib_fill(d))
                for d in FIB}
    return {"meta": m, "f1": f1, "f2": f2, "f3": {s: study_f3(p, cost) for s, p in pops.items()},
            "f4": study_f4(pops["brk20"], by_depth, cost)}


# ----------------------------------------------------------------------------- rendering
TITLE = "FIBONACCI RETRACEMENT STUDY"
num = rf.num


def _cohorts(b: dict[str, Any], n: int) -> list[str]:
    out = ["  cohort            events  voids      n  hit%net  med gross   med net  mean net"]
    for k, c in b["cohorts"].items():
        hit = "-" if c["hit_rate_net"] is None else f"{c['hit_rate_net'] * 100:.1f}"
        out.append(f"  {k:<16} {c['events']:>7} {c['voids']:>6} {c['n']:>6} {hit:>8} {num(c['median_gross']):>10}"
                   f" {num(c['median_net']):>9} {num(c['mean_net']):>9}")
    out.append(f"  CPCV at T+20 (N={n}, fold_pass_min {fold_pass_min(n):.0%}):")
    for k, c in b["cpcv"].items():
        fp = "-" if c["fold_pass_fraction"] is None else f"{c['fold_pass_fraction'] * 100:.1f}%"
        out.append(f"    {k:<16} {c['cv_method']}  splits {c['n_splits']}  fold pass {fp}  promotable {c['promotable']}")
    out.append(f"  held-out (signals >= {rc.HELD_OUT_FROM}, diagnostic): " + ", ".join(
        f"{k} n={h['n']} med net {num(h['median_net'])}" for k, h in b["held_out"].items()))
    return out


def _f1_lines(b: dict[str, Any]) -> list[str]:
    p = b["population"]
    out = ["", f"F1 FIBONACCI PULLBACK ENTRY ({b['family']}, N={b['family_n']}, alpha {b['alpha']:.4f})",
           f"  {p['source']}  [{p['label']}]  symbols {p['n_symbols']}",
           "  roles: " + ", ".join(f"{k}={v}" for k, v in b["roles"].items()),
           "  counts: " + "; ".join(f"{k} {c}" for k, c in p["counts"].items())]
    out += _cohorts(b, b["family_n"])
    out.append("  benchmark (EW close(exit)/open(fill) over the universe; excess = gross - benchmark; CI clustered "
               "on fill dates):")
    for k, e in b["benchmark"].items():
        bt = e["bootstrap"]
        out.append(f"    {k:<16} n={e['n']:<6} undefined {e['undefined']}  mean bench {num(e['mean_benchmark'])}  "
                   f"mean excess {num(e['mean_excess'])}  median excess {num(e['median_excess'])}  "
                   f"95% CI [{num(bt['ci_lo'])}, {num(bt['ci_hi'])}]  clusters {bt['n_clusters']}")
    for k, q in b["permutation"].items():
        out.append(f"  permutation {k}: p {num(q['p'], '.4f')} (median vs {q['draws']} same-size subsamples of "
                   f"{'+'.join(q['pool'])}, n {q['n']} of pool {q['pool_n']}, seed {q['seed']})")
    for k, c in b["classic"].items():
        out.append(f"  classic geometry {k} (stop L, target H, T+20 cap; descriptive): n={c['n']} "
                   f"med net {num(c['median_net'])} mean net {num(c['mean_net'])} hit "
                   f"{'-' if c['hit_rate_net'] is None else format(c['hit_rate_net'] * 100, '.1f')}% "
                   f"exits {c['exit_reasons']}")
    out += [f"  DECISION {b['family']} {k}: {d}" for k, d in b["decision"].items()]
    out.append(f"  rule: {F1_RULE}")
    return out


def _f2_lines(b: dict[str, Any]) -> list[str]:
    fib, plc, bt, ho = b["pooled"]["fib"], b["pooled"]["placebo"], b["bootstrap"], b["held_out"]
    out = ["", f"F2 DO PULLBACKS STOP AT FIBONACCI LEVELS? ({b['family']}, N={b['family_n']}; no trades, no geometry)",
           "  legs: " + ", ".join(f"{k} {v}" for k, v in b["counts"].items()),
           "  centre      obs  shoulders  expected   ratio"]
    for k, c in b["centres"].items():
        out.append(f"  {k:<8} {c['obs']:>6} {c['shoulders']:>10} {c['expected']:>9.1f} {num(c['ratio'], '.3f'):>7}")
    out += [f"  pooled Fibonacci obs {fib['obs']} expected {fib['expected']:.1f} ratio {num(fib['ratio'], '.3f')}  "
            f"95% CI [{num(bt['ci_lo'], '.3f')}, {num(bt['ci_hi'], '.3f')}] ({bt['n_clusters']} h-date clusters, "
            f"{bt['reps']} reps, seed {bt['seed']}, undefined reps {bt['undefined_reps']})",
            f"  pooled placebo obs {plc['obs']} expected {plc['expected']:.1f} ratio {num(plc['ratio'], '.3f')}",
            f"  held-out (h >= {rc.HELD_OUT_FROM}, diagnostic): completed {ho['n']}, Fibonacci ratio "
            f"{num(ho['fib']['ratio'], '.3f')}, placebo {num(ho['placebo']['ratio'], '.3f')}",
            "  histogram of r, 1-pp bins from 0%: " + " ".join(str(v) for v in b["histogram_1pp"]),
            f"  DECISION {b['family']}: {b['decision']}", f"  rule: {F2_RULE}"]
    return out


def _f3_lines(f3: dict[str, Any]) -> list[str]:
    out = ["", "F3 FIBONACCI CONFLUENCE ON THE SWING SIGNALS (C2's populations, arm (ii))"]
    for s, b in f3.items():
        p, q = b["population"], b["permutation"]
        out += [f" [{s}] {b['family']} N={b['family_n']} alpha {b['alpha']:.4f}  {p['source']}  [{p['label']}]",
                f"  symbols {p['n_symbols']}  admitted events {p['n_events']}  leg undefined {p['leg_undefined']}  "
                f"counts {p['counts']}"]
        out += _cohorts(b, b["family_n"])
        out += [f"  permutation p {num(q['p'], '.4f')} (confluence median vs {q['draws']} FULL subsamples, "
                f"seed {q['seed']})", f"  DECISION {b['family']}: {b['decision']}"]
    out.append(f"  rule: {F3_RULE}")
    return out


def _f4_lines(b: dict[str, Any]) -> list[str]:
    out = ["", f"F4 BRK20 LIMIT AT A FIBONACCI LEVEL ({b['family']}, N={b['family_n']}; window y+1..y+{F4_WINDOW}, "
               f"L20 over the {L20_LOOKBACK} sessions before y)"]
    for k, p in b["population"].items():
        out.append(f"  {k:<8} {p['source']}  [{p['label']}]  events {p['n_events']}  fill rate "
                   f"{num(p['fill_rate'], '.4f')}  counts {p['counts']}")
    out += _cohorts(b, b["family_n"])
    for k, mt in b["matched"].items():
        out.append(f"  matched {k} vs {BASE} (same signals, descriptive): n={mt['n']} med net "
                   f"{num(mt['median_net'])} vs {num(mt[f'median_net_{BASE}'])}")
    out += [f"  DECISION {b['family']} {k}: {d}" for k, d in b["decision"].items()]
    out += [f"  WINNER: {b['winner']}", f"  rule: {F4_RULE}"]
    return out


def render(doc: dict[str, Any]) -> str:
    m = doc["meta"]
    out = [f"{TITLE} ({m['registration']})",
           f"db {m['db']}  end {m['window_end']}  {m['construct']}  purge/embargo {m['purge']}/{m['embargo']}  "
           f"seed {m['seed']}",
           f"round trip {m['cost_round_trip_pct']:.4f}% {m['product']} at Rs {m['notional_inr']}",
           f"leg: {m['leg_rule']}", ""]
    if not m["registered_population"]:
        out += ["POPULATION OVERRIDDEN (--symbols/--max-symbols): smoke run, NOT the registered study", ""]
    out.append("GEOMETRY FIRST - median GROSS at T+20 vs one round trip (WO-3 margin floor per session)")
    blocks = [("F1", doc["f1"])] + [(f"F3 {s}", b) for s, b in doc["f3"].items()] + [("F4", doc["f4"])]
    for label, b in blocks:
        for k, g in b["geometry"].items():
            out.append(f"  {label:<9} {k:<16} n={g['n']:<6} median gross {num(g['median_gross_pct'])}%  "
                       f"cost {g['cost_floor_pct']:.4f}%  margin floor {num(g['margin_floor_pct_per_session'], '.5f')}"
                       f"%/session  GEOMETRY: {g['verdict']}")
    out += _f1_lines(doc["f1"]) + _f2_lines(doc["f2"]) + _f3_lines(doc["f3"]) + _f4_lines(doc["f4"])
    out += ["", "Every population is a " + rc.SURVIVORSHIP_LABEL + ".", TOUCH_CAVEAT]
    return rc.ascii_text("\n".join(out))


def main(argv: list[str] | None = None) -> int:
    return rf.cli(argv, prog="backtest_fib", title=TITLE, run_fn=run, render_fn=render, prefix="fib_retracement")


if __name__ == "__main__":
    raise SystemExit(main())
