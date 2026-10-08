#!/usr/bin/env python
"""hvol PRE-REGISTERED backtest harness (IMPLEMENTATION_PLAN.md ``hvol`` paragraph, 2026-10-09).

Abnormal-volume swing drift (H-VOL, Gervais-Kaniel-Mingelgrin 2001): a stock whose daily volume is
abnormally high against its own recent history, on a day it does NOT break out, is tested for drift
over the next 5-20 sessions net of CNC costs AND in excess of the equal-weight market of comparable
names over the same dates. Evidence only: this script wires nothing and reads a research snapshot.

===============================================================================================
PRE-REGISTRATION / MULTIPLICITY DISCIPLINE - READ FIRST
===============================================================================================
**ONE rule is registered and NO parameter sweep is run, here or before.** :data:`PRIMARY` is the
registered rule (VR >= 3.0, not a breakout, fresh, structural-ex-date clean); every threshold below
is a module constant copied from the paragraph, and no CLI flag changes any of them. The trial count
cited to the §6.4 deflation machinery is therefore **N = 1** (:data:`TRIAL_COUNT_N`).

The robustness variants (:data:`VR_2`, :data:`VR_5`, :data:`BREAKOUT_INCLUSIVE`, and the primary
cell's own T+5 / T+10 rows) are REPORTED, NEVER SELECTED: no code path picks, pools or max-picks
them, and the PROMOTION table reads the primary cell at T+20 only. The splits are descriptive.
``--symbols`` / ``--max-symbols`` change the POPULATION only (smoke runs); the report then says it
is not the registered study (``meta.registered_population = false``).

===============================================================================================
CONVENTIONS (each one a choice; the plan paragraph is the spec of record)
===============================================================================================
* **Sessions.** Every window ("sessions strictly before y", "y-50", "y+21", "T+k") is counted in
  the SYMBOL'S OWN ``bars_1d`` series, as ``backtest_hi52`` does. Two descriptive splits are
  market-wide by nature and use the snapshot's union session grid instead: the earnings +-2
  session window and the market-wide volume day.
* **Decision function.** :func:`signal_mask` is THE one function that decides whether (symbol, y)
  is a signal; it ANDs :func:`signal_conditions`, nothing else does. Every input to it is a bar at
  or before ``y`` EXCEPT the registered structural ex-date veto, which reads ex-dates out to
  session ``y+21`` (the plan's own forward window - see "known optimism").
* **Population (as-of y).** >= 60 prior sessions (``y`` index >= 60); median(close x volume) over
  the 20 sessions strictly before ``y`` >= Rs 5cr; close(y) >= Rs 10; >= 40 of the 50 baseline
  sessions with volume > 0. Traded value is rounded to paise before the median so the floor
  comparison is not decided by float noise. Exactly 20 prior sessions are required (the live
  builder accepts <= 20; with >= 60 prior sessions that difference cannot bind for a signal, it
  binds only for benchmark members).
* **VR.** ``volume(y) / median(volume over y-50 .. y-1)``. For the FRESHNESS look-back the VR of a
  day ``j`` in ``y-20 .. y-1`` uses the same definition; when ``j < 50`` (possible only for
  ``y < 70``) its baseline is the ``j`` available sessions (>= 40). A zero baseline median with
  positive volume is VR = +inf (a spike, so it blocks freshness); 0/0 is undefined (no spike).
* **Not a breakout.** ``close(y) <= max(high over y-20 .. y-1)`` (equality is NOT a breakout).
* **Fresh.** No day in ``y-20 .. y-1`` with VR >= the rule's spike threshold, breakout or not.
* **Structural veto.** Any ``bonus/split/rights/demerger`` ex-date (``load_structural_ex_dates``,
  the live kind test) with ``date(y-50) <= ex <= date(y+21)``, both ends inclusive. When session
  ``y+21`` does not exist in the symbol's series the window is UNBOUNDED forward (conservative).
* **Entry / exit.** ``backtest_hi52``: entry ``open(y+1)``, return ``close(y+k) / open(y+1) - 1``
  for k in {5, 10, 20} (T = y). Admission is ``backtest_brk20.bars_usable(series, y, 20)``: every
  bar ``y+1 .. y+20`` must exist with finite positive OHLC, else the event is dropped from EVERY
  horizon (so T+5/T+10/T+20 share one event set).
* **Costs.** One ``CostModel.breakeven_pct(notional, "CNC")`` round trip per trade (fees +
  measured spread) at ``--notional`` (default Rs 20,000 - the number every criterion reads), and
  the same at Rs 6,000 (the §7.1 worst-book swing notional) reported beside it.
* **Benchmark.** Per event and horizon: the equal-weight MEAN gross ``close(X)/open(E) - 1`` over
  every symbol that is as-of liquid on the signal date (the same 20-session floor, evaluated on
  that date from the symbol's own sessions strictly before it - a bar ON the signal date is not
  required), has bars on both the entry date ``E`` and exit date ``X``, and has no structural
  ex-date in ``[E, X]`` inclusive. The event's own symbol qualifies and is included (literal
  "every as-of-liquid symbol"). Excess = event gross - benchmark gross (cost-free on both sides).
* **Inference.** Date-clustered bootstrap: resample the distinct signal dates with replacement
  (2,000 reps, seed 20261009, one draw matrix shared by mean net and mean excess), 95% percentile
  CI, clustered t = full-sample mean / bootstrap SE. A cell with < 2 signal dates or a zero SE is
  DEGENERATE: t is undefined and criterion (4) fails rather than reading a zero-width CI as proof.
* **CPCV.** ``backtest_hi52.cpcv_report`` (per-signal-day mean net / horizon, purge = embargo =
  horizon) with ``promotion_decision`` at N = 1.
* **Tail sensitivity.** Mean net after removing the ``ceil(1% x n)`` highest-net trades (ceil, so
  at least one trade is removed; a cell of one trade has no tail-dropped mean and fails).
* **Variants.** VR >= 2.0 and VR >= 5.0 change the spike threshold in BOTH places the rule uses it
  (VR(y) and the freshness look-back), because "first spike of a cluster" is defined by what a
  spike is. :class:`Rule` carries the two thresholds separately so the other reading is one line.
* **Splits (primary cell, descriptive).** formation sign ``close(y) > close(y-1)``; market-wide
  volume day = >= 5% of the day's as-of-liquid names WITH A BAR that day at full-baseline VR >= 3.0;
  earnings = an ``earnings_calendar`` ``kind='results'`` event dated within grid sessions
  ``y-2 .. y+2`` ("unknown" when none is found and the window starts before the table's first
  results date or ends after its last); index class by CURRENT membership (NIFTY 500 and NIFTY
  Total Market lists, sources per Amendment A1 - SURVIVORSHIP-TAINTED); as-of liquidity tercile = the event's
  liquidity against the cross-section of as-of-liquid names with a bar on ``y`` (``np.quantile``
  cuts, knowable at the close - not full-sample cuts); year; entry gap ``|open(y+1)/close(y) - 1|
  > 5%``; data-source switch = ``src(y)`` differs from the strict majority ``src`` over
  ``y-50 .. y-1`` (a tie has no majority and counts as a switch).
* **Promotion.** All eight criteria at T+20 on the primary cell; decisions read UNROUNDED values,
  and a missing value (empty cell, undefined t) is a FAIL.

KNOWN OPTIMISM (stated, not corrected)
* open(T+1) fills assume the open is reachable: no circuit-lock detection (the entry-gap split is
  the only check).
* Current-membership cells (index class, the deployable cell of criterion 7) are survivorship-
  tainted; the full-market primary is not, since ``bars_1d`` was built from historical bhavcopy
  files that include names that later delisted.
* MIS eligibility and surveillance history are not stored, so the population is wider than the live
  eligible rule.
* Forward ex-veto: the structural veto reads ex-dates up to session y+21 (~a month AFTER the
  signal), a selection on information possibly not public at the close; a live rule could only veto
  actions already announced. It stays as registered (unadjusted stored prices make an in-window
  split a fake -50%) and applies to the benchmark identically (Amendment A1).

===============================================================================================
AMENDMENT A1 (2026-10-09, before the full-population run)
===============================================================================================
* **ETFs excluded (Amendment A1, 2026-10-09).** The population is STOCKS: every symbol on NSE's ETF
  list (``--etf-list``, default ``data/research/nse_etf_list_2026-10-09.csv``, its ``Symbol``
  column stripped and uppercased) plus any symbol matching ``(BEES|ETF|IETF)$`` is removed BEFORE
  anything is computed, so it is absent from the signal population AND from the benchmark, the
  market-wide-volume-day count and the liquidity-tercile cross-section. :func:`is_etf` is the one
  test. A missing, unreadable or empty list REFUSES (exit 2) - never a silent "no exclusion". The
  report prints the list-matched and pattern-only counts and the file's sha256.
* **Current NIFTY 500** for the index-class split = ``data/universe/core_index_cached.csv`` when it
  exists and parses non-empty, else ``config/universe/nifty500_seed.csv``; the report names which.
  The Total Market list stays ``config/universe/niftytotalmarket_seed.csv``.

===============================================================================================
DATA ACCESS
===============================================================================================
Read-only against a research snapshot through ``research_common.open_snapshot``, which REFUSES the
live ``data/market.duckdb``; ``research_common.market_hours_refusal`` refuses 09:15-15:30 IST on
NSE trading days. Index rows (``NIFTY 50``, ``INDIA VIX``) and ETFs (A1) are dropped from the population.
Writes ``<out-dir>/backtest_hvol_<run date>.json`` and ``.md``. Exit codes: 0 ran; 2 refused.
"""

from __future__ import annotations

import argparse
import hashlib
import math
import re
import sys
import time
from collections import Counter, defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import research_common as rc  # noqa: E402
from engine.core.clock import IST  # noqa: E402
from engine.core.config import repo_root  # noqa: E402
from engine.learning.validate import fold_pass_min  # noqa: E402
from engine.strategy.cost_model import CostModel  # noqa: E402
from engine.universe.builder import parse_index_constituents_csv  # noqa: E402

bb = rc.bb          # backtest_brk20: Series, load_series, bars_usable, _stats, structural loaders
bh = rc.bh          # backtest_hi52: cpcv_report, geometry
Series = bb.Series

# =============================================================================== registered constants
REGISTERED_ON = "2026-10-09"
TRIAL_COUNT_N = 1
HORIZONS: tuple[int, ...] = (5, 10, 20)
DECISION_HORIZON = 20
BASELINE_SESSIONS = 50              # VR baseline: median volume over y-50 .. y-1
MIN_POSITIVE_BASELINE = 40          # >= 40 of those 50 with volume > 0
MIN_PRIOR_SESSIONS = 60
LIQUIDITY_SESSIONS = 20             # median(close x volume) over the 20 sessions strictly before y
LIQUIDITY_FLOOR_INR = 50_000_000.0  # Rs 5cr, the live §3.2.4 floor (frozen here, not read from settings)
MIN_CLOSE_INR = 10.0
BREAKOUT_SESSIONS = 20              # not a breakout: close(y) <= max(high over y-20 .. y-1)
FRESH_SESSIONS = 20                 # no spike in y-20 .. y-1
EX_VETO_BEFORE = 50                 # structural ex-date veto from session y-50 ...
EX_VETO_AFTER = 21                  # ... to session y+21
PRODUCT = "CNC"
REFERENCE_NOTIONAL = Decimal("20000")
WORST_BOOK_NOTIONAL = Decimal("6000")
BOOTSTRAP_REPS = 2000
BOOTSTRAP_SEED = 20261009
CI_LEVEL = 0.95
TOP_TAIL_FRAC = 0.01
MIN_N = 200
CLUSTERED_T_MIN = 2.0
HELD_OUT_FROM = date(2026, 1, 1)
MARKET_VOLUME_VR = 3.0
MARKET_VOLUME_DAY_FRAC = 0.05
ENTRY_GAP_MAX = 0.05
EARNINGS_WINDOW_SESSIONS = 2
EARNINGS_KIND = "results"
INDEX_ROWS: tuple[str, ...] = ("NIFTY 50", "INDIA VIX")
DEFAULT_DB_REL = Path("data") / "research" / "market_2026-10-07.duckdb"
NIFTY500_SEED_REL = Path("config") / "universe" / "nifty500_seed.csv"
NIFTY500_CORE_CACHE_REL = Path("data") / "universe" / "core_index_cached.csv"     # A1: preferred over the seed
TOTAL_MARKET_SEED_REL = Path("config") / "universe" / "niftytotalmarket_seed.csv"
AMENDMENT_A1 = "Amendment A1, 2026-10-09"
DEFAULT_ETF_LIST_REL = Path("data") / "research" / "nse_etf_list_2026-10-09.csv"
ETF_PATTERN = re.compile(r"(BEES|ETF|IETF)$")

VERDICT_PROMOTABLE = "PROMOTABLE (candidate only, §8.6 applies)"
VERDICT_REFUTED = "REFUTED"

_SE_EPS = 1e-12
_FOREVER = np.datetime64("9999-12-31", "D")


@dataclass(frozen=True)
class Rule:
    """One registered rule shape. ``fresh_vr_min`` is the spike threshold of the freshness look-back."""

    name: str
    vr_min: float
    fresh_vr_min: float
    require_not_breakout: bool


PRIMARY = Rule("primary", 3.0, 3.0, True)
VR_2 = Rule("vr_ge_2.0", 2.0, 2.0, True)
VR_5 = Rule("vr_ge_5.0", 5.0, 5.0, True)
BREAKOUT_INCLUSIVE = Rule("breakout_inclusive", 3.0, 3.0, False)
RULES: tuple[Rule, ...] = (PRIMARY, VR_2, VR_5, BREAKOUT_INCLUSIVE)
VARIANTS: tuple[Rule, ...] = RULES[1:]

#: Evaluation order of :func:`signal_conditions` (also the funnel's order).
CONDITIONS: tuple[str, ...] = (
    "history", "liquid", "min_close", "baseline_volume", "vr", "not_breakout", "fresh", "no_structural_ex",
)


# =============================================================================== signal
@dataclass(frozen=True)
class Features:
    """Per-index rolling reads of one symbol's own series; index ``i`` uses bars ``< i`` except ``vr``."""

    vr: np.ndarray                  # volume(i) / median(volume[i-50 .. i-1]) (baseline min(50, i))
    positive_baseline: np.ndarray   # count of volume > 0 in i-50 .. i-1 (nan below 50 prior)
    liquidity: np.ndarray           # median traded value over i-20 .. i-1 (nan below 20 prior)
    tv_median_through: np.ndarray   # median traded value over i-19 .. i (the as-of read for i+1)
    prior_high: np.ndarray          # max(high[i-20 .. i-1]) (nan below 20 prior)


def features(s: Series) -> Features:
    v = np.asarray(s.volume, dtype="float64")
    vol = pd.Series(v)
    base = vol.shift(1).rolling(BASELINE_SESSIONS, min_periods=1).median().to_numpy()
    with np.errstate(divide="ignore", invalid="ignore"):
        vr = v / base
    pos = ((vol > 0).astype("float64").shift(1)
           .rolling(BASELINE_SESSIONS, min_periods=BASELINE_SESSIONS).sum().to_numpy())
    tv = pd.Series(np.round(np.asarray(s.close, dtype="float64") * v, 2))
    through = tv.rolling(LIQUIDITY_SESSIONS, min_periods=LIQUIDITY_SESSIONS).median().to_numpy()
    liq = np.concatenate([[np.nan], through[:-1]]) if len(through) else through
    prior_high = (pd.Series(np.asarray(s.high, dtype="float64")).shift(1)
                  .rolling(BREAKOUT_SESSIONS, min_periods=BREAKOUT_SESSIONS).max().to_numpy())
    return Features(vr, pos, liq, through, prior_high)


def _no_structural_ex(dates: Sequence[date], structural: Sequence[date]) -> np.ndarray:
    """True where no structural ex-date lies in ``[date(i-50), date(i+21)]`` (unbounded past the end)."""
    n = len(dates)
    if not structural or n == 0:
        return np.ones(n, dtype=bool)
    exd = np.array(sorted(structural), dtype="datetime64[D]")
    d64 = np.array(dates, dtype="datetime64[D]")
    idx = np.arange(n)
    lo = d64[np.clip(idx - EX_VETO_BEFORE, 0, n - 1)]
    hi_idx = idx + EX_VETO_AFTER
    hi = np.where(hi_idx < n, d64[np.minimum(hi_idx, n - 1)], _FOREVER)
    return (np.searchsorted(exd, hi, side="right") - np.searchsorted(exd, lo, side="left")) == 0


def signal_conditions(
    s: Series, structural: Sequence[date], rule: Rule = PRIMARY, feats: Features | None = None
) -> dict[str, np.ndarray]:
    """Each registered condition as a boolean array over the series, in :data:`CONDITIONS` order."""
    f = feats if feats is not None else features(s)
    n = len(s)
    close = np.asarray(s.close, dtype="float64")
    with np.errstate(invalid="ignore"):
        spike = f.vr >= rule.fresh_vr_min
        prior_spikes = (pd.Series(spike.astype("float64")).shift(1)
                        .rolling(FRESH_SESSIONS, min_periods=1).sum().to_numpy())
        conds = {
            "history": np.arange(n) >= MIN_PRIOR_SESSIONS,
            "liquid": f.liquidity >= LIQUIDITY_FLOOR_INR,
            "min_close": close >= MIN_CLOSE_INR,
            "baseline_volume": f.positive_baseline >= MIN_POSITIVE_BASELINE,
            "vr": f.vr >= rule.vr_min,
            "not_breakout": (close <= f.prior_high) if rule.require_not_breakout else np.ones(n, dtype=bool),
            "fresh": ~(prior_spikes > 0),
            "no_structural_ex": _no_structural_ex(s.dates, structural),
        }
    return conds


def signal_mask(
    s: Series, structural: Sequence[date], rule: Rule = PRIMARY, feats: Features | None = None
) -> np.ndarray:
    """THE decision: ``mask[y]`` is True iff (symbol, y) is a signal under ``rule``."""
    conds = signal_conditions(s, structural, rule, feats)
    return np.logical_and.reduce([conds[c] for c in CONDITIONS])


# =============================================================================== measurement
def event_returns(s: Series, y: int, horizons: Sequence[int] = HORIZONS) -> dict[int, float] | None:
    """``{k: (close(y+k) / open(y+1) - 1) x 100}``; ``None`` (dropped whole) if short of the longest k."""
    if not bb.bars_usable(s, y, max(horizons)):
        return None
    entry = float(s.open[y + 1])
    return {k: (float(s.close[y + k]) / entry - 1.0) * 100.0 for k in horizons}


@dataclass
class CrossSection:
    """Every loaded symbol on the snapshot's union session grid, for the benchmark and the market splits."""

    dates: list[date]
    pos: dict[date, int]
    symbols: list[str]
    open: np.ndarray
    close: np.ndarray
    present: np.ndarray
    liquidity: np.ndarray           # as-of value on each grid date: own 20 sessions strictly before it
    vr: np.ndarray                  # full-baseline VR on own bar days (nan otherwise)
    ex_lt: np.ndarray               # structural ex-dates strictly before each grid date
    ex_le: np.ndarray               # structural ex-dates on or before each grid date
    market_volume_frac: np.ndarray
    _bench: dict[tuple[int, int, int], tuple[float | None, int]] = field(default_factory=dict)
    _cuts: dict[int, tuple[float, float]] = field(default_factory=dict)

    @classmethod
    def build(cls, series: Mapping[str, Series], structural: Mapping[str, Sequence[date]],
              feats: Mapping[str, Features] | None = None) -> CrossSection:
        dates = sorted({d for s in series.values() for d in s.dates})
        grid = np.array(dates, dtype="datetime64[D]")
        symbols = sorted(series)
        shape = (len(dates), len(symbols))
        o, c, liq, vr = (np.full(shape, np.nan) for _ in range(4))
        present = np.zeros(shape, dtype=bool)
        ex_lt, ex_le = np.zeros(shape, dtype=np.int32), np.zeros(shape, dtype=np.int32)
        for j, sym in enumerate(symbols):
            s = series[sym]
            f = (feats or {}).get(sym) or features(s)
            d64 = np.array(s.dates, dtype="datetime64[D]")
            rows = np.searchsorted(grid, d64)
            o[rows, j] = s.open
            c[rows, j] = s.close
            present[rows, j] = True
            vr[rows, j] = np.where(np.arange(len(s)) >= BASELINE_SESSIONS, f.vr, np.nan)
            k = np.searchsorted(d64, grid, side="left")      # own sessions strictly before each date
            liq[:, j] = np.concatenate([[np.nan], f.tv_median_through])[k]
            ex = structural.get(sym)
            if ex:
                exd = np.array(sorted(ex), dtype="datetime64[D]")
                ex_lt[:, j] = np.searchsorted(exd, grid, side="left")
                ex_le[:, j] = np.searchsorted(exd, grid, side="right")
        with np.errstate(invalid="ignore", divide="ignore"):
            live = (liq >= LIQUIDITY_FLOOR_INR) & present
            den = live.sum(axis=1)
            num = (live & (vr >= MARKET_VOLUME_VR)).sum(axis=1)
            frac = np.where(den > 0, num / np.maximum(den, 1), np.nan)
        return cls(dates, {d: i for i, d in enumerate(dates)}, symbols, o, c, present, liq, vr,
                   ex_lt, ex_le, frac)

    def benchmark(self, signal_date: date, entry_date: date, exit_date: date) -> tuple[float | None, int]:
        """``(EW mean gross %, n members)`` over the identical ``open(entry) -> close(exit)`` dates."""
        key = (self.pos[signal_date], self.pos[entry_date], self.pos[exit_date])
        hit = self._bench.get(key)
        if hit is not None:
            return hit
        gy, ge, gx = key
        with np.errstate(invalid="ignore"):
            ok = ((self.liquidity[gy] >= LIQUIDITY_FLOOR_INR) & self.present[ge] & self.present[gx]
                  & ((self.ex_le[gx] - self.ex_lt[ge]) == 0) & (self.open[ge] > 0) & (self.close[gx] > 0))
        r = (self.close[gx][ok] / self.open[ge][ok] - 1.0) * 100.0
        out = (float(r.mean()) if r.size else None, int(r.size))
        self._bench[key] = out
        return out

    def market_volume_day(self, d: date) -> bool:
        f = self.market_volume_frac[self.pos[d]]
        return bool(np.isfinite(f) and f >= MARKET_VOLUME_DAY_FRAC)

    def liquidity_tercile(self, d: date, value: float) -> int:
        """1 (lowest) .. 3 against the as-of-liquid names with a bar on ``d``."""
        g = self.pos[d]
        cuts = self._cuts.get(g)
        if cuts is None:
            with np.errstate(invalid="ignore"):
                live = (self.liquidity[g] >= LIQUIDITY_FLOOR_INR) & self.present[g]
            q1, q2 = np.quantile(self.liquidity[g][live], [1 / 3, 2 / 3])
            cuts = self._cuts[g] = (float(q1), float(q2))
        return 1 if value <= cuts[0] else (2 if value <= cuts[1] else 3)

    def session_window(self, d: date, k: int) -> tuple[date | None, date | None]:
        g = self.pos[d]
        lo = self.dates[g - k] if g - k >= 0 else None
        hi = self.dates[g + k] if g + k < len(self.dates) else None
        return lo, hi


@dataclass(frozen=True)
class Earnings:
    by_symbol: dict[str, list[date]]
    first: date | None
    last: date | None

    def label(self, symbol: str, lo: date | None, hi: date | None) -> str:
        """``yes`` if a results event is in ``[lo, hi]``; else ``unknown`` outside coverage; else ``no``."""
        events = self.by_symbol.get(symbol, [])
        lo_ok = lo if lo is not None else date.min
        hi_ok = hi if hi is not None else date.max
        if any(lo_ok <= e <= hi_ok for e in events):
            return "yes"
        if self.first is None or lo is None or hi is None or lo < self.first or hi > self.last:
            return "unknown"
        return "no"


def source_switch(src: Sequence[str], y: int) -> bool:
    """``src(y)`` differs from the strict-majority ``src`` of ``y-50 .. y-1`` (a tie is a switch)."""
    top = Counter(src[max(0, y - BASELINE_SESSIONS):y]).most_common(2)
    if not top or (len(top) == 2 and top[0][1] == top[1][1]):
        return True
    return src[y] != top[0][0]


@dataclass
class Trade:
    """One measured event. Returns are PERCENT; ``net`` at the reference notional, ``net_6k`` at Rs 6k."""

    symbol: str
    rule: str
    signal_date: date
    entry_date: date
    vr: float
    gross: dict[int, float] = field(default_factory=dict)
    net: dict[int, float] = field(default_factory=dict)
    net_6k: dict[int, float] = field(default_factory=dict)
    benchmark: dict[int, float] = field(default_factory=dict)
    benchmark_n: dict[int, int] = field(default_factory=dict)
    excess: dict[int, float] = field(default_factory=dict)
    formation_up: bool = False
    market_volume_day: bool = False
    earnings: str = "unknown"
    index_class: str = "neither"
    deployable: bool = False
    liquidity_tercile: int = 0
    entry_gap: bool = False
    source_switch: bool = False


# =============================================================================== inference
def _empty_boot(n_clusters: int = 0) -> dict[str, Any]:
    return {"mean": None, "se": None, "ci_lo": None, "ci_hi": None, "t": None,
            "n_clusters": n_clusters, "reps": BOOTSTRAP_REPS, "seed": BOOTSTRAP_SEED, "degenerate": True}


def clustered_bootstrap(
    dates: Sequence[date], columns: Mapping[str, Sequence[float]], *,
    reps: int = BOOTSTRAP_REPS, seed: int = BOOTSTRAP_SEED, level: float = CI_LEVEL,
) -> dict[str, dict[str, Any]]:
    """Resample DISTINCT signal dates with replacement; per column the mean, SE, percentile CI and t."""
    if not dates:
        return {name: _empty_boot() for name in columns}
    _, inv = np.unique(np.array([d.toordinal() for d in dates]), return_inverse=True)
    n_clusters = int(inv.max()) + 1
    cnt = np.bincount(inv, minlength=n_clusters).astype("float64")
    draws = np.random.default_rng(seed).integers(0, n_clusters, size=(reps, n_clusters))
    cnt_b = cnt[draws].sum(axis=1)
    tail = (1.0 - level) / 2.0 * 100.0
    out: dict[str, dict[str, Any]] = {}
    for name, vals in columns.items():
        v = np.asarray(vals, dtype="float64")
        mean = float(v.mean())
        means_b = np.bincount(inv, weights=v, minlength=n_clusters)[draws].sum(axis=1) / cnt_b
        se = float(means_b.std(ddof=1)) if reps > 1 else 0.0
        lo, hi = np.percentile(means_b, [tail, 100.0 - tail])
        degenerate = n_clusters < 2 or se <= _SE_EPS * max(1.0, abs(mean))
        out[name] = {"mean": mean, "se": se, "ci_lo": float(lo), "ci_hi": float(hi),
                     "t": None if degenerate else mean / se, "n_clusters": n_clusters,
                     "reps": reps, "seed": seed, "degenerate": bool(degenerate)}
    return out


def top_tail_dropped_mean(values: Sequence[float], frac: float = TOP_TAIL_FRAC) -> float | None:
    """Mean after removing the ``ceil(frac x n)`` largest values; ``None`` when nothing is left."""
    if not values:
        return None
    drop = math.ceil(frac * len(values))
    kept = sorted(values)[: len(values) - drop]
    return float(np.mean(kept)) if kept else None


def _mean(xs: Sequence[float]) -> float | None:
    return float(np.mean(xs)) if len(xs) else None


def _median(xs: Sequence[float]) -> float | None:
    return float(np.median(xs)) if len(xs) else None


def _r(x: float | None, nd: int = 6) -> float | None:
    return None if x is None else round(float(x), nd)


def _boot_rounded(b: dict[str, Any]) -> dict[str, Any]:
    return {k: (_r(v) if isinstance(v, float) else v) for k, v in b.items()}


def horizon_block(trades: Sequence[Trade], k: int) -> dict[str, Any]:
    gross = [t.gross[k] for t in trades]
    net = [t.net[k] for t in trades]
    net6 = [t.net_6k[k] for t in trades]
    excess = [t.excess[k] for t in trades]
    boot = clustered_bootstrap([t.signal_date for t in trades], {"net": net, "excess": excess})
    out = dict(bb._stats(gross, net))
    out.update({
        "n_signal_dates": len({t.signal_date for t in trades}),
        "mean_net_6k": _r(_mean(net6)), "median_net_6k": _r(_median(net6)),
        "hit_rate_net_6k": _r(sum(1 for v in net6 if v > 0) / len(net6) if net6 else None, 4),
        "mean_benchmark": _r(_mean([t.benchmark[k] for t in trades])),
        "mean_benchmark_members": _r(_mean([t.benchmark_n[k] for t in trades]), 1),
        "mean_excess": _r(_mean(excess)), "median_excess": _r(_median(excess)),
        "bootstrap_net": _boot_rounded(boot["net"]), "bootstrap_excess": _boot_rounded(boot["excess"]),
        "top1_dropped_mean_net": _r(top_tail_dropped_mean(net)),
    })
    return out


def cell(trades: Sequence[Trade], label: str | None = None) -> dict[str, Any]:
    out: dict[str, Any] = {"n_trades": len(trades), "horizons": {str(k): horizon_block(trades, k) for k in HORIZONS}}
    if label:
        out["label"] = label
    return out


# =============================================================================== promotion
def _gt(x: Any, thr: float) -> bool:
    return x is not None and not isinstance(x, bool) and math.isfinite(float(x)) and float(x) > thr


def _fmt(x: Any, nd: int = 4) -> str:
    return "n/a" if x is None else (f"{x:+.{nd}f}" if isinstance(x, float) else str(x))


def promotion_inputs(trades: Sequence[Trade], cost_pct: float, cpcv_promotable: bool) -> dict[str, Any]:
    """Criterion inputs at T+20 from the primary cell's UNROUNDED per-trade values."""
    k = DECISION_HORIZON
    net = [t.net[k] for t in trades]
    excess = [t.excess[k] for t in trades]
    boot = clustered_bootstrap([t.signal_date for t in trades], {"net": net, "excess": excess})
    deploy = [t for t in trades if t.deployable]
    held = [t for t in trades if t.signal_date >= HELD_OUT_FROM]
    return {
        "n": len(trades),
        "median_gross": _median([t.gross[k] for t in trades]),
        "cost_pct": cost_pct,
        "mean_net": _mean(net),
        "t_net": boot["net"]["t"],
        "mean_excess": _mean(excess),
        "excess_ci_lo": boot["excess"]["ci_lo"],
        "excess_ci_hi": boot["excess"]["ci_hi"],
        "excess_degenerate": boot["excess"]["degenerate"],
        "cpcv_promotable": bool(cpcv_promotable),
        "top1_dropped_mean_net": top_tail_dropped_mean(net),
        "deployable_mean_net": _mean([t.net[k] for t in deploy]),
        "deployable_mean_excess": _mean([t.excess[k] for t in deploy]),
        "held_out_mean_excess": _mean([t.excess[k] for t in held]),
    }


def promotion_table(m: Mapping[str, Any]) -> dict[str, Any]:
    """Criteria (1)-(8), each PASS/FAIL with its number; all must hold."""
    lo, hi = m.get("excess_ci_lo"), m.get("excess_ci_hi")
    ci_excludes_0 = (not m.get("excess_degenerate", True) and lo is not None and hi is not None
                     and (_gt(lo, 0.0) or _gt(-hi, 0.0)))
    rows = [
        (1, f"n >= {MIN_N}", f"n = {m.get('n')}", m.get("n") is not None and m["n"] >= MIN_N),
        (2, "GEOMETRY viable: median gross > round-trip cost",
         f"median gross {_fmt(m.get('median_gross'))}% vs cost {_fmt(m.get('cost_pct'))}%",
         _gt(m.get("median_gross"), float(m.get("cost_pct") or math.inf))),
        (3, f"mean net > 0 AND clustered t > {CLUSTERED_T_MIN:g}",
         f"mean net {_fmt(m.get('mean_net'))}%, t {_fmt(m.get('t_net'), 2)}",
         _gt(m.get("mean_net"), 0.0) and _gt(m.get("t_net"), CLUSTERED_T_MIN)),
        (4, "mean excess > 0 AND its 95% clustered CI excludes 0",
         f"mean excess {_fmt(m.get('mean_excess'))}%, CI [{_fmt(lo)}, {_fmt(hi)}]"
         + (" DEGENERATE" if m.get("excess_degenerate") else ""),
         _gt(m.get("mean_excess"), 0.0) and ci_excludes_0),
        (5, f"CPCV promotable (N={TRIAL_COUNT_N})", f"promotable = {m.get('cpcv_promotable')}",
         m.get("cpcv_promotable") is True),
        (6, "top-1%-dropped mean net > 0", f"{_fmt(m.get('top1_dropped_mean_net'))}%",
         _gt(m.get("top1_dropped_mean_net"), 0.0)),
        (7, f"deployable cell (CURRENT NIFTY Total Market, {rc.SURVIVORSHIP_LABEL}): "
            "mean net > 0 AND mean excess > 0",
         f"mean net {_fmt(m.get('deployable_mean_net'))}%, mean excess {_fmt(m.get('deployable_mean_excess'))}%",
         _gt(m.get("deployable_mean_net"), 0.0) and _gt(m.get("deployable_mean_excess"), 0.0)),
        (8, f"held-out slice (signals >= {HELD_OUT_FROM}): mean excess > 0",
         f"mean excess {_fmt(m.get('held_out_mean_excess'))}%", _gt(m.get("held_out_mean_excess"), 0.0)),
    ]
    out = [{"id": i, "criterion": c, "value": v, "pass": bool(p)} for i, c, v, p in rows]
    return {"horizon": DECISION_HORIZON, "rows": out,
            "verdict": VERDICT_PROMOTABLE if all(r["pass"] for r in out) else VERDICT_REFUTED}


# =============================================================================== loaders
def load_sources(conn, series: Mapping[str, Series]) -> dict[str, list[str]]:
    """``bars_1d.src`` aligned to each loaded series' own dates."""
    frame = conn.execute("SELECT symbol, d, src FROM bars_1d").df()
    by: dict[str, dict[date, str]] = defaultdict(dict)
    for sym, d, src in zip(frame["symbol"].tolist(), frame["d"].tolist(), frame["src"].tolist(), strict=True):
        if sym in series:
            by[sym][d.date() if hasattr(d, "date") else d] = str(src)
    return {sym: [by[sym][d] for d in s.dates] for sym, s in series.items()}


def load_earnings(conn) -> Earnings:
    try:
        rows = conn.execute(
            "SELECT symbol, event_date FROM earnings_calendar WHERE kind = ?", [EARNINGS_KIND]
        ).fetchall()
    except Exception:  # noqa: BLE001 - a missing table makes every label "unknown", never a crash
        rows = []
    by: dict[str, list[date]] = defaultdict(list)
    for sym, d in rows:
        by[str(sym)].append(d.date() if hasattr(d, "date") else d)
    every = [d for v in by.values() for d in v]
    return Earnings({k: sorted(v) for k, v in by.items()}, min(every, default=None), max(every, default=None))


def _parse_members(path: Path) -> set[str]:
    """Non-empty member set from an index CSV, or raise ``OSError``/``ValueError``."""
    members = set(parse_index_constituents_csv(path.read_text(encoding="utf-8-sig")))
    if not members:
        raise ValueError("no symbols")
    return members


def load_membership(root: Path | None = None) -> tuple[set[str], set[str], dict[str, str]]:
    """``(nifty500, total_market, sources)``. NIFTY 500 prefers the downloaded core list (A1)."""
    root = Path(root) if root is not None else repo_root()
    nifty500: set[str] | None = None
    n500_source = NIFTY500_CORE_CACHE_REL
    if (root / NIFTY500_CORE_CACHE_REL).exists():
        try:
            nifty500 = _parse_members(root / NIFTY500_CORE_CACHE_REL)
        except (OSError, ValueError):
            nifty500 = None              # unreadable / empty core list: fall back to the seed
    sets: list[set[str]] = []
    for rel in ((NIFTY500_SEED_REL,) if nifty500 is None else ()) + (TOTAL_MARKET_SEED_REL,):
        try:
            sets.append(_parse_members(root / rel))
        except (OSError, ValueError) as exc:
            raise rc.Refusal(f"index membership list unreadable or empty: {root / rel} ({exc})") from exc
    if nifty500 is None:
        nifty500, n500_source = sets.pop(0), NIFTY500_SEED_REL
    return nifty500, sets[0], {"nifty500": str(n500_source), "total_market": str(TOTAL_MARKET_SEED_REL)}


@dataclass(frozen=True)
class EtfList:
    path: str
    symbols: frozenset[str]
    sha256: str


def load_etf_list(path: Path) -> EtfList:
    """NSE's ETF list (``Symbol`` column, stripped + uppercased). Missing / unparseable / empty REFUSES."""
    p = Path(path)
    try:
        raw = p.read_bytes()
        symbols = frozenset(parse_index_constituents_csv(raw.decode("utf-8-sig")))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise rc.Refusal(f"ETF list unreadable: {p} ({type(exc).__name__}: {exc}); "
                         f"{AMENDMENT_A1} requires it - no silent 'no exclusion'") from exc
    if not symbols:
        raise rc.Refusal(f"ETF list has no symbols: {p}; {AMENDMENT_A1} requires it")
    return EtfList(str(p), symbols, hashlib.sha256(raw).hexdigest())


def is_etf(symbol: str, etf_symbols: frozenset[str] | set[str]) -> bool:
    """THE ETF test (Amendment A1): on NSE's ETF list, or the name ends BEES / ETF / IETF."""
    sym = symbol.strip().upper()
    return sym in etf_symbols or ETF_PATTERN.search(sym) is not None


def exclude_etfs(series: Mapping[str, Series], etf: EtfList) -> tuple[dict[str, Series], dict[str, int]]:
    """``(stocks only, {via_list, via_pattern_only})``; nothing downstream ever sees an ETF."""
    kept: dict[str, Series] = {}
    counts = {"via_list": 0, "via_pattern_only": 0}
    for sym, s in series.items():
        if not is_etf(sym, etf.symbols):
            kept[sym] = s
        elif sym.strip().upper() in etf.symbols:
            counts["via_list"] += 1
        else:
            counts["via_pattern_only"] += 1
    return kept, counts


def index_class(symbol: str, nifty500: set[str], total_market: set[str]) -> str:
    sym = symbol.upper()
    if sym in nifty500:
        return "nifty500"
    return "total_market_only" if sym in total_market else "neither"


# =============================================================================== the study
SplitFn = Callable[[Trade], str]
SPLITS: tuple[tuple[str, SplitFn, tuple[str, ...] | None, str | None], ...] = (
    ("formation_day_return", lambda t: "up" if t.formation_up else "down_or_flat", ("up", "down_or_flat"), None),
    ("market_wide_volume_day", lambda t: "yes" if t.market_volume_day else "no", ("yes", "no"), None),
    ("earnings_within_2_sessions", lambda t: t.earnings, ("yes", "no", "unknown"), None),
    ("index_class_current_membership", lambda t: t.index_class, ("nifty500", "total_market_only", "neither"),
     rc.SURVIVORSHIP_LABEL),
    ("liquidity_tercile_as_of", lambda t: ("t1_lowest", "t2", "t3_highest")[t.liquidity_tercile - 1],
     ("t1_lowest", "t2", "t3_highest"), None),
    ("year", lambda t: str(t.signal_date.year), None, None),
    ("entry_gap_gt_5pct", lambda t: "yes" if t.entry_gap else "no", ("yes", "no"), None),
    ("data_source_switch", lambda t: "yes" if t.source_switch else "no", ("yes", "no"), None),
)


def split_cells(trades: Sequence[Trade]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for name, fn, order, label in SPLITS:
        groups: dict[str, list[Trade]] = defaultdict(list)
        for t in trades:
            groups[fn(t)].append(t)
        keys = order if order is not None else tuple(sorted(groups))
        out[name] = {"label": label, "cells": {key: cell(groups.get(key, []), label) for key in keys}}
    return out


def measure_rule(
    rule: Rule, series: Mapping[str, Series], feats: Mapping[str, Features], structural: Mapping[str, list[date]],
    cs: CrossSection, *, sources: Mapping[str, list[str]], earnings: Earnings, nifty500: set[str],
    total_market: set[str], cost_pct: float, cost_6k: float,
) -> tuple[list[Trade], dict[str, int]]:
    trades: list[Trade] = []
    funnel: dict[str, int] = dict.fromkeys(("symbol_days", *CONDITIONS, "signals",
                                            "dropped_short_forward_window", "measured"), 0)
    for sym in sorted(series):
        s, f, sx = series[sym], feats[sym], structural.get(sym, [])
        conds = signal_conditions(s, sx, rule, f)
        running = np.ones(len(s), dtype=bool)
        funnel["symbol_days"] += len(s)
        for c in CONDITIONS:
            running &= conds[c]
            funnel[c] += int(running.sum())
        mask = signal_mask(s, sx, rule, f)
        if not np.array_equal(mask, running):
            raise AssertionError(f"{sym}: funnel and signal_mask disagree")
        for y in np.flatnonzero(mask).tolist():
            funnel["signals"] += 1
            rets = event_returns(s, y)
            if rets is None:
                funnel["dropped_short_forward_window"] += 1
                continue
            t = Trade(sym, rule.name, s.dates[y], s.dates[y + 1], float(f.vr[y]))
            for k, g in rets.items():
                bench, n_b = cs.benchmark(s.dates[y], s.dates[y + 1], s.dates[y + k])
                if bench is None:
                    raise AssertionError(f"{sym} {s.dates[y]} T+{k}: empty benchmark (the event itself qualifies)")
                t.gross[k], t.net[k], t.net_6k[k] = g, g - cost_pct, g - cost_6k
                t.benchmark[k], t.benchmark_n[k], t.excess[k] = bench, n_b, g - bench
            close_y = float(s.close[y])
            t.formation_up = close_y > float(s.close[y - 1])
            t.market_volume_day = cs.market_volume_day(s.dates[y])
            t.earnings = earnings.label(sym, *cs.session_window(s.dates[y], EARNINGS_WINDOW_SESSIONS))
            t.index_class = index_class(sym, nifty500, total_market)
            t.deployable = sym.upper() in total_market
            t.liquidity_tercile = cs.liquidity_tercile(s.dates[y], float(f.liquidity[y]))
            t.entry_gap = abs(float(s.open[y + 1]) / close_y - 1.0) > ENTRY_GAP_MAX
            t.source_switch = source_switch(sources[sym], y)
            trades.append(t)
            funnel["measured"] += 1
    return trades, funnel


def rule_block(rule: Rule, trades: list[Trade], funnel: dict[str, int], cost_pct: float) -> dict[str, Any]:
    return {
        "rule": {"name": rule.name, "vr_min": rule.vr_min, "fresh_vr_min": rule.fresh_vr_min,
                 "require_not_breakout": rule.require_not_breakout},
        "funnel": funnel,
        "geometry": bh.geometry(trades, cost_pct, HORIZONS),
        "horizons": {str(k): horizon_block(trades, k) for k in HORIZONS},
        "cpcv": {str(k): bh.cpcv_report(trades, k, cost_pct, TRIAL_COUNT_N) for k in HORIZONS},
    }


def run_study(conn, *, db_path: Path, etf: EtfList, notional: Decimal = REFERENCE_NOTIONAL,
              symbols: Sequence[str] | None = None, max_symbols: int | None = None) -> dict[str, Any]:
    t0 = time.monotonic()
    cm = CostModel.from_config()
    cost_pct = float(cm.breakeven_pct(notional, PRODUCT))
    cost_6k = float(cm.breakeven_pct(WORST_BOOK_NOTIONAL, PRODUCT))
    series = bb.load_series(conn, rc.FULL_HISTORY_START, date.max, symbols=symbols, max_symbols=max_symbols)
    for name in INDEX_ROWS:
        series.pop(name, None)
    # A1: removed before features / CrossSection, so the signal population AND every cross-section
    # (benchmark, market-wide volume day, liquidity terciles) are stocks only.
    series, etf_counts = exclude_etfs(series, etf)
    if not series:
        raise rc.Refusal("no bars_1d rows for the requested population")
    sources = load_sources(conn, series)
    structural = bb.load_structural_ex_dates(conn)
    earnings = load_earnings(conn)
    nifty500, total_market, seeds = load_membership()
    feats = {sym: features(s) for sym, s in series.items()}
    cs = CrossSection.build(series, structural, feats)
    common = {"sources": sources, "earnings": earnings, "nifty500": nifty500, "total_market": total_market,
              "cost_pct": cost_pct, "cost_6k": cost_6k}
    measured = {r.name: measure_rule(r, series, feats, structural, cs, **common) for r in RULES}

    prim_trades, _ = measured[PRIMARY.name]
    blocks = {r.name: rule_block(r, *measured[r.name], cost_pct) for r in RULES}
    deploy = [t for t in prim_trades if t.deployable]
    held = [t for t in prim_trades if t.signal_date >= HELD_OUT_FROM]
    inputs = promotion_inputs(prim_trades, cost_pct,
                              blocks[PRIMARY.name]["cpcv"][str(DECISION_HORIZON)]["promotable"])
    registered = not symbols and not max_symbols
    notes = [
        f"NO PARAMETER SWEEP: one registered rule, trial count N={TRIAL_COUNT_N} "
        f"(fold_pass_min = {fold_pass_min(TRIAL_COUNT_N):.0%}). Variants are reported, never selected.",
        "Current-membership cells (index class; the deployable cell of criterion 7) are a "
        + rc.SURVIVORSHIP_LABEL + ". The full-market primary is not: bars_1d holds delisted names' history.",
        "Known optimism: open(T+1) fills assume the open is reachable (no circuit-lock detection); MIS "
        "eligibility and surveillance history are not stored, so the population is wider than the live "
        "eligible rule; the structural veto reads ex-dates up to session y+21 (forward ex-veto, possibly not "
        "public at the close; applied to the benchmark identically).",
        f"ETFs excluded ({AMENDMENT_A1}) from the signal population AND the benchmark / market-wide / tercile "
        "cross-sections: NSE ETF list plus symbols matching " + ETF_PATTERN.pattern + ".",
        f"Earnings split reads earnings_calendar kind='{EARNINGS_KIND}' covering {earnings.first} -> "
        f"{earnings.last}; a window with no event outside that span is 'unknown'.",
        "Benchmark members: as-of liquid on the signal date, bars on the entry and exit dates, no structural "
        "ex-date in [entry, exit]; the event's own symbol is a member.",
    ]
    if not registered:
        notes.insert(0, "POPULATION OVERRIDDEN (--symbols/--max-symbols): smoke run, NOT the registered study; "
                        "the benchmark and market-wide splits are computed over the loaded names only.")
    n_sessions = len(cs.dates)
    return {
        "meta": {
            "script": "scripts/backtest_hvol.py",
            "registration": f"hvol, IMPLEMENTATION_PLAN.md, pre-registered {REGISTERED_ON}",
            "generated_at": datetime.now(IST).isoformat(timespec="seconds"),
            "db": str(db_path),
            "window": {"start": str(cs.dates[0]), "end": str(cs.dates[-1])},
            "n_symbols": len(series), "n_sessions": n_sessions,
            "population_override": {"symbols": list(symbols) if symbols else None, "max_symbols": max_symbols},
            "registered_population": registered,
            "index_rows_excluded": list(INDEX_ROWS),
            "etf_exclusion": {"amendment": AMENDMENT_A1, "list_path": etf.path, "sha256": etf.sha256,
                              "list_size": len(etf.symbols), "pattern": ETF_PATTERN.pattern, **etf_counts},
            "trial_count_n": TRIAL_COUNT_N, "fold_pass_min": fold_pass_min(TRIAL_COUNT_N),
            "parameter_sweep_run": False,
            "horizons_sessions": list(HORIZONS), "decision_horizon": DECISION_HORIZON,
            "entry_convention": "open(y+1)", "exit_convention": "close(y+k) / open(y+1) - 1",
            "product": PRODUCT,
            "reference_notional_inr": str(notional), "cost_round_trip_pct": round(cost_pct, 6),
            "cost_fees_pct": round(float(cm.fee_breakeven_pct(notional, PRODUCT)), 6),
            "cost_spread_pct": round(float(cm.spread_pct), 6),
            "worst_book_notional_inr": str(WORST_BOOK_NOTIONAL), "cost_round_trip_6k_pct": round(cost_6k, 6),
            "constants": {
                "baseline_sessions": BASELINE_SESSIONS, "min_positive_baseline": MIN_POSITIVE_BASELINE,
                "min_prior_sessions": MIN_PRIOR_SESSIONS, "liquidity_sessions": LIQUIDITY_SESSIONS,
                "liquidity_floor_inr": LIQUIDITY_FLOOR_INR, "min_close_inr": MIN_CLOSE_INR,
                "breakout_sessions": BREAKOUT_SESSIONS, "fresh_sessions": FRESH_SESSIONS,
                "ex_veto_sessions": [-EX_VETO_BEFORE, EX_VETO_AFTER],
                "bootstrap_reps": BOOTSTRAP_REPS, "bootstrap_seed": BOOTSTRAP_SEED, "ci_level": CI_LEVEL,
                "top_tail_frac": TOP_TAIL_FRAC, "min_n": MIN_N, "clustered_t_min": CLUSTERED_T_MIN,
                "held_out_from": str(HELD_OUT_FROM), "market_volume_vr": MARKET_VOLUME_VR,
                "market_volume_day_frac": MARKET_VOLUME_DAY_FRAC, "entry_gap_max": ENTRY_GAP_MAX,
                "earnings_window_sessions": EARNINGS_WINDOW_SESSIONS,
            },
            "membership_seeds": seeds, "nifty500_size": len(nifty500), "total_market_size": len(total_market),
            "nifty500_not_in_total_market": len(nifty500 - total_market),
            "earnings_coverage": {"kind": EARNINGS_KIND, "first": str(earnings.first), "last": str(earnings.last)},
            "corp_actions_coverage": bb.corp_actions_coverage(conn),
            "verdict_promotable_text": VERDICT_PROMOTABLE,
            "runtime_seconds": None,
        },
        "primary": blocks[PRIMARY.name],
        "variants": {r.name: blocks[r.name] for r in VARIANTS},
        "splits": split_cells(prim_trades),
        "promotion_cells": {
            "deployable_total_market_current": cell(deploy, rc.SURVIVORSHIP_LABEL),
            "held_out_2026": cell(held),
        },
        "promotion": {**promotion_table(inputs), "inputs": {k: (_r(v) if isinstance(v, float) else v)
                                                              for k, v in inputs.items()}},
        "notes": notes,
        "_runtime_t0": t0,
    }


# =============================================================================== rendering
_HDR = ("  {:<26} | hor |     n | dates | mean gr | med gr  | mean net | med net  | net 6k   | med 6k   | "
        "hit% | bench   | excess   | excess 95% CI         | t(net) | t(exc) | top1%-drop")


def _num(v: Any, nd: int = 4, width: int = 8) -> str:
    return ("-" if v is None else f"{v:+.{nd}f}").rjust(width)


def _hrow(name: str, k: str, h: dict[str, Any]) -> str:
    bn, bx = h["bootstrap_net"], h["bootstrap_excess"]
    ci = "-" if bx["ci_lo"] is None else f"[{bx['ci_lo']:+.4f}, {bx['ci_hi']:+.4f}]"
    hit = "-" if h["hit_rate_net"] is None else f"{h['hit_rate_net'] * 100:.1f}"
    return (f"  {name:<26} | T+{k:<2}| {h['n']:>5} | {h['n_signal_dates']:>5} | {_num(h['mean_gross'], 3, 7)} | "
            f"{_num(h['median_gross'], 3, 7)} | {_num(h['mean_net'])} | {_num(h['median_net'])} | "
            f"{_num(h['mean_net_6k'])} | {_num(h['median_net_6k'])} | {hit:>4} | {_num(h['mean_benchmark'], 3, 7)} | "
            f"{_num(h['mean_excess'])} | {ci:<21} | {_num(bn['t'], 2, 6)} | {_num(bx['t'], 2, 6)} | "
            f"{_num(h['top1_dropped_mean_net'])}")


def _cell_rows(name: str, c: dict[str, Any]) -> list[str]:
    return [_hrow(name, k, c["horizons"][k]) for k in sorted(c["horizons"], key=int)]


def _cpcv_lines(block: dict[str, Any]) -> list[str]:
    out = []
    for k in sorted(block["cpcv"], key=int):
        c = block["cpcv"][k]
        fp = "-" if c["fold_pass_fraction"] is None else f"{c['fold_pass_fraction'] * 100:.1f}%"
        out.append(f"  CPCV T+{k}: {c['cv_method']}  obs_days={c['n_obs_days']}  splits={c['n_splits']}  "
                   f"fold_pass={fp} (need {c['fold_pass_min'] * 100:.0f}%)  promotable={c['promotable']}")
    return out


def render_text(doc: dict[str, Any]) -> str:
    m = doc["meta"]
    out: list[str] = []
    add = out.append
    add("=" * 110)
    add(f"hvol PRE-REGISTERED BACKTEST - abnormal-volume swing drift ({m['registration']})")
    add("=" * 110)
    add(f"generated : {m['generated_at']}   runtime {m['runtime_seconds']} s")
    add(f"db        : {m['db']}")
    add(f"window    : {m['window']['start']} -> {m['window']['end']}  ({m['n_sessions']} sessions, "
        f"{m['n_symbols']} symbols; index rows excluded: {', '.join(m['index_rows_excluded'])})")
    e = m["etf_exclusion"]
    add(f"ETFs excluded ({e['amendment']}): {e['via_list']} via list + {e['via_pattern_only']} via pattern "
        f"{e['pattern']} only; list {e['list_path']} ({e['list_size']} names) sha256 {e['sha256']}")
    add(f"NIFTY 500 list: {m['membership_seeds']['nifty500']}   Total Market list: "
        f"{m['membership_seeds']['total_market']}")
    if not m["registered_population"]:
        add("POPULATION OVERRIDDEN (--symbols/--max-symbols): smoke run, NOT the registered study")
    add(f"rule      : VR(y) >= {PRIMARY.vr_min} vs median volume y-{BASELINE_SESSIONS}..y-1; "
        f"close(y) <= max(high y-{BREAKOUT_SESSIONS}..y-1); no VR >= {PRIMARY.fresh_vr_min} in "
        f"y-{FRESH_SESSIONS}..y-1; no structural ex-date y-{EX_VETO_BEFORE}..y+{EX_VETO_AFTER}")
    add(f"population: >= {MIN_PRIOR_SESSIONS} prior sessions; median traded value (20 prior) >= Rs 5cr; "
        f"close >= Rs {MIN_CLOSE_INR:g}; >= {MIN_POSITIVE_BASELINE}/{BASELINE_SESSIONS} positive-volume baseline")
    add(f"trade     : entry {m['entry_convention']}, return {m['exit_convention']}, k in {m['horizons_sessions']}")
    add(f"cost      : {m['cost_round_trip_pct']:.4f}% {m['product']} round trip at Rs {m['reference_notional_inr']} "
        f"({m['cost_fees_pct']:.4f}% fees + {m['cost_spread_pct']:.4f}% spread); "
        f"{m['cost_round_trip_6k_pct']:.4f}% at Rs {m['worst_book_notional_inr']}")
    add(f"trials    : N={m['trial_count_n']} (fold_pass_min {m['fold_pass_min']:.0%}); no sweep; "
        "variants reported, never selected")
    add("")
    add("-" * 110)
    add("STEP 1 - COST GEOMETRY (median per-trade GROSS vs one round trip; read BEFORE any other number)")
    add("-" * 110)
    for name, block in (("primary", doc["primary"]), *doc["variants"].items()):
        geo = block["geometry"]
        for k in sorted(geo["horizons"], key=int):
            g = geo["horizons"][k]
            add(f"  GEOMETRY: {g['verdict']} at T+{k}  [{name}]  n={g['n']}  median gross "
                f"{_num(g['median_gross_pct']).strip()}%  cost {g['cost_floor_pct']:.4f}%  "
                f"margin {_num(g['margin_pct']).strip()}%")
    add("")
    add("-" * 110)
    add("STEP 2 - PRIMARY CELL (full market). Returns %, net at Rs 20k and Rs 6k, excess vs the matched EW benchmark")
    add("-" * 110)
    f = doc["primary"]["funnel"]
    add("  funnel (cumulative symbol-days): " + ", ".join(f"{k} {v}" for k, v in f.items()))
    add(_HDR.format("cell"))
    out.extend(_cell_rows("primary", doc["primary"]))
    out.extend(_cpcv_lines(doc["primary"]))
    add("")
    add("-" * 110)
    add("STEP 3 - ROBUSTNESS VARIANTS (reported, never selected; the T+5 / T+10 variants are the primary rows above)")
    add("-" * 110)
    for name, block in doc["variants"].items():
        add(f"  [{name}] funnel: signals {block['funnel']['signals']}, measured {block['funnel']['measured']}")
        add(_HDR.format("cell"))
        out.extend(_cell_rows(name, block))
        out.extend(_cpcv_lines(block))
        add("")
    add("-" * 110)
    add("STEP 4 - SPLITS (primary cell, descriptive)")
    add("-" * 110)
    for name, sp in doc["splits"].items():
        add(f"  [{name}]" + (f"  [{sp['label']}]" if sp["label"] else ""))
        add(_HDR.format("cell"))
        for key, c in sp["cells"].items():
            out.extend(_cell_rows(key, c))
        add("")
    for name, c in doc["promotion_cells"].items():
        add(f"  [{name}]" + (f"  [{c['label']}]" if c.get("label") else ""))
        add(_HDR.format("cell"))
        out.extend(_cell_rows(name, c))
        add("")
    add("-" * 110)
    add(f"STEP 5 - PROMOTION CRITERION (T+{doc['promotion']['horizon']}, primary cell, full market; all must hold)")
    add("-" * 110)
    for r in doc["promotion"]["rows"]:
        add(f"  ({r['id']}) {'PASS' if r['pass'] else 'FAIL'}  {r['criterion']}  ->  {r['value']}")
    add("")
    if not m["registered_population"]:
        add("  (population overridden: this verdict is NOT the registered result)")
    add(f"VERDICT: {doc['promotion']['verdict']}")
    add("")
    add("-" * 110)
    add("NOTES")
    add("-" * 110)
    cov = m["corp_actions_coverage"]
    add(f"  * structural corp_actions rows {cov['structural_rows']}, ex_date {cov['structural_ex_date_min']} -> "
        f"{cov['structural_ex_date_max']}: the veto reaches only as far as these rows do.")
    add(f"  * membership seeds {m['membership_seeds']}: NIFTY 500 {m['nifty500_size']}, Total Market "
        f"{m['total_market_size']}, NIFTY 500 names absent from Total Market {m['nifty500_not_in_total_market']}")
    for n in doc["notes"]:
        add(f"  * {n}")
    return "\n".join(out) + "\n"


def _console(text: str) -> str:
    return rc.ascii_text(text.replace("§", "section "))


# =============================================================================== CLI
def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="backtest_hvol", description="hvol pre-registered backtest (N=1, no sweep)")
    ap.add_argument("--db", type=Path, default=repo_root() / DEFAULT_DB_REL, help="research snapshot (never the live store)")
    ap.add_argument("--notional", type=Decimal, default=REFERENCE_NOTIONAL, help="reference notional for the round-trip cost")
    ap.add_argument("--symbols", default=None, help="comma-separated subset (smoke runs only)")
    ap.add_argument("--max-symbols", type=int, default=None, help="cap the symbol count (smoke runs only)")
    ap.add_argument("--out-dir", type=Path, default=repo_root() / "data" / "reports", help="report directory")
    ap.add_argument("--etf-list", type=Path, default=repo_root() / DEFAULT_ETF_LIST_REL,
                    help="NSE ETF list CSV (Amendment A1); missing or unparseable refuses")
    return ap


def main(argv: list[str] | None = None, *, now: datetime | None = None) -> int:
    args = build_parser().parse_args(argv)
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()] if args.symbols else None
    try:
        if reason := rc.market_hours_refusal(now):
            raise rc.Refusal(reason)
        etf = load_etf_list(args.etf_list)
        conn = rc.open_snapshot(args.db)
        try:
            doc = run_study(conn, db_path=Path(args.db), etf=etf, notional=Decimal(args.notional),
                            symbols=symbols, max_symbols=args.max_symbols)
        finally:
            conn.close()
    except rc.Refusal as exc:
        print(f"backtest_hvol: REFUSED: {rc.ascii_text(str(exc))}", file=sys.stderr)
        return 2
    doc["meta"]["runtime_seconds"] = round(time.monotonic() - doc.pop("_runtime_t0"), 1)
    stamp = datetime.now(IST).date().isoformat()     # the RUN date; ``now`` only feeds the hours check
    out_json = Path(args.out_dir) / f"backtest_hvol_{stamp}.json"
    text = render_text(doc)
    md = rc.write_reports(doc, f"# hvol pre-registered backtest ({REGISTERED_ON})\n\n```\n{text}```\n", out_json)
    print(_console(text))
    print(f"JSON -> {out_json}\nMD   -> {md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
