#!/usr/bin/env python
"""Factor portfolio study (``edge/factor_portfolio``, registered 2026-11-12): does a ranked, diversified long-only book
beat the equal-weight return of its liquid universe after costs?

This docstring, the constants and the report's notes are the record of the registered rules.

* **Sessions.** The dates on which NIFTY 50 has a bar. Bars on other dates are dropped (counted). A session on which a
  stock has no bar contributes nothing to it: values carry and its next return runs from its last bar.
* **Clean daily return.** ``close / close(the stock's previous bar) - 1``, set to 0 and counted when a structural
  ex-date (``hi52.UNADJUSTED_KINDS``) falls after the previous bar and on or before the session (the stored series is
  not re-adjusted), or, failing that, when its absolute value is above 35% (an unrecorded action or a bad row).
* **Formation and trading.** Formation ``f`` is the last session of each calendar month whose next session ``r`` is in
  a later month; the first is the first month-end with 253 sessions of history. Every input is known at ``f``'s
  close; trades fill at ``open(r)``. A period runs from ``open(r_k)`` to ``open(r_k+1)``; formations from
  ``FORWARD_FROM`` are the forward sample, earlier ones the history the verdicts read.
* **Universe at f.** A bar on ``f``, bars on at least 200 of the 252 sessions to ``f``, a bar on or before session
  ``f - 252``, not an ETF (the hvol study's ``is_etf``), no ``-`` series suffix, no index row (a space in the
  symbol); the ``UNIVERSE_SIZE`` with the highest median ``close x volume`` over the 126 sessions to ``f`` (descending,
  symbol ascending on ties).
* **Scores (clean returns).** sigma = sample std of the returns over the 252 sessions to ``f`` x sqrt(252); momentum =
  compounded return over sessions ``f-251 .. f-21`` / sigma; low volatility = -sigma; composite = mean of the two
  scores' cross-sectional z-scores in the universe, each clipped to +-3. Rank descending, symbol ascending on ties; a
  non-finite score is unranked.
* **Book.** Rs 120,000, 15 names, hold band 30: at ``r`` a holding that left the universe, is unranked or ranks worse
  than 30 is sold; names ranked 1..30 not held are bought in rank order until 15 are held, each at ``cash / buys
  needed`` (fixed before buying) in whole shares at ``open(r)``; a name without a bar on ``r`` or priced above its
  allocation is skipped. A sale without a bar on ``r`` fills at the stock's next open; after 5 sessions without a bar
  it closes at its last value. Position value follows clean returns; a sale's overnight leg under the ex-date or 35%
  rule fills at the last value.
* **Costs and equity.** Equity is cash plus every position's value less ``CostModel.round_trip(value, "CNC")``; a sale
  credits value less its round trip. A period's net return is the change in that equity measured at ``open(r)``
  before trades.
* **Benchmark.** The equal-weight clean-return path of the universe names with a bar on ``r_k`` from ``open(r_k)`` to
  ``open(r_k+1)``, no costs. e = net - benchmark.
* **Verdict per construct** (N = 3, threshold ``registration_alpha(3)``): supported iff >= 30 periods, mean and median
  e > 0, mean net > 0, a circular moving-block bootstrap (blocks of 3, 10,000 draws) puts fewer than alpha of its means
  at or below 0, mean e > 0 in both halves and with the 3 best periods removed; refuted iff >= 30 periods and mean
  e <= 0; insufficient below 30; otherwise null.

Read-only against a research snapshot; refuses the live store, and the full run refuses market hours. Exit codes: 0 ran,
2 refused.
"""

from __future__ import annotations

import argparse
import math
import sys
import time
import warnings
from collections.abc import Callable, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
import research_common as rc  # noqa: E402,I001  imports engine first (native import-order guard)
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from engine.core.clock import IST  # noqa: E402
from engine.core.config import repo_root  # noqa: E402
from engine.learning.multiplicity import registration_alpha  # noqa: E402
from engine.strategy.cost_model import CostModel  # noqa: E402

bb = rc.bb
hv = rc._load_script("backtest_hvol")

# =============================================================================== registered constants
REGISTERED_ON = "2026-11-12"
FAMILY = "edge/factor_portfolio"
FAMILY_N = 3
ALPHA = registration_alpha(FAMILY_N)
DEFAULT_DB_REL = Path("data") / "research" / "full_market_2026-11-09.duckdb"
INDEX_SYMBOL = "NIFTY 50"
INDEX_ETF_SYMBOL = "NIFTYBEES"
JUMP_LIMIT = 0.35
WINDOW = 252                 # sessions of history to f: sigma, the bar count, the 52-week high
MIN_BARS = 200
LIQ_SESSIONS = 126
MOM_SKIP = 21                # momentum ends at f - 21
MOM12_FROM = 251             # returns f-251 .. f-21 = close(f-252) -> close(f-21)
MOM6_FROM = 125
MOM20_SESSIONS = 20
TREND_SESSIONS = 200
UNIVERSE_SIZE = 200
DIAG_UNIVERSE_SIZE = 500
N_NAMES = 15
DIAG_N: tuple[int, ...] = (10, 20, 30)
CAPITAL = 120_000.0
PRODUCT = "CNC"
DEFERRED_SALE_SESSIONS = 5
ANNUAL_SESSIONS = 252
Z_CLIP = 3.0
MIN_PERIODS = 30
BOOT_BLOCK = 3
BOOT_DRAWS = 10_000
SEED = 20261112
TRIM_BEST = 3
RANDOM_DRAWS = 2_000
FORWARD_FROM = date(2026, 11, 30)
FORWARD_MIN_PERIODS = 24
THIN_SESSION_STOCKS = 100

REGISTERED: tuple[str, ...] = ("momentum", "lowvol", "composite")
SUPPORTED, REFUTED, INSUFFICIENT, NULL = "supported", "refuted", "insufficient", "null"
HISTORY, FORWARD = "history", "forward"
DECIDES_NOTHING = "diagnostic: decides nothing"


@contextmanager
def _quiet():
    with np.errstate(all="ignore"), warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        yield


# =============================================================================== the panel
@dataclass
class Panel:
    """Sessions x symbols. Columns are in symbol order, so a column index tie-break is a symbol tie-break.

    ``R``: clean return (NaN without a bar or a previous bar). ``ON``: ``open / close(previous bar)`` and ``ER``:
    ``close / open`` on the session, both 1.0 where the ex-date or 35% rule fires (``on_zeroed``, ``er_zeroed``)."""

    dates: list[date]
    symbols: list[str]
    col: dict[str, int]
    present: np.ndarray
    open: np.ndarray
    high: np.ndarray
    close: np.ndarray
    volume: np.ndarray
    R: np.ndarray
    ON: np.ndarray
    on_zeroed: np.ndarray
    ER: np.ndarray
    er_zeroed: np.ndarray
    first_bar: np.ndarray
    struct_cum: np.ndarray       # structural ex-dates on or before each session
    stock: np.ndarray            # name rules: not an index row, not an ETF, no series suffix
    name_rule: dict[str, np.ndarray]
    counts: dict[str, int]
    off_calendar_inside: list[date] = field(default_factory=list)   # bar dates without a NIFTY 50 bar

    @property
    def n_sessions(self) -> int:
        return len(self.dates)


def load_bars(conn, as_of: date | None) -> pd.DataFrame:
    sql = ('SELECT symbol, d, CAST("open" AS DOUBLE) AS open, CAST(high AS DOUBLE) AS high, '
           'CAST("close" AS DOUBLE) AS close, CAST(volume AS DOUBLE) AS volume FROM bars_1d')
    args: list[Any] = []
    if as_of is not None:
        sql += " WHERE d <= ?"
        args.append(as_of)
    return conn.execute(sql, args).df()


def build_panel(bars: pd.DataFrame, structural: Mapping[str, Sequence[date]],
                etf_symbols: frozenset[str] | set[str]) -> Panel:
    """``bars``: one row per (symbol, d) with open, high, close, volume."""
    if bars.empty:
        raise rc.Refusal("bars_1d is empty")
    dvals = pd.to_datetime(bars["d"]).to_numpy().astype("datetime64[D]")
    syms = bars["symbol"].astype(str).to_numpy()
    sessions = np.unique(dvals[syms == INDEX_SYMBOL])
    if sessions.size == 0:
        raise rc.Refusal(f"no {INDEX_SYMBOL} bars: no session calendar")
    S = sessions.size
    pos = np.searchsorted(sessions, dvals)
    on_cal = (pos < S) & (sessions[np.minimum(pos, S - 1)] == dvals)
    off = np.unique(dvals[~on_cal])
    inside = off[(off > sessions[0]) & (off < sessions[-1])]
    counts = {"off_calendar_rows": int((~on_cal).sum()), "off_calendar_dates": int(off.size),
              "off_calendar_dates_inside_range": int(inside.size)}
    pos, syms = pos[on_cal], syms[on_cal]
    symbols, cols = np.unique(syms, return_inverse=True)
    shape = (S, symbols.size)

    def grid(name: str) -> np.ndarray:
        out = np.full(shape, np.nan)
        out[pos, cols] = bars[name].to_numpy(dtype="float64")[on_cal]
        return out

    o, h, c, v = grid("open"), grid("high"), grid("close"), grid("volume")
    with _quiet():
        has_row = np.zeros(shape, dtype=bool)
        has_row[pos, cols] = True
        present = has_row & np.isfinite(o) & np.isfinite(c) & (o > 0) & (c > 0)
    counts["unusable_rows"] = int((has_row & ~present).sum())
    for a in (o, h, c, v):
        a[~present] = np.nan

    sidx = np.arange(S)[:, None]
    last = np.maximum.accumulate(np.where(present, sidx, -1), axis=0)
    prev = np.vstack([np.full((1, shape[1]), -1), last[:-1]])
    has_prev = present & (prev >= 0)
    prev0 = np.maximum(prev, 0)
    first_bar = np.where(present.any(axis=0), present.argmax(axis=0), S)

    col = {str(s): j for j, s in enumerate(symbols)}
    struct_cum = np.zeros(shape, dtype=np.int32)
    for sym, exd in structural.items():
        j = col.get(str(sym))
        if j is not None and exd:
            struct_cum[:, j] = np.searchsorted(np.array(sorted(exd), dtype="datetime64[D]"), sessions, side="right")
    struct_hit = has_prev & ((struct_cum - np.take_along_axis(struct_cum, prev0, axis=0)) > 0)

    prev_close = np.take_along_axis(c, prev0, axis=0)
    prev_close[~has_prev] = np.nan
    with _quiet():
        raw = c / prev_close - 1.0
        jump = has_prev & ~struct_hit & (np.abs(raw) > JUMP_LIMIT)
        R = raw.copy()
        R[struct_hit | jump] = 0.0
        ON = o / prev_close
        on_zeroed = has_prev & (struct_hit | (np.abs(ON - 1.0) > JUMP_LIMIT))
        ON[on_zeroed] = 1.0
        ER = c / o
        er_zeroed = present & (np.abs(ER - 1.0) > JUMP_LIMIT)
        ER[er_zeroed] = 1.0

    rules = {
        "index_row": np.array([" " in s for s in symbols], dtype=bool),
        "etf": np.array([hv.is_etf(s, etf_symbols) for s in symbols], dtype=bool),
        "series_suffix": np.array(["-" in s for s in symbols], dtype=bool),
    }
    stock = ~(rules["index_row"] | rules["etf"] | rules["series_suffix"])
    counts["zeroed_structural"] = int(struct_hit[:, stock].sum())
    counts["zeroed_jump"] = int(jump[:, stock].sum())
    dates = [d.astype(object) for d in sessions]
    return Panel(dates, [str(s) for s in symbols], col, present, o, h, c, v, R, ON, on_zeroed, ER, er_zeroed,
                 first_bar, struct_cum, stock, rules, counts, [d.astype(object) for d in inside])


# =============================================================================== formations + universe
def formation_indices(panel: Panel) -> list[int]:
    """Month-end sessions with 253 sessions of history whose next session is in a later month."""
    d = panel.dates
    return [f for f in range(WINDOW, len(d) - 1) if (d[f + 1].year, d[f + 1].month) != (d[f].year, d[f].month)]


def universe_at(panel: Panel, f: int, size: int) -> tuple[np.ndarray, dict[str, int]]:
    """``(columns by liquidity rank, top ``size``, funnel)``; the funnel counts each rule's exclusions in order."""
    p = panel.present
    funnel: dict[str, int] = {"symbols": len(panel.symbols)}
    alive = np.ones(len(panel.symbols), dtype=bool)
    for name, out in (*panel.name_rule.items(), ("no_bar_on_f", ~p[f]),
                      ("under_200_bars_in_252", p[f - WINDOW + 1:f + 1].sum(axis=0) < MIN_BARS),
                      ("first_bar_after_f_minus_252", panel.first_bar > f - WINDOW)):
        funnel[name] = int((alive & out).sum())
        alive &= ~out
    cols = np.flatnonzero(alive)
    with _quiet():
        liq = np.nanmedian(panel.close[f - LIQ_SESSIONS + 1:f + 1, cols] * panel.volume[f - LIQ_SESSIONS + 1:f + 1, cols],
                           axis=0)
    ok = np.isfinite(liq)
    funnel["no_liquidity"] = int((~ok).sum())
    cols, liq = cols[ok], liq[ok]
    order = cols[np.argsort(-liq, kind="stable")]
    funnel["eligible"] = int(order.size)
    return order[:size], funnel


# =============================================================================== scores
def compounded(panel: Panel, a: int, b: int, cols: np.ndarray) -> np.ndarray:
    """``prod(1 + r) - 1`` over sessions ``a .. b``; sessions without a return contribute nothing."""
    with _quiet():
        return np.nanprod(1.0 + panel.R[a:b + 1][:, cols], axis=0) - 1.0


def sigma(panel: Panel, f: int, cols: np.ndarray) -> np.ndarray:
    w = panel.R[f - WINDOW + 1:f + 1][:, cols]
    with _quiet():
        sd = np.nanstd(w, axis=0, ddof=1) * math.sqrt(ANNUAL_SESSIONS)
    sd[(np.isfinite(w).sum(axis=0) < 2) | ~(sd > 0)] = np.nan
    return sd


def zscore(x: np.ndarray) -> np.ndarray:
    """Cross-sectional z (population std) over the finite values, clipped to +-``Z_CLIP``; NaN elsewhere."""
    out = np.full(x.shape, np.nan)
    fin = np.isfinite(x)
    if fin.sum() < 2:
        return out
    sd = float(x[fin].std())
    if not sd > 0:
        return out
    out[fin] = np.clip((x[fin] - x[fin].mean()) / sd, -Z_CLIP, Z_CLIP)
    return out


def scores(panel: Panel, f: int, cols: np.ndarray, construct: str) -> np.ndarray:
    """Scores at ``f`` from bars on or before ``f``; NaN = unranked."""
    cols = np.asarray(cols, dtype=np.int64)
    with _quiet():
        if construct == "mom20":
            return compounded(panel, f - MOM20_SESSIONS + 1, f, cols)
        if construct == "mom12_1_raw":
            return compounded(panel, f - MOM12_FROM, f - MOM_SKIP, cols)
        if construct == "hi52":
            hi = np.nanmax(panel.high[f - WINDOW + 1:f + 1][:, cols], axis=0)
            out = panel.close[f, cols] / hi
            out[(panel.struct_cum[f, cols] - panel.struct_cum[f - WINDOW, cols]) > 0] = np.nan
            return out
        sd = sigma(panel, f, cols)
        mom = compounded(panel, f - MOM12_FROM, f - MOM_SKIP, cols) / sd
        if construct == "momentum":
            return mom
        if construct == "lowvol":
            return -sd
        if construct == "composite":
            return (zscore(mom) + zscore(-sd)) / 2.0
        if construct == "mom6_1":
            return compounded(panel, f - MOM6_FROM, f - MOM_SKIP, cols) / sd
    raise ValueError(f"unknown construct {construct!r}")


def rank_order(cols: np.ndarray, sc: np.ndarray) -> np.ndarray:
    """Columns with a finite score, descending score, column (symbol) ascending on ties."""
    cols = np.asarray(cols, dtype=np.int64)
    fin = np.isfinite(sc)
    c, s = cols[fin], sc[fin]
    return c[np.lexsort((c, -s))]


def trend_on(panel: Panel, f: int) -> bool:
    """NIFTY 50's close at ``f`` above the mean of its last 200 closes; fewer than 200 closes = invested."""
    j = panel.col.get(INDEX_SYMBOL)
    if j is None:
        return True
    c = panel.close[:f + 1, j]
    c = c[np.isfinite(c)]
    if c.size < TREND_SESSIONS:
        return True
    return bool(c[-1] > c[-TREND_SESSIONS:].mean())


@dataclass(frozen=True)
class Formation:
    f: int
    r: int
    universe: np.ndarray        # columns, liquidity order
    ranked: np.ndarray          # columns with a finite score, rank order
    invest: bool = True


def build_formations(panel: Panel, universes: Mapping[int, np.ndarray], construct: str, *,
                     overlay: bool = False) -> list[Formation]:
    return [Formation(f, f + 1, u, rank_order(u, scores(panel, f, u, construct)),
                      trend_on(panel, f) if overlay else True) for f, u in sorted(universes.items())]


# =============================================================================== the book
def round_trip_cost(cm: CostModel, value: float) -> float:
    """``CostModel.round_trip`` (CNC) on ``value`` rounded to the paisa; 0 for a non-positive value."""
    if not math.isfinite(value) or value <= 0:
        return 0.0
    notional = Decimal(f"{value:.2f}")
    return float(cm.round_trip(notional, PRODUCT).total_cost) if notional > 0 else 0.0


@dataclass
class Position:
    qty: int
    value: float
    entry: int
    pending: bool = False
    pending_sessions: int = 0


@dataclass
class SimResult:
    start: int
    equity_before: list[float]          # per formation, at open(r) before trades
    embedded_cost: list[float]          # round trips inside that equity
    buys: list[int]
    sells: list[int]
    held_after: list[int]
    close_equity: np.ndarray            # per session from ``start``; NaN before
    cash: np.ndarray
    realized_cost: np.ndarray
    trades: list[tuple[int, int, str, float, float]] = field(default_factory=list)
    counts: dict[str, int] = field(default_factory=dict)


def _bump(counts: dict[str, int], key: str, by: int = 1) -> None:
    counts[key] = counts.get(key, 0) + by


def simulate(panel: Panel, formations: Sequence[Formation], *, n: int, cost: Callable[[float], float],
             capital: float = CAPITAL) -> SimResult:
    """The registered book over ``formations`` from the first ``r`` to the last session.

    ``trades``: ``(session, column, "buy", qty, open)`` and ``(session, column, "sell", value credited before cost,
    cost)``."""
    S = panel.n_sessions
    band = 2 * n
    start = formations[0].r if formations else S
    by_r = {fm.r: k for k, fm in enumerate(formations)}
    K = len(formations)
    res = SimResult(start, [math.nan] * K, [0.0] * K, [0] * K, [0] * K, [0] * K,
                    np.full(S, np.nan), np.full(S, np.nan), np.zeros(S))
    counts = res.counts
    for key in ("skipped_buy_price", "skipped_buy_no_bar", "deferred_sales", "deferred_sales_filled",
                "deferred_sales_closed_at_last_value", "sales_at_previous_value", "entry_ratio_zeroed",
                "short_book_formations", "short_book_names", "sold_left_universe", "sold_unranked",
                "sold_out_of_band", "sold_trend_overlay"):
        counts[key] = 0
    cash = float(capital)
    pos: dict[int, Position] = {}
    present, ON, onz = panel.present, panel.ON, panel.on_zeroed

    def sell(s: int, j: int, v: float) -> None:
        nonlocal cash
        c = cost(v)
        cash += v - c
        res.realized_cost[s] += c
        res.trades.append((s, j, "sell", v, c))
        del pos[j]

    for s in range(start, S):
        k = by_r.get(s)
        if k is not None:
            eq, emb = cash, 0.0
            for j, p in pos.items():
                v = p.value * ON[s, j] if present[s, j] else p.value
                c = cost(v)
                eq += v - c
                emb += c
            res.equity_before[k], res.embedded_cost[k] = eq, emb
        for j in [j for j, p in pos.items() if p.pending]:
            p = pos[j]
            if present[s, j]:
                if onz[s, j]:
                    _bump(counts, "sales_at_previous_value")
                _bump(counts, "deferred_sales_filled")
                sell(s, j, p.value * ON[s, j])
            else:
                p.pending_sessions += 1
                if p.pending_sessions >= DEFERRED_SALE_SESSIONS:
                    _bump(counts, "deferred_sales_closed_at_last_value")
                    sell(s, j, p.value)
        if k is not None:
            fm = formations[k]
            rank = {int(c): i + 1 for i, c in enumerate(fm.ranked)}
            univ = set(fm.universe.tolist())
            for j in [j for j, p in pos.items() if not p.pending]:
                rk = rank.get(j)
                if not fm.invest:
                    reason = "sold_trend_overlay"
                elif j not in univ:
                    reason = "sold_left_universe"
                elif rk is None:
                    reason = "sold_unranked"
                elif rk > band:
                    reason = "sold_out_of_band"
                else:
                    continue
                _bump(counts, reason)
                res.sells[k] += 1
                if present[s, j]:
                    if onz[s, j]:
                        _bump(counts, "sales_at_previous_value")
                    sell(s, j, pos[j].value * ON[s, j])
                else:
                    _bump(counts, "deferred_sales")
                    pos[j].pending, pos[j].pending_sessions = True, 1
            need = n - sum(not p.pending for p in pos.values())
            if fm.invest and need > 0:
                alloc = cash / need
                bought = 0
                for c in fm.ranked[:band]:
                    if bought == need:
                        break
                    j = int(c)
                    if j in pos:
                        continue
                    if not present[s, j]:
                        _bump(counts, "skipped_buy_no_bar")
                        continue
                    px = float(panel.open[s, j])
                    qty = math.floor(alloc / px)
                    if qty < 1:
                        _bump(counts, "skipped_buy_price")
                        continue
                    cash -= qty * px
                    pos[j] = Position(qty, qty * px, s)
                    res.trades.append((s, j, "buy", qty, px))
                    bought += 1
                res.buys[k] = bought
                if bought < need:
                    _bump(counts, "short_book_formations")
                    _bump(counts, "short_book_names", need - bought)
            res.held_after[k] = sum(not p.pending for p in pos.values())
        eq = cash
        for j, p in pos.items():
            if present[s, j]:
                if p.entry == s:
                    if panel.er_zeroed[s, j]:
                        _bump(counts, "entry_ratio_zeroed")
                    p.value *= panel.ER[s, j]
                else:
                    p.value *= 1.0 + panel.R[s, j]
            eq += p.value - cost(p.value)
        res.close_equity[s], res.cash[s] = eq, cash
    return res


# =============================================================================== benchmark paths
def path_growth(panel: Panel, a: int, b: int, cols: np.ndarray) -> np.ndarray:
    """Clean-return path ``open(a) -> open(b)`` - 1 of columns with a bar on ``a``; no bar on ``b`` = the last
    value."""
    cols = np.asarray(cols, dtype=np.int64)
    with _quiet():
        g = panel.ER[a, cols] * np.nanprod(1.0 + panel.R[a + 1:b][:, cols], axis=0)
        g = g * np.where(panel.present[b, cols], panel.ON[b, cols], 1.0)
    return g - 1.0


def benchmark(panel: Panel, fm: Formation, b: int) -> tuple[float | None, int]:
    cols = fm.universe[panel.present[fm.r, fm.universe]]
    if cols.size == 0:
        return None, 0
    return float(path_growth(panel, fm.r, b, cols).mean()), int(cols.size)


def single_path(panel: Panel, symbol: str, a: int, b: int) -> float | None:
    j = panel.col.get(symbol)
    if j is None or not panel.present[a, j]:
        return None
    return float(path_growth(panel, a, b, np.array([j]))[0])


def random_percentile(panel: Panel, fm: Formation, b: int, n: int, rng: np.random.Generator) -> float | None:
    """Share of ``RANDOM_DRAWS`` equal-weight n-name draws (universe names with a bar on ``r``) whose gross
    return is <= that of the ``n`` top-ranked names with a bar on ``r``."""
    cand = fm.universe[panel.present[fm.r, fm.universe]]
    top = np.array([c for c in fm.ranked if panel.present[fm.r, c]][:n], dtype=np.int64)
    if cand.size < n or top.size < n:
        return None
    g = path_growth(panel, fm.r, b, cand)
    top_ret = float(path_growth(panel, fm.r, b, top).mean())
    pick = np.argpartition(rng.random((RANDOM_DRAWS, cand.size)), n - 1, axis=1)[:, :n]
    return float((g[pick].mean(axis=1) <= top_ret).mean())


# =============================================================================== statistics + verdict
def bootstrap_p(e: Sequence[float], *, block: int = BOOT_BLOCK, draws: int = BOOT_DRAWS,
                seed: int = SEED) -> float | None:
    """Share of circular moving-block bootstrap means at or below 0. Each draw takes ceil(K / block) uniform block
    starts, ``block`` periods from each (wrapping), truncated to K."""
    x = np.asarray(e, dtype="float64")
    K = x.size
    if K == 0:
        return None
    blk = min(block, K)
    nb = math.ceil(K / blk)
    rng = np.random.default_rng(seed)
    starts = rng.integers(0, K, (draws, nb))
    idx = ((starts[:, :, None] + np.arange(blk)[None, None, :]) % K).reshape(draws, nb * blk)[:, :K]
    return float((x[idx].mean(axis=1) <= 0.0).mean())


def _mean(x: Sequence[float]) -> float | None:
    return float(np.mean(x)) if len(x) else None


def _median(x: Sequence[float]) -> float | None:
    return float(np.median(x)) if len(x) else None


def _pos(x: float | None) -> bool:
    return x is not None and math.isfinite(x) and x > 0.0


def verdict(e: Sequence[float], net: Sequence[float], *, seed: int = SEED) -> dict[str, Any]:
    """The registered criteria on the history sample, unrounded. Halves split at ``K // 2`` (an odd middle period is
    in the second half)."""
    e = [float(v) for v in e]
    K = len(e)
    h = K // 2
    trimmed = sorted(e)[:-TRIM_BEST] if K > TRIM_BEST else []
    p = bootstrap_p(e, seed=seed)
    vals = {
        "periods": K, "mean_e": _mean(e), "median_e": _median(e), "mean_net": _mean(net), "bootstrap_p": p,
        "alpha": ALPHA, "mean_e_first_half": _mean(e[:h]), "mean_e_second_half": _mean(e[h:]),
        "mean_e_without_best_3": _mean(trimmed),
    }
    rows = [
        (1, f"periods >= {MIN_PERIODS}", K >= MIN_PERIODS),
        (2, "mean e > 0", _pos(vals["mean_e"])),
        (3, "median e > 0", _pos(vals["median_e"])),
        (4, "mean net portfolio return > 0", _pos(vals["mean_net"])),
        (5, f"bootstrap share of means <= 0 below {ALPHA:.6f}", p is not None and p < ALPHA),
        (6, "mean e > 0 in both halves", _pos(vals["mean_e_first_half"]) and _pos(vals["mean_e_second_half"])),
        (7, f"mean e > 0 with the {TRIM_BEST} best periods removed", _pos(vals["mean_e_without_best_3"])),
    ]
    out = [{"id": i, "criterion": c, "pass": bool(ok)} for i, c, ok in rows]
    if K < MIN_PERIODS:
        label = INSUFFICIENT
    elif all(r["pass"] for r in out):
        label = SUPPORTED
    elif vals["mean_e"] is not None and vals["mean_e"] <= 0.0:
        label = REFUTED
    else:
        label = NULL
    return {"label": label, "values": vals, "criteria": out}


def _compound(x: Sequence[float]) -> float:
    return float(np.prod(1.0 + np.asarray(x, dtype="float64")) - 1.0)


# =============================================================================== one construct
@dataclass
class Run:
    name: str
    formations: list[Formation]
    sim: SimResult
    periods: list[dict[str, Any]]


def period_rows(panel: Panel, formations: Sequence[Formation], sim: SimResult,
                bench: Mapping[int, tuple[float | None, int]]) -> list[dict[str, Any]]:
    """One row per period ``r_k -> r_k+1``. ``trend_benchmark`` is the benchmark under the trend overlay (0 when
    NIFTY 50 is at or below its 200-session mean at ``f``)."""
    rows = []
    for k in range(len(formations) - 1):
        fm, b = formations[k], formations[k + 1].r
        net = sim.equity_before[k + 1] / sim.equity_before[k] - 1.0
        bm, bn = bench[fm.f]
        trend = trend_on(panel, fm.f)
        eq_r = sim.close_equity[fm.r]
        rows.append({
            "k": k, "formation": panel.dates[fm.f], "trade": panel.dates[fm.r], "next_trade": panel.dates[b],
            "sample": FORWARD if panel.dates[fm.f] >= FORWARD_FROM else HISTORY,
            "invested": fm.invest, "universe_n": int(fm.universe.size), "ranked_n": int(fm.ranked.size),
            "bench_n": bn, "equity_before": sim.equity_before[k], "equity_next": sim.equity_before[k + 1],
            "net": net, "benchmark": bm, "e": None if bm is None else net - bm, "trend_on": trend,
            "trend_benchmark": None if bm is None else (bm if trend else 0.0),
            "nifty50": single_path(panel, INDEX_SYMBOL, fm.r, b),
            "niftybees": single_path(panel, INDEX_ETF_SYMBOL, fm.r, b),
            "buys": sim.buys[k], "sells": sim.sells[k], "held_after": sim.held_after[k],
            "cash_share_close_r": float(sim.cash[fm.r] / eq_r) if eq_r > 0 else None,
        })
    return rows


def run_construct(panel: Panel, universes: Mapping[int, np.ndarray], bench: Mapping[int, tuple[float | None, int]],
                  construct: str, *, n: int, cost: Callable[[float], float], overlay: bool = False,
                  name: str | None = None) -> Run:
    fms = build_formations(panel, universes, construct, overlay=overlay)
    sim = simulate(panel, fms, n=n, cost=cost)
    return Run(name or construct, fms, sim, period_rows(panel, fms, sim, bench))


def summary(panel: Panel, run: Run, sample: str = HISTORY, *,
            bench_key: str = "benchmark") -> dict[str, Any]:
    """Descriptive statistics of ``run`` over the periods of ``sample`` with a benchmark."""
    rows = [p for p in run.periods if p["sample"] == sample and p[bench_key] is not None]
    out: dict[str, Any] = {"periods": len(rows), "periods_without_benchmark":
                           sum(p["sample"] == sample and p[bench_key] is None for p in run.periods)}
    if not rows:
        return out
    net = [p["net"] for p in rows]
    bm = [p[bench_key] for p in rows]
    e = [a - b for a, b in zip(net, bm, strict=True)]
    k0, k1 = rows[0]["k"], rows[-1]["k"] + 1
    sim, fms = run.sim, run.formations
    s0, s1 = fms[k0].r, fms[k1].r
    path = np.concatenate([[sim.equity_before[k0]], sim.close_equity[s0:s1], [sim.equity_before[k1]]])
    daily = path[1:] / path[:-1] - 1.0
    years = (s1 - s0) / ANNUAL_SESSIONS
    costs = float(sim.realized_cost[s0:s1].sum()) + sim.embedded_cost[k1] - sim.embedded_cost[k0]
    replaced = [sim.buys[p["k"]] for p in rows if p["k"] > 0]
    by_year: dict[int, list[dict[str, Any]]] = {}
    for p in rows:
        by_year.setdefault(p["trade"].year, []).append(p)
    out.update({
        "first_trade": rows[0]["trade"], "last_period_end": rows[-1]["next_trade"],
        "mean_net": _mean(net), "median_net": _median(net), "mean_benchmark": _mean(bm),
        "mean_e": _mean(e), "median_e": _median(e), "bootstrap_p": bootstrap_p(e),
        "compounded_net": _compound(net), "compounded_benchmark": _compound(bm),
        "annualised_return": float((path[-1] / path[0]) ** (1.0 / years) - 1.0) if years > 0 else None,
        "annualised_volatility": float(np.std(daily, ddof=1) * math.sqrt(ANNUAL_SESSIONS)) if daily.size > 1 else None,
        "worst_drawdown": float((path / np.maximum.accumulate(path) - 1.0).min()),
        "mean_names_replaced": _mean(replaced),
        "cost_share_of_mean_equity_per_year": float(costs / path.mean() / years) if years > 0 else None,
        "mean_cash_share": float(np.mean(sim.cash[s0:s1] / sim.close_equity[s0:s1])),
        "by_year": {str(y): {"periods": len(ps), "net": _compound([p["net"] for p in ps]),
                             "benchmark": _compound([p[bench_key] for p in ps])} for y, ps in sorted(by_year.items())},
    })
    return out


def forward_block(panel: Panel, run: Run) -> dict[str, Any]:
    s = summary(panel, run, FORWARD)
    return {"label": None, "note": f"no label before {FORWARD_MIN_PERIODS} forward periods", **{
        k: s.get(k) for k in ("periods", "mean_net", "mean_benchmark", "mean_e", "median_e", "compounded_net",
                              "compounded_benchmark")}}


# =============================================================================== the study
def benchmarks(panel: Panel, universes: Mapping[int, np.ndarray]) -> dict[int, tuple[float | None, int]]:
    fs = sorted(universes)
    out: dict[int, tuple[float | None, int]] = {}
    for f, f_next in zip(fs, fs[1:], strict=False):
        out[f] = benchmark(panel, Formation(f, f + 1, universes[f], np.array([], dtype=np.int64)), f_next + 1)
    if fs:
        out[fs[-1]] = (None, 0)
    return out


def counts_doc(panel: Panel) -> dict[str, Any]:
    """Outcome-free facts only: no return, score or portfolio is computed."""
    fs = formation_indices(panel)
    per = []
    funnel: dict[str, int] = {}
    for f in fs:
        u, funnel = universe_at(panel, f, UNIVERSE_SIZE)
        per.append({"formation": panel.dates[f], "trade": panel.dates[f + 1], "universe": int(u.size),
                    "eligible": funnel["eligible"],
                    "sample": FORWARD if panel.dates[f] >= FORWARD_FROM else HISTORY})
    stocks_per_session = panel.present[:, panel.stock].sum(axis=1)
    thin = [panel.dates[i] for i in np.flatnonzero(stocks_per_session < THIN_SESSION_STOCKS)]
    return {
        "sessions": panel.n_sessions, "first_session": panel.dates[0], "last_session": panel.dates[-1],
        "symbols": len(panel.symbols), "stock_symbols": int(panel.stock.sum()),
        "formations": per, "history_periods_possible": sum(p["sample"] == HISTORY for p in per[:-1]),
        "counters": dict(panel.counts), "off_calendar_dates_inside_range": panel.off_calendar_inside,
        "last_formation_funnel": funnel,
        "thin_sessions": {"threshold_stocks": THIN_SESSION_STOCKS, "count": len(thin), "dates": thin},
    }


def run_study(panel: Panel, *, cm: CostModel) -> dict[str, Any]:
    t0 = time.monotonic()
    fs = formation_indices(panel)
    if not fs:
        raise rc.Refusal(f"no formation: fewer than {WINDOW + 1} sessions before a month-end")

    def cost(v: float) -> float:
        return round_trip_cost(cm, v)

    uni = {size: {f: universe_at(panel, f, size)[0] for f in fs} for size in (UNIVERSE_SIZE, DIAG_UNIVERSE_SIZE)}
    bench = {size: benchmarks(panel, u) for size, u in uni.items()}
    main_runs = {c: run_construct(panel, uni[UNIVERSE_SIZE], bench[UNIVERSE_SIZE], c, n=N_NAMES, cost=cost)
                 for c in REGISTERED}
    constructs: dict[str, Any] = {}
    for c, run in main_runs.items():
        hist = [p for p in run.periods if p["sample"] == HISTORY and p["e"] is not None]
        constructs[c] = {
            "verdict": verdict([p["e"] for p in hist], [p["net"] for p in hist]),
            "summary": summary(panel, run),
            "forward": forward_block(panel, run),
            "counters": run.sim.counts,
            "periods": run.periods,
        }

    diag_runs: list[tuple[Run, dict[str, Any]]] = []
    for c in REGISTERED:
        for n in DIAG_N:
            diag_runs.append((run_construct(panel, uni[UNIVERSE_SIZE], bench[UNIVERSE_SIZE], c, n=n, cost=cost,
                                            name=f"{c} n={n}"), {"construct": c, "n": n, "universe": UNIVERSE_SIZE}))
    for c in REGISTERED:
        diag_runs.append((run_construct(panel, uni[DIAG_UNIVERSE_SIZE], bench[DIAG_UNIVERSE_SIZE], c, n=N_NAMES,
                                        cost=cost, name=f"{c} universe={DIAG_UNIVERSE_SIZE}"),
                          {"construct": c, "n": N_NAMES, "universe": DIAG_UNIVERSE_SIZE}))
    for c, label in (("mom6_1", "momentum 6-1 / sigma"), ("mom12_1_raw", "momentum 12-1 without sigma"),
                     ("mom20", "engine mom control (20-session return)"), ("hi52", "52-week-high proximity")):
        diag_runs.append((run_construct(panel, uni[UNIVERSE_SIZE], bench[UNIVERSE_SIZE], c, n=N_NAMES, cost=cost,
                                        name=label), {"construct": c, "n": N_NAMES, "universe": UNIVERSE_SIZE}))
    diagnostics: dict[str, Any] = {"label": DECIDES_NOTHING, "runs": {}}
    for run, params in diag_runs:
        diagnostics["runs"][run.name] = {**params, "summary": summary(panel, run), "counters": run.sim.counts}

    trend: dict[str, Any] = {}
    for c in REGISTERED:
        run = run_construct(panel, uni[UNIVERSE_SIZE], bench[UNIVERSE_SIZE], c, n=N_NAMES, cost=cost, overlay=True,
                            name=f"{c} trend overlay")
        trend[run.name] = {"summary": summary(panel, run),
                           "summary_vs_trend_benchmark": summary(panel, run, bench_key="trend_benchmark"),
                           "counters": run.sim.counts}
    hist_b = [p for p in main_runs[REGISTERED[0]].periods if p["sample"] == HISTORY and p["benchmark"] is not None]
    tb = [p["trend_benchmark"] for p in hist_b]
    trend["benchmark trend overlay"] = {
        "periods": len(tb), "invested_periods": sum(p["trend_on"] for p in hist_b),
        "mean": _mean(tb), "compounded": _compound(tb) if tb else None,
        "plain_mean": _mean([p["benchmark"] for p in hist_b]),
        "plain_compounded": _compound([p["benchmark"] for p in hist_b]) if hist_b else None}
    diagnostics["trend_overlay"] = trend

    pct: dict[str, Any] = {}
    for c, run in main_runs.items():
        rng = np.random.default_rng(SEED)
        vals = []
        for k, p in enumerate(run.periods):
            if p["sample"] != HISTORY:
                continue
            v = random_percentile(panel, run.formations[k], run.formations[k + 1].r, N_NAMES, rng)
            if v is not None:
                vals.append(v)
        pct[c] = {"periods": len(vals), "mean_percentile": _mean(vals), "periods_above_50": sum(v > 0.5 for v in vals)}
    diagnostics["random_draw_percentile"] = pct

    return {
        "meta": {
            "script": "scripts/factor_portfolio_study.py", "registration": f"factor portfolio, registered {REGISTERED_ON}",
            "family": FAMILY, "family_n": FAMILY_N, "alpha": ALPHA,
            "generated_at": datetime.now(IST).isoformat(timespec="seconds"),
            "sessions": {"n": panel.n_sessions, "first": panel.dates[0], "last": panel.dates[-1]},
            "formations": len(fs), "first_formation": panel.dates[fs[0]], "last_formation": panel.dates[fs[-1]],
            "params": {
                "capital_inr": CAPITAL, "names": N_NAMES, "hold_band": 2 * N_NAMES, "universe_size": UNIVERSE_SIZE,
                "window_sessions": WINDOW, "min_bars": MIN_BARS, "liquidity_sessions": LIQ_SESSIONS,
                "momentum": f"returns f-{MOM12_FROM}..f-{MOM_SKIP} / sigma", "jump_limit": JUMP_LIMIT,
                "deferred_sale_sessions": DEFERRED_SALE_SESSIONS, "product": PRODUCT, "z_clip": Z_CLIP,
                "min_periods": MIN_PERIODS, "boot_block": BOOT_BLOCK, "boot_draws": BOOT_DRAWS, "seed": SEED,
                "trim_best": TRIM_BEST, "random_draws": RANDOM_DRAWS, "forward_from": FORWARD_FROM,
                "trend_sessions": TREND_SESSIONS, "diag_n": list(DIAG_N), "diag_universe_size": DIAG_UNIVERSE_SIZE,
                "cost_spread_pct": float(cm.spread_pct),
            },
            "data_counters": dict(panel.counts),
            "runtime_seconds": round(time.monotonic() - t0, 1),
        },
        "constructs": constructs,
        "diagnostics": diagnostics,
        "notes": [
            "Verdicts read the history sample only; the forward sample carries no label before "
            f"{FORWARD_MIN_PERIODS} periods.",
            "Known optimism: fills at the open assume it is reachable; a delisted holding closes at its last value; "
            "names under surveillance are included; the spread is the cost file's single figure.",
            "Diagnostics decide nothing.",
        ],
    }


# =============================================================================== rendering
def _clean(x: Any) -> Any:
    if isinstance(x, float):
        return x if math.isfinite(x) else None
    if isinstance(x, dict):
        return {str(k): _clean(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [_clean(v) for v in x]
    if isinstance(x, np.generic):
        return _clean(x.item())
    return x


def _f(v: Any, spec: str = "+.4f") -> str:
    return "-" if v is None or (isinstance(v, float) and not math.isfinite(v)) else format(v, spec)


def render_counts(doc: Mapping[str, Any]) -> str:
    out = ["FACTOR PORTFOLIO STUDY: COUNTS ONLY (no returns, scores or portfolios computed)",
           f"sessions {doc['sessions']}  {doc['first_session']} -> {doc['last_session']}  symbols "
           f"{doc['symbols']} (stocks by name rule {doc['stock_symbols']})",
           f"data counters {doc['counters']}",
           f"bar dates without a {INDEX_SYMBOL} bar inside the session range: "
           f"{[str(d) for d in doc['off_calendar_dates_inside_range']]}",
           f"thin sessions (< {doc['thin_sessions']['threshold_stocks']} stocks with a bar): "
           f"{doc['thin_sessions']['count']} {[str(d) for d in doc['thin_sessions']['dates']]}",
           f"formations {len(doc['formations'])}; history periods possible {doc['history_periods_possible']}",
           "  formation   trade       universe  eligible  sample"]
    out += [f"  {p['formation']}  {p['trade']}  {p['universe']:>8}  {p['eligible']:>8}  {p['sample']}"
            for p in doc["formations"]]
    out.append(f"last formation funnel (each rule counted after the earlier ones): {doc['last_formation_funnel']}")
    return rc.ascii_text("\n".join(out))


def _summary_line(name: str, s: Mapping[str, Any]) -> str:
    return (f"  {name:<40} {s.get('periods', 0):>3} {_f(s.get('mean_net')):>8} {_f(s.get('mean_benchmark')):>8} "
            f"{_f(s.get('mean_e')):>8} {_f(s.get('median_e')):>8} {_f(s.get('bootstrap_p'), '.4f'):>7} "
            f"{_f(s.get('annualised_return')):>8} {_f(s.get('annualised_volatility'), '.4f'):>7} "
            f"{_f(s.get('worst_drawdown')):>8} {_f(s.get('cost_share_of_mean_equity_per_year'), '.4f'):>7} "
            f"{_f(s.get('mean_cash_share'), '.3f'):>6}")


_SUM_HDR = ("  name                                       K  mean net  mean bm   mean e  median e  boot p   ann ret  "
            "ann vol  worst dd  cost/yr  cash")


def render_text(doc: Mapping[str, Any]) -> str:
    m = doc["meta"]
    out = [f"FACTOR PORTFOLIO STUDY ({m['registration']}, {m['family']}, N={m['family_n']}, alpha {m['alpha']:.6f})",
           f"sessions {m['sessions']['n']}  {m['sessions']['first']} -> {m['sessions']['last']}  formations "
           f"{m['formations']} ({m['first_formation']} -> {m['last_formation']})",
           f"data counters {m['data_counters']}", ""]
    for c, b in doc["constructs"].items():
        v = b["verdict"]
        out.append(f"[{c}] VERDICT {v['label'].upper()}")
        out += [f"  {r['id']} {r['criterion']:<55} {'pass' if r['pass'] else 'FAIL'}" for r in v["criteria"]]
        out.append("  values " + ", ".join(f"{k} {_f(x, '.6f') if isinstance(x, float) else x}"
                                            for k, x in v["values"].items()))
        out += [_SUM_HDR, _summary_line(c, b["summary"])]
        out.append("  by year: " + "; ".join(f"{y} K={r['periods']} net {_f(r['net'])} bm {_f(r['benchmark'])}"
                                             for y, r in b["summary"].get("by_year", {}).items()))
        out.append(f"  forward: {b['forward']}")
        out.append(f"  counters {b['counters']}")
        out.append("    k  formation   trade       next        sample    net       bm        e         nifty50   "
                   "bees      buys sells held")
        out += [f"  {p['k']:>3}  {p['formation']}  {p['trade']}  {p['next_trade']}  {p['sample']:<8} "
                f"{_f(p['net']):>9} {_f(p['benchmark']):>9} {_f(p['e']):>9} {_f(p['nifty50']):>9} "
                f"{_f(p['niftybees']):>9} {p['buys']:>4} {p['sells']:>5} {p['held_after']:>4}" for p in b["periods"]]
        out.append("")
    d = doc["diagnostics"]
    out += [f"DIAGNOSTICS ({d['label']})", _SUM_HDR]
    out += [_summary_line(name, r["summary"]) for name, r in d["runs"].items()]
    for name, r in d["trend_overlay"].items():
        if "summary" in r:
            out.append(_summary_line(name, r["summary"]))
            out.append(_summary_line(name + " vs trend bm", r["summary_vs_trend_benchmark"]))
        else:
            out.append(f"  {name}: {r}")
    out.append(f"  random-draw percentile: {d['random_draw_percentile']}")
    out += [""] + list(doc["notes"])
    return rc.ascii_text("\n".join(out))


def render_markdown(doc: Mapping[str, Any]) -> str:
    return f"# Factor portfolio study\n\n```\n{render_text(doc)}\n```\n"


# =============================================================================== CLI
def _as_of(text: str) -> date:
    return date.fromisoformat(text)


def main(argv: list[str] | None = None, *, now: datetime | None = None) -> int:
    ap = argparse.ArgumentParser(prog="factor_portfolio_study", description=__doc__.splitlines()[0])
    ap.add_argument("--db", type=Path, default=repo_root() / DEFAULT_DB_REL, help="whole-market research snapshot")
    ap.add_argument("--as-of", type=_as_of, default=None, help="YYYY-MM-DD: drop bars after it")
    ap.add_argument("--etf-list", type=Path, default=repo_root() / hv.DEFAULT_ETF_LIST_REL)
    ap.add_argument("--out", type=Path, default=None, help="JSON path; the .md is written beside it")
    ap.add_argument("--counts-only", action="store_true",
                    help="outcome-free facts: sessions, formations, universe sizes, counters; nothing written")
    args = ap.parse_args(argv)
    try:
        if not args.counts_only and (reason := rc.market_hours_refusal(now)):
            raise rc.Refusal(reason)
        etf = hv.load_etf_list(args.etf_list)
        conn = rc.open_snapshot(args.db)
        try:
            bars = load_bars(conn, args.as_of)
            structural = bb.load_structural_ex_dates(conn)
        finally:
            conn.close()
        panel = build_panel(bars, structural, etf.symbols)
        del bars
        if args.counts_only:
            print(render_counts(counts_doc(panel)))
            return 0
        doc = run_study(panel, cm=CostModel.from_config())
    except rc.Refusal as exc:
        print(f"factor_portfolio_study: REFUSED: {rc.ascii_text(str(exc))}", file=sys.stderr)
        return 2
    doc["meta"].update({"db": str(args.db), "as_of": args.as_of or panel.dates[-1],
                        "etf_list": {"path": etf.path, "sha256": etf.sha256}})
    doc = _clean(doc)
    tag = (args.as_of or panel.dates[-1]).isoformat()
    out = args.out or rc.default_out(args.db, "factor_portfolio").with_name(f"factor_portfolio_{tag}.json")
    md = rc.write_reports(doc, render_markdown(doc), out)
    print(render_text(doc))
    print(f"JSON -> {out}\nMD   -> {md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
