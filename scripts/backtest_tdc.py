#!/usr/bin/env python
"""``tdc`` PRE-REGISTERED backtest harness (pre-registered 2026-09-04).

Trend-day continuation, intraday. Hypothesis (``trend_day``): on a session where a NIFTY200 stock
shows ACCEPTANCE above its opening range with sustained PARTICIPATION and RELATIVE STRENGTH by late
morning, the move continues into the close net of MIS costs.

===============================================================================================
PRE-REGISTRATION / MULTIPLICITY DISCIPLINE - READ FIRST
===============================================================================================
No parameter sweep is run here or was run before it. Exactly one :data:`PARAMS` dict and one
:data:`VARIANTS` dict, fixed at import time. Every variant is REPORTED; none is SELECTED, ranked or
promoted over another. The robustness list (``T`` in {10:30, 12:00}; ``rvol_min`` in {1.2, 2.0};
``stop`` 0.5%) shows whether the number is a knife edge — it is not a menu. The CLI exposes no flag
that changes a signal parameter; one would turn this into a grid search and invalidate every number.

``promotable`` is the pre-registration's own boolean: ``mean net % > 0`` AND ``t > 2`` AND
``n >= 200`` AND ``CPCV positive-split share >= 0.60``. Anything less is a refutation, reported in
exactly those terms; no other deployment language appears in the report.

===============================================================================================
THE RULE (transcribed from the pre-registration; every clause is evaluated at the T bar CLOSE)
===============================================================================================
For each eligible symbol on each session, at the decision bar ``T`` (default the 11:00 IST bar):

* **(i) Acceptance.** ``close(T) > VWAP(T)`` AND ``close(T) > OR high`` AND each of the 15 one-minute
  closes immediately before T (10:45..10:59 when T = 11:00) is above VWAP.

  - ``VWAP`` is the session volume-weighted average of the typical price ``(high+low+close)/3``,
    cumulative from the 09:15 bar. ``VWAP(T)`` is that cumulative value AT the T bar.
  - ``OR high`` is ``max(high)`` over the bars ``09:15..09:29`` - the first fifteen 1m bars, i.e.
    the 09:15-09:30 opening range. ``ts_minute`` is the bar's minute START (``engine.core.types.Bar``),
    so the 09:29 bar is the last bar of that range and the 09:30 bar is not in it.
  - PINNED INTERPRETATION (the pre-registration does not say *which* VWAP the last 15 closes must
    exceed): each is compared with the RUNNING session VWAP AT ITS OWN MINUTE, ``close(m) > VWAP(m)``,
    not the scalar ``VWAP(T)`` — the causal reading, and what "acceptance" means (price HELD above
    the average). The alternative was NOT computed, so no selection between the two took place.

* **(ii) Participation.** ``rel_volume_tod >= rvol_min`` where ``rel_volume_tod`` is the cumulative
  volume 09:15..T divided by the MEDIAN, over the 20 sessions immediately preceding this one, of that
  symbol's cumulative volume to the SAME minute. At least
  :data:`PARAMS`\\ ``["rvol_min_valid_sessions"]`` (10) of those 20 prior sessions must carry a usable
  cumulative volume, else the SYMBOL-DAY IS SKIPPED (counted, and reported, as
  ``rvol_insufficient_history_symbol_days``) rather than admitted on a thin denominator.

* **(iii) Relative strength.** ``ret_from_open(T) >= +1.0%`` AND
  ``ret_from_open(T) >= index_ret_from_open(T) + 1.0%``, where ``ret_from_open`` is
  ``close(T) / open(09:15) - 1``.

Passers are ranked by ``ret_from_open - index_ret_from_open`` (descending; ties broken by symbol,
ascending, for determinism) and the TOP 5 per session are taken. Entry is the OPEN of the bar
immediately after T (the 11:01 bar when T = 11:00). Exit is the first 1m CLOSE below
``VWAP(T) * (1 - stop_pct/100)``, filled at the FOLLOWING bar's OPEN; if that never happens, the
15:15 bar's close (or the last bar at or before 15:15). No target. The ``no_stop`` variant removes the stop.

===============================================================================================
LOOK-AHEAD DISCIPLINE
===============================================================================================
* Every input to conditions (i)-(iii) comes from bars with ``ts_minute <= T`` ONLY: the feature SQL
  filters ``time <= T`` before the cumulative window functions run, so later bars are not in the
  relation.
* The ``rel_volume_tod`` denominator uses STRICTLY PRIOR sessions; today's volume never enters its
  own median.
* Entry is the OPEN of the bar AFTER T; the decision bar's close is never a fill price.
* The VWAP-loss exit triggers on a bar CLOSE and fills at the NEXT bar's OPEN, never at the
  triggering close.
* The 15:15 exit uses the 15:15 bar's close (or the last bar at or before 15:15).
* The breadth split comes from the same ``T``-bar snapshot as the signal, not the day's outcome.

===============================================================================================
COSTS
===============================================================================================
``CostModel.breakeven_pct(notional, "MIS")`` — ONE full round trip, statutory fees PLUS measured
spread, at :data:`PARAMS`\\ ``["reference_notional_inr"]`` — plus ONE TICK of slippage on EACH side,
``2 * tick / entry_px * 100`` percent (larger for a cheap stock). MIS, never CNC: an intraday leg
squared off the same session. ``breakeven_pct`` rather than ``fee_breakeven_pct`` is deliberate
(fees-only is not a viability number); spread is not counted twice: tick slippage is the queue cost of
crossing, the spread in ``breakeven_pct`` is the quoted half-spread per leg.

===============================================================================================
INDEX PROXY (owner/manager-directed deviation from the pre-registered text)
===============================================================================================
The pre-registration says "NIFTY 50 1m for RS", but ``bars_1m`` carries "NIFTY 50" only from
2026-07-22 while stock 1m history begins 2023-07-17. For earlier sessions condition (iii)'s index
return is the EQUAL-WEIGHT MEAN of ``ret_from_open(T)`` across that session's own eligible universe:
a proxy, not free-float-weighted, over this study's eligible set rather than the NIFTY 50's names, and
on proxy days condition (iii) becomes a purely cross-sectional statement. Every cell is therefore ALSO
reported for the ``real_nifty50_index_only`` split (sessions with the true index series), so no
number depends on trusting the proxy.

===============================================================================================
ATR% TERCILE SPLIT (REPORTING ONLY, selects nothing, wires nothing)
===============================================================================================
A SPLIT of the SAME registered result (tests whether alpha lives in high-ATR names), never a new rule:
no parameter moved, no variant or trade added or removed, no cell selected.

* **The statistic.** Wilder ATR(:data:`PARAMS`\\ ``["atr_lookback_sessions"]`` = 14) of DAILY bars
  over the 14 completed sessions STRICTLY BEFORE the signal day, divided by the close of the last of
  those sessions, in percent. ``bars_1d`` is read for this and nothing else.
* **The cut.** Terciles taken ONCE over the DISTINCT measured ``(symbol, signal day)`` pairs, so a
  symbol-day sits in the same cell under every variant (``scripts/backtest_brk20.py``'s margin-tercile
  posture).
* **DESCRIPTIVE, NOT TRADEABLE.** The cuts are full-sample statistics, unknowable at signal time. A
  symbol-day with fewer than 14 prior daily sessions is labelled ``atr_pct_unclassified`` and kept,
  so the four ATR cells sum to the measured population.

===============================================================================================
DATA ACCESS
===============================================================================================
Read-only (``duckdb.connect(..., read_only=True)``): ``MarketStore.open()`` runs schema DDL and is a
WRITER, so, as in ``scripts/backtest_hi52.py`` and ``scripts/entity_resolution_sample.py``, it is not
used. DuckDB locks the file even for a read-only attach, so the engine service must be stopped. A
missing/locked file REFUSES with exit code 2.

Exit codes: 0 = ran; 2 = DB unopenable / no bars in window.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import statistics
import sys
from bisect import bisect_left, bisect_right
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

_REPO_SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if _REPO_SRC not in sys.path:  # pragma: no cover - loose-script shim
    sys.path.insert(0, _REPO_SRC)
_SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPTS_DIR not in sys.path:  # pragma: no cover - loose-script shim
    sys.path.append(_SCRIPTS_DIR)

import engine  # noqa: E402,F401,I001  native import-order guard: sklearn's OpenMP runtime MUST be
#                                      established before skfolio/cvxpy loads (engine._preload). The
#                                      isort suppression keeps it FIRST; re-sorting would sink it
#                                      below the third-party block and re-open the segfault.
import duckdb  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from backtest_common import DbUnopenable, _purged_kfold_splits, open_readonly  # noqa: E402
from engine.core.calendar import NSECalendar  # noqa: E402
from engine.core.clock import IST, Clock  # noqa: E402
from engine.core.config import config_dir, repo_root  # noqa: E402
from engine.learning.validate import cpcv_splits  # noqa: E402
from engine.marketdata.reconcile import DEFAULT_TICK_SIZE  # noqa: E402
from engine.strategy.cost_model import CostModel  # noqa: E402
from engine.strategy.indicators import wilder_atr  # noqa: E402

# ================================================================================================
# THE ONE PRE-REGISTERED PARAMETER SET. Not a knob, not a grid, not reachable from the CLI.
# ================================================================================================
PARAMS: dict[str, Any] = {
    # --- (i) acceptance
    "decision_time": "11:00",          # T: the bar whose CLOSE is the decision point
    "or_start": "09:15",               # opening range, inclusive
    "or_end_exclusive": "09:30",       # opening range, exclusive -> bars 09:15..09:29
    "acceptance_bars": 15,             # the 15 one-minute closes immediately before T
    # --- (ii) participation
    "rvol_min": 1.5,
    "rvol_lookback_sessions": 20,      # the 20 sessions immediately preceding this one
    "rvol_min_valid_sessions": 10,     # fewer usable priors than this -> SKIP the symbol-day
    # --- (iii) relative strength
    "ret_from_open_min_pct": 1.0,
    "rs_over_index_min_pct": 1.0,
    "index_symbol": "NIFTY 50",
    # --- selection / execution
    "top_n_per_day": 5,
    "stop_vwap_pct": 0.25,             # exit trigger: close < VWAP(T) * (1 - 0.25%); None = no stop
    "squareoff_time": "15:15",
    # --- costs
    "product": "MIS",
    "reference_notional_inr": "20000",
    "slippage_ticks_per_side": 1,
    "tick_size_inr": str(DEFAULT_TICK_SIZE),
    # --- splits
    "breadth_trend_day_min": 0.5,      # share of eligible universe above its OR high at T
    "sector_confirm_min_others": 2,    # variant C: OTHER same-sector passers required
    "atr_lookback_sessions": 14,       # ATR% split: DAILY sessions strictly before the signal day
    # --- the pre-registered promotion rule (a boolean, not a recommendation)
    "promote_min_n": 200,
    "promote_min_t": 2.0,
    "promote_min_cpcv_positive_share": 0.60,
    # --- CPCV (engine.learning.validate defaults: 6 folds / 2 test folds / purge 5 / embargo 5)
    "cpcv_purge_obs": 5,
    "cpcv_embargo_obs": 5,
}

# ================================================================================================
# EVERY VARIANT IS REPORTED. NONE IS SELECTED. Each value is an override applied on top of PARAMS.
# trend_day is the hypothesis; no_stop the owner's observed pattern; sector_confirm adds a
# same-sector confirmation leg (needs `sector_map`); the rest are robustness checks.
# ================================================================================================
VARIANTS: dict[str, dict[str, Any]] = {
    "trend_day":     {},                                   # the pre-registered hypothesis
    "no_stop":       {"stop_vwap_pct": None},              # hold to 15:15, no stop
    "T_1030":        {"decision_time": "10:30"},           # robustness: earlier decision bar
    "T_1200":        {"decision_time": "12:00"},           # robustness: later decision bar
    "RVOL_1.2":      {"rvol_min": 1.2},                    # robustness: looser participation
    "RVOL_2.0":      {"rvol_min": 2.0},                    # robustness: tighter participation
    "STOP_0.5":      {"stop_vwap_pct": 0.5},               # robustness: wider VWAP-loss stop
    "sector_confirm": {"sector_confirm": True},            # trend_day + >=2 OTHER same-sector passers at T
}

#: The ONE pre-registered rule; every other :data:`VARIANTS` entry is a reported robustness cell,
#: never a competitor it is picked from.
PRE_REGISTERED_HYPOTHESIS = "trend_day"

SPLIT_ALL = "all"
SPLIT_INDEX_UP = "index_up_at_T"
SPLIT_INDEX_DOWN = "index_down_at_T"
SPLIT_BREADTH_HIGH = "breadth_ge_50pct"
SPLIT_BREADTH_LOW = "breadth_lt_50pct"
SPLIT_REAL_INDEX = "real_nifty50_index_only"
SPLIT_PLAN_WINDOW = "plan_window_2025_07_10_onward"
SPLIT_CATALYST_TRUE = "catalyst_at_T_true"
SPLIT_CATALYST_FALSE = "catalyst_at_T_false"
SPLIT_ATR_HIGH = "atr_pct_tercile_high"
SPLIT_ATR_MID = "atr_pct_tercile_mid"
SPLIT_ATR_LOW = "atr_pct_tercile_low"
SPLIT_ATR_UNCLASSIFIED = "atr_pct_unclassified"

#: The three ATR% tercile cells, high first.
ATR_TERCILE_CELLS: tuple[str, str, str] = (SPLIT_ATR_HIGH, SPLIT_ATR_MID, SPLIT_ATR_LOW)
#: Those three plus the residue, making the four cells a partition of the population.
ATR_REPORT_CELLS: tuple[str, ...] = (*ATR_TERCILE_CELLS, SPLIT_ATR_UNCLASSIFIED)

ATR_SPLIT_DEFINITION = (
    "Wilder ATR(14) of DAILY bars over the 14 completed sessions STRICTLY BEFORE the signal day, "
    "divided by the close of the last of those sessions, in percent; terciles cut on the full "
    "measured population (DESCRIPTIVE, full-sample statistics - unknowable at signal time)"
)

#: Calendar days of daily-bar warm-up before the first measured signal day (14 sessions span at most
#: ~25 calendar days even across a holiday cluster).
ATR_DAILY_WARMUP_DAYS = 120

#: The pre-registration's stated 1m floor (the store holds 1m bars from 2023-07-17); kept only so
#: that literal window is reported as its own split cell.
PLAN_STATED_1M_FLOOR = date(2025, 7, 10)

_CV_SKFOLIO = "cpcv_skfolio_CombinatorialPurgedCV"
_CV_FALLBACK = "purged_kfold_with_embargo_fallback"

_TIME_RE = re.compile(r"^([01]\d|2[0-3]):([0-5]\d)$")


# =============================================================================== data structures
@dataclass
class Trade:
    """One measured entry. ``gross_pct``/``net_pct`` are PERCENT, per-trade equal notional."""

    variant: str
    symbol: str
    d: date
    entry_px: float
    exit_px: float
    exit_reason: str
    gross_pct: float
    cost_pct: float
    net_pct: float
    rs_pct: float
    rvol: float
    index_ret_pct: float
    index_is_real: bool
    breadth: float
    catalyst_at_T: bool
    catalyst_split_covered: bool
    sector: str | None = None
    #: ATR% tercile split, stamped AFTER the trade exists (:func:`stamp_atr_terciles`); None = unclassified.
    atr_pct: float | None = None
    atr_cell: str | None = None


@dataclass
class DayFeatures:
    """The T-bar snapshot for one session: per-symbol features plus the day's aggregates."""

    d: date
    symbols: list[str] = field(default_factory=list)
    ret_pct: dict[str, float] = field(default_factory=dict)
    close_t: dict[str, float] = field(default_factory=dict)
    vwap_t: dict[str, float] = field(default_factory=dict)
    or_high: dict[str, float] = field(default_factory=dict)
    rvol: dict[str, float] = field(default_factory=dict)
    accepted: dict[str, bool] = field(default_factory=dict)


# =============================================================================== time helpers
def _validate_hhmm(s: str) -> str:
    if not _TIME_RE.match(s):
        raise ValueError(f"not an HH:MM time literal: {s!r}")
    return s


def _to_time(s: str) -> time:
    h, m = _validate_hhmm(s).split(":")
    return time(int(h), int(m))


def _shift(s: str, minutes: int) -> str:
    """``HH:MM`` shifted by ``minutes`` (same day; the NSE session never crosses midnight)."""
    t = _to_time(s)
    total = t.hour * 60 + t.minute + minutes
    return f"{total // 60:02d}:{total % 60:02d}"


# =============================================================================== feature extraction
def acceptance_window(decision_time: str, n_bars: int) -> tuple[str, str]:
    """The ``[start, end]`` inclusive minute range of the ``n_bars`` closes immediately BEFORE T.

    For T = 11:00 and 15 bars this is ``("10:45", "10:59")``; derived from T so the T=10:30 / T=12:00
    cells shift with it.
    """
    return _shift(decision_time, -n_bars), _shift(decision_time, -1)


def load_features(
    conn: duckdb.DuckDBPyConnection,
    start: date,
    end: date,
    *,
    decision_time: str,
    params: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Per ``(symbol, d)`` T-bar snapshot, computed from bars with ``ts_minute <= T`` ONLY.

    Returns columns ``symbol, d, open_0915, or_high, close_t, vwap_t, cumvol_t, n_acc, n_acc_ok``.
    The ``time <= T`` filter runs BEFORE the cumulative window functions, so look-ahead is
    structurally impossible here.
    """
    p = dict(params or PARAMS)
    t_lit = _validate_hhmm(decision_time)
    or_lo = _validate_hhmm(p["or_start"])
    or_hi = _validate_hhmm(p["or_end_exclusive"])
    acc_lo, acc_hi = acceptance_window(t_lit, int(p["acceptance_bars"]))
    sql = f"""
    WITH b AS (
      SELECT symbol,
             (ts_minute AT TIME ZONE 'Asia/Kolkata') AS tl,
             "open"::DOUBLE AS o, high::DOUBLE AS h, low::DOUBLE AS l,
             "close"::DOUBLE AS c, volume::DOUBLE AS v
      FROM bars_1m
    ),
    b2 AS (
      SELECT symbol, tl::DATE AS d, tl::TIME AS tt, o, h, l, c, v
      FROM b
      WHERE tl::TIME <= TIME '{t_lit}' AND tl::DATE >= ? AND tl::DATE <= ?
    ),
    r AS (
      SELECT symbol, d, tt, o, h, l, c, v,
             SUM((h + l + c) / 3.0 * v) OVER w / NULLIF(SUM(v) OVER w, 0) AS vwap_run,
             SUM(v) OVER w AS cumvol
      FROM b2
      WINDOW w AS (PARTITION BY symbol, d ORDER BY tt
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    )
    SELECT symbol, d,
           MAX(o)        FILTER (tt = TIME '{or_lo}')                        AS open_0915,
           MAX(h)        FILTER (tt >= TIME '{or_lo}' AND tt < TIME '{or_hi}') AS or_high,
           MAX(c)        FILTER (tt = TIME '{t_lit}')                        AS close_t,
           MAX(vwap_run) FILTER (tt = TIME '{t_lit}')                        AS vwap_t,
           MAX(cumvol)   FILTER (tt = TIME '{t_lit}')                        AS cumvol_t,
           COUNT(*)      FILTER (tt >= TIME '{acc_lo}' AND tt <= TIME '{acc_hi}') AS n_acc,
           COUNT(*)      FILTER (tt >= TIME '{acc_lo}' AND tt <= TIME '{acc_hi}'
                                 AND c > vwap_run)                           AS n_acc_ok
    FROM r
    GROUP BY 1, 2
    """
    return conn.execute(sql, [start, end]).df()


def load_exit_bars(
    conn: duckdb.DuckDBPyConnection,
    pairs: pd.DataFrame,
    *,
    decision_time: str,
    squareoff_time: str,
) -> dict[tuple[str, date], tuple[list[str], list[float], list[float]]]:
    """``(symbol, d) -> (times, opens, closes)`` for the bars in ``(T, squareoff]``, ascending.

    Only the selected symbol-days are fetched, never the full post-T tape.
    """
    if pairs.empty:
        return {}
    t_lit = _validate_hhmm(decision_time)
    sq = _validate_hhmm(squareoff_time)
    conn.register("tdc_sel", pairs)
    try:
        sql = f"""
        WITH b AS (
          SELECT symbol, (ts_minute AT TIME ZONE 'Asia/Kolkata') AS tl,
                 "open"::DOUBLE AS o, "close"::DOUBLE AS c
          FROM bars_1m
        )
        SELECT b.symbol AS symbol, b.tl::DATE AS d, strftime(b.tl, '%H:%M') AS tt, b.o, b.c
        FROM b JOIN tdc_sel s ON s.symbol = b.symbol AND s.d = b.tl::DATE
        WHERE b.tl::TIME > TIME '{t_lit}' AND b.tl::TIME <= TIME '{sq}'
        ORDER BY 1, 2, 3
        """
        frame = conn.execute(sql).df()
    finally:
        conn.unregister("tdc_sel")
    out: dict[tuple[str, date], tuple[list[str], list[float], list[float]]] = {}
    for (sym, d), grp in frame.groupby(["symbol", "d"], sort=False):
        dd = d.date() if hasattr(d, "date") else d
        out[(str(sym), dd)] = (
            grp["tt"].tolist(),
            grp["o"].astype(float).tolist(),
            grp["c"].astype(float).tolist(),
        )
    return out


# =============================================================================== universe / context
def load_universe_daily(conn: duckdb.DuckDBPyConnection) -> dict[date, set[str]]:
    """``d -> {included symbols}`` from ``universe_daily``. Days with no included row are omitted,
    so the caller's fallback fires for them (and records the date)."""
    try:
        rows = conn.execute(
            "SELECT d, symbol FROM universe_daily WHERE included ORDER BY d, symbol"
        ).fetchall()
    except Exception:  # noqa: BLE001 - a missing table must not sink an offline study
        return {}
    out: dict[date, set[str]] = defaultdict(set)
    for d, sym in rows:
        out[d.date() if hasattr(d, "date") else d].add(str(sym))
    return dict(out)


def load_sector_map(conn: duckdb.DuckDBPyConnection) -> tuple[dict[str, list[tuple[date, str]]], str]:
    """``symbol -> [(as_of, sector), ...]`` ascending, plus a provenance string.

    Sessions before the first snapshot get the earliest snapshot applied BACKWARDS: a point-in-time
    proxy, labelled as such.
    """
    try:
        rows = conn.execute("SELECT as_of, symbol, sector FROM sector_map ORDER BY symbol, as_of").fetchall()
    except Exception:  # noqa: BLE001
        return {}, "UNAVAILABLE (sector_map absent; variant C not computable)"
    out: dict[str, list[tuple[date, str]]] = defaultdict(list)
    for as_of, sym, sector in rows:
        out[str(sym)].append((as_of.date() if hasattr(as_of, "date") else as_of, str(sector)))
    if not out:
        return {}, "UNAVAILABLE (sector_map empty; variant C not computable)"
    stamps = sorted({a for v in out.values() for a, _ in v})
    return dict(out), (
        f"sector_map, {len(out)} symbols, {len(stamps)} snapshots "
        f"{stamps[0]} -> {stamps[-1]} [POINT-IN-TIME PROXY: applied backwards before {stamps[0]}]"
    )


def sector_at(smap: dict[str, list[tuple[date, str]]], symbol: str, d: date) -> str | None:
    """The last snapshot at or before ``d``; the earliest snapshot when ``d`` precedes them all."""
    hist = smap.get(symbol)
    if not hist:
        return None
    i = bisect_right([a for a, _ in hist], d)
    return hist[i - 1][1] if i > 0 else hist[0][1]


def load_catalyst_context(
    conn: duckdb.DuckDBPyConnection,
) -> tuple[set[tuple[date, str]], dict[str, list[datetime]], dict[str, Any]]:
    """``({(d, symbol) on the watchlist}, {symbol -> sorted news first_seen}, coverage)``.

    ``coverage`` carries the date range over which the catalyst split is meaningful; outside it the
    flag would only measure the feed's absence, so the split cells are restricted to that range.
    """
    watch: set[tuple[date, str]] = set()
    cov: dict[str, Any] = {
        "catalyst_watchlist_rows": 0, "catalyst_watchlist_first_d": None,
        "catalyst_watchlist_last_d": None, "news_clusters_rows": 0,
        "news_clusters_first_seen": None, "news_clusters_last_seen": None,
        "split_covered_from": None,
    }
    try:
        rows = conn.execute("SELECT d, symbol FROM catalyst_watchlist").fetchall()
        for d, sym in rows:
            watch.add(((d.date() if hasattr(d, "date") else d), str(sym)))
        lo, hi = conn.execute("SELECT min(d), max(d) FROM catalyst_watchlist").fetchone()
        cov["catalyst_watchlist_rows"] = len(rows)
        cov["catalyst_watchlist_first_d"] = None if lo is None else str(lo)
        cov["catalyst_watchlist_last_d"] = None if hi is None else str(hi)
    except Exception:  # noqa: BLE001
        pass
    news: dict[str, list[datetime]] = defaultdict(list)
    try:
        rows = conn.execute(
            "SELECT unnest(symbols) AS sym, first_seen FROM news_clusters WHERE symbols IS NOT NULL"
        ).fetchall()
        for sym, ts in rows:
            if ts is not None:
                news[str(sym)].append(ts)
        n, lo, hi = conn.execute(
            "SELECT count(*), min(first_seen), max(first_seen) FROM news_clusters"
        ).fetchone()
        cov["news_clusters_rows"] = int(n or 0)
        cov["news_clusters_first_seen"] = None if lo is None else str(lo)
        cov["news_clusters_last_seen"] = None if hi is None else str(hi)
    except Exception:  # noqa: BLE001
        pass
    starts = [
        datetime.fromisoformat(s).date() if isinstance(s, str) else s
        for s in (cov["news_clusters_first_seen"], cov["catalyst_watchlist_first_d"])
        if s is not None
    ]
    if starts:
        cov["split_covered_from"] = str(min(starts))
    return watch, {k: sorted(v) for k, v in news.items()}, cov


def coverage_by_year(conn: duckdb.DuckDBPyConnection, index_symbol: str) -> list[dict[str, Any]]:
    """Per-calendar-year ``bars_1m`` shape: sessions, symbols, and how many carry a FULL session.

    "Full session" = >= 330 of the 375 one-minute bars.
    """
    sql = """
    WITH b AS (
      SELECT symbol, (ts_minute AT TIME ZONE 'Asia/Kolkata')::DATE AS d, count(*) AS n
      FROM bars_1m WHERE symbol <> ? GROUP BY 1, 2
    )
    SELECT year(d) AS y, count(DISTINCT d) AS sessions, count(DISTINCT symbol) AS symbols,
           count(DISTINCT CASE WHEN n >= 330 THEN symbol END) AS symbols_full_session,
           count(*) AS symbol_days,
           sum(CASE WHEN n >= 330 THEN 1 ELSE 0 END) AS symbol_days_full_session,
           min(d) AS first_d, max(d) AS last_d
    FROM b GROUP BY 1 ORDER BY 1
    """
    rows = conn.execute(sql, [index_symbol]).fetchall()
    return [
        {
            "year": int(r[0]), "sessions": int(r[1]), "symbols": int(r[2]),
            "symbols_full_session": int(r[3]), "symbol_days": int(r[4]),
            "symbol_days_full_session": int(r[5]),
            "first_session": str(r[6]), "last_session": str(r[7]),
        }
        for r in rows
    ]


# =============================================================================== daily ATR% split
def load_daily_ohlc(
    conn: duckdb.DuckDBPyConnection,
    symbols: Sequence[str],
    lo: date,
    hi: date,
) -> dict[str, tuple[list[date], list[float], list[float], list[float]]]:
    """``symbol -> (sessions ascending, high, low, close)`` from ``bars_1d`` over ``[lo, hi]``.

    Only LABELS each measured symbol-day with prior volatility; no signal, fill or cost reads it. A
    missing or empty ``bars_1d`` yields an empty map (every trade unclassified).
    """
    if not symbols:
        return {}
    conn.register("tdc_atr_syms", pd.DataFrame({"symbol": sorted(set(symbols))}))
    try:
        frame = conn.execute(
            """
            SELECT b.symbol AS symbol, b.d AS d, b.high::DOUBLE AS h, b.low::DOUBLE AS l,
                   b."close"::DOUBLE AS c
            FROM bars_1d b JOIN tdc_atr_syms s ON s.symbol = b.symbol
            WHERE b.d >= ? AND b.d <= ?
            ORDER BY 1, 2
            """,
            [lo, hi],
        ).df()
    except Exception:  # noqa: BLE001 - a missing bars_1d must not sink an offline study
        return {}
    finally:
        conn.unregister("tdc_atr_syms")
    out: dict[str, tuple[list[date], list[float], list[float], list[float]]] = {}
    for sym, grp in frame.groupby("symbol", sort=False):
        out[str(sym)] = (
            [x.date() if hasattr(x, "date") else x for x in grp["d"]],
            grp["h"].astype(float).tolist(),
            grp["l"].astype(float).tolist(),
            grp["c"].astype(float).tolist(),
        )
    return out


def atr_pct_at(
    daily: tuple[list[date], list[float], list[float], list[float]] | None,
    d: date,
    *,
    lookback: int,
) -> float | None:
    """Wilder ATR(``lookback``) over the daily sessions STRICTLY BEFORE ``d``, as a percent of the
    close of the last of them - or ``None`` when the symbol has fewer than ``lookback`` prior ones.

    LOOK-AHEAD: ``bisect_left`` lands ON ``d`` when the symbol traded that day, so the window ends at
    the prior session and the signal day's own range and close never enter. The window is EXACTLY
    ``lookback`` sessions, so :func:`wilder_atr` is still at its SMA seed: the mean true range.
    PINNED READING: the earliest bar has no prior close inside the window, so its true range is the
    plain high-low (``engine.strategy.indicators._true_range``); a gap-aware TR was not computed, so
    no selection between the readings took place.
    """
    if daily is None:
        return None
    days, highs, lows, closes = daily
    i = bisect_left(days, d)
    if i < lookback:
        return None
    prior_close = float(closes[i - 1])
    if not math.isfinite(prior_close) or prior_close <= 0.0:
        return None
    atr = float(wilder_atr(highs[i - lookback:i], lows[i - lookback:i], closes[i - lookback:i],
                           lookback).iloc[-1])
    if not math.isfinite(atr):
        return None
    return atr / prior_close * 100.0


def atr_tercile_cuts(values: Iterable[float | None]) -> tuple[float, float] | None:
    """The two cuts of the ATR% tercile split, or ``None`` when fewer than three values are known."""
    vals = sorted(v for v in values if v is not None and math.isfinite(v))
    if len(vals) < 3:
        return None
    c1, c2 = statistics.quantiles(vals, n=3)
    return float(c1), float(c2)


def atr_tercile_of(atr_pct: float | None, cuts: tuple[float, float] | None) -> str | None:
    """Which tercile cell ``atr_pct`` falls in: ``<= c1`` low, ``<= c2`` mid, else high."""
    if cuts is None or atr_pct is None or not math.isfinite(atr_pct):
        return None
    c1, c2 = cuts
    if atr_pct <= c1:
        return SPLIT_ATR_LOW
    if atr_pct <= c2:
        return SPLIT_ATR_MID
    return SPLIT_ATR_HIGH


def stamp_atr_terciles(
    groups: Iterable[Sequence[Trade]],
    atr_by_pair: Mapping[tuple[str, date], float | None],
) -> dict[str, Any]:
    """Stamp every trade with its symbol-day ATR% and tercile cell; return the split's provenance.

    The cuts are taken ONCE over the DISTINCT measured ``(symbol, signal day)`` pairs, so a symbol-day
    sits in the same cell under every variant. Full-sample statistics: DESCRIPTIVE.
    """
    batches = [list(g) for g in groups]
    pairs = sorted({(t.symbol, t.d) for g in batches for t in g})
    cuts = atr_tercile_cuts(atr_by_pair.get(p) for p in pairs)
    counts: dict[str, int] = dict.fromkeys(ATR_REPORT_CELLS, 0)
    for g in batches:
        for t in g:
            t.atr_pct = atr_by_pair.get((t.symbol, t.d))
            t.atr_cell = atr_tercile_of(t.atr_pct, cuts)
            counts[t.atr_cell or SPLIT_ATR_UNCLASSIFIED] += 1
    known = [
        v for v in (atr_by_pair.get(p) for p in pairs) if v is not None and math.isfinite(v)
    ]
    return {
        "definition": ATR_SPLIT_DEFINITION,
        "source_table": "bars_1d",
        "cut_population": "distinct measured (symbol, signal day) pairs",
        "n_symbol_days": len(pairs),
        "n_symbol_days_with_atr": len(known),
        "n_symbol_days_without_atr": len(pairs) - len(known),
        "cuts": None if cuts is None else [round(cuts[0], 4), round(cuts[1], 4)],
        "atr_pct_min": round(min(known), 4) if known else None,
        "atr_pct_median": round(statistics.median(known), 4) if known else None,
        "atr_pct_max": round(max(known), 4) if known else None,
        "n_trades_by_cell": counts,
        "descriptive_not_tradeable": True,
    }


# =============================================================================== rel_volume_tod
def rvol_medians(
    cumvol: dict[str, dict[date, float]],
    sessions: list[date],
    *,
    lookback: int,
    min_valid: int,
) -> tuple[dict[str, dict[date, float]], int]:
    """``symbol -> d -> median prior cumulative volume to T``; plus the count of SKIPPED symbol-days.

    The window is the ``lookback`` sessions immediately preceding ``d`` in the GLOBAL session list
    (strictly prior). A symbol-day with fewer than ``min_valid`` usable priors gets NO entry and is
    counted as a skip, not admitted on a thinner median.
    """
    pos = {d: i for i, d in enumerate(sessions)}
    out: dict[str, dict[date, float]] = {}
    skipped = 0
    for sym, by_day in cumvol.items():
        med: dict[date, float] = {}
        for d, _v in by_day.items():
            i = pos.get(d)
            if i is None:
                continue
            priors = [
                by_day[pd_]
                for pd_ in sessions[max(0, i - lookback):i]
                if pd_ in by_day and math.isfinite(by_day[pd_]) and by_day[pd_] > 0.0
            ]
            if len(priors) < min_valid:
                skipped += 1
                continue
            med[d] = statistics.median(priors)
        out[sym] = med
    return out, skipped


# =============================================================================== selection
def select_day(
    feats: DayFeatures,
    *,
    index_ret_pct: float,
    eligible: Sequence[str],
    params: dict[str, Any],
    sectors: dict[str, str | None] | None = None,
) -> list[str]:
    """The session's ranked top-N passers of conditions (i)-(iii), ranked by relative strength.

    ``sectors`` non-None turns on variant C's confirmation: a passer is kept only when at least
    ``sector_confirm_min_others`` OTHER symbols in the SAME sector also passed (i)-(iii) at T.
    """
    rvol_min = float(params["rvol_min"])
    ret_min = float(params["ret_from_open_min_pct"])
    rs_min = float(params["rs_over_index_min_pct"])
    passers: list[str] = []
    for sym in eligible:
        if not feats.accepted.get(sym, False):
            continue
        rv = feats.rvol.get(sym)
        if rv is None or not math.isfinite(rv) or rv < rvol_min:
            continue
        ret = feats.ret_pct.get(sym)
        if ret is None or not math.isfinite(ret):
            continue
        if ret < ret_min or ret < index_ret_pct + rs_min:
            continue
        passers.append(sym)
    if sectors is not None:
        need = int(params["sector_confirm_min_others"])
        counts: dict[str, int] = defaultdict(int)
        for sym in passers:
            sec = sectors.get(sym)
            if sec is not None:
                counts[sec] += 1
        passers = [
            s for s in passers
            if sectors.get(s) is not None and counts[sectors[s]] - 1 >= need
        ]
    passers.sort(key=lambda s: (-(feats.ret_pct[s] - index_ret_pct), s))
    return passers[: int(params["top_n_per_day"])]


# =============================================================================== execution
def simulate_exit(
    times: Sequence[str],
    opens: Sequence[float],
    closes: Sequence[float],
    *,
    vwap_t: float,
    stop_pct: float | None,
    entry_time: str,
    squareoff_time: str,
) -> tuple[float, float, str] | None:
    """``(entry_px, exit_px, reason)`` for one trade, or ``None`` when the entry bar is missing.

    Entry is the OPEN of the ``entry_time`` bar (the bar AFTER T), which must exist at exactly that
    minute; a gap there is a skipped trade, never a fill at another price.

    Exit: the FIRST bar from the entry bar forward whose CLOSE is below ``vwap_t * (1 - stop_pct/100)``
    fills at the NEXT bar's OPEN, never at the triggering price. If none triggers (or ``stop_pct`` is
    None, variant B), the exit is the CLOSE of the last bar at or before ``squareoff_time``.
    """
    try:
        i0 = list(times).index(entry_time)
    except ValueError:
        return None
    last = len(times) - 1
    while last >= 0 and times[last] > squareoff_time:
        last -= 1
    if last < i0:
        return None
    entry_px = float(opens[i0])
    if not math.isfinite(entry_px) or entry_px <= 0.0:
        return None
    if stop_pct is not None:
        threshold = float(vwap_t) * (1.0 - float(stop_pct) / 100.0)
        for k in range(i0, last):                      # the squareoff bar itself is handled below
            if float(closes[k]) < threshold:
                return entry_px, float(opens[k + 1]), "vwap_loss_next_open"
    return entry_px, float(closes[last]), "squareoff_1515"


def trade_cost_pct(cost_model: CostModel, params: dict[str, Any], entry_px: float) -> float:
    """One MIS round trip at the reference notional PLUS one tick of slippage on EACH side.

    The slippage term is price-relative: one tick costs more percent on a cheap stock.
    """
    rt = float(cost_model.breakeven_pct(Decimal(params["reference_notional_inr"]), params["product"]))
    tick = float(Decimal(params["tick_size_inr"]))
    slip = 2.0 * int(params["slippage_ticks_per_side"]) * tick / entry_px * 100.0
    return rt + slip


# =============================================================================== metrics
def daily_net_series(trades: Sequence[Trade]) -> tuple[list[date], np.ndarray]:
    """Per-SESSION mean net %-return. The observation unit for CPCV is the session, not the trade:
    the day's five picks share one regime and are not five independent draws."""
    by_day: dict[date, list[float]] = defaultdict(list)
    for t in trades:
        by_day[t.d].append(t.net_pct)
    days = sorted(by_day)
    return days, np.array([statistics.fmean(by_day[d]) for d in days], dtype="float64")


def cpcv_positive_share(trades: Sequence[Trade], params: dict[str, Any]) -> dict[str, Any]:
    """CPCV over the per-session net series; the reported share is (splits with test mean > 0) / all.

    Purge and embargo are the ``engine.learning.validate`` defaults (5 each): a ``tdc`` trade opens
    and closes inside ONE session, so observations do not overlap and the default is conservative.
    """
    days, vals = daily_net_series(trades)
    n_obs = int(vals.size)
    method = _CV_SKFOLIO
    splits: list[tuple[np.ndarray, np.ndarray]] = []
    purge = int(params["cpcv_purge_obs"])
    embargo = int(params["cpcv_embargo_obs"])
    if n_obs:
        try:
            splits = cpcv_splits(n_obs, purge=purge, embargo=embargo)
        except Exception as exc:  # noqa: BLE001 - skfolio absent must not sink the run
            method = f"{_CV_FALLBACK} (skfolio unavailable: {type(exc).__name__})"
            splits = _purged_kfold_splits(n_obs, purge=purge, embargo=embargo)
        else:
            if not splits:
                method = _CV_FALLBACK
                splits = _purged_kfold_splits(n_obs, purge=purge, embargo=embargo)
    positives = 0
    for _train, test in splits:
        sl = vals[np.asarray(test, dtype=np.int64)]
        if sl.size and float(sl.mean()) > 0.0:
            positives += 1
    share = (positives / len(splits)) if splits else None
    return {
        "cv_method": method if splits else "none (insufficient observations)",
        "n_obs_sessions": n_obs,
        "first_session": str(days[0]) if days else None,
        "last_session": str(days[-1]) if days else None,
        "purge_obs": purge,
        "embargo_obs": embargo,
        "n_splits": len(splits),
        "n_positive_splits": positives,
        "positive_share": None if share is None else round(share, 4),
    }


def metrics(trades: Sequence[Trade], params: dict[str, Any]) -> dict[str, Any]:
    """``n``, mean/median net %, win rate, t-stat, CPCV positive share, and the pre-registration's boolean.

    ``t = mean / (sd / sqrt(n))`` with the SAMPLE standard deviation (ddof=1), over TRADES. The day's
    five picks are correlated, so this t-stat overstates the effective sample size; the CPCV share
    does not.
    """
    net = [t.net_pct for t in trades]
    n = len(net)
    base: dict[str, Any] = {
        "n": n, "mean_net_pct": None, "median_net_pct": None, "win_rate": None,
        "t_stat": None, "mean_gross_pct": None, "median_gross_pct": None, "mean_cost_pct": None,
    }
    if n:
        base["mean_net_pct"] = round(statistics.fmean(net), 4)
        base["median_net_pct"] = round(statistics.median(net), 4)
        base["win_rate"] = round(sum(1 for v in net if v > 0) / n, 4)
        base["mean_gross_pct"] = round(statistics.fmean([t.gross_pct for t in trades]), 4)
        base["median_gross_pct"] = round(statistics.median([t.gross_pct for t in trades]), 4)
        base["mean_cost_pct"] = round(statistics.fmean([t.cost_pct for t in trades]), 4)
        if n >= 2:
            sd = statistics.stdev(net)
            base["t_stat"] = round(base["mean_net_pct"] / (sd / math.sqrt(n)), 4) if sd > 0 else None
    cv = cpcv_positive_share(trades, params)
    base["cpcv"] = cv
    share = cv["positive_share"]
    base["promotable"] = bool(
        base["mean_net_pct"] is not None and base["mean_net_pct"] > 0.0
        and base["t_stat"] is not None and base["t_stat"] > float(params["promote_min_t"])
        and n >= int(params["promote_min_n"])
        and share is not None and share >= float(params["promote_min_cpcv_positive_share"])
    )
    return base


def split_cells(trades: Sequence[Trade], params: dict[str, Any]) -> dict[str, list[Trade]]:
    """Every mandatory split, as named cells. The pooled number is never the only number.

    The four ATR% cells partition the trades (three terciles plus the not-computable residue), so
    their counts sum to ``SPLIT_ALL``.
    """
    b = float(params["breadth_trend_day_min"])
    cat = [t for t in trades if t.catalyst_split_covered]
    cells = {
        SPLIT_ALL: list(trades),
        SPLIT_INDEX_UP: [t for t in trades if t.index_ret_pct >= 0.0],
        SPLIT_INDEX_DOWN: [t for t in trades if t.index_ret_pct < 0.0],
        SPLIT_BREADTH_HIGH: [t for t in trades if t.breadth >= b],
        SPLIT_BREADTH_LOW: [t for t in trades if t.breadth < b],
        SPLIT_REAL_INDEX: [t for t in trades if t.index_is_real],
        SPLIT_PLAN_WINDOW: [t for t in trades if t.d >= PLAN_STATED_1M_FLOOR],
        SPLIT_CATALYST_TRUE: [t for t in cat if t.catalyst_at_T],
        SPLIT_CATALYST_FALSE: [t for t in cat if not t.catalyst_at_T],
    }
    for name in ATR_TERCILE_CELLS:
        cells[name] = [t for t in trades if t.atr_cell == name]
    cells[SPLIT_ATR_UNCLASSIFIED] = [t for t in trades if t.atr_cell is None]
    return cells


# =============================================================================== the study
def run_study(
    conn: duckdb.DuckDBPyConnection,
    *,
    start: date,
    end: date,
    cost_model: CostModel,
    params: dict[str, Any] | None = None,
    variants: dict[str, dict[str, Any]] | None = None,
    calendar: NSECalendar | None = None,
) -> tuple[dict[str, Any], dict[str, list[Trade]]]:
    """Run every variant end to end. Returns ``(json document, trades by variant)``."""
    base = dict(params or PARAMS)
    var_defs = dict(variants if variants is not None else VARIANTS)
    # The post-T tape is fetched once per decision time up to ONE squareoff cutoff; a variant moving
    # the squareoff would have its tape silently truncated.
    if any(
        str(v.get("squareoff_time", base["squareoff_time"])) != str(base["squareoff_time"])
        for v in var_defs.values()
    ):
        raise ValueError("a variant overrides squareoff_time; the shared exit-bar cache assumes one")

    # ---------------------------------------------------------------- shared context, loaded once
    universe = load_universe_daily(conn)
    smap, sector_source = load_sector_map(conn)
    watchlist, news_by_symbol, cat_cov = load_catalyst_context(conn)
    per_year = coverage_by_year(conn, base["index_symbol"])
    notes: list[str] = []

    cat_from: date | None = None
    if cat_cov["split_covered_from"]:
        cat_from = date.fromisoformat(str(cat_cov["split_covered_from"])[:10])

    # One feature pull per DISTINCT decision time.
    times_needed = sorted({str(v.get("decision_time", base["decision_time"])) for v in var_defs.values()})
    feature_cache: dict[str, pd.DataFrame] = {
        t: load_features(conn, start, end, decision_time=t, params=base) for t in times_needed
    }

    trades_by_variant: dict[str, list[Trade]] = {}
    diagnostics: dict[str, Any] = {}
    fallback_dates: set[date] = set()
    real_index_dates: set[date] = set()
    non_trading_dates: set[str] = set()
    years_without_calendar: set[int] = set()
    all_sessions: set[date] = set()

    for t_lit, frame in feature_cache.items():
        # ------------------------------------------------------- per-T: sessions, features, rvol
        frame = frame.copy()
        frame["d"] = [x.date() if hasattr(x, "date") else x for x in frame["d"]]
        idx_rows = frame[frame["symbol"] == base["index_symbol"]]
        stock = frame[frame["symbol"] != base["index_symbol"]]

        sessions = sorted(set(stock["d"]))
        all_sessions |= set(sessions)
        if calendar is not None:
            for d in sessions:
                if d.year not in getattr(calendar, "_years", {}):
                    years_without_calendar.add(d.year)
                elif not calendar.is_trading_day(d):
                    non_trading_dates.add(str(d))

        cumvol: dict[str, dict[date, float]] = defaultdict(dict)
        for sym, d, cv in zip(stock["symbol"], stock["d"], stock["cumvol_t"], strict=True):
            if cv is not None and math.isfinite(float(cv)):
                cumvol[str(sym)][d] = float(cv)
        med, n_skipped = rvol_medians(
            cumvol, sessions,
            lookback=int(base["rvol_lookback_sessions"]),
            min_valid=int(base["rvol_min_valid_sessions"]),
        )

        # index return from open at T, real where the series exists
        index_ret: dict[date, float] = {}
        for d, o, c in zip(idx_rows["d"], idx_rows["open_0915"], idx_rows["close_t"], strict=True):
            if o and c and float(o) > 0:
                index_ret[d] = (float(c) / float(o) - 1.0) * 100.0
        real_index_dates |= set(index_ret)

        # ------------------------------------------------------- per-session feature snapshots
        by_day: dict[date, DayFeatures] = {}
        n_acc_req = int(base["acceptance_bars"])
        for row in stock.itertuples(index=False):
            d = row.d
            f = by_day.get(d)
            if f is None:
                f = by_day[d] = DayFeatures(d)
            sym = str(row.symbol)
            o, orh, ct, vt = row.open_0915, row.or_high, row.close_t, row.vwap_t
            if o is None or orh is None or ct is None or vt is None:
                continue
            o, orh, ct, vt = float(o), float(orh), float(ct), float(vt)
            if not (math.isfinite(o) and o > 0 and math.isfinite(vt) and vt > 0):
                continue
            f.symbols.append(sym)
            f.close_t[sym] = ct
            f.vwap_t[sym] = vt
            f.or_high[sym] = orh
            f.ret_pct[sym] = (ct / o - 1.0) * 100.0
            f.accepted[sym] = bool(
                ct > vt and ct > orh
                and int(row.n_acc) == n_acc_req and int(row.n_acc_ok) == n_acc_req
            )
            m = med.get(sym, {}).get(d)
            if m is not None and m > 0:
                f.rvol[sym] = float(row.cumvol_t) / m

        # ------------------------------------------------------- per-session eligible set + context
        eligible_by_day: dict[date, list[str]] = {}
        breadth_by_day: dict[date, float] = {}
        index_used: dict[date, tuple[float, bool]] = {}
        for d, f in by_day.items():
            present = set(f.symbols)
            uni = universe.get(d)
            if uni:
                elig = sorted(present & uni)
                if not elig:
                    elig = sorted(present)
                    fallback_dates.add(d)
            else:
                elig = sorted(present)
                fallback_dates.add(d)
            eligible_by_day[d] = elig
            above = [s for s in elig if f.close_t[s] > f.or_high[s]]
            breadth_by_day[d] = (len(above) / len(elig)) if elig else 0.0
            if d in index_ret:
                index_used[d] = (index_ret[d], True)
            else:
                rets = [f.ret_pct[s] for s in elig if s in f.ret_pct]
                index_used[d] = (statistics.fmean(rets) if rets else 0.0, False)

        diagnostics[t_lit] = {
            "n_sessions": len(sessions),
            "n_symbol_days": int(len(stock)),
            "rvol_insufficient_history_symbol_days": n_skipped,
            "n_sessions_real_index": len(set(index_ret) & set(by_day)),
            "n_sessions_proxy_index": len(set(by_day) - set(index_ret)),
        }

        # ------------------------------------------------------- selection, for every variant at this T
        picks_by_variant: dict[str, dict[date, list[str]]] = {}
        for name, override in var_defs.items():
            p = {**base, **override}
            if str(p.get("decision_time")) != t_lit:
                continue
            use_sectors = bool(override.get("sector_confirm"))
            if use_sectors and not smap:
                notes.append(f"variant {name} SKIPPED: no sector source in the store.")
                continue
            picks_by_day: dict[date, list[str]] = {}
            for d, f in by_day.items():
                sectors = (
                    {s: sector_at(smap, s, d) for s in eligible_by_day[d]} if use_sectors else None
                )
                picks = select_day(
                    f, index_ret_pct=index_used[d][0], eligible=eligible_by_day[d],
                    params=p, sectors=sectors,
                )
                if picks:
                    picks_by_day[d] = picks
            picks_by_variant[name] = picks_by_day

        # The post-T tape is fetched ONCE per decision time for the UNION of every variant's picks
        # (re-scanning bars_1m per variant is the study's most expensive query).
        union_pairs = sorted(
            {(s, d) for pbd in picks_by_variant.values() for d, syms in pbd.items() for s in syms}
        )
        bars = load_exit_bars(
            conn,
            pd.DataFrame([{"symbol": s, "d": d} for s, d in union_pairs], columns=["symbol", "d"]),
            decision_time=t_lit, squareoff_time=str(base["squareoff_time"]),
        )
        entry_time = _shift(t_lit, 1)

        # ------------------------------------------------------- execute every variant at this T
        for name, picks_by_day in picks_by_variant.items():
            p = {**base, **var_defs[name]}
            out: list[Trade] = []
            n_no_entry_bar = 0
            for d, syms in sorted(picks_by_day.items()):
                f = by_day[d]
                iret, ireal = index_used[d]
                for sym in syms:
                    got = bars.get((sym, d))
                    if got is None:
                        n_no_entry_bar += 1
                        continue
                    sim = simulate_exit(
                        *got, vwap_t=f.vwap_t[sym], stop_pct=p["stop_vwap_pct"],
                        entry_time=entry_time, squareoff_time=str(p["squareoff_time"]),
                    )
                    if sim is None:
                        n_no_entry_bar += 1
                        continue
                    entry_px, exit_px, reason = sim
                    gross = (exit_px / entry_px - 1.0) * 100.0
                    cost = trade_cost_pct(cost_model, p, entry_px)
                    covered = cat_from is not None and d >= cat_from
                    out.append(Trade(
                        variant=name, symbol=sym, d=d, entry_px=entry_px, exit_px=exit_px,
                        exit_reason=reason, gross_pct=gross, cost_pct=cost, net_pct=gross - cost,
                        rs_pct=f.ret_pct[sym] - iret, rvol=f.rvol.get(sym, float("nan")),
                        index_ret_pct=iret, index_is_real=ireal, breadth=breadth_by_day[d],
                        catalyst_at_T=(
                            covered and _catalyst_at(
                                sym, d, t_lit, watchlist, news_by_symbol, sessions
                            )
                        ),
                        catalyst_split_covered=covered,
                        sector=sector_at(smap, sym, d) if smap else None,
                    ))
            trades_by_variant[name] = out
            diagnostics.setdefault("variants", {})[name] = {
                "decision_time": t_lit,
                "n_signal_days": len(picks_by_day),
                "n_selected_symbol_days": int(sum(len(v) for v in picks_by_day.values())),
                "n_dropped_missing_entry_bar": n_no_entry_bar,
                "n_trades": len(out),
                "exit_reason_counts": {
                    r: sum(1 for t in out if t.exit_reason == r)
                    for r in sorted({t.exit_reason for t in out})
                },
            }

    # ---------------------------------------------------------------- the ATR% tercile split
    # Runs AFTER every variant's trades exist and only labels them; bars_1d is read here and nowhere else.
    atr_lookback = int(base["atr_lookback_sessions"])
    measured_pairs = sorted({(t.symbol, t.d) for v in trades_by_variant.values() for t in v})
    daily_ohlc = (
        load_daily_ohlc(
            conn,
            [s for s, _ in measured_pairs],
            min(d for _, d in measured_pairs) - timedelta(days=ATR_DAILY_WARMUP_DAYS),
            max(d for _, d in measured_pairs),
        )
        if measured_pairs else {}
    )
    atr_by_pair: dict[tuple[str, date], float | None] = {
        (s, d): atr_pct_at(daily_ohlc.get(s), d, lookback=atr_lookback) for s, d in measured_pairs
    }
    atr_block = stamp_atr_terciles(trades_by_variant.values(), atr_by_pair)

    # ---------------------------------------------------------------- notes / caveats
    notes.append(
        "NO PARAMETER SWEEP was run and no variant was selected: PARAMS is one dict, VARIANTS is a "
        f"fixed list of {len(var_defs)} REPORTED cells, and the pre-registered hypothesis is "
        f"{PRE_REGISTERED_HYPOTHESIS}. 'promotable' is the pre-registered boolean (mean net > 0, t > 2, "
        "n >= 200, CPCV positive share >= 0.60) and carries no other meaning."
    )
    notes.append(
        "ACCEPTANCE READING PINNED: the 15 pre-T closes are compared with the RUNNING session VWAP at "
        "their own minute, not with the scalar VWAP(T). The alternative reading was not computed, so "
        "no selection between the two occurred."
    )
    notes.append(
        "INDEX PROXY: bars_1m carries '" + str(base["index_symbol"]) + "' only from "
        + (str(min(real_index_dates)) if real_index_dates else "n/a")
        + f"; for earlier sessions condition (iii)'s index return is the EQUAL-WEIGHT MEAN "
        f"ret_from_open of that session's eligible universe. On those sessions (iii) is a purely "
        f"cross-sectional statement. The '{SPLIT_REAL_INDEX}' split is the subset measured against "
        "the real index."
    )
    notes.append(
        "UNIVERSE: universe_daily included rows where present, else the symbol set present in "
        f"bars_1m that session ({len(fallback_dates)} fallback sessions - listed in the JSON). Where "
        "universe_daily IS present its included count ramps 0 -> 50 -> 100 -> 200 across 2026-07/08 "
        "(the watchlist-cap rollout), so the eligible-set SIZE is not constant across the window and "
        "the breadth split's denominator changes with it."
    )
    notes.append(
        "SURVIVORSHIP: the symbol set is whatever bars_1m holds today (~200 names); names that left "
        "the index or delisted are absent, which biases every cell optimistically."
    )
    notes.append(
        "ATR% TERCILE SPLIT (pre-registered 2026-09-12) IS DESCRIPTIVE, NOT TRADEABLE: "
        + ATR_SPLIT_DEFINITION
        + ". It SPLITS the registered result and adds nothing to it - no parameter moved, no variant "
        "was added, no trade was added or removed and no cell is selected, so it is not a new trial. "
        f"{atr_block['n_symbol_days_without_atr']} of {atr_block['n_symbol_days']} measured "
        f"symbol-days lack {atr_lookback} prior daily sessions in bars_1d and sit in "
        f"'{SPLIT_ATR_UNCLASSIFIED}' rather than being dropped, so the four ATR cells sum to "
        f"'{SPLIT_ALL}' exactly."
    )
    notes.append(
        "ATR% SURVIVORSHIP runs the SAME way as the price survivorship above and one way further: "
        "the terciles are cut over the names bars_1m holds TODAY under today's index membership "
        "applied backwards, so the high-ATR cell holds the SURVIVING high-volatility names, not the "
        "high-volatility names of the time. A name whose volatility took it out of the universe is "
        "not in the population at all."
    )
    notes.append(
        "T-STAT OVERSTATES INDEPENDENCE: up to 5 picks share each session and each regime, so the "
        "per-trade t-stat treats correlated draws as independent. The CPCV positive-split share, "
        "computed on per-SESSION means, is the check that does not."
    )
    notes.append(f"SECTOR SOURCE (variant C): {sector_source}")
    notes.append(
        "CATALYST SPLIT COVERAGE: catalyst_watchlist "
        f"{cat_cov['catalyst_watchlist_first_d']} -> {cat_cov['catalyst_watchlist_last_d']} "
        f"({cat_cov['catalyst_watchlist_rows']} rows); news_clusters "
        f"{cat_cov['news_clusters_first_seen']} -> {cat_cov['news_clusters_last_seen']} "
        f"({cat_cov['news_clusters_rows']} rows). The split cells cover ONLY trades on or after "
        f"{cat_cov['split_covered_from']}; before that the flag would measure the absence of the "
        "feed, so those trades are in neither catalyst cell."
    )
    if years_without_calendar:
        notes.append(
            "CALENDAR: config/calendar has no year file for "
            + ", ".join(str(y) for y in sorted(years_without_calendar))
            + " (only 2024-2026 are present), so NSECalendar cannot confirm those sessions. Sessions "
            "are therefore taken from bars_1m's own distinct dates, which are trading days by "
            "construction; calendar confirmation was applied for every year that does have a file."
        )
    if non_trading_dates:
        notes.append(
            "CALENDAR MISMATCH: bars_1m holds bars on "
            f"{len(non_trading_dates)} date(s) NSECalendar calls non-trading: "
            + ", ".join(sorted(non_trading_dates)[:10])
            + " - both are WEEKEND Union-Budget special sessions carrying a full tape (~200 symbols, "
            "~75k bars each). The calendar YAML records only Diwali muhurat under `special_sessions`, "
            "so `is_trading_day` calls a weekend Budget session non-trading. They are real sessions "
            "and are KEPT in the study; the mismatch is a gap in config/calendar, not in the data."
        )

    rt_pct = float(cost_model.breakeven_pct(Decimal(base["reference_notional_inr"]), base["product"]))
    doc: dict[str, Any] = {
        "meta": {
            "script": "scripts/backtest_tdc.py",
            "strategy_id": "tdc",
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "window": {"start": str(start), "end": str(end)},
            "n_sessions": len(all_sessions),
            "first_session": str(min(all_sessions)) if all_sessions else None,
            "last_session": str(max(all_sessions)) if all_sessions else None,
            "params": {k: (str(v) if isinstance(v, Decimal) else v) for k, v in base.items()},
            "variants": {k: v for k, v in var_defs.items()},
            "pre_registered_hypothesis": PRE_REGISTERED_HYPOTHESIS,
            "parameter_sweep_run": False,
            "variant_selection_performed": False,
            "entry_convention": "OPEN of the bar immediately after T (never the decision bar close)",
            "exit_convention": (
                "first 1m CLOSE below VWAP(T)*(1 - stop_pct/100) -> fill at the FOLLOWING bar OPEN; "
                "else the 15:15 bar CLOSE (or the last bar at or before 15:15)"
            ),
            "product": base["product"],
            "reference_notional_inr": base["reference_notional_inr"],
            "cost_round_trip_pct": round(rt_pct, 6),
            "cost_slippage": (
                f"{base['slippage_ticks_per_side']} tick of Rs {base['tick_size_inr']} each side, "
                "charged as 2*tick/entry_px*100 percent"
            ),
            "sector_source": sector_source,
            "promotion_rule": (
                "mean net % > 0 AND t > 2 AND n >= 200 AND CPCV positive-split share >= 0.60"
            ),
        },
        "coverage": {
            "bars_1m_by_year": per_year,
            "universe_daily_dates_present": sorted(str(d) for d in universe),
            "universe_daily_absent_fallback_dates": sorted(str(d) for d in fallback_dates),
            "n_universe_daily_absent_fallback_dates": len(fallback_dates),
            "real_index_sessions": {
                "n": len(real_index_dates),
                "first": str(min(real_index_dates)) if real_index_dates else None,
                "last": str(max(real_index_dates)) if real_index_dates else None,
            },
            "catalyst_sources": cat_cov,
            "atr_pct_tercile_split": atr_block,
        },
        "diagnostics": diagnostics,
        "results": {},
        "notes": notes,
    }

    for name, trades in trades_by_variant.items():
        p = {**base, **var_defs[name]}
        doc["results"][name] = {
            "params": {k: (str(v) if isinstance(v, Decimal) else v) for k, v in p.items()},
            "n_trades": len(trades),
            "splits": {
                cell: metrics(cell_trades, p)
                for cell, cell_trades in split_cells(trades, p).items()
            },
        }
    return doc, trades_by_variant


def _catalyst_at(
    symbol: str,
    d: date,
    decision_time: str,
    watchlist: set[tuple[date, str]],
    news_by_symbol: dict[str, list[datetime]],
    sessions: Sequence[date],
) -> bool:
    """True when the symbol carried a catalyst KNOWN BY T on ``d``.

    Two sources: (1) a ``catalyst_watchlist`` row for ``(d, symbol)``; (2) a ``news_clusters`` row
    naming the symbol whose ``first_seen`` falls between the PRIOR session's 15:00 IST (catching
    overnight and pre-open flow, no older news) and T on ``d`` (nothing the decision could not see).
    """
    if (d, symbol) in watchlist:
        return True
    stamps = news_by_symbol.get(symbol)
    if not stamps:
        return False
    i = bisect_left(list(sessions), d)
    prev = sessions[i - 1] if i > 0 else d - timedelta(days=1)
    lo = datetime.combine(prev, time(15, 0), tzinfo=IST)
    hi = datetime.combine(d, _to_time(decision_time), tzinfo=IST)
    j = bisect_left(stamps, lo)
    return j < len(stamps) and stamps[j] <= hi


# =============================================================================== rendering
_ASCII_MAP = str.maketrans({
    "—": "-", "–": "-", "‘": "'", "’": "'", "“": '"', "”": '"',
    "→": "->", "≥": ">=", "≤": "<=", "×": "x", "₹": "Rs ", " ": " ",
})


def _ascii(text: str) -> str:
    return text.translate(_ASCII_MAP).encode("ascii", "replace").decode("ascii")


def _f(v: Any, width: int = 9, prec: int = 4) -> str:
    if v is None:
        return "-".rjust(width)
    if isinstance(v, float):
        return f"{v:+.{prec}f}".rjust(width)
    return str(v).rjust(width)


def render_text(doc: dict[str, Any]) -> str:
    """The printed report, folded to ASCII (the Windows console is cp1252)."""
    m = doc["meta"]
    out: list[str] = []
    add = out.append
    add("=" * 118)
    add("tdc PRE-REGISTERED BACKTEST - trend-day continuation, intraday")
    add("(`tdc` pre-registration, 2026-09-04)")
    add("=" * 118)
    add(f"generated        : {m['generated_at']}")
    add(f"window           : {m['window']['start']} -> {m['window']['end']}  "
        f"({m['n_sessions']} sessions, {m['first_session']} .. {m['last_session']})")
    add(f"hypothesis       : {m['pre_registered_hypothesis']}   "
        f"(sweep run: {m['parameter_sweep_run']}, variant selection: {m['variant_selection_performed']})")
    add(f"entry            : {m['entry_convention']}")
    add(f"exit             : {m['exit_convention']}")
    add(f"cost             : {m['cost_round_trip_pct']:.4f}% {m['product']} round trip at Rs "
        f"{m['reference_notional_inr']} + {m['cost_slippage']}")
    add(f"promotion rule   : {m['promotion_rule']}")
    add("")

    add("-" * 118)
    add("STEP 1 - SAMPLE SHAPE (bars_1m coverage per calendar year; 'full session' = >= 330 of 375 bars)")
    add("-" * 118)
    add("  year |  sessions | symbols | symbols full | symbol-days | sym-days full | first        last")
    add("  -----+-----------+---------+--------------+-------------+---------------+---------------------------")
    for y in doc["coverage"]["bars_1m_by_year"]:
        add(f"  {y['year']:<4} | {y['sessions']:>9} | {y['symbols']:>7} | {y['symbols_full_session']:>12} | "
            f"{y['symbol_days']:>11} | {y['symbol_days_full_session']:>13} | {y['first_session']}  {y['last_session']}")
    cov = doc["coverage"]
    add("")
    add(f"  universe_daily present on {len(cov['universe_daily_dates_present'])} session(s); "
        f"{cov['n_universe_daily_absent_fallback_dates']} session(s) fell back to the bars_1m symbol set.")
    ri = cov["real_index_sessions"]
    add(f"  real '{m['params']['index_symbol']}' 1m series available on {ri['n']} session(s) "
        f"({ri['first']} -> {ri['last']}); earlier sessions use the equal-weight universe proxy.")
    cc = cov["catalyst_sources"]
    add(f"  catalyst split covers trades on/after {cc['split_covered_from']} "
        f"(watchlist {cc['catalyst_watchlist_first_d']}..{cc['catalyst_watchlist_last_d']}, "
        f"news_clusters {str(cc['news_clusters_first_seen'])[:10]}..{str(cc['news_clusters_last_seen'])[:10]}).")
    add("")
    for t_lit, dg in sorted(doc["diagnostics"].items()):
        if t_lit == "variants":
            continue
        add(f"  T={t_lit}: {dg['n_sessions']} sessions, {dg['n_symbol_days']} symbol-days, "
            f"{dg['rvol_insufficient_history_symbol_days']} symbol-days SKIPPED for insufficient "
            f"RVOL history, real index on {dg['n_sessions_real_index']} / proxy on "
            f"{dg['n_sessions_proxy_index']}")
    add("")
    for name, dg in sorted(doc["diagnostics"].get("variants", {}).items()):
        add(f"  [{name:<9}] T={dg['decision_time']}  signal days={dg['n_signal_days']:>4}  "
            f"selected={dg['n_selected_symbol_days']:>5}  trades={dg['n_trades']:>5}  "
            f"dropped(no entry bar)={dg['n_dropped_missing_entry_bar']:>3}  "
            f"exits={dg['exit_reason_counts']}")
    add("")

    add("-" * 118)
    add("STEP 2 - RESULTS: EVERY VARIANT x EVERY SPLIT (all reported; none selected)")
    add("-" * 118)
    add("  variant   | split                          |     n | mean net% |  med net% |   win% |  t-stat | CPCV+ | promotable")
    add("  ----------+--------------------------------+-------+-----------+-----------+--------+---------+-------+-----------")
    for name in doc["results"]:
        block = doc["results"][name]
        for cell, s in block["splits"].items():
            win = "-" if s["win_rate"] is None else f"{s['win_rate'] * 100:.1f}"
            share = s["cpcv"]["positive_share"]
            sh = "-" if share is None else f"{share * 100:.0f}%"
            add(f"  {name:<9} | {cell:<30} | {s['n']:>5} | {_f(s['mean_net_pct'], 9)} | "
                f"{_f(s['median_net_pct'], 9)} | {win:>6} | {_f(s['t_stat'], 7, 2)} | {sh:>5} | "
                f"{str(s['promotable'])}")
        add("  " + "-" * 114)
    add("")

    ab = doc["coverage"]["atr_pct_tercile_split"]
    add("-" * 118)
    add("STEP 2B - ATR% TERCILE SPLIT (pre-registered 2026-09-12; DESCRIPTIVE, full-sample cuts)")
    add("-" * 118)
    add(f"  definition : {ab['definition']}")
    add(f"  cuts       : {ab['cuts']}   population: {ab['n_symbol_days']} measured symbol-days, "
        f"{ab['n_symbol_days_without_atr']} without {m['params']['atr_lookback_sessions']} prior "
        f"daily sessions")
    add(f"  ATR% spread: min {ab['atr_pct_min']} / median {ab['atr_pct_median']} / "
        f"max {ab['atr_pct_max']}")
    add("")
    add("  variant   | ATR cell                 |     n | mean gross% |  med gross% | mean net% |"
        "  med net% |   win% |  t-stat | CPCV+ | promotable")
    add("  ----------+--------------------------+-------+-------------+-------------+-----------+"
        "-----------+--------+---------+-------+-----------")
    for name in doc["results"]:
        for cell in ATR_REPORT_CELLS:
            s = doc["results"][name]["splits"][cell]
            win = "-" if s["win_rate"] is None else f"{s['win_rate'] * 100:.1f}"
            share = s["cpcv"]["positive_share"]
            sh = "-" if share is None else f"{share * 100:.0f}%"
            add(f"  {name:<9} | {cell:<24} | {s['n']:>5} | {_f(s['mean_gross_pct'], 11)} | "
                f"{_f(s['median_gross_pct'], 11)} | {_f(s['mean_net_pct'], 9)} | "
                f"{_f(s['median_net_pct'], 9)} | {win:>6} | {_f(s['t_stat'], 7, 2)} | {sh:>5} | "
                f"{str(s['promotable'])}")
        add("  " + "-" * 114)
    add("")

    add("-" * 118)
    add("STEP 3 - PROMOTABLE CELLS (the pre-registered boolean, verbatim; no other claim is made)")
    add("-" * 118)
    hits = [
        (n, c) for n, b in doc["results"].items()
        for c, s in b["splits"].items() if s["promotable"]
    ]
    if hits:
        for n, c in hits:
            s = doc["results"][n]["splits"][c]
            add(f"  promotable=True: variant {n}, split {c} "
                f"(n={s['n']}, mean net {s['mean_net_pct']}%, t={s['t_stat']}, "
                f"CPCV+ {s['cpcv']['positive_share']})")
    else:
        add("  promotable=True in NO variant x split cell.")
    add("")

    add("-" * 118)
    add("NOTES / CAVEATS (reported, not massaged)")
    add("-" * 118)
    for n in doc["notes"]:
        add(f"  * {n}")
    add("")
    return _ascii("\n".join(out))


# =============================================================================== CLI
def _date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _default_db() -> Path:
    return repo_root() / "data" / "market.duckdb"


def _default_out() -> Path:
    return repo_root() / "data" / "reports" / "backtest_tdc_2026-09-04.json"


def build_calendar() -> NSECalendar | None:
    try:
        return NSECalendar(config_dir() / "calendar", Clock(), strict=False)
    except Exception:  # noqa: BLE001 - calendar confirmation is a check, never load-bearing
        return None


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog="backtest_tdc",
        description=(
            "tdc pre-registered backtest (one PARAMS dict, one fixed VARIANTS list, no sweep and no "
            "selection). Read-only against bars_1m; refuses to run if the DuckDB file is missing or "
            "locked by the engine."
        ),
    )
    ap.add_argument("--db", type=Path, default=_default_db(), help="path to market.duckdb")
    ap.add_argument("--start", type=_date, default=date(2000, 1, 1), help="window start (YYYY-MM-DD)")
    ap.add_argument("--end", type=_date, default=date.today() - timedelta(days=1),
                    help="window end, inclusive (default: yesterday - today's tape is partial)")
    ap.add_argument("--out", type=Path, default=None, help="JSON results path")
    return ap


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    db_path = Path(args.db)
    try:
        conn = open_readonly(db_path)
    except DbUnopenable as exc:
        print(f"backtest_tdc: REFUSING TO RUN.\n  {_ascii(str(exc))}", file=sys.stderr)
        return 2

    out_path = Path(args.out) if args.out is not None else _default_out()
    try:
        doc, _trades = run_study(
            conn,
            start=args.start,
            end=args.end,
            cost_model=CostModel.from_config(),
            calendar=build_calendar(),
        )
    except ValueError as exc:
        print(f"backtest_tdc: {exc}", file=sys.stderr)
        return 2
    finally:
        conn.close()

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(doc, indent=2, default=str), encoding="utf-8")
    print(render_text(doc))
    print(f"JSON results -> {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
