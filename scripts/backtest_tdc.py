#!/usr/bin/env python
"""``tdc`` PRE-REGISTERED backtest harness (IMPLEMENTATION_PLAN.md `tdc` pre-registration, 2026-09-04).

Trend-day continuation, intraday. This is the §8.6 owner-gate input for hypothesis **H1**:

    on a session where a NIFTY200 stock shows ACCEPTANCE above its opening range with sustained
    PARTICIPATION and RELATIVE STRENGTH by late morning, the move continues into the close net of
    MIS costs.

===============================================================================================
PRE-REGISTRATION / MULTIPLICITY DISCIPLINE - READ FIRST
===============================================================================================
**NO PARAMETER SWEEP IS RUN BY THIS SCRIPT, AND NONE WAS RUN BEFORE IT.** There is exactly one
:data:`PARAMS` dict and one :data:`VARIANTS` dict, both fixed at import time and both transcribed
from the plan paragraph. Every variant in :data:`VARIANTS` is REPORTED; none is SELECTED, ranked,
or promoted over another. The plan's robustness list (``T`` in {10:30, 12:00}; ``rvol_min`` in
{1.2, 2.0}; ``stop`` in {0.5%}) exists precisely so the reader can see whether H1's number is a
knife edge - it is not a menu. The CLI exposes no flag that changes a signal parameter; adding one
would silently convert this study from "one pre-registered rule plus its stated robustness checks"
into a grid search, and would invalidate every number printed here.

``promotable`` is the plan's own boolean and nothing more: ``mean net % > 0`` AND ``t > 2`` AND
``n >= 200`` AND ``CPCV positive-split share >= 0.60``. Anything less is a refutation, and this
script says so in exactly those terms; no other language about deployment appears anywhere in it.

===============================================================================================
THE RULE (transcribed from the plan; every clause is evaluated at the T bar CLOSE)
===============================================================================================
For each eligible symbol on each session, at the decision bar ``T`` (default the 11:00 IST bar):

* **(i) Acceptance.** ``close(T) > VWAP(T)`` AND ``close(T) > OR high`` AND each of the 15 one-minute
  closes immediately before T (10:45..10:59 when T = 11:00) is above VWAP.

  - ``VWAP`` is the session volume-weighted average of the typical price ``(high+low+close)/3``,
    cumulative from the 09:15 bar. ``VWAP(T)`` is that cumulative value AT the T bar.
  - ``OR high`` is ``max(high)`` over the bars ``09:15..09:29`` - the first fifteen 1m bars, i.e.
    the 09:15-09:30 opening range. ``ts_minute`` is the bar's minute START (``engine.core.types.Bar``),
    so the 09:29 bar is the last bar of that range and the 09:30 bar is not in it.
  - PINNED INTERPRETATION (the plan's phrase "the last 15 one-minute closes all > VWAP" does not say
    *which* VWAP): each of those 15 closes is compared with the RUNNING session VWAP AT ITS OWN
    MINUTE, ``close(m) > VWAP(m)``, not with the single value ``VWAP(T)``. That is the causal reading
    (every comparison uses only bars at or before its own minute), and it is what "acceptance" means
    as a market statement - price HELD above the volume-weighted average through the stretch. The
    alternative reading (all 15 closes vs the scalar ``VWAP(T)``) was NOT computed, so no selection
    between the two has taken place. This choice is a documented resolution of an ambiguity in the
    pre-registration text, not a tuned parameter.

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
15:15 bar's close (or the last bar at or before 15:15). No target. Variant B removes the stop.

===============================================================================================
LOOK-AHEAD DISCIPLINE (the one class of error that would make every number here a fiction)
===============================================================================================
* Every input to conditions (i)-(iii) is computed from bars with ``ts_minute <= T`` ONLY. The
  feature SQL filters ``time <= T`` before the cumulative window functions run, so a bar after T is
  not merely unused - it is not in the relation.
* The ``rel_volume_tod`` denominator uses STRICTLY PRIOR sessions (the 20 sessions before this one
  in the global session list). Today's own volume never enters its own median.
* Entry is the OPEN of the bar AFTER T. The decision bar's own close is never a fill price.
* The VWAP-loss exit triggers on a bar CLOSE and fills at the NEXT bar's OPEN - the first price an
  order placed on that close could actually reach. It is never filled at the triggering close.
* The 15:15 exit uses the 15:15 bar's close (or the last bar at or before 15:15).
* The breadth split is computed from the same ``T``-bar snapshot as the signal, not from the day's
  outcome.

===============================================================================================
COSTS
===============================================================================================
``CostModel.breakeven_pct(notional, "MIS")`` - ONE full round trip, statutory fees PLUS the measured
bid-ask spread (WO-2), at :data:`PARAMS`\\ ``["reference_notional_inr"]`` - plus ONE TICK of slippage
on EACH side, charged as ``2 * tick / entry_px * 100`` percent, so the slippage term is larger for a
cheap stock exactly as it is in life. MIS, never CNC: this is an intraday leg squared off the same
session. ``breakeven_pct`` (not ``fee_breakeven_pct``) is deliberate - the fees-only view is the
contract-note anchor, not a viability number - and spread is not modelled twice: the tick slippage is
the *queue* cost of crossing, the spread inside ``breakeven_pct`` is the quoted half-spread on each
leg, and both are real.

===============================================================================================
INDEX PROXY (owner/manager-directed 2026-09-04, recorded as a deviation from the plan text)
===============================================================================================
The plan says "NIFTY 50 1m for RS". ``bars_1m`` carries "NIFTY 50" only from 2026-07-22, while stock
1m history begins 2023-07-17. For sessions before the index series starts, condition (iii)'s index
return is the EQUAL-WEIGHT MEAN of ``ret_from_open(T)`` across that session's own eligible universe.
That is a proxy, not the index: it is equal- rather than free-float-weighted, and its constituent set
is this study's eligible set rather than the NIFTY 50's fifty names. Because a stock is measured
against the mean of its own cohort, condition (iii) becomes a strictly cross-sectional statement on
proxy days. Every cell is therefore ALSO reported for the ``real_nifty50_index_only`` split, which is
the subset of sessions where the true index series was used - the proxy's effect is visible by
comparing the two, and no number in this report depends on the reader trusting the proxy.

===============================================================================================
DATA ACCESS
===============================================================================================
Read-only, always (``duckdb.connect(..., read_only=True)``): ``MarketStore.open()`` runs schema DDL
and is a WRITER, so it is deliberately not used, exactly as ``scripts/backtest_hi52.py`` and
``scripts/g1_entity_sample.py`` do it. DuckDB takes a file lock even for a read-only attach, so the
engine service must be stopped; if the file is missing or locked this script REFUSES with exit code 2
rather than degrading. Nothing here ever writes to the store.

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
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

_REPO_SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if _REPO_SRC not in sys.path:  # pragma: no cover - loose-script shim
    sys.path.insert(0, _REPO_SRC)

import engine  # noqa: E402,F401,I001  native import-order guard: sklearn's OpenMP runtime MUST be
#                                      established before skfolio/cvxpy loads (engine._preload). The
#                                      isort suppression keeps it FIRST; re-sorting would sink it
#                                      below the third-party block and re-open the segfault.
import duckdb  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from engine.core.calendar import NSECalendar  # noqa: E402
from engine.core.clock import IST, Clock  # noqa: E402
from engine.core.config import config_dir, repo_root  # noqa: E402
from engine.learning.validate import cpcv_splits  # noqa: E402
from engine.marketdata.reconcile import DEFAULT_TICK_SIZE  # noqa: E402
from engine.strategy.cost_model import CostModel  # noqa: E402

# ================================================================================================
# THE ONE PRE-REGISTERED PARAMETER SET. Transcribed from IMPLEMENTATION_PLAN.md, the `tdc`
# pre-registration paragraph (the one immediately before WO-20). Not a knob, not a grid, not
# reachable from the CLI.
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
    # --- the plan's promotion rule (a boolean, not a recommendation)
    "promote_min_n": 200,
    "promote_min_t": 2.0,
    "promote_min_cpcv_positive_share": 0.60,
    # --- CPCV (engine.learning.validate defaults: 6 folds / 2 test folds / purge 5 / embargo 5)
    "cpcv_purge_obs": 5,
    "cpcv_embargo_obs": 5,
}

# ================================================================================================
# EVERY VARIANT IS REPORTED. NONE IS SELECTED. Each value is an override applied on top of PARAMS.
# H1 is the hypothesis; B is the owner's observed pattern; the rest are the plan's stated robustness
# checks; C is the manager-added sector-confirmation leg (computable because `sector_map` exists).
# ================================================================================================
VARIANTS: dict[str, dict[str, Any]] = {
    "H1":            {},                                   # the pre-registered hypothesis
    "B":             {"stop_vwap_pct": None},              # hold to 15:15, no stop
    "T_1030":        {"decision_time": "10:30"},           # robustness: earlier decision bar
    "T_1200":        {"decision_time": "12:00"},           # robustness: later decision bar
    "RVOL_1.2":      {"rvol_min": 1.2},                    # robustness: looser participation
    "RVOL_2.0":      {"rvol_min": 2.0},                    # robustness: tighter participation
    "STOP_0.5":      {"stop_vwap_pct": 0.5},               # robustness: wider VWAP-loss stop
    "C_sector":      {"sector_confirm": True},             # H1 + >=2 OTHER same-sector passers at T
}

#: Trial count cited to the reader. ONE pre-registered rule (H1); every other entry in
#: :data:`VARIANTS` is a reported robustness cell or a separately-stated construct, never a
#: competitor H1 is picked from.
PRE_REGISTERED_HYPOTHESIS = "H1"

SPLIT_ALL = "all"
SPLIT_INDEX_UP = "index_up_at_T"
SPLIT_INDEX_DOWN = "index_down_at_T"
SPLIT_BREADTH_HIGH = "breadth_ge_50pct"
SPLIT_BREADTH_LOW = "breadth_lt_50pct"
SPLIT_REAL_INDEX = "real_nifty50_index_only"
SPLIT_PLAN_WINDOW = "plan_window_2025_07_10_onward"
SPLIT_CATALYST_TRUE = "catalyst_at_T_true"
SPLIT_CATALYST_FALSE = "catalyst_at_T_false"

#: The plan text's stated 1m floor. The store actually holds 1m bars from 2023-07-17; this date is
#: kept only so the plan's literal window is reported as its own split cell.
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


# =============================================================================== read-only DB access
class DbUnopenable(RuntimeError):
    """The DuckDB file is missing, locked by the engine, or otherwise not readable."""


def open_readonly(db_path: Path) -> duckdb.DuckDBPyConnection:
    """Attach ``db_path`` READ-ONLY, or raise :class:`DbUnopenable` with an actionable message.

    Same posture as ``scripts/backtest_hi52.py``: ``MarketStore.open()`` runs schema DDL and is a
    writer, so it is never used here. A running engine holds the file lock and makes this fail, which
    is the refusal the plan asks for rather than a half-run against a moving store.
    """
    p = Path(db_path)
    if not p.exists():
        raise DbUnopenable(
            f"no such database file: {p}\n"
            "  Pass --db with the path to market.duckdb (default: <repo>/data/market.duckdb)."
        )
    if p.is_dir():
        raise DbUnopenable(f"--db points at a directory, not a DuckDB file: {p}")
    try:
        return duckdb.connect(str(p), read_only=True)
    except Exception as exc:  # noqa: BLE001 - every failure mode becomes one clear refusal
        raise DbUnopenable(
            f"cannot open read-only: {p}\n"
            f"  reason: {type(exc).__name__}: {exc}\n"
            "  DuckDB is single-writer and takes a file lock: if the mt-engine service is running it\n"
            "  owns market.duckdb and nothing else can attach, even read-only. Stop the engine (or\n"
            "  run this when it is stopped) and retry. This script NEVER opens the store read-write."
        ) from exc


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

    For T = 11:00 and 15 bars this is ``("10:45", "10:59")`` - the plan's own window. It is derived
    from T rather than hard-coded so the T=10:30 / T=12:00 robustness cells shift with it.
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
    The ``time <= T`` filter is applied BEFORE the cumulative window functions, so no bar after the
    decision minute is in the relation the VWAP/cumulative-volume sums are taken over - look-ahead is
    structurally impossible here, not merely avoided.
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

    Only the selected symbol-days are fetched (a few thousand), so the exit scan never materializes
    the full post-T tape.
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

    ``sector_map`` is snapshotted weekly and its earliest snapshot is recent, so for any session
    before the first snapshot the earliest snapshot is applied BACKWARDS. That is a point-in-time
    proxy, labelled as such everywhere it appears.
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

    Both sources are recent additions, so ``coverage`` carries the date range over which the
    catalyst split is meaningful; outside it the flag would only be measuring the absence of the
    feed, and the split cells are therefore restricted to that range.
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

    "Full session" = >= 330 of the 375 one-minute bars, i.e. a symbol that traded essentially all day
    rather than one with a sparse or truncated tape.
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


# =============================================================================== rel_volume_tod
def rvol_medians(
    cumvol: dict[str, dict[date, float]],
    sessions: list[date],
    *,
    lookback: int,
    min_valid: int,
) -> tuple[dict[str, dict[date, float]], int]:
    """``symbol -> d -> median prior cumulative volume to T``; plus the count of SKIPPED symbol-days.

    The window is the ``lookback`` sessions immediately preceding ``d`` in the GLOBAL session list -
    strictly prior, so today's own volume can never enter its own denominator. A symbol-day whose
    window holds fewer than ``min_valid`` usable priors gets NO entry here and is counted as a skip:
    the plan says skip, not "use a thinner median".
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
    ``sector_confirm_min_others`` OTHER symbols in the SAME sector also passed (i)-(iii) at T. The
    confirmation is computed over the passer set, so it is a statement about the sector's behaviour
    at T, never about the day's outcome.
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

    Entry is the OPEN of the ``entry_time`` bar - the bar AFTER T - and the bar must be present at
    exactly that minute; a data gap there is a skipped trade, never a fill at some other price.

    Exit: scanning from the entry bar forward, the FIRST bar whose CLOSE is below
    ``vwap_t * (1 - stop_pct/100)`` fills at the NEXT bar's OPEN. The trigger is a close and the fill
    is the following open - a trade is never filled at the price that triggered it. If no bar
    triggers (or ``stop_pct`` is None, variant B), the exit is the squareoff bar's CLOSE, where the
    squareoff bar is the last bar at or before ``squareoff_time``.
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

    The slippage term is price-relative on purpose: one tick is a bigger percentage of a Rs 90 stock
    than of a Rs 4,000 one, and the study should feel that.
    """
    rt = float(cost_model.breakeven_pct(Decimal(params["reference_notional_inr"]), params["product"]))
    tick = float(Decimal(params["tick_size_inr"]))
    slip = 2.0 * int(params["slippage_ticks_per_side"]) * tick / entry_px * 100.0
    return rt + slip


# =============================================================================== metrics
def _purged_kfold_splits(
    n_obs: int, *, n_folds: int = 6, purge: int = 5, embargo: int = 5
) -> list[tuple[np.ndarray, np.ndarray]]:
    """Fallback when skfolio is unavailable or its precondition cannot be met: contiguous K-fold test
    blocks with the same purge/embargo guarantee ``cpcv_splits`` enforces. Not combinatorial, and
    labelled as such wherever it is used (mirrors ``scripts/backtest_hi52.py``)."""
    if n_obs < n_folds * 2:
        return []
    out: list[tuple[np.ndarray, np.ndarray]] = []
    for test in np.array_split(np.arange(n_obs), n_folds):
        if test.size == 0:
            continue
        lo, hi = int(test.min()), int(test.max())
        all_idx = np.arange(n_obs)
        train = all_idx[(all_idx < lo - purge) | (all_idx > hi + embargo)]
        if train.size == 0:
            continue
        out.append((train, test))
    return out


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

    Purge and embargo are the ``engine.learning.validate`` §6.4 defaults (5 observations each), which
    is what ``hi52`` inherited. ``hi52`` raised them to its holding horizon because a 20-session swing
    overlaps its neighbours; a ``tdc`` trade is opened and closed inside ONE session, so consecutive
    observations do not overlap at all and the default is already strictly more conservative than the
    leakage it guards against.
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
    """``n``, mean/median net %, win rate, t-stat, CPCV positive share, and the plan's boolean.

    ``t = mean / (sd / sqrt(n))`` with the SAMPLE standard deviation (ddof=1), over TRADES. The
    honest caveat is stated in the report rather than hidden: the day's five picks are correlated, so
    this t-stat overstates the effective sample size; the CPCV share is the check that does not.
    """
    net = [t.net_pct for t in trades]
    n = len(net)
    base: dict[str, Any] = {
        "n": n, "mean_net_pct": None, "median_net_pct": None, "win_rate": None,
        "t_stat": None, "mean_gross_pct": None, "mean_cost_pct": None,
    }
    if n:
        base["mean_net_pct"] = round(statistics.fmean(net), 4)
        base["median_net_pct"] = round(statistics.median(net), 4)
        base["win_rate"] = round(sum(1 for v in net if v > 0) / n, 4)
        base["mean_gross_pct"] = round(statistics.fmean([t.gross_pct for t in trades]), 4)
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
    """Every mandatory split, as named cells. The pooled number is never the only number."""
    b = float(params["breadth_trend_day_min"])
    cat = [t for t in trades if t.catalyst_split_covered]
    return {
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
    # The post-T tape is fetched once per decision time up to ONE squareoff cutoff, so a variant that
    # moved the squareoff would silently have its tape truncated. No pre-registered variant does;
    # this makes that fact enforced rather than assumed.
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

    # One feature pull per DISTINCT decision time (three, for T = 10:30 / 11:00 / 12:00).
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

        # The post-T tape is fetched ONCE per decision time, for the UNION of every variant's picks:
        # the variants at a given T differ only in which symbol-days they select, and re-scanning
        # bars_1m per variant would repeat the most expensive query in the study eight times.
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

    # ---------------------------------------------------------------- notes / caveats
    notes.append(
        "NO PARAMETER SWEEP was run and no variant was selected: PARAMS is one dict, VARIANTS is a "
        f"fixed list of {len(var_defs)} REPORTED cells, and the pre-registered hypothesis is "
        f"{PRE_REGISTERED_HYPOTHESIS}. 'promotable' is the plan's boolean (mean net > 0, t > 2, "
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

    Two sources, per the manager's split definition: (1) a ``catalyst_watchlist`` row for that
    ``(d, symbol)``; (2) a ``news_clusters`` row naming the symbol whose ``first_seen`` falls between
    the PRIOR session's 15:00 IST and T on ``d``. The lower bound is the prior session's 15:00 so the
    window catches the overnight and pre-open flow without reaching back into an older session's
    news; the upper bound is T, so nothing the decision could not have seen is counted.
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
    add("(IMPLEMENTATION_PLAN.md `tdc` pre-registration, 2026-09-04)")
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

    add("-" * 118)
    add("STEP 3 - PROMOTABLE CELLS (the plan's boolean, verbatim; no other claim is made)")
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
