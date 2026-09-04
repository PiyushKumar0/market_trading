#!/usr/bin/env python
"""``candles`` PRE-REGISTERED price-action battery (runbooks/briefs/candles_backtest_brief_2026-09-04.md).

Owner question: "can't we track the candles and trade based on their movements?" This script answers
it with evidence: FIVE long-only 5-minute candle rules x TWO exit styles = TEN cells, every cell
reported, none selected. ``scripts/backtest_tdc.py`` is the immediate precedent and this harness is
deliberately its sibling - same read-only DuckDB posture, same ``time <= signal`` filtering BEFORE
any window function, same :func:`trade_cost_pct`, same CPCV wiring, same report shape.

===============================================================================================
PRE-REGISTRATION / MULTIPLICITY DISCIPLINE - READ FIRST
===============================================================================================
**NO PARAMETER SWEEP IS RUN BY THIS SCRIPT, AND NONE WAS RUN BEFORE IT.** There is exactly one
:data:`PARAMS` dict, one :data:`RULES` dict and one :data:`EXITS` dict, all three fixed at import
time and all three transcribed from the brief. Every rule x exit cell is REPORTED; none is SELECTED,
ranked or promoted over another. The CLI exposes no flag that changes a signal parameter, a
threshold or an exit; adding one would convert this study from "five pre-registered rules" into a
grid search and would invalidate every number printed here.

``promotable`` is the brief's own boolean and nothing more: ``mean net % > 0`` AND ``t > 2`` AND
``n >= 200`` AND ``CPCV positive-split share >= 0.60``. Anything less is a refutation, and this
script says so in exactly those terms; no other language about deployment appears anywhere in it.

===============================================================================================
THE BARS
===============================================================================================
``bars_1m.ts_minute`` is the bar's minute START (``engine.core.types.Bar``). ``mfo`` throughout is
"minutes from the 09:15 open", so the 09:15 bar is ``mfo = 0`` and the 15:29 bar is ``mfo = 374``.

* **5m bars** are the 09:15-anchored buckets ``[09:15,09:20)``, ``[09:20,09:25)``, ... i.e.
  ``bk = mfo // 5``. ``open`` = the first 1m open in the bucket, ``high`` = max high, ``low`` = min
  low, ``close`` = the last 1m close, ``volume`` = the sum. A bucket carrying fewer than
  :data:`PARAMS`\\ ``["min_1m_bars_per_bucket"]`` (3) one-minute bars is INVALID and is dropped from
  the relation entirely - it is not a bar, so it is neither a signal bar, nor a lag, nor a member of
  the ``medvol20`` window, nor a trail bar.
* **VWAP** is the session typical-price ``(h+l+c)/3`` VWAP accumulated over 1m bars from 09:15, read
  AT the 5m bar's last minute. It is computed at 1m granularity and sampled at the bucket, so a
  bucket that lost a minute still carries the true session VWAP through that minute.
* **medvol20** is the median volume of the 20 VALID 5m bars immediately preceding the signal bar in
  the SAME session; fewer than 8 such bars and the bar cannot be a signal (``n20 >= 8``). Because a
  signal bar therefore always has >= 8 prior valid bars, every rule's 6-bar and 4-bar lag windows
  are guaranteed to be fully populated - no rule can fire on a partial window.

PINNED INTERPRETATIONS (documented resolutions of ambiguity in the brief, NOT tuned parameters -
the alternatives were not computed, so no selection between them took place):

1. **"close minute"** of a 5m bar = the START minute of its LAST constituent 1m bar (the repo's
   ``ts_minute`` convention). The signal window "close minute in [09:45, 14:00]" is therefore
   ``last_mfo BETWEEN 30 AND 285``, i.e. bucket starts 09:45 .. 13:55.
2. **"three consecutive bullish bars, each close > the prior bar's HIGH"** (R2): each of the three
   closes is compared with ITS OWN prior bar's high - ``c[i] > h[i-1]``, ``c[i-1] > h[i-2]``,
   ``c[i-2] > h[i-3]``.
3. **"the session high was set >= 3 bars ago"** (R3): the running max high over bars ``<= i-3``
   equals the running max high over bars ``<= i``. The running max is taken over the session's VALID
   5m bars.
4. **"the 3 bars before the current one ... all stayed above VWAP"** (R3): each of bars ``i-1``,
   ``i-2``, ``i-3`` closed above the running VWAP AT ITS OWN minute - the causal reading, the same
   one ``backtest_tdc.py`` pinned for its acceptance window.
5. **breadth "at 11:00"**: the share of the eligible universe whose last COMPLETED 5m bar before
   11:00 (the bucket ``[10:55,11:00)``) closed above its opening-range high (max high over
   ``[09:15,09:30)``). It is a snapshot at 11:00, never a function of the day's outcome.

===============================================================================================
LOOK-AHEAD DISCIPLINE (the one class of error that would make every number here a fiction)
===============================================================================================
* Every rule clause is evaluated on a COMPLETED 5m bar from bars at or before that bar. The 1m
  relation is filtered to ``mfo <= 284`` (13:59, the last possible signal minute) BEFORE the
  cumulative VWAP window runs, so no bar after the signal window is even in the relation the VWAP
  sums are taken over; the per-bar lags and the ``medvol20`` window are ``ROWS BETWEEN n PRECEDING
  AND 1 PRECEDING``, so the current bar never enters its own denominator and a later bar never
  enters anything.
* ENTRY is the OPEN of the 1m bar at ``signal.last_mfo + 1``. The signal bar's own close is never a
  fill price. If that exact minute has no bar the trade is SKIPPED and counted - never filled at
  some other price.
* E1 stop/target fills are scanned on 1m bars STRICTLY AFTER the entry bar. Within a bar the STOP
  wins ties (both levels touched in one minute -> stop), which is the pessimistic assumption.
* E2's trail trigger is a COMPLETED 5m bar closing below the PRIOR 5m bar's low, filled at the next
  1m OPEN after that bar - never at the close that triggered it.
* The 15:15 exit uses the last 1m close at or before 15:15.
* The breadth and index-regime splits are snapshots at 11:00 and at the signal minute respectively,
  not functions of the outcome.

ONE HONEST EXCEPTION, disclosed rather than hidden: the brief's "at most 5 trades per rule per day,
ranked by volume ratio descending" ranks the day's FIRST-triggers against each other, and a trigger
at 13:30 is in that ranking when the 09:55 trigger is chosen. That is a look-ahead in the DAILY CAP
(never in a fill price). It is pre-registered, so it is applied as written and reported here, and
the per-cell diagnostics carry ``n_first_triggers`` alongside ``n_trades`` so the size of the
discarded tail is visible.

===============================================================================================
COSTS
===============================================================================================
``CostModel.breakeven_pct(notional, "MIS")`` - ONE full round trip, statutory fees PLUS the measured
bid-ask spread (WO-2), at :data:`PARAMS`\\ ``["reference_notional_inr"]`` - plus ONE TICK of slippage
on EACH side, charged as ``2 * tick / entry_px * 100`` percent, so the slippage term is larger for a
cheap stock exactly as it is in life. MIS, never CNC. This is byte-for-byte the ``tdc`` treatment;
``tests/unit/test_backtest_candles.py`` asserts the two functions agree numerically.

===============================================================================================
INDEX PROXY (as tdc; recorded as a deviation from "NIFTY 50 1m")
===============================================================================================
``bars_1m`` carries "NIFTY 50" only from 2026-07-22, while stock 1m history begins 2023-07-17. For
sessions before the index series starts, the index return at the signal minute is the EQUAL-WEIGHT
MEAN of ``close/open_0915 - 1`` across that session's own eligible universe, read on the same 5m
grid. Every cell is ALSO reported for the ``real_nifty50_index_only`` split, which is the subset of
trades measured against the true index.

===============================================================================================
DATA ACCESS
===============================================================================================
Read-only, always (``duckdb.connect(..., read_only=True)``): ``MarketStore.open()`` runs schema DDL
and is a WRITER, so it is deliberately not used, exactly as ``scripts/backtest_tdc.py`` does it. A
running engine holds the file lock, so this script REFUSES with exit code 2 rather than degrading.
Nothing here ever writes to the store.

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
from bisect import bisect_left
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
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
# THE ONE PRE-REGISTERED PARAMETER SET. Transcribed from
# runbooks/briefs/candles_backtest_brief_2026-09-04.md. Not a knob, not a grid, not reachable from
# the CLI.
# ================================================================================================
PARAMS: dict[str, Any] = {
    # --- the bar grid
    "bucket_minutes": 5,
    "session_open": "09:15",              # mfo = 0
    "min_1m_bars_per_bucket": 3,          # fewer -> the bucket is not a bar at all
    "signal_window_start": "09:45",       # on the 5m bar's CLOSE minute (its last 1m bar), inclusive
    "signal_window_end": "14:00",         # inclusive
    "medvol_lookback_bars": 20,           # prior VALID 5m bars, same session
    "medvol_min_bars": 8,                 # fewer -> the bar cannot be a signal
    # --- rule thresholds (each transcribed from the brief's RULES paragraph)
    "r1_breakout_lookback_bars": 6,
    "r1_vol_mult": 2.0,
    "r1_close_in_range_frac": 0.75,
    "r2_vol_mult": 1.5,
    "r3_session_high_min_bars_ago": 3,
    "r3_pullback_bars": 3,
    "r3_vol_mult": 1.2,
    "r4_prior_bars_above_vwap": 6,
    "r4_vwap_touch_mult": 1.0015,
    "r5_prior_bars_below_vwap": 6,
    "r5_vol_mult": 1.5,
    # --- selection / execution
    "max_trades_per_rule_per_day": 5,
    "e1_target_r_multiple": 2.0,
    "squareoff_time": "15:15",
    # --- universe / context
    "index_symbol": "NIFTY 50",
    "non_equity_symbols": ["NIFTY 50", "INDIA VIX"],
    "session_min_symbols": 150,           # a session is a bars_1m date with MORE than this many symbols
    "breadth_time": "11:00",
    "breadth_trend_day_min": 0.5,
    "or_start": "09:15",
    "or_end_exclusive": "09:30",
    # --- costs
    "product": "MIS",
    "reference_notional_inr": "20000",
    "slippage_ticks_per_side": 1,
    "tick_size_inr": str(DEFAULT_TICK_SIZE),
    # --- the brief's promotion rule (a boolean, not a recommendation)
    "promote_min_n": 200,
    "promote_min_t": 2.0,
    "promote_min_cpcv_positive_share": 0.60,
    # --- CPCV (engine.learning.validate defaults: 6 folds / 2 test folds / purge 5 / embargo 5)
    "cpcv_purge_obs": 5,
    "cpcv_embargo_obs": 5,
}

# ================================================================================================
# THE FIVE PRE-REGISTERED RULES. ``sql`` is the clause set, evaluated on ONE completed 5m bar; it is
# the single source of truth for the rule (there is no second, Python, copy to drift from it). The
# columns it may reference are the ones :func:`signal_frame_sql` defines:
#
#   o5 h5 l5 c5 v5   this bar's 5m OHLCV          vwap5     session VWAP at this bar's last minute
#   medvol20 n20     the prior-20 median + count  n6        count of the prior 6 bars (always 6 here)
#   mx6h             max high of the prior 6      n6_above/n6_below  of those 6, closes >/< own VWAP
#   c1..c4 o1 o2     lagged closes / opens        h1 h2 h3  lagged highs   w1 w2 w3  lagged VWAPs
#   run_max_h        running max high <= i        max_h_upto_i3   running max high <= i-3
#
# EVERY rule also carries the brief's global clause "close > VWAP" (R5 satisfies it by construction).
# ================================================================================================
RULES: dict[str, dict[str, str]] = {
    "R1": {
        "name": "momentum_burst",
        "text": (
            "close > max(high) of the prior 6 five-min bars; volume >= 2.0 x medvol20; "
            "close >= low + 0.75 x (high - low); close > VWAP"
        ),
        "sql": (
            "c5 > vwap5 "
            "AND n6 = {r1_breakout_lookback_bars} AND c5 > mx6h "
            "AND v5 >= {r1_vol_mult} * medvol20 "
            "AND c5 >= l5 + {r1_close_in_range_frac} * (h5 - l5)"
        ),
    },
    "R2": {
        "name": "three_soldiers",
        "text": (
            "three consecutive bullish bars (close > open), each close > the prior bar's HIGH; "
            "third bar's volume >= 1.5 x medvol20; close > VWAP"
        ),
        "sql": (
            "c5 > vwap5 "
            "AND c5 > o5 AND c1 > o1 AND c2 > o2 "
            "AND c5 > h1 AND c1 > h2 AND c2 > h3 "
            "AND v5 >= {r2_vol_mult} * medvol20"
        ),
    },
    "R3": {
        "name": "engulf_pullback",
        "text": (
            "session high set >= 3 bars ago; the 3 bars before this one each closed below the prior "
            "close and all stayed above VWAP; this bar is bullish with open <= prior close and "
            "close >= prior open; volume >= 1.2 x medvol20; close > VWAP"
        ),
        "sql": (
            "c5 > vwap5 "
            "AND max_h_upto_i3 >= run_max_h "
            "AND c1 < c2 AND c2 < c3 AND c3 < c4 "
            "AND c1 > w1 AND c2 > w2 AND c3 > w3 "
            "AND c5 > o5 AND o5 <= c1 AND c5 >= o1 "
            "AND v5 >= {r3_vol_mult} * medvol20"
        ),
    },
    "R4": {
        "name": "vwap_hold_buy",
        "text": (
            "the prior 6 bars all closed above VWAP; this bar's low <= VWAP x 1.0015 (a touch) and "
            "it closes above VWAP with close > open"
        ),
        "sql": (
            "n6 = {r4_prior_bars_above_vwap} AND n6_above = {r4_prior_bars_above_vwap} "
            "AND l5 <= vwap5 * {r4_vwap_touch_mult} "
            "AND c5 > vwap5 AND c5 > o5"
        ),
    },
    "R5": {
        "name": "vwap_reclaim",
        "text": (
            "the prior 6 bars all closed BELOW VWAP; this bar closes above VWAP; "
            "volume >= 1.5 x medvol20"
        ),
        "sql": (
            "n6 = {r5_prior_bars_below_vwap} AND n6_below = {r5_prior_bars_below_vwap} "
            "AND c5 > vwap5 "
            "AND v5 >= {r5_vol_mult} * medvol20"
        ),
    },
}

# ================================================================================================
# THE TWO PRE-REGISTERED EXITS. Both are implemented in :func:`simulate_e1` / :func:`simulate_e2`.
# ================================================================================================
EXITS: dict[str, dict[str, str]] = {
    "E1": {
        "name": "fixed",
        "text": (
            "stop = the signal bar's LOW (R5: stop = VWAP at the signal bar); "
            "target = entry + 2 x (entry - stop); checked on 1m bars STRICTLY AFTER the entry bar "
            "(low <= stop -> fill at stop; high >= target -> fill at target; both in one bar -> the "
            "STOP wins); else the 15:15 close"
        ),
    },
    "E2": {
        "name": "trail",
        "text": (
            "exit at the next 1m OPEN after the first completed 5m bar that closes below the PRIOR "
            "5m bar's low; else the 15:15 close. No target."
        ),
    },
}

#: The rule whose E1 stop is the signal bar's VWAP rather than its low (the brief's parenthesis).
E1_STOP_IS_VWAP_RULES = frozenset({"R5"})

SPLIT_ALL = "all"
SPLIT_INDEX_UP = "index_up_at_signal"
SPLIT_INDEX_DOWN = "index_down_at_signal"
SPLIT_BREADTH_HIGH = "breadth_1100_ge_50pct"
SPLIT_BREADTH_LOW = "breadth_1100_lt_50pct"
SPLIT_CATALYST_TRUE = "catalyst_at_signal_true"
SPLIT_CATALYST_FALSE = "catalyst_at_signal_false"
SPLIT_REAL_INDEX = "real_nifty50_index_only"

_CV_SKFOLIO = "cpcv_skfolio_CombinatorialPurgedCV"
_CV_FALLBACK = "purged_kfold_with_embargo_fallback"

_TIME_RE = re.compile(r"^([01]\d|2[0-3]):([0-5]\d)$")

_SESSION_OPEN_MINUTES = 9 * 60 + 15      # 09:15 -> the mfo origin
_LAST_1M_MFO = 374                       # the 15:29 bar


# =============================================================================== data structures
@dataclass
class Trade:
    """One measured entry. ``gross_pct``/``net_pct`` are PERCENT, per-trade equal notional."""

    rule: str
    exit_style: str
    symbol: str
    d: date
    signal_bk: int
    signal_mfo: int
    entry_mfo: int
    entry_px: float
    exit_px: float
    exit_mfo: int
    exit_reason: str
    gross_pct: float
    cost_pct: float
    net_pct: float
    vol_ratio: float
    stop_px: float | None
    target_px: float | None
    stop_gap_through: bool
    index_ret_pct: float
    index_is_real: bool
    breadth: float
    catalyst_at_signal: bool
    catalyst_split_covered: bool


# =============================================================================== read-only DB access
class DbUnopenable(RuntimeError):
    """The DuckDB file is missing, locked by the engine, or otherwise not readable."""


def open_readonly(db_path: Path) -> duckdb.DuckDBPyConnection:
    """Attach ``db_path`` READ-ONLY, or raise :class:`DbUnopenable` with an actionable message.

    Same posture as ``scripts/backtest_tdc.py``: ``MarketStore.open()`` runs schema DDL and is a
    writer, so it is never used here.
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


def to_mfo(hhmm: str) -> int:
    """``HH:MM`` -> minutes from the 09:15 open (the 09:15 bar is 0, the 15:29 bar is 374)."""
    h, m = _validate_hhmm(hhmm).split(":")
    return int(h) * 60 + int(m) - _SESSION_OPEN_MINUTES


def from_mfo(mfo: int) -> str:
    """The inverse of :func:`to_mfo`, for reporting."""
    total = _SESSION_OPEN_MINUTES + int(mfo)
    return f"{total // 60:02d}:{total % 60:02d}"


def signal_bucket_bounds(params: dict[str, Any]) -> tuple[int, int]:
    """``(lo_mfo, hi_mfo)`` of the signal window, on the 5m bar's LAST 1m minute (pinned reading 1)."""
    return to_mfo(str(params["signal_window_start"])), to_mfo(str(params["signal_window_end"]))


def signal_relation_mfo_hi(params: dict[str, Any]) -> int:
    """The LAST 1m minute that can belong to a signal bar - the cutoff for the signal pass.

    For the pre-registered window this is 284 (13:59): the ``[13:55,14:00)`` bucket is the last one
    whose close minute is inside ``[09:45, 14:00]``, and the ``[14:00,14:05)`` bucket could only
    reach a close minute of 14:00 by holding ONE bar, which is below the 3-bar validity floor. Every
    1m bar after this minute is filtered OUT of the relation before the VWAP window runs, so the
    signal pass cannot see the afternoon at all.
    """
    _lo, hi = signal_bucket_bounds(params)
    bucket = int(params["bucket_minutes"])
    bk = hi // bucket
    if hi - bk * bucket + 1 < int(params["min_1m_bars_per_bucket"]):
        bk -= 1
    return bk * bucket + bucket - 1


# =============================================================================== the 5m aggregation
def five_min_cte(
    *,
    params: dict[str, Any],
    mfo_hi: int,
    with_vwap: bool,
    join_sql: str = "",
    symbol_where: str | None = None,
    materialize: bool = False,
) -> str:
    """SQL text for the CTE chain ending in ``five`` - THE 5m aggregation, single-sourced.

    ``five`` carries one row per VALID (symbol, d, bk) bucket:
    ``symbol, d, bk, n1m, last_mfo, o5, h5, l5, c5, v5`` (+ ``vwap5`` when ``with_vwap``).

    The 1m relation is filtered to ``mfo BETWEEN 0 AND mfo_hi`` and to ``join_sql``'s symbol-days
    BEFORE the cumulative VWAP window runs, so a bar outside the window is not merely unused - it is
    not in the relation the sums are taken over. ``bk = mfo // 5`` makes the buckets 09:15-anchored
    by construction: ``[09:15,09:20)``, ``[09:20,09:25)``, ...
    """
    n_min = int(params["min_1m_bars_per_bucket"])
    bucket = int(params["bucket_minutes"])
    if symbol_where is None:
        excluded = ", ".join(
            "'" + str(s).replace("'", "''") + "'" for s in params["non_equity_symbols"]
        )
        symbol_where = f"symbol NOT IN ({excluded})" if excluded else "TRUE"
    mat = " MATERIALIZED" if materialize else ""
    vwap_1m = (
        ",\n             SUM((h + l + c) / 3.0 * v) OVER w / NULLIF(SUM(v) OVER w, 0) AS vwap_run"
        if with_vwap else ""
    )
    vwap_win = (
        "\n      WINDOW w AS (PARTITION BY symbol, d ORDER BY mfo "
        "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
        if with_vwap else ""
    )
    vwap_5m = ",\n             arg_max(vwap_run, mfo) AS vwap5" if with_vwap else ""
    return f"""
    b AS (
      SELECT symbol, (ts_minute AT TIME ZONE 'Asia/Kolkata') AS tl,
             "open"::DOUBLE AS o, high::DOUBLE AS h, low::DOUBLE AS l,
             "close"::DOUBLE AS c, volume::DOUBLE AS v
      FROM bars_1m
      WHERE {symbol_where}
    ),
    m AS (
      SELECT symbol, tl::DATE AS d,
             (datepart('hour', tl) * 60 + datepart('minute', tl) - {_SESSION_OPEN_MINUTES}) AS mfo,
             o, h, l, c, v
      FROM b
      WHERE tl::DATE >= ? AND tl::DATE <= ?
    ),
    mf AS (
      SELECT m.symbol, m.d, m.mfo, m.o, m.h, m.l, m.c, m.v
      FROM m {join_sql}
      WHERE m.mfo BETWEEN 0 AND {int(mfo_hi)}
    ),
    r AS (
      SELECT symbol, d, mfo, o, h, l, c, v{vwap_1m}
      FROM mf{vwap_win}
    ),
    g AS (
      SELECT symbol, d, mfo // {bucket} AS bk, count(*) AS n1m, max(mfo) AS last_mfo,
             arg_min(o, mfo) AS o5, max(h) AS h5, min(l) AS l5, arg_max(c, mfo) AS c5,
             sum(v) AS v5{vwap_5m}
      FROM r GROUP BY 1, 2, 3
    ),
    five AS{mat} (SELECT * FROM g WHERE n1m >= {n_min})
    """


def load_five_min(
    conn: duckdb.DuckDBPyConnection,
    start: date,
    end: date,
    *,
    params: dict[str, Any] | None = None,
    mfo_hi: int = _LAST_1M_MFO,
    with_vwap: bool = True,
    join_sql: str = "",
) -> pd.DataFrame:
    """The valid 5m bars for the window, ascending - the public face of :func:`five_min_cte`."""
    p = dict(params or PARAMS)
    sql = "WITH " + five_min_cte(params=p, mfo_hi=mfo_hi, with_vwap=with_vwap, join_sql=join_sql)
    sql += "\nSELECT * FROM five ORDER BY symbol, d, bk"
    return conn.execute(sql, [start, end]).df()


# =============================================================================== sessions / universe
def load_symbol_days(
    conn: duckdb.DuckDBPyConnection, start: date, end: date, *, params: dict[str, Any]
) -> dict[date, set[str]]:
    """``d -> {symbols with any 1m bar that day}``, non-equity series excluded."""
    excluded = ", ".join("'" + str(s).replace("'", "''") + "'" for s in params["non_equity_symbols"])
    rows = conn.execute(
        f"""
        SELECT (ts_minute AT TIME ZONE 'Asia/Kolkata')::DATE AS d, symbol
        FROM bars_1m
        WHERE symbol NOT IN ({excluded})
          AND (ts_minute AT TIME ZONE 'Asia/Kolkata')::DATE >= ?
          AND (ts_minute AT TIME ZONE 'Asia/Kolkata')::DATE <= ?
        GROUP BY 1, 2
        """,
        [start, end],
    ).fetchall()
    out: dict[date, set[str]] = defaultdict(set)
    for d, sym in rows:
        out[d.date() if hasattr(d, "date") else d].add(str(sym))
    return dict(out)


def load_universe_daily(conn: duckdb.DuckDBPyConnection) -> dict[date, set[str]]:
    """``d -> {included symbols}`` from ``universe_daily``; days with no included row are omitted,
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


def eligible_universe(
    symbol_days: dict[date, set[str]],
    universe: dict[date, set[str]],
    *,
    params: dict[str, Any],
) -> tuple[dict[date, list[str]], set[date], set[date]]:
    """``(d -> eligible symbols, fallback dates, dropped thin dates)``.

    A SESSION is a ``bars_1m`` date carrying MORE than ``session_min_symbols`` symbols (the brief's
    definition); thinner dates are dropped whole and reported. Within a session the eligible set is
    ``universe_daily`` included INTERSECT the symbols actually present, falling back to the present
    set when ``universe_daily`` has no included row for that date (or the intersection is empty).
    """
    floor = int(params["session_min_symbols"])
    elig: dict[date, list[str]] = {}
    fallback: set[date] = set()
    dropped: set[date] = set()
    for d, present in symbol_days.items():
        if len(present) <= floor:
            dropped.add(d)
            continue
        uni = universe.get(d)
        if uni:
            keep = sorted(present & uni)
            if not keep:
                keep = sorted(present)
                fallback.add(d)
        else:
            keep = sorted(present)
            fallback.add(d)
        elig[d] = keep
    return elig, fallback, dropped


# =============================================================================== context (index/breadth)
def load_context_grid(
    conn: duckdb.DuckDBPyConnection,
    start: date,
    end: date,
    *,
    params: dict[str, Any],
    mfo_hi: int,
) -> pd.DataFrame:
    """``(d, bk, proxy_ret_pct, n_proxy, breadth, n_breadth)`` - the equal-weight index proxy on the
    5m grid, plus the 11:00 breadth snapshot, both over the ELIGIBLE set (view ``cnd_elig``).

    The proxy is the equal-weight mean of ``close(bk)/open(bucket 0) - 1``; breadth is the share of
    the eligible set whose ``[10:55,11:00)`` close is above its ``[09:15,09:30)`` high. No VWAP is
    needed for either, so this pass skips the 1m cumulative window entirely.
    """
    p = dict(params)
    join = "JOIN cnd_elig e ON e.symbol = m.symbol AND e.d = m.d"
    or_hi_bk = (to_mfo(str(p["or_end_exclusive"])) // int(p["bucket_minutes"])) - 1
    breadth_bk = (to_mfo(str(p["breadth_time"])) // int(p["bucket_minutes"])) - 1
    lo, hi = signal_bucket_bounds(p)
    sql = (
        "WITH "
        + five_min_cte(params=p, mfo_hi=mfo_hi, with_vwap=False, join_sql=join, materialize=True)
        + f""",
    op AS (SELECT symbol, d, o5 AS open_0915 FROM five WHERE bk = 0),
    orh AS (SELECT symbol, d, max(h5) AS or_high FROM five WHERE bk <= {or_hi_bk} GROUP BY 1, 2),
    b11 AS (SELECT symbol, d, c5 AS close_1100 FROM five WHERE bk = {breadth_bk}),
    breadth AS (
      SELECT b11.d AS d,
             avg(CASE WHEN b11.close_1100 > orh.or_high THEN 1.0 ELSE 0.0 END) AS breadth,
             count(*) AS n_breadth
      FROM b11 JOIN orh ON orh.symbol = b11.symbol AND orh.d = b11.d
      GROUP BY 1
    ),
    proxy AS (
      SELECT five.d AS d, five.bk AS bk,
             avg(five.c5 / op.open_0915 - 1.0) * 100.0 AS proxy_ret_pct,
             count(*) AS n_proxy
      FROM five JOIN op ON op.symbol = five.symbol AND op.d = five.d
      WHERE op.open_0915 > 0 AND five.last_mfo BETWEEN {lo} AND {hi}
      GROUP BY 1, 2
    )
    SELECT p.d, p.bk, p.proxy_ret_pct, p.n_proxy,
           COALESCE(br.breadth, 0.0) AS breadth, COALESCE(br.n_breadth, 0) AS n_breadth
    FROM proxy p LEFT JOIN breadth br ON br.d = p.d
    ORDER BY 1, 2
    """
    )
    return conn.execute(sql, [start, end]).df()


def load_index_grid(
    conn: duckdb.DuckDBPyConnection,
    start: date,
    end: date,
    *,
    params: dict[str, Any],
    mfo_hi: int,
) -> pd.DataFrame:
    """``(d, bk, index_ret_pct)`` for the REAL index series, on the same 5m grid."""
    p = dict(params)
    sym = str(p["index_symbol"]).replace("'", "''")
    sql = (
        "WITH "
        # the index IS the subject here, so the non-equity exclusion is inverted rather than dropped
        + five_min_cte(params=p, mfo_hi=mfo_hi, with_vwap=False, join_sql="",
                       symbol_where=f"symbol = '{sym}'")
        + """,
    fi AS (SELECT * FROM five),
    op AS (SELECT d, o5 AS open_0915 FROM fi WHERE bk = 0)
    SELECT fi.d AS d, fi.bk AS bk, (fi.c5 / op.open_0915 - 1.0) * 100.0 AS index_ret_pct
    FROM fi JOIN op ON op.d = fi.d
    WHERE op.open_0915 > 0
    ORDER BY 1, 2
    """
    )
    return conn.execute(sql, [start, end]).df()


# =============================================================================== the signal pass
def signal_frame_sql(*, params: dict[str, Any], mfo_hi: int) -> str:
    """The full signal SQL: 5m bars -> per-bar features -> the five RULES -> the FIRST trigger.

    Every feature window is ``ROWS BETWEEN n PRECEDING AND 1 PRECEDING`` (or ``... AND 3 PRECEDING``
    for R3's session-high clause), so the current bar is never in its own denominator and no later
    bar is in anything. Returns one row per ``(symbol, d, rule)`` - the day's FIRST trigger of that
    rule for that symbol.
    """
    p = dict(params)
    lo, hi = signal_bucket_bounds(p)
    join = "JOIN cnd_elig e ON e.symbol = m.symbol AND e.d = m.d"
    look6 = int(p["r1_breakout_lookback_bars"])
    back = int(p["medvol_lookback_bars"])
    ago = int(p["r3_session_high_min_bars_ago"])
    rule_names = ", ".join("'" + k + "'" for k in RULES)
    rule_flags = ", ".join("(" + v["sql"].format(**p) + ")" for v in RULES.values())
    return (
        "WITH "
        + five_min_cte(params=p, mfo_hi=mfo_hi, with_vwap=True, join_sql=join)
        + f""",
    w AS (
      SELECT symbol, d, bk, n1m, last_mfo, o5, h5, l5, c5, v5, vwap5,
             count(*)     OVER p6  AS n6,
             max(h5)      OVER p6  AS mx6h,
             sum(CASE WHEN c5 > vwap5 THEN 1 ELSE 0 END) OVER p6 AS n6_above,
             sum(CASE WHEN c5 < vwap5 THEN 1 ELSE 0 END) OVER p6 AS n6_below,
             count(*)     OVER p20 AS n20,
             median(v5)   OVER p20 AS medvol20,
             lag(c5, 1)   OVER pd  AS c1, lag(c5, 2) OVER pd AS c2,
             lag(c5, 3)   OVER pd  AS c3, lag(c5, 4) OVER pd AS c4,
             lag(o5, 1)   OVER pd  AS o1, lag(o5, 2) OVER pd AS o2,
             lag(h5, 1)   OVER pd  AS h1, lag(h5, 2) OVER pd AS h2, lag(h5, 3) OVER pd AS h3,
             lag(vwap5, 1) OVER pd AS w1, lag(vwap5, 2) OVER pd AS w2, lag(vwap5, 3) OVER pd AS w3,
             max(h5)      OVER pall AS run_max_h,
             max(h5)      OVER pago AS max_h_upto_i3
      FROM five
      WINDOW pd   AS (PARTITION BY symbol, d ORDER BY bk),
             p6   AS (PARTITION BY symbol, d ORDER BY bk ROWS BETWEEN {look6} PRECEDING AND 1 PRECEDING),
             p20  AS (PARTITION BY symbol, d ORDER BY bk ROWS BETWEEN {back} PRECEDING AND 1 PRECEDING),
             pall AS (PARTITION BY symbol, d ORDER BY bk ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
             pago AS (PARTITION BY symbol, d ORDER BY bk ROWS BETWEEN UNBOUNDED PRECEDING AND {ago} PRECEDING)
    ),
    cand AS (
      SELECT * FROM w
      WHERE last_mfo BETWEEN {lo} AND {hi}
        AND n20 >= {int(p["medvol_min_bars"])} AND medvol20 > 0
    ),
    long AS (
      SELECT symbol, d, bk, last_mfo, o5, h5, l5, c5, v5, vwap5, medvol20, rule, fired
      FROM (
        SELECT symbol, d, bk, last_mfo, o5, h5, l5, c5, v5, vwap5, medvol20,
               unnest([{rule_names}]) AS rule,
               unnest([{rule_flags}]) AS fired
        FROM cand
      )
      WHERE fired
    )
    SELECT symbol, d, bk, last_mfo, o5, h5, l5, c5, v5, vwap5, medvol20,
           v5 / medvol20 AS vol_ratio, rule
    FROM long
    QUALIFY row_number() OVER (PARTITION BY symbol, d, rule ORDER BY bk) = 1
    ORDER BY d, rule, symbol
    """
    )


def load_signals(
    conn: duckdb.DuckDBPyConnection,
    start: date,
    end: date,
    *,
    params: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """One row per ``(symbol, d, rule)``: the day's FIRST trigger of that rule for that symbol."""
    p = dict(params or PARAMS)
    frame = conn.execute(
        signal_frame_sql(params=p, mfo_hi=signal_relation_mfo_hi(p)), [start, end]
    ).df()
    if not frame.empty:
        frame["d"] = [x.date() if hasattr(x, "date") else x for x in frame["d"]]
    return frame


def rank_and_cap(signals: pd.DataFrame, *, params: dict[str, Any] | None = None) -> pd.DataFrame:
    """The brief's daily cap: at most N trades per RULE per DAY, ranked by ``vol_ratio`` DESC then
    symbol ASC. Applied in Python so it is directly testable; see the module docstring's disclosed
    exception - this ranking compares first-triggers from different minutes of the same day."""
    p = dict(params or PARAMS)
    cap = int(p["max_trades_per_rule_per_day"])
    if signals.empty:
        return signals
    out = signals.sort_values(
        ["d", "rule", "vol_ratio", "symbol"], ascending=[True, True, False, True], kind="mergesort"
    )
    return out.groupby(["d", "rule"], sort=False, group_keys=False).head(cap).reset_index(drop=True)


# =============================================================================== post-signal tape
def load_trail_bars(
    conn: duckdb.DuckDBPyConnection,
    pairs: pd.DataFrame,
    start: date,
    end: date,
    *,
    params: dict[str, Any],
) -> dict[tuple[str, date], list[tuple[int, int, float, float]]]:
    """``(symbol, d) -> [(bk, last_mfo, close, PRIOR bar's low), ...]`` for E2's trail check.

    Only the SELECTED symbol-days are aggregated. The lag runs over the session's full valid 5m
    sequence, so the first candidate bar's "prior bar" is the signal bar itself, exactly as the
    brief's E2 reads.
    """
    if pairs.empty:
        return {}
    conn.register("cnd_sel", pairs)
    try:
        join = "JOIN cnd_sel s ON s.symbol = m.symbol AND s.d = m.d"
        sql = (
            "WITH "
            + five_min_cte(params=params, mfo_hi=_LAST_1M_MFO, with_vwap=False, join_sql=join)
            + """,
        t AS (
          SELECT symbol, d, bk, last_mfo, c5,
                 lag(l5) OVER (PARTITION BY symbol, d ORDER BY bk) AS prev_l5
          FROM five
        )
        SELECT symbol, d, bk, last_mfo, c5, prev_l5 FROM t
        WHERE prev_l5 IS NOT NULL ORDER BY symbol, d, bk
        """
        )
        frame = conn.execute(sql, [start, end]).df()
    finally:
        conn.unregister("cnd_sel")
    out: dict[tuple[str, date], list[tuple[int, int, float, float]]] = defaultdict(list)
    for sym, d, bk, last_mfo, c5, prev_l5 in zip(
        frame["symbol"], frame["d"], frame["bk"], frame["last_mfo"], frame["c5"], frame["prev_l5"],
        strict=True,
    ):
        dd = d.date() if hasattr(d, "date") else d
        out[(str(sym), dd)].append((int(bk), int(last_mfo), float(c5), float(prev_l5)))
    return dict(out)


def load_post_signal_1m(
    conn: duckdb.DuckDBPyConnection,
    pairs: pd.DataFrame,
    start: date,
    end: date,
    *,
    params: dict[str, Any],
) -> dict[tuple[str, date], tuple[list[int], list[float], list[float], list[float], list[float]]]:
    """``(symbol, d) -> (mfo, open, high, low, close)`` for the bars in ``[signal window, 15:15]``.

    Only the SELECTED symbol-days are fetched, so the fill scan never materializes the full tape.
    """
    if pairs.empty:
        return {}
    lo, _hi = signal_bucket_bounds(params)
    sq = to_mfo(str(params["squareoff_time"]))
    excluded = ", ".join(
        "'" + str(s).replace("'", "''") + "'" for s in params["non_equity_symbols"]
    )
    conn.register("cnd_sel", pairs)
    try:
        sql = f"""
        WITH b AS (
          SELECT symbol, (ts_minute AT TIME ZONE 'Asia/Kolkata') AS tl,
                 "open"::DOUBLE AS o, high::DOUBLE AS h, low::DOUBLE AS l, "close"::DOUBLE AS c
          FROM bars_1m WHERE symbol NOT IN ({excluded})
        ),
        m AS (
          SELECT symbol, tl::DATE AS d,
                 (datepart('hour', tl) * 60 + datepart('minute', tl) - {_SESSION_OPEN_MINUTES}) AS mfo,
                 o, h, l, c
          FROM b WHERE tl::DATE >= ? AND tl::DATE <= ?
        )
        SELECT m.symbol, m.d, m.mfo, m.o, m.h, m.l, m.c
        FROM m JOIN cnd_sel s ON s.symbol = m.symbol AND s.d = m.d
        WHERE m.mfo BETWEEN {int(lo)} AND {int(sq)}
        ORDER BY 1, 2, 3
        """
        frame = conn.execute(sql, [start, end]).df()
    finally:
        conn.unregister("cnd_sel")
    out: dict[tuple[str, date], tuple[list[int], list[float], list[float], list[float], list[float]]] = {}
    for (sym, d), grp in frame.groupby(["symbol", "d"], sort=False):
        dd = d.date() if hasattr(d, "date") else d
        out[(str(sym), dd)] = (
            [int(x) for x in grp["mfo"]],
            grp["o"].astype(float).tolist(),
            grp["h"].astype(float).tolist(),
            grp["l"].astype(float).tolist(),
            grp["c"].astype(float).tolist(),
        )
    return out


# =============================================================================== execution
@dataclass
class Fill:
    """One simulated round trip. ``exit_mfo`` is the minute of the bar the exit was read from."""

    entry_px: float
    exit_px: float
    exit_mfo: int
    reason: str
    stop_px: float | None = None
    target_px: float | None = None
    stop_gap_through: bool = False


def _entry_index(mfos: Sequence[int], entry_mfo: int) -> int | None:
    """The index of the entry bar, or None when that exact minute has no bar (a SKIPPED trade)."""
    try:
        return list(mfos).index(int(entry_mfo))
    except ValueError:
        return None


def simulate_e1(
    bars: tuple[list[int], list[float], list[float], list[float], list[float]],
    *,
    entry_mfo: int,
    stop_px: float,
    target_r_multiple: float,
    squareoff_mfo: int,
) -> Fill | None:
    """E1 fixed: entry at the ``entry_mfo`` OPEN, then stop/target on bars STRICTLY AFTER it.

    ``target = entry + R * (entry - stop)``. Scanning forward from the bar after the entry bar to the
    squareoff bar (the last bar at or before 15:15): ``low <= stop`` fills at ``stop``; ``high >=
    target`` fills at ``target``; when one minute touches BOTH, the STOP wins (checked first) - the
    pessimistic assumption, since a 1m bar does not say which came first. If neither is touched the
    exit is the squareoff bar's CLOSE.

    Returns ``None`` when the entry bar is missing OR when ``stop >= entry`` - a protective stop at
    or above the fill price is not a trade, and booking it at ``stop`` would print a fictitious gain.
    """
    mfos, opens, highs, lows, closes = bars
    i0 = _entry_index(mfos, entry_mfo)
    if i0 is None:
        return None
    entry_px = float(opens[i0])
    if not math.isfinite(entry_px) or entry_px <= 0.0:
        return None
    if not math.isfinite(stop_px) or stop_px >= entry_px:
        return None
    last = len(mfos) - 1
    while last >= 0 and mfos[last] > squareoff_mfo:
        last -= 1
    if last < i0:
        return None
    target_px = entry_px + float(target_r_multiple) * (entry_px - float(stop_px))
    for k in range(i0 + 1, last + 1):
        if float(lows[k]) <= stop_px:
            return Fill(entry_px, float(stop_px), int(mfos[k]), "stop", stop_px, target_px,
                        float(opens[k]) < float(stop_px))
        if float(highs[k]) >= target_px:
            return Fill(entry_px, float(target_px), int(mfos[k]), "target", stop_px, target_px)
    return Fill(entry_px, float(closes[last]), int(mfos[last]), "squareoff_1515", stop_px, target_px)


def simulate_e2(
    bars: tuple[list[int], list[float], list[float], list[float], list[float]],
    trail: Sequence[tuple[int, int, float, float]],
    *,
    entry_mfo: int,
    signal_bk: int,
    squareoff_mfo: int,
) -> Fill | None:
    """E2 trail: exit at the next 1m OPEN after the first completed 5m bar closing below the PRIOR
    5m bar's low; else the squareoff CLOSE. No target, no stop.

    Only 5m bars AFTER the signal bar are candidates, and the fill is the first 1m bar strictly after
    the triggering bar's last minute - never the close that triggered it. If no bar exists between
    the trigger and 15:15, the exit falls back to the squareoff close.
    """
    mfos, opens, _highs, _lows, closes = bars
    i0 = _entry_index(mfos, entry_mfo)
    if i0 is None:
        return None
    entry_px = float(opens[i0])
    if not math.isfinite(entry_px) or entry_px <= 0.0:
        return None
    last = len(mfos) - 1
    while last >= 0 and mfos[last] > squareoff_mfo:
        last -= 1
    if last < i0:
        return None
    for bk, last_mfo, c5, prev_l5 in trail:
        if bk <= signal_bk or last_mfo >= mfos[last]:
            continue
        if c5 < prev_l5:
            for k in range(i0 + 1, last + 1):
                if mfos[k] > last_mfo:
                    return Fill(entry_px, float(opens[k]), int(mfos[k]), "trail_next_open")
            break
    return Fill(entry_px, float(closes[last]), int(mfos[last]), "squareoff_1515")


def trade_cost_pct(cost_model: CostModel, params: dict[str, Any], entry_px: float) -> float:
    """One MIS round trip at the reference notional PLUS one tick of slippage on EACH side.

    Byte-for-byte the ``scripts/backtest_tdc.py`` treatment (the test asserts the two agree). The
    slippage term is price-relative on purpose: one tick is a bigger percentage of a Rs 90 stock than
    of a Rs 4,000 one, and the study should feel that.
    """
    rt = float(cost_model.breakeven_pct(Decimal(params["reference_notional_inr"]), params["product"]))
    tick = float(Decimal(params["tick_size_inr"]))
    slip = 2.0 * int(params["slippage_ticks_per_side"]) * tick / entry_px * 100.0
    return rt + slip


# =============================================================================== catalyst context
def load_catalyst_context(
    conn: duckdb.DuckDBPyConnection,
) -> tuple[set[tuple[date, str]], dict[str, list[datetime]], dict[str, Any]]:
    """``({(d, symbol) on the watchlist}, {symbol -> sorted news first_seen}, coverage)`` - tdc's
    definition, verbatim, so the two studies' catalyst splits mean the same thing."""
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


def catalyst_at(
    symbol: str,
    d: date,
    signal_mfo: int,
    watchlist: set[tuple[date, str]],
    news_by_symbol: dict[str, list[datetime]],
    sessions: Sequence[date],
) -> bool:
    """True when the symbol carried a catalyst KNOWN BY the signal minute on ``d`` (tdc's rule).

    Sources: (1) a ``catalyst_watchlist`` row for ``(d, symbol)``; (2) a ``news_clusters`` row naming
    the symbol whose ``first_seen`` falls between the PRIOR session's 15:00 IST and the signal minute
    on ``d`` - so nothing the decision could not have seen is counted.
    """
    if (d, symbol) in watchlist:
        return True
    stamps = news_by_symbol.get(symbol)
    if not stamps:
        return False
    i = bisect_left(list(sessions), d)
    prev = sessions[i - 1] if i > 0 else d - timedelta(days=1)
    lo = datetime.combine(prev, time(15, 0), tzinfo=IST)
    hh, mm = from_mfo(signal_mfo).split(":")
    hi = datetime.combine(d, time(int(hh), int(mm)), tzinfo=IST)
    j = bisect_left(stamps, lo)
    return j < len(stamps) and stamps[j] <= hi


# =============================================================================== metrics
def _purged_kfold_splits(
    n_obs: int, *, n_folds: int = 6, purge: int = 5, embargo: int = 5
) -> list[tuple[np.ndarray, np.ndarray]]:
    """Fallback when skfolio is unavailable: contiguous K-fold test blocks with the same
    purge/embargo guarantee ``cpcv_splits`` enforces (mirrors tdc and hi52)."""
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
    """Per-SESSION mean net %. The CPCV observation unit is the session, not the trade: the day's
    picks share one regime and are not independent draws."""
    by_day: dict[date, list[float]] = defaultdict(list)
    for t in trades:
        by_day[t.d].append(t.net_pct)
    days = sorted(by_day)
    return days, np.array([statistics.fmean(by_day[d]) for d in days], dtype="float64")


def cpcv_positive_share(trades: Sequence[Trade], params: dict[str, Any]) -> dict[str, Any]:
    """CPCV over the per-session net series; the reported share is (splits with test mean > 0) / all.

    Purge and embargo are the ``engine.learning.validate`` §6.4 defaults (5 observations each), which
    is what tdc used: a candle trade opens and closes inside ONE session, so consecutive observations
    do not overlap at all and the default is already stricter than the leakage it guards against.
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
    """``n``, mean/median net %, win rate, t-stat, mean GROSS %, mean cost %, CPCV share, boolean.

    ``t = mean / (sd / sqrt(n))`` with the SAMPLE standard deviation (ddof=1), over TRADES. The
    honest caveat is stated in the report rather than hidden: same-day picks are correlated, so this
    t-stat overstates the effective sample size; the CPCV share is the check that does not.
    """
    net = [t.net_pct for t in trades]
    n = len(net)
    base: dict[str, Any] = {
        "n": n, "mean_net_pct": None, "median_net_pct": None, "win_rate": None, "t_stat": None,
        "mean_gross_pct": None, "mean_cost_pct": None,
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
        SPLIT_CATALYST_TRUE: [t for t in cat if t.catalyst_at_signal],
        SPLIT_CATALYST_FALSE: [t for t in cat if not t.catalyst_at_signal],
        SPLIT_REAL_INDEX: [t for t in trades if t.index_is_real],
    }


def _trade_row(t: Trade) -> dict[str, Any]:
    return {
        "symbol": t.symbol, "d": str(t.d), "rule": t.rule, "exit_style": t.exit_style,
        "signal_bar": (
            f"{from_mfo(t.signal_bk * int(PARAMS['bucket_minutes']))}-{from_mfo(t.signal_mfo)}"
        ),
        "signal_close_minute": from_mfo(t.signal_mfo),
        "entry_minute": from_mfo(t.entry_mfo), "entry_px": round(t.entry_px, 4),
        "exit_minute": from_mfo(t.exit_mfo), "exit_px": round(t.exit_px, 4),
        "exit_reason": t.exit_reason,
        "stop_px": None if t.stop_px is None else round(t.stop_px, 4),
        "target_px": None if t.target_px is None else round(t.target_px, 4),
        "gross_pct": round(t.gross_pct, 4), "cost_pct": round(t.cost_pct, 4),
        "net_pct": round(t.net_pct, 4), "vol_ratio": round(t.vol_ratio, 4),
        "index_ret_pct": round(t.index_ret_pct, 4), "index_is_real": t.index_is_real,
        "breadth_1100": round(t.breadth, 4), "catalyst_at_signal": t.catalyst_at_signal,
    }


def example_trades(trades: Sequence[Trade], per_reason: int = 2) -> list[dict[str, Any]]:
    """A deterministic audit sample: the first ``per_reason`` trades of EACH exit reason, by (d,
    symbol). These are the rows the manager hand-checks against the raw 1m tape."""
    by_reason: dict[str, list[Trade]] = defaultdict(list)
    for t in sorted(trades, key=lambda x: (x.d, x.symbol)):
        if len(by_reason[t.exit_reason]) < per_reason:
            by_reason[t.exit_reason].append(t)
    return [_trade_row(t) for r in sorted(by_reason) for t in by_reason[r]]


# =============================================================================== the study
def run_study(
    conn: duckdb.DuckDBPyConnection,
    *,
    start: date,
    end: date,
    cost_model: CostModel,
    params: dict[str, Any] | None = None,
    calendar: NSECalendar | None = None,
) -> tuple[dict[str, Any], dict[str, list[Trade]]]:
    """Run all ``len(RULES) x len(EXITS)`` cells end to end. Returns ``(json document, trades)``."""
    p = dict(params or PARAMS)
    sq_mfo = to_mfo(str(p["squareoff_time"]))
    notes: list[str] = []

    # ---------------------------------------------------------------- sessions and eligible sets
    symbol_days = load_symbol_days(conn, start, end, params=p)
    universe = load_universe_daily(conn)
    elig, fallback_dates, dropped_thin = eligible_universe(symbol_days, universe, params=p)
    if not elig:
        raise ValueError(
            f"no sessions with more than {p['session_min_symbols']} symbols in "
            f"{start}..{end}: nothing to study"
        )
    sessions = sorted(elig)
    elig_df = pd.DataFrame(
        [{"symbol": s, "d": d} for d in sessions for s in elig[d]], columns=["symbol", "d"]
    )
    watchlist, news_by_symbol, cat_cov = load_catalyst_context(conn)
    cat_from: date | None = None
    if cat_cov["split_covered_from"]:
        cat_from = date.fromisoformat(str(cat_cov["split_covered_from"])[:10])

    non_trading_dates: set[str] = set()
    years_without_calendar: set[int] = set()
    if calendar is not None:
        for d in sessions:
            if d.year not in getattr(calendar, "_years", {}):
                years_without_calendar.add(d.year)
            elif not calendar.is_trading_day(d):
                non_trading_dates.add(str(d))

    _lo_mfo, hi_mfo = signal_bucket_bounds(p)
    ctx_hi = signal_relation_mfo_hi(p)      # the last 1m minute any signal bar can contain (13:59)

    conn.register("cnd_elig", elig_df)
    try:
        # ------------------------------------------------------------ context: proxy index + breadth
        ctx = load_context_grid(conn, start, end, params=p, mfo_hi=ctx_hi)
        idx = load_index_grid(conn, start, end, params=p, mfo_hi=ctx_hi)
        # ------------------------------------------------------------ signals (the expensive pass)
        signals = load_signals(conn, start, end, params=p)
    finally:
        conn.unregister("cnd_elig")

    proxy_ret: dict[tuple[date, int], float] = {}
    breadth_by_day: dict[date, float] = {}
    for d, bk, pr, _npx, br, _nbr in zip(
        ctx["d"], ctx["bk"], ctx["proxy_ret_pct"], ctx["n_proxy"], ctx["breadth"], ctx["n_breadth"],
        strict=True,
    ):
        dd = d.date() if hasattr(d, "date") else d
        proxy_ret[(dd, int(bk))] = float(pr)
        breadth_by_day[dd] = float(br)
    real_ret: dict[tuple[date, int], float] = {}
    real_index_dates: set[date] = set()
    for d, bk, rr in zip(idx["d"], idx["bk"], idx["index_ret_pct"], strict=True):
        dd = d.date() if hasattr(d, "date") else d
        if dd in elig:
            real_ret[(dd, int(bk))] = float(rr)
            real_index_dates.add(dd)

    n_first_triggers = {r: int((signals["rule"] == r).sum()) if not signals.empty else 0 for r in RULES}
    selected = rank_and_cap(signals, params=p)

    # ---------------------------------------------------------------- post-signal tape, once
    pairs = (
        selected[["symbol", "d"]].drop_duplicates().reset_index(drop=True)
        if not selected.empty else pd.DataFrame(columns=["symbol", "d"])
    )
    bars_by_pair = load_post_signal_1m(conn, pairs, start, end, params=p)
    trail_by_pair = load_trail_bars(conn, pairs, start, end, params=p)

    # ---------------------------------------------------------------- simulate every rule x exit
    trades_by_cell: dict[str, list[Trade]] = {f"{r}|{e}": [] for r in RULES for e in EXITS}
    drops: dict[str, dict[str, int]] = {
        k: {"no_entry_bar": 0, "stop_at_or_above_entry": 0, "no_post_signal_tape": 0, "other": 0}
        for k in trades_by_cell
    }
    for row in selected.itertuples(index=False):
        sym, d, rule = str(row.symbol), row.d, str(row.rule)
        key = (sym, d)
        bars = bars_by_pair.get(key)
        entry_mfo = int(row.last_mfo) + 1
        covered = cat_from is not None and d >= cat_from
        cat_flag = covered and catalyst_at(sym, d, int(row.last_mfo), watchlist, news_by_symbol, sessions)
        bk = int(row.bk)
        ireal = (d, bk) in real_ret
        iret = real_ret[(d, bk)] if ireal else proxy_ret.get((d, bk), 0.0)
        breadth = breadth_by_day.get(d, 0.0)
        stop_px = float(row.vwap5) if rule in E1_STOP_IS_VWAP_RULES else float(row.l5)
        for style in EXITS:
            cell = f"{rule}|{style}"
            if bars is None:
                drops[cell]["no_post_signal_tape"] += 1
                continue
            if style == "E1":
                fill = simulate_e1(
                    bars, entry_mfo=entry_mfo, stop_px=stop_px,
                    target_r_multiple=float(p["e1_target_r_multiple"]), squareoff_mfo=sq_mfo,
                )
            else:
                fill = simulate_e2(
                    bars, trail_by_pair.get(key, ()), entry_mfo=entry_mfo, signal_bk=bk,
                    squareoff_mfo=sq_mfo,
                )
            if fill is None:
                i0 = _entry_index(bars[0], entry_mfo)
                if i0 is None:
                    drops[cell]["no_entry_bar"] += 1
                elif style == "E1" and stop_px >= float(bars[1][i0]):
                    drops[cell]["stop_at_or_above_entry"] += 1
                else:
                    drops[cell]["other"] += 1
                continue
            gross = (fill.exit_px / fill.entry_px - 1.0) * 100.0
            cost = trade_cost_pct(cost_model, p, fill.entry_px)
            trades_by_cell[cell].append(Trade(
                rule=rule, exit_style=style, symbol=sym, d=d, signal_bk=bk,
                signal_mfo=int(row.last_mfo), entry_mfo=entry_mfo, entry_px=fill.entry_px,
                exit_px=fill.exit_px, exit_mfo=fill.exit_mfo, exit_reason=fill.reason,
                gross_pct=gross, cost_pct=cost, net_pct=gross - cost,
                vol_ratio=float(row.vol_ratio), stop_px=fill.stop_px, target_px=fill.target_px,
                stop_gap_through=fill.stop_gap_through, index_ret_pct=iret, index_is_real=ireal,
                breadth=breadth, catalyst_at_signal=cat_flag, catalyst_split_covered=covered,
            ))

    # ---------------------------------------------------------------- notes / caveats
    notes.append(
        "NO PARAMETER SWEEP was run and no cell was selected: PARAMS is one dict, RULES is five "
        "pre-registered rules and EXITS two pre-registered exits, all transcribed from "
        "runbooks/briefs/candles_backtest_brief_2026-09-04.md. Every rule x exit cell is REPORTED. "
        "'promotable' is the brief's boolean (mean net > 0, t > 2, n >= 200, CPCV positive share "
        ">= 0.60) and carries no other meaning."
    )
    notes.append(
        "DAILY-CAP LOOK-AHEAD (disclosed): 'at most 5 trades per rule per day, ranked by volume "
        "ratio' ranks first-triggers from different minutes of the same session against each other, "
        "so a 13:30 trigger can displace a 09:55 one. That is a look-ahead in the CAP, never in a "
        "fill price; it is pre-registered and applied as written. n_first_triggers per rule is "
        "reported next to n_trades so the discarded tail is visible."
    )
    notes.append(
        "PINNED READINGS (ambiguity resolutions, not tuned parameters; the alternatives were not "
        "computed): (1) a 5m bar's 'close minute' is the START minute of its last 1m bar, so the "
        f"signal window is last_mfo in [{_lo_mfo}, {hi_mfo}] = "
        f"{from_mfo(_lo_mfo)}..{from_mfo(hi_mfo)} (bucket starts 09:45..13:55); (2) R2's 'each close "
        "> the prior bar's HIGH' compares each of the three closes with ITS OWN prior bar's high; "
        "(3) R3's 'session high set >= 3 bars ago' compares the running max high over bars <= i-3 "
        "with the running max over bars <= i; (4) R3's 'stayed above VWAP' compares each pullback "
        "bar's close with the running VWAP AT ITS OWN minute; (5) breadth 'at 11:00' is the last "
        "COMPLETED 5m bar before 11:00, i.e. the [10:55,11:00) bucket."
    )
    notes.append(
        "MEDVOL20 FLOOR: a bar needs >= 8 prior VALID 5m bars in the same session, so the earliest "
        "possible signal bar is the [09:55,10:00) bucket even though the window opens at 09:45. "
        "That same floor guarantees every rule's 4-bar and 6-bar lag windows are fully populated."
    )
    notes.append(
        f"SESSION FILTER: a session is a bars_1m date carrying MORE than {p['session_min_symbols']} "
        f"symbols. {len(dropped_thin)} date(s) failed that and were dropped whole"
        + (
            f" ({min(dropped_thin)} .. {max(dropped_thin)}) - these are the watchlist-cap rollout "
            "days when the poller held only 50-103 names. The drop is material to two splits: it "
            "removes most of the window in which the REAL 'NIFTY 50' 1m series exists, and most of "
            "the catalyst-feed window."
            if dropped_thin else "."
        )
    )
    notes.append(
        "INDEX PROXY: bars_1m carries '" + str(p["index_symbol"]) + "' 1m only from "
        + (str(min(real_index_dates)) if real_index_dates else "n/a")
        + " (within the kept sessions); for earlier sessions the index return at the signal minute "
        "is the EQUAL-WEIGHT MEAN of close/open_0915 - 1 across that session's eligible universe, "
        f"read on the same 5m grid. The '{SPLIT_REAL_INDEX}' split is the subset measured against "
        "the real index."
    )
    notes.append(
        "UNIVERSE: universe_daily included rows where present, else the symbol set present in "
        f"bars_1m that session ({len(fallback_dates)} fallback sessions). Where universe_daily IS "
        "present its included count ramps 0 -> 50 -> 100 -> 200 across 2026-07/08 (the watchlist-cap "
        "rollout), so the eligible-set SIZE is not constant across the window and the breadth "
        "split's denominator changes with it."
    )
    notes.append(
        "SURVIVORSHIP: the symbol set is whatever bars_1m holds today (~200 names); names that left "
        "the index or delisted are absent, which biases every cell optimistically."
    )
    notes.append(
        "E1 STOP FILLS ARE OPTIMISTIC WHERE A BAR GAPS THROUGH: the brief says 'low <= stop -> fill "
        "at stop', so a minute that opens BELOW the stop still books the stop price. The count of "
        "such fills is reported per cell as n_stop_gap_through; a live stop would have filled worse."
    )
    notes.append(
        "E1 SKIPS A TRADE WHOSE STOP IS AT OR ABOVE THE ENTRY (the next 1m open gapped below the "
        "signal bar's low, or below VWAP for R5). Booking it at 'stop' would print a fictitious "
        "gain, so it is dropped and counted as stop_at_or_above_entry. E2 has no stop and keeps "
        "those symbol-days, so E1 and E2 populations differ by exactly that count."
    )
    notes.append(
        "T-STAT OVERSTATES INDEPENDENCE: up to 5 picks share each session and each regime, so the "
        "per-trade t-stat treats correlated draws as independent. The CPCV positive-split share, "
        "computed on per-SESSION means, is the check that does not."
    )
    notes.append(
        "CATALYST SPLIT COVERAGE: catalyst_watchlist "
        f"{cat_cov['catalyst_watchlist_first_d']} -> {cat_cov['catalyst_watchlist_last_d']} "
        f"({cat_cov['catalyst_watchlist_rows']} rows); news_clusters "
        f"{str(cat_cov['news_clusters_first_seen'])[:10]} -> "
        f"{str(cat_cov['news_clusters_last_seen'])[:10]} ({cat_cov['news_clusters_rows']} rows). The "
        f"split cells cover ONLY trades on or after {cat_cov['split_covered_from']}; before that the "
        "flag would measure the absence of the feed, so those trades are in neither catalyst cell."
    )
    if years_without_calendar:
        notes.append(
            "CALENDAR: config/calendar has no year file for "
            + ", ".join(str(y) for y in sorted(years_without_calendar))
            + ", so NSECalendar cannot confirm those sessions. Sessions are taken from bars_1m's own "
            "distinct dates, which are trading days by construction; calendar confirmation was "
            "applied for every year that does have a file."
        )
    if non_trading_dates:
        notes.append(
            "CALENDAR MISMATCH: bars_1m holds bars on "
            f"{len(non_trading_dates)} date(s) NSECalendar calls non-trading: "
            + ", ".join(sorted(non_trading_dates)[:10])
            + " - weekend Union-Budget special sessions carrying a full tape. They are real sessions "
            "and are KEPT; the mismatch is a gap in config/calendar, not in the data."
        )

    rt_pct = float(cost_model.breakeven_pct(Decimal(p["reference_notional_inr"]), p["product"]))
    doc: dict[str, Any] = {
        "meta": {
            "script": "scripts/backtest_candles.py",
            "study_id": "candles",
            "brief": "runbooks/briefs/candles_backtest_brief_2026-09-04.md",
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "window": {"start": str(start), "end": str(end)},
            "n_sessions": len(sessions),
            "first_session": str(sessions[0]),
            "last_session": str(sessions[-1]),
            "params": {k: (str(v) if isinstance(v, Decimal) else v) for k, v in p.items()},
            "rules": {k: {"name": v["name"], "text": v["text"], "sql": v["sql"].format(**p)}
                      for k, v in RULES.items()},
            "exits": {k: dict(v) for k, v in EXITS.items()},
            "parameter_sweep_run": False,
            "cell_selection_performed": False,
            "bar_convention": (
                "5m buckets anchored at 09:15 ([09:15,09:20), ...); a bucket with fewer than "
                f"{p['min_1m_bars_per_bucket']} 1m bars is INVALID and dropped from the relation"
            ),
            "entry_convention": (
                "OPEN of the 1m bar at signal.last_minute + 1 (never the signal bar's own close); "
                "a missing bar at that exact minute is a SKIPPED trade"
            ),
            "exit_conventions": {k: v["text"] for k, v in EXITS.items()},
            "product": p["product"],
            "reference_notional_inr": p["reference_notional_inr"],
            "cost_round_trip_pct": round(rt_pct, 6),
            "cost_slippage": (
                f"{p['slippage_ticks_per_side']} tick of Rs {p['tick_size_inr']} each side, "
                "charged as 2*tick/entry_px*100 percent"
            ),
            "promotion_rule": (
                "mean net % > 0 AND t > 2 AND n >= 200 AND CPCV positive-split share >= 0.60"
            ),
        },
        "coverage": {
            "n_sessions_kept": len(sessions),
            "n_dates_dropped_thin": len(dropped_thin),
            "dates_dropped_thin": sorted(str(d) for d in dropped_thin),
            "n_universe_daily_absent_fallback_dates": len(fallback_dates),
            "universe_daily_dates_present": sorted(str(d) for d in universe),
            "eligible_symbols_per_session": {
                "min": min(len(v) for v in elig.values()),
                "median": int(statistics.median([len(v) for v in elig.values()])),
                "max": max(len(v) for v in elig.values()),
            },
            "real_index_sessions": {
                "n": len(real_index_dates),
                "first": str(min(real_index_dates)) if real_index_dates else None,
                "last": str(max(real_index_dates)) if real_index_dates else None,
            },
            "catalyst_sources": cat_cov,
        },
        "diagnostics": {
            "n_first_triggers_by_rule": n_first_triggers,
            "n_selected_symbol_days_by_rule": (
                {r: int((selected["rule"] == r).sum()) for r in RULES} if not selected.empty
                else dict.fromkeys(RULES, 0)
            ),
            "n_selected_pairs_fetched": int(len(pairs)),
        },
        "results": {},
        "notes": notes,
    }

    for cell, trades in trades_by_cell.items():
        rule, style = cell.split("|")
        doc["results"][cell] = {
            "rule": rule, "rule_name": RULES[rule]["name"], "exit": style,
            "exit_name": EXITS[style]["name"],
            "n_trades": len(trades),
            "n_first_triggers": n_first_triggers[rule],
            "dropped": drops[cell],
            "n_stop_gap_through": sum(1 for t in trades if t.stop_gap_through),
            "exit_reason_counts": {
                r: sum(1 for t in trades if t.exit_reason == r)
                for r in sorted({t.exit_reason for t in trades})
            },
            "splits": {c: metrics(ct, p) for c, ct in split_cells(trades, p).items()},
            "example_trades": example_trades(trades),
        }
    return doc, trades_by_cell


# =============================================================================== rendering
_ASCII_MAP = str.maketrans({
    "—": "-", "–": "-", "‘": "'", "’": "'", "“": '"', "”": '"',
    "→": "->", "≥": ">=", "≤": "<=", "×": "x", "₹": "Rs ", " ": " ",
    "§": "S",
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
    add("=" * 132)
    add("candles PRE-REGISTERED BACKTEST - five 5-minute price-action rules x two exits (10 cells)")
    add(f"({m['brief']})")
    add("=" * 132)
    add(f"generated        : {m['generated_at']}")
    add(f"window           : {m['window']['start']} -> {m['window']['end']}  "
        f"({m['n_sessions']} sessions, {m['first_session']} .. {m['last_session']})")
    add(f"sweep run        : {m['parameter_sweep_run']}   cell selection: {m['cell_selection_performed']}")
    add(f"bars             : {m['bar_convention']}")
    add(f"entry            : {m['entry_convention']}")
    for k, v in m["exit_conventions"].items():
        add(f"exit {k}           : {v}")
    add(f"cost             : {m['cost_round_trip_pct']:.4f}% {m['product']} round trip at Rs "
        f"{m['reference_notional_inr']} + {m['cost_slippage']}")
    add(f"promotion rule   : {m['promotion_rule']}")
    add("")
    add("-" * 132)
    add("THE FIVE PRE-REGISTERED RULES (transcribed; all long-only, evaluated on a COMPLETED 5m bar)")
    add("-" * 132)
    for k, v in m["rules"].items():
        add(f"  {k} {v['name']:<16}: {v['text']}")
    add("")

    cov = doc["coverage"]
    add("-" * 132)
    add("STEP 1 - SAMPLE SHAPE")
    add("-" * 132)
    add(f"  sessions kept                    : {cov['n_sessions_kept']}")
    add(f"  dates dropped (thin, <= floor)   : {cov['n_dates_dropped_thin']}"
        + (f"  ({cov['dates_dropped_thin'][0]} .. {cov['dates_dropped_thin'][-1]})"
           if cov["dates_dropped_thin"] else ""))
    e = cov["eligible_symbols_per_session"]
    add(f"  eligible symbols per session     : min {e['min']} / median {e['median']} / max {e['max']}")
    add(f"  universe_daily present on        : {len(cov['universe_daily_dates_present'])} session(s); "
        f"{cov['n_universe_daily_absent_fallback_dates']} session(s) used the bars_1m fallback")
    ri = cov["real_index_sessions"]
    add(f"  real index sessions              : {ri['n']} ({ri['first']} -> {ri['last']})")
    cc = cov["catalyst_sources"]
    add(f"  catalyst split covers trades on/after {cc['split_covered_from']}")
    add("")
    dg = doc["diagnostics"]
    add("  rule | first triggers | selected (after the 5/day cap)")
    add("  -----+----------------+-------------------------------")
    for r, n in dg["n_first_triggers_by_rule"].items():
        add(f"  {r:<4} | {n:>14} | {dg['n_selected_symbol_days_by_rule'][r]:>10}")
    add("")

    add("-" * 132)
    add("STEP 2 - RESULTS: EVERY RULE x EXIT x SPLIT (all reported; none selected)")
    add("-" * 132)
    add("  cell     | split                        |     n | mean net% |  med net% | mean gross% |"
        "  mean cost% |   win% |  t-stat | CPCV+ | promotable")
    add("  ---------+------------------------------+-------+-----------+-----------+-------------+"
        "-------------+--------+---------+-------+-----------")
    for cell, block in doc["results"].items():
        for name, s in block["splits"].items():
            win = "-" if s["win_rate"] is None else f"{s['win_rate'] * 100:.1f}"
            share = s["cpcv"]["positive_share"]
            sh = "-" if share is None else f"{share * 100:.0f}%"
            add(f"  {cell:<8} | {name:<28} | {s['n']:>5} | {_f(s['mean_net_pct'], 9)} | "
                f"{_f(s['median_net_pct'], 9)} | {_f(s['mean_gross_pct'], 11)} | "
                f"{_f(s['mean_cost_pct'], 11)} | {win:>6} | {_f(s['t_stat'], 7, 2)} | {sh:>5} | "
                f"{str(s['promotable'])}")
        add(f"  {cell:<8} | exits={block['exit_reason_counts']} dropped={block['dropped']} "
            f"stop_gap_through={block['n_stop_gap_through']}")
        add("  " + "-" * 128)
    add("")

    add("-" * 132)
    add("STEP 3 - PROMOTABLE CELLS (the brief's boolean, verbatim; no other claim is made)")
    add("-" * 132)
    hits = [
        (c, n) for c, b in doc["results"].items() for n, s in b["splits"].items() if s["promotable"]
    ]
    if hits:
        for c, n in hits:
            s = doc["results"][c]["splits"][n]
            add(f"  promotable=True: cell {c}, split {n} "
                f"(n={s['n']}, mean net {s['mean_net_pct']}%, t={s['t_stat']}, "
                f"CPCV+ {s['cpcv']['positive_share']})")
    else:
        add("  promotable=True in NO rule x exit x split cell.")
    add("")

    add("-" * 132)
    add("NOTES / CAVEATS (reported, not massaged)")
    add("-" * 132)
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
    return repo_root() / "data" / "reports" / "backtest_candles_2026-09-04.json"


def build_calendar() -> NSECalendar | None:
    try:
        return NSECalendar(config_dir() / "calendar", Clock(), strict=False)
    except Exception:  # noqa: BLE001 - calendar confirmation is a check, never load-bearing
        return None


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog="backtest_candles",
        description=(
            "candles pre-registered backtest (one PARAMS dict, five fixed RULES, two fixed EXITS, no "
            "sweep and no selection). Read-only against bars_1m; refuses to run if the DuckDB file "
            "is missing or locked by the engine."
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
        print(f"backtest_candles: REFUSING TO RUN.\n  {_ascii(str(exc))}", file=sys.stderr)
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
        print(f"backtest_candles: {exc}", file=sys.stderr)
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
