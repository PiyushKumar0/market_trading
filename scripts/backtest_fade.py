#!/usr/bin/env python
"""``fade`` PRE-REGISTERED backtest harness (IMPLEMENTATION_PLAN.md `fade` pre-registration, R4,
2026-09-12) - the SHORT side of intraday, which no backtest in this repo has ever run.

Every intraday sweep here has been LONG-ONLY under the §1.4.9 short gate: ORB twice, `tdc`, the
candle battery, the aborted VWAP-deviation study. The one piece of evidence pointing the other way
is the `orb` v2 finding of a **negative-gross fade of breakouts in 2025-H2** - i.e. the thing that
killed the long side is the prior FOR this study. This harness measures two fade rules and nothing
else.

===============================================================================================
PRE-REGISTRATION / MULTIPLICITY DISCIPLINE - READ FIRST
===============================================================================================
**NO PARAMETER SWEEP IS RUN BY THIS SCRIPT, AND NONE WAS RUN BEFORE IT.** There is exactly one
:data:`PARAMS` dict and one :data:`RULES` dict, both fixed at import time and both transcribed from
the plan paragraph. **TRIAL COUNT N = 2** - :data:`PRE_REGISTERED_RULES` (``F1``, ``F2``). Every
other entry in :data:`RULES` is a REPORTING-ONLY cell, marked ``registered=False``, and is never a
competitor F1/F2 is picked from: two of them are the plan's stated reporting variants (F1 at 12:00,
F2 with a 15:15-only exit) and two are EXECUTION-CONSERVATISM cells (the stop evaluated on the 1m
HIGH instead of the 1m close) that can only make a fade look WORSE. The CLI exposes no flag that
changes a signal parameter.

``promotable`` is the house `tdc` boolean and nothing more: ``mean net % > 0`` AND ``t > 2`` AND
``n >= 200`` AND ``CPCV positive-split share >= 0.60``. Anything less is a REFUTATION, it is
reported in exactly those terms, and it changes nothing.

**Shorts are gated for AUTO by §1.4.9 whatever this study finds.** A promotable cell is evidence for
a pre-registered origination proposal, never a wiring: this script wires nothing.

===============================================================================================
THE TWO RULES (transcribed from the plan; every clause is evaluated at a bar CLOSE)
===============================================================================================
**F1 - failed-breakout fade.** At the decision bar ``T`` (11:00 IST):

* the symbol's 1m close broke ABOVE its opening-range high at some bar in ``[09:30, 10:30)``
  ("before 10:30"). The OR high is ``max(high)`` over ``09:15..09:29`` - the first fifteen 1m bars,
  COMPLETED bars only (``ts_minute`` is the bar's minute START, so the 09:29 bar is the last bar of
  the 09:15-09:30 range and the 09:30 bar is not in it). The break scan therefore starts at 09:30:
  an OR bar cannot break the high it is itself part of.
* ``rel_volume_tod >= 1.5`` - cumulative volume 09:15..T over the MEDIAN, across the 20 sessions
  immediately preceding this one, of that symbol's cumulative volume to the SAME minute. Fewer than
  ``rvol_min_valid_sessions`` (10) usable priors ⇒ the symbol-day is SKIPPED and counted, never
  admitted on a thin denominator. PINNED INTERPRETATION: the plan phrase "broke ... on
  rel_volume_tod >= 1.5" does not say at WHICH minute participation is measured; it is measured at
  the DECISION bar T, which is the `tdc` definition verbatim, so the same words mean the same thing
  in both studies. The alternative (at the break minute) was NOT computed, so no selection between
  the two took place.
* the T bar's close is back BELOW the OR high (strictly).

**F2 - gap fade.** At the decision bar ``T`` (10:00 IST):

* the 09:15 bar's OPEN is ``>= +2.0%`` above the PRIOR CLOSE, and
* the T bar's close is below the session open (strictly).

**Both:** SHORT at the OPEN of the bar immediately after T (11:01 / 10:01). The stop is the SESSION
HIGH SO FAR - ``max(high)`` over ``09:15..T``, fixed at entry, never trailed. The exit is the first
1m close ABOVE the fade-failure level - the OR HIGH for F1, the T BAR's HIGH for F2 - filled at the
FOLLOWING bar's open; else the 15:15 squareoff (the 15:15 bar's close, or the last bar at or before
it). Whichever comes first. No target.

**The stop is structurally DOMINATED and the report says so rather than hiding it:** the session
high through T is by construction ``>=`` the OR high (F1) and ``>=`` the T bar's high (F2), so a
close above the stop is also a close above the failure level and the failure exit fires on the same
bar at the same price. Under the registered close-trigger the stop can therefore never be the SOLE
reason for an exit; ``n_exits_stop_first`` is reported and is expected to be 0. That is exactly why
the two ``*_stop_on_1m_high`` reporting cells exist: evaluated on the bar HIGH the stop DOES bind,
strictly earlier and strictly worse for the short, and those cells are the honest execution check.

===============================================================================================
SHORT ARITHMETIC AND COSTS
===============================================================================================
A short's equal-notional percent return is ``(1 - exit/entry) * 100`` - profit per share over the
ENTRY notional. (``entry/exit - 1`` is a different number and is not used anywhere.)

``CostModel.breakeven_pct(notional, "MIS")`` - ONE full round trip, statutory fees PLUS the measured
bid-ask spread (WO-2) - plus ONE TICK of slippage on EACH side, charged as ``2 * tick / entry_px *
100`` percent. The round trip is SIDE-AGNOSTIC: a short is the same two legs in the reverse order,
and the components that are per-side (intraday STT on the sell, stamp duty on the buy) are each
charged exactly once either way. MIS, never CNC - a short leg cannot be a delivery position.

**Not modelled, and material:** shortability. NSE permits intraday short selling on MIS, but a name
in a T2T/GSM/ASM surveillance bucket, a stock the broker has blocked for MIS, or a security under an
F&O ban is not shortable at all on the day. The eligible universe already screens ``mis_candidate``
and surveillance flags AS OF ITS LATEST SNAPSHOT, not as of each historical session, so this study
assumes a shortability it does not prove. Every number here is therefore an UPPER bound on what the
short side could actually have been traded at.

===============================================================================================
LOOK-AHEAD DISCIPLINE (the one class of error that would make every number here a fiction)
===============================================================================================
* Every signal input is computed from bars with ``ts_minute <= T`` ONLY. The feature SQL filters
  ``time <= T`` BEFORE it aggregates, so a bar after T is not merely unused - it is not in the
  relation. The stop level (session high) and the F2 exit level (the T bar's high) come from that
  same pre-T relation.
* The ``rel_volume_tod`` denominator uses STRICTLY PRIOR sessions. Today's own volume never enters
  its own median.
* Entry is the OPEN of the bar AFTER T. The decision bar's own close is never a fill price.
* A close-triggered exit fills at the NEXT bar's OPEN - the first price an order placed on that
  close could actually reach - never at the close that triggered it.
* The F2 prior close is the symbol's last ``bars_1d`` close STRICTLY BEFORE ``d``.
* The index-regime split is computed from the same T-bar snapshot as the signal, not from the
  session's outcome. The ATR tercile (R3's statistic, imported definition) is computed over the 14
  daily sessions STRICTLY BEFORE the signal day, so the signal day's own range never enters the
  label it is sorted by.

===============================================================================================
POPULATION (survivorship, stated not buried)
===============================================================================================
The ELIGIBLE universe - the CURRENT (latest ``universe_daily`` day) ``included`` rows plus rows
excluded for ``['watchlist_cap']`` exactly, i.e. the set ``get_universe_eligible_symbols`` hands the
live sweep, which is NIFTY 500-based since O15 (2026-09-04) - applied BACKWARDS over the whole
window and intersected with the symbols ``bars_1m`` actually holds that session. This is a
**survivorship-tainted proxy** in the same sense the `brk20` and `hi52` reports carry: index
membership as of each historical session is stored nowhere in this platform. For a LONG continuation
study that bias is optimistic; for a SHORT study the sign is not obvious (a name that survived to be
in today's NIFTY 500 is a name that did not collapse), so it is reported, not corrected, and the
JSON carries ``population_is_survivorship_tainted_proxy: true``.

``bars_1m`` holds only the ~200-symbol tick watchlist plus the index, so the measured population is
the LIQUID head of the eligible set, not its tail.

===============================================================================================
EX-DATE VETO (universal, one window, both rules)
===============================================================================================
A symbol-day is VETOED when ``corp_actions`` carries ANY ex-date in the 35 CALENDAR days ending at
``d`` (the `brk20`/`hi52` window). Kite minute candles are adjusted AT FETCH TIME (§14 Q10), so a
series stitched across an ex-date can hold two unit systems, and the 20-session ``rel_volume_tod``
denominator is distorted by a split's volume multiplier either way. F2 additionally compares two
SERIES (a ``bars_1d`` prior close against a ``bars_1m`` open); that cross-source comparison gets its
own guards - a prior close older than ``prior_close_max_age_days`` (7) is a suspension, not a gap,
and a ``bars_1d``/``bars_1m`` prior-close disagreement over 3% is a unit mismatch rather than the
~0.5% the NSE's 30-minute closing VWAP routinely differs from the 15:29 print by. Both veto and are
counted.

===============================================================================================
SHARED PLUMBING
===============================================================================================
``scripts/backtest_tdc.py`` is IMPORTED, not copied: the read-only DB attach, the HH:MM helpers, the
``rel_volume_tod`` medians, the per-year coverage shape, the cost function, the CPCV/metrics/
promotion boolean and the ASCII folding are all its code, so the two intraday studies cannot drift
apart in definition. Only the two SQL pulls that genuinely differ (fade features, exit bars WITH the
bar high) and the fade execution model are new here.

===============================================================================================
DATA ACCESS
===============================================================================================
Read-only, always (``duckdb.connect(..., read_only=True)``): ``MarketStore.open()`` runs schema DDL
and is a WRITER, so it is deliberately not used. DuckDB takes a file lock even for a read-only
attach, so the engine service must be stopped; if the file is missing or locked this script REFUSES
with exit code 2 rather than degrading. Nothing here ever writes to the store.

Exit codes: 0 = ran; 2 = DB unopenable / no eligible universe / no bars in window.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import math
import os
import statistics
import sys
from bisect import bisect_left
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
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
import pandas as pd  # noqa: E402
from engine.core.calendar import NSECalendar  # noqa: E402
from engine.core.config import repo_root  # noqa: E402
from engine.marketdata.reconcile import DEFAULT_TICK_SIZE  # noqa: E402
from engine.strategy.cost_model import CostModel  # noqa: E402
from engine.strategy.indicators import wilder_atr  # noqa: E402


# ================================================================================================
# The `tdc` harness is the shared plumbing (see SHARED PLUMBING above). It is a loose script, not a
# package module, so it is loaded by path exactly as its own test suite loads it.
# ================================================================================================
def _load_tdc() -> Any:
    path = Path(__file__).resolve().parent / "backtest_tdc.py"
    spec = importlib.util.spec_from_file_location("mt_backtest_tdc_shared", path)
    if spec is None or spec.loader is None:  # pragma: no cover - only if the sibling script is gone
        raise RuntimeError(f"cannot load the shared tdc plumbing from {path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


tdc = _load_tdc()

DbUnopenable = tdc.DbUnopenable
open_readonly = tdc.open_readonly
rvol_medians = tdc.rvol_medians
coverage_by_year = tdc.coverage_by_year
trade_cost_pct = tdc.trade_cost_pct
metrics = tdc.metrics
build_calendar = tdc.build_calendar
_validate_hhmm = tdc._validate_hhmm
_shift = tdc._shift
_f = tdc._f


def _ascii(text: str) -> str:
    """``tdc``'s ASCII fold plus the section sign, which its map does not carry - left alone it
    reaches the cp1252 console as '?', which reads as a corrupted report rather than a citation."""
    return tdc._ascii(text.replace("§", "sec."))

#: ``universe_daily`` exclusion reason that still leaves a symbol ELIGIBLE (it cleared every §3.2.4
#: criterion and lost only the 200-name tick-watchlist cap). Same constant `brk20` reads.
EXCL_WATCHLIST_CAP = "watchlist_cap"

# ================================================================================================
# THE ONE PRE-REGISTERED PARAMETER SET. Transcribed from IMPLEMENTATION_PLAN.md, the `fade` (R4)
# pre-registration paragraph. Not a knob, not a grid, not reachable from the CLI.
# ================================================================================================
PARAMS: dict[str, Any] = {
    # --- the opening range (completed bars only: 09:15..09:29)
    "or_start": "09:15",
    "or_end_exclusive": "09:30",
    # --- F1: the breakout must happen in [09:30, 10:30)
    "break_window_end_exclusive": "10:30",
    "f1_rvol_min": 1.5,
    "rvol_lookback_sessions": 20,
    "rvol_min_valid_sessions": 10,
    # --- F2: the gap
    "f2_gap_min_pct": 2.0,
    "prior_close_max_age_days": 7,
    "prior_close_source_max_disagreement_pct": 3.0,
    # --- execution
    "squareoff_time": "15:15",
    "index_symbol": "NIFTY 50",
    # --- costs
    "product": "MIS",
    "reference_notional_inr": "20000",
    "slippage_ticks_per_side": 1,
    "tick_size_inr": str(DEFAULT_TICK_SIZE),
    # --- vetoes / splits
    "exdate_veto_window_days": 35,
    "atr_period": 14,
    # --- the plan's promotion rule (a boolean, not a recommendation)
    "promote_min_n": 200,
    "promote_min_t": 2.0,
    "promote_min_cpcv_positive_share": 0.60,
    # --- CPCV (engine.learning.validate defaults: 6 folds / 2 test folds / purge 5 / embargo 5)
    "cpcv_purge_obs": 5,
    "cpcv_embargo_obs": 5,
}

KIND_FAILED_BREAKOUT = "failed_breakout"
KIND_GAP = "gap"
EXIT_OR_HIGH = "or_high"
EXIT_T_BAR_HIGH = "t_bar_high"
STOP_SESSION_HIGH = "session_high_at_entry"
TRIGGER_CLOSE = "close"
TRIGGER_HIGH = "high"


@dataclass(frozen=True)
class RuleSpec:
    """One measured cell. ``registered`` marks the two that carry the trial count.

    ``family`` is the REGISTERED rule a cell belongs to. The ATR tercile cuts are taken once per
    family (R3's posture), so a symbol-day sits in the same cell under every variant of its rule and
    the cells compare populations rather than cuts. F1 and F2 measure different populations at
    different decision bars, so their cuts are never pooled with each other.
    """

    name: str
    family: str
    kind: str
    decision_time: str
    exit_level: str | None
    stop: str | None
    stop_trigger: str
    registered: bool
    label: str


# ================================================================================================
# EVERY RULE IS REPORTED. NONE IS SELECTED. Two are REGISTERED (the trial count is 2); the other four
# are reporting-only cells - two named by the plan paragraph, two execution-conservatism checks that
# can only make the short look worse.
# ================================================================================================
RULES: dict[str, RuleSpec] = {
    "F1": RuleSpec(
        "F1", "F1", KIND_FAILED_BREAKOUT, "11:00", EXIT_OR_HIGH, STOP_SESSION_HIGH, TRIGGER_CLOSE,
        True,
        "failed-breakout fade: broke the OR high before 10:30 on rvol>=1.5, back below it at 11:00",
    ),
    "F2": RuleSpec(
        "F2", "F2", KIND_GAP, "10:00", EXIT_T_BAR_HIGH, STOP_SESSION_HIGH, TRIGGER_CLOSE, True,
        "gap fade: open >= +2.0% vs the prior close, 10:00 close below the session open",
    ),
    "F1_T1200": RuleSpec(
        "F1_T1200", "F1", KIND_FAILED_BREAKOUT, "12:00", EXIT_OR_HIGH, STOP_SESSION_HIGH,
        TRIGGER_CLOSE, False, "REPORTING ONLY (plan): F1 with the decision bar at 12:00",
    ),
    "F2_1515_only": RuleSpec(
        "F2_1515_only", "F2", KIND_GAP, "10:00", None, None, TRIGGER_CLOSE, False,
        "REPORTING ONLY (plan): F2 held to the 15:15 squareoff, no failure exit and no stop",
    ),
    "F1_stop_on_1m_high": RuleSpec(
        "F1_stop_on_1m_high", "F1", KIND_FAILED_BREAKOUT, "11:00", EXIT_OR_HIGH,
        STOP_SESSION_HIGH, TRIGGER_HIGH, False,
        "REPORTING ONLY (execution conservatism): F1 with the stop evaluated on the 1m HIGH",
    ),
    "F2_stop_on_1m_high": RuleSpec(
        "F2_stop_on_1m_high", "F2", KIND_GAP, "10:00", EXIT_T_BAR_HIGH, STOP_SESSION_HIGH,
        TRIGGER_HIGH, False,
        "REPORTING ONLY (execution conservatism): F2 with the stop evaluated on the 1m HIGH",
    ),
}

#: The rules the trial count is computed over. Everything else in :data:`RULES` is reported beside
#: them and can never be selected in their place.
PRE_REGISTERED_RULES: tuple[str, ...] = ("F1", "F2")
TRIAL_COUNT_N = len(PRE_REGISTERED_RULES)

SPLIT_ALL = "all"
SPLIT_INDEX_UP = "index_up_at_entry"
SPLIT_INDEX_DOWN = "index_down_at_entry"
SPLIT_REAL_INDEX = "real_nifty50_index_only"
ATR_LOW = "atr_tercile_1_calmest"
ATR_MID = "atr_tercile_2_middle"
ATR_HIGH = "atr_tercile_3_wildest"
ATR_UNCLASSIFIED = "atr_pct_unclassified"
#: The four ATR cells sum to the pooled cell exactly - a symbol-day without 14 prior daily sessions
#: is LABELLED, never dropped (R3's construction, so the two studies' ATR rows are comparable).
ATR_CELLS = (ATR_LOW, ATR_MID, ATR_HIGH, ATR_UNCLASSIFIED)

EXIT_FADE_FAILED = "fade_failed_next_open"
EXIT_STOP_NEXT_OPEN = "stop_next_open"
EXIT_STOP_INTRABAR = "stop_intrabar_high"
EXIT_SQUAREOFF = "squareoff_1515"


# =============================================================================== data structures
@dataclass
class Trade:
    """One measured SHORT. ``gross_pct``/``net_pct`` are PERCENT, per-trade equal notional."""

    rule: str
    symbol: str
    d: date
    entry_px: float
    exit_px: float
    exit_reason: str
    gross_pct: float
    cost_pct: float
    net_pct: float
    or_high: float
    stop_level: float
    exit_level: float | None
    rvol: float
    gap_pct: float | None
    index_ret_pct: float
    index_is_real: bool
    atr_pct: float | None = None
    atr_tercile: str = ATR_UNCLASSIFIED
    side: str = "SHORT"


@dataclass
class DayFeatures:
    """The T-bar snapshot for one session, per symbol. Every value comes from bars at or before T."""

    d: date
    symbols: list[str] = field(default_factory=list)
    open_0915: dict[str, float] = field(default_factory=dict)
    or_high: dict[str, float] = field(default_factory=dict)
    max_close_break: dict[str, float] = field(default_factory=dict)
    close_t: dict[str, float] = field(default_factory=dict)
    high_t: dict[str, float] = field(default_factory=dict)
    sess_high_t: dict[str, float] = field(default_factory=dict)
    rvol: dict[str, float] = field(default_factory=dict)
    ret_pct: dict[str, float] = field(default_factory=dict)


# =============================================================================== universe
def load_eligible_universe(conn: duckdb.DuckDBPyConnection) -> tuple[set[str], str, date | None]:
    """``(symbols, provenance, as_of_day)`` - the CURRENT eligible universe, applied backwards.

    ``MarketStore.get_universe_eligible_symbols``'s predicate restated over the latest
    ``universe_daily`` day: ``included`` OR ``exclusion_reasons == ['watchlist_cap']`` exactly (the
    strict single-reason equality - a row excluded for the cap AND anything else is NOT eligible).
    Empty set when the table is absent or holds no rows; the caller REFUSES on that, because the
    eligible universe IS the population of this study and a fallback would silently measure a
    different one.
    """
    try:
        as_of = conn.execute("SELECT max(d) FROM universe_daily").fetchone()[0]
    except Exception:  # noqa: BLE001 - a missing/renamed table is a refusal, not a crash
        return set(), "UNAVAILABLE (no universe_daily table)", None
    if as_of is None:
        return set(), "UNAVAILABLE (universe_daily is empty)", None
    as_of = as_of.date() if hasattr(as_of, "date") else as_of
    rows = conn.execute(
        "SELECT symbol, included, exclusion_reasons FROM universe_daily WHERE d = ?", [as_of]
    ).fetchall()
    symbols = {
        str(sym).upper()
        for sym, included, reasons in rows
        if bool(included) or list(reasons or []) == [EXCL_WATCHLIST_CAP]
    }
    return symbols, f"universe_daily as of {as_of} (latest snapshot, applied backwards)", as_of


# =============================================================================== feature extraction
def load_features(
    conn: duckdb.DuckDBPyConnection,
    start: date,
    end: date,
    *,
    decision_time: str,
    params: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Per ``(symbol, d)`` T-bar snapshot, computed from bars with ``ts_minute <= T`` ONLY.

    Columns: ``symbol, d, open_0915, or_high, max_close_break, close_t, high_t, n_t, sess_high_t,
    cumvol_t, n_bars_to_t``. The ``time <= T`` filter is applied BEFORE the aggregates, so no bar
    after the decision minute is in the relation they are taken over - look-ahead is structurally
    impossible here, not merely avoided. ``sess_high_t`` (the stop level) and ``cumvol_t`` are
    therefore plain aggregates of that pre-T relation.

    ``max_close_break`` is the greatest 1m CLOSE in the break window; "some close broke above the OR
    high" is exactly ``max_close_break > or_high``, which is why the window's own aggregate is
    enough and no self-join on the group's own ``or_high`` is needed. The window is clamped at T so
    the column is never a look-ahead when a rule's T precedes the window end (only
    ``failed_breakout`` rules read it, and :func:`run_study` refuses such a rule outright).
    """
    p = dict(params or PARAMS)
    t_lit = _validate_hhmm(decision_time)
    or_lo = _validate_hhmm(p["or_start"])
    or_hi = _validate_hhmm(p["or_end_exclusive"])
    brk_hi = min(_validate_hhmm(p["break_window_end_exclusive"]), t_lit)
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
    )
    SELECT symbol, d,
           MAX(o) FILTER (tt = TIME '{or_lo}')                                 AS open_0915,
           MAX(h) FILTER (tt >= TIME '{or_lo}' AND tt < TIME '{or_hi}')        AS or_high,
           MAX(c) FILTER (tt >= TIME '{or_hi}' AND tt < TIME '{brk_hi}')       AS max_close_break,
           MAX(c) FILTER (tt = TIME '{t_lit}')                                 AS close_t,
           MAX(h) FILTER (tt = TIME '{t_lit}')                                 AS high_t,
           COUNT(*) FILTER (tt = TIME '{t_lit}')                               AS n_t,
           MAX(h)                                                              AS sess_high_t,
           SUM(v)                                                              AS cumvol_t,
           COUNT(*)                                                            AS n_bars_to_t
    FROM b2
    GROUP BY 1, 2
    """
    return conn.execute(sql, [start, end]).df()


def load_exit_bars(
    conn: duckdb.DuckDBPyConnection,
    pairs: pd.DataFrame,
    *,
    decision_time: str,
    squareoff_time: str,
) -> dict[tuple[str, date], tuple[list[str], list[float], list[float], list[float]]]:
    """``(symbol, d) -> (times, opens, highs, closes)`` for the bars in ``(T, squareoff]``, ascending.

    The bar HIGH is fetched because the execution-conservatism cells evaluate the stop on it; the
    registered rules never read it. Only the selected symbol-days are fetched, so the exit scan never
    materializes the full post-T tape.
    """
    if pairs.empty:
        return {}
    t_lit = _validate_hhmm(decision_time)
    sq = _validate_hhmm(squareoff_time)
    conn.register("fade_sel", pairs)
    try:
        sql = f"""
        WITH b AS (
          SELECT symbol, (ts_minute AT TIME ZONE 'Asia/Kolkata') AS tl,
                 "open"::DOUBLE AS o, high::DOUBLE AS h, "close"::DOUBLE AS c
          FROM bars_1m
        )
        SELECT upper(b.symbol) AS symbol, b.tl::DATE AS d, strftime(b.tl, '%H:%M') AS tt,
               b.o, b.h, b.c
        FROM b JOIN fade_sel s ON s.symbol = upper(b.symbol) AND s.d = b.tl::DATE
        WHERE b.tl::TIME > TIME '{t_lit}' AND b.tl::TIME <= TIME '{sq}'
        ORDER BY 1, 2, 3
        """
        frame = conn.execute(sql).df()
    finally:
        conn.unregister("fade_sel")
    out: dict[tuple[str, date], tuple[list[str], list[float], list[float], list[float]]] = {}
    for (sym, d), grp in frame.groupby(["symbol", "d"], sort=False):
        dd = d.date() if hasattr(d, "date") else d
        out[(str(sym), dd)] = (
            grp["tt"].tolist(),
            grp["o"].astype(float).tolist(),
            grp["h"].astype(float).tolist(),
            grp["c"].astype(float).tolist(),
        )
    return out


# =============================================================================== daily context
def load_daily(
    conn: duckdb.DuckDBPyConnection, symbols: Sequence[str], end: date
) -> dict[str, tuple[list[date], list[float], list[float], list[float]]]:
    """``symbol -> (days, highs, lows, closes)`` ascending, from ``bars_1d`` up to ``end``.

    Feeds two things: F2's PRIOR CLOSE (the exchange's own previous-close reference, which is the
    30-minute closing VWAP and NOT the 15:29 print - so it is the right anchor for a gap and the
    wrong one to compare a 1m close against without tolerance) and the ATR(14,1d) tercile split.
    """
    wanted = sorted({s.strip().upper() for s in symbols})
    if not wanted:
        return {}
    sql = (
        'SELECT upper(symbol) AS symbol, d, high::DOUBLE AS h, low::DOUBLE AS l, '
        '"close"::DOUBLE AS c FROM bars_1d WHERE d <= ? AND upper(symbol) IN ('
        + ", ".join("?" * len(wanted))
        + ") ORDER BY 1, 2"
    )
    frame = conn.execute(sql, [end, *wanted]).df()
    out: dict[str, tuple[list[date], list[float], list[float], list[float]]] = {}
    if frame.empty:
        return out
    for sym, grp in frame.groupby("symbol", sort=False):
        days = [x.date() if hasattr(x, "date") else x for x in grp["d"]]
        out[str(sym)] = (
            days,
            grp["h"].astype(float).tolist(),
            grp["l"].astype(float).tolist(),
            grp["c"].astype(float).tolist(),
        )
    return out


def load_prior_close_1m(
    conn: duckdb.DuckDBPyConnection, start: date, end: date
) -> dict[tuple[str, date], float]:
    """``(symbol, d) -> the session's LAST 1m close at or after 15:00``, for the F2 cross-check.

    Restricted to the closing half hour: the last bar of a full session is 15:29, and a session with
    no bar at all after 15:00 is truncated (or a muhurat session), in which case the cross-check is
    simply unavailable and F2 falls back to the ``bars_1d`` close alone - counted, never vetoed on
    the absence of a CHECK.
    """
    sql = """
    WITH b AS (
      SELECT symbol, (ts_minute AT TIME ZONE 'Asia/Kolkata') AS tl, "close"::DOUBLE AS c
      FROM bars_1m
    )
    SELECT upper(symbol) AS symbol, tl::DATE AS d, arg_max(c, tl::TIME) AS last_c
    FROM b
    WHERE tl::TIME >= TIME '15:00' AND tl::DATE >= ? AND tl::DATE <= ?
    GROUP BY 1, 2
    """
    rows = conn.execute(sql, [start, end]).fetchall()
    return {
        (str(sym), (d.date() if hasattr(d, "date") else d)): float(c)
        for sym, d, c in rows
        if c is not None
    }


def load_ex_dates(conn: duckdb.DuckDBPyConnection) -> dict[str, list[date]]:
    """``symbol -> sorted ex-dates`` from ``corp_actions`` (every kind: a dividend moves the open
    too, and the veto is deliberately blunt rather than clever)."""
    try:
        rows = conn.execute("SELECT upper(symbol), ex_date FROM corp_actions").fetchall()
    except Exception:  # noqa: BLE001 - a missing table must not sink an offline study
        return {}
    out: dict[str, list[date]] = defaultdict(list)
    for sym, ex in rows:
        if ex is not None:
            out[str(sym)].append(ex.date() if hasattr(ex, "date") else ex)
    return {k: sorted(v) for k, v in out.items()}


def has_ex_date_within(ex_dates: dict[str, list[date]], symbol: str, d: date, days: int) -> bool:
    """True when ``symbol`` has an ex-date in the ``days``-calendar-day window ENDING at ``d``."""
    stamps = ex_dates.get(symbol)
    if not stamps:
        return False
    lo = d - timedelta(days=int(days))
    i = bisect_left(stamps, lo)
    return i < len(stamps) and stamps[i] <= d


def prior_daily_index(days: Sequence[date], d: date) -> int | None:
    """Index of the last daily bar STRICTLY BEFORE ``d``, or None. ``days`` must be ascending.

    ``bisect`` reads the sequence in place: this runs once per scanned symbol-day over a ~1,000-row
    daily history, so copying it here would cost more than the search.
    """
    i = bisect_left(days, d)
    return i - 1 if i > 0 else None


def atr_pct_at(
    daily: dict[str, tuple[list[date], list[float], list[float], list[float]]],
    symbol: str,
    d: date,
    *,
    period: int,
    cache: dict[tuple[str, date], float | None] | None = None,
) -> float | None:
    """R3's statistic VERBATIM: Wilder ATR(``period``) over the ``period`` completed daily sessions
    STRICTLY BEFORE ``d``, over the close of the last of them, in percent. ``None`` when the symbol
    has fewer than ``period`` prior daily sessions.

    Two consequences of "strictly before", both deliberate: the signal day's own range never enters
    the label it is sorted by, and the window's FIRST bar has no prior close inside the window, so
    ``wilder_atr`` gives it the plain ``high - low`` true range (R3's pinned reading; the gap-aware
    alternative was not computed, so no selection between the two took place). With exactly
    ``period`` bars the Wilder recursion never runs and the value is the SMA seed - the mean of the
    window's true ranges - which is what makes the window definition and the house function agree.

    Percent-of-close, because a rupee ATR is not comparable across a Rs 90 and a Rs 4,000 stock and
    the tercile would then be a price ranking. Only measured trades ask for this, so the cache keeps
    the per-window call count at the number of distinct measured symbol-days.
    """
    key = (symbol, d)
    if cache is not None and key in cache:
        return cache[key]
    val: float | None = None
    series = daily.get(symbol)
    if series is not None:
        days, highs, lows, closes = series
        i = prior_daily_index(days, d)
        if i is not None and period >= 1 and i + 1 >= period:
            lo = i + 1 - period
            atr = wilder_atr(highs[lo: i + 1], lows[lo: i + 1], closes[lo: i + 1], period)
            a, c = float(atr.iloc[-1]), float(closes[i])
            if math.isfinite(a) and math.isfinite(c) and c > 0:
                val = a / c * 100.0
    if cache is not None:
        cache[key] = val
    return val


def tercile_cuts(values: Sequence[float]) -> tuple[float, float] | None:
    """The two cuts of a tercile split, or None when it is not computable (needs 3+ finite values).

    ``statistics.quantiles(..., n=3)`` - the same construction ``backtest_brk20.margin_tercile_cuts``
    uses, so the two studies' tercile rows mean the same thing. A FULL-SAMPLE statistic of the
    measured population, so the split is DESCRIPTIVE, never a tradeable filter - the cuts are not
    knowable at signal time (the `brk20`/`hi52` caveat, verbatim in force). Equal cuts (a degenerate
    population) yield None rather than three cells two of which are empty by arithmetic.
    """
    xs = sorted(v for v in values if v is not None and math.isfinite(v))
    if len(xs) < 3:
        return None
    c1, c2 = statistics.quantiles(xs, n=3)
    return (float(c1), float(c2)) if c1 < c2 else None


def tercile_of(value: float | None, cuts: tuple[float, float] | None) -> str:
    """Which ATR cell ``value`` falls in - ``<= c1`` calmest, ``<= c2`` middle, else wildest, a
    partition by construction. An unclassifiable value is LABELLED, never dropped, so the four cells
    sum to the measured population exactly."""
    if value is None or cuts is None or not math.isfinite(value):
        return ATR_UNCLASSIFIED
    if value <= cuts[0]:
        return ATR_LOW
    return ATR_MID if value <= cuts[1] else ATR_HIGH


# =============================================================================== execution
def simulate_short(
    times: Sequence[str],
    opens: Sequence[float],
    highs: Sequence[float],
    closes: Sequence[float],
    *,
    entry_time: str,
    squareoff_time: str,
    exit_level: float | None,
    stop_level: float | None,
    stop_trigger: str,
) -> tuple[float, float, str] | None:
    """``(entry_px, exit_px, reason)`` for one SHORT, or ``None`` when the entry bar is missing.

    Entry is the OPEN of the ``entry_time`` bar - the bar AFTER T - and the bar must be present at
    exactly that minute; a data gap there is a skipped trade, never a fill at some other price.

    Exit, scanning from the entry bar forward:

    * ``stop_trigger == "high"`` (the reporting-only conservatism cells): the FIRST bar whose HIGH
      exceeds ``stop_level`` fills at ``max(stop_level, open)`` - once the stop is touched a short
      cannot be bought back better than the stop, and a bar that OPENED through it fills at that
      open. The intrabar touch precedes that bar's close, so it is checked FIRST, and the squareoff
      bar itself is included (a stop hit at 15:14 is a stop, not a squareoff). Still optimistic
      against real life, which adds slippage past the trigger.
    * the FIRST bar whose CLOSE is above ``exit_level`` (the fade has failed) fills at the NEXT
      bar's OPEN - never at the close that triggered it.
    * then the stop on a CLOSE, same next-open fill. Ordered AFTER the failure level because the
      stop is by construction the higher of the two, so any close above it is also above the failure
      level: under the close trigger this branch is unreachable by arithmetic, and the diagnostics
      count it to prove that rather than assume it.
    * otherwise the squareoff bar's CLOSE, the last bar at or before ``squareoff_time``.

    A non-finite or non-positive exit price is a corrupt bar, and the trade is DROPPED rather than
    booked: the asymmetry matters here in a way it never did for a long study, because a zero or
    negative buy-back price reads as a +100% win on a short. The caller counts the drop.
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

    def _done(exit_px: float, reason: str) -> tuple[float, float, str] | None:
        return (entry_px, exit_px, reason) if math.isfinite(exit_px) and exit_px > 0.0 else None

    intrabar = stop_trigger == TRIGGER_HIGH and stop_level is not None
    for k in range(i0, last + 1):
        if intrabar and float(highs[k]) > float(stop_level):
            return _done(max(float(stop_level), float(opens[k])), EXIT_STOP_INTRABAR)
        if k == last:
            break                                    # a close trigger here has no bar to fill at
        c = float(closes[k])
        if exit_level is not None and c > float(exit_level):
            return _done(float(opens[k + 1]), EXIT_FADE_FAILED)
        if stop_level is not None and stop_trigger == TRIGGER_CLOSE and c > float(stop_level):
            return _done(float(opens[k + 1]), EXIT_STOP_NEXT_OPEN)
    return _done(float(closes[last]), EXIT_SQUAREOFF)


def short_gross_pct(entry_px: float, exit_px: float) -> float:
    """Equal-notional percent return of a SHORT: profit per share over the ENTRY notional."""
    return (1.0 - exit_px / entry_px) * 100.0


# =============================================================================== splits
def split_cells(trades: Sequence[Trade]) -> dict[str, list[Trade]]:
    """Every mandatory split, as named cells. The pooled number is never the only number."""
    cells: dict[str, list[Trade]] = {
        SPLIT_ALL: list(trades),
        SPLIT_INDEX_UP: [t for t in trades if t.index_ret_pct >= 0.0],
        SPLIT_INDEX_DOWN: [t for t in trades if t.index_ret_pct < 0.0],
        SPLIT_REAL_INDEX: [t for t in trades if t.index_is_real],
    }
    for name in ATR_CELLS:
        cells[name] = [t for t in trades if t.atr_tercile == name]
    for y in sorted({t.d.year for t in trades}):
        cells[f"year_{y}"] = [t for t in trades if t.d.year == y]
    return cells


# =============================================================================== the study
def _finite(*vals: Any) -> bool:
    for v in vals:
        if v is None:
            return False
        f = float(v)
        if not math.isfinite(f):
            return False
    return True


def run_study(
    conn: duckdb.DuckDBPyConnection,
    *,
    start: date,
    end: date,
    cost_model: CostModel,
    params: dict[str, Any] | None = None,
    rules: dict[str, RuleSpec] | None = None,
    calendar: NSECalendar | None = None,
) -> tuple[dict[str, Any], dict[str, list[Trade]]]:
    """Run every rule end to end. Returns ``(json document, trades by rule)``."""
    base = dict(params or PARAMS)
    rule_defs = dict(rules if rules is not None else RULES)
    sq = _validate_hhmm(str(base["squareoff_time"]))
    for spec in rule_defs.values():
        t_lit = _validate_hhmm(spec.decision_time)
        if spec.kind == KIND_FAILED_BREAKOUT and t_lit < _validate_hhmm(
            str(base["break_window_end_exclusive"])
        ):
            # The break window would be clamped at T and the rule would silently become "broke the
            # OR high before T" - a different rule wearing the registered one's name.
            raise ValueError(
                f"rule {spec.name}: decision_time {t_lit} precedes the break-window end "
                f"{base['break_window_end_exclusive']}"
            )
        if t_lit >= sq:
            raise ValueError(f"rule {spec.name}: decision_time {t_lit} is not before squareoff {sq}")

    eligible, universe_source, universe_as_of = load_eligible_universe(conn)
    if not eligible:
        raise ValueError(
            "no eligible universe: universe_daily is empty or absent, and the eligible set IS this "
            f"study's population ({universe_source})"
        )

    ex_dates = load_ex_dates(conn)
    daily = load_daily(conn, sorted(eligible), end)
    atr_period = int(base["atr_period"])
    atr_cache: dict[tuple[str, date], float | None] = {}
    prior_1m = load_prior_close_1m(conn, start - timedelta(days=10), end)
    per_year = coverage_by_year(conn, base["index_symbol"])

    notes: list[str] = []
    trades_by_rule: dict[str, list[Trade]] = {}
    diagnostics: dict[str, Any] = {"by_decision_time": {}, "by_rule": {}}
    real_index_dates: set[date] = set()
    non_trading_dates: set[str] = set()
    years_without_calendar: set[int] = set()
    all_sessions: set[date] = set()
    symbols_measured: set[str] = set()

    times_needed = sorted({spec.decision_time for spec in rule_defs.values()})
    for t_lit in times_needed:
        frame = load_features(conn, start, end, decision_time=t_lit, params=base)
        if frame.empty:
            continue
        frame = frame.copy()
        frame["d"] = [x.date() if hasattr(x, "date") else x for x in frame["d"]]
        frame["symbol"] = [str(s).upper() for s in frame["symbol"]]
        idx_rows = frame[frame["symbol"] == str(base["index_symbol"]).upper()]
        stock = frame[frame["symbol"].isin(eligible)]

        sessions = sorted(set(stock["d"]))
        all_sessions |= set(sessions)
        symbols_measured |= set(stock["symbol"])
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
        med, n_rvol_skipped = rvol_medians(
            cumvol, sessions,
            lookback=int(base["rvol_lookback_sessions"]),
            min_valid=int(base["rvol_min_valid_sessions"]),
        )

        index_ret: dict[date, float] = {}
        for d, o, c in zip(idx_rows["d"], idx_rows["open_0915"], idx_rows["close_t"], strict=True):
            if _finite(o, c) and float(o) > 0:
                index_ret[d] = (float(c) / float(o) - 1.0) * 100.0
        real_index_dates |= set(index_ret)

        by_day: dict[date, DayFeatures] = {}
        n_no_t_bar = 0
        for row in stock.itertuples(index=False):
            d, sym = row.d, str(row.symbol)
            f = by_day.get(d)
            if f is None:
                f = by_day[d] = DayFeatures(d)
            if int(row.n_t or 0) != 1:
                n_no_t_bar += 1
                continue
            if not _finite(row.open_0915, row.or_high, row.close_t, row.high_t, row.sess_high_t):
                continue
            o = float(row.open_0915)
            if o <= 0.0:
                continue
            f.symbols.append(sym)
            f.open_0915[sym] = o
            f.or_high[sym] = float(row.or_high)
            f.close_t[sym] = float(row.close_t)
            f.high_t[sym] = float(row.high_t)
            f.sess_high_t[sym] = float(row.sess_high_t)
            f.ret_pct[sym] = (float(row.close_t) / o - 1.0) * 100.0
            mcb = row.max_close_break
            f.max_close_break[sym] = float(mcb) if _finite(mcb) else float("nan")
            m = med.get(sym, {}).get(d)
            if m is not None and m > 0 and _finite(row.cumvol_t):
                f.rvol[sym] = float(row.cumvol_t) / m

        index_used: dict[date, tuple[float, bool]] = {}
        for d, f in by_day.items():
            if d in index_ret:
                index_used[d] = (index_ret[d], True)
            else:
                rets = [f.ret_pct[s] for s in f.symbols]
                index_used[d] = (statistics.fmean(rets) if rets else 0.0, False)

        diagnostics["by_decision_time"][t_lit] = {
            "n_sessions": len(sessions),
            "n_symbol_days": int(len(stock)),
            "n_symbol_days_without_a_T_bar": n_no_t_bar,
            "rvol_insufficient_history_symbol_days": n_rvol_skipped,
            "n_sessions_real_index": len(set(index_ret) & set(by_day)),
            "n_sessions_proxy_index": len(set(by_day) - set(index_ret)),
        }

        # ------------------------------------------------------- selection, per rule at this T
        picks_by_rule: dict[str, dict[date, list[tuple[str, float | None]]]] = {}
        for name, spec in rule_defs.items():
            if spec.decision_time != t_lit:
                continue
            picks, veto = select_rule(
                spec, by_day, params=base, ex_dates=ex_dates, daily=daily, prior_1m=prior_1m,
            )
            picks_by_rule[name] = picks
            diagnostics["by_rule"][name] = {
                "decision_time": t_lit,
                "registered": spec.registered,
                "label": spec.label,
                "n_signal_days": len(picks),
                "n_signals": int(sum(len(v) for v in picks.values())),
                "max_signals_in_one_session": max((len(v) for v in picks.values()), default=0),
                "vetoes": veto,
            }

        union_pairs = sorted(
            {(s, d) for pbd in picks_by_rule.values() for d, syms in pbd.items() for s, _g in syms}
        )
        bars = load_exit_bars(
            conn,
            pd.DataFrame([{"symbol": s, "d": d} for s, d in union_pairs], columns=["symbol", "d"]),
            decision_time=t_lit, squareoff_time=sq,
        )
        entry_time = _shift(t_lit, 1)

        # ------------------------------------------------------- execute every rule at this T
        for name, picks in picks_by_rule.items():
            spec = rule_defs[name]
            out: list[Trade] = []
            n_no_entry_bar = 0
            n_stop_below_exit_level = 0
            for d, syms in sorted(picks.items()):
                f = by_day[d]
                iret, ireal = index_used[d]
                for sym, gap in syms:
                    got = bars.get((sym, d))
                    if got is None:
                        n_no_entry_bar += 1
                        continue
                    stop = f.sess_high_t[sym] if spec.stop == STOP_SESSION_HIGH else None
                    if spec.exit_level == EXIT_OR_HIGH:
                        lvl: float | None = f.or_high[sym]
                    elif spec.exit_level == EXIT_T_BAR_HIGH:
                        lvl = f.high_t[sym]
                    else:
                        lvl = None
                    if stop is not None and lvl is not None and stop < lvl:
                        n_stop_below_exit_level += 1
                    sim = simulate_short(
                        *got, entry_time=entry_time, squareoff_time=sq,
                        exit_level=lvl, stop_level=stop, stop_trigger=spec.stop_trigger,
                    )
                    if sim is None:
                        n_no_entry_bar += 1
                        continue
                    entry_px, exit_px, reason = sim
                    gross = short_gross_pct(entry_px, exit_px)
                    cost = trade_cost_pct(cost_model, base, entry_px)
                    out.append(Trade(
                        rule=name, symbol=sym, d=d, entry_px=entry_px, exit_px=exit_px,
                        exit_reason=reason, gross_pct=gross, cost_pct=cost, net_pct=gross - cost,
                        or_high=f.or_high[sym], stop_level=stop if stop is not None else float("nan"),
                        exit_level=lvl, rvol=f.rvol.get(sym, float("nan")), gap_pct=gap,
                        index_ret_pct=iret, index_is_real=ireal,
                        atr_pct=atr_pct_at(daily, sym, d, period=atr_period, cache=atr_cache),
                    ))
            # How far above the entry the two levels sit, as a percent of the entry price. This is
            # the geometry of the rule as WRITTEN, and it is reported because it decides the exit
            # mix: a failure level a few ticks above the fill is a hair-trigger stop, not a thesis.
            lvl_gaps = [
                (t.exit_level / t.entry_px - 1.0) * 100.0 for t in out if t.exit_level is not None
            ]
            stop_gaps = [
                (t.stop_level / t.entry_px - 1.0) * 100.0 for t in out if math.isfinite(t.stop_level)
            ]
            trades_by_rule[name] = out
            dg = diagnostics["by_rule"][name]
            dg.update({
                "n_trades": len(out),
                "n_dropped_unusable_entry_or_exit_bar": n_no_entry_bar,
                "median_entry_to_exit_level_pct": (
                    round(statistics.median(lvl_gaps), 4) if lvl_gaps else None
                ),
                "median_entry_to_stop_pct": (
                    round(statistics.median(stop_gaps), 4) if stop_gaps else None
                ),
                "n_entered_at_or_above_the_exit_level": sum(
                    1 for t in out if t.exit_level is not None and t.entry_px >= t.exit_level
                ),
                "n_signals_with_stop_below_exit_level": n_stop_below_exit_level,
                "n_exits_stop_first": sum(
                    1 for t in out if t.exit_reason in (EXIT_STOP_NEXT_OPEN, EXIT_STOP_INTRABAR)
                ),
                "exit_reason_counts": {
                    r: sum(1 for t in out if t.exit_reason == r)
                    for r in sorted({t.exit_reason for t in out})
                },
            })

    # ---------------------------------------------------------------- ATR cells, cut ONCE per family
    # R3's posture: the cuts are taken over the DISTINCT measured (symbol, signal day) pairs of the
    # rule family, so a symbol-day sits in the same cell under every variant of its rule and the
    # cells compare populations rather than cuts. F1 and F2 measure different populations at
    # different decision bars, so their cuts are never pooled with one another.
    cuts_by_family: dict[str, tuple[float, float] | None] = {}
    for fam in sorted({spec.family for spec in rule_defs.values()}):
        pairs: dict[tuple[str, date], float | None] = {}
        for name, spec in rule_defs.items():
            if spec.family != fam:
                continue
            for t in trades_by_rule.get(name, []):
                pairs[(t.symbol, t.d)] = t.atr_pct
        cuts_by_family[fam] = tercile_cuts([v for v in pairs.values() if v is not None])
    for name, spec in rule_defs.items():
        cuts = cuts_by_family.get(spec.family)
        trades = trades_by_rule.get(name, [])
        for t in trades:
            t.atr_tercile = tercile_of(t.atr_pct, cuts)
        if name in diagnostics["by_rule"]:
            diagnostics["by_rule"][name].update({
                "atr_family": spec.family,
                "atr_tercile_cuts_pct": None if cuts is None else [round(c, 4) for c in cuts],
                "n_trades_without_atr": sum(1 for t in trades if t.atr_pct is None),
            })

    # ---------------------------------------------------------------- notes / caveats
    notes.append(
        "NO PARAMETER SWEEP was run and no rule was selected: PARAMS is one dict, RULES is a fixed "
        f"list of {len(rule_defs)} REPORTED cells of which {TRIAL_COUNT_N} are PRE-REGISTERED "
        f"({', '.join(PRE_REGISTERED_RULES)}); the rest are reporting-only and can never be "
        "selected in their place. 'promotable' is the house tdc boolean (mean net > 0, t > 2, "
        "n >= 200, CPCV positive share >= 0.60) and carries no other meaning. A fail is a "
        "REFUTATION and changes nothing."
    )
    notes.append(
        "SHORTS ARE GATED FOR AUTO BY §1.4.9 WHATEVER THIS SAYS. This study wires nothing; a "
        "promotable cell would be evidence for a separately pre-registered origination proposal and "
        "an owner decision (§8.6), never a deployment."
    )
    notes.append(
        "SHORTABILITY IS ASSUMED, NOT PROVEN: a T2T/GSM/ASM bucket, a broker MIS block or an F&O "
        "ban makes a name unshortable on the day, and the eligible snapshot screens those flags as "
        "of its LATEST day only. Every cell here is an upper bound on the tradeable short side."
    )
    notes.append(
        "PINNED INTERPRETATION (F1): rel_volume_tod is measured at the DECISION bar T, the tdc "
        "definition verbatim. Measuring it at the breakout minute was NOT computed, so no selection "
        "between the two readings took place."
    )
    notes.append(
        "THE STOP IS DOMINATED UNDER THE CLOSE TRIGGER: the session high through T is by "
        "construction >= the OR high (F1) and >= the T bar's high (F2), so a close above the stop "
        "is also a close above the failure level and the failure exit fires first at the same "
        "price. 'n_exits_stop_first' is 0 for every close-trigger rule and the diagnostics prove "
        "it rather than assume it. The *_stop_on_1m_high cells are where the stop actually binds."
    )
    notes.append(
        "NO PER-DAY CAP AND NO RANKING: every passer is measured. The plan paragraph names no "
        "selection criterion, and inventing one would be a free parameter. A wide-participation "
        "session can therefore contribute hundreds of correlated trades, which is exactly why the "
        "CPCV observation unit is the SESSION mean and not the trade."
    )
    notes.append(
        "T-STAT OVERSTATES INDEPENDENCE: the trades of one session share a regime, so the "
        "per-trade t-stat treats correlated draws as independent. The CPCV positive-split share, "
        "computed on per-SESSION means, is the check that does not."
    )
    notes.append(
        f"POPULATION: {universe_source} - a SURVIVORSHIP-TAINTED PROXY (index membership as of each "
        "historical session is stored nowhere here), intersected with the symbols bars_1m holds, "
        f"which is the ~200-name tick watchlist plus the index ({len(symbols_measured)} symbols "
        "measured). For a SHORT study the sign of the bias is not obvious - a name in today's "
        "NIFTY 500 is a name that did not collapse - so it is reported, not corrected."
    )
    notes.append(
        "INDEX PROXY: bars_1m carries '" + str(base["index_symbol"]) + "' only from "
        + (str(min(real_index_dates)) if real_index_dates else "n/a")
        + "; for earlier sessions the index-regime split uses the EQUAL-WEIGHT MEAN ret_from_open "
        f"of that session's eligible universe at T. The '{SPLIT_REAL_INDEX}' cell is the subset "
        "measured against the real index."
    )
    notes.append(
        "ATR TERCILE = R3's statistic verbatim: Wilder ATR(14) over the 14 completed DAILY sessions "
        "STRICTLY BEFORE the signal day (engine.strategy.indicators.wilder_atr), over the close of "
        "the last of them, in percent; a symbol-day with fewer than 14 prior daily sessions is "
        f"labelled '{ATR_UNCLASSIFIED}' and KEPT, so the four cells sum to the pooled cell exactly. "
        "Cuts are taken ONCE per rule family over its distinct measured symbol-days. They are "
        "FULL-SAMPLE statistics and were unknowable at signal time: DESCRIPTIVE, not a tradeable "
        "filter (the brk20/hi52 caveat, verbatim in force). Cuts per family: "
        + ", ".join(
            f"{fam}={'n/a' if c is None else f'{c[0]:.3f}/{c[1]:.3f}'}"
            for fam, c in sorted(cuts_by_family.items())
        )
        + "."
    )
    notes.append(
        f"EX-DATE VETO (universal): any corp_actions ex-date in the {base['exdate_veto_window_days']} "
        "calendar days ending at d vetoes the symbol-day for BOTH rules. Kite minute candles are "
        "adjusted at fetch time (§14 Q10), so a stitched series can hold two unit systems, and a "
        "split multiplies the 20-session volume denominator either way."
    )
    if years_without_calendar:
        notes.append(
            "CALENDAR: config/calendar has no year file for "
            + ", ".join(str(y) for y in sorted(years_without_calendar))
            + ", so NSECalendar cannot confirm those sessions. Sessions are taken from bars_1m's "
            "own distinct dates, which are trading days by construction."
        )
    if non_trading_dates:
        notes.append(
            "CALENDAR MISMATCH: bars_1m holds bars on "
            f"{len(non_trading_dates)} date(s) NSECalendar calls non-trading: "
            + ", ".join(sorted(non_trading_dates)[:10])
            + " - the WEEKEND Union-Budget special sessions, which carry a full tape and are KEPT "
            "(the gap is in config/calendar, not in the data; same treatment as tdc)."
        )

    rt_pct = float(cost_model.breakeven_pct(Decimal(base["reference_notional_inr"]), base["product"]))
    doc: dict[str, Any] = {
        "meta": {
            "script": "scripts/backtest_fade.py",
            "strategy_id": "fade",
            "research_id": "R4",
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "window": {"start": str(start), "end": str(end)},
            "n_sessions": len(all_sessions),
            "first_session": str(min(all_sessions)) if all_sessions else None,
            "last_session": str(max(all_sessions)) if all_sessions else None,
            "side": "SHORT",
            "params": {k: (str(v) if isinstance(v, Decimal) else v) for k, v in base.items()},
            "rules": {
                k: {
                    "kind": v.kind, "decision_time": v.decision_time, "exit_level": v.exit_level,
                    "stop": v.stop, "stop_trigger": v.stop_trigger, "registered": v.registered,
                    "label": v.label,
                }
                for k, v in rule_defs.items()
            },
            "pre_registered_rules": list(PRE_REGISTERED_RULES),
            "trial_count_n": TRIAL_COUNT_N,
            "parameter_sweep_run": False,
            "rule_selection_performed": False,
            "universe_source": universe_source,
            "universe_as_of": None if universe_as_of is None else str(universe_as_of),
            "n_eligible_symbols": len(eligible),
            "n_symbols_measured": len(symbols_measured),
            "population_is_survivorship_tainted_proxy": True,
            "shorts_gated_for_auto_by_1_4_9": True,
            "entry_convention": "SHORT at the OPEN of the bar immediately after T",
            "exit_convention": (
                "first 1m CLOSE above the failure level (F1: OR high; F2: the T bar's high) -> fill "
                "at the FOLLOWING bar OPEN; stop = the session high through T (dominated under the "
                "close trigger); else the 15:15 bar CLOSE"
            ),
            "return_convention": "short: (1 - exit/entry) * 100, equal notional per trade",
            "product": base["product"],
            "reference_notional_inr": base["reference_notional_inr"],
            "cost_round_trip_pct": round(rt_pct, 6),
            "cost_slippage": (
                f"{base['slippage_ticks_per_side']} tick of Rs {base['tick_size_inr']} each side, "
                "charged as 2*tick/entry_px*100 percent"
            ),
            "promotion_rule": (
                "mean net % > 0 AND t > 2 AND n >= 200 AND CPCV positive-split share >= 0.60"
            ),
        },
        "coverage": {
            "bars_1m_by_year": per_year,
            "real_index_sessions": {
                "n": len(real_index_dates),
                "first": str(min(real_index_dates)) if real_index_dates else None,
                "last": str(max(real_index_dates)) if real_index_dates else None,
            },
        },
        "diagnostics": diagnostics,
        "results": {},
        "notes": notes,
    }

    for name in rule_defs:
        trades = trades_by_rule.get(name, [])
        doc["results"][name] = {
            "registered": rule_defs[name].registered,
            "label": rule_defs[name].label,
            "n_trades": len(trades),
            "mean_gross_pct": (
                round(statistics.fmean([t.gross_pct for t in trades]), 4) if trades else None
            ),
            "splits": {
                cell: metrics(cell_trades, base)
                for cell, cell_trades in split_cells(trades).items()
            },
        }
    doc["verdict"] = verdict(doc)
    return doc, trades_by_rule


def select_rule(
    spec: RuleSpec,
    by_day: dict[date, DayFeatures],
    *,
    params: dict[str, Any],
    ex_dates: dict[str, list[date]],
    daily: dict[str, tuple[list[date], list[float], list[float], list[float]]],
    prior_1m: dict[tuple[str, date], float],
) -> tuple[dict[date, list[tuple[str, float | None]]], dict[str, int]]:
    """``(d -> [(symbol, gap_pct or None)], veto counts)`` - the rule's signals, no cap and no rank.

    Every predicate reads the T-bar snapshot only. F2's prior close is the symbol's last ``bars_1d``
    close strictly before ``d``, guarded three ways (ex-date, age, cross-source agreement) before it
    is allowed to define a gap.
    """
    veto: dict[str, int] = defaultdict(int)
    picks: dict[date, list[tuple[str, float | None]]] = {}
    exd_days = int(params["exdate_veto_window_days"])
    max_age = int(params["prior_close_max_age_days"])
    max_dis = float(params["prior_close_source_max_disagreement_pct"])
    for d, f in sorted(by_day.items()):
        chosen: list[tuple[str, float | None]] = []
        for sym in f.symbols:
            if has_ex_date_within(ex_dates, sym, d, exd_days):
                veto["ex_date_window"] += 1
                continue
            if spec.kind == KIND_FAILED_BREAKOUT:
                mcb, orh = f.max_close_break[sym], f.or_high[sym]
                if not math.isfinite(mcb) or mcb <= orh:
                    veto["no_breakout_before_window_end"] += 1
                    continue
                rv = f.rvol.get(sym)
                if rv is None or not math.isfinite(rv):
                    veto["rvol_unavailable"] += 1
                    continue
                if rv < float(params["f1_rvol_min"]):
                    veto["rvol_below_min"] += 1
                    continue
                if f.close_t[sym] >= orh:
                    veto["still_above_or_high_at_T"] += 1
                    continue
                chosen.append((sym, None))
            else:
                series = daily.get(sym)
                if not series:
                    veto["no_daily_history"] += 1
                    continue
                days, _h, _l, closes = series
                i = prior_daily_index(days, d)
                if i is None:
                    veto["no_prior_close"] += 1
                    continue
                prev_d, prev_c = days[i], closes[i]
                if not _finite(prev_c) or prev_c <= 0:
                    veto["no_prior_close"] += 1
                    continue
                if (d - prev_d).days > max_age:
                    veto["stale_prior_close"] += 1
                    continue
                cross = prior_1m.get((sym, prev_d))
                if cross is not None and prev_c > 0 and abs(cross / prev_c - 1.0) * 100.0 > max_dis:
                    veto["prior_close_source_disagreement"] += 1
                    continue
                if cross is None:
                    veto["prior_close_crosscheck_unavailable"] += 1
                gap = (f.open_0915[sym] / float(prev_c) - 1.0) * 100.0
                if gap < float(params["f2_gap_min_pct"]):
                    veto["gap_below_min"] += 1
                    continue
                if f.close_t[sym] >= f.open_0915[sym]:
                    veto["not_below_session_open_at_T"] += 1
                    continue
                chosen.append((sym, gap))
        if chosen:
            picks[d] = chosen
    return picks, dict(veto)


def verdict(doc: dict[str, Any]) -> dict[str, Any]:
    """The registered outcome, computed from the report itself: which cells clear the plan's boolean.

    A REFUTATION is the default and is stated as one. Reporting-only cells are listed separately and
    are never part of the outcome - they cannot promote what the registered rules did not.
    """
    promotable = [
        {"rule": name, "split": cell, "n": s["n"], "mean_net_pct": s["mean_net_pct"],
         "t_stat": s["t_stat"], "cpcv_positive_share": s["cpcv"]["positive_share"]}
        for name, block in doc["results"].items()
        for cell, s in block["splits"].items()
        if s["promotable"]
    ]
    registered_hits = [h for h in promotable if doc["results"][h["rule"]]["registered"]]
    pooled = {
        name: doc["results"][name]["splits"][SPLIT_ALL]
        for name in PRE_REGISTERED_RULES
        if name in doc["results"]
    }
    return {
        "rule": (
            "promotable iff mean net % > 0 AND t > 2 AND n >= 200 AND CPCV positive-split share "
            ">= 0.60; anything less is a refutation and changes nothing"
        ),
        "registered_promotable_cells": registered_hits,
        "reporting_only_promotable_cells": [
            h for h in promotable if not doc["results"][h["rule"]]["registered"]
        ],
        "outcome": "PROMOTABLE_CELLS_FOUND" if registered_hits else "REFUTED",
        "pooled_registered": {
            name: {
                "n": s["n"], "mean_net_pct": s["mean_net_pct"], "median_net_pct": s["median_net_pct"],
                "mean_gross_pct": s["mean_gross_pct"], "mean_cost_pct": s["mean_cost_pct"],
                "win_rate": s["win_rate"], "t_stat": s["t_stat"],
                "cpcv_positive_share": s["cpcv"]["positive_share"], "promotable": s["promotable"],
            }
            for name, s in pooled.items()
        },
        "geometry_first": {
            name: (
                "gross <= 0: there is no edge for cost to erode"
                if (s["mean_gross_pct"] is not None and s["mean_gross_pct"] <= 0.0)
                else "gross > 0: the cost floor is what decides"
            )
            for name, s in pooled.items()
        },
    }


# =============================================================================== rendering
def render_text(doc: dict[str, Any]) -> str:
    """The printed report, folded to ASCII (the Windows console is cp1252)."""
    m = doc["meta"]
    out: list[str] = []
    add = out.append
    add("=" * 122)
    add("fade PRE-REGISTERED BACKTEST - the SHORT/fade side of intraday (R4)")
    add("(IMPLEMENTATION_PLAN.md `fade` pre-registration, 2026-09-12)")
    add("=" * 122)
    add(f"generated        : {m['generated_at']}")
    add(f"window           : {m['window']['start']} -> {m['window']['end']}  "
        f"({m['n_sessions']} sessions, {m['first_session']} .. {m['last_session']})")
    add(f"population       : {m['universe_source']}  [SURVIVORSHIP-TAINTED PROXY]")
    add(f"                   {m['n_symbols_measured']} of {m['n_eligible_symbols']} eligible symbols "
        "carry 1m bars")
    add(f"registered rules : {', '.join(m['pre_registered_rules'])}   (trial count N="
        f"{m['trial_count_n']}, sweep run: {m['parameter_sweep_run']}, rule selection: "
        f"{m['rule_selection_performed']})")
    add(f"entry            : {m['entry_convention']}")
    add(f"exit             : {m['exit_convention']}")
    add(f"return           : {m['return_convention']}")
    add(f"cost             : {m['cost_round_trip_pct']:.4f}% {m['product']} round trip at Rs "
        f"{m['reference_notional_inr']} + {m['cost_slippage']}")
    add(f"promotion rule   : {m['promotion_rule']}")
    add("")

    add("-" * 122)
    add("STEP 1 - SAMPLE SHAPE (bars_1m coverage per calendar year; 'full session' = >= 330 of 375 bars)")
    add("-" * 122)
    add("  year |  sessions | symbols | symbols full | symbol-days | sym-days full | first        last")
    add("  -----+-----------+---------+--------------+-------------+---------------+---------------------------")
    for y in doc["coverage"]["bars_1m_by_year"]:
        add(f"  {y['year']:<4} | {y['sessions']:>9} | {y['symbols']:>7} | {y['symbols_full_session']:>12} | "
            f"{y['symbol_days']:>11} | {y['symbol_days_full_session']:>13} | {y['first_session']}  {y['last_session']}")
    ri = doc["coverage"]["real_index_sessions"]
    add("")
    add(f"  real '{m['params']['index_symbol']}' 1m series on {ri['n']} session(s) "
        f"({ri['first']} -> {ri['last']}); earlier sessions use the equal-weight universe proxy.")
    add("")
    for t_lit, dg in sorted(doc["diagnostics"]["by_decision_time"].items()):
        add(f"  T={t_lit}: {dg['n_sessions']} sessions, {dg['n_symbol_days']} symbol-days, "
            f"{dg['n_symbol_days_without_a_T_bar']} without a T bar, "
            f"{dg['rvol_insufficient_history_symbol_days']} skipped for thin RVOL history, "
            f"real index on {dg['n_sessions_real_index']} / proxy on {dg['n_sessions_proxy_index']}")
    add("")
    add("-" * 122)
    add("STEP 2 - FUNNEL (every veto counted; a silent veto class is undiagnosable)")
    add("-" * 122)
    for name, dg in doc["diagnostics"]["by_rule"].items():
        tag = "REGISTERED " if dg["registered"] else "reporting  "
        add(f"  [{tag}{name:<19}] T={dg['decision_time']}  signal days={dg['n_signal_days']:>4}  "
            f"signals={dg['n_signals']:>6}  trades={dg['n_trades']:>6}  "
            f"max/session={dg['max_signals_in_one_session']:>4}  "
            f"dropped(unusable bars)={dg['n_dropped_unusable_entry_or_exit_bar']:>4}")
        add(f"      vetoes={dg['vetoes']}")
        add(f"      exits={dg['exit_reason_counts']}  stop-first={dg['n_exits_stop_first']}  "
            f"stop<exit-level={dg['n_signals_with_stop_below_exit_level']}  "
            f"ATR cuts%={dg['atr_tercile_cuts_pct']}  no-ATR={dg['n_trades_without_atr']}")
        add(f"      geometry: median entry->failure level "
            f"{_f(dg['median_entry_to_exit_level_pct'], 8)}%  median entry->stop "
            f"{_f(dg['median_entry_to_stop_pct'], 8)}%  entered at/above the failure level="
            f"{dg['n_entered_at_or_above_the_exit_level']}")
    add("")

    add("-" * 122)
    add("STEP 3 - GEOMETRY FIRST (the ORB lesson: mean GROSS against the cost floor, BEFORE any")
    add("         signal-quality claim)")
    add("-" * 122)
    for name, text in doc["verdict"]["geometry_first"].items():
        s = doc["verdict"]["pooled_registered"][name]
        add(f"  {name:<6} mean gross {_f(s['mean_gross_pct'], 9)}%  vs mean cost "
            f"{_f(s['mean_cost_pct'], 9)}%  ->  {text}")
    add("")

    add("-" * 122)
    add("STEP 4 - RESULTS: EVERY RULE x EVERY SPLIT (all reported; none selected)")
    add("-" * 122)
    add("  rule                | split                     |     n | mean net% |  med net% |   win% |"
        "  t-stat | CPCV+ | promotable")
    add("  --------------------+---------------------------+-------+-----------+-----------+--------+"
        "---------+-------+-----------")
    for name in doc["results"]:
        block = doc["results"][name]
        for cell, s in block["splits"].items():
            win = "-" if s["win_rate"] is None else f"{s['win_rate'] * 100:.1f}"
            share = s["cpcv"]["positive_share"]
            sh = "-" if share is None else f"{share * 100:.0f}%"
            add(f"  {name:<19} | {cell:<25} | {s['n']:>5} | {_f(s['mean_net_pct'], 9)} | "
                f"{_f(s['median_net_pct'], 9)} | {win:>6} | {_f(s['t_stat'], 7, 2)} | {sh:>5} | "
                f"{str(s['promotable'])}")
        add("  " + "-" * 118)
    add("")

    add("-" * 122)
    add("STEP 5 - VERDICT (the plan's boolean, verbatim; no other claim is made)")
    add("-" * 122)
    v = doc["verdict"]
    add(f"  outcome: {v['outcome']}")
    if v["registered_promotable_cells"]:
        for h in v["registered_promotable_cells"]:
            add(f"  promotable=True: REGISTERED rule {h['rule']}, split {h['split']} "
                f"(n={h['n']}, mean net {h['mean_net_pct']}%, t={h['t_stat']}, "
                f"CPCV+ {h['cpcv_positive_share']})")
    else:
        add("  promotable=True in NO REGISTERED rule x split cell -> the fade side is REFUTED on "
            "this population at retail cost. Nothing changes.")
    for h in v["reporting_only_promotable_cells"]:
        add(f"  (reporting-only cell {h['rule']} / {h['split']} clears the boolean; it is NOT a "
            "registered rule and cannot be promoted)")
    add("")

    add("-" * 122)
    add("NOTES / CAVEATS (reported, not massaged)")
    add("-" * 122)
    for n in doc["notes"]:
        add(f"  * {n}")
    add("")
    return _ascii("\n".join(out))


def _md_num(v: Any, prec: int = 4) -> str:
    return "-" if v is None else (f"{v:+.{prec}f}" if isinstance(v, float) else str(v))


def render_markdown(doc: dict[str, Any]) -> str:
    """The ``.md`` artifact: the verdict and the headline table up top, the full text report below."""
    m = doc["meta"]
    v = doc["verdict"]
    out: list[str] = []
    add = out.append
    add("# fade backtest - the short/fade side of intraday (R4, pre-registered 2026-09-12)")
    add("")
    add(f"*Generated {m['generated_at']} - window {m['window']['start']} -> {m['window']['end']}, "
        f"{m['n_sessions']} sessions, {m['n_symbols_measured']} of {m['n_eligible_symbols']} "
        f"eligible symbols with 1m bars. Trial count N={m['trial_count_n']} "
        f"({', '.join(m['pre_registered_rules'])}). Round trip {m['cost_round_trip_pct']:.4f}% "
        f"{m['product']} at Rs {m['reference_notional_inr']} + 1 tick/side. SHORT side; "
        "§1.4.9 gates shorts for AUTO whatever this says, and this study wires nothing.*")
    add("")
    add(f"## Outcome: **{v['outcome']}**")
    add("")
    if v["registered_promotable_cells"]:
        for h in v["registered_promotable_cells"]:
            add(f"- REGISTERED `{h['rule']}` / `{h['split']}`: n={h['n']}, mean net "
                f"{_md_num(h['mean_net_pct'])}%, t={_md_num(h['t_stat'], 2)}, "
                f"CPCV+ {h['cpcv_positive_share']}")
    else:
        add("No registered rule x split cell clears `mean net > 0 AND t > 2 AND n >= 200 AND "
            "CPCV >= 60%`. The fade side is **refuted** on this population at retail cost, and "
            "nothing changes.")
    add("")
    add("## Geometry first (mean GROSS vs the cost floor, before any signal-quality claim)")
    add("")
    add("| rule | n | mean gross % | mean cost % | mean net % | median net % | win % | t | CPCV+ | "
        "promotable |")
    add("|---|---|---|---|---|---|---|---|---|---|")
    for name, s in v["pooled_registered"].items():
        win = "-" if s["win_rate"] is None else f"{s['win_rate'] * 100:.1f}"
        share = "-" if s["cpcv_positive_share"] is None else f"{s['cpcv_positive_share'] * 100:.0f}%"
        add(f"| `{name}` | {s['n']} | {_md_num(s['mean_gross_pct'])} | {_md_num(s['mean_cost_pct'])} "
            f"| {_md_num(s['mean_net_pct'])} | {_md_num(s['median_net_pct'])} | {win} | "
            f"{_md_num(s['t_stat'], 2)} | {share} | **{s['promotable']}** |")
    add("")
    add("## Every rule x split cell")
    add("")
    add("| rule | registered | split | n | mean net % | median net % | win % | t | CPCV+ | promotable |")
    add("|---|---|---|---|---|---|---|---|---|---|")
    for name, block in doc["results"].items():
        for cell, s in block["splits"].items():
            win = "-" if s["win_rate"] is None else f"{s['win_rate'] * 100:.1f}"
            share = s["cpcv"]["positive_share"]
            sh = "-" if share is None else f"{share * 100:.0f}%"
            add(f"| `{name}` | {block['registered']} | `{cell}` | {s['n']} | "
                f"{_md_num(s['mean_net_pct'])} | {_md_num(s['median_net_pct'])} | {win} | "
                f"{_md_num(s['t_stat'], 2)} | {sh} | {s['promotable']} |")
    add("")
    add("## Notes / caveats")
    add("")
    for n in doc["notes"]:
        add(f"- {n}")
    add("")
    add("## Full report")
    add("")
    add("```")
    add(render_text(doc))
    add("```")
    add("")
    return "\n".join(out)


# =============================================================================== CLI
def _default_db() -> Path:
    return repo_root() / "data" / "market.duckdb"


def _default_out() -> Path:
    return repo_root() / "data" / "reports" / "backtest_fade_2026-09-12.json"


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog="backtest_fade",
        description=(
            "fade pre-registered backtest, R4 - the short side of intraday (one PARAMS dict, one "
            "fixed RULES dict, no sweep and no selection). Read-only against bars_1m/bars_1d; "
            "refuses to run if the DuckDB file is missing or locked by the engine."
        ),
    )
    ap.add_argument("--db", type=Path, default=_default_db(), help="path to market.duckdb")
    ap.add_argument("--start", type=tdc._date, default=date(2000, 1, 1),
                    help="window start (YYYY-MM-DD)")
    ap.add_argument("--end", type=tdc._date, default=date.today() - timedelta(days=1),
                    help="window end, inclusive (default: yesterday - today's tape is partial)")
    ap.add_argument("--out", type=Path, default=None,
                    help="JSON results path (the .md report is written beside it, same stem)")
    return ap


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        conn = open_readonly(Path(args.db))
    except DbUnopenable as exc:
        print(f"backtest_fade: REFUSING TO RUN.\n  {_ascii(str(exc))}", file=sys.stderr)
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
        print(f"backtest_fade: REFUSING TO RUN.\n  {_ascii(str(exc))}", file=sys.stderr)
        return 2
    finally:
        conn.close()

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(doc, indent=2, default=str), encoding="utf-8")
    md_path = out_path.with_suffix(".md")
    md_path.write_text(render_markdown(doc), encoding="utf-8")
    print(render_text(doc))
    print(f"JSON results -> {out_path}")
    print(f"MD report    -> {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
