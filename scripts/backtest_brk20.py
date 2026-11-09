#!/usr/bin/env python
"""``brk20`` ENTRY-MECHANISM backtest (pre-registered 2026-09-12).

Event study comparing the shipped limit-at-level entry (``entry = H20``) with entering at market on
confirmation. NOT a portfolio simulator; it wires nothing.

===============================================================================================
PRE-REGISTRATION / MULTIPLICITY DISCIPLINE — READ FIRST
===============================================================================================
No parameter sweep is run here or was run before it. The signal is the live rule at its frozen
defaults: :data:`PRE_REGISTERED_PARAMS` equals ``brk20.DEFAULT_PARAMS`` and
:data:`PRE_REGISTERED_FLOOR_PARAMS` equals ``brk20.FLOOR_PARAMS`` (the owner-only stop-geometry floor
knobs); the unit test asserts it. Exactly THREE ENTRY VARIANTS are registered (:data:`VARIANTS`):

* ``next_open``       — the next session's OPEN after the fresh cross (market-on-confirmation).
* ``limit_3_sessions`` — the SHIPPED mechanism: a resting limit at the candidate's own
  ``raw_levels.entry``, filled within 3 sessions.
* ``limit_5_sessions`` — the same at 5 sessions.

Trial count for the deflation machinery is **N = 3** (:data:`TRIAL_COUNT_N`), ``fold_pass_min(3) =
60%``. Adding a knob that changes the signal or the fill rule (``--vol-mult``, ``--fill-window``,
"best of") would silently turn N=3 into N=k and invalidate every deflated number; a re-parameterised
rule is a NEW pre-registration.

DECISION RULE (:data:`DECISION_RULE`, :func:`decision_outcome`): prefer the variant with the higher
median NET return at T+10, PROVIDED it is CPCV-promotable at T+10; if none is, the outcome is
``neither`` and the shipped mechanism stands. Nothing max-picks across horizons. A winner is evidence
about the MECHANISM only — ``brk20`` carries no expectancy presumption and wiring is a separate owner
decision.

===============================================================================================
CONVENTIONS
===============================================================================================
* **Signal.** ``brk20.scan_daily`` is IMPORTED and called on every eligible session; nothing of the
  rule is re-implemented. Rows are ``brk20.DailyRow`` as the live sweep builds them. This module only
  chooses the trailing window handed to ``scan_daily`` (:func:`_scan_window_rows`, an exact
  equivalence) and applies the live sweep's 35-day unadjusted-history veto around it.
* **Population.** The CURRENT (latest ``universe_daily`` day) eligible universe — ``included`` rows
  plus ``exclusion_reasons == ['watchlist_cap']`` rows, the predicate of
  ``MarketStore.get_universe_eligible_symbols`` — applied BACKWARDS over the whole window.
  SURVIVORSHIP-TAINTED PROXY: membership as of each signal date is stored nowhere, so a name that
  joined the index after a big run reads as eligible throughout it and a dropped name is mislabelled
  the other way. Every rendering carries the label; the JSON carries
  ``population_is_survivorship_tainted_proxy: true``.
* **Entry** (:func:`fill_for`). Next-open fills at ``open(y+1)``; no same-bar fill anywhere. A limit
  rests at the candidate's OWN ``raw_levels.entry`` (``round_to_tick(H20)``) and fills on the FIRST
  session in ``y+1 .. y+N`` whose ``low <= level``, at ``min(open, level)`` (a gap through the level
  fills at the open). Unfilled at N ⇒ NO TRADE; the FILL RATE is reported as a first-class number.
  TWO DIVERGENCES FROM ``engine.paper.broker``'s tick fill model, both biasing the limit numbers
  UPWARD: (i) that model requires a strict trade-through, this one counts a daily ``low == level``
  touch, so limit fill rates are an UPPER bound; (ii) that model never fills better than the limit,
  while ``min(open, level)`` books the opening print when a session opens below the level.
* **Exit.** ``close(fill + k) / fill_px - 1`` for k = 5 / 10 / 20 TRADING SESSIONS, anchored on the
  FILL, unlike the signal-anchored ``hi52``/``scripts/event_study.py`` convention
  (``close(T+k) / open(T+1)``): the variants fill on different sessions, so a signal anchor would
  charge the limit variants for waiting days.
* **Admission — ONE SIGNAL population; THREE DIFFERENT TRADE SETS.** A signal is admitted only when
  ``MAX_FILL_WINDOW + max(HORIZONS)`` forward bars exist and are all usable (finite, positive OHLC),
  so the latest possible limit fill still has a complete T+20 and the fill rate has an honest
  denominator. A short signal is dropped whole. A limit fills only when price RETURNS to the level,
  so the limit variants' trades are a pullback-SELECTED subset; any pooled next-open vs limit number
  is a per-FILLED-TRADE comparison across two event sets. Hence
  :func:`matched_cohort_decomposition`, reported next to every pooled cell.
* **Costs.** ONE full round trip per trade: ``CostModel.breakeven_pct(notional, "CNC")`` (statutory
  fees PLUS measured spread) at the ``--notional`` reference size (default Rs 20,000). CNC, never
  MIS: ``brk20`` is a swing rule. ``breakeven_pct`` rather than ``fee_breakeven_pct`` is deliberate —
  fees-only is the contract-note anchor, not a viability number.
* **Sizing.** Per-trade EQUAL NOTIONAL, returns in PERCENT. No compounding, portfolio construction,
  position interaction, capital constraint or cap on concurrent signals — not a claim that the
  population is simultaneously tradeable at the owner's capital.
* **Mandatory split — BREAKOUT-MARGIN TERCILE** (``close(y)/H20 - 1``): wide margins put the rested
  level far below LTP, so this is where the mechanisms must differ if they differ at all. The cuts
  are full-sample statistics, so the split is DESCRIPTIVE, not a tradeable filter. Terciles are
  stamped on the SIGNAL, so a signal sits in the same cell under every variant.
* **Validation.** CPCV via ``engine.learning.validate.cpcv_splits`` (6 folds / 2 test folds) over a
  per-FILL-DAY series: day *t* = mean NET %-return of trades FILLED on *t*, divided by the horizon, a
  per-session net return comparable with the margin floor. Keyed on the fill day (``hi52`` keys on the
  signal day because its fill is always signal+1), so purge = embargo = the horizon covers trade
  overlap exactly. Promotion is ``promotion_decision`` at ``n = TRIAL_COUNT_N`` with
  ``margin_floor_days`` = the horizon. If skfolio is unavailable or the split is degenerate it falls
  back to a purged K-fold with embargo and says so (``cv_method``).
* **ORB-lesson arithmetic check runs FIRST.** Before any signal-quality statistic, the report states
  the median per-trade GROSS drift at each horizon against the round-trip cost floor and emits
  ``GEOMETRY: viable/dead at T+N``. ORB was killed by cost geometry, not signal quality.
* **Matched-cohort decomposition** (STEP 3B), DESCRIPTIVE, not a fourth trial: **PRICE effect** =
  limit minus next-open on the signals the limit FILLED (for fills at delay ≥ 2 the holding window
  also shifts by the fill delay, so it is an entry price-and-timing effect); **SELECTION effect** =
  next-open on the filled cohort minus next-open on the UNFILLED cohort. The registered decision rule
  is still evaluated on the pooled cells.
* **Reporting rule** (tightening of how a result is REPORTED, not a new selection). A variant ×
  horizon is reported PROMOTABLE only if CPCV/deflation passes AND the geometry is viable (median NET
  > 0). The CPCV series is a per-fill-day MEAN while geometry and decision statistics are MEDIANS, so
  a tail-driven variant can pass CPCV while its typical trade cannot pay the toll. The rule can only
  WITHHOLD a promotable label; the registered outcome is re-stated beside the tightened one.

===============================================================================================
DATA ACCESS
===============================================================================================
Read-only, always. ``MarketStore.open()`` runs schema DDL and is a WRITER, so this script, like
``scripts/backtest_hi52.py`` and ``scripts/entity_resolution_sample.py``, attaches DuckDB directly
with ``read_only=True`` and SELECTs from ``bars_1d`` / ``corp_actions`` / ``universe_daily``. DuckDB
still takes a file lock, so the engine service must be idle. A missing/locked file REFUSES with exit
code 2. An empty ``universe_daily`` is also a refusal: the eligible universe IS the population.

Exit codes: 0 = ran; 2 = DB unopenable / no bars in window / no eligible universe.
"""

from __future__ import annotations

import argparse
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
_SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPTS_DIR not in sys.path:  # pragma: no cover - loose-script shim
    sys.path.append(_SCRIPTS_DIR)

import engine  # noqa: E402,F401,I001  native import-order guard: sklearn's OpenMP runtime MUST be
#                                      established before skfolio/cvxpy loads (engine._preload). The
#                                      isort suppression keeps it FIRST; re-sorting would sink it
#                                      below the third-party block and re-open the segfault.
import duckdb  # noqa: E402
import numpy as np  # noqa: E402
from backtest_common import DbUnopenable, _purged_kfold_splits, open_readonly  # noqa: E402
from engine.core.config import repo_root  # noqa: E402
from engine.learning.validate import (  # noqa: E402
    cpcv_splits,
    fold_pass_min,
    margin_floor_pct_per_day,
    promotion_decision,
)
from engine.strategy.cost_model import CostModel  # noqa: E402
from engine.strategy.scanners.brk20 import (  # noqa: E402
    DEFAULT_PARAMS,
    FLOOR_PARAMS,
    STRATEGY_ID,
    VETO_FLOOR_UNAVAILABLE,
    VETO_GAP_FLOOR,
    VETO_UNADJUSTED_HISTORY,
    DailyRow,
    _bump,       # the live veto accumulator, so a count here means what the live sweep's count means
    scan_daily,  # the ONE brk20 rule — imported, never copied (plan: one crossing function)
)
from engine.strategy.scanners.hi52 import UNADJUSTED_KINDS  # noqa: E402
from engine.strategy.types import round_to_tick  # noqa: E402

# =============================================================================== pinned constants
#: The ONE pre-registered parameter set: frozen copies of the live rule's own mappings.
PRE_REGISTERED_PARAMS: dict[str, float] = dict(DEFAULT_PARAMS)
PRE_REGISTERED_FLOOR_PARAMS: dict[str, float] = dict(FLOOR_PARAMS)

#: Trial count for the deflation machinery: three entry variants, no sweep. Not a knob.
TRIAL_COUNT_N = 3

HORIZONS: tuple[int, ...] = (5, 10, 20)           # trading sessions, counted FROM THE FILL
REFERENCE_NOTIONAL = Decimal("20000")             # repo cost-calibration size
PRODUCT = "CNC"                                   # delivery/swing — NEVER MIS (overnight holds)

#: The one horizon the decision rule reads; "best of T+5/T+10/T+20" would be three more trials.
DECISION_HORIZON = 10

VARIANT_NEXT_OPEN = "next_open"
VARIANT_LIMIT_3 = "limit_3_sessions"
VARIANT_LIMIT_5 = "limit_5_sessions"
VARIANTS: tuple[str, ...] = (VARIANT_NEXT_OPEN, VARIANT_LIMIT_3, VARIANT_LIMIT_5)

#: Sessions a limit rests for, per variant. ``None`` = next-open, which is not a resting limit at all.
FILL_WINDOW: dict[str, int | None] = {VARIANT_NEXT_OPEN: None, VARIANT_LIMIT_3: 3, VARIANT_LIMIT_5: 5}

#: The widest fill window; admission reserves this many sessions ahead of the longest horizon.
MAX_FILL_WINDOW = max(w for w in FILL_WINDOW.values() if w is not None)

#: Trailing CALENDAR-day window of the unadjusted-history veto, the live sweep's own brk20 window
#: (``ex_from=today - 35d, ex_to=yesterday``), not hi52's 400.
UNADJUSTED_LOOKBACK_DAYS = 35

#: ``universe_daily`` exclusion reason that still leaves a symbol ELIGIBLE (lost only the top-N
#: focus cut). Mirrors ``MarketStore.get_universe_eligible_symbols``.
EXCL_WATCHLIST_CAP = "watchlist_cap"

CELL_ALL = "all"
CELL_MARGIN_LOW = "margin_tercile_1_narrowest"
CELL_MARGIN_MID = "margin_tercile_2_middle"
CELL_MARGIN_HIGH = "margin_tercile_3_widest"
MARGIN_CELLS: tuple[str, ...] = (CELL_MARGIN_LOW, CELL_MARGIN_MID, CELL_MARGIN_HIGH)

_CV_SKFOLIO = "cpcv_skfolio_CombinatorialPurgedCV"
_CV_FALLBACK = "purged_kfold_with_embargo_fallback"

#: The decision rule as registered; carried into every report and JSON beside the outcome.
DECISION_RULE = (
    "Prefer the variant with the higher median NET return at T+10, PROVIDED that variant is "
    "CPCV-promotable at T+10; if no variant is CPCV-promotable at T+10 the outcome is 'neither' and "
    "the shipped LIMIT-AT-LEVEL mechanism stands unchanged. Fixed 2026-09-12, before the run."
)
DECISION_NEITHER = "neither"
DECISION_TIE = "neither (exact tie on the decision metric)"

#: The REPORTING amendment, carried into the JSON and both renderings. It can only withhold a
#: "promotable" label, so it changes no registered selection and adds no trial.
REPORTING_RULE_AMENDMENT = (
    "REPORTING RULE (2026-09-12 amendment to the registration; a TIGHTENING of how a result is "
    "reported, NOT a new selection and NOT a new trial): a variant x horizon is reported PROMOTABLE "
    "only if the CPCV/deflation rule passes AND the geometry is viable at that horizon (median NET > "
    "0, i.e. the TYPICAL trade clears one full round trip). Rationale: the CPCV fold series is a "
    "per-fill-day MEAN (house convention) while every geometry and decision statistic is a MEDIAN, so "
    "a tail-driven cell can pass CPCV while its median trade pays the round trip for nothing. The "
    "amendment can only WITHHOLD a promotable label, never grant one, so no result the original rule "
    "refused can be promoted by it; both outcomes are reported side by side."
)

#: Matched-cohort cell stems: ``filled`` = signals the limit variant DID fill, ``unfilled`` = the rest.
COHORT_FILLED = "filled"
COHORT_UNFILLED = "unfilled"

#: The two effects the decomposition separates, named once so report text and JSON agree.
EFFECT_PRICE = "price"
EFFECT_SELECTION = "selection"


# =============================================================================== data structures
@dataclass
class Series:
    """One symbol's ascending completed-session history, in both the float-array and DailyRow views."""

    symbol: str
    dates: list[date]
    open: np.ndarray
    high: np.ndarray
    low: np.ndarray
    close: np.ndarray
    volume: np.ndarray
    rows: list[DailyRow]

    def __len__(self) -> int:
        return len(self.dates)


@dataclass
class Signal:
    """One admitted ``brk20`` fresh cross, shared by every entry variant.

    ``level`` is the candidate's OWN ``raw_levels.entry`` (``round_to_tick(H20)``) as a float.
    ``h20`` is the raw broken level, the MARGIN denominator only; :func:`signals_for` tripwires on
    the two disagreeing after tick rounding.
    """

    symbol: str
    idx: int                      # index of y, the last completed session (the trigger)
    signal_date: date
    level: float
    h20: float
    margin: float                 # close(y)/H20 - 1, the rule's own strength signal
    score: float
    tercile: str | None = None    # stamped once the population's cuts are known


@dataclass
class Trade:
    """One measured signal under ONE variant. ``gross``/``net`` are PERCENT, equal notional."""

    symbol: str
    variant: str
    signal_date: date
    fill_date: date
    fill_idx: int
    fill_px: float
    fill_delay_sessions: int      # fill_idx - signal idx; 1 for next-open by construction
    margin: float
    tercile: str | None
    gross: dict[int, float] = field(default_factory=dict)
    net: dict[int, float] = field(default_factory=dict)


# =============================================================================== read-only DB access
def load_eligible_universe(conn: duckdb.DuckDBPyConnection) -> tuple[set[str], str, date | None]:
    """``(symbols, provenance, as_of_day)`` — the CURRENT eligible universe, applied backwards.

    Predicate of ``MarketStore.get_universe_eligible_symbols`` over the latest ``universe_daily`` day:
    ``included`` OR ``exclusion_reasons == ['watchlist_cap']`` exactly (cap plus any other reason is
    NOT eligible). Empty set when the table is absent or empty; the caller refuses on that.
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


def load_series(
    conn: duckdb.DuckDBPyConnection,
    start: date,
    end: date,
    *,
    symbols: Sequence[str] | None = None,
    max_symbols: int | None = None,
) -> dict[str, Series]:
    """Every requested symbol's ascending ``bars_1d`` history in ``[start, end]``.

    ``symbols`` is the eligible universe (never optional in a real run). The (symbol, d) primary key
    gives one row per symbol-session, so no src filter or de-duplication. The symbol filter is pushed
    into SQL because the full frame is millions of rows.
    """
    wanted = sorted({s.strip().upper() for s in symbols}) if symbols is not None else None
    sql = (
        'SELECT symbol, d, "open", high, low, "close", volume FROM bars_1d '
        "WHERE d >= ? AND d <= ?"
    )
    args: list[Any] = [start, end]
    if wanted is not None:
        if not wanted:
            return {}
        sql += " AND upper(symbol) IN (" + ", ".join("?" * len(wanted)) + ")"
        args.extend(wanted)
    frame = conn.execute(sql + " ORDER BY symbol, d", args).df()
    if frame.empty:
        return {}
    out: dict[str, Series] = {}
    for symbol, grp in frame.groupby("symbol", sort=True):
        sym = str(symbol)
        dates = [d.date() if hasattr(d, "date") else d for d in grp["d"].tolist()]
        o = grp["open"].to_numpy(dtype="float64")
        h = grp["high"].to_numpy(dtype="float64")
        low = grp["low"].to_numpy(dtype="float64")
        c = grp["close"].to_numpy(dtype="float64")
        v = grp["volume"].to_numpy(dtype="float64")
        rows = [
            DailyRow(high=float(h[i]), close=float(c[i]), volume=float(v[i]), open=float(o[i]))
            for i in range(len(dates))
        ]
        out[sym] = Series(sym, dates, o, h, low, c, v, rows)
        if max_symbols is not None and len(out) >= max_symbols:
            break
    return out


def load_ex_dates(conn: duckdb.DuckDBPyConnection) -> dict[str, list[date]]:
    """``symbol -> sorted ex-dates`` from ``corp_actions`` (empty dict when the table is absent)."""
    try:
        rows = conn.execute("SELECT symbol, ex_date FROM corp_actions").fetchall()
    except Exception:  # noqa: BLE001 - a missing/renamed table must not sink an offline study
        return {}
    out: dict[str, list[date]] = defaultdict(list)
    for sym, xd in rows:
        if xd is None:
            continue
        out[str(sym)].append(xd.date() if hasattr(xd, "date") else xd)
    return {k: sorted(v) for k, v in out.items()}


def load_structural_ex_dates(conn: duckdb.DuckDBPyConnection) -> dict[str, list[date]]:
    """``symbol -> sorted ex-dates`` for the RESCALING kinds alone (``hi52.UNADJUSTED_KINDS``).

    The kind test is the live one (``hi52.unadjusted_history``): exact membership, never case-folded.
    """
    try:
        rows = conn.execute("SELECT symbol, ex_date, kind FROM corp_actions").fetchall()
    except Exception:  # noqa: BLE001 - a missing/renamed table must not sink an offline study
        return {}
    out: dict[str, list[date]] = defaultdict(list)
    for sym, xd, kind in rows:
        if xd is None or kind not in UNADJUSTED_KINDS:
            continue
        out[str(sym)].append(xd.date() if hasattr(xd, "date") else xd)
    return {k: sorted(v) for k, v in out.items()}


def corp_actions_coverage(conn: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    """``{rows, ex_date_min, ex_date_max, structural_rows, structural_ex_date_min, ..._max}``.

    The veto reaches only as far as the STRUCTURAL rows (the table is dividend-dominated); an empty
    or short structural span silently disables it, so the span is reported.
    """
    empty = {"rows": 0, "ex_date_min": None, "ex_date_max": None,
             "structural_rows": 0, "structural_ex_date_min": None, "structural_ex_date_max": None}
    kinds = ", ".join(f"'{k}'" for k in sorted(UNADJUSTED_KINDS))
    try:
        rows, lo, hi = conn.execute(
            "SELECT count(*), min(ex_date), max(ex_date) FROM corp_actions"
        ).fetchone()
        s_rows, s_lo, s_hi = conn.execute(
            f"SELECT count(*), min(ex_date), max(ex_date) FROM corp_actions WHERE kind IN ({kinds})"
        ).fetchone()
    except Exception:  # noqa: BLE001 - same posture as load_ex_dates
        return empty
    return {
        "rows": int(rows or 0),
        "ex_date_min": None if lo is None else str(lo),
        "ex_date_max": None if hi is None else str(hi),
        "structural_rows": int(s_rows or 0),
        "structural_ex_date_min": None if s_lo is None else str(s_lo),
        "structural_ex_date_max": None if s_hi is None else str(s_hi),
    }


# =============================================================================== signal generation
def _scan_window_rows(params: dict[str, float], floor_params: dict[str, float]) -> int:
    """How many trailing rows :func:`scan_daily` must be handed for an EXACT read.

    ``scan_daily`` slices only from the END of ``rows`` (H20, the fresh-cross band, the prior and
    trigger sessions), and ``_gap_floor_frac`` takes ``min(gap_lookback, len(rows)-1)`` pairs ending
    at ``rows[-1]``. So any suffix of at least ``max(lookback + 2, gap_lookback + 1)`` rows gives
    BIT-IDENTICAL output to the full prefix; the bounded window only avoids re-slicing long
    histories. ``test_windowed_scan_matches_the_full_prefix`` asserts the equality.
    """
    return max(int(params["lookback_days"]) + 2, int(floor_params["gap_lookback_sessions"]) + 1)


def _window(series: Series, i: int, width: int) -> list[DailyRow]:
    """The trailing ``min(width, i+1)`` rows ending at ``i``."""
    return series.rows[max(0, i + 1 - width) : i + 1]


def _unadjusted_at(structural: Sequence[date], today: date) -> bool:
    """Whether a rescaling ex-date falls in ``[today - UNADJUSTED_LOOKBACK_DAYS, today - 1]``.

    ``structural`` must be ascending. ``today`` itself is OUT of range: on the ex-date every bar in
    the window is still pre-ex, in one unit (the upcoming-ex skip's case). Mirrors the live sweep's
    ``ex_from=today - 35d, ex_to=yesterday`` window.
    """
    if not structural:
        return False
    j = bisect_left(structural, today - timedelta(days=UNADJUSTED_LOOKBACK_DAYS))
    return j < len(structural) and structural[j] < today


def _h20_of(window: Sequence[DailyRow], lookback: int) -> float:
    """The broken level ``scan_daily`` tested: max high of the ``lookback`` sessions STRICTLY before
    the window's last row. Margin denominator only; the level a variant rests is ``raw_levels.entry``."""
    return max(r.high for r in window[-(lookback + 1) : -1])


def signals_for(
    series: Series,
    params: dict[str, float],
    ex_dates: Sequence[date],
    structural_ex_dates: Sequence[date] = (),
    *,
    floor_params: dict[str, float] | None = None,
    veto_counts: dict[str, int] | None = None,
    full_prefix: bool = False,
) -> list[Signal]:
    """Every ``brk20`` fresh cross in this symbol's history, as :class:`Signal` rows.

    ``scan_daily`` decides; this chooses the window and applies the unadjusted-history veto to the
    DECISION day. ``full_prefix`` hands ``rows[:i+1]`` instead of the bounded window (the equality
    check for the unit test and ``--verify-window``).

    ``veto_counts``, when supplied, accumulates ``gap_floor`` / ``floor_unavailable`` (from
    ``scan_daily``) plus ``unadjusted_history``; ``None`` keeps this function pure.
    """
    fp = dict(floor_params or PRE_REGISTERED_FLOOR_PARAMS)
    lookback = int(params["lookback_days"])
    width = _scan_window_rows(params, fp)
    n = len(series)
    out: list[Signal] = []
    for i in range(lookback + 1, n):
        # `today` = the session AFTER y (when the sweep runs and the order is placed); y is the last
        # COMPLETED session. At the last bar there is no next session: that signal can never be
        # admitted but is still scanned so the fired count means "fresh crosses in the window".
        today = series.dates[i + 1] if i + 1 < n else series.dates[i]
        window = series.rows[: i + 1] if full_prefix else _window(series, i, width)
        cand = scan_daily(
            series.symbol, window, today=today, upcoming_ex_dates=ex_dates, params=params,
            veto_counts=veto_counts,
        )
        if cand is None:
            continue
        if _unadjusted_at(structural_ex_dates, today):
            _bump(veto_counts, VETO_UNADJUSTED_HISTORY)
            continue
        h20 = _h20_of(window, lookback)
        if round_to_tick(h20) != cand.raw_levels.entry:
            # Tripwire, not a fallback: a disagreement means the window slicing stopped matching
            # scan_daily's, which would corrupt every margin tercile.
            raise AssertionError(
                f"H20 read-back disagrees with the shipped level on {series.symbol} "
                f"{series.dates[i]}: round_to_tick({h20}) != {cand.raw_levels.entry}"
            )
        out.append(Signal(
            symbol=series.symbol,
            idx=i,
            signal_date=series.dates[i],
            level=float(cand.raw_levels.entry),
            h20=h20,
            margin=float(series.close[i]) / h20 - 1.0,
            score=float(cand.score),
        ))
    return out


# =============================================================================== admission + fills
def bars_usable(series: Series, i: int, span: int) -> bool:
    """Whether sessions ``i+1 .. i+span`` all exist with finite, positive OHLC.

    A signal whose forward window holds an unusable bar is dropped whole, so the variants never get
    different denominators.
    """
    if i + span >= len(series):
        return False
    for j in range(i + 1, i + span + 1):
        for arr in (series.open, series.high, series.low, series.close):
            v = float(arr[j])
            if not math.isfinite(v) or v <= 0.0:
                return False
    return True


def fill_for(series: Series, sig: Signal, variant: str) -> tuple[int, float] | None:
    """``(fill index, fill price)`` for ``sig`` under ``variant``, or ``None`` when it never fills.

    Next-open is unconditional at ``open(y+1)``. A limit fills on the FIRST session in ``y+1 .. y+N``
    whose ``low <= level``, at ``min(open, level)``. See the module docstring for the two
    divergences from ``engine.paper.broker``'s tick fill model and their bias.
    """
    window = FILL_WINDOW[variant]
    if window is None:
        return sig.idx + 1, float(series.open[sig.idx + 1])
    for j in range(sig.idx + 1, sig.idx + window + 1):
        if float(series.low[j]) <= sig.level:
            return j, min(float(series.open[j]), sig.level)
    return None


def measure(
    series: Series,
    sig: Signal,
    variant: str,
    fill: tuple[int, float],
    *,
    cost_pct: float,
    horizons: Sequence[int] = HORIZONS,
) -> Trade:
    """Measure one filled signal: exits at ``close(fill + k)``, minus ONE round trip at every horizon.

    Admission (:func:`bars_usable`) guarantees every exit bar exists, so a missing bar here is a bug.
    """
    fill_idx, fill_px = fill
    trade = Trade(
        symbol=series.symbol,
        variant=variant,
        signal_date=sig.signal_date,
        fill_date=series.dates[fill_idx],
        fill_idx=fill_idx,
        fill_px=fill_px,
        fill_delay_sessions=fill_idx - sig.idx,
        margin=sig.margin,
        tercile=sig.tercile,
    )
    for k in horizons:
        gross = (float(series.close[fill_idx + k]) / fill_px - 1.0) * 100.0
        trade.gross[k] = gross
        trade.net[k] = gross - cost_pct
    return trade


# =============================================================================== aggregation
def _stats(gross: list[float], net: list[float]) -> dict[str, Any]:
    if not net:
        return {
            "n": 0, "hit_rate_gross": None, "hit_rate_net": None,
            "mean_gross": None, "median_gross": None, "mean_net": None, "median_net": None,
        }
    return {
        "n": len(net),
        "hit_rate_gross": round(sum(1 for v in gross if v > 0) / len(gross), 4),
        "hit_rate_net": round(sum(1 for v in net if v > 0) / len(net), 4),
        "mean_gross": round(statistics.fmean(gross), 4),
        "median_gross": round(statistics.median(gross), 4),
        "mean_net": round(statistics.fmean(net), 4),
        "median_net": round(statistics.median(net), 4),
    }


def cell_stats(trades: Sequence[Trade], horizons: Sequence[int] = HORIZONS) -> dict[str, Any]:
    return {
        "n_trades": len(trades),
        "horizons": {
            str(k): _stats(
                [t.gross[k] for t in trades if k in t.gross],
                [t.net[k] for t in trades if k in t.net],
            )
            for k in horizons
        },
    }


def margin_tercile_cuts(signals: Sequence[Signal]) -> tuple[float, float] | None:
    """The two cuts of the breakout-margin tercile split, or ``None`` when it is not computable.

    Taken once over the ADMITTED SIGNAL population, so a signal sits in the same cell under every
    variant. Full-sample statistics, so the split is DESCRIPTIVE.
    """
    margins = sorted(s.margin for s in signals if math.isfinite(s.margin))
    if len(margins) < 3:
        return None
    c1, c2 = statistics.quantiles(margins, n=3)
    return float(c1), float(c2)


def tercile_of(margin: float, cuts: tuple[float, float] | None) -> str | None:
    """Which tercile cell ``margin`` falls in. ``<= c1`` low, ``<= c2`` middle, else high — a
    partition by construction, so the three cells sum to the measured population exactly."""
    if cuts is None or not math.isfinite(margin):
        return None
    c1, c2 = cuts
    if margin <= c1:
        return CELL_MARGIN_LOW
    if margin <= c2:
        return CELL_MARGIN_MID
    return CELL_MARGIN_HIGH


def split_cells(trades: Sequence[Trade]) -> dict[str, list[Trade]]:
    """The pooled cell plus the mandatory margin-tercile cells. Every cell is reported separately."""
    cells: dict[str, list[Trade]] = {CELL_ALL: list(trades)}
    for name in MARGIN_CELLS:
        cells[name] = [t for t in trades if t.tercile == name]
    return cells


def signal_counts_by_cell(signals: Sequence[Signal]) -> dict[str, int]:
    """``cell -> number of admitted SIGNALS in it`` — the honest denominator of a per-cell fill rate.

    A variant's per-cell trade count over its own total is NOT a fill rate; the cell's signals are the
    denominator that makes a tercile row comparable across variants.
    """
    counts = {CELL_ALL: len(signals)}
    for name in MARGIN_CELLS:
        counts[name] = sum(1 for s in signals if s.tercile == name)
    return counts


def cohort_label(variant: str, kind: str) -> str:
    """The labelled matched-cohort cell, e.g. ``next_open_on_limit_5_sessions_filled``."""
    return f"{VARIANT_NEXT_OPEN}_on_{variant}_{kind}"


def _trade_key(t: Trade) -> tuple[str, date]:
    """The identity of the SIGNAL behind a trade — one per (symbol, signal date) by construction."""
    return (t.symbol, t.signal_date)


def _med_net(cell: dict[str, Any], horizon: int) -> float | None:
    return cell["horizons"][str(horizon)]["median_net"]


def _delta(a: float | None, b: float | None) -> float | None:
    """``a - b`` in percentage POINTS, or ``None`` when either side is an honest n=0."""
    return None if a is None or b is None else round(a - b, 4)


def matched_cohort_decomposition(
    trades_by_variant: dict[str, list[Trade]],
    signals_by_cell: dict[str, int],
    horizons: Sequence[int] = HORIZONS,
) -> dict[str, Any]:
    """Separate the entry-PRICE effect from the SELECTION effect, per limit variant, in labelled cells.

    The variants share a SIGNAL population but NOT a trade set, so the pooled next-open vs limit gap
    mixes two things, pulled apart at every horizon:

    * **PRICE effect** = ``median_net(limit) - median_net(next-open on the cohort the limit FILLED)``.
      Same events; the limit paid the level instead of open(y+1) AND, for fills at delay >= 2, held a
      window shifted by the fill delay (exits anchor on the fill) — price-and-timing, not pure price.
    * **SELECTION effect** = ``median_net(next-open on the FILLED cohort) - median_net(next-open on
      the UNFILLED cohort)``. Same entry mechanism; only which signals came back to the level differs.

    DESCRIPTIVE and post-hoc, NOT a fourth trial; the registered decision rule still reads the pooled
    cells. Next-open fills every admitted signal, so the filled and unfilled cohorts partition its
    trades; ``every_limit_fill_has_a_next_open_leg`` reports that reconciliation.
    """
    next_open_trades = list(trades_by_variant[VARIANT_NEXT_OPEN])
    next_open_all = cell_stats(next_open_trades, horizons)
    out: dict[str, Any] = {
        "definition": (
            "next-open re-quoted on exactly the signals each limit variant FILLED (the matched cohort) and on "
            "exactly the ones it did NOT fill. PRICE effect = limit - next-open on the filled cohort (same "
            "events; the limit pays the level instead of open(y+1) and, for fills at delay >= 2, holds a "
            "window shifted by the fill delay - an entry price-AND-timing effect, not a pure price "
            "effect); SELECTION effect = next-open on filled - next-open on unfilled (same "
            "entry price rule, different signals). DESCRIPTIVE decomposition of the registered "
            "result - no new variant, no new parameter, no new selection, not a fourth trial. "
            "MEDIANS DO NOT DECOMPOSE ADDITIVELY: the pooled gap, the price effect and the selection "
            "effect are three separate comparisons of medians over three different sets, not the "
            "terms of a variance decomposition, and they are not expected to sum."
        ),
        "descriptive_post_hoc_not_a_registered_trial": True,
        "variants": {},
    }
    for variant in (VARIANT_LIMIT_3, VARIANT_LIMIT_5):
        limit_trades = list(trades_by_variant[variant])
        filled_keys = {_trade_key(t) for t in limit_trades}
        next_open_filled = [t for t in next_open_trades if _trade_key(t) in filled_keys]
        next_open_unfilled = [t for t in next_open_trades if _trade_key(t) not in filled_keys]
        cells = {
            cohort_label(variant, COHORT_FILLED): cell_stats(next_open_filled, horizons),
            cohort_label(variant, COHORT_UNFILLED): cell_stats(next_open_unfilled, horizons),
            f"{variant}_{COHORT_FILLED}": cell_stats(limit_trades, horizons),
        }
        c_no_f = cells[cohort_label(variant, COHORT_FILLED)]
        c_no_u = cells[cohort_label(variant, COHORT_UNFILLED)]
        c_lim = cells[f"{variant}_{COHORT_FILLED}"]

        effects: dict[str, Any] = {}
        for k in horizons:
            price = _delta(_med_net(c_lim, k), _med_net(c_no_f, k))
            selection = _delta(_med_net(c_no_f, k), _med_net(c_no_u, k))
            pooled = _delta(_med_net(c_lim, k), _med_net(next_open_all, k))
            effects[str(k)] = {
                "pooled_gap_median_net_pp": pooled,
                "price_effect_median_net_pp": price,
                "selection_effect_median_net_pp": selection,
                "carried_by": _carried_by(price, selection),
            }

        by_tercile: dict[str, Any] = {}
        beats: dict[str, Any] = {}
        for name in MARGIN_CELLS:
            f_cell = cell_stats([t for t in next_open_filled if t.tercile == name], horizons)
            u_cell = cell_stats([t for t in next_open_unfilled if t.tercile == name], horizons)
            limit_cell = cell_stats([t for t in limit_trades if t.tercile == name], horizons)
            n_sig = signals_by_cell.get(name, 0)
            by_tercile[name] = {
                "n_signals": n_sig,
                "n_limit_filled": limit_cell["n_trades"],
                "n_limit_unfilled": n_sig - limit_cell["n_trades"],
                "fill_rate": None if not n_sig else round(limit_cell["n_trades"] / n_sig, 4),
                cohort_label(variant, COHORT_FILLED): f_cell,
                cohort_label(variant, COHORT_UNFILLED): u_cell,
                f"{variant}_{COHORT_FILLED}": limit_cell,
                "price_effect_median_net_pp": {
                    str(k): _delta(_med_net(limit_cell, k), _med_net(f_cell, k)) for k in horizons
                },
            }
        for k in horizons:
            verdicts = [
                by_tercile[name]["price_effect_median_net_pp"][str(k)] for name in MARGIN_CELLS
            ]
            # None anywhere = a cell with no comparison, so the claim is NOT TESTABLE, never "true".
            beats[str(k)] = None if any(v is None for v in verdicts) else all(v > 0 for v in verdicts)

        out["variants"][variant] = {
            "n_limit_filled": len(limit_trades),
            "n_limit_unfilled": len(next_open_trades) - len(next_open_filled),
            "n_next_open_on_filled_cohort": len(next_open_filled),
            "n_next_open_on_unfilled_cohort": len(next_open_unfilled),
            "every_limit_fill_has_a_next_open_leg": len(next_open_filled) == len(filled_keys),
            "cells": cells,
            "effects": effects,
            "by_margin_tercile": by_tercile,
            "limit_beats_matched_next_open_in_every_margin_tercile": beats,
        }
    return out


def _carried_by(price: float | None, selection: float | None) -> str:
    """Which effect carries the limit's advantage at one horizon.

    Signs, on median NET: ``price > 0`` means the level is the better PRICE on the same events;
    ``selection < 0`` means the cohort the limit never catches is the better one under next-open, so
    selection works AGAINST the limit.
    """
    if price is None or selection is None:
        return "unknown (an n=0 cohort cell at this horizon)"
    if price > 0 and selection <= 0:
        return (
            f"{EFFECT_PRICE.upper()} - the entry price carries the limit's advantage ({price:+.4f} pp on the "
            f"matched cohort), and the SELECTION runs AGAINST it ({selection:+.4f} pp: the signals "
            "the limit never catches are the better ones under next-open)"
        )
    if price <= 0 and selection > 0:
        return (
            f"{EFFECT_SELECTION.upper()} - the limit's advantage is which signals it books, not the price "
            f"({selection:+.4f} pp selection vs {price:+.4f} pp price on the matched cohort)"
        )
    if price > 0 and selection > 0:
        return (
            f"BOTH - price {price:+.4f} pp on the matched cohort AND selection {selection:+.4f} pp"
        )
    return (
        f"NEITHER - the limit has no advantage at this horizon on the matched cohort "
        f"(price {price:+.4f} pp, selection {selection:+.4f} pp)"
    )


# =============================================================================== geometry (ORB lesson)
def geometry(
    trades: Sequence[Trade], cost_pct: float, horizons: Sequence[int] = HORIZONS
) -> dict[str, Any]:
    """Median per-trade GROSS drift vs the round-trip cost floor, per horizon, with a hard verdict.

    ``viable`` iff the MEDIAN gross drift exceeds one full round trip. Median, not mean: tails can
    hide a typical trade that never covers its costs.
    """
    out: dict[str, Any] = {"cost_floor_pct": round(cost_pct, 6), "horizons": {}}
    for k in horizons:
        vals = [t.gross[k] for t in trades if k in t.gross]
        med = statistics.median(vals) if vals else None
        out["horizons"][str(k)] = {
            "n": len(vals),
            "median_gross_pct": None if med is None else round(med, 4),
            "cost_floor_pct": round(cost_pct, 6),
            "margin_pct": None if med is None else round(med - cost_pct, 4),
            "verdict": "unknown" if med is None else ("viable" if med > cost_pct else "dead"),
        }
    return out


def apply_reporting_rule(
    cpcv: dict[str, Any], geo_horizon: dict[str, Any], median_net: float | None
) -> dict[str, Any]:
    """Stamp the tightened REPORTING rule onto one variant × horizon CPCV block.

    ``reported_promotable`` = the CPCV/deflation gate passed AND the geometry is viable at this
    horizon (median NET > 0). The CPCV series is a per-fill-day MEAN and the geometry verdict a
    MEDIAN, so they can disagree (``gates_disagree``). It only WITHHOLDS a label:
    ``reported_promotable`` is never True where ``promotable`` is False. Mutates and returns ``cpcv``.
    """
    verdict = geo_horizon.get("verdict", "unknown")
    viable = median_net is not None and median_net > 0.0
    cpcv_pass = bool(cpcv["promotable"])
    reasons: list[str] = []
    if cpcv_pass and not viable:
        reasons.append(
            "TIGHTENED REPORTING RULE (2026-09-12): CPCV/deflation PASSES but the geometry is "
            f"{verdict} at this horizon (median net "
            f"{'n/a' if median_net is None else f'{median_net:+.4f}%'} <= 0) - the TYPICAL trade "
            "cannot pay one full round trip, so this cell is reported NOT VIABLE and NOT promotable."
        )
    if not cpcv_pass and viable:
        reasons.append(
            "Geometry is viable at this horizon but the CPCV/deflation gate refuses it; the "
            "tightened rule is an AND, so the cell is not promotable."
        )
    cpcv["median_net_pct"] = median_net
    cpcv["geometry_verdict"] = verdict
    cpcv["cpcv_series_statistic"] = "per-fill-day MEAN net / horizon (house convention, hi52)"
    cpcv["geometry_statistic"] = "MEDIAN net per trade (the ORB lesson)"
    cpcv["gates_disagree"] = bool(cpcv_pass != viable)
    cpcv["reported_promotable"] = bool(cpcv_pass and viable)
    cpcv["reported_promotable_reasons"] = reasons
    return cpcv


# =============================================================================== CPCV / deflation
def daily_net_series(trades: Sequence[Trade], horizon: int) -> tuple[list[date], np.ndarray]:
    """Per-FILL-DAY cost-adjusted return series for one horizon.

    Day *t* = mean NET %-return of the trades FILLED on *t*, divided by ``horizon`` sessions: a
    per-session net return in percent, the margin floor's units. Keyed on the fill day (module
    docstring).
    """
    by_day: dict[date, list[float]] = defaultdict(list)
    for t in trades:
        if horizon in t.net:
            by_day[t.fill_date].append(t.net[horizon])
    days = sorted(by_day)
    vals = np.array([statistics.fmean(by_day[d]) / float(horizon) for d in days], dtype="float64")
    return days, vals


def cpcv_report(
    trades: Sequence[Trade], horizon: int, cost_pct: float, trial_count: int = TRIAL_COUNT_N
) -> dict[str, Any]:
    """CPCV + the deflated promotion decision for one variant at one horizon.

    Purge and embargo are the HORIZON, not the default 5: a 5-observation purge would leak a T+20
    trade across the fold boundary. ``trial_count`` is the registered N=3.
    """
    days, vals = daily_net_series(trades, horizon)
    n_obs = int(vals.size)
    method = _CV_SKFOLIO
    splits: list[tuple[np.ndarray, np.ndarray]] = []
    if n_obs:
        try:
            splits = cpcv_splits(n_obs, purge=horizon, embargo=horizon)
        except Exception as exc:  # noqa: BLE001 - skfolio absent/incompatible must not sink the run
            method = f"{_CV_FALLBACK} (skfolio unavailable: {type(exc).__name__})"
            splits = _purged_kfold_splits(n_obs, purge=horizon, embargo=horizon)
        else:
            if not splits:
                method = _CV_FALLBACK
                splits = _purged_kfold_splits(n_obs, purge=horizon, embargo=horizon)
    folds: list[dict[str, Any]] = []
    for j, (train_idx, test_idx) in enumerate(splits):
        sl = vals[np.asarray(test_idx, dtype=np.int64)]
        exp = float(sl.mean()) if sl.size else None
        folds.append({
            "split": j,
            "n_train_obs": int(np.size(train_idx)),
            "n_test_obs": int(sl.size),
            "expectancy_pct_per_day": None if exp is None else round(exp, 6),
            "passed": bool(exp is not None and exp > 0.0),
        })
    pass_fraction = (sum(f["passed"] for f in folds) / len(folds)) if folds else None
    passing = [f["expectancy_pct_per_day"] for f in folds if f["passed"]]
    median_passing = float(np.median(passing)) if passing else None
    promotable, reasons = promotion_decision(
        trial_count,
        pass_fraction,
        median_passing_expectancy_pct=median_passing,
        cost_floor_pct=cost_pct,
        margin_floor_days=horizon,
    )
    return {
        "cv_method": method if splits else "none (insufficient observations)",
        "n_obs_days": n_obs,
        "first_day": str(days[0]) if days else None,
        "last_day": str(days[-1]) if days else None,
        "purge_obs": horizon,
        "embargo_obs": horizon,
        "trial_count_n": trial_count,
        "fold_pass_min": fold_pass_min(trial_count),
        "n_splits": len(folds),
        "fold_pass_fraction": None if pass_fraction is None else round(pass_fraction, 4),
        "median_passing_expectancy_pct_per_day": None if median_passing is None else round(median_passing, 6),
        "margin_floor_pct_per_day": margin_floor_pct_per_day(cost_pct, margin_floor_days=horizon),
        "promotable": promotable,
        "reasons": reasons,
        "folds": folds,
    }


# =============================================================================== the decision rule
def decision_outcome(
    doc: dict[str, Any], horizon: int = DECISION_HORIZON, *, gate_key: str = "promotable"
) -> dict[str, Any]:
    """Evaluate :data:`DECISION_RULE` against a finished document. Pure; reads, never chooses.

    Ranks only variants that are promotable at ``horizon`` under ``gate_key`` and have a median net
    there; ``neither`` when none. An exact tie between the top two is ``neither`` too.

    ``gate_key`` selects the promotability gate: ``"promotable"`` is the registered rule;
    ``"reported_promotable"`` is the tightening (:data:`REPORTING_RULE_AMENDMENT`). Both are always
    reported side by side so the tightening never silently replaces the registered outcome.
    """
    ranked: list[tuple[float, str]] = []
    for name in VARIANTS:
        block = doc["variants"][name]
        cpcv = block["cpcv"][str(horizon)]
        med = block["cells"][CELL_ALL]["horizons"][str(horizon)]["median_net"]
        if cpcv[gate_key] and med is not None:
            ranked.append((float(med), name))
    ranked.sort(key=lambda x: (-x[0], x[1]))
    if not ranked:
        outcome, winner = DECISION_NEITHER, None
    elif len(ranked) > 1 and ranked[0][0] == ranked[1][0]:
        outcome, winner = DECISION_TIE, None
    else:
        outcome, winner = ranked[0][1], ranked[0][1]
    return {
        "rule": DECISION_RULE,
        "promotability_gate": gate_key,
        "gate_description": (
            "the registered CPCV/deflation gate, verbatim" if gate_key == "promotable"
            else "the 2026-09-12 tightened reporting rule: CPCV/deflation AND geometry viable"
        ),
        "horizon_sessions": horizon,
        "promotable_at_horizon": [name for _m, name in ranked],
        "median_net_pct_at_horizon": {
            name: doc["variants"][name]["cells"][CELL_ALL]["horizons"][str(horizon)]["median_net"]
            for name in VARIANTS
        },
        "outcome": outcome,
        "winner": winner,
        "shipped_mechanism_unchanged": winner is None or winner != VARIANT_NEXT_OPEN,
    }


# =============================================================================== the study
def run_study(
    conn: duckdb.DuckDBPyConnection,
    *,
    start: date,
    end: date,
    cost_model: CostModel,
    notional: Decimal = REFERENCE_NOTIONAL,
    symbols: Sequence[str] | None = None,
    max_symbols: int | None = None,
    params: dict[str, float] | None = None,
    floor_params: dict[str, float] | None = None,
    verify_window: int = 0,
) -> tuple[dict[str, Any], dict[str, list[Trade]]]:
    """Run all three entry variants end to end over ONE shared SIGNAL population.

    The signal population is shared; the TRADE sets are not (see :func:`matched_cohort_decomposition`).

    Returns ``(document, trades_by_variant)``: the document is the JSON payload and report input;
    per-trade lists are not embedded in it but returned for callers and tests.

    ``symbols`` overrides the eligible universe (smoke runs only; the document says it is not the
    registered study). ``verify_window`` re-scans the first K symbols with the full prefix and aborts
    on any disagreement with the bounded window.
    """
    p = dict(params or PRE_REGISTERED_PARAMS)
    fp = dict(floor_params or PRE_REGISTERED_FLOOR_PARAMS)
    cost_pct = float(cost_model.breakeven_pct(notional, PRODUCT))
    fees_pct = float(cost_model.fee_breakeven_pct(notional, PRODUCT))
    spread_pct = float(cost_model.spread_pct)
    admission_span = MAX_FILL_WINDOW + max(HORIZONS)

    eligible, universe_source, universe_as_of = load_eligible_universe(conn)
    if symbols is not None:
        eligible = {s.strip().upper() for s in symbols if s.strip()}
        universe_source = f"--symbols override ({len(eligible)} names) - NOT the registered population"
    if not eligible:
        raise ValueError(
            "no eligible universe: universe_daily is empty or absent, and the eligible set IS this "
            "study's population - refusing rather than measuring an empty one"
        )
    series_by_symbol = load_series(conn, start, end, symbols=sorted(eligible), max_symbols=max_symbols)
    if not series_by_symbol:
        raise ValueError("no bars_1d rows for the eligible universe in the requested window")
    ex_dates = load_ex_dates(conn)
    structural = load_structural_ex_dates(conn)
    coverage = corp_actions_coverage(conn)

    # ------------------------------------------------------ signals (ONE shared SIGNAL population)
    vetoes: dict[str, int] = {}
    fired: list[tuple[Series, Signal]] = []
    n_fired = 0
    verify_left = int(verify_window)
    verify_checked = 0
    for sym in sorted(series_by_symbol):
        s = series_by_symbol[sym]
        sigs = signals_for(
            s, p, ex_dates.get(sym, []), structural.get(sym, []),
            floor_params=fp, veto_counts=vetoes,
        )
        if verify_left > 0:
            slow = signals_for(
                s, p, ex_dates.get(sym, []), structural.get(sym, []),
                floor_params=fp, full_prefix=True,
            )
            if [(x.idx, x.level) for x in slow] != [(x.idx, x.level) for x in sigs]:
                raise AssertionError(
                    f"bounded-window/full-prefix scan_daily disagreement on {sym}: "
                    f"windowed={[x.idx for x in sigs]} full={[x.idx for x in slow]}"
                )
            verify_left -= 1
            verify_checked += 1
        n_fired += len(sigs)
        for sig in sigs:
            if bars_usable(s, sig.idx, admission_span):
                fired.append((s, sig))

    cuts = margin_tercile_cuts([sig for _s, sig in fired])
    for _s, sig in fired:
        sig.tercile = tercile_of(sig.margin, cuts)
    # Signals per cell: the denominator of every per-cell fill rate.
    signals_by_cell = signal_counts_by_cell([sig for _s, sig in fired])

    # ---------------------------------------------------------------- fills + measurement per variant
    trades_by_variant: dict[str, list[Trade]] = {v: [] for v in VARIANTS}
    unfilled: dict[str, int] = {v: 0 for v in VARIANTS}
    for s, sig in fired:
        for variant in VARIANTS:
            fill = fill_for(s, sig, variant)
            if fill is None:
                unfilled[variant] += 1
                continue
            trades_by_variant[variant].append(measure(s, sig, variant, fill, cost_pct=cost_pct))

    all_dates = sorted({d for s in series_by_symbol.values() for d in s.dates})
    notes: list[str] = [
        "SHARED SIGNAL POPULATION, DIFFERENT TRADE SETS - read every pooled next-open vs limit number in this "
        "light. All three variants are offered the SAME admitted signals, but a limit only fills "
        "when price RETURNS to the broken level, i.e. conditional on the breakout not running away, "
        "so each limit variant trades a pullback-SELECTED subset and the per-variant n differ. A pooled "
        "next-open vs limit comparison is therefore a PER-FILLED-TRADE comparison across two different event "
        "sets, NOT a per-signal-originated one, and it does not price the signals the resting limit "
        "forfeits. STEP 3B (matched_cohorts) is the decomposition that separates the entry-PRICE "
        "effect from the SELECTION effect; read it before quoting any headline gap.",
        "PER-SIGNAL-ORIGINATED EXPECTANCY, if you need the deployment basis rather than the "
        "per-filled-trade one, is an arithmetic identity on the cells already printed: book an "
        "unfilled signal at 0.0% and the per-signal MEAN is mean_net x fill_rate (next-open's fill rate is "
        "1.0, so its pooled mean already is one). It is deliberately not printed as a headline "
        "because it is a MEAN statistic and the ORB lesson distrusts means: the per-signal MEDIAN is "
        "degenerate at these fill rates (~51-59% filled, so the median sits on the zero-return "
        "plateau of the unfilled signals and says nothing about either mechanism). The matched-cohort "
        "cells below are the robust read of the same question.",
        "MARGIN-TERCILE CELLS ARE PER COHORT, NOT PER TRADE SET: fill rate varies sharply by tercile, "
        "so a next-open tercile row and a limit tercile row cover different signals unless matched. Every cell "
        "now carries its own n_signals_in_cell and fill_rate_in_cell, and the per-tercile matched "
        "comparison (next-open restricted to the cohort that variant filled) is in matched_cohorts."
        "by_margin_tercile with n per cell. Any 'in every margin tercile' claim must be read off "
        "limit_beats_matched_next_open_in_every_margin_tercile, never off the unmatched pooled rows.",
        REPORTING_RULE_AMENDMENT,
        "POPULATION IS A SURVIVORSHIP-TAINTED PROXY: NIFTY 500 / eligible membership as of each "
        "signal date is not stored anywhere in this platform, so the CURRENT eligible set ("
        + universe_source + ") is applied backwards over the whole window. Names that joined the "
        "index after a run appear eligible throughout that run; names that were dropped are "
        "mislabelled the other way. The population is indicative, not an as-of-date membership set.",
        "SURVIVORSHIP (bars): the symbol set is whatever bars_1d holds for those names. Since the "
        "2026-09-03 archive backfill it carries since-delisted history, but a name delisted inside "
        "the horizon books no trade, so the residual bias is optimistic and only partly corrected.",
        "MARGIN TERCILE CUTS ARE DESCRIPTIVE, NOT TRADEABLE: they are full-sample statistics of the "
        "measured signal population, unknowable at signal time. The split says where the two "
        "mechanisms differ; it is not a filter anyone could have run.",
        "limit FILL RATES ARE AN UPPER BOUND: a daily bar whose low EQUALS the level counts as a fill "
        "here, while engine.paper.broker's tick model requires a strict trade-through. The other "
        "divergence runs the same way: min(open, level) books the opening print when a session gaps "
        "through the level, where that model never fills better than the limit.",
        "HORIZONS ARE MEASURED FROM THE FILL, not from the signal - a deliberate divergence from the "
        "hi52/event_study convention, because the variants fill on different sessions and a signal "
        "anchor would charge the limit variants for the days it spent waiting.",
        f"NO PARAMETER SWEEP was run: the live rule at its frozen defaults, three pre-registered "
        f"entry variants, trial count N={TRIAL_COUNT_N} (fold_pass_min = "
        f"{fold_pass_min(TRIAL_COUNT_N):.0%}).",
        "UNADJUSTED-HISTORY VETO mirrors the live brk20 sweep: a symbol carrying a "
        + "/".join(sorted(UNADJUSTED_KINDS))
        + f" ex-date in the {UNADJUSTED_LOOKBACK_DAYS} calendar days before the decision day is not "
        "read at all (stored bars_1d history is never re-adjusted, so its 20-session window holds "
        f"bars in two units). The veto's reach IS the STRUCTURAL corp_actions coverage - "
        f"{coverage['structural_rows']} rows, ex_date {coverage['structural_ex_date_min']} -> "
        f"{coverage['structural_ex_date_max']} (all kinds: {coverage['rows']} rows): an empty or "
        "short span silently disables it and the measured population then exceeds the one the live "
        "sweep can originate.",
        "EX-DATE VETO IS NOT STRICTLY-BEFORE: the upcoming-ex skip is handed the complete "
        "historical corp_actions table per symbol, so a signal in 2022 is tested against ex-dates "
        "that may have been DECLARED after it. The declaration date is not stored, so this cannot be "
        "corrected here. It can only REMOVE signals, so the measured population is a subset of what "
        "was live-originable; the direction of the return bias is unknown and unmeasured.",
        "brk20 carries NO expectancy presumption. A variant winning the decision rule is evidence "
        "about the ENTRY MECHANISM only; wiring anything is a separate section 8.6 owner decision.",
    ]
    if cuts is None:
        notes.append(
            "MARGIN TERCILE SPLIT NOT COMPUTABLE: fewer than 3 admitted signals. The tercile cells "
            "below are empty by arithmetic, not by evidence."
        )
    if symbols is not None:
        notes.append(
            "POPULATION OVERRIDDEN with --symbols: this run is a smoke test, NOT the registered "
            "study. The pre-registered population is the eligible universe."
        )

    doc: dict[str, Any] = {
        "meta": {
            "script": "scripts/backtest_brk20.py",
            "strategy_id": STRATEGY_ID,
            "registration": "brk20 entry-mechanism (pre-registered 2026-09-12)",
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "window": {"start": str(start), "end": str(end)},
            "n_eligible_symbols": len(eligible),
            "n_symbols_with_bars": len(series_by_symbol),
            "n_sessions": len(all_dates),
            "params": p,
            "params_match_live_defaults": p == dict(DEFAULT_PARAMS),
            "floor_params": fp,
            "floor_params_match_live_defaults": fp == dict(FLOOR_PARAMS),
            "parameter_sweep_run": False,
            "trial_count_n": TRIAL_COUNT_N,
            "fold_pass_min": fold_pass_min(TRIAL_COUNT_N),
            "variants": list(VARIANTS),
            "horizons_sessions": list(HORIZONS),
            "horizon_anchor": "the FILL session: return = close(fill + k) / fill_px - 1",
            "admission_span_sessions": admission_span,
            "product": PRODUCT,
            "reference_notional_inr": str(notional),
            "cost_round_trip_pct": round(cost_pct, 6),
            "cost_fees_pct": round(fees_pct, 6),
            "cost_spread_pct": round(spread_pct, 6),
            "sizing": "per-trade equal notional; percent returns; no compounding; event study",
            "universe_source": universe_source,
            "universe_as_of": None if universe_as_of is None else str(universe_as_of),
            "population_is_survivorship_tainted_proxy": True,
            "unadjusted_lookback_calendar_days": UNADJUSTED_LOOKBACK_DAYS,
            "window_verified_symbols": verify_checked,
            # POST-veto: the raw fresh-cross count is this plus every entry in `vetoes` below.
            "n_signals_after_vetoes": n_fired,
            "n_signals_admitted": len(fired),
        },
        "corp_actions_coverage": coverage,
        "vetoes": {
            VETO_GAP_FLOOR: vetoes.get(VETO_GAP_FLOOR, 0),
            VETO_FLOOR_UNAVAILABLE: vetoes.get(VETO_FLOOR_UNAVAILABLE, 0),
            VETO_UNADJUSTED_HISTORY: vetoes.get(VETO_UNADJUSTED_HISTORY, 0),
        },
        "margin_tercile_cuts": None if cuts is None else {
            "cut_1": round(cuts[0], 6), "cut_2": round(cuts[1], 6),
            "metric": "close(y)/H20 - 1",
            "rule": "tercile 1 <= cut_1 < tercile 2 <= cut_2 < tercile 3 (population quantiles)",
        },
        "geometry": {},
        "variants": {},
        "notes": notes,
    }

    for variant in VARIANTS:
        trades = trades_by_variant[variant]
        n_filled = len(trades)
        delays = [t.fill_delay_sessions for t in trades]
        geo = geometry(trades, cost_pct)
        doc["geometry"][variant] = geo
        cells: dict[str, Any] = {}
        for name, cell in split_cells(trades).items():
            stats = cell_stats(cell)
            # Per-cell fill rate on the cell's own SIGNAL denominator.
            n_sig = signals_by_cell.get(name, 0)
            stats["n_signals_in_cell"] = n_sig
            stats["n_unfilled_in_cell"] = n_sig - stats["n_trades"]
            stats["fill_rate_in_cell"] = None if not n_sig else round(stats["n_trades"] / n_sig, 4)
            cells[name] = stats
        cpcv = {str(k): cpcv_report(trades, k, cost_pct) for k in HORIZONS}
        for k in HORIZONS:
            apply_reporting_rule(
                cpcv[str(k)], geo["horizons"][str(k)], cells[CELL_ALL]["horizons"][str(k)]["median_net"]
            )
        doc["variants"][variant] = {
            "fill_window_sessions": FILL_WINDOW[variant],
            "n_signals_admitted": len(fired),
            "n_filled": n_filled,
            "n_unfilled": unfilled[variant],
            "fill_rate": None if not fired else round(n_filled / len(fired), 4),
            "fill_delay_sessions": {
                "median": None if not delays else statistics.median(delays),
                "mean": None if not delays else round(statistics.fmean(delays), 4),
                "histogram": {str(d): delays.count(d) for d in sorted(set(delays))},
            },
            "cells": cells,
            "cpcv": cpcv,
        }
    doc["matched_cohorts"] = matched_cohort_decomposition(trades_by_variant, signals_by_cell)
    # Both outcomes side by side: a tightening must never silently replace the registered outcome.
    doc["decision"] = decision_outcome(doc)
    tightened = decision_outcome(doc, gate_key="reported_promotable")
    tightened["amendment"] = REPORTING_RULE_AMENDMENT
    tightened["changes_the_registered_outcome"] = bool(
        tightened["outcome"] != doc["decision"]["outcome"]
        or tightened["winner"] != doc["decision"]["winner"]
    )
    doc["decision_under_tightened_reporting_rule"] = tightened
    return doc, trades_by_variant


# =============================================================================== rendering
#: Transliterations for non-ASCII characters on borrowed strings (em dashes, rupee sign). Only the
#: CONSOLE rendering is folded; a cp1252 console cannot print them. The JSON keeps the original.
_ASCII_MAP = str.maketrans({
    "—": "-", "–": "-", "‘": "'", "’": "'", "“": '"', "”": '"',
    "→": "->", "≥": ">=", "≤": "<=", "×": "x", "₹": "Rs ", " ": " ",
})


def _ascii(text: str) -> str:
    return text.translate(_ASCII_MAP).encode("ascii", "replace").decode("ascii")


def _f(v: Any, width: int = 9, prec: int = 4) -> str:
    if v is None:
        return "-".rjust(width)
    if isinstance(v, float):
        return f"{v:+.{prec}f}".rjust(width)
    return str(v).rjust(width)


def _md_pct(v: float | None) -> str:
    """A [0, 1] fraction as a cell; em-dash for ``None`` — never 0.0%, which reads as a measured zero."""
    return "—" if v is None else f"{v * 100:.1f}%"


def _md_ret(v: float | None) -> str:
    """A signed percent return as a cell; em-dash on ``None``."""
    return "—" if v is None else f"{v:+.4f}%"


def render_text(doc: dict[str, Any]) -> str:
    """Plain-text report, folded to ASCII. GEOMETRY prints FIRST and the DECISION last."""
    m = doc["meta"]
    out: list[str] = []
    add = out.append
    add("=" * 100)
    add("brk20 ENTRY-MECHANISM BACKTEST")
    add("=" * 100)
    add(f"generated       : {m['generated_at']}")
    add(f"registration    : {m['registration']}")
    add(f"window          : {m['window']['start']} -> {m['window']['end']}  "
        f"({m['n_sessions']} sessions, {m['n_symbols_with_bars']} of {m['n_eligible_symbols']} "
        "eligible symbols have bars)")
    add(f"population      : {m['universe_source']}  [SURVIVORSHIP-TAINTED PROXY]")
    add(f"params          : {json.dumps(m['params'], sort_keys=True)}")
    add(f"floor params    : {json.dumps(m['floor_params'], sort_keys=True)}")
    add(f"params == live  : rule {m['params_match_live_defaults']}, floor "
        f"{m['floor_params_match_live_defaults']}   (sweep run: {m['parameter_sweep_run']}, "
        f"trial count N={m['trial_count_n']}, fold_pass_min = {m['fold_pass_min']:.0%})")
    add(f"entry variants  : {', '.join(m['variants'])}")
    add(f"horizons        : T+{'/T+'.join(str(k) for k in m['horizons_sessions'])} from "
        f"{m['horizon_anchor']}")
    add(f"admission       : ONE SHARED SIGNAL POPULATION - {m['admission_span_sessions']} forward "
        "sessions required per signal, so every variant is offered the same signals and no horizon "
        "is measured on a short window")
    add("trade sets      : NOT SHARED. A limit fills only when price RETURNS to the level, so each "
        "limit variant")
    add("                  trades a pullback-SELECTED subset (see the fill rates below). Pooled "
        "next-open vs limit numbers are")
    add("                  PER FILLED TRADE across different event sets - STEP 3B decomposes them.")
    add(f"sizing          : {m['sizing']}")
    add(f"cost round trip : {m['cost_round_trip_pct']:.4f}% {m['product']} at Rs {m['reference_notional_inr']} "
        f"= {m['cost_fees_pct']:.4f}% fees + {m['cost_spread_pct']:.4f}% spread (both legs, once per trade)")
    v = doc["vetoes"]
    add(f"signals         : {m['n_signals_after_vetoes']} fresh crosses survived every veto, "
        f"{m['n_signals_admitted']} admitted (full forward window)   vetoes (all already excluded "
        f"from that count): gap_floor {v['gap_floor']}, floor_unavailable {v['floor_unavailable']}, "
        f"unadjusted_history {v['unadjusted_history']}")
    if m["window_verified_symbols"]:
        add(f"window check    : full-prefix scan_daily agreement verified on "
            f"{m['window_verified_symbols']} symbol(s)")
    cuts = doc["margin_tercile_cuts"]
    cut_text = (
        "NOT COMPUTABLE" if cuts is None
        else f"{cuts['cut_1']:+.4f} / {cuts['cut_2']:+.4f}"
    )
    add(f"margin terciles : {cut_text}  on close(y)/H20 - 1  "
        "[DESCRIPTIVE, full-sample cuts - not a tradeable filter]")
    add("")

    # ------------------------------------------------------------------ ORB-lesson geometry FIRST
    add("-" * 100)
    add("STEP 1 - COST GEOMETRY (the ORB lesson: arithmetic BEFORE any signal-quality claim)")
    add("-" * 100)
    add("Median per-trade GROSS drift vs one full round trip. A variant whose TYPICAL trade cannot")
    add("pay the toll is dead regardless of hit rate, t-stat or fold count.")
    add("")
    for variant, geo in doc["geometry"].items():
        add(f"[{variant}]  cost floor = {geo['cost_floor_pct']:.4f}%")
        if all(h["n"] == 0 for h in geo["horizons"].values()):
            add("  no measured trades - GEOMETRY: unknown at every horizon (honest n=0).")
            add("")
            continue
        add("  horizon |     n | median gross % | cost floor % |   margin % | verdict")
        add("  --------+-------+----------------+--------------+------------+---------")
        for k in sorted(geo["horizons"], key=int):
            h = geo["horizons"][k]
            add(f"  T+{k:<5} | {h['n']:>5} | {_f(h['median_gross_pct'], 14)} | "
                f"{h['cost_floor_pct']:>12.4f} | {_f(h['margin_pct'], 10)} | {h['verdict']}")
        for k in sorted(geo["horizons"], key=int):
            h = geo["horizons"][k]
            add(f"  GEOMETRY: {h['verdict']} at T+{k}  [{variant}]")
        add("")

    # ------------------------------------------------------------------ fills
    add("-" * 100)
    add("STEP 2 - FILL RATE (an unfilled breakout is the mechanism's real cost, not an absent trade)")
    add("-" * 100)
    add("  variant                   | window |  admitted |    filled |  unfilled | fill rate | med delay")
    add("  --------------------------+--------+-----------+-----------+-----------+-----------+----------")
    for variant in VARIANTS:
        b = doc["variants"][variant]
        win = b["fill_window_sessions"]
        w = "market" if win is None else f"{win}d"
        fr = "-" if b["fill_rate"] is None else f"{b['fill_rate'] * 100:.1f}%"
        med_delay = b["fill_delay_sessions"]["median"]
        dly = "-" if med_delay is None else f"{med_delay:.1f}"
        add(f"  {variant:<25} | {w:>6} | {b['n_signals_admitted']:>9} | {b['n_filled']:>9} | "
            f"{b['n_unfilled']:>9} | {fr:>9} | {dly:>9}")
    add("")

    # ------------------------------------------------------------------ signal quality by cell
    add("-" * 100)
    add("STEP 3 - RETURNS BY VARIANT AND MANDATORY MARGIN-TERCILE SPLIT (never the pooled number alone)")
    add("-" * 100)
    add("EVERY CELL CARRIES ITS OWN FILL RATE (filled trades / SIGNALS in that cell). Two variants'")
    add("rows in the same cell are NOT the same trades unless that fill rate is 100% - fill rate")
    add("varies sharply by margin tercile, which is exactly where the mechanisms are compared.")
    add("")
    for variant in VARIANTS:
        block = doc["variants"][variant]
        add(f"[{variant}]  measured trades: {block['n_filled']}")
        if not block["n_filled"]:
            add("  no measured trades - every split cell is empty (honest n=0).")
            add("")
            continue
        add("  cell                      | hor | sig n |     n |  fill% | hit% net | mean gross | med gross |  mean net |   med net")
        add("  --------------------------+-----+-------+-------+--------+----------+------------+-----------+-----------+----------")
        for cell, stats in block["cells"].items():
            fr = stats.get("fill_rate_in_cell")
            frs = "-" if fr is None else f"{fr * 100:.1f}"
            for k in sorted(stats["horizons"], key=int):
                s = stats["horizons"][k]
                hit = "-" if s["hit_rate_net"] is None else f"{s['hit_rate_net'] * 100:.1f}"
                add(f"  {cell:<25} | T+{k:<2} | {stats.get('n_signals_in_cell', 0):>5} | "
                    f"{s['n']:>5} | {frs:>6} | {hit:>8} | "
                    f"{_f(s['mean_gross'], 10)} | {_f(s['median_gross'], 9)} | "
                    f"{_f(s['mean_net'], 9)} | {_f(s['median_net'], 9)}")
        add("")

    # ------------------------------------------------- matched cohorts: PRICE effect vs SELECTION
    add("-" * 100)
    add("STEP 3B - MATCHED-COHORT DECOMPOSITION (entry PRICE effect vs SELECTION effect)")
    add("-" * 100)
    mc = doc.get("matched_cohorts")
    if not mc:
        add("  not computed.")
        add("")
    else:
        add("The three variants share a SIGNAL population but NOT a trade set, so a pooled next-open vs limit gap")
        add("mixes two effects. These cells pull them apart, on medians of NET returns:")
        add("  PRICE effect     = limit  minus  next-open restricted to the signals the limit variant FILLED (same events;")
        add("                     the level instead of open(y+1), and for fills at delay >= 2 a holding")
        add("                     window shifted by the fill delay - price AND timing, not price alone).")
        add("  SELECTION effect = next-open on the FILLED cohort  minus  next-open on the UNFILLED cohort (same")
        add("                     entry rule, different signals - what the resting limit forfeits).")
        add("DESCRIPTIVE, post-hoc, NOT a fourth trial: no new variant, parameter or selection rule,")
        add("and the registered decision rule is still evaluated on the pooled cells.")
        add("MEDIANS DO NOT DECOMPOSE ADDITIVELY - the pooled gap, the price effect and the selection")
        add("effect are three comparisons over three different sets and are not expected to sum.")
        add("")
        for variant, blk in mc["variants"].items():
            add(f"[{variant}]  limit filled {blk['n_limit_filled']} of "
                f"{blk['n_next_open_on_filled_cohort'] + blk['n_next_open_on_unfilled_cohort']} admitted signals "
                f"({blk['n_limit_unfilled']} never filled)")
            add(f"  every limit fill has a next-open leg: {blk['every_limit_fill_has_a_next_open_leg']}")
            add("  cell                                        | hor |     n | hit% net | mean gross | med gross |  mean net |   med net")
            add("  --------------------------------------------+-----+-------+----------+------------+-----------+-----------+----------")
            for cell, stats in blk["cells"].items():
                for k in sorted(stats["horizons"], key=int):
                    s = stats["horizons"][k]
                    hit = "-" if s["hit_rate_net"] is None else f"{s['hit_rate_net'] * 100:.1f}"
                    add(f"  {cell:<43} | T+{k:<2} | {s['n']:>5} | {hit:>8} | "
                        f"{_f(s['mean_gross'], 10)} | {_f(s['median_gross'], 9)} | "
                        f"{_f(s['mean_net'], 9)} | {_f(s['median_net'], 9)}")
            add("")
            add("  horizon | pooled gap | price eff | selectn eff | which effect carries the limit's advantage")
            add("  --------+------------+-----------+-------------+------------------------------------")
            for k in sorted(blk["effects"], key=int):
                e = blk["effects"][k]
                add(f"  T+{k:<5} | {_f(e['pooled_gap_median_net_pp'], 10)} | "
                    f"{_f(e['price_effect_median_net_pp'], 9)} | "
                    f"{_f(e['selection_effect_median_net_pp'], 11)} | {e['carried_by']}")
            add("")
            add("  by margin tercile (n per cell, matched):")
            add("    tercile                   | sig n | filled | fill% | hor | next-open matched med net | limit med net | price eff")
            add("    --------------------------+-------+--------+-------+-----+--------------------+------------+----------")
            for name in MARGIN_CELLS:
                cell = blk["by_margin_tercile"][name]
                frs = "-" if cell["fill_rate"] is None else f"{cell['fill_rate'] * 100:.1f}"
                v1c = cell[cohort_label(variant, COHORT_FILLED)]
                v2c = cell[f"{variant}_{COHORT_FILLED}"]
                for k in sorted(v2c["horizons"], key=int):
                    add(f"    {name:<25} | {cell['n_signals']:>5} | {cell['n_limit_filled']:>6} | "
                        f"{frs:>5} | T+{k:<2} | {_f(v1c['horizons'][k]['median_net'], 18)} | "
                        f"{_f(v2c['horizons'][k]['median_net'], 10)} | "
                        f"{_f(cell['price_effect_median_net_pp'][k], 9)}")
            add("")
            for k in sorted(blk["limit_beats_matched_next_open_in_every_margin_tercile"], key=int):
                held = blk["limit_beats_matched_next_open_in_every_margin_tercile"][k]
                verdict = ("NOT TESTABLE (an empty cell)" if held is None
                           else ("HOLDS" if held else "DOES NOT HOLD"))
                add(f"  T+{k}: 'beats next-open in EVERY margin tercile', ON THE MATCHED COHORT: {verdict}")
            add("")

    # ------------------------------------------------------------------ CPCV / deflation
    add("-" * 100)
    add(f"STEP 4 - CPCV + DEFLATED PROMOTION DECISION (N={m['trial_count_n']}, "
        f"fold_pass_min = {m['fold_pass_min']:.0%}; series keyed on the FILL day)")
    add("-" * 100)
    add("THE TWO GATES ARE BUILT ON DIFFERENT STATISTICS, deliberately reported together: the CPCV")
    add("fold series is a per-fill-day MEAN net / horizon (house convention), while the geometry")
    add("verdict is the MEDIAN net per trade. A tail-driven cell can pass CPCV while its TYPICAL")
    add("trade cannot pay the round trip, so a cell is reported promotable only when BOTH agree")
    add("(2026-09-12 reporting amendment). Any disagreement is printed as GATES DISAGREE.")
    add("")
    for variant in VARIANTS:
        block = doc["variants"][variant]
        add(f"[{variant}]")
        for k in sorted(block["cpcv"], key=int):
            c = block["cpcv"][k]
            add(f"  T+{k}: method={c['cv_method']}  obs_days={c['n_obs_days']}  splits={c['n_splits']}  "
                f"purge/embargo={c['purge_obs']}/{c['embargo_obs']} obs")
            fp = "-" if c["fold_pass_fraction"] is None else f"{c['fold_pass_fraction'] * 100:.1f}%"
            add(f"        fold_pass={fp} (need {c['fold_pass_min'] * 100:.0f}%)  "
                f"median_passing={c['median_passing_expectancy_pct_per_day']} %/day  "
                f"margin_floor={None if c['margin_floor_pct_per_day'] is None else round(c['margin_floor_pct_per_day'], 5)} %/day")
            add(f"        CPCV gate (mean-based)   : {c['promotable']}")
            add(f"        GEOMETRY gate (median)   : {c.get('geometry_verdict', 'unknown')} "
                f"(median net {_md_ret(c.get('median_net_pct'))})")
            add(f"        REPORTED PROMOTABLE      : {c.get('reported_promotable')}"
                + ("   *** GATES DISAGREE ***" if c.get("gates_disagree") else ""))
            for r in c["reasons"]:
                add(f"          - {r}")
            for r in c.get("reported_promotable_reasons", []):
                add(f"          - {r}")
        add("")

    # ------------------------------------------------------------------ the pre-registered decision
    d = doc["decision"]
    add("-" * 100)
    add("STEP 5 - THE PRE-REGISTERED DECISION RULE (fixed before the run; applied verbatim)")
    add("-" * 100)
    add(f"  RULE: {d['rule']}")
    medians = ", ".join(
        f"{name} {_md_ret(val)}" for name, val in d["median_net_pct_at_horizon"].items()
    )
    promotable = ", ".join(d["promotable_at_horizon"]) or "NONE"
    add(f"  median net at T+{d['horizon_sessions']}: {medians}")
    add(f"  CPCV-promotable at T+{d['horizon_sessions']}: {promotable}")
    add(f"  DECISION-RULE OUTCOME: {d['outcome']}")
    add(f"  shipped LIMIT-AT-LEVEL mechanism unchanged: {d['shipped_mechanism_unchanged']}")
    add("")
    t = doc.get("decision_under_tightened_reporting_rule")
    if t:
        add("  SAME RULE, TIGHTENED REPORTING GATE (2026-09-12 amendment: CPCV AND geometry viable):")
        add(f"    {t['amendment']}")
        t_prom = ", ".join(t["promotable_at_horizon"]) or "NONE"
        add(f"    reported-promotable at T+{t['horizon_sessions']}: {t_prom}")
        add(f"    DECISION-RULE OUTCOME UNDER THE TIGHTENED GATE: {t['outcome']}")
        add(f"    does the tightening CHANGE the registered outcome? "
            f"{t['changes_the_registered_outcome']}")
        add("")

    add("-" * 100)
    add("NOTES / CAVEATS (reported, not massaged)")
    add("-" * 100)
    cov = doc["corp_actions_coverage"]
    add(f"  structural corp_actions rows={cov['structural_rows']}, ex_date "
        f"{cov['structural_ex_date_min']} -> {cov['structural_ex_date_max']}; all kinds {cov['rows']}")
    for n in doc["notes"]:
        add(f"  * {n}")
    add("")
    return _ascii("\n".join(out))


def _md_pp(v: float | None) -> str:
    """A difference in percentage POINTS as a cell; em-dash on an honest ``None``."""
    return "—" if v is None else f"{v:+.4f} pp"


def _matched_cohort_markdown(doc: dict[str, Any]) -> str:
    """The STEP 3B decomposition as labelled MD cells (the cohorts differ from the pooled event sets)."""
    mc = doc.get("matched_cohorts")
    if not mc:
        return ""
    out: list[str] = []
    add = out.append
    add("## Matched-cohort decomposition — entry PRICE effect vs SELECTION effect")
    add("")
    add(f"{mc['definition']}")
    add("")
    for variant, blk in mc["variants"].items():
        n_sig = blk["n_next_open_on_filled_cohort"] + blk["n_next_open_on_unfilled_cohort"]
        add(f"### `{variant}` — filled {blk['n_limit_filled']} of {n_sig} admitted signals "
            f"({blk['n_limit_unfilled']} never filled)")
        add("")
        add("| cell | horizon | n | hit% net | mean gross | median gross | mean net | median net |")
        add("|---|---|---|---|---|---|---|---|")
        for cell, stats in blk["cells"].items():
            for k in sorted(stats["horizons"], key=int):
                s = stats["horizons"][k]
                add(f"| `{cell}` | T+{k} | {s['n']} | {_md_pct(s['hit_rate_net'])} | "
                    f"{_md_ret(s['mean_gross'])} | {_md_ret(s['median_gross'])} | "
                    f"{_md_ret(s['mean_net'])} | {_md_ret(s['median_net'])} |")
        add("")
        add("| horizon | pooled gap | PRICE effect | SELECTION effect | which effect carries the limit's "
            "advantage |")
        add("|---|---|---|---|---|")
        for k in sorted(blk["effects"], key=int):
            e = blk["effects"][k]
            add(f"| T+{k} | {_md_pp(e['pooled_gap_median_net_pp'])} | "
                f"{_md_pp(e['price_effect_median_net_pp'])} | "
                f"{_md_pp(e['selection_effect_median_net_pp'])} | {e['carried_by']} |")
        add("")
        add("Per margin tercile, matched (n per cell, and the cell's own fill rate):")
        add("")
        add("| margin tercile | signals | filled | fill rate | horizon | next-open median net on the matched "
            "cohort | limit median net | PRICE effect |")
        add("|---|---|---|---|---|---|---|---|")
        for name in MARGIN_CELLS:
            cell = blk["by_margin_tercile"][name]
            v1c = cell[cohort_label(variant, COHORT_FILLED)]
            v2c = cell[f"{variant}_{COHORT_FILLED}"]
            for k in sorted(v2c["horizons"], key=int):
                add(f"| `{name}` | {cell['n_signals']} | {cell['n_limit_filled']} | "
                    f"{_md_pct(cell['fill_rate'])} | T+{k} | "
                    f"{_md_ret(v1c['horizons'][k]['median_net'])} | "
                    f"{_md_ret(v2c['horizons'][k]['median_net'])} | "
                    f"{_md_pp(cell['price_effect_median_net_pp'][k])} |")
        add("")
        for k in sorted(blk["limit_beats_matched_next_open_in_every_margin_tercile"], key=int):
            held = blk["limit_beats_matched_next_open_in_every_margin_tercile"][k]
            verdict = ("**NOT TESTABLE** (an empty cell)" if held is None
                       else ("**HOLDS**" if held else "**DOES NOT HOLD**"))
            add(f"- T+{k}: \"beats next-open in every margin tercile\", evaluated ON THE MATCHED COHORT: "
                f"{verdict}")
        add("")
    return "\n".join(out)


def render_markdown(doc: dict[str, Any]) -> str:
    """The ``.md`` artifact: headline table and decision outcome up top, the full text report below."""
    m = doc["meta"]
    d = doc["decision"]
    out: list[str] = []
    add = out.append
    add("# brk20 entry-mechanism backtest (pre-registered 2026-09-12)")
    add("")
    add(f"*Generated {m['generated_at']} — window {m['window']['start']} → {m['window']['end']}, "
        f"{m['n_symbols_with_bars']} of {m['n_eligible_symbols']} eligible symbols, "
        f"{m['n_signals_after_vetoes']} fresh crosses past every veto / "
        f"{m['n_signals_admitted']} admitted. "
        f"Trial count N={m['trial_count_n']}, fold_pass_min {m['fold_pass_min']:.0%}. "
        f"Round trip {m['cost_round_trip_pct']:.4f}% {m['product']} at ₹{m['reference_notional_inr']}.*")
    add("")
    add("## Headline")
    add("")
    add("**The three variants share a SIGNAL population but NOT a trade set.** A limit fills only "
        "when price RETURNS to the broken level, so each limit variant trades a pullback-SELECTED "
        "subset of the admitted signals and the per-variant `n filled` below differ for that reason. "
        "Every pooled next-open vs limit number in this report is therefore a comparison PER FILLED TRADE "
        "across two different event sets — it is not a per-signal-originated comparison and it does "
        "not price the signals the resting limit forfeits. The matched-cohort decomposition below "
        "separates the entry-PRICE effect from the SELECTION effect; read it before quoting any gap.")
    add("")
    add("| variant | fill window | n filled | fill rate | median net T+5 | median net T+10 | "
        "median net T+20 | hit% net T+10 | CPCV fold pass T+10 | CPCV gate T+10 | geometry T+10 | "
        "reported promotable T+10 |")
    add("|---|---|---|---|---|---|---|---|---|---|---|---|")
    for variant in VARIANTS:
        b = doc["variants"][variant]
        cells = b["cells"][CELL_ALL]["horizons"]
        c10 = b["cpcv"][str(DECISION_HORIZON)]
        win = b["fill_window_sessions"]
        window = "market" if win is None else f"{win} sessions"
        fill_rate = _md_pct(b["fill_rate"])
        hit = _md_pct(cells[str(DECISION_HORIZON)]["hit_rate_net"])
        fold = _md_pct(c10["fold_pass_fraction"])
        medians = " | ".join(_md_ret(cells[str(k)]["median_net"]) for k in HORIZONS)
        geo10 = c10.get("geometry_verdict", "unknown")
        rep10 = c10.get("reported_promotable")
        add(f"| `{variant}` | {window} | {b['n_filled']} | {fill_rate} | {medians} | {hit} "
            f"| {fold} | {c10['promotable']} | {geo10} | **{rep10}** |")
    add("")
    add(f"*Reporting rule: {REPORTING_RULE_AMENDMENT}*")
    add("")
    add("## Decision rule (fixed before the run, applied verbatim)")
    add("")
    add(f"> {d['rule']}")
    add("")
    promotable = ", ".join(f"`{n}`" for n in d["promotable_at_horizon"]) or "**none**"
    add(f"**Outcome under the registered rule: `{d['outcome']}`.** CPCV-promotable at "
        f"T+{d['horizon_sessions']}: {promotable}. Shipped LIMIT-AT-LEVEL mechanism unchanged: "
        f"**{d['shipped_mechanism_unchanged']}**.")
    add("")
    t = doc.get("decision_under_tightened_reporting_rule")
    if t:
        t_prom = ", ".join(f"`{n}`" for n in t["promotable_at_horizon"]) or "**none**"
        add(f"**Outcome under the 2026-09-12 tightened reporting gate: `{t['outcome']}`.** "
            f"Reported-promotable at T+{t['horizon_sessions']}: {t_prom}. "
            f"Does the tightening change the registered outcome? "
            f"**{t['changes_the_registered_outcome']}**.")
        add("")
    add(_matched_cohort_markdown(doc))
    add("## Full report")
    add("")
    add("```")
    add(render_text(doc))
    add("```")
    add("")
    return "\n".join(out)


# =============================================================================== CLI
def _date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _default_db() -> Path:
    return repo_root() / "data" / "market.duckdb"


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog="backtest_brk20",
        description=(
            "brk20 entry-mechanism backtest (three pre-registered entry variants, no sweep). "
            "Read-only against bars_1d/universe_daily; refuses to run if the DuckDB file is missing "
            "or locked by the engine."
        ),
    )
    ap.add_argument("--db", type=Path, default=_default_db(), help="path to market.duckdb")
    ap.add_argument("--start", type=_date, default=date(2000, 1, 1),
                    help="window start (YYYY-MM-DD); the default reaches before bars_1d begins, so "
                         "the registered window is the FULL stored history")
    ap.add_argument("--end", type=_date, default=date.today(), help="window end (YYYY-MM-DD)")
    ap.add_argument("--out", type=Path, default=None,
                    help="JSON results path (the .md report is written beside it, same stem)")
    ap.add_argument("--notional", type=Decimal, default=REFERENCE_NOTIONAL,
                    help="reference per-trade notional the round-trip cost is quoted at")
    # Overrides the pre-registered population; a run using it is stamped as a smoke run.
    ap.add_argument("--symbols", default=None,
                    help="comma-separated symbol subset (SMOKE RUNS ONLY - overrides the "
                         "pre-registered eligible-universe population)")
    ap.add_argument("--max-symbols", type=int, default=None, help="cap the symbol count (smoke runs)")
    ap.add_argument("--verify-window", type=int, default=0, metavar="K",
                    help="re-scan the first K symbols with the full row prefix and abort on any "
                         "disagreement with the bounded scan window")
    return ap


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    db_path = Path(args.db)
    try:
        conn = open_readonly(db_path)
    except DbUnopenable as exc:
        print(f"backtest_brk20: REFUSING TO RUN.\n  {_ascii(str(exc))}", file=sys.stderr)
        return 2

    out_path = args.out
    if out_path is None:
        out_path = db_path.parent / "reports" / f"backtest_brk20_{date.today().isoformat()}.json"
    out_path = Path(out_path)

    try:
        doc, _trades = run_study(
            conn,
            start=args.start,
            end=args.end,
            cost_model=CostModel.from_config(),
            notional=Decimal(args.notional),
            symbols=[s.strip() for s in args.symbols.split(",") if s.strip()] if args.symbols else None,
            max_symbols=args.max_symbols,
            verify_window=args.verify_window,
        )
    except ValueError as exc:
        print(f"backtest_brk20: {exc}", file=sys.stderr)
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
