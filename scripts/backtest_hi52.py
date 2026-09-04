#!/usr/bin/env python
"""hi52 PRE-REGISTERED backtest harness (IMPLEMENTATION_PLAN.md `hi52` addendum, 2026-09-01).

This is the §8.6 owner-gate input for the ``hi52`` SHADOW rule. It is an **event study**, not a
portfolio simulator (see "Sizing" below), run over the full-market ``bars_1d`` bhavcopy history with
the engine idle.

===============================================================================================
PRE-REGISTRATION / MULTIPLICITY DISCIPLINE — READ FIRST
===============================================================================================
**NO PARAMETER SWEEP IS RUN BY THIS SCRIPT, AND NONE WAS RUN BEFORE IT.** Exactly ONE parameter
set is evaluated — :data:`PRE_REGISTERED_PARAMS`, byte-identical to the shadow rule's frozen
``hi52.DEFAULT_PARAMS`` (``proximity_min`` 0.95, ``lookback_sessions`` 252, ``min_sessions`` 126,
``vol_mult`` 1.0, ``ex_skip_days`` 10, ``stop_pct`` 6.0). There is no grid, no ``--proximity-min``
flag, no optimizer and no "best of" selection anywhere in this module: the trial count cited to the
§6.4 deflation machinery is therefore **N = 1** (:data:`TRIAL_COUNT_N`), which is the honest count,
and ``fold_pass_min(1) = 60%``. Adding a knob that changes the signal definition would silently
convert N=1 into N=k and invalidate every deflated number this script prints — if the rule is to be
re-parameterised, that is a NEW pre-registration, not a flag on this one.

The two reported constructs are NOT two trials of one hypothesis: they are the two DIFFERENT
constructs the plan block names (the platform's discrete trigger, and the George & Hwang academic
continuous rank), each reported separately and never pooled or max-picked.

===============================================================================================
CONVENTIONS (pinned; every one of them is a choice this docstring is obliged to state)
===============================================================================================
* **Signal (a) — discrete fresh cross.** ``engine.strategy.scanners.hi52.scan_daily`` is IMPORTED
  and called; the crossing arithmetic is never re-implemented here (the plan's "live code MUST
  consume the same crossing function" discipline, inherited from the ``ins`` leg). Rows are
  :class:`engine.strategy.scanners.brk20.DailyRow`, exactly as the live sweep builds them.
  For tractability over ~2000 symbols x ~1650 sessions the script first computes a VECTORIZED
  rolling-max proximity index and only calls ``scan_daily`` on the days that index says could be a
  fresh cross. That index is an *exact* restatement of ``hi52._proximity``'s window
  (``close[i] / max(high[i-251..i])``, short window at the series head, and the previous session's
  own window is the same array at ``i-1``) — it decides only WHICH days are offered to
  ``scan_daily``, never whether a signal exists, what its levels are, or whether volume/ex-date
  confirm. ``--verify-prefilter K`` brute-forces every eligible day for the first K symbols and
  aborts on any disagreement; the unit test asserts the same equality on synthetic data.
* **Signal (b) — continuous rank.** At each MONTHLY rebalance (the last trading session of each
  calendar month in the window) every symbol with >= ``min_sessions`` history is scored with the
  SAME ``hi52._proximity`` function, ranked cross-sectionally, and the TOP DECILE is taken long.
  The bottom decile and the top-minus-bottom spread are also printed as the academic reference
  number — the short leg is NOT executable in NSE cash (CNC cannot short overnight) and is labelled
  as such wherever it appears.
* **Entry.** The NEXT session's OPEN after the signal session. No same-bar fill anywhere: every
  input to a signal is strictly before the decision timestamp, and the first price the study can
  book is the first price an order placed after that close could actually reach.
* **Exit.** The CLOSE of the horizon session — ``return = close(T+k) / open(T+1) - 1``. This is the
  convention of record in this repo: ``scripts/event_study.py`` (WO-16 ``next_open`` default, the
  ``ins``/filings study that produced the promoted +0.73%/+1.58% T+10/T+20 numbers) measures exactly
  ``close_(T+k) / open_(T+1)``. Horizons are TRADING SESSIONS: T+5, T+10, T+20.
* **Event admission.** A signal is measured only if all of ``T+1 .. T+20`` exist in that symbol's
  own bar series; a signal short of the LONGEST horizon is dropped entirely rather than measured at
  the short horizons only, so T+5 / T+10 / T+20 are always quoted on the SAME event set and are
  therefore comparable (``event_study.measure_directional`` drops on the same rule).
* **Costs.** ONE full round trip per trade, both legs, from the repo's single source of truth:
  ``CostModel.breakeven_pct(notional, "CNC")`` — statutory fees PLUS the measured bid-ask spread
  (WO-2), charged at the ``--notional`` reference size (default Rs 20,000, the repo's calibration
  size). CNC/delivery, never MIS: hi52 is a 5-20 session swing rule and cash equities cannot be held
  overnight under MIS. ``breakeven_pct`` (not ``fee_breakeven_pct``) is deliberate — the fees-only
  view is the contract-note anchor, not a viability number, and spread is not modelled separately
  anywhere here so there is nothing to double-count.
* **Sizing.** Per-trade EQUAL NOTIONAL, returns reported in PERCENT. No compounding, no portfolio
  construction, no position interaction, no capital constraint, no per-day cap on concurrent
  signals. This is an event study measuring the drift after a signal; it is NOT a claim that the
  full population is simultaneously tradeable at the owner's capital.
* **Splits (all mandatory, all reported per cell — the pooled number is never the only number):**
  - *smooth vs jumpy* (Frog-in-the-Pan). From ``hi52.diagnostics_for`` on the signal-day window.
    EXACT CUT: **smooth iff ``up_day_frac >= median(up_day_frac)`` AND
    ``max_day_move <= median(max_day_move)``**, both medians taken over THAT CONSTRUCT's own
    measured signal population; everything else is *jumpy*. The cut is a two-sided AND, so "smooth"
    is a minority cell (~25% under independence) and "jumpy" is its complement, not a mirror image.
    Honest caveat: the medians are full-sample statistics, so the split is DESCRIPTIVE — a live rule
    could not have known them at signal time. It is not a tradeable filter and is not reported as
    one.
  - *index members vs extended names*. Index membership as of the SIGNAL DATE is not stored
    anywhere in this platform, so this split uses the **CURRENT** membership list (the runtime cache
    ``<data>/universe/index_cached.csv``, falling back to the committed
    ``config/universe/nifty500_seed.csv``) applied backwards over the whole window. That is a
    **SURVIVORSHIP-TAINTED PROXY**: a name that entered the index after a big run appears as an
    "index member" throughout its climb, and a name that was dropped is mislabelled the other way.
    Every rendering of this split carries the label; the JSON carries
    ``index_split_is_survivorship_tainted_proxy: true``.
  - *news-gap-day A/B*. A gap proxy for discrete-information days (Frog-in-the-Pan): a signal whose
    TRIGGER day moved ``|close/prev_close - 1| > 5%`` is a gap day. Cell A = every signal; cell B =
    gap days EXCLUDED; the gap-only cell is printed too so the excluded cohort is visible rather
    than merely subtracted.
* **Validation.** CPCV through the repo's own machinery — ``engine.learning.validate.cpcv_splits``
  (skfolio ``CombinatorialPurgedCV``, 6 folds / 2 test folds) — over a per-signal-day series of
  cost-adjusted returns: day *t*'s observation is the mean NET %-return of the trades signalled on
  *t*, divided by the horizon in sessions, i.e. a per-session net return directly comparable with
  the WO-3 margin floor. Purge and embargo are raised from the §6.4 default 5 to **the horizon
  itself** (5/10/20 observations) because an event study at horizon N has N sessions of overlap
  between neighbouring signals; leaving them at 5 would leak a T+20 trade across the fold boundary.
  Deflation/promotion is the repo's ``promotion_decision`` with ``n=1`` and the WO-3 margin floor,
  with ``margin_floor_days`` set to the horizon (the floor's own derivation: "a position must, over
  the horizon it is ACTUALLY held, earn at least the one round trip it costs to hold it"). If
  skfolio is unavailable or the split is degenerate the script falls back to a self-implemented
  purged K-fold with embargo and says so in both the text report and the JSON (``cv_method``).
* **The ORB-lesson arithmetic check runs FIRST.** Before any signal-quality statistic is printed,
  the report states the median per-trade GROSS drift at each horizon against the round-trip cost
  floor and emits a hard ``GEOMETRY: viable/dead at T+N`` verdict line. ORB was killed twice by cost
  geometry, not by signal quality; the geometry line exists so nobody reads a hit rate before
  reading whether the trade can pay for itself.

===============================================================================================
DATA ACCESS
===============================================================================================
Read-only, always. ``MarketStore.open()`` runs its schema DDL and is therefore a WRITER, so this
script does what ``scripts/g1_entity_sample.py`` does: attaches DuckDB directly with
``read_only=True`` and issues its own SELECTs against ``bars_1d`` / ``corp_actions``. DuckDB still
takes a file lock, so the engine service must be idle/stopped; if the file is missing, locked or
otherwise unopenable the script REFUSES with an explicit message and exit code 2 rather than
degrading. Nothing in this module ever writes to the store.

Universe = the full market: ``bars_1d`` carries ``src='bhavcopy'`` rows for every NSE EQ symbol and
``src='kite_official'`` rows for the ~200 index names; the primary key is (symbol, d), so one row
per symbol-session regardless of provenance and no de-duplication is needed.

A full-market run holds every symbol's history in memory (float arrays plus the ``DailyRow`` view
``scan_daily`` consumes) — order 0.5-1 GB for ~2000 symbols x ~6 years. ``--max-symbols`` and
``--symbols`` are the escape hatches for a constrained box or a smoke run; neither changes any
convention, only the population.

Exit codes: 0 = ran; 2 = DB unopenable / no bars in window.
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

import engine  # noqa: E402,F401,I001  native import-order guard: sklearn's OpenMP runtime MUST be
#                                      established before skfolio/cvxpy loads (engine._preload). The
#                                      isort suppression keeps it FIRST; re-sorting would sink it
#                                      below the third-party block and re-open the segfault.
import duckdb  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from engine.core.config import repo_root  # noqa: E402
from engine.learning.validate import (  # noqa: E402
    cpcv_splits,
    fold_pass_min,
    margin_floor_pct_per_day,
    promotion_decision,
)
from engine.strategy.cost_model import CostModel  # noqa: E402
from engine.strategy.scanners.brk20 import DailyRow  # noqa: E402
from engine.strategy.scanners.hi52 import (  # noqa: E402
    DEFAULT_PARAMS,
    STRATEGY_ID,
    UNADJUSTED_KINDS,
    VETO_UNADJUSTED_HISTORY,
    _bump,       # the live veto accumulator, so a count here means what the live sweep's count means
    _proximity,  # the ONE proximity definition — imported, never copied (plan: one crossing function)
    diagnostics_for,
    scan_daily,
)
from engine.universe.builder import parse_index_constituents_csv  # noqa: E402

# =============================================================================== pinned constants
#: The ONE pre-registered parameter set. Frozen copy of the shadow rule's own DEFAULT_PARAMS; the
#: assertion below makes a silent drift between this study and the live rule impossible.
PRE_REGISTERED_PARAMS: dict[str, float] = dict(DEFAULT_PARAMS)

#: Trial count cited to the §6.4 deflation machinery. ONE parameter set, no sweep (see the module
#: docstring). Not a knob.
TRIAL_COUNT_N = 1

HORIZONS: tuple[int, ...] = (5, 10, 20)          # trading sessions
GAP_DAY_PCT = 5.0                                 # |close/prev_close - 1| > 5% on the trigger day
REFERENCE_NOTIONAL = Decimal("20000")             # repo cost-calibration size (§6.4/§7.1)
PRODUCT = "CNC"                                   # delivery/swing — NEVER MIS (overnight holds)
RANK_TOP_DECILE = 0.10

#: Trailing CALENDAR-day window of the unadjusted-history veto. Source of truth is the live sweep's
#: own history window, ``hi52_start = today - timedelta(days=400)`` (engine/ops/main.py window_open).
UNADJUSTED_LOOKBACK_DAYS = 400

CONSTRUCT_DISCRETE = "discrete_fresh_cross"
CONSTRUCT_RANK_TOP = "rank_top_decile"
CONSTRUCT_RANK_BOTTOM = "rank_bottom_decile_reference"

CELL_ALL = "all"
CELL_SMOOTH = "smooth_approach"
CELL_JUMPY = "jumpy_approach"
CELL_INDEX = "index_member_proxy"
CELL_EXTENDED = "extended_non_index_proxy"
CELL_NO_GAP = "gap_days_excluded"
CELL_GAP_ONLY = "gap_days_only"

_CV_SKFOLIO = "cpcv_skfolio_CombinatorialPurgedCV"
_CV_FALLBACK = "purged_kfold_with_embargo_fallback"


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
class Trade:
    """One measured signal. ``gross``/``net`` are PERCENT returns per horizon, equal notional."""

    symbol: str
    construct: str
    signal_date: date
    entry_date: date
    entry_px: float
    prox: float
    score: float | None
    up_day_frac: float
    max_day_move: float
    gap_day: bool
    in_index_proxy: bool
    gross: dict[int, float] = field(default_factory=dict)
    net: dict[int, float] = field(default_factory=dict)


# =============================================================================== read-only DB access
class DbUnopenable(RuntimeError):
    """The DuckDB file is missing, locked by the engine, or otherwise not readable."""


def open_readonly(db_path: Path) -> duckdb.DuckDBPyConnection:
    """Attach ``db_path`` READ-ONLY, or raise :class:`DbUnopenable` with an actionable message.

    ``MarketStore.open()`` runs schema DDL and is a writer, so it is deliberately not used here
    (same reasoning, same pattern as ``scripts/g1_entity_sample.py``). DuckDB takes a file lock even
    for a read-only attach, so a running engine makes this fail — which is exactly the refusal the
    plan asks for, rather than a half-run against a moving store.
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


def load_series(
    conn: duckdb.DuckDBPyConnection,
    start: date,
    end: date,
    *,
    symbols: Sequence[str] | None = None,
    max_symbols: int | None = None,
) -> dict[str, Series]:
    """Every symbol's ascending ``bars_1d`` history in ``[start, end]``.

    Full market by construction: bhavcopy rows exist for every NSE EQ symbol and the (symbol, d)
    primary key means one row per symbol-session, so no src filter and no de-duplication.
    """
    sql = (
        'SELECT symbol, d, "open", high, low, "close", volume FROM bars_1d '
        "WHERE d >= ? AND d <= ? ORDER BY symbol, d"
    )
    frame = conn.execute(sql, [start, end]).df()
    if frame.empty:
        return {}
    out: dict[str, Series] = {}
    wanted = {s.strip().upper() for s in symbols} if symbols else None
    for symbol, grp in frame.groupby("symbol", sort=True):
        sym = str(symbol)
        if wanted is not None and sym.upper() not in wanted:
            continue
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

    The kind test is the live one (``hi52.unadjusted_history``): exact membership on the stored
    ``kind``, never case-folded or fuzzy, so this study vetoes the same rows the sweep vetoes.
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

    The veto reaches exactly as far as the STRUCTURAL rows do — the table is dividend-dominated, so
    an all-kinds count says only whether it was backfilled at all — and an empty or short structural
    span silently disables it, so both are reported next to the counts rather than assumed.
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


def load_index_members(db_path: Path, override: Path | None = None) -> tuple[set[str], str]:
    """CURRENT index membership + its provenance string (a survivorship-tainted proxy, see above).

    Ladder: explicit ``--index-csv`` (alias ``--nifty200-csv``) -> the runtime cache beside the DB
    (``<data>/universe/index_cached.csv``) -> the committed seed. Both file names, and the seed's
    index, changed with O15 (2026-09-04) when the eligible index became config (NIFTY 500);
    a run against an older DB tree simply falls through to the committed seed. Parsing reuses the
    universe builder's own ``parse_index_constituents_csv`` (one CSV definition, never a copy).
    """
    candidates: list[tuple[Path, str]] = []
    if override is not None:
        candidates.append((Path(override), "explicit --index-csv"))
    candidates.append((Path(db_path).parent / "universe" / "index_cached.csv", "runtime cache"))
    candidates.append((repo_root() / "config" / "universe" / "nifty500_seed.csv", "committed seed"))
    for path, label in candidates:
        try:
            if not path.exists():
                continue
            members = parse_index_constituents_csv(path.read_text(encoding="utf-8"))
            if members:
                return {m.upper() for m in members}, f"{label} ({path})"
        except (OSError, ValueError):
            continue
    return set(), "UNAVAILABLE (index/extended split not computable)"


# =============================================================================== signal generation
def _rolling_max(vals: np.ndarray, window: int) -> np.ndarray:
    """Trailing max over ``min(window, i+1)`` values ending at each ``i`` — the exact window
    ``hi52._proximity`` takes its high from (``rows[-min(lookback, len(rows)):]``)."""
    return pd.Series(vals).rolling(window=window, min_periods=1).max().to_numpy()


def prefilter_cross_indices(series: Series, params: dict[str, float]) -> list[int]:
    """Indices that COULD be a fresh cross — the vectorized narrowing described in the docstring.

    Exactly restates ``_proximity``'s window arithmetic: ``prox[i] = close[i] / rolling_max(high)[i]``
    and the previous session's own-window proximity is that same array at ``i-1``. It never decides a
    signal; ``scan_daily`` does, on every index returned here.
    """
    n = len(series)
    min_sessions = int(params["min_sessions"])
    lookback = int(params["lookback_sessions"])
    pmin = float(params["proximity_min"])
    if n < min_sessions or min_sessions < 2 or lookback < 1:
        return []
    hi = _rolling_max(series.high, lookback)
    with np.errstate(divide="ignore", invalid="ignore"):
        prox = np.where(hi > 0.0, series.close / hi, np.nan)
    out: list[int] = []
    for i in range(min_sessions - 1, n):
        cur, prev = prox[i], prox[i - 1]
        if math.isfinite(cur) and cur >= pmin and (not math.isfinite(prev) or prev < pmin):
            out.append(i)
    return out


def _window(series: Series, i: int, lookback: int) -> list[DailyRow]:
    """The trailing ``min(lookback, i+1)`` rows ending at ``i``.

    Passing this instead of ``rows[:i+1]`` is an EXACT equivalence for both ``scan_daily`` and
    ``_proximity``, not an approximation: both take ``rows[-min(lookback, len(rows)):]`` for the
    high window, ``rows[:-1][-20:]`` for the volume window and ``rows[:-1]`` (again windowed) for
    the fresh-cross check, and ``min_sessions`` (126) <= ``lookback`` (252) so the length gate lands
    identically. It exists purely so a 1600-row history is not re-sliced 1600 times.
    """
    return series.rows[max(0, i + 1 - lookback) : i + 1]


def _unadjusted_at(structural: Sequence[date], today: date) -> bool:
    """Whether a rescaling ex-date falls in ``[today - UNADJUSTED_LOOKBACK_DAYS, today - 1]``.

    ``structural`` must be ascending (:func:`load_structural_ex_dates` sorts). ``today`` itself is
    OUT of range: on the ex-date every bar the window holds is still pre-ex, in one unit — that day
    is the upcoming-ex skip's case, not this one.
    """
    if not structural:
        return False
    j = bisect_left(structural, today - timedelta(days=UNADJUSTED_LOOKBACK_DAYS))
    return j < len(structural) and structural[j] < today


def discrete_signals(
    series: Series,
    params: dict[str, float],
    ex_dates: Sequence[date],
    structural_ex_dates: Sequence[date] = (),
    *,
    exhaustive: bool = False,
    veto_counts: dict[str, int] | None = None,
) -> list[tuple[int, float]]:
    """``(signal index, score)`` for every hi52 fresh cross in this symbol's history.

    ``exhaustive`` bypasses the prefilter and offers EVERY eligible day to ``scan_daily`` — the
    ``--verify-prefilter`` path (and the unit test's equality assertion).

    ``structural_ex_dates`` applies the live sweep's unadjusted-history veto (:func:`_unadjusted_at`)
    to the decision day, so the measured population is the one the shadow can actually originate.
    Counted on ``veto_counts`` when one is supplied; ``None`` keeps this function pure.
    """
    n = len(series)
    min_sessions = int(params["min_sessions"])
    lookback = int(params["lookback_sessions"])
    idxs = range(min_sessions - 1, n) if exhaustive else prefilter_cross_indices(series, params)
    out: list[tuple[int, float]] = []
    for i in idxs:
        # `today` = the session on which the sweep would run and the order would be placed, i.e. the
        # entry session. The last bar in the window is `y`, the last COMPLETED session, as the live
        # rule requires.
        today = series.dates[i + 1] if i + 1 < n else series.dates[i]
        cand = scan_daily(
            series.symbol,
            _window(series, i, lookback),
            today=today,
            upcoming_ex_dates=ex_dates,
            params=params,
        )
        if cand is not None:
            if _unadjusted_at(structural_ex_dates, today):
                _bump(veto_counts, VETO_UNADJUSTED_HISTORY)
                continue
            out.append((i, float(cand.score)))
    return out


def month_end_indices(dates: Sequence[date]) -> list[int]:
    """Index of the LAST session of each calendar month present in ``dates`` (ascending)."""
    last: dict[tuple[int, int], int] = {}
    for i, d in enumerate(dates):
        last[(d.year, d.month)] = i
    return sorted(last.values())


# =============================================================================== measurement
def measure(
    series: Series,
    i: int,
    *,
    construct: str,
    prox: float,
    score: float | None,
    cost_pct: float,
    index_members: set[str],
    params: dict[str, float],
    horizons: Sequence[int] = HORIZONS,
) -> Trade | None:
    """Measure one signal at index ``i``: entry ``open(i+1)``, exits ``close(i+k)``, minus one
    round trip. ``None`` when the signal is not measurable at the LONGEST horizon (dropped whole, so
    every horizon is quoted on the same event set) or the entry price is unusable.
    """
    n = len(series)
    kmax = max(horizons)
    if i + kmax >= n:
        return None
    entry = float(series.open[i + 1])
    if not math.isfinite(entry) or entry <= 0.0:
        return None
    prev_close = float(series.close[i - 1]) if i >= 1 else float("nan")
    cur_close = float(series.close[i])
    gap_day = bool(
        math.isfinite(prev_close)
        and prev_close > 0.0
        and abs(cur_close / prev_close - 1.0) * 100.0 > GAP_DAY_PCT
    )
    diag = diagnostics_for(_window(series, i, int(params["lookback_sessions"])), params=params)
    trade = Trade(
        symbol=series.symbol,
        construct=construct,
        signal_date=series.dates[i],
        entry_date=series.dates[i + 1],
        entry_px=entry,
        prox=float(prox),
        score=score,
        up_day_frac=float(diag.up_day_frac) if diag is not None else float("nan"),
        max_day_move=float(diag.max_day_move) if diag is not None else float("nan"),
        gap_day=gap_day,
        in_index_proxy=series.symbol.upper() in index_members,
    )
    for k in horizons:
        exit_px = float(series.close[i + k])
        if not math.isfinite(exit_px) or exit_px <= 0.0:
            return None
        gross = (exit_px / entry - 1.0) * 100.0
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


def smoothness_cut(trades: Sequence[Trade]) -> tuple[float | None, float | None]:
    """The population medians defining the smooth/jumpy cut (see the docstring's EXACT CUT)."""
    ups = [t.up_day_frac for t in trades if math.isfinite(t.up_day_frac)]
    moves = [t.max_day_move for t in trades if math.isfinite(t.max_day_move)]
    return (
        statistics.median(ups) if ups else None,
        statistics.median(moves) if moves else None,
    )


def is_smooth(trade: Trade, up_med: float | None, move_med: float | None) -> bool:
    if up_med is None or move_med is None:
        return False
    if not (math.isfinite(trade.up_day_frac) and math.isfinite(trade.max_day_move)):
        return False
    return trade.up_day_frac >= up_med and trade.max_day_move <= move_med


def split_cells(trades: Sequence[Trade]) -> dict[str, list[Trade]]:
    """The three mandatory splits, as named cells. Every cell is reported separately."""
    up_med, move_med = smoothness_cut(trades)
    smooth = [t for t in trades if is_smooth(t, up_med, move_med)]
    jumpy = [t for t in trades if not is_smooth(t, up_med, move_med)]
    return {
        CELL_ALL: list(trades),
        CELL_SMOOTH: smooth,
        CELL_JUMPY: jumpy,
        CELL_INDEX: [t for t in trades if t.in_index_proxy],
        CELL_EXTENDED: [t for t in trades if not t.in_index_proxy],
        CELL_NO_GAP: [t for t in trades if not t.gap_day],
        CELL_GAP_ONLY: [t for t in trades if t.gap_day],
    }


# =============================================================================== geometry (ORB lesson)
def geometry(trades: Sequence[Trade], cost_pct: float, horizons: Sequence[int] = HORIZONS) -> dict[str, Any]:
    """Median per-trade GROSS drift vs the round-trip cost floor, per horizon, with a hard verdict.

    ``viable`` iff the MEDIAN gross drift exceeds one full round trip. Median, not mean: ORB's mean
    was repeatedly dragged around by a handful of tails while the typical trade never covered its
    costs, and the question this line answers is whether the TYPICAL trade can pay the toll.
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


# =============================================================================== CPCV / deflation
def _purged_kfold_splits(
    n_obs: int, *, n_folds: int = 6, purge: int = 5, embargo: int = 5
) -> list[tuple[np.ndarray, np.ndarray]]:
    """Fallback for when skfolio is unavailable or its precondition cannot be met: contiguous K-fold
    test blocks with the same purge/embargo guarantee ``cpcv_splits`` enforces (train observations
    within ``purge`` before or ``embargo`` after any test block are dropped). Not combinatorial —
    K folds, not C(K, 2) splits — and labelled as such wherever it is used."""
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


def daily_net_series(trades: Sequence[Trade], horizon: int) -> tuple[list[date], np.ndarray]:
    """Per-SIGNAL-DAY cost-adjusted return series for one horizon.

    Day *t* = mean NET %-return of the trades signalled on *t*, divided by ``horizon`` sessions, so
    the value is a per-session net return in percent — the same units the WO-3 margin floor
    (%/day) and ``ValidationPipeline``'s cost-adjusted daily series are quoted in.
    """
    by_day: dict[date, list[float]] = defaultdict(list)
    for t in trades:
        if horizon in t.net:
            by_day[t.signal_date].append(t.net[horizon])
    days = sorted(by_day)
    vals = np.array([statistics.fmean(by_day[d]) / float(horizon) for d in days], dtype="float64")
    return days, vals


def cpcv_report(trades: Sequence[Trade], horizon: int, cost_pct: float) -> dict[str, Any]:
    """CPCV + the §6.4/WO-3 deflated promotion decision for one construct at one horizon.

    Purge and embargo are the HORIZON, not the §6.4 default 5: at horizon N a signal's trade overlaps
    the next N sessions of signals, so a 5-observation purge leaks a T+20 trade straight across the
    fold boundary.
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
        TRIAL_COUNT_N,
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
        "trial_count_n": TRIAL_COUNT_N,
        "fold_pass_min": fold_pass_min(TRIAL_COUNT_N),
        "n_splits": len(folds),
        "fold_pass_fraction": None if pass_fraction is None else round(pass_fraction, 4),
        "median_passing_expectancy_pct_per_day": None if median_passing is None else round(median_passing, 6),
        "margin_floor_pct_per_day": margin_floor_pct_per_day(cost_pct, margin_floor_days=horizon),
        "promotable": promotable,
        "reasons": reasons,
        "folds": folds,
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
    nifty200_csv: Path | None = None,
    db_path: Path | None = None,
    verify_prefilter: int = 0,
    params: dict[str, float] | None = None,
) -> tuple[dict[str, Any], dict[str, list[Trade]]]:
    """Run both constructs end to end.

    Returns ``(document, trades_by_construct)`` — the document is the JSON payload and the text
    report's input; the per-trade lists are deliberately NOT embedded in it (a full-market run books
    tens of thousands of trades) but are returned so callers and tests can assert on individual
    fills without re-deriving them.
    """
    p = dict(params or PRE_REGISTERED_PARAMS)
    cost_pct = float(cost_model.breakeven_pct(notional, PRODUCT))
    fees_pct = float(cost_model.fee_breakeven_pct(notional, PRODUCT))
    spread_pct = float(cost_model.spread_pct)

    series_by_symbol = load_series(conn, start, end, symbols=symbols, max_symbols=max_symbols)
    if not series_by_symbol:
        raise ValueError("no bars_1d rows in the requested window")
    ex_dates = load_ex_dates(conn)
    structural = load_structural_ex_dates(conn)
    coverage = corp_actions_coverage(conn)
    index_members, index_source = load_index_members(
        Path(db_path) if db_path is not None else repo_root() / "data" / "market.duckdb",
        nifty200_csv,
    )
    notes: list[str] = []

    # ---------------------------------------------------------------- (a) discrete fresh cross
    discrete: list[Trade] = []
    n_signals_discrete = 0
    discrete_vetoes: dict[str, int] = {}
    verify_left = int(verify_prefilter)
    verify_checked = 0
    for sym in sorted(series_by_symbol):
        s = series_by_symbol[sym]
        xd = ex_dates.get(sym, [])
        sx = structural.get(sym, [])
        sigs = discrete_signals(s, p, xd, sx, veto_counts=discrete_vetoes)
        if verify_left > 0:
            exhaustive = discrete_signals(s, p, xd, sx, exhaustive=True)
            if [i for i, _ in exhaustive] != [i for i, _ in sigs]:
                raise AssertionError(
                    f"prefilter/scan_daily disagreement on {sym}: "
                    f"prefiltered={[i for i, _ in sigs]} exhaustive={[i for i, _ in exhaustive]}"
                )
            verify_left -= 1
            verify_checked += 1
        n_signals_discrete += len(sigs)
        for i, score in sigs:
            prox_hi = _proximity(_window(s, i, int(p["lookback_sessions"])), lookback=int(p["lookback_sessions"]))
            t = measure(
                s, i, construct=CONSTRUCT_DISCRETE,
                prox=prox_hi[0] if prox_hi else float("nan"), score=score,
                cost_pct=cost_pct, index_members=index_members, params=p,
            )
            if t is not None:
                discrete.append(t)

    # ---------------------------------------------------------------- (b) monthly cross-sectional rank
    all_dates = sorted({d for s in series_by_symbol.values() for d in s.dates})
    rebalances = [all_dates[i] for i in month_end_indices(all_dates)]
    pos: dict[str, dict[date, int]] = {
        sym: {d: i for i, d in enumerate(s.dates)} for sym, s in series_by_symbol.items()
    }
    rank_top: list[Trade] = []
    rank_bottom: list[Trade] = []
    rank_vetoes: dict[str, int] = {}
    n_rebalances_used = 0
    lookback = int(p["lookback_sessions"])
    min_sessions = int(p["min_sessions"])
    for d in rebalances:
        scored: list[tuple[float, str, int]] = []
        for sym, s in series_by_symbol.items():
            i = pos[sym].get(d)
            if i is None or i + 1 < min_sessions:
                continue
            # The rank window ENDS at d (the discrete window ends at y = d - 1), so an ex-date ON d
            # already puts a post-ex close under a pre-ex high: the taint range here is [d-400, d].
            if _unadjusted_at(structural.get(sym, []), d + timedelta(days=1)):
                _bump(rank_vetoes, VETO_UNADJUSTED_HISTORY)
                continue
            prox_hi = _proximity(_window(s, i, lookback), lookback=lookback)
            if prox_hi is None:
                continue
            scored.append((prox_hi[0], sym, i))
        if len(scored) < 10:                       # a decile of fewer than 10 names is not a decile
            continue
        n_rebalances_used += 1
        scored.sort(key=lambda x: (-x[0], x[1]))   # deterministic: prox desc, symbol asc (§9.6)
        cut = max(1, int(math.ceil(len(scored) * RANK_TOP_DECILE)))
        for bucket, construct, sink in (
            (scored[:cut], CONSTRUCT_RANK_TOP, rank_top),
            (scored[-cut:], CONSTRUCT_RANK_BOTTOM, rank_bottom),
        ):
            for prox, sym, i in bucket:
                t = measure(
                    series_by_symbol[sym], i, construct=construct, prox=prox, score=None,
                    cost_pct=cost_pct, index_members=index_members, params=p,
                )
                if t is not None:
                    sink.append(t)

    if not index_members:
        notes.append(
            "Index membership list UNAVAILABLE - the index/extended split is not computable; "
            "every trade fell into the extended cell by default. Treat that split as absent."
        )
    notes.append(
        "INDEX SPLIT IS A SURVIVORSHIP-TAINTED PROXY: membership as of each signal date is not "
        "stored anywhere in this platform, so CURRENT membership (" + index_source + ") is applied "
        "backwards over the whole window. Names that joined the index after a run appear as members "
        "throughout that run; names that were dropped are mislabelled the other way. The split is "
        "indicative only and must not be read as an as-of-date membership result."
    )
    notes.append(
        "SMOOTH/JUMPY CUT IS DESCRIPTIVE, NOT TRADEABLE: the medians defining it are full-sample "
        "statistics of the signal population, unknowable at signal time."
    )
    notes.append(
        "SURVIVORSHIP (bars): the symbol set is whatever bars_1d holds today. Delisted names are "
        "absent, which biases every cell optimistically. Not correctable with stored data."
    )
    notes.append(
        "The bottom-decile and top-minus-bottom numbers are the ACADEMIC reference only: NSE cash "
        "equities cannot be shorted overnight (CNC), so the short leg is NOT executable here and no "
        "borrow cost is modelled for it."
    )
    notes.append(
        "NO PARAMETER SWEEP was run: one pre-registered parameter set, trial count N=1 "
        "(fold_pass_min = 60%)."
    )
    notes.append(
        "UNADJUSTED-HISTORY VETO mirrors the live sweep: a symbol carrying a "
        + "/".join(sorted(UNADJUSTED_KINDS))
        + f" ex-date in the {UNADJUSTED_LOOKBACK_DAYS} calendar days before the decision day is not "
        "read at all (stored bars_1d history is never re-adjusted, so its window holds bars in two "
        f"units). The veto's reach IS the STRUCTURAL corp_actions coverage - "
        f"{coverage['structural_rows']} rows, ex_date {coverage['structural_ex_date_min']} -> "
        f"{coverage['structural_ex_date_max']} (all kinds: {coverage['rows']} rows): an empty or "
        "short span silently disables it and the measured population then exceeds the one the shadow "
        "can originate. Counts are in different units: discrete = suppressed SIGNALS, rank = "
        "suppressed SYMBOL-DAYS, the live sweep = symbols per sweep."
    )

    doc: dict[str, Any] = {
        "meta": {
            "script": "scripts/backtest_hi52.py",
            "strategy_id": STRATEGY_ID,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "window": {"start": str(start), "end": str(end)},
            "n_symbols": len(series_by_symbol),
            "n_sessions": len(all_dates),
            "params": p,
            "params_match_live_defaults": p == dict(DEFAULT_PARAMS),
            "trial_count_n": TRIAL_COUNT_N,
            "parameter_sweep_run": False,
            "horizons_sessions": list(HORIZONS),
            "entry_convention": "next session's OPEN after the signal session (no same-bar fill)",
            "exit_convention": "CLOSE of the horizon session: close(T+k) / open(T+1) - 1",
            "product": PRODUCT,
            "reference_notional_inr": str(notional),
            "cost_round_trip_pct": round(cost_pct, 6),
            "cost_fees_pct": round(fees_pct, 6),
            "cost_spread_pct": round(spread_pct, 6),
            "sizing": "per-trade equal notional; percent returns; no compounding; event study",
            "gap_day_threshold_pct": GAP_DAY_PCT,
            "index_membership_source": index_source,
            "index_split_is_survivorship_tainted_proxy": True,
            "prefilter_verified_symbols": verify_checked,
            "n_rebalances": n_rebalances_used,
            "n_discrete_signals_fired": n_signals_discrete,
        },
        "corp_actions_coverage": coverage,
        "unadjusted_vetoes": {
            "discrete": discrete_vetoes.get(VETO_UNADJUSTED_HISTORY, 0),
            "rank": rank_vetoes.get(VETO_UNADJUSTED_HISTORY, 0),
        },
        "geometry": {},
        "constructs": {},
        "notes": notes,
    }

    for construct, trades in (
        (CONSTRUCT_DISCRETE, discrete),
        (CONSTRUCT_RANK_TOP, rank_top),
        (CONSTRUCT_RANK_BOTTOM, rank_bottom),
    ):
        doc["geometry"][construct] = geometry(trades, cost_pct)
        cells = split_cells(trades)
        up_med, move_med = smoothness_cut(trades)
        doc["constructs"][construct] = {
            "n_measured_trades": len(trades),
            "smoothness_cut": {
                "up_day_frac_median": None if up_med is None else round(up_med, 4),
                "max_day_move_median": None if move_med is None else round(move_med, 4),
                "rule": "smooth iff up_day_frac >= median AND max_day_move <= median (population medians)",
            },
            "cells": {name: cell_stats(cell) for name, cell in cells.items()},
            "cpcv": {str(k): cpcv_report(trades, k, cost_pct) for k in HORIZONS},
        }

    # Long-short spread (academic reference; short leg not executable — see notes).
    doc["rank_long_short_spread_pct"] = {}
    for k in HORIZONS:
        top = [t.gross[k] for t in rank_top if k in t.gross]
        bot = [t.gross[k] for t in rank_bottom if k in t.gross]
        doc["rank_long_short_spread_pct"][str(k)] = (
            None if not top or not bot
            else round(statistics.fmean(top) - statistics.fmean(bot), 4)
        )
    trades_by_construct = {
        CONSTRUCT_DISCRETE: discrete,
        CONSTRUCT_RANK_TOP: rank_top,
        CONSTRUCT_RANK_BOTTOM: rank_bottom,
    }
    return doc, trades_by_construct


# =============================================================================== rendering
#: Transliterations for the few non-ASCII characters that arrive on borrowed strings (the §6.4
#: ``promotion_decision`` reasons use em dashes, the cost model quotes rupees). The JSON artifact
#: keeps the original text verbatim; only the CONSOLE rendering is folded to ASCII, because a
#: Windows console at cp1252 cannot print a rupee sign or an arrow and would kill the run on the
#: last line of a multi-hour study.
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
    """Plain-text report, folded to ASCII (see :data:`_ASCII_MAP`). GEOMETRY is printed FIRST."""
    m = doc["meta"]
    out: list[str] = []
    add = out.append
    add("=" * 96)
    add("hi52 PRE-REGISTERED BACKTEST (IMPLEMENTATION_PLAN hi52 addendum, 2026-09-01)")
    add("=" * 96)
    add(f"generated       : {m['generated_at']}")
    add(f"window          : {m['window']['start']} -> {m['window']['end']}  "
        f"({m['n_sessions']} sessions, {m['n_symbols']} symbols)")
    add(f"params          : {json.dumps(m['params'], sort_keys=True)}")
    add(f"params == live  : {m['params_match_live_defaults']}   "
        f"(sweep run: {m['parameter_sweep_run']}, trial count N={m['trial_count_n']})")
    add(f"entry           : {m['entry_convention']}")
    add(f"exit            : {m['exit_convention']}")
    add(f"sizing          : {m['sizing']}")
    add(f"cost round trip : {m['cost_round_trip_pct']:.4f}% {m['product']} at Rs {m['reference_notional_inr']} "
        f"= {m['cost_fees_pct']:.4f}% fees + {m['cost_spread_pct']:.4f}% spread (both legs, once per trade)")
    add(f"index proxy     : {m['index_membership_source']}  [SURVIVORSHIP-TAINTED PROXY]")
    add(f"discrete fires  : {m['n_discrete_signals_fired']}   monthly rebalances used: {m['n_rebalances']}")
    if m["prefilter_verified_symbols"]:
        add(f"prefilter check : exhaustive scan_daily agreement verified on "
            f"{m['prefilter_verified_symbols']} symbol(s)")
    add("")

    # ------------------------------------------------------------------ ORB-lesson geometry FIRST
    add("-" * 96)
    add("STEP 1 - COST GEOMETRY (the ORB lesson: arithmetic BEFORE any signal-quality claim)")
    add("-" * 96)
    add("Median per-trade GROSS drift vs one full round trip. A construct whose TYPICAL trade cannot")
    add("pay the toll is dead regardless of hit rate, t-stat or fold count.")
    add("")
    for construct, geo in doc["geometry"].items():
        add(f"[{construct}]  cost floor = {geo['cost_floor_pct']:.4f}%")
        if all(h["n"] == 0 for h in geo["horizons"].values()):
            add("  no measured trades - GEOMETRY: unknown at every horizon (honest n=0, C9).")
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
            add(f"  GEOMETRY: {h['verdict']} at T+{k}  [{construct}]")
        add("")

    # ------------------------------------------------------------------ signal quality by cell
    add("-" * 96)
    add("STEP 2 - SIGNAL QUALITY BY MANDATORY SPLIT (never the pooled number alone)")
    add("-" * 96)
    for construct, block in doc["constructs"].items():
        add(f"[{construct}]  measured trades: {block['n_measured_trades']}")
        if not block["n_measured_trades"]:
            add("  no measured trades - every split cell is empty (honest n=0, C9).")
            add("")
            continue
        cut = block["smoothness_cut"]
        add(f"  smooth/jumpy cut: {cut['rule']}")
        add(f"    up_day_frac median = {cut['up_day_frac_median']}   "
            f"max_day_move median = {cut['max_day_move_median']}")
        add("")
        add("  cell                      | hor |     n | hit% net | mean gross | med gross |  mean net |   med net")
        add("  --------------------------+-----+-------+----------+------------+-----------+-----------+----------")
        for cell, stats in block["cells"].items():
            for k in sorted(stats["horizons"], key=int):
                s = stats["horizons"][k]
                hit = "-" if s["hit_rate_net"] is None else f"{s['hit_rate_net'] * 100:.1f}"
                add(f"  {cell:<25} | T+{k:<2} | {s['n']:>5} | {hit:>8} | "
                    f"{_f(s['mean_gross'], 10)} | {_f(s['median_gross'], 9)} | "
                    f"{_f(s['mean_net'], 9)} | {_f(s['median_net'], 9)}")
        add("")

    spread = doc.get("rank_long_short_spread_pct") or {}
    if spread:
        add("  rank long-short spread (mean gross top decile - bottom decile), ACADEMIC REFERENCE ONLY:")
        for k in sorted(spread, key=int):
            add(f"    T+{k}: {_f(spread[k], 9)} %   [short leg NOT executable in NSE cash/CNC]")
        add("")

    # ------------------------------------------------------------------ CPCV / deflation
    add("-" * 96)
    add("STEP 3 - CPCV + DEFLATED PROMOTION DECISION (one pre-registered param set, N=1)")
    add("-" * 96)
    for construct, block in doc["constructs"].items():
        add(f"[{construct}]")
        for k in sorted(block["cpcv"], key=int):
            c = block["cpcv"][k]
            add(f"  T+{k}: method={c['cv_method']}  obs_days={c['n_obs_days']}  splits={c['n_splits']}  "
                f"purge/embargo={c['purge_obs']}/{c['embargo_obs']} obs")
            fp = "-" if c["fold_pass_fraction"] is None else f"{c['fold_pass_fraction'] * 100:.1f}%"
            add(f"        fold_pass={fp} (need {c['fold_pass_min'] * 100:.0f}%)  "
                f"median_passing={c['median_passing_expectancy_pct_per_day']} %/day  "
                f"margin_floor={None if c['margin_floor_pct_per_day'] is None else round(c['margin_floor_pct_per_day'], 5)} %/day")
            add(f"        PROMOTABLE: {c['promotable']}")
            for r in c["reasons"]:
                add(f"          - {r}")
        add("")

    add("-" * 96)
    add("NOTES / CAVEATS (reported, not massaged)")
    add("-" * 96)
    cov = doc["corp_actions_coverage"]
    vetoes = doc["unadjusted_vetoes"]
    add(f"  unadjusted-history vetoes: discrete {vetoes['discrete']} signals, rank {vetoes['rank']} "
        f"symbol-days   [structural corp_actions rows={cov['structural_rows']}, ex_date "
        f"{cov['structural_ex_date_min']} -> {cov['structural_ex_date_max']}; all kinds {cov['rows']}]")
    for n in doc["notes"]:
        add(f"  * {n}")
    add("")
    return _ascii("\n".join(out))


# =============================================================================== CLI
def _date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _default_db() -> Path:
    return repo_root() / "data" / "market.duckdb"


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog="backtest_hi52",
        description=(
            "hi52 pre-registered backtest (one parameter set, no sweep). Read-only against "
            "bars_1d; refuses to run if the DuckDB file is missing or locked by the engine."
        ),
    )
    ap.add_argument("--db", type=Path, default=_default_db(), help="path to market.duckdb")
    ap.add_argument("--start", type=_date, default=date(2020, 1, 1), help="window start (YYYY-MM-DD)")
    ap.add_argument("--end", type=_date, default=date.today(), help="window end (YYYY-MM-DD)")
    ap.add_argument("--out", type=Path, default=None, help="JSON results path")
    ap.add_argument("--notional", type=Decimal, default=REFERENCE_NOTIONAL,
                    help="reference per-trade notional the round-trip cost is quoted at")
    ap.add_argument("--symbols", default=None, help="comma-separated symbol subset (smoke runs)")
    ap.add_argument("--max-symbols", type=int, default=None, help="cap the symbol count (smoke runs)")
    # ``--index-csv`` is the reading name since O15 (2026-09-04, index became config); the original
    # ``--nifty200-csv`` stays as an alias onto the SAME dest so recorded run commands, COMMANDS.md
    # entries and the harness tests keep working verbatim. dest is pinned because argparse would
    # otherwise derive it from the first option string and silently orphan ``args.nifty200_csv``.
    ap.add_argument("--index-csv", "--nifty200-csv", type=Path, default=None, dest="nifty200_csv",
                    help="override the CURRENT index-membership CSV used by the index/extended split")
    ap.add_argument("--verify-prefilter", type=int, default=0, metavar="K",
                    help="brute-force every eligible day for the first K symbols and abort on any "
                         "disagreement with the vectorized prefilter")
    return ap


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    db_path = Path(args.db)
    try:
        conn = open_readonly(db_path)
    except DbUnopenable as exc:
        print(f"backtest_hi52: REFUSING TO RUN.\n  {_ascii(str(exc))}", file=sys.stderr)
        return 2

    out_path = args.out
    if out_path is None:
        stamp = datetime.now().strftime("%Y%m%dT%H%M%S")
        out_path = db_path.parent / "reports" / f"backtest_hi52_{stamp}.json"
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
            nifty200_csv=args.nifty200_csv,
            db_path=db_path,
            verify_prefilter=args.verify_prefilter,
        )
    except ValueError as exc:
        print(f"backtest_hi52: {exc}", file=sys.stderr)
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
