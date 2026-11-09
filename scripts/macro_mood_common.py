"""Macro mood-signal study (``edge/macro_mood``, registered 2026-11-10): the shared core.

The registered constants, the no-lookahead timing rules, the index trade construct and the per-hypothesis
verdict that the backtests and the forward evaluator all read. Every threshold, window and sample date is the
registration's; changing one is a new registration. Evidence only; nothing here is wired into the engine.

Conventions:
* Returns are in percent units (1.25 is +1.25%), as in ``research_common``.
* Dates are ``datetime.date`` throughout. A Yahoo bar's date is its stamp read in ``meta.exchangeTimezoneName``
  through ``zoneinfo``; ``meta.gmtoffset`` holds only the offset at fetch time and would put every US-summer
  New-York-midnight bar a day early.
* A US or Brent session dated u is known before D's open iff u < D.
* "Previous ``window`` values" in the trailing statistics are the series' previous non-missing observations,
  strictly before the current one; a missing observation neither signals nor counts toward a window.
"""

from __future__ import annotations

import functools
import json
import math
import statistics
import sys
from bisect import bisect_left
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from zoneinfo import ZoneInfo

import duckdb
import numpy as np
import pandas as pd
from numpy.lib.stride_tricks import sliding_window_view

sys.path.insert(0, str(Path(__file__).resolve().parent))
import research_common as rc  # noqa: E402
from engine.learning.multiplicity import permutation_pvalue  # noqa: E402
from engine.strategy.cost_model import CostModel  # noqa: E402

hv = rc._load_script("backtest_hvol")
bb = rc.bb

NOTIONAL = rc.NOTIONAL
Refusal = rc.Refusal
open_snapshot = rc.open_snapshot
market_hours_refusal = rc.market_hours_refusal
write_reports = rc.write_reports
default_out = rc.default_out

# ----------------------------------------------------------------------------- registered constants
FAMILY = "edge/macro_mood"
N_TRIALS = 10
ALPHA = 0.005                     # registration_alpha(N_TRIALS)
FOLD_PASS_MIN = 0.60              # fold_pass_min(N_TRIALS)
MIN_TRADES = 30
PERM_SEED = 20261110

US_FALL_PCT = -2.0
US_RISE_PCT = 1.0
OIL_DROP_PCT = -5.0
OIL_SESSIONS = 3

FII_SESSIONS = 3
FII_WINDOW = 250
FII_PCTL = 90

BREADTH_MIN_PCT = 75.0
LIQUID_MIN_TV = 1e8               # Rs 10 cr median close x volume
LIQUID_WINDOW = 20

Z_WINDOW = 250
COMBINED_TOP = 0.10

#: Sessions held, counting the entry session as 1 (1 = sell at D's close).
HOLD = {"us_rebound": 1, "us_follow": 5, "oil_drop": 5, "fii_short_cover": 5, "breadth_thrust": 5,
        "combined": 5, "news_tone": 5, "policy": 10}
PRODUCT = {name: ("MIS" if name == "us_rebound" else "CNC") for name in HOLD}

#: The motivating 2026-10-29 -> 11-06 turn's window, inclusive; never sampled by any backtest.
EXCLUDED = (date(2026, 9, 1), date(2026, 11, 9))
#: The first regular session after the registering commit.
REGISTERED_FROM = date(2026, 11, 11)

_SEEN_US = (date(2022, 7, 14), date(2026, 8, 31))
_SEEN_NEWS = (date(2026, 7, 16), date(2026, 11, 10))
#: Inclusive (start, end) windows; ``None`` is open. ``combined`` confirms from its first signal-capable
#: session; the forward hypotheses confirm from ``REGISTERED_FROM`` onward.
SAMPLES: dict[str, dict[str, tuple[date | None, date | None] | None]] = {
    "us_rebound": {"confirm": (date(2008, 1, 1), date(2022, 7, 13)), "seen": _SEEN_US},
    "us_follow": {"confirm": (date(2008, 1, 1), date(2022, 7, 13)), "seen": _SEEN_US},
    "oil_drop": {"confirm": (date(2008, 1, 1), date(2026, 8, 31)), "seen": None},
    "fii_short_cover": {"confirm": (date(2012, 1, 2), date(2026, 8, 31)), "seen": None},
    "breadth_thrust": {"confirm": (date(2022, 7, 14), date(2026, 8, 31)), "seen": None},
    "combined": {"confirm": (None, date(2022, 7, 13)), "seen": _SEEN_US},
    "news_tone": {"confirm": (REGISTERED_FROM, None), "seen": _SEEN_NEWS},
    "policy": {"confirm": (REGISTERED_FROM, None), "seen": _SEEN_NEWS},
}

ETF_LIST = Path(__file__).resolve().parents[1] / "data" / "research" / "nse_etf_list_2026-10-09.csv"


def in_window(d: date, window: tuple[date | None, date | None]) -> bool:
    lo, hi = window
    return (lo is None or d >= lo) and (hi is None or d <= hi)


def is_excluded(d: date) -> bool:
    return in_window(d, EXCLUDED)


def _as_date(x: Any) -> date:
    return x.date() if isinstance(x, datetime) else x


def _require_increasing(days: Sequence[date], what: str) -> None:
    for a, b in zip(days, days[1:], strict=False):
        if not a < b:
            raise ValueError(f"{what} must be strictly increasing ({a} then {b})")


def _date_series(s: pd.Series, what: str) -> tuple[list[date], np.ndarray]:
    """``s`` without missing values, as ascending dates and float values; a repeated date raises."""
    s = s.dropna()
    days = [_as_date(d) for d in s.index]
    order = sorted(range(len(days)), key=days.__getitem__)
    days = [days[i] for i in order]
    _require_increasing(days, what)
    return days, s.to_numpy(dtype="float64")[order]


# ----------------------------------------------------------------------------- external series
def yahoo_daily(path: Path) -> pd.DataFrame:
    """A stored Yahoo v8 chart document as daily bars indexed by the exchange-local session ``date``.

    Columns open, high, low, close as floats; a bar without a close is dropped. A document without a result,
    misaligned arrays or two bars on one session raise ``ValueError``."""
    doc = json.loads(Path(path).read_text(encoding="utf-8"))
    chart = doc.get("chart") or {}
    results = chart.get("result") or []
    if not results:
        raise ValueError(f"{path}: chart has no result (error: {chart.get('error')!r})")
    res = results[0]
    tz = ZoneInfo(res["meta"]["exchangeTimezoneName"])
    stamps = res.get("timestamp") or []
    quote = res["indicators"]["quote"][0]
    cols = {k: quote.get(k) or [None] * len(stamps) for k in ("open", "high", "low", "close")}
    if any(len(v) != len(stamps) for v in cols.values()):
        raise ValueError(f"{path}: quote arrays do not match {len(stamps)} timestamps")
    days = [datetime.fromtimestamp(int(t), tz).date() for t in stamps]
    df = pd.DataFrame({k: pd.to_numeric(pd.Series(v, dtype="object"), errors="coerce").astype("float64")
                       for k, v in cols.items()})
    df.index = pd.Index(days, name="date")
    df = df[df["close"].notna()].sort_index()
    dup = df.index[df.index.duplicated()]
    if len(dup):
        raise ValueError(f"{path}: duplicate session dates {sorted(set(dup))[:5]}")
    return df


def us_overnight(nifty_days: Sequence[date], close: pd.Series) -> pd.Series:
    """Per NIFTY day D: the compounded % return of the ``close`` sessions u with D_prev <= u < D.

    D_prev is the previous element of ``nifty_days``, which must be the complete NIFTY session list (a gap in
    it would compound across the gap). NaN for the first day, when no u exists, or when the first u has no
    earlier close to compound from."""
    days = [_as_date(d) for d in nifty_days]
    _require_increasing(days, "nifty_days")
    us, px = _date_series(close, "close")
    out = np.full(len(days), np.nan)
    for k in range(1, len(days)):
        lo, hi = bisect_left(us, days[k - 1]), bisect_left(us, days[k])
        if lo < hi and lo > 0:
            out[k] = (px[hi - 1] / px[lo - 1] - 1.0) * 100.0
    return pd.Series(out, index=pd.Index(days, name="date"), name="us_overnight")


def _is_last_weekday(d: date) -> bool:
    last = date(d.year + d.month // 12, d.month % 12 + 1, 1) - timedelta(days=1)
    while last.weekday() >= 5:
        last -= timedelta(days=1)
    return d == last


def brent_change(nifty_days: Sequence[date], close: pd.Series, n: int = OIL_SESSIONS) -> pd.DataFrame:
    """Per D: ``pct``, the latest ``close`` session dated before D against the one ``n`` sessions earlier, in %;
    ``roll_flag``, True when any of the n + 1 compared sessions is the last weekday of its month (an ICE Brent
    expiry). NaN / False without n + 1 sessions before D."""
    if n < 1:
        raise ValueError(f"n must be >= 1, got {n}")
    days = [_as_date(d) for d in nifty_days]
    _require_increasing(days, "nifty_days")
    br, px = _date_series(close, "close")
    roll = [_is_last_weekday(d) for d in br]
    pct = np.full(len(days), np.nan)
    flag = np.zeros(len(days), dtype=bool)
    for k, d in enumerate(days):
        j = bisect_left(br, d) - 1
        if j - n >= 0:
            pct[k] = (px[j] / px[j - n] - 1.0) * 100.0
            flag[k] = any(roll[j - n:j + 1])
    return pd.DataFrame({"pct": pct, "roll_flag": flag}, index=pd.Index(days, name="date"))


# ----------------------------------------------------------------------------- trailing statistics
def _previous_windows(x: pd.Series, window: int) -> tuple[pd.Series, np.ndarray | None]:
    """``x``'s non-missing values and, for the k-th of them from position ``window`` on, the ``window`` values
    strictly before it (row k - window)."""
    if window < 1:
        raise ValueError(f"window must be >= 1, got {window}")
    if not (x.index.is_monotonic_increasing and x.index.is_unique):
        raise ValueError("series index must be strictly increasing")
    v = x.dropna()
    arr = v.to_numpy(dtype="float64")
    if arr.size <= window:
        return v, None
    return v, sliding_window_view(arr[:-1], window)


def trailing_pctl_signal(x: pd.Series, window: int, pctl: float) -> pd.Series:
    """True where ``x`` strictly exceeds numpy's linear ``pctl`` percentile of its previous ``window`` values;
    False while fewer than ``window`` exist and where ``x`` is missing."""
    v, w = _previous_windows(x, window)
    out = pd.Series(False, index=x.index, dtype=bool)
    if w is not None:
        cur = v.to_numpy(dtype="float64")[window:]
        hit = cur > np.percentile(w, pctl, axis=1)
        out.loc[v.index[window:][hit]] = True
    return out


def zscore_trailing(x: pd.Series, window: int) -> pd.Series:
    """``(x - mean) / sd`` over the previous ``window`` values, sample SD (ddof = 1); NaN before that, where
    ``x`` is missing, and where the window is constant (zero SD)."""
    if window < 2:
        raise ValueError(f"a sample SD needs window >= 2, got {window}")
    v, w = _previous_windows(x, window)
    out = pd.Series(np.nan, index=x.index, dtype="float64")
    if w is not None:
        cur = v.to_numpy(dtype="float64")[window:]
        flat = w.max(axis=1) == w.min(axis=1)
        sd = np.where(flat, np.nan, w.std(axis=1, ddof=1))
        out.loc[v.index[window:]] = (cur - w.mean(axis=1)) / sd
    return out


# ----------------------------------------------------------------------------- trade construct
@dataclass(frozen=True)
class TradeRow:
    signal_date: date
    entry_date: date
    exit_date: date
    gross: float
    net: float
    flagged: bool = False


def _session_index(sessions: Sequence[date], d: date) -> int:
    i = bisect_left(sessions, d)
    if i == len(sessions) or sessions[i] != d:
        raise ValueError(f"{d} is not a session")
    return i


def index_trade(px: pd.DataFrame, sessions: Sequence[date], d: date, hold: int) -> tuple[date, float] | None:
    """Gross % from open(d) to the close of the ``hold``-th session counting d as 1, as ``(exit_date, gross)``.

    ``sessions`` is ascending; ``px`` is indexed by ``date``. None when the exit session lies beyond
    ``sessions`` or either bar is missing, non-finite or non-positive."""
    if hold < 1:
        raise ValueError(f"hold must be >= 1, got {hold}")
    if isinstance(px.index, pd.DatetimeIndex):
        raise TypeError("px must be indexed by datetime.date, not Timestamps")
    d = _as_date(d)
    k = _session_index(sessions, d) + hold - 1
    if k >= len(sessions):
        return None
    exit_d = sessions[k]
    try:
        o, c = float(px.at[d, "open"]), float(px.at[exit_d, "close"])
    except KeyError:
        return None
    if not (math.isfinite(o) and math.isfinite(c) and o > 0 and c > 0):
        return None
    return exit_d, (c / o - 1.0) * 100.0


def non_overlapping(signal_days: Sequence[date], sessions: Sequence[date], hold: int) -> list[date]:
    """The signals kept under one position at a time, greedily in date order.

    A position opened on session i is held through session i + hold - 1, so a signal on its exit session is
    skipped too. Every signal must be a session; pass only tradeable signals, since a kept one blocks its
    window whether or not it fills."""
    if hold < 1:
        raise ValueError(f"hold must be >= 1, got {hold}")
    _require_increasing(sessions, "sessions")
    kept: list[date] = []
    busy_through = -1
    for d in sorted({_as_date(s) for s in signal_days}):
        i = _session_index(sessions, d)
        if i > busy_through:
            kept.append(d)
            busy_through = i + hold - 1
    return kept


@functools.cache
def _cost_model() -> CostModel:
    return CostModel.from_config()


def cost_pct(product: str) -> float:
    """One round trip at the research notional, in %: fees plus the measured spread."""
    return float(_cost_model().breakeven_pct(NOTIONAL, product))


# ----------------------------------------------------------------------------- verdict
def verdict(trades: list[TradeRow], population_gross: Sequence[float], hold: int, cost: float) -> dict:
    """The registered verdict on one confirmatory sample, with every metric that can be computed.

    ``insufficient`` below ``MIN_TRADES``; else ``refuted`` when mean net <= 0; else ``supported`` when every
    criterion holds; else ``null``. The data gate is the caller's. ``margin_floor_promotable`` is
    ``cpcv_report``'s promotion decision, a diagnostic only."""
    rows = sorted(trades, key=lambda t: (t.entry_date, t.signal_date))
    n = len(rows)
    nets = [t.net for t in rows]
    pop = list(population_gross)
    # A NaN draw median never compares >= the sample's, so a NaN would read as evidence.
    for what, vals in (("population_gross", pop), ("trade gross", [t.gross for t in rows]), ("trade net", nets)):
        if not all(math.isfinite(v) for v in vals):
            raise ValueError(f"{what} has non-finite values")
    out: dict[str, Any] = {"label": "insufficient", "n": n, "mean_net": None, "median_net": None,
                           "perm_p": None, "fold_pass": None, "half1_mean": None, "half2_mean": None,
                           "drop_top3_mean": None, "margin_floor_promotable": None, "criteria": None,
                           "cpcv": None}
    if not n:
        return out
    half = n // 2
    out["mean_net"] = statistics.fmean(nets)
    out["median_net"] = statistics.median(nets)
    out["half1_mean"] = statistics.fmean(nets[:half]) if half else None
    out["half2_mean"] = statistics.fmean(nets[half:])
    out["drop_top3_mean"] = statistics.fmean(sorted(nets)[:-3]) if n > 3 else None
    if n <= len(pop):
        out["perm_p"] = permutation_pvalue([t.gross for t in rows], pop, seed=PERM_SEED)
    cv_rows = [SimpleNamespace(fill_date=t.entry_date, net={hold: t.net}) for t in rows]
    rep = bb.cpcv_report(cv_rows, hold, cost, trial_count=N_TRIALS)
    folds = rep["folds"]
    # unrounded: the report's fold_pass_fraction is rounded to 4 dp
    out["fold_pass"] = sum(f["passed"] for f in folds) / len(folds) if folds else None
    out["margin_floor_promotable"] = bool(rep["promotable"])
    out["cpcv"] = rep

    def pos(x: float | None) -> bool:
        return x is not None and x > 0

    criteria = {
        "min_trades": n >= MIN_TRADES,
        "mean_net_positive": pos(out["mean_net"]),
        "median_net_positive": pos(out["median_net"]),
        "permutation": out["perm_p"] is not None and out["perm_p"] < ALPHA,
        "fold_pass": out["fold_pass"] is not None and out["fold_pass"] >= FOLD_PASS_MIN,
        "both_halves_positive": pos(out["half1_mean"]) and pos(out["half2_mean"]),
        "drop_top3_positive": pos(out["drop_top3_mean"]),
    }
    out["criteria"] = criteria
    if n < MIN_TRADES:
        out["label"] = "insufficient"
    elif out["mean_net"] <= 0:
        out["label"] = "refuted"
    elif all(criteria.values()):
        out["label"] = "supported"
    else:
        out["label"] = "null"
    return out


# ----------------------------------------------------------------------------- liquid universe
#: id(conn) -> (conn, {day: liquid symbols}, {ex_date: symbols}). The connection is held so its id is
#: never reused while the entry exists.
_LIQUID_CACHE: dict[int, tuple[Any, dict[date, frozenset[str]], dict[date, frozenset[str]]]] = {}


def _liquid_tables(conn: duckdb.DuckDBPyConnection) -> tuple[dict[date, frozenset[str]], dict[date, frozenset[str]]]:
    hit = _LIQUID_CACHE.get(id(conn))
    if hit is not None and hit[0] is conn:
        return hit[1], hit[2]
    try:
        ex_rows = conn.execute(
            "SELECT ex_date, list(DISTINCT symbol) FROM corp_actions WHERE ex_date IS NOT NULL GROUP BY ex_date"
        ).fetchall()
    except duckdb.Error as exc:
        raise Refusal(f"corp_actions unreadable ({type(exc).__name__}: {exc}); the breadth universe's "
                      "ex-date exclusion requires it") from exc
    # Index rows (e.g. "NIFTY 50", "INDIA VIX") share bars_1d; NSE equity symbols never contain a space.
    liquid_rows = conn.execute(
        f"""
        SELECT d, list(symbol) FROM (
            SELECT symbol, d,
                   median("close" * volume) OVER w AS med_tv,
                   count(*) OVER w AS n_sessions
            FROM bars_1d
            WHERE strpos(symbol, ' ') = 0
            WINDOW w AS (PARTITION BY symbol ORDER BY d
                         ROWS BETWEEN {LIQUID_WINDOW - 1} PRECEDING AND CURRENT ROW)
        )
        WHERE n_sessions = {LIQUID_WINDOW} AND med_tv >= ?
        GROUP BY d
        """,
        [int(LIQUID_MIN_TV)],
    ).fetchall()
    liquid = {_as_date(d): frozenset(syms) for d, syms in liquid_rows}
    ex = {_as_date(d): frozenset(syms) for d, syms in ex_rows}
    _LIQUID_CACHE[id(conn)] = (conn, liquid, ex)
    return liquid, ex


def liquid_universe(conn: duckdb.DuckDBPyConnection, d_prev: date, etf_symbols: frozenset[str]) -> set[str]:
    """The breadth universe as of D-1: ``bars_1d`` stocks with a bar on ``d_prev`` whose median close x volume
    over their own 20 sessions to ``d_prev`` (inclusive) is >= Rs 10 cr; fewer than 20 sessions is not liquid.
    Drops ETFs (``backtest_hvol.is_etf``) and any name with a ``corp_actions`` ex-date on ``d_prev``.

    The rolling medians are computed once per connection and cached; a fresh set is returned per call."""
    liquid, ex = _liquid_tables(conn)
    d_prev = _as_date(d_prev)
    drop = ex.get(d_prev, frozenset())
    return {s for s in liquid.get(d_prev, ()) if s not in drop and not hv.is_etf(s, etf_symbols)}


def load_etf_symbols(path: Path = ETF_LIST) -> frozenset[str]:
    """NSE's ETF list (``Symbol`` column); missing or empty refuses, as in the hvol study."""
    return hv.load_etf_list(path).symbols
