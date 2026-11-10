#!/usr/bin/env python
"""Macro mood study (``edge/macro_mood``, registered 2026-11-10): the six registered backtests.

US big-fall rebound, US rally follow-through, oil drop, foreign short-covering, breadth thrust and the combined
mood score, each a long NIFTY index trade from D's open, with the registered verdict on its confirmatory
window, the seen windows reported as not evidence, the 2026-09-01 -> 11-09 motivating window listed apart and
never sampled, and a diagnostics block that decides nothing. Reads only stored external files (verified against
their manifest) and a research snapshot. Evidence only; wires nothing. Exit codes: 0 ran, 2 refused.

Every value read for D's signal is known before D's 09:15 open: US and Brent sessions dated before D, the FII
open interest NSE publishes on D-1's evening, and stock closes through D-1.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import statistics
import sys
from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal
from fractions import Fraction
from functools import cache
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
import research_common as rc  # noqa: E402,I001  engine's native import order first
import macro_mood_common as mm  # noqa: E402
import fetch_macro_external as fx  # noqa: E402

import duckdb  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from engine.strategy.cost_model import CostModel  # noqa: E402
from engine.strategy.indicators import wilder_atr  # noqa: E402
from engine.strategy.regime import INDEX_SMA, RISING_LOOKBACK, index_regime  # noqa: E402

Refusal = rc.Refusal
TradeRow = mm.TradeRow
Window = tuple[date | None, date | None]

HYPOTHESES = ("us_rebound", "us_follow", "oil_drop", "fii_short_cover", "breadth_thrust", "combined")
ARMS = {"combined_arm_us": "us", "combined_arm_oil": "oil", "combined_arm_fii": "fii"}
#: Hypotheses whose verdict the ^NSEI overlap check gates: their samples end 2022-07-13, all on Yahoo opens.
GATED = frozenset({"us_rebound", "us_follow", "combined"})
SEEN_KEY = "seen (not evidence)"

INDEX_SYMBOL = "NIFTY 50"
#: The first session priced from ``bars_1d``; earlier sessions are Yahoo ``^NSEI``.
NIFTY_SWITCH = date(2022, 7, 12)
#: Yahoo opens are binary floats; ``bars_1d`` holds paise. Equal means within half a paisa.
OPEN_TOL = 0.005
YEAR_DROP_MAX = Fraction(1, 10)     # exact: a year at exactly 10% passes
COMBINED_PCTL = round(100.0 * (1.0 - mm.COMBINED_TOP), 9)
REQUIRED_FILES = ("yahoo/^NSEI.json", "yahoo/^GSPC.json", "yahoo/^IXIC.json", "yahoo/BZ=F.json")
DEFAULT_EXTERNAL = Path(__file__).resolve().parents[1] / "data" / "research" / "external"

# ----------------------------------------------------------------------------- diagnostics (decide nothing)
HORIZONS = (1, 3, 10)
ATR_PERIOD = 14
ATR_STOP_MULT = 2.5
ETF_STT_SELL_PCT = Decimal("0.001")
HIGH_BETA_WINDOW = (date(2022, 7, 14), date(2026, 8, 31))
BETA_SESSIONS = 250
BETA_MIN_PAIRS = 200            # of the 250 returns; a suspended or newly listed name has no beta
TOP_TRADED = 200
QUINTILE = 5
#: Per-day rows fetched before the ETF test trims them to ``TOP_TRADED``; exhausting it raises.
_TV_FETCH = 600


@dataclass
class StudyContext:
    """Everything the backtests read, loaded once. ``cache`` memoises signals and derived tables by name."""

    conn: duckdb.DuckDBPyConnection
    external: Path
    nifty: pd.DataFrame                 # open/high/low/close by session date: ^NSEI, then bars_1d
    sessions: list[date]
    pos: dict[date, int]
    gate: dict[str, Any]
    spx: pd.Series
    ixic: pd.Series
    brent: pd.Series
    fii: pd.Series
    etf_symbols: frozenset[str]
    oi_coverage: dict[int, dict[str, int]]
    manifest: dict[str, Any] | None = None
    db: str | None = None
    cache: dict[Any, Any] = field(default_factory=dict)

    @property
    def clean(self) -> frozenset[date]:
        return self.gate["clean_days"]


# ----------------------------------------------------------------------------- loading
def verify_manifest(external: Path) -> dict[str, Any]:
    """The fetch manifest, every listed file present with its recorded sha256, the four Yahoo series listed.
    Anything else refuses: the verdict run reads only stored, recorded files."""
    ext = Path(external)
    mpath = ext / "manifest.json"
    if not mpath.is_file():
        raise Refusal(f"no manifest file at {mpath}; run scripts/fetch_macro_external.py first")
    raw = mpath.read_bytes()
    try:
        manifest = json.loads(raw.decode("utf-8"))
    except ValueError as exc:
        raise Refusal(f"{mpath} is not JSON ({exc})") from exc
    files = manifest.get("files") or {}
    absent = [r for r in REQUIRED_FILES if r not in files]
    if absent:
        raise Refusal(f"manifest {mpath} lacks {absent}")
    bad: list[str] = []
    for rel, info in sorted(files.items()):
        p = ext / rel
        if not p.is_file():
            bad.append(f"{rel}: listed in the manifest, missing on disk")
        elif hashlib.sha256(p.read_bytes()).hexdigest() != (info or {}).get("sha256"):
            bad.append(f"{rel}: sha256 differs from the manifest")
    if bad:
        raise Refusal(f"{len(bad)} manifest file(s) fail verification, e.g. {bad[:5]}")
    oi_dir = ext / "nse_oi"
    on_disk = {f"nse_oi/{p.name}" for p in oi_dir.iterdir()} if oi_dir.is_dir() else set()
    errors = manifest.get("errors") or {}
    return {"path": str(mpath), "sha256": hashlib.sha256(raw).hexdigest(),
            "created_at": manifest.get("created_at"), "period1": manifest.get("period1"),
            "period2": manifest.get("period2"), "files": len(files),
            "oi_files": sum(r.startswith("nse_oi/") for r in files),
            "oi_unlisted_on_disk": len(on_disk - set(files)),
            "missing_404": len(manifest.get("missing") or []), "errors": len(errors),
            "error_sample": dict(sorted(errors.items())[:10])}


def _db_nifty(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    rows = conn.execute('SELECT d, "open", high, low, "close" FROM bars_1d WHERE symbol = ? ORDER BY d',
                        [INDEX_SYMBOL]).fetchall()
    if not rows:
        raise Refusal(f"bars_1d has no {INDEX_SYMBOL!r} rows")
    return pd.DataFrame({k: [float(r[i]) for r in rows] for i, k in enumerate(("open", "high", "low", "close"), 1)},
                        index=pd.Index([mm._as_date(r[0]) for r in rows], name="date"))


def _yahoo(path: Path) -> pd.DataFrame:
    try:
        return mm.yahoo_daily(path)
    except (OSError, ValueError, KeyError, TypeError, IndexError) as exc:
        raise Refusal(f"{path}: unreadable Yahoo chart ({type(exc).__name__}: {exc})") from exc


def _fii(oi_dir: Path) -> pd.Series:
    if not oi_dir.is_dir():
        return pd.Series(dtype="float64", index=pd.Index([], name="date"))
    s = fx.fii_long_share(oi_dir)
    return pd.Series(s.to_numpy(dtype="float64"), index=pd.Index(list(s.index), name="date"))


def oi_coverage(sessions: Sequence[date], oi_dir: Path, fii: pd.Series) -> dict[int, dict[str, int]]:
    """Per calendar year from the OI start: NIFTY sessions, OI files on disk, files the parser read, and
    sessions without a value. The parser skips a failing file silently, so an unparsed year shows here."""
    files: Counter[int] = Counter()
    if oi_dir.is_dir():
        for p in oi_dir.iterdir():
            m = fx._OI_NAME_RE.match(p.name)
            if m:
                try:
                    files[date(int(m.group(3)), int(m.group(2)), int(m.group(1))).year] += 1
                except ValueError:
                    continue
    have = set(fii.index)
    sess = [d for d in sessions if d >= fx.OI_FIRST_DAY]
    n_sess = Counter(d.year for d in sess)
    parsed = Counter(d.year for d in have)
    without = Counter(d.year for d in sess if d not in have)
    return {y: {"nifty_sessions": n_sess[y], "oi_files": files[y], "oi_parsed": parsed[y],
                "unparsed": files[y] - parsed[y], "sessions_without_oi": without[y]}
            for y in sorted(set(n_sess) | set(files) | set(parsed))}


def load_context(conn: duckdb.DuckDBPyConnection, external: Path, etf_symbols: frozenset[str], *,
                 manifest: dict[str, Any] | None = None, db: Path | str | None = None) -> StudyContext:
    ext = Path(external)
    nsei = _yahoo(ext / "yahoo" / "^NSEI.json")
    db_nifty = _db_nifty(conn)
    gate = nsei_gate(nsei, db_nifty)
    nifty = pd.concat([nsei.loc[[d < NIFTY_SWITCH for d in nsei.index]],
                       db_nifty.loc[[d >= NIFTY_SWITCH for d in db_nifty.index]]])
    sessions = list(nifty.index)
    mm._require_increasing(sessions, "NIFTY sessions")
    fii = _fii(ext / "nse_oi")
    return StudyContext(
        conn=conn, external=ext, nifty=nifty, sessions=sessions, pos={d: i for i, d in enumerate(sessions)},
        gate=gate, spx=_yahoo(ext / "yahoo" / "^GSPC.json")["close"],
        ixic=_yahoo(ext / "yahoo" / "^IXIC.json")["close"], brent=_yahoo(ext / "yahoo" / "BZ=F.json")["close"],
        fii=fii, etf_symbols=frozenset(etf_symbols), oi_coverage=oi_coverage(sessions, ext / "nse_oi", fii),
        manifest=manifest, db=None if db is None else str(db))


# ----------------------------------------------------------------------------- the ^NSEI data gate
def _r2(x: float) -> float:
    return round(float(x), 2)


def nsei_gate(yahoo: pd.DataFrame, db_nifty: pd.DataFrame) -> dict[str, Any]:
    """The registered ``^NSEI`` data gate.

    ``ok`` is False when any open on a date both carry differs from ``bars_1d`` by more than half a paisa, or
    when no date overlaps (nothing verified). Each Yahoo session before ``NIFTY_SWITCH`` with no ``bars_1d``
    counterpart is dropped from the open-anchored arms when its open equals the previous close or lies outside
    [low, high] at 2 dp (a missing open, high or low drops it too); a mismatched overlap day is dropped. A year
    dropping more than 10% of its pre-switch sessions fails whole. ``clean_days`` holds the pre-switch sessions
    that survive plus every ``bars_1d`` session from the switch on."""
    db_open = {d: float(o) for d, o in zip(db_nifty.index, db_nifty["open"], strict=True)}
    common = [d for d in yahoo.index if d in db_open]
    mismatch = [d for d in common
                if not (math.isfinite(yahoo.at[d, "open"]) and abs(yahoo.at[d, "open"] - db_open[d]) <= OPEN_TOL)]
    mismatched = set(mismatch)
    by_year: dict[int, dict[str, int]] = {}
    kept: set[date] = set()
    prev_close: float | None = None
    for d, row in yahoo.iterrows():
        if d >= NIFTY_SWITCH:
            break
        y = by_year.setdefault(d.year, {"sessions": 0, "dropped": 0, "open_eq_prev_close": 0,
                                        "open_outside_range": 0, "missing_ohl": 0, "overlap_mismatch": 0})
        y["sessions"] += 1
        o, h, lo = row["open"], row["high"], row["low"]
        reason = None
        if d in db_open:
            reason = "overlap_mismatch" if d in mismatched else None
        elif not all(math.isfinite(v) for v in (o, h, lo)):
            reason = "missing_ohl"
        elif prev_close is not None and _r2(o) == _r2(prev_close):
            reason = "open_eq_prev_close"
        elif _r2(o) < _r2(lo) or _r2(o) > _r2(h):
            reason = "open_outside_range"
        if reason:
            y["dropped"] += 1
            y[reason] += 1
        else:
            kept.add(d)
        prev_close = float(row["close"])
    failed = sorted(yr for yr, c in by_year.items() if Fraction(c["dropped"], c["sessions"]) > YEAR_DROP_MAX)
    clean = {d for d in kept if d.year not in failed} | {d for d in db_open if d >= NIFTY_SWITCH}
    return {"ok": bool(common) and not mismatch, "clean_days": frozenset(clean), "dropped_by_year": by_year,
            "failed_years": failed, "overlap_days": len(common),
            "overlap_first": min(common) if common else None, "overlap_last": max(common) if common else None,
            "mismatches": len(mismatch),
            "mismatch_sample": [{"date": d, "yahoo_open": float(yahoo.at[d, "open"]), "bars_1d_open": db_open[d]}
                                for d in mismatch[:20]]}


# ----------------------------------------------------------------------------- signal inputs
def _frame(ctx: StudyContext, value: pd.Series, signal: pd.Series, flagged: pd.Series | None = None) -> pd.DataFrame:
    idx = pd.Index(ctx.sessions, name="date")
    v = pd.Series(value.to_numpy(dtype="float64"), index=idx)
    sig = pd.Series(signal.to_numpy(dtype=bool), index=idx) & v.notna()
    flag = pd.Series(False, index=idx) if flagged is None else pd.Series(flagged.to_numpy(dtype=bool), index=idx)
    return pd.DataFrame({"signal": sig.astype(bool), "flagged": flag.astype(bool), "value": v})


def _memo(ctx: StudyContext, key: Any, build):
    if key not in ctx.cache:
        ctx.cache[key] = build()
    return ctx.cache[key]


def us_return(ctx: StudyContext, src: str = "spx") -> pd.Series:
    """The compounded US return of the sessions dated D_prev <= u < D (``mm.us_overnight``)."""
    return _memo(ctx, ("us", src), lambda: mm.us_overnight(ctx.sessions, ctx.spx if src == "spx" else ctx.ixic))


def oil_change(ctx: StudyContext) -> pd.DataFrame:
    return _memo(ctx, "oil", lambda: mm.brent_change(ctx.sessions, ctx.brent, mm.OIL_SESSIONS))


def fii_change(ctx: StudyContext) -> pd.Series:
    """share(D-1) - share(D-4) over NIFTY sessions; NaN unless all four sessions D-4..D-1 have a parsed value."""
    def build() -> pd.Series:
        sh = ctx.fii.reindex(ctx.sessions).to_numpy(dtype="float64")
        n = mm.FII_SESSIONS
        out = np.full(len(sh), np.nan)
        for k in range(n + 1, len(sh)):
            w = sh[k - n - 1:k]
            if np.isfinite(w).all():
                out[k] = w[-1] - w[0]
        return pd.Series(out, index=pd.Index(ctx.sessions, name="date"))
    return _memo(ctx, "fii_change", build)


def _stock_panel(ctx: StudyContext) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Stock opens and closes (``bars_1d`` rows whose symbol has no space), session x symbol."""
    def build():
        a = ctx.conn.execute('SELECT symbol, d, CAST("open" AS DOUBLE) AS o, CAST("close" AS DOUBLE) AS c '
                             "FROM bars_1d WHERE strpos(symbol, ' ') = 0").fetchnumpy()
        df = pd.DataFrame({"symbol": np.asarray(a["symbol"], dtype=object),
                           "d": [x.date() for x in pd.to_datetime(a["d"])], "o": a["o"], "c": a["c"]})
        if df.empty:
            empty = pd.DataFrame(index=pd.Index([], name="date"))
            return empty, empty
        o = df.pivot(index="d", columns="symbol", values="o").sort_index()
        c = df.pivot(index="d", columns="symbol", values="c").sort_index()
        return o, c
    return _memo(ctx, "stock_panel", build)


def breadth(ctx: StudyContext) -> pd.Series:
    """% of ``liquid_universe`` as of D-1 whose D-1 close exceeds its D-2 close (D-1, D-2 NIFTY sessions);
    NaN when no universe name has both closes."""
    def build() -> pd.DataFrame:
        _, close = _stock_panel(ctx)
        have = set(close.index)
        val = np.full(len(ctx.sessions), np.nan)
        sig = np.zeros(len(ctx.sessions), dtype=bool)
        for k in range(2, len(ctx.sessions)):
            d1, d2 = ctx.sessions[k - 1], ctx.sessions[k - 2]
            if d1 not in have or d2 not in have:
                continue
            univ = sorted(mm.liquid_universe(ctx.conn, d1, ctx.etf_symbols))
            if not univ:
                continue
            c1 = close.loc[d1, univ].to_numpy(dtype="float64")
            c2 = close.loc[d2, univ].to_numpy(dtype="float64")
            ok = np.isfinite(c1) & np.isfinite(c2)
            n = int(ok.sum())
            if n:
                up = int((c1[ok] > c2[ok]).sum())
                val[k] = 100.0 * up / n
                sig[k] = 100 * up >= mm.BREADTH_MIN_PCT * n
        idx = pd.Index(ctx.sessions, name="date")
        return pd.DataFrame({"value": val, "signal": sig}, index=idx)
    return _memo(ctx, "breadth", build)


def combined_inputs(ctx: StudyContext) -> pd.DataFrame:
    """The three trailing z-scores and their mean; the score is NaN unless all three exist."""
    def build() -> pd.DataFrame:
        z = pd.DataFrame({
            "us": mm.zscore_trailing(us_return(ctx, "spx"), mm.Z_WINDOW),
            "oil": mm.zscore_trailing(-oil_change(ctx)["pct"], mm.Z_WINDOW),
            "fii": mm.zscore_trailing(fii_change(ctx), mm.Z_WINDOW),
        })
        z["score"] = z[["us", "oil", "fii"]].mean(axis=1, skipna=False)
        return z
    return _memo(ctx, "combined_inputs", build)


def combined_first_session(ctx: StudyContext) -> date | None:
    """The first session with ``Z_WINDOW`` previous scores, i.e. the first that can signal."""
    s = combined_inputs(ctx)["score"].dropna()
    return s.index[mm.Z_WINDOW] if len(s) > mm.Z_WINDOW else None


def signals(name: str, ctx: StudyContext) -> pd.DataFrame:
    """Per NIFTY session D: ``signal`` (bool), ``flagged`` (the oil roll flag; False elsewhere), ``value``."""
    def build() -> pd.DataFrame:
        base, _, src = name.partition("@")
        if base in ("us_rebound", "us_follow"):
            v = us_return(ctx, src or "spx")
            return _frame(ctx, v, v <= mm.US_FALL_PCT if base == "us_rebound" else v >= mm.US_RISE_PCT)
        if base == "oil_drop":
            o = oil_change(ctx)
            return _frame(ctx, o["pct"], o["pct"] <= mm.OIL_DROP_PCT, o["roll_flag"])
        if base == "fii_short_cover":
            v = fii_change(ctx)
            return _frame(ctx, v, mm.trailing_pctl_signal(v, mm.FII_WINDOW, mm.FII_PCTL))
        if base == "breadth_thrust":
            b = breadth(ctx)
            return _frame(ctx, b["value"], b["signal"])
        if base == "combined":
            v = combined_inputs(ctx)["score"]
            return _frame(ctx, v, mm.trailing_pctl_signal(v, mm.Z_WINDOW, COMBINED_PCTL))
        if base in ARMS:
            v = combined_inputs(ctx)[ARMS[base]]
            return _frame(ctx, v, mm.trailing_pctl_signal(v, mm.Z_WINDOW, COMBINED_PCTL))
        raise ValueError(f"unknown signal {name!r}")
    return _memo(ctx, name, build)


# ----------------------------------------------------------------------------- trades
def window_days(ctx: StudyContext, window: Window | None, *, include_excluded: bool = False) -> list[date]:
    if window is None:
        return []
    return [d for d in ctx.sessions if mm.in_window(d, window) and (include_excluded or not mm.is_excluded(d))]


def _index_trade(ctx: StudyContext, d: date, hold: int) -> tuple[date, float] | None:
    return _memo(ctx, ("index_trade", d, hold), lambda: mm.index_trade(ctx.nifty, ctx.sessions, d, hold))


def build_trades(ctx: StudyContext, days: Iterable[date], hold: int, cost: float,
                 flagged: pd.Series | None = None) -> tuple[list[TradeRow], dict[str, int]]:
    """Index trades for the signal ``days``: entry open must be clean and the trade constructible before
    ``non_overlapping`` (a kept signal blocks its window whether or not it fills)."""
    counts = {"signals": 0, "entry_open_unclean": 0, "unconstructible": 0, "kept": 0}
    built: dict[date, tuple[date, float]] = {}
    for d in days:
        counts["signals"] += 1
        if d not in ctx.clean:
            counts["entry_open_unclean"] += 1
            continue
        t = _index_trade(ctx, d, hold)
        if t is None:
            counts["unconstructible"] += 1
            continue
        built[d] = t
    kept = mm.non_overlapping(list(built), ctx.sessions, hold)
    counts["kept"] = len(kept)
    rows = [TradeRow(d, d, built[d][0], built[d][1], built[d][1] - cost,
                     bool(flagged.at[d]) if flagged is not None else False) for d in kept]
    return rows, counts


def signal_days(ctx: StudyContext, name: str, window: Window | None, *, include_excluded: bool = False,
                unflagged_only: bool = False) -> list[date]:
    sig = signals(name, ctx)
    return [d for d in window_days(ctx, window, include_excluded=include_excluded)
            if sig.at[d, "signal"] and not (unflagged_only and sig.at[d, "flagged"])]


def trades_for(ctx: StudyContext, name: str, window: Window | None, *, hold: int | None = None,
               cost: float | None = None, include_excluded: bool = False,
               unflagged_only: bool = False) -> tuple[list[TradeRow], dict[str, int]]:
    """The kept trades of signal ``name`` entering inside ``window`` (never ``EXCLUDED`` unless asked)."""
    base = name.partition("@")[0]
    key = "combined" if base in ARMS else base
    hold = mm.HOLD[key] if hold is None else hold
    cost = mm.cost_pct(mm.PRODUCT[key]) if cost is None else cost
    days = signal_days(ctx, name, window, include_excluded=include_excluded, unflagged_only=unflagged_only)
    return build_trades(ctx, days, hold, cost, signals(name, ctx)["flagged"])


def population(ctx: StudyContext, window: Window | None, hold: int) -> list[float]:
    """Every clean session's same-construct gross return in ``window`` (``EXCLUDED`` never)."""
    def build() -> list[float]:
        out = []
        for d in window_days(ctx, window):
            if d in ctx.clean and (t := _index_trade(ctx, d, hold)) is not None:
                out.append(t[1])
        return out
    return _memo(ctx, ("population", window, hold), build)


def confirm_window(name: str, ctx: StudyContext) -> Window | None:
    lo, hi = mm.SAMPLES[name]["confirm"]
    if name == "combined":
        first = combined_first_session(ctx)
        return None if first is None else (first, hi)
    return lo, hi


# ----------------------------------------------------------------------------- per-hypothesis results
def summary(rows: Sequence[TradeRow]) -> dict[str, Any]:
    nets = [t.net for t in rows]
    return {"n": len(rows),
            "mean_gross": statistics.fmean(t.gross for t in rows) if rows else None,
            "mean_net": statistics.fmean(nets) if rows else None,
            "median_net": statistics.median(nets) if rows else None,
            "hit_rate_net": sum(n > 0 for n in nets) / len(nets) if rows else None}


def _rows(rows: Sequence[TradeRow]) -> list[dict[str, Any]]:
    return [asdict(t) for t in rows]


def _window(w: Window | None) -> list[date | None] | None:
    return None if w is None else [w[0], w[1]]


def hypothesis(name: str, ctx: StudyContext) -> dict[str, Any]:
    """The registered verdict on the confirm window, the seen window (not evidence) and the excluded window."""
    hold, product = mm.HOLD[name], mm.PRODUCT[name]
    cost = mm.cost_pct(product)
    window = confirm_window(name, ctx)
    rows, counts = trades_for(ctx, name, window)
    pop = population(ctx, window, hold)
    v = mm.verdict(rows, pop, hold, cost)
    out: dict[str, Any] = {"hold": hold, "product": product, "cost_pct": cost, "confirm_window": _window(window),
                           "counts": counts, "population_n": len(pop), "verdict": v, "label": v["label"],
                           "label_reason": "verdict", "gate_failed": False, "trades": _rows(rows)}
    if name == "oil_drop":
        un_rows, un_counts = trades_for(ctx, name, window, unflagged_only=True)
        vu = mm.verdict(un_rows, pop, hold, cost)
        out |= {"verdict_unflagged": vu, "counts_unflagged": un_counts, "trades_unflagged": _rows(un_rows),
                "labels": {"all": v["label"], "unflagged": vu["label"]}}
        if v["label"] != vu["label"]:
            out["label"], out["label_reason"] = "null", "all and unflagged verdicts disagree"
    if name == "combined":
        out |= combined_arms(ctx, window, out["verdict"]["mean_net"])
        out["first_signal_capable"] = window[0] if window else None
    if name in GATED and not ctx.gate["ok"]:
        out["label"], out["gate_failed"] = "insufficient", True
        out["label_reason"] = "data gate: ^NSEI opens differ from bars_1d over the overlap (or no overlap)"
    seen = mm.SAMPLES[name]["seen"]
    if seen is None:
        out[SEEN_KEY] = None
    else:
        s_rows, s_counts = trades_for(ctx, name, seen)
        out[SEEN_KEY] = {"window": _window(seen), "summary": summary(s_rows), "counts": s_counts,
                         "trades": _rows(s_rows)}
    x_rows, x_counts = trades_for(ctx, name, mm.EXCLUDED, include_excluded=True)
    out["excluded_window"] = {"window": _window(mm.EXCLUDED), "summary": summary(x_rows), "counts": x_counts,
                              "trades": _rows(x_rows)}
    return out


def combined_arms(ctx: StudyContext, window: Window | None, combined_mean: float | None) -> dict[str, Any]:
    """Each input's own z under the same top-10% rule and window. Combining helps iff the score's mean net
    exceeds every arm's; undetermined (None) when the score or any arm has no trade."""
    arms = {}
    for arm in ARMS:
        rows, counts = trades_for(ctx, arm, window)
        arms[arm] = {**summary(rows), "counts": counts}
    means = [a["mean_net"] for a in arms.values()]
    helps = None if combined_mean is None or any(m is None for m in means) else all(combined_mean > m for m in means)
    return {"arms": arms, "combining_helps": helps}


# ----------------------------------------------------------------------------- diagnostics
@cache
def _cost_model() -> CostModel:
    return CostModel.from_config()


def etf_cost_pct(product: str) -> float:
    """The round trip with the equity STT share replaced by ETF STT, 0.001% on the sell leg (delivery only:
    an intraday ETF sale pays the equity intraday rate, so MIS is unchanged)."""
    if product != "CNC":
        return mm.cost_pct(product)
    cm = _cost_model()
    bd = cm.round_trip(rc.NOTIONAL, "CNC")
    paisa = Decimal("0.01")
    stt = bd.components["stt"]
    etf_stt = (rc.NOTIONAL * ETF_STT_SELL_PCT / 100).quantize(paisa, rounding=ROUND_HALF_UP)
    gst = Decimal("0")
    if "stt" in cm.rates.gst_base_components:
        gst = (cm.rates.gst_pct / 100 * (etf_stt - stt)).quantize(paisa, rounding=ROUND_HALF_UP)
    return float((bd.total_cost - stt + etf_stt + gst) / rc.NOTIONAL * 100)


def _atr(ctx: StudyContext) -> dict[date, float]:
    def build() -> dict[date, float]:
        n = ctx.nifty[np.isfinite(ctx.nifty[["high", "low", "close"]]).all(axis=1)]
        a = wilder_atr(n["high"].to_numpy(), n["low"].to_numpy(), n["close"].to_numpy(), ATR_PERIOD)
        return {d: float(x) for d, x in zip(n.index, a.to_numpy(), strict=True) if math.isfinite(x)}
    return _memo(ctx, "atr", build)


def atr_stop_trade(ctx: StudyContext, t: TradeRow, cost: float) -> TradeRow | None:
    """``t`` with a stop at entry open - 2.5 x ATR14 through D-1, exiting at the stop when a session's low
    touches it (at the open when a later session opens through it); None without an ATR or a usable bar."""
    k = ctx.pos[t.entry_date]
    atr = _atr(ctx).get(ctx.sessions[k - 1]) if k else None
    if atr is None:
        return None
    o = float(ctx.nifty.at[t.entry_date, "open"])
    stop = o - ATR_STOP_MULT * atr
    for j in range(k, ctx.pos[t.exit_date] + 1):
        d = ctx.sessions[j]
        lo, op = float(ctx.nifty.at[d, "low"]), float(ctx.nifty.at[d, "open"])
        if not math.isfinite(lo) or (j > k and not math.isfinite(op)):
            return None
        if lo <= stop:
            px = stop if j == k else min(op, stop)
            g = (px / o - 1.0) * 100.0
            return TradeRow(t.signal_date, t.entry_date, d, g, g - cost, t.flagged)
    g = t.gross
    return TradeRow(t.signal_date, t.entry_date, t.exit_date, g, g - cost, t.flagged)


def regime_at(ctx: StudyContext, d: date) -> str:
    """``index_regime`` over NIFTY closes through D-1: up, down or undefined."""
    k = ctx.pos[d]
    if k == 0:
        return "undefined"
    closes = ctx.nifty["close"].to_numpy(dtype="float64")[max(0, k - INDEX_SMA - RISING_LOOKBACK):k]
    r = index_regime(list(closes), ctx.sessions[k - 1])
    return "undefined" if r is None else ("up" if r.up else "down")


def _structural(ctx: StudyContext) -> dict[str, list[date]]:
    return _memo(ctx, "structural", lambda: rc.bb.load_structural_ex_dates(ctx.conn))


def _top_traded(ctx: StudyContext) -> dict[date, list[str]]:
    """Per session in the high-beta window (and the one before): the ``TOP_TRADED`` non-ETF stocks by median
    close x volume over their 20 sessions to that day (20 required), highest first."""
    def build() -> dict[date, list[str]]:
        lo, hi = HIGH_BETA_WINDOW
        k0 = max(0, next((i for i, d in enumerate(ctx.sessions) if d >= lo), len(ctx.sessions)) - 1)
        first = ctx.sessions[k0] if ctx.sessions else lo
        rows = ctx.conn.execute(
            f"""
            SELECT d, symbol, med_tv FROM (
                SELECT symbol, d, median("close" * volume) OVER w AS med_tv, count(*) OVER w AS n
                FROM bars_1d WHERE strpos(symbol, ' ') = 0
                WINDOW w AS (PARTITION BY symbol ORDER BY d
                             ROWS BETWEEN {mm.LIQUID_WINDOW - 1} PRECEDING AND CURRENT ROW)
            ) WHERE n = {mm.LIQUID_WINDOW} AND d BETWEEN ? AND ?
            QUALIFY row_number() OVER (PARTITION BY d ORDER BY med_tv DESC, symbol) <= {_TV_FETCH}
            """, [first, hi]).fetchall()
        by_day: dict[date, list[tuple[float, str]]] = {}
        for d, sym, tv in rows:
            by_day.setdefault(mm._as_date(d), []).append((-float(tv), sym))
        out = {}
        for d, lst in by_day.items():
            names = [s for _, s in sorted(lst) if not mm.hv.is_etf(s, ctx.etf_symbols)]
            if len(names) < TOP_TRADED and len(lst) == _TV_FETCH:
                raise RuntimeError(f"{d}: ETFs exhausted the {_TV_FETCH}-row traded-value fetch")
            out[d] = names[:TOP_TRADED]
        return out
    return _memo(ctx, "top_traded", build)


def high_beta_legs(ctx: StudyContext, d1: date) -> list[str]:
    """The top quintile by 250-session NIFTY beta (returns through ``d1``, structural ex-date returns dropped)
    among the ``TOP_TRADED`` names as of ``d1``, highest beta first."""
    def build() -> list[str]:
        names = _top_traded(ctx).get(d1, [])
        k1 = ctx.pos.get(d1)
        if not names or k1 is None or k1 < BETA_SESSIONS:
            return []
        days = ctx.sessions[k1 - BETA_SESSIONS:k1 + 1]
        nc = ctx.nifty["close"].reindex(days).to_numpy(dtype="float64")
        rn = nc[1:] / nc[:-1] - 1.0
        _, close = _stock_panel(ctx)
        panel = close.reindex(index=days, columns=names)
        ex = _structural(ctx)
        betas: list[tuple[float, str]] = []
        for sym in names:
            c = panel[sym].to_numpy(dtype="float64")
            r = c[1:] / c[:-1] - 1.0
            exd = set(ex.get(sym, ()))
            ok = np.isfinite(r) & np.isfinite(rn) & np.array([d not in exd for d in days[1:]])
            if ok.sum() < BETA_MIN_PAIRS:
                continue
            x, y = rn[ok], r[ok]
            var = float(np.var(x, ddof=1))
            if var > 0:
                betas.append((float(np.cov(x, y, ddof=1)[0, 1]) / var, sym))
        betas.sort(key=lambda b: (-b[0], b[1]))
        return [s for _, s in betas[:max(1, len(betas) // QUINTILE)]] if betas else []
    return _memo(ctx, ("high_beta", d1), build)


def basket_trade(ctx: StudyContext, d: date, hold: int) -> tuple[date, float, int, int] | None:
    """Equal-weight gross % of the high-beta legs from open(D) to the close of the ``hold``-th session, as
    ``(exit, gross, legs, legs dropped for a structural ex-date in [D, exit])``; None without a leg."""
    k = ctx.pos[d]
    if k == 0 or k + hold - 1 >= len(ctx.sessions):
        return None
    exit_d = ctx.sessions[k + hold - 1]
    legs = high_beta_legs(ctx, ctx.sessions[k - 1])
    opens, close = _stock_panel(ctx)
    ex = _structural(ctx)
    gross, dropped = [], 0
    for sym in legs:
        if any(d <= x <= exit_d for x in ex.get(sym, ())):
            dropped += 1
            continue
        try:
            o, c = float(opens.at[d, sym]), float(close.at[exit_d, sym])
        except KeyError:
            continue
        if math.isfinite(o) and math.isfinite(c) and o > 0 and c > 0:
            gross.append((c / o - 1.0) * 100.0)
    if not gross:
        return None
    return exit_d, statistics.fmean(gross), len(gross), dropped


def high_beta_arm(ctx: StudyContext, name: str, hold: int) -> dict[str, Any]:
    cost = mm.cost_pct("CNC")                        # one CNC round trip per leg at the research notional
    days = signal_days(ctx, name, HIGH_BETA_WINDOW)
    built = {d: b for d in days if (b := basket_trade(ctx, d, hold)) is not None}
    kept = mm.non_overlapping(list(built), ctx.sessions, hold)
    rows = [TradeRow(d, d, built[d][0], built[d][1], built[d][1] - cost) for d in kept]
    return {"window": _window(HIGH_BETA_WINDOW), "cost_pct_per_leg": cost, "signals": len(days),
            "no_legs": len(days) - len(built), "legs": sum(built[d][2] for d in kept),
            "legs_dropped_ex_date": sum(built[d][3] for d in kept), "summary": summary(rows)}


def _group(rows: Sequence[TradeRow], key) -> dict[str, Any]:
    groups: dict[Any, list[TradeRow]] = {}
    for t in rows:
        groups.setdefault(key(t), []).append(t)
    return {str(k): summary(v) for k, v in sorted(groups.items(), key=lambda kv: str(kv[0]))}


def diagnostics(name: str, ctx: StudyContext) -> dict[str, Any]:
    """Decides nothing. Computed on the confirm window's signals (oil: all signals)."""
    hold, product = mm.HOLD[name], mm.PRODUCT[name]
    window = confirm_window(name, ctx)
    rows, _ = trades_for(ctx, name, window)
    horizons = {}
    for h in HORIZONS:
        hp = "MIS" if h == 1 else "CNC"          # a one-session hold is a same-day round trip
        h_rows, _ = trades_for(ctx, name, window, hold=h, cost=mm.cost_pct(hp))
        horizons[str(h)] = {"product": hp, **summary(h_rows)}
    cost = mm.cost_pct(product)
    stopped = [atr_stop_trade(ctx, t, cost) for t in rows]
    stop_rows = [t for t in stopped if t is not None]
    etf = etf_cost_pct(product)
    out: dict[str, Any] = {
        "window": _window(window),
        "horizons": horizons,
        "regime_at_d_minus_1": _group(rows, lambda t: regime_at(ctx, t.entry_date)),
        "by_year": _group(rows, lambda t: t.entry_date.year),
        "atr_stop": {"mult": ATR_STOP_MULT, "period": ATR_PERIOD, "no_atr_or_bar": len(rows) - len(stop_rows),
                     "stopped": sum(s.exit_date != t.exit_date for s, t in zip(stopped, rows, strict=True)
                                    if s is not None),
                     "summary": summary(stop_rows)},
        "etf_cost": {"cost_pct": etf, "summary": summary(
            [TradeRow(t.signal_date, t.entry_date, t.exit_date, t.gross, t.gross - etf, t.flagged) for t in rows])},
        "high_beta_basket": high_beta_arm(ctx, name, hold),
    }
    if name in ("us_rebound", "us_follow"):
        n_rows, n_counts = trades_for(ctx, f"{name}@ixic", window)
        out["nasdaq"] = {"summary": summary(n_rows), "counts": n_counts}
    return out


# ----------------------------------------------------------------------------- run
def run(ctx: StudyContext) -> dict[str, Any]:
    gate = {k: v for k, v in ctx.gate.items() if k != "clean_days"} | {"clean_days": len(ctx.clean)}

    def span(s: pd.Series | pd.DataFrame) -> dict[str, Any]:
        idx = list(s.dropna().index) if isinstance(s, pd.Series) else list(s.index)
        return {"n": len(idx), "first": idx[0] if idx else None, "last": idx[-1] if idx else None}

    meta = {
        "script": "scripts/macro_mood_study.py", "registration": f"{mm.FAMILY}, registered 2026-11-10",
        "generated_at": datetime.now().isoformat(timespec="seconds"), "db": ctx.db, "external": str(ctx.external),
        "manifest": ctx.manifest, "family_n": mm.N_TRIALS, "alpha": mm.ALPHA, "fold_pass_min": mm.FOLD_PASS_MIN,
        "min_trades": mm.MIN_TRADES, "perm_seed": mm.PERM_SEED, "notional_inr": str(rc.NOTIONAL),
        "cost_round_trip_pct": {"MIS": mm.cost_pct("MIS"), "CNC": mm.cost_pct("CNC")},
        "excluded": _window(mm.EXCLUDED), "nifty_source": f"^NSEI before {NIFTY_SWITCH}, bars_1d {INDEX_SYMBOL!r} from it",
        "samples": {n: {k: _window(w) for k, w in mm.SAMPLES[n].items()} for n in HYPOTHESES},
    }
    data = {"nsei_gate": gate, "oi_coverage": ctx.oi_coverage,
            "series": {"nifty": span(ctx.nifty), "spx": span(ctx.spx), "ixic": span(ctx.ixic),
                       "brent": span(ctx.brent), "fii_long_share": span(ctx.fii)}}
    return {"meta": meta, "data": data, "hypotheses": {n: hypothesis(n, ctx) for n in HYPOTHESES},
            "diagnostics": {"note": "Diagnostics decide nothing.", **{n: diagnostics(n, ctx) for n in HYPOTHESES}}}


# ----------------------------------------------------------------------------- rendering
def _num(v: Any, fmt: str = "+.4f") -> str:
    return "-" if v is None else format(v, fmt)


def _w(w: list | None) -> str:
    return "none" if w is None else f"{w[0] or 'first'} -> {w[1] or 'open'}"


def _sum_line(s: dict[str, Any]) -> str:
    hit = "-" if s["hit_rate_net"] is None else f"{s['hit_rate_net'] * 100:.1f}%"
    return (f"n={s['n']} mean gross {_num(s['mean_gross'])} mean net {_num(s['mean_net'])} "
            f"median net {_num(s['median_net'])} hit {hit}")


def _verdict_line(v: dict[str, Any]) -> str:
    return (f"n={v['n']} mean net {_num(v['mean_net'])} median net {_num(v['median_net'])} "
            f"perm p {_num(v['perm_p'], '.4f')} fold pass {_num(v['fold_pass'], '.2f')} "
            f"halves {_num(v['half1_mean'])}/{_num(v['half2_mean'])} drop-top-3 {_num(v['drop_top3_mean'])}")


def _trade_list(rows: list[dict[str, Any]]) -> str:
    return ", ".join(f"{t['entry_date']}->{t['exit_date']} {t['net']:+.2f}" for t in rows) or "none"


def render(doc: dict[str, Any]) -> str:
    m, data = doc["meta"], doc["data"]
    g = data["nsei_gate"]
    out = [f"MACRO MOOD STUDY ({m['registration']})",
           f"db {m['db']}  external {m['external']}",
           f"N={m['family_n']} alpha={m['alpha']} fold_pass_min={m['fold_pass_min']:.0%} "
           f"min_trades={m['min_trades']}  round trip MIS {m['cost_round_trip_pct']['MIS']:.4f}% "
           f"CNC {m['cost_round_trip_pct']['CNC']:.4f}% at Rs {m['notional_inr']}",
           f"NIFTY: {m['nifty_source']}", "",
           f"DATA GATE (^NSEI): ok={g['ok']}  overlap {g['overlap_days']} days ({g['overlap_first']} -> "
           f"{g['overlap_last']}), {g['mismatches']} open mismatches; failed years {g['failed_years'] or 'none'}"]
    for s in g["mismatch_sample"][:5]:
        out.append(f"  mismatch {s['date']}: yahoo {s['yahoo_open']:.4f} bars_1d {s['bars_1d_open']:.2f}")
    dropped = ", ".join(f"{y} {c['dropped']}/{c['sessions']}" for y, c in g["dropped_by_year"].items() if c["dropped"])
    out.append(f"  dropped pre-switch opens by year: {dropped or 'none'}")
    out += ["", "OI COVERAGE by year (NIFTY sessions / OI files on disk / parsed / sessions without OI)"]
    for y, c in data["oi_coverage"].items():
        flag = "  <- UNPARSED FILES" if c["unparsed"] else ""
        out.append(f"  {y}: {c['nifty_sessions']} / {c['oi_files']} / {c['oi_parsed']} / "
                   f"{c['sessions_without_oi']}{flag}")
    out += ["", "VERDICTS (confirm windows; the only evidence)"]
    for n, h in doc["hypotheses"].items():
        out.append(f"  {n:<16} {h['label'].upper():<12} [{_w(h['confirm_window'])}] hold {h['hold']} "
                   f"{h['product']} cost {h['cost_pct']:.4f}%  ({h['label_reason']})")
        out.append(f"    {_verdict_line(h['verdict'])}  population {h['population_n']}  counts {h['counts']}")
        if n == "oil_drop":
            out.append(f"    all {h['labels']['all']} / unflagged {h['labels']['unflagged']}: "
                       f"{_verdict_line(h['verdict_unflagged'])}")
        if n == "combined":
            for a, s in h["arms"].items():
                out.append(f"    arm {a:<18} {_sum_line(s)}")
            out.append(f"    combining helps: {h['combining_helps']}  (first signal-capable {h['first_signal_capable']})")
    out += ["", "seen (not evidence): examined before registration; reported, never counted"]
    for n, h in doc["hypotheses"].items():
        s = h[SEEN_KEY]
        if s is not None:
            out.append(f"  {n:<16} seen (not evidence) [{_w(s['window'])}] {_sum_line(s['summary'])}")
            out.append(f"    trades: {_trade_list(s['trades'])}")
    out += ["", f"EXCLUDED WINDOW {_w(_window(mm.EXCLUDED))} (the motivating case; never sampled)"]
    for n, h in doc["hypotheses"].items():
        out.append(f"  {n:<16} {_trade_list(h['excluded_window']['trades'])}")
    out += ["", f"DIAGNOSTICS ({doc['diagnostics']['note'].lower()})"]
    for n in HYPOTHESES:
        dg = doc["diagnostics"][n]
        out.append(f"  [{n}] window {_w(dg['window'])}")
        for h, s in dg["horizons"].items():
            out.append(f"    horizon {h:>2} {s['product']}: {_sum_line(s)}")
        for k, s in dg["regime_at_d_minus_1"].items():
            out.append(f"    regime {k:<9} {_sum_line(s)}")
        for k, s in dg["by_year"].items():
            out.append(f"    year {k} {_sum_line(s)}")
        a = dg["atr_stop"]
        out.append(f"    {a['mult']}xATR{a['period']} stop: stopped {a['stopped']}, no ATR {a['no_atr_or_bar']}; "
                   f"{_sum_line(a['summary'])}")
        out.append(f"    ETF cost {dg['etf_cost']['cost_pct']:.4f}%: {_sum_line(dg['etf_cost']['summary'])}")
        if "nasdaq" in dg:
            out.append(f"    Nasdaq for S&P: {_sum_line(dg['nasdaq']['summary'])}")
        b = dg["high_beta_basket"]
        out.append(f"    high-beta basket [{_w(b['window'])}]: signals {b['signals']} no legs {b['no_legs']} "
                   f"legs {b['legs']} dropped (ex-date) {b['legs_dropped_ex_date']}; {_sum_line(b['summary'])}")
    return rc.ascii_text("\n".join(out))


# ----------------------------------------------------------------------------- CLI
def default_out(db: Path) -> Path:
    """``<data>/reports/macro_mood_<ts>.json`` beside the snapshot's ``<data>/research`` dir."""
    return Path(db).resolve().parent.parent / "reports" / f"macro_mood_{datetime.now():%Y%m%dT%H%M%S}.json"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="macro_mood_study", description=__doc__.splitlines()[0])
    ap.add_argument("--db", type=Path, required=True, help="research snapshot (data/research/*.duckdb)")
    ap.add_argument("--external", type=Path, default=DEFAULT_EXTERNAL, help="fetch_macro_external.py output dir")
    ap.add_argument("--etf-list", type=Path, default=mm.ETF_LIST, help="NSE ETF list (Symbol column)")
    ap.add_argument("--out", type=Path, default=None, help="JSON path; the .md is written beside it")
    args = ap.parse_args(argv)
    try:
        if reason := rc.market_hours_refusal():
            raise Refusal(reason)
        conn = rc.open_snapshot(args.db)
        try:
            manifest = verify_manifest(args.external)
            etfs = mm.load_etf_symbols(args.etf_list)
            doc = run(load_context(conn, args.external, etfs, manifest=manifest, db=args.db))
        finally:
            conn.close()
    except Refusal as exc:
        print(f"macro_mood_study: REFUSED: {rc.ascii_text(str(exc))}", file=sys.stderr)
        return 2
    out = args.out or default_out(args.db)
    text = render(doc)
    md = rc.write_reports(doc, f"# MACRO MOOD STUDY\n\n```\n{text}\n```\n", out)
    print(text)
    print(f"JSON -> {out}\nMD   -> {md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
