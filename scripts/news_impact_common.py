#!/usr/bin/env python
"""News impact study (``edge/news_impact``): the shared data layer.

The registered constants, the first-score loader, the timing rules, the symbol resolver, the market panel with its
benchmarks, the trade constructors and the per-hypothesis verdict that the mapper and the study script build on.
Every threshold, window and sample date is the registration's; changing one is a new registration. Evidence only;
nothing here is wired into the engine.

Conventions:
* Returns are in percent units (1.25 is +1.25%), as in ``research_common``; ``cost()`` is in the same units.
* Times are IST. t is the ``at`` of a cluster's first successful News Analyst score. The entry session D is the first
  regular session whose 09:00 cut-off is after t: the open is the pre-open auction's price, whose order entry closes
  about 09:07, so a later score could not trade it. P0 is the close of the last regular session whose 15:30 close is
  before t. Regular sessions come from the NSE calendar with special sessions treated as closed.
* The panel's sessions are the NIFTY 50 bar dates that are regular sessions, so holds, D-1, liquidity windows and
  populations never count a special session's bar. D-1 is the panel session before D.
* A hold of h sessions runs from open(entry) to close(exit), the entry session counted as 1.
* Read-only: ``state.db`` through a ``mode=ro`` URI, the research DuckDB as the caller opened it.
"""

from __future__ import annotations

import importlib.util
import math
import sqlite3
import statistics
import sys
from bisect import bisect_left, bisect_right
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Literal
from zoneinfo import ZoneInfo

_ROOT = Path(__file__).resolve().parents[1]
_REPO_SRC = str(_ROOT / "src")
if _REPO_SRC not in sys.path:  # pragma: no cover - loose-script shim
    sys.path.insert(0, _REPO_SRC)

import engine  # noqa: E402,F401,I001  native import-order guard (engine._preload), keep FIRST
import duckdb  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from engine.datafeeds.news_pipeline import EntityResolver, NewsCluster  # noqa: E402
from engine.intelligence.agents import news_analyst  # noqa: E402
from engine.learning.multiplicity import permutation_pvalue, registration_alpha  # noqa: E402
from engine.learning.validate import fold_pass_min  # noqa: E402

_SCRIPTS = str(Path(__file__).resolve().parent)
if _SCRIPTS not in sys.path:  # pragma: no cover - loose-script shim
    sys.path.insert(0, _SCRIPTS)


def _load_sibling(name: str) -> Any:
    mod_name = f"mt_{name}"
    if mod_name in sys.modules:
        return sys.modules[mod_name]
    spec = importlib.util.spec_from_file_location(mod_name, Path(__file__).with_name(f"{name}.py"))
    mod = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = mod
    spec.loader.exec_module(mod)
    return mod


mmf = _load_sibling("macro_mood_forward")
ncs = _load_sibling("news_content_score")
#: The very module instances ``macro_mood_forward`` uses, so the liquidity cache is shared rather than duplicated.
mm = mmf.mm
rc = mmf.rc

Unavailable = mmf.Unavailable
load_calendar = mmf.load_calendar
regular_sessions = mmf.regular_sessions
load_theme_lists = mmf.load_theme_lists
load_etf_symbols = mm.load_etf_symbols
NOTIONAL = rc.NOTIONAL

IST = ZoneInfo("Asia/Kolkata")
INDEX_SYMBOL = "NIFTY 50"
UNCLASSIFIED = "UNCLASSIFIED"

# ----------------------------------------------------------------------------- registered constants
FAMILY = "edge/news_impact"
N_TRIALS = 6
ALPHA = registration_alpha(N_TRIALS)
FOLD_PASS_MIN = fold_pass_min(N_TRIALS)
MIN_TRADES = 30
MIN_INFO_OBS = 300

MATERIAL = 0.6
POSITIVE = 0.4
NEGATIVE = -0.4

#: Sessions held, counting the entry session as 1.
HOLD = {"stock_next_open": 10, "stock_pullback": 10, "domain_companies": 10, "domain_groups": 10,
        "global_sectors": 5, "info": 10}
PULLBACK_SESSIONS = 5
MIN_PEERS = 5
MIN_BASKET = 3
BETA_SESSIONS = 250
BETA_MIN_PAIRS = 200
BOOT_DRAWS = 2000
#: Sessions per moving block, the hold: consecutive sessions' 10-session returns overlap.
BOOT_BLOCK = 10
SPLIT_DRAWS = 10000
SEED = 20261110
#: Every D-1 before this date reads this ``sector_map`` snapshot: earlier ones used an older label scheme and left
#: most names unclassified. A sector is not an outcome, so a later snapshot adds no hindsight.
SECTOR_SNAPSHOT_FLOOR = date(2026, 11, 8)

#: The pre-open auction's order entry closes about 09:07; 09:00 leaves a margin, as the macro mood study's cut-off does.
ENTRY_CUTOFF = time(9, 0)
CLOSE_TIME = time(15, 30)

#: Inclusive entry-session windows.
SAMPLES = {"A": (date(2026, 7, 29), date(2026, 10, 23)), "excluded": (date(2026, 10, 26), date(2026, 11, 10)),
           "B": (date(2026, 11, 11), date(2027, 2, 26))}

NEVER_TYPES = {"pump_promo_suspect"}
#: Ex-dividend drops are not in the stored bars, so dividend stories would read as price falls.
STOCK_SKIP_TYPES = {"pump_promo_suspect", "dividend_corp_action"}
DOMAIN_MARKET_TYPES = {"govt_program", "sector_policy", "regulatory_policy"}
GLOBAL_TYPES = {"global_market", "macro_data", "disruption"}
HORIZONS = (1, 3, 5, 10, 20)

# Reasons a constructor makes no trade.
NO_PRIOR_SESSION = "no_prior_session"
NOT_LIQUID = "not_liquid"
FEW_PEERS = "few_peers"
NO_BETA = "no_beta"
VOIDED = "voided"
NO_BAR = "no_bar"
OPEN = "open"
UNFILLED = "unfilled"
SMALL_BASKET = "small_basket"


# ----------------------------------------------------------------------------- first scores
@dataclass(frozen=True)
class Story:
    cluster_id: str
    at: datetime
    call_id: str
    model: str | None
    scope: str
    event_type: str
    sentiment: float
    materiality: float
    entities: tuple[str, ...]
    headline: str | None


def load_first_scores(state_db: Path, limit: datetime) -> tuple[list[Story], dict[str, int]]:
    """The first successful News Analyst score of every cluster first scored before ``limit``, oldest first.

    The first score is the earliest call (by ``at``, then rowid) whose parsed output scores the cluster; a later
    re-score never replaces it. Its headline comes from that call's archived prompt. Counts: ``calls`` read,
    ``calls_at_unparsed`` (no usable ``at``, so never placed), ``calls_unparsed`` (output not parseable),
    ``headline_missing`` and ``stories``. A missing or unreadable store raises ``Unavailable``."""
    if limit.tzinfo is None:
        raise ValueError("limit must be tz-aware")
    con = mmf._ro(state_db)
    try:
        return _read_first_scores(con, limit)
    except sqlite3.Error as exc:
        raise Unavailable(f"{state_db}: agent_calls unreadable ({type(exc).__name__}: {exc})") from exc
    finally:
        con.close()


def _read_first_scores(con: sqlite3.Connection, limit: datetime) -> tuple[list[Story], dict[str, int]]:
    index = con.execute("SELECT rowid, call_id, at, model FROM agent_calls WHERE agent_id = ? AND ok = 1",
                        (news_analyst.AGENT_ID,)).fetchall()
    counts = {"calls": 0, "calls_at_unparsed": 0, "calls_unparsed": 0, "headline_missing": 0, "stories": 0}
    calls = []
    for rowid, call_id, raw_at, model in index:
        t = ncs._parse_ts(raw_at)
        if t is None:
            counts["calls_at_unparsed"] += 1
        elif t < limit:
            calls.append((t, rowid, call_id, model))
    calls.sort(key=lambda c: (c[0], c[1]))
    first: dict[str, tuple[datetime, str, Any, Any]] = {}
    for t, rowid, call_id, model in calls:      # one output at a time: the table grows by ~100 calls a day
        counts["calls"] += 1
        row = con.execute("SELECT output_json FROM agent_calls WHERE rowid = ?", (rowid,)).fetchone()
        try:
            scores, _ = news_analyst.parse_output(row[0] if row else None)
        except (ValueError, TypeError):
            counts["calls_unparsed"] += 1
            continue
        for s in scores:
            first.setdefault(s.cluster_id, (t, call_id, model, s))
    contexts: dict[str, dict[str, str]] = {}
    out: list[Story] = []
    for cid, (t, call_id, model, s) in first.items():
        if call_id not in contexts:
            blob = con.execute("SELECT context_gz FROM agent_calls WHERE call_id = ?", (call_id,)).fetchone()
            contexts[call_id] = ncs._context_headlines(blob[0] if blob else None)
        head = contexts[call_id].get(cid)
        if head is None:
            counts["headline_missing"] += 1
        out.append(Story(cid, t, call_id, None if model is None else str(model), s.scope, s.event_type,
                         float(s.sentiment), float(s.materiality), tuple(str(e) for e in s.entities), head))
    out.sort(key=lambda s: (s.at, s.cluster_id))
    counts["stories"] = len(out)
    return out, counts


def story_kind(s: Story) -> str | None:
    """``stock`` for a stock-scope story of any materiality (dividend and promotion stories excluded); ``domain``
    for a material sector or theme story, or a material market story about a programme or a policy; ``global`` for
    a material market story about global markets, macro data or a disruption; otherwise None."""
    if s.event_type in NEVER_TYPES:
        return None
    if s.scope == "stock":
        return None if s.event_type in STOCK_SKIP_TYPES else "stock"
    if not s.materiality >= MATERIAL:
        return None
    if s.scope in ("sector", "theme"):
        return "domain"
    if s.scope == "market":
        if s.event_type in DOMAIN_MARKET_TYPES:
            return "domain"
        if s.event_type in GLOBAL_TYPES:
            return "global"
    return None


# ----------------------------------------------------------------------------- timing
def _ist(t: datetime) -> datetime:
    if t.tzinfo is None:
        raise ValueError("timestamps must be tz-aware")
    return t.astimezone(IST)


def entry_session(t: datetime, sessions: Sequence[date]) -> date | None:
    """D: the first of ``sessions`` whose 09:00 IST cut-off is strictly after ``t``. ``sessions`` is ascending regular
    sessions reaching past t; None when t is after the last one's cut-off."""
    local = _ist(t)
    first = local.date() if local.time() < ENTRY_CUTOFF else local.date() + timedelta(days=1)
    i = bisect_left(sessions, first)
    return sessions[i] if i < len(sessions) else None


def pre_news_session(t: datetime, sessions: Sequence[date]) -> date | None:
    """The session whose close is P0: the last of ``sessions`` whose 15:30 IST close is strictly before ``t``. A
    score stamped exactly at 15:30 has not seen that day's close. None when no listed session closed before t."""
    local = _ist(t)
    last = local.date() if local.time() > CLOSE_TIME else local.date() - timedelta(days=1)
    j = bisect_right(sessions, last) - 1
    return sessions[j] if j >= 0 else None


# ----------------------------------------------------------------------------- symbols
_NO_TIME = datetime(2000, 1, 1, tzinfo=IST)


def build_resolver(conn: duckdb.DuckDBPyConnection) -> EntityResolver:
    """The engine's resolver over the copy's own tables: ``entity_aliases`` with ``EntityResolver.load``'s rule (an
    alias with any curated row loads only its curated symbols), the latest ``sector_map`` snapshot, ``theme_map``
    keywords, and no universe filter. A copy without aliases would resolve nothing, so it refuses."""
    try:
        alias_rows = conn.execute("SELECT alias, tradingsymbol, source FROM entity_aliases").fetchall()
        sectors = conn.execute("SELECT symbol, sector FROM sector_map "
                               "WHERE as_of = (SELECT max(as_of) FROM sector_map)").fetchall()
        themes = conn.execute("SELECT theme, keywords FROM theme_map").fetchall()
    except duckdb.Error as exc:
        raise Unavailable(f"resolver tables unreadable ({type(exc).__name__}: {exc})") from exc
    if not alias_rows:
        raise Unavailable("entity_aliases is empty: no story could resolve to a symbol")
    every: dict[str, set[str]] = {}
    curated: dict[str, set[str]] = {}
    for alias, sym, source in alias_rows:
        every.setdefault(str(alias), set()).add(str(sym))
        if source == "curated":
            curated.setdefault(str(alias), set()).add(str(sym))
    aliases = {a: tuple(sorted(curated.get(a) or syms)) for a, syms in every.items()}
    return EntityResolver(aliases=aliases, sector_map={str(s): str(sec) for s, sec in sectors},
                          theme_map={str(t): [str(k) for k in (kws or [])] for t, kws in themes}, universe=None)


def stock_symbols(resolver: EntityResolver, s: Story) -> frozenset[str]:
    """The symbols the resolver reads in the story's headline, with its scored entities as extra texts."""
    c = NewsCluster(cluster_id=s.cluster_id, representative=s.headline or "", source_domains=[], first_seen=s.at,
                    last_seen=s.at)
    extra = [e for e in s.entities if isinstance(e, str) and e.strip()]
    return frozenset(resolver.resolve(c, extra_texts=extra).symbols)


def company_symbols(resolver: EntityResolver, names: Sequence[str]) -> tuple[list[str | None], int]:
    """One symbol per mapped company name, or None when the name resolves to no symbol, to several, or contains an
    ambiguous alias: the resolver never guesses. The int counts the Nones."""
    out: list[str | None] = []
    for name in names:
        sym = None
        if isinstance(name, str) and name.strip():
            c = NewsCluster(cluster_id="company", representative="", source_domains=[], first_seen=_NO_TIME,
                            last_seen=_NO_TIME)
            r = resolver.resolve(c, extra_texts=[name])
            if len(r.symbols) == 1 and not any(u.reason == "ambiguous" for u in r.unresolved):
                sym = r.symbols[0]
        out.append(sym)
    return out, sum(s is None for s in out)


# ----------------------------------------------------------------------------- the panel
@dataclass
class Panel:
    conn: duckdb.DuckDBPyConnection
    sessions: list[date]
    pos: dict[date, int]
    sym_col: dict[str, int]
    opens: np.ndarray
    highs: np.ndarray
    lows: np.ndarray
    closes: np.ndarray
    volumes: np.ndarray
    nifty: pd.DataFrame
    etfs: frozenset[str]
    themes: dict[str, frozenset[str]]
    structural: dict[str, list[date]]
    sector_asof: list[date]
    sector_of: dict[date, dict[str, str]]
    sector_members: dict[date, dict[str, frozenset[str]]]
    #: Bar dates the calendar does not count as regular sessions, left out of every liquidity window.
    skip_days: frozenset[date] = frozenset()
    notes: list[str] = field(default_factory=list)
    cache: dict[Any, Any] = field(default_factory=dict)


def load_panel(conn: duckdb.DuckDBPyConnection, start: date, as_of: date, cal: Any, *,
               etfs: frozenset[str] | None = None, themes: dict[str, frozenset[str]] | None = None) -> Panel:
    """Sessions from ``start`` through ``as_of`` (NIFTY 50 bar dates that are regular sessions in ``cal``) with every
    equity's open, high, low, close and volume on them, the structural ex-dates, every ``sector_map`` snapshot, the
    frozen theme lists and the ETF list. ``etfs`` and ``themes`` default to the macro mood study's files.

    A beta needs ``BETA_SESSIONS`` returns to D-1 inside the panel, so ``start`` must lie more than that many
    sessions before the first D-1 wanted. A store that cannot be read, a copy without ``corp_actions`` or
    ``sector_map`` (which would silently void nothing and give every stock no peers), or one without the
    ``SECTOR_SNAPSHOT_FLOOR`` snapshot (which the sector rule cannot do without) raises ``Unavailable``."""
    etfs = load_etf_symbols() if etfs is None else frozenset(etfs)
    themes = load_theme_lists() if themes is None else {str(k): frozenset(v) for k, v in themes.items()}
    try:
        nifty_rows = conn.execute(
            'SELECT d, CAST("open" AS DOUBLE), CAST(high AS DOUBLE), CAST(low AS DOUBLE), CAST("close" AS DOUBLE) '
            "FROM bars_1d WHERE symbol = ? AND d >= ? AND d <= ? ORDER BY d", [INDEX_SYMBOL, start, as_of]).fetchall()
        rows = [r for r in nifty_rows if cal.is_trading_day(mm._as_date(r[0]))]
        if not rows:
            raise Unavailable(f"bars_1d has no {INDEX_SYMBOL!r} rows on regular sessions in [{start}, {as_of}]")
        bar_days = {mm._as_date(d) for (d,) in conn.execute(
            "SELECT DISTINCT d FROM bars_1d WHERE d <= ?", [as_of]).fetchall()}
        covered = {y for y in {d.year for d in bar_days} if mmf._calendar_has_year(cal, y)}
        skip_days = frozenset(d for d in bar_days if d.year in covered and not cal.is_trading_day(d))
        sessions = [mm._as_date(r[0]) for r in rows]
        bars = conn.execute(
            'SELECT symbol, d, CAST("open" AS DOUBLE) AS o, CAST(high AS DOUBLE) AS h, CAST(low AS DOUBLE) AS l, '
            'CAST("close" AS DOUBLE) AS c, CAST(volume AS DOUBLE) AS v FROM bars_1d '
            "WHERE strpos(symbol, ' ') = 0 AND d >= ? AND d <= ?", [sessions[0], as_of]).df()
        conn.execute("SELECT count(*) FROM corp_actions").fetchone()
        structural = rc.bb.load_structural_ex_dates(conn)
        sm = conn.execute("SELECT as_of, symbol, sector FROM sector_map").fetchall()
    except duckdb.Error as exc:
        raise Unavailable(f"snapshot unreadable ({type(exc).__name__}: {exc})") from exc
    nifty = pd.DataFrame({k: [float(r[i]) if r[i] is not None else np.nan for r in rows]
                          for i, k in enumerate(("open", "high", "low", "close"), start=1)},
                         index=pd.Index(sessions, name="date"))
    pos = {d: i for i, d in enumerate(sessions)}
    syms = sorted(set(bars["symbol"]))
    sym_col = {s: j for j, s in enumerate(syms)}
    shape = (len(sessions), len(syms))
    mats = {k: np.full(shape, np.nan) for k in ("o", "h", "l", "c", "v")}
    if len(bars):
        days = [mm._as_date(x) for x in pd.to_datetime(bars["d"]).dt.date]
        r_idx = np.array([pos.get(d, -1) for d in days], dtype=np.int64)
        c_idx = bars["symbol"].map(sym_col).to_numpy(dtype=np.int64)
        keep = r_idx >= 0                      # a special session's bar is not a session
        for k, mat in mats.items():
            mat[r_idx[keep], c_idx[keep]] = bars[k].to_numpy(dtype="float64")[keep]
    sector_of: dict[date, dict[str, str]] = defaultdict(dict)
    members: dict[date, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    for a, sym, sector in sm:
        a = mm._as_date(a)
        sector_of[a][str(sym)] = str(sector)
        members[a][str(sector)].add(str(sym))
    sector_members = {a: {s: frozenset(v) for s, v in secs.items()} for a, secs in members.items()}
    if SECTOR_SNAPSHOT_FLOOR not in sector_members:
        raise Unavailable(f"sector_map has no {SECTOR_SNAPSHOT_FLOOR} snapshot; every D-1 before that date reads it")
    return Panel(conn, sessions, pos, sym_col, mats["o"], mats["h"], mats["l"], mats["c"], mats["v"], nifty, etfs,
                 themes, structural, sorted(sector_members), dict(sector_of), sector_members, skip_days=skip_days)


def liquid(p: Panel, d_prev: date) -> frozenset[str]:
    """The macro mood study's liquid universe as of ``d_prev``, special sessions left out of its windows."""
    key = ("liquid", d_prev)
    if key not in p.cache:
        p.cache[key] = frozenset(mm.liquid_universe(p.conn, d_prev, p.etfs, p.skip_days))
    return p.cache[key]


def sector_snapshot(p: Panel, d_prev: date) -> date:
    """The ``sector_map`` snapshot a D-1 reads: ``SECTOR_SNAPSHOT_FLOOR``'s for every D-1 before that date, else the
    latest dated on or before D-1. Every sector lookup goes through here."""
    if SECTOR_SNAPSHOT_FLOOR not in p.sector_members:
        raise Unavailable(f"the panel has no sector_map snapshot of {SECTOR_SNAPSHOT_FLOOR}; the sector rule needs it")
    if d_prev < SECTOR_SNAPSHOT_FLOOR:
        return SECTOR_SNAPSHOT_FLOOR
    # the floor snapshot is dated on or before d_prev here, so the search always lands on or after it
    return p.sector_asof[bisect_right(p.sector_asof, d_prev) - 1]


def peers(p: Panel, sym: str, d_prev: date) -> frozenset[str]:
    """The liquid members of ``sym``'s sector in the snapshot ``sector_snapshot`` gives for D-1, ``sym`` excluded.
    None for a stock that snapshot does not classify, or classifies ``UNCLASSIFIED``."""
    snap = sector_snapshot(p, d_prev)
    sector = p.sector_of[snap].get(sym)
    if sector is None or sector == UNCLASSIFIED:
        return frozenset()
    return (p.sector_members[snap].get(sector, frozenset()) & liquid(p, d_prev)) - {sym}


def group_members(p: Panel, group: str, d_prev: date) -> frozenset[str]:
    """A theme's frozen list, or a sector's names in the snapshot ``sector_snapshot`` gives for D-1, liquid as of
    D-1."""
    if group in p.themes:
        base = p.themes[group]
    else:
        if group == UNCLASSIFIED:
            return frozenset()
        base = p.sector_members[sector_snapshot(p, d_prev)].get(group, frozenset())
    return base & liquid(p, d_prev)


def _has_ex(p: Panel, sym: str, lo: date, hi: date, *, lo_inclusive: bool = True) -> bool:
    """Whether ``sym`` has a structural ex-date in [lo, hi] (or (lo, hi])."""
    xs = p.structural.get(sym)
    if not xs:
        return False
    k = bisect_left(xs, lo) if lo_inclusive else bisect_right(xs, lo)
    return k < len(xs) and xs[k] <= hi


def _returns(p: Panel) -> np.ndarray:
    """Daily close-to-close returns in %, one row per session (row 0 has none). A leg spanning a structural ex-date,
    a missing bar or a non-positive close is NaN."""
    key = ("returns",)
    if key not in p.cache:
        c = np.where(p.closes > 0, p.closes, np.nan)
        r = np.full(c.shape, np.nan)
        if len(c) > 1:
            with np.errstate(invalid="ignore", divide="ignore"):
                r[1:] = (c[1:] / c[:-1] - 1.0) * 100.0
        for sym, xs in p.structural.items():
            j = p.sym_col.get(sym)
            if j is None:
                continue
            for x in xs:
                i = bisect_left(p.sessions, x)        # the first session on or after the ex-date
                if 0 < i < len(p.sessions):
                    r[i, j] = np.nan
        p.cache[key] = r
    return p.cache[key]


def universe_daily(p: Panel, s: date) -> float | None:
    """The equal-weight close-to-close % return on session ``s`` of the names liquid as of the session before it,
    ex-date legs out; None for the panel's first session or when no leg has a return."""
    key = ("universe_daily", s)
    if key in p.cache:
        return p.cache[key]
    k = p.pos.get(s)
    out = None
    if k is not None and k > 0:
        cols = [p.sym_col[n] for n in liquid(p, p.sessions[k - 1]) if n in p.sym_col]
        vals = _returns(p)[k, cols]
        vals = vals[np.isfinite(vals)]
        out = float(vals.mean()) if vals.size else None
    p.cache[key] = out
    return out


def _universe_series(p: Panel) -> np.ndarray:
    key = ("universe_series",)
    if key not in p.cache:
        vals = [universe_daily(p, s) for s in p.sessions]
        p.cache[key] = np.array([np.nan if v is None else v for v in vals], dtype="float64")
    return p.cache[key]


def beta(p: Panel, names: frozenset[str], d_prev: date) -> float | None:
    """The OLS slope of the equal-weight basket of ``names`` (a single stock is a basket of one) on the liquid
    universe, daily close-to-close, over the ``BETA_SESSIONS`` sessions to ``d_prev`` inclusive. A day counts when
    the universe and at least one name have a return; fewer than ``BETA_MIN_PAIRS`` such days, or a universe without
    variance, gives None."""
    names = frozenset(names)
    key = ("beta", names, d_prev)
    if key in p.cache:
        return p.cache[key]
    out = None
    k = p.pos.get(d_prev)
    cols = sorted(p.sym_col[n] for n in names if n in p.sym_col)
    if k is not None and cols:
        lo = max(0, k - BETA_SESSIONS + 1)
        sub = _returns(p)[lo:k + 1][:, cols]
        n_ok = np.isfinite(sub).sum(axis=1)
        with np.errstate(invalid="ignore", divide="ignore"):
            basket = np.where(n_ok > 0, np.nansum(sub, axis=1) / np.maximum(n_ok, 1), np.nan)
        u = _universe_series(p)[lo:k + 1]
        ok = np.isfinite(basket) & np.isfinite(u)
        if int(ok.sum()) >= BETA_MIN_PAIRS:
            x, y = u[ok], basket[ok]
            xc = x - x.mean()
            den = float((xc * xc).sum())
            if den > 0:
                out = float((xc * (y - y.mean())).sum() / den)
    p.cache[key] = out
    return out


# ----------------------------------------------------------------------------- trades
@dataclass(frozen=True)
class Trade:
    key: str
    signal_date: date
    entry_date: date
    exit_date: date
    gross: float
    bench: float
    excess: float
    net: float
    legs: int = 1
    fill_price: float | None = None
    #: A basket's members, or a sector benchmark's peers, dropped from the trade: a structural ex-date inside the
    #: hold, or a missing bar.
    voided_legs: int = 0
    no_bar_legs: int = 0


def cost() -> float:
    """One CNC round trip at the research notional, in %, spread included."""
    return mm.cost_pct("CNC")


def _legs(p: Panel, i: int, j: int, d_prev: date) -> dict[str, float | str]:
    """Each name liquid as of ``d_prev``: its open(session i) -> close(session j) %, or ``voided`` (a structural
    ex-date in [session i, session j]) or ``no_bar`` (a missing or non-positive bar)."""
    key = ("legs", i, j, d_prev)
    if key in p.cache:
        return p.cache[key]
    lo, hi = p.sessions[i], p.sessions[j]
    out: dict[str, float | str] = {}
    for sym in sorted(liquid(p, d_prev)):
        if _has_ex(p, sym, lo, hi):
            out[sym] = VOIDED
            continue
        col = p.sym_col.get(sym)
        o = p.opens[i, col] if col is not None else np.nan
        c = p.closes[j, col] if col is not None else np.nan
        out[sym] = float((c / o - 1.0) * 100.0) if math.isfinite(o) and math.isfinite(c) and o > 0 and c > 0 \
            else NO_BAR
    p.cache[key] = out
    return out


def _peer_bench(legs: dict[str, float | str], peer_set: frozenset[str]) -> tuple[float, int, int] | None:
    """The peers' equal-weight return over the window with the counts of peers dropped as voided and as no-bar;
    None when fewer than ``MIN_PEERS`` peers have a usable leg, since a thinned benchmark is mostly noise."""
    vals = [legs[s] for s in sorted(peer_set)]
    usable = [v for v in vals if isinstance(v, float)]
    if len(usable) < MIN_PEERS:
        return None
    return statistics.fmean(usable), sum(v == VOIDED for v in vals), sum(v == NO_BAR for v in vals)


def _universe_window(legs: dict[str, float | str]) -> float | None:
    vals = [v for v in legs.values() if isinstance(v, float)]
    return statistics.fmean(vals) if vals else None


def _entry(p: Panel, d: date) -> int | str:
    """``d``'s panel row, or why it has none: before the panel or its first row (no D-1), past the data (open),
    or a regular session without a NIFTY bar."""
    k = p.pos.get(d)
    if k is None:
        if d > p.sessions[-1]:
            return OPEN
        return NO_PRIOR_SESSION if d < p.sessions[0] else NO_BAR
    return NO_PRIOR_SESSION if k == 0 else k


def _check_hold(hold: int) -> None:
    if hold < 1:
        raise ValueError(f"hold must be >= 1, got {hold}")


def stock_trade(p: Panel, sym: str, d: date, hold: int, bench: Literal["sector", "beta"]) -> Trade | str:
    """Buy ``sym`` at open(D), sell at the close of the ``hold``-th session, judged against its sector peers' equal-
    weight return over the same window (``sector``) or beta times the liquid universe's (``beta``). Peers and beta
    are fixed as of D-1; a peer whose leg is voided or has no bar leaves the benchmark and is counted, and fewer
    than ``MIN_PEERS`` peers left is ``few_peers``."""
    _check_hold(hold)
    if bench not in ("sector", "beta"):
        raise ValueError(f"bench must be 'sector' or 'beta', got {bench!r}")
    k = _entry(p, d)
    if isinstance(k, str):
        return k
    d_prev = p.sessions[k - 1]
    if sym not in liquid(p, d_prev):
        return NOT_LIQUID
    peer_set: frozenset[str] = frozenset()
    b: float | None = None
    if bench == "sector":
        peer_set = peers(p, sym, d_prev)
        if len(peer_set) < MIN_PEERS:
            return FEW_PEERS
    else:
        b = beta(p, frozenset({sym}), d_prev)
        if b is None:
            return NO_BETA
    j = k + hold - 1
    if j >= len(p.sessions):
        return OPEN
    legs = _legs(p, k, j, d_prev)
    own = legs[sym]
    if isinstance(own, str):
        return own
    voided = no_bar = 0
    if bench == "sector":
        pb = _peer_bench(legs, peer_set)
        if pb is None:
            return FEW_PEERS
        bench_ret, voided, no_bar = pb
    else:
        u = _universe_window(legs)
        if u is None:
            return NO_BAR
        bench_ret = b * u
    excess = own - bench_ret
    return Trade(sym, d, d, p.sessions[j], own, bench_ret, excess, excess - cost(), voided_legs=voided,
                 no_bar_legs=no_bar)


def pullback_trade(p: Panel, sym: str, d: date, p0_session: date, hold: int) -> Trade | str:
    """A limit buy at P0 = close(``p0_session``) resting over D and the next ``PULLBACK_SESSIONS`` - 1 sessions. It
    fills on the first session whose low is at or below P0, at min(that session's open, P0), and is held ``hold``
    sessions from the fill session, judged against the peers (fixed as of D-1) from that session's open. Peers
    drop out of the benchmark as in ``stock_trade``, with the same ``MIN_PEERS`` floor on those left.

    A structural ex-date after ``p0_session`` voids the signal from the session it reaches: bars after it are in
    other units than P0, so a split's lower prices must not read as a fill. ``open`` while the order rests or the
    position is held past the data."""
    _check_hold(hold)
    k = _entry(p, d)
    if isinstance(k, str):
        return k
    if not p0_session < d:
        raise ValueError(f"P0's session {p0_session} must be before the entry session {d}")
    d_prev = p.sessions[k - 1]
    if sym not in liquid(p, d_prev):
        return NOT_LIQUID
    peer_set = peers(p, sym, d_prev)
    if len(peer_set) < MIN_PEERS:
        return FEW_PEERS
    col = p.sym_col.get(sym)
    i0 = p.pos.get(p0_session)
    p0 = p.closes[i0, col] if col is not None and i0 is not None else np.nan
    if not (math.isfinite(p0) and p0 > 0):
        return NO_BAR
    fill_i, fill = None, math.nan
    for i in range(k, k + PULLBACK_SESSIONS):
        if i >= len(p.sessions):
            return OPEN
        if _has_ex(p, sym, p0_session, p.sessions[i], lo_inclusive=False):
            return VOIDED
        lo, o = p.lows[i, col], p.opens[i, col]
        if not (math.isfinite(lo) and math.isfinite(o) and lo > 0 and o > 0):
            return NO_BAR                      # whether it would have filled that session is unknown
        if lo <= p0:
            fill_i, fill = i, float(min(o, p0))
            break
    if fill_i is None:
        return UNFILLED
    j = fill_i + hold - 1
    if j >= len(p.sessions):
        return OPEN
    if _has_ex(p, sym, p0_session, p.sessions[j], lo_inclusive=False):
        return VOIDED
    c = p.closes[j, col]
    if not (math.isfinite(c) and c > 0):
        return NO_BAR
    gross = (float(c) / fill - 1.0) * 100.0
    pb = _peer_bench(_legs(p, fill_i, j, d_prev), peer_set)
    if pb is None:
        return FEW_PEERS
    bench_ret, voided, no_bar = pb
    excess = gross - bench_ret
    return Trade(sym, d, p.sessions[fill_i], p.sessions[j], gross, bench_ret, excess, excess - cost(),
                 fill_price=fill, voided_legs=voided, no_bar_legs=no_bar)


def basket_trade(p: Panel, group: str, d: date, hold: int) -> Trade | str:
    """Buy ``group``'s members liquid as of D-1 equal-weight at open(D), sell at the close of the ``hold``-th
    session, judged on beta-excess: the basket's return minus its beta (over the same members) times the liquid
    universe's return over the window. A member whose leg is voided or has no bar is dropped and counted, and fewer
    than ``MIN_BASKET`` usable legs is ``small_basket``; the basket pays one round trip."""
    _check_hold(hold)
    k = _entry(p, d)
    if isinstance(k, str):
        return k
    d_prev = p.sessions[k - 1]
    names = group_members(p, group, d_prev)
    if len(names) < MIN_BASKET:
        return SMALL_BASKET
    b = beta(p, names, d_prev)
    if b is None:
        return NO_BETA
    j = k + hold - 1
    if j >= len(p.sessions):
        return OPEN
    legs = _legs(p, k, j, d_prev)
    vals = [legs[s] for s in sorted(names)]
    usable = [v for v in vals if isinstance(v, float)]
    voided = sum(v == VOIDED for v in vals)
    no_bar = sum(v == NO_BAR for v in vals)
    if len(usable) < MIN_BASKET:
        return SMALL_BASKET
    u = _universe_window(legs)
    if u is None:
        return NO_BAR
    gross = statistics.fmean(usable)
    bench_ret = b * u
    excess = gross - bench_ret
    return Trade(group, d, d, p.sessions[j], gross, bench_ret, excess, excess - cost(), legs=len(usable),
                 voided_legs=voided, no_bar_legs=no_bar)


def keep_non_overlapping(signals: Sequence[tuple[str, date]], sessions: Sequence[date],
                         hold: int) -> list[tuple[str, date]]:
    """One position per key at a time: each key's signals through ``macro_mood_common.non_overlapping``, so a
    signal inside a kept one's hold (its exit session included) is skipped and a repeat is one signal. Sorted by
    (date, key)."""
    sessions = list(sessions)
    by_key: dict[str, list[date]] = defaultdict(list)
    for key, d in signals:
        by_key[key].append(d)
    out = [(key, d) for key, days in by_key.items() for d in mm.non_overlapping(days, sessions, hold)]
    return sorted(out, key=lambda kd: (kd[1], kd[0]))


# ----------------------------------------------------------------------------- verdict
def verdict(trades: Sequence[Trade], population_gross: Sequence[float], hold: int) -> dict:
    """The macro mood study's verdict rule at this family's N, on one sample.

    ``insufficient`` below ``MIN_TRADES``; else ``refuted`` when mean net <= 0; else ``supported`` when every
    criterion holds; else ``null``. The permutation test compares the trades' median gross excess with same-size
    draws from ``population_gross``; everything else reads net. The data gate is the caller's.
    ``margin_floor_promotable`` is ``cpcv_report``'s promotion decision, a diagnostic only."""
    rows = sorted(trades, key=lambda t: (t.entry_date, t.signal_date, t.key))
    n = len(rows)
    nets = [t.net for t in rows]
    excess = [t.excess for t in rows]
    pop = list(population_gross)
    # A NaN draw median never compares >= the sample's, so a NaN would read as evidence.
    for what, vals in (("population_gross", pop), ("trade excess", excess), ("trade net", nets)):
        if not all(math.isfinite(v) for v in vals):
            raise ValueError(f"{what} has non-finite values")
    # The population holds every trade's own construct on its entry session, so a shorter one is a caller error.
    if n > len(pop):
        raise ValueError(f"population_gross has {len(pop)} values, fewer than the {n} trades")
    out: dict[str, Any] = {"label": "insufficient", "n": n, "mean_net": None, "median_net": None,
                           "perm_p": None, "fold_pass": None, "half1_mean": None, "half2_mean": None,
                           "drop_top3_mean": None, "margin_floor_promotable": None, "criteria": None,
                           "cpcv": None}
    if not n:
        return out
    # Halves split at the lower median entry date: trades on one date always share a half, and an empty half
    # fails the criterion.
    mid = statistics.median_low(t.entry_date for t in rows)
    first = [t.net for t in rows if t.entry_date <= mid]
    second = [t.net for t in rows if t.entry_date > mid]
    out["mean_net"] = statistics.fmean(nets)
    out["median_net"] = statistics.median(nets)
    out["half1_mean"] = statistics.fmean(first) if first else None
    out["half2_mean"] = statistics.fmean(second) if second else None
    out["drop_top3_mean"] = statistics.fmean(sorted(nets)[:-3]) if n > 3 else None
    out["perm_p"] = permutation_pvalue(excess, pop, seed=SEED)
    cv_rows = [SimpleNamespace(fill_date=t.entry_date, net={hold: t.net}) for t in rows]
    rep = rc.bb.cpcv_report(cv_rows, hold, cost(), trial_count=N_TRIALS)
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
