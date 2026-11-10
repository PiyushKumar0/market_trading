#!/usr/bin/env python
"""Macro mood study (``edge/macro_mood``, registered 2026-11-10): the forward looks and the comparison.

The four forward hypotheses are news tone and policy push, each read two ways: from the News Analyst's headline
scores (``state.db`` ``agent_calls``) and from the study's own content scores (variant C of
``news_content_score.py``). Each one accrues closed trades from its start and is judged once, at its 30th closed
trade. One with fewer than 30 closed trades at ``REGISTERED_FROM`` + 18 months is insufficient. The pre-declared
headlines-versus-content comparison runs over the study's own H and C scores and decides nothing.

Read-only everywhere. ``state.db`` and the content file are opened through ``mode=ro`` URIs, because the capture
and the scorer write the content file concurrently; the market is read from a research snapshot. The script writes
only its report files and always exits 0; an unreadable store prints ``UNAVAILABLE``.

No lookahead. tone(D) and D's policy candidates come from D's news window, [09:00 IST on the previous regular
NSE session, 09:00 IST on D), so weekend and holiday news counts for the next session and every item belongs to
exactly one session. Regular sessions come from ``config/calendar`` (muhurat excluded), never from the snapshot's
bars, since D's own bar may not exist yet. Items are timed by ``agent_calls.at`` for headlines and by the cluster
representative's ``first_captured_at`` for content (the anchor the scorer's cut-off uses). Baskets hold members
liquid as of D-1, under the ``sector_map`` snapshot in force on D-1.
"""

from __future__ import annotations

import argparse
import calendar
import json
import math
import sqlite3
import statistics
import sys
import traceback
from bisect import bisect_left, bisect_right
from collections import defaultdict
from collections.abc import Callable, Iterable, Sequence
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parent))
import research_common as rc  # noqa: E402,I001  engine's native import order first
import macro_mood_common as mm  # noqa: E402

import duckdb  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import yaml  # noqa: E402
from scipy.stats import rankdata  # noqa: E402
from engine.datafeeds.news_pipeline import title_tokens  # noqa: E402
from engine.intelligence.agents import news_analyst  # noqa: E402

ncs = rc._load_script("news_content_score")
ncc = rc._load_script("news_content_capture")

TradeRow = mm.TradeRow
IST = ZoneInfo("Asia/Kolkata")
_ROOT = Path(__file__).resolve().parents[1]
THEME_LISTS_PATH = _ROOT / "config" / "research" / "macro_mood" / "theme_lists.yaml"
DEFAULT_STATE_DB = ncs.DEFAULT_STATE_DB
DEFAULT_CONTENT_DB = ncs.DEFAULT_CONTENT_DB
INDEX_SYMBOL = "NIFTY 50"
NAMES = ("news_tone_headline", "policy_headline", "news_tone_content", "policy_content")


def _add_months(d: date, n: int) -> date:
    y, m = divmod(d.month - 1 + n, 12)
    y += d.year
    return date(y, m + 1, min(d.day, calendar.monthrange(y, m + 1)[1]))


# ----------------------------------------------------------------------------- registered rules
CUTOFF = time(9, 0)
#: The registered headline key is the (model, prompt_version) of the first ok News Analyst call from here on.
KEY_FROM = datetime.combine(mm.REGISTERED_FROM, time(0, 0), tzinfo=IST)
#: A forward hypothesis with fewer than 30 closed trades by this date is insufficient.
DEADLINE = _add_months(mm.REGISTERED_FROM, 18)
TONE_HOLD = mm.HOLD["news_tone"]
POLICY_HOLD = mm.HOLD["policy"]
TONE_MIN_HISTORY, TONE_MAX_HISTORY = 20, 60
#: Top third: strictly above numpy's linear 2/3 quantile of the previous tones.
TONE_PCTL = 200.0 / 3.0
BOOT_DRAWS = 2000
BOOT_BLOCK = 5
#: The policy comparison answers only when each side has at least this many events.
MIN_EVENTS = 10
#: Headline tone history and the headline "seen" report start here; it is reported, never evidence.
SEEN = mm.SAMPLES["news_tone"]["seen"]
#: A complete content window: an error-free poll of every feed in [09:00, 10:00) on the previous regular session's
#: date and in [08:00, 09:00) on D (the host sleeps at night, so only the window's two ends are judged).
COMPLETE_FIRST_DAY = (time(9, 0), time(10, 0))
COMPLETE_ON_D = (time(8, 0), time(9, 0))
FEEDS: tuple[str, ...] = tuple(ncc.FEEDS)
SCORE_MODEL, MAP_MODEL = ncs.SCORE_MODEL, ncs.MAP_MODEL
VOCAB: tuple[str, ...] = ncs.VOCAB
#: ``config/themes.yaml`` keywords frozen at registration; a test holds them equal to the file, so an edit there
#: stops the suite instead of silently changing the theme map.
THEME_KEYWORDS: dict[str, tuple[str, ...]] = {
    "ev_mobility": ("electric vehicle", "EV", "e-mobility", "battery", "charging infrastructure", "lithium"),
    "defence": ("defence", "defense", "DRDO", "ordnance", "missile", "military procurement"),
    "railways": ("railway", "railways", "rail corridor", "metro rail", "rolling stock"),
    "infra_capex": ("infrastructure", "capex", "highway", "port", "airport", "construction order"),
    "renewables": ("solar", "wind energy", "renewable", "green hydrogen", "green energy"),
    "psu_disinvestment": ("disinvestment", "divestment", "PSU stake sale", "privatisation", "privatization"),
    "data_ai": ("data centre", "data center", "artificial intelligence", "semiconductor", "chip fab"),
    "make_in_india": ("PLI scheme", "production linked incentive", "make in india", "manufacturing incentive"),
}
_THEME_TOKENS = {t: tuple(tuple(title_tokens(k)) for k in kws) for t, kws in THEME_KEYWORDS.items()}


class Unavailable(RuntimeError):
    """A store cannot be read; the CLI prints UNAVAILABLE and exits 0."""


# ----------------------------------------------------------------------------- windows and themes
def load_calendar(calendar_dir: Path | None = None) -> Any:
    """The NSE calendar with special sessions (muhurat, DR drills) treated as closed: regular sessions only."""
    return rc.NSECalendar(Path(calendar_dir) if calendar_dir is not None else rc.config_dir() / "calendar",
                          rc.Clock(), include_special=False)


def regular_sessions(cal: Any, lo: date, hi: date) -> list[date]:
    """The calendar's regular sessions in [lo, hi]. A year in the range with no session at all has no calendar
    file; the windows cannot be drawn there, so that raises ``Unavailable`` rather than reading as no news."""
    for y in range(lo.year, hi.year + 1):
        d = date(y, 1, 1)
        while d.year == y and not cal.is_trading_day(d):
            d += timedelta(days=1)
        if d.year != y:
            raise Unavailable(f"the NSE calendar has no {y} sessions (config/calendar/{y}.yaml missing); "
                              "the news windows need it")
    return [lo + timedelta(days=i) for i in range((hi - lo).days + 1) if cal.is_trading_day(lo + timedelta(days=i))]


def session_for(at: datetime, regular: Sequence[date]) -> date | None:
    """The regular session D whose news window [09:00 IST on the previous regular session, 09:00 IST on D) holds
    ``at``: the first session on or after the first calendar day whose 09:00 is strictly after ``at``.

    ``regular`` is ascending and starts one session before the first window wanted (that session's own window
    is not drawn). None when ``at`` falls before the first window or after the last."""
    local = at.astimezone(IST)
    first_nine = local.date() if local.time() < CUTOFF else local.date() + timedelta(days=1)
    i = bisect_left(regular, first_nine)
    return regular[i] if 0 < i < len(regular) else None


def _has_phrase(tokens: Sequence[str], phrase: Sequence[str]) -> bool:
    n = len(phrase)
    return n > 0 and any(tuple(tokens[i:i + n]) == tuple(phrase) for i in range(len(tokens) - n + 1))


def theme_hits(texts: Iterable[str | None]) -> set[str]:
    """Themes whose keyword occurs whole-word (the resolver's rule) in any one of ``texts``; each text is matched
    on its own, so a phrase never spans a headline and its summary."""
    out: set[str] = set()
    for text in texts:
        tokens = title_tokens(text or "")
        for theme, phrases in _THEME_TOKENS.items():
            if any(_has_phrase(tokens, p) for p in phrases):
                out.add(theme)
    return out


def load_theme_lists(path: Path = THEME_LISTS_PATH) -> dict[str, frozenset[str]]:
    try:
        doc = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        themes = {str(t): frozenset(str(s) for s in v["symbols"]) for t, v in doc["themes"].items()}
    except (OSError, ValueError, KeyError, TypeError, AttributeError, yaml.YAMLError) as exc:
        raise Unavailable(f"theme lists unreadable: {path} ({type(exc).__name__}: {exc})") from exc
    stray = sorted(set(themes) - set(VOCAB))
    if stray:
        raise Unavailable(f"theme lists name themes outside the mapper vocabulary: {stray}")
    return themes


# ----------------------------------------------------------------------------- stores
@dataclass(frozen=True)
class Scored:
    """One cluster's counted score, anchored at its window time. ``texts`` feed the theme map."""

    key: str
    at: datetime
    scope: str | None
    event_type: str | None
    sentiment: float | None
    materiality: float | None
    texts: tuple[str, ...] = ()


def _ro(path: Path) -> sqlite3.Connection:
    p = Path(path)
    if not p.is_file():
        raise Unavailable(f"{p}: no such file")
    try:
        # the capture and the scorer commit into the content file while this reads it: wait out their locks
        con = sqlite3.connect(f"{p.resolve().as_uri()}?mode=ro", uri=True, timeout=30)
        con.execute("SELECT count(*) FROM sqlite_master").fetchone()
    except sqlite3.Error as exc:
        raise Unavailable(f"{p}: unreadable ({type(exc).__name__}: {exc})") from exc
    return con


def _num(x: Any) -> float | None:
    return float(x) if isinstance(x, int | float) and math.isfinite(float(x)) else None


@dataclass
class HeadlineData:
    key: tuple[Any, Any] | None
    scores: list[Scored]
    counts: dict[str, int]


def load_headline(state_db: Path, limit: datetime, forward_open: datetime, seen_open: datetime) -> HeadlineData:
    """The first ok News Analyst score of every cluster scored before ``limit``. A cluster whose first score
    came from a call with another key than the registered one is dropped; such calls are counted, apart for the
    forward windows (from ``forward_open``) and the seen ones (from ``seen_open``)."""
    con = _ro(state_db)
    try:
        return _read_headline(con, limit, forward_open, seen_open)
    except sqlite3.Error as exc:
        raise Unavailable(f"{state_db}: agent_calls unreadable ({type(exc).__name__}: {exc})") from exc
    finally:
        con.close()


def _read_headline(con: sqlite3.Connection, limit: datetime, forward_open: datetime,
                   seen_open: datetime) -> HeadlineData:
    index = con.execute(
        "SELECT rowid, call_id, at, model, prompt_version FROM agent_calls WHERE agent_id = ? AND ok = 1",
        (news_analyst.AGENT_ID,)).fetchall()
    counts = {"engine_calls_excluded": 0, "engine_calls_excluded_seen": 0, "engine_at_unparsed": 0,
              "engine_outputs_unparsed": 0, "engine_clusters_excluded": 0, "headline_no_text": 0}
    calls = []
    for rowid, call_id, raw_at, model, pv in index:
        t = ncs._parse_ts(raw_at)
        if t is None:
            counts["engine_at_unparsed"] += 1
        elif t < limit:
            calls.append((t, rowid, call_id, (model, pv)))
    calls.sort(key=lambda c: (c[0], c[1]))
    key = next((k for t, _, _, k in calls if t >= KEY_FROM), None)
    first: dict[str, tuple[datetime, str, tuple[Any, Any], Any]] = {}
    for t, rowid, call_id, k in calls:      # one output at a time: the table grows by ~100 calls a day
        if key is not None and k != key:        # no key before the first call on the registration date
            if t >= forward_open:
                counts["engine_calls_excluded"] += 1
            elif t >= seen_open:
                counts["engine_calls_excluded_seen"] += 1
        row = con.execute("SELECT output_json FROM agent_calls WHERE rowid = ?", (rowid,)).fetchone()
        try:
            scores, _ = news_analyst.parse_output(row[0] if row else None)
        except (ValueError, TypeError):
            counts["engine_outputs_unparsed"] += 1
            continue
        for s in scores:
            first.setdefault(s.cluster_id, (t, call_id, k, s))
    contexts: dict[str, dict[str, str]] = {}
    out: list[Scored] = []
    for cid, (t, call_id, k, s) in first.items():
        if k != key:
            counts["engine_clusters_excluded"] += 1
            continue
        texts: tuple[str, ...] = ()
        if ncs.meets_policy_bars(s.event_type, s.sentiment, s.materiality):
            if call_id not in contexts:
                blob = con.execute("SELECT context_gz FROM agent_calls WHERE call_id = ?", (call_id,)).fetchone()
                contexts[call_id] = ncs._context_headlines(blob[0] if blob else None)
            head = contexts[call_id].get(cid)
            if head is None:
                counts["headline_no_text"] += 1
            else:
                texts = (head,)
        out.append(Scored(cid, t, s.scope, s.event_type, _num(s.sentiment), _num(s.materiality), texts))
    return HeadlineData(key, out, counts)


@dataclass
class StudyData:
    scores: dict[str, list[Scored]]                 # variant -> scores
    mappings: dict[str, dict[str, list[str]]]       # source -> candidate key -> groups
    runs: list[tuple[datetime, str, Any]]
    pinned_last_ok: dict[str, date | None]
    counts: dict[str, int]
    missing_tables: list[str]


_STUDY_TABLES = ("items", "capture_runs", "clusters", "llm_calls", "scores", "mappings")


def load_content(content_db: Path, limit: datetime, as_of: date) -> StudyData:
    """The study's H and C scores of clusters captured before ``limit``, its mappings and its capture log. A
    score or mapping from a call logged under another model than the pinned one is dropped and counted."""
    con = _ro(content_db)
    counts = {"study_scores_excluded_model": 0, "study_mappings_excluded_model": 0, "study_at_unparsed": 0,
              "study_mappings_unparsed": 0}
    try:
        tables = {n for (n,) in con.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
        missing = [t for t in _STUDY_TABLES if t not in tables]
        day_end = datetime.combine(as_of + timedelta(days=1), time(0, 0), tzinfo=IST)
        models: dict[str, str] = {}
        last_ok: dict[str, date | None] = {SCORE_MODEL: None, MAP_MODEL: None}
        if "llm_calls" in tables:
            for call_id, raw_at, model, ok in con.execute("SELECT call_id, at, model, ok FROM llm_calls"):
                models[call_id] = model
                t = ncs._parse_ts(raw_at)
                if ok == 1 and model in last_ok and t is not None and t < day_end:
                    d = t.date()
                    last_ok[model] = d if last_ok[model] is None else max(last_ok[model], d)
        summaries: dict[str, str | None] = {}
        if "items" in tables:
            summaries = dict(con.execute("SELECT url, summary FROM items").fetchall())
        clusters: dict[str, tuple[datetime, str, str]] = {}
        if "clusters" in tables:
            for key, raw, url, title in con.execute(
                    "SELECT cluster_key, first_captured_at, representative_url, representative_title FROM clusters"):
                t = ncs._parse_ts(raw)
                if t is None:
                    counts["study_at_unparsed"] += 1
                elif t < limit:
                    clusters[key] = (t, url, title)
        scores: dict[str, list[Scored]] = {"H": [], "C": []}
        if "scores" in tables:
            for key, variant, scope, event_type, sent, mat, call_id in con.execute(
                    "SELECT cluster_key, variant, scope, event_type, sentiment, materiality, call_id FROM scores "
                    "ORDER BY cluster_key, variant"):
                if key not in clusters or variant not in scores:
                    continue
                if models.get(call_id) != SCORE_MODEL:
                    counts["study_scores_excluded_model"] += 1
                    continue
                t, url, title = clusters[key]
                texts = (title,) if variant == "H" else (title, summaries.get(url) or "")
                scores[variant].append(Scored(key, t, scope, event_type, _num(sent), _num(mat), texts))
        mappings: dict[str, dict[str, list[str]]] = {"headline": {}, "study_h": {}, "content": {}}
        if "mappings" in tables:
            for key, source, raw, call_id in con.execute(
                    "SELECT candidate_key, source, groups, call_id FROM mappings"):
                if source not in mappings:
                    continue
                if models.get(call_id) != MAP_MODEL:
                    counts["study_mappings_excluded_model"] += 1
                    continue
                try:
                    groups = json.loads(raw)
                except (TypeError, ValueError):
                    counts["study_mappings_unparsed"] += 1
                    continue
                if not isinstance(groups, list):
                    counts["study_mappings_unparsed"] += 1
                    continue
                mappings[source][key] = [g for g in groups if isinstance(g, str) and g in VOCAB]
        runs: list[tuple[datetime, str, Any]] = []
        if "capture_runs" in tables:
            for raw, feed, error in con.execute("SELECT run_at, feed, error FROM capture_runs"):
                t = ncs._parse_ts(raw)
                if t is not None:
                    runs.append((t, feed, error))
    except sqlite3.Error as exc:
        raise Unavailable(f"{content_db}: unreadable ({type(exc).__name__}: {exc})") from exc
    finally:
        con.close()
    return StudyData(scores, mappings, runs, last_ok, counts, missing)


# ----------------------------------------------------------------------------- the market
@dataclass(frozen=True)
class Basket:
    exit_date: date
    gross: float          # basket mean minus the liquid universe's mean, in %
    legs: int
    voided: int
    no_bar: int


@dataclass
class Market:
    conn: duckdb.DuckDBPyConnection
    sessions: list[date]
    pos: dict[date, int]
    nifty: pd.DataFrame
    etfs: frozenset[str]
    themes: dict[str, frozenset[str]]
    structural: dict[str, list[date]]
    sector_asof: list[date]
    sector_members: dict[date, dict[str, frozenset[str]]]
    day_row: dict[date, int]
    sym_col: dict[str, int]
    opens: np.ndarray
    closes: np.ndarray
    notes: list[str] = field(default_factory=list)
    cache: dict[Any, Any] = field(default_factory=dict)


def load_market(conn: duckdb.DuckDBPyConnection, as_of: date, etfs: frozenset[str],
                themes: dict[str, frozenset[str]]) -> Market:
    """The NIFTY session grid through ``as_of`` (every date with a NIFTY 50 bar) and the stock opens and closes
    from the session before the seen window on."""
    notes: list[str] = []
    try:
        rows = conn.execute('SELECT d, "open", "close" FROM bars_1d WHERE symbol = ? AND d <= ? ORDER BY d',
                            [INDEX_SYMBOL, as_of]).fetchall()
        if not rows:
            raise Unavailable(f"bars_1d has no {INDEX_SYMBOL!r} rows on or before {as_of}")
        sessions = [mm._as_date(r[0]) for r in rows]
        nifty = pd.DataFrame({"open": [float(r[1]) for r in rows], "close": [float(r[2]) for r in rows]},
                             index=pd.Index(sessions, name="date"))
        k0 = max(0, bisect_right(sessions, SEEN[0]) - 2)
        panel_days = sessions[k0:]
        day_row = {d: i for i, d in enumerate(panel_days)}
        bars = conn.execute(
            "SELECT symbol, d, \"open\", \"close\" FROM bars_1d WHERE strpos(symbol, ' ') = 0 AND d >= ? AND d <= ?",
            [panel_days[0], as_of]).fetchall()
        syms = sorted({b[0] for b in bars})
        sym_col = {s: j for j, s in enumerate(syms)}
        opens = np.full((len(panel_days), len(syms)), np.nan)
        closes = np.full((len(panel_days), len(syms)), np.nan)
        for sym, d, o, c in bars:
            i = day_row.get(mm._as_date(d))
            if i is not None:
                opens[i, sym_col[sym]] = float(o) if o is not None else np.nan
                closes[i, sym_col[sym]] = float(c) if c is not None else np.nan
        structural = rc.bb.load_structural_ex_dates(conn)
        try:
            sm = conn.execute("SELECT as_of, symbol, sector FROM sector_map").fetchall()
        except duckdb.Error:
            sm = []
            notes.append("sector_map unreadable: sector groups have no members")
    except duckdb.Error as exc:
        raise Unavailable(f"snapshot unreadable ({type(exc).__name__}: {exc})") from exc
    by_asof: dict[date, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    for a, sym, sector in sm:
        by_asof[mm._as_date(a)][str(sector)].add(str(sym))
    sector_members = {a: {s: frozenset(v) for s, v in secs.items()} for a, secs in by_asof.items()}
    return Market(conn, sessions, {d: i for i, d in enumerate(sessions)}, nifty, etfs, themes, structural,
                  sorted(sector_members), sector_members, day_row, sym_col, opens, closes, notes)


def _liquid(m: Market, d_prev: date) -> frozenset[str]:
    key = ("liquid", d_prev)
    if key not in m.cache:
        m.cache[key] = frozenset(mm.liquid_universe(m.conn, d_prev, m.etfs))
    return m.cache[key]


def members(m: Market, group: str, d_prev: date) -> frozenset[str]:
    """A theme's frozen list, or the sector's names in the ``sector_map`` snapshot with the latest as_of <= D-1."""
    if group in m.themes:
        return m.themes[group]
    i = bisect_right(m.sector_asof, d_prev) - 1
    if i < 0:
        return frozenset()
    return m.sector_members[m.sector_asof[i]].get(group, frozenset())


_VOID, _NO_BAR = "voided", "no_bar"


def _legs(m: Market, d: date, exit_d: date, d_prev: date) -> dict[str, float | str]:
    """Each liquid name's open(D) -> close(exit) %, or why it has none: a structural ex-date in [D, exit]
    voids it; a missing or non-positive bar leaves it without a return."""
    key = ("legs", d, exit_d)
    if key in m.cache:
        return m.cache[key]
    out: dict[str, float | str] = {}
    i, j = m.day_row.get(d), m.day_row.get(exit_d)
    for sym in _liquid(m, d_prev):
        if any(d <= x <= exit_d for x in m.structural.get(sym, ())):
            out[sym] = _VOID
            continue
        col = m.sym_col.get(sym)
        o = m.opens[i, col] if col is not None and i is not None else np.nan
        c = m.closes[j, col] if col is not None and j is not None else np.nan
        out[sym] = (c / o - 1.0) * 100.0 if math.isfinite(o) and math.isfinite(c) and o > 0 and c > 0 else _NO_BAR
    m.cache[key] = out
    return out


def basket(m: Market, group: str, d: date, hold: int = POLICY_HOLD) -> Basket | str:
    """Group ``group``'s equal-weight liquid basket from open(D) to the close of the ``hold``-th session, as its
    excess over the equal-weight liquid universe; otherwise why not: ``no_prior_session``, ``no_liquid_members``,
    ``open`` (the exit session is not in the snapshot yet) or ``no_usable_leg``."""
    key = ("basket", group, d, hold)
    if key in m.cache:
        return m.cache[key]
    k = m.pos[d]
    if k == 0:
        res: Basket | str = "no_prior_session"
    else:
        d_prev = m.sessions[k - 1]
        names = members(m, group, d_prev) & _liquid(m, d_prev)
        if not names:
            res = "no_liquid_members"
        elif k + hold - 1 >= len(m.sessions):
            res = "open"
        else:
            exit_d = m.sessions[k + hold - 1]
            legs = _legs(m, d, exit_d, d_prev)
            vals = [legs[s] for s in sorted(names)]
            gross = [v for v in vals if isinstance(v, float)]
            universe = [v for v in legs.values() if isinstance(v, float)]
            if not gross:
                res = "no_usable_leg"
            else:
                res = Basket(exit_d, statistics.fmean(gross) - statistics.fmean(universe), len(gross),
                             sum(v == _VOID for v in vals), sum(v == _NO_BAR for v in vals))
    m.cache[key] = res
    return res


# ----------------------------------------------------------------------------- tone
def tone_series(scores: Iterable[Scored], windows: Sequence[date]) -> pd.Series:
    """tone(D) for each regular session after ``windows[0]`` (the anchor, see ``session_for``): the mean of
    sentiment x materiality over the ``market``-scope scores in D's news window; NaN when there is none."""
    acc: dict[date, list[float]] = defaultdict(list)
    for s in scores:
        if s.scope != "market" or s.sentiment is None or s.materiality is None:
            continue
        d = session_for(s.at, windows)
        if d is not None:
            acc[d].append(s.sentiment * s.materiality)
    days = list(windows[1:])
    return pd.Series([statistics.fmean(acc[d]) if acc[d] else np.nan for d in days],
                     index=pd.Index(days, name="date"), dtype="float64")


def tone_signal(tone: pd.Series) -> pd.Series:
    """True where tone(D) is strictly above the 2/3 quantile of its previous non-missing tones, using at most
    60 of them and only once 20 exist. A missing tone never signals and never counts toward the history."""
    out = pd.Series(False, index=tone.index, dtype=bool)
    v = tone.dropna()
    vals = v.to_numpy(dtype="float64")
    for k in range(TONE_MIN_HISTORY, len(vals)):
        prev = vals[max(0, k - TONE_MAX_HISTORY):k]
        if vals[k] > np.percentile(prev, TONE_PCTL):
            out.loc[v.index[k]] = True
    return out


def _entry_ok(m: Market, d: date) -> bool:
    o = float(m.nifty.at[d, "open"])
    return math.isfinite(o) and o > 0


def _no_entry(m: Market, d: date) -> str | None:
    """Why calendar session ``d`` cannot enter from the snapshot: its NIFTY bar is not in yet (the snapshot ends
    before it) or is missing inside the snapshot. None when it has a bar."""
    if d in m.pos:
        return None
    return "entry_not_in_snapshot" if d > m.sessions[-1] else "no_entry_bar"


def tone_trades(m: Market, signal: pd.Series, lo: date | None, hi: date | None,
                cost: float) -> tuple[list[tuple[str, TradeRow]], list[dict[str, Any]], dict[str, int]]:
    """Kept NIFTY trades for the signal days in [lo, hi]: closed ones as rows, open ones (exit not yet in the
    snapshot) as entries; both block their window."""
    counts = {"signals": 0, "entry_not_in_snapshot": 0, "no_entry_bar": 0, "unconstructible": 0, "kept": 0,
              "open": 0}
    if lo is None:
        return [], [], counts
    built: dict[date, tuple[date, float]] = {}
    pending: set[date] = set()
    for d in signal.index[signal.to_numpy()]:
        if d < lo or (hi is not None and d > hi):
            continue
        counts["signals"] += 1
        if (why := _no_entry(m, d)) is not None:
            counts[why] += 1
            continue
        t = mm.index_trade(m.nifty, m.sessions, d, TONE_HOLD)
        if t is not None:
            built[d] = t
        elif m.pos[d] + TONE_HOLD - 1 >= len(m.sessions) and _entry_ok(m, d):
            pending.add(d)
        else:
            counts["unconstructible"] += 1
    kept = mm.non_overlapping(sorted(built.keys() | pending), m.sessions, TONE_HOLD)
    rows = [("", TradeRow(d, d, built[d][0], built[d][1], built[d][1] - cost)) for d in kept if d in built]
    open_ = [{"entry_date": d} for d in kept if d in pending]
    counts["kept"], counts["open"] = len(rows), len(open_)
    return rows, open_, counts


def tone_population(m: Market, lo: date) -> Callable[[date], list[float]]:
    def pop(cut: date) -> list[float]:
        out = []
        for d in m.sessions[bisect_right(m.sessions, lo - timedelta(days=1)):]:
            t = mm.index_trade(m.nifty, m.sessions, d, TONE_HOLD)
            if t is not None and t[0] <= cut:
                out.append(t[1])
        return out
    return pop


# ----------------------------------------------------------------------------- policy push
def policy_events(scores: Iterable[Scored], windows: Sequence[date], mapping: dict[str, list[str]],
                  themes: dict[str, frozenset[str]]) -> tuple[dict[date, dict[str, list[str]]], dict[str, int]]:
    """``D -> group -> candidate keys``: candidates meeting the policy bars in D's news window, each mapped to the
    union of its LLM groups and its theme keyword hits (themes with a frozen list only)."""
    events: dict[date, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
    counts = {"candidates": 0, "unmapped": 0, "no_group": 0, "theme_hit_without_list": 0, "no_session": 0}
    for s in scores:
        if not ncs.meets_policy_bars(s.event_type, s.sentiment, s.materiality):
            continue
        d = session_for(s.at, windows)
        if d is None:
            counts["no_session"] += 1
            continue
        counts["candidates"] += 1
        groups = set(mapping.get(s.key, ()))
        if s.key not in mapping:
            counts["unmapped"] += 1
        hits = theme_hits(s.texts)
        counts["theme_hit_without_list"] += len(hits - set(themes))
        groups |= hits & set(themes)
        if not groups:
            counts["no_group"] += 1
        for g in sorted(groups):
            events[d][g].append(s.key)
    return events, counts


def policy_trades(m: Market, events: dict[date, dict[str, list[str]]], lo: date | None, hi: date | None,
                  cost: float) -> tuple[list[tuple[str, TradeRow]], list[dict[str, Any]], dict[str, int],
                                        dict[tuple[str, date], Basket]]:
    """One position per group: each group's signal days in [lo, hi] through ``non_overlapping``. Returns the
    closed trades sorted by (entry date, group), the open ones, the counts and each kept trade's basket."""
    counts = {"signals": 0, "entry_not_in_snapshot": 0, "no_entry_bar": 0, "no_prior_session": 0,
              "no_liquid_members": 0, "no_usable_leg": 0, "kept": 0, "open": 0, "legs": 0, "legs_voided": 0,
              "legs_no_bar": 0}
    rows: list[tuple[str, TradeRow]] = []
    open_: list[dict[str, Any]] = []
    baskets: dict[tuple[str, date], Basket] = {}
    if lo is None:
        return rows, open_, counts, baskets
    by_group: dict[str, list[date]] = defaultdict(list)
    for d, groups in events.items():
        if d >= lo and (hi is None or d <= hi):
            for g in groups:
                by_group[g].append(d)
    for g in sorted(by_group):
        built: dict[date, Basket] = {}
        pending: set[date] = set()
        for d in sorted(by_group[g]):
            counts["signals"] += 1
            if (why := _no_entry(m, d)) is not None:
                counts[why] += 1
                continue
            b = basket(m, g, d)
            if isinstance(b, Basket):
                built[d] = b
            elif b == "open":
                pending.add(d)
            else:
                counts[b] += 1
        for d in mm.non_overlapping(sorted(built.keys() | pending), m.sessions, POLICY_HOLD):
            if d in pending:
                open_.append({"group": g, "entry_date": d})
                continue
            b = built[d]
            baskets[(g, d)] = b
            rows.append((g, TradeRow(d, d, b.exit_date, b.gross, b.gross - cost)))
            counts["legs"] += b.legs
            counts["legs_voided"] += b.voided
            counts["legs_no_bar"] += b.no_bar
    rows.sort(key=lambda r: (r[1].entry_date, r[0]))
    open_.sort(key=lambda o: (o["entry_date"], o["group"]))
    counts["kept"], counts["open"] = len(rows), len(open_)
    return rows, open_, counts, baskets


def policy_population(m: Market, lo: date) -> Callable[[date], list[float]]:
    """Every (group, session) 10-session excess from ``lo`` that closed by the cut: the mapper vocabulary, each
    group counted on the sessions where it has a liquid member."""
    def pop(cut: date) -> list[float]:
        out = []
        for d in m.sessions[bisect_right(m.sessions, lo - timedelta(days=1)):]:
            k = m.pos[d]
            if k + POLICY_HOLD - 1 >= len(m.sessions) or m.sessions[k + POLICY_HOLD - 1] > cut:
                break
            for g in VOCAB:
                b = basket(m, g, d)
                if isinstance(b, Basket):
                    out.append(b.gross)
        return out
    return pop


# ----------------------------------------------------------------------------- looks
def judge(closed: list[tuple[str, TradeRow]], population: Callable[[date], list[float]], cost: float, hold: int,
          as_of: date, last_session: date) -> dict[str, Any]:
    """The look: a verdict on every trade closed by the exit of the 30th to close (if that is by the deadline);
    else insufficient once the deadline has passed and the snapshot reaches it; else accruing."""
    order = sorted(closed, key=lambda r: (r[1].exit_date, r[1].entry_date, r[0]))
    n = len(order)
    notes: list[str] = []
    if n >= mm.MIN_TRADES and order[mm.MIN_TRADES - 1][1].exit_date <= DEADLINE:
        status, cut = "verdict", order[mm.MIN_TRADES - 1][1].exit_date
    elif as_of >= DEADLINE and last_session >= DEADLINE:
        status, cut = "insufficient", DEADLINE
    else:
        status, cut = "accruing", None
        if as_of >= DEADLINE:
            notes.append(f"the snapshot ends {last_session}, before the {DEADLINE} deadline; refresh it to judge")
    out: dict[str, Any] = {"status": status, "look_date": cut if status == "verdict" else None, "cut": cut,
                           "n_closed": n, "verdict": None, "population_n": None, "notes": notes}
    if cut is None:
        out |= {"status_text": f"accruing {n}/{mm.MIN_TRADES}", "label": "accruing", "sample": closed}
        return out
    sample = sorted((r for r in closed if r[1].exit_date <= cut), key=lambda r: (r[1].entry_date, r[0]))
    pop = population(cut) if sample else []
    v = mm.verdict([t for _, t in sample], pop, hold, cost)
    out |= {"verdict": v, "population_n": len(pop), "sample": sample}
    if status == "verdict":
        out |= {"label": v["label"], "status_text": f"judged at the 30th closed trade ({cut}): {v['label']}"}
    else:
        out |= {"label": "insufficient",
                "status_text": f"insufficient: {len(sample)}/{mm.MIN_TRADES} closed by {DEADLINE}"}
    return out


def _trade_dicts(rows: list[tuple[str, TradeRow]], baskets: dict[tuple[str, date], Basket] | None) -> list[dict]:
    out = []
    for g, t in rows:
        row = asdict(t)
        if baskets is not None:
            b = baskets[(g, t.entry_date)]
            row = {"group": g, **row, "legs": b.legs, "legs_voided": b.voided, "legs_no_bar": b.no_bar}
        out.append(row)
    return out


def _hypothesis(name: str, start: date | None, rows: list[tuple[str, TradeRow]], open_: list[dict],
                counts: dict[str, int], population: Callable[[date], list[float]] | None, cost: float, hold: int,
                as_of: date, last: date, baskets: dict[tuple[str, date], Basket] | None) -> dict[str, Any]:
    pop = population if population is not None else (lambda cut: [])
    look = judge(rows, pop, cost, hold, as_of, last)
    sample = look.pop("sample")
    if start is None:
        look["notes"].append("not started: the content start has not been reached")
    return {"hypothesis": name, "start": start, "hold": hold, "product": "CNC", "cost_pct": cost, **look,
            "trades": _trade_dicts(sample, baskets), "trades_after_look": len(rows) - len(sample),
            "open": open_, "counts": counts}


def _summary(rows: list[tuple[str, TradeRow]]) -> dict[str, Any]:
    nets = [t.net for _, t in rows]
    return {"n": len(rows), "mean_gross": statistics.fmean(t.gross for _, t in rows) if rows else None,
            "mean_net": statistics.fmean(nets) if rows else None,
            "median_net": statistics.median(nets) if rows else None}


# ----------------------------------------------------------------------------- content start
def content_start(runs: Iterable[tuple[datetime, str, Any]], windows: Sequence[date]) -> date | None:
    """The first regular session D on or after ``REGISTERED_FROM`` whose news window is complete: every feed
    polled without error in [09:00, 10:00) on the previous regular session's date and in [08:00, 09:00) on D.
    ``windows`` is ascending regular sessions, as for ``session_for``."""
    polled: dict[tuple[date, str], set[str]] = defaultdict(set)
    for t, feed, error in runs:
        if error is not None:
            continue
        local = t.astimezone(IST)
        for slot, (a, b) in (("first", COMPLETE_FIRST_DAY), ("on_d", COMPLETE_ON_D)):
            if a <= local.time() < b:
                polled[(local.date(), slot)].add(feed)
    need = set(FEEDS)
    for prev, d in zip(windows, windows[1:], strict=False):
        if d >= mm.REGISTERED_FROM and need <= polled[(prev, "first")] and need <= polled[(d, "on_d")]:
            return d
    return None


# ----------------------------------------------------------------------------- comparison
def answer(ci: tuple[float, float] | None) -> str:
    if ci is None:
        return "not enough data"
    lo, hi = ci
    if lo > 0:
        return "content adds leverage"
    if hi < 0:
        return "content hurts"
    return "no measurable difference"


def _spearman_rows(x: np.ndarray, y: np.ndarray) -> np.ndarray:
    """Row-wise Spearman rank correlation (average ranks); NaN for a row with no rank variance."""
    rx, ry = rankdata(x, axis=-1), rankdata(y, axis=-1)
    rx = rx - rx.mean(axis=-1, keepdims=True)
    ry = ry - ry.mean(axis=-1, keepdims=True)
    den = np.sqrt((rx * rx).sum(axis=-1) * (ry * ry).sum(axis=-1))
    with np.errstate(invalid="ignore", divide="ignore"):
        return np.where(den > 0, (rx * ry).sum(axis=-1) / np.where(den > 0, den, 1.0), np.nan)


def _ci(diffs: np.ndarray) -> tuple[tuple[float, float] | None, int]:
    ok = diffs[np.isfinite(diffs)]
    if not ok.size:
        return None, 0
    return (float(np.percentile(ok, 2.5)), float(np.percentile(ok, 97.5))), int(ok.size)


def compare_tone(h: pd.Series, c: pd.Series, ret: pd.Series) -> dict[str, Any]:
    """Spearman IC of each tone with the NIFTY open(D) -> close(D+4) return over the sessions where all three
    exist; C minus H with a moving-block bootstrap 95% CI (block 5, 2,000 draws, seed ``PERM_SEED``)."""
    df = pd.concat({"h": h, "c": c, "r": ret}, axis=1).dropna()
    n = len(df)
    out: dict[str, Any] = {"n": n, "ic_h": None, "ic_c": None, "diff": None, "ci": None, "draws_used": 0,
                           "answer": "not enough data"}
    if n < 2 * BOOT_BLOCK:
        return out
    hv, cv, rv = (df[k].to_numpy(dtype="float64") for k in ("h", "c", "r"))
    ic_h, ic_c = float(_spearman_rows(hv, rv)), float(_spearman_rows(cv, rv))
    if not (math.isfinite(ic_h) and math.isfinite(ic_c)):
        return out
    rng = np.random.default_rng(mm.PERM_SEED)
    blocks = -(-n // BOOT_BLOCK)
    starts = rng.integers(0, n - BOOT_BLOCK + 1, size=(BOOT_DRAWS, blocks))
    idx = (starts[:, :, None] + np.arange(BOOT_BLOCK)).reshape(BOOT_DRAWS, -1)[:, :n]
    diffs = _spearman_rows(cv[idx], rv[idx]) - _spearman_rows(hv[idx], rv[idx])
    ci, used = _ci(diffs)
    out |= {"ic_h": ic_h, "ic_c": ic_c, "diff": ic_c - ic_h, "ci": ci, "draws_used": used, "answer": answer(ci)}
    return out


def compare_policy(c: Sequence[tuple[date, float]], h: Sequence[tuple[date, float]]) -> dict[str, Any]:
    """Mean excess per event, C-identified minus H-identified; each event is ``(entry session, excess)``.

    Each side needs ``MIN_EVENTS`` events, else "not enough data". The 95% CI is a bootstrap over whole entry
    sessions, so all events entered on one session go in or out together (2,000 draws, seed ``PERM_SEED``). Both
    sides draw from one session list, the union of their entry sessions, so same-session C and H events stay
    paired; a draw with no event on one side is dropped and counted out of ``draws_used``."""
    out: dict[str, Any] = {"n_c": len(c), "n_h": len(h), "sessions": 0, "mean_c": None, "mean_h": None,
                           "diff": None, "ci": None, "draws_used": 0, "answer": "not enough data"}
    if c:
        out["mean_c"] = statistics.fmean(v for _, v in c)
    if h:
        out["mean_h"] = statistics.fmean(v for _, v in h)
    if c and h:
        out["diff"] = out["mean_c"] - out["mean_h"]
    if len(c) < MIN_EVENTS or len(h) < MIN_EVENTS:
        return out
    days = sorted({d for d, _ in c} | {d for d, _ in h})
    col = {d: i for i, d in enumerate(days)}
    sums = np.zeros((2, len(days)))
    counts = np.zeros((2, len(days)))
    for side, events in enumerate((c, h)):
        for d, v in events:
            sums[side, col[d]] += v
            counts[side, col[d]] += 1
    rng = np.random.default_rng(mm.PERM_SEED)
    picks = rng.integers(0, len(days), size=(BOOT_DRAWS, len(days)))
    with np.errstate(invalid="ignore", divide="ignore"):
        means = sums[:, picks].sum(axis=2) / counts[:, picks].sum(axis=2)     # (2, draws)
    ci, used = _ci(means[0] - means[1])
    out |= {"sessions": len(days), "ci": ci, "draws_used": used, "answer": answer(ci)}
    return out


def _event_excess(m: Market, events: dict[date, dict[str, list[str]]], lo: date,
                  cut: date) -> list[tuple[date, float]]:
    out = []
    for d in sorted(events):
        if d < lo or d not in m.pos:
            continue
        for g in sorted(events[d]):
            b = basket(m, g, d)
            if isinstance(b, Basket) and b.exit_date <= cut:
                out.append((d, b.gross))
    return out


# ----------------------------------------------------------------------------- evaluate
def news_windows(cal: Any, as_of: date) -> list[date]:
    """The regular sessions whose windows the looks draw, from ``SEEN[0]`` to ``as_of``, preceded by the anchor
    session whose 09:00 opens the first of them."""
    lo = SEEN[0] - timedelta(days=30)
    days = regular_sessions(cal, lo, max(as_of, SEEN[0]))
    k = bisect_left(days, SEEN[0])
    if k == 0:
        raise Unavailable(f"the NSE calendar has no regular session in the month before {SEEN[0]}")
    return [days[k - 1], *(d for d in days[k:] if d <= as_of)]


def _window_open(windows: Sequence[date], d: date) -> datetime:
    """09:00 IST on the regular session before the first window session on or after ``d`` (the last drawn
    session when ``d`` lies past them all)."""
    i = bisect_left(windows, d)
    return datetime.combine(windows[i - 1] if i > 0 else windows[0], CUTOFF, tzinfo=IST)


def evaluate(state_db: Path, content_db: Path, snapshot: Path, as_of: date, *,
             etf_symbols: frozenset[str] | None = None, theme_lists: Path = THEME_LISTS_PATH,
             calendar_dir: Path | None = None) -> dict[str, Any]:
    """The four forward looks and the comparison as of ``as_of``: windows of sessions on or before ``as_of`` only.
    Raises ``Unavailable`` (or ``research_common.Refusal``) for an unreadable store or a calendar that does not
    cover the windows."""
    as_of = mm._as_date(as_of)
    limit = datetime.combine(as_of, CUTOFF, tzinfo=IST)
    try:
        windows = news_windows(load_calendar(calendar_dir), as_of)
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise Unavailable(f"NSE calendar unreadable ({type(exc).__name__}: {exc})") from exc
    themes = load_theme_lists(theme_lists)
    etfs = mm.load_etf_symbols() if etf_symbols is None else frozenset(etf_symbols)
    head = load_headline(state_db, limit, _window_open(windows, mm.REGISTERED_FROM), _window_open(windows, SEEN[0]))
    study = load_content(content_db, limit, as_of)
    conn = rc.open_snapshot(snapshot)
    try:
        m = load_market(conn, as_of, etfs, themes)
        doc = _evaluate(m, head, study, as_of, windows)
    finally:
        mm._LIQUID_CACHE.pop(id(conn), None)
        conn.close()
    doc["meta"] |= {"state_db": str(state_db), "content_db": str(content_db), "snapshot": str(snapshot)}
    return doc


def _evaluate(m: Market, head: HeadlineData, study: StudyData, as_of: date,
              windows: Sequence[date]) -> dict[str, Any]:
    last = m.sessions[-1]
    cost = mm.cost_pct("CNC")
    start_c = content_start(study.runs, windows)
    h_all, c_all = study.scores["H"], study.scores["C"]
    both = {s.key for s in h_all} & {s.key for s in c_all}
    h_both, c_both = [s for s in h_all if s.key in both], [s for s in c_all if s.key in both]

    tones = {"headline": tone_series(head.scores, windows), "content": tone_series(c_all, windows),
             "study_h": tone_series(h_both, windows), "study_c": tone_series(c_both, windows)}
    ev_head, cnt_head = policy_events(head.scores, windows, study.mappings["headline"], m.themes)
    ev_content, cnt_content = policy_events(c_all, windows, study.mappings["content"], m.themes)
    ev_sc, _ = policy_events(c_both, windows, study.mappings["content"], m.themes)
    ev_sh, cnt_sh = policy_events(h_both, windows, study.mappings["study_h"], m.themes)

    hyps: dict[str, Any] = {}
    seen: dict[str, Any] = {}
    for name, series, start in (("news_tone_headline", tones["headline"], mm.REGISTERED_FROM),
                                ("news_tone_content", tones["content"], start_c)):
        sig = tone_signal(series)
        rows, open_, counts = tone_trades(m, sig, start, None, cost)
        pop = tone_population(m, start) if start is not None else None
        hyps[name] = _hypothesis(name, start, rows, open_, counts, pop, cost, TONE_HOLD, as_of, last, None)
        if name == "news_tone_headline":
            s_rows, _, s_counts = tone_trades(m, sig, SEEN[0], SEEN[1], cost)
            seen[name] = {"window": list(SEEN), "summary": _summary(s_rows), "counts": s_counts}
    for name, events, ev_counts, start in (("policy_headline", ev_head, cnt_head, mm.REGISTERED_FROM),
                                           ("policy_content", ev_content, cnt_content, start_c)):
        rows, open_, counts, baskets = policy_trades(m, events, start, None, cost)
        pop = policy_population(m, start) if start is not None else None
        hyps[name] = _hypothesis(name, start, rows, open_, counts | {f"candidates_{k}": v for k, v in
                                                                      ev_counts.items()},
                                 pop, cost, POLICY_HOLD, as_of, last, baskets)
        if name == "policy_headline":
            s_rows, _, s_counts, _ = policy_trades(m, events, SEEN[0], SEEN[1], cost)
            seen[name] = {"window": list(SEEN), "summary": _summary(s_rows), "counts": s_counts}

    comparison = _comparison(m, tones, ev_sc, ev_sh, start_c, hyps, last)
    comparison["policy"]["study_h_candidates"] = cnt_sh

    def no_tone(series: pd.Series, start: date | None) -> int | None:
        if start is None:
            return None
        return int(series[[d >= start for d in series.index]].isna().sum())

    counts = {**head.counts, **study.counts,
              "sessions_no_tone": {"headline": no_tone(tones["headline"], mm.REGISTERED_FROM),
                                   "content": no_tone(tones["content"], start_c),
                                   "study_h": no_tone(tones["study_h"], start_c)},
              "legs_voided": {n: hyps[n]["counts"]["legs_voided"] for n in ("policy_headline", "policy_content")}}
    meta = {"family": mm.FAMILY, "as_of": as_of, "registered_from": mm.REGISTERED_FROM, "deadline": DEADLINE,
            "last_session": last, "min_trades": mm.MIN_TRADES, "cost_round_trip_pct_cnc": cost,
            "notional_inr": str(mm.NOTIONAL), "headline_key": list(head.key) if head.key is not None else None,
            "content_start": start_c, "pinned_models_last_ok": study.pinned_last_ok,
            "study_tables_missing": study.missing_tables, "notes": m.notes}
    return {"meta": meta, "counts": counts,
            "tones": {k: {d.isoformat(): (None if math.isnan(v) else float(v)) for d, v in s.items()}
                      for k, s in tones.items()},
            "hypotheses": {n: hyps[n] for n in NAMES}, "seen (not evidence)": seen, "comparison": comparison}


def _comparison(m: Market, tones: dict[str, pd.Series], ev_c: dict, ev_h: dict, start_c: date | None,
                hyps: dict[str, Any], last: date) -> dict[str, Any]:
    """The headlines-versus-content comparison (decides nothing), cut where its content hypothesis is judged,
    else at the snapshot's last session (an interim reading)."""
    out: dict[str, Any] = {"note": "decides nothing; read when the content hypothesis is judged or at 18 months"}
    for kind, hyp_name in (("tone", "news_tone_content"), ("policy", "policy_content")):
        cut = hyps[hyp_name]["cut"] or last
        read = hyps[hyp_name]["status"] in ("verdict", "insufficient")
        if start_c is None:
            res = compare_tone(pd.Series(dtype="float64"), pd.Series(dtype="float64"), pd.Series(dtype="float64")) \
                if kind == "tone" else compare_policy([], [])
        elif kind == "tone":
            days = [d for d in tones["study_h"].index if start_c <= d <= cut and d in m.pos]
            ret = {}
            for d in days:
                t = mm.index_trade(m.nifty, m.sessions, d, TONE_HOLD)
                if t is not None and t[0] <= cut:
                    ret[d] = t[1]
            r = pd.Series(ret, dtype="float64")
            res = compare_tone(tones["study_h"].loc[days], tones["study_c"].loc[days], r)
        else:
            res = compare_policy(_event_excess(m, ev_c, start_c, cut), _event_excess(m, ev_h, start_c, cut))
        out[kind] = {**res, "cut": cut, "read": read}
    return out


# ----------------------------------------------------------------------------- rendering
def _f(v: Any, fmt: str = "+.4f") -> str:
    return "-" if v is None else format(v, fmt)


def render(doc: dict[str, Any]) -> str:
    m, c = doc["meta"], doc["counts"]
    key = m["headline_key"]
    out = [f"MACRO MOOD FORWARD LOOKS ({m['family']}) as of {m['as_of']}",
           f"snapshot {m.get('snapshot')}  last session {m['last_session']}",
           f"registered from {m['registered_from']}; deadline {m['deadline']}; judged at trade {m['min_trades']}; "
           f"CNC round trip {m['cost_round_trip_pct_cnc']:.4f}% at Rs {m['notional_inr']}",
           f"headline key: {'none yet' if key is None else f'{key[0]} / {key[1]}'}; content start: "
           f"{m['content_start'] or 'not reached'}",
           "pinned models, last ok call: " + ", ".join(f"{k} {v or 'never'}"
                                                         for k, v in m["pinned_models_last_ok"].items()),
           f"excluded engine calls {c['engine_calls_excluded']} (seen window {c['engine_calls_excluded_seen']}); "
           f"engine clusters dropped {c['engine_clusters_excluded']}; unparsed outputs "
           f"{c['engine_outputs_unparsed']}; study scores/mappings off-model {c['study_scores_excluded_model']}/"
           f"{c['study_mappings_excluded_model']}",
           f"sessions with no tone: {c['sessions_no_tone']}; voided legs: {c['legs_voided']}"]
    if m["study_tables_missing"]:
        out.append(f"study tables missing (read as empty): {m['study_tables_missing']}")
    out += [f"note: {n}" for n in m["notes"]]
    out += ["", "LOOKS (each judged once, at its 30th closed trade)"]
    for name, h in doc["hypotheses"].items():
        out.append(f"  {name:<20} {h['status_text']}  [start {h['start'] or 'not reached'}; closed {h['n_closed']}, "
                   f"open {len(h['open'])}]")
        v = h["verdict"]
        if v is not None:
            out.append(f"    n={v['n']} mean net {_f(v['mean_net'])} median net {_f(v['median_net'])} perm p "
                       f"{_f(v['perm_p'], '.4f')} fold pass {_f(v['fold_pass'], '.2f')} halves "
                       f"{_f(v['half1_mean'])}/{_f(v['half2_mean'])} drop-top-3 {_f(v['drop_top3_mean'])}")
        out.append(f"    counts {h['counts']}")
        out += [f"    note: {n}" for n in h["notes"]]
    out += ["", "SEEN (not evidence; before registration)"]
    for name, s in doc["seen (not evidence)"].items():
        sm = s["summary"]
        out.append(f"  {name:<20} n={sm['n']} mean net {_f(sm['mean_net'])} median net {_f(sm['median_net'])}")
    cmp_ = doc["comparison"]
    out += ["", f"HEADLINES VERSUS CONTENT ({cmp_['note']})"]
    t, p = cmp_["tone"], cmp_["policy"]
    ci_t = "-" if t["ci"] is None else f"[{t['ci'][0]:+.4f}, {t['ci'][1]:+.4f}]"
    ci_p = "-" if p["ci"] is None else f"[{p['ci'][0]:+.4f}, {p['ci'][1]:+.4f}]"
    out.append(f"  tone   {t['answer']}  ({'read' if t['read'] else 'interim'}; cut {t['cut']}) n={t['n']} "
               f"IC H {_f(t['ic_h'])} C {_f(t['ic_c'])} C-H {_f(t['diff'])} CI {ci_t}")
    out.append(f"  policy {p['answer']}  ({'read' if p['read'] else 'interim'}; cut {p['cut']}) events C {p['n_c']} "
               f"H {p['n_h']} mean C {_f(p['mean_c'])} H {_f(p['mean_h'])} C-H {_f(p['diff'])} CI {ci_p}")
    return rc.ascii_text("\n".join(out))


# ----------------------------------------------------------------------------- CLI
def default_out(db: Path, as_of: date) -> Path:
    """``<data>/reports/macro_mood_forward_<as_of>.json`` beside the snapshot's ``<data>/research`` dir."""
    return Path(db).resolve().parent.parent / "reports" / f"macro_mood_forward_{as_of.isoformat()}.json"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="macro_mood_forward", description=__doc__.splitlines()[0])
    ap.add_argument("--state-db", type=Path, default=DEFAULT_STATE_DB, help="engine state.db (opened read-only)")
    ap.add_argument("--content-db", type=Path, default=DEFAULT_CONTENT_DB, help="the study's content SQLite")
    ap.add_argument("--db", type=Path, required=True, help="research snapshot (data/research/*.duckdb)")
    ap.add_argument("--as-of", type=date.fromisoformat, default=None, help="YYYY-MM-DD; default today (IST)")
    ap.add_argument("--etf-list", type=Path, default=mm.ETF_LIST, help="NSE ETF list (Symbol column)")
    ap.add_argument("--out", type=Path, default=None, help="JSON path; the .md is written beside it")
    try:
        args = ap.parse_args(argv)
    except SystemExit as exc:
        if exc.code not in (0, None):
            print("macro_mood_forward: UNAVAILABLE: bad arguments")
        return 0
    as_of = args.as_of or datetime.now(IST).date()
    try:
        etfs = mm.load_etf_symbols(args.etf_list)
        doc = evaluate(args.state_db, args.content_db, args.db, as_of, etf_symbols=etfs)
    except (Unavailable, rc.Refusal) as exc:
        print(f"macro_mood_forward: UNAVAILABLE: {rc.ascii_text(str(exc))}")
        return 0
    except Exception as exc:  # noqa: BLE001 - a look never fails its caller; the trace says what broke
        traceback.print_exc()
        print(f"macro_mood_forward: UNAVAILABLE: unexpected {type(exc).__name__}: {rc.ascii_text(str(exc))}")
        return 0
    text = render(doc)
    out = args.out or default_out(args.db, as_of)
    try:
        md = rc.write_reports(doc, f"# MACRO MOOD FORWARD LOOKS\n\n```\n{text}\n```\n", out)
    except OSError as exc:
        print(text)
        print(f"macro_mood_forward: report not written ({type(exc).__name__}: {exc})")
        return 0
    print(text)
    print(f"JSON -> {out}\nMD   -> {md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
