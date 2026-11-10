"""``scripts/macro_mood_forward.py``: the macro mood study's forward looks and its headlines-versus-content comparison.

Every store is a synthetic temp file: a research DuckDB, an ``agent_calls`` SQLite and the study's content SQLite.
"""

from __future__ import annotations

import gzip
import hashlib
import importlib.util
import json
import sqlite3
import sys
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
import numpy as np
import pandas as pd
import pytest

_ROOT = Path(__file__).resolve().parents[2]


def _load(mod_name: str, file: str):
    if mod_name in sys.modules:
        return sys.modules[mod_name]
    spec = importlib.util.spec_from_file_location(mod_name, _ROOT / "scripts" / file)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = mod
    spec.loader.exec_module(mod)
    return mod


mmf = _load("mt_macro_mood_forward", "macro_mood_forward.py")
mm, ncs, ncc = mmf.mm, mmf.ncs, mmf.ncc

from engine.core.config import config_dir, load_yaml  # noqa: E402
from engine.intelligence.agents import news_analyst  # noqa: E402
from engine.intelligence.context import ContextAssembler  # noqa: E402

IST = ZoneInfo("Asia/Kolkata")
UTC = ZoneInfo("UTC")
HAIKU, SONNET = "claude-haiku-5-5", "claude-sonnet-5-5"

AGENT_CALLS_DDL = """
CREATE TABLE agent_calls (
    call_id TEXT PRIMARY KEY, agent_id TEXT NOT NULL, trigger TEXT, model TEXT, inputs_digest TEXT,
    context_gz BLOB, output_json TEXT, ok INTEGER NOT NULL, fail_reason TEXT, in_tokens INTEGER,
    out_tokens INTEGER, cache_read INTEGER, cache_write INTEGER, cost_usd TEXT, duration_ms INTEGER,
    at TEXT NOT NULL, prompt_version TEXT)
"""


# --------------------------------------------------------------------------- synthetic world
def weekdays(start: date, end: date) -> list[date]:
    out, d = [], start
    while d <= end:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


def at(day: date, hh: int, mi: int = 0, ss: int = 0, us: int = 0) -> datetime:
    return datetime(day.year, day.month, day.day, hh, mi, ss, us, tzinfo=IST)


def D(month: int, day: int, year: int = 2026) -> date:
    return date(year, month, day)


def score(cid: str, scope: str = "market", sentiment: float = 0.5, materiality: float = 1.0,
          event_type: str = "macro_data") -> dict:
    return {"cluster_id": cid, "scope": scope, "entities": [], "sectors": [], "themes": [],
            "sentiment": sentiment, "materiality": materiality, "event_type": event_type, "novelty": 0.5}


def engine_prompt(rows: list[dict]) -> str:
    asm = object.__new__(ContextAssembler)
    return asm.for_news_batch(rows, list(ncs.THEME_VOCABULARY)).prompt()


@pytest.fixture(scope="session")
def cal_dir(tmp_path_factory) -> Path:
    """The real ``config/calendar`` files (2026: 11-10, 11-24 and 12-25 are holidays, 11-08 a Sunday muhurat)
    plus holiday-free 2027 and 2028 files, so the long tests reach the 18-month deadline."""
    out = tmp_path_factory.mktemp("calendar")
    for p in (config_dir() / "calendar").glob("*.yaml"):
        (out / p.name).write_bytes(p.read_bytes())
    for y in (2027, 2028):
        (out / f"{y}.yaml").write_text(f'year: {y}\nverified: true\nverified_through: "{y}-12-31"\nholidays: []\n',
                                       encoding="utf-8")
    return out


def regular(cal_dir: Path, lo: date, hi: date) -> list[date]:
    return mmf.regular_sessions(mmf.load_calendar(cal_dir), lo, hi)


class World:
    """A research snapshot, an engine ``agent_calls`` table and the study's content file, all synthetic. NIFTY and
    stock bars exist on the calendar's regular sessions in [lo, hi]."""

    def __init__(self, root: Path, cal_dir: Path, lo: date, hi: date) -> None:
        self.root, self.cal_dir, self.sessions = root, cal_dir, regular(cal_dir, lo, hi)
        self.snapshot = root / "research" / "market_test.duckdb"
        self.state_db = root / "state.db"
        self.content_db = root / "news_content.sqlite"
        self.nifty: dict[date, tuple[float, float]] = {d: (100.0, 100.5) for d in self.sessions}
        self.stocks: dict[str, dict[date, tuple[float, float, int]]] = {}
        self.corp: list[tuple] = []
        self.sector_rows: list[tuple] = []
        con = sqlite3.connect(self.state_db)
        con.execute(AGENT_CALLS_DDL)
        con.commit()
        con.close()
        con = sqlite3.connect(self.content_db)
        con.executescript(ncc._SCHEMA)
        con.executescript(ncs._SCHEMA)
        con.close()

    # ---- market
    def stock(self, sym: str, *, volume: int = 2_000_000, price: float = 100.0,
              bars: dict[date, tuple[float, float, int]] | None = None) -> None:
        rows = {d: (price, price, volume) for d in self.sessions}
        rows.update(bars or {})
        self.stocks[sym] = rows

    def build_snapshot(self, path: Path | None = None, until: date | None = None) -> Path:
        path = path or self.snapshot
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            path.unlink()
        conn = duckdb.connect(str(path))
        conn.execute('CREATE TABLE bars_1d (symbol TEXT NOT NULL, d DATE NOT NULL, "open" DECIMAL(12,2) NOT NULL, '
                     'high DECIMAL(12,2) NOT NULL, low DECIMAL(12,2) NOT NULL, "close" DECIMAL(12,2) NOT NULL, '
                     "volume BIGINT NOT NULL, src TEXT NOT NULL DEFAULT 'kite_official', PRIMARY KEY (symbol, d))")
        conn.execute("CREATE TABLE corp_actions (symbol TEXT NOT NULL, ex_date DATE NOT NULL, kind TEXT NOT NULL, "
                     "ratio TEXT, amount DECIMAL(12,2), source TEXT, recorded_at TIMESTAMPTZ, "
                     "PRIMARY KEY (symbol, ex_date, kind))")
        conn.execute("CREATE TABLE sector_map (as_of DATE NOT NULL, symbol TEXT NOT NULL, sector TEXT NOT NULL, "
                     "PRIMARY KEY (as_of, symbol))")
        rows = []
        for d, (o, c) in self.nifty.items():
            if until is None or d <= until:
                rows.append(("NIFTY 50", d, o, max(o, c), min(o, c), c, 0))
        for sym, bars in self.stocks.items():
            for d, (o, c, v) in bars.items():
                if until is None or d <= until:
                    rows.append((sym, d, o, max(o, c), min(o, c), c, v))
        conn.executemany('INSERT INTO bars_1d (symbol, d, "open", high, low, "close", volume) '
                         "VALUES (?, ?, ?, ?, ?, ?, ?)", rows)
        if self.corp:
            conn.executemany("INSERT INTO corp_actions (symbol, ex_date, kind) VALUES (?, ?, ?)", self.corp)
        if self.sector_rows:
            conn.executemany("INSERT INTO sector_map VALUES (?, ?, ?)", self.sector_rows)
        conn.close()
        return path

    # ---- the engine's News Analyst calls
    def call(self, when: datetime, scores: list[dict], *, model: str = "claude-sonnet-5-5", pv: str | None = "v1",
             ok: int = 1, heads: dict[str, str] | None = None, agent_id: str = "news_analyst",
             stamp: str | None = None) -> str:
        call_id = uuid.uuid4().hex
        heads = heads or {}
        rows = [{"cluster_id": s["cluster_id"], "representative": heads.get(s["cluster_id"], f"story {s['cluster_id']}"),
                 "source_domains": ["a.com"], "first_seen": when} for s in scores]
        blob = gzip.compress(f"{news_analyst.SYSTEM_PROMPT}\n\n{engine_prompt(rows)}".encode())
        con = sqlite3.connect(self.state_db)
        con.execute("INSERT INTO agent_calls (call_id, agent_id, trigger, model, context_gz, output_json, ok, at, "
                    "prompt_version) VALUES (?,?,?,?,?,?,?,?,?)",
                    (call_id, agent_id, "batch", model, blob, json.dumps({"scores": scores}), ok,
                     stamp or when.isoformat(), pv))
        con.commit()
        con.close()
        return call_id

    # ---- the study's content file
    def _llm_call(self, con: sqlite3.Connection, purpose: str, model: str, when: datetime, ok: int = 1) -> str:
        call_id = uuid.uuid4().hex
        con.execute("INSERT INTO llm_calls (call_id, at, purpose, model, prompt_sha256, ok, error, effort) "
                    "VALUES (?,?,?,?,?,?,?,?)",
                    (call_id, when.isoformat(), purpose, model, hashlib.sha256(b"x").hexdigest(), ok, None,
                     "default"))
        return call_id

    def cluster(self, key: str, captured: datetime, title: str, *, summary: str | None = None) -> None:
        url = f"https://a.com/{key}"
        con = sqlite3.connect(self.content_db)
        con.execute("INSERT INTO items VALUES (?,?,?,?,?,?)", (url, "et", title, summary, None, captured.isoformat()))
        con.execute("INSERT INTO clusters VALUES (?,?,?,?,?,?,?,?)",
                    (key, captured.isoformat(), url, json.dumps([url]), title, json.dumps(["a.com"]),
                     captured.isoformat(), captured.isoformat()))
        con.execute("INSERT INTO cluster_members VALUES (?, ?)", (url, key))
        con.commit()
        con.close()

    def study_score(self, key: str, variant: str, scope: str = "market", sentiment: float = 0.5,
                    materiality: float = 1.0, event_type: str = "macro_data", *, model: str = HAIKU) -> None:
        con = sqlite3.connect(self.content_db)
        call_id = self._llm_call(con, f"score_{variant}", model, at(D(11, 30), 22))
        con.execute("INSERT INTO scores VALUES (?,?,?,?,?,?,?,?)",
                    (key, variant, scope, event_type, sentiment, materiality, 0.5, call_id))
        con.commit()
        con.close()

    def mapping(self, key: str, source: str, groups: list[str], *, model: str = SONNET) -> None:
        con = sqlite3.connect(self.content_db)
        call_id = self._llm_call(con, "map_content" if source == "content" else "map_headline", model,
                                 at(D(11, 30), 22, 30))
        con.execute("INSERT INTO mappings VALUES (?,?,?,?)", (key, source, json.dumps(groups), call_id))
        con.commit()
        con.close()

    def poll(self, when: datetime, *, skip: tuple[str, ...] = (), errored: tuple[str, ...] = ()) -> None:
        con = sqlite3.connect(self.content_db)
        for feed in ncc.FEEDS:
            if feed in skip:
                continue
            con.execute("INSERT INTO capture_runs VALUES (?,?,?,?,?)",
                        (when.isoformat(), feed, 20, 1, "RuntimeError: http 503" if feed in errored else None))
        con.commit()
        con.close()

    def evaluate(self, as_of: date, snapshot: Path | None = None, calendar_dir: Path | None = None) -> dict:
        snap = snapshot or self.snapshot
        if not snap.exists():
            self.build_snapshot()
        return mmf.evaluate(self.state_db, self.content_db, snap, as_of, etf_symbols=frozenset(),
                            calendar_dir=calendar_dir or self.cal_dir)


def tone(doc: dict, series: str, day: date):
    return doc["tones"][series].get(day.isoformat())


def hyp(doc: dict, name: str) -> dict:
    return doc["hypotheses"][name]


def rising_calls(w: World, days: list[date], step: float = 0.0065) -> None:
    """One market-scope score a session at 08:00, strictly rising: every session past the warm-up signals."""
    for i, d in enumerate(days):
        w.call(at(d, 8), [score(f"m{i}", "market", -0.95 + step * i, 1.0)])


# --------------------------------------------------------------------------- the brief's seven
def test_cutoff_0900_exclusive_boundary(tmp_path, cal_dir):
    s = regular(cal_dir, D(10, 1), D(12, 31))
    assert mmf.session_for(at(D(11, 12), 9, 0, 0), s) == D(11, 13)
    assert mmf.session_for(at(D(11, 12), 8, 59, 59, 999_999), s) == D(11, 12)
    assert mmf.session_for(datetime(2026, 11, 12, 3, 30, tzinfo=UTC), s) == D(11, 13)    # 09:00 IST stamped in UTC
    assert mmf.session_for(at(D(10, 1), 8), s) is None             # before the first drawn window

    w = World(tmp_path, cal_dir, D(10, 1), D(12, 31))
    w.call(at(D(11, 12), 8, 59, 59), [score("a", "market", 0.5, 1.0)])
    w.call(at(D(11, 12), 9, 0, 0), [score("b", "market", 0.2, 1.0)])
    w.cluster("c-1", at(D(11, 12), 8, 59, 59), "first story here")
    w.study_score("c-1", "C", "market", 0.4, 1.0)
    w.cluster("c-2", at(D(11, 12), 9, 0, 0), "second story here")
    w.study_score("c-2", "C", "market", -0.3, 1.0)
    doc = w.evaluate(D(11, 20))
    assert tone(doc, "headline", D(11, 12)) == pytest.approx(0.5)
    assert tone(doc, "headline", D(11, 13)) == pytest.approx(0.2)
    assert tone(doc, "content", D(11, 12)) == pytest.approx(0.4)
    assert tone(doc, "content", D(11, 13)) == pytest.approx(-0.3)


def test_weekend_and_holiday_news_counts_for_the_next_session(tmp_path, cal_dir):
    s = regular(cal_dir, D(10, 1), D(12, 31))
    assert D(11, 10) not in s and D(11, 24) not in s and D(11, 8) not in s     # holidays; 11-08 muhurat
    assert mmf.session_for(at(D(11, 13), 9, 0), s) == D(11, 16)                # Friday 09:00 opens Monday's window
    assert mmf.session_for(at(D(11, 14), 12), s) == D(11, 16)                  # Saturday
    assert mmf.session_for(at(D(11, 16), 8, 59, 59), s) == D(11, 16)
    assert mmf.session_for(at(D(11, 16), 9, 0), s) == D(11, 17)
    assert mmf.session_for(at(D(11, 10), 12), s) == D(11, 11)                  # mid-week holiday
    assert mmf.session_for(at(D(11, 8), 15), s) == D(11, 9)                    # the muhurat session is not regular
    # the window of the session after a holiday opens at 09:00 on the session before the holiday
    assert mmf.session_for(at(D(11, 9), 8, 59, 59), s) == D(11, 9)
    assert mmf.session_for(at(D(11, 9), 9, 0), s) == D(11, 11)
    assert mmf.session_for(at(D(11, 23), 9, 0), s) == D(11, 25)
    assert mmf.session_for(at(D(12, 24), 9, 0), s) == D(12, 28)                # Christmas Friday, then the weekend

    w = World(tmp_path, cal_dir, D(10, 1), D(12, 31))
    w.call(at(D(11, 14), 12), [score("sat", "market", 0.6, 1.0)])
    w.call(at(D(11, 16), 7), [score("mon", "market", 0.2, 1.0)])
    w.call(at(D(11, 9), 9, 0), [score("pre", "market", 0.1, 1.0)])
    w.call(at(D(11, 10), 12), [score("hol", "market", 0.5, 1.0)])
    w.cluster("c-1", at(D(11, 15), 20), "a sunday story")
    w.study_score("c-1", "C", "market", -0.4, 1.0)
    doc = w.evaluate(D(11, 30))
    assert tone(doc, "headline", D(11, 16)) == pytest.approx(0.4)
    assert tone(doc, "headline", D(11, 11)) == pytest.approx(0.3)
    assert tone(doc, "headline", D(11, 13)) is None and "2026-11-14" not in doc["tones"]["headline"]
    assert "2026-11-10" not in doc["tones"]["headline"]
    assert tone(doc, "content", D(11, 16)) == pytest.approx(-0.4)


def test_a_calendar_without_the_year_is_unavailable(tmp_path, cal_dir):
    w = World(tmp_path, cal_dir, D(10, 1), D(12, 31))
    with pytest.raises(mmf.Unavailable, match="2027"):
        w.evaluate(D(1, 15, 2027), calendar_dir=config_dir() / "calendar")


def test_session_without_market_scores_has_no_tone(tmp_path, cal_dir):
    w = World(tmp_path, cal_dir, D(10, 1), D(11, 20))
    w.call(at(D(11, 12), 7), [score("s1", "sector", 0.9, 1.0), score("s2", "stock", 0.9, 1.0)])
    w.call(at(D(11, 13), 7), [score("m1", "market", 0.3, 1.0)])
    w.cluster("c-1", at(D(11, 12), 7), "sector only story")
    w.study_score("c-1", "C", "sector", 0.9, 1.0)
    doc = w.evaluate(D(11, 20))
    assert tone(doc, "headline", D(11, 12)) is None
    assert tone(doc, "headline", D(11, 13)) == pytest.approx(0.3)
    assert tone(doc, "content", D(11, 12)) is None
    # forward sessions 11-11 .. 11-20 are eight; only 11-13 has a market-scope score
    assert doc["counts"]["sessions_no_tone"]["headline"] == 7
    assert hyp(doc, "news_tone_headline")["counts"]["signals"] == 0


def test_other_prompt_version_excluded_and_counted(tmp_path, cal_dir):
    w = World(tmp_path, cal_dir, D(10, 1), D(11, 20))
    w.call(at(D(11, 10), 20), [score("pre", "market", 0.2, 1.0)])                 # same key, before the key date
    w.call(at(D(11, 10), 21), [score("old", "market", -0.8, 1.0)], pv="v0")       # another key: excluded
    w.call(at(D(11, 11), 7), [score("reg", "market", 0.4, 1.0)])                  # first call on 11-11: the key
    w.call(at(D(11, 11), 8), [score("new", "market", 0.9, 1.0)], pv="v2")         # key change mid-window
    w.call(at(D(11, 12), 7), [score("only", "market", 0.7, 1.0)], model="other")  # the session's only call
    w.call(at(D(11, 13), 7), [score("x", "market", 0.6, 1.0)], ok=0)              # failed calls never count
    doc = w.evaluate(D(11, 20))
    assert doc["meta"]["headline_key"] == ["claude-sonnet-5-5", "v1"]
    assert tone(doc, "headline", D(11, 11)) == pytest.approx((0.2 + 0.4) / 2)
    assert tone(doc, "headline", D(11, 12)) is None
    assert doc["counts"]["engine_calls_excluded"] == 3


def test_policy_basket_excess_over_ew_universe_with_leg_costs(tmp_path, cal_dir):
    w = World(tmp_path, cal_dir, D(10, 1), D(12, 31))
    sessions = w.sessions
    d0 = D(11, 12)
    k = sessions.index(d0)
    exit_d = sessions[k + 9]
    w.stock("AUTOA", bars={exit_d: (100.0, 110.0, 2_000_000)})
    w.stock("AUTOB")                                                             # split inside the window: voided
    w.corp.append(("AUTOB", sessions[k + 3], "split"))
    w.corp.append(("BANKA", sessions[k + 3], "dividend"))                         # not structural: kept
    w.stock("AUTOC", volume=10, bars={d0: (100.0, 100.0, 1_000_000_000)})         # liquid on D only: never a leg
    w.stock("HAL", bars={exit_d: (100.0, 95.0, 2_000_000)})                       # defence theme list
    w.stock("BANKA", bars={exit_d: (100.0, 102.0, 2_000_000)})
    for sym, sector in (("AUTOA", "AUTO"), ("AUTOB", "AUTO"), ("AUTOC", "AUTO"), ("HAL", "CAPITAL_GOODS"),
                        ("BANKA", "BANK")):
        w.sector_rows.append((D(10, 1), sym, sector))
    w.sector_rows.append((d0, "AUTOA", "IT"))                                     # published on D: not in force on D-1
    for sym, sector in (("AUTOB", "AUTO"), ("AUTOC", "AUTO"), ("HAL", "CAPITAL_GOODS"), ("BANKA", "BANK")):
        w.sector_rows.append((d0, sym, sector))
    w.call(at(d0, 8), [score("k1", "sector", 0.5, 0.7, "sector_policy"),
                       score("k2", "sector", 0.39, 0.9, "sector_policy")],         # below the sentiment bar
           heads={"k1": "Government boosts defence orders", "k2": "Railway order book"})
    w.mapping("k1", "headline", ["AUTO"])
    w.mapping("k2", "headline", ["BANK"])
    doc = w.evaluate(D(12, 31))

    h = hyp(doc, "policy_headline")
    cost = mm.cost_pct("CNC")
    universe = (10.0 - 5.0 + 2.0) / 3                                             # AUTOA, HAL, BANKA; AUTOB voided
    assert [(t["group"], t["entry_date"], t["exit_date"]) for t in h["trades"]] == [
        ("AUTO", d0, exit_d), ("defence", d0, exit_d)]
    auto, defence = h["trades"]
    assert auto["gross"] == pytest.approx(10.0 - universe)
    assert auto["net"] == pytest.approx(10.0 - universe - cost)
    assert auto["legs"] == 1 and auto["legs_voided"] == 1
    assert defence["gross"] == pytest.approx(-5.0 - universe)
    assert defence["net"] == pytest.approx(-5.0 - universe - cost)
    assert h["counts"]["legs_voided"] == 1
    assert doc["counts"]["legs_voided"]["policy_headline"] == 1
    assert hyp(doc, "policy_content")["trades"] == []


def test_accruing_until_30_closed_then_verdict(tmp_path, cal_dir):
    w = World(tmp_path, cal_dir, D(9, 1), D(9, 30, 2027))
    sessions = w.sessions
    for i, d in enumerate(sessions):
        o = 100.0 + 0.05 * i
        w.nifty[d] = (o, o * (1.0 + 0.002 * ((i % 7) - 2)))
    rising_calls(w, [d for d in sessions if d <= D(8, 31, 2027)], step=0.0065)
    doc = w.evaluate(D(9, 30, 2027))
    h = hyp(doc, "news_tone_headline")
    assert h["status"] == "verdict"
    assert h["verdict"]["n"] == 30 and len(h["trades"]) == 30
    assert h["trades"][0]["entry_date"] >= mm.REGISTERED_FROM
    assert h["look_date"] == h["trades"][29]["exit_date"]
    assert h["label"] in {"supported", "null", "refuted"}
    assert h["n_closed"] > 30                                         # later trades exist but are not judged

    look = h["look_date"]
    before = w.evaluate(look - timedelta(days=1))
    hb = hyp(before, "news_tone_headline")
    assert hb["status"] == "accruing" and hb["status_text"] == "accruing 29/30"
    assert hb["label"] == "accruing" and hb["verdict"] is None
    assert hyp(before, "news_tone_content")["status_text"] == "accruing 0/30"


def test_insufficient_after_18_months(tmp_path, cal_dir):
    assert mmf.DEADLINE == D(5, 11, 2028)
    w = World(tmp_path, cal_dir, D(10, 1), D(5, 19, 2028))
    sessions = w.sessions
    rising_calls(w, [d for d in sessions if d <= D(11, 25)], step=0.02)
    doc = w.evaluate(D(5, 10, 2028))
    h = hyp(doc, "news_tone_headline")
    assert h["status"] == "accruing" and 0 < h["n_closed"] < 30

    doc = w.evaluate(mmf.DEADLINE)
    for name in ("news_tone_headline", "policy_headline", "news_tone_content", "policy_content"):
        assert hyp(doc, name)["status"] == "insufficient", name
        assert hyp(doc, name)["label"] == "insufficient", name
    h = hyp(doc, "news_tone_headline")
    assert h["verdict"]["label"] == "insufficient" and h["verdict"]["n"] == h["n_closed"]

    stale = w.build_snapshot(tmp_path / "research" / "market_stale.duckdb", until=D(5, 5, 2028))
    doc = w.evaluate(D(6, 1, 2028), snapshot=stale)
    assert hyp(doc, "news_tone_headline")["status"] == "accruing"        # closes up to the deadline are not all known


def test_comparison_ci_answer_mapping():
    assert mmf.answer((0.1, 0.5)) == "content adds leverage"
    assert mmf.answer((-0.5, -0.1)) == "content hurts"
    assert mmf.answer((-0.1, 0.2)) == "no measurable difference"
    assert mmf.answer((0.0, 0.3)) == "no measurable difference"
    assert mmf.answer(None) == "not enough data"

    rng = np.random.default_rng(1)
    days = weekdays(D(11, 11), D(3, 31, 2027))[:80]
    ret = pd.Series(rng.normal(0, 1, len(days)), index=days)
    noise = pd.Series(rng.normal(0, 1, len(days)), index=days)
    up = mmf.compare_tone(noise, ret, ret)
    assert up["answer"] == "content adds leverage" and up["ic_c"] == pytest.approx(1.0)
    assert up == mmf.compare_tone(noise, ret, ret)                     # seeded: the same CI every run
    assert mmf.compare_tone(ret, noise, ret)["answer"] == "content hurts"
    assert mmf.compare_tone(ret, ret, ret)["answer"] == "no measurable difference"
    assert mmf.compare_tone(ret.iloc[:0], ret, ret)["answer"] == "not enough data"

    hi = [(d, v) for d, v in zip(days[:40], rng.normal(3, 1, 40), strict=True)]
    lo = [(d, v) for d, v in zip(days[:40], rng.normal(-3, 1, 40), strict=True)]
    assert mmf.compare_policy(hi, lo)["answer"] == "content adds leverage"
    assert mmf.compare_policy(lo, hi)["answer"] == "content hurts"
    assert mmf.compare_policy(hi, hi)["answer"] == "no measurable difference"
    assert mmf.compare_policy(hi, lo) == mmf.compare_policy(hi, lo)       # seeded
    assert mmf.compare_policy([], hi)["answer"] == "not enough data"
    assert mmf.compare_policy(hi, [])["answer"] == "not enough data"


def test_policy_comparison_needs_ten_events_a_side():
    days = weekdays(D(11, 11), D(12, 31))[:20]
    c = [(d, 5.0 + 0.1 * i) for i, d in enumerate(days[:10])]
    h9 = [(d, -5.0 + 0.1 * i) for i, d in enumerate(days[10:19])]
    short = mmf.compare_policy(c, h9)
    assert short["answer"] == "not enough data" and short["ci"] is None
    assert (short["n_c"], short["n_h"]) == (10, 9) and short["diff"] == pytest.approx(5.45 - (-4.6))
    assert mmf.compare_policy(h9, c)["answer"] == "not enough data"
    assert mmf.compare_policy(c, h9 + [(days[19], -4.1)])["answer"] == "content adds leverage"


def test_policy_bootstrap_resamples_whole_entry_sessions():
    s1, s2 = D(11, 12), D(11, 13)
    c = [(s1, float(v)) for v in range(10)]          # wide spread inside one entry session, mean 4.5
    h = [(s2, 1.0)] * 10
    res = mmf.compare_policy(c, h)
    assert res["sessions"] == 2
    # every usable draw holds both sessions whole; splitting s1's events would spread the C mean
    assert res["ci"] == pytest.approx((3.5, 3.5))
    assert 0 < res["draws_used"] < mmf.BOOT_DRAWS
    assert res["answer"] == "content adds leverage"


# --------------------------------------------------------------------------- further rules
def test_content_start_needs_both_ends_of_the_window_polled(tmp_path, cal_dir):
    w = World(tmp_path, cal_dir, D(10, 1), D(11, 30))
    w.poll(at(D(11, 9), 9, 5))                                   # 11-11's window opens on 11-09 (11-10 holiday)
    w.poll(at(D(11, 11), 8, 35), errored=("ndtvprofit",))       # an errored poll is not a poll
    w.poll(at(D(11, 11), 10, 0))                                 # 10:00 is outside [09:00, 10:00)
    w.poll(at(D(11, 12), 8, 20))
    w.poll(at(D(11, 12), 9, 30))
    w.poll(at(D(11, 13), 9, 0))                                  # outside [08:00, 09:00); opens 11-16's window
    w.poll(at(D(11, 16), 8, 59), skip=("bs_companies",))        # a feed missing
    w.poll(at(D(11, 16), 9, 10))
    w.poll(at(D(11, 17), 8, 10))
    assert w.evaluate(D(11, 30))["meta"]["content_start"] == D(11, 17)
    assert w.evaluate(D(11, 16))["meta"]["content_start"] is None

    root = tmp_path / "holiday"
    root.mkdir()
    w2 = World(root, cal_dir, D(10, 1), D(11, 30))
    w2.poll(at(D(11, 24), 9, 30))                                # the holiday is not the window's first day
    w2.poll(at(D(11, 25), 8, 30))
    assert w2.evaluate(D(11, 30))["meta"]["content_start"] is None
    w2.poll(at(D(11, 23), 9, 30))                                # the session before the holiday is
    assert w2.evaluate(D(11, 30))["meta"]["content_start"] == D(11, 25)


def test_same_day_groups_sort_by_group_and_keep_one_position_per_group(tmp_path, cal_dir):
    w = World(tmp_path, cal_dir, D(10, 1), D(12, 31))
    for sym, sector in (("ITA", "IT"), ("BANKA", "BANK"), ("METALA", "METAL")):
        w.stock(sym)
        w.sector_rows.append((D(10, 1), sym, sector))
    d0, d1 = D(11, 12), D(11, 16)
    w.call(at(d0, 8), [score("k1", "sector", 0.6, 0.8, "govt_program")])
    w.call(at(d1, 8), [score("k2", "sector", 0.6, 0.8, "govt_program")])
    w.mapping("k1", "headline", ["METAL", "BANK"])
    w.mapping("k2", "headline", ["IT", "BANK"])                 # BANK is still held from d0
    doc = w.evaluate(D(12, 31))
    h = hyp(doc, "policy_headline")
    assert [(t["entry_date"], t["group"]) for t in h["trades"]] == [(d0, "BANK"), (d0, "METAL"), (d1, "IT")]


def test_basket_with_every_leg_voided_is_no_trade_and_blocks_nothing(tmp_path, cal_dir):
    w = World(tmp_path, cal_dir, D(10, 1), D(12, 31))
    sessions = w.sessions
    d0 = D(11, 12)
    k = sessions.index(d0)
    d2 = sessions[k + 2]
    w.stock("METALA")
    w.stock("BANKA")
    w.corp.append(("METALA", d0, "bonus"))                       # inside d0's window, before d2's
    w.sector_rows += [(D(10, 1), "METALA", "METAL"), (D(10, 1), "BANKA", "BANK")]
    w.call(at(d0, 8), [score("k1", "sector", 0.6, 0.8, "govt_program")])
    w.call(at(d2, 8), [score("k2", "sector", 0.6, 0.8, "govt_program")])
    w.mapping("k1", "headline", ["METAL"])
    w.mapping("k2", "headline", ["METAL"])
    h = hyp(w.evaluate(D(12, 31)), "policy_headline")
    assert [(t["entry_date"], t["group"]) for t in h["trades"]] == [(d2, "METAL")]
    assert h["counts"]["no_usable_leg"] == 1 and h["counts"]["signals"] == 2


def test_comparison_uses_study_h_and_c_over_clusters_scored_both_ways(tmp_path, cal_dir):
    w = World(tmp_path, cal_dir, D(10, 1), D(12, 31))
    sessions = w.sessions
    d0 = D(11, 12)
    exit_d = sessions[sessions.index(d0) + 9]
    w.stock("AUTOA", bars={exit_d: (100.0, 110.0, 2_000_000)})
    w.stock("BANKA", bars={exit_d: (100.0, 96.0, 2_000_000)})
    w.stock("ITA")
    w.sector_rows += [(D(10, 1), "AUTOA", "AUTO"), (D(10, 1), "BANKA", "BANK"), (D(10, 1), "ITA", "IT")]
    w.poll(at(D(11, 9), 9, 5))
    w.poll(at(D(11, 11), 8, 30))
    w.cluster("c-1", at(d0, 8), "Cabinet clears a scheme", summary="Support for the auto industry")
    w.study_score("c-1", "H", "sector", 0.6, 0.8, "govt_program")
    w.study_score("c-1", "C", "sector", 0.6, 0.8, "govt_program")
    w.mapping("c-1", "content", ["AUTO"])
    w.mapping("c-1", "study_h", ["BANK"])
    w.cluster("c-2", at(d0, 8, 30), "A policy story scored C only")
    w.study_score("c-2", "C", "sector", 0.6, 0.8, "sector_policy")
    w.mapping("c-2", "content", ["IT"])
    w.mapping("c-3", "study_h", ["IT"], model="claude-opus-5-5")   # an off-model mapping is dropped and counted
    doc = w.evaluate(D(12, 31))
    assert doc["meta"]["content_start"] == D(11, 11)
    assert [t["group"] for t in hyp(doc, "policy_content")["trades"]] == ["AUTO", "IT"]
    p = doc["comparison"]["policy"]
    universe = (10.0 - 4.0 + 0.0) / 3
    assert (p["n_c"], p["n_h"]) == (1, 1)
    assert p["mean_c"] == pytest.approx(10.0 - universe) and p["mean_h"] == pytest.approx(-4.0 - universe)
    assert p["answer"] == "not enough data" and p["ci"] is None and p["read"] is False     # one event a side
    assert doc["counts"]["study_mappings_excluded_model"] == 1


def test_empty_content_file_and_an_early_as_of_still_report(tmp_path, cal_dir):
    w = World(tmp_path, cal_dir, D(6, 1), D(11, 30))
    w.content_db.unlink()
    sqlite3.connect(w.content_db).close()
    w.call(at(D(11, 4), 8), [score("a", "market", 0.5, 1.0)])
    doc = w.evaluate(D(11, 5))
    assert doc["meta"]["headline_key"] is None and doc["meta"]["content_start"] is None
    assert doc["counts"]["engine_calls_excluded"] == 0 and doc["counts"]["engine_clusters_excluded"] == 1
    assert len(doc["meta"]["study_tables_missing"]) == 6
    assert {h["status_text"] for h in doc["hypotheses"].values()} == {"accruing 0/30"}
    assert doc["comparison"]["tone"]["answer"] == "not enough data"
    assert "LOOKS" in mmf.render(doc)


def test_theme_keywords_are_themes_yaml():
    doc = load_yaml(config_dir() / "themes.yaml")
    assert {k: tuple(v["keywords"]) for k, v in doc["themes"].items()} == mmf.THEME_KEYWORDS


def test_cli_prints_unavailable_and_exits_zero_and_never_writes_the_stores(tmp_path, cal_dir, capsys):
    w = World(tmp_path, cal_dir, D(10, 1), D(11, 30))
    w.call(at(D(11, 12), 8), [score("a", "market", 0.5, 1.0)])
    w.build_snapshot()
    out = tmp_path / "reports" / "fwd.json"
    etfs = tmp_path / "etf.csv"
    etfs.write_text("Company Name,Industry,Symbol,Series,ISIN Code\nNippon Nifty,ETF,NIFTYBEES,EQ,INF204KB14I2\n",
                    encoding="utf-8")
    before = (w.state_db.read_bytes(), w.content_db.read_bytes())
    rc = mmf.main(["--state-db", str(w.state_db), "--content-db", str(w.content_db), "--db", str(w.snapshot),
                   "--as-of", "2026-11-30", "--out", str(out), "--etf-list", str(etfs)])
    assert rc == 0
    assert json.loads(out.read_text(encoding="utf-8"))["meta"]["as_of"] == "2026-11-30"
    assert out.with_suffix(".md").is_file()
    assert (w.state_db.read_bytes(), w.content_db.read_bytes()) == before
    capsys.readouterr()

    rc = mmf.main(["--state-db", str(tmp_path / "missing.db"), "--content-db", str(w.content_db),
                   "--db", str(w.snapshot), "--as-of", "2026-11-30", "--out", str(out), "--etf-list", str(etfs)])
    assert rc == 0
    assert "UNAVAILABLE" in capsys.readouterr().out
