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
             stamp: str | None = None, context: bytes | None = None) -> str:
        call_id = uuid.uuid4().hex
        heads = heads or {}
        rows = [{"cluster_id": s["cluster_id"], "representative": heads.get(s["cluster_id"], f"story {s['cluster_id']}"),
                 "source_domains": ["a.com"], "first_seen": when} for s in scores]
        blob = context if context is not None else gzip.compress(
            f"{news_analyst.SYSTEM_PROMPT}\n\n{engine_prompt(rows)}".encode())
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
        con.execute("INSERT INTO llm_calls (call_id, at, purpose, model, prompt_sha256, ok, error, effort, "
                    "system_sha256, cli) VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (call_id, when.isoformat(), purpose, model, hashlib.sha256(b"x").hexdigest(), ok, None,
                     "default", hashlib.sha256(b"s").hexdigest(), "claude.exe"))
        return call_id

    def cluster(self, key: str, captured: datetime, title: str, *, summary: str | None = None,
                published: datetime | None | str = "captured") -> None:
        """A one-item cluster; its item is published when captured unless ``published`` says otherwise."""
        url = f"https://a.com/{key}"
        pub = captured if published == "captured" else published
        con = sqlite3.connect(self.content_db)
        con.execute("INSERT INTO items VALUES (?,?,?,?,?,?)",
                    (url, "et", title, summary, pub.isoformat() if pub is not None else None, captured.isoformat()))
        con.execute("INSERT INTO clusters VALUES (?,?,?,?,?,?,?,?)",
                    (key, captured.isoformat(), url, json.dumps([url]), title, json.dumps(["a.com"]),
                     captured.isoformat(), captured.isoformat()))
        con.execute("INSERT INTO cluster_members VALUES (?, ?)", (url, key))
        con.commit()
        con.close()

    def item(self, url: str, captured: datetime, title: str = "an unclustered story") -> None:
        con = sqlite3.connect(self.content_db)
        con.execute("INSERT INTO items VALUES (?,?,?,?,?,?)",
                    (url, "et", title, None, captured.isoformat(), captured.isoformat()))
        con.commit()
        con.close()

    def study_score(self, key: str, variant: str, scope: str = "market", sentiment: float = 0.5,
                    materiality: float = 1.0, event_type: str = "macro_data", *, model: str = HAIKU,
                    omitted: bool = False) -> None:
        con = sqlite3.connect(self.content_db)
        call_id = self._llm_call(con, f"score_{variant}", model, at(D(11, 30), 22))
        if omitted:
            con.execute("INSERT INTO scores (cluster_key, variant, call_id, omitted) VALUES (?,?,?,1)",
                        (key, variant, call_id))
        else:
            con.execute("INSERT INTO scores (cluster_key, variant, scope, event_type, sentiment, materiality, "
                        "novelty, call_id) VALUES (?,?,?,?,?,?,?,?)",
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


# --------------------------------------------------------------------------- windows, keys, baskets, looks, the comparison
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
    short = tmp_path / "calendar_2024_2026"
    short.mkdir()
    for y in (2024, 2025, 2026):
        (short / f"{y}.yaml").write_bytes((cal_dir / f"{y}.yaml").read_bytes())
    with pytest.raises(mmf.Unavailable, match="2027"):
        w.evaluate(D(1, 15, 2027), calendar_dir=short)


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
    # Thin every session but D: one huge bar cannot lift a 20-session median, so it is never liquid, never a leg.
    w.stock("AUTOC", volume=10, bars={d0: (100.0, 100.0, 1_000_000_000)})
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
    assert len(doc["meta"]["study_tables_missing"]) == 7
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


def test_cli_exits_zero_when_rendering_fails(tmp_path, cal_dir, capsys, monkeypatch):
    w = World(tmp_path, cal_dir, D(10, 1), D(11, 30))
    w.build_snapshot()
    etfs = tmp_path / "etf.csv"
    etfs.write_text("Symbol\nNIFTYBEES\n", encoding="utf-8")

    def broken(doc):
        raise KeyError("a field the renderer expects")

    monkeypatch.setattr(mmf, "render", broken)
    out = tmp_path / "reports" / "fwd.json"
    assert mmf.main(["--state-db", str(w.state_db), "--content-db", str(w.content_db), "--db", str(w.snapshot),
                     "--as-of", "2026-11-30", "--out", str(out), "--etf-list", str(etfs)]) == 0
    assert "UNAVAILABLE: unexpected KeyError" in capsys.readouterr().out
    assert not out.exists()


def test_content_tables_are_read_in_one_transaction(tmp_path, cal_dir, monkeypatch):
    w = World(tmp_path, cal_dir, D(10, 1), D(11, 30))
    w.cluster("c-1", at(D(11, 12), 8), "a market story")
    w.study_score("c-1", "C", "market", 0.4, 1.0)
    real = mmf._read_content
    blocked: list[str] = []

    def mid_read(con, *args):
        con.execute("SELECT count(*) FROM llm_calls").fetchone()     # the read has begun
        writer = sqlite3.connect(w.content_db, timeout=0)
        try:
            writer.execute("INSERT INTO llm_calls (call_id, at, purpose, model, prompt_sha256, ok, effort, "
                           "system_sha256, cli) VALUES ('late', 'x', 'score_C', 'm', 'h', 1, 'e', 's', 'c')")
            writer.commit()
        except sqlite3.OperationalError as exc:
            blocked.append(str(exc))
        finally:
            writer.close()
        return real(con, *args)

    monkeypatch.setattr(mmf, "_read_content", mid_read)
    study = mmf.load_content(w.content_db, at(D(11, 30), 9), D(11, 30))
    assert blocked and "locked" in blocked[0]          # a scorer's commit waits until the whole read is done
    assert [s.key for s in study.scores["C"]] == ["c-1"]


# --------------------------------------------------------------------------- looks form on final inputs
def _thirty_trade_world(tmp_path, cal_dir):
    """23 liquid one-stock sector groups. Events at d0 on 20 groups and at d1 = d0 + 10 sessions on 10 groups give
    30 policy trades, the 30th closing at e1 = d0 + 19 sessions: the look's cut."""
    w = World(tmp_path, cal_dir, D(10, 1), D(1, 29, 2027))
    sectors = [g for g in mmf.VOCAB[:23]]
    for i, sector in enumerate(sectors):
        w.stock(f"S{i:02d}")
        w.sector_rows.append((D(10, 1), f"S{i:02d}", sector))
    s = w.sessions
    k0 = s.index(D(11, 12))
    days = {"d0": s[k0], "d2": s[k0 + 5], "d1": s[k0 + 10], "e1": s[k0 + 19], "d3": s[k0 + 21]}
    return w, sectors, days


def policy(cid: str) -> dict:
    return score(cid, "sector", 0.6, 0.8, "govt_program")


def look(doc: dict, name: str) -> tuple:
    h = hyp(doc, name)
    v = {k: x for k, x in h["verdict"].items() if k != "cpcv"} if h["verdict"] else None
    return h["status"], h["cut"], h["trades"], v


def test_a_pending_headline_mapping_holds_the_look_until_it_lands(tmp_path, cal_dir):
    w, sectors, d = _thirty_trade_world(tmp_path, cal_dir)
    w.call(at(d["d0"], 8), [policy("k1")])
    w.call(at(d["d1"], 8), [policy("k2")])
    w.mapping("k1", "headline", sectors[:20])
    w.mapping("k2", "headline", sectors[:10])
    w.call(at(d["d2"], 8), [policy("k3")])                         # unmapped, before the cut
    as_of = D(1, 29, 2027)

    h = hyp(w.evaluate(as_of), "policy_headline")
    assert h["status"] == "waiting" and h["label"] == "waiting" and h["verdict"] is None and h["cut"] is None
    assert h["status_text"] == f"waiting: 1 candidate unmapped in windows up to {d['e1']}"
    assert len(h["trades"]) == 30                                   # provisional trades stay visible

    w.call(at(d["d2"], 8, 30), [policy("k5")], context=b"not gzip")  # unreadable headline: final, no groups
    assert hyp(w.evaluate(as_of), "policy_headline")["status"] == "waiting"
    w.mapping("k3", "headline", [sectors[20]])
    doc = w.evaluate(as_of)
    status, cut, trades, verdict = look(doc, "policy_headline")
    assert status == "verdict" and cut == d["e1"] and verdict["n"] == 31
    assert (sectors[20], d["d2"]) in {(t["group"], t["entry_date"]) for t in trades}
    assert hyp(doc, "policy_headline")["pending"] is None

    w.call(at(d["d3"], 8), [policy("k4")])                         # after the cut: never holds the formed look
    after = w.evaluate(as_of)
    assert look(after, "policy_headline") == look(doc, "policy_headline")
    w.mapping("k4", "headline", [sectors[21]])
    mapped = w.evaluate(as_of)
    assert look(mapped, "policy_headline") == look(doc, "policy_headline")
    assert hyp(mapped, "policy_headline")["trades_after_look"] == 1


def test_unclustered_and_unscored_content_hold_the_content_look(tmp_path, cal_dir):
    w, sectors, d = _thirty_trade_world(tmp_path, cal_dir)
    w.poll(at(D(11, 9), 9, 5))
    w.poll(at(D(11, 11), 8, 30))
    for key, day, groups in (("c-1", d["d0"], sectors[:20]), ("c-2", d["d1"], sectors[:10])):
        w.cluster(key, at(day, 8), f"cabinet clears scheme {key}")
        w.study_score(key, "C", "sector", 0.6, 0.8, "govt_program")
        w.mapping(key, "content", groups)
    as_of = D(1, 29, 2027)
    base = hyp(w.evaluate(as_of), "policy_content")
    assert base["status"] == "verdict" and base["cut"] == d["e1"]

    w.item("https://a.com/blank", at(d["d2"], 8), title=" -- ")    # never clustered: no words to cluster on
    assert hyp(w.evaluate(as_of), "policy_content")["status"] == "verdict"
    w.item("https://a.com/u1", at(d["d2"], 8))
    doc = w.evaluate(as_of)
    p = hyp(doc, "policy_content")
    assert p["status_text"] == f"waiting: 1 item unclustered in windows up to {d['e1']}"
    assert doc["comparison"]["policy"]["read"] is False

    con = sqlite3.connect(w.content_db)
    con.execute("DELETE FROM items WHERE url = 'https://a.com/u1'")
    con.commit()
    con.close()
    w.cluster("c-3", at(d["d2"], 8), "a cluster still to be scored")
    assert hyp(w.evaluate(as_of), "policy_content")["status_text"] == (
        f"waiting: 1 cluster unscored in windows up to {d['e1']}")
    w.study_score("c-3", "C", omitted=True)                         # an omission is final
    final = hyp(w.evaluate(as_of), "policy_content")
    assert (final["status"], final["cut"], final["trades"]) == (base["status"], base["cut"], base["trades"])


def test_an_off_model_score_leaves_its_cluster_pending(tmp_path, cal_dir):
    w, sectors, d = _thirty_trade_world(tmp_path, cal_dir)
    w.poll(at(D(11, 9), 9, 5))
    w.poll(at(D(11, 11), 8, 30))
    for key, day, groups in (("c-1", d["d0"], sectors[:20]), ("c-2", d["d1"], sectors[:10])):
        w.cluster(key, at(day, 8), f"cabinet clears scheme {key}")
        w.study_score(key, "C", "sector", 0.6, 0.8, "govt_program")
        w.mapping(key, "content", groups)
    w.cluster("c-3", at(d["d2"], 8), "a cluster scored by another model")
    w.study_score("c-3", "C", "market", 0.5, 1.0, model="claude-opus-5-5")
    doc = w.evaluate(D(1, 29, 2027))
    assert hyp(doc, "policy_content")["status"] == "waiting"
    assert doc["counts"]["study_scores_excluded_model"] == 1


def test_a_content_cluster_is_placed_by_the_later_of_capture_and_publication(tmp_path, cal_dir):
    w = World(tmp_path, cal_dir, D(10, 1), D(11, 30))
    w.cluster("c-1", at(D(11, 12), 8), "published after it was captured", published=at(D(11, 12), 9, 30))
    w.study_score("c-1", "C", "market", 0.4, 1.0)
    w.cluster("c-2", at(D(11, 16), 8), "captured after it was published", published=at(D(11, 13), 7))
    w.study_score("c-2", "C", "market", -0.2, 1.0)
    w.cluster("c-3", at(D(11, 17), 8), "no publication time", published=None)
    w.study_score("c-3", "C", "market", 0.9, 1.0)
    doc = w.evaluate(D(11, 30))
    assert tone(doc, "content", D(11, 12)) is None
    assert tone(doc, "content", D(11, 13)) == pytest.approx(0.4)
    assert tone(doc, "content", D(11, 16)) == pytest.approx(-0.2)
    assert tone(doc, "content", D(11, 17)) is None
    assert doc["counts"]["content_no_published_at"] == 1


def test_capture_completeness_is_reported_per_session_from_the_content_start(tmp_path, cal_dir):
    w = World(tmp_path, cal_dir, D(10, 1), D(11, 30))
    w.poll(at(D(11, 9), 9, 5))
    w.poll(at(D(11, 11), 8, 30))                                    # content start 11-11
    w.poll(at(D(11, 12), 8, 59))
    w.poll(at(D(11, 13), 8, 30), skip=("et",))
    w.poll(at(D(11, 16), 8, 30), errored=("bs_markets",))
    w.poll(at(D(11, 18), 9, 0))                                     # 09:00 is outside [08:00, 09:00)
    doc = w.evaluate(D(11, 18))
    assert doc["meta"]["content_start"] == D(11, 11)
    assert doc["meta"]["capture_incomplete"] == {"n": 4, "dates": [D(11, 13), D(11, 16), D(11, 17), D(11, 18)]}
    assert "capture incomplete (a feed missed its 08:00-09:00 poll; decides nothing): 4 session(s): " \
           "2026-11-13, 2026-11-16, 2026-11-17, 2026-11-18" in mmf.render(doc)
    assert w.evaluate(D(11, 10))["meta"]["capture_incomplete"] == {"n": 0, "dates": []}   # no start yet


def test_a_special_session_bar_is_not_a_session(tmp_path, cal_dir):
    w = World(tmp_path, cal_dir, D(9, 1), D(11, 30))
    s = w.sessions
    sunday = D(11, 8)                                               # the muhurat session, not a regular one
    assert sunday not in s
    w.nifty[sunday] = (100.0, 100.5)
    w.stock("S1")
    w.stocks["S1"][sunday] = (100.0, 100.0, 2_000_000)
    i9 = s.index(D(11, 9))
    w.stocks["EDGE"] = {d: (100.0, 100.0, 2_000_000) for d in s[i9 - 18:i9 + 1]} | {
        sunday: (100.0, 100.0, 2_000_000)}                          # 19 regular bars to 11-09, plus the Sunday
    w.sector_rows += [(D(9, 1), "S1", "BANK"), (sunday, "S1", "IT")]
    conn = mmf.rc.open_snapshot(w.build_snapshot())
    try:
        m = mmf.load_market(conn, D(11, 30), frozenset(), {}, mmf.load_calendar(cal_dir))
        assert sunday not in m.sessions and m.skip_days == frozenset({sunday})
        assert mm.index_trade(m.nifty, m.sessions, D(11, 5), 5)[0] == D(11, 12)   # Thu, Fri, Mon, Wed, Thu
        d_prev = m.sessions[m.pos[D(11, 9)] - 1]
        assert d_prev == D(11, 6)                                   # D-1 of the Monday is the Friday
        assert mmf.members(m, "IT", d_prev) == frozenset() and mmf.members(m, "BANK", d_prev) == {"S1"}
        assert "EDGE" in mm.liquid_universe(conn, D(11, 9), frozenset())          # counting the Sunday: 20 bars
        assert "EDGE" not in mmf._liquid(m, D(11, 9)) and "S1" in mmf._liquid(m, D(11, 9))
    finally:
        mm._LIQUID_CACHE.pop(id(conn), None)
        conn.close()


def test_the_headline_path_keeps_each_clusters_first_score(tmp_path, cal_dir):
    w = World(tmp_path, cal_dir, D(10, 1), D(12, 31))
    for sym, sector in (("BANKA", "BANK"), ("ITA", "IT")):
        w.stock(sym)
        w.sector_rows.append((D(10, 1), sym, sector))
    w.call(at(D(11, 12), 8), [score("A", "market", 0.5, 1.0), score("P", "sector", 0.6, 0.8, "other")])
    w.call(at(D(11, 13), 7), [score("A", "market", -0.9, 1.0), policy("P")])     # later re-scores
    w.mapping("P", "headline", ["BANK"])
    doc = w.evaluate(D(12, 31))
    assert tone(doc, "headline", D(11, 12)) == pytest.approx(0.5)
    assert tone(doc, "headline", D(11, 13)) is None
    p = hyp(doc, "policy_headline")
    assert p["counts"]["candidates_candidates"] == 0 and p["trades"] == []


def test_an_omitted_row_adds_nothing_and_is_final(tmp_path, cal_dir):
    w = World(tmp_path, cal_dir, D(10, 1), D(12, 31))
    w.poll(at(D(11, 9), 9, 5))
    w.poll(at(D(11, 11), 8, 30))
    w.cluster("c-1", at(D(11, 12), 8), "a story the model left out")
    w.study_score("c-1", "C", omitted=True)
    w.study_score("c-1", "H", "market", 0.7, 1.0)
    w.cluster("c-2", at(D(11, 12), 8, 10), "a story both variants scored")
    w.study_score("c-2", "C", "market", 0.3, 1.0)
    w.study_score("c-2", "H", "market", 0.1, 1.0)
    w.cluster("c-3", at(D(11, 13), 8), "an H score still to come")
    w.study_score("c-3", "C", "sector", 0.6, 0.8, "govt_program")
    w.mapping("c-3", "content", ["BANK"])
    doc = w.evaluate(D(12, 31))
    assert tone(doc, "content", D(11, 12)) == pytest.approx(0.3)
    assert tone(doc, "study_c", D(11, 12)) == pytest.approx(0.3) and tone(doc, "study_h", D(11, 12)) == (
        pytest.approx(0.1))
    assert hyp(doc, "policy_content")["counts"]["candidates_candidates"] == 1     # c-3 only
    assert doc["comparison"]["h_side"] == {"clusters_unscored_h": 1, "study_h_candidates_unmapped": 0,
                                           "comparison_excluded_omitted": 1}
    for name in ("news_tone_content", "policy_content"):
        assert hyp(doc, name)["status"] == "accruing"
