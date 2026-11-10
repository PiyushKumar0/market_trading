"""``scripts/news_impact_study.py``: the news impact study's signals, trades, populations, verdicts and report.

Every store is synthetic and lives under ``tmp_path``: a research DuckDB file with random-walk bars, an
``agent_calls`` SQLite and a mapper ``mappings`` SQLite. The calendar is the pinned config copy's (2026: 11-10 is a
holiday). Nothing here reads ``data/``: the ETF list and theme lists are passed in.
"""

from __future__ import annotations

import gzip
import importlib.util
import json
import re
import sqlite3
import sys
import types
import uuid
from datetime import date, datetime
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


nic = _load("mt_news_impact_common", "news_impact_common.py")
nis = _load("mt_news_impact_study", "news_impact_study.py")

from engine.intelligence.agents import news_analyst  # noqa: E402
from engine.intelligence.context import ContextAssembler  # noqa: E402

IST = ZoneInfo("Asia/Kolkata")
#: The synthetic copy's last session: sample A's last possible exit is 13 sessions after its last entry (10-23),
#: on 11-12.
AS_OF = date(2026, 11, 13)
SECTORS = {"AUTO": ("X", "P1", "P2", "P3", "P4", "P5"), "BANK": ("B1", "B2", "B3", "B4", "B5", "B6"),
           "METAL": ("M1", "M2", "M3", "M4")}
#: Liquid, but in no sector of the snapshot.
LOOSE = ("NOSEC",)
SNAP = date(2026, 11, 8)


def at(day: date, hh: int, mi: int = 0) -> datetime:
    return datetime(day.year, day.month, day.day, hh, mi, tzinfo=IST)


@pytest.fixture(scope="module")
def cal():
    return nic.load_calendar()


@pytest.fixture(scope="module")
def sessions(cal) -> list[date]:
    return nic.regular_sessions(cal, date(2025, 7, 1), AS_OF)


def nth(sessions: list[date], d: date, k: int) -> date:
    return sessions[sessions.index(d) + k]


def first_on_or_after(sessions: list[date], d: date) -> date:
    return next(s for s in sessions if s >= d)


# --------------------------------------------------------------------------- synthetic stores
def build_market(path: Path, sessions: list[date], *, news_rows: list[tuple] | None = None) -> Path:
    """Random-walk daily bars for every name (so betas and universe returns exist), NIFTY 50 on every session, one
    ``sector_map`` snapshot, an alias per name (``<sym>corp``), a theme row and, optionally, a ``news`` table."""
    rng = np.random.default_rng(7)
    rows = []
    nifty = 100.0
    for d in sessions:
        o, nifty = nifty, nifty * (1.0 + rng.normal(0.0, 0.01))
        rows.append(("NIFTY 50", d, o, max(o, nifty), min(o, nifty), nifty, 0))
    names = [s for group in SECTORS.values() for s in group] + list(LOOSE)
    for sym in names:
        px = 100.0
        for d in sessions:
            o, px = px, px * (1.0 + rng.normal(0.0, 0.01))
            rows.append((sym, d, o, max(o, px) * 1.002, min(o, px) * 0.998, px, 2_000_000))
    conn = duckdb.connect(str(path))
    conn.execute('CREATE TABLE bars_1d (symbol TEXT NOT NULL, d DATE NOT NULL, "open" DOUBLE NOT NULL, '
                 'high DOUBLE NOT NULL, low DOUBLE NOT NULL, "close" DOUBLE NOT NULL, volume BIGINT NOT NULL, '
                 "src TEXT NOT NULL DEFAULT 'kite_official', PRIMARY KEY (symbol, d))")
    conn.execute("CREATE TABLE corp_actions (symbol TEXT NOT NULL, ex_date DATE NOT NULL, kind TEXT NOT NULL, "
                 "ratio TEXT, amount DECIMAL(12,2), source TEXT, recorded_at TIMESTAMPTZ, "
                 "PRIMARY KEY (symbol, ex_date, kind))")
    conn.execute("CREATE TABLE sector_map (as_of DATE NOT NULL, symbol TEXT NOT NULL, sector TEXT NOT NULL, "
                 "PRIMARY KEY (as_of, symbol))")
    conn.execute("CREATE TABLE entity_aliases (alias TEXT NOT NULL, tradingsymbol TEXT NOT NULL, "
                 "source TEXT NOT NULL DEFAULT 'seed', added_at TIMESTAMPTZ, PRIMARY KEY (alias, tradingsymbol))")
    conn.execute("CREATE TABLE theme_map (theme TEXT PRIMARY KEY, keywords TEXT[] NOT NULL, symbols TEXT[] NOT NULL, "
                 "updated_at TIMESTAMPTZ)")
    df = pd.DataFrame(rows, columns=["symbol", "d", "o", "h", "l", "c", "v"])
    conn.register("bars_df", df)
    conn.execute('INSERT INTO bars_1d (symbol, d, "open", high, low, "close", volume) '
                 "SELECT symbol, d, o, h, l, c, v FROM bars_df")
    conn.unregister("bars_df")
    conn.executemany("INSERT INTO sector_map VALUES (?, ?, ?)",
                     [(SNAP, s, sector) for sector, syms in SECTORS.items() for s in syms])
    conn.executemany("INSERT INTO entity_aliases (alias, tradingsymbol, source) VALUES (?, ?, 'seed')",
                     [(f"{s.lower()}corp", s) for s in names])
    conn.execute("INSERT INTO theme_map VALUES ('defence', ['missile'], [], NULL)")
    if news_rows is not None:
        conn.execute("CREATE TABLE news (headline_id TEXT PRIMARY KEY, title TEXT NOT NULL, source_domain TEXT NOT "
                     "NULL, url TEXT NOT NULL, published_at TIMESTAMPTZ NOT NULL, cluster_id TEXT, untrusted BOOLEAN "
                     "NOT NULL DEFAULT TRUE, ingested_at TIMESTAMPTZ NOT NULL)")
        conn.executemany("INSERT INTO news (headline_id, title, source_domain, url, published_at, cluster_id, "
                         "ingested_at) VALUES (?, 't', ?, 'u', ?, ?, ?)",
                         [(uuid.uuid4().hex, dom, when, cid, when) for cid, dom, when in news_rows])
    conn.close()
    return path


AGENT_CALLS_DDL = """
CREATE TABLE agent_calls (
    call_id TEXT PRIMARY KEY, agent_id TEXT NOT NULL, trigger TEXT, model TEXT, inputs_digest TEXT,
    context_gz BLOB, output_json TEXT, ok INTEGER NOT NULL, fail_reason TEXT, in_tokens INTEGER,
    out_tokens INTEGER, cache_read INTEGER, cache_write INTEGER, cost_usd TEXT, duration_ms INTEGER,
    at TEXT NOT NULL, prompt_version TEXT)
"""


def engine_prompt(rows: list[dict]) -> str:
    asm = object.__new__(ContextAssembler)
    return asm.for_news_batch(rows, ["defence"]).prompt()


class State:
    def __init__(self, path: Path) -> None:
        self.path = path
        con = sqlite3.connect(path)
        con.execute(AGENT_CALLS_DDL)
        con.commit()
        con.close()

    def call(self, when: datetime, scores: list[dict], heads: dict[str, str], model: str = "claude-sonnet-5-5"):
        rows = [{"cluster_id": s["cluster_id"], "representative": heads[s["cluster_id"]],
                 "source_domains": ["a.com"], "first_seen": when} for s in scores]
        blob = gzip.compress(f"{news_analyst.SYSTEM_PROMPT}\n\n{engine_prompt(rows)}".encode())
        con = sqlite3.connect(self.path)
        con.execute("INSERT INTO agent_calls (call_id, agent_id, model, context_gz, output_json, ok, at) "
                    "VALUES (?,?,?,?,?,?,?)",
                    (uuid.uuid4().hex, "news_analyst", model, blob, json.dumps({"scores": scores}), 1,
                     when.isoformat()))
        con.commit()
        con.close()

    def stock(self, cid: str, sym: str, when: datetime, sentiment: float = 0.8, materiality: float = 0.9,
              event_type: str = "order_win") -> None:
        self.call(when, [_score(cid, "stock", sentiment, materiality, event_type)],
                  {cid: f"{sym.lower()}corp shares in focus"})

    def domain(self, cid: str, when: datetime, scope: str = "sector", event_type: str = "sector_policy") -> None:
        self.call(when, [_score(cid, scope, 0.5, 0.8, event_type)], {cid: f"policy story {cid}"})

    def glob(self, cid: str, when: datetime) -> None:
        self.call(when, [_score(cid, "market", 0.5, 0.8, "global_market")], {cid: f"global story {cid}"})


def _score(cid: str, scope: str, sentiment: float, materiality: float, event_type: str) -> dict:
    return {"cluster_id": cid, "scope": scope, "entities": [], "sectors": [], "themes": [], "sentiment": sentiment,
            "materiality": materiality, "event_type": event_type, "novelty": 0.5}


def write_mappings(path: Path, rows: dict[str, tuple[int, list, list]]) -> Path:
    """The mapper's ``mappings`` table: ``groups`` and ``companies`` are JSON lists of ``[name, direction]``."""
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE IF NOT EXISTS mappings(cluster_id TEXT PRIMARY KEY, call_rowid INTEGER NOT NULL, "
                "market INTEGER NOT NULL, groups TEXT NOT NULL, companies TEXT NOT NULL)")
    con.executemany("INSERT INTO mappings VALUES (?, 1, ?, ?, ?)",
                    [(cid, m, json.dumps(g), json.dumps(c)) for cid, (m, g, c) in rows.items()])
    con.commit()
    con.close()
    return path


@pytest.fixture(scope="module")
def d1(sessions) -> date:
    return first_on_or_after(sessions, date(2026, 8, 3))


@pytest.fixture(scope="module")
def market_db(tmp_path_factory, sessions, d1) -> Path:
    cx = [("sx1", "a.com", at(d1, 7)), ("sx1", "b.com", at(d1, 7, 30)), ("sx1", "c.com", at(d1, 9)),
          ("sx1", "a.com", at(d1, 7, 40))]
    return build_market(tmp_path_factory.mktemp("market") / "full_market.duckdb", sessions, news_rows=cx)


def run(state: State, db: Path, maps: Path, as_of: date = AS_OF) -> dict:
    return nis.run("A", state.path, db, maps, as_of, etfs=frozenset(), themes={})


def story(cid: str, when: datetime, *, scope: str = "stock", sentiment: float = 0.8, materiality: float = 0.9,
          event_type: str = "order_win", model: str = "m") -> object:
    return nic.Story(cid, when, "call", model, scope, event_type, sentiment, materiality, (), "h")


# --------------------------------------------------------------------------- waiting
def test_waiting_when_unmapped(tmp_path, market_db, sessions, d1):
    st = State(tmp_path / "state.db")
    d_ex = first_on_or_after(sessions, date(2026, 10, 26))
    st.stock("sx1", "X", at(d1, 8))
    st.domain("dom1", at(d1, 8))                               # in the sample window, unmapped
    st.domain("dom2", at(d_ex, 8))                             # in the excluded window, unmapped
    st.glob("glb1", at(d_ex, 8))                               # mapped
    st.domain("dom0", at(date(2026, 7, 27), 8))                # enters before the sample window: not needed
    maps = write_mappings(tmp_path / "maps.sqlite", {"glb1": (1, [["BANK", 1]], [])})
    doc = run(st, market_db, maps)
    assert doc["status"] == "waiting: 2 stories unmapped (domain 2, global 0)"
    assert doc["hypotheses"] == {} and doc["diagnostics"] == {} and doc["excluded_window"] == {}
    out = tmp_path / "rep" / "r.json"
    rc = nis.main(["--sample", "A", "--db", str(market_db), "--state-db", str(st.path), "--mappings", str(maps),
                   "--as-of", AS_OF.isoformat(), "--out", str(out)], etfs=frozenset(), themes={})
    assert rc == 0
    assert json.loads(out.read_text(encoding="utf-8"))["status"].startswith("waiting: 2 stories unmapped")
    # no mappings store at all reads as nothing mapped yet
    assert run(st, market_db, tmp_path / "absent.sqlite")["status"] == "waiting: 3 stories unmapped (domain 2, global 1)"


# --------------------------------------------------------------------------- signals
def test_signal_dedupe_same_day(sessions, d1):
    prev, prev2 = nth(sessions, d1, -1), nth(sessions, d1, -2)
    a = story("a", at(prev, 11), sentiment=0.8, materiality=0.9)      # intraday on D-1: P0 = close(D-2)
    b = story("b", at(d1, 8), sentiment=0.5, materiality=0.7)         # before D's open: P0 = close(D-1)
    c = story("c", at(d1, 8, 30), sentiment=0.1, materiality=0.2)     # neither material nor positive
    placed = nis.place([a, b, c], sessions)
    assert [p.d for p in placed] == [d1, d1, d1]
    syms = {"a": frozenset({"X"}), "b": frozenset({"X", "Y"}), "c": frozenset({"X"})}
    window = (d1, d1)
    sig, counts = nis.stock_signals(placed, syms, window, sessions)
    by = {s.key: s for s in sig}
    assert sorted(by) == ["X", "Y"]
    assert [s.cluster_id for s in by["X"].stories] == ["a", "b"]
    assert by["X"].p0_session == prev2 and by["Y"].p0_session == prev
    assert counts["stories"] == 2 and counts["conflict"] == 0
    info, _ = nis.info_signals(placed, syms, window)
    x = next(s for s in info if s.key == "X")
    assert len([s for s in info if s.key == "X"]) == 1
    assert nis.info_x(x) == pytest.approx((0.8 * 0.9 + 0.5 * 0.7 + 0.1 * 0.2) / 3)
    # a story without a resolved symbol is counted, never a signal
    _, counts = nis.stock_signals(placed, {"a": frozenset()}, window, sessions)
    assert counts["stories_unresolved"] == 2


def test_conflicting_direction_skips(sessions, d1):
    d2 = nth(sessions, d1, 1)
    pos = story("p", at(d1, 8), sentiment=0.6, materiality=0.7)
    neg = story("n", at(d1, 8), sentiment=-0.5, materiality=0.6)
    neg_weak = story("w", at(d1, 8), sentiment=-0.9, materiality=0.5)  # not material: no conflict
    neg_later = story("l", at(d2, 8), sentiment=-0.9, materiality=0.9)  # another day: no conflict
    placed = nis.place([pos, neg, neg_weak, neg_later], sessions)
    sig, counts = nis.stock_signals(placed, {"p": frozenset({"X", "Y"}), "n": frozenset({"X"}),
                                             "w": frozenset({"Y"}), "l": frozenset({"Y"})}, (d1, d2), sessions)
    assert [s.key for s in sig] == ["Y"] and counts["conflict"] == 1
    # domain stories: a company another domain story maps -1 on the same D is skipped, and so is a group
    s1 = story("d1", at(d1, 8), scope="sector", event_type="sector_policy")
    s2 = story("d2", at(d1, 8, 30), scope="theme", event_type="other")
    s3 = story("d3", at(d2, 8), scope="sector", event_type="sector_policy")
    placed = nis.place([s1, s2, s3], sessions)
    maps = {"d1": (1, [["AUTO", 1], ["BANK", 1]], [["Xcorp Ltd", 1], ["Ycorp", 1], ["Nowhere", 1]]),
            "d2": (0, [["AUTO", -1]], [["X Corp", -1]]),
            "d3": (0, [["AUTO", 1]], [["Xcorp Ltd", 1]])}
    names = {"Xcorp Ltd": "X", "X Corp": "X", "Ycorp": "Y", "Nowhere": None}

    def resolve(ns):
        return [names[n] for n in ns]

    plus, minus, counts = nis.mapped_pairs(placed, maps, (d1, d2), "domain", "companies", resolve)
    sig, conflicts = nis.mapped_signals(plus, minus)
    assert [(s.key, s.d) for s in sig] == [("Y", d1), ("X", d2)] and conflicts == 1
    assert counts["unresolved_plus"] == 1 and counts["unresolved_minus"] == 0
    plus, minus, _ = nis.mapped_pairs(placed, maps, (d1, d2), "domain", "groups", None)
    sig, conflicts = nis.mapped_signals(plus, minus)
    assert [(s.key, s.d) for s in sig] == [("BANK", d1), ("AUTO", d2)] and conflicts == 1
    # a global story is not a domain story
    plus, _, _ = nis.mapped_pairs(placed, maps, (d1, d2), "global", "groups", None)
    assert plus == {}


# --------------------------------------------------------------------------- the resolver
def test_indexed_resolver_matches_the_whole_table_scan():
    conn = duckdb.connect()
    conn.execute("CREATE TABLE entity_aliases (alias TEXT, tradingsymbol TEXT, source TEXT, added_at TIMESTAMPTZ)")
    conn.execute("CREATE TABLE sector_map (as_of DATE, symbol TEXT, sector TEXT)")
    conn.execute("CREATE TABLE theme_map (theme TEXT, keywords TEXT[], symbols TEXT[], updated_at TIMESTAMPTZ)")
    conn.executemany("INSERT INTO entity_aliases (alias, tradingsymbol, source) VALUES (?, ?, ?)", [
        ("pvr", "PVRINOX", "seed"), ("pvr inox", "PVRINOX", "seed"), ("inox", "INOXWIND", "seed"),
        ("sbi", "SBIN", "curated"), ("sbi card", "SBICARD", "seed"), ("tata", "TATAMOTORS", "seed"),
        ("tata", "TATASTEEL", "seed"), ("tata steel", "TATASTEEL", "seed"), ("steel authority", "SAIL", "seed"),
        ("reliance", "RELIANCE", "curated"), ("reliance", "RELINFRA", "seed"), ("hdfc bank", "HDFCBANK", "seed"),
        ("bank of baroda", "BANKBARODA", "seed"), ("baroda", "BARODA", "seed"), ("l t", "LT", "seed")])
    conn.execute("INSERT INTO sector_map VALUES (DATE '2026-11-08', 'HDFCBANK', 'BANK')")
    conn.execute("INSERT INTO theme_map VALUES ('defence', ['missile'], [], NULL)")
    plain, fast = nic.build_resolver(conn), nis.indexed_resolver(nic.build_resolver(conn))
    texts = ["PVR Inox shares jump", "PVR and Inox Wind rally", "SBI Card SBI results", "Tata Steel Authority deal",
             "Tata stocks rally", "Reliance and HDFC Bank lead", "Bank of Baroda raises rates in Baroda",
             "L&T wins missile order", "", "nothing here", "tata tata steel steel authority", "[NSE:INFY] Infosys up"]
    for text in texts:
        for extra in ([], ["Tata Steel", "Bank of Baroda", "unknown co"]):
            c = nic.NewsCluster(cluster_id="c", representative=text, source_domains=[],
                                first_seen=at(date(2026, 8, 3), 8), last_seen=at(date(2026, 8, 3), 8))
            assert fast.resolve(c, extra_texts=extra) == plain.resolve(c, extra_texts=extra), (text, extra)
    # the whole table is restored after every call
    assert fast._aliases == plain._aliases


# --------------------------------------------------------------------------- non-overlap
def _trade(key: str, signal: date, entry: date, exit_: date) -> object:
    return nic.Trade(key, signal, entry, exit_, 1.0, 0.0, 1.0, 0.5)


def test_non_overlap_and_unfilled_blocks_nothing(sessions, d1):
    s = sessions[sessions.index(d1):]
    sig = [nis.Signal("K", s[0], ()), nis.Signal("K", s[1], ()), nis.Signal("K", s[4], ()), nis.Signal("K", s[5], ()),
           nis.Signal("K", s[9], ()), nis.Signal("J", s[1], ())]
    # K on s0 does not fill and blocks nothing; s1 fills on s2 and is held through s4 (three sessions from the fill;
    # from the signal it would end on s3), so s4 is skipped and s5 opens the next; J is another key
    outcome = {(("K", s[0])): "unfilled", ("K", s[1]): _trade("K", s[1], s[2], s[4]),
               ("K", s[5]): _trade("K", s[5], s[5], s[9]), ("J", s[1]): "few_peers"}

    def make(signal, hold):
        assert hold == 3
        return outcome[(signal.key, signal.d)]

    kept, counts = nis.keep_trades(sig, make, 3)
    assert [(g.key, g.d) for g, _ in kept] == [("K", s[1]), ("K", s[5])]
    assert counts == {"unfilled": 1, "overlap": 2, "few_peers": 1}      # s4 on s1's exit, s9 on s5's exit


# --------------------------------------------------------------------------- info
def test_info_spearman_known(sessions, d1):
    days = sessions[sessions.index(d1):][:32]
    rng = np.random.default_rng(3)
    xs = rng.normal(size=320)
    obs = [(days[i % 32], float(x), float(x)) for i, x in enumerate(xs)]
    v = nis.info_verdict(obs)
    assert v["n"] == 320 and v["sessions"] == 32
    assert v["rho"] == pytest.approx(1.0)
    assert v["ci"][0] > 0 and v["label"] == "supported"
    v = nis.info_verdict([(d, x, -y) for d, x, y in obs])
    assert v["rho"] == pytest.approx(-1.0) and v["label"] == "refuted"
    assert v["ci"][1] < 0
    short = nis.info_verdict(obs[:299])
    assert short["label"] == "insufficient" and short["rho"] == pytest.approx(1.0)
    # the fixed seed gives the same interval every run
    assert nis.info_verdict(obs)["ci"] == nis.info_verdict(obs)["ci"]
    assert nis.info_verdict([])["label"] == "insufficient"
    # a constant side has no rho: null at any size, never supported
    flat = nis.info_verdict([(d, x, 0.0) for d, x, _ in obs])
    assert flat["rho"] is None and flat["label"] == "null"


def _runs(seq: list[int], block: int) -> list[list[int]]:
    return [seq[i:i + block] for i in range(0, len(seq), block)]


@pytest.mark.parametrize("n_sessions", [25, 30, 10, 4, 1])
def test_session_draws_are_circular_moving_blocks(sessions, d1, n_sessions):
    """Each draw is S sessions in runs of BOOT_BLOCK consecutive sessions (wrapping from the last to the first),
    the last run cut short, with every observation of each chosen session; fewer sessions than a block use one
    block of S."""
    days_s = sessions[sessions.index(d1):][:n_sessions]
    rng = np.random.default_rng(1)
    # two observations on each session, shuffled: a draw takes both
    days = [days_s[i] for i in rng.permutation(np.repeat(np.arange(n_sessions), 2))]
    pos = {d: i for i, d in enumerate(days_s)}
    block = min(10, n_sessions)                                     # the registered block, the hold
    draws = list(nis._session_draws(days, 300, nic.SEED))
    assert len(draws) == 300
    starts = set()
    for idx in draws:
        assert len(idx) == 2 * n_sessions
        picked = [pos[days[i]] for i in idx]
        seq = picked[::2]
        assert picked[1::2] == seq                                 # a session's observations come together
        runs = _runs(seq, block)
        assert [len(r) for r in runs] == [block] * (n_sessions // block) + (
            [n_sessions % block] if n_sessions % block else [])
        for r in runs:
            assert all((b - a) % n_sessions == 1 % n_sessions for a, b in zip(r, r[1:], strict=False)), r
            starts.add(r[0])
    if n_sessions > 1:
        assert len(starts) > 1                                      # the starts are drawn, not fixed
    assert [d.tolist() for d in nis._session_draws(days, 5, nic.SEED)] == [d.tolist() for d in draws[:5]]
    assert list(nis._session_draws([], 5, nic.SEED)) == []


# --------------------------------------------------------------------------- end to end
def test_population_same_sessions(tmp_path, market_db, sessions, d1, cal):
    st = State(tmp_path / "state.db")
    d2 = nth(sessions, d1, 4)
    st.stock("sx1", "X", at(d1, 8))
    st.stock("sx2", "X", at(nth(sessions, d1, 2), 8))           # inside X's hold: overlap
    st.stock("sb1", "B1", at(d2, 8))
    st.stock("sm1", "M1", at(d1, 8))                             # 3 METAL peers: few_peers
    st.stock("sn1", "NOSEC", at(d1, 8))                          # no sector in the snapshot: no_sector
    st.domain("dom1", at(d1, 8))
    maps = write_mappings(tmp_path / "maps.sqlite", {"dom1": (1, [["AUTO", 1]], [["P1corp", 1]])})
    doc = run(st, market_db, maps)
    assert doc["status"] == "complete"
    h = doc["hypotheses"]["stock_next_open"]
    assert [(t["key"], t["signal_date"]) for t in h["trades"]] == [("X", d1), ("B1", d2)]
    assert h["drops"]["overlap"] == 1 and h["drops"]["few_peers"] == 1 and h["drops"]["no_sector"] == 1
    assert h["stories"] == 5 and "stories" not in h["drops"]                  # a total, not a drop
    assert h["population"]["sessions"] == [d1, d2]
    conn = nis.rc.open_snapshot(market_db)
    try:
        p = nic.load_panel(conn, nic.SAMPLES["A"][0] - nis.PANEL_LOOKBACK, AS_OF, cal, etfs=frozenset(), themes={})
        expect = 0
        for d in (d1, d2):
            d_prev = p.sessions[p.pos[d] - 1]
            expect += sum(isinstance(nic.stock_trade(p, s, d, 10, "sector"), nic.Trade)
                          for s in nic.liquid(p, d_prev))
        assert h["population"]["n"] == expect == 24                # AUTO's and BANK's six names on each session
        g = doc["hypotheses"]["domain_groups"]
        assert [t["key"] for t in g["trades"]] == ["AUTO"] and g["population"]["sessions"] == [d1]
        d_prev = p.sessions[p.pos[d1] - 1]
        assert g["population"]["n"] == sum(isinstance(nic.basket_trade(p, k, d1, 10), nic.Trade)
                                           for k in nic.ncs.VOCAB) == 3
        c = doc["hypotheses"]["domain_companies"]
        assert [t["key"] for t in c["trades"]] == ["P1"] and c["population"]["sessions"] == [d1]
        assert c["population"]["n"] == sum(isinstance(nic.stock_trade(p, s, d1, 10, "beta"), nic.Trade)
                                           for s in nic.liquid(p, d_prev))
    finally:
        conn.close()
    assert doc["hypotheses"]["info"]["trades"][0]["key"] == "X"
    # the info hypothesis has no population; its observations carry x
    assert "population" not in doc["hypotheses"]["info"]
    assert doc["hypotheses"]["info"]["trades"][0]["x"] == pytest.approx(0.8 * 0.9)


def test_excluded_window_not_counted(tmp_path, market_db, sessions, d1):
    st = State(tmp_path / "state.db")
    d_ex = first_on_or_after(sessions, date(2026, 10, 26))
    late = first_on_or_after(sessions, date(2026, 11, 2))
    st.stock("sx1", "X", at(d1, 8))
    st.stock("sx9", "X", at(d_ex, 8))
    st.stock("sp9", "P2", at(late, 8))                          # its exit is past the data: open
    doc = run(st, market_db, write_mappings(tmp_path / "maps.sqlite", {}))
    h = doc["hypotheses"]["stock_next_open"]
    assert [t["signal_date"] for t in h["trades"]] == [d1]
    assert h["verdict"]["n"] == 1 and h["population"]["sessions"] == [d1]
    assert doc["hypotheses"]["info"]["verdict"]["n"] == 1
    ex = doc["excluded_window"]
    assert ex["label"] == "not evidence" and ex["window"] == list(nic.SAMPLES["excluded"])
    e = ex["hypotheses"]["stock_next_open"]
    assert [t["signal_date"] for t in e["trades"]] == [d_ex]
    assert e["drops"]["open"] == 1
    assert "verdict" not in e and ex["info"]["n"] == 1


def test_data_gate_refuses(tmp_path, market_db, d1):
    st = State(tmp_path / "state.db")
    st.stock("sx1", "X", at(d1, 8))
    maps = write_mappings(tmp_path / "maps.sqlite", {})
    with pytest.raises(nis.rc.Refusal, match="data gate"):
        run(st, market_db, maps, as_of=date(2026, 11, 2))
    out = tmp_path / "rep" / "r.json"
    rc = nis.main(["--sample", "A", "--db", str(market_db), "--state-db", str(st.path), "--mappings", str(maps),
                   "--as-of", "2026-11-02", "--out", str(out)], etfs=frozenset(), themes={})
    assert rc == 2 and not out.exists()
    # a missing store refuses too
    assert nis.main(["--sample", "A", "--db", str(tmp_path / "absent.duckdb"), "--state-db", str(st.path),
                     "--mappings", str(maps), "--out", str(out)], etfs=frozenset(), themes={}) == 2
    # sample B needs its own fresh copy named explicitly
    assert nis.main(["--sample", "B", "--state-db", str(st.path)], etfs=frozenset(), themes={}) == 2


def test_data_gate_covers_the_pullback_longest_exit(sessions):
    """The last possible exit is a pullback filled on its fifth resting session and held 10: 13 sessions after the
    sample's last entry."""
    last = nic.SAMPLES["A"][1]
    i = sessions.index(last)
    assert sessions[i + 13] == date(2026, 11, 12)
    with pytest.raises(nis.rc.Refusal, match="data gate"):
        nis.data_gate(types.SimpleNamespace(sessions=sessions[:i + 13]), sessions, "A")     # ends 12 after
    nis.data_gate(types.SimpleNamespace(sessions=sessions[:i + 14]), sessions, "A")         # ends 13 after
    # a calendar that stops short of that exit refuses too
    with pytest.raises(nis.rc.Refusal, match="calendar"):
        nis.data_gate(types.SimpleNamespace(sessions=sessions), sessions[:i + 13], "A")
    assert nis.LAST_EXIT_OFFSET == nic.PULLBACK_SESSIONS - 1 + max(nic.HOLD.values()) - 1 == 13


def test_pullback_population_uses_the_kept_trades_p0_sessions(tmp_path, market_db, sessions, cal, monkeypatch):
    """A signal scored intraday on D-1 rests its limit at close(D-2); the population's limits sit there too."""
    conn = nis.rc.open_snapshot(market_db)
    try:
        p = nic.load_panel(conn, nic.SAMPLES["A"][0] - nis.PANEL_LOOKBACK, AS_OF, cal, etfs=frozenset(), themes={})
        lo, hi = nic.SAMPLES["A"]
        # the first sample session where X's limit at close(D-2) fills: the synthetic bars are a random walk
        d = next(s for s in sessions if lo <= s <= hi
                 and isinstance(nic.pullback_trade(p, "X", s, nth(sessions, s, -2), 10), nic.Trade))
        p0 = nth(sessions, d, -2)
        expect = [r for s in sorted(nic.liquid(p, nth(sessions, d, -1)))
                  if isinstance(r := nic.pullback_trade(p, s, d, p0, 10), nic.Trade)]
    finally:
        conn.close()
    st = State(tmp_path / "state.db")
    st.stock("sx1", "X", at(nth(sessions, d, -1), 11))
    seen = []
    real = nis.make_trade

    def spy(panel, construct, sig, hold):
        if construct == "pullback" and not sig.stories:            # the population's signals carry no story
            seen.append((sig.key, sig.d, sig.p0_session))
        return real(panel, construct, sig, hold)

    monkeypatch.setattr(nis, "make_trade", spy)
    doc = run(st, market_db, write_mappings(tmp_path / "maps.sqlite", {}))
    h = doc["hypotheses"]["stock_pullback"]
    assert [(t["key"], t["signal_date"], t["p0_session"]) for t in h["trades"]] == [("X", d, p0)]
    assert seen and {(sd, sp) for _, sd, sp in seen} == {(d, p0)}
    assert h["population"]["n"] == len(expect)
    # the stock arm's population is untouched: one run per entry session
    assert doc["hypotheses"]["stock_next_open"]["population"]["sessions"] == [d]


def test_unmappable_story_never_holds_a_look(tmp_path, market_db, d1):
    st = State(tmp_path / "state.db")
    st.stock("sx1", "X", at(d1, 8))
    st.call(at(d1, 8), [_score("g0", "market", 0.5, 0.8, "global_market")], {"g0": "   "})   # no readable headline
    maps = write_mappings(tmp_path / "maps.sqlite", {})
    out = tmp_path / "rep" / "r.json"
    rc = nis.main(["--sample", "A", "--db", str(market_db), "--state-db", str(st.path), "--mappings", str(maps),
                   "--as-of", AS_OF.isoformat(), "--out", str(out)], etfs=frozenset(), themes={})
    assert rc == 0
    doc = json.loads(out.read_text(encoding="utf-8"))
    assert doc["status"] == "complete"
    assert doc["meta"]["unmappable"] == 1
    assert "unmappable: 1 " in out.with_suffix(".md").read_text(encoding="utf-8")
    # a mappable one still waits, and the unmappable one is not counted among the waiting
    st.glob("g1", at(d1, 8))
    doc = run(st, market_db, maps)
    assert doc["status"] == "waiting: 1 stories unmapped (domain 0, global 1)" and doc["meta"]["unmappable"] == 1


_OUTCOME_KEY = re.compile(r"\b(excess|net|gross|rho|label)\b", re.IGNORECASE)
_PERCENT = re.compile(r"\d\s*%")


def test_counts_only_prints_no_outcomes(tmp_path, market_db, sessions, d1, capsys):
    st = State(tmp_path / "state.db")
    st.stock("sx1", "X", at(d1, 8))
    st.stock("sx2", "B2", at(nth(sessions, d1, 3), 8), sentiment=-0.2, materiality=0.3)
    st.stock("sm1", "M1", at(d1, 8))
    st.domain("dom1", at(d1, 8))                                # unmapped: counts-only skips the waiting check
    out_dir = tmp_path / "rep"
    rc = nis.main(["--sample", "A", "--db", str(market_db), "--state-db", str(st.path), "--mappings",
                   str(tmp_path / "absent.sqlite"), "--as-of", AS_OF.isoformat(), "--counts-only", "--out",
                   str(out_dir / "r.json")], etfs=frozenset(), themes={})
    text = capsys.readouterr().out
    assert rc == 0, text
    assert not out_dir.exists()
    assert "stock_next_open" in text and "stock_pullback" in text and "population" in text
    assert "domain_companies" not in text and "waiting" not in text
    assert not _PERCENT.search(text), text
    assert not _OUTCOME_KEY.search(text), text
    c = nis.counts_only("A", st.path, market_db, AS_OF, etfs=frozenset(), themes={})
    assert c["windows"]["sample"]["stock_next_open"]["trades"] == 1
    assert c["windows"]["sample"]["info"]["signals"] == 3
    assert not _OUTCOME_KEY.search(json.dumps(c, default=str))


def test_report_roundtrip(tmp_path, market_db, sessions, d1):
    st = State(tmp_path / "state.db")
    st.stock("sx1", "X", at(d1, 8))
    st.stock("sb1", "B1", at(d1, 8), sentiment=-0.7, materiality=0.8)
    st.domain("dom1", at(d1, 8))
    st.glob("glb1", at(nth(sessions, d1, 1), 8))
    maps = write_mappings(tmp_path / "maps.sqlite", {
        "dom1": (1, [["AUTO", 1], ["METAL", -1]], [["P1corp", 1], ["B2corp", -1], ["Unknown Ltd", 1]]),
        "glb1": (-1, [["BANK", 1]], [])})
    out = tmp_path / "rep" / "news_impact_A.json"
    rc = nis.main(["--sample", "A", "--db", str(market_db), "--state-db", str(st.path), "--mappings", str(maps),
                   "--as-of", AS_OF.isoformat(), "--out", str(out)], etfs=frozenset(), themes={})
    assert rc == 0
    doc = json.loads(out.read_text(encoding="utf-8"))
    md = out.with_suffix(".md").read_text(encoding="utf-8")
    assert md == f"# NEWS IMPACT STUDY\n\n```\n{nis.render(doc)}\n```\n"
    assert doc["status"] == "complete"
    assert list(doc["hypotheses"]) == list(nis.HYPOTHESES)
    m = doc["meta"]
    assert m["sample"] == "A" and m["n_trials"] == 6 and m["alpha"] == pytest.approx(0.05 / 6)
    assert m["windows"]["sample"] == ["2026-07-29", "2026-10-23"]
    assert doc["hypotheses"]["domain_companies"]["drops"]["unresolved_names"] == 1
    assert [t["key"] for t in doc["hypotheses"]["global_sectors"]["trades"]] == ["BANK"]
    dg = doc["diagnostics"]
    assert set(dg) >= {"horizons", "splits", "negative_side", "market_direction", "pullback_fill_rate"}
    assert set(dg["horizons"]["stock_next_open"]) == {str(h) for h in nic.HORIZONS}
    # two source domains ingested before the first score (c.com came after it)
    assert dg["splits"]["stock_next_open"]["boolean"]["two_domains"]["n"] == 1
    neg = dg["negative_side"]
    assert neg["stock_material_negative"]["n"] == 1
    assert neg["domain_companies_minus"]["n"] == 1 and neg["domain_groups_minus"]["n"] == 1
    assert dg["market_direction"]["n"] == 1
    assert "not evidence" in md and "NEWS IMPACT STUDY" in md
