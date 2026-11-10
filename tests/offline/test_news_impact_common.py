"""``scripts/news_impact_common.py``: the news impact study's constants, timing rules, benchmarks, trade constructors
and verdict.

Every store is synthetic: an in-memory research DuckDB and a temp ``agent_calls`` SQLite. The calendar is the pinned
config copy's (2026: 11-10 and 11-24 are holidays, 11-08 a Sunday muhurat session).
"""

from __future__ import annotations

import gzip
import importlib.util
import json
import sqlite3
import sys
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

from engine.intelligence.agents import news_analyst  # noqa: E402
from engine.intelligence.context import ContextAssembler  # noqa: E402
from engine.learning.multiplicity import registration_alpha  # noqa: E402
from engine.learning.validate import fold_pass_min  # noqa: E402

IST = ZoneInfo("Asia/Kolkata")
UTC = ZoneInfo("UTC")

AGENT_CALLS_DDL = """
CREATE TABLE agent_calls (
    call_id TEXT PRIMARY KEY, agent_id TEXT NOT NULL, trigger TEXT, model TEXT, inputs_digest TEXT,
    context_gz BLOB, output_json TEXT, ok INTEGER NOT NULL, fail_reason TEXT, in_tokens INTEGER,
    out_tokens INTEGER, cache_read INTEGER, cache_write INTEGER, cost_usd TEXT, duration_ms INTEGER,
    at TEXT NOT NULL, prompt_version TEXT)
"""


def at(day: date, hh: int, mi: int = 0, ss: int = 0, us: int = 0) -> datetime:
    return datetime(day.year, day.month, day.day, hh, mi, ss, us, tzinfo=IST)


def D(month: int, day: int, year: int = 2026) -> date:
    return date(year, month, day)


@pytest.fixture(scope="module")
def cal():
    return nic.load_calendar()


def regular(cal, lo: date, hi: date) -> list[date]:
    return nic.regular_sessions(cal, lo, hi)


# --------------------------------------------------------------------------- registered constants
def test_registered_constants_are_the_spec_values():
    assert (nic.FAMILY, nic.N_TRIALS, nic.MIN_TRADES, nic.MIN_INFO_OBS) == ("edge/news_impact", 6, 30, 300)
    assert nic.ALPHA == registration_alpha(6) == pytest.approx(0.05 / 6)
    assert nic.FOLD_PASS_MIN == fold_pass_min(6) == 0.60
    assert (nic.MATERIAL, nic.POSITIVE, nic.NEGATIVE) == (0.6, 0.4, -0.4)
    assert nic.HOLD == {"stock_next_open": 10, "stock_pullback": 10, "domain_companies": 10, "domain_groups": 10,
                        "global_sectors": 5, "info": 10}
    assert (nic.PULLBACK_SESSIONS, nic.MIN_PEERS, nic.MIN_BASKET) == (5, 5, 3)
    assert (nic.BETA_SESSIONS, nic.BETA_MIN_PAIRS, nic.BOOT_DRAWS, nic.SPLIT_DRAWS, nic.SEED) == (
        250, 200, 2000, 10000, 20261110)
    assert (nic.ENTRY_CUTOFF.isoformat(), nic.CLOSE_TIME.isoformat()) == ("09:15:00", "15:30:00")
    assert nic.SAMPLES == {"A": (D(7, 29), D(10, 23)), "excluded": (D(10, 26), D(11, 10)),
                           "B": (D(11, 11), D(2, 26, 2027))}
    assert nic.NEVER_TYPES == {"pump_promo_suspect"}
    assert nic.STOCK_SKIP_TYPES == {"pump_promo_suspect", "dividend_corp_action"}
    assert nic.DOMAIN_MARKET_TYPES == {"govt_program", "sector_policy", "regulatory_policy"}
    assert nic.GLOBAL_TYPES == {"global_market", "macro_data", "disruption"}
    assert nic.HORIZONS == (1, 3, 5, 10, 20)


# --------------------------------------------------------------------------- timing
def test_entry_session_cutoff(cal):
    s = regular(cal, D(11, 1), D(11, 30))
    assert nic.entry_session(at(D(11, 9), 9, 14, 59, 999999), s) == D(11, 9)
    # 09:15:00 is too late for the day's open; 11-10 is a holiday, so the next regular session is 11-11
    assert nic.entry_session(at(D(11, 9), 9, 15), s) == D(11, 11)
    # a Saturday score enters Monday; the Sunday muhurat session is not a regular session
    assert nic.entry_session(at(D(11, 7), 10), s) == D(11, 9)
    assert nic.entry_session(at(D(11, 8), 18, 30), s) == D(11, 9)
    # a Monday evening before the 11-24 holiday enters Wednesday
    assert nic.entry_session(at(D(11, 23), 16), s) == D(11, 25)
    # the cut-off is IST whatever zone the stamp carries: 03:44:59 UTC is 09:14:59 IST
    assert nic.entry_session(datetime(2026, 11, 9, 3, 44, 59, tzinfo=UTC), s) == D(11, 9)
    assert nic.entry_session(datetime(2026, 11, 9, 3, 45, tzinfo=UTC), s) == D(11, 11)
    # past the last session in the list: unknown, not a guess
    assert nic.entry_session(at(D(11, 30), 10), s) is None


def test_pre_news_session(cal):
    s = regular(cal, D(11, 1), D(11, 30))
    # 11:00 on a session day: that day had not closed, so P0 is the previous session's close (11-10 a holiday)
    assert nic.pre_news_session(at(D(11, 11), 11), s) == D(11, 9)
    assert nic.pre_news_session(at(D(11, 11), 16), s) == D(11, 11)
    assert nic.pre_news_session(at(D(11, 11), 8), s) == D(11, 9)
    # the close is at 15:30, so a score stamped exactly then has not seen it
    assert nic.pre_news_session(at(D(11, 11), 15, 30), s) == D(11, 9)
    assert nic.pre_news_session(at(D(11, 11), 15, 30, 0, 1), s) == D(11, 11)
    # a weekend score (the muhurat Sunday included) prices off Friday's close
    assert nic.pre_news_session(at(D(11, 7), 10), s) == D(11, 6)
    assert nic.pre_news_session(at(D(11, 8), 19), s) == D(11, 6)
    assert nic.pre_news_session(at(D(11, 2), 9), s) is None


def test_entry_and_p0_for_an_intraday_score_leave_the_scoring_day_between(cal):
    s = regular(cal, D(11, 1), D(11, 30))
    t = at(D(11, 12), 11)
    assert (nic.pre_news_session(t, s), nic.entry_session(t, s)) == (D(11, 11), D(11, 13))


# --------------------------------------------------------------------------- story kinds
def story(scope: str = "stock", event_type: str = "order_win", sentiment: float = 0.5, materiality: float = 0.7,
          **kw) -> object:
    base = dict(cluster_id="c1", at=at(D(8, 3), 8), call_id="k", model="m", scope=scope, event_type=event_type,
                sentiment=sentiment, materiality=materiality, entities=(), headline="h")
    base.update(kw)
    return nic.Story(**base)


@pytest.mark.parametrize("scope, event_type, materiality, kind", [
    ("stock", "earnings_result", 0.1, "stock"),             # any materiality: the info hypothesis uses them all
    ("stock", "dividend_corp_action", 0.9, None),
    ("stock", "pump_promo_suspect", 0.9, None),
    ("sector", "other", 0.6, "domain"),
    ("sector", "sector_policy", 0.59, None),
    ("theme", "capacity_expansion", 0.7, "domain"),
    ("sector", "pump_promo_suspect", 0.9, None),
    ("market", "govt_program", 0.6, "domain"),
    ("market", "sector_policy", 0.8, "domain"),
    ("market", "regulatory_policy", 0.8, "domain"),
    ("market", "govt_program", 0.5, None),
    ("market", "global_market", 0.8, "global"),
    ("market", "macro_data", 0.6, "global"),
    ("market", "disruption", 1.0, "global"),
    ("market", "disruption", 0.59, None),
    ("market", "other", 0.9, None),
    ("market", "earnings_result", 0.9, None),
])
def test_story_kind(scope, event_type, materiality, kind):
    assert nic.story_kind(story(scope, event_type, materiality=materiality)) == kind


# --------------------------------------------------------------------------- first scores
def score(cid: str, scope: str = "stock", sentiment: float = 0.5, materiality: float = 0.7,
          event_type: str = "order_win", entities: list[str] | None = None) -> dict:
    return {"cluster_id": cid, "scope": scope, "entities": entities or [], "sectors": [], "themes": [],
            "sentiment": sentiment, "materiality": materiality, "event_type": event_type, "novelty": 0.5}


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

    def call(self, when: datetime, scores: list[dict], *, heads: dict[str, str] | None = None, ok: int = 1,
             model: str = "claude-sonnet-5-5", agent_id: str = "news_analyst", output: str | None = None,
             context: bytes | None = None, stamp: str | None = None) -> str:
        call_id = uuid.uuid4().hex
        heads = heads or {}
        rows = [{"cluster_id": s["cluster_id"], "representative": heads.get(s["cluster_id"], f"story {s['cluster_id']}"),
                 "source_domains": ["a.com"], "first_seen": when} for s in scores]
        blob = context if context is not None else gzip.compress(
            f"{news_analyst.SYSTEM_PROMPT}\n\n{engine_prompt(rows)}".encode())
        con = sqlite3.connect(self.path)
        con.execute("INSERT INTO agent_calls (call_id, agent_id, model, context_gz, output_json, ok, at) "
                    "VALUES (?,?,?,?,?,?,?)",
                    (call_id, agent_id, model, blob, output if output is not None else json.dumps({"scores": scores}),
                     ok, stamp or when.isoformat()))
        con.commit()
        con.close()
        return call_id


def test_first_score_wins(tmp_path):
    st = State(tmp_path / "state.db")
    t1, t2 = at(D(8, 3), 8), at(D(8, 4), 8)
    # the later call is inserted first: the first score is the earliest ``at``, never the lowest rowid
    late = st.call(t2, [score("c1", sentiment=-0.5, materiality=0.2, entities=["Late Co"]), score("c2")],
                   heads={"c1": "second headline", "c2": "c2 headline"}, model="claude-haiku-4-5")
    first = st.call(t1, [score("c1", sentiment=0.8, materiality=0.9, entities=["First Co"])],
                    heads={"c1": "first headline"})
    st.call(at(D(8, 2), 8), [score("c1", sentiment=0.1)], ok=0)                 # a failed call scores nothing
    st.call(at(D(8, 2), 9), [score("c1", sentiment=0.1)], agent_id="other")      # another agent's call
    stories, counts = nic.load_first_scores(st.path, at(D(9, 1), 0))
    by = {s.cluster_id: s for s in stories}
    assert set(by) == {"c1", "c2"}
    c1 = by["c1"]
    assert (c1.at, c1.call_id, c1.model, c1.headline) == (t1, first, "claude-sonnet-5-5", "first headline")
    assert (c1.sentiment, c1.materiality, c1.entities, c1.scope, c1.event_type) == (
        0.8, 0.9, ("First Co",), "stock", "order_win")
    assert (by["c2"].call_id, by["c2"].headline, by["c2"].model) == (late, "c2 headline", "claude-haiku-4-5")
    assert [s.cluster_id for s in stories] == ["c1", "c2"]                       # oldest first
    assert counts["calls_unparsed"] == 0 and counts["headline_missing"] == 0


def test_first_score_is_found_before_the_limit_applies(tmp_path):
    """A cluster first scored at or after the limit is not in the sample, and a re-score before the limit never
    stands in for a first score made later: the limit filters calls, the earliest call decides."""
    st = State(tmp_path / "state.db")
    limit = at(D(8, 10), 0)
    st.call(at(D(8, 9), 23, 59), [score("c1")])
    st.call(limit, [score("c2")])
    stories, _ = nic.load_first_scores(st.path, limit)
    assert [s.cluster_id for s in stories] == ["c1"]


def test_first_scores_count_unparsed_calls_and_missing_headlines(tmp_path):
    st = State(tmp_path / "state.db")
    st.call(at(D(8, 3), 8), [], output="not json")
    st.call(at(D(8, 3), 9), [], output=json.dumps({"scores": "nope"}))
    st.call(at(D(8, 3), 10), [score("c1")], context=b"not gzip")
    st.call(at(D(8, 3), 11), [score("c2")], stamp="yesterday")
    stories, counts = nic.load_first_scores(st.path, at(D(9, 1), 0))
    assert [(s.cluster_id, s.headline) for s in stories] == [("c1", None)]
    assert counts["calls_unparsed"] == 2
    assert counts["headline_missing"] == 1
    assert counts["calls_at_unparsed"] == 1


def test_first_scores_missing_store_is_unavailable(tmp_path):
    with pytest.raises(nic.Unavailable):
        nic.load_first_scores(tmp_path / "absent.db", at(D(9, 1), 0))
    assert not (tmp_path / "absent.db").exists()


# --------------------------------------------------------------------------- synthetic market
class Market:
    """``bars_1d`` (with ``NIFTY 50`` on every session), ``corp_actions``, ``sector_map``, ``entity_aliases`` and
    ``theme_map`` in an in-memory DuckDB."""

    def __init__(self, sessions: list[date], price_type: str = "DECIMAL(12,2)") -> None:
        self.sessions = sessions
        self.price_type = price_type
        self.bars: dict[str, dict[date, tuple[float, float, float, float, int]]] = {}
        self.corp: list[tuple[str, date, str]] = []
        self.sectors: list[tuple[date, str, str]] = []

    def flat(self, sym: str, price: float = 100.0, volume: int = 2_000_000) -> None:
        self.bars[sym] = {d: (price, price, price, price, volume) for d in self.sessions}

    def bar(self, sym: str, d: date, o: float, c: float, *, h: float | None = None, lo: float | None = None,
            v: int | None = None) -> None:
        vol = v if v is not None else self.bars[sym][d][4]
        self.bars[sym][d] = (o, h if h is not None else max(o, c), lo if lo is not None else min(o, c), c, vol)

    def sector(self, as_of: date, sector: str, *syms: str) -> None:
        self.sectors += [(as_of, s, sector) for s in syms]

    def build(self) -> duckdb.DuckDBPyConnection:
        conn = duckdb.connect()
        p = self.price_type
        conn.execute(f'CREATE TABLE bars_1d (symbol TEXT NOT NULL, d DATE NOT NULL, "open" {p} NOT NULL, '
                     f'high {p} NOT NULL, low {p} NOT NULL, "close" {p} NOT NULL, volume BIGINT NOT NULL, '
                     "src TEXT NOT NULL DEFAULT 'kite_official', PRIMARY KEY (symbol, d))")
        conn.execute("CREATE TABLE corp_actions (symbol TEXT NOT NULL, ex_date DATE NOT NULL, kind TEXT NOT NULL, "
                     "ratio TEXT, amount DECIMAL(12,2), source TEXT, recorded_at TIMESTAMPTZ, "
                     "PRIMARY KEY (symbol, ex_date, kind))")
        conn.execute("CREATE TABLE sector_map (as_of DATE NOT NULL, symbol TEXT NOT NULL, sector TEXT NOT NULL, "
                     "PRIMARY KEY (as_of, symbol))")
        rows = [("NIFTY 50", d, 100.0, 100.0, 100.0, 100.0, 0) for d in self.sessions]
        for sym, bars in self.bars.items():
            rows += [(sym, d, *b) for d, b in bars.items()]
        df = pd.DataFrame(rows, columns=["symbol", "d", "o", "h", "l", "c", "v"])
        conn.register("bars_df", df)
        conn.execute('INSERT INTO bars_1d (symbol, d, "open", high, low, "close", volume) '
                     "SELECT symbol, d, o, h, l, c, v FROM bars_df")
        conn.unregister("bars_df")
        if self.corp:
            conn.executemany("INSERT INTO corp_actions (symbol, ex_date, kind) VALUES (?, ?, ?)", self.corp)
        if self.sectors:
            conn.executemany("INSERT INTO sector_map VALUES (?, ?, ?)", self.sectors)
        return conn

    def panel(self, cal, themes: dict[str, frozenset[str]] | None = None):
        return nic.load_panel(self.build(), self.sessions[0], self.sessions[-1], cal, etfs=frozenset(),
                              themes=themes or {})


PEERS = ("P1", "P2", "P3", "P4", "P5")
#: Every test D-1 before it reads this snapshot.
SNAP = D(11, 8)


def auto_market(cal) -> tuple[Market, date]:
    """Flat names on the June-August 2026 sessions: X and five peers in AUTO, a four-name METAL sector, an
    UNCLASSIFIED bucket and a thin name. Returns the market and the entry session D used by the tests."""
    s = regular(cal, D(6, 1), D(8, 31))
    m = Market(s)
    for sym in ("X", *PEERS, "Y", "M1", "M2", "M3", "M4", "Q1", "Q2", "Q3", "Q4", "Q5", "Q6", "W", "NOSECTOR"):
        m.flat(sym)
    m.flat("THIN", volume=10)
    m.sector(SNAP, "AUTO", "X", *PEERS, "THIN")
    m.sector(SNAP, "METAL", "Y", "M1", "M2", "M3", "M4")
    m.sector(SNAP, "UNCLASSIFIED", "Q1", "Q2", "Q3", "Q4", "Q5", "Q6")
    return m, D(8, 3)


def nth(m: Market, d: date, k: int) -> date:
    return m.sessions[m.sessions.index(d) + k]


# --------------------------------------------------------------------------- the stock benchmarks
def test_stock_trade_sector_excess(cal):
    m, d = auto_market(cal)
    m.bar("X", d, 100.0, 104.0)
    m.bar("X", nth(m, d, 1), 104.0, 106.0)
    m.bar("X", nth(m, d, 2), 106.0, 110.0)
    for k, sym in enumerate(PEERS, start=1):
        m.bar(sym, nth(m, d, 2), 100.0, 100.0 + k)
    # an older snapshot in force on D-1 by date moves X to METAL: before the floor it is never read
    m.sector(nth(m, d, -2), "METAL", "X")
    p = m.panel(cal)
    t = nic.stock_trade(p, "X", d, 3, "sector")
    assert isinstance(t, nic.Trade), t
    assert (t.key, t.signal_date, t.entry_date, t.exit_date) == ("X", d, d, nth(m, d, 2))
    assert t.gross == pytest.approx(10.0)
    assert t.bench == pytest.approx(3.0)                       # the peers' equal-weight 1..5%
    assert t.excess == pytest.approx(7.0)
    assert t.net == pytest.approx(7.0 - nic.cost())
    assert t.legs == 1 and t.fill_price is None
    assert nic.peers(p, "X", nth(m, d, -1)) == frozenset(PEERS)   # THIN is in AUTO but not liquid


def test_few_peers(cal):
    m, d = auto_market(cal)
    p = m.panel(cal)
    assert nic.stock_trade(p, "Y", d, 3, "sector") == "few_peers"           # 4 liquid METAL peers
    assert nic.stock_trade(p, "Q1", d, 3, "sector") == "few_peers"          # UNCLASSIFIED has no peers
    assert nic.stock_trade(p, "NOSECTOR", d, 3, "sector") == "few_peers"    # no sector in the snapshot
    assert nic.peers(p, "Q1", nth(m, d, -1)) == frozenset()
    assert nic.stock_trade(p, "THIN", d, 3, "sector") == "not_liquid"
    assert nic.stock_trade(p, "X", m.sessions[0], 3, "sector") == "no_prior_session"
    assert nic.stock_trade(p, "X", m.sessions[-2], 3, "sector") == "open"   # the exit is past the data
    assert nic.stock_trade(p, "X", d, 3, "sector") != "few_peers"


def floor_market(cal, snapshots: tuple[date, ...]) -> Market:
    """X sits in a different sector in each snapshot: METAL on 09-06, AUTO on 11-08, IT on 11-15, five liquid
    peers in each."""
    m = Market(regular(cal, D(8, 3), D(11, 20)))
    names = {"METAL": ("M1", "M2", "M3", "M4", "M5"), "AUTO": PEERS, "IT": ("I1", "I2", "I3", "I4", "I5")}
    for sym in ("X", *(s for group in names.values() for s in group)):
        m.flat(sym)
    for as_of, sector in ((D(9, 6), "METAL"), (D(11, 8), "AUTO"), (D(11, 15), "IT")):
        if as_of in snapshots:
            m.sector(as_of, sector, "X", *names[sector])
    return m


def test_sector_snapshot_floor(cal):
    assert nic.SECTOR_SNAPSHOT_FLOOR == D(11, 8) == SNAP
    p = floor_market(cal, (D(9, 6), D(11, 8), D(11, 15))).panel(cal)
    assert nic.sector_snapshot(p, D(9, 10)) == D(11, 8)        # before the floor: the floor's snapshot
    assert nic.sector_snapshot(p, D(11, 6)) == D(11, 8)
    assert nic.sector_snapshot(p, D(11, 8)) == D(11, 8)
    assert nic.sector_snapshot(p, D(11, 9)) == D(11, 8)
    assert nic.sector_snapshot(p, D(11, 13)) == D(11, 8)
    assert nic.sector_snapshot(p, D(11, 16)) == D(11, 15)      # after it: the snapshot in force on D-1
    # peers and sector-group members both read it
    assert nic.peers(p, "X", D(9, 10)) == frozenset(PEERS)
    assert nic.peers(p, "X", D(11, 9)) == frozenset(PEERS)
    assert nic.peers(p, "X", D(11, 16)) == frozenset({"I1", "I2", "I3", "I4", "I5"})
    assert nic.group_members(p, "AUTO", D(9, 10)) == frozenset({"X", *PEERS})
    assert nic.group_members(p, "METAL", D(9, 10)) == frozenset()
    assert nic.group_members(p, "IT", D(11, 16)) == frozenset({"X", "I1", "I2", "I3", "I4", "I5"})
    assert nic.group_members(p, "AUTO", D(11, 16)) == frozenset()       # the 11-15 snapshot has no AUTO names


def test_a_copy_without_the_floor_snapshot_is_unavailable(cal):
    with pytest.raises(nic.Unavailable, match="2026-11-08"):
        floor_market(cal, (D(9, 6), D(11, 15))).panel(cal)


def test_voided_leg(cal):
    m, d = auto_market(cal)
    exit_d = nth(m, d, 2)
    m.corp += [("X", nth(m, d, 1), "split"), ("P1", d, "bonus"), ("P2", nth(m, d, 3), "split"),
               ("P3", nth(m, d, 1), "dividend")]
    for k, sym in enumerate(PEERS, start=1):
        m.bar(sym, exit_d, 100.0, 100.0 + k)
    p = m.panel(cal)
    assert nic.stock_trade(p, "X", d, 3, "sector") == "voided"
    # a split the day after the exit, and a dividend inside the hold, void nothing
    m2, _ = auto_market(cal)
    m2.corp += [("X", nth(m2, d, 3), "split"), ("X", nth(m2, d, 1), "dividend"), ("P1", d, "bonus")]
    for k, sym in enumerate(PEERS, start=1):
        m2.bar(sym, exit_d, 100.0, 100.0 + k)
    t = nic.stock_trade(m2.panel(cal), "X", d, 3, "sector")
    assert isinstance(t, nic.Trade), t
    # P1's bonus on the entry session is inside the hold: its leg leaves the benchmark
    assert t.bench == pytest.approx((2 + 3 + 4 + 5) / 4)


def test_missing_exit_bar_is_no_bar(cal):
    m, d = auto_market(cal)
    del m.bars["X"][nth(m, d, 2)]
    assert nic.stock_trade(m.panel(cal), "X", d, 3, "sector") == "no_bar"


# --------------------------------------------------------------------------- the pullback arm
def test_pullback_fill(cal):
    m, d = auto_market(cal)
    p0 = nth(m, d, -1)                                          # close 100
    m.bar("W", d, 102.0, 102.0, h=103.0, lo=101.0)
    m.bar("W", nth(m, d, 1), 102.0, 101.0, lo=100.5)            # the low stays above P0: no fill
    m.bar("W", nth(m, d, 2), 99.0, 100.0, h=101.0, lo=98.0)     # opens below P0: fills at the open
    m.bar("W", nth(m, d, 4), 100.0, 108.0)
    m.bar("P1", nth(m, d, 2), 95.0, 100.0)                      # the peers are measured from the fill open
    m.sector(SNAP, "AUTO", "W")
    p = m.panel(cal)
    t = nic.pullback_trade(p, "W", d, p0, 3)
    assert isinstance(t, nic.Trade), t
    assert (t.signal_date, t.entry_date, t.exit_date) == (d, nth(m, d, 2), nth(m, d, 4))
    assert t.fill_price == pytest.approx(99.0)
    assert t.gross == pytest.approx((108.0 / 99.0 - 1.0) * 100.0)
    peers = ["X", *PEERS]
    assert nic.peers(p, "W", p0) == frozenset(peers)
    assert t.bench == pytest.approx((100.0 / 95.0 - 1.0) * 100.0 / len(peers))
    assert t.net == pytest.approx(t.gross - t.bench - nic.cost())


def test_pullback_touching_p0_fills_at_p0(cal):
    m, d = auto_market(cal)
    m.bar("W", d, 103.0, 102.0, lo=101.0)
    m.bar("W", nth(m, d, 1), 102.0, 101.0, lo=100.0)             # the low equals P0, the open is above
    m.sector(SNAP, "AUTO", "W")
    t = nic.pullback_trade(m.panel(cal), "W", d, nth(m, d, -1), 3)
    assert isinstance(t, nic.Trade), t
    assert (t.entry_date, t.fill_price, t.exit_date) == (nth(m, d, 1), pytest.approx(100.0), nth(m, d, 3))


def test_pullback_unfilled(cal):
    m, d = auto_market(cal)
    for k in range(nic.PULLBACK_SESSIONS):
        m.bar("W", nth(m, d, k), 101.0, 101.0, lo=100.01)
    m.bar("W", nth(m, d, nic.PULLBACK_SESSIONS), 90.0, 90.0)     # the sixth session is past the resting window
    m.sector(SNAP, "AUTO", "W")
    # resting past the end of the data is still open, not unfilled
    late = m.sessions[-3]
    for k in range(3):
        m.bar("W", nth(m, late, k), 101.0, 101.0, lo=100.5)
    p = m.panel(cal)
    assert nic.pullback_trade(p, "W", d, nth(m, d, -1), 3) == "unfilled"
    assert nic.pullback_trade(p, "W", late, nth(m, late, -1), 3) == "open"


def test_pullback_p0_from_before_the_scoring_day(cal):
    """A score at 11:00 on S enters on S+1 with P0 = close(S-1)."""
    m, d = auto_market(cal)
    s_day, p0 = nth(m, d, -1), nth(m, d, -2)
    m.bar("W", s_day, 100.0, 101.0)
    m.bar("W", d, 101.0, 101.0, lo=99.5)
    m.sector(SNAP, "AUTO", "W")
    t = nic.pullback_trade(m.panel(cal), "W", d, p0, 3)
    assert isinstance(t, nic.Trade) and t.entry_date == d and t.fill_price == pytest.approx(100.0)
    # an ex-date on S is also D-1, which the liquid universe already drops
    m.corp.append(("W", s_day, "split"))
    assert nic.pullback_trade(m.panel(cal), "W", d, p0, 3) == "not_liquid"


def test_pullback_split_while_resting_voids_instead_of_filling(cal):
    """An unadjusted split puts the bars after it in other units than P0: the halved low must not read as a fill."""
    m, d = auto_market(cal)
    m.bar("W", d, 101.0, 101.0, lo=100.5)
    for k in range(1, 8):
        m.bar("W", nth(m, d, k), 50.5, 50.5)
    m.sector(SNAP, "AUTO", "W")
    m.corp.append(("W", nth(m, d, 1), "split"))
    assert nic.pullback_trade(m.panel(cal), "W", d, nth(m, d, -1), 3) == "voided"
    # on the entry session itself, too: the open is already in the new units
    m2, _ = auto_market(cal)
    for k in range(0, 8):
        m2.bar("W", nth(m2, d, k), 50.5, 50.5)
    m2.sector(SNAP, "AUTO", "W")
    m2.corp.append(("W", d, "split"))
    assert nic.pullback_trade(m2.panel(cal), "W", d, nth(m2, d, -1), 3) == "voided"


# --------------------------------------------------------------------------- baskets and beta
def beta_market(cal) -> tuple[Market, date]:
    """Daily close-to-close returns: A, B and E move 2u, C, F and G stay flat, so the equal-weight universe moves
    u and any equal-weight basket of A, B and E has a beta of exactly 2. Opens equal the previous close."""
    s = regular(cal, D(6, 2, 2025), D(8, 31))
    m = Market(s, price_type="DOUBLE")
    rng = np.random.default_rng(7)
    u = rng.normal(0.0, 0.01, len(s))
    for sym in ("A", "B", "E"):
        px, rows = 1000.0, {}
        for i, d in enumerate(s):
            o = px
            px = px * (1.0 + 2.0 * u[i]) if i else px
            rows[d] = (o, max(o, px), min(o, px), px, 10_000_000)
        m.bars[sym] = rows
    for sym in ("C", "F", "G"):
        m.flat(sym, price=1000.0, volume=10_000_000)
    m.flat("THIN", volume=10)
    m.sector(SNAP, "AUTO", "A", "B", "E", "THIN")
    m.sector(SNAP, "BANK", "C", "F", "G")
    return m, D(8, 3)


def test_basket_beta_excess(cal):
    m, d = beta_market(cal)
    p = m.panel(cal, themes={"defence": frozenset({"A", "B", "E", "THIN", "ABSENT"}),
                             "railways": frozenset({"A", "C", "THIN"})})
    d_prev = nth(m, d, -1)
    assert nic.group_members(p, "AUTO", d_prev) == frozenset({"A", "B", "E"})
    assert nic.group_members(p, "defence", d_prev) == frozenset({"A", "B", "E"})
    assert nic.beta(p, frozenset({"A", "B", "E"}), d_prev) == pytest.approx(2.0, abs=1e-9)
    assert nic.beta(p, frozenset({"C"}), d_prev) == pytest.approx(0.0, abs=1e-12)
    for hold in (1, 5):
        t = nic.basket_trade(p, "AUTO", d, hold)
        assert isinstance(t, nic.Trade), t
        assert t.legs == 3 and t.key == "AUTO"
        assert t.excess == pytest.approx(0.0, abs=1e-9)
        assert t.bench == pytest.approx(t.gross)
        assert t.net == pytest.approx(-nic.cost())               # one round trip for the whole basket
    assert nic.basket_trade(p, "railways", d, 5) == "small_basket"    # two liquid members
    assert nic.basket_trade(p, "METAL", d, 5) == "small_basket"       # no member at all
    assert nic.basket_trade(p, "UNCLASSIFIED", d, 5) == "small_basket"
    t = nic.basket_trade(p, "BANK", d, 5)
    assert isinstance(t, nic.Trade) and t.gross == 0.0 and t.excess == pytest.approx(0.0, abs=1e-12)
    # a stock judged on beta: A is 2x the universe as well
    t = nic.stock_trade(p, "A", d, 5, "beta")
    assert isinstance(t, nic.Trade) and t.excess == pytest.approx(0.0, abs=1e-9)


def test_beta_needs_200_pairs(cal):
    m, _ = beta_market(cal)
    p = m.panel(cal)
    # the universe is liquid from the 20th session on, so its first return is on the 21st (index 20): through
    # index k there are k - 19 pairs
    assert nic.beta(p, frozenset({"A"}), m.sessions[218]) is None
    assert nic.beta(p, frozenset({"A"}), m.sessions[219]) == pytest.approx(2.0, abs=1e-9)
    assert nic.stock_trade(p, "A", m.sessions[150], 1, "beta") == "no_beta"


def test_universe_daily_uses_previous_liquid_names_and_drops_ex_date_legs(cal):
    m, d = beta_market(cal)
    m.corp.append(("C", d, "split"))
    m.bar("C", d, 1000.0, 500.0)                                 # the unadjusted split halves the price
    p = m.panel(cal)
    a = p.closes[p.pos[d], p.sym_col["A"]] / p.closes[p.pos[d] - 1, p.sym_col["A"]] - 1.0
    # A, B and E move 2u; C's split leg is out; F and G are flat
    assert nic.universe_daily(p, d) == pytest.approx(3 * a * 100.0 / 5)
    assert nic.universe_daily(p, m.sessions[0]) is None


def test_basket_leg_voided_and_counted(cal):
    m, d = beta_market(cal)
    m.corp.append(("A", nth(m, d, 1), "split"))
    t = nic.basket_trade(m.panel(cal), "AUTO", d, 5)
    assert isinstance(t, nic.Trade) and t.legs == 2 and t.voided_legs == 1


# --------------------------------------------------------------------------- non-overlap
def test_non_overlap_per_key(cal):
    s = regular(cal, D(8, 3), D(8, 31))
    sig = [("A", s[0]), ("A", s[1]), ("B", s[1]), ("A", s[3]), ("A", s[3]), ("B", s[2]), ("C", s[2])]
    kept = nic.keep_non_overlapping(sig, s, 3)
    # A's position opened on s0 is held through s2, so s1 is skipped and s3 opens the next; one position per key
    assert kept == [("A", s[0]), ("B", s[1]), ("C", s[2]), ("A", s[3])]
    assert nic.keep_non_overlapping([], s, 3) == []


# --------------------------------------------------------------------------- the resolver
def resolver_db() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    conn.execute("CREATE TABLE entity_aliases (alias TEXT NOT NULL, tradingsymbol TEXT NOT NULL, "
                 "source TEXT NOT NULL DEFAULT 'seed', added_at TIMESTAMPTZ, PRIMARY KEY (alias, tradingsymbol))")
    conn.execute("CREATE TABLE sector_map (as_of DATE NOT NULL, symbol TEXT NOT NULL, sector TEXT NOT NULL, "
                 "PRIMARY KEY (as_of, symbol))")
    conn.execute("CREATE TABLE theme_map (theme TEXT PRIMARY KEY, keywords TEXT[] NOT NULL, symbols TEXT[] NOT NULL, "
                 "updated_at TIMESTAMPTZ)")
    conn.executemany("INSERT INTO entity_aliases (alias, tradingsymbol, source) VALUES (?, ?, ?)", [
        ("reliance", "RELIANCE", "curated"), ("reliance", "RELINFRA", "seed"),
        ("tata", "TATAMOTORS", "seed"), ("tata", "TATASTEEL", "seed"),
        ("hdfc bank", "HDFCBANK", "seed"), ("infosys", "INFY", "seed"),
    ])
    conn.executemany("INSERT INTO sector_map VALUES (?, ?, ?)", [
        (D(7, 1), "HDFCBANK", "BANK"), (D(11, 8), "HDFCBANK", "BANK"), (D(11, 8), "INFY", "IT"),
    ])
    conn.execute("INSERT INTO theme_map VALUES ('defence', ['missile', 'DRDO'], [], NULL)")
    return conn


def test_resolver_curated_override():
    r = nic.build_resolver(resolver_db())
    s = story(headline="Reliance shares jump on retail listing", entities=())
    assert nic.stock_symbols(r, s) == frozenset({"RELIANCE"})              # the curated row wins over the seed
    assert nic.stock_symbols(r, story(headline="Tata stocks rally", entities=())) == frozenset()   # ambiguous
    # the entities are extra texts; a missing headline leaves only them
    assert nic.stock_symbols(r, story(headline=None, entities=("HDFC Bank", "Infosys"))) == frozenset(
        {"HDFCBANK", "INFY"})
    names, missing = nic.company_symbols(r, ["Reliance Industries", "Tata", "Nowhere Corp", "",
                                             "HDFC Bank and Infosys", "Infosys"])
    assert names == ["RELIANCE", None, None, None, None, "INFY"]
    assert missing == 4
    res = r.resolve(nic.NewsCluster(cluster_id="t", representative="DRDO missile test", source_domains=[],
                                    first_seen=at(D(8, 3), 8), last_seen=at(D(8, 3), 8)))
    assert res.themes == ["defence"]


def test_resolver_refuses_a_copy_without_aliases():
    conn = resolver_db()
    conn.execute("DELETE FROM entity_aliases")
    with pytest.raises(nic.Unavailable):
        nic.build_resolver(conn)


# --------------------------------------------------------------------------- the verdict
def trades(n: int, *, excess: float, net: float, second_half_net: float | None = None) -> list:
    days = regular(nic.load_calendar(), D(1, 5), D(12, 31))[:n]
    out = []
    for i, d in enumerate(days):
        v = second_half_net if second_half_net is not None and i >= n // 2 else net
        out.append(nic.Trade("K", d, d, d, gross=excess, bench=0.0, excess=excess, net=v))
    return out


def test_verdict_labels():
    pop = [0.0] * 1000
    assert nic.verdict(trades(29, excess=3.0, net=2.0), pop, 10)["label"] == "insufficient"
    assert nic.verdict(trades(30, excess=0.1, net=-0.1), pop, 10)["label"] == "refuted"
    assert nic.verdict(trades(40, excess=3.0, net=2.7), pop, 10)["label"] == "supported"
    assert nic.verdict(trades(40, excess=3.0, net=2.7, second_half_net=-1.0), pop, 10)["label"] == "null"
    # a sample whose gross excess is ordinary for the population is null, however positive its net
    v = nic.verdict(trades(40, excess=2.1, net=1.8), list(np.linspace(-1.0, 5.0, 1000)), 10)
    assert v["perm_p"] >= nic.ALPHA and v["label"] == "null"
    assert nic.verdict([], [0.0] * 10, 10)["label"] == "insufficient"


def test_verdict_permutation_reads_excess_not_net():
    rows = trades(40, excess=3.0, net=2.7)
    rows = [nic.Trade(t.key, t.signal_date, t.entry_date, t.exit_date, gross=-50.0, bench=-53.0, excess=t.excess,
                      net=t.net) for t in rows]
    v = nic.verdict(rows, [0.0] * 1000, 10)
    assert v["perm_p"] == 0.0 and v["label"] == "supported"


def test_verdict_refuses_non_finite_and_short_population():
    with pytest.raises(ValueError, match="non-finite"):
        nic.verdict(trades(40, excess=3.0, net=2.7), [0.0] * 999 + [float("nan")], 10)
    with pytest.raises(ValueError):
        nic.verdict(trades(40, excess=3.0, net=2.7), [0.0] * 10, 10)


def test_cost_is_one_cnc_round_trip_at_the_research_notional():
    mm = nic.mm
    assert nic.cost() == pytest.approx(mm.cost_pct("CNC")) and nic.cost() > 0
