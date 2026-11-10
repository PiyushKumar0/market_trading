"""``scripts/news_content_score.py``: the macro mood study's nightly H/C scoring and policy mapping.

Every LLM is a fake (``FakeLLM``, or the real ``StudyLLM`` over a fake ``query_fn``); every database is a temp file.
"""

from __future__ import annotations

import asyncio
import copy
import gzip
import hashlib
import importlib.util
import json
import os
import re
import sqlite3
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

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


ncs = _load("mt_news_content_score", "news_content_score.py")
ncc = _load("_news_content_capture_for_score", "news_content_capture.py")
mml = ncs.mml

from engine.core.config import config_dir, load_yaml  # noqa: E402
from engine.intelligence.agents import news_analyst  # noqa: E402
from engine.intelligence.context import ContextAssembler  # noqa: E402

IST = ZoneInfo("Asia/Kolkata")
HAIKU, SONNET = "claude-haiku-5-5", "claude-sonnet-5-5"
# Thursday and Friday nights: a cluster captured on 11-11 after 09:00 is past its cut-off (11-12 09:00) on NIGHT1.
NIGHT1 = datetime(2026, 11, 12, 22, 0, tzinfo=IST)
NIGHT2 = datetime(2026, 11, 13, 22, 0, tzinfo=IST)
_IDS = re.compile(r"cluster_id=(\S+)")

AGENT_CALLS_DDL = """
CREATE TABLE agent_calls (
    call_id TEXT PRIMARY KEY, agent_id TEXT NOT NULL, trigger TEXT, model TEXT, inputs_digest TEXT,
    context_gz BLOB, output_json TEXT, ok INTEGER NOT NULL, fail_reason TEXT, in_tokens INTEGER,
    out_tokens INTEGER, cache_read INTEGER, cache_write INTEGER, cost_usd TEXT, duration_ms INTEGER,
    at TEXT NOT NULL, prompt_version TEXT)
"""


# --------------------------------------------------------------------------- fixtures and builders
def ts(day: int, hh: int, mm: int = 0, ss: int = 0, month: int = 11) -> datetime:
    return datetime(2026, month, day, hh, mm, ss, tzinfo=IST)


def word(i: int, j: int) -> str:
    """A pseudo-word unique to (i, j): distinct titles never cluster together."""
    return "".join(chr(ord("a") + int(c, 16)) for c in hashlib.sha1(f"{i}-{j}".encode()).hexdigest()[:9])


def title(i: int) -> str:
    return " ".join(word(i, j) for j in range(5))


@pytest.fixture
def content_db(tmp_path) -> Path:
    db = tmp_path / "news_content.sqlite"
    con = sqlite3.connect(db)
    con.executescript(ncc._SCHEMA)
    con.close()
    return db


@pytest.fixture
def state_db(tmp_path) -> Path:
    db = tmp_path / "state.db"
    con = sqlite3.connect(db)
    con.execute(AGENT_CALLS_DDL)
    con.commit()
    con.close()
    return db


def add_item(db: Path, url: str, ttl: str, captured: datetime, *, summary: str | None = None,
             published: datetime | None = None, body: str | None = None,
             fetched: datetime | None = None) -> None:
    con = sqlite3.connect(db)
    con.execute("INSERT INTO items VALUES (?,?,?,?,?,?)",
                (url, "et", ttl, summary, published.isoformat() if published else None, captured.isoformat()))
    if body is not None:
        con.execute("INSERT INTO bodies VALUES (?,?,?,?,?)",
                    (url, (fetched or captured).isoformat(), 200, "p", body))
    con.commit()
    con.close()


def engine_prompt(rows: list[dict[str, Any]], themes=None) -> str:
    """The engine's own News Analyst user prompt for ``rows`` (it reads no instance state)."""
    asm = object.__new__(ContextAssembler)
    return asm.for_news_batch(rows, list(themes or ncs.THEME_VOCABULARY)).prompt()


def score(cid: str, event_type: str = "govt_program", sentiment: float = 0.5, materiality: float = 0.7,
          scope: str = "sector") -> dict[str, Any]:
    return {"cluster_id": cid, "scope": scope, "entities": [], "sectors": [], "themes": [],
            "sentiment": sentiment, "materiality": materiality, "event_type": event_type, "novelty": 0.5}


def add_call(db: Path, call_id: str, at: datetime, clusters: list[tuple[str, str]], scores: list[dict],
             *, ok: int = 1, agent_id: str = "news_analyst", context: bytes | None = None) -> None:
    rows = [{"cluster_id": cid, "representative": head, "source_domains": ["a.com"], "first_seen": at}
            for cid, head in clusters]
    blob = context if context is not None else gzip.compress(
        f"{news_analyst.SYSTEM_PROMPT}\n\n{engine_prompt(rows)}".encode())
    con = sqlite3.connect(db)
    con.execute(
        "INSERT INTO agent_calls (call_id, agent_id, trigger, model, context_gz, output_json, ok, at, "
        "prompt_version) VALUES (?,?,?,?,?,?,?,?,?)",
        (call_id, agent_id, "batch", "sonnet-5.5", blob, json.dumps({"scores": scores}), ok, at.isoformat(), "v1"))
    con.commit()
    con.close()


def q(db: Path, sql: str, *args) -> list[tuple]:
    con = sqlite3.connect(db)
    try:
        return con.execute(sql, args).fetchall()
    finally:
        con.close()


@dataclass
class FakeLLM:
    """``StudyLLM``'s surface: the cap and the stop raise as the real one does; answers follow the prompt."""

    max_calls: int = 40
    at: datetime = NIGHT1
    groups: tuple = ("ENERGY",)
    scorer: Any = None                       # (cid, model-variant prompt) -> score dict | None
    stop_at: int | None = None               # the call number that returns a stopping failure
    fail_at: frozenset = frozenset()         # call numbers that return an ordinary failure
    calls: list[dict] = field(default_factory=list)
    calls_made: int = 0
    stopped: bool = False

    async def call(self, *, model: str, system_prompt: str, prompt: str, json_schema: dict) -> Any:
        if self.stopped:
            raise mml.LLMStopped("overloaded: stopped")
        if self.calls_made >= self.max_calls:
            raise mml.CallCapReached(f"{self.max_calls} calls made")
        self.calls_made += 1
        n = self.calls_made
        self.calls.append({"model": model, "system_prompt": system_prompt, "prompt": prompt,
                           "json_schema": json_schema})
        if n == self.stop_at:
            self.stopped = True
            return mml.LLMResult(False, None, "overloaded: session limit", model, self.at, 0.1)
        if n in self.fail_at:
            return mml.LLMResult(False, None, "timeout: no complete response", model, self.at, 0.1)
        if model == HAIKU:
            out = []
            for cid in _IDS.findall(prompt):
                s = self.scorer(cid, prompt) if self.scorer else score(cid)
                if s is not None:
                    out.append(s)
            payload: dict = {"scores": out}
        else:
            payload = {"groups": list(self.groups)}
        return mml.LLMResult(True, payload, None, model, self.at, 0.1)


def run(content_db: Path, state_db: Path, llm: Any, now: datetime = NIGHT1, clock: Any = None) -> dict:
    """One night whose per-call market-hours clock reads ``now`` unless given: never the wall clock."""
    return asyncio.run(ncs.run_night(content_db, state_db, llm, now, clock=clock or (lambda: now)))


def prompts(llm: FakeLLM, model: str) -> list[str]:
    return [c["prompt"] for c in llm.calls if c["model"] == model]


def head_line(prompt: str) -> list[str]:
    """Each cluster's ``N. cluster_id=... | first seen ... | k source domain(s)`` line."""
    return [line for line in prompt.split("\n") if _IDS.search(line)]


# --------------------------------------------------------------------------- prompts, cut-offs, candidates, CLI
def test_h_and_c_prompts_differ_only_by_summary_and_excerpt_lines(content_db, state_db):
    add_item(content_db, "https://a.com/1", title(1), ts(11, 10), summary="Summary one.", body="Body one text.")
    add_item(content_db, "https://b.com/2", title(2), ts(11, 11), summary=None, body="Body two text.")
    add_item(content_db, "https://c.com/3", title(3), ts(11, 12), summary="Summary three.", body="")
    llm = FakeLLM()

    run(content_db, state_db, llm)

    h, c = prompts(llm, HAIKU)
    assert h != c
    stripped = "\n".join(line for line in c.split("\n")
                         if not line.startswith(("   summary: ", "   excerpt: ")))
    assert stripped == h
    assert "   summary: Summary one.\n   excerpt: Body one text." in c
    assert "   excerpt: Body two text." in c and c.count("   summary: ") == 2
    assert c.count("   excerpt: ") == 2                 # the empty body has no excerpt line
    for call in llm.calls[:2]:
        assert call["system_prompt"] == news_analyst.SYSTEM_PROMPT
        assert call["json_schema"] == news_analyst.output_json_schema()


def test_excerpt_omitted_when_body_fetched_after_cutoff(content_db, state_db):
    # Captured 08:59:59, body fetched 09:00:05: the window closed at 09:00 that day.
    add_item(content_db, "https://a.com/late", title(1), ts(11, 8, 59, 59), summary="S late.",
             body="Late body.", fetched=ts(11, 9, 0, 5))
    # Captured exactly 09:00:00: its window closes at 09:00 the next day.
    add_item(content_db, "https://a.com/edge", title(2), ts(11, 9, 0, 0), summary="S edge.",
             body="Edge body.", fetched=ts(11, 9, 0, 5))
    # Fetched exactly at the cut-off: not earlier than it, so omitted.
    add_item(content_db, "https://a.com/exact", title(3), ts(11, 8, 0), body="Exact body.",
             fetched=ts(11, 9, 0, 0))
    add_item(content_db, "https://a.com/ok", title(4), ts(11, 7, 0), body="Early body.",
             fetched=ts(11, 8, 59, 59))
    llm = FakeLLM()

    run(content_db, state_db, llm)

    c = prompts(llm, HAIKU)[1]
    assert "   summary: S late." in c and "Late body." not in c
    assert "   excerpt: Edge body." in c
    assert "Exact body." not in c
    assert "   excerpt: Early body." in c
    assert ncs.cutoff_for(ts(11, 8, 59, 59)) == ts(11, 9)
    assert ncs.cutoff_for(ts(11, 9, 0, 0)) == ts(12, 9)
    assert ncs.cutoff_for(ts(11, 23, 59, 59)) == ts(12, 9)
    assert ncs.cutoff_for(datetime(2026, 11, 11, 3, 29, 59, tzinfo=ZoneInfo("UTC"))) == ts(11, 9)
    assert ncs.cutoff_for(datetime(2026, 11, 11, 3, 30, 0, tzinfo=ZoneInfo("UTC"))) == ts(12, 9)


def test_headline_candidates_read_from_agent_calls_only_ok_news_analyst(content_db, state_db):
    t = ts(10, 10)
    add_call(state_db, "k1", t, [("c-A", "Govt clears PLI for drones"), ("c-B", "Quiet market wrap"),
                                 ("c-C", "Weak policy"), ("c-D", "Minor policy")],
             [score("c-A"), score("c-B", event_type="other"), score("c-C", sentiment=0.39),
              score("c-D", event_type="sector_policy", materiality=0.59)])
    add_call(state_db, "k2", t + timedelta(minutes=5), [("c-E", "Failed call story")], [score("c-E")], ok=0)
    add_call(state_db, "k3", t + timedelta(minutes=6), [("c-F", "Other agent story")], [score("c-F")],
             agent_id="intraday_analyst")
    # A later rescore never replaces the first score, in either direction.
    add_call(state_db, "k4", t + timedelta(minutes=7), [("c-B", "Quiet market wrap"), ("c-A", "Rewritten")],
             [score("c-B"), score("c-A", event_type="other")])
    add_call(state_db, "k5", t + timedelta(minutes=8), [("c-G", "Broken context story")], [score("c-G")],
             context=b"not gzip at all")
    add_call(state_db, "k6", t + timedelta(minutes=9), [("c-H", "Boundary scheme story")],
             [score("c-H", event_type="sector_policy", sentiment=0.4, materiality=0.6)])
    # Duplicate scores for one cluster inside one call: the first wins.
    add_call(state_db, "k7", t + timedelta(minutes=10), [("c-I", "Dup story")],
             [score("c-I", event_type="other"), score("c-I")])
    llm = FakeLLM()

    out = run(content_db, state_db, llm)

    mapped = q(content_db, "SELECT candidate_key, groups FROM mappings WHERE source='headline' ORDER BY 1")
    assert mapped == [("c-A", '["ENERGY"]'), ("c-H", '["ENERGY"]')]
    sonnet = prompts(llm, SONNET)
    assert sonnet == ["headline: Govt clears PLI for drones", "headline: Boundary scheme story"]
    assert all(c["system_prompt"] == ncs.MAPPER_PROMPT_PATH.read_text(encoding="utf-8")
               for c in llm.calls)
    assert out["headline_no_text_total"] == 1            # c-G: its context is unreadable


def test_context_headline_parsing_survives_damage():
    good = gzip.compress(f"{news_analyst.SYSTEM_PROMPT}\n\n{engine_prompt([
        {'cluster_id': 'c-A', 'representative': 'Rail capex boost', 'source_domains': [], 'first_seen': ts(10, 10)},
        {'cluster_id': 'c-B', 'representative': '', 'source_domains': [], 'first_seen': ts(10, 10)},
    ])}".encode())
    assert ncs._context_headlines(good) == {"c-A": "Rail capex boost"}       # an empty headline is no headline
    assert ncs._context_headlines(good[:-12]) == {}                           # truncated gzip
    assert ncs._context_headlines(None) == {} and ncs._context_headlines(b"") == {}
    assert ncs._context_headlines(gzip.compress(b"1. cluster_id=c-A | x\nheadline: no indent")) == {}
    assert ncs._context_headlines(gzip.compress(b"\xff\xfe1. cluster_id=c-A | x\n   headline: H")) == {}


def test_malformed_output_json_rows_are_skipped(content_db, state_db):
    add_call(state_db, "k1", ts(10, 10), [("c-A", "Govt scheme")], [score("c-A")])
    con = sqlite3.connect(state_db)
    con.execute("UPDATE agent_calls SET output_json = '{not json' WHERE call_id = 'k1'")
    con.commit()
    con.close()
    add_call(state_db, "k2", ts(10, 11), [("c-A", "Govt scheme again")], [score("c-A")])
    add_call(state_db, "k3", ts(10, 12), [("c-B", "Null output")], [])
    con = sqlite3.connect(state_db)
    con.execute("UPDATE agent_calls SET output_json = NULL WHERE call_id = 'k3'")
    con.commit()
    con.close()
    llm = FakeLLM()

    run(content_db, state_db, llm)

    assert prompts(llm, SONNET) == ["headline: Govt scheme again"]       # the first READABLE score counts


def test_cli_refuses_a_cap_above_forty_and_skips_a_stopped_day(content_db, state_db, monkeypatch):
    with pytest.raises(SystemExit) as exc:
        ncs.main(["--content-db", str(content_db), "--state-db", str(state_db), "--max-calls", "41"])
    assert exc.value.code == 2

    add_item(content_db, "https://a.com/1", title(1), ts(11, 10))
    run(content_db, state_db, FakeLLM(stop_at=1, at=datetime.now(IST)))
    monkeypatch.setattr(ncs, "call_refusal", lambda now: None)

    def no_llm(**kw):
        raise AssertionError("a stopped day must not build an LLM")

    monkeypatch.setattr(ncs.mml, "StudyLLM", no_llm)
    assert ncs.main(["--content-db", str(content_db), "--state-db", str(state_db)]) == 0
    assert not (content_db.parent / "news_content_score.lock").exists()      # released on the way out


def test_cli_skips_while_another_run_holds_the_lock(content_db, state_db, monkeypatch):
    lock = content_db.parent / "news_content_score.lock"
    lock.write_bytes(b"")
    monkeypatch.setattr(ncs, "call_refusal", lambda now: None)

    def no_llm(**kw):
        raise AssertionError("a locked run must not build an LLM")

    monkeypatch.setattr(ncs.mml, "StudyLLM", no_llm)
    assert ncs.main(["--content-db", str(content_db), "--state-db", str(state_db)]) == 0
    assert lock.exists()                                                        # never another run's lock

    old = lock.stat().st_mtime - ncs.LOCK_STALE_S - 1
    os.utime(lock, (old, old))
    assert ncs._acquire_lock(lock) is True                                      # a crashed run's lock is reclaimed


def test_cli_hands_the_budgets_remaining_calls_to_the_llm(content_db, state_db, monkeypatch):
    add_item(content_db, "https://a.com/1", title(1), ts(11, 10))
    add_item(content_db, "https://a.com/2", title(2), ts(11, 10, 1))
    today = datetime.now(IST)
    run(content_db, state_db, FakeLLM(at=today, max_calls=3))                   # three calls logged today
    assert ncs.night_budget(content_db, today) == (37, None)
    monkeypatch.setattr(ncs, "call_refusal", lambda now: None)
    built: list[int] = []
    nights: list[tuple] = []

    class Spy:
        def __init__(self, *, max_calls):
            built.append(max_calls)

    async def fake_night(content, state, llm, now, **kw):
        nights.append((content, state, type(llm).__name__))
        return ncs._summary()

    monkeypatch.setattr(ncs.mml, "StudyLLM", Spy)
    monkeypatch.setattr(ncs, "run_night", fake_night)
    assert ncs.main(["--content-db", str(content_db), "--state-db", str(state_db)]) == 0
    assert built == [37] and nights == [(content_db, state_db, "Spy")]
    assert ncs.main(["--content-db", str(content_db), "--state-db", str(state_db), "--max-calls", "5"]) == 0
    assert built == [37, 2]                                                     # the cap minus today's three


def test_representatives_capture_sets_the_window(content_db, state_db):
    # B was published first, so it founds the cluster and represents it, but it was captured after 09:00:
    # the scored text was not known before that 09:00, so the cluster belongs to the next window.
    base = "Centre notifies production linked incentive for specialty steel makers"
    add_item(content_db, "https://a.com/A", base + " sector", ts(11, 8, 30), published=ts(11, 8, 20))
    add_item(content_db, "https://b.com/B", base, ts(11, 9, 30), published=ts(11, 8, 0))
    llm = FakeLLM()
    run(content_db, state_db, llm)

    assert q(content_db, "SELECT cluster_key, first_captured_at, representative_url, member_urls FROM clusters") == [
        ("c-" + ncs.headline_id_for("https://b.com/B"), ts(11, 9, 30).isoformat(), "https://b.com/B",
         '["https://b.com/B", "https://a.com/A"]')]
    assert ncs.cutoff_for(ts(11, 9, 30)) == ts(12, 9)
    assert "| 2 source domain(s)" in prompts(llm, HAIKU)[0]


def test_mapper_keeps_first_three_in_vocab_groups(content_db, state_db):
    raw = {"groups": ["AUTO", "Tata Motors", "defence", "AUTO", "auto", 7, "IT", "BANK"]}
    assert ncs.parse_mapper_output(raw) == ["AUTO", "defence", "IT"]
    assert ncs.parse_mapper_output({"groups": []}) == []
    assert ncs.parse_mapper_output({"groups": "AUTO"}) is None
    assert ncs.parse_mapper_output(["AUTO"]) is None
    assert ncs.parse_mapper_output(None) is None

    add_call(state_db, "k1", ts(10, 10), [("c-A", "Govt scheme")], [score("c-A")])
    llm = FakeLLM(groups=("NOT_A_GROUP", "railways", "METAL", "ENERGY", "IT"))
    run(content_db, state_db, llm)

    assert q(content_db, "SELECT groups FROM mappings") == [('["railways", "METAL", "ENERGY"]',)]
    assert llm.calls[0]["json_schema"] == ncs.MAPPER_SCHEMA


@dataclass
class FakeOptions:
    model: str | None = None
    fallback_model: str | None = None
    system_prompt: str | None = None
    setting_sources: list[str] | None = None
    max_turns: int | None = None
    tools: list[str] | None = None
    allowed_tools: list[str] = field(default_factory=list)
    disallowed_tools: list[str] = field(default_factory=list)
    output_format: dict[str, Any] | None = None
    effort: str | None = None
    env: dict[str, str] = field(default_factory=dict)
    strict_mcp_config: bool = False
    extra_args: dict[str, Any] = field(default_factory=dict)
    cli_path: str | None = None


class FakeResult:
    def __init__(self, structured_output: Any) -> None:
        self.structured_output = structured_output
        self.is_error = False
        self.result = None
        self.subtype = "success"
        self.errors = []
        self.total_cost_usd = 0.001
        self.usage = {"input_tokens": 10, "output_tokens": 5}


class Responder:
    """A ``query_fn`` that answers a scoring prompt with policy scores and a mapper prompt with groups."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    def __call__(self, *, prompt: str, options: Any) -> Any:
        self.calls.append((prompt, options))
        if options.model == HAIKU:
            payload: dict = {"scores": [score(cid) for cid in _IDS.findall(prompt)]}
        else:
            payload = {"groups": ["ENERGY"]}
        return self._stream(FakeResult(payload))

    async def _stream(self, msg: Any) -> Any:
        yield msg


@pytest.fixture
def agents_cfg(tmp_path, monkeypatch) -> dict[str, Any]:
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    cli = tmp_path / "claude.exe"
    cli.write_bytes(b"")
    cfg = copy.deepcopy(load_yaml(config_dir() / "agents.yaml"))
    cfg.setdefault("llm", {})["cli_path"] = str(cli)
    return cfg


def test_night_stops_at_cap_and_resumes_next_night(content_db, state_db, agents_cfg):
    for i in range(31):
        add_item(content_db, f"https://a.com/{i}", title(i), ts(11, 10) + timedelta(minutes=i), summary=f"S{i}.")
    add_call(state_db, "k1", ts(11, 9, 30), [("c-A", "Govt scheme")], [score("c-A")])
    responder = Responder()
    llm = mml.StudyLLM(max_calls=3, query_fn=responder, options_cls=FakeOptions, agents_cfg=agents_cfg)

    out1 = run(content_db, state_db, llm, NIGHT1)

    assert out1["stopped"].startswith("CallCapReached")
    # Batch 1 (30 oldest) scored H and C, batch 2 scored H only; nothing mapped yet.
    assert q(content_db, "SELECT variant, count(*) FROM scores GROUP BY 1 ORDER BY 1") == [("C", 30), ("H", 31)]
    assert q(content_db, "SELECT count(*) FROM mappings") == [(0,)]
    assert [len(_IDS.findall(p)) for p, _ in responder.calls] == [30, 30, 1]
    assert q(content_db, "SELECT purpose FROM llm_calls ORDER BY at, rowid") == [
        ("score_H",), ("score_C",), ("score_H",)]

    llm2 = mml.StudyLLM(max_calls=40, query_fn=responder, options_cls=FakeOptions, agents_cfg=agents_cfg)
    out2 = run(content_db, state_db, llm2, NIGHT1 + timedelta(days=1))

    assert out2["stopped"].startswith("CallCapReached")
    assert q(content_db, "SELECT variant, count(*) FROM scores GROUP BY 1 ORDER BY 1") == [("C", 31), ("H", 31)]
    assert len(_IDS.findall(responder.calls[3][0])) == 1           # only the leftover C, never a re-score
    # The leftover C, the engine headline, 31 content candidates, then study_h until the cap.
    assert q(content_db, "SELECT source, count(*) FROM mappings GROUP BY 1 ORDER BY 1") == [
        ("content", 31), ("headline", 1), ("study_h", 7)]

    llm3 = mml.StudyLLM(max_calls=40, query_fn=responder, options_cls=FakeOptions, agents_cfg=agents_cfg)
    out3 = run(content_db, state_db, llm3, NIGHT1 + timedelta(days=2))

    assert out3["stopped"] is None and llm3.calls_made == 24
    assert q(content_db, "SELECT source, count(*) FROM mappings GROUP BY 1 ORDER BY 1") == [
        ("content", 31), ("headline", 1), ("study_h", 31)]
    assert q(content_db, "SELECT count(*) FROM llm_calls") == [(3 + 40 + 24,)]


def test_never_writes_state_db(content_db, state_db, monkeypatch):
    add_call(state_db, "k1", ts(10, 10), [("c-A", "Govt scheme")], [score("c-A")])
    add_item(content_db, "https://a.com/1", title(1), ts(11, 10))
    before = state_db.read_bytes()
    opened: list[tuple] = []
    real_connect = sqlite3.connect

    def spy(database, *args, **kwargs):
        opened.append((str(database), kwargs.get("uri", False)))
        return real_connect(database, *args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", spy)
    run(content_db, state_db, FakeLLM())
    monkeypatch.undo()

    assert state_db.read_bytes() == before
    state_opens = [o for o in opened if "state.db" in o[0]]
    assert state_opens and all(o[0].endswith("?mode=ro") and o[1] for o in state_opens)
    con = ncs.open_state_ro(state_db)
    try:
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            con.execute("INSERT INTO agent_calls (call_id, agent_id, ok, at) VALUES ('x','y',1,'z')")
    finally:
        con.close()


# --------------------------------------------------------------------------- seams with frozen inputs
def test_h_prompt_is_the_engine_news_batch_layout():
    rows = [ncs.PromptCluster(f"c-s{i}", title(i), ts(11, 10 + i), ("a.com", "b.com")[: 1 + i % 2],
                              f"sum {i}", f"body {i}") for i in range(3)]
    expected = engine_prompt([{"cluster_id": r.cluster_key, "representative": r.headline,
                               "source_domains": list(r.source_domains), "first_seen": r.first_seen}
                              for r in rows])
    assert ncs.build_prompt(rows, "H") == expected
    with pytest.raises(ValueError):
        ncs.build_prompt(rows * 11, "H")


def test_theme_vocabulary_is_themes_yaml_in_file_order():
    assert list(ncs.THEME_VOCABULARY) == list(load_yaml(config_dir() / "themes.yaml")["themes"])
    assert len(ncs.THEME_VOCABULARY) == 8


def test_vocab_matches_mapper_prompt_and_theme_lists():
    text = ncs.MAPPER_PROMPT_PATH.read_text(encoding="utf-8")
    sectors = re.search(r"Sector groups: (.*?)\n\nTheme groups: (.*?)\n", text, re.S)
    listed = [s.strip() for s in sectors.group(1).replace("\n", " ").split(",")]
    themes = [s.strip() for s in sectors.group(2).split(",")]
    assert list(ncs.VOCAB) == listed + themes
    assert len(listed) == 23 and len(themes) == 7
    frozen = load_yaml(config_dir() / "research" / "macro_mood" / "theme_lists.yaml")["themes"]
    assert set(themes) == set(frozen)
    assert ncs.MAPPER_SCHEMA == {"type": "object",
                                 "properties": {"groups": {"type": "array",
                                                           "items": {"type": "string", "enum": list(ncs.VOCAB)}}},
                                 "required": ["groups"], "additionalProperties": False}


# --------------------------------------------------------------------------- budget, stop and refusal
def test_budget_subtracts_todays_calls_and_a_stop_ends_the_day(content_db, state_db):
    add_item(content_db, "https://a.com/1", title(1), ts(11, 10))
    add_item(content_db, "https://a.com/2", title(2), ts(11, 10, 5))
    assert ncs.night_budget(content_db, NIGHT1) == (40, None)

    llm = FakeLLM(stop_at=2, at=ts(12, 21, 59))
    out = run(content_db, state_db, llm)

    assert out["stopped"].startswith("LLMStopped")
    assert llm.calls_made == 2
    remaining, stop = ncs.night_budget(content_db, NIGHT1)
    assert remaining == 38 and stop.startswith("overloaded:")
    assert ncs.night_budget(content_db, NIGHT1, max_calls=1)[0] == 0

    again = FakeLLM()
    rerun = run(content_db, state_db, again)                  # same night after the stop: nothing happens
    assert rerun["skipped"].startswith("stopped earlier today") and again.calls_made == 0

    assert ncs.night_budget(content_db, NIGHT2) == (40, None)    # a new IST day
    nxt = FakeLLM(at=NIGHT2)
    run(content_db, state_db, nxt, NIGHT2)
    assert q(content_db, "SELECT variant, count(*) FROM scores GROUP BY 1 ORDER BY 1") == [("C", 2), ("H", 2)]


def test_budget_counts_by_ist_date_across_midnight(content_db, state_db):
    add_item(content_db, "https://a.com/1", title(1), ts(11, 10))
    run(content_db, state_db, FakeLLM(at=ts(12, 23, 59, 59), max_calls=1), ts(12, 23, 59, 59))
    assert ncs.night_budget(content_db, ts(12, 23, 59, 59))[0] == 39
    assert ncs.night_budget(content_db, ts(13, 0, 0, 0))[0] == 40


@pytest.mark.parametrize("now", [ts(11, 9, 15), ts(11, 15, 29, 59), ts(13, 12, 0)])
def test_market_hours_are_refused(content_db, state_db, now):
    add_item(content_db, "https://a.com/1", title(1), ts(11, 8))
    llm = FakeLLM()
    out = run(content_db, state_db, llm, now)
    assert out["skipped"].startswith("market hours") and llm.calls_made == 0
    assert q(content_db, "SELECT name FROM sqlite_master WHERE name='clusters'") == []


@pytest.mark.parametrize("now", [ts(11, 9, 14, 59), ts(11, 15, 30), ts(15, 11, 0)])   # 11-15 is a Sunday
def test_outside_market_hours_runs(content_db, state_db, now):
    add_item(content_db, "https://a.com/1", title(1), ts(11, 8))
    assert run(content_db, state_db, FakeLLM(at=now), now)["skipped"] is None


def test_a_night_resumed_inside_market_hours_stops_before_its_next_call(content_db, state_db):
    add_item(content_db, "https://a.com/1", title(1), ts(11, 10))
    reads = iter([NIGHT1])                                   # the first call at 22:00, then standby until 09:20

    def clock():
        return next(reads, ts(13, 9, 20))

    llm = FakeLLM()
    out = run(content_db, state_db, llm, NIGHT1, clock=clock)
    assert llm.calls_made == 1
    assert out["stopped"].startswith("MarketHoursHalt: market hours (2026-11-13 09:20 IST)")
    assert q(content_db, "SELECT purpose FROM llm_calls") == [("score_H",)]


def test_the_calendar_rule_alone_refuses(content_db, state_db, monkeypatch):
    add_item(content_db, "https://a.com/1", title(1), ts(11, 10))
    sunday_session = ts(15, 10)                              # a weekend special session the weekday rule misses
    assert ncs.market_hours_refusal(sunday_session) is None
    monkeypatch.setattr(ncs, "calendar_refusal",
                        lambda now: "market hours (special session)" if now == sunday_session else None)
    assert ncs.call_refusal(sunday_session) == "market hours (special session)"
    llm = FakeLLM()
    out = run(content_db, state_db, llm, sunday_session)
    assert out["skipped"] == "market hours (special session)" and llm.calls_made == 0

    reads = iter([NIGHT1])                                   # and before a call, on the injected clock
    llm2 = FakeLLM()
    out2 = run(content_db, state_db, llm2, NIGHT1, clock=lambda: next(reads, sunday_session))
    assert llm2.calls_made == 1 and out2["stopped"] == "MarketHoursHalt: market hours (special session)"


def test_the_calendar_rule_runs_through_the_real_calendar():
    session = datetime(2026, 11, 12, 10, 0, tzinfo=IST)        # a regular Thursday session
    assert ncs.calendar_refusal(session).startswith("market hours")
    assert ncs.calendar_refusal(session.replace(hour=22)) is None


def test_a_calendar_that_raises_is_ignored_and_the_weekday_rule_stands(content_db, state_db, monkeypatch):
    def broken(now):
        raise FileNotFoundError("config/calendar/2027.yaml")

    monkeypatch.setattr(ncs, "calendar_refusal", broken)
    assert ncs.call_refusal(ts(15, 10)) is None                                 # Sunday: nothing refuses
    assert ncs.call_refusal(ts(13, 10)).startswith("market hours")               # Friday: the weekday rule
    add_item(content_db, "https://a.com/1", title(1), ts(11, 10))
    llm = FakeLLM()
    assert run(content_db, state_db, llm)["stopped"] is None and llm.calls_made == 4
    assert run(content_db, state_db, FakeLLM(), ts(13, 10))["skipped"].startswith("market hours")


def test_prompt_pins_are_the_registered_prompts():
    mapper = ncs.MAPPER_PROMPT_PATH.read_bytes()
    assert hashlib.sha256(mapper).hexdigest() == ncs.MAPPER_PROMPT_SHA256
    score_text = news_analyst.SYSTEM_PROMPT + "\n" + json.dumps(news_analyst.output_json_schema(), sort_keys=True)
    assert hashlib.sha256(score_text.encode("utf-8")).hexdigest() == ncs.SCORE_PROMPT_SHA256
    assert ncs.prompt_pin_mismatch(mapper) is None
    assert ncs.prompt_pin_mismatch(mapper + b" ") == "the mapper prompt"


@pytest.mark.parametrize("pin, which", [("MAPPER_PROMPT_SHA256", "the mapper prompt"),
                                        ("SCORE_PROMPT_SHA256", "the scoring prompt")])
def test_a_changed_prompt_stops_accrual_without_a_call(content_db, state_db, monkeypatch, pin, which):
    add_item(content_db, "https://a.com/1", title(1), ts(11, 10))
    add_call(state_db, "k1", ts(10, 10), [("c-A", "Govt scheme")], [score("c-A")])
    monkeypatch.setattr(ncs, pin, "0" * 64)
    llm = FakeLLM()
    out = run(content_db, state_db, llm)
    assert out["skipped"] == (f"{which} changed since registration: content accrual stops until the owner "
                              "re-registers")
    assert llm.calls_made == 0 and q(content_db, "SELECT name FROM sqlite_master WHERE name='llm_calls'") == []


def test_every_run_appends_its_summary_to_score_runs(content_db, state_db, monkeypatch):
    add_item(content_db, "https://a.com/1", title(1), ts(11, 10))
    finished = [ts(12, 22, 5)]
    run(content_db, state_db, FakeLLM(), NIGHT1, clock=lambda: finished[0])     # a full night
    run(content_db, state_db, FakeLLM(), ts(13, 10))                            # skipped: market hours
    run(content_db, state_db, FakeLLM(max_calls=0), NIGHT2)                     # nothing to do, never stopped
    add_item(content_db, "https://a.com/2", title(2), ts(12, 10))
    run(content_db, state_db, FakeLLM(max_calls=0), NIGHT2)                     # stopped by the cap
    def unreadable(path):
        raise sqlite3.OperationalError("gone")

    monkeypatch.setattr(ncs, "open_state_ro", unreadable)
    with pytest.raises(sqlite3.OperationalError):
        run(content_db, state_db, FakeLLM(), NIGHT2)                            # a setup fault, logged then raised
    rows = q(content_db, "SELECT run_at, finished_at, summary_json FROM score_runs ORDER BY rowid")
    assert [(r[0], r[1]) for r in rows[:2]] == [(NIGHT1.isoformat(), ts(12, 22, 5).isoformat()),
                                                (ts(13, 10).isoformat(), ts(13, 10).isoformat())]
    docs = [json.loads(r[2]) for r in rows]
    assert docs[0]["scored"] == {"H": 1, "C": 1} and docs[0]["skipped"] is None and docs[0]["calls"] == 4
    assert docs[1]["skipped"].startswith("market hours")
    assert docs[2]["calls"] == 0 and docs[2]["stopped"] is None
    assert docs[3]["stopped"].startswith("CallCapReached")
    assert docs[4]["error"] == "OperationalError: gone"
    assert len(rows) == 5


def test_a_missing_content_file_raises_and_logs_nothing(tmp_path, state_db):
    with pytest.raises(FileNotFoundError):
        run(tmp_path / "absent.sqlite", state_db, FakeLLM())
    assert not (tmp_path / "absent.sqlite").exists()


# --------------------------------------------------------------------------- the cut-off rule
def test_a_cluster_is_not_scored_before_its_cutoff(content_db, state_db):
    add_item(content_db, "https://a.com/1", title(1), ts(12, 9, 0))                # cut-off 11-13 09:00
    add_item(content_db, "https://a.com/2", title(2), ts(12, 8, 59, 59))           # cut-off 11-12 09:00
    llm = FakeLLM()
    out = run(content_db, state_db, llm, NIGHT1)
    assert out["new_clusters"] == 2
    assert [_IDS.findall(p) for p in prompts(llm, HAIKU)] == [["c-" + ncs.headline_id_for("https://a.com/2")]] * 2

    llm0 = FakeLLM()
    run(content_db, state_db, llm0, ts(13, 8, 59, 59))                          # still before it: no call
    assert llm0.calls_made == 0
    llm2 = FakeLLM(at=NIGHT2)
    run(content_db, state_db, llm2, NIGHT2)
    assert [_IDS.findall(p) for p in prompts(llm2, HAIKU)] == [["c-" + ncs.headline_id_for("https://a.com/1")]] * 2


def test_a_retried_c_sees_the_members_h_saw(content_db, state_db):
    base = "Cabinet clears new semiconductor fab incentive worth Rs 76,000 crore"
    add_item(content_db, "https://a.com/1", base, ts(11, 10), published=ts(11, 9, 50), summary="S.")
    llm1 = FakeLLM(fail_at=frozenset({2}))                                      # H scored, C fails
    run(content_db, state_db, llm1, NIGHT1)
    assert q(content_db, "SELECT variant FROM scores") == [("H",)]
    # A new member captured after the 11-12 09:00 cut-off joins before C is retried.
    add_item(content_db, "https://b.com/2", base + " now", ts(12, 23), published=ts(12, 8, 0))
    llm2 = FakeLLM(at=NIGHT2)
    run(content_db, state_db, llm2, NIGHT2)
    (h,) = prompts(llm1, HAIKU)[:1]
    (c,) = prompts(llm2, HAIKU)
    assert "   summary: S." in c and c.count("cluster_id=") == 1
    assert head_line(c) == head_line(h) == [
        f"1. cluster_id={'c-' + ncs.headline_id_for('https://a.com/1')} | first seen 2026-11-11 | 1 source domain(s)"]
    assert json.loads(q(content_db, "SELECT member_urls FROM clusters")[0][0]) == [
        "https://a.com/1", "https://b.com/2"]


# --------------------------------------------------------------------------- clustering and scoring
def test_clustering_is_stable_across_two_incremental_nights(content_db, state_db):
    base = "Government approves Rs 10,000 crore PLI scheme for drone makers in India"
    add_item(content_db, "https://a.com/x1", base, ts(11, 10), published=ts(11, 9, 55), summary="X1.")
    add_item(content_db, "https://b.com/y", title(7), ts(11, 11))
    llm1 = FakeLLM()
    run(content_db, state_db, llm1)
    key_x = "c-" + ncs.headline_id_for("https://a.com/x1")
    assert q(content_db, "SELECT count(*) FROM clusters") == [(2,)]

    add_item(content_db, "https://b.com/x2", base + " today", ts(12, 9, 30), published=ts(12, 9, 25))
    add_item(content_db, "https://c.com/z", title(8), ts(12, 10))
    llm2 = FakeLLM(at=NIGHT2)
    out2 = run(content_db, state_db, llm2, NIGHT2)

    assert out2["new_clusters"] == 1 and out2["clustered_items"] == 2
    rows = dict((r[0], r[1:]) for r in q(
        content_db, "SELECT cluster_key, first_captured_at, representative_url, member_urls FROM clusters"))
    assert len(rows) == 3
    assert rows[key_x][0] == ts(11, 10).isoformat()
    assert rows[key_x][1] == "https://a.com/x1"
    assert json.loads(rows[key_x][2]) == ["https://a.com/x1", "https://b.com/x2"]
    # Night 2 scores only the new cluster; the joined one keeps its night-1 scores.
    assert all(_IDS.findall(p) == ["c-" + ncs.headline_id_for("https://c.com/z")] for p in prompts(llm2, HAIKU))
    night1_calls = {r[0] for r in q(content_db, "SELECT call_id FROM llm_calls WHERE at < ?", NIGHT2.isoformat())}
    assert {r[0] for r in q(content_db, "SELECT call_id FROM scores WHERE cluster_key=?", key_x)} <= night1_calls

    llm3 = FakeLLM(at=NIGHT2)
    out3 = run(content_db, state_db, llm3, NIGHT2 + timedelta(minutes=30))     # nothing new: a no-op
    assert out3["clustered_items"] == 0 and llm3.calls_made == 0
    assert q(content_db, "SELECT count(*) FROM clusters") == [(3,)]


def test_prompt_counts_only_members_captured_before_the_cutoff(content_db, state_db):
    base = "Cabinet clears new semiconductor fab incentive worth Rs 76,000 crore"
    add_item(content_db, "https://a.com/1", base, ts(11, 10), published=ts(11, 9, 50))
    llm1 = FakeLLM(max_calls=0)
    run(content_db, state_db, llm1)                                            # clustered, not scored
    add_item(content_db, "https://b.com/2", base + " now", ts(12, 10), published=ts(12, 9, 50))
    llm2 = FakeLLM(at=NIGHT2)
    run(content_db, state_db, llm2, NIGHT2)

    h = prompts(llm2, HAIKU)[0]
    assert "| first seen 2026-11-11 | 1 source domain(s)" in h        # b.com joined after the 11-12 09:00 cut-off
    assert q(content_db, "SELECT source_domains FROM clusters") == [('["a.com", "b.com"]',)]


def test_pre_window_clusters_are_clustered_but_never_scored(content_db, state_db):
    add_item(content_db, "https://a.com/old", title(1), ts(10, 8, 59, 59))
    add_item(content_db, "https://a.com/new", title(2), ts(10, 9, 0, 0))
    llm = FakeLLM(at=ts(11, 22))
    run(content_db, state_db, llm, ts(11, 22))

    assert q(content_db, "SELECT count(*) FROM clusters") == [(2,)]
    assert q(content_db, "SELECT DISTINCT cluster_key FROM scores") == [("c-" + ncs.headline_id_for("https://a.com/new"),)]


def test_items_after_now_and_tokenless_titles_wait(content_db, state_db):
    add_item(content_db, "https://a.com/1", title(1), ts(11, 10))
    add_item(content_db, "https://a.com/2", title(2), NIGHT1 + timedelta(seconds=1))
    add_item(content_db, "https://a.com/3", " -- ", ts(11, 10))
    out = run(content_db, state_db, FakeLLM())
    assert out["clustered_items"] == 1


def test_null_published_at_falls_back_to_capture_time(content_db, state_db):
    add_item(content_db, "https://a.com/1", title(1), ts(11, 10), published=None)
    llm = FakeLLM()
    run(content_db, state_db, llm)
    assert "| first seen 2026-11-11 |" in prompts(llm, HAIKU)[0]
    assert q(content_db, "SELECT first_seen FROM clusters") == [(ts(11, 10).isoformat(),)]


def test_a_failed_call_is_retried_and_an_omitted_cluster_is_final(content_db, state_db):
    for i in range(3):
        add_item(content_db, f"https://a.com/{i}", title(i), ts(11, 10, i))
    key0 = "c-" + ncs.headline_id_for("https://a.com/0")

    def scorer(cid, prompt):
        if cid == key0:
            return None                                            # the model leaves one cluster out
        return score(cid)

    llm = FakeLLM(fail_at=frozenset({2}), scorer=scorer)           # the C call fails outright
    out = run(content_db, state_db, llm)
    assert out["stopped"] is None
    assert out["scored"] == {"H": 2, "C": 0} and out["scores_omitted"] == {"H": 1, "C": 0}
    h_call = q(content_db, "SELECT call_id FROM llm_calls WHERE purpose='score_H'")[0][0]
    assert q(content_db, "SELECT cluster_key, scope, event_type, sentiment, materiality, novelty, call_id, omitted "
                         "FROM scores WHERE omitted=1") == [(key0, None, None, None, None, None, h_call, 1)]
    assert q(content_db, "SELECT variant, omitted, count(*) FROM scores GROUP BY 1, 2 ORDER BY 1, 2") == [
        ("H", 0, 2), ("H", 1, 1)]
    assert q(content_db, "SELECT purpose, ok, error FROM llm_calls WHERE ok=0") == [
        ("score_C", 0, "timeout: no complete response")]

    llm2 = FakeLLM(at=NIGHT2)
    out2 = run(content_db, state_db, llm2, NIGHT2)
    assert q(content_db, "SELECT variant, count(*) FROM scores GROUP BY 1 ORDER BY 1") == [("C", 3), ("H", 3)]
    (c2,) = prompts(llm2, HAIKU)                                   # the omitted H is never re-sent; C retried
    assert len(_IDS.findall(c2)) == 3 and out2["scored"] == {"H": 0, "C": 3}
    assert q(content_db, "SELECT omitted FROM scores WHERE cluster_key=? AND variant='H'", key0) == [(1,)]


@pytest.mark.parametrize("garbled", [{"not_scores": []}, {"scores": "none"}, {"scores": None}])
def test_an_unparsed_score_output_writes_no_rows_and_is_retried(content_db, state_db, garbled):
    add_item(content_db, "https://a.com/1", title(1), ts(11, 10))

    class Garbled(FakeLLM):
        async def call(self, **kw):
            res = await super().call(**kw)
            if kw["model"] == HAIKU:
                res.output = garbled
            return res

    out = run(content_db, state_db, Garbled())
    assert out["unparsed_outputs"] == 2 and out["scores_omitted"] == {"H": 0, "C": 0}
    assert q(content_db, "SELECT count(*) FROM scores") == [(0,)]
    assert q(content_db, "SELECT purpose, ok FROM llm_calls ORDER BY rowid") == [("score_H", 1), ("score_C", 1)]

    llm2 = FakeLLM(at=NIGHT2)
    run(content_db, state_db, llm2, NIGHT2)
    assert len(prompts(llm2, HAIKU)) == 2
    assert q(content_db, "SELECT variant, omitted FROM scores ORDER BY 1") == [("C", 0), ("H", 0)]


def test_unknown_duplicate_and_invalid_score_ids_are_ignored(content_db, state_db):
    add_item(content_db, "https://a.com/1", title(1), ts(11, 10))
    key = "c-" + ncs.headline_id_for("https://a.com/1")

    class Odd(FakeLLM):
        async def call(self, **kw):
            res = await super().call(**kw)
            if kw["model"] == HAIKU:
                res.output = {"scores": [score("c-ghost"), score(key, sentiment=5.0), score(key, sentiment=-0.2),
                                         score(key, sentiment=0.9)]}
            return res

    run(content_db, state_db, Odd())
    assert q(content_db, "SELECT cluster_key, variant, sentiment FROM scores ORDER BY variant") == [
        (key, "C", -0.2), (key, "H", -0.2)]


def test_llm_calls_rows_carry_effort_hash_and_prompt(content_db, state_db):
    add_item(content_db, "https://a.com/1", title(1), ts(11, 10))
    llm = FakeLLM()
    run(content_db, state_db, llm)

    rows = q(content_db, "SELECT purpose, model, prompt_sha256, ok, error, effort, prompt_gz, at, system_sha256, cli "
                         "FROM llm_calls ORDER BY rowid")
    assert [r[0] for r in rows] == ["score_H", "score_C", "map_content", "map_headline"]
    for (_purpose, model, sha, ok, error, effort, blob, at, system_sha, cli), call in zip(rows, llm.calls,
                                                                                          strict=True):
        text = call["system_prompt"] + "\n" + call["prompt"]
        assert sha == hashlib.sha256(text.encode("utf-8")).hexdigest()
        assert gzip.decompress(blob).decode("utf-8") == text
        assert system_sha == hashlib.sha256(call["system_prompt"].encode("utf-8")).hexdigest()
        assert model == call["model"] and ok == 1 and error is None and effort == "default"
        assert at == NIGHT1.isoformat() and cli == "unknown"                    # FakeLLM has no cli_path
    mapper_sha = hashlib.sha256(ncs.MAPPER_PROMPT_PATH.read_bytes()).hexdigest()
    assert [r[8] for r in rows[2:]] == [mapper_sha, mapper_sha]                 # the spec's per-call mapper hash
    assert [c["model"] for c in llm.calls] == [HAIKU, HAIKU, SONNET, SONNET]


def test_llm_calls_log_the_study_llms_cli_path(content_db, state_db, agents_cfg):
    add_item(content_db, "https://a.com/1", title(1), ts(11, 10))
    llm = mml.StudyLLM(max_calls=1, query_fn=Responder(), options_cls=FakeOptions, agents_cfg=agents_cfg)
    run(content_db, state_db, llm)
    assert q(content_db, "SELECT cli FROM llm_calls") == [(agents_cfg["llm"]["cli_path"],)]


# --------------------------------------------------------------------------- candidates and priority
def test_priority_scores_then_headlines_then_content_then_study_h(content_db, state_db):
    add_item(content_db, "https://a.com/1", title(1), ts(11, 10), summary="Study summary.", body="Study body.")
    add_call(state_db, "old", ts(7, 20, month=8), [("c-OLD", "Backlog scheme")], [score("c-OLD")])
    add_call(state_db, "pre", ts(9, 8, 59, 59), [("c-PRE", "Just before the start")], [score("c-PRE")])
    add_call(state_db, "edge", ts(9, 9, 0), [("c-EDGE", "Forward from the start")], [score("c-EDGE")])
    add_call(state_db, "fwd2", ts(11, 8), [("c-F2", "Forward two")], [score("c-F2")])
    add_call(state_db, "fwd1", ts(10, 9, 0), [("c-F1", "Forward one")], [score("c-F1")])
    # First scored before the start, re-scored after it: its first score decides, so it is never a candidate.
    add_call(state_db, "first", ts(9, 8), [("c-RE", "Re-scored story")], [score("c-RE", event_type="other")])
    add_call(state_db, "again", ts(11, 9), [("c-RE", "Re-scored story")], [score("c-RE")])
    add_call(state_db, "late", NIGHT1 + timedelta(seconds=1), [("c-LATE", "After now")], [score("c-LATE")])
    llm = FakeLLM(max_calls=5)

    out = run(content_db, state_db, llm)

    assert prompts(llm, SONNET) == ["headline: Forward from the start", "headline: Forward one",
                                    "headline: Forward two"]
    assert out["stopped"].startswith("CallCapReached")
    assert out["headline_candidates"] == 3 and out["mapped"] == {"headline": 3, "content": 0, "study_h": 0}

    llm2 = FakeLLM(at=NIGHT2)
    out2 = run(content_db, state_db, llm2, NIGHT2)
    # c-LATE was scored after night 1's `now`, so it is new work on night 2 and goes first.
    assert prompts(llm2, SONNET) == ["headline: After now",
                                     f"headline: {title(1)}\nsummary: Study summary.\nexcerpt: Study body.",
                                     f"headline: {title(1)}"]
    assert out2["mapped"] == {"headline": 1, "content": 1, "study_h": 1}
    keys = {r[0] for r in q(content_db, "SELECT candidate_key FROM mappings WHERE source='headline'")}
    assert keys == {"c-EDGE", "c-F1", "c-F2", "c-LATE"}                     # nothing first scored before the start


def test_policy_bars_gate_study_candidates(content_db, state_db):
    add_item(content_db, "https://a.com/1", title(1), ts(11, 10))
    add_item(content_db, "https://a.com/2", title(2), ts(11, 10, 1))
    key1 = "c-" + ncs.headline_id_for("https://a.com/1")

    def scorer(cid, prompt):
        content = "   excerpt: " in prompt or "   summary: " in prompt
        if cid == key1:
            return score(cid) if content else score(cid, materiality=0.59)
        return score(cid, event_type="regulatory_policy")

    add_item(content_db, "https://a.com/3", title(3), ts(11, 10, 2), summary="S.")   # makes the C prompt distinct
    llm = FakeLLM(scorer=scorer)
    run(content_db, state_db, llm)

    assert q(content_db, "SELECT candidate_key, source FROM mappings") == [(key1, "content")]
