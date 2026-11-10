"""``scripts/news_impact_map.py``: the capped nightly mapper of the news impact study.

A fake ``query_fn`` and options class stand in for the SDK, and the clock is injected: no test makes a real LLM call
or reads ``data/``. The store is a SQLite file in ``tmp_path``.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import importlib.util
import json
import sqlite3
import sys
from dataclasses import dataclass, field
from datetime import date, datetime
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


nim = _load("mt_news_impact_map", "news_impact_map.py")
nic = nim.nic
mml = nim.mml

from engine.core.config import config_dir, load_yaml  # noqa: E402

IST = ZoneInfo("Asia/Kolkata")
NIGHT = datetime(2026, 11, 4, 22, 0, tzinfo=IST)      # a Wednesday, outside the session
MIDDAY = datetime(2026, 11, 4, 10, 0, tzinfo=IST)
SESSION_LIMIT = "You've hit your session limit · resets 3pm (Asia/Calcutta)"


@dataclass
class FakeOptions:
    """The SDK's ClaudeAgentOptions knobs the harness probes for (no SDK import)."""

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
    def __init__(self, structured_output: Any = None, *, is_error: bool = False, result: str | None = None) -> None:
        self.structured_output = structured_output
        self.is_error = is_error
        self.result = result
        self.subtype = "success"
        self.errors: list[str] = []
        self.total_cost_usd = 0.001
        self.usage = {"input_tokens": 10, "output_tokens": 5}


class FakeQuery:
    """Injected ``query``: one script per call, the last repeating. ``answer`` builds a script from a payload."""

    def __init__(self, *scripts: Any) -> None:
        self.scripts = list(scripts)
        self.calls: list[tuple[str, Any]] = []

    def __call__(self, *, prompt: str, options: Any) -> Any:
        self.calls.append((prompt, options))
        script = self.scripts[min(len(self.calls) - 1, len(self.scripts) - 1)]
        if callable(script):
            script = script(prompt)
        return self._stream(script)

    async def _stream(self, script: list[Any]) -> Any:
        for item in script:
            yield item


def answer(payload: dict[str, Any]) -> list[Any]:
    return [FakeResult(payload)]


def echo_all(prompt: str) -> list[Any]:
    """Maps every story of the prompt: market 1, no groups, no companies."""
    ids = [line.split(" | ", 1)[0] for line in prompt.splitlines()[1:]]
    return answer({"stories": [{"id": i, "market": 1, "groups": [], "companies": []} for i in ids]})


@pytest.fixture(autouse=True)
def _no_api_key(monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)


@pytest.fixture
def agents_cfg(tmp_path) -> dict[str, Any]:
    cli = tmp_path / "claude.exe"
    cli.write_bytes(b"")
    cfg = copy.deepcopy(load_yaml(config_dir() / "agents.yaml"))
    cfg.setdefault("llm", {})["cli_path"] = str(cli)
    return cfg


def make_llm(agents_cfg, fake, max_calls: int = 40):
    return mml.StudyLLM(max_calls=max_calls, query_fn=fake, options_cls=FakeOptions, agents_cfg=agents_cfg)


def story(cid: str, *, scope: str = "sector", event: str = "sector_policy", headline: str | None = "A headline",
          at: datetime = datetime(2026, 8, 3, 12, 0, tzinfo=IST), materiality: float = 0.8):
    return nic.Story(cid, at, "call-" + cid, "m", scope, event, 0.5, materiality, (), headline)


def run(db, todo, llm, *, now=NIGHT, clock=None):
    return asyncio.run(nim.run_night(db, todo, llm, now, "SYSTEM PROMPT", clock=clock or (lambda: now)))


def rows(db, sql):
    con = sqlite3.connect(db)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


# --------------------------------------------------------------------------- the pinned prompt and the schema
def test_prompt_hash_pinned():
    data = nim.PROMPT_PATH.read_bytes()
    assert hashlib.sha256(data).hexdigest() == nim.MAPPER_PROMPT_SHA256
    assert nim.PROMPT_PATH == _ROOT / "config" / "research" / "news_impact" / "mapper_prompt.md"
    item = nim.MAPPER_SCHEMA["properties"]["stories"]["items"]
    assert item["properties"]["groups"]["items"]["properties"]["name"]["enum"] == list(nim.ncs.VOCAB)
    assert len(nim.ncs.VOCAB) == 30
    assert "maxLength" not in json.dumps(nim.MAPPER_SCHEMA) and "maxItems" not in json.dumps(nim.MAPPER_SCHEMA)


def test_main_exits_2_on_prompt_hash_mismatch(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(nim, "MAPPER_PROMPT_SHA256", "0" * 64)
    assert nim.main(["--sample", "A", "--db", str(tmp_path / "x.sqlite"), "--dry-run"]) == 2
    assert "prompt" in capsys.readouterr().out
    assert not (tmp_path / "x.sqlite").exists()


# --------------------------------------------------------------------------- the parser
def test_parse_mapping_trims():
    item = {
        "id": "c1", "market": -1,
        "groups": [
            {"name": "AUTO", "direction": 1},
            {"name": "NOT_A_GROUP", "direction": 1},        # out of vocabulary
            {"name": "BANK", "direction": 2},               # bad direction
            {"name": "defence", "direction": -1},
            {"name": "IT", "direction": 1},
            {"name": "METAL", "direction": 1},              # a fourth valid group
        ],
        "companies": [{"name": f"Co{i}", "direction": 1} for i in range(6)] + [{"name": "", "direction": 1}],
    }
    market, groups, companies, dropped = nim.parse_mapping(item)
    assert market == -1
    assert groups == [("AUTO", 1), ("defence", -1), ("IT", 1)]
    assert companies == [(f"Co{i}", 1) for i in range(5)]
    assert dropped == 2                                     # the out-of-vocabulary group and the direction of 2


def test_parse_mapping_drops_bad_items_and_coerces_market():
    item = {"id": "c1", "market": 5,
            "groups": [{"name": "AUTO", "direction": 0}, "AUTO", {"name": 7, "direction": 1}],
            "companies": [{"name": "  ", "direction": 1}, {"name": "Acme", "direction": 0},
                          {"name": "Acme", "direction": True}, {"name": "Beta", "direction": -1}, None]}
    market, groups, companies, dropped = nim.parse_mapping(item)
    assert market == 0 and groups == [] and companies == [("Beta", -1)]
    assert dropped == 8        # market 1, three bad groups, four bad companies (a bool is not a direction)


def test_parse_mapping_tolerates_missing_lists_and_rejects_non_items():
    assert nim.parse_mapping({"id": "c1", "market": 1}) == (1, [], [], 0)
    assert nim.parse_mapping({"id": "c1", "market": "up", "groups": "x", "companies": None})[0] == 0
    for bad in (None, [], "c1", {"market": 1}, {"id": 5, "market": 1}):
        assert nim.parse_mapping(bad) is None


# --------------------------------------------------------------------------- what to map
def test_stories_to_map_window():
    cal = nic.load_calendar()
    lo, hi = nic.SAMPLES["A"]
    sessions = nic.regular_sessions(cal, date(2026, 7, 1), date(2026, 12, 31))
    wed = datetime(2026, 8, 5, 12, 0, tzinfo=IST)
    stories = [
        story("dom", at=wed),
        story("glob", scope="market", event="global_market", at=wed),
        story("mkt_domain", scope="market", event="govt_program", at=wed),
        story("stock", scope="stock", event="results", at=wed),                     # stock kind: never mapped
        story("weak", materiality=0.59, at=wed),                                    # below the material bar
        story("pump", event="pump_promo_suspect", at=wed),
        story("early", at=datetime(2026, 7, 28, 22, 0, tzinfo=IST)),                # enters 07-29 = first day
        story("before", at=datetime(2026, 7, 28, 9, 0, tzinfo=IST)),                # enters 07-28, before A
        story("last_in", at=datetime(2026, 10, 23, 9, 14, 59, tzinfo=IST)),         # enters 10-23, A's last day
        story("after", at=datetime(2026, 10, 23, 9, 15, tzinfo=IST)),               # enters 10-26: excluded window
    ]
    got = nim.stories_to_map(stories, sessions, "A")
    assert [s.cluster_id for s in got] == ["early", "dom", "glob", "mkt_domain", "last_in"]   # oldest first
    assert [s.cluster_id for s in nim.stories_to_map(stories, sessions, "excluded")] == ["after"]
    assert lo <= nic.entry_session(got[0].at, sessions) <= hi
    with pytest.raises(KeyError):
        nim.stories_to_map(stories, sessions, "Z")


# --------------------------------------------------------------------------- the night
def test_market_hours_refusal_stops_before_call(agents_cfg, tmp_path):
    fake = FakeQuery(echo_all)
    db = tmp_path / "map.sqlite"
    out = run(db, [story("c1")], make_llm(agents_cfg, fake), now=MIDDAY)
    assert fake.calls == []
    assert out["skipped"] and "market hours" in out["skipped"]
    assert not db.exists()


def test_market_hours_rechecked_before_each_call(agents_cfg, tmp_path):
    """A run suspended past the open must not call: the clock is read again before every call."""
    fake = FakeQuery(echo_all)
    ticks = iter([NIGHT, MIDDAY, MIDDAY])
    db = tmp_path / "map.sqlite"
    todo = [story(f"c{i:02d}") for i in range(45)]
    out = run(db, todo, make_llm(agents_cfg, fake), clock=lambda: next(ticks))
    assert len(fake.calls) == 1
    assert "MarketHoursHalt" in out["stopped"]
    assert out["mapped"] == 20


def test_cap_and_stop(agents_cfg, tmp_path):
    db = tmp_path / "map.sqlite"
    fake = FakeQuery([FakeResult(None, is_error=True, result=SESSION_LIMIT)], echo_all)
    todo = [story(f"c{i:02d}") for i in range(45)]
    out = run(db, todo, make_llm(agents_cfg, fake), now=NIGHT)
    assert len(fake.calls) == 1                                    # the first stop error ends the run
    assert out["stopped"] and "overloaded" in out["stopped"]
    assert rows(db, "SELECT ok, error FROM llm_calls")[0][0] == 0
    assert rows(db, "SELECT count(*) FROM mappings") == [(0,)]
    # the call is stamped with the real clock (StudyLLM's own), so the budget is read at the real date
    left, stop = nim.night_budget(db, datetime.now(IST))
    assert left == 39 and stop.startswith(nim.STOP_PREFIXES)

    # a spent budget: 40 calls logged today leave nothing, and the next day is fresh
    full = tmp_path / "full.sqlite"
    con = nim.open_store(full)
    with con:
        for _ in range(40):
            con.execute("INSERT INTO llm_calls (at, model, prompt_sha256, cluster_ids, ok) VALUES (?,?,?,?,1)",
                        (NIGHT.isoformat(), nim.MAPPER_MODEL, "h", "[]"))
    con.close()
    assert nim.night_budget(full, NIGHT) == (0, None)
    assert nim.night_budget(full, NIGHT, 10) == (0, None)
    assert nim.night_budget(full, datetime(2026, 11, 5, 22, 0, tzinfo=IST)) == (40, None)
    fake2 = FakeQuery(echo_all)
    out2 = run(full, todo, make_llm(agents_cfg, fake2, max_calls=0))
    assert fake2.calls == [] and "CallCapReached" in out2["stopped"]


def test_budget_counts_ist_date_and_missing_store(tmp_path):
    assert nim.night_budget(tmp_path / "none.sqlite", NIGHT) == (40, None)
    db = tmp_path / "map.sqlite"
    con = nim.open_store(db)
    with con:
        # 00:30 IST on 11-04 is 11-03 UTC: counted against the IST date
        con.execute("INSERT INTO llm_calls (at, model, prompt_sha256, cluster_ids, ok) VALUES (?,?,?,?,1)",
                    (datetime(2026, 11, 4, 0, 30, tzinfo=IST).isoformat(), "m", "h", "[]"))
        con.execute("INSERT INTO llm_calls (at, model, prompt_sha256, cluster_ids, ok) VALUES (?,?,?,?,1)",
                    (datetime(2026, 11, 3, 23, 59, tzinfo=IST).isoformat(), "m", "h", "[]"))
    con.close()
    assert nim.night_budget(db, NIGHT) == (39, None)


def test_store_append_only(agents_cfg, tmp_path):
    db = tmp_path / "map.sqlite"
    todo = [story(f"c{i:02d}") for i in range(25)]
    fake = FakeQuery(echo_all)
    out = run(db, todo[:20], make_llm(agents_cfg, fake))
    assert out["mapped"] == 20 and out["calls"] == 1
    first = rows(db, "SELECT cluster_id, call_rowid, market FROM mappings ORDER BY cluster_id")

    # second run: the caller filters out mapped stories, a model that answers a mapped id again changes nothing
    again = [story("c00"), *todo[20:]]
    answer_old = lambda p: answer({"stories": [                                   # noqa: E731
        {"id": "c00", "market": -1, "groups": [], "companies": []},
        *[{"id": s.cluster_id, "market": 0, "groups": [], "companies": []} for s in todo[20:]]]})
    out2 = run(db, again, make_llm(agents_cfg, FakeQuery(answer_old)))
    assert out2["mapped"] == 5 and out2["already_mapped"] == 1
    after = rows(db, "SELECT cluster_id, call_rowid, market FROM mappings ORDER BY cluster_id")
    assert after[:20] == first
    assert len(after) == 25
    assert rows(db, "SELECT market FROM mappings WHERE cluster_id = 'c00'") == [(1,)]
    assert rows(db, "SELECT count(*) FROM llm_calls") == [(2,)]
    assert set(nim.read_mappings(db)) == {s.cluster_id for s in todo}


def test_unknown_id_ignored_and_omitted_story_stays_unmapped(agents_cfg, tmp_path):
    db = tmp_path / "map.sqlite"
    todo = [story("a"), story("b"), story("c")]
    payload = {"stories": [
        {"id": "a", "market": 1, "groups": [{"name": "AUTO", "direction": 1}], "companies": [
            {"name": "Acme Ltd", "direction": 1}]},
        {"id": "zzz", "market": 1, "groups": [], "companies": []},                 # not in the batch
        {"id": "c", "market": 3, "groups": [], "companies": []},
        "junk",
    ]}
    out = run(db, todo, make_llm(agents_cfg, FakeQuery(answer(payload))))
    assert out["mapped"] == 2 and out["unknown_ids"] == 1 and out["malformed_items"] == 1
    assert out["omitted"] == 1 and out["dropped"] == 1                              # c's market coerced
    got = nim.read_mappings(db)
    assert set(got) == {"a", "c"}                                                  # b stays unmapped
    assert got["a"] == (1, [["AUTO", 1]], [["Acme Ltd", 1]])
    assert got["c"][0] == 0
    # the next night maps only b
    fake = FakeQuery(echo_all)
    run(db, [s for s in todo if s.cluster_id not in got], make_llm(agents_cfg, fake))
    assert len(fake.calls) == 1 and "b | " in fake.calls[0][0] and "a | " not in fake.calls[0][0]


def test_unparsed_answer_writes_nothing_and_continues(agents_cfg, tmp_path):
    db = tmp_path / "map.sqlite"
    todo = [story(f"c{i:02d}") for i in range(25)]
    fake = FakeQuery(answer({"stories": "oops"}), echo_all)
    out = run(db, todo, make_llm(agents_cfg, fake))
    assert len(fake.calls) == 2 and out["unparsed_outputs"] == 1 and out["mapped"] == 5
    assert rows(db, "SELECT ok FROM llm_calls ORDER BY rowid") == [(1,), (1,)]


def test_batches_and_prompt(agents_cfg, tmp_path):
    db = tmp_path / "map.sqlite"
    todo = [story(f"c{i:02d}", headline=f"Headline {i}") for i in range(45)]
    todo.insert(3, story("nohead", headline=None))
    todo.insert(5, story("blank", headline="  "))
    fake = FakeQuery(echo_all)
    out = run(db, todo, make_llm(agents_cfg, fake))
    assert [len(p.splitlines()) - 1 for p, _ in fake.calls] == [20, 20, 5]
    assert out["no_headline"] == 2 and out["mapped"] == 45 and out["calls"] == 3
    prompt, options = fake.calls[0]
    assert "c00 | Headline 0\n" in prompt and "nohead" not in prompt and "blank" not in prompt
    assert "cluster_id" in prompt.splitlines()[0]
    assert options.system_prompt == "SYSTEM PROMPT" and options.model == nim.MAPPER_MODEL
    assert options.output_format["schema"] == nim.MAPPER_SCHEMA
    logged = rows(db, "SELECT model, prompt_sha256, cluster_ids, ok, error, raw_output FROM llm_calls ORDER BY rowid")
    assert len(logged) == 3 and json.loads(logged[2][2]) == [f"c{i:02d}" for i in range(40, 45)]
    assert logged[0][0] == nim.MAPPER_MODEL and logged[0][3] == 1 and logged[0][4] is None
    assert logged[0][1] == hashlib.sha256(b"SYSTEM PROMPT").hexdigest()
    assert json.loads(logged[0][5])["stories"][0]["id"] == "c00"


def test_user_prompt_collapses_headline_whitespace():
    text = nim.user_prompt([story("c1", headline="Line one\nline | two\t end")])
    assert text.splitlines()[-1] == "c1 | Line one line | two end"


# --------------------------------------------------------------------------- the CLI
def test_dry_run_reports_without_calling(monkeypatch, tmp_path, capsys):
    db = tmp_path / "map.sqlite"
    wed = datetime(2026, 8, 5, 12, 0, tzinfo=IST)
    stories = [story("d1", at=wed), story("d2", at=wed), story("g1", scope="market", event="global_market", at=wed),
               story("nh", headline=None, at=wed), story("st", scope="stock", event="results", at=wed)]
    seen = {}

    def fake_load(state_db, limit):
        seen["limit"] = limit
        return stories, {"stories": len(stories)}

    monkeypatch.setattr(nim.nic, "load_first_scores", fake_load)
    monkeypatch.setattr(nim.mml, "StudyLLM", lambda **kw: pytest.fail("a dry run must not build the LLM"))
    con = nim.open_store(db)
    with con:
        cur = con.execute("INSERT INTO llm_calls (at, model, prompt_sha256, cluster_ids, ok) VALUES (?,?,?,?,1)",
                          (datetime.now(IST).isoformat(), "m", "h", "[]"))
        con.execute("INSERT INTO mappings VALUES ('d1', ?, 1, '[]', '[]')", (cur.lastrowid,))
    con.close()
    assert nim.main(["--sample", "A", "--dry-run", "--db", str(db), "--state-db", str(tmp_path / "state.db")]) == 0
    out = capsys.readouterr().out
    assert seen["limit"].tzinfo is not None
    assert "domain: mapped 1, unmapped 2 (1 without a headline), batches needed 1" in out
    assert "global: mapped 0, unmapped 1 (0 without a headline), batches needed 1" in out
    assert "calls left tonight: 39" in out
    assert rows(db, "SELECT count(*) FROM llm_calls") == [(1,)]


def test_main_rejects_max_calls_above_ceiling(capsys):
    with pytest.raises(SystemExit):
        nim.main(["--sample", "A", "--max-calls", "41"])
