"""News impact study (``edge/news_impact``): the capped nightly mapper of domain, global and orphan news stories.

For each story first scored by the engine's News Analyst whose kind is ``domain`` or ``global``, or which is an
orphan (a material stock story the resolver reads no symbol in: ``news_impact_common.orphan_ids``), and whose entry
session falls in the chosen sample window, the pinned Sonnet mapper reads the headline alone, never a score, and
returns the direction for the whole Indian market, up to 3 stock groups from a closed vocabulary and up to 5 listed
companies, each with a direction. A batch is up to 20 stories in one call.

Writes only ``data/research/news_impact.sqlite``; ``state.db`` is read-only. Every call goes through ``StudyLLM`` (the
night's cap, no retries, a permanent stop on a quota or login failure) and is logged append-only in ``llm_calls``
with the system prompt's hash. A story has one ``mappings`` row, written once and never replaced; a story the model
leaves out stays unmapped for the next night. No call is made in market hours, checked again before each call,
because a run suspended by host standby can resume inside the session. No call is made once the prompt file differs
from its pinned hash.

Orphans are decided by the resolver over ``--market-db`` (a whole-file ``market.duckdb`` copy, opened read-only and
used for nothing else) with the frozen alias supplement; no call is made while the supplement is missing or differs
from its registered digest.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib.util
import json
import sqlite3
import sys
from collections.abc import Callable, Collection, Mapping, Sequence
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

_ROOT = Path(__file__).resolve().parents[1]
_REPO_SRC = str(_ROOT / "src")
if _REPO_SRC not in sys.path:  # pragma: no cover - loose-script shim
    sys.path.insert(0, _REPO_SRC)

import engine  # noqa: E402,F401,I001  native import-order guard (engine._preload), keep FIRST

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


mml = _load_sibling("macro_mood_llm")
ncs = _load_sibling("news_content_score")
nic = _load_sibling("news_impact_common")

IST = ZoneInfo("Asia/Kolkata")
DEFAULT_DB = _ROOT / "data" / "research" / "news_impact.sqlite"
DEFAULT_STATE_DB = _ROOT / "data" / "state.db"
PROMPT_PATH = _ROOT / "config" / "research" / "news_impact" / "mapper_prompt.md"

MAPPER_MODEL = "claude-sonnet-5-5"
BATCH = 20
MAX_CALLS_PER_NIGHT = 40
MAX_GROUPS = 3
MAX_COMPANIES = 5
#: The prompt file's sha256, registered with the study: an edit would silently change the instrument mid-study.
MAPPER_PROMPT_SHA256 = "200875da005040a3f659f7b1d4b65ce920d64c3f845bb1c6c62a88cb3a70b8c2"
STOP_PREFIXES = ncs.STOP_PREFIXES
MarketHoursHalt = ncs.MarketHoursHalt
KINDS = nic.MAPPED_KINDS

_DIRECTION = {"type": "integer", "enum": [-1, 1]}
MAPPER_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {"stories": {"type": "array", "items": {
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "market": {"type": "integer", "enum": [-1, 0, 1]},
            "groups": {"type": "array", "items": {
                "type": "object",
                "properties": {"name": {"type": "string", "enum": list(ncs.VOCAB)}, "direction": _DIRECTION},
                "required": ["name", "direction"], "additionalProperties": False}},
            "companies": {"type": "array", "items": {
                "type": "object",
                "properties": {"name": {"type": "string"}, "direction": _DIRECTION},
                "required": ["name", "direction"], "additionalProperties": False}},
        },
        "required": ["id", "market", "groups", "companies"], "additionalProperties": False}}},
    "required": ["stories"], "additionalProperties": False,
}

_SCHEMA = """
CREATE TABLE IF NOT EXISTS llm_calls(
    at TEXT NOT NULL, model TEXT NOT NULL, prompt_sha256 TEXT NOT NULL, cluster_ids TEXT NOT NULL,
    ok INTEGER NOT NULL, error TEXT, raw_output TEXT);
CREATE TABLE IF NOT EXISTS mappings(
    cluster_id TEXT PRIMARY KEY, call_rowid INTEGER NOT NULL, market INTEGER NOT NULL,
    groups TEXT NOT NULL, companies TEXT NOT NULL);
"""


# --------------------------------------------------------------------------- the parser
def _is_direction(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value in (-1, 1)


def parse_mapping(item: Any) -> tuple[int, list[tuple[str, int]], list[tuple[str, int]], int] | None:
    """``(market, groups, companies, dropped)`` from one answer item, never raising on a malformed one.

    A schema is not a guarantee, so the answer is re-checked: the first 3 groups whose name is in the vocabulary and
    whose direction is -1 or +1, the first 5 companies with a non-empty name and such a direction. A market value
    outside {-1, 0, +1} becomes 0. ``dropped`` counts the invalid items met before a cap was reached, plus a coerced
    market; a valid item beyond a cap is trimmed, not counted. None when ``item`` is not a dict with a string id."""
    if not isinstance(item, dict) or not isinstance(item.get("id"), str):
        return None
    dropped = 0
    market = item.get("market")
    if not (isinstance(market, int) and not isinstance(market, bool) and market in (-1, 0, 1)):
        market, dropped = 0, 1
    vocab = frozenset(ncs.VOCAB)
    groups: list[tuple[str, int]] = []
    raw_groups = item.get("groups")
    for g in raw_groups if isinstance(raw_groups, list) else []:
        if len(groups) == MAX_GROUPS:
            break
        if isinstance(g, dict) and g.get("name") in vocab and _is_direction(g.get("direction")):
            groups.append((g["name"], g["direction"]))
        else:
            dropped += 1
    companies: list[tuple[str, int]] = []
    raw_companies = item.get("companies")
    for c in raw_companies if isinstance(raw_companies, list) else []:
        if len(companies) == MAX_COMPANIES:
            break
        name = c.get("name") if isinstance(c, dict) else None
        if isinstance(name, str) and name.strip() and _is_direction(c.get("direction")):
            companies.append((name, c["direction"]))
        else:
            dropped += 1
    return market, groups, companies, dropped


# --------------------------------------------------------------------------- what to map
def stories_to_map(stories: Sequence[Any], sessions: Sequence[date], sample: str,
                   orphans: Collection[str]) -> list[Any]:
    """Domain, global and orphan stories (``orphans``, from ``nic.orphan_ids``) whose entry session lies in the
    sample's inclusive window, oldest first. ``sessions`` must start before the window's first day; a story whose
    entry session lies past their end is not placed, so it is left out."""
    lo, hi = nic.SAMPLES[sample]
    keep = []
    for s in stories:
        if nic.map_kind(s, orphans) not in KINDS:
            continue
        d = nic.entry_session(s.at, sessions)
        if d is not None and lo <= d <= hi:
            keep.append(s)
    return sorted(keep, key=lambda s: (s.at, s.cluster_id))


def _headline(s: Any) -> str | None:
    return ncs._clean(s.headline)


def user_prompt(batch: Sequence[Any]) -> str:
    """One line per story, ``<cluster_id> | <headline>``; whitespace in a headline is collapsed so it cannot forge a
    line."""
    header = ("Map each of the stories below. Every line is `<cluster_id> | <headline>`. "
              "Each returned `id` must be exactly the given cluster_id.")
    return "\n".join([header, *(f"{s.cluster_id} | {_headline(s)}" for s in batch)])


def split_batches(todo: Sequence[Any]) -> tuple[list[list[Any]], int]:
    """``(batches of up to BATCH stories with a headline, the number without one)``, in the given order."""
    sendable = [s for s in todo if _headline(s) is not None]
    return [sendable[i:i + BATCH] for i in range(0, len(sendable), BATCH)], len(todo) - len(sendable)


# --------------------------------------------------------------------------- the store
def open_store(db: Path) -> sqlite3.Connection:
    db = Path(db)
    db.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db, timeout=30)
    con.executescript(_SCHEMA)
    return con


def _open_ro(db: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"{Path(db).resolve().as_uri()}?mode=ro", uri=True)


def _has_table(con: sqlite3.Connection, name: str) -> bool:
    return con.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (name,)).fetchone() is not None


def read_mappings(db: Path) -> dict[str, tuple[int, list, list]]:
    """``cluster_id -> (market, groups, companies)``; empty when the store does not exist yet."""
    if not Path(db).is_file():
        return {}
    con = _open_ro(db)
    try:
        if not _has_table(con, "mappings"):
            return {}
        return {cid: (market, json.loads(groups), json.loads(companies))
                for cid, market, groups, companies in con.execute(
                    "SELECT cluster_id, market, groups, companies FROM mappings")}
    finally:
        con.close()


def night_start(now: datetime) -> datetime:
    """The most recent 15:30 IST at or before ``now``: a mapping night starts at the close and runs past midnight,
    so a calendar-date count would give a night that crosses midnight two caps."""
    local = now.astimezone(IST)
    start = datetime.combine(local.date(), nic.CLOSE_TIME, IST)
    return start if start <= local else start - timedelta(days=1)


def night_budget(db: Path, now: datetime, max_calls: int = MAX_CALLS_PER_NIGHT) -> tuple[int, str | None]:
    """``(calls left this night, the first stopping error logged this night)``; the night is every call stamped at
    or after ``night_start(now)``."""
    if now.tzinfo is None:
        raise ValueError("now must be tz-aware")
    if not Path(db).is_file():
        return max_calls, None
    con = _open_ro(db)
    try:
        if not _has_table(con, "llm_calls"):
            return max_calls, None
        rows = con.execute("SELECT at, error FROM llm_calls ORDER BY rowid").fetchall()
    finally:
        con.close()
    start = night_start(now)
    used, stop = 0, None
    for raw_at, error in rows:
        at = ncs._parse_ts(raw_at)
        if at is None or at < start:
            continue
        used += 1
        if stop is None and isinstance(error, str) and error.startswith(STOP_PREFIXES):
            stop = error
    return max(0, max_calls - used), stop


# --------------------------------------------------------------------------- the night
def _summary() -> dict[str, Any]:
    return {"skipped": None, "stopped": None, "calls": 0, "mapped": 0, "already_mapped": 0, "omitted": 0,
            "unknown_ids": 0, "malformed_items": 0, "dropped": 0, "unparsed_outputs": 0, "no_headline": 0,
            "mapper_prompt_sha256": None}


async def run_night(db: Path, todo: Sequence[Any], llm: Any, now: datetime, system_prompt: str, *,
                    clock: Callable[[], datetime] | None = None) -> dict[str, Any]:
    """Map ``todo`` (unmapped stories, oldest first) in batches until it is done, the cap is spent, a quota or login
    failure stops the account, or the clock reads market hours. Each of those ends the night cleanly, recorded in the
    summary's ``stopped`` or ``skipped``; a setup fault (an unknown model, a missing pinned CLI) raises."""
    if now.tzinfo is None:
        raise ValueError("now must be tz-aware")
    clock = clock if clock is not None else (lambda: datetime.now(IST))
    out = _summary()
    out["mapper_prompt_sha256"] = hashlib.sha256(system_prompt.encode("utf-8")).hexdigest()
    if reason := ncs.call_refusal(now):
        out["skipped"] = reason
        return out
    batches, out["no_headline"] = split_batches(todo)
    if not batches:
        return out
    con = open_store(db)
    try:
        for batch in batches:
            try:
                if reason := ncs.call_refusal(clock()):
                    raise MarketHoursHalt(reason)
                res = await llm.call(model=MAPPER_MODEL, system_prompt=system_prompt, prompt=user_prompt(batch),
                                     json_schema=MAPPER_SCHEMA)
            except mml.LLMHalt as exc:
                out["stopped"] = f"{type(exc).__name__}: {exc}"
                break
            _record(con, batch, res, system_prompt, out)
            if isinstance(res.error, str) and res.error.startswith(STOP_PREFIXES):
                out["stopped"] = f"quota or login failure: {res.error}"
                break
    finally:
        con.close()
    return out


def _record(con: sqlite3.Connection, batch: Sequence[Any], res: Any, system_prompt: str, out: dict[str, Any]) -> None:
    """The call and every mapping it yields, in one transaction. A failed or unparsable answer writes no mapping."""
    sent = {s.cluster_id for s in batch}
    items: list[Any] | None = None
    if res.ok:
        stories = res.output.get("stories") if isinstance(res.output, dict) else None
        if isinstance(stories, list):
            items = stories
        else:
            out["unparsed_outputs"] += 1
    with con:
        cur = con.execute(
            "INSERT INTO llm_calls (at, model, prompt_sha256, cluster_ids, ok, error, raw_output) "
            "VALUES (?,?,?,?,?,?,?)",
            (res.at.astimezone(IST).isoformat(), res.model, hashlib.sha256(system_prompt.encode("utf-8")).hexdigest(),
             json.dumps([s.cluster_id for s in batch]), 1 if res.ok else 0, res.error,
             json.dumps(res.output, sort_keys=True) if res.output is not None else None))
        out["calls"] += 1
        if items is None:
            return
        answered: set[str] = set()
        for item in items:
            parsed = parse_mapping(item)
            if parsed is None:
                out["malformed_items"] += 1
                continue
            market, groups, companies, dropped = parsed
            cid = item["id"]
            if cid not in sent:
                out["unknown_ids"] += 1
                continue
            written = con.execute(
                "INSERT OR IGNORE INTO mappings (cluster_id, call_rowid, market, groups, companies) VALUES (?,?,?,?,?)",
                (cid, cur.lastrowid, market, json.dumps([list(g) for g in groups]),
                 json.dumps([list(c) for c in companies]))).rowcount
            answered.add(cid)
            if written:
                out["mapped"] += 1
                out["dropped"] += dropped
            else:
                out["already_mapped"] += 1      # the first mapping stays
        out["omitted"] += len(sent - answered)


# --------------------------------------------------------------------------- the CLI
def _calendar_end(cal: Any, lo: date, hi: date) -> date:
    """``hi``, or 31 December of the year before the first year after ``lo``'s without a calendar file: NSE
    publishes the next year's holidays late in the year, and the stories before then can still be placed. ``lo``'s
    own year is never capped away, so a range starting without a calendar still refuses."""
    for y in range(lo.year + 1, hi.year + 1):
        if not nic.mmf._calendar_has_year(cal, y):
            return date(y - 1, 12, 31)
    return hi


def open_resolver(market_db: Path, supplement: Mapping[str, str]) -> Any:
    """The study's resolver over a read-only ``market.duckdb`` copy and the alias supplement; the copy is closed
    before returning, since the resolver holds its tables in memory."""
    conn = nic.rc.open_snapshot(Path(market_db))
    try:
        return nic.indexed_resolver(nic.build_resolver(conn, supplement))
    finally:
        conn.close()


def _todo_for(sample: str, state_db: Path, now: datetime, resolver: Any) -> tuple[list[Any], int]:
    """The sample's stories to map, read from ``state.db`` as scored before ``now`` with orphans by ``resolver``,
    and the number of such stories left out because their entry session lies past the last calendar year
    (``beyond_calendar``); those are mapped on a later run, once the calendar reaches them. A range starting in a
    year without a calendar raises ``Unavailable``."""
    stories, _ = nic.load_first_scores(state_db, now)
    orphans = nic.orphan_ids(stories, resolver)
    lo, hi = nic.SAMPLES[sample]
    cal = nic.load_calendar()
    first = lo - timedelta(days=14)
    last = min(hi, now.astimezone(IST).date()) + timedelta(days=14)
    end = _calendar_end(cal, first, last)
    sessions = nic.regular_sessions(cal, first, end)
    beyond = 0
    if end < last:
        could_enter = datetime.combine(hi, nic.ENTRY_CUTOFF, IST)  # a story at or after it enters past the window
        beyond = sum(1 for s in stories if nic.map_kind(s, orphans) in KINDS and s.at < could_enter
                     and nic.entry_session(s.at, sessions) is None)
    return stories_to_map(stories, sessions, sample, orphans), beyond


def _todo_kind(s: Any) -> str | None:
    """A selected story's kind: ``stories_to_map`` admits a stock story only as an orphan."""
    kind = nic.story_kind(s)
    return "orphan" if kind == "stock" else kind


def _dry_run(todo: Sequence[Any], mapped: dict[str, Any], remaining: int, sample: str, beyond: int = 0) -> None:
    print(f"sample {sample}: {len(todo)} domain, global and orphan stories")
    print(f"beyond_calendar {beyond}: stories whose entry session lies past the last config/calendar year, "
          "not mapped this run")
    needed = 0
    for kind in KINDS:
        of_kind = [s for s in todo if _todo_kind(s) == kind]
        unmapped = [s for s in of_kind if s.cluster_id not in mapped]
        batches, no_head = split_batches(unmapped)
        needed += len(batches)
        print(f"  {kind}: mapped {len(of_kind) - len(unmapped)}, unmapped {len(unmapped)} "
              f"({no_head} without a headline), batches needed {len(batches)}")
    print(f"batches needed {needed}; calls left tonight: {remaining}")


def main(argv: list[str] | None = None, *, clock: Callable[[], datetime] | None = None,
         supplement: Mapping[str, str] | None = None) -> int:
    """The CLI. ``supplement`` None reads the frozen alias supplement; the CLI never passes one."""
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--sample", required=True, choices=sorted(nic.SAMPLES))
    ap.add_argument("--state-db", type=Path, default=DEFAULT_STATE_DB)
    ap.add_argument("--db", type=Path, default=DEFAULT_DB)
    ap.add_argument("--market-db", type=Path, required=True,
                    help="a whole-file market.duckdb copy, read-only; its alias table, with the frozen supplement, "
                         "decides which stock stories are orphans")
    ap.add_argument("--max-calls", type=int, default=MAX_CALLS_PER_NIGHT,
                    help=f"the night's call cap, at most {MAX_CALLS_PER_NIGHT}; calls already made since the last "
                         "15:30 IST count")
    ap.add_argument("--dry-run", action="store_true",
                    help="report what is mapped and what is left; reads only, makes no call")
    args = ap.parse_args(argv)
    if not 0 <= args.max_calls <= MAX_CALLS_PER_NIGHT:
        ap.error(f"--max-calls must be 0..{MAX_CALLS_PER_NIGHT}")
    prompt_bytes = PROMPT_PATH.read_bytes()
    digest = hashlib.sha256(prompt_bytes).hexdigest()
    if digest != MAPPER_PROMPT_SHA256:
        print(f"the mapper prompt changed since registration ({digest} != {MAPPER_PROMPT_SHA256}); no call is made")
        return 2
    try:
        extra = nic.frozen_alias_supplement() if supplement is None else supplement
    except nic.Unavailable as exc:
        print(f"refused: {exc}; no call is made")
        return 2
    now = (clock or (lambda: datetime.now(IST)))()
    try:
        remaining, stop = night_budget(args.db, now, args.max_calls)
        if args.dry_run:
            todo, beyond = _todo_for(args.sample, args.state_db, now, open_resolver(args.market_db, extra))
            _dry_run(todo, read_mappings(args.db), remaining, args.sample, beyond)
            return 0
        if reason := ncs.call_refusal(now):
            print(f"refused: {reason}")
            return 0
        if stop is not None:
            print(f"stopped earlier this night ({stop}); nothing to do until the next 15:30 IST")
            return 0
        lock = Path(args.db).with_name("news_impact_map.lock")
        lock.parent.mkdir(parents=True, exist_ok=True)
        if not ncs._acquire_lock(lock):
            print(f"skipped: another mapping run holds {lock}")
            return 0
        try:
            mapped = read_mappings(args.db)
            resolver = open_resolver(args.market_db, extra)
            todo = [s for s in _todo_for(args.sample, args.state_db, now, resolver)[0] if s.cluster_id not in mapped]
            llm = mml.StudyLLM(max_calls=remaining)
            out = asyncio.run(run_night(args.db, todo, llm, now, prompt_bytes.decode("utf-8"), clock=clock))
            if out["stopped"] is None and getattr(llm, "stopped", False):
                out["stopped"] = f"LLMStopped: {getattr(llm, 'stop_reason', None) or 'stopped'}"
        finally:
            lock.unlink(missing_ok=True)
    except (sqlite3.Error, nic.Unavailable, nic.rc.Refusal) as exc:
        print(f"unreadable input: {type(exc).__name__}: {exc}")
        return 2
    print(json.dumps(out, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
