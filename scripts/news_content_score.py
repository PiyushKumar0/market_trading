"""Nightly scoring and policy mapping for the macro mood-signal study (``edge/macro_mood``).

Runs at 22:00 IST from Windows Task Scheduler over the SQLite file ``news_content_capture.py`` fills. One night:

1. clusters the newly captured items with the engine's ``HeadlineClusterer`` at its pinned threshold, passing the
   persisted study clusters as ``existing`` so a run of incremental nights groups as one pass would;
2. scores each study cluster captured from ``STUDY_FORWARD_START`` on, once its cut-off has passed, twice with the
   pinned Haiku model, the News Analyst's system prompt and its output schema, in separate blind calls: **H**, the
   engine's own News Analyst prompt layout (representative headline only), and **C**, the same layout plus the
   representative's summary and article excerpt;
3. maps policy stories to stock groups with the pinned Sonnet mapper, one story per call: the News Analyst's own
   first scores from ``state.db`` first scored from ``HEADLINE_FORWARD_START`` on (``headline``), the study's C
   scores (``content``) and its H scores (``study_h``).

Writes only to the study's own file; ``state.db`` is opened read-only. Every call goes through ``StudyLLM`` (the
night's cap, no retries, a permanent stop on a quota or login failure) and is logged append-only in ``llm_calls``
with its prompt hashes and CLI; every run appends its summary to ``score_runs``. Work runs in priority order, so a
capped or stopped night leaves the least urgent part for the next one. No call is made in market hours, checked
again before each call, because a run suspended by host standby can resume inside the session. No call is made
once either registered prompt differs from its pinned hash.

No lookahead: a cluster's cut-off is the first 09:00 IST strictly after its representative's capture. For a
weekend or holiday capture that is earlier than the close of the cluster's news window; the cut-off only drops
late members and excerpts, so it is conservative. A cluster is scored only once its cut-off has passed, so both
variants see the same members. Its prompts count only members captured before the cut-off, and show an excerpt
only when the page was fetched before it.
"""

from __future__ import annotations

import argparse
import asyncio
import gzip
import hashlib
import importlib.util
import json
import os
import re
import sqlite3
import sys
import time as time_mod
import uuid
import zlib
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

_ROOT = Path(__file__).resolve().parents[1]
_REPO_SRC = str(_ROOT / "src")
if _REPO_SRC not in sys.path:  # pragma: no cover - loose-script shim
    sys.path.insert(0, _REPO_SRC)

import engine  # noqa: E402,F401,I001  native import-order guard (engine._preload), keep FIRST
from engine.datafeeds.news import Headline, _domain  # noqa: E402
from engine.datafeeds.news_pipeline import HeadlineClusterer, NewsCluster, title_tokens  # noqa: E402
from engine.intelligence.agents import news_analyst  # noqa: E402

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

IST = ZoneInfo("Asia/Kolkata")
DEFAULT_CONTENT_DB = _ROOT / "data" / "research" / "news_content.sqlite"
DEFAULT_STATE_DB = _ROOT / "data" / "state.db"
MAPPER_PROMPT_PATH = _ROOT / "config" / "research" / "macro_mood" / "mapper_prompt.md"

SCORE_MODEL = "claude-haiku-5-5"
MAP_MODEL = "claude-sonnet-5-5"
MAX_CALLS_PER_NIGHT = 40
#: Older than the longest possible night (40 calls at the 240 s timeout is 2 h 40 min): a crashed run's lock.
LOCK_STALE_S = 6 * 3600
#: Study calls set no effort, so each model's documented default applies (the ``news_analyst`` def has none).
DEFAULT_EFFORT = "default"
#: ``llm_calls.error`` prefixes of a quota or login failure: one of them today ends the day's work.
STOP_PREFIXES = tuple(f"{reason}:" for reason in sorted(mml.STOP_REASONS))

#: Engine headline candidates are mapped from here: the first registered session is 2026-11-11, and 2026-11-10 is a
#: holiday, so that session's news window opens at 09:00 on 2026-11-09. Earlier candidates were first scored under
#: an older engine key the forward evaluator never uses, so mapping them would spend calls for nothing.
HEADLINE_FORWARD_START = datetime(2026, 11, 9, 9, 0, tzinfo=IST)
#: Study clusters are scored and mapped from here, the registration day's 09:00.
STUDY_FORWARD_START = datetime(2026, 11, 10, 9, 0, tzinfo=IST)
CUTOFF_TIME = time(9, 0)

#: Registered instruments, pinned at registration: the mapper prompt file's bytes, and the scoring prompt the study
#: imports live from the engine (its system prompt, a newline, then its output schema as sorted-key JSON). A change
#: to either would silently change a registered instrument mid-study, so the night refuses to call.
MAPPER_PROMPT_SHA256 = "89ada625ed09846a38d57f04fac7903bfe392b94aa36ca006f0afdef4d94aaf8"
SCORE_PROMPT_SHA256 = "549b6c6feba7960261e5871f24fbcca867e0dae2106ed3e19d8c596542c4e91b"
MARKET_OPEN, MARKET_CLOSE = time(9, 15), time(15, 30)

CLUSTER_SIM_THRESHOLD = 0.75
#: ``HeadlineClusterer``'s own default event-age window, passed explicitly so existing clusters are loaded to match.
CLUSTER_WINDOW_DAYS = 2

POLICY_EVENTS = frozenset({"govt_program", "sector_policy"})
MIN_SENTIMENT = 0.4
MIN_MATERIALITY = 0.6
MAX_GROUPS = 3

#: ``config/themes.yaml`` theme names in file order, frozen at registration like the capture's feed list; a test
#: holds it equal to the file, so an edit there stops the suite instead of silently changing the study's prompt.
THEME_VOCABULARY: tuple[str, ...] = (
    "ev_mobility", "defence", "railways", "infra_capex", "renewables", "psu_disinvestment", "data_ai",
    "make_in_india",
)
#: The mapper's closed vocabulary, in ``mapper_prompt.md`` order: 23 ``sector_map`` labels, then 7 themes.
VOCAB: tuple[str, ...] = (
    "AUTO", "BANK", "CAPITAL_GOODS", "CHEMICALS", "CONSTRUCTION", "CONSTRUCTION_MATERIALS", "CONSUMER_DURABLES",
    "CONSUMER_SERVICES", "DIVERSIFIED", "ENERGY", "FINANCIAL_SERVICES", "FMCG", "FOREST_MATERIALS", "IT",
    "MEDIA_ENTERTAINMENT_PUBLICATION", "METAL", "PHARMA", "PSU_BANK", "REALTY", "SERVICES", "TELECOMMUNICATION",
    "TEXTILES", "UTILITIES",
    "defence", "ev_mobility", "railways", "infra_capex", "psu_disinvestment", "data_ai", "make_in_india",
)
_VOCAB_SET = frozenset(VOCAB)
MAPPER_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {"groups": {"type": "array", "items": {"type": "string", "enum": list(VOCAB)}}},
    "required": ["groups"],
    "additionalProperties": False,
}

#: The ``scores`` column marking a row the model left out of a parsed answer: final, with no score values.
OMITTED_COLUMN = "omitted"

_SCHEMA = f"""
CREATE TABLE IF NOT EXISTS clusters(
    cluster_key TEXT PRIMARY KEY, first_captured_at TEXT NOT NULL, representative_url TEXT NOT NULL,
    member_urls TEXT NOT NULL, representative_title TEXT NOT NULL, source_domains TEXT NOT NULL,
    first_seen TEXT NOT NULL, last_seen TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS cluster_members(
    url TEXT PRIMARY KEY, cluster_key TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS idx_cluster_members_key ON cluster_members(cluster_key);
CREATE TABLE IF NOT EXISTS llm_calls(
    call_id TEXT PRIMARY KEY, at TEXT NOT NULL,
    purpose TEXT NOT NULL CHECK(purpose IN ('score_H','score_C','map_headline','map_content')),
    model TEXT NOT NULL, prompt_sha256 TEXT NOT NULL, ok INTEGER NOT NULL, error TEXT,
    effort TEXT NOT NULL, duration_s REAL, output_json TEXT, prompt_gz BLOB,
    system_sha256 TEXT NOT NULL, cli TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS scores(
    cluster_key TEXT NOT NULL, variant TEXT NOT NULL CHECK(variant IN ('H','C')), scope TEXT, event_type TEXT,
    sentiment REAL, materiality REAL, novelty REAL, call_id TEXT NOT NULL,
    {OMITTED_COLUMN} INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(cluster_key, variant));
CREATE TABLE IF NOT EXISTS mappings(
    candidate_key TEXT NOT NULL, source TEXT NOT NULL CHECK(source IN ('headline','study_h','content')),
    groups TEXT NOT NULL, call_id TEXT NOT NULL, PRIMARY KEY(candidate_key, source));
"""
_RUNS_SCHEMA = """
CREATE TABLE IF NOT EXISTS score_runs(
    run_at TEXT NOT NULL, finished_at TEXT NOT NULL, summary_json TEXT NOT NULL);
"""

_CONTEXT_CLUSTER = re.compile(r"^\d+\. cluster_id=(\S+) \| ")
_CONTEXT_HEADLINE = "   headline: "


# --------------------------------------------------------------------------- pure helpers
def _parse_ts(raw: Any) -> datetime | None:
    """An ISO-8601 tz-aware timestamp in IST; ``None`` when absent, malformed or naive."""
    if not isinstance(raw, str) or not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    return parsed.astimezone(IST) if parsed.tzinfo is not None else None


def _clean(text: Any) -> str | None:
    """Whitespace collapsed to single spaces (a line break would forge a prompt line); ``None`` when empty."""
    if not isinstance(text, str):
        return None
    return " ".join(text.split()) or None


def cutoff_for(captured: datetime) -> datetime:
    """The first 09:00 IST strictly after ``captured``. For a weekend or holiday capture this is earlier than the
    close of the cluster's news window; it only drops late members and excerpts, so it is conservative."""
    local = captured.astimezone(IST)
    nine = datetime.combine(local.date(), CUTOFF_TIME, tzinfo=IST)
    return nine if local < nine else nine + timedelta(days=1)


def headline_id_for(url: str) -> str:
    """A stable per-url id, so a study cluster's key ("c-" + its founding item's id) never depends on run order."""
    return "s" + hashlib.sha1(url.encode("utf-8")).hexdigest()[:25]


def meets_policy_bars(event_type: Any, sentiment: Any, materiality: Any) -> bool:
    return (event_type in POLICY_EVENTS and isinstance(sentiment, int | float) and sentiment >= MIN_SENTIMENT
            and isinstance(materiality, int | float) and materiality >= MIN_MATERIALITY)


def market_hours_refusal(now: datetime) -> str | None:
    """Any weekday 09:15-15:30 IST: stricter than the trading calendar, and needs no calendar file."""
    local = now.astimezone(IST)
    if local.weekday() < 5 and MARKET_OPEN <= local.time() < MARKET_CLOSE:
        return f"market hours ({local:%Y-%m-%d %H:%M} IST); run outside 09:15-15:30 on a weekday"
    return None


def calendar_refusal(now: datetime) -> str | None:
    """``research_common``'s trading-calendar rule, imported on first use: that module pulls in the backtest
    scripts, and a fault in one of them must not kill the nightly run before the weekday rule has answered."""
    import research_common  # noqa: PLC0415

    return research_common.market_hours_refusal(now)


def call_refusal(now: datetime) -> str | None:
    """The weekday rule, else the trading-calendar rule (which also covers a special session on a weekend). A
    calendar rule that cannot answer (no file for the year, or a fault importing it) is ignored: the weekday rule
    still covers weekdays."""
    if reason := market_hours_refusal(now):
        return reason
    try:
        return calendar_refusal(now)
    except Exception:  # noqa: BLE001 - a calendar fault must not crash the night; the weekday rule stands
        return None


class MarketHoursHalt(mml.LLMHalt):
    """A call refused because the clock reads market hours: the engine's analysts share this subscription."""


def prompt_pin_mismatch(mapper_bytes: bytes) -> str | None:
    """Which registered prompt differs from its pinned hash, or None when both match."""
    score = f"{news_analyst.SYSTEM_PROMPT}\n{json.dumps(news_analyst.output_json_schema(), sort_keys=True)}"
    changed = [name for name, text, pin in (
        ("the scoring prompt", score.encode("utf-8"), SCORE_PROMPT_SHA256),
        ("the mapper prompt", mapper_bytes, MAPPER_PROMPT_SHA256),
    ) if hashlib.sha256(text).hexdigest() != pin]
    return " and ".join(changed) if changed else None


@dataclass(frozen=True)
class PromptCluster:
    """One cluster as a scoring prompt shows it; ``summary`` and ``excerpt`` appear in variant C only."""

    cluster_key: str
    headline: str
    first_seen: datetime
    source_domains: tuple[str, ...]
    summary: str | None = None
    excerpt: str | None = None


def _section(title: str, lines: Sequence[str]) -> str:
    if not lines:
        return f"{title}: none"
    return "\n".join([f"{title}:", *(f"  - {line}" for line in lines)])


def build_prompt(rows: Sequence[PromptCluster], variant: str) -> str:
    """The News Analyst user prompt (``ContextAssembler.for_news_batch(...).prompt()``, byte for byte) for H; C
    adds ``summary`` and ``excerpt`` lines under each headline."""
    if variant not in ("H", "C"):
        raise ValueError(f"variant must be 'H' or 'C', got {variant!r}")
    if len(rows) > news_analyst.MAX_CLUSTERS_PER_CALL:
        raise ValueError(f"batch of {len(rows)} exceeds the {news_analyst.MAX_CLUSTERS_PER_CALL}-cluster cap")
    stable = "\n".join([
        "== SCORING TASK (stable) ==",
        "Score every numbered cluster below exactly once, against the rubric in your instructions.",
    ])
    parts = ["== CLUSTERS (volatile) =="]
    for index, row in enumerate(rows, start=1):
        parts.append(f"{index}. cluster_id={row.cluster_key} | first seen {row.first_seen.date().isoformat()} "
                     f"| {len(row.source_domains)} source domain(s)")
        parts.append(f"{_CONTEXT_HEADLINE}{row.headline}")
        if variant == "C":
            if row.summary:
                parts.append(f"   summary: {row.summary}")
            if row.excerpt:
                parts.append(f"   excerpt: {row.excerpt}")
    parts.append(_section("theme vocabulary (the only themes you may emit)", THEME_VOCABULARY))
    return f"{stable}\n\n" + "\n".join(parts)


def story_text(headline: str, summary: str | None = None, excerpt: str | None = None) -> str:
    """The mapper's user prompt: one story."""
    lines = [f"headline: {headline}"]
    if summary:
        lines.append(f"summary: {summary}")
    if excerpt:
        lines.append(f"excerpt: {excerpt}")
    return "\n".join(lines)


def parse_mapper_output(output: Any) -> list[str] | None:
    """The first three distinct in-vocabulary groups, in the model's order; ``None`` for a malformed output."""
    if not isinstance(output, dict) or not isinstance(output.get("groups"), list):
        return None
    kept: list[str] = []
    for group in output["groups"]:
        if isinstance(group, str) and group in _VOCAB_SET and group not in kept:
            kept.append(group)
            if len(kept) == MAX_GROUPS:
                break
    return kept


# --------------------------------------------------------------------------- the engine's headline candidates
def open_state_ro(path: Path) -> sqlite3.Connection:
    """``state.db`` read-only: a write raises, and a missing file is an error rather than a new empty db."""
    return sqlite3.connect(f"{Path(path).resolve().as_uri()}?mode=ro", uri=True)


@dataclass(frozen=True)
class HeadlineCandidate:
    cluster_id: str
    at: datetime            # the News Analyst call that first scored it
    call_id: str
    headline: str | None    # from that call's archived prompt; ``None`` when unreadable


def _context_headlines(blob: Any) -> dict[str, str]:
    """``cluster_id -> headline`` out of an archived News Analyst prompt; ``{}`` when unreadable."""
    if not blob:
        return {}
    try:
        text = gzip.decompress(bytes(blob)).decode("utf-8", errors="replace")
    except (OSError, EOFError, zlib.error, TypeError, ValueError):
        return {}
    lines = text.split("\n")          # not splitlines(): it also splits on characters a headline may carry
    out: dict[str, str] = {}
    for line, nxt in zip(lines, lines[1:], strict=False):
        match = _CONTEXT_CLUSTER.match(line)
        if match and nxt.startswith(_CONTEXT_HEADLINE):
            headline = _clean(nxt[len(_CONTEXT_HEADLINE):])
            if headline:
                out.setdefault(match.group(1), headline)
    return out


def headline_candidates(state: sqlite3.Connection, now: datetime) -> list[HeadlineCandidate]:
    """Engine clusters whose FIRST successful News Analyst score meets the policy bars, first scored from
    ``HEADLINE_FORWARD_START`` up to ``now``, oldest first. The first score is found across every call before the
    start filter applies, so a cluster first scored before the start never qualifies through a later re-score, and a
    later re-score never replaces the first, in either direction."""
    index = state.execute(
        "SELECT rowid, call_id, at FROM agent_calls WHERE agent_id = ? AND ok = 1", (news_analyst.AGENT_ID,),
    ).fetchall()
    calls = sorted(((at, rowid, call_id) for rowid, call_id, raw_at in index
                    if (at := _parse_ts(raw_at)) is not None and at <= now), key=lambda r: (r[0], r[1]))
    first: dict[str, tuple[datetime, str, Any]] = {}
    for at, rowid, call_id in calls:     # one output at a time: the table grows by ~100 calls a day
        row = state.execute("SELECT output_json FROM agent_calls WHERE rowid = ?", (rowid,)).fetchone()
        try:
            scores, _ = news_analyst.parse_output(row[0] if row else None)
        except (ValueError, TypeError):
            continue
        for s in scores:
            first.setdefault(s.cluster_id, (at, call_id, s))
    picked = [(cid, at, call_id) for cid, (at, call_id, s) in first.items()
              if at >= HEADLINE_FORWARD_START and meets_policy_bars(s.event_type, s.sentiment, s.materiality)]
    contexts: dict[str, dict[str, str]] = {}
    for call_id in {call_id for _, _, call_id in picked}:
        row = state.execute("SELECT context_gz FROM agent_calls WHERE call_id = ?", (call_id,)).fetchone()
        contexts[call_id] = _context_headlines(row[0] if row else None)
    return sorted((HeadlineCandidate(cid, at, call_id, contexts[call_id].get(cid)) for cid, at, call_id in picked),
                  key=lambda c: (c.at, c.cluster_id))


# --------------------------------------------------------------------------- the night's budget
def night_budget(content_db: Path, now: datetime, max_calls: int = MAX_CALLS_PER_NIGHT) -> tuple[int, str | None]:
    """``(calls left today, the first stopping error logged today)``; today is ``now``'s IST calendar date."""
    content_db = Path(content_db)
    if not content_db.is_file():
        return max_calls, None
    con = sqlite3.connect(f"{content_db.resolve().as_uri()}?mode=ro", uri=True)
    try:
        if con.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'llm_calls'").fetchone() is None:
            return max_calls, None
        rows = con.execute("SELECT at, error FROM llm_calls ORDER BY rowid").fetchall()
    finally:
        con.close()
    today: date = now.astimezone(IST).date()
    used, stop = 0, None
    for raw_at, error in rows:
        at = _parse_ts(raw_at)
        if at is None or at.date() != today:
            continue
        used += 1
        if stop is None and isinstance(error, str) and error.startswith(STOP_PREFIXES):
            stop = error
    return max(0, max_calls - used), stop


# --------------------------------------------------------------------------- clustering
def _cluster_new_items(con: sqlite3.Connection, now: datetime) -> tuple[int, int]:
    """Cluster every item captured by ``now`` that is in no cluster yet. Returns ``(items clustered, new clusters)``.

    A cluster's representative and ``first_captured_at`` are fixed when it is first stored: its scores and its
    window hang on that text, so a later member never re-seats it.
    """
    fresh: list[tuple[str, str, datetime, datetime]] = []
    for url, ttl, published_raw, captured_raw in con.execute(
        "SELECT i.url, i.title, i.published_at, i.first_captured_at FROM items i "
        "LEFT JOIN cluster_members m ON m.url = i.url WHERE m.url IS NULL"
    ).fetchall():
        captured = _parse_ts(captured_raw)
        if captured is None or captured > now or not title_tokens(ttl or ""):
            continue      # not yet visible to this night, or a title with no words (it would match every other)
        fresh.append((url, ttl, captured, _parse_ts(published_raw) or captured))
    if not fresh:
        return 0, 0

    by_hid: dict[str, tuple[str, str, datetime]] = {}
    headlines: list[Headline] = []
    for url, ttl, captured, published in fresh:
        hid = headline_id_for(url)
        by_hid[hid] = (url, ttl, captured)
        headlines.append(Headline(headline_id=hid, title=ttl, source_domain=_domain(url), url=url,
                                  published_at=published))
    window_start = min(h.published_at for h in headlines) - timedelta(days=CLUSTER_WINDOW_DAYS)

    stored: dict[str, list[str]] = {}
    existing: list[NewsCluster] = []
    for key, rep_title, domains, first_seen, last_seen, members in con.execute(
        "SELECT cluster_key, representative_title, source_domains, first_seen, last_seen, member_urls FROM clusters"
    ).fetchall():
        last = _parse_ts(last_seen)
        if last is None or last < window_start:
            continue
        stored[key] = json.loads(members)
        existing.append(NewsCluster(cluster_id=key, representative=rep_title, source_domains=json.loads(domains),
                                    first_seen=_parse_ts(first_seen), last_seen=last))

    touched = HeadlineClusterer(sim_threshold=CLUSTER_SIM_THRESHOLD,
                                max_event_age_days=CLUSTER_WINDOW_DAYS).cluster(headlines, existing)
    new_clusters = 0
    with con:
        for c in touched:
            urls = [by_hid[hid][0] for hid in c.headline_ids]
            if c.cluster_id in stored:
                con.execute(
                    "UPDATE clusters SET member_urls = ?, source_domains = ?, first_seen = ?, last_seen = ? "
                    "WHERE cluster_key = ?",
                    (json.dumps(stored[c.cluster_id] + urls), json.dumps(c.source_domains),
                     c.first_seen.isoformat(), c.last_seen.isoformat(), c.cluster_id))
            else:
                founder = by_hid[c.cluster_id[len("c-"):]]
                rep_url, rep_title, rep_captured = next(
                    (by_hid[hid] for hid in c.headline_ids if by_hid[hid][1] == c.representative), founder)
                con.execute(
                    "INSERT INTO clusters (cluster_key, first_captured_at, representative_url, member_urls, "
                    "representative_title, source_domains, first_seen, last_seen) VALUES (?,?,?,?,?,?,?,?)",
                    (c.cluster_id, rep_captured.isoformat(), rep_url, json.dumps(urls), rep_title,
                     json.dumps(c.source_domains), c.first_seen.isoformat(), c.last_seen.isoformat()))
                new_clusters += 1
            con.executemany("INSERT INTO cluster_members (url, cluster_key) VALUES (?, ?)",
                            [(url, c.cluster_id) for url in urls])
    return len(fresh), new_clusters


# --------------------------------------------------------------------------- the night
@dataclass(frozen=True)
class _Cluster:
    key: str
    first_captured_at: datetime
    representative_url: str
    representative_title: str


class _Night:
    def __init__(self, con: sqlite3.Connection, llm: Any, now: datetime, mapper_prompt: str,
                 out: dict[str, Any], clock: Callable[[], datetime]) -> None:
        self.con, self.llm, self.now, self.mapper_prompt, self.out = con, llm, now, mapper_prompt, out
        self.clock = clock
        self.effort = getattr(getattr(llm, "_def", None), "effort", None) or DEFAULT_EFFORT
        self.cli = getattr(llm, "cli_path", None) or "unknown"
        self.clusters = {
            key: _Cluster(key, captured, rep_url, rep_title)
            for key, raw, rep_url, rep_title in con.execute(
                "SELECT cluster_key, first_captured_at, representative_url, representative_title FROM clusters")
            if (captured := _parse_ts(raw)) is not None
        }

    async def work(self, headlines: list[HeadlineCandidate]) -> None:
        """Priority: H/C scoring, headline maps, content maps, study_h maps."""
        await self._score_forward()
        mapped = self._mapped("headline")
        for cand in (c for c in headlines if c.cluster_id not in mapped):
            await self._map_headline(cand)
        for key, text in self._study_candidates("C"):
            await self._map(key, "content", text)
        for key, text in self._study_candidates("H"):
            await self._map(key, "study_h", text)

    async def _call(self, **kw: Any) -> Any:
        """``llm.call`` after a fresh market-hours check on the injected clock."""
        if reason := call_refusal(self.clock()):
            raise MarketHoursHalt(reason)
        return await self.llm.call(**kw)

    # ---------------------------------------------------------------- scoring
    def _scored(self) -> set[tuple[str, str]]:
        return {(k, v) for k, v in self.con.execute("SELECT cluster_key, variant FROM scores")}

    async def _score_forward(self) -> None:
        # Only after its cut-off are a cluster's members and excerpt before the cut-off final: scoring earlier would
        # tie the score to when the scorer ran, and let a C retried a night later see members H never saw.
        scored = self._scored()
        pending = sorted((c for c in self.clusters.values()
                          if c.first_captured_at >= STUDY_FORWARD_START
                          and cutoff_for(c.first_captured_at) <= self.now
                          and ((c.key, "H") not in scored or (c.key, "C") not in scored)),
                         key=lambda c: (c.first_captured_at, c.key))
        size = news_analyst.MAX_CLUSTERS_PER_CALL
        for start in range(0, len(pending), size):
            chunk = pending[start:start + size]
            rows = [self._prompt_cluster(c) for c in chunk]
            need_h = [r for r in rows if (r.cluster_key, "H") not in scored]
            need_c = [r for r in rows if (r.cluster_key, "C") not in scored]
            if need_h:
                await self._score(need_h, "H")
            if need_c:
                await self._score(need_c, "C")

    async def _score(self, rows: list[PromptCluster], variant: str) -> None:
        prompt = build_prompt(rows, variant)
        system = news_analyst.SYSTEM_PROMPT
        res = await self._call(model=SCORE_MODEL, system_prompt=system, prompt=prompt,
                               json_schema=news_analyst.output_json_schema())
        scores: list[Any] = []
        parsed = False
        if res.ok:
            try:
                # The engine's parser reads a dict without "scores" as an empty batch; here that is a malformed
                # answer, not one that omits every cluster.
                if not isinstance(res.output, dict) or not isinstance(res.output.get("scores"), list):
                    raise ValueError("no scores list")
                scores, _ = news_analyst.parse_output(res.output)
                parsed = True
            except (ValueError, TypeError):
                self.out["unparsed_outputs"] += 1
        sent = [r.cluster_key for r in rows]
        written: set[str] = set()
        with self.con:
            call_id = self._log_call(f"score_{variant}", system, prompt, res)
            for s in scores:
                if s.cluster_id not in sent:
                    continue      # an id the model invented: nothing to attach it to
                cur = self.con.execute(
                    "INSERT OR IGNORE INTO scores (cluster_key, variant, scope, event_type, sentiment, materiality, "
                    "novelty, call_id) VALUES (?,?,?,?,?,?,?,?)",
                    (s.cluster_id, variant, s.scope, s.event_type, s.sentiment, s.materiality, s.novelty, call_id))
                self.out["scored"][variant] += cur.rowcount
                if cur.rowcount:
                    written.add(s.cluster_id)
            # A parsed answer that leaves a sent cluster out is final for it: a forward look waits for every cluster
            # in its windows, and one the model keeps omitting would hold it forever. A failed or unparsed call
            # writes nothing and is retried next night.
            if parsed:
                for key in sent:
                    if key in written:
                        continue
                    cur = self.con.execute(
                        f"INSERT OR IGNORE INTO scores (cluster_key, variant, call_id, {OMITTED_COLUMN}) "
                        "VALUES (?,?,?,1)",
                        (key, variant, call_id))
                    self.out["scores_omitted"][variant] += cur.rowcount

    def _prompt_cluster(self, c: _Cluster) -> PromptCluster:
        """The cluster as it stood at its cut-off: members captured before it, an excerpt fetched before it."""
        cut = cutoff_for(c.first_captured_at)
        domains: set[str] = set()
        first_seen: datetime | None = None
        for url, published_raw, captured_raw in self.con.execute(
            "SELECT i.url, i.published_at, i.first_captured_at FROM cluster_members m "
            "JOIN items i ON i.url = m.url WHERE m.cluster_key = ?", (c.key,)
        ).fetchall():
            captured = _parse_ts(captured_raw)
            if captured is None or captured >= cut:
                continue
            domains.add(_domain(url))
            seen = _parse_ts(published_raw) or captured
            first_seen = seen if first_seen is None else min(first_seen, seen)
        if first_seen is None:          # unreachable while the representative is a member; fall back to it
            domains, first_seen = {_domain(c.representative_url)}, c.first_captured_at
        item = self.con.execute("SELECT summary FROM items WHERE url = ?", (c.representative_url,)).fetchone()
        body = self.con.execute("SELECT fetched_at, text FROM bodies WHERE url = ?",
                                (c.representative_url,)).fetchone()
        excerpt = None
        if body is not None:
            fetched = _parse_ts(body[0])
            if fetched is not None and fetched < cut and fetched <= self.now:
                excerpt = _clean(body[1])
        return PromptCluster(c.key, c.representative_title, first_seen, tuple(sorted(domains)),
                             _clean(item[0]) if item else None, excerpt)

    # ---------------------------------------------------------------- mapping
    def _mapped(self, source: str) -> set[str]:
        return {k for (k,) in self.con.execute("SELECT candidate_key FROM mappings WHERE source = ?", (source,))}

    def _study_candidates(self, variant: str) -> list[tuple[str, str]]:
        source = "content" if variant == "C" else "study_h"
        mapped = self._mapped(source)
        keys = [key for key, event_type, sentiment, materiality in self.con.execute(
                    "SELECT cluster_key, event_type, sentiment, materiality FROM scores WHERE variant = ?", (variant,))
                if key not in mapped and key in self.clusters
                and meets_policy_bars(event_type, sentiment, materiality)]
        keys.sort(key=lambda k: (self.clusters[k].first_captured_at, k))
        out = []
        for key in keys:
            if variant == "C":
                row = self._prompt_cluster(self.clusters[key])
                out.append((key, story_text(row.headline, row.summary, row.excerpt)))
            else:
                out.append((key, story_text(self.clusters[key].representative_title)))
        return out

    async def _map_headline(self, cand: HeadlineCandidate) -> None:
        if cand.headline is None:
            return        # never mapped; counted up front in ``headline_no_text_total``
        await self._map(cand.cluster_id, "headline", story_text(cand.headline))

    async def _map(self, key: str, source: str, text: str) -> None:
        res = await self._call(model=MAP_MODEL, system_prompt=self.mapper_prompt, prompt=text,
                               json_schema=MAPPER_SCHEMA)
        groups = parse_mapper_output(res.output) if res.ok else None
        if res.ok and groups is None:
            self.out["unparsed_outputs"] += 1
        with self.con:
            call_id = self._log_call("map_content" if source == "content" else "map_headline",
                                     self.mapper_prompt, text, res)
            if groups is not None:
                cur = self.con.execute(
                    "INSERT OR IGNORE INTO mappings (candidate_key, source, groups, call_id) VALUES (?,?,?,?)",
                    (key, source, json.dumps(groups), call_id))
                self.out["mapped"][source] += cur.rowcount

    # ---------------------------------------------------------------- the call log
    def _log_call(self, purpose: str, system: str, prompt: str, res: Any) -> str:
        """One append-only ``llm_calls`` row; the caller's transaction makes it atomic with what the call wrote.
        ``system_sha256`` hashes the system prompt alone, so a mapper call carries the mapper prompt's hash."""
        call_id = uuid.uuid4().hex
        hashed = f"{system}\n{prompt}".encode()
        self.con.execute(
            "INSERT INTO llm_calls (call_id, at, purpose, model, prompt_sha256, ok, error, effort, duration_s, "
            "output_json, prompt_gz, system_sha256, cli) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (call_id, res.at.astimezone(IST).isoformat(), purpose, res.model, hashlib.sha256(hashed).hexdigest(),
             1 if res.ok else 0, res.error, self.effort, res.duration_s,
             json.dumps(res.output, sort_keys=True) if res.output is not None else None, gzip.compress(hashed),
             hashlib.sha256(system.encode()).hexdigest(), self.cli))
        self.out["calls"] += 1
        return call_id


def _summary() -> dict[str, Any]:
    return {"skipped": None, "stopped": None, "clustered_items": 0, "new_clusters": 0, "calls": 0,
            "scored": {"H": 0, "C": 0}, "scores_omitted": {"H": 0, "C": 0},
            "mapped": {"headline": 0, "content": 0, "study_h": 0},
            "headline_candidates": 0, "headline_no_text_total": 0, "unparsed_outputs": 0,
            "mapper_prompt_sha256": None}


def _log_run(content_db: Path, run_at: datetime, finished_at: datetime, out: dict[str, Any]) -> None:
    """One ``score_runs`` row: the scheduled task runs under pythonw, so its printed summary is lost."""
    con = sqlite3.connect(content_db, timeout=30)
    try:
        con.executescript(_RUNS_SCHEMA)
        with con:
            con.execute("INSERT INTO score_runs (run_at, finished_at, summary_json) VALUES (?,?,?)",
                        (run_at.isoformat(), finished_at.astimezone(IST).isoformat(),
                         json.dumps(out, sort_keys=True, default=str)))
    finally:
        con.close()


async def run_night(content_db: Path, state_db: Path, llm: Any, now: datetime, *,
                    clock: Callable[[], datetime] | None = None) -> dict[str, Any]:
    """One night's clustering, scoring and mapping. A cap, a stop or the market-hours check before a call ends the
    night cleanly (``stopped``); a setup fault (an unknown model, a missing pinned CLI, an unreadable store) raises.
    ``clock`` is read before every call and when the run ends. Every run whose content file exists, skipped,
    stopped or raising, appends its summary to ``score_runs``."""
    if now.tzinfo is None:
        raise ValueError("now must be tz-aware")
    now = now.astimezone(IST)
    clock = clock if clock is not None else (lambda: datetime.now(IST))
    content_db = Path(content_db)
    out = _summary()
    try:
        await _run_night(content_db, state_db, llm, now, clock, out)
    except BaseException as exc:
        out["error"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        if content_db.is_file():
            _log_run(content_db, now, clock(), out)
    return out


async def _run_night(content_db: Path, state_db: Path, llm: Any, now: datetime, clock: Callable[[], datetime],
                     out: dict[str, Any]) -> None:
    if reason := call_refusal(now):
        out["skipped"] = reason
        return
    if not content_db.is_file():
        raise FileNotFoundError(f"{content_db}: the capture's database is missing")
    mapper_bytes = MAPPER_PROMPT_PATH.read_bytes()
    out["mapper_prompt_sha256"] = hashlib.sha256(mapper_bytes).hexdigest()
    if changed := prompt_pin_mismatch(mapper_bytes):
        out["skipped"] = f"{changed} changed since registration: content accrual stops until the owner re-registers"
        return
    _, stop = night_budget(content_db, now)
    if stop is not None:
        out["skipped"] = f"stopped earlier today: {stop}"
        return

    state = open_state_ro(state_db)
    try:
        headlines = headline_candidates(state, now)
    finally:
        state.close()
    out["headline_candidates"] = len(headlines)
    out["headline_no_text_total"] = sum(1 for c in headlines if c.headline is None)

    con = sqlite3.connect(content_db, timeout=30)
    try:
        con.executescript(_SCHEMA)
        out["clustered_items"], out["new_clusters"] = _cluster_new_items(con, now)
        try:
            await _Night(con, llm, now, mapper_bytes.decode("utf-8"), out, clock).work(headlines)
        except mml.LLMHalt as exc:
            out["stopped"] = f"{type(exc).__name__}: {exc}"
        if out["stopped"] is None and getattr(llm, "stopped", False):
            out["stopped"] = f"LLMStopped: {getattr(llm, 'stop_reason', None) or 'stopped'}"
    finally:
        con.close()


def _acquire_lock(path: Path) -> bool:
    """One scoring run at a time: each run sizes its cap from the calls logged before it starts."""
    for _ in range(2):
        try:
            os.close(os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY))
            return True
        except FileExistsError:
            try:
                age = time_mod.time() - path.stat().st_mtime
            except FileNotFoundError:
                continue
            if age < LOCK_STALE_S:
                return False
            path.unlink(missing_ok=True)
    return False


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--content-db", type=Path, default=DEFAULT_CONTENT_DB)
    ap.add_argument("--state-db", type=Path, default=DEFAULT_STATE_DB)
    ap.add_argument("--max-calls", type=int, default=MAX_CALLS_PER_NIGHT,
                    help=f"the night's call cap, at most {MAX_CALLS_PER_NIGHT}; calls already made today count")
    args = ap.parse_args(argv)
    if not 0 <= args.max_calls <= MAX_CALLS_PER_NIGHT:
        ap.error(f"--max-calls must be 0..{MAX_CALLS_PER_NIGHT}")
    now = datetime.now(IST)
    if reason := call_refusal(now):
        print(f"refused: {reason}")
        return 2
    lock = Path(args.content_db).with_name("news_content_score.lock")
    if not _acquire_lock(lock):
        print(f"skipped: another scoring run holds {lock}")
        return 0
    try:
        remaining, stop = night_budget(args.content_db, now, args.max_calls)
        if stop is not None:
            print(f"stopped earlier today ({stop}); nothing to do until tomorrow")
            return 0
        llm = mml.StudyLLM(max_calls=remaining)
        out = asyncio.run(run_night(args.content_db, args.state_db, llm, now))
    finally:
        lock.unlink(missing_ok=True)
    print(json.dumps(out, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
