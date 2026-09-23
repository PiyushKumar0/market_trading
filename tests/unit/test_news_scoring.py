"""NewsScoringJob (§5.4 / §2.7 step 4) — the batch triggers and the §3.2.4 fan-out purity rule.

The harness is faked (no SDK anywhere in the unit tier) but everything downstream of it is REAL: the
fake returns raw JSON and the job's own ``news_analyst.parse_output`` validator turns it into scores,
so the D7 per-item drop path (out-of-enum ``event_type``) is exercised end to end. The resolver is
the real :class:`EntityResolver` with a seeded alias map, because "the LLM never assigns a symbol"
is only meaningfully tested against the real ambiguity rules (§2.7 step 3).
"""

from __future__ import annotations

import asyncio
import json
import re
from datetime import date, datetime, timedelta

import pytest

from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.config import config_dir
from engine.core.enums import DegradeTier
from engine.datafeeds.news_pipeline import EntityResolver
from engine.intelligence.context import ContextAssembler
from engine.intelligence.governor import InvokeDecision
from engine.intelligence.harness import AgentDef, AgentResult
from engine.marketdata.store import MarketStore
from engine.ops.news_scoring import (
    SKIP_BATCH_THRESHOLD,
    SKIP_GOVERNOR,
    SKIP_OUTSIDE_WINDOWS,
    NewsScoringJob,
)

CAL_DIR = config_dir() / "calendar"

#: Real 2026 calendar dates (config/calendar/2026.yaml): Wed is a trading day, Sun is not.
WED = date(2026, 6, 17)
SUN = date(2026, 6, 14)

#: The §5.4 agents.yaml definition, built directly (agents.yaml loading is test_agent_defs' job).
NEWS_DEF = AgentDef(
    agent_id="news_analyst",
    model="haiku-4.5",
    shape="single_shot",
    tools_enabled=False,
    allowed_tools=[],
    max_output_tokens=2000,
)

_ID_RE = re.compile(r"cluster_id=(\S+)")


def at(d: date, hour: int, minute: int = 0) -> datetime:
    return datetime(d.year, d.month, d.day, hour, minute, tzinfo=IST)


# --------------------------------------------------------------------------- fakes
def score_item(cluster_id: str, **overrides) -> dict:
    """One well-formed §5.4 cluster score (the model's raw JSON item, pre-validation)."""
    item = {
        "cluster_id": cluster_id,
        "scope": "stock",
        "entities": [],
        "sectors": [],
        "themes": [],
        "sentiment": 0.5,
        "materiality": 0.7,
        "event_type": "order_win",
        "novelty": 0.8,
    }
    item.update(overrides)
    return item


class FakeHarness:
    """Returns one score per cluster it was actually sent, as RAW JSON through the real validator."""

    def __init__(self, *, overrides=None, fail_after=None, extra_items=()) -> None:
        self.calls: list[list[str]] = []
        self.contexts: list[object] = []
        self.json_schemas: list[object] = []
        self.call_classes: list[object] = []
        self._overrides = dict(overrides or {})
        self._fail_after = fail_after
        self._extra = list(extra_items)

    async def run_single_shot(
        self, agent_def, context, validate, *, json_schema=None, call_class=None
    ) -> AgentResult:
        ids = _ID_RE.findall(context.volatile_block)
        self.calls.append(ids)
        self.contexts.append(context)
        self.json_schemas.append(json_schema)
        self.call_classes.append(call_class)
        if self._fail_after is not None and len(self.calls) > self._fail_after:
            return AgentResult.Failed("timeout", "fake harness failure", call_id="call-failed")
        items = [{**score_item(cid), **self._overrides.get(cid, {})} for cid in ids]
        items.extend(self._extra)
        return AgentResult.Ok(
            call_id=f"call-{len(self.calls)}", payload=validate(json.dumps({"scores": items}))
        )


class FakeGovernor:
    """Admission + the §5.6 DG2 news cadence knob — the only two governor surfaces this job uses."""

    def __init__(self, *, allowed: bool = True, cadence_min: int = 30) -> None:
        self.allowed = allowed
        self.cadence_min = cadence_min
        self.decisions: list[tuple[str, str]] = []

    def can_invoke(self, agent_id: str, call_class: str = "schedule") -> InvokeDecision:
        self.decisions.append((agent_id, call_class))
        return InvokeDecision(
            allowed=self.allowed,
            tier=DegradeTier.DG0 if self.allowed else DegradeTier.DG3,
            reason=None if self.allowed else "DG3_intraday_llm_off",
        )

    def news_batch_cadence_min(self) -> int:
        return self.cadence_min


# --------------------------------------------------------------------------- fixtures
@pytest.fixture
def now_box() -> list[datetime]:
    """Mutable "now" — the time gate is the point of several tests."""
    return [at(WED, 10, 5)]


@pytest.fixture
def scoring_clock(now_box) -> Clock:
    return Clock(time_source=lambda: now_box[0])


@pytest.fixture
def store(tmp_path, scoring_clock):
    s = MarketStore(tmp_path / "market.duckdb", tmp_path / "parquet", scoring_clock)
    s.open()
    yield s
    s.close()


@pytest.fixture
def resolver() -> EntityResolver:
    """Real resolver: one unambiguous alias, one alias mapping to two symbols (⇒ never a match)."""
    return EntityResolver(
        aliases={
            "reliance": "RELIANCE",
            "tata motors": "TATAMOTORS",
            "bharti": ("BHARTIARTL", "BHARTIHEXA"),
        },
        universe={"RELIANCE", "TATAMOTORS", "BHARTIARTL", "BHARTIHEXA"},
    )


@pytest.fixture
def make_job(store, conn, resolver, scoring_clock):
    calendar = NSECalendar(CAL_DIR, scoring_clock, strict=False)
    assembler = ContextAssembler(store, conn, scoring_clock, calendar)

    def _make(harness, governor=None, store_override=None) -> NewsScoringJob:
        # ``store_override`` (2026-09-09) lets the lock tests wrap the real store in a probe; the
        # assembler keeps the real one — only the job's own hops are under test.
        return NewsScoringJob(
            store_override if store_override is not None else store,
            resolver,
            assembler,
            harness,
            {"news_analyst": NEWS_DEF},
            governor or FakeGovernor(),
            scoring_clock,
            calendar,
        )

    return _make


# --------------------------------------------------------------------------- seeding
def seed_cluster(
    store: MarketStore,
    cluster_id: str,
    *,
    first_seen: datetime,
    representative: str = "Company bags large order from state utility",
    symbols: tuple[str, ...] = (),
    sectors: tuple[str, ...] = (),
    themes: tuple[str, ...] = (),
    domains: tuple[str, ...] = ("economictimes.com", "moneycontrol.com"),
) -> str:
    """One UNSCORED cluster (step-2/step-3 columns only — scored_at NULL is what queues it)."""
    store.upsert_news_clusters([{
        "cluster_id": cluster_id,
        "representative": representative,
        "source_domains": list(domains),
        "first_seen": first_seen,
        "last_seen": first_seen,
        "scope": None,
        "entities": [],
        "symbols": list(symbols),
        "sectors": list(sectors),
        "themes": list(themes),
        "sentiment": None,
        "materiality": None,
        "event_type": None,
        "novelty": None,
        "scored_at": None,
        "scorer_model": None,
        "untrusted": True,
    }])
    return cluster_id


def seed_many(store: MarketStore, n: int, *, base: datetime) -> list[str]:
    """``n`` unscored clusters with strictly increasing first_seen (⇒ deterministic queue order)."""
    return [
        seed_cluster(store, f"c-{i:03d}", first_seen=base + timedelta(seconds=i)) for i in range(n)
    ]


def rows_by_id(store: MarketStore) -> dict[str, dict]:
    return {r["cluster_id"]: r for r in store.get_news_clusters()}


# --------------------------------------------------------------------------- batch triggers (§5.4)
async def test_seven_fresh_clusters_do_not_meet_the_batch_threshold(store, make_job, now_box):
    seed_many(store, 7, base=now_box[0] - timedelta(minutes=5))
    harness = FakeHarness()

    result = await make_job(harness).run_batch()

    assert result.skipped_reason == SKIP_BATCH_THRESHOLD
    assert result.scored == 0
    assert harness.calls == []                       # nothing assembled, nothing billed
    assert all(r["scored_at"] is None for r in rows_by_id(store).values())


async def test_eight_clusters_fire_one_batch(store, make_job, now_box):
    seed_many(store, 8, base=now_box[0] - timedelta(minutes=5))
    harness = FakeHarness()

    result = await make_job(harness).run_batch()

    assert result.skipped_reason is None
    assert result.scored == 8
    assert len(harness.calls) == 1 and len(harness.calls[0]) == 8
    assert harness.call_classes == ["batch"]         # the governor's admission key (§5.6)
    assert harness.json_schemas[0] is not None       # structured output declared (D5)
    assert all(r["scored_at"] is not None for r in rows_by_id(store).values())


async def test_oldest_cluster_older_than_the_cadence_triggers_a_batch(store, make_job, now_box):
    """One cluster is below the ≥8 minimum but has waited out the 30-min cadence (§5.4)."""
    seed_cluster(store, "c-old", first_seen=now_box[0] - timedelta(minutes=45))
    harness = FakeHarness()

    result = await make_job(harness).run_batch()

    assert result.scored == 1
    assert harness.calls == [["c-old"]]


async def test_cluster_younger_than_the_cadence_does_not_trigger(store, make_job, now_box):
    seed_cluster(store, "c-new", first_seen=now_box[0] - timedelta(minutes=29))
    harness = FakeHarness()

    result = await make_job(harness).run_batch()

    assert result.skipped_reason == SKIP_BATCH_THRESHOLD
    assert harness.calls == []


async def test_degraded_cadence_lengthens_the_age_trigger(store, make_job, now_box):
    """§5.6 DG2: the governor coarsens news batching to 60 min, so a 45-min wait is not yet due."""
    seed_cluster(store, "c-old", first_seen=now_box[0] - timedelta(minutes=45))
    harness = FakeHarness()

    result = await make_job(harness, FakeGovernor(cadence_min=60)).run_batch()

    assert result.skipped_reason == SKIP_BATCH_THRESHOLD
    assert harness.calls == []


# --------------------------------------------------------------------------- time gate (§5.4)
async def test_outside_the_scoring_windows_the_job_skips(store, make_job, now_box):
    now_box[0] = at(WED, 17, 0)                      # after the close, before the evening sweep
    seed_many(store, 10, base=now_box[0] - timedelta(minutes=5))
    harness = FakeHarness()

    result = await make_job(harness).run_batch()

    assert result.skipped_reason == SKIP_OUTSIDE_WINDOWS
    assert harness.calls == []


async def test_the_evening_sweep_window_scores(store, make_job, now_box):
    now_box[0] = at(WED, 20, 30)
    seed_many(store, 8, base=now_box[0] - timedelta(minutes=5))

    result = await make_job(FakeHarness()).run_batch()

    assert result.scored == 8


async def test_force_ignores_both_the_time_gate_and_the_threshold(store, make_job, now_box):
    """The pre-open batch (§5.4): fires at 08:15, scores ALL unscored regardless of the ≥8 minimum."""
    now_box[0] = at(SUN, 8, 15)                      # not a trading day, not a scoring window
    seed_cluster(store, "c-1", first_seen=now_box[0] - timedelta(minutes=2))
    harness = FakeHarness()
    job = make_job(harness)

    assert (await job.run_batch()).skipped_reason == SKIP_OUTSIDE_WINDOWS
    assert harness.calls == []

    forced = await job.run_batch(force=True)

    assert forced.skipped_reason is None
    assert forced.scored == 1
    assert harness.calls == [["c-1"]]


# --------------------------------------------------------------------------- chunking (§5.4 ≤30)
async def test_the_queue_is_chunked_at_thirty_clusters(store, make_job, now_box):
    ids = seed_many(store, 61, base=now_box[0] - timedelta(minutes=90))
    harness = FakeHarness()

    result = await make_job(harness).run_batch()

    assert [len(call) for call in harness.calls] == [30, 30, 1]
    assert harness.calls[0] == ids[:30]              # oldest first (get_news_clusters orders so)
    assert result.scored == 61


# --------------------------------------------------------------------------- fan-out purity (§3.2.4)
async def test_llm_sectors_and_themes_are_intersected_with_the_resolver_tags(
    store, make_job, now_box, scoring_clock
):
    """§3.2.4/§9.1: an LLM-emitted sector/theme absent from the resolver's tags causes NO fan-out."""
    store.upsert_theme_map([
        {"theme": "EV", "keywords": ["electric vehicle"], "symbols": ["TATAMOTORS"],
         "updated_at": scoring_clock.now()},
        {"theme": "DEFENCE", "keywords": ["defence"], "symbols": ["RELIANCE"],
         "updated_at": scoring_clock.now()},
    ])
    seed_cluster(
        store, "c-1", first_seen=now_box[0] - timedelta(minutes=45), sectors=("BANKS",), themes=("EV",)
    )
    harness = FakeHarness(
        overrides={"c-1": {"sectors": ["BANKS", "IT"], "themes": ["EV", "DEFENCE"]}}
    )

    result = await make_job(harness).run_batch()

    assert result.scored == 1
    row = rows_by_id(store)["c-1"]
    assert list(row["sectors"]) == ["BANKS"]         # "IT" was never a resolver tag -> dropped
    assert list(row["themes"]) == ["EV"]             # "DEFENCE" likewise
    # The in-prompt vocabulary is the theme_map keys (§5.4) — the model may emit nothing else.
    assert "EV" in harness.contexts[0].volatile_block


async def test_an_llm_sector_with_no_resolver_tag_persists_nothing(store, make_job, now_box):
    seed_cluster(store, "c-1", first_seen=now_box[0] - timedelta(minutes=45))   # no step-3 tags
    harness = FakeHarness(overrides={"c-1": {"sectors": ["BANKS"], "themes": ["EV"]}})

    await make_job(harness).run_batch()

    row = rows_by_id(store)["c-1"]
    assert list(row["sectors"]) == []
    assert list(row["themes"]) == []


# --------------------------------------------------------------------------- entities (§2.7 step 3)
async def test_llm_entity_strings_resolve_to_a_new_unambiguous_symbol(store, make_job, now_box):
    """The LLM never assigns a symbol: its verbatim strings re-enter the resolver (§3.2.4)."""
    seed_cluster(
        store,
        "c-1",
        first_seen=now_box[0] - timedelta(minutes=45),
        representative="Quarterly numbers impress the street",   # matches no alias
    )
    harness = FakeHarness(overrides={"c-1": {"entities": ["Reliance Industries"]}})

    await make_job(harness).run_batch()

    row = rows_by_id(store)["c-1"]
    assert list(row["symbols"]) == ["RELIANCE"]
    assert list(row["entities"]) == ["reliance"]     # the resolver's matched alias, not model prose


async def test_ambiguous_and_unmatched_entity_strings_add_no_symbol(store, make_job, now_box):
    seed_cluster(
        store,
        "c-1",
        first_seen=now_box[0] - timedelta(minutes=45),
        representative="Telecom operator raises tariffs across circles",
    )
    harness = FakeHarness(
        overrides={"c-1": {"entities": ["Bharti", "Some Unknown Startup"]}}
    )

    await make_job(harness).run_batch()

    row = rows_by_id(store)["c-1"]
    assert list(row["symbols"]) == []                # "bharti" maps to 2 symbols => never a guess
    # The unmatched string is recorded for the §5.5 weekly alias-suggestion loop.
    unresolved = {(r["entity_text"], r["reason"]) for r in store.get_unresolved_entities()}
    assert ("Some Unknown Startup", "no_match") in unresolved


# --------------------------------------------------------------------------- D7 drops
async def test_out_of_enum_event_type_drops_that_cluster_and_leaves_it_unscored(
    store, make_job, now_box
):
    base = now_box[0] - timedelta(minutes=45)
    seed_cluster(store, "c-good", first_seen=base)
    seed_cluster(store, "c-bad", first_seen=base + timedelta(seconds=1))
    harness = FakeHarness(overrides={"c-bad": {"event_type": "moon_landing"}})

    result = await make_job(harness).run_batch()

    assert result.dropped == ("c-bad",)
    assert result.scored == 1
    rows = rows_by_id(store)
    assert rows["c-good"]["scored_at"] is not None
    assert rows["c-bad"]["scored_at"] is None        # unscored => excluded from origination (§2.7)


async def test_a_score_for_an_unsent_cluster_id_is_ignored(store, make_job, now_box):
    seed_cluster(store, "c-1", first_seen=now_box[0] - timedelta(minutes=45))
    harness = FakeHarness(extra_items=[score_item("c-ghost")])

    result = await make_job(harness).run_batch()

    assert result.scored == 1
    assert set(rows_by_id(store)) == {"c-1"}         # the model cannot conjure a cluster row


# --------------------------------------------------------------------------- failure paths
async def test_governor_block_makes_no_call_and_leaves_the_queue_unscored(store, make_job, now_box):
    seed_many(store, 8, base=now_box[0] - timedelta(minutes=5))
    harness = FakeHarness()
    governor = FakeGovernor(allowed=False)

    result = await make_job(harness, governor).run_batch()

    assert result.skipped_reason == SKIP_GOVERNOR
    assert result.scored == 0
    assert harness.calls == []                       # zero SDK calls (§2.7 fail-safe: `cat` -> zero)
    assert all(r["scored_at"] is None for r in rows_by_id(store).values())
    assert governor.decisions == [("news_analyst", "batch")]


async def test_a_failed_chunk_keeps_the_earlier_chunks_and_leaves_the_rest_queued(
    store, make_job, now_box
):
    ids = seed_many(store, 61, base=now_box[0] - timedelta(minutes=90))
    harness = FakeHarness(fail_after=1)

    result = await make_job(harness).run_batch()

    assert len(harness.calls) == 2                   # chunk 2 failed; chunk 3 was never attempted
    assert result.scored == 30
    assert result.skipped_reason is None             # a partial run is not a skip
    rows = rows_by_id(store)
    assert all(rows[cid]["scored_at"] is not None for cid in ids[:30])
    assert all(rows[cid]["scored_at"] is None for cid in ids[30:])


# --------------------------------------------------------------------------- stamps
async def test_scored_at_and_scorer_model_are_platform_stamped(store, make_job, now_box):
    seed_cluster(store, "c-1", first_seen=now_box[0] - timedelta(minutes=45))
    harness = FakeHarness(overrides={"c-1": {"scope": "sector", "sentiment": -0.4, "novelty": 0.25}})

    await make_job(harness).run_batch()

    row = rows_by_id(store)["c-1"]
    assert row["scored_at"] == now_box[0]            # Clock.now(), tz-aware IST (§3.2)
    assert row["scorer_model"] == "haiku-4.5"        # the def's model NAME, not the API id
    assert row["scope"] == "sector"
    assert row["sentiment"] == pytest.approx(-0.4)
    assert row["materiality"] == pytest.approx(0.7)
    assert row["novelty"] == pytest.approx(0.25)
    assert row["event_type"] == "order_win"
    assert row["untrusted"] is True                  # §2.4: every LLM score stays untrusted evidence


# -------------------------------------------------------- chain-lock hold (§2.6 hardening ii, 09-09)
# The 2026-07-28 news_chain_lock serialises the news_clusters read-modify-write. Held around the
# WHOLE batch it also spanned one LLM await per chunk, so the per-feed polls (whose resolve deadline
# covers lock ACQUISITION) queued behind it — 14 news_resolve_timeout on 2026-09-07, nine inside one
# second. These tests pin the new discipline: store hops under the lock, model calls outside it.
class LockProbeStore:
    """Delegates to the real store, recording ``lock.locked()`` at every store hop the job makes."""

    def __init__(self, inner, lock) -> None:
        self._inner = inner
        self._lock = lock
        self.reads: list[bool] = []                  # locked-state at each get_news_clusters hop
        self.writes: list[bool] = []                 # ... at each write-back hop

    def __getattr__(self, name):
        return getattr(self._inner, name)

    async def arun(self, fn, /, *args, **kwargs):
        name = getattr(fn, "__name__", "")
        if name == "get_news_clusters":
            self.reads.append(self._lock.locked())
        elif name == "log_unresolved_entity":
            self.writes.append(self._lock.locked())
        return await self._inner.arun(fn, *args, **kwargs)

    async def aupsert_news_clusters(self, rows):
        self.writes.append(self._lock.locked())
        return await self._inner.aupsert_news_clusters(rows)


class LockProbeHarness(FakeHarness):
    """Records the lock state during the model call and can mutate the store mid-call (a rival poll)."""

    def __init__(self, lock, *, on_call=None, **kwargs) -> None:
        super().__init__(**kwargs)
        self._lock = lock
        self._on_call = on_call
        self.locked_during: list[bool] = []

    async def run_single_shot(self, agent_def, context, validate, **kwargs) -> AgentResult:
        self.locked_during.append(self._lock.locked())
        if self._on_call is not None:
            self._on_call()
        return await super().run_single_shot(agent_def, context, validate, **kwargs)


class LockGrabHarness(FakeHarness):
    """A concurrent per-feed poll: every chunk's call tries to TAKE the chain lock while in flight."""

    def __init__(self, lock, **kwargs) -> None:
        super().__init__(**kwargs)
        self._lock = lock
        self.grabbed: list[int] = []
        self.blocked: list[int] = []

    async def run_single_shot(self, agent_def, context, validate, **kwargs) -> AgentResult:
        chunk_no = len(self.calls)
        try:
            await asyncio.wait_for(self._lock.acquire(), 0.5)
        except TimeoutError:
            self.blocked.append(chunk_no)
        else:
            self.grabbed.append(chunk_no)
            self._lock.release()
        return await super().run_single_shot(agent_def, context, validate, **kwargs)


async def test_the_lock_covers_the_store_hops_but_never_the_model_call(store, make_job, now_box):
    seed_many(store, 2, base=now_box[0] - timedelta(minutes=45))
    lock = asyncio.Lock()
    probe = LockProbeStore(store, lock)
    # An unmatchable entity string makes _log_unmatched fire, so the §5.5 unresolved-entity hop —
    # the job's OTHER write, easy to leave outside the hold — is pinned under the lock too.
    harness = LockProbeHarness(lock, overrides={"c-000": {"entities": ["Some Unknown Startup"]}})

    result = await make_job(harness, store_override=probe).run_batch(lock=lock)

    assert result.scored == 2
    assert harness.locked_during == [False]          # the LLM await is OUTSIDE the hold
    assert probe.reads == [True, True]               # initial queue read + the write-back re-read
    assert probe.writes == [True, True]              # unresolved-entity log + upsert, both INSIDE
    assert not lock.locked()                         # released on the way out


async def test_a_cluster_deleted_mid_call_is_not_resurrected_by_the_write_back(
    store, make_job, now_box, caplog
):
    """A poll merging a cluster away during the model call must not get a zombie row written back."""
    base = now_box[0] - timedelta(minutes=45)
    seed_cluster(store, "c-keep", first_seen=base)
    seed_cluster(store, "c-gone", first_seen=base + timedelta(seconds=1))
    lock = asyncio.Lock()
    harness = LockProbeHarness(
        lock,
        # _execute: the pipeline's own merge path is not the subject here — only the row's absence is.
        on_call=lambda: store._execute(
            "DELETE FROM news_clusters WHERE cluster_id = ?", ["c-gone"]
        ),
    )

    with caplog.at_level("INFO", logger="engine.ops.news_scoring"):
        result = await make_job(harness).run_batch(lock=lock)

    assert result.scored == 1
    rows = rows_by_id(store)
    assert set(rows) == {"c-keep"}                   # never re-created
    assert rows["c-keep"]["scored_at"] is not None   # the survivor in the SAME chunk is written
    (dropped,) = [r for r in caplog.records if r.getMessage() == "news_score_writeback_dropped"]
    assert dropped.count == 1
    assert list(dropped.cluster_ids) == ["c-gone"]


async def test_a_cluster_rescored_mid_call_is_not_overwritten(store, make_job, now_box):
    """Re-scored between the read and the write-back ⇒ no longer unscored ⇒ this batch skips it."""
    base = now_box[0] - timedelta(minutes=45)
    seed_cluster(store, "c-keep", first_seen=base)
    seed_cluster(store, "c-rescored", first_seen=base + timedelta(seconds=1))
    lock = asyncio.Lock()
    harness = LockProbeHarness(
        lock,
        on_call=lambda: store._execute(
            "UPDATE news_clusters SET scored_at = ?, scorer_model = ? WHERE cluster_id = ?",
            [now_box[0], "rival-poll", "c-rescored"],
        ),
    )

    result = await make_job(harness).run_batch(lock=lock)

    rows = rows_by_id(store)
    assert result.scored == 1
    assert rows["c-rescored"]["scorer_model"] == "rival-poll"   # this batch did not clobber it
    assert rows["c-keep"]["scorer_model"] == "haiku-4.5"


async def test_a_poll_merge_during_the_model_call_survives_the_write_back(store, make_job, now_box):
    """The score columns are applied to the CURRENT row, never to the pre-LLM snapshot (09-09).

    The upsert overwrites every non-key column, so re-emitting the snapshot would REVERT whatever a
    per-feed poll merged in while the model was thinking — precisely the clobber the chain lock
    exists to prevent, reintroduced by shortening the hold.
    """
    base = now_box[0] - timedelta(minutes=45)
    seed_cluster(store, "c-1", first_seen=base, domains=("economictimes.com",))
    merged_at = base + timedelta(minutes=10)
    lock = asyncio.Lock()
    harness = LockProbeHarness(
        lock,
        overrides={"c-1": {"sectors": ["BANKS", "IT"], "themes": ["EV", "DEFENCE"]}},
        # A rival poll merges a second source, a fresher last_seen, a new headline and the step-3
        # tags it implies. _execute: the pipeline's own merge path is not the subject here.
        on_call=lambda: store._execute(
            "UPDATE news_clusters SET source_domains = ?, last_seen = ?, representative = ?, "
            "symbols = ?, sectors = ?, themes = ? WHERE cluster_id = ?",
            [
                ["economictimes.com", "livemint.com"],
                merged_at,
                "Tata Motors bags large order from state utility",
                ["TATAMOTORS"],
                ["BANKS"],
                ["EV"],
                "c-1",
            ],
        ),
    )

    result = await make_job(harness).run_batch(lock=lock)

    assert result.scored == 1
    row = rows_by_id(store)["c-1"]
    assert list(row["source_domains"]) == ["economictimes.com", "livemint.com"]
    assert row["last_seen"] == merged_at
    assert row["representative"].startswith("Tata Motors")
    assert list(row["symbols"]) == ["TATAMOTORS"]    # the poll's resolved symbol is not reverted
    assert list(row["sectors"]) == ["BANKS"]         # advisory ∩ the FRESH step-3 tags
    assert list(row["themes"]) == ["EV"]
    assert row["scored_at"] is not None              # ... and the step-4 columns still landed
    assert row["scorer_model"] == "haiku-4.5"


async def test_the_lock_is_free_during_every_chunk_of_a_multi_chunk_batch(store, make_job, now_box):
    seed_many(store, 61, base=now_box[0] - timedelta(minutes=90))
    lock = asyncio.Lock()
    harness = LockGrabHarness(lock)

    result = await make_job(harness).run_batch(lock=lock)

    assert len(harness.calls) == 3
    assert harness.grabbed == [0, 1, 2]              # a rival poll gets in between EVERY chunk
    assert harness.blocked == []
    assert result.scored == 61


async def test_without_a_lock_the_batch_still_does_the_conditional_reread(store, make_job, now_box):
    """``lock=None`` is no longer a distinct code path (2026-09-23): the job owns a private lock for
    the run and the write-back's conditional re-read always happens, so the queue is read TWICE (the
    initial read, then the write-back's re-read) even with no caller-supplied lock."""
    seed_many(store, 2, base=now_box[0] - timedelta(minutes=45))
    probe = LockProbeStore(store, asyncio.Lock())    # the lock is never handed to the job

    result = await make_job(FakeHarness(), store_override=probe).run_batch()

    assert result.scored == 2
    assert probe.reads == [False, False]             # initial read + the write-back's conditional reread
    assert probe.writes == [False]


async def test_a_stale_skip_is_excluded_from_the_chunk_failure_remaining_count(
    store, make_job, now_box, caplog
):
    """``remaining`` on ``news_scoring_chunk_failed`` must not count an id that already left the queue.

    Mirrors ``test_a_cluster_deleted_mid_call_is_not_resurrected_by_the_write_back`` (the stale
    mechanic: a cluster deleted during chunk 1's model call is skipped, not resurrected, at that
    chunk's write-back) combined with ``test_a_failed_chunk_keeps_the_earlier_chunks_and_leaves_the_
    rest_queued`` (chunk 2's model call fails). 61 clusters make three 30/30/1 chunks. Before the fix,
    ``remaining = len(clusters) - scored - len(dropped)`` still counts the deleted id as queued
    (61 - 29 - 0 = 32); the fix subtracts the stale count too (61 - 29 - 0 - 1 = 31), matching the
    actually-still-queued chunk 2 (30) + chunk 3 (1).
    """
    base = now_box[0] - timedelta(minutes=90)
    ids = seed_many(store, 61, base=base)
    lock = asyncio.Lock()
    harness = LockProbeHarness(
        lock,
        fail_after=1,                                 # chunk 1 succeeds, chunk 2 fails, chunk 3 unsent
        # _execute: the pipeline's own merge path is not the subject here — only the row's absence is.
        on_call=lambda: store._execute(
            "DELETE FROM news_clusters WHERE cluster_id = ?", [ids[0]]
        ),
    )

    with caplog.at_level("WARNING", logger="engine.ops.news_scoring"):
        result = await make_job(harness).run_batch(lock=lock)

    assert len(harness.calls) == 2                    # chunk 2 failed; chunk 3 was never attempted
    assert result.scored == 29                        # chunk 1's 30 sent, minus the 1 stale-skipped
    (failed_log,) = [r for r in caplog.records if r.getMessage() == "news_scoring_chunk_failed"]
    assert failed_log.remaining == 31                 # NOT 32: the stale id is not still queued


# ------------------------------------------------------ batch single-flight (§2.6 hardening ii, 09-09)
async def test_two_concurrent_batches_send_each_cluster_to_the_model_exactly_once(
    store, make_job, now_box
):
    """job_news_chain's forced batch and the 300 s scoring_tick are DISTINCT scheduler jobs (09-09).

    Now that the chain lock is dropped across the model call, nothing else keeps them apart: both
    would read the same unscored queue and bill the same clusters twice. The job's own run lock makes
    the second WAIT, so it re-reads an emptied queue and returns cheaply. Both calls pass the SAME
    ``news_chain_lock`` (as the two real scheduler jobs would), so this exercises the shipped
    combination — chain lock + concurrent batches + conditional write-back — not just the run lock in
    isolation.
    """
    ids = seed_many(store, 8, base=now_box[0] - timedelta(minutes=5))
    harness = FakeHarness()
    job = make_job(harness)
    chain_lock = asyncio.Lock()

    first, second = await asyncio.gather(
        job.run_batch(force=True, lock=chain_lock), job.run_batch(force=True, lock=chain_lock)
    )

    sent = [cid for call in harness.calls for cid in call]
    assert len(harness.calls) == 1                   # one batch reached the SDK, not two
    assert sorted(sent) == ids                       # every cluster sent exactly once
    assert first.scored + second.scored == 8
    assert min(first.scored, second.scored) == 0     # the waiter found the queue emptied
    assert all(r["scored_at"] is not None for r in rows_by_id(store).values())
