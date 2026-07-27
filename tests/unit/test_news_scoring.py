"""NewsScoringJob (§5.4 / §2.7 step 4) — the batch triggers and the §3.2.4 fan-out purity rule.

The harness is faked (no SDK anywhere in the unit tier) but everything downstream of it is REAL: the
fake returns raw JSON and the job's own ``news_analyst.parse_output`` validator turns it into scores,
so the D7 per-item drop path (out-of-enum ``event_type``) is exercised end to end. The resolver is
the real :class:`EntityResolver` with a seeded alias map, because "the LLM never assigns a symbol"
is only meaningfully tested against the real ambiguity rules (§2.7 step 3).
"""

from __future__ import annotations

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

    def _make(harness, governor=None) -> NewsScoringJob:
        return NewsScoringJob(
            store,
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
