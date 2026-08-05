"""CatalystDigestJob (§2.7 step 5 / §3.2.4 / §4.4 job 14) — the deterministic heart of the news layer.

Covers the two pinned outputs and every rule that can license origination: the decay-weighted SUM
(§2.7 step 5(i)) at its half-life boundaries, the AND-list per condition (§2.7 step 5(ii)), the
fan-out weight applied to materiality AND |sentiment| BEFORE the thresholds, trading-SESSION event
age (Friday event ⇒ age 1 on Monday, O13), the T+1 PEAD sign agreement, the inclusion floor, the
§6.1 deterministic levels against a hand-computed ATR, and the fail-safe ladder (unscored ⇒
empty-but-FRESH; only stale/missing disables `cat`).
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest
import yaml

from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.config import config_dir
from engine.core.enums import Actor
from engine.core.protected_store import ProtectedStore
from engine.core.types import OwnerConfirmation
from engine.datafeeds.news_pipeline import (
    INCLUSION_FLOOR,
    CatalystDigestJob,
    originating_conditions,
)
from engine.marketdata.store import DailyBar, MarketStore

CAL_DIR = config_dir() / "calendar"
OWNER_OK = OwnerConfirmation(actor=Actor.OWNER, confirmed=True, note="test seed")

# Real 2026 calendar dates (config/calendar/2026.yaml — no June holiday before the 26th).
FRI = date(2026, 6, 12)
MON = date(2026, 6, 15)
TUE = date(2026, 6, 16)
WED = date(2026, 6, 17)

#: The shipped §7.1 `catalyst_guard` block (config/limits.yaml), verbatim.
GUARD = {
    "min_source_domains": 2,
    "sentiment_min_long": 0.30,
    "max_catalyst_entries_day": 2,
    "digest_stale_max_h": 20,
    "originating_event_types": [
        "earnings_result", "earnings_guidance", "order_win", "m_and_a",
        "regulatory_policy", "govt_program", "sector_policy", "rating_change",
    ],
}


def at(d: date, hour: int = 8, minute: int = 35) -> datetime:
    return datetime(d.year, d.month, d.day, hour, minute, tzinfo=IST)


# --------------------------------------------------------------------------- fixtures
@pytest.fixture
def now_box() -> list[datetime]:
    """Mutable "now" so a test can advance the clock between runs (staleness, multi-day age)."""
    return [at(WED)]


@pytest.fixture
def digest_clock(now_box) -> Clock:
    return Clock(time_source=lambda: now_box[0])


@pytest.fixture
def store(tmp_path, digest_clock):
    s = MarketStore(tmp_path / "market.duckdb", tmp_path / "parquet", digest_clock)
    s.open()
    yield s
    s.close()


@pytest.fixture
def make_job(store, conn, tmp_path, digest_clock):
    def _make(*, envelope=None, guard=None):
        cfg = tmp_path / "config"
        cfg.mkdir(exist_ok=True)
        (cfg / "limits.yaml").write_text(
            yaml.safe_dump({"schema_version": 1, "limits": {"catalyst_guard": guard or GUARD}}),
            encoding="utf-8",
        )
        protected = ProtectedStore(cfg, conn, digest_clock)
        protected.register_initial("limits.yaml", OWNER_OK)   # hash-verified load path (§2.4)
        calendar = NSECalendar(CAL_DIR, digest_clock, strict=False)
        return CatalystDigestJob(store, digest_clock, calendar, protected, envelope=envelope)

    return _make


# --------------------------------------------------------------------------- seeding helpers
def seed_cluster(
    store: MarketStore,
    cluster_id: str,
    *,
    first_seen: datetime,
    sentiment: float = 0.60,
    materiality: float = 0.80,
    scope: str = "stock",
    symbols: tuple[str, ...] = (),
    sectors: tuple[str, ...] = (),
    themes: tuple[str, ...] = (),
    event_type: str | None = "order_win",
    novelty: float = 0.80,
    domains: tuple[str, ...] = ("economictimes.com", "moneycontrol.com"),
    scored: bool = True,
) -> None:
    store.upsert_news_clusters([{
        "cluster_id": cluster_id,
        "representative": f"headline {cluster_id}",
        "source_domains": list(domains),
        "first_seen": first_seen,
        "last_seen": first_seen,
        "scope": scope,
        "entities": [],
        "symbols": list(symbols),
        "sectors": list(sectors),
        "themes": list(themes),
        "sentiment": sentiment,
        "materiality": materiality,
        "event_type": event_type,
        "novelty": novelty,
        "scored_at": first_seen if scored else None,
        "scorer_model": "haiku-4.5" if scored else None,
        "untrusted": True,
    }])


def seed_universe(store: MarketStore, d: date, symbols: tuple[str, ...]) -> None:
    store.upsert_universe_daily([{"d": d, "symbol": s, "included": True} for s in symbols])


def seed_bars(
    store: MarketStore,
    symbol: str,
    d: date,
    *,
    n: int = 20,
    close: Decimal = Decimal("250.13"),
    spread: Decimal = Decimal("2.50"),
) -> None:
    """``n`` identical weekday bars ending the weekday before ``d`` — constant TR ⇒ ATR == 2×spread."""
    bars: list[DailyBar] = []
    probe = d - timedelta(days=1)
    while len(bars) < n:
        if probe.weekday() < 5:
            bars.append(DailyBar(
                symbol=symbol, d=probe, open=close, high=close + spread,
                low=close - spread, close=close, volume=100_000,
            ))
        probe -= timedelta(days=1)
    store.upsert_bars_1d(bars)


def sentiment_map(store: MarketStore, ran_at: datetime) -> dict[tuple[str, str], float]:
    return {(r["scope"], r["scope_key"]): r["value"] for r in store.get_sentiment_agg(ran_at)}


def seed_qualifying(store: MarketStore, **cluster_kwargs) -> None:
    """A single cluster that satisfies EVERY §2.7 step-5(ii) condition for ACME at the WED digest."""
    seed_universe(store, WED, ("ACME",))
    seed_bars(store, "ACME", WED)
    kwargs = {"first_seen": at(TUE, 18, 0), "symbols": ("ACME",)}
    kwargs.update(cluster_kwargs)
    seed_cluster(store, "c-1", **kwargs)


# --------------------------------------------------------------------------- step 5(i): decay SUM
async def test_decay_at_halflife_boundaries(store, make_job):
    """0.5^(age/half-life) exactly at 0/1/2/6 half-lives; > 6× half-life drops out entirely."""
    ran = at(WED)
    for symbol, hours in (("AAA", 0), ("BBB", 24), ("CCC", 48), ("DDD", 144)):
        seed_cluster(
            store, f"c-{symbol}", first_seen=ran - timedelta(hours=hours),
            sentiment=0.5, materiality=1.0, symbols=(symbol,),
        )
    seed_cluster(                                     # one minute past 6× the half-life
        store, "c-EEE", first_seen=ran - timedelta(hours=144, minutes=1),
        sentiment=0.5, materiality=1.0, symbols=("EEE",),
    )

    result = await make_job().run(WED)
    values = sentiment_map(store, result.ran_at)

    assert values[("symbol", "AAA")] == pytest.approx(0.5)
    assert values[("symbol", "BBB")] == pytest.approx(0.25)
    assert values[("symbol", "CCC")] == pytest.approx(0.125)
    assert values[("symbol", "DDD")] == pytest.approx(0.5 * 0.5 ** 6)
    assert ("symbol", "EEE") not in values


async def test_decayed_sum_is_clipped_to_unit_interval(store, make_job):
    """A decayed SUM can exceed ±1 — the §2.7 formula clips, so |value| ≤ 1 always."""
    ran = at(WED)
    for i in range(5):
        seed_cluster(store, f"c-up-{i}", first_seen=ran, sentiment=0.9, materiality=1.0, symbols=("UP",))
        seed_cluster(store, f"c-dn-{i}", first_seen=ran, sentiment=-0.9, materiality=1.0, symbols=("DN",))

    result = await make_job().run(WED)
    values = sentiment_map(store, result.ran_at)

    assert values[("symbol", "UP")] == 1.0
    assert values[("symbol", "DN")] == -1.0
    assert all(abs(v) <= 1.0 for v in values.values())


async def test_sum_not_mean_and_old_news_decays_to_zero(store, make_job, now_box):
    """Two fresh clusters ADD (0.8, not the 0.4 a mean would give); the same pair aged 5 half-lives
    collapses toward neutral — the property a weighted mean could never have."""
    ran = at(WED)
    for i in range(2):
        seed_cluster(store, f"c-{i}", first_seen=ran, sentiment=0.4, materiality=1.0, symbols=("ACME",))
    fresh = await make_job().run(WED)
    assert sentiment_map(store, fresh.ran_at)[("symbol", "ACME")] == pytest.approx(0.8)

    now_box[0] = ran + timedelta(hours=120)           # 5 half-lives later, same two clusters
    aged = await make_job().run(WED)
    aged_value = sentiment_map(store, aged.ran_at)[("symbol", "ACME")]
    assert aged_value == pytest.approx(2 * 0.4 * 0.5 ** 5)
    assert aged_value < 0.05


# --------------------------------------------------------------------------- step 5(ii): AND-list
BASE_CONDITIONS = {
    "weighted_materiality": 0.80,
    "weighted_sentiment": 0.60,
    "event_type": "order_win",
    "source_domain_count": 2,
    "novelty": 0.80,
    "in_universe": True,
    "flagged": False,
    "results_day_t": False,
    "earnings_reaction_agrees": True,
    "materiality_min": 0.70,
    "novelty_min": 0.50,
    "guard": GUARD,
}


def test_all_conditions_true_is_originating():
    assert all(originating_conditions(**BASE_CONDITIONS).values())


@pytest.mark.parametrize(
    ("failing_key", "override"),
    [
        ("materiality", {"weighted_materiality": 0.69}),
        ("sentiment_long", {"weighted_sentiment": 0.29}),
        ("event_type", {"event_type": "pump_promo_suspect"}),
        ("event_type", {"event_type": None}),
        ("source_domains", {"source_domain_count": 1}),
        ("novelty", {"novelty": 0.49}),
        ("novelty", {"novelty": None}),
        ("in_universe", {"in_universe": False}),
        ("not_flagged", {"flagged": True}),
        ("not_results_day", {"results_day_t": True}),
        ("earnings_reaction", {"earnings_reaction_agrees": False}),
    ],
)
def test_each_condition_alone_blocks_origination(failing_key: str, override: dict):
    conditions = originating_conditions(**{**BASE_CONDITIONS, **override})
    assert conditions[failing_key] is False
    assert all(v for k, v in conditions.items() if k != failing_key)
    assert not all(conditions.values())


def test_truncated_guard_block_originates_nothing():
    """A `catalyst_guard` missing `originating_event_types` fails to LESS activity, never more."""
    conditions = originating_conditions(**{**BASE_CONDITIONS, "guard": {"min_source_domains": 2}})
    assert conditions["event_type"] is False
    assert conditions["sentiment_long"] is True          # falls back to the pinned +0.30 floor


# --------------------------------------------------------------------------- corroboration (§7.1)
@pytest.mark.parametrize(("n_domains", "grade"), [(1, "context"), (2, "originating"), (3, "originating")])
async def test_single_source_cluster_can_never_originate(store, make_job, n_domains: int, grade: str):
    domains = ("economictimes.com", "moneycontrol.com", "livemint.com")[:n_domains]
    seed_qualifying(store, domains=domains)

    await make_job().run(WED)
    rows = store.get_catalyst_watchlist(WED)

    assert [r["grade"] for r in rows] == [grade]
    assert rows[0]["source_domain_count"] == n_domains


async def test_story_level_union_corroborates_across_single_domain_clusters(store, make_job):
    """§2.7 step 5(ii) 2026-08-05 amendment: cross-outlet paraphrase never merges under the pinned
    §3.2.4 similarity (0/1,400 live pairs ≥ 0.75), so corroboration counts the domain UNION across
    age-eligible clusters for the same (symbol, event_type). Two single-domain clusters, different
    outlets ⇒ originating; the ITC/MARUTI/BEL results shape from the 2026-08-05 research."""
    seed_qualifying(store, domains=("economictimes.indiatimes.com",))
    seed_cluster(
        store, "c-2", first_seen=at(TUE, 19, 0), symbols=("ACME",),
        materiality=0.75, domains=("livemint.com",),
    )

    await make_job().run(WED)
    rows = store.get_catalyst_watchlist(WED)

    assert [r["grade"] for r in rows] == ["originating"]
    assert rows[0]["source_domain_count"] == 2
    # §6.5 audit: best cluster (higher weighted materiality) FIRST, then the corroborator.
    assert list(rows[0]["cluster_refs"]) == ["c-1", "c-2"]


async def test_event_type_disagreement_loses_the_corroboration(store, make_job):
    """The union is keyed on (symbol, event_type): outlets typing the story differently fail to
    LESS activity — single-domain count, context grade, no corroborator in cluster_refs."""
    seed_qualifying(store, domains=("economictimes.indiatimes.com",))
    seed_cluster(
        store, "c-2", first_seen=at(TUE, 19, 0), symbols=("ACME",),
        materiality=0.75, event_type="earnings_result", domains=("livemint.com",),
    )

    await make_job().run(WED)
    rows = store.get_catalyst_watchlist(WED)

    assert [r["grade"] for r in rows] == ["context"]          # source_domains blocks
    assert rows[0]["source_domain_count"] == 1
    assert list(rows[0]["cluster_refs"]) == ["c-1"]


async def test_below_inclusion_floor_cluster_still_corroborates(store, make_job):
    """A tiny follow-up mention (weighted materiality < the 0.2 inclusion floor) makes no row of
    its own but IS a corroborating publication — its domain counts toward the story union."""
    seed_qualifying(store, domains=("economictimes.indiatimes.com",))
    seed_cluster(
        store, "c-2", first_seen=at(TUE, 19, 0), symbols=("ACME",),
        materiality=0.10, domains=("livemint.com",),
    )

    await make_job().run(WED)
    rows = store.get_catalyst_watchlist(WED)

    assert [r["symbol"] for r in rows] == ["ACME"]            # one row: the floor still binds c-2
    assert rows[0]["grade"] == "originating"
    assert rows[0]["source_domain_count"] == 2
    assert list(rows[0]["cluster_refs"]) == ["c-1", "c-2"]


async def test_fanout_cluster_corroborates_constituent_story(store, make_job):
    """A sector cluster from a second outlet corroborates a constituent's stock story of the same
    event_type — `_symbol_targets` semantics, consistent with candidacy."""
    seed_universe(store, WED, ("ACME",))
    seed_bars(store, "ACME", WED)
    store.upsert_sector_map(WED, [{"symbol": "ACME", "sector": "METALS"}])
    seed_cluster(store, "c-1", first_seen=at(TUE, 18, 0), symbols=("ACME",),
                 domains=("economictimes.indiatimes.com",))
    seed_cluster(
        store, "c-2", first_seen=at(TUE, 19, 0), scope="sector", sectors=("METALS",),
        materiality=0.60, domains=("livemint.com",),
    )

    await make_job().run(WED)
    rows = store.get_catalyst_watchlist(WED)

    assert [r["symbol"] for r in rows] == ["ACME"]
    assert rows[0]["source_domain_count"] == 2
    assert rows[0]["grade"] == "originating"
    assert list(rows[0]["cluster_refs"]) == ["c-1", "c-2"]


# --------------------------------------------------------------------------- fan-out weighting
@pytest.mark.parametrize(
    ("scope", "materiality", "sentiment", "grade", "stored_materiality"),
    [
        ("stock", 0.80, 0.32, "originating", 0.80),   # weight 1: both thresholds clear
        ("sector", 0.80, 0.32, "context", 0.72),      # 0.32*0.9 = 0.288 < 0.30 => sentiment blocks
        ("sector", 0.75, 0.50, "context", 0.675),     # 0.75*0.9 = 0.675 < 0.70 => materiality blocks
    ],
)
async def test_fanout_weight_multiplies_materiality_and_sentiment_pre_threshold(
    store, make_job, scope: str, materiality: float, sentiment: float, grade: str, stored_materiality: float
):
    """`cat.fanout_weight` scales BOTH inputs before the comparison — each leg blocks on its own."""
    seed_universe(store, WED, ("ACME",))
    seed_bars(store, "ACME", WED)
    store.upsert_sector_map(WED, [{"symbol": "ACME", "sector": "METALS"}])
    seed_cluster(
        store, "c-1", first_seen=at(TUE, 18, 0), scope=scope, sentiment=sentiment,
        materiality=materiality,
        symbols=("ACME",) if scope == "stock" else (),
        sectors=("METALS",) if scope == "sector" else (),
    )

    await make_job(envelope={"cat.fanout_weight": 0.9}).run(WED)
    rows = store.get_catalyst_watchlist(WED)

    assert [r["symbol"] for r in rows] == ["ACME"]
    assert rows[0]["grade"] == grade
    assert rows[0]["materiality"] == pytest.approx(stored_materiality)


async def test_fanout_weight_scales_the_symbol_row_but_not_the_sector_row(store, make_job):
    """§2.7 step 5(i): wᵢ = cat.fanout_weight only for a sector cluster's SYMBOL contribution."""
    seed_universe(store, WED, ("ACME",))
    store.upsert_sector_map(WED, [{"symbol": "ACME", "sector": "METALS"}])
    seed_cluster(
        store, "c-1", first_seen=at(WED), scope="sector", sentiment=0.8, materiality=1.0,
        sectors=("METALS",),
    )

    result = await make_job(envelope={"cat.fanout_weight": 0.5}).run(WED)
    values = sentiment_map(store, result.ran_at)

    assert values[("sector", "METALS")] == pytest.approx(0.8)
    assert values[("symbol", "ACME")] == pytest.approx(0.4)


# --------------------------------------------------------------------------- session age (R6/O13)
def test_friday_evening_event_is_age_one_on_monday(make_job):
    job = make_job()
    friday_evening = at(FRI, 18, 30)
    assert job.event_age_sessions(friday_evening, FRI) == 0     # same day: not yet a session old
    assert job.event_age_sessions(friday_evening, MON) == 1     # weekend skipped (T+1 PEAD alive)
    assert job.event_age_sessions(friday_evening, TUE) == 2
    assert job.event_age_sessions(friday_evening, WED) == 3
    assert job.event_age_sessions(at(date(2026, 6, 13), 11, 0), MON) == 1   # Saturday event


async def test_friday_event_stays_eligible_until_it_ages_out(store, make_job, now_box):
    """Unconfirmed eligibility PERSISTS across days and then ages out — sentiment_agg keeps it."""
    seed_universe(store, MON, ("ACME",))
    seed_universe(store, WED, ("ACME",))
    seed_bars(store, "ACME", MON)
    seed_bars(store, "ACME", WED)
    seed_cluster(store, "c-1", first_seen=at(FRI, 18, 30), symbols=("ACME",))

    now_box[0] = at(MON)
    monday = await make_job().run(MON)
    rows = store.get_catalyst_watchlist(MON)
    assert monday.n_originating == 1
    assert rows[0]["event_age_sessions"] == 1
    assert rows[0]["expires_at"] == WED           # first day the session age exceeds 2

    now_box[0] = at(WED)
    wednesday = await make_job().run(WED)
    assert (wednesday.n_originating, wednesday.n_context) == (0, 0)
    assert store.get_catalyst_watchlist(WED) == []
    assert ("symbol", "ACME") in sentiment_map(store, wednesday.ran_at)   # still feeds sentiment


# --------------------------------------------------------------------------- O13 PEAD agreement
@pytest.mark.parametrize(
    ("open_t", "close_t", "sentiment", "grade"),
    [
        (Decimal("100.00"), Decimal("105.00"), 0.60, "originating"),   # up day + positive surprise
        (Decimal("100.00"), Decimal("95.00"), 0.60, "context"),        # reaction disagrees
        (Decimal("100.00"), Decimal("100.00"), 0.60, "context"),       # sign 0 => never eligible
    ],
)
async def test_earnings_t_plus_one_requires_reaction_sign_agreement(
    store, make_job, open_t: Decimal, close_t: Decimal, sentiment: float, grade: str
):
    seed_qualifying(store, event_type="earnings_result", sentiment=sentiment)
    store.upsert_earnings_calendar([{"symbol": "ACME", "event_date": TUE, "kind": "results"}])
    store.upsert_bars_1d([DailyBar(
        symbol="ACME", d=TUE, open=open_t, high=max(open_t, close_t),
        low=min(open_t, close_t), close=close_t, volume=100_000,
    )])

    await make_job().run(WED)
    rows = store.get_catalyst_watchlist(WED)

    assert [r["grade"] for r in rows] == [grade]


async def test_results_day_t_is_never_originating(store, make_job):
    """Day T stays banned (`no_entry_on_results_day`); only T+1 onward can originate (O13)."""
    seed_qualifying(store, event_type="earnings_result")
    store.upsert_earnings_calendar([{"symbol": "ACME", "event_date": WED, "kind": "results"}])

    await make_job().run(WED)

    assert [r["grade"] for r in store.get_catalyst_watchlist(WED)] == ["context"]


# --------------------------------------------------------------------------- inclusion floor
async def test_inclusion_floor_keeps_noise_out_of_the_watchlist(store, make_job):
    """Below 0.2 weighted materiality: sentiment_agg only, no row. At/above: a `context` row."""
    seed_universe(store, WED, ("NOISE", "WEAK"))
    seed_cluster(store, "c-noise", first_seen=at(TUE, 18, 0), materiality=0.15, symbols=("NOISE",))
    seed_cluster(store, "c-weak", first_seen=at(TUE, 18, 0), materiality=0.25, symbols=("WEAK",))

    result = await make_job().run(WED)
    rows = {r["symbol"]: r for r in store.get_catalyst_watchlist(WED)}
    values = sentiment_map(store, result.ran_at)

    assert set(rows) == {"WEAK"}
    assert rows["WEAK"]["grade"] == "context"
    assert INCLUSION_FLOOR == 0.2
    assert ("symbol", "NOISE") in values          # excluded from the watchlist, NOT from sentiment


# --------------------------------------------------------------------------- fail-safe ladder
async def test_unscored_clusters_are_excluded_entirely(store, make_job):
    """A dead scorer yields an EMPTY-but-FRESH digest, never a watchlist from unscored clusters."""
    seed_universe(store, WED, ("ACME",))
    seed_bars(store, "ACME", WED)
    seed_cluster(
        store, "c-1", first_seen=at(TUE, 18, 0), symbols=("ACME",),
        sentiment=1.0, materiality=1.0, scored=False,
    )

    job = make_job()
    result = await job.run(WED)

    assert (result.n_originating, result.n_context) == (0, 0)
    assert store.get_catalyst_watchlist(WED) == []
    assert sentiment_map(store, result.ran_at) == {("market", "market"): 0.0}
    assert job.digest_status(WED) == "fresh"


async def test_empty_corpus_is_zero_zero_and_fresh(store, make_job):
    job = make_job()
    result = await job.run(WED)

    assert (result.n_originating, result.n_context) == (0, 0)
    assert result.sentiment_rows == 1              # the market row is the "digest ran" marker
    assert job.digest_status(WED) == "fresh"


async def test_digest_status_missing_stale_fresh(store, make_job, now_box):
    job = make_job()
    assert job.digest_status(WED) == "missing"

    await job.run(WED)
    assert job.digest_status(WED) == "fresh"

    now_box[0] = at(WED) + timedelta(hours=21)     # > catalyst_guard.digest_stale_max_h (20)
    assert job.digest_status(WED) == "stale"


# --------------------------------------------------------------------------- §6.1 levels
async def test_deterministic_levels_worked_example(store, make_job):
    """20 identical bars (close 250.13, ±2.50) ⇒ TR == 5.00 every bar ⇒ ATR(14) == 5.00 exactly.

    trigger = 250.13 × 1.01 = 252.6313 → 252.63; stop = 252.63 − 1.5×5.00 = 245.13;
    target = 252.63 + 1.5×(252.63 − 245.13) = 263.88; invalidation = prior close.
    """
    seed_qualifying(store)

    await make_job().run(WED)
    row = store.get_catalyst_watchlist(WED)[0]

    assert row["grade"] == "originating"
    assert row["confirm_trigger"] == Decimal("252.63")
    assert row["invalidation"] == Decimal("250.13")
    assert row["stop_band_low"] == row["stop_band_high"] == Decimal("245.13")
    assert row["target_band_low"] == row["target_band_high"] == Decimal("263.88")


async def test_context_rows_carry_no_levels(store, make_job):
    seed_qualifying(store, event_type="pump_promo_suspect")

    await make_job().run(WED)
    row = store.get_catalyst_watchlist(WED)[0]

    assert row["grade"] == "context"
    assert row["confirm_trigger"] is None
    assert row["stop_band_low"] is None
    assert row["target_band_high"] is None


async def test_thin_daily_history_nulls_levels_without_blocking_the_grade(store, make_job):
    """Fewer than 15 bars ⇒ levels NULL + logged; the grade itself is never blocked (E5)."""
    seed_universe(store, WED, ("ACME",))
    seed_bars(store, "ACME", WED, n=10)
    seed_cluster(store, "c-1", first_seen=at(TUE, 18, 0), symbols=("ACME",))

    result = await make_job().run(WED)
    row = store.get_catalyst_watchlist(WED)[0]

    assert result.n_originating == 1
    assert row["grade"] == "originating"
    assert row["confirm_trigger"] is None
    assert row["invalidation"] is None


# --------------------------------------------------------------------------- wiring / persistence
async def test_universe_and_flagged_exclusions_are_wired(store, make_job):
    """Two store-sourced conditions that no pure-function test can prove: universe + block deals."""
    seed_universe(store, WED, ("ACME", "FLAGGED"))
    for symbol in ("ACME", "FLAGGED", "OFFUNIVERSE"):
        seed_bars(store, symbol, WED)
        seed_cluster(store, f"c-{symbol}", first_seen=at(TUE, 18, 0), symbols=(symbol,))
    store.upsert_flagged_instrument_days([{"symbol": "FLAGGED", "d": WED, "reason": "block_deal"}])

    await make_job().run(WED)
    grades = {r["symbol"]: r["grade"] for r in store.get_catalyst_watchlist(WED)}

    assert grades == {"ACME": "originating", "FLAGGED": "context", "OFFUNIVERSE": "context"}


async def test_rerun_replaces_the_day_and_carries_cluster_refs(store, make_job):
    """Run-latest catch-up (§2.6): a second run for the same day never duplicates rows."""
    seed_qualifying(store)
    job = make_job()

    await job.run(WED)
    first = store.get_catalyst_watchlist(WED)
    await job.run(WED)
    second = store.get_catalyst_watchlist(WED)

    assert len(first) == len(second) == 1
    assert second[0]["cluster_refs"] == ["c-1"]
    assert second[0]["direction"] == "long"
    assert second[0]["entry_id"] != first[0]["entry_id"]
