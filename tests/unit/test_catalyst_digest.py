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


# ------------------------------------------------------- best-cluster selection (recency-decayed)
# The 2026-08-27 HINDZINC fix. "Best" used to mean highest RAW weighted materiality, so a stale
# cluster held the symbol's slot for its whole `cat.max_event_age_days` life no matter what
# followed it. Ranking now applies step 5(i)'s own decay — 0.5^(age_h / cat.decay_halflife_h),
# half-life 24h by default — to the COMPARISON key only.
#
# Every rank below is hand-computed against ran_at = WED 08:35 IST (the `now_box` default):
#   MON 04:00 -> age 52.5833h -> 0.5^2.190972 = 0.21899
#   MON 10:00 -> age 46.5833h -> 0.5^1.940972 = 0.26043
#   TUE 11:00 -> age 21.5833h -> 0.5^0.899306 = 0.53614
#   TUE 18:00 -> age 14.5833h -> 0.5^0.607639 = 0.65625
#   WED 08:00 -> age  0.5833h -> 0.5^0.024306 = 0.98329
#   WED 08:30 -> age  0.0833h -> 0.5^0.003472 = 0.99759
async def test_fresher_cluster_overtakes_a_stale_higher_materiality_one(store, make_job):
    """HINDZINC, 2026-08-27 — the bug this selection rule exists to prevent.

    A `regulatory_policy` cluster (Aug 24-25: government stake-sale/OFS speculation, direction
    SHORT, materiality 0.6) was followed by DIPAM officially denying any stake-sale plan (Aug 26;
    the stock rallied +5.1%). ``sentiment_agg`` — which decays — netted correctly positive
    (+0.569), but the discrete watchlist row still read ``direction: short`` and fed a live
    recommendation with already-resolved information framed as current, because the original
    cluster's RAW materiality was the higher of the two forever.

    Ranks: stale 0.60 x 0.26043 = 0.1563 vs fresh 0.50 x 0.53614 = 0.2681 -> the denial wins.
    Under the old rule the comparison was 0.60 vs 0.50 on raw materiality, so the stale
    speculation won and this test would assert `short` / c-stale / 0.60 on every line below.
    """
    seed_universe(store, WED, ("ACME",))
    seed_bars(store, "ACME", WED)
    seed_cluster(
        store, "c-stale", first_seen=at(MON, 10, 0), symbols=("ACME",),
        event_type="regulatory_policy", sentiment=-0.55, materiality=0.60,
    )
    seed_cluster(
        store, "c-fresh", first_seen=at(TUE, 11, 0), symbols=("ACME",),
        event_type="regulatory_policy", sentiment=0.50, materiality=0.50,
    )

    await make_job().run(WED)
    rows = store.get_catalyst_watchlist(WED)

    assert [r["symbol"] for r in rows] == ["ACME"]
    assert rows[0]["direction"] == "long"                     # the denial, not the speculation
    assert list(rows[0]["cluster_refs"]) == ["c-fresh", "c-stale"]   # best FIRST (§6.5 audit)
    # The STORED materiality is the winner's UNDECAYED weighted value — decay ranks, never labels.
    assert rows[0]["materiality"] == pytest.approx(0.50)
    assert rows[0]["event_age_h"] == pytest.approx(21.58333, abs=1e-4)
    assert rows[0]["event_age_sessions"] == 1


# ------------------------------------------------------------------ story-level REVERSAL detection
# §2.7 `cat_reversal` (2026-08-27). The other half of the HINDZINC lesson: once decay lets the denial
# take the slot, the fact that it REVERSED this platform's own earlier bearish reading is itself
# information, and it was being thrown away. `reversal_of` records it deterministically — the same
# (symbol, event_type) story contains an EARLIER cluster that cleared the inclusion floor on its own
# UNDECAYED materiality and carried the opposite (short) direction. Purely additive: no grade, level,
# condition or existing column reads it, so `cat` v2's event definition is untouched (its clock is
# not — the decay ranking restarted it, §2.7 2026-08-28).
async def test_hindzinc_shape_is_flagged_as_a_reversal(store, make_job):
    """The motivating case, now labelled. Same corpus as the decay test above: the Aug-24/25 bearish
    stake-sale speculation (short, materiality 0.60 — floor-clearing, so a real established claim)
    followed by the DIPAM denial that resolves it (long, the decay winner). The row must say WHICH
    cluster was reversed, not merely that today's reading is positive.

    The denial is seeded at materiality 0.75 — above ``cat.materiality_min`` (0.70) — so the row
    actually reaches ``originating``, which is the only grade ``cat_reversal`` can originate from. The
    reversal flag itself is computed for context rows too (it describes the corpus, not the grade);
    what the scanner needs is a row that is BOTH originating and a reversal, and that is this one.
    Rank: stale 0.60 x 0.26043 = 0.1563 vs fresh 0.75 x 0.53614 = 0.4021 -> the denial holds the slot.
    """
    seed_universe(store, WED, ("ACME",))
    seed_bars(store, "ACME", WED)
    seed_cluster(
        store, "c-stale", first_seen=at(MON, 10, 0), symbols=("ACME",),
        event_type="regulatory_policy", sentiment=-0.55, materiality=0.60,
    )
    seed_cluster(
        store, "c-fresh", first_seen=at(TUE, 11, 0), symbols=("ACME",),
        event_type="regulatory_policy", sentiment=0.50, materiality=0.75,
    )

    await make_job().run(WED)
    rows = store.get_catalyst_watchlist(WED)

    assert rows[0]["direction"] == "long"
    assert rows[0]["grade"] == "originating"
    assert rows[0]["event_age_sessions"] == 1                   # cat_reversal's age bound, satisfied
    # The whole point: the reversed cluster is NAMED, and it is the earlier bearish one.
    assert rows[0]["reversal_of"] == "c-stale"


async def test_a_story_with_no_directional_conflict_is_not_a_reversal(store, make_job):
    """The control. Two positive clusters of one story — ordinary catalyst drift, exactly what `cat`
    v2 already originates on. Nothing was reversed, so the flag must stay NULL: if plain good news
    were flagged, `cat_reversal` would silently become a duplicate of `cat` and its separate shadow
    population would measure nothing of its own."""
    seed_universe(store, WED, ("ACME",))
    seed_bars(store, "ACME", WED)
    seed_cluster(store, "c-major", first_seen=at(TUE, 18, 0), symbols=("ACME",), materiality=0.80)
    seed_cluster(store, "c-minor", first_seen=at(WED, 8, 0), symbols=("ACME",), materiality=0.30)

    await make_job().run(WED)
    rows = store.get_catalyst_watchlist(WED)

    assert rows[0]["direction"] == "long"
    assert rows[0]["grade"] == "originating"
    assert rows[0]["reversal_of"] is None


async def test_a_sub_floor_bearish_murmur_does_not_establish_a_reversal(store, make_job):
    """The earlier opposite cluster must clear :data:`INCLUSION_FLOOR` on its OWN undecayed weighted
    materiality — the same bar that decides whether a cluster may represent a symbol at all.

    A 0.15-materiality bearish mention is noise the digest already refuses to build a row from, and
    it must not become promotable into "an established bearish claim" merely because something
    positive followed it. Otherwise almost any long row with one stray negative headline in its
    history would qualify, and the reversal population would be indistinguishable from `cat`'s."""
    seed_universe(store, WED, ("ACME",))
    seed_bars(store, "ACME", WED)
    seed_cluster(
        store, "c-murmur", first_seen=at(MON, 10, 0), symbols=("ACME",),
        event_type="regulatory_policy", sentiment=-0.55, materiality=0.15,   # < 0.2 floor
    )
    seed_cluster(
        store, "c-fresh", first_seen=at(TUE, 11, 0), symbols=("ACME",),
        event_type="regulatory_policy", sentiment=0.50, materiality=0.50,
    )

    await make_job().run(WED)
    rows = store.get_catalyst_watchlist(WED)

    assert INCLUSION_FLOOR == 0.2                              # the bar this test is pinned to
    assert rows[0]["direction"] == "long"
    assert rows[0]["reversal_of"] is None


async def test_a_LATER_opposite_cluster_is_not_a_reversal(store, make_job):
    """Direction of time is load-bearing: a reversal needs something that came BEFORE it to reverse.

    Here the bearish cluster is the FRESHER one and merely loses the slot on rank
    (0.25 x 0.98329 = 0.2458 < 0.50 x 0.53614 = 0.2681). That is a story turning negative, which is
    the mirror case this rule deliberately does NOT trade — it is an exit-side signal on an existing
    position (§5.2(b)), not an entry."""
    seed_universe(store, WED, ("ACME",))
    seed_bars(store, "ACME", WED)
    seed_cluster(
        store, "c-good", first_seen=at(TUE, 11, 0), symbols=("ACME",),
        event_type="regulatory_policy", sentiment=0.50, materiality=0.50,
    )
    seed_cluster(
        store, "c-bad-later", first_seen=at(WED, 8, 0), symbols=("ACME",),
        event_type="regulatory_policy", sentiment=-0.55, materiality=0.25,
    )

    await make_job().run(WED)
    rows = store.get_catalyst_watchlist(WED)

    assert rows[0]["direction"] == "long"                       # c-good still holds the slot
    assert rows[0]["reversal_of"] is None


async def test_a_winning_SHORT_row_is_never_flagged_as_a_reversal(store, make_job):
    """LONG-ONLY, enforced at detection. A positive story getting denied is a real event, but it is
    an exit-side signal on an existing position — already owned by the §5.2(b) risk-reducing exit
    path — and NSE cash equities cannot be shorted overnight anyway. Building the mirror case would
    add a short-side origination surface behind a flag nobody gated."""
    seed_universe(store, WED, ("ACME",))
    seed_bars(store, "ACME", WED)
    seed_cluster(
        store, "c-good-old", first_seen=at(MON, 10, 0), symbols=("ACME",),
        event_type="regulatory_policy", sentiment=0.55, materiality=0.60,
    )
    seed_cluster(
        store, "c-denial", first_seen=at(TUE, 11, 0), symbols=("ACME",),
        event_type="regulatory_policy", sentiment=-0.50, materiality=0.50,
    )

    await make_job().run(WED)
    rows = store.get_catalyst_watchlist(WED)

    assert rows[0]["direction"] == "short"
    assert rows[0]["reversal_of"] is None


async def test_reversal_is_scoped_to_ONE_story_not_the_whole_symbol(store, make_job):
    """The story key is ``(symbol, event_type)`` — the same grouping the 2026-08-05 corroboration
    amendment already uses — not the symbol alone.

    An unrelated bearish rating_change does not make a positive regulatory_policy headline a
    "reversal" of it; they are two different stories that happen to share an issuer. Scoping to the
    symbol would manufacture reversals out of ordinary mixed news flow, which is the most likely way
    this detector could quietly over-fire."""
    seed_universe(store, WED, ("ACME",))
    seed_bars(store, "ACME", WED)
    seed_cluster(
        store, "c-other-story", first_seen=at(MON, 10, 0), symbols=("ACME",),
        event_type="rating_change", sentiment=-0.55, materiality=0.60,
    )
    seed_cluster(
        store, "c-fresh", first_seen=at(TUE, 11, 0), symbols=("ACME",),
        event_type="regulatory_policy", sentiment=0.50, materiality=0.50,
    )

    await make_job().run(WED)
    rows = store.get_catalyst_watchlist(WED)

    assert rows[0]["event_type"] == "regulatory_policy"         # 0.2681 rank beats 0.1563
    assert rows[0]["direction"] == "long"
    assert rows[0]["reversal_of"] is None


# The predecessor must have NAMED the symbol. These two differ ONLY in the predecessor's scope; the
# weighted materiality (0.30) and weighted sentiment (−0.35) it reaches the predicate with are equal
# in both, so nothing but the scope can explain the different verdicts.
async def test_a_FANNED_OUT_sector_predecessor_does_not_establish_a_reversal(store, make_job):
    """A sector story is not "this symbol's own earlier bearish claim". At the default 0.5 fan-out a
    sector-wide bearish cluster (materiality 0.60, sentiment −0.70) clears the floor AND the direction
    bar for EVERY constituent at once, so any later symbol-specific positive headline of the same
    event_type would be labelled a reversal of a story that never named the symbol — one sector
    headline manufacturing a reversal per constituent. Fan-out corroborates (step 5(ii)) and it can
    still WIN a row; what it cannot do is be the thing that was reversed."""
    seed_universe(store, WED, ("ACME",))
    seed_bars(store, "ACME", WED)
    store.upsert_sector_map(WED, [{"symbol": "ACME", "sector": "METALS"}])
    seed_cluster(
        store, "c-sector-bear", first_seen=at(MON, 10, 0), scope="sector", sectors=("METALS",),
        event_type="regulatory_policy", sentiment=-0.70, materiality=0.60,   # ×0.5 ⇒ −0.35 / 0.30
    )
    seed_cluster(
        store, "c-fresh", first_seen=at(TUE, 11, 0), symbols=("ACME",),
        event_type="regulatory_policy", sentiment=0.50, materiality=0.75,
    )

    await make_job().run(WED)
    rows = store.get_catalyst_watchlist(WED)

    assert rows[0]["direction"] == "long"
    assert rows[0]["grade"] == "originating"
    assert rows[0]["reversal_of"] is None


async def test_a_STOCK_scope_predecessor_of_the_same_strength_does_establish_a_reversal(store, make_job):
    """The control for the test above: identical weighted numbers (0.30 materiality, −0.35 sentiment),
    reached WITHOUT fan-out because the cluster resolved directly to ACME. This one is flagged — the
    scope filter must not have narrowed the rule to nothing."""
    seed_universe(store, WED, ("ACME",))
    seed_bars(store, "ACME", WED)
    store.upsert_sector_map(WED, [{"symbol": "ACME", "sector": "METALS"}])
    seed_cluster(
        store, "c-stock-bear", first_seen=at(MON, 10, 0), symbols=("ACME",),
        event_type="regulatory_policy", sentiment=-0.35, materiality=0.30,   # weight 1 ⇒ as seeded
    )
    seed_cluster(
        store, "c-fresh", first_seen=at(TUE, 11, 0), symbols=("ACME",),
        event_type="regulatory_policy", sentiment=0.50, materiality=0.75,
    )

    await make_job().run(WED)
    rows = store.get_catalyst_watchlist(WED)

    assert rows[0]["direction"] == "long"
    assert rows[0]["grade"] == "originating"
    assert rows[0]["reversal_of"] == "c-stock-bear"


async def test_a_sector_cluster_that_NAMES_the_symbol_still_establishes_a_reversal(store, make_job):
    """The filter is "did this cluster resolve to the symbol", not "was its scope literally stock" — a
    sector-scope cluster whose resolver DID name ACME made a claim about ACME and keeps its standing
    (its materiality still carries the fan-out weight, as everywhere else in this module)."""
    seed_universe(store, WED, ("ACME",))
    seed_bars(store, "ACME", WED)
    store.upsert_sector_map(WED, [{"symbol": "ACME", "sector": "METALS"}])
    seed_cluster(
        store, "c-sector-named", first_seen=at(MON, 10, 0), scope="sector", sectors=("METALS",),
        symbols=("ACME",), event_type="regulatory_policy", sentiment=-0.70, materiality=0.60,
    )
    seed_cluster(
        store, "c-fresh", first_seen=at(TUE, 11, 0), symbols=("ACME",),
        event_type="regulatory_policy", sentiment=0.50, materiality=0.75,
    )

    await make_job().run(WED)
    rows = store.get_catalyst_watchlist(WED)

    assert rows[0]["direction"] == "long"
    assert rows[0]["reversal_of"] == "c-sector-named"


async def test_reversal_detection_is_deterministic_across_runs(store, make_job):
    """§9.1: the verdict is a pure function of (corpus, ran_at) — no clock read of its own, no
    cross-day DB lookup, no LLM call. Re-running the same digest must reproduce it exactly."""
    seed_universe(store, WED, ("ACME",))
    seed_bars(store, "ACME", WED)
    seed_cluster(
        store, "c-stale", first_seen=at(MON, 10, 0), symbols=("ACME",),
        event_type="regulatory_policy", sentiment=-0.55, materiality=0.60,
    )
    seed_cluster(
        store, "c-stale-2", first_seen=at(MON, 4, 0), symbols=("ACME",),
        event_type="regulatory_policy", sentiment=-0.45, materiality=0.55,
    )
    seed_cluster(
        store, "c-fresh", first_seen=at(TUE, 11, 0), symbols=("ACME",),
        event_type="regulatory_policy", sentiment=0.50, materiality=0.50,
    )

    await make_job().run(WED)
    first = store.get_catalyst_watchlist(WED)[0]["reversal_of"]
    await make_job().run(WED)
    second = store.get_catalyst_watchlist(WED)[0]["reversal_of"]

    # Two floor-clearing bearish predecessors ⇒ the tie-break must be total and stable: the STRONGEST
    # claim (0.60 > 0.55) is the one named, not "whichever the row iteration reached first".
    assert first == "c-stale"
    assert second == first


async def test_stale_cluster_keeps_the_slot_against_fresher_trivia(store, make_job):
    """The other half of the rule: recency is a WEIGHT, not an override. A materially bigger
    story from yesterday evening still outranks a minor mention filed this morning — otherwise
    the fix would trade one stale-row failure for a churn-on-every-headline failure."""
    seed_universe(store, WED, ("ACME",))
    seed_bars(store, "ACME", WED)
    seed_cluster(store, "c-major", first_seen=at(TUE, 18, 0), symbols=("ACME",), materiality=0.80)
    seed_cluster(store, "c-minor", first_seen=at(WED, 8, 0), symbols=("ACME",), materiality=0.30)

    await make_job().run(WED)
    rows = store.get_catalyst_watchlist(WED)

    # 0.80 x 0.65625 = 0.5250 vs 0.30 x 0.98329 = 0.2950.
    assert list(rows[0]["cluster_refs"]) == ["c-major", "c-minor"]
    assert rows[0]["materiality"] == pytest.approx(0.80)
    assert rows[0]["grade"] == "originating"


async def test_sub_floor_mention_corroborates_but_never_takes_the_slot(store, make_job):
    """The inclusion floor gates CANDIDACY, not the finished row (2026-08-27).

    On decayed rank alone the sub-floor mention would win here (0.19 x 0.99759 = 0.1895 >
    0.80 x 0.21899 = 0.1752) and — being below the 0.2 floor — would then delete ACME's row
    outright, losing a still-material story. Filtering candidates on the UNDECAYED floor keeps
    the pre-decay invariant exact: a symbol carries a row iff some cluster of its own clears the
    floor. The mention still corroborates, exactly as before."""
    seed_universe(store, WED, ("ACME",))
    seed_bars(store, "ACME", WED)
    seed_cluster(
        store, "c-major", first_seen=at(MON, 4, 0), symbols=("ACME",), materiality=0.80,
        domains=("economictimes.indiatimes.com",),
    )
    seed_cluster(
        store, "c-mention", first_seen=at(WED, 8, 30), symbols=("ACME",), materiality=0.19,
        domains=("livemint.com",),
    )

    await make_job().run(WED)
    rows = store.get_catalyst_watchlist(WED)

    assert [r["symbol"] for r in rows] == ["ACME"]
    assert list(rows[0]["cluster_refs"]) == ["c-major", "c-mention"]
    assert rows[0]["materiality"] == pytest.approx(0.80)
    assert rows[0]["source_domain_count"] == 2               # story union is untouched by ranking
    assert rows[0]["grade"] == "originating"


async def test_fresher_winner_can_narrow_origination_to_context(store, make_job):
    """Origination follows whichever cluster now REPRESENTS the symbol, and the materiality leg
    can only tighten: the old rule picked the raw-materiality argmax, so any cluster the decay
    promotes has materiality <= the one it displaced. Here a fresh 0.60 cluster takes the slot
    from a stale 0.90 one and the row drops below `cat.materiality_min` (0.70) to `context` —
    the §2.7 fail-safe direction (LESS activity), on the correct, current story."""
    seed_universe(store, WED, ("ACME",))
    seed_bars(store, "ACME", WED)
    seed_cluster(store, "c-stale", first_seen=at(MON, 4, 0), symbols=("ACME",), materiality=0.90)
    seed_cluster(store, "c-fresh", first_seen=at(WED, 8, 0), symbols=("ACME",), materiality=0.60)

    result = await make_job().run(WED)
    rows = store.get_catalyst_watchlist(WED)

    # 0.90 x 0.21899 = 0.1971 vs 0.60 x 0.98329 = 0.5900.
    assert list(rows[0]["cluster_refs"]) == ["c-fresh", "c-stale"]
    assert rows[0]["materiality"] == pytest.approx(0.60)
    assert rows[0]["grade"] == "context"
    assert (result.n_originating, result.n_context) == (0, 1)
    assert rows[0]["confirm_trigger"] is None                # context rows carry no §6.1 levels


async def test_decayed_ranking_stays_deterministic_across_runs(store, make_job):
    """§9.1: the same corpus at the same clock must yield the same watchlist. Decay is a pure
    function of ``first_seen`` against the run's single fixed ``ran_at``, and equal ranks still
    break on ``cluster_id`` — so a re-run reproduces every field but the minted ``entry_id``."""
    seed_universe(store, WED, ("ACME", "BETA"))
    seed_bars(store, "ACME", WED)
    seed_bars(store, "BETA", WED)
    seed_cluster(store, "c-a1", first_seen=at(MON, 10, 0), symbols=("ACME",), materiality=0.60)
    seed_cluster(store, "c-a2", first_seen=at(TUE, 11, 0), symbols=("ACME",), materiality=0.50)
    seed_cluster(store, "c-b1", first_seen=at(TUE, 18, 0), symbols=("BETA",), materiality=0.80)
    seed_cluster(store, "c-b2", first_seen=at(TUE, 18, 0), symbols=("BETA",), materiality=0.80)
    job = make_job()

    await job.run(WED)
    first = [{k: v for k, v in r.items() if k != "entry_id"} for r in store.get_catalyst_watchlist(WED)]
    await job.run(WED)
    second = [{k: v for k, v in r.items() if k != "entry_id"} for r in store.get_catalyst_watchlist(WED)]

    assert first == second
    refs = {r["symbol"]: list(r["cluster_refs"]) for r in store.get_catalyst_watchlist(WED)}
    assert refs["ACME"] == ["c-a2", "c-a1"]                  # fresher wins on decayed materiality
    assert refs["BETA"] == ["c-b1", "c-b2"]                  # identical rank => cluster_id breaks it


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
    """Two store-sourced conditions that no pure-function test can prove: universe + block deals.

    Flags are the PRIOR session's (2026-08-18 fix): the ~08:35 digest runs before the 20:30 deals
    job can have written day-``d`` rows, so a ``d`` read was structurally empty and ``not_flagged``
    had never bound. A day-``d`` row must NOT flag (it cannot exist at digest time in live use)."""
    seed_universe(store, WED, ("ACME", "FLAGGED", "SAMEDAY"))
    for symbol in ("ACME", "FLAGGED", "SAMEDAY", "OFFUNIVERSE"):
        seed_bars(store, symbol, WED)
        seed_cluster(store, f"c-{symbol}", first_seen=at(TUE, 18, 0), symbols=(symbol,))
    store.upsert_flagged_instrument_days([{"symbol": "FLAGGED", "d": TUE, "reason": "block_deal"}])
    store.upsert_flagged_instrument_days([{"symbol": "SAMEDAY", "d": WED, "reason": "block_deal"}])

    await make_job().run(WED)
    grades = {r["symbol"]: r["grade"] for r in store.get_catalyst_watchlist(WED)}

    assert grades == {"ACME": "originating", "FLAGGED": "context",
                      "SAMEDAY": "originating", "OFFUNIVERSE": "context"}


async def test_watchlist_cap_only_symbol_passes_in_universe(store, make_job):
    """The 2026-08-18 news-layer fix: a rule-passing symbol excluded ONLY for the top-N cap
    (``exclusion_reasons == ['watchlist_cap']``) must clear ``in_universe`` and originate — this
    used to be silently dropped because the digest fetched only the ``included`` top-100 watchlist
    (the 2026-08-04 BPCL lesson, re-found in the news layer)."""
    store.upsert_universe_daily([
        {"d": WED, "symbol": "ACME", "included": False, "exclusion_reasons": ["watchlist_cap"]},
    ])
    seed_bars(store, "ACME", WED)
    seed_cluster(store, "c-1", first_seen=at(TUE, 18, 0), symbols=("ACME",))

    await make_job().run(WED)
    rows = store.get_catalyst_watchlist(WED)

    assert [r["symbol"] for r in rows] == ["ACME"]
    assert rows[0]["grade"] == "originating"


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


async def test_unresolvable_prior_session_degrades_to_no_flags(store, make_job):
    """The E5 degrade on the prior-session flags read (2026-08-18): a calendar that cannot resolve
    a prior trading day yields NO flags — origination proceeds unflagged rather than the digest
    dying on a calendar hole."""
    seed_universe(store, WED, ("ACME",))
    seed_bars(store, "ACME", WED)
    seed_cluster(store, "c-ACME", first_seen=at(TUE, 18, 0), symbols=("ACME",))

    job = make_job()
    real_calendar = job._calendar

    class _HolyCalendar:
        def __getattr__(self, name):  # noqa: ANN001, ANN204 - delegate everything else
            return getattr(real_calendar, name)

        def previous_trading_day(self, d):  # noqa: ANN001 - the hole under test
            raise ValueError("no trading day within ~1y")

    job._calendar = _HolyCalendar()
    await job.run(WED)

    rows = store.get_catalyst_watchlist(WED)
    assert [r["symbol"] for r in rows] == ["ACME"]
    assert rows[0]["grade"] == "originating"              # not_flagged passed via the degrade
