"""HeadlineClusterer (§2.7 step 2 / §3.2.4 pinned algorithm / §9.1): golden-file byte-identity
(clusterer_headlines.json in ⇒ clusterer_expected.json out), input-order invariance, the
greedy-earliest-first_seen assignment, the ``cat.max_event_age_days`` window, distinct
``source_domains`` (the §7.1 corroboration input), and the store-wired ``run`` preserving persisted
step-4 LLM scores on re-upsert (R8)."""

from __future__ import annotations

import json
import random
from datetime import timedelta
from pathlib import Path

import pytest

from engine.datafeeds.news import Headline
from engine.datafeeds.news_pipeline import (
    HeadlineClusterer,
    NewsCluster,
    clusterer_normalize,
    similarity,
)
from engine.marketdata.store import MarketStore
from tests.conftest import FIXED_NOW

FIXTURES = Path(__file__).parent / "fixtures" / "news"


def _load_fixture() -> tuple[dict, list[Headline]]:
    fx = json.loads((FIXTURES / "clusterer_headlines.json").read_text(encoding="utf-8"))
    return fx, [Headline(**h) for h in fx["headlines"]]


def _clusterer(fx: dict) -> HeadlineClusterer:
    return HeadlineClusterer(sim_threshold=fx["sim_threshold"], max_event_age_days=fx["max_event_age_days"])


def _serialize(clusters: list[NewsCluster]) -> list[dict]:
    return [
        {
            "cluster_id": c.cluster_id,
            "representative": c.representative,
            "source_domains": c.source_domains,
            "first_seen": c.first_seen.isoformat(),
            "last_seen": c.last_seen.isoformat(),
            "headline_ids": c.headline_ids,
        }
        for c in clusters
    ]


@pytest.fixture
def store(tmp_path, clock):
    s = MarketStore(tmp_path / "market.duckdb", tmp_path / "parquet", clock)
    s.open()
    yield s
    s.close()


# --------------------------------------------------------------------------- golden file (§9.1)
def test_golden_file_byte_identity():
    """Same headlines in ⇒ byte-identical clusters out (committed golden pair)."""
    fx, hs = _load_fixture()
    expected = json.loads((FIXTURES / "clusterer_expected.json").read_text(encoding="utf-8"))
    got = _clusterer(fx).cluster(hs)
    dump = lambda obj: json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=2)  # noqa: E731
    assert dump(_serialize(got)) == dump(expected["clusters"])


def test_input_order_invariance():
    """The algorithm processes in published_at order — the CALLER's list order must not matter."""
    fx, hs = _load_fixture()
    baseline = _serialize(_clusterer(fx).cluster(hs))
    for seed in (1, 7, 42):
        shuffled = hs[:]
        random.Random(seed).shuffle(shuffled)
        assert _serialize(_clusterer(fx).cluster(shuffled)) == baseline
    assert _serialize(_clusterer(fx).cluster(list(reversed(hs)))) == baseline


# --------------------------------------------------------------------------- pinned semantics
def test_greedy_earliest_first_seen_assignment():
    """h13 clears the threshold against BOTH the h11 and h12 representatives; the pinned rule
    assigns it to the EARLIEST-first_seen cluster (c-h11), never the best-scoring one."""
    fx, hs = _load_fixture()
    by_id = {h.headline_id: h for h in hs}
    sim = lambda a, b: similarity(  # noqa: E731
        clusterer_normalize(by_id[a].title), clusterer_normalize(by_id[b].title)
    )
    assert sim("h12", "h11") < fx["sim_threshold"]   # h12 opens its own cluster
    assert sim("h13", "h11") >= fx["sim_threshold"]  # h13 matches both representatives...
    assert sim("h13", "h12") >= fx["sim_threshold"]

    by_cid = {c.cluster_id: c for c in _clusterer(fx).cluster(hs)}
    assert by_cid["c-h11"].headline_ids == ["h11", "h13"]  # ...and joins the earliest
    assert by_cid["c-h12"].headline_ids == ["h12"]


def test_age_window_excludes_stale_clusters():
    """h14's title is IDENTICAL to c-h01's representative (similarity 1.0), but c-h01's last_seen is
    outside the max_event_age_days window at h14's published_at ⇒ h14 opens a NEW cluster."""
    fx, hs = _load_fixture()
    by_id = {h.headline_id: h for h in hs}
    assert by_id["h14"].title == by_id["h01"].title
    by_cid = {c.cluster_id: c for c in _clusterer(fx).cluster(hs)}
    assert by_cid["c-h14"].headline_ids == ["h14"]
    assert "h14" not in by_cid["c-h01"].headline_ids
    # h15 (one day later, INSIDE the window) did merge into c-h01.
    assert by_cid["c-h01"].headline_ids == ["h01", "h02", "h03", "h15"]


def test_source_domains_are_distinct_and_sorted():
    """The cluster's source_domains set is the §7.1 min_source_domains corroboration input:
    DISTINCT domains, so a second headline from the same outlet adds no corroboration."""
    fx, hs = _load_fixture()
    by_cid = {c.cluster_id: c for c in _clusterer(fx).cluster(hs)}
    # c-h04: h04 + h05 share economictimes; distinct count stays 2, not 3.
    assert by_cid["c-h04"].headline_ids == ["h04", "h05", "h06"]
    assert by_cid["c-h04"].source_domains == ["economictimes.indiatimes.com", "moneycontrol.com"]
    assert by_cid["c-h01"].source_domains == [
        "economictimes.indiatimes.com", "livemint.com", "moneycontrol.com",
    ]
    for c in by_cid.values():
        assert c.source_domains == sorted(set(c.source_domains))


def test_existing_clusters_join_without_mutation_and_only_touched_returned():
    fx, hs = _load_fixture()
    by_id = {h.headline_id: h for h in hs}
    existing = NewsCluster(
        cluster_id="c-old",
        representative="RBI cuts repo rate by 25 basis points in surprise move",
        source_domains=["thehindubusinessline.com"],
        first_seen=FIXED_NOW - timedelta(hours=2),
        last_seen=FIXED_NOW - timedelta(hours=2),
        sentiment=0.4,  # a scored cluster (step 4) — must be carried through untouched
    )
    joiner = by_id["h02"]  # near-duplicate of the existing representative
    out = _clusterer(fx).cluster([joiner], existing=[existing])

    assert [c.cluster_id for c in out] == ["c-old"]  # ONLY touched clusters come back
    (c,) = out
    assert c.headline_ids == ["h02"]  # only the ids assigned THIS run (news.cluster_id targets)
    assert c.source_domains == ["moneycontrol.com", "thehindubusinessline.com"]
    assert c.last_seen == joiner.published_at
    assert c.sentiment == 0.4
    # Input object is never mutated (pure function).
    assert existing.source_domains == ["thehindubusinessline.com"]
    assert existing.headline_ids == []


def test_pinned_normalization():
    """Sorted set of unique lowercase alphanumeric tokens — punctuation and dups vanish."""
    assert clusterer_normalize("RBI cuts, cuts & CUTS repo-rate!") == "cuts rate rbi repo"
    assert clusterer_normalize("") == ""
    a = clusterer_normalize("Infosys beats Q1 estimates")
    b = clusterer_normalize("estimates Q1 beats Infosys???")
    assert a == b and similarity(a, b) == 1.0


# --------------------------------------------------------------------------- store-wired run (R8)
async def test_run_persists_clusters_links_news_and_preserves_scores(store, clock):
    fx, _ = _load_fixture()
    clusterer = HeadlineClusterer(store, sim_threshold=fx["sim_threshold"], max_event_age_days=2)

    h1 = Headline(
        headline_id="n1",
        title="Infosys beats Q1 profit estimates raises FY27 revenue guidance",
        source_domain="economictimes.indiatimes.com",
        url="https://economictimes.indiatimes.com/markets/n1.cms",
        published_at=FIXED_NOW - timedelta(hours=3),
    )
    store.insert_news([h1.model_dump(exclude={"headline_id"}) | {"headline_id": "n1"}])
    (c1,) = await clusterer.run([h1])
    assert c1.cluster_id == "c-n1"

    rows = store.get_news()
    assert rows[0]["cluster_id"] == "c-n1"  # news.cluster_id linked (§4.3)

    # Step 4 (Phase 2) scores the cluster; simulate the persisted scores.
    (persisted,) = store.get_news_clusters()
    assert persisted["untrusted"] is True  # §2.4: always
    store.upsert_news_clusters([
        dict(persisted) | {"sentiment": 0.8, "materiality": 0.9, "event_type": "earnings_beat",
                           "novelty": 0.6, "scored_at": FIXED_NOW, "scorer_model": "haiku"}
    ])

    # A later poll brings a near-duplicate: the cluster gains a member WITHOUT losing its scores (R8).
    h2 = Headline(
        headline_id="n2",
        title="Infosys Q1 profit beats estimates raises FY27 guidance",
        source_domain="moneycontrol.com",
        url="https://www.moneycontrol.com/news/n2.html",
        published_at=FIXED_NOW - timedelta(hours=1),
    )
    store.insert_news([h2.model_dump(exclude={"headline_id"}) | {"headline_id": "n2"}])
    (c1b,) = await clusterer.run([h2])
    assert c1b.cluster_id == "c-n1"

    (row,) = store.get_news_clusters()
    assert row["sentiment"] == pytest.approx(0.8)   # scores survived the member-join re-upsert
    assert row["scored_at"] is not None
    assert row["source_domains"] == ["economictimes.indiatimes.com", "moneycontrol.com"]
    assert row["last_seen"] == h2.published_at
    news_by_id = {r["headline_id"]: r for r in store.get_news()}
    assert news_by_id["n2"]["cluster_id"] == "c-n1"


async def test_run_requires_store_and_handles_empty_batch(store):
    pure = HeadlineClusterer()
    with pytest.raises(RuntimeError, match="requires a MarketStore"):
        await pure.run([])
    wired = HeadlineClusterer(store)
    assert await wired.run([]) == []
