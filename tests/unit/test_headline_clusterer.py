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


@pytest.mark.asyncio
async def test_run_executes_the_cluster_pass_off_the_event_loop(store):
    """2026-08-10: the pure-difflib pass blocked the event loop ~95 s on a weekend backlog (measured
    90.6 s at 429×1,500). ``run()`` must execute :meth:`cluster` in a worker thread (§3.2 convention
    12) — pinned so a refactor cannot silently re-inline it."""
    import threading

    loop_thread = threading.get_ident()
    fx, headlines = _load_fixture()
    clusterer = HeadlineClusterer(store, sim_threshold=fx["sim_threshold"],
                                  max_event_age_days=fx["max_event_age_days"])
    seen_threads: list[int] = []
    original = clusterer.cluster

    def spy(hs, existing=None):
        seen_threads.append(threading.get_ident())
        return original(hs, existing=existing)

    clusterer.cluster = spy                                    # instance attribute shadows the method
    touched = await clusterer.run(headlines)
    assert touched                                             # the pass genuinely ran
    assert seen_threads and all(t != loop_thread for t in seen_threads)


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


# ----------------------------------------- partial persist -> orphan re-sweep convergence (2026-08-10)
#: The §4.4 job-10 orphan re-sweep's own bounds (``engine.ops.main.job_news_chain``): unclustered
#: headlines newer than the cutoff are re-swept, oldest-first, capped. Mirrored here so the
#: convergence test drives the re-sweep through EXACTLY the production query (the cutoff itself is
#: pinned in ``test_ops_main_wiring.test_orphan_resweep_abandons_headlines_older_than_four_days``).
_RESWEEP_ABANDON_DAYS = 4
_RESWEEP_CAP = 500
_NEWS_ROW_KEYS = ("headline_id", "title", "source_domain", "url", "published_at")


def _persisted_state(s: MarketStore) -> tuple[list[dict], dict[str, str | None]]:
    """The whole persisted step-2 state: cluster rows + the ``news.cluster_id`` membership map."""
    return (
        sorted((dict(r) for r in s.get_news_clusters()), key=lambda r: r["cluster_id"]),
        {r["headline_id"]: r["cluster_id"] for r in s.get_news()},
    )


async def test_partial_persist_then_resweep_converges_on_the_clean_pass(tmp_path, clock, store):
    """2026-08-10 filed follow-up: a resolve pass killed by the 600 s bound persists PARTIALLY — the
    cluster upsert lands as one batch, then the per-cluster ``set_news_cluster`` loop is cut mid-way,
    leaving inserted-but-unlinked headlines. The next chain run re-sweeps that remainder, and the
    worklog claims it CONVERGES because :meth:`HeadlineClusterer.cluster` is pure, the cluster upsert
    is idempotent, and ``headline_ids`` is not a persisted column. This pins that claim end-to-end:
    the real bound (``resolve_news_bounded``), the real store, the real re-sweep query — final state
    must be identical to a single clean pass over the same headlines, with no duplicated rows."""
    import asyncio

    from engine.ops.main import resolve_news_bounded

    fx, headlines = _load_fixture()
    news_rows = [h.model_dump() for h in headlines]

    def _wired(s: MarketStore) -> HeadlineClusterer:
        return HeadlineClusterer(
            s, sim_threshold=fx["sim_threshold"], max_event_age_days=fx["max_event_age_days"]
        )

    # (a) Reference: ONE clean pass over the whole batch, in its own store.
    baseline = MarketStore(tmp_path / "baseline.duckdb", tmp_path / "baseline-parquet", clock)
    baseline.open()
    try:
        baseline.insert_news(news_rows)
        await _wired(baseline).run(headlines)
        expected = _persisted_state(baseline)
    finally:
        baseline.close()

    # (b) The interrupted pass: the third link write wedges ON THE EVENT LOOP (so cancellation is
    # clean and no worker thread is left behind) and the production bound kills the chain.
    store.insert_news(news_rows)
    real_arun = store.arun
    links = 0

    async def arun_wedging_the_third_link(fn, *args, **kwargs):
        nonlocal links
        if getattr(fn, "__name__", "") == "set_news_cluster":
            links += 1
            if links > 2:
                await asyncio.Event().wait()          # the hang the 600 s bound exists for
        return await real_arun(fn, *args, **kwargs)

    store.arun = arun_wedging_the_third_link           # instance attribute shadows the method
    try:
        completed = await resolve_news_bounded(
            asyncio.Lock(), lambda: _wired(store).run(headlines), timeout_s=1.0
        )
    finally:
        store.arun = real_arun
    assert completed is False                          # the bound fired; the chain was abandoned
    partial_clusters, partial_links = _persisted_state(store)
    assert partial_clusters == expected[0]             # the cluster batch DID land (partial persist)
    linked = {h for h, c in partial_links.items() if c is not None}
    assert 0 < len(linked) < len(headlines)            # ...but the link loop died part-way through

    # (c) The re-sweep, issued exactly as job_news_chain issues it (oldest-first, cutoff, cap).
    orphans = [
        Headline(**{k: r[k] for k in _NEWS_ROW_KEYS})
        for r in store.get_news(
            published_after=clock.now() - timedelta(days=_RESWEEP_ABANDON_DAYS),
            unclustered_only=True,
        )
    ][:_RESWEEP_CAP]
    assert {h.headline_id for h in orphans} == {h.headline_id for h in headlines} - linked
    await _wired(store).run(orphans)

    # (d) Convergence: same clusters, same membership, no duplicate rows, nothing left orphaned.
    final_clusters, final_links = _persisted_state(store)
    assert (final_clusters, final_links) == expected
    assert len({r["cluster_id"] for r in final_clusters}) == len(final_clusters)
    assert all(c is not None for c in final_links.values())


async def test_run_requires_store_and_handles_empty_batch(store):
    pure = HeadlineClusterer()
    with pytest.raises(RuntimeError, match="requires a MarketStore"):
        await pure.run([])
    wired = HeadlineClusterer(store)
    assert await wired.run([]) == []


# --------------------------------------------------------------------------- boilerplate strip (2026-08-03)
def test_boilerplate_phrases_do_not_contribute_to_similarity():
    """The G1-sample production finding (defense-in-depth leg): template phrases are stripped from the
    similarity input so shared boilerplate cannot glue unrelated titles together on its own. The
    PRIMARY guard is upstream — NewsIngest drops live-blog page titles entirely (news.drop_title_patterns);
    this strip covers residual template fragments that slip a pattern list."""
    a = clusterer_normalize("Dr Reddys Share Price Live Updates")
    b = clusterer_normalize("IndusInd Bank Share Price Live Updates")
    assert "share" not in a and "updates" not in b     # template tokens gone from the similarity input
    assert similarity(a, b) < 0.5                      # only company tokens remain ⇒ clearly dissimilar
    # A title that IS pure boilerplate falls back to its unstripped tokens — an EMPTY norm would
    # make every template-only title cluster with every other one at similarity 1.0.
    assert clusterer_normalize("Live updates") == "live updates"


def test_earnings_template_does_not_bridge_different_companies():
    """G1 seed-6 golden pairs — the three REAL cross-company merges observed live (2026-08-03):
    same-day "Q1 Results" template headlines cleared the 0.75 bar on shared template tokens alone.
    With the earnings-template vocabulary stripped, similarity runs on distinctive tokens only."""
    contaminated = [
        ("Maruti Suzuki Q1 Results: Profit falls 11% YoY to Rs 3,352 crore; revenue rises 36%",
         "CDSL Q1 Results: Net profit rises 15% YoY to Rs 118 crore, revenue up 13%"),
        ("Tata Steel Q1 Results: Profit rises 15% to Rs 2,318 crore, revenue climbs 14%",
         "Sun Pharma Q1 Results: Profit rises 27% YoY to Rs 2,895 crore; revenue climbs 10.5%"),
        ("Infosys Q1 Results: Profit rises 12% YoY to Rs 7,769 crore; co trims upper-end revenue forecast",
         "Tata Consumer Q1 Results: Net profit rises 28% YoY to Rs 427 crore, revenue up 12%"),
    ]
    for a, b in contaminated:
        assert similarity(clusterer_normalize(a), clusterer_normalize(b)) < 0.75, (a, b)


def test_same_story_rereports_still_cluster_after_template_strip():
    """Counterpart golden pairs: near-duplicate re-reports of the SAME story (live clean clusters)
    must stay at/above the 0.75 bar after the template strip."""
    clean = [
        ("HUL Q1 Results: Revenue rises 10% YoY to Rs 17,149 crore, but profit falls 3% on one-off tax credit",
         "HUL Q1 Results: Revenue rises 10% YoY to Rs 17,341 crore, but profit falls 3% on one-off tax credit"),
        ("Maruti Suzuki Q1 profit drops 11% YoY to Rs 3,352 crore on higher input costs",
         "Maruti Suzuki Q1 profit drops 11% to Rs 3,352 crore amid rising input costs"),
        ("HUL shares slide 5% after weaker-than-expected Q1; PAT dips 3% to Rs 2,673 crore on one-time credit",
         "HUL shares slide over 6% after weaker-than-expected Q1; PAT dips 3% to Rs 2,673 crore on one-time credit"),
    ]
    for a, b in clean:
        assert similarity(clusterer_normalize(a), clusterer_normalize(b)) >= 0.75, (a, b)


# ------------------------------------------ token-bearing representative (§2.7 amendment 2026-09-04)
def _hl(hid: str, title: str, domain: str, minutes: int) -> Headline:
    return Headline(
        headline_id=hid,
        title=title,
        source_domain=domain,
        url=f"https://{domain}/{hid}",
        published_at=FIXED_NOW - timedelta(minutes=minutes),
    )


FILING = ("[NSE:HINDZINC] Outcome of Board Meeting: Hindustan Zinc Limited has informed the "
          "Exchange that the Board approved a Letter of Intent for a zinc smelter acquisition")
PRESS = ("Hindustan Zinc board approves Letter of Intent for a zinc smelter acquisition, "
         "informs the Exchange")


def test_token_bearing_headline_becomes_the_representative_on_join():
    """A filing merged with its press coverage must stay resolvable: whichever member carries the
    exchange token becomes the cluster's representative (the resolver only sees that string)."""
    press = _hl("h-press", PRESS, "economictimes.indiatimes.com", 90)
    filing = _hl("h-filing", FILING, "nseindia.com", 60)
    assert similarity(clusterer_normalize(PRESS), clusterer_normalize(FILING)) >= 0.75

    (c,) = HeadlineClusterer(sim_threshold=0.75).cluster([press, filing])
    assert c.cluster_id == "c-h-press"                     # the press headline still OPENED it…
    assert c.headline_ids == ["h-press", "h-filing"]
    assert c.representative == FILING                      # …but the token-bearer represents it
    assert c.source_domains == ["economictimes.indiatimes.com", "nseindia.com"]


def test_plain_headline_never_displaces_a_token_representative():
    filing = _hl("h-filing", FILING, "nseindia.com", 90)
    press = _hl("h-press", PRESS, "economictimes.indiatimes.com", 60)
    (c,) = HeadlineClusterer(sim_threshold=0.75).cluster([filing, press])
    assert c.headline_ids == ["h-filing", "h-press"]
    assert c.representative == FILING


def test_second_token_headline_does_not_displace_the_first():
    """Deterministic: the EARLIEST token-bearer (published_at, url order) keeps the seat."""
    first = _hl("h-a", FILING, "nseindia.com", 90)
    second = _hl("h-b", FILING + " (revised)", "nseindia.com", 60)
    (c,) = HeadlineClusterer(sim_threshold=0.75).cluster([first, second])
    assert c.headline_ids == ["h-a", "h-b"]
    assert c.representative == FILING


async def test_token_representative_promotion_reaches_the_store(store, clock):
    """The promotion must be what gets PERSISTED — the resolver reads `news_clusters`."""
    press = _hl("h-press", PRESS, "economictimes.indiatimes.com", 90)
    filing = _hl("h-filing", FILING, "nseindia.com", 60)
    for h in (press, filing):
        store.insert_news([h.model_dump()])

    clusterer = HeadlineClusterer(store, sim_threshold=0.75, max_event_age_days=2)
    await clusterer.run([press, filing])
    (row,) = store.get_news_clusters()
    assert row["representative"] == FILING
