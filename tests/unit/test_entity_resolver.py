"""EntityResolver (§2.7 step 3 / §3.2.4 pinned rule / §9.1): case-insensitive WHOLE-WORD PHRASE
containment; alias seed = instruments-dump names with the pinned legal-suffix list stripped MINUS the
curated common-English-word stoplist; AMBIGUOUS (multi-symbol alias OR different companies' aliases on
overlapping title spans) ⇒ NO match, never a guess; out-of-universe recorded, never traded;
sector/theme tagging via sector_map + theme_map under the same whole-word rule."""

from __future__ import annotations

from datetime import timedelta

import pytest

from engine.datafeeds.news_pipeline import (
    ALIAS_STOPLIST,
    LEGAL_SUFFIX_TOKENS,
    EntityResolver,
    NewsCluster,
    strip_legal_suffixes,
)
from engine.marketdata.store import MarketStore
from tests.conftest import FIXED_NOW


def _cluster(title: str, cid: str = "c-1") -> NewsCluster:
    return NewsCluster(
        cluster_id=cid,
        representative=title,
        source_domains=["economictimes.indiatimes.com"],
        first_seen=FIXED_NOW - timedelta(hours=2),
        last_seen=FIXED_NOW - timedelta(hours=1),
    )


@pytest.fixture
def store(tmp_path, clock):
    s = MarketStore(tmp_path / "market.duckdb", tmp_path / "parquet", clock)
    s.open()
    yield s
    s.close()


# --------------------------------------------------------------------------- alias seed (pinned lists)
def test_pinned_legal_suffix_list():
    """The §3.2.4 suffix list, as normalized tokens ('&' is non-alphanumeric so '& CO' ⇒ 'co')."""
    assert set(LEGAL_SUFFIX_TOKENS) == {
        "ltd", "limited", "pvt", "private", "co", "corp", "corporation",
        "india", "industries", "enterprises",
    }


def test_stoplist_contains_the_plan_named_common_words():
    assert {"trent", "idea"} <= set(ALIAS_STOPLIST)  # §3.2.4: TRENT/IDEA/… owner-reviewed in Phase 1


def test_strip_legal_suffixes():
    assert strip_legal_suffixes("INFOSYS LIMITED") == "infosys"
    assert strip_legal_suffixes("TATA CONSULTANCY SERVICES LTD") == "tata consultancy services"
    assert strip_legal_suffixes("HINDUSTAN UNILEVER LIMITED") == "hindustan unilever"
    assert strip_legal_suffixes("BAJAJ & CO") == "bajaj"
    assert strip_legal_suffixes("COAL INDIA LIMITED") == "coal"      # iterative strip ⇒ stoplist's job
    assert strip_legal_suffixes("RELIANCE INDUSTRIES LTD") == "reliance"
    assert strip_legal_suffixes("LIMITED") == "limited"              # never strips below one token


def test_seed_from_instruments_dump_applies_suffixes_and_stoplist(store, clock):
    resolver = EntityResolver(store, clock)
    seeded = resolver.seed_aliases([
        {"name": "INFOSYS LIMITED", "tradingsymbol": "INFY"},
        {"name": "HINDUSTAN UNILEVER LIMITED", "tradingsymbol": "HINDUNILVR"},
        ("TRENT LTD", "TRENT"),          # stoplisted: common English word
        ("COAL INDIA LTD", "COALINDIA"), # strips to 'coal' ⇒ stoplisted
        {"name": "", "tradingsymbol": "NONAME"},  # malformed row skipped
    ])
    assert seeded == 2

    rows = store.get_entity_aliases()
    assert {(r["alias"], r["tradingsymbol"]) for r in rows} == {
        ("infosys", "INFY"), ("hindustan unilever", "HINDUNILVR"),
    }
    assert all(r["source"] == "seed" for r in rows)

    rc = resolver.resolve(_cluster("Infosys wins large European banking deal"))
    assert rc.symbols == ["INFY"]
    # Stoplisted aliases never match — TRENT the company is invisible to the resolver seed.
    rc = resolver.resolve(_cluster("Trent shares surge on strong festive sales"))
    assert rc.symbols == [] and rc.entities == []


# --------------------------------------------------------------------------- pinned match rule
def test_whole_word_phrase_containment_case_insensitive():
    resolver = EntityResolver(aliases={"infosys": "INFY", "hdfc bank": "HDFCBANK"})
    assert resolver.resolve(_cluster("INFOSYS beats Q1 estimates")).symbols == ["INFY"]
    assert resolver.resolve(_cluster("HDFC Bank raises MCLR by 10 bps")).symbols == ["HDFCBANK"]
    # Substrings of a longer token are NOT whole-word matches.
    assert resolver.resolve(_cluster("Infosystems Pvt announces expansion")).symbols == []
    # A multi-token alias must appear as a CONTIGUOUS phrase.
    assert resolver.resolve(_cluster("HDFC Life and Axis Bank tie up")).symbols == []
    # Punctuation never blocks the token match.
    assert resolver.resolve(_cluster("Infosys: Q1 results today")).symbols == ["INFY"]


def test_suffix_stripped_alias_matches_headline_without_suffix(store, clock):
    resolver = EntityResolver(store, clock)
    resolver.seed_aliases([("HINDUSTAN UNILEVER LIMITED", "HINDUNILVR")])
    rc = resolver.resolve(_cluster("Hindustan Unilever raises soap prices by 4 percent"))
    assert rc.symbols == ["HINDUNILVR"]
    assert rc.entities == ["hindustan unilever"]


def test_ambiguous_multi_symbol_alias_resolves_to_nothing():
    resolver = EntityResolver(aliases={"adani": ("ADANIENT", "ADANIPORTS"), "infosys": "INFY"})
    rc = resolver.resolve(_cluster("Adani stocks rally after clarification"))
    assert rc.symbols == []                                     # never a guess
    (u,) = rc.unresolved
    assert u.reason == "ambiguous"
    assert u.entity_text == "adani"
    assert set(u.candidate_symbols) == {"ADANIENT", "ADANIPORTS"}


def test_overlapping_spans_of_different_companies_resolve_to_nothing():
    resolver = EntityResolver(aliases={"tata motors": "TATAMOTORS", "motors finance": "TMFL"})
    rc = resolver.resolve(_cluster("Tata Motors Finance eyes public listing"))
    assert rc.symbols == []                                     # overlapping spans ⇒ ambiguous
    assert {u.entity_text for u in rc.unresolved} == {"tata motors", "motors finance"}
    assert all(u.reason == "ambiguous" for u in rc.unresolved)
    assert all(set(u.candidate_symbols) == {"TATAMOTORS", "TMFL"} for u in rc.unresolved)


def test_non_overlapping_matches_both_resolve():
    resolver = EntityResolver(aliases={"infosys": "INFY", "wipro": "WIPRO"})
    rc = resolver.resolve(_cluster("Infosys and Wipro rally on strong IT spending"))
    assert rc.symbols == ["INFY", "WIPRO"]
    assert rc.unresolved == []


def test_same_company_overlap_is_not_ambiguous():
    """Two aliases of the SAME symbol overlapping is fine — the span union has one symbol."""
    resolver = EntityResolver(aliases={"maruti": "MARUTI", "maruti suzuki": "MARUTI"})
    rc = resolver.resolve(_cluster("Maruti Suzuki recalls 40000 cars"))
    assert rc.symbols == ["MARUTI"]
    assert rc.unresolved == []


# --------------------------------------------------------------------------- universe filter
def test_out_of_universe_recorded_never_traded():
    resolver = EntityResolver(aliases={"paytm": "PAYTM", "infosys": "INFY"}, universe={"INFY"})
    rc = resolver.resolve(_cluster("Paytm and Infosys announce payments tie-up"))
    assert rc.symbols == ["INFY"]                # in-universe resolves
    (u,) = rc.unresolved
    assert u.reason == "out_of_universe"         # out-of-universe recorded (§5.5 loop) …
    assert u.entity_text == "paytm"
    assert u.candidate_symbols == ("PAYTM",)     # … and never reaches symbols[]


def test_unknown_universe_defers_filtering():
    """universe=None (before the 08:30 build) ⇒ no filtering here; the step-5 digest re-checks."""
    resolver = EntityResolver(aliases={"paytm": "PAYTM"}, universe=None)
    rc = resolver.resolve(_cluster("Paytm shares climb 5 percent"))
    assert rc.symbols == ["PAYTM"]
    assert rc.unresolved == []


# --------------------------------------------------------------------------- sector / theme tagging
def test_sector_and_theme_tags_use_the_same_whole_word_rule():
    resolver = EntityResolver(
        aliases={"infosys": "INFY"},
        sector_map={"INFY": "IT", "HDFCBANK": "Bank", "ZZZ": "UNCLASSIFIED"},
        theme_map={"defence": ["defence", "DRDO", "missile"], "ev_mobility": ["electric vehicle", "EV"]},
    )
    rc = resolver.resolve(_cluster("IT stocks rally as banks slip"))
    assert rc.sectors == ["IT"]
    assert rc.themes == []
    # Whole-word: 'banks' is not the sector keyword 'bank'; 'unclassified' is never a keyword.
    rc = resolver.resolve(_cluster("Unclassified banks data released"))
    assert rc.sectors == []
    # Multi-token theme keyword must be a contiguous phrase; acronyms match case-insensitively.
    rc = resolver.resolve(_cluster("Defence stocks rally on new DRDO missile order win"))
    assert rc.themes == ["defence"]
    rc = resolver.resolve(_cluster("Electric vehicle sales double as EV subsidies extended"))
    assert rc.themes == ["ev_mobility"]
    rc = resolver.resolve(_cluster("Vehicle electric grid upgrade planned"))  # scrambled ⇒ no phrase
    assert rc.themes == []


# --------------------------------------------------------------------------- Phase-2 seam
def test_extra_texts_reenter_the_same_rule_and_log_no_match():
    """News-Analyst-emitted entity STRINGS re-enter this resolver — the LLM never assigns a symbol."""
    resolver = EntityResolver(aliases={"hindustan unilever": "HINDUNILVR"})
    rc = resolver.resolve(
        _cluster("FMCG major raises prices"),
        extra_texts=["Hindustan Unilever", "Totally Unknown Corp"],
    )
    assert rc.symbols == ["HINDUNILVR"]
    (u,) = rc.unresolved
    assert u.reason == "no_match"
    assert u.entity_text == "Totally Unknown Corp"


# --------------------------------------------------------------------------- store-wired run
async def test_run_persists_tags_logs_unresolved_and_preserves_scores(store, clock):
    d = FIXED_NOW.date()
    store.upsert_entity_aliases([
        {"alias": "infosys", "tradingsymbol": "INFY"},
        {"alias": "adani", "tradingsymbol": "ADANIENT"},
        {"alias": "adani", "tradingsymbol": "ADANIPORTS"},   # one alias, two symbols ⇒ ambiguous
        {"alias": "paytm", "tradingsymbol": "PAYTM"},
    ])
    store.upsert_sector_map(d, [{"symbol": "INFY", "sector": "IT"}])
    store.upsert_theme_map([{"theme": "defence", "keywords": ["DRDO", "missile"], "symbols": []}])
    store.upsert_universe_daily([
        {"d": d, "symbol": "INFY", "included": True},
        {"d": d, "symbol": "PAYTM", "included": False},      # in the table but excluded ⇒ out-of-universe
    ])

    resolver = EntityResolver(store, clock)
    await resolver.aload(d)

    scored = _cluster("Infosys wins DRDO missile software deal as Adani and Paytm watch", cid="c-x")
    scored = scored.model_copy(update={"sentiment": 0.7, "scored_at": FIXED_NOW, "scorer_model": "haiku"})
    store.upsert_news_clusters([scored.to_row()])

    (rc,) = await resolver.run([scored])
    assert rc.symbols == ["INFY"]
    assert rc.sectors == []                                   # 'it' the sector keyword is absent here
    assert rc.themes == ["defence"]
    assert {(u.entity_text, u.reason) for u in rc.unresolved} == {
        ("adani", "ambiguous"), ("paytm", "out_of_universe"),
    }

    (row,) = store.get_news_clusters()
    assert row["symbols"] == ["INFY"]                         # EntityResolver output ONLY (§4.3)
    assert row["themes"] == ["defence"]
    assert row["sentiment"] == pytest.approx(0.7)             # step-4 scores never clobbered (R8)
    assert row["untrusted"] is True

    logged = store.get_unresolved_entities()
    assert {(r["entity_text"], r["reason"], r["cluster_id"]) for r in logged} == {
        ("adani", "ambiguous", "c-x"), ("paytm", "out_of_universe", "c-x"),
    }
    amb = next(r for r in logged if r["reason"] == "ambiguous")
    assert set(amb["candidate_symbols"]) == {"ADANIENT", "ADANIPORTS"}


async def test_pure_resolver_requires_store_for_run_and_load(clock):
    resolver = EntityResolver(aliases={"infosys": "INFY"})
    with pytest.raises(RuntimeError, match="requires a MarketStore"):
        resolver.load()
    with pytest.raises(RuntimeError, match="requires a MarketStore"):
        await resolver.run([])
