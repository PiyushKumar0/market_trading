"""NewsIngest (§3.2.4 / §2.7 step 1 / §4.4 job 10): offline fixture parses of ET/Moneycontrol RSS +
GDELT DOC 2.0 artlist, URL dedupe (within a batch, across feeds, across polls), tz-correctness
(RFC-2822 / GDELT seendate → tz-aware IST; unparsable ⇒ Clock ingest time), the GDELT domain
allowlist + timespan windows (routine vs §4.4 job-10 backfill), and E5 degradation (a dead feed
contributes zero headlines and never raises)."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import httpx
import pytest
from pydantic import ValidationError

from engine.core.clock import IST
from engine.core.config import NewsCfg
from engine.datafeeds.news import GDELT_DOC_URL, Headline, NewsIngest
from engine.marketdata.store import MarketStore
from tests.conftest import FIXED_NOW

FIXTURES = Path(__file__).parent / "fixtures" / "news"

# The unique URLs each fixture feed contributes (see fixture comments for the malformed items).
ET_URLS = {
    "https://economictimes.indiatimes.com/markets/infosys-q1.cms",
    "https://economictimes.indiatimes.com/markets/rbi-rate-cut.cms",
    "https://economictimes.indiatimes.com/markets/bad-date.cms",
    "https://www.moneycontrol.com/news/business/markets/shared-story.html",
}
MC_URLS = {
    "https://www.moneycontrol.com/news/business/markets/rbi-rate-cut-analysts.html",
    "https://www.moneycontrol.com/news/business/markets/shared-story.html",  # dup of an ET item
}
GDELT_URLS = {
    "https://www.livemint.com/market/rbi-rate-cut-liveblog.html",
    "https://economictimes.indiatimes.com/markets/rbi-rate-cut.cms",  # dup of an ET item
    "https://www.business-standard.com/markets/bad-seendate.html",
}
ALL_UNIQUE_URLS = ET_URLS | MC_URLS | GDELT_URLS  # 7 distinct


@pytest.fixture
def store(tmp_path, clock):
    s = MarketStore(tmp_path / "market.duckdb", tmp_path / "parquet", clock)
    s.open()
    yield s
    s.close()


def _make_ingest(
    store: MarketStore,
    clock,
    *,
    overrides: dict[str, httpx.Response | Exception] | None = None,
    record: list[httpx.Request] | None = None,
) -> tuple[NewsIngest, httpx.AsyncClient]:
    """Ingest wired to a MockTransport serving the fixture payloads (offline, convention 11)."""
    cfg = NewsCfg()

    def handler(request: httpx.Request) -> httpx.Response:
        if record is not None:
            record.append(request)
        url = str(request.url)
        for prefix, outcome in (overrides or {}).items():
            if url.startswith(prefix):
                if isinstance(outcome, Exception):
                    raise outcome
                return outcome
        if url.startswith(cfg.feeds.et_markets_rss):
            return httpx.Response(200, content=(FIXTURES / "et_markets_rss.xml").read_bytes())
        if url.startswith(cfg.feeds.moneycontrol_rss):
            return httpx.Response(200, content=(FIXTURES / "moneycontrol_rss.xml").read_bytes())
        if url.startswith(GDELT_DOC_URL):
            return httpx.Response(200, content=(FIXTURES / "gdelt_artlist.json").read_bytes())
        return httpx.Response(404)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return NewsIngest(cfg, store, clock, client), client


# --------------------------------------------------------------------------- parse + dedupe + persist
async def test_poll_parses_all_feeds_dedupes_by_url_and_persists(store, clock):
    ingest, client = _make_ingest(store, clock)
    async with client:
        got = await ingest.poll()

    assert {h.url for h in got} == ALL_UNIQUE_URLS
    assert len(got) == len(ALL_UNIQUE_URLS)  # cross-feed dup URLs collapsed within the batch
    assert all(h.headline_id for h in got)   # ULIDs minted at ingest
    assert len({h.headline_id for h in got}) == len(got)

    rows = store.get_news()
    assert {r["url"] for r in rows} == ALL_UNIQUE_URLS
    assert all(r["untrusted"] for r in rows)            # §2.4: forced TRUE, always
    assert all(r["cluster_id"] is None for r in rows)   # clustering is step 2, not ingest
    # source_domain is the normalized registrable host (www. stripped).
    by_url = {r["url"]: r for r in rows}
    assert by_url["https://www.moneycontrol.com/news/business/markets/shared-story.html"][
        "source_domain"
    ] == "moneycontrol.com"
    assert by_url["https://www.livemint.com/market/rbi-rate-cut-liveblog.html"]["source_domain"] == "livemint.com"


async def test_repoll_is_idempotent(store, clock):
    ingest, client = _make_ingest(store, clock)
    async with client:
        first = await ingest.poll()
        second = await ingest.poll()
    assert len(first) == len(ALL_UNIQUE_URLS)
    assert second == []  # every URL already in `news` ⇒ nothing inserted, nothing returned
    assert len(store.get_news()) == len(ALL_UNIQUE_URLS)


async def test_rss_malformed_items_are_tolerated(store, clock):
    ingest, client = _make_ingest(store, clock)
    async with client:
        got = await ingest.poll(feeds=("et",))
    # 7 items in the fixture; the no-title / no-link / relative-link ones are skipped, 4 survive.
    assert {h.url for h in got} == ET_URLS


# --------------------------------------------------------------------------- tz-correctness
async def test_published_at_is_tz_aware_ist(store, clock):
    ingest, client = _make_ingest(store, clock)
    async with client:
        got = await ingest.poll()
    by_url = {h.url: h for h in got}

    for h in got:
        assert h.published_at.tzinfo is not None
        assert h.published_at.utcoffset().total_seconds() == 5.5 * 3600  # IST always

    # RFC-2822 UTC pubDate → IST (+05:30).
    infosys = by_url["https://economictimes.indiatimes.com/markets/infosys-q1.cms"]
    assert infosys.published_at == datetime(2026, 6, 17, 9, 0, tzinfo=IST)
    # RFC-2822 +0530 pubDate stays as-is.
    rbi = by_url["https://economictimes.indiatimes.com/markets/rbi-rate-cut.cms"]
    assert rbi.published_at == datetime(2026, 6, 17, 9, 0, tzinfo=IST)
    # GDELT seendate (UTC, YYYYMMDDTHHMMSSZ) → IST.
    livemint = by_url["https://www.livemint.com/market/rbi-rate-cut-liveblog.html"]
    assert livemint.published_at == datetime(2026, 6, 17, 10, 0, tzinfo=IST)
    # Unparsable pubDate / seendate ⇒ ingest time from the injected Clock, never naive.
    assert by_url["https://economictimes.indiatimes.com/markets/bad-date.cms"].published_at == FIXED_NOW
    assert by_url["https://www.business-standard.com/markets/bad-seendate.html"].published_at == FIXED_NOW


def test_headline_rejects_naive_published_at():
    with pytest.raises(ValidationError):
        Headline(
            title="t",
            source_domain="x.com",
            url="https://x.com/1",
            published_at=datetime(2026, 6, 17, 9, 0),  # naive ⇒ a bug (§3.2)
        )


# --------------------------------------------------------------------------- GDELT specifics
async def test_gdelt_domain_allowlist_and_pinned_query(store, clock):
    record: list[httpx.Request] = []
    ingest, client = _make_ingest(store, clock, record=record)
    async with client:
        got = await ingest.poll(feeds=("gdelt",))

    assert {h.url for h in got} == GDELT_URLS  # reuters.com filtered out; no-url article skipped
    assert not any(h.source_domain == "reuters.com" for h in got)

    (req,) = record
    assert req.url.params["query"] == NewsCfg().feeds.gdelt_doc_query  # pinned query from settings
    assert req.url.params["mode"] == "artlist"
    assert req.url.params["format"] == "json"


async def test_gdelt_timespan_routine_vs_backfill_windows(store, clock):
    record: list[httpx.Request] = []
    ingest, client = _make_ingest(store, clock, record=record)
    async with client:
        await ingest.poll(feeds=("gdelt",))                       # routine: 2× 900 s cadence = 30 min
        await ingest.poll(feeds=("gdelt",), lookback_h=48)        # widened poll window
        await ingest.backfill()                                   # §4.4 job 10 default = 72 h
        await ingest.backfill(lookback_h=30 * 24)                 # sized to a long off period
        await ingest.backfill(lookback_h=365 * 24)                # capped at the ~3-month DOC window

    spans = [r.url.params["timespan"] for r in record if str(r.url).startswith(GDELT_DOC_URL)]
    assert spans == ["30min", "48h", "72h", "30d", "90d"]


# --------------------------------------------------------------------------- E5 degradation + selection
async def test_dead_feeds_degrade_to_zero_headlines_never_raise(store, clock):
    cfg = NewsCfg()
    overrides: dict[str, httpx.Response | Exception] = {
        cfg.feeds.et_markets_rss: httpx.Response(500),                       # HTTP failure
        cfg.feeds.moneycontrol_rss: httpx.Response(200, content=b"<not xml"),  # unparsable body
    }
    ingest, client = _make_ingest(store, clock, overrides=overrides)
    async with client:
        got = await ingest.poll()
    # ET + MC contribute nothing; GDELT still lands (feeds fail independently, E5).
    assert {h.url for h in got} == GDELT_URLS


async def test_all_feeds_down_yields_empty_poll(store, clock):
    cfg = NewsCfg()
    overrides: dict[str, httpx.Response | Exception] = {
        cfg.feeds.et_markets_rss: httpx.ConnectError("boom"),
        cfg.feeds.moneycontrol_rss: httpx.Response(503),
        GDELT_DOC_URL: httpx.Response(200, content=b"{ not json"),
    }
    ingest, client = _make_ingest(store, clock, overrides=overrides)
    async with client:
        got = await ingest.poll()
    assert got == []
    assert store.get_news() == []


async def test_feed_subset_polls_only_selected_sources(store, clock):
    record: list[httpx.Request] = []
    ingest, client = _make_ingest(store, clock, record=record)
    async with client:
        got = await ingest.poll(feeds=("et",))
    assert len(record) == 1
    assert str(record[0].url).startswith(NewsCfg().feeds.et_markets_rss)
    assert {h.url for h in got} == ET_URLS


async def test_unknown_feed_key_is_rejected(store, clock):
    ingest, client = _make_ingest(store, clock)
    async with client:
        with pytest.raises(ValueError, match="unknown feed key"):
            await ingest.poll(feeds=("et", "bloomberg"))


async def test_gdelt_payload_is_headline_level_only(store, clock):
    """A3r: only title/source_domain/url/published_at survive ingest — no body-ish fields leak."""
    ingest, client = _make_ingest(store, clock)
    async with client:
        got = await ingest.poll(feeds=("gdelt",))
    assert set(Headline.model_fields) == {"headline_id", "title", "source_domain", "url", "published_at"}
    payload = json.loads((FIXTURES / "gdelt_artlist.json").read_bytes())
    assert len(payload["articles"][0]) > len(Headline.model_fields) - 1  # fixture carries extras we drop
    assert all(h.title and h.source_domain and h.url for h in got)
