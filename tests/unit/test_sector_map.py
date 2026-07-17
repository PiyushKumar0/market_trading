"""SectorMapJob (§4.4 job 13, R1/E5): first-wins sector classification over the pinned source
order (PSU Bank before Bank before Financial Services), UNCLASSIFIED for unclaimed universe
symbols, the per-source frozen-fallback + alert path, keep-previous-snapshot when nothing
classifies, verbatim ``theme_map`` seed refresh, and the never-raise guarantee."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import httpx
import pytest

from engine.core.config import repo_root
from engine.datafeeds import sector_map as sm
from engine.datafeeds.sector_map import (
    SECTOR_SOURCES,
    UNCLASSIFIED,
    SectorMapJob,
    load_theme_seed,
    parse_constituents_csv,
)
from engine.marketdata.store import MarketStore
from tests.conftest import FIXED_NOW

FIXTURES = Path(__file__).parent / "fixtures"
D = FIXED_NOW.date()

PSU_BANK_CSV = (FIXTURES / "sector_psubank.csv").read_text(encoding="utf-8")
BANK_CSV = (FIXTURES / "sector_bank.csv").read_text(encoding="utf-8")
IT_CSV = (FIXTURES / "sector_it.csv").read_text(encoding="utf-8")

_URLS = dict(SECTOR_SOURCES)

#: Per-sector CSV payloads: the three real fixtures + a one-symbol filler per remaining index so
#: the happy path has ALL ten sources healthy (an empty CSV would read as a failed source).
SECTOR_PAYLOADS: dict[str, str] = {}
for _sector, _url in SECTOR_SOURCES:
    if _sector == "PSU_BANK":
        SECTOR_PAYLOADS[_url] = PSU_BANK_CSV
    elif _sector == "BANK":
        SECTOR_PAYLOADS[_url] = BANK_CSV
    elif _sector == "IT":
        SECTOR_PAYLOADS[_url] = IT_CSV
    else:
        SECTOR_PAYLOADS[_url] = (
            "Company Name,Industry,Symbol,Series,ISIN Code\n"
            f"{_sector.title()} Co,{_sector},{_sector}STK,EQ,\n"
        )

THEMES_YAML = (
    "schema_version: 1\n"
    "themes:\n"
    "  ev_mobility:\n"
    "    keywords: [electric vehicle, lithium]\n"
    "    symbols: [TATAMOTORS]\n"
    "  defence:\n"
    "    keywords: [missile]\n"
    "    symbols: []\n"
)


@pytest.fixture
def store(tmp_path, clock):
    s = MarketStore(tmp_path / "market.duckdb", tmp_path / "parquet", clock).open()
    yield s
    s.close()


def collect_alerts():
    msgs = []

    async def sink(msg):
        msgs.append(msg)

    return msgs, sink


def make_client(fail_urls: set[str] = frozenset()) -> httpx.AsyncClient:
    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url in fail_urls:
            raise httpx.ConnectError("blocked by anti-bot", request=request)
        return httpx.Response(200, text=SECTOR_PAYLOADS[url])

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def failing_client() -> httpx.AsyncClient:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("nse unreachable", request=request)

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def make_job(
    tmp_path, store, clock, client, *, cache_name="sector_lists.json", themes=THEMES_YAML,
    notify=None,
) -> SectorMapJob:
    themes_path = tmp_path / "themes.yaml"
    if themes is not None and not themes_path.exists():
        themes_path.write_text(themes, encoding="utf-8")
    return SectorMapJob(
        store, clock, client, tmp_path / cache_name, themes_path=themes_path, notify=notify
    )


# --------------------------------------------------------------------------- happy path (R1)
async def test_run_classifies_first_wins_and_unclassified(tmp_path, store, clock):
    """SBIN is in BOTH the PSU-Bank and Bank lists — the pinned order classifies it PSU_BANK.
    A universe symbol in no index gets an explicit UNCLASSIFIED row (gate caps it at 1)."""
    job = make_job(tmp_path, store, clock, make_client())
    result = await job.run(D, universe_symbols=["RELIANCE", "TCS", "sbin", " "])

    assert result.ok is True and result.degraded_sources == ()
    rows = {r["symbol"]: r["sector"] for r in store.get_sector_map(as_of=D)}
    assert rows["SBIN"] == "PSU_BANK"                    # first-wins: PSU Bank before Bank
    assert rows["CANBK"] == "PSU_BANK"
    assert rows["HDFCBANK"] == "BANK" and rows["ICICIBANK"] == "BANK"
    assert rows["TCS"] == "IT" and rows["INFY"] == "IT"  # universe symbol already classified
    assert rows["RELIANCE"] == UNCLASSIFIED              # unclaimed universe symbol
    assert result.unclassified == 1
    # theme_map refreshed verbatim from the seed (owner-approved content only).
    themes = {t["theme"]: t for t in store.get_theme_map()}
    assert set(themes) == {"ev_mobility", "defence"}
    assert themes["ev_mobility"]["symbols"] == ["TATAMOTORS"]     # exactly as the owner wrote it
    assert themes["defence"]["symbols"] == []
    assert result.themes_ok is True and result.themes_written == 2


async def test_rerun_is_idempotent_and_snapshot_selectable_by_date(tmp_path, store, clock):
    job = make_job(tmp_path, store, clock, make_client())
    first = await job.run(D)
    again = await job.run(D)                             # §2.6 run-latest-once: harmless re-run
    assert first.ok and again.ok
    assert len(store.get_sector_map(as_of=D)) == first.rows_written
    assert store.get_sector_map(as_of=D - timedelta(days=1)) == []   # no earlier snapshot


# --------------------------------------------------------------------------- failure model (E5)
async def test_failed_source_reuses_frozen_copy_and_alerts(tmp_path, store, clock):
    await make_job(tmp_path, store, clock, make_client()).run(D)     # seeds the frozen copies

    msgs, sink = collect_alerts()
    job = make_job(
        tmp_path, store, clock, make_client(fail_urls={_URLS["PSU_BANK"]}), notify=sink
    )
    result = await job.run(D)

    assert result.ok is True and result.degraded_sources == ("PSU_BANK",)
    rows = {r["symbol"]: r["sector"] for r in store.get_sector_map(as_of=D)}
    assert rows["SBIN"] == "PSU_BANK"                    # frozen copy reused — never shrunk
    assert any(m.severity == "warning" and "PSU_BANK" in m.body for m in msgs)
    assert msgs[-1].data["job_id"] == "sector_map"


async def test_all_sources_down_no_cache_keeps_previous_snapshot(tmp_path, store, clock):
    """Nothing classifies at all ⇒ NO new snapshot (an all-UNCLASSIFIED snapshot would clobber
    the previous good map); previous snapshot stays the latest; critical alert; never raises."""
    earlier = D - timedelta(days=7)
    good = make_job(tmp_path, store, clock, make_client())
    assert (await good.run(earlier)).ok is True

    msgs, sink = collect_alerts()
    job = make_job(
        tmp_path, store, clock, failing_client(), cache_name="empty_cache.json", notify=sink
    )
    result = await job.run(D, universe_symbols=["RELIANCE"])

    assert result.ok is False
    assert set(result.degraded_sources) == {s for s, _ in SECTOR_SOURCES}
    latest = store.get_sector_map()
    assert latest and all(r["as_of"] == earlier for r in latest)     # previous snapshot kept
    assert any(m.severity == "critical" for m in msgs)


async def test_theme_seed_unreadable_alerts_but_sector_part_proceeds(tmp_path, store, clock):
    msgs, sink = collect_alerts()
    job = SectorMapJob(
        store, clock, make_client(), tmp_path / "cache.json",
        themes_path=tmp_path / "missing_themes.yaml", notify=sink,
    )
    result = await job.run(D)
    assert result.ok is True and result.themes_ok is False
    assert store.get_sector_map(as_of=D)                 # sector snapshot still written
    assert any("theme" in m.title.lower() for m in msgs)


# --------------------------------------------------------------------------- parsers / seeds
def test_parse_constituents_csv_defensive():
    text = (
        "# frozen-copy note\n"
        "Company Name,Industry,SYMBOL,Series,ISIN Code\n"
        "A Co,X,AAA,EQ,\n"
        "B Co,X,BBB,BE,\n"                               # non-EQ series dropped
        "A Co dup,X,AAA,EQ,\n"                           # de-duplicated
        "C Co,X,ccc,EQ,\n"                               # uppercased
    )
    assert parse_constituents_csv(text) == ["AAA", "CCC"]
    with pytest.raises(ValueError):
        parse_constituents_csv("Company Name,Industry\nA,B\n")


def test_source_order_is_most_specific_first():
    """ORDER IS LOAD-BEARING (first-wins): PSU Bank ⊂ Bank ⊂ Financial Services."""
    order = [sector for sector, _ in SECTOR_SOURCES]
    assert order.index("PSU_BANK") < order.index("BANK") < order.index("FINANCIAL_SERVICES")
    assert all(url.startswith("https://") for _, url in SECTOR_SOURCES)


def test_committed_theme_seed_loads_verbatim():
    """The shipped config/themes.yaml parses; symbols lists are owner-approved verbatim (empty
    until the owner applies weekly-researcher suggestions, §5.5)."""
    rows = load_theme_seed(repo_root() / "config" / "themes.yaml")
    assert {r["theme"] for r in rows} >= {"ev_mobility", "defence", "railways"}
    for row in rows:
        assert isinstance(row["keywords"], list) and row["keywords"]
        assert row["symbols"] == []                      # nothing auto-added, ever


def test_load_theme_seed_rejects_non_mapping(tmp_path):
    bad = tmp_path / "themes.yaml"
    bad.write_text("themes: [not, a, mapping]\n", encoding="utf-8")
    with pytest.raises(ValueError):
        load_theme_seed(bad)


def test_unclassified_constant_matches_plan():
    assert sm.UNCLASSIFIED == "UNCLASSIFIED"             # §4.4 job 13 vocabulary, gate-visible
