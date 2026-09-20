"""SectorMapJob (§4.4 job 13, R1/E5): first-wins sector classification over the pinned source
order (PSU Bank before Bank before Financial Services), the owner-override and NSE-Industry
supplement rungs (2026-09-21) beneath it, UNCLASSIFIED for whatever none of them claims, the
per-source frozen-fallback + alert path, keep-previous-snapshot when nothing classifies, verbatim
``theme_map`` seed refresh, and the never-raise guarantee."""

from __future__ import annotations

import logging
from datetime import timedelta
from pathlib import Path

import httpx
import pytest

from engine.core.config import repo_root
from engine.datafeeds import sector_map as sm
from engine.datafeeds.sector_map import (
    INDUSTRY_SECTOR_ALIASES,
    SECTOR_SOURCES,
    UNCLASSIFIED,
    SectorMapJob,
    industry_sector,
    load_sector_overrides,
    load_theme_seed,
    parse_constituents_csv,
    parse_industry_csv,
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

#: Default for tests that don't care about overrides — isolates every test from the real
#: config/sector_overrides.yaml (mirrors the THEMES_YAML isolation pattern above).
OVERRIDES_YAML_EMPTY = "schema_version: 1\noverrides: {}\n"

#: HDFCAMC/ICICIAMC — the two real AMC seed entries (mirrors what's shipped in
#: config/sector_overrides.yaml), used by the override-specific tests below.
OVERRIDES_YAML = (
    "schema_version: 1\n"
    "overrides:\n"
    "  FINANCIAL_SERVICES: [HDFCAMC, ICICIAMC]\n"
)

#: Universe index CSV shape (``Company Name,Industry,Symbol,Series,ISIN Code``) carrying NSE's
#: Industry column — the third classification rung's input (data/universe/index_cached.csv).
INDUSTRY_CSV = (
    "# NIFTY 500 constituents (runtime cache)\n"
    "Company Name,Industry,Symbol,Series,ISIN Code\n"
    "Acme Labs Ltd.,Healthcare,ACMELAB,EQ,INE000A01001\n"          # aliased onto the PHARMA index
    "Bolt Engineering Ltd.,Capital Goods,BOLTENG,EQ,INE000A01002\n"  # no index ⇒ normalised bucket
    "Drift Corp Ltd.,Consumer Services,DRIFTBE,BE,INE000A01003\n"    # non-EQ series dropped
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
    overrides=OVERRIDES_YAML_EMPTY, industry_paths=(), notify=None,
) -> SectorMapJob:
    themes_path = tmp_path / "themes.yaml"
    if themes is not None and not themes_path.exists():
        themes_path.write_text(themes, encoding="utf-8")
    overrides_path = tmp_path / "sector_overrides.yaml"
    if overrides is not None and not overrides_path.exists():
        overrides_path.write_text(overrides, encoding="utf-8")
    return SectorMapJob(
        store, clock, client, tmp_path / cache_name, themes_path=themes_path,
        overrides_path=overrides_path, industry_paths=industry_paths, notify=notify,
    )


def write_industry_csv(tmp_path, text: str, name="index_cached.csv") -> Path:
    path = tmp_path / name
    path.write_text(text, encoding="utf-8")
    return path


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


async def test_degraded_source_alert_dedups_and_rearms(tmp_path, store, clock):
    """2026-08-13 (structural deviation representative — sector_map's 'degraded — frozen copies
    reused' alert fires on an otherwise ``ok=True`` run, unlike bhavcopy's binary shape): dedup
    guards each ``_alert`` call site, keyed on ``d``, not on ``ok``. Two consecutive runs with the
    SAME failing source produce exactly one notify; a fully clean run re-arms it for a later streak."""
    await make_job(tmp_path, store, clock, make_client()).run(D)     # seeds the frozen copies

    msgs, sink = collect_alerts()
    fail_urls = {_URLS["PSU_BANK"]}

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url in fail_urls:
            raise httpx.ConnectError("blocked by anti-bot", request=request)
        return httpx.Response(200, text=SECTOR_PAYLOADS[url])

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    job = make_job(tmp_path, store, clock, client, notify=sink)

    result1 = await job.run(D)
    result2 = await job.run(D)
    assert result1.ok is True and result2.ok is True                 # ok stays True both times
    assert result1.degraded_sources == ("PSU_BANK",) and result2.degraded_sources == ("PSU_BANK",)
    assert len(msgs) == 1                                            # deduped across the same streak

    fail_urls.clear()                                                # next run for D is fully clean
    result3 = await job.run(D)
    assert result3.ok is True and result3.degraded_sources == ()
    assert len(msgs) == 1                                            # no alert on a clean run

    fail_urls.add(_URLS["PSU_BANK"])                                 # a NEW failing streak, same D
    result4 = await job.run(D)
    assert result4.degraded_sources == ("PSU_BANK",)
    assert len(msgs) == 2                                            # re-armed: alerts again


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
    overrides_path = tmp_path / "sector_overrides.yaml"
    overrides_path.write_text(OVERRIDES_YAML_EMPTY, encoding="utf-8")  # isolate from the real file
    msgs, sink = collect_alerts()
    job = SectorMapJob(
        store, clock, make_client(), tmp_path / "cache.json",
        themes_path=tmp_path / "missing_themes.yaml", overrides_path=overrides_path, notify=sink,
    )
    result = await job.run(D)
    assert result.ok is True and result.themes_ok is False
    assert store.get_sector_map(as_of=D)                 # sector snapshot still written
    assert any("theme" in m.title.lower() for m in msgs)


# --------------------------------------------------------------------------- sector overrides (owner supplement)
async def test_override_classifies_amc_as_financial_services(tmp_path, store, clock):
    """HDFCAMC/ICICIAMC are NOT constituents of the real scraped Financial Services index (the
    happy-path FINANCIAL_SERVICES fixture is a one-symbol filler, not either AMC) — the
    owner-curated override supplement folds them into FINANCIAL_SERVICES anyway."""
    job = make_job(tmp_path, store, clock, make_client(), overrides=OVERRIDES_YAML)
    result = await job.run(D, universe_symbols=["HDFCAMC", "ICICIAMC"])

    assert result.ok is True
    rows = {r["symbol"]: r["sector"] for r in store.get_sector_map(as_of=D)}
    assert rows["HDFCAMC"] == "FINANCIAL_SERVICES"
    assert rows["ICICIAMC"] == "FINANCIAL_SERVICES"
    assert result.unclassified == 0                      # neither fell through to UNCLASSIFIED


async def test_override_never_reclassifies_a_symbol_already_in_a_real_index(tmp_path, store, clock):
    """A conflicting override entry for a symbol ALREADY classified by a real scraped index must
    never win: ``mapping.setdefault`` means the override only fills gaps, never overwrites real
    classification. SBIN is genuinely PSU_BANK (first-wins over BANK too) via the real fixtures;
    an override file that (wrongly) claims it for IT must be silently ignored for SBIN specifically,
    while a non-conflicting entry in the SAME file still applies."""
    conflicting = (
        "schema_version: 1\n"
        "overrides:\n"
        "  IT: [SBIN]\n"                                 # conflicts with the real PSU_BANK scrape
        "  FINANCIAL_SERVICES: [HDFCAMC]\n"               # no conflict — HDFCAMC is in no real index
    )
    job = make_job(tmp_path, store, clock, make_client(), overrides=conflicting)
    result = await job.run(D, universe_symbols=["SBIN", "HDFCAMC"])

    assert result.ok is True
    rows = {r["symbol"]: r["sector"] for r in store.get_sector_map(as_of=D)}
    assert rows["SBIN"] == "PSU_BANK"                     # real scrape wins; override ignored here
    assert rows["HDFCAMC"] == "FINANCIAL_SERVICES"        # non-conflicting override entry still applies


async def test_override_applies_on_reused_frozen_cache_not_just_fresh_scrape(tmp_path, store, clock):
    """The override merge happens AFTER the SECTOR_SOURCES loop, whose ``mapping`` already absorbed
    both fresh-scrape and frozen-cache-reuse results by that point — so a run where a source is
    degraded (reusing its frozen copy) still gets the override applied, not just the happy path."""
    seed = make_job(tmp_path, store, clock, make_client(), overrides=OVERRIDES_YAML)
    assert (await seed.run(D)).ok is True                 # seeds the frozen cache for D

    job = make_job(
        tmp_path, store, clock, make_client(fail_urls={_URLS["FINANCIAL_SERVICES"]}),
        overrides=OVERRIDES_YAML,
    )
    result = await job.run(D, universe_symbols=["HDFCAMC"])

    assert result.ok is True and result.degraded_sources == ("FINANCIAL_SERVICES",)
    rows = {r["symbol"]: r["sector"] for r in store.get_sector_map(as_of=D)}
    assert rows["HDFCAMC"] == "FINANCIAL_SERVICES"        # override applied despite the degraded source
    assert rows["FINANCIAL_SERVICESSTK"] == "FINANCIAL_SERVICES"  # frozen copy's own symbol still present


async def test_override_unknown_sector_name_skipped_and_alerts(tmp_path, store, clock):
    """A typo'd sector name (not one of SECTOR_SOURCES) must not create a phantom one-symbol
    bucket that per_sector_exposure never groups on — the affected symbol stays UNCLASSIFIED
    (conservative, gate-visible) rather than escaping the cap silently. A valid sibling entry in
    the SAME file still applies, and the alert names the bad sector string."""
    typo = (
        "schema_version: 1\n"
        "overrides:\n"
        "  FINANCIALSERVICES: [HDFCAMC]\n"          # typo — missing underscore, not a real sector
        "  FINANCIAL_SERVICES: [ICICIAMC]\n"        # valid sibling entry, same file
    )
    msgs, sink = collect_alerts()
    job = make_job(tmp_path, store, clock, make_client(), overrides=typo, notify=sink)
    result = await job.run(D, universe_symbols=["HDFCAMC", "ICICIAMC"])

    assert result.ok is True
    rows = {r["symbol"]: r["sector"] for r in store.get_sector_map(as_of=D)}
    assert rows["HDFCAMC"] == UNCLASSIFIED                 # bad-sector entry skipped, stays conservative
    assert rows["ICICIAMC"] == "FINANCIAL_SERVICES"        # valid sibling entry still applied
    assert any("FINANCIALSERVICES" in m.body for m in msgs)
    assert any(m.severity == "warning" for m in msgs)


async def test_missing_overrides_file_alerts_but_sector_part_proceeds(tmp_path, store, clock):
    """Missing config/sector_overrides.yaml is E5 (§ module docstring: never load-bearing) — the
    real index-scrape classification proceeds unaffected, just without any override applied."""
    themes_path = tmp_path / "themes.yaml"
    themes_path.write_text(THEMES_YAML, encoding="utf-8")
    msgs, sink = collect_alerts()
    job = SectorMapJob(
        store, clock, make_client(), tmp_path / "cache.json",
        themes_path=themes_path, overrides_path=tmp_path / "missing_overrides.yaml",  # never created
        notify=sink,
    )
    result = await job.run(D, universe_symbols=["SBIN"])

    assert result.ok is True and result.themes_ok is True
    rows = {r["symbol"]: r["sector"] for r in store.get_sector_map(as_of=D)}
    assert rows["SBIN"] == "PSU_BANK"                     # real classification unaffected
    assert any("sector overrides" in m.title.lower() for m in msgs)


async def test_malformed_overrides_file_degrades_gracefully(tmp_path, store, clock):
    """``overrides`` must be a mapping — a list (or any other malformed schema) is E5: alert, but
    classification proceeds exactly as if there were no overrides configured this run."""
    (tmp_path / "sector_overrides.yaml").write_text("overrides: [not, a, mapping]\n", encoding="utf-8")
    msgs, sink = collect_alerts()
    job = make_job(tmp_path, store, clock, make_client(), overrides=None, notify=sink)  # keep the file above
    result = await job.run(D, universe_symbols=["SBIN"])

    assert result.ok is True
    rows = {r["symbol"]: r["sector"] for r in store.get_sector_map(as_of=D)}
    assert rows["SBIN"] == "PSU_BANK"                     # sector classification unaffected
    assert any("sector overrides" in m.title.lower() for m in msgs)


def test_load_sector_overrides_rejects_non_mapping(tmp_path):
    bad = tmp_path / "sector_overrides.yaml"
    bad.write_text("overrides: [not, a, mapping]\n", encoding="utf-8")
    with pytest.raises(ValueError):
        load_sector_overrides(bad)


def test_load_sector_overrides_normalizes_and_flattens(tmp_path):
    path = tmp_path / "sector_overrides.yaml"
    path.write_text(
        "overrides:\n"
        "  financial_services: [hdfcamc, ' iciciamc ']\n",
        encoding="utf-8",
    )
    assert load_sector_overrides(path) == {
        "HDFCAMC": "FINANCIAL_SERVICES", "ICICIAMC": "FINANCIAL_SERVICES",
    }


def test_load_sector_overrides_missing_key_returns_empty_dict(tmp_path):
    path = tmp_path / "sector_overrides.yaml"
    path.write_text("schema_version: 1\n", encoding="utf-8")   # no 'overrides' key at all
    assert load_sector_overrides(path) == {}


def test_committed_sector_overrides_loads_and_contains_amc_seed():
    """The shipped config/sector_overrides.yaml parses; seeded with the AMC names confirmed absent
    from the real scraped Nifty Financial Services index (data/datafeeds/sector_lists.json,
    as_of 2026-08-24) despite NSE tagging both Industry=Financial Services in the universe CSVs."""
    overrides = load_sector_overrides(repo_root() / "config" / "sector_overrides.yaml")
    assert overrides["HDFCAMC"] == "FINANCIAL_SERVICES"
    assert overrides["ICICIAMC"] == "FINANCIAL_SERVICES"


# --------------------------------------------------------------------------- industry fallback (3rd rung)
async def test_industry_label_classifies_symbols_no_index_or_override_claims(tmp_path, store, clock):
    """2026-09-21: under the NIFTY 500 universe the ten sectoral indices claim ~170 names, so the
    rest fell into the single UNCLASSIFIED bucket the gate caps at 1 open position. NSE's Industry
    column classifies them instead — aliased onto a real index bucket where one exists
    (Healthcare → PHARMA), else normalised (Capital Goods → CAPITAL_GOODS)."""
    path = write_industry_csv(tmp_path, INDUSTRY_CSV)
    job = make_job(tmp_path, store, clock, make_client(), industry_paths=(path,))
    result = await job.run(D, universe_symbols=["ACMELAB", "BOLTENG", "DRIFTBE", "RELIANCE"])

    assert result.ok is True
    rows = {r["symbol"]: r["sector"] for r in store.get_sector_map(as_of=D)}
    assert rows["ACMELAB"] == "PHARMA"                   # INDUSTRY_SECTOR_ALIASES hit
    assert rows["BOLTENG"] == "CAPITAL_GOODS"            # normalised label, no index behind it
    assert result.industry_classified == 2
    assert rows["DRIFTBE"] == UNCLASSIFIED               # non-EQ row never reaches the fallback
    assert rows["RELIANCE"] == UNCLASSIFIED              # not in the CSV at all — ladder still ends here
    assert result.unclassified == 2                      # DRIFTBE + RELIANCE only


async def test_industry_never_overrides_an_index_or_an_override_classification(tmp_path, store, clock):
    """The fallback is the THIRD rung: ``mapping.setdefault`` after both the index scrape and the
    owner overrides, so a mislabelled Industry column can never move a symbol a real index (SBIN =
    PSU_BANK) or the owner (HDFCAMC = FINANCIAL_SERVICES) already placed."""
    path = write_industry_csv(
        tmp_path,
        "Company Name,Industry,Symbol,Series,ISIN Code\n"
        "State Bank,Capital Goods,SBIN,EQ,\n"            # conflicts with the real PSU_BANK scrape
        "HDFC AMC,Capital Goods,HDFCAMC,EQ,\n"           # conflicts with the owner override
        "New Co,Capital Goods,NEWCO,EQ,\n",              # unclaimed — the only one the rung adds
    )
    job = make_job(
        tmp_path, store, clock, make_client(), overrides=OVERRIDES_YAML, industry_paths=(path,)
    )
    result = await job.run(D, universe_symbols=["SBIN", "HDFCAMC", "NEWCO"])

    assert result.ok is True
    rows = {r["symbol"]: r["sector"] for r in store.get_sector_map(as_of=D)}
    assert rows["SBIN"] == "PSU_BANK"                    # index scrape wins
    assert rows["HDFCAMC"] == "FINANCIAL_SERVICES"       # owner override wins
    assert rows["NEWCO"] == "CAPITAL_GOODS"
    assert result.industry_classified == 1               # NEWCO only


async def test_no_readable_industry_source_degrades_silently_to_unclassified(tmp_path, store, clock, caplog):
    """E5 supplement, weaker than the override rung: a missing path and a path whose CSV has no
    Industry column are both just "no fallback this run" — the run stays ok, nothing raises, the
    symbol keeps the pre-2026-09-21 UNCLASSIFIED behaviour, and NO owner alert fires (the log line
    ``sector_industry_source_unavailable`` is the whole signal)."""
    broken = write_industry_csv(
        tmp_path, "Company Name,Symbol,Series\nA Co,AAA,EQ\n", name="no_industry.csv"
    )
    msgs, sink = collect_alerts()
    job = make_job(
        tmp_path, store, clock, make_client(), notify=sink,
        industry_paths=(tmp_path / "does_not_exist.csv", broken),
    )
    with caplog.at_level(logging.WARNING, logger="engine.datafeeds.sector_map"):
        result = await job.run(D, universe_symbols=["AAA"])

    assert result.ok is True and result.industry_classified == 0
    rows = {r["symbol"]: r["sector"] for r in store.get_sector_map(as_of=D)}
    assert rows["AAA"] == UNCLASSIFIED
    assert msgs == []                                    # E5 supplement: logged, never alerted
    events = [r.getMessage() for r in caplog.records]
    assert "sector_industry_source_unavailable" in events
    assert "sector_industry_source_unreadable" in events  # the no-Industry-column candidate


# --------------------------------------------------------------------------- parsers / seeds
def test_industry_sector_aliases_and_normalises():
    """Aliases fold a label onto its real index bucket; everything else becomes a normalised
    bucket. A blank label is NO classification ("" — never a phantom bucket)."""
    assert industry_sector("Healthcare") == "PHARMA"
    assert industry_sector("  financial services  ") == "FINANCIAL_SERVICES"   # strip + case-insensitive
    assert industry_sector("Power") == "ENERGY"
    assert industry_sector("Oil Gas & Consumable Fuels") == "ENERGY"           # both fold to ENERGY
    assert industry_sector("Capital Goods") == "CAPITAL_GOODS"
    assert industry_sector("Media Entertainment & Publication") == "MEDIA_ENTERTAINMENT_PUBLICATION"
    assert industry_sector("Fast Moving Consumer Goods") == "FMCG"
    assert industry_sector("") == "" and industry_sector("   ") == "" and industry_sector("&&") == ""


def test_industry_aliases_only_name_real_index_sectors():
    """An alias target outside SECTOR_SOURCES would be a phantom bucket per_sector_exposure never
    groups on — the same rule _load_overrides enforces for the owner file."""
    known = {sector for sector, _ in SECTOR_SOURCES}
    assert set(INDUSTRY_SECTOR_ALIASES.values()) <= known


def test_parse_industry_csv_defensive():
    text = (
        "# frozen-copy note\n"
        "Company Name,INDUSTRY,SYMBOL,Series,ISIN Code\n"   # columns located case-insensitively
        "A Co,Healthcare,AAA,EQ,\n"
        "B Co,Capital Goods,BBB,BE,\n"                      # non-EQ series dropped
        "A Co dup,Power,AAA,EQ,\n"                          # first occurrence wins
        "C Co, Capital Goods ,ccc,EQ,\n"                    # label stripped, symbol uppercased
        "D Co,,DDD,EQ,\n"                                   # blank label kept verbatim here
    )
    assert parse_industry_csv(text) == {
        "AAA": "Healthcare", "CCC": "Capital Goods", "DDD": "",
    }
    with pytest.raises(ValueError):                          # no Industry column
        parse_industry_csv("Company Name,Symbol\nA Co,AAA\n")
    with pytest.raises(ValueError):                          # no Symbol column either
        parse_industry_csv("Company Name,Industry\nA Co,Healthcare\n")



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
