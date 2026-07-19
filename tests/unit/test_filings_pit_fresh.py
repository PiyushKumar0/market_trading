"""§2.8 stage-3 BSE FRESH insider feed (``filings_pit_fresh``): defensive parse over the probe-verified
``getCorp_Regulation_ng/w`` shape, the equity filter (startswith 'equity'), scrip->symbol resolution +
skip counting, txn_type mapping (incl. Revoke / Pledge pass-through), source-tagged (``bse:``) content-
hash ids, the ~25-row-cap per-day subdivision, store round-trip + idempotency, and the E5
degrade-never-raise contract.

Fixtures (``fixtures/filings_pit_fresh.json``) are lifted VERBATIM from the 2026-07-19 BSE probe
captures (``bse_insider_out/01_default_isdefault1.json`` + ``14_reliance_2023_2024.json``). The live
``data/market.duckdb`` is never touched — every store is a tmp DuckDB.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from engine.core.clock import IST
from engine.datafeeds import filings_pit_fresh as fresh
from engine.datafeeds.filings_pit_fresh import (
    BSE_ID_PREFIX,
    FilingsPitFreshJob,
    fresh_url,
    parse_pit_fresh,
)
from engine.marketdata.store import MarketStore
from tests.conftest import FIXED_NOW

FIXTURES = Path(__file__).parent / "fixtures"
D = FIXED_NOW.date()

FRESH_JSON = json.loads((FIXTURES / "filings_pit_fresh.json").read_text(encoding="utf-8"))

# scrip -> symbol reverse map (bare-int-string keys, as store.bse_scrip_symbol_map builds). NINtec's
# 539843 is DELIBERATELY absent so it exercises the unmapped-scrip skip on an EQUITY row.
SCRIP_MAP = {
    "544759": "GOLDLINE", "539436": "COFFEEDAY", "504341": "RAVINDRA",
    "940227": "PRACHAY", "533148": "JSWENERGY", "500325": "RELIANCE",
}


def serving_json(payload) -> httpx.AsyncClient:
    """A client that returns a FRESH Response(200, json=payload) for every request (the fresh job
    fetches the Isdefault=1 + Isdefault=2 surfaces, so each call needs its own response)."""
    return httpx.AsyncClient(transport=httpx.MockTransport(lambda _req: httpx.Response(200, json=payload)))


def failing_client() -> httpx.AsyncClient:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("unreachable", request=request)

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def collect_alerts():
    msgs = []

    async def sink(msg):
        msgs.append(msg)

    return msgs, sink


@pytest.fixture
def store(tmp_path, clock):
    s = MarketStore(tmp_path / "market.duckdb", tmp_path / "parquet", clock).open()
    yield s
    s.close()


@pytest.fixture
def seeded_store(store):
    """Store with the symbol_isin scrip mappings (NINtec's 539843 left unmapped)."""
    store.upsert_symbol_isin(
        [
            {"symbol": sym, "isin": f"INE{code}", "bse_scrip_code": code, "as_of": D}
            for code, sym in SCRIP_MAP.items()
        ]
    )
    return store


# =========================================================================== URL + id
def test_fresh_url_surfaces():
    assert fresh_url(isdefault=1) == (
        "https://api.bseindia.com/BseIndiaAPI/api/getCorp_Regulation_ng/w"
        "?scripCode=&Regulation=&fromDT=&ToDate=&Isdefault=1"
    )
    assert "fromDT=20260614&ToDate=20260617&Isdefault=2" in fresh_url(
        isdefault=2, frm="20260614", to="20260617"
    )


# =========================================================================== parse (verbatim probe)
def test_parse_pit_fresh_field_map_and_filters():
    parse = parse_pit_fresh(FRESH_JSON, SCRIP_MAP)
    assert parse.raw_rows == 8
    # 5 equity+mapped rows kept; Ravindra ('Any other instrument') + Prachay ('Debentures') are
    # non-equity; NINtec (scrip 539843) is equity but unmapped.
    assert parse.skipped_non_equity == 2
    assert parse.skipped_unmapped_scrip == 1
    assert parse.skipped_no_broadcast == 0
    assert len(parse.rows) == 5

    by_id = {r["id"]: r for r in parse.rows}
    assert all(rid.startswith(BSE_ID_PREFIX) and len(rid) == len(BSE_ID_PREFIX) + 64 for rid in by_id)

    gold = next(r for r in parse.rows if r["symbol"] == "GOLDLINE")
    assert gold["txn_type"] == "Buy" and gold["acq_mode"] == "Market Purchase"       # Acquisition -> Buy
    assert gold["qty"] == 60000 and gold["value"] == Decimal("2595960.00")
    assert gold["before_pct"] == 28.65 and gold["after_pct"] == 29.27
    assert gold["txn_from"] == date(2026, 6, 22) and gold["intim_dt"] == date(2026, 6, 23)
    assert gold["person_category"] == "Promoter & Director"
    # Fld_CreateDate -> broadcast_dt (IST, fractional seconds preserved)
    assert gold["broadcast_dt"] == datetime(2026, 6, 23, 20, 30, 11, 843000, tzinfo=IST)


def test_parse_pit_fresh_txn_type_mapping_incl_revoke_and_pledge():
    parse = parse_pit_fresh(FRESH_JSON, SCRIP_MAP)
    coffee = next(r for r in parse.rows if r["symbol"] == "COFFEEDAY")
    assert coffee["txn_type"] == "Revoke" and coffee["acq_mode"] == "Pledge Released"  # Revoke -> Revoke
    jsw = next(r for r in parse.rows if r["symbol"] == "JSWENERGY")
    assert jsw["txn_type"] == "Sell" and jsw["acq_mode"] == "ESOP"                      # Disposal -> Sell
    reliance = [r for r in parse.rows if r["symbol"] == "RELIANCE"]
    kinds = {r["txn_type"] for r in reliance}
    assert kinds == {"Buy", "Pledge"}                                                  # Pledge passes through
    buy = next(r for r in reliance if r["txn_type"] == "Buy")
    assert buy["value"] == Decimal("0")                                                # zero-consideration, kept


def test_parse_pit_fresh_empty_payloads():
    # BSE wide-range Isdefault=2 returns {} and a scrip query returns {"Table": []} (both -> 0 rows).
    for empty in ({}, {"Table": []}):
        parse = parse_pit_fresh(empty, SCRIP_MAP)
        assert parse.rows == [] and parse.raw_rows == 0


# =========================================================================== store round-trip / job
async def test_job_persists_deduped_and_is_idempotent(seeded_store, clock):
    job = FilingsPitFreshJob(seeded_store, clock, serving_json(FRESH_JSON))
    result = await job.run(D)
    # Both surfaces return the same 8-row payload -> 5 valid rows, deduped on the content-hash id.
    assert result.ok is True and result.degraded is False
    assert result.rows_written == 5 and result.windows_subdivided == 0
    # Both surfaces parse the SAME 8-row payload here: rows dedupe on the id (5 unique), but the skip
    # tallies are per-parse diagnostics and so accumulate across the two surfaces (2+2 / 1+1).
    assert result.skipped_non_equity == 4 and result.skipped_unmapped_scrip == 2

    rows = seeded_store.get_insider_trades()
    assert len(rows) == 5
    assert all(r["id"].startswith(BSE_ID_PREFIX) for r in rows)
    gold = next(r for r in rows if r["symbol"] == "GOLDLINE")
    assert gold["value"] == Decimal("2595960.00")
    assert gold["broadcast_dt"] == datetime(2026, 6, 23, 20, 30, 11, 843000, tzinfo=IST)
    assert gold["ingested_at"] == FIXED_NOW                       # Clock-stamped, tz-aware IST

    again = await job.run(D)                                       # content-hash PK -> upsert, no dupes
    assert again.ok is True
    assert len(seeded_store.get_insider_trades()) == 5


# =========================================================================== ~25-row cap subdivision
def _cap_rows(n: int) -> list[dict]:
    return [
        {
            "Fld_ScripCode": 500325, "Fld_SecurityTypeName": "Equity Shares",
            "Fld_TransactionType": "Acquisition", "ModeOfAquisation": "Market Purchase",
            "Fld_SecurityNo": 100 + i, "Fld_SecurityValue": "1000000.00",
            "Fld_CreateDate": "2026-06-15T20:00:00", "Fld_PromoterName": f"P{i}",
        }
        for i in range(n)
    ]


async def test_windowed_cap_triggers_per_day_subdivision(seeded_store, clock):
    """An Isdefault=2 window returning >= 25 rows is assumed truncated -> refetched per-day (§2.8)."""
    per_day: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        q = request.url.params
        if q.get("Isdefault") == "1":
            return httpx.Response(200, json={"Table": []})
        frm, to = q.get("fromDT"), q.get("ToDate")
        if frm != to:                                   # the [d-3, d] window -> hit the cap
            return httpx.Response(200, json={"Table": _cap_rows(25)})
        per_day.append(frm)                             # a per-day refetch
        return httpx.Response(200, json={"Table": []})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    result = await FilingsPitFreshJob(seeded_store, clock, client).run(D)
    assert result.windows_subdivided == 1
    # D = 2026-06-17, window [d-3, d] -> the four days 14..17 refetched individually.
    assert per_day == ["20260614", "20260615", "20260616", "20260617"]
    assert result.ok is True


# =========================================================================== E5 degrade
async def test_both_surfaces_fail_degrades_and_warns(seeded_store, clock):
    msgs, sink = collect_alerts()
    result = await FilingsPitFreshJob(seeded_store, clock, failing_client(), notify=sink).run(D)
    assert result.ok is False and result.degraded is True         # never raises (E5)
    assert result.failed_sources == ("isdefault1", "isdefault2")
    assert result.rows_written == 0
    assert msgs and msgs[0].data["job_id"] == "filings_pit_fresh"
    assert msgs[0].severity == "warning"                          # filings are NOT safety-critical
    assert fresh.BSE_FRESH_URL.startswith("https://api.bseindia.com/")


def test_bse_scrip_symbol_map_round_trip(seeded_store):
    m = seeded_store.bse_scrip_symbol_map()
    assert m["500325"] == "RELIANCE" and m["544759"] == "GOLDLINE"
    assert "539843" not in m                                      # NINtec never seeded
