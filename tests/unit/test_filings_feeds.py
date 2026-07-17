"""§2.8 corporate-filings layer (stage 1, O14, E5): defensive parsers over probe-verified NSE shapes,
the BSE ``error_Bse.html``-as-200 quirk, content-hash id stability, watermark windowing, store
round-trips, and the per-source/per-symbol degrade-never-raise contract.

Fixtures are lifted VERBATIM from the Phase-1 probe result files where a real capture exists (PIT,
results, SHP-master, PeerSmartSearch HTML). The BSE SHP DETAIL stack (``shp_quarter_index.json`` /
``shp_detail.json``) is REPRESENTATIVE, not probe-verified: those endpoints returned ``error_Bse.html``
during probing (see the ``filings_shp`` module docstring) — the parsers key off BSE's
structurally-verified Table/Table1 envelope with [VERIFY Phase-1] field aliases.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from engine.core.bse_http import BseError, bse_get
from engine.core.clock import IST
from engine.datafeeds import filings_pit as fpit
from engine.datafeeds import filings_results as fres
from engine.datafeeds import filings_shp as fshp
from engine.datafeeds.earnings_calendar import EarningsCalendarJob
from engine.datafeeds.filings_pit import FilingsPitJob, insider_id, parse_pit, pit_url
from engine.datafeeds.filings_results import FilingsResultsJob, parse_results
from engine.datafeeds.filings_shp import (
    FilingsShpJob,
    parse_shp_detail,
    parse_shp_master,
    parse_shp_quarter_index,
    qtrid_for,
)
from engine.datafeeds.isin_map import (
    IsinMapJob,
    parse_announcements_isin,
    parse_constituents_isin,
    parse_peersmartsearch,
    scrip_for_isin,
)
from engine.marketdata.store import MarketStore
from tests.conftest import FIXED_NOW

FIXTURES = Path(__file__).parent / "fixtures"
D = FIXED_NOW.date()

PIT_JSON = json.loads((FIXTURES / "filings_pit.json").read_text(encoding="utf-8"))
RESULTS_JSON = json.loads((FIXTURES / "filings_results.json").read_text(encoding="utf-8"))
SHP_MASTER_JSON = json.loads((FIXTURES / "shp_master.json").read_text(encoding="utf-8"))
SHP_QUARTER_INDEX_JSON = json.loads((FIXTURES / "shp_quarter_index.json").read_text(encoding="utf-8"))
SHP_DETAIL_JSON = json.loads((FIXTURES / "shp_detail.json").read_text(encoding="utf-8"))
EVENT_CALENDAR_JSON = json.loads((FIXTURES / "event_calendar.json").read_text(encoding="utf-8"))
PEER_HTML = (FIXTURES / "peersmartsearch.html").read_text(encoding="utf-8")
BSE_ERROR_HTML = (FIXTURES / "bse_error_page.html").read_text(encoding="utf-8")

CONSTITUENTS_CSV = (
    "Company Name,Industry,Symbol,Series,ISIN Code\n"
    "Reliance Industries Ltd.,Energy,RELIANCE,EQ,INE002A01018\n"
    "Tata Consultancy Services Ltd.,IT,TCS,EQ,INE467B01029\n"
    "Some Bond,Debt,BONDX,N2,INE999X01011\n"       # non-EQ series dropped
)


@pytest.fixture(autouse=True)
def _no_waits(monkeypatch):
    """Collapse every backoff/pacing sleep so degrade-path + BSE-spacing tests never actually wait."""

    async def _instant(_delay):
        return None

    monkeypatch.setattr("engine.core.nse_http._sleep", _instant)
    monkeypatch.setattr("engine.core.bse_http._sleep", _instant)
    monkeypatch.setattr("engine.datafeeds.filings_shp._sleep", _instant)
    monkeypatch.setattr("engine.datafeeds.isin_map._sleep", _instant)


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


def client_serving(response: httpx.Response) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(lambda request: response))


def failing_client() -> httpx.AsyncClient:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("unreachable", request=request)

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def routed_client(routes: dict, recorder: list | None = None) -> httpx.AsyncClient:
    """MockTransport routing by URL substring; the homepage prime (nse_get) always 200s."""

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if recorder is not None:
            recorder.append(url)
        if request.url.host == "www.nseindia.com" and request.url.path == "/":
            return httpx.Response(200, text="ok")
        for needle, resp in routes.items():
            if needle in url:
                return resp() if callable(resp) else resp
        raise AssertionError(f"unrouted URL: {url}")

    return httpx.AsyncClient(transport=httpx.MockTransport(handler), follow_redirects=True)


# =========================================================================== bse_http (§2.8 quirk)
async def test_bse_get_success_returns_response():
    resp = await bse_get(client_serving(httpx.Response(200, json={"Table": []})), "https://api.bseindia.com/x", timeout=5)
    assert resp.status_code == 200 and json.loads(resp.content) == {"Table": []}


async def test_bse_get_error_page_body_is_a_failure():
    """A 200 whose body is the error_Bse.html HTML (fails json.loads) raises BseError (§2.8)."""
    client = client_serving(httpx.Response(200, text=BSE_ERROR_HTML))
    with pytest.raises(BseError):
        await bse_get(client, "https://api.bseindia.com/BseIndiaAPI/api/ShareholdingPattern/w", timeout=5)


async def test_bse_get_error_url_is_a_failure():
    """A 200 whose FINAL URL is error_Bse.html raises BseError even with a JSON body (§2.8)."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("error_Bse.html"):
            return httpx.Response(200, json={"ok": 1})          # even a JSON body at that URL is the quirk
        return httpx.Response(302, headers={"Location": "https://api.bseindia.com/error_Bse.html"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler), follow_redirects=True)
    with pytest.raises(BseError):
        await bse_get(client, "https://api.bseindia.com/x", timeout=5)


async def test_bse_get_retries_transient_then_succeeds():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] < 2:
            raise httpx.ConnectError("boom", request=request)
        return httpx.Response(200, json={"Table": []})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    resp = await bse_get(client, "https://api.bseindia.com/x", timeout=5)
    assert resp.status_code == 200 and calls["n"] == 2


# =========================================================================== PIT (insider trades)
def test_parse_pit_fixture():
    rows = parse_pit(PIT_JSON)
    by_symbol = {r["symbol"]: r for r in rows}
    assert set(by_symbol) == {"HDFCLIFE", "RELIANCE"}          # blank-symbol/date row skipped
    hl = by_symbol["HDFCLIFE"]
    assert hl["acq_mode"] == "ESOP" and hl["txn_type"] == "Buy"
    assert hl["qty"] == 37620 and hl["value"] == Decimal("20280942")
    assert hl["person_category"] == "Employees/Designated Employees"
    assert hl["before_pct"] == 0.01 and hl["after_pct"] == 0.01
    assert hl["txn_from"] == date(2023, 1, 25) and hl["intim_dt"] == date(2023, 1, 30)
    assert hl["broadcast_dt"] == datetime(2023, 1, 31, 20, 39, tzinfo=IST)   # point-in-time key
    # A gift's secVal='0' is a real zero value, NOT dropped by or-chaining (the _first_present fix).
    assert by_symbol["RELIANCE"]["value"] == Decimal("0") and by_symbol["RELIANCE"]["qty"] == 150000


def test_insider_id_content_hash_is_stable_and_discriminating():
    dt = datetime(2023, 1, 31, 20, 39, tzinfo=IST)
    a = insider_id("HDFCLIFE", "PANKAJ GUPTA", dt, "Buy", 37620, Decimal("20280942"))
    b = insider_id("HDFCLIFE", "PANKAJ GUPTA", dt, "Buy", 37620, Decimal("20280942"))
    c = insider_id("HDFCLIFE", "PANKAJ GUPTA", dt, "Buy", 37621, Decimal("20280942"))  # qty differs
    assert a == b and a != c and len(a) == 64          # sha256 hexdigest


def test_pit_url_has_explicit_window():
    assert pit_url(date(2023, 1, 1), date(2023, 1, 31)) == (
        "https://www.nseindia.com/api/corporates-pit?index=equities"
        "&from_date=01-01-2023&to_date=31-01-2023"
    )


async def test_filings_pit_run_persists_and_is_idempotent(store, clock):
    job = FilingsPitJob(store, clock, client_serving(httpx.Response(200, json=PIT_JSON)))
    result = await job.run(D)
    assert result.ok is True and result.rows_written == 2
    again = await job.run(D)                            # content-hash PK ⇒ upsert, not duplicate
    assert again.ok is True
    rows = store.get_insider_trades()
    assert len(rows) == 2
    hl = next(r for r in rows if r["symbol"] == "HDFCLIFE")
    assert hl["value"] == Decimal("20280942.00") and hl["broadcast_dt"] == datetime(2023, 1, 31, 20, 39, tzinfo=IST)
    assert hl["ingested_at"] == FIXED_NOW              # Clock-stamped, tz-aware IST


async def test_filings_pit_window_keys_off_watermark(store, clock):
    # Seed a stored broadcast on 2026-06-10; the run-day is D (2026-06-17): window = [10-06, 17-06].
    store.upsert_insider_trades(
        [{"id": "seed", "symbol": "X", "broadcast_dt": datetime(2026, 6, 10, 18, 0, tzinfo=IST)}]
    )
    seen: list[str] = []
    job = FilingsPitJob(store, clock, routed_client({"corporates-pit": httpx.Response(200, json={"data": []})}, seen))
    await job.run(D)
    pit_calls = [u for u in seen if "corporates-pit" in u]
    assert pit_calls and "from_date=10-06-2026" in pit_calls[0] and "to_date=17-06-2026" in pit_calls[0]


async def test_filings_pit_failure_degrades_and_warns(store, clock):
    msgs, sink = collect_alerts()
    result = await FilingsPitJob(store, clock, failing_client(), notify=sink).run(D)
    assert result.ok is False and result.degraded is True    # never raises (E5)
    assert msgs and msgs[0].data["job_id"] == "filings_pit"
    assert msgs[0].severity == "warning"                     # filings are NOT safety-critical (§2.8 rule iii)
    assert fpit.NSE_PIT_URL.startswith("https://www.nseindia.com/")


# =========================================================================== results filings
def test_parse_results_fixture():
    rows = parse_results(RESULTS_JSON)
    by_symbol = {r["symbol"]: r for r in rows}
    assert set(by_symbol) == {"VIDEOIND", "RNAVAL"}          # blank-symbol row skipped
    vi = by_symbol["VIDEOIND"]
    assert vi["period_end"] == date(2024, 12, 31)            # from toDate
    assert vi["consolidated"] is False and vi["audited"] is False
    assert vi["broadcast_dt"] == datetime(2026, 6, 25, 16, 39, 17, tzinfo=IST)
    assert vi["revenue"] is None and vi["pat"] is None and vi["eps"] is None   # stage-2 columns NULL
    rn = by_symbol["RNAVAL"]
    assert rn["consolidated"] is True and rn["audited"] is True
    assert rn["exchdiss_dt"] == datetime(2023, 4, 6, 18, 18, 2, tzinfo=IST)


async def test_filings_results_persists_both_legs(store, clock):
    client = routed_client({
        "corporates-financial-results": httpx.Response(200, json=RESULTS_JSON),
        "event-calendar": httpx.Response(200, json=EVENT_CALENDAR_JSON),
    })
    earnings = EarningsCalendarJob(store, clock, client)   # provider shares the routed client
    job = FilingsResultsJob(store, clock, client, earnings=earnings)
    result = await job.run(D)
    assert result.ok is True and result.degraded is False
    assert result.results_written == 2 and result.events_written == 3
    assert {r["symbol"] for r in store.get_results_filings()} == {"VIDEOIND", "RNAVAL"}
    assert store.get_earnings_calendar(date(2026, 7, 1), date(2026, 7, 31))   # board-meeting dates merged


async def test_filings_results_partial_failure_keeps_other_leg(store, clock):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "www.nseindia.com" and request.url.path == "/":
            return httpx.Response(200, text="ok")
        if "corporates-financial-results" in str(request.url):
            raise httpx.ConnectError("results down", request=request)
        return httpx.Response(200, json=EVENT_CALENDAR_JSON)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler), follow_redirects=True)
    earnings = EarningsCalendarJob(store, clock, client)
    msgs, sink = collect_alerts()
    result = await FilingsResultsJob(store, clock, client, earnings=earnings, notify=sink).run(D)
    assert result.ok is False and result.degraded is True
    assert result.failed_legs == ("results",)
    assert result.events_written == 3                        # the other leg still ingested
    assert msgs and msgs[0].data["job_id"] == "filings_results" and msgs[0].severity == "warning"


# =========================================================================== SHP (master + BSE detail)
def test_parse_shp_master_fixture():
    subs = parse_shp_master(SHP_MASTER_JSON)
    by_symbol = {s.symbol: s for s in subs}
    assert set(by_symbol) == {"SHAH", "RELIANCE"}            # blank-symbol row skipped
    shah = by_symbol["SHAH"]
    assert shah.qtr_end == date(2026, 6, 30) and shah.revised is False
    assert shah.broadcast_dt == datetime(2026, 7, 1, 15, 2, 19, tzinfo=IST)
    assert shah.isin == "INE482J01021"


def test_parse_shp_quarter_index_and_qtrid_lookup():
    # Live-verified: qtrid is a float ('130.0') normalized to '130'; qtr is a 'June 2026' LABEL whose
    # month-end (30-Jun-2026) matches the SHP-master's date.
    idx = parse_shp_quarter_index(SHP_QUARTER_INDEX_JSON)
    assert {e["qtrid"] for e in idx} == {"130", "129", "128"}
    assert qtrid_for(idx, date(2026, 6, 30)) == "130"
    assert qtrid_for(idx, date(2026, 3, 31)) == "129"
    assert qtrid_for(idx, date(2020, 1, 1)) is None


def test_parse_shp_detail_verified_shape():
    # Live-verified: the SEBI per-category table is Table1 (Fld_ShortName / Fld_TotalNoOfShares /
    # Fld_TotalPercentageOf_A_B_C2 / Fld_PledgeEncumbered*).
    explicit = datetime(2026, 7, 2, 11, 20, 5, tzinfo=IST)
    rows = parse_shp_detail(
        SHP_DETAIL_JSON, symbol="RELIANCE", qtr_end=date(2026, 6, 30),
        broadcast_dt=explicit, revised=False,
    )
    by_cat = {r["category"]: r for r in rows}
    assert set(by_cat) == {"(A) Promoter & Promoter Group", "(B) Public"}
    prom = by_cat["(A) Promoter & Promoter Group"]
    assert prom["holders"] == 47 and prom["shares"] == 6715496096 and prom["pct"] == 50.48
    assert prom["pledged_shares"] == 84720861 and prom["pledged_pct"] == 1.26
    assert prom["source"] == "bse" and prom["symbol"] == "RELIANCE"
    # An explicitly supplied broadcast (the daily NSE-master path) always wins.
    assert all(r["broadcast_dt"] == explicit for r in rows)


def test_parse_shp_detail_falls_back_to_declaration_authorise_date():
    # §2.8 rule i: the backfill path passes broadcast_dt=None (quarter loops carry no NSE-master
    # timestamp); the payload's OWN declaration-row Fld_AuthoriseDate must land on every category
    # row — this was the defect that NULLed all 13k backfilled rows and faked pledge-leg n=0.
    rows = parse_shp_detail(
        SHP_DETAIL_JSON, symbol="RELIANCE", qtr_end=date(2026, 6, 30),
        broadcast_dt=None, revised=False,
    )
    assert len(rows) == 2
    expected = datetime(2026, 7, 16, 19, 24, 53, 207000, tzinfo=IST)   # fixture Fld_AuthoriseDate
    assert all(r["broadcast_dt"] == expected for r in rows)


def test_parse_shp_detail_missing_authorise_date_stays_null():
    # No declaration timestamp anywhere ⇒ NULL broadcast_dt (never invented, §2.8 rule i).
    payload = {
        "Table": [{"Fld_TransactionId": 1, "Qtr_Id": 130.0}],           # no Fld_AuthoriseDate
        "Table1": SHP_DETAIL_JSON["Table1"],
    }
    rows = parse_shp_detail(
        payload, symbol="RELIANCE", qtr_end=date(2026, 6, 30), broadcast_dt=None, revised=False,
    )
    assert len(rows) == 2
    assert all(r["broadcast_dt"] is None for r in rows)


async def test_filings_shp_fetches_new_submissions(store, clock):
    # symbol_isin must carry the BSE scrip code (else the symbol is skipped-and-counted).
    store.upsert_symbol_isin([
        {"symbol": "SHAH", "isin": "INE482J01021", "bse_scrip_code": "543210", "as_of": D},
        {"symbol": "RELIANCE", "isin": "INE002A01018", "bse_scrip_code": "500325", "as_of": D},
    ])
    client = routed_client({
        "corporate-share-holdings-master": httpx.Response(200, json=SHP_MASTER_JSON),
        "SHPQNewFormat": httpx.Response(200, json=SHP_QUARTER_INDEX_JSON),
        "CorporatesSHPSecuritybeta": httpx.Response(200, json=SHP_DETAIL_JSON),
    })
    result = await FilingsShpJob(store, clock, client).run()
    assert result.ok is True
    assert result.new_submissions == 2 and result.symbols_upserted == 2
    assert result.rows_written == 4                          # 2 categories × 2 symbols
    assert result.skipped_no_scrip == 0 and result.failed_symbols == 0
    cats = {(r["symbol"], r["category"]) for r in store.get_shp_quarterly()}
    assert ("SHAH", "(B) Public") in cats and ("RELIANCE", "(A) Promoter & Promoter Group") in cats


async def test_filings_shp_skips_symbol_without_scrip_code(store, clock):
    store.upsert_symbol_isin([{"symbol": "RELIANCE", "isin": "INE002A01018", "bse_scrip_code": "500325", "as_of": D}])
    client = routed_client({
        "corporate-share-holdings-master": httpx.Response(200, json=SHP_MASTER_JSON),
        "SHPQNewFormat": httpx.Response(200, json=SHP_QUARTER_INDEX_JSON),
        "CorporatesSHPSecuritybeta": httpx.Response(200, json=SHP_DETAIL_JSON),
    })
    result = await FilingsShpJob(store, clock, client).run()
    assert result.skipped_no_scrip == 1                      # SHAH has no mapping ⇒ skipped, not failed
    assert result.symbols_upserted == 1 and result.degraded is True


async def test_filings_shp_watermark_filters_old_submissions(store, clock):
    store.upsert_symbol_isin([{"symbol": "RELIANCE", "isin": "INE002A01018", "bse_scrip_code": "500325", "as_of": D}])
    # Watermark = SHAH's broadcast (01-Jul 15:02:19). Only RELIANCE (02-Jul) is newer.
    store.upsert_shp_quarterly([{
        "symbol": "SHAH", "qtr_end": date(2026, 6, 30), "category": "Public",
        "broadcast_dt": datetime(2026, 7, 1, 15, 2, 19, tzinfo=IST), "source": "bse",
    }])
    client = routed_client({
        "corporate-share-holdings-master": httpx.Response(200, json=SHP_MASTER_JSON),
        "SHPQNewFormat": httpx.Response(200, json=SHP_QUARTER_INDEX_JSON),
        "CorporatesSHPSecuritybeta": httpx.Response(200, json=SHP_DETAIL_JSON),
    })
    result = await FilingsShpJob(store, clock, client).run()
    assert result.new_submissions == 1 and result.symbols_upserted == 1


async def test_filings_shp_bse_error_page_degrades_without_raising(store, clock):
    store.upsert_symbol_isin([{"symbol": "RELIANCE", "isin": "INE002A01018", "bse_scrip_code": "500325", "as_of": D}])
    store.upsert_symbol_isin([{"symbol": "SHAH", "isin": "INE482J01021", "bse_scrip_code": "543210", "as_of": D}])
    client = routed_client({
        "corporate-share-holdings-master": httpx.Response(200, json=SHP_MASTER_JSON),
        "SHPQNewFormat": httpx.Response(200, text=BSE_ERROR_HTML),   # the documented 200+error page
        "CorporatesSHPSecuritybeta": httpx.Response(200, json=SHP_DETAIL_JSON),
    })
    result = await FilingsShpJob(store, clock, client).run()     # must not raise (E5)
    assert result.ok is True and result.degraded is True
    assert result.failed_symbols == 2 and result.rows_written == 0


async def test_filings_shp_master_failure_degrades(store, clock):
    msgs, sink = collect_alerts()
    result = await FilingsShpJob(store, clock, failing_client(), notify=sink).run()
    assert result.ok is False and result.degraded is True
    assert msgs and msgs[0].data["job_id"] == "filings_shp"
    assert fshp.NSE_SHP_MASTER_URL.startswith("https://www.nseindia.com/")


# =========================================================================== ISIN map
def test_parse_constituents_isin_keeps_isin_column():
    mapping = parse_constituents_isin(CONSTITUENTS_CSV)
    assert mapping == {"RELIANCE": "INE002A01018", "TCS": "INE467B01029"}   # non-EQ BONDX dropped


def test_parse_announcements_isin_fallback():
    payload = [{"symbol": "KTKBANK", "sm_isin": "INE614B01018"}, {"symbol": "", "sm_isin": "X"}]
    assert parse_announcements_isin(payload) == {"KTKBANK": "INE614B01018"}


def test_parse_peersmartsearch_and_scrip_lookup():
    entries = parse_peersmartsearch(PEER_HTML)
    assert len(entries) == 4
    assert scrip_for_isin(entries, "INE002A01018") == "500325"     # RELIANCE, among several results
    assert scrip_for_isin(entries, "INE036A01016") == "500390"     # RELINFRA
    assert scrip_for_isin(entries, "INEZZZZZZZZZ0") is None


async def test_isin_map_build_persists_with_scrip_codes(store, clock, monkeypatch):
    # Isolate from the filesystem: the CSV layer returns a fixed mapping; the BSE resolve is mocked.
    monkeypatch.setattr(
        "engine.datafeeds.isin_map.load_constituents_isin",
        lambda settings: {"RELIANCE": "INE002A01018"},
    )
    from engine.core.config import load_settings

    # PeerSmartSearch is served application/json with the HTML wrapped as a JSON string (probe-verified).
    client = client_serving(httpx.Response(200, json=PEER_HTML))
    job = IsinMapJob(load_settings(), store, clock, client)
    result = await job.run(["RELIANCE"])
    assert result.ok is True and result.with_isin == 1 and result.scrip_resolved == 1
    row = store.get_symbol_isin(symbol="RELIANCE")[0]
    assert row["isin"] == "INE002A01018" and row["bse_scrip_code"] == "500325"
    assert row["as_of"] == D


async def test_isin_map_missing_scrip_is_null_not_a_failure(store, clock, monkeypatch):
    monkeypatch.setattr(
        "engine.datafeeds.isin_map.load_constituents_isin",
        lambda settings: {"RELIANCE": "INE002A01018"},
    )
    from engine.core.config import load_settings

    # PeerSmartSearch returns the BSE error page ⇒ resolve degrades to a NULL scrip code (never raises).
    client = client_serving(httpx.Response(200, text=BSE_ERROR_HTML))
    result = await IsinMapJob(load_settings(), store, clock, client).run(["RELIANCE"])
    assert result.ok is True and result.scrip_resolved == 0
    row = store.get_symbol_isin(symbol="RELIANCE")[0]
    assert row["isin"] == "INE002A01018" and row["bse_scrip_code"] is None
