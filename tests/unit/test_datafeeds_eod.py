"""EOD datafeeds jobs (§4.4 jobs 6–9, E5): bhavcopy UDiFF parse + bars_1d cross-check/fill,
corp-actions purpose classification + persistence (A12), earnings-calendar kinds (R2/O13), and
bulk/block deals → ``flagged_instrument_days`` with per-source degradation. All offline fixtures;
every job degrades + alerts and never raises into the scheduler."""

from __future__ import annotations

import io
import json
import zipfile
from datetime import date
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from engine.datafeeds import corp_actions as ca
from engine.datafeeds import deals as dl
from engine.datafeeds import earnings_calendar as ec
from engine.datafeeds.bhavcopy import BhavcopyJob, parse_bhavcopy_csv
from engine.datafeeds.corp_actions import CorpActionsJob, classify_purpose, parse_corp_actions
from engine.datafeeds.deals import REASON_BLOCK, REASON_BULK, DealsJob, parse_deals
from engine.datafeeds.earnings_calendar import (
    KIND_BOARD_MEETING,
    KIND_RESULTS,
    EarningsCalendarJob,
    classify_event,
    parse_event_calendar,
)
from engine.marketdata.store import DailyBar, MarketStore
from tests.conftest import FIXED_NOW

FIXTURES = Path(__file__).parent / "fixtures"
D = FIXED_NOW.date()

BHAVCOPY_CSV = (FIXTURES / "bhavcopy_udiff.csv").read_text(encoding="utf-8")
CORP_ACTIONS_JSON = json.loads((FIXTURES / "corp_actions.json").read_text(encoding="utf-8"))
EVENT_CALENDAR_JSON = json.loads((FIXTURES / "event_calendar.json").read_text(encoding="utf-8"))
BULK_DEALS_JSON = json.loads((FIXTURES / "bulk_deals.json").read_text(encoding="utf-8"))
BLOCK_DEALS_JSON = json.loads((FIXTURES / "block_deals.json").read_text(encoding="utf-8"))


@pytest.fixture(autouse=True)
def _no_backoff_wait(monkeypatch):
    """A3/A4: the feeds now fetch through nse_get, which retries transient failures with exponential
    backoff. Collapse the sleep so the degrade-path tests (failing clients) don't actually wait — the
    retry/backoff schedule itself is asserted in test_nse_http.py."""

    async def _instant(_delay):
        return None

    monkeypatch.setattr("engine.core.nse_http._sleep", _instant)


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
        raise httpx.ConnectError("nse unreachable", request=request)

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


# =========================================================================== bhavcopy (§4.4 job 6)
def test_parse_bhavcopy_udiff_defensive():
    bars = parse_bhavcopy_csv(BHAVCOPY_CSV, D)
    by_symbol = {b.symbol: b for b in bars}
    # Non-EQ series dropped, other-day row dropped (wrong file), malformed row skipped-and-counted.
    assert set(by_symbol) == {"RELIANCE", "TCS"}
    rel = by_symbol["RELIANCE"]
    assert rel.d == D and rel.src == "bhavcopy"
    assert rel.open == Decimal("2900.00") and rel.close == Decimal("2940.25")
    assert rel.high == Decimal("2955.50") and rel.low == Decimal("2890.10")
    assert rel.volume == 5_000_000


async def test_bhavcopy_run_unzips_fills_and_cross_checks(store, clock):
    # A pre-existing Kite-official row for RELIANCE that DISAGREES with the bhavcopy: it stays
    # canonical (A11) — cross-checked + reported, never overwritten. TCS has no row: filled.
    store.upsert_bars_1d(
        [
            DailyBar(
                symbol="RELIANCE", d=D, open=Decimal("2900.00"), high=Decimal("2955.50"),
                low=Decimal("2890.10"), close=Decimal("2999.99"), volume=5_000_000,
                src="kite_official",
            )
        ]
    )
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("BhavCopy_NSE_CM_0_0_0_20260617_F_0000.csv", BHAVCOPY_CSV)
    job = BhavcopyJob(store, clock, client_serving(httpx.Response(200, content=buf.getvalue())))
    result = await job.run(D)

    assert result.ok is True and result.rows_parsed == 2
    assert result.rows_written == 1 and result.rows_cross_checked == 1
    assert result.mismatched_symbols == ("RELIANCE",)
    rel = store.get_bars_1d("RELIANCE", D, D)[0]
    assert rel.src == "kite_official" and rel.close == Decimal("2999.99")   # canonical kept
    tcs = store.get_bars_1d("TCS", D, D)[0]
    assert tcs.src == "bhavcopy" and tcs.close == Decimal("3875.60")        # gap filled


async def test_bhavcopy_failure_degrades_and_alerts(store, clock):
    msgs, sink = collect_alerts()
    job = BhavcopyJob(store, clock, failing_client(), notify=sink)
    result = await job.run(D)                            # must not raise (E5)
    assert result.ok is False and result.degraded is True
    assert msgs and msgs[0].data["job_id"] == "bhavcopy"
    assert store.get_bars_1d("RELIANCE", D, D) == []     # keeps whatever it had (here: nothing)


# =========================================================================== corp actions (job 7)
def test_classify_purpose_deterministic():
    assert classify_purpose("Dividend - Rs 9 Per Share") == ("dividend", None, Decimal("9"))
    assert classify_purpose("Bonus 1:1") == ("bonus", "1:1", None)
    kind, ratio, amount = classify_purpose("Face Value Split (Sub-Division) - From Rs 10/- To Rs 2/-")
    assert kind == "split" and amount is None            # a face value is NOT a payout amount
    # Compound purpose classifies by the structural action (bonus/split before dividend).
    assert classify_purpose("Bonus 2:1 and Dividend Rs 3")[0] == "bonus"
    assert classify_purpose("Scheme of Arrangement") == ("other", None, None)   # recorded, not guessed


def test_parse_corp_actions_fixture():
    rows = parse_corp_actions(CORP_ACTIONS_JSON)
    by_symbol = {r["symbol"]: r for r in rows}
    assert set(by_symbol) == {"RELIANCE", "BEL", "TATAMOTORS", "XYZCO"}     # NODATE skipped
    assert by_symbol["RELIANCE"]["kind"] == "dividend"
    assert by_symbol["RELIANCE"]["amount"] == Decimal("9")
    assert by_symbol["RELIANCE"]["ex_date"] == date(2026, 7, 15)
    assert by_symbol["BEL"]["kind"] == "bonus" and by_symbol["BEL"]["ratio"] == "1:1"
    assert by_symbol["TATAMOTORS"]["kind"] == "split"
    assert by_symbol["XYZCO"]["kind"] == "other"


async def test_corp_actions_run_persists_idempotently(store, clock):
    client = client_serving(httpx.Response(200, json=CORP_ACTIONS_JSON))
    job = CorpActionsJob(store, clock, client)
    result = await job.run(D)
    assert result.ok is True and result.rows_written == 4
    again = await job.run(D)                             # upsert on (symbol, ex_date, kind)
    assert again.ok is True
    rows = store.get_corp_actions(ex_from=date(2026, 7, 1), ex_to=date(2026, 8, 31))
    assert len(rows) == 4
    rel = next(r for r in rows if r["symbol"] == "RELIANCE")
    assert rel["kind"] == "dividend" and rel["amount"] == Decimal("9.00")
    assert rel["recorded_at"] == FIXED_NOW               # Clock-stamped, tz-aware IST


async def test_corp_actions_failure_alerts_critical(store, clock):
    msgs, sink = collect_alerts()
    result = await CorpActionsJob(store, clock, failing_client(), notify=sink).run(D)
    assert result.ok is False and result.degraded is True
    assert msgs and msgs[0].severity == "critical"       # A12 safety-critical set (§2.6 step 5)
    assert msgs[0].data["job_id"] == "corp_actions"
    assert ca.NSE_CORP_ACTIONS_URL.startswith("https://www.nseindia.com/")


# =========================================================================== earnings (job 8)
def test_classify_event_results_vs_board_meeting():
    assert classify_event("Financial Results") == KIND_RESULTS
    assert classify_event("To consider and approve the financial results") == KIND_RESULTS
    assert classify_event("Board Meeting - Fund Raising") == KIND_BOARD_MEETING


def test_parse_event_calendar_fixture():
    rows = parse_event_calendar(EVENT_CALENDAR_JSON)     # top-level list shape
    by_symbol = {r["symbol"]: r for r in rows}
    assert set(by_symbol) == {"TCS", "INFY", "RELIANCE"}                    # blank-symbol row skipped
    assert by_symbol["TCS"] == {
        "symbol": "TCS", "event_date": date(2026, 7, 9), "kind": KIND_RESULTS, "source": "nse",
    }
    assert by_symbol["INFY"]["kind"] == KIND_BOARD_MEETING
    assert by_symbol["RELIANCE"]["event_date"] == date(2026, 7, 19)         # bm_date alias


async def test_earnings_run_persists_and_reads_back(store, clock):
    client = client_serving(httpx.Response(200, json=EVENT_CALENDAR_JSON))
    result = await EarningsCalendarJob(store, clock, client).run(D)
    assert result.ok is True and result.rows_written == 3
    rows = store.get_earnings_calendar(date(2026, 7, 1), date(2026, 7, 31))
    assert {(r["symbol"], r["kind"]) for r in rows} == {
        ("TCS", KIND_RESULTS), ("RELIANCE", KIND_RESULTS), ("INFY", KIND_BOARD_MEETING),
    }
    tcs = store.get_earnings_calendar(date(2026, 7, 9), date(2026, 7, 9), symbol="TCS")
    assert len(tcs) == 1                                 # the R2 per-instrument gate-time lookup


async def test_earnings_failure_alerts_critical(store, clock):
    msgs, sink = collect_alerts()
    result = await EarningsCalendarJob(store, clock, failing_client(), notify=sink).run(D)
    assert result.ok is False and result.degraded is True
    assert msgs and msgs[0].severity == "critical"       # R2 safety-critical set (§2.6 step 5)
    assert msgs[0].data["job_id"] == "earnings_calendar"
    assert ec.NSE_EVENT_CALENDAR_URL.startswith("https://www.nseindia.com/")


# =========================================================================== deals (job 9)
def test_parse_deals_fixture():
    rows = parse_deals(BULK_DEALS_JSON, D, REASON_BULK)
    # Wrong-day row and blank-symbol row dropped; both LOWFLT prints keyed to (symbol, d, reason).
    assert [r["symbol"] for r in rows] == ["LOWFLT", "LOWFLT"]
    assert all(r["d"] == D and r["reason"] == REASON_BULK for r in rows)
    details = json.loads(rows[0]["details"])
    assert details == {"client": "BIG WHALE FUND", "qty": "500000", "price": "101.35"}


async def test_deals_run_flags_bulk_and_block_days(store, clock):
    def handler(request: httpx.Request) -> httpx.Response:
        if "bulk-deals" in str(request.url):
            return httpx.Response(200, json=BULK_DEALS_JSON)
        return httpx.Response(200, json=BLOCK_DEALS_JSON)

    job = DealsJob(store, clock, httpx.AsyncClient(transport=httpx.MockTransport(handler)))
    result = await job.run(D)

    assert result.ok is True and result.degraded is False
    flagged = {(r["symbol"], r["reason"]) for r in store.get_flagged_instrument_days(D)}
    assert flagged == {("LOWFLT", REASON_BULK), ("BLOCKED", REASON_BLOCK)}
    assert store.get_flagged_instrument_days(date(2026, 6, 16)) == []       # wrong-day row not keyed


async def test_deals_partial_failure_keeps_other_source(store, clock):
    def handler(request: httpx.Request) -> httpx.Response:
        if "block-deals" in str(request.url):
            raise httpx.ConnectError("blocked", request=request)
        return httpx.Response(200, json=BULK_DEALS_JSON)

    msgs, sink = collect_alerts()
    job = DealsJob(store, clock, httpx.AsyncClient(transport=httpx.MockTransport(handler)), notify=sink)
    result = await job.run(D)

    assert result.ok is True and result.degraded is True          # one source still ingested
    assert result.failed_sources == ("block",)
    assert {r["symbol"] for r in store.get_flagged_instrument_days(D)} == {"LOWFLT"}
    assert msgs and msgs[0].data["failed_sources"] == ["block"]


async def test_deals_total_failure_never_raises(store, clock):
    msgs, sink = collect_alerts()
    result = await DealsJob(store, clock, failing_client(), notify=sink).run(D)
    assert result.ok is False and result.degraded is True
    assert set(result.failed_sources) == {"bulk", "block"}
    assert store.get_flagged_instrument_days(D) == [] and msgs
    assert dl.NSE_BULK_DEALS_URL_TEMPLATE.startswith("https://www.nseindia.com/")
