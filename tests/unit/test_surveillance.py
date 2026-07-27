"""SurveillanceIngest (§4.4 job 5, A8/E5): per-source parsing (nested NSE JSON shapes + the T2T
series CSV), per-source reuse-yesterday on failure with ``unconfirmed_symbols`` + alert, the
never-raise guarantee, and the held-position-migration diff seam (``new_entries``)."""

from __future__ import annotations

import json

import httpx
import pytest

from engine.universe import surveillance as sv
from engine.universe.surveillance import SurveillanceIngest, SurveillanceLists
from tests.conftest import FIXED_NOW

D = FIXED_NOW.date()

#: The www API host is now cookie-primed by nse_get (A3): the first www fetch GETs this homepage first.
NSE_HOMEPAGE = "https://www.nseindia.com/"


@pytest.fixture(autouse=True)
def _no_backoff_wait(monkeypatch):
    """A3/A4: nse_get retries transient failures with backoff — collapse the sleep so the reuse-yesterday
    (failing-source) tests don't actually wait. Backoff semantics are covered in test_nse_http.py."""

    async def _instant(_delay):
        return None

    monkeypatch.setattr("engine.core.nse_http._sleep", _instant)

# EQUITY_L-style securities master: header cells carry stray spaces (defensive lookup), BE/BZ = T2T.
T2T_CSV = (
    "SYMBOL,NAME OF COMPANY, SERIES, DATE OF LISTING,PAID UP VALUE,MARKET LOT,ISIN NUMBER,FACE VALUE\n"
    "T2TSTK,T2T Company,BE,01-JAN-2010,1,1,INE0T2T01018,1\n"
    "NORMALEQ,Normal Company,EQ,01-JAN-2010,1,1,INE0EQ001018,1\n"
    "T2TBZ,T2T BZ Company,BZ,01-JAN-2010,1,1,INE0BZ001018,1\n"
)

# Deliberately different wrapper shapes per endpoint — exercises the recursive symbol walker.
PAYLOADS: dict[str, httpx.Response] = {
    sv.NSE_GSM_URL: httpx.Response(200, json={"data": [{"symbol": "GSMSTK", "gsmStage": "I"}]}),
    sv.NSE_ASM_URL: httpx.Response(
        200,
        json={
            "longterm": {"data": [{"symbol": "ASMLONG", "asmSurvIndicator": "LT"}]},
            "shortterm": {"data": [{"symbol": "ASMSHORT"}]},
        },
    ),
    sv.NSE_ESM_URL: httpx.Response(200, json=[{"symbol": "ESMSTK"}]),
    sv.NSE_SMS_URL: httpx.Response(200, json={"data": [{"symbol": "PUMPED", "reason": "sms tips"}]}),
    sv.NSE_T2T_URL: httpx.Response(200, text=T2T_CSV),
}


def make_client(fail_urls: set[str] = frozenset()) -> httpx.AsyncClient:
    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url == NSE_HOMEPAGE:
            return httpx.Response(200)          # nse_get cookie-priming GET (A3) — mints the jar
        if url in fail_urls:
            raise httpx.ConnectError("blocked by anti-bot", request=request)
        return PAYLOADS[url]

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def collect_alerts():
    msgs = []

    async def sink(msg):
        msgs.append(msg)

    return msgs, sink


def make_ingest(tmp_path, clock, *, fail_urls=frozenset(), notify=None) -> SurveillanceIngest:
    return SurveillanceIngest(
        clock, make_client(fail_urls), tmp_path / "surveillance.json", notify=notify
    )


# --------------------------------------------------------------------------- happy path
async def test_refresh_parses_all_sources(tmp_path, clock):
    ingest = make_ingest(tmp_path, clock)
    lists = await ingest.refresh()

    assert lists.gsm == {"GSMSTK"}
    assert lists.asm == {"ASMLONG", "ASMSHORT"}         # nested long/short shapes both walked
    assert lists.t2t == {"T2TSTK", "T2TBZ"}             # BE + BZ series; EQ row not T2T
    assert lists.esm == {"ESMSTK"}
    assert lists.sms == {"PUMPED"}
    assert lists.degraded_sources == () and lists.unconfirmed_symbols == frozenset()
    assert lists.as_of == D

    # flagged() is the §3.2.4 exclusion set: the four lists, NOT the SMS list.
    assert lists.flagged() == {"GSMSTK", "ASMLONG", "ASMSHORT", "T2TSTK", "T2TBZ", "ESMSTK"}
    assert "PUMPED" not in lists.flagged()
    assert lists.reasons_for("gsmstk") == ["surveillance_gsm"]      # case-insensitive
    assert lists.reasons_for("NORMALEQ") == []

    # Last-good cache written per source (the reuse-yesterday fallback, E5).
    cache = json.loads((tmp_path / "surveillance.json").read_text(encoding="utf-8"))
    assert set(cache) == {"gsm", "asm", "t2t", "esm", "sms"}
    assert cache["gsm"]["symbols"] == ["GSMSTK"]


async def test_current_refreshes_once_then_reuses(tmp_path, clock):
    ingest = make_ingest(tmp_path, clock)
    first = await ingest.current()                      # §2.6: self-refresh on first use
    assert first.gsm == {"GSMSTK"}
    assert await ingest.current() is first              # no second fetch round


# --------------------------------------------------------------------------- failure model (E5)
async def test_failed_source_reuses_yesterday_and_alerts(tmp_path, clock):
    await make_ingest(tmp_path, clock).refresh()        # seed the cache (yesterday's lists)

    msgs, sink = collect_alerts()
    ingest = make_ingest(tmp_path, clock, fail_urls={sv.NSE_ASM_URL}, notify=sink)
    lists = await ingest.refresh()

    assert lists.asm == {"ASMLONG", "ASMSHORT"}         # yesterday reused, never shrunk
    assert lists.degraded_sources == ("asm",)
    assert lists.unconfirmed_symbols == {"ASMLONG", "ASMSHORT"}     # Phase-2 gate seam
    assert lists.gsm == {"GSMSTK"}                      # other sources unaffected
    assert len(msgs) == 1 and msgs[0].severity == "critical"
    assert msgs[0].data["degraded_sources"] == ["asm"]


async def test_failed_source_without_cache_is_empty_and_degraded(tmp_path, clock):
    msgs, sink = collect_alerts()
    ingest = make_ingest(tmp_path, clock, fail_urls={sv.NSE_GSM_URL}, notify=sink)
    lists = await ingest.refresh()

    assert lists.gsm == frozenset()                     # nothing to reuse — but still no raise
    assert lists.degraded_sources == ("gsm",)
    assert msgs


async def test_all_sources_down_never_raises(tmp_path, clock):
    msgs, sink = collect_alerts()
    ingest = make_ingest(tmp_path, clock, fail_urls=set(PAYLOADS), notify=sink)
    lists = await ingest.refresh()                      # must not raise into the scheduler (E5)
    assert set(lists.degraded_sources) == {"gsm", "asm", "t2t", "esm", "sms"}
    assert lists.flagged() == frozenset()
    assert msgs


# --------------------------------------------------------------------------- migration diff (A8 seam)
def test_new_entries_diff_for_held_position_migration():
    yesterday = SurveillanceLists(as_of=D, gsm={"OLDGSM"})
    today = SurveillanceLists(as_of=D, gsm={"OLDGSM"}, asm={"NEWASM"})
    assert today.new_entries(yesterday) == {"NEWASM"}   # Phase 2 intersects with held symbols
    assert yesterday.new_entries(today) == frozenset()
