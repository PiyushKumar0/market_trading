"""SurveillanceIngest (§4.4 job 5, A8/E5): per-source parsing (nested NSE JSON shapes + the T2T
series CSV), per-source reuse-yesterday on failure with ``unconfirmed_symbols`` + alert, the
never-raise guarantee, and the held-position surveillance alert (per-list diff, over-the-gap
wording, never breaking the refresh)."""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import timedelta

import httpx
import pytest

from engine.notify import catalog
from engine.notify.catalog import MessageKind
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
    sv.NSE_T2T_URL: httpx.Response(200, text=T2T_CSV),
}


def make_client(fail_urls: set[str] = frozenset(), payloads: dict | None = None) -> httpx.AsyncClient:
    """``payloads`` overrides PAYLOADS per URL and is read per request, so a test may mutate it."""
    overrides = payloads if payloads is not None else {}

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url == NSE_HOMEPAGE:
            return httpx.Response(200)          # nse_get cookie-priming GET (A3) — mints the jar
        if url in fail_urls:
            raise httpx.ConnectError("blocked by anti-bot", request=request)
        return overrides[url] if url in overrides else PAYLOADS[url]

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def collect_alerts():
    msgs = []

    async def sink(msg):
        msgs.append(msg)

    return msgs, sink


def make_ingest(tmp_path, clock, *, fail_urls=frozenset(), notify=None, payloads=None,
                held=None) -> SurveillanceIngest:
    return SurveillanceIngest(
        clock, make_client(fail_urls, payloads), tmp_path / "surveillance.json", notify=notify,
        held_positions=held,
    )


# --------------------------------------------------------------------------- happy path
async def test_refresh_parses_all_sources(tmp_path, clock):
    ingest = make_ingest(tmp_path, clock)
    lists = await ingest.refresh()

    assert lists.gsm == {"GSMSTK"}
    assert lists.asm == {"ASMLONG", "ASMSHORT"}         # nested long/short shapes both walked
    assert lists.t2t == {"T2TSTK", "T2TBZ"}             # BE + BZ series; EQ row not T2T
    assert lists.esm == {"ESMSTK"}
    assert lists.sms == frozenset()                     # retired source (NSE 404, 2026-07-28) — pinned empty
    assert lists.degraded_sources == () and lists.unconfirmed_symbols == frozenset()
    assert lists.as_of == D

    # flagged() is the §3.2.4 exclusion set: the four lists, NOT the SMS list.
    assert lists.flagged() == {"GSMSTK", "ASMLONG", "ASMSHORT", "T2TSTK", "T2TBZ", "ESMSTK"}
    assert lists.reasons_for("gsmstk") == ["surveillance_gsm"]      # case-insensitive
    assert lists.reasons_for("NORMALEQ") == []

    # Listed-equity master (2026-09-01 batch-universe extended leg): EQ-series rows of the same
    # EQUITY_L pass — companies only, never part of flagged()/reasons_for.
    assert lists.equity_master == {"NORMALEQ"}

    # Last-good cache written per source (the reuse-yesterday fallback, E5) + the master.
    cache = json.loads((tmp_path / "surveillance.json").read_text(encoding="utf-8"))
    assert set(cache) == {"gsm", "asm", "t2t", "esm", "equity_master"}
    assert cache["gsm"]["symbols"] == ["GSMSTK"]
    assert cache["equity_master"]["symbols"] == ["NORMALEQ"]


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


async def test_failed_t2t_reuses_yesterdays_equity_master(tmp_path, clock):
    """The equity master rides the T2T download (one EQUITY_L pass) — a failed fetch reuses the
    cached master exactly like the cached T2T list (2026-09-01 extended leg, fail-closed-not-empty
    only when BOTH source and cache are gone)."""
    await make_ingest(tmp_path, clock).refresh()        # seed the cache

    ingest = make_ingest(tmp_path, clock, fail_urls={sv.NSE_T2T_URL})
    lists = await ingest.refresh()

    assert lists.t2t == {"T2TSTK", "T2TBZ"}             # yesterday reused
    assert lists.equity_master == {"NORMALEQ"}          # master reused from the same cache
    assert "t2t" in lists.degraded_sources


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
    assert set(lists.degraded_sources) == {"gsm", "asm", "t2t", "esm"}
    assert lists.flagged() == frozenset()
    assert msgs


# --------------------------------------------------------------------------- migration diff (A8 seam)
def test_new_entries_diff_for_held_position_migration():
    yesterday = SurveillanceLists(as_of=D, gsm={"OLDGSM"})
    today = SurveillanceLists(as_of=D, gsm={"OLDGSM"}, asm={"NEWASM"})
    assert today.new_entries(yesterday) == {"NEWASM"}
    assert yesterday.new_entries(today) == frozenset()


def test_new_listings_is_per_list_so_a_move_between_lists_counts():
    yesterday = SurveillanceLists(as_of=D, asm={"HELDX"})
    today = SurveillanceLists(as_of=D, asm={"HELDX"}, gsm={"HELDX"})
    assert today.new_entries(yesterday) == frozenset()          # the union diff misses ASM→GSM
    assert today.new_listings(yesterday) == {
        "gsm": {"HELDX"}, "asm": frozenset(), "t2t": frozenset(), "esm": frozenset(),
    }


# --------------------------------------------------------------------------- held-position alert (A8)
HELD = "HELDX"


def _json_list(url: str, *symbols: str) -> dict[str, httpx.Response]:
    return {url: httpx.Response(200, json={"data": [{"symbol": s} for s in symbols]})}


def held_one(symbol: str = "heldx", pid: str = "P1", qty: int = 10):
    """Lower-case on purpose: positions rows are matched case-insensitively against the lists."""
    return lambda: [(pid, symbol, qty)]


def migrations(msgs) -> list:
    return [m for m in msgs if m.kind is MessageKind.SURVEILLANCE_MIGRATION]


def write_cache(tmp_path, as_of, **lists) -> None:
    cache = {key: {"as_of": as_of.isoformat(), "symbols": sorted(lists.get(key, ()))}
             for key in sv.SURVEILLANCE_KEYS}
    (tmp_path / "surveillance.json").write_text(json.dumps(cache), encoding="utf-8")


async def test_held_position_entering_asm_sends_one_critical_alert(tmp_path, clock):
    await make_ingest(tmp_path, clock).refresh()                 # yesterday's cache: no HELDX
    msgs, sink = collect_alerts()
    ingest = make_ingest(tmp_path, clock, notify=sink, held=held_one(),
                         payloads=_json_list(sv.NSE_ASM_URL, "ASMLONG", HELD, "NEWUNHELD"))
    lists = await ingest.refresh()

    assert HELD in lists.asm
    [msg] = migrations(msgs)                                     # NEWUNHELD is not held: no alert
    assert msgs == [msg]
    assert msg.severity == "critical" and msg.render().startswith("🚨 HELDX is now on the NSE ASM list")
    assert msg.dedupe_key == "surveillance_migration:P1:asm"
    assert msg.data == {
        "symbol": HELD, "position_id": "P1", "qty": 10, "list_key": "asm", "as_of": D.isoformat(),
        "previous_as_of": D.isoformat(), "status": "new", "unconfirmed": False,
        "list_as_of": D.isoformat(),
    }
    # Same-day comparison: never "on the D list and was not on the D list".
    assert f"on the ASM list now and was not on it earlier on {D.isoformat()}" in msg.body
    assert "was not on the" not in msg.body
    assert "Additional Surveillance Measure" in msg.body and "does not exit or change" in msg.body


async def test_offline_gap_diff_uses_disk_cache_after_restart(tmp_path, clock):
    gap_start = D - timedelta(days=4)
    write_cache(tmp_path, gap_start, gsm={"GSMSTK"})
    msgs, sink = collect_alerts()
    ingest = make_ingest(tmp_path, clock, notify=sink, held=held_one(),        # fresh process
                         payloads=_json_list(sv.NSE_GSM_URL, "GSMSTK", HELD))
    await ingest.refresh()

    [msg] = migrations(msgs)
    assert (msg.data["list_key"], msg.data["status"]) == ("gsm", "new")
    assert msg.data["previous_as_of"] == gap_start.isoformat()
    assert f"on the {D.isoformat()} list and was not on the {gap_start.isoformat()} list" in msg.body


async def test_in_process_previous_snapshot_is_used_over_the_cache(tmp_path, clock):
    """Another writer (scripts/build_universe.py) may advance the cache; the in-process snapshot is
    the comparison while there is one."""
    msgs, sink = collect_alerts()
    payloads: dict[str, httpx.Response] = {}
    ingest = make_ingest(tmp_path, clock, notify=sink, held=held_one(), payloads=payloads)
    await ingest.refresh()
    assert migrations(msgs) == []                                # not listed anywhere yet

    write_cache(tmp_path, D, esm={"ESMSTK", HELD})               # the other writer saw it first
    payloads |= _json_list(sv.NSE_ESM_URL, "ESMSTK", HELD)
    await ingest.refresh()

    [msg] = migrations(msgs)
    assert (msg.data["list_key"], msg.data["status"]) == ("esm", "new")
    assert msg.data["previous_as_of"] == D.isoformat()


async def test_asm_to_gsm_move_alerts_for_gsm_as_new(tmp_path, clock):
    write_cache(tmp_path, D - timedelta(days=1), asm={HELD})
    msgs, sink = collect_alerts()
    ingest = make_ingest(tmp_path, clock, notify=sink, held=held_one(), payloads={
        **_json_list(sv.NSE_ASM_URL, HELD), **_json_list(sv.NSE_GSM_URL, HELD)})
    await ingest.refresh()

    by_list = {m.data["list_key"]: m for m in migrations(msgs)}
    assert set(by_list) == {"gsm", "asm"}
    assert by_list["gsm"].data["status"] == "new"
    assert by_list["gsm"].dedupe_key == "surveillance_migration:P1:gsm"
    assert by_list["asm"].data["status"] == "listed"             # its own key; the journal drops a resend


async def test_already_listed_held_symbol_uses_listed_wording_and_same_key(tmp_path, clock):
    yesterday = D - timedelta(days=1)
    write_cache(tmp_path, yesterday, asm={HELD})
    msgs, sink = collect_alerts()
    ingest = make_ingest(tmp_path, clock, notify=sink, held=held_one(),
                         payloads=_json_list(sv.NSE_ASM_URL, HELD))
    await ingest.refresh()

    [msg] = migrations(msgs)
    assert msg.data["status"] == "listed" and msg.dedupe_key == "surveillance_migration:P1:asm"
    assert msg.title == "HELDX is on the NSE ASM list (you hold it)"
    assert f"it was already listed on {yesterday.isoformat()}" in msg.body


async def test_no_previous_snapshot_reports_unknown(tmp_path, clock):
    t2t = "SYMBOL,NAME OF COMPANY, SERIES\nHELDX,Held Co,BE\nNORMALEQ,Normal,EQ\n"
    msgs, sink = collect_alerts()
    ingest = make_ingest(tmp_path, clock, notify=sink, held=held_one(),
                         payloads={sv.NSE_T2T_URL: httpx.Response(200, text=t2t)})
    await ingest.refresh()                                       # no cache file, no in-process snapshot

    [msg] = migrations(msgs)
    assert (msg.data["list_key"], msg.data["status"], msg.data["previous_as_of"]) == ("t2t", "unknown", None)
    assert "no earlier list to compare against" in msg.body and "trade-to-trade" in msg.body


async def test_in_process_failed_empty_source_is_unknown_not_known_empty(tmp_path, clock):
    msgs, sink = collect_alerts()
    payloads: dict[str, httpx.Response] = {}
    fail = {sv.NSE_GSM_URL}
    ingest = SurveillanceIngest(clock, make_client(fail, payloads), tmp_path / "surveillance.json",
                                notify=sink, held_positions=held_one())
    await ingest.refresh()                                       # GSM failed, nothing cached: empty
    fail.clear()
    payloads |= _json_list(sv.NSE_GSM_URL, HELD)
    await ingest.refresh()

    [msg] = migrations(msgs)
    assert (msg.data["list_key"], msg.data["status"]) == ("gsm", "unknown")


async def test_degraded_source_marks_unconfirmed_and_keeps_the_degraded_alert(tmp_path, clock):
    write_cache(tmp_path, D - timedelta(days=1), asm={HELD})
    msgs, sink = collect_alerts()
    ingest = make_ingest(tmp_path, clock, notify=sink, held=held_one(), fail_urls={sv.NSE_ASM_URL})
    lists = await ingest.refresh()

    assert lists.degraded_sources == ("asm",)
    [msg] = migrations(msgs)
    # The cached copy is both the list in use and the comparison: a list against itself is unknown.
    assert msg.data["status"] == "unknown" and msg.data["unconfirmed"] is True
    assert "no earlier list to compare against" in msg.body and "already listed" not in msg.body
    assert "Today's ASM download failed" in msg.body and "unconfirmed" in msg.body
    # The first clause dates the list actually used (the cached copy), never today.
    yesterday = (D - timedelta(days=1)).isoformat()
    assert f"is on the last good ASM list ({yesterday})" in msg.body
    assert D.isoformat() not in msg.body and msg.data["list_as_of"] == yesterday
    assert [m.kind for m in msgs if m is not msg] == [MessageKind.DATA_FRESHNESS_FROZEN]


async def test_same_day_re_refresh_new_listing_words_it_as_earlier_today(tmp_path, clock):
    msgs, sink = collect_alerts()
    payloads: dict[str, httpx.Response] = {}
    ingest = make_ingest(tmp_path, clock, notify=sink, held=held_one(), payloads=payloads)
    await ingest.refresh()
    payloads |= _json_list(sv.NSE_ESM_URL, HELD)
    await ingest.refresh()                                       # same day: previous_as_of == as_of

    [msg] = migrations(msgs)
    assert msg.data["status"] == "new" and msg.data["previous_as_of"] == D.isoformat()
    assert f"on the ESM list now and was not on it earlier on {D.isoformat()}" in msg.body


def _day(n: int):
    return D.replace(year=2026, month=10, day=n)


def _ingest_on_moving_clock(tmp_path, now: list, *, notify, fail: set, payloads: dict):
    from engine.core.clock import Clock

    clock = Clock(time_source=lambda: FIXED_NOW.replace(year=2026, month=10, day=now[0]))
    return SurveillanceIngest(clock, make_client(fail, payloads), tmp_path / "surveillance.json",
                              notify=notify, held_positions=held_one())


async def test_failed_gsm_two_days_running_never_names_a_list_that_was_not_fetched(tmp_path):
    """Cache holds the 10-12 GSM list with HELDX on it; GSM then fails on 10-13 and 10-14. Only the
    10-12 list was ever downloaded: no alert may name 10-13 / 10-14 as a list date, and comparing
    that list with itself is 'unknown', not 'listed'."""
    write_cache(tmp_path, _day(12), gsm={HELD})
    msgs, sink = collect_alerts()
    now = [13]
    ingest = _ingest_on_moving_clock(tmp_path, now, notify=sink, fail={sv.NSE_GSM_URL}, payloads={})
    await ingest.refresh()
    now[0] = 14
    await ingest.refresh()

    gsm = [m for m in migrations(msgs) if m.data["list_key"] == "gsm"]
    assert len(gsm) == 2                                         # the alert itself is kept each refresh
    for msg in gsm:
        assert msg.data["status"] == "unknown" and msg.data["previous_as_of"] is None
        assert msg.data["unconfirmed"] is True and msg.data["list_as_of"] == _day(12).isoformat()
        assert "last good GSM list (2026-10-12)" in msg.body
        assert "2026-10-13" not in msg.body and "2026-10-14" not in msg.body
        assert "already listed" not in msg.body


async def test_gsm_failed_then_fetched_dates_the_comparison_by_the_list_actually_used(tmp_path):
    """GSM fails 10-13 (reusing the 10-12 copy) and succeeds 10-14 with a new held symbol: the
    comparison is against the 10-12 list, never a '10-13 list' that was not downloaded."""
    write_cache(tmp_path, _day(12), gsm={"GSMSTK"})
    msgs, sink = collect_alerts()
    now = [13]
    fail = {sv.NSE_GSM_URL}
    payloads: dict[str, httpx.Response] = {}
    ingest = _ingest_on_moving_clock(tmp_path, now, notify=sink, fail=fail, payloads=payloads)
    await ingest.refresh()
    assert migrations(msgs) == []
    now[0] = 14
    fail.clear()
    payloads |= _json_list(sv.NSE_GSM_URL, "GSMSTK", HELD)
    await ingest.refresh()

    [msg] = migrations(msgs)
    assert (msg.data["list_key"], msg.data["status"]) == ("gsm", "new")
    assert msg.data["previous_as_of"] == "2026-10-12" and msg.data["unconfirmed"] is False
    assert "on the 2026-10-14 list and was not on the 2026-10-12 list" in msg.body
    assert "2026-10-13" not in msg.body


def test_catalog_unconfirmed_without_a_cache_date_says_so():
    msg = catalog.surveillance_migration(
        symbol=HELD, position_id="P1", qty=10, list_key="asm", as_of=D.isoformat(),
        previous_as_of=None, status="unknown", unconfirmed=True)
    assert "last good ASM list (date unknown)" in msg.body and D.isoformat() not in msg.body


async def test_no_held_fn_sends_no_migration_alert(tmp_path, clock):
    """The scripts/build_universe.py shape: no held_positions, so nothing new is sent."""
    msgs, sink = collect_alerts()
    lists = await make_ingest(tmp_path, clock, notify=sink,
                              payloads=_json_list(sv.NSE_ASM_URL, HELD)).refresh()
    assert HELD in lists.asm and msgs == []


async def test_held_fn_raising_never_breaks_refresh(tmp_path, clock, caplog):
    def broken():
        raise sqlite3.OperationalError("database is locked")

    msgs, sink = collect_alerts()
    ingest = make_ingest(tmp_path, clock, notify=sink, held=broken, fail_urls={sv.NSE_ESM_URL})
    with caplog.at_level(logging.WARNING, logger="engine.universe.surveillance"):
        lists = await ingest.refresh()

    assert await ingest.current() is lists and lists.gsm == {"GSMSTK"}
    assert [m.kind for m in msgs] == [MessageKind.DATA_FRESHNESS_FROZEN]    # degraded alert unaffected
    assert [r for r in caplog.records if r.getMessage() == "surveillance_migration_check_failed"]


async def test_notify_raising_is_swallowed_and_other_holdings_still_alert(tmp_path, clock):
    sent = []

    async def sink(msg):
        if msg.data.get("position_id") == "P1":
            raise ConnectionError("telegram down")
        sent.append(msg)

    held = lambda: [("P1", HELD, 10), ("P2", "GSMSTK", 5)]  # noqa: E731
    ingest = make_ingest(tmp_path, clock, notify=sink, held=held,
                         payloads=_json_list(sv.NSE_ASM_URL, HELD))
    lists = await ingest.refresh()

    assert HELD in lists.asm
    assert [(m.data["position_id"], m.data["list_key"]) for m in sent] == [("P2", "gsm")]


async def test_unheld_and_unlisted_held_send_nothing(tmp_path, clock):
    msgs, sink = collect_alerts()
    await make_ingest(tmp_path, clock, notify=sink, held=held_one("NORMALEQ"),
                      payloads=_json_list(sv.NSE_ASM_URL, "NEWASM")).refresh()
    assert msgs == []
    await make_ingest(tmp_path, clock, notify=sink, held=lambda: []).refresh()
    assert msgs == []
