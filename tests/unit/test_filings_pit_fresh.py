"""§2.8 stage-3 BSE FRESH insider feed (``filings_pit_fresh``): defensive parse over the probe-verified
``getCorp_Regulation_ng/w`` shape, the equity filter (startswith 'equity'), scrip->symbol resolution +
skip counting, txn_type mapping (incl. Revoke / Pledge pass-through), source-tagged (``bse:``) content-
hash ids, the ~25-row-cap per-day subdivision, store round-trip + idempotency, the 2026-09-12
per-run scrip-map rebuild (stored codes union the index-CSV x BSE-bulk-master join) with its
per-stage funnel counts, and the E5 degrade-never-raise contract.

Fixtures (``fixtures/filings_pit_fresh.json``) are lifted VERBATIM from the 2026-07-19 BSE probe
captures (``bse_insider_out/01_default_isdefault1.json`` + ``14_reliance_2023_2024.json``). The live
``data/market.duckdb`` is never touched — every store is a tmp DuckDB.
"""

from __future__ import annotations

import csv
import json
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from engine.core.clock import IST, Clock
from engine.core.config import load_settings
from engine.datafeeds import filings_pit_fresh as fresh
from engine.datafeeds.filings_pit_fresh import (
    BSE_ID_PREFIX,
    FilingsPitFreshJob,
    fresh_url,
    parse_pit_fresh,
)
from engine.datafeeds.isin_map import BSE_SCRIP_MASTER_URL
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


#: BSE bulk scrip master shape (probe-verified 2026-09-12): a BARE LIST, SCRIP_CD + ISIN_NUMBER.
#: NINtec's 539843 is the row that turns the fixture's unmapped-scrip skip into a resolved AIAENG row.
MASTER_JSON = [
    {"SCRIP_CD": "539843", "Scrip_Name": "AIA Engineering Ltd", "ISIN_NUMBER": "INE212H01026"},
    {"SCRIP_CD": "500325", "Scrip_Name": "Reliance Industries Ltd", "ISIN_NUMBER": "INE002A01018"},
]

#: symbol -> ISIN half of the map (the cached index-constituents CSV in production). ONLYNSE has no
#: BSE listing at all — the expected residue (BSE/CDSL in the live universe), not a failure.
CONSTITUENTS_ISIN = {
    "AIAENG": "INE212H01026", "RELIANCE": "INE002A01018", "ONLYNSE": "INE999Z01010",
}


@pytest.fixture(autouse=True)
def _isolate_constituents(monkeypatch):
    """Default the symbol->ISIN half to EMPTY so no test reads the repo's live universe cache.

    Empty ⇒ the job skips the master request entirely and resolves off the stored ``symbol_isin``
    map alone (the pre-2026-09-12 behaviour), which is exactly the baseline the older tests assert.
    Tests that exercise the rebuilt map override this.
    """
    monkeypatch.setattr("engine.datafeeds.filings_pit_fresh.load_constituents_isin", lambda _s: {})


@pytest.fixture
def settings():
    return load_settings()


def serving_json(payload) -> httpx.AsyncClient:
    """A client that returns a FRESH Response(200, json=payload) for every request (the fresh job
    fetches the Isdefault=1 + Isdefault=2 surfaces, so each call needs its own response)."""
    return httpx.AsyncClient(transport=httpx.MockTransport(lambda _req: httpx.Response(200, json=payload)))


def routed_client(master, insider=FRESH_JSON, recorder: list | None = None):
    """Route the scrip-master URL to ``master()`` and every insider surface to ``insider``.

    ``master`` is a CALLABLE (a fresh Response per call): the job fetches the master once per run and
    a test may run twice, and a Response instance cannot be replayed once its stream is read."""

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if recorder is not None:
            recorder.append(url)
        if url.startswith(BSE_SCRIP_MASTER_URL.split("?", 1)[0]):
            return master()
        return httpx.Response(200, json=insider)

    return httpx.AsyncClient(transport=httpx.MockTransport(handler), follow_redirects=True)


def failing_client() -> httpx.AsyncClient:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("unreachable", request=request)

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def collect_alerts():
    msgs = []

    async def sink(msg):
        msgs.append(msg)

    return msgs, sink


def stepping_clock():
    """A Clock the test can advance — the per-day master memo and its failure cooldown are the only
    behaviour in this job that depends on the passage of time."""
    cur = [FIXED_NOW]

    def step(days: int = 0, minutes: int = 0):
        cur[0] = cur[0] + timedelta(days=days, minutes=minutes)

    return Clock(time_source=lambda: cur[0]), step


def _record(caplog, event: str):
    """The single LogRecord for ``event`` (structured fields ride as record attributes, R8)."""
    hits = [r for r in caplog.records if r.getMessage() == event]
    assert len(hits) == 1, [r.getMessage() for r in caplog.records]
    return hits[0]


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
async def test_job_persists_deduped_and_is_idempotent(seeded_store, clock, settings):
    job = FilingsPitFreshJob(seeded_store, clock, serving_json(FRESH_JSON), settings=settings)
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


async def test_windowed_cap_triggers_per_day_subdivision(seeded_store, clock, settings):
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
    result = await FilingsPitFreshJob(seeded_store, clock, client, settings=settings).run(D)
    assert result.windows_subdivided == 1
    # D = 2026-06-17, window [d-3, d] -> the four days 14..17 refetched individually.
    assert per_day == ["20260614", "20260615", "20260616", "20260617"]
    assert result.ok is True


# =========================================================================== E5 degrade
async def test_both_surfaces_fail_degrades_and_warns(seeded_store, clock, settings):
    msgs, sink = collect_alerts()
    result = await FilingsPitFreshJob(
        seeded_store, clock, failing_client(), settings=settings, notify=sink
    ).run(D)
    assert result.ok is False and result.degraded is True         # never raises (E5)
    assert result.failed_sources == ("isdefault1", "isdefault2")
    assert result.rows_written == 0
    assert msgs and msgs[0].data["job_id"] == "filings_pit_fresh"
    assert msgs[0].severity == "warning"                          # filings are NOT safety-critical
    assert fresh.BSE_FRESH_URL.startswith("https://api.bseindia.com/")


# =================================================== per-run scrip-map rebuild (2026-09-12 §2.8)
async def test_scrip_map_rebuild_resolves_rows_the_stored_map_alone_drops(
    seeded_store, clock, settings, monkeypatch
):
    """The 09-12 starvation fix: index-CSV ISINs joined to the BSE bulk master recover an issuer the
    one-shot ``symbol_isin`` backfill never mapped (fixture scrip 539843 -> AIAENG)."""
    monkeypatch.setattr(
        "engine.datafeeds.filings_pit_fresh.load_constituents_isin", lambda _s: CONSTITUENTS_ISIN
    )
    client = routed_client(lambda: httpx.Response(200, json=MASTER_JSON))
    result = await FilingsPitFreshJob(seeded_store, clock, client, settings=settings).run(D)

    assert result.scrip_map_degraded is False
    assert result.scrip_codes == len(SCRIP_MAP) + 1          # +539843, the master's only new code
    assert result.scrip_symbols_unresolved == 1              # ONLYNSE: no BSE listing, not a failure
    assert result.skipped_unmapped_scrip == 0                # the row the stored map used to drop
    symbols = {r["symbol"] for r in seeded_store.get_insider_trades()}
    assert "AIAENG" in symbols and len(symbols) == 5


async def test_stored_code_wins_over_a_disagreeing_master(seeded_store, clock, settings, monkeypatch):
    """Precedence is stored-wins on a scrip key: the stored codes are the per-symbol PeerSmartSearch
    resolutions, so a master that claims 500325 for someone else must not silently re-point it."""
    monkeypatch.setattr(
        "engine.datafeeds.filings_pit_fresh.load_constituents_isin",
        lambda _s: {"IMPOSTOR": "INE002A01018"},                       # same ISIN -> RELIANCE's code
    )
    client = routed_client(lambda: httpx.Response(200, json=MASTER_JSON))
    await FilingsPitFreshJob(seeded_store, clock, client, settings=settings).run(D)
    symbols = {r["symbol"] for r in seeded_store.get_insider_trades()}
    assert "IMPOSTOR" not in symbols and "RELIANCE" in symbols


async def test_scrip_master_failure_degrades_to_stored_map_and_alerts_once(
    seeded_store, clock, settings, monkeypatch, caplog
):
    """A failed master leg silently reverts the feed to the 199-code coverage that starved §6.1 `ins`
    for two months, and §6.1's own ``ins_feed_coverage_low`` measures the 120-day CORPUS, so it
    cannot see it. The map stage therefore owns the detection: it warns, and alerts ONCE on the
    transition (a replay must not storm). Rows keep flowing, so ``ok`` stays true."""
    monkeypatch.setattr(
        "engine.datafeeds.filings_pit_fresh.load_constituents_isin", lambda _s: CONSTITUENTS_ISIN
    )
    msgs, sink = collect_alerts()
    job = FilingsPitFreshJob(
        seeded_store, clock,
        routed_client(lambda: httpx.Response(200, text="<html>error_Bse</html>")),
        settings=settings, notify=sink,
    )
    with caplog.at_level("INFO"):
        result = await job.run(D)

    assert result.scrip_map_degraded is True
    assert result.ok is True and result.degraded is False    # not a watermark failure, not a retry
    assert result.scrip_codes == len(SCRIP_MAP)              # exactly the stored map, nothing lost
    assert result.scrip_master_rows == 0
    assert result.skipped_unmapped_scrip == 2                # 539843 dropped again, as before
    assert _record(caplog, "filings_pit_fresh_scrip_map_degraded").stored == len(SCRIP_MAP)
    assert len(msgs) == 1 and msgs[0].severity == "warning"
    assert msgs[0].data["job_id"] == "filings_pit_fresh"

    # The latch: a second degraded run through the SAME instance (a catch-up replaying another day)
    # re-warns but does NOT re-alert.
    caplog.clear()
    with caplog.at_level("INFO"):
        await job.run(D - timedelta(days=1))
    assert "filings_pit_fresh_scrip_map_degraded" in caplog.text
    assert len(msgs) == 1


async def test_a_healthy_map_re_arms_the_degradation_alert(seeded_store, settings, monkeypatch):
    """Symmetric clear (the 2026-09-01 latch lesson): a recovery must re-arm the alarm, or the next
    collapse is silent for the life of the process. Each run is a fresh IST day, which is also what
    expires the per-day master memo."""
    monkeypatch.setattr(
        "engine.datafeeds.filings_pit_fresh.load_constituents_isin", lambda _s: CONSTITUENTS_ISIN
    )
    msgs, sink = collect_alerts()
    responses = iter([
        lambda: httpx.Response(200, text="<html>error_Bse</html>"),   # degraded -> alert 1
        lambda: httpx.Response(200, json=MASTER_JSON),                # healthy  -> clears the latch
        lambda: httpx.Response(200, text="<html>error_Bse</html>"),   # degraded -> alert 2
    ])
    clock, step = stepping_clock()
    job = FilingsPitFreshJob(
        seeded_store, clock, routed_client(lambda: next(responses)()), settings=settings, notify=sink
    )
    degraded = []
    for _ in range(3):
        degraded.append((await job.run(clock.today())).scrip_map_degraded)
        step(days=1)
    assert degraded == [True, False, True]
    assert len(msgs) == 2


async def test_master_shape_change_is_degraded_not_a_silent_revert(
    seeded_store, clock, settings, monkeypatch
):
    """BSE renames SCRIP_CD/ISIN_NUMBER: a 200 that parses to nothing. The old code called that a
    success (``degraded=False``) and reverted to the stored codes with the run line certifying a
    healthy map — the exact regression this work order exists to make visible."""
    monkeypatch.setattr(
        "engine.datafeeds.filings_pit_fresh.load_constituents_isin", lambda _s: CONSTITUENTS_ISIN
    )
    msgs, sink = collect_alerts()
    renamed = [{"ScripCd": "539843", "Isin": "INE212H01026"}]
    result = await FilingsPitFreshJob(
        seeded_store, clock, routed_client(lambda: httpx.Response(200, json=renamed)),
        settings=settings, notify=sink,
    ).run(D)

    assert result.scrip_master_rows == 0 and result.scrip_map_degraded is True
    assert result.scrip_codes == len(SCRIP_MAP)
    # The alert must name the leg that failed: three legs, three hosts, three different fixes.
    assert len(msgs) == 1 and msgs[0].data["cause"] == "bulk_master_empty"
    assert "renamed" in msgs[0].body


async def test_stale_stored_symbol_shadows_the_master_and_is_counted(
    store, clock, settings, monkeypatch, caplog
):
    """``symbol_isin`` has no refresh job, so after a tradingsymbol rename (MINDTREE->LTIM class) the
    stale stored row keeps shadowing the right symbol. Stored still wins — but the loser is counted
    and warned, or the run line would certify full resolution while that issuer starves forever."""
    monkeypatch.setattr(
        "engine.datafeeds.filings_pit_fresh.load_constituents_isin",
        lambda _s: {"RELIANCE": "INE002A01018"},          # the master codes it to the stored 500325
    )
    store.upsert_symbol_isin(
        [{"symbol": "OLDNAME", "isin": "INE002A01018", "bse_scrip_code": "500325", "as_of": D}]
    )
    with caplog.at_level("INFO"):
        result = await FilingsPitFreshJob(
            store, clock, routed_client(lambda: httpx.Response(200, json=MASTER_JSON)),
            settings=settings,
        ).run(D)

    assert result.scrip_shadowed == 1
    assert result.scrip_symbols_unresolved == 0            # it resolved — to the WRONG symbol
    assert result.scrip_map_degraded is False              # the leg worked; the STORED row is stale
    assert _record(caplog, "filings_pit_fresh_scrip_codes_shadowed").shadowed == 1
    symbols = {r["symbol"] for r in store.get_insider_trades()}
    assert symbols == {"OLDNAME"} and "RELIANCE" not in symbols


async def test_an_empty_master_body_is_not_memoised_for_the_day(
    seeded_store, settings, monkeypatch
):
    """2026-09-12 review: a 200 that parses to nothing is a transient BSE shape, not a fact about the
    day. Memoising it pinned the feed at the pre-fix coverage through the 19:00 run that followed a
    healthy BSE. It takes the failure cooldown instead: inside it the next run does not re-fetch
    (and reads as a cooling-down master), after it the healthy master is fetched and used."""
    monkeypatch.setattr(
        "engine.datafeeds.filings_pit_fresh.load_constituents_isin", lambda _s: CONSTITUENTS_ISIN
    )
    responses = iter([
        lambda: httpx.Response(200, json=[]),               # 200, parses to nothing
        lambda: httpx.Response(200, json=MASTER_JSON),      # BSE healthy again
    ])
    seen: list[str] = []
    clock, step = stepping_clock()
    job = FilingsPitFreshJob(
        seeded_store, clock, routed_client(lambda: next(responses)(), recorder=seen), settings=settings
    )
    master_root = BSE_SCRIP_MASTER_URL.split("?", 1)[0]

    first = await job.run(clock.today())
    assert first.scrip_map_degraded is True and first.scrip_master_rows == 0

    step(minutes=5)                                          # inside the cooldown: no re-fetch
    second = await job.run(clock.today())
    assert second.scrip_map_degraded is True
    assert sum(1 for url in seen if url.startswith(master_root)) == 1

    step(minutes=int(fresh.SCRIP_MASTER_RETRY_COOLDOWN_S // 60) + 1)   # same IST day, past it
    third = await job.run(clock.today())
    assert third.scrip_map_degraded is False and third.scrip_master_rows == len(MASTER_JSON)
    assert sum(1 for url in seen if url.startswith(master_root)) == 2


async def test_a_truncated_master_trips_the_coverage_floor(
    seeded_store, clock, settings, monkeypatch, caplog
):
    """2026-09-12 review: `not master` is a presence bit. A master BSE truncates still parses to
    hundreds of pairs and would have certified a map covering a tenth of the universe as healthy.
    The detector is a floor on constituents that ended up with their own code."""
    constituents = {f"SYM{i:02d}": f"INE_T{i:02d}" for i in range(10)}
    truncated = [
        {"SCRIP_CD": "600001", "Scrip_Name": "Sym 01", "ISIN_NUMBER": "INE_T01"},
        {"SCRIP_CD": "600002", "Scrip_Name": "Sym 02", "ISIN_NUMBER": "INE_T02"},
    ]
    monkeypatch.setattr(
        "engine.datafeeds.filings_pit_fresh.load_constituents_isin", lambda _s: constituents
    )
    msgs, sink = collect_alerts()
    with caplog.at_level("INFO"):
        result = await FilingsPitFreshJob(
            seeded_store, clock, routed_client(lambda: httpx.Response(200, json=truncated)),
            settings=settings, notify=sink,
        ).run(D)

    assert result.scrip_master_rows == 2 and result.scrip_added_codes == 2
    assert result.scrip_covered == 2                          # 8 of 10 missing > max(1, 5) allowed
    assert result.scrip_map_degraded is True
    assert _record(caplog, "filings_pit_fresh_scrip_map_degraded").cause == "map_coverage_low"
    assert len(msgs) == 1 and msgs[0].data["cause"] == "map_coverage_low"
    assert "2 of 10" in msgs[0].body and "90%" in msgs[0].body and "5 unresolved" in msgs[0].body


async def test_two_constituents_on_one_isin_are_not_counted_as_shadowed(
    seeded_store, clock, settings, monkeypatch, caplog
):
    """`shadowed` means a STORED row shadows the master's symbol. Two constituents sharing an ISIN
    (a dual listing, a rename window) collide inside the same loop and are a CSV fact — counted
    apart, so the shadowed alarm keeps its one meaning."""
    monkeypatch.setattr(
        "engine.datafeeds.filings_pit_fresh.load_constituents_isin",
        lambda _s: {"AIAENG": "INE212H01026", "AIAENGDUP": "INE212H01026"},   # not a stored code
    )
    with caplog.at_level("INFO"):
        result = await FilingsPitFreshJob(
            seeded_store, clock, routed_client(lambda: httpx.Response(200, json=MASTER_JSON)),
            settings=settings,
        ).run(D)
    assert result.scrip_shadowed == 0
    assert _record(caplog, "filings_pit_fresh_duplicate_isin").duplicate_isin == 1
    assert not [r for r in caplog.records if r.getMessage() == "filings_pit_fresh_scrip_codes_shadowed"]


def test_non_finite_considerations_read_as_no_value():
    """A bare ``NaN`` token survives json.loads and constructs a Decimal; every ordering comparison
    on it raises — outside the E5 guard, in the ins-eligible tally. Non-finite is 'no value'."""
    assert fresh._dec("NaN") is None
    assert fresh._dec(float("nan")) is None
    assert fresh._dec("Infinity") is None
    assert fresh._dec("1,23,456.50") == Decimal("123456.50")


async def test_store_read_failure_degrades_instead_of_propagating(
    seeded_store, clock, settings, monkeypatch, caplog
):
    """E5: the ``symbol_isin`` read is inside the guard too. A locked duckdb file must not turn the
    run into a bare ``scheduled_job_failed`` with no funnel line at all."""

    async def boom():
        raise RuntimeError("duckdb: could not set lock on file")

    monkeypatch.setattr(seeded_store, "abse_scrip_symbol_map", boom)
    msgs, sink = collect_alerts()
    with caplog.at_level("INFO"):
        result = await FilingsPitFreshJob(
            seeded_store, clock, serving_json(FRESH_JSON), settings=settings, notify=sink
        ).run(D)
    assert result.scrip_map_degraded is True and result.ok is True
    assert result.scrip_codes == 0 and result.rows_written == 0   # nothing resolves without a map
    # A store failure must NOT be reported as a BSE problem (symbols==0 is produced by both).
    assert _record(caplog, "filings_pit_fresh_scrip_map_degraded").cause == "symbol_isin_read"
    assert msgs[0].data["cause"] == "symbol_isin_read"


async def test_master_is_fetched_once_per_day_across_replayed_runs(
    seeded_store, clock, settings, monkeypatch
):
    """DATE_KEYED catch-up replays one run per missed day through ONE job instance. The master is a
    today-view of BSE's active list — identical on every replayed date — so a multi-day replay must
    cost one 1.7 MB fetch, not one per day, and must not burst an unpaced host."""
    monkeypatch.setattr(
        "engine.datafeeds.filings_pit_fresh.load_constituents_isin", lambda _s: CONSTITUENTS_ISIN
    )
    seen: list[str] = []
    job = FilingsPitFreshJob(
        seeded_store, clock,
        routed_client(lambda: httpx.Response(200, json=MASTER_JSON), recorder=seen),
        settings=settings,
    )
    for day in (D - timedelta(days=2), D - timedelta(days=1), D):
        result = await job.run(day)
        assert result.scrip_map_degraded is False            # every replayed day gets the full map
    master_root = BSE_SCRIP_MASTER_URL.split("?", 1)[0]
    assert sum(1 for url in seen if url.startswith(master_root)) == 1


async def test_failed_master_is_not_refetched_inside_the_cooldown(
    seeded_store, clock, settings, monkeypatch
):
    """The failure path is the one that must not burst: a BSE outage during a boot catch-up would
    otherwise meet every replayed day with its own 1.7 MB attempt, back-to-back and unspaced — and
    an unspaced burst is what makes BSE serve ``error_Bse.html`` in the first place."""
    monkeypatch.setattr(
        "engine.datafeeds.filings_pit_fresh.load_constituents_isin", lambda _s: CONSTITUENTS_ISIN
    )
    seen: list[str] = []
    job = FilingsPitFreshJob(
        seeded_store, clock,
        routed_client(lambda: httpx.Response(200, text="<html>error_Bse</html>"), recorder=seen),
        settings=settings,
    )
    for day in (D - timedelta(days=1), D):
        assert (await job.run(day)).scrip_map_degraded is True
    master_root = BSE_SCRIP_MASTER_URL.split("?", 1)[0]
    assert sum(1 for url in seen if url.startswith(master_root)) == 1


async def test_constituents_read_that_raises_degrades_instead_of_propagating(
    seeded_store, clock, settings, monkeypatch
):
    """E5: a corrupt universe cache raises shapes ``load_constituents_isin`` does not catch — the
    feed must still ingest off the stored map rather than take the exception into the scheduler."""

    def boom(_s):
        raise csv.Error("field larger than field limit")

    monkeypatch.setattr("engine.datafeeds.filings_pit_fresh.load_constituents_isin", boom)
    result = await FilingsPitFreshJob(
        seeded_store, clock, serving_json(FRESH_JSON), settings=settings
    ).run(D)
    assert result.scrip_map_degraded is True and result.ok is True
    assert result.rows_written == 5


async def test_master_is_fetched_once_per_run_not_once_per_surface(
    seeded_store, clock, settings, monkeypatch
):
    monkeypatch.setattr(
        "engine.datafeeds.filings_pit_fresh.load_constituents_isin", lambda _s: CONSTITUENTS_ISIN
    )
    seen: list[str] = []
    client = routed_client(lambda: httpx.Response(200, json=MASTER_JSON), recorder=seen)
    await FilingsPitFreshJob(seeded_store, clock, client, settings=settings).run(D)
    master_root = BSE_SCRIP_MASTER_URL.split("?", 1)[0]
    assert sum(1 for url in seen if url.startswith(master_root)) == 1


async def test_empty_constituents_skips_the_master_request(seeded_store, clock, settings, caplog):
    """No symbol->ISIN half ⇒ nothing to join ⇒ the 1.7 MB master request is not spent at all."""
    seen: list[str] = []
    client = routed_client(lambda: httpx.Response(200, json=MASTER_JSON), recorder=seen)
    with caplog.at_level("INFO"):
        result = await FilingsPitFreshJob(seeded_store, clock, client, settings=settings).run(D)
    assert not any(url.startswith(BSE_SCRIP_MASTER_URL.split("?", 1)[0]) for url in seen)
    assert result.scrip_map_degraded is True and result.scrip_codes == len(SCRIP_MAP)
    assert _record(caplog, "filings_pit_fresh_scrip_map_degraded").cause == "constituents_csv"


# =========================================================================== per-stage funnel counts
async def test_stage_counts_close_and_count_ins_eligible_buys(seeded_store, clock, settings):
    """The funnel has to close arithmetically or it cannot tell starvation from a quiet day:
    fetched = non_equity + unmapped + no_broadcast + resolved (per-fetch sums over both surfaces),
    with parsed/written after the id dedupe."""
    result = await FilingsPitFreshJob(
        seeded_store, clock, serving_json(FRESH_JSON), settings=settings
    ).run(D)

    assert result.rows_fetched == 16                          # the same 8-row payload on 2 surfaces
    assert result.rows_fetched == (
        result.skipped_non_equity + result.skipped_unmapped_scrip
        + result.skipped_no_broadcast + result.rows_resolved
    )
    assert result.rows_resolved == 10 and result.rows_parsed == 5 and result.rows_written == 5
    # Of the 5 deduped rows, GOLDLINE (Market Purchase) and RELIANCE (Off Market) are the open-market
    # BUYs — the Revoke, the ESOP Sell and the Pledge are not what §6.1 `ins` aggregates. But the
    # counter must match what the RULE can act on, and insider_cluster_events drops a filing with no
    # positive value AFTER the predicate passes: RELIANCE's buy is zero-consideration, so the rule
    # sees one. A counter that said 2 would overstate on exactly the shape that starves it silently.
    assert result.rows_ins_eligible == 1
    zero_value_buys = [
        r for r in seeded_store.get_insider_trades()
        if r["symbol"] == "RELIANCE" and r["txn_type"] == "Buy"
    ]
    assert [r["value"] for r in zero_value_buys] == [Decimal("0")]   # the row deliberately not counted


async def test_run_line_carries_the_whole_funnel(seeded_store, clock, settings, caplog):
    """The work order's measurability deliverable is the LOG LINE, not the result model: a rename or
    a dropped kwarg on ``filings_pit_fresh_ingested`` would leave every result-field assertion green
    while the next starvation became invisible on the one line built to expose it."""
    with caplog.at_level("INFO"):
        result = await FilingsPitFreshJob(
            seeded_store, clock, serving_json(FRESH_JSON), settings=settings
        ).run(D)

    line = _record(caplog, "filings_pit_fresh_ingested")
    got = {
        key: getattr(line, key) for key in (
            "d", "fetched", "parsed", "written", "subdivided", "skipped_non_equity",
            "skipped_unmapped", "skipped_no_broadcast", "resolved", "ins_eligible", "scrip_codes",
            "scrip_stored", "scrip_master", "scrip_added", "scrip_symbols", "scrip_unresolved",
            "scrip_shadowed", "scrip_map_degraded", "failed",
        )
    }
    assert got["d"] == D.isoformat()
    assert got["fetched"] == (
        got["skipped_non_equity"] + got["skipped_unmapped"] + got["skipped_no_broadcast"]
        + got["resolved"]
    )
    assert got["parsed"] == result.rows_parsed and got["written"] == result.rows_written
    assert got["ins_eligible"] == result.rows_ins_eligible == 1
    assert got["scrip_codes"] == result.scrip_codes and got["scrip_stored"] == len(SCRIP_MAP)
    assert got["failed"] == []


def test_bse_scrip_symbol_map_round_trip(seeded_store):
    m = seeded_store.bse_scrip_symbol_map()
    assert m["500325"] == "RELIANCE" and m["544759"] == "GOLDLINE"
    assert "539843" not in m                                      # NINtec never seeded
