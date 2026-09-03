"""scripts/backfill_bhavcopy.py: archive URL selection/fallback/holiday, checkpoint resume, and
failure isolation (worklog 2026-09-01 follow-up — full-market bars_1d history for backtest_hi52.py).

Loose-script import mechanism copied from tests/unit/test_backtest_cli.py (spec_from_file_location).
"""

from __future__ import annotations

import importlib.util
import io
import sys
import zipfile
from datetime import date
from pathlib import Path

import httpx

_SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "backfill_bhavcopy.py"
_spec = importlib.util.spec_from_file_location("mt_backfill_bhavcopy", _SCRIPT_PATH)
mod = importlib.util.module_from_spec(_spec)
sys.modules["mt_backfill_bhavcopy"] = mod
_spec.loader.exec_module(mod)

from engine.datafeeds.bhavcopy import BhavcopyJob, parse_bhavcopy_csv  # noqa: E402 - after the shim above


# --------------------------------------------------------------------------- fixtures / fakes
def _http_404(url: str) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", url)
    response = httpx.Response(404, request=request)
    return httpx.HTTPStatusError("404 not found", request=request, response=response)


def _legacy_csv_text(d: date) -> str:
    ts = d.strftime("%d-%b-%Y").upper()
    header = "SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTTRDQTY,TOTTRDVAL,TIMESTAMP,TOTALTRADES,ISIN\n"
    row = (
        f"RELIANCE,EQ,2900.00,2955.50,2890.10,2940.25,2940.00,2895.00,5000000,10000000.00,"
        f"{ts},12345,INE002A01018\n"
    )
    return header + row


def _zip_bytes(csv_text: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("bhav.csv", csv_text)
    return buf.getvalue()


class FakeStore:
    """Minimal duck-typed stand-in: only what BhavcopyJob._persist touches."""

    def __init__(self) -> None:
        self.upserted: list = []

    def get_bars_1d_for_day(self, d):
        return []

    def upsert_bars_1d(self, bars):
        self.upserted.extend(bars)
        return len(bars)

    async def arun(self, fn, *args, **kwargs):
        return fn(*args, **kwargs)


# --------------------------------------------------------------------------- (1) URL choice
def test_legacy_url_exact_for_pre_switch_date():
    urls = mod._candidate_urls(date(2023, 8, 1))
    assert urls[0] == (
        "https://nsearchives.nseindia.com/content/historical/EQUITIES/2023/AUG/cm01AUG2023bhav.csv.zip"
    )
    assert urls[1] == mod.BHAVCOPY_URL_TEMPLATE.format(d=date(2023, 8, 1))


def test_udiff_first_for_post_switch_date():
    d = date(2025, 8, 1)
    urls = mod._candidate_urls(d)
    assert urls[0] == mod.BHAVCOPY_URL_TEMPLATE.format(d=d)
    assert urls[1] == mod._legacy_bhavcopy_url(d)


def test_switch_date_itself_tries_udiff_first():
    d = date(2024, 7, 8)
    urls = mod._candidate_urls(d)
    assert urls[0] == mod.BHAVCOPY_URL_TEMPLATE.format(d=d)


# --------------------------------------------------------------------------- (2) fallback + holiday
async def test_fallback_to_legacy_ingests_and_checkpoints(conn, clock, monkeypatch):
    d = date(2025, 8, 1)  # UDiFF tried first
    urls = mod._candidate_urls(d)
    calls: list[str] = []

    async def fake_nse_get(http, url, *, timeout):
        calls.append(url)
        if url == urls[0]:
            raise _http_404(url)
        assert url == urls[1]
        return httpx.Response(200, content=_zip_bytes(_legacy_csv_text(d)))

    monkeypatch.setattr(mod, "nse_get", fake_nse_get)  # the name the script imports
    store = FakeStore()
    job = BhavcopyJob(store, clock, http=None)
    report = mod._new_report()

    await mod._ingest_bhavcopy_date(conn, store, None, clock, job, d, report)

    assert calls == [urls[0], urls[1]]
    assert report["bhavcopy"]["ingested"] == 1
    assert report["bhavcopy"]["failed"] == 0
    assert len(store.upserted) == 1 and store.upserted[0].symbol == "RELIANCE"
    assert mod._cp_done(conn, mod._FEED_BHAVCOPY, d.isoformat())
    row = conn.execute(
        "SELECT through_date FROM filings_backfill_checkpoints WHERE feed=? AND unit=?",
        (mod._FEED_BHAVCOPY, d.isoformat()),
    ).fetchone()
    assert row["through_date"] == d.isoformat()


async def test_both_404_checkpoints_holiday_and_writes_nothing(conn, clock, monkeypatch):
    d = date(2025, 8, 2)  # a Saturday date value is fine here - this test drives _ingest directly

    async def fake_nse_get(http, url, *, timeout):
        raise _http_404(url)

    monkeypatch.setattr(mod, "nse_get", fake_nse_get)
    store = FakeStore()
    job = BhavcopyJob(store, clock, http=None)
    report = mod._new_report()

    await mod._ingest_bhavcopy_date(conn, store, None, clock, job, d, report)

    assert report["bhavcopy"]["holiday"] == 1
    assert report["bhavcopy"]["ingested"] == 0
    assert store.upserted == []
    row = conn.execute(
        "SELECT through_date FROM filings_backfill_checkpoints WHERE feed=? AND unit=?",
        (mod._FEED_BHAVCOPY, d.isoformat()),
    ).fetchone()
    assert row["through_date"] == "holiday"


# --------------------------------------------------------------------------- (3) resume
async def test_resume_skips_a_checkpointed_date_without_fetching(conn, clock, monkeypatch):
    d = date(2025, 8, 1)
    mod._cp_set(conn, mod._FEED_BHAVCOPY, d.isoformat(), d.isoformat(), clock.now().isoformat())

    async def fail_if_called(http, url, *, timeout):
        raise AssertionError(f"nse_get must not be called for a checkpointed date, got {url}")

    monkeypatch.setattr(mod, "nse_get", fail_if_called)
    store = FakeStore()
    job = BhavcopyJob(store, clock, http=None)
    report = mod._new_report()

    await mod._run_bhavcopy_leg(conn, store, None, clock, job, d, d, 0.0, report)

    assert report["bhavcopy"]["skipped"] == 1
    assert report["bhavcopy"]["attempted"] == 0
    assert store.upserted == []


# --------------------------------------------------------------------------- (4) non-404 failure
def _udiff_csv_text(d: date) -> str:
    header = "TckrSymb,SctySrs,OpnPric,HghPric,LwPric,ClsPric,TtlTradgVol,TradDt\n"
    row = f"RELIANCE,EQ,2900.00,2955.50,2890.10,2940.25,5000000,{d:%Y-%m-%d}\n"
    return header + row


async def test_non_404_error_fails_the_date_and_loop_continues(conn, clock, monkeypatch):
    """Friday fails on a non-404; Saturday and Sunday are ATTEMPTED (NSE holds weekend sessions) and
    404 on both formats -> holidays; Monday ingests. The failed date carries no checkpoint."""
    d1 = date(2025, 8, 1)  # Friday - first (UDiFF) URL raises a non-404 error, date fails outright
    d2 = date(2025, 8, 4)  # Monday - succeeds on its first (UDiFF) URL
    urls1 = mod._candidate_urls(d1)
    urls2 = mod._candidate_urls(d2)

    async def fake_nse_get(http, url, *, timeout):
        if url == urls1[0]:
            raise RuntimeError("connection reset")  # non-404, non-HTTPStatusError
        if url == urls2[0]:
            return httpx.Response(200, content=_zip_bytes(_udiff_csv_text(d2)))
        raise _http_404(url)                         # the weekend: no file in either format

    monkeypatch.setattr(mod, "nse_get", fake_nse_get)
    store = FakeStore()
    job = BhavcopyJob(store, clock, http=None)
    report = mod._new_report()

    await mod._run_bhavcopy_leg(conn, store, None, clock, job, d1, d2, 0.0, report)

    assert report["bhavcopy"]["failed"] == 1 and report["bhavcopy"]["failed_dates"] == [d1.isoformat()]
    assert report["bhavcopy"]["holiday"] == 2
    assert report["bhavcopy"]["ingested"] == 1
    assert mod._cp_done(conn, mod._FEED_BHAVCOPY, d1.isoformat()) is False
    assert mod._cp_done(conn, mod._FEED_BHAVCOPY, "2025-08-02") is True     # a weekend holiday row
    assert mod._cp_done(conn, mod._FEED_BHAVCOPY, d2.isoformat()) is True


async def test_a_non_404_status_on_the_first_url_never_tries_the_fallback(conn, clock, monkeypatch):
    d = date(2025, 8, 1)
    calls: list[str] = []

    async def fake_nse_get(http, url, *, timeout):
        calls.append(url)
        request = httpx.Request("GET", url)
        raise httpx.HTTPStatusError("503", request=request, response=httpx.Response(503, request=request))

    monkeypatch.setattr(mod, "nse_get", fake_nse_get)
    store = FakeStore()
    report = mod._new_report()

    outcome = await mod._ingest_bhavcopy_date(
        conn, store, None, clock, BhavcopyJob(store, clock, http=None), d, report
    )

    assert outcome == "failed" and calls == [mod._candidate_urls(d)[0]]
    assert mod._cp_done(conn, mod._FEED_BHAVCOPY, d.isoformat()) is False


async def test_a_holiday_streak_stops_the_leg_and_clears_its_checkpoints(conn, clock, monkeypatch):
    """An archive outage (or a wrong URL era) answers 404 to everything. NSE never closes for
    _MAX_HOLIDAY_STREAK consecutive days, so the leg stops there and hands the streak back to the
    next run instead of leaving a permanent 'holiday' verdict on real sessions."""
    async def fake_nse_get(http, url, *, timeout):
        raise _http_404(url)

    monkeypatch.setattr(mod, "nse_get", fake_nse_get)
    store = FakeStore()
    report = mod._new_report()
    frm, to = date(2025, 8, 1), date(2025, 8, 31)

    await mod._run_bhavcopy_leg(
        conn, store, None, clock, BhavcopyJob(store, clock, http=None), frm, to, 0.0, report
    )

    n = mod._MAX_HOLIDAY_STREAK
    assert report["bhavcopy"]["attempted"] == n                       # stopped, did not walk to `to`
    assert report["bhavcopy"]["holiday"] == 0 and report["bhavcopy"]["failed"] == n
    assert report["bhavcopy"]["aborted_at"] == "2025-08-07"
    assert report["bhavcopy"]["failed_dates"] == [f"2025-08-0{i}" for i in range(1, n + 1)]
    for unit in report["bhavcopy"]["failed_dates"]:
        assert mod._cp_done(conn, mod._FEED_BHAVCOPY, unit) is False   # un-checkpointed: retried next run


# --------------------------------------------------------------------------- corp-actions leg
async def test_corp_actions_windows_checkpoint_with_rows_and_fail_empty(conn, clock, monkeypatch):
    class CorpStore:
        def __init__(self) -> None:
            self.rows: list = []

        def upsert_corp_actions(self, rows):
            self.rows.extend(rows)
            return len(rows)

        async def arun(self, fn, *args, **kwargs):
            return fn(*args, **kwargs)

    frm, to = date(2023, 8, 1), date(2023, 9, 10)          # two <=35-day windows
    windows = mod._windows(frm, to, mod._CORP_ACTIONS_WINDOW_DAYS)
    assert len(windows) == 2
    served = {
        mod._corp_actions_window_url(*windows[0]):
            b'[{"symbol": "ABC", "exDate": "15-Aug-2023", "subject": "Bonus 1:1"}]',
        mod._corp_actions_window_url(*windows[1]): b"[]",   # an empty window is a capped response
    }

    async def fake_nse_get(http, url, *, timeout):
        return httpx.Response(200, content=served[url])

    monkeypatch.setattr(mod, "nse_get", fake_nse_get)
    store = CorpStore()
    report = mod._new_report()

    await mod._run_corp_actions_leg(conn, store, None, clock, frm, to, 0.0, report)

    unit0 = f"{windows[0][0].isoformat()}..{windows[0][1].isoformat()}"
    unit1 = f"{windows[1][0].isoformat()}..{windows[1][1].isoformat()}"
    assert report["corp_actions"]["windows"] == 1 and report["corp_actions"]["failed"] == 1
    assert report["corp_actions"]["rows_by_window"] == {unit0: 1}
    assert report["corp_actions"]["failed_windows"] == [unit1]
    assert mod._cp_done(conn, mod._FEED_CORP_ACTIONS, unit0) is True
    assert mod._cp_done(conn, mod._FEED_CORP_ACTIONS, unit1) is False
    assert store.rows[0]["symbol"] == "ABC" and store.rows[0]["kind"] == "bonus"
    assert store.rows[0]["recorded_at"] == clock.now()                # stamped as CorpActionsJob.run does


async def test_a_served_file_with_zero_eq_rows_fails_the_date(conn, clock, monkeypatch):
    """A 200 that parses to nothing (header-only / corrupt member) is a failure, not an ingested day --
    the live job raises on it too (bhavcopy.py: 'parsed to zero EQ rows'); no checkpoint, so it retries."""
    d = date(2025, 8, 1)

    async def fake_nse_get(http, url, *, timeout):
        return httpx.Response(200, content=_zip_bytes("TckrSymb,SctySrs,ClsPric,TradDt\n"))

    monkeypatch.setattr(mod, "nse_get", fake_nse_get)
    store = FakeStore()
    report = mod._new_report()

    await mod._ingest_bhavcopy_date(conn, store, None, clock, BhavcopyJob(store, clock, http=None), d, report)

    assert report["bhavcopy"]["failed"] == 1 and report["bhavcopy"]["ingested"] == 0
    assert store.upserted == []
    assert mod._cp_done(conn, mod._FEED_BHAVCOPY, d.isoformat()) is False


# --------------------------------------------------------------------------- (5) legacy TIMESTAMP format
def test_parse_bhavcopy_csv_accepts_legacy_uppercase_timestamp():
    """Pins that the EXISTING bhavcopy._parse_date already handles '01-AUG-2023' (via its %d-%b-%Y
    format, which strptime matches case-insensitively) - no source change was needed for this script."""
    d = date(2023, 8, 1)
    bars = parse_bhavcopy_csv(_legacy_csv_text(d), d)
    assert len(bars) == 1
    assert bars[0].symbol == "RELIANCE" and bars[0].d == d
