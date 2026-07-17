"""BackfillJob (§3.2.3 / §4.4 job 3 / §2.6 step 4): chunking, checkpoint resume, throttle wiring.

Uses a fake kite client that records every historical() request, so chunk boundaries (the pinned
Kite range caps: minute ≤60 days/request, day ≤2000 days/request), checkpoint resume after an
interruption, and the src provenance of written bars are asserted exactly. Throttle: one test runs
the REAL ``KiteClient`` facade with a spy limiter to prove every backfill request is acquired
through the shared RateLimiter's ``historical`` bucket (A2 ≤3 req/s — pacing itself is covered by
test_rate_limiter.py).
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest

from engine.broker.kite_client import KiteClient
from engine.core.clock import IST
from engine.core.config import Settings
from engine.marketdata.backfill import (
    KITE_DAY_CHUNK_DAYS,
    KITE_MINUTE_CHUNK_DAYS,
    BackfillJob,
)
from engine.marketdata.store import MarketStore

TOKENS = {"RELIANCE": 408065, "TCS": 2953217}


@pytest.fixture
def store(tmp_path, clock):
    s = MarketStore(tmp_path / "market.duckdb", tmp_path / "pq", clock)
    s.open()
    yield s
    s.close()


class FakeKite:
    """Duck-typed stand-in for KiteClient.historical: records calls, returns canned candles."""

    def __init__(self, candles_fn=None, fail_on_call: set[int] | None = None) -> None:
        self.calls: list[tuple[int, dt.datetime, dt.datetime, str]] = []
        self._candles_fn = candles_fn or (lambda token, frm, to, interval: [])
        self._fail_on_call = fail_on_call or set()

    async def historical(self, token, frm, to, interval):
        n = len(self.calls)
        self.calls.append((token, frm, to, interval))
        if n in self._fail_on_call:
            raise RuntimeError("kite says no")
        return self._candles_fn(token, frm, to, interval)


def one_minute_candle(token, frm, to, interval):
    """One 09:15 candle on the chunk's first day — enough to count written bars per request."""
    ts = frm.replace(hour=9, minute=15, second=0, microsecond=0)
    return [{"date": ts, "open": 100.0, "high": 101.0, "low": 99.5, "close": 100.55, "volume": 1234}]


def _job(store, kite, clock, conn, settings=None) -> BackfillJob:
    return BackfillJob(
        store, kite, clock, settings or Settings(), conn, lambda s: TOKENS.get(s)
    )


def _checkpoint(conn, symbol, interval):
    row = conn.execute(
        "SELECT through_date FROM backfill_checkpoints WHERE symbol=? AND interval=?",
        (symbol, interval),
    ).fetchone()
    return None if row is None else row["through_date"]


# ------------------------------------------------------------------ chunking (pinned Kite caps)
async def test_minute_backfill_chunks_at_60_days(store, clock, conn):
    assert KITE_MINUTE_CHUNK_DAYS == 60 and KITE_DAY_CHUNK_DAYS == 2000
    kite = FakeKite(one_minute_candle)
    job = _job(store, kite, clock, conn)
    start, end = dt.date(2026, 1, 1), dt.date(2026, 5, 10)     # 130 days inclusive → 3 chunks

    report = await job.run(["RELIANCE"], "minute", start, end)

    spans = [(frm.date(), to.date()) for _, frm, to, _ in kite.calls]
    assert spans == [
        (dt.date(2026, 1, 1), dt.date(2026, 3, 1)),            # 60 days
        (dt.date(2026, 3, 2), dt.date(2026, 4, 30)),           # 60 days
        (dt.date(2026, 5, 1), dt.date(2026, 5, 10)),           # remainder
    ]
    assert all(interval == "minute" for _, _, _, interval in kite.calls)
    assert all(token == TOKENS["RELIANCE"] for token, _, _, _ in kite.calls)
    assert report.bars_written == 3
    assert len(report.fetched) == 3 and not report.failed
    assert _checkpoint(conn, "RELIANCE", "minute") == end.isoformat()

    # Bars landed src='kite_official' (canonical official rows), prices Decimal-exact, NO
    # re-adjustment applied (A11: Kite candles are already corp-action adjusted).
    bars = store.get_bars_1m(
        "RELIANCE",
        clock.combine(dt.date(2026, 1, 1), dt.time(0, 0)),
        clock.combine(dt.date(2026, 5, 11), dt.time(0, 0)),
    )
    assert len(bars) == 3
    assert {b.src for b in bars} == {"kite_official"}
    assert bars[0].close == Decimal("100.55")


async def test_day_backfill_single_chunk_writes_bars_1d(store, clock, conn):
    def day_candle(token, frm, to, interval):
        return [{"date": frm.replace(hour=0, minute=0, second=0, microsecond=0),
                 "open": 10.0, "high": 12.0, "low": 9.0, "close": 11.5, "volume": 999}]

    kite = FakeKite(day_candle)
    job = _job(store, kite, clock, conn)
    start, end = dt.date(2024, 7, 1), dt.date(2026, 6, 30)     # ~2y « 2000-day cap → 1 request

    report = await job.run(["TCS"], "day", start, end)
    assert len(kite.calls) == 1
    assert kite.calls[0][3] == "day"
    assert report.bars_written == 1
    rows = store.get_bars_1d("TCS", start, end)
    assert len(rows) == 1
    assert rows[0].close == Decimal("11.5") and rows[0].src == "kite_official"
    assert _checkpoint(conn, "TCS", "day") == end.isoformat()


# ------------------------------------------------------------------ checkpoint resume (A2)
async def test_completed_run_is_a_resume_noop(store, clock, conn):
    kite = FakeKite(one_minute_candle)
    job = _job(store, kite, clock, conn)
    start, end = dt.date(2026, 1, 1), dt.date(2026, 2, 15)
    await job.run(["RELIANCE"], "minute", start, end)
    calls_before = len(kite.calls)

    report = await job.run(["RELIANCE"], "minute", start, end)  # idempotent re-run
    assert len(kite.calls) == calls_before                      # zero new requests
    assert report.fetched == [] and report.bars_written == 0
    assert len(report.requested) == 1                           # still reported as requested


async def test_failure_keeps_checkpoint_and_resume_continues(store, clock, conn):
    start, end = dt.date(2026, 1, 1), dt.date(2026, 5, 10)
    kite = FakeKite(one_minute_candle, fail_on_call={1})        # chunk 2 of 3 blows up
    job = _job(store, kite, clock, conn)

    report = await job.run(["RELIANCE"], "minute", start, end)
    assert len(kite.calls) == 2                                 # stopped at the failure
    assert len(report.fetched) == 1
    assert len(report.failed) == 1
    assert report.failed[0].frm == "2026-03-02"                 # the abandoned span, with its error
    assert "kite says no" in report.failed[0].error
    assert _checkpoint(conn, "RELIANCE", "minute") == "2026-03-01"   # last SUCCESS only

    kite2 = FakeKite(one_minute_candle)
    report2 = await _job(store, kite2, clock, conn).run(["RELIANCE"], "minute", start, end)
    spans = [(frm.date(), to.date()) for _, frm, to, _ in kite2.calls]
    assert spans == [                                           # resumed from checkpoint + 1 day
        (dt.date(2026, 3, 2), dt.date(2026, 4, 30)),
        (dt.date(2026, 5, 1), dt.date(2026, 5, 10)),
    ]
    assert not report2.failed
    assert _checkpoint(conn, "RELIANCE", "minute") == end.isoformat()


async def test_unknown_token_is_reported_failed_without_a_request(store, clock, conn):
    kite = FakeKite(one_minute_candle)
    job = _job(store, kite, clock, conn)
    report = await job.run(["NOSUCH"], "minute", dt.date(2026, 1, 1), dt.date(2026, 1, 10))
    assert kite.calls == []
    assert len(report.failed) == 1
    assert report.failed[0].error == "unknown_instrument_token"


# ------------------------------------------------------------------ throttle wiring (A2)
class _SpyLimiter:
    def __init__(self) -> None:
        self.acquired: list[str] = []

    async def acquire(self, endpoint_class, intent="entry"):
        self.acquired.append(endpoint_class)


class _FakeKC:
    """pykiteconnect-shaped sync client."""

    def historical_data(self, instrument_token, from_date, to_date, interval):
        return []


async def test_every_request_goes_through_the_rate_limiter(store, clock, conn):
    """BackfillJob → KiteClient.historical → RateLimiter.acquire('historical') — the ≤3 req/s A2
    budget is enforced by the shared limiter, one acquire per chunk request."""
    spy = _SpyLimiter()
    real_client = KiteClient(_FakeKC(), spy, clock)
    job = _job(store, real_client, clock, conn)
    await job.run(["RELIANCE"], "minute", dt.date(2026, 1, 1), dt.date(2026, 5, 10))
    assert spy.acquired == ["historical"] * 3


# ------------------------------------------------------------------ §2.6 step 4: warmup_gap
async def test_warmup_gap_fills_half_open_range_src_gap_backfilled(store, clock, conn):
    base = dt.datetime(2026, 6, 17, 10, 0, tzinfo=IST)

    def gap_candles(token, frm, to, interval):
        # Official candles 10:00..10:06 — the job must keep only [frm, to).
        return [
            {"date": base + dt.timedelta(minutes=i), "open": 100.0 + i, "high": 101.0 + i,
             "low": 99.0 + i, "close": 100.5 + i, "volume": 100 * (i + 1)}
            for i in range(7)
        ]

    kite = FakeKite(gap_candles)
    job = _job(store, kite, clock, conn)
    frm = base + dt.timedelta(minutes=2)            # last-bar-seen anchor (10:02)
    to = base + dt.timedelta(minutes=5)             # now (10:05) — exclusive

    report = await job.warmup_gap(["RELIANCE"], frm, to)
    assert report.interval == "minute"
    assert report.bars_written == 3                 # 10:02, 10:03, 10:04
    assert not report.failed

    bars = store.get_bars_1m("RELIANCE", base, base + dt.timedelta(minutes=10))
    assert [b.ts_minute for b in bars] == [frm, frm + dt.timedelta(minutes=1),
                                           frm + dt.timedelta(minutes=2)]
    assert {b.src for b in bars} == {"gap_backfilled"}          # §2.6 offline-span provenance
    assert bars[0].close == Decimal("102.5")

    # warmup_gap is NOT checkpointed — every startup computes its own gap.
    n = conn.execute("SELECT COUNT(*) AS n FROM backfill_checkpoints").fetchone()["n"]
    assert n == 0


async def test_warmup_gap_failure_isolated_per_symbol(store, clock, conn):
    base = dt.datetime(2026, 6, 17, 10, 0, tzinfo=IST)

    def gap_candles(token, frm, to, interval):
        return [{"date": base, "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1}]

    kite = FakeKite(gap_candles, fail_on_call={0})   # first symbol's fetch fails
    job = _job(store, kite, clock, conn)
    report = await job.warmup_gap(
        ["RELIANCE", "TCS"], base, base + dt.timedelta(minutes=1)
    )
    assert len(report.failed) == 1 and report.failed[0].symbol == "RELIANCE"
    assert len(report.fetched) == 1 and report.fetched[0].symbol == "TCS"
    assert report.bars_written == 1
