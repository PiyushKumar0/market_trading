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
from kiteconnect.exceptions import TokenException

from engine.broker.kite_client import KiteClient
from engine.core.clock import IST
from engine.core.config import Settings
from engine.marketdata.backfill import (
    KITE_DAY_CHUNK_DAYS,
    KITE_MINUTE_CHUNK_DAYS,
    BackfillJob,
)
from engine.marketdata.store import DailyBar, MarketStore

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
    """One 09:15 candle on the chunk's LAST day — data present through the requested end, so the
    observed-through checkpoint lands on the chunk end (one bar per request keeps counting easy)."""
    ts = to.replace(hour=9, minute=15, second=0, microsecond=0)
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
        return [{"date": to.replace(hour=0, minute=0, second=0, microsecond=0),
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


# ---------------------------------------------------- observed-through checkpoints (2026-07-28)
async def test_empty_chunk_never_advances_the_checkpoint(store, clock, conn):
    """The 2026-07-28 poisoning: a day requested before its bar exists (pre-close "today") must
    stay un-checkpointed — recording the REQUESTED end as complete made the hole permanent
    ("already_complete" on every later pass; warm-up froze on the missing session)."""
    kite = FakeKite()                                           # returns [] for every request
    d = dt.date(2026, 7, 28)
    report = await _job(store, kite, clock, conn).run(["TCS"], "day", d, d)
    assert len(kite.calls) == 1
    assert report.bars_written == 0 and not report.failed
    assert _checkpoint(conn, "TCS", "day") is None              # NOT '2026-07-28'

    # Self-healing: the next run re-requests the same day and checkpoints once the bar exists.
    def now_published(token, frm, to, interval):
        return [{"date": frm, "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1}]
    kite2 = FakeKite(now_published)
    await _job(store, kite2, clock, conn).run(["TCS"], "day", d, d)
    assert len(kite2.calls) == 1
    assert _checkpoint(conn, "TCS", "day") == d.isoformat()


async def test_checkpoint_advances_only_to_the_last_observed_candle(store, clock, conn):
    """Candles short of the requested end (unpublished tail): checkpoint = observed-through, so
    the missing tail is re-fetched by the next run instead of being skipped forever."""
    def first_day_only(token, frm, to, interval):
        return [{"date": frm, "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1}]

    start, end = dt.date(2026, 7, 24), dt.date(2026, 7, 28)
    await _job(store, FakeKite(first_day_only), clock, conn).run(["TCS"], "day", start, end)
    assert _checkpoint(conn, "TCS", "day") == start.isoformat()


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


# ------------------------------------------------------------------ TokenException abort (2026-07-21)
class _TokenKite:
    """historical() always raises TokenException (a dead token fails every call identically)."""

    def __init__(self) -> None:
        self.calls: list[tuple] = []

    async def historical(self, token, frm, to, interval):
        self.calls.append((token, frm, to, interval))
        raise TokenException("Incorrect api_key or access_token")


async def test_run_aborts_whole_run_on_token_rejection(store, clock, conn):
    """A rejected token aborts the ENTIRE run after the first request: the attempted span carries the
    real token error, every remaining symbol is reported 'aborted_token_rejected' with no extra
    requests (no per-symbol hammering)."""
    kite = _TokenKite()
    job = _job(store, kite, clock, conn)
    report = await job.run(
        ["RELIANCE", "TCS", "INFY"], "minute", dt.date(2026, 1, 1), dt.date(2026, 1, 10)
    )
    assert len(kite.calls) == 1                            # aborted after the very first request
    assert len(report.failed) == 3                         # attempted + 2 aborted-remainder
    by_symbol = {s.symbol: s.error for s in report.failed}
    assert "TokenException" in by_symbol["RELIANCE"]       # the attempted span keeps the real error
    assert by_symbol["TCS"] == "aborted_token_rejected"
    assert by_symbol["INFY"] == "aborted_token_rejected"
    assert report.fetched == []


async def test_run_non_token_error_only_fails_that_symbol_and_continues(store, clock, conn):
    """A NON-token error is isolated to its symbol (existing behaviour): the next symbol still runs."""
    kite = FakeKite(one_minute_candle, fail_on_call={0})   # RELIANCE (call 0) raises RuntimeError
    job = _job(store, kite, clock, conn)
    report = await job.run(["RELIANCE", "TCS"], "minute", dt.date(2026, 1, 1), dt.date(2026, 1, 10))
    assert len(kite.calls) == 2                            # both symbols attempted — no abort
    assert [f.symbol for f in report.failed] == ["RELIANCE"]
    assert "RuntimeError" in report.failed[0].error
    assert [f.symbol for f in report.fetched] == ["TCS"]   # TCS proceeded after RELIANCE failed


async def test_warmup_gap_aborts_whole_run_on_token_rejection(store, clock, conn):
    base = dt.datetime(2026, 6, 17, 10, 0, tzinfo=IST)
    kite = _TokenKite()
    job = _job(store, kite, clock, conn)
    report = await job.warmup_gap(
        ["RELIANCE", "TCS", "INFY"], base, base + dt.timedelta(minutes=5)
    )
    assert len(kite.calls) == 1                            # aborted after the first symbol's fetch
    assert len(report.failed) == 3
    by_symbol = {s.symbol: s.error for s in report.failed}
    assert "TokenException" in by_symbol["RELIANCE"]
    assert by_symbol["TCS"] == "aborted_token_rejected"
    assert by_symbol["INFY"] == "aborted_token_rejected"
    assert report.fetched == []


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


# ------------------------- 2026-09-18: upstream-confirmed no-trade minutes (bars_1m_no_trade) ----
# A thin, high-priced symbol (PTCIL 13:36 on 09-16; DEEPAKNTR 374/375 on 09-17) has minutes in which
# NOTHING trades: no tick ⇒ no self-built bar, and Kite publishes no candle for a tradeless minute ⇒
# the gap fill fetches the day and writes nothing. The minute was a permanent hole that refused the
# symbol for the whole session. ``confirm_until`` lets warmup_gap record it as an OBSERVATION instead,
# under four guards. The conftest clock is 2026-06-17 10:05 IST, so gaps are built well before it.
NOW = dt.datetime(2026, 6, 17, 10, 5, tzinfo=IST)          # == tests.conftest.FIXED_NOW
FOUR_TOKENS = {"PTCIL": 1, "MRF": 2, "SHYAMMETL": 3, "KIMS": 4}


def _job_tokens(store, kite, clock, conn, tokens) -> BackfillJob:
    """A job over an explicit tradingsymbol → token map (the guard-D sweep needs four symbols)."""
    return BackfillJob(store, kite, clock, Settings(), conn, lambda s: tokens.get(s))


def _candles(minutes, *, extra=()):
    """Kite-shaped minute candles at exactly ``minutes`` (+ any ``extra``, e.g. a later one)."""
    return [
        {"date": m, "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5, "volume": 10}
        for m in (*minutes, *extra)
    ]


def _minutes(base, *offsets):
    return [base + dt.timedelta(minutes=o) for o in offsets]


async def test_warmup_gap_confirms_upstream_empty_minutes(store, clock, conn):
    """The PTCIL case end to end: Kite returns the day's candles for the symbol and a LATER one, but
    none for 09:57 — which the store also lacks. That minute is upstream-confirmed no-trade, so it is
    recorded in bars_1m_no_trade (never as a bar) and coverage_gaps then reads clean."""
    base = dt.datetime(2026, 6, 17, 9, 55, tzinfo=IST)
    frm, to = base, base + dt.timedelta(minutes=5)          # 09:55..09:59, ends 6 min before "now"
    quiet = base + dt.timedelta(minutes=2)                  # 09:57 — nothing traded
    traded = _minutes(base, 0, 1, 3, 4)
    kite = FakeKite(lambda *_a: _candles(traded, extra=_minutes(base, 5)))   # + a LATER candle
    job = _job_tokens(store, kite, clock, conn, {"PTCIL": 1})

    report = await job.warmup_gap(["PTCIL"], frm, to, confirm_until=to)

    assert report.bars_written == 4                         # only the four in-span candles
    assert report.no_trade_confirmed == 1
    assert report.no_trade_correlated_skipped == 0
    assert not report.failed
    # No synthetic bar: bars_1m holds the four real minutes and nothing at 09:57.
    assert [b.ts_minute for b in store.get_bars_1m("PTCIL", frm, to)] == traded
    assert store.no_trade_minutes("PTCIL", frm, to) == {quiet}
    # …and §2.6 now sees contiguous coverage — the symbol is no longer refused all session.
    assert store.coverage_gaps("PTCIL", frm, to) == []
    assert store.has_contiguous_coverage("PTCIL", frm, to) is True


async def test_warmup_gap_guard_b_needs_a_later_kite_candle(store, clock, conn):
    """Guard B: the missing minute must be BEFORE the symbol's last returned candle. Here it is the
    last minute Kite could have returned, so a Kite-side truncation and a tradeless minute look
    identical — leave the hole; the next pass will have a later candle to prove the feed got there."""
    base = dt.datetime(2026, 6, 17, 9, 55, tzinfo=IST)
    frm, to = base, base + dt.timedelta(minutes=5)
    tail = base + dt.timedelta(minutes=4)                   # 09:59 — the LAST minute in the span
    kite = FakeKite(lambda *_a: _candles(_minutes(base, 0, 1, 2, 3)))        # nothing after 09:58
    job = _job_tokens(store, kite, clock, conn, {"PTCIL": 1})

    report = await job.warmup_gap(["PTCIL"], frm, to, confirm_until=to)

    assert report.bars_written == 4
    assert report.no_trade_confirmed == 0
    assert store.no_trade_minutes("PTCIL", frm, to) == set()
    assert store.coverage_gaps("PTCIL", frm, to) == [tail]  # still a hole, deliberately


async def test_warmup_gap_guard_c_never_confirms_a_minute_kite_may_not_have_published_yet(
    store, clock, conn
):
    """Guard C: the cutoff is ``min(confirm_until, now − 2 min)``. 10:04 is one minute old — Kite may
    simply not have published it yet — so it stays a hole even though a 10:05 candle exists (guard B
    passes). The older 10:01 miss, the control, IS confirmed: only the cutoff separates them."""
    base = dt.datetime(2026, 6, 17, 10, 0, tzinfo=IST)
    frm, to = base, base + dt.timedelta(minutes=6)          # 10:00..10:05, now == 10:05
    too_new = base + dt.timedelta(minutes=4)                # 10:04 — inside now − 2 min
    old_miss = base + dt.timedelta(minutes=1)               # 10:01 — safely published
    kite = FakeKite(lambda *_a: _candles(_minutes(base, 0, 2, 3, 5)))
    job = _job_tokens(store, kite, clock, conn, {"PTCIL": 1})

    report = await job.warmup_gap(["PTCIL"], frm, to, confirm_until=NOW)

    assert report.no_trade_confirmed == 1
    assert store.no_trade_minutes("PTCIL", frm, to) == {old_miss}
    assert store.coverage_gaps("PTCIL", frm, to) == [too_new]


async def test_warmup_gap_guard_a_confirms_nothing_when_kite_returns_no_candles(store, clock, conn):
    """Guard A: if Kite answered with NOTHING for the span, a broker outage and a genuinely dead
    symbol are indistinguishable. Confirm nothing — every minute stays a hole."""
    base = dt.datetime(2026, 6, 17, 9, 55, tzinfo=IST)
    frm, to = base, base + dt.timedelta(minutes=5)
    kite = FakeKite()                                       # returns [] for every request
    job = _job_tokens(store, kite, clock, conn, {"PTCIL": 1})

    report = await job.warmup_gap(["PTCIL"], frm, to, confirm_until=to)

    assert len(kite.calls) == 1 and report.bars_written == 0
    assert report.no_trade_confirmed == 0 and report.no_trade_correlated_skipped == 0
    assert store.no_trade_minutes("PTCIL", frm, to) == set()
    assert len(store.coverage_gaps("PTCIL", frm, to)) == 5


async def test_warmup_gap_guard_d_skips_correlated_misses(store, clock, conn):
    """Guard D: thin symbols go quiet INDEPENDENTLY. The same minute missing across
    ``max(3, ceil(5% of answered))`` of the swept symbols is a FEED gap, not four coincidences — it is
    dropped for every symbol and counted, never confirmed. One symbol missing it alone is confirmed."""
    syms = list(FOUR_TOKENS)

    # (a) ALL FOUR miss 09:57 ⇒ 4 >= max(3, ceil(0.05*4)) = 3 ⇒ correlated ⇒ nothing confirmed.
    base = dt.datetime(2026, 6, 17, 9, 55, tzinfo=IST)
    frm, to = base, base + dt.timedelta(minutes=5)
    quiet = base + dt.timedelta(minutes=2)
    kite = FakeKite(lambda *_a: _candles(_minutes(base, 0, 1, 3, 4)))
    report = await _job_tokens(store, kite, clock, conn, FOUR_TOKENS).warmup_gap(
        syms, frm, to, confirm_until=to
    )
    assert report.no_trade_confirmed == 0
    assert report.no_trade_correlated_skipped == 4          # one (symbol, minute) drop per symbol
    for s in syms:
        assert store.no_trade_minutes(s, frm, to) == set()
        assert store.coverage_gaps(s, frm, to) == [quiet]

    # (b) Same sweep, a fresh window in which only PTCIL is quiet at 09:42 ⇒ 1 < 3 ⇒ confirmed.
    base2 = dt.datetime(2026, 6, 17, 9, 40, tzinfo=IST)
    frm2, to2 = base2, base2 + dt.timedelta(minutes=5)
    lone = base2 + dt.timedelta(minutes=2)
    full = _minutes(base2, 0, 1, 2, 3, 4)

    def per_symbol(token, _frm, _to, _interval):
        if token == FOUR_TOKENS["PTCIL"]:
            return _candles([m for m in full if m != lone])
        return _candles(full)

    kite2 = FakeKite(per_symbol)
    report2 = await _job_tokens(store, kite2, clock, conn, FOUR_TOKENS).warmup_gap(
        syms, frm2, to2, confirm_until=to2
    )
    assert report2.no_trade_confirmed == 1
    assert report2.no_trade_correlated_skipped == 0
    assert store.no_trade_minutes("PTCIL", frm2, to2) == {lone}
    assert store.coverage_gaps("PTCIL", frm2, to2) == []
    assert all(store.coverage_gaps(s, frm2, to2) == [] for s in syms)


async def test_warmup_gap_confirms_nothing_without_confirm_until(store, clock, conn):
    """The legacy default is untouched: no ``confirm_until`` ⇒ no confirmation, no table row, and the
    tradeless minute stays a coverage hole exactly as before 2026-09-18."""
    base = dt.datetime(2026, 6, 17, 9, 55, tzinfo=IST)
    frm, to = base, base + dt.timedelta(minutes=5)
    quiet = base + dt.timedelta(minutes=2)
    kite = FakeKite(lambda *_a: _candles(_minutes(base, 0, 1, 3, 4)))
    job = _job_tokens(store, kite, clock, conn, {"PTCIL": 1})

    report = await job.warmup_gap(["PTCIL"], frm, to)

    assert report.bars_written == 4
    assert report.no_trade_confirmed == 0 and report.no_trade_correlated_skipped == 0
    assert store.no_trade_minutes("PTCIL", frm, to) == set()
    assert store.coverage_gaps("PTCIL", frm, to) == [quiet]


async def test_confirmed_minutes_are_not_refetched(store, clock, conn):
    """The confirmation is spent ONCE: coverage_gaps now reads clean for the healed symbol, so the
    next warmup_gap's per-symbol gap check skips it before any network call (the same
    fill-gaps-only contract that makes a fully-covered symbol free)."""
    base = dt.datetime(2026, 6, 17, 9, 55, tzinfo=IST)
    frm, to = base, base + dt.timedelta(minutes=5)
    kite = FakeKite(lambda *_a: _candles(_minutes(base, 0, 1, 3, 4)))
    job = _job_tokens(store, kite, clock, conn, {"PTCIL": 1})
    assert (await job.warmup_gap(["PTCIL"], frm, to, confirm_until=to)).no_trade_confirmed == 1
    assert len(kite.calls) == 1

    kite2 = FakeKite(lambda *_a: _candles(_minutes(base, 0, 1, 3, 4)))
    report2 = await _job_tokens(store, kite2, clock, conn, {"PTCIL": 1}).warmup_gap(
        ["PTCIL"], frm, to, confirm_until=to
    )
    assert kite2.calls == []                                # ZERO broker requests
    assert report2.bars_written == 0 and report2.no_trade_confirmed == 0
    assert report2.fetched == []


class _LateTokenKite(_TokenKite):
    """Answers the FIRST symbol (leaving it a real confirmation candidate), then dies like its
    parent — the shape that proves the abort discards work already staged in phase 1."""

    def __init__(self, candles_fn) -> None:
        super().__init__()
        self._candles_fn = candles_fn

    async def historical(self, token, frm, to, interval):
        if not self.calls:
            self.calls.append((token, frm, to, interval))
            return self._candles_fn(token, frm, to, interval)
        return await super().historical(token, frm, to, interval)


async def test_warmup_gap_token_abort_discards_pending_confirmations(store, clock, conn):
    """A token rejection aborts the sweep mid-way, so ``answered`` is not the denominator guard D was
    calibrated on. PTCIL was processed first and HAD a candidate — phase 2 still confirms nothing."""
    base = dt.datetime(2026, 6, 17, 9, 55, tzinfo=IST)
    frm, to = base, base + dt.timedelta(minutes=5)
    quiet = base + dt.timedelta(minutes=2)
    kite = _LateTokenKite(lambda *_a: _candles(_minutes(base, 0, 1, 3, 4)))
    job = _job_tokens(store, kite, clock, conn, FOUR_TOKENS)

    report = await job.warmup_gap(list(FOUR_TOKENS), frm, to, confirm_until=to)

    assert len(kite.calls) == 2                             # PTCIL answered, MRF rejected, then abort
    assert report.no_trade_confirmed == 0 and report.no_trade_correlated_skipped == 0
    assert store.no_trade_minutes("PTCIL", frm, to) == set()
    assert store.coverage_gaps("PTCIL", frm, to) == [quiet]  # the hole survives the abort
    assert report.bars_written == 4                          # PTCIL's real bars still landed


# ------------------------------------------------------------------ 2026-09-15: daily_gap (WarmupGate
# daily window repair — OLAELEC entered the watchlist with a 57-session bars_1d hole and froze the
# DAILY class for 12 h; the minute-only newcomer fill never touched daily history at all).
DAILY_SESSIONS_5 = [
    dt.date(2026, 6, 10), dt.date(2026, 6, 11), dt.date(2026, 6, 12),
    dt.date(2026, 6, 13), dt.date(2026, 6, 14),
]


def five_session_day_candles(token, frm, to, interval):
    """One candle per session in DAILY_SESSIONS_5 — the whole 5-day span fits one chunk (« 2000-day
    cap), so a single request returns candles for every requested date regardless of frm/to."""
    return [
        {"date": dt.datetime(d.year, d.month, d.day, tzinfo=IST), "open": 10.0, "high": 12.0,
         "low": 9.0, "close": 50.0 + i, "volume": 100 + i}
        for i, d in enumerate(DAILY_SESSIONS_5)
    ]


async def test_daily_gap_fills_only_missing_sessions_and_never_overwrites(store, clock, conn):
    # 3 of 5 sessions already present — one a bhavcopy cross-check row that a gap fill must not touch.
    store.upsert_bars_1d([
        DailyBar(symbol="RELIANCE", d=DAILY_SESSIONS_5[0], open=Decimal("1"), high=Decimal("1"),
                 low=Decimal("1"), close=Decimal("999"), volume=1, src="bhavcopy"),
        DailyBar(symbol="RELIANCE", d=DAILY_SESSIONS_5[1], open=Decimal("1"), high=Decimal("1"),
                 low=Decimal("1"), close=Decimal("2"), volume=1, src="kite_official"),
        DailyBar(symbol="RELIANCE", d=DAILY_SESSIONS_5[2], open=Decimal("1"), high=Decimal("1"),
                 low=Decimal("1"), close=Decimal("3"), volume=1, src="kite_official"),
    ])
    kite = FakeKite(five_session_day_candles)
    job = _job(store, kite, clock, conn)

    report = await job.daily_gap(["RELIANCE"], DAILY_SESSIONS_5)

    assert len(kite.calls) == 1 and kite.calls[0][3] == "day"
    assert report.bars_written == 2
    assert report.skipped_covered == 0
    assert report.interval == "day"

    rows = store.get_bars_1d("RELIANCE", DAILY_SESSIONS_5[0], DAILY_SESSIONS_5[-1])
    by_d = {r.d: r for r in rows}
    assert by_d[DAILY_SESSIONS_5[0]].close == Decimal("999")     # bhavcopy row UNCHANGED
    assert by_d[DAILY_SESSIONS_5[0]].src == "bhavcopy"
    assert by_d[DAILY_SESSIONS_5[3]].src == "kite_official"      # the two newly-filled rows
    assert by_d[DAILY_SESSIONS_5[4]].src == "kite_official"

    n = conn.execute("SELECT COUNT(*) AS n FROM backfill_checkpoints").fetchone()["n"]
    assert n == 0                                                 # NOT checkpointed


async def test_daily_gap_skips_covered_symbol_with_no_request(store, clock, conn):
    store.upsert_bars_1d([
        DailyBar(symbol="TCS", d=d, open=Decimal("1"), high=Decimal("1"), low=Decimal("1"),
                 close=Decimal("1"), volume=1, src="kite_official")
        for d in DAILY_SESSIONS_5
    ])
    kite = FakeKite(five_session_day_candles)
    job = _job(store, kite, clock, conn)

    report = await job.daily_gap(["TCS"], DAILY_SESSIONS_5)

    assert kite.calls == []                # fully covered — no Kite request at all
    assert report.skipped_covered == 1
    assert report.fetched == []
    assert report.bars_written == 0


async def test_daily_gap_failure_isolated_per_symbol(store, clock, conn):
    kite = FakeKite(five_session_day_candles, fail_on_call={0})   # RELIANCE's fetch fails
    job = _job(store, kite, clock, conn)

    report = await job.daily_gap(["RELIANCE", "TCS"], DAILY_SESSIONS_5)

    assert len(report.failed) == 1 and report.failed[0].symbol == "RELIANCE"
    assert "RuntimeError" in report.failed[0].error
    assert len(report.fetched) == 1 and report.fetched[0].symbol == "TCS"


async def test_daily_gap_aborts_whole_run_on_token_rejection(store, clock, conn):
    kite = _TokenKite()
    job = _job(store, kite, clock, conn)

    report = await job.daily_gap(["RELIANCE", "TCS", "INFY"], DAILY_SESSIONS_5)

    assert len(kite.calls) == 1                              # aborted after the first symbol's fetch
    assert len(report.failed) == 3
    by_symbol = {s.symbol: s.error for s in report.failed}
    assert "TokenException" in by_symbol["RELIANCE"]
    assert by_symbol["TCS"] == "aborted_token_rejected"
    assert by_symbol["INFY"] == "aborted_token_rejected"
    assert report.fetched == []


async def test_daily_gap_empty_sessions_is_a_noop(store, clock, conn):
    kite = FakeKite(five_session_day_candles)
    job = _job(store, kite, clock, conn)

    report = await job.daily_gap(["RELIANCE"], [])

    assert kite.calls == []
    assert report.requested == [] and report.fetched == [] and report.failed == []
    assert report.bars_written == 0 and report.skipped_covered == 0
