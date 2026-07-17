"""BarBuilder (§3.2.3 / §4.4 job 1): hand-computed tick sequences → exact bars.

Covers the pinned rules: pre-open drop + auction open on the 09:15 row (A14), volume =
Δ(cumulative day volume) (A13), minute+5s-grace Clock-driven finalization, late-tick corrections,
the cumulative-decrease restatement guard (never negative volume), day rollover, and the
mid-session first-tick rule. Time is controlled through an injected mutable Clock time source.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest

from engine.core.clock import IST, Clock
from engine.core.types import Tick
from engine.marketdata.bar_builder import BAR_1M_TOPIC, BarBuilder
from engine.marketdata.store import MarketStore

D = dt.date(2026, 6, 17)          # a real 2026 trading day (matches conftest FIXED_NOW)


def at(h: int, m: int, s: int = 0, day: dt.date = D) -> dt.datetime:
    return dt.datetime(day.year, day.month, day.day, h, m, s, tzinfo=IST)


class _Now:
    """Mutable time source injected into Clock — tests move time explicitly."""

    def __init__(self, start: dt.datetime) -> None:
        self.value = start

    def __call__(self) -> dt.datetime:
        return self.value

    def set(self, value: dt.datetime) -> None:
        self.value = value


@pytest.fixture
def now() -> _Now:
    return _Now(at(9, 0))


@pytest.fixture
def mclock(now: _Now) -> Clock:
    return Clock(time_source=now)


@pytest.fixture
def store(tmp_path, mclock):
    s = MarketStore(tmp_path / "market.duckdb", tmp_path / "pq", mclock)
    s.open()
    yield s
    s.close()


def tick(sym: str, ts: dt.datetime, ltp: str, cum: int, **kw) -> Tick:
    return Tick(
        instrument_token=1, tradingsymbol=sym, ltp=Decimal(ltp), volume_traded=cum,
        exchange_ts=ts, **kw,
    )


def feed(bb: BarBuilder, now: _Now, t: Tick) -> None:
    """Deliver a tick with the wall clock at the tick's own timestamp (live arrival)."""
    now.set(t.exchange_ts)
    bb.on_tick(t)


# ------------------------------------------------------------------ A14: pre-open + auction open
def test_preopen_dropped_auction_open_stamped_grace_finalization(store, mclock, now):
    bb = BarBuilder(store, mclock)
    feed(bb, now, tick("R", at(9, 6, 59), "100.10", 0))     # pre-open — dropped from bars
    feed(bb, now, tick("R", at(9, 14, 59), "100.25", 0))    # LAST pre-open print = auction open
    feed(bb, now, tick("R", at(9, 15, 1), "101.00", 500))   # session: full cum → 09:15 bar (A13)
    feed(bb, now, tick("R", at(9, 15, 30), "100.50", 700))

    now.set(at(9, 16, 4))                                   # grace: 09:15 bar closes at 09:16:05
    assert bb.advance() == []
    now.set(at(9, 16, 5))
    bars = bb.advance()
    assert len(bars) == 1
    bar = bars[0]
    assert bar.ts_minute == at(9, 15)
    assert bar.open == Decimal("101.00")                    # first in-session print, NOT the auction
    assert bar.high == Decimal("101.00")
    assert bar.low == Decimal("100.50")
    assert bar.close == Decimal("100.50")
    assert bar.volume == 700                                # 500 (incl. auction volume) + 200
    assert bar.auction_open == Decimal("100.25")            # stamped on the 09:15 row only (A14)
    assert bar.src == "self"

    stored = store.get_bars_1m("R", at(9, 15), at(9, 16))   # batch-written via MarketStore
    assert len(stored) == 1
    assert stored[0].volume == 700
    assert stored[0].auction_open == Decimal("100.25")
    # Zero bar contamination from pre-open ticks (A14).
    assert store.get_bars_1m("R", at(9, 0), at(9, 15)) == []


def test_bar_published_on_bus(store, mclock, now, bus):
    events = []

    async def handler(bar):
        events.append(bar)

    bus.subscribe(BAR_1M_TOPIC, handler)
    bb = BarBuilder(store, mclock, bus)
    feed(bb, now, tick("R", at(9, 15, 10), "50.00", 100))
    now.set(at(9, 16, 5))
    bb.advance()
    assert len(events) == 1
    assert events[0].ts_minute == at(9, 15)
    assert events[0].volume == 100


# ------------------------------------------------------------------ A13: cumulative-volume deltas
def test_cumulative_volume_delta_across_minutes(store, mclock, now):
    bb = BarBuilder(store, mclock)
    feed(bb, now, tick("R", at(9, 15, 10), "100.00", 1000))
    feed(bb, now, tick("R", at(9, 16, 10), "101.00", 1500))
    feed(bb, now, tick("R", at(9, 16, 40), "102.00", 1800))
    feed(bb, now, tick("R", at(9, 17, 10), "102.00", 1800))  # no trade: delta 0
    now.set(at(9, 18, 5))
    bb.advance()                                  # earlier bars finalized on the fly by on_tick
    bars = store.get_bars_1m("R", at(9, 15), at(9, 18))
    assert [b.ts_minute for b in bars] == [at(9, 15), at(9, 16), at(9, 17)]
    assert [b.volume for b in bars] == [1000, 800, 0]
    assert bars[1].open == Decimal("101.00")
    assert bars[1].close == Decimal("102.00")
    assert bars[1].high == Decimal("102.00")


def test_restatement_guard_never_negative_volume(store, mclock, now):
    bb = BarBuilder(store, mclock)
    feed(bb, now, tick("R", at(9, 15, 5), "100.00", 1000))
    feed(bb, now, tick("R", at(9, 15, 20), "99.00", 800))    # cumulative DECREASE — restatement
    feed(bb, now, tick("R", at(9, 15, 40), "99.50", 1200))   # recovers past the high-water mark
    now.set(at(9, 16, 5))
    (bar,) = bb.advance()
    assert bar.volume == 1200                # 1000 + 0 (guarded) + 200 (1200-1000 high-water)
    assert bar.volume >= 0
    corrections = store.get_corrections(D)
    assert len(corrections) == 1
    row = corrections[0]
    assert row["symbol"] == "R"
    assert row["minute"] == at(9, 15)
    assert row["cumulative_volume"] == 800
    assert row["value"] == Decimal("99.00")
    assert row["amended"] is False


def test_first_tick_mid_session_contributes_zero(store, mclock, now):
    """Engine started mid-day: the unseen span's delta is unknowable — 0, warmup_gap owns the fill."""
    bb = BarBuilder(store, mclock)
    feed(bb, now, tick("R", at(11, 3, 10), "200.00", 50_000))
    feed(bb, now, tick("R", at(11, 3, 20), "200.50", 50_100))
    now.set(at(11, 4, 5))
    (bar,) = bb.advance()
    assert bar.ts_minute == at(11, 3)
    assert bar.volume == 100                 # only the observed delta, never the day's 50k
    assert bar.auction_open is None


# ------------------------------------------------------------------ late ticks past grace
def test_late_tick_goes_to_corrections_and_amends_range(store, mclock, now):
    bb = BarBuilder(store, mclock)
    feed(bb, now, tick("R", at(9, 15, 1), "101.00", 500))
    feed(bb, now, tick("R", at(9, 15, 30), "100.50", 700))
    now.set(at(9, 16, 5))
    assert len(bb.advance()) == 1            # 09:15 finalized

    # A late print OUTSIDE the finalized range: bar high amended, correction amended=True.
    now.set(at(9, 16, 10))
    bb.on_tick(tick("R", at(9, 15, 59), "102.00", 720))
    stored = store.get_bars_1m("R", at(9, 15), at(9, 16))[0]
    assert stored.high == Decimal("102.00")
    assert stored.low == Decimal("100.50")
    assert stored.close == Decimal("100.50")     # close never restated post-finalize
    assert stored.volume == 700                  # volume never restated post-finalize

    # A late print INSIDE the range: logged only, bar untouched.
    now.set(at(9, 16, 12))
    bb.on_tick(tick("R", at(9, 15, 45), "100.80", 721))
    again = store.get_bars_1m("R", at(9, 15), at(9, 16))[0]
    assert again.high == Decimal("102.00") and again.volume == 700

    corrections = store.get_corrections(D)          # ordered by tick_ts, not insertion order
    assert len(corrections) == 2
    by_tick_ts = {row["tick_ts"]: row for row in corrections}
    outside = by_tick_ts[at(9, 15, 59)]             # the range-amending late print
    inside = by_tick_ts[at(9, 15, 45)]              # the inside-range late print
    assert outside["amended"] is True
    assert outside["value"] == Decimal("102.00")
    assert outside["cumulative_volume"] == 720
    assert inside["amended"] is False

    # The cumulative chain ignored the late ticks: next live minute deltas off the 700 baseline.
    feed(bb, now, tick("R", at(9, 16, 20), "101.00", 900))
    now.set(at(9, 17, 5))
    (bar_916,) = bb.advance()
    assert bar_916.volume == 200


# ------------------------------------------------------------------ day rollover
def test_day_rollover_resets_state_and_flushes_open_bars(store, mclock, now):
    d2 = dt.date(2026, 6, 18)
    bb = BarBuilder(store, mclock)
    feed(bb, now, tick("R", at(9, 15, 10), "100.00", 1000))          # day-1 bar left open
    # Next day's first tick: prior-day open bar force-finalized; cum baseline resets.
    feed(bb, now, tick("R", at(9, 15, 3, day=d2), "105.00", 400))
    day1 = store.get_bars_1m("R", at(9, 15), at(9, 16))
    assert len(day1) == 1 and day1[0].volume == 1000                  # flushed, not lost

    now.set(at(9, 16, 5, day=d2))
    (bar,) = bb.advance()
    assert bar.ts_minute == at(9, 15, day=d2)
    assert bar.volume == 400                  # fresh day: full cum belongs to the 09:15 bar
    assert bar.auction_open is None           # no pre-open print seen on day 2


# ------------------------------------------------------------------ raw tick persistence (§4.3)
def test_raw_ticks_buffered_including_preopen(store, mclock, now):
    bb = BarBuilder(store, mclock)
    pre = tick("R", at(9, 10, 0), "99.90", 0)
    live = tick("R", at(9, 15, 30), "100.00", 10)
    feed(bb, now, pre)
    feed(bb, now, live)
    store.flush_ticks()
    persisted = store.get_ticks("R", D)
    assert [t.exchange_ts for t in persisted] == [pre.exchange_ts, live.exchange_ts]
