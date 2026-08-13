"""BarBuilder (§3.2.3 / §4.4 job 1): hand-computed tick sequences → exact bars.

Covers the pinned rules: pre-open drop + auction open on the 09:15 row (A14), the symmetric
post-close drop (WO-5), volume = Δ(cumulative day volume) (A13), minute+5s-grace Clock-driven
finalization, late-tick corrections incl. the official-bar-untouchable guard (WO-5), the
cumulative-decrease restatement guard (never negative volume), day rollover, and the mid-session
first-tick rule. Time is controlled through an injected mutable Clock time source.
"""

from __future__ import annotations

import datetime as dt
import threading
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


# ---------------------------------------------- WO-5: official bars are untouchable by late ticks
def _finalize_915_self_bar(bb: BarBuilder, now: _Now) -> None:
    """Build + finalize a 09:15 src='self' bar (high 101.00 / low 100.50, volume 700)."""
    feed(bb, now, tick("R", at(9, 15, 1), "101.00", 500))
    feed(bb, now, tick("R", at(9, 15, 30), "100.50", 700))
    now.set(at(9, 16, 5))
    assert len(bb.advance()) == 1


@pytest.mark.parametrize("src", ["kite_official", "gap_backfilled"])
def test_late_tick_never_amends_a_non_self_bar(store, mclock, now, src):
    """A reconciled/backfilled row is CANONICAL (§4.4 job 2) and reconcile never revisits a
    checkpointed day — a stray late tick must leave it byte-intact and say so in corrections_log."""
    bb = BarBuilder(store, mclock)
    _finalize_915_self_bar(bb, now)

    # The nightly reconcile (or warmup gap-fill) makes the official candle canonical for that minute.
    self_bar = store.get_bars_1m("R", at(9, 15), at(9, 16))[0]
    store.insert_bars_1m([self_bar.model_copy(update={
        "src": src, "open": Decimal("100.95"), "high": Decimal("101.20"),
        "low": Decimal("100.40"), "close": Decimal("100.60"), "volume": 812,
    })])
    before = store.get_bars_1m("R", at(9, 15), at(9, 16))[0]

    now.set(at(9, 16, 10))
    bb.on_tick(tick("R", at(9, 15, 59), "150.00", 720))     # a wild late print, way outside the range

    after = store.get_bars_1m("R", at(9, 15), at(9, 16))[0]
    assert after == before                                   # every column identical — nothing rewritten
    assert (after.high, after.low, after.src) == (Decimal("101.20"), Decimal("100.40"), src)

    (corr,) = store.get_corrections(D)                       # the refusal is recorded, not swallowed
    assert corr["amended"] is False
    assert corr["reason"] == "official_bar_untouchable"
    assert corr["value"] == Decimal("150.00") and corr["cumulative_volume"] == 720


def test_late_tick_queued_behind_a_reconcile_write_decides_on_the_current_row(store, mclock, now):
    """TOCTOU pin (WO-5 iii): the amendment's read-decide-write happens in ONE store-lock hold, so a
    reconcile write that lands while the tick thread is queued is seen by the decision — the late
    tick can never act on (or write back) the pre-reconcile snapshot it might have read first."""
    bb = BarBuilder(store, mclock)
    _finalize_915_self_bar(bb, now)
    self_bar = store.get_bars_1m("R", at(9, 15), at(9, 16))[0]
    official = self_bar.model_copy(update={
        "src": "kite_official", "high": Decimal("101.20"), "low": Decimal("100.40"), "volume": 812,
    })

    holding = threading.Event()
    reconcile_done = threading.Event()

    def reconcile() -> None:
        # Hold the single-writer lock the way any in-flight store call does, then write the official
        # candle before releasing — the amendment must not have read anything yet.
        with store._lock:
            holding.set()
            reconcile_done.wait(5)
            store.insert_bars_1m([official])

    worker = threading.Thread(target=reconcile, daemon=True)
    worker.start()
    assert holding.wait(5)

    now.set(at(9, 16, 10))
    late = threading.Thread(target=bb.on_tick, args=(tick("R", at(9, 15, 59), "150.00", 720),))
    late.start()
    reconcile_done.set()                                     # release: the official write lands first
    worker.join(5)
    late.join(5)
    assert not late.is_alive()

    assert store.get_bars_1m("R", at(9, 15), at(9, 16))[0] == official   # official survives intact
    (corr,) = store.get_corrections(D)
    assert corr["amended"] is False and corr["reason"] == "official_bar_untouchable"


# ------------------------------------------------- WO-5: post-close exclusion (symmetric to A14)
def test_post_close_ticks_build_no_bar_and_are_counted(store, mclock, now):
    """Post-15:30 prints sit outside the nightly reconcile's comparison window forever, so they may
    never build a bar. Symmetric to pre-open: dropped from bars, counted, still persisted raw."""
    bb = BarBuilder(store, mclock)
    feed(bb, now, tick("R", at(15, 28, 0), "99.50", 900))        # mid-session first sight: delta 0
    feed(bb, now, tick("R", at(15, 29, 50), "100.00", 1000))     # delta 100
    feed(bb, now, tick("R", at(15, 31, 0), "100.90", 1200))      # post-close — dropped
    feed(bb, now, tick("R", at(16, 5, 0), "99.00", 1300))        # post-close — dropped
    now.set(at(16, 10))
    bb.advance()

    bars = store.get_bars_1m("R", at(15, 0), at(23, 0))
    assert [b.ts_minute for b in bars] == [at(15, 28), at(15, 29)]   # no 15:31 / 16:05 bar exists
    assert bars[-1].close == Decimal("100.00") and bars[-1].volume == 100

    snap = bb.stats_snapshot()
    assert snap["ticks_dropped"] == {"post_close": 2}            # side-channel counter (feed_stats)
    assert bb.stats_snapshot()["ticks_dropped"] == {}            # reset-on-read

    store.flush_ticks()                                          # the exclusion is a BAR rule only
    assert [t.exchange_ts for t in store.get_ticks("R", D)] == [
        at(15, 28, 0), at(15, 29, 50), at(15, 31, 0), at(16, 5, 0)
    ]


def test_session_close_boundary_is_strict_and_constructor_overridable(store, mclock, now):
    """Exactly-at-close is IN (strictly-after rule); the close is overridable for shortened/muhurat
    sessions exactly like session_open."""
    bb = BarBuilder(store, mclock, session_close=dt.time(12, 30))
    feed(bb, now, tick("R", at(12, 29, 30), "99.00", 900))       # in-session baseline (delta 0)
    feed(bb, now, tick("R", at(12, 30, 0), "100.00", 1000))      # exactly at the close: kept
    feed(bb, now, tick("R", at(12, 30, 1), "105.00", 1100))      # one second past: dropped
    now.set(at(12, 31, 10))
    bars = bb.advance()
    assert [b.ts_minute for b in bars] == [at(12, 29), at(12, 30)]
    assert bars[-1].high == Decimal("100.00")                    # the 105.00 print never landed
    assert bars[-1].volume == 100                                # nor its cumulative delta
    assert bb.stats_snapshot()["ticks_dropped"] == {"post_close": 1}


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


# ------------------------------------------------------------------ feed_stats counters (R8)
def test_stats_snapshot_counts_finalized_and_written_then_resets(store, mclock, now):
    """The periodic feed_stats line reads bars_finalized/bars_written from here; reset-on-read."""
    bb = BarBuilder(store, mclock)
    feed(bb, now, tick("R", at(9, 15, 10), "100.00", 1000))
    feed(bb, now, tick("R", at(9, 16, 10), "101.00", 1500))
    now.set(at(9, 17, 5))
    bb.advance()                                  # both 09:15 and 09:16 finalize + write

    snap = bb.stats_snapshot()
    assert snap["bars_finalized"] == 2
    assert snap["bars_written"] == 2
    # reset-on-read (ticks_dropped: bar-exclusion reason -> count, WO-5)
    assert bb.stats_snapshot() == {"bars_finalized": 0, "bars_written": 0, "ticks_dropped": {}}


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
