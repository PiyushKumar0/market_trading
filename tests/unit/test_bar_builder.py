"""BarBuilder: hand-computed tick sequences → exact bars.

Covers the pinned rules: pre-open drop + auction open on the 09:15 row, the symmetric
post-close drop, volume = Δ(cumulative day volume), minute+5s-grace Clock-driven
finalization, late-tick corrections incl. the official-bar-untouchable guard, the
cumulative-decrease restatement guard (never negative volume), day rollover, and the mid-session
first-tick rule. Time is controlled through an injected mutable Clock time source.

Late-tick cost tests: an in-range late tick must touch NO store method at all, an out-of-range one
must still amend through the CAS path, the log must be deduped per (symbol, minute) behind a
per-wall-minute aggregate, and the processing-lag watchdog must fire once per episode and recover.

Reconnect grace: a feed-health transition into WARMING/HEALTHY silences the lag watchdog for
LAG_RECONNECT_GRACE_S so Kite's connect-time snapshot (one last-trade-stamped tick per subscribed
instrument) cannot flap the single global episode — without disarming the watchdog for a genuine
lag once the grace lapses.
"""

from __future__ import annotations

import datetime as dt
import logging
import threading
from decimal import Decimal

import pytest

from engine.broker.ticker_supervisor import FeedHealth
from engine.core.clock import IST, Clock
from engine.core.types import Tick
from engine.marketdata.bar_builder import (
    BAR_1M_TOPIC,
    LAG_LOG_INTERVAL_S,
    LAG_RECONNECT_GRACE_S,
    LAG_THRESHOLD_S,
    RECENT_BARS_PER_SYMBOL,
    BarBuilder,
)
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


# ------------------------------------------------------------------ pre-open + auction open
def test_preopen_dropped_auction_open_stamped_grace_finalization(store, mclock, now):
    bb = BarBuilder(store, mclock)
    feed(bb, now, tick("R", at(9, 6, 59), "100.10", 0))     # pre-open — dropped from bars
    feed(bb, now, tick("R", at(9, 14, 59), "100.25", 0))    # LAST pre-open print = auction open
    feed(bb, now, tick("R", at(9, 15, 1), "101.00", 500))   # session: full cum → 09:15 bar
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
    assert bar.auction_open == Decimal("100.25")            # stamped on the 09:15 row only
    assert bar.src == "self"

    stored = store.get_bars_1m("R", at(9, 15), at(9, 16))   # batch-written via MarketStore
    assert len(stored) == 1
    assert stored[0].volume == 700
    assert stored[0].auction_open == Decimal("100.25")
    # Zero bar contamination from pre-open ticks.
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


# ------------------------------------------------------------------ cumulative-volume deltas
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

    # A late print INSIDE the (now amended) range: bar untouched AND no correction row ("nothing to
    # widen" is free, not worth a DuckDB INSERT).
    now.set(at(9, 16, 12))
    bb.on_tick(tick("R", at(9, 15, 45), "100.80", 721))
    again = store.get_bars_1m("R", at(9, 15), at(9, 16))[0]
    assert again.high == Decimal("102.00") and again.volume == 700

    corrections = store.get_corrections(D)          # ordered by tick_ts, not insertion order
    assert len(corrections) == 1                    # only the amending print is recorded
    outside = corrections[0]                        # the range-amending late print
    assert outside["tick_ts"] == at(9, 15, 59)
    assert outside["amended"] is True
    assert outside["value"] == Decimal("102.00")
    assert outside["cumulative_volume"] == 720

    # The cumulative chain ignored the late ticks: next live minute deltas off the 700 baseline.
    feed(bb, now, tick("R", at(9, 16, 20), "101.00", 900))
    now.set(at(9, 17, 5))
    (bar_916,) = bb.advance()
    assert bar_916.volume == 200


# ---------------------------------------------- official bars are untouchable by late ticks
def _finalize_915_self_bar(bb: BarBuilder, now: _Now) -> None:
    """Build + finalize a 09:15 src='self' bar (high 101.00 / low 100.50, volume 700)."""
    feed(bb, now, tick("R", at(9, 15, 1), "101.00", 500))
    feed(bb, now, tick("R", at(9, 15, 30), "100.50", 700))
    now.set(at(9, 16, 5))
    assert len(bb.advance()) == 1


@pytest.mark.parametrize("src", ["kite_official", "gap_backfilled"])
def test_late_tick_never_amends_a_non_self_bar(store, mclock, now, src):
    """A reconciled/backfilled row is CANONICAL (reconcile never revisits a checkpointed day): a late
    tick leaves it byte-intact and says so in corrections_log."""
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
    """TOCTOU pin: the amendment's read-decide-write is ONE store-lock hold, so a reconcile write that
    lands while the tick thread is queued is seen by the decision, never the pre-reconcile snapshot."""
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


# ------------------------------------------------- post-close exclusion (symmetric to the pre-open drop)
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


# ------------------------------------------------------------------ feed_stats counters
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
    assert (snap["late_ticks"], snap["late_store_calls"]) == (0, 0)   # late-tick pressure
    # reset-on-read (ticks_dropped: bar-exclusion reason -> count)
    assert bb.stats_snapshot() == {
        "bars_finalized": 0, "bars_written": 0, "ticks_dropped": {},
        "late_ticks": 0, "late_store_calls": 0,
    }


# ------------------------------------------------------------------ raw tick persistence
def test_raw_ticks_buffered_including_preopen(store, mclock, now):
    bb = BarBuilder(store, mclock)
    pre = tick("R", at(9, 10, 0), "99.90", 0)
    live = tick("R", at(9, 15, 30), "100.00", 10)
    feed(bb, now, pre)
    feed(bb, now, live)
    store.flush_ticks()
    persisted = store.get_ticks("R", D)
    assert [t.exchange_ts for t in persisted] == [pre.exchange_ts, live.exchange_ts]


# ==================================================================== the late-tick cost
# Once processing slips past the finalize grace EVERY tick takes the late path; if each paid store
# round-trips (amend CAS + corrections INSERT + an INFO line) to discover there was nothing to do,
# throughput would fall below real time and the lag would grow without bound.


class _SpyStore:
    """Delegating :class:`MarketStore` proxy that records which store methods a path actually calls.

    A spy, not a stub: the store still does the real DuckDB work, so a test asserts BOTH the call
    ledger (the cost) and the persisted outcome (the correctness).
    """

    def __init__(self, inner: MarketStore) -> None:
        self._inner = inner
        self.calls: list[str] = []

    def __getattr__(self, name: str):
        attr = getattr(self._inner, name)
        if not callable(attr):
            return attr

        def _record(*args, **kwargs):
            self.calls.append(name)
            return attr(*args, **kwargs)

        return _record


def late_events(caplog, event: str) -> list:
    return [r for r in caplog.records if r.getMessage() == event]


def test_in_range_late_tick_touches_no_store_method_at_all(store, mclock, now):
    """A late print already inside the finalized bar's range has nothing to widen or correct: ZERO
    store calls — no CAS read, no corrections INSERT."""
    spy = _SpyStore(store)
    bb = BarBuilder(spy, mclock, persist_raw_ticks=False)
    _finalize_915_self_bar(bb, now)              # 09:15 src='self', high 101.00 / low 100.50
    spy.calls.clear()

    now.set(at(9, 16, 10))
    for i, px in enumerate(("100.50", "100.75", "101.00", "100.60")):   # incl. both range endpoints
        bb.on_tick(tick("R", at(9, 15, 40 + i), px, 700 + i))

    assert spy.calls == []
    assert store.get_corrections(D) == []
    stored = store.get_bars_1m("R", at(9, 15), at(9, 16))[0]
    assert (stored.high, stored.low) == (Decimal("101.00"), Decimal("100.50"))
    snap = bb.stats_snapshot()
    assert (snap["late_ticks"], snap["late_store_calls"]) == (4, 0)


def test_out_of_range_late_tick_still_amends_through_the_cas_path(store, mclock, now):
    """An amending late tick costs one ``amend_bar_1m_extremes`` (the single-lock CAS) plus its
    ``corrections_log`` row; after it lands the print is free (the REMEMBERED range widens in the same step)."""
    spy = _SpyStore(store)
    bb = BarBuilder(spy, mclock, persist_raw_ticks=False)
    _finalize_915_self_bar(bb, now)
    spy.calls.clear()

    now.set(at(9, 16, 10))
    bb.on_tick(tick("R", at(9, 15, 59), "102.00", 720))
    assert spy.calls == ["amend_bar_1m_extremes", "append_correction"]
    stored = store.get_bars_1m("R", at(9, 15), at(9, 16))[0]
    assert (stored.high, stored.low) == (Decimal("102.00"), Decimal("100.50"))
    assert stored.close == Decimal("100.50") and stored.volume == 700   # never restated post-finalize
    (corr,) = store.get_corrections(D)
    assert corr["amended"] is True and corr["value"] == Decimal("102.00")

    # Memory now mirrors the widened row: the same print costs nothing the second time.
    spy.calls.clear()
    bb.on_tick(tick("R", at(9, 15, 58), "102.00", 721))
    assert spy.calls == []
    assert len(store.get_corrections(D)) == 1
    assert bb.stats_snapshot()["late_store_calls"] == 1


def test_recent_bar_window_is_bounded_so_an_old_minute_falls_back_to_the_store(store, mclock, now):
    """The in-memory window is small (N=5); a minute aged out of it is NOT assumed in-range but goes to the store."""
    spy = _SpyStore(store)
    bb = BarBuilder(spy, mclock, persist_raw_ticks=False)
    minutes = range(15, 15 + RECENT_BARS_PER_SYMBOL + 2)
    for i, m in enumerate(minutes):
        feed(bb, now, tick("R", at(9, m, 10), f"{100 + i}.00", 1000 * (i + 1)))
    oldest, newest = 15, 15 + RECENT_BARS_PER_SYMBOL   # the last minute fed is still OPEN, not late
    assert bb._finalized_through["R"] == at(9, newest)

    now.set(at(9, newest, 30))
    spy.calls.clear()
    bb.on_tick(tick("R", at(9, newest, 20), f"{100 + RECENT_BARS_PER_SYMBOL}.00", 99_000))
    assert spy.calls == []                                  # newest minute: remembered, so free

    spy.calls.clear()
    bb.on_tick(tick("R", at(9, oldest, 20), "100.00", 99_001))   # evicted minute: back to the store
    assert spy.calls == ["amend_bar_1m_extremes", "append_correction"]
    (corr,) = store.get_corrections(D)
    assert corr["minute"] == at(9, oldest) and corr["amended"] is False   # its own price: in range


def test_late_tick_logs_at_most_one_line_per_symbol_minute(store, mclock, now, caplog):
    """A per-tick line costs a structlog render + a synchronous file write: one line per (symbol,
    minute), with volume carried by the aggregate."""
    bb = BarBuilder(store, mclock, persist_raw_ticks=False)
    _finalize_915_self_bar(bb, now)
    with caplog.at_level(logging.INFO, logger="engine.marketdata.bar_builder"):
        now.set(at(9, 16, 10))
        for i in range(50):
            bb.on_tick(tick("R", at(9, 15, 10 + (i % 40)), "100.75", 700 + i))

        lines = late_events(caplog, "late_tick_past_grace")
        assert len(lines) == 1                        # 50 late ticks, ONE line
        assert (lines[0].symbol, lines[0].outcome) == ("R", "in_range")
        assert lines[0].minute == at(9, 15).isoformat()

        # A different (symbol, minute) gets its own line — the budget is per key, not global.
        feed(bb, now, tick("Z", at(9, 15, 5), "50.00", 10))
        now.set(at(9, 16, 20))
        bb.advance()
        for i in range(20):
            bb.on_tick(tick("Z", at(9, 15, 6), "50.00", 11 + i))
        keys = {(r.symbol, r.minute) for r in late_events(caplog, "late_tick_past_grace")}
    assert keys == {("R", at(9, 15).isoformat()), ("Z", at(9, 15).isoformat())}


def test_late_ticks_summary_aggregates_the_wall_minute(store, mclock, now, caplog):
    """The per-minute aggregate carries how many late ticks, how many symbols, how far behind, and how
    much reached DuckDB."""
    bb = BarBuilder(store, mclock, persist_raw_ticks=False)
    _finalize_915_self_bar(bb, now)
    with caplog.at_level(logging.INFO, logger="engine.marketdata.bar_builder"):
        now.set(at(9, 16, 10))
        for i in range(30):
            bb.on_tick(tick("R", at(9, 15, 20), "100.75", 700 + i))
        assert late_events(caplog, "late_ticks_summary") == []   # still inside the same wall minute

        now.set(at(9, 17, 1))                                    # wall minute turns over
        bb.advance()
        (summary,) = late_events(caplog, "late_ticks_summary")

    assert summary.late_ticks == 30
    assert summary.symbols == 1
    assert (summary.store_calls, summary.amended) == (0, 0)
    assert summary.max_lag_s == pytest.approx(50.0)              # 09:16:10 − 09:15:20
    assert summary.window == at(9, 16).isoformat()


# ------------------------------------------------------------- processing-lag watchdog
async def test_stale_snapshot_echoes_never_page_the_lag_watchdog(store, mclock, now, caplog):
    """An after-hours reconnect replays snapshot frames with old stamps, so 'now − ts' reads as a
    backlog on a stream with none. Guard (i): a previous-day stamp is a snapshot echo, never lag.
    Guard (ii): outside session hours the alarm neither fires nor keeps an episode alive."""
    sent = []

    async def notify(msg) -> None:
        sent.append(msg)

    bb = BarBuilder(store, mclock, persist_raw_ticks=False, notify=notify)

    with caplog.at_level(logging.INFO, logger="engine.marketdata.bar_builder"):
        # (i) previous-day stamp at 23:21.
        wall = at(23, 21, 0)
        now.set(wall)
        await bb.on_tick_event(tick("R", wall - dt.timedelta(hours=5, minutes=46), "100.00", 1000))
        assert late_events(caplog, "tick_processing_lagging") == []
        assert sent == []

        # (ii) SAME-day stale stamp but outside session hours (post-close) — still no page.
        await bb.on_tick_event(tick("R", wall - dt.timedelta(minutes=30), "100.10", 1100))
        assert late_events(caplog, "tick_processing_lagging") == []
        assert sent == []

        # An episode opened in-session is reset quietly by an out-of-session tick, never paged.
        in_session = at(15, 40, 0)
        now.set(in_session)
        await bb.on_tick_event(
            tick("R", in_session - dt.timedelta(seconds=LAG_THRESHOLD_S + 60), "100.20", 1200)
        )
        assert len(sent) == 1                       # genuine in-session episode still pages
        post_close = at(15, 50, 0)
        now.set(post_close)
        await bb.on_tick_event(
            tick("R", post_close - dt.timedelta(seconds=LAG_THRESHOLD_S + 60), "100.30", 1300)
        )
        assert len(late_events(caplog, "tick_lag_watch_suspended_out_of_session")) == 1
        assert len(sent) == 1                       # no second page from the reset


async def test_lag_watchdog_window_follows_overridden_session_times(store, mclock, now, caplog):
    """The lag watch window follows the constructor's session_open/session_close rather than the
    regular 09:15-15:45 constants: an evening muhurat session must be watched, and a session ending
    well before 15:45 must not stay "watched" past its own close."""
    sent = []

    async def notify(msg) -> None:
        sent.append(msg)

    bb = BarBuilder(
        store, mclock, persist_raw_ticks=False, notify=notify,
        session_open=dt.time(18, 0), session_close=dt.time(19, 0),
    )

    with caplog.at_level(logging.INFO, logger="engine.marketdata.bar_builder"):
        # Inside the muhurat window but well outside the regular-hours 09:15-15:45 constants: a
        # genuine lag here must still page.
        wall = at(19, 10, 0)
        now.set(wall)
        await bb.on_tick_event(
            tick("R", wall - dt.timedelta(seconds=LAG_THRESHOLD_S + 30), "100.00", 1000)
        )
        assert len(late_events(caplog, "tick_processing_lagging")) == 1
        assert len(sent) == 1

        # Past session_close (19:00) + the 15-minute buffer (i.e. after 19:15): the window has
        # closed, so the open episode is suspended quietly rather than left watched through 15:45.
        after_buffer = at(19, 16, 0)
        now.set(after_buffer)
        await bb.on_tick_event(
            tick("R", after_buffer - dt.timedelta(seconds=LAG_THRESHOLD_S + 30), "100.10", 1100)
        )
        assert len(late_events(caplog, "tick_lag_watch_suspended_out_of_session")) == 1
        assert len(sent) == 1                     # no second page from the reset


async def test_lag_watchdog_fires_once_per_episode_paces_its_log_and_recovers(store, mclock, now, caplog):
    """ERROR at :data:`LAG_THRESHOLD_S`, re-logged no more than every :data:`LAG_LOG_INTERVAL_S`,
    ONE owner alert per episode, and an INFO line when it clears."""
    sent = []

    async def notify(msg) -> None:
        sent.append(msg)

    bb = BarBuilder(store, mclock, persist_raw_ticks=False, notify=notify)

    async def at_lag(wall: dt.datetime, lag_s: int, px: str, cum: int) -> None:
        now.set(wall)
        await bb.on_tick_event(tick("R", wall - dt.timedelta(seconds=lag_s), px, cum))

    with caplog.at_level(logging.INFO, logger="engine.marketdata.bar_builder"):
        await at_lag(at(10, 0, 0), 0, "100.00", 1000)                       # healthy
        assert late_events(caplog, "tick_processing_lagging") == []
        assert sent == []

        await at_lag(at(10, 5, 0), LAG_THRESHOLD_S + 30, "100.10", 1100)    # episode 1 opens
        await at_lag(at(10, 5, 30), LAG_THRESHOLD_S + 30, "100.20", 1200)   # inside the log interval
        assert len(late_events(caplog, "tick_processing_lagging")) == 1
        assert len(sent) == 1                                               # ONE page per episode

        # Past the pacing interval: a second ERROR, still no second page.
        await at_lag(at(10, 5, 0) + dt.timedelta(seconds=LAG_LOG_INTERVAL_S + 5),
                     LAG_THRESHOLD_S + 30, "100.30", 1300)
        assert len(late_events(caplog, "tick_processing_lagging")) == 2
        assert len(sent) == 1

        await at_lag(at(10, 12, 0), 0, "100.40", 1400)                      # caught up
        recovered = late_events(caplog, "tick_processing_recovered")
        assert len(recovered) == 1 and recovered[0].levelname == "INFO"

        await at_lag(at(10, 20, 0), LAG_THRESHOLD_S + 60, "100.50", 1500)   # episode 2: pages again
        errors = late_events(caplog, "tick_processing_lagging")

    assert len(errors) == 3
    assert all(r.levelname == "ERROR" for r in errors)
    assert [r.lag_s for r in errors] == [150.0, 150.0, 180.0]
    assert len(sent) == 2
    assert {m.severity for m in sent} == {"warning"}
    assert [m.data["lag_s"] for m in sent] == [150.0, 180.0]


async def test_lag_watchdog_without_a_notify_sink_is_log_only(store, mclock, now, caplog):
    """``notify=None`` (bare/offline contexts) must still log — and must never raise."""
    bb = BarBuilder(store, mclock, persist_raw_ticks=False)
    with caplog.at_level(logging.INFO, logger="engine.marketdata.bar_builder"):
        now.set(at(10, 5, 0))
        await bb.on_tick_event(tick("R", at(10, 2, 0), "100.00", 1000))
    assert len(late_events(caplog, "tick_processing_lagging")) == 1


# =================================================== reconnect-snapshot grace
# On kws.connect Kite delivers ONE snapshot tick per subscribed instrument carrying that instrument's
# LAST-TRADE exchange_timestamp, interleaved with near-fresh ticks. The watchdog keeps ONE global
# episode, so each stale tick would re-open it and the next fresh tick close it (ERROR/recovered
# pairs and owner pages). The out-of-session guards cannot catch it (same day; 15:30-15:45 is
# deliberately watched via _LAG_WATCH_END_BUFFER) and the wire has no snapshot marker, so the grace
# is driven by the supervisor's feed.health transitions instead.


def _health(state: str) -> FeedHealth:
    """The payload ``TickerSupervisor._publish_health`` publishes on ``feed.health``: a real
    :class:`FeedHealth` (not a stub, so a field rename breaks this test) whose ``state`` is the
    transition the handler reads."""
    return FeedHealth(last_tick_age_s=0.4, heartbeat_age_s=0.9, last_frame_age_s=0.4, state=state)


async def test_reconnect_snapshot_storm_never_pages_during_the_grace(store, mclock, now, caplog):
    """Grace armed by the HEALTHY transition at 15:40:02, then 14 interleaved snapshot/fresh frames:
    zero ERROR lines, zero recoveries, zero pages."""
    sent = []

    async def notify(msg) -> None:
        sent.append(msg)

    bb = BarBuilder(store, mclock, persist_raw_ticks=False, notify=notify)
    boot = at(15, 40, 2)

    with caplog.at_level(logging.INFO, logger="engine.marketdata.bar_builder"):
        now.set(boot)
        await bb.on_feed_health(_health("HEALTHY"))
        armed = late_events(caplog, "tick_lag_watch_grace_armed")
        assert len(armed) == 1 and armed[0].state == "HEALTHY"

        # 14 frames inside 83 ms, alternating snapshot echo (last trade ~15:31) and near-fresh (26 s),
        # each for a different instrument.
        for i in range(14):
            wall = boot + dt.timedelta(milliseconds=6 * i)
            now.set(wall)
            stamp = at(15, 31, 10 + i) if i % 2 == 0 else wall - dt.timedelta(seconds=26)
            await bb.on_tick_event(tick(f"S{i}", stamp, "100.00", 1000 + i))

    assert late_events(caplog, "tick_processing_lagging") == []
    assert late_events(caplog, "tick_processing_recovered") == []
    assert sent == []
    assert bb._lagging is False


async def test_the_watchdog_is_live_again_once_the_grace_lapses(store, mclock, now, caplog):
    """The grace is a delay, not a disable: a genuine 130 s lag one second past
    :data:`LAG_RECONNECT_GRACE_S` opens an episode and pages exactly once."""
    sent = []

    async def notify(msg) -> None:
        sent.append(msg)

    bb = BarBuilder(store, mclock, persist_raw_ticks=False, notify=notify)
    boot = at(15, 40, 2)

    with caplog.at_level(logging.INFO, logger="engine.marketdata.bar_builder"):
        now.set(boot)
        await bb.on_feed_health(_health("HEALTHY"))

        now.set(boot + dt.timedelta(seconds=LAG_RECONNECT_GRACE_S - 2))   # still inside the grace
        await bb.on_tick_event(tick("A", at(15, 31, 0), "100.00", 1000))
        assert late_events(caplog, "tick_processing_lagging") == []
        assert sent == []

        wall = boot + dt.timedelta(seconds=LAG_RECONNECT_GRACE_S + 1)     # grace lapsed
        now.set(wall)
        await bb.on_tick_event(tick("B", wall - dt.timedelta(seconds=130), "100.10", 1100))

    errors = late_events(caplog, "tick_processing_lagging")
    assert len(errors) == 1 and errors[0].lag_s == 130.0
    assert len(sent) == 1


async def test_a_warming_transition_mid_session_arms_the_grace(store, mclock, now, caplog):
    """A mid-session respawn publishes WARMING before it publishes HEALTHY, and its snapshot is
    the same shape — so WARMING arms the grace too, and lapses on its own 30 s later."""
    sent = []

    async def notify(msg) -> None:
        sent.append(msg)

    bb = BarBuilder(store, mclock, persist_raw_ticks=False, notify=notify)

    with caplog.at_level(logging.INFO, logger="engine.marketdata.bar_builder"):
        now.set(at(11, 47, 0))
        await bb.on_feed_health(_health("WARMING"))
        armed = late_events(caplog, "tick_lag_watch_grace_armed")
        assert len(armed) == 1 and armed[0].state == "WARMING"

        now.set(at(11, 47, 20))                                  # inside the grace
        await bb.on_tick_event(tick("A", at(11, 40, 0), "100.00", 1000))
        assert late_events(caplog, "tick_processing_lagging") == []
        assert sent == []

        now.set(at(11, 47, 31))                                  # 29 s later: grace lapsed
        await bb.on_tick_event(tick("B", at(11, 40, 0), "100.10", 1100))

    assert len(late_events(caplog, "tick_processing_lagging")) == 1
    assert len(sent) == 1


async def test_arming_closes_an_open_episode_quietly(store, mclock, now, caplog):
    """A reconnect while an episode is already open closes it with the grace reason — not with
    ``tick_processing_recovered``, which would claim a recovery nothing observed."""
    sent = []

    async def notify(msg) -> None:
        sent.append(msg)

    bb = BarBuilder(store, mclock, persist_raw_ticks=False, notify=notify)

    with caplog.at_level(logging.INFO, logger="engine.marketdata.bar_builder"):
        wall = at(11, 0, 0)
        now.set(wall)
        await bb.on_tick_event(
            tick("R", wall - dt.timedelta(seconds=LAG_THRESHOLD_S + 30), "100.00", 1000)
        )
        assert len(late_events(caplog, "tick_processing_lagging")) == 1
        assert len(sent) == 1
        assert bb._lagging is True

        now.set(at(11, 0, 5))
        await bb.on_feed_health(_health("HEALTHY"))

    closed = late_events(caplog, "tick_lag_watch_reconnect_grace")
    assert len(closed) == 1 and closed[0].levelname == "INFO"
    assert late_events(caplog, "tick_processing_recovered") == []
    assert bb._lagging is False
    assert len(sent) == 1                                   # the closed episode sends no new page


@pytest.mark.parametrize("state", ["STOPPED", "DEGRADED", "STALE"])
async def test_a_non_arming_feed_state_leaves_the_watchdog_live(store, mclock, now, caplog, state):
    """Only WARMING/HEALTHY arm; the other states of the closed ``FeedHealth.state`` set are ignored,
    so a DEGRADED or STALE feed cannot silence the lag alarm."""
    sent = []

    async def notify(msg) -> None:
        sent.append(msg)

    bb = BarBuilder(store, mclock, persist_raw_ticks=False, notify=notify)

    with caplog.at_level(logging.INFO, logger="engine.marketdata.bar_builder"):
        now.set(at(11, 0, 0))
        await bb.on_feed_health(_health(state))
        assert late_events(caplog, "tick_lag_watch_grace_armed") == []

        wall = at(11, 0, 10)
        now.set(wall)
        await bb.on_tick_event(
            tick("R", wall - dt.timedelta(seconds=LAG_THRESHOLD_S + 30), "100.00", 1000)
        )

    assert len(late_events(caplog, "tick_processing_lagging")) == 1
    assert len(sent) == 1


async def test_an_in_child_reconnect_edge_arms_the_grace_mid_session(store, mclock, now, caplog):
    """KiteTicker reconnects INSIDE the child while it keeps heartbeating, so the supervisor's state
    never transitions; it publishes a HEALTHY ``FeedHealth`` on the reconnect edge
    (ticker_supervisor.py `_handle_frame`, heartbeat branch). At the BarBuilder end a mid-session
    HEALTHY event arms the grace and the snapshot/fresh burst that follows pages nothing."""
    sent = []

    async def notify(msg) -> None:
        sent.append(msg)

    bb = BarBuilder(store, mclock, persist_raw_ticks=False, notify=notify)
    edge = at(11, 47, 0)

    with caplog.at_level(logging.INFO, logger="engine.marketdata.bar_builder"):
        now.set(edge)
        await bb.on_feed_health(_health("HEALTHY"))          # what the ws_connected edge publishes
        armed = late_events(caplog, "tick_lag_watch_grace_armed")
        assert len(armed) == 1 and armed[0].state == "HEALTHY"

        # Re-subscribe + FULL mode ⇒ Kite re-sends the connect-time snapshot: one last-trade-stamped
        # tick per instrument (here ~11:31, well past LAG_THRESHOLD_S) interleaved with fresh ticks.
        for i in range(14):
            wall = edge + dt.timedelta(milliseconds=6 * i)
            now.set(wall)
            stamp = at(11, 31, 10 + i) if i % 2 == 0 else wall - dt.timedelta(seconds=2)
            await bb.on_tick_event(tick(f"S{i}", stamp, "100.00", 1000 + i))

    assert late_events(caplog, "tick_processing_lagging") == []
    assert late_events(caplog, "tick_processing_recovered") == []
    assert sent == []
    assert bb._lagging is False


def test_a_stale_minute_placeholder_never_evicts_a_real_cached_bar(store, mclock, now):
    """ranged=False placeholders share the 5-entry LRU with real finalized bars: a reconnect replaying
    5+ distinct stale minutes must not wipe the late-tick cache. A placeholder is remembered only when
    it does not displace a real range."""
    spy = _SpyStore(store)
    bb = BarBuilder(spy, mclock, persist_raw_ticks=False)
    minutes = range(15, 15 + RECENT_BARS_PER_SYMBOL + 2)
    for i, m in enumerate(minutes):
        feed(bb, now, tick("R", at(9, m, 10), f"{100 + i}.00", 1000 * (i + 1)))
    newest = 15 + RECENT_BARS_PER_SYMBOL
    now.set(at(9, newest, 30))

    # A burst of late ticks for RECENT_BARS_PER_SYMBOL distinct ANCIENT minutes (all long evicted).
    for j in range(RECENT_BARS_PER_SYMBOL):
        bb.on_tick(tick("R", at(9, 15 + j, 20), "100.00", 90_000 + j))

    # The real cached ranges survived: the newest minute's in-range late tick is still FREE.
    spy.calls.clear()
    bb.on_tick(tick("R", at(9, newest, 20), f"{100 + RECENT_BARS_PER_SYMBOL}.00", 99_500))
    assert spy.calls == []
    # And no placeholder displaced a real entry: every remembered bar still carries a real range.
    assert all(b.ranged for b in bb._recent["R"].values())
