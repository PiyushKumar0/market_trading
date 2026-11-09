"""Store isolation: the tick-flush starvation class.

``flush_ticks`` runs once per due tick event and serializes on ``_flush_lock``; if store wrappers
shared the process's default ``asyncio.to_thread`` executor, a long flush would turn every further
tick into a blocked worker and starve the whole engine of threads. Pinned here:

* a flush that finds another in flight RETURNS -- it never queues (and the staged ticks survive it);
* store work runs on the store's OWN pools, ``mt-store`` for reads and ``mt-flush`` for the flush;
* saturating the store pool leaves an unrelated ``asyncio.to_thread`` untouched;
* a flush does not re-prove ~200 partition directories into existence on every pass;
* the watchdog gets FRESH evidence every fifth pending pulse instead of re-reporting one stuck probe.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from engine.core.clock import Clock
from engine.core.types import Tick
from engine.marketdata.store import _STORE_EXECUTOR_WORKERS, MarketStore
from engine.ops.health import _STORE_REPROBE_EVERY
from tests.unit.test_store_stall_watchdog import (
    NOW,
    StuckThenFreshStore,
    Ticker,
    drain_all,
    events,
    monitor,
)

#: Every deliberately-slow flush in this file self-releases after this long. A regression must fail
#: the assertion, never hang the suite (there is no pytest-timeout here).
SLOW_FLUSH_MAX_S = 5.0

#: Enough blocked calls to saturate the DEFAULT executor too (its width is min(32, cpu+4)), so the
#: isolation test is a real regression guard rather than a tautology.
SATURATING_CALLS = 40

STORE_LOGGER = "engine.marketdata.store"


def _tick(ts: datetime, *, symbol: str = "RELIANCE", ltp: str = "2338.55", vol: int = 100) -> Tick:
    return Tick(
        instrument_token=738561, tradingsymbol=symbol, ltp=Decimal(ltp), volume_traded=vol,
        exchange_ts=ts, avg_price=Decimal("2338.1234"), bid=Decimal("2338.50"), ask=Decimal("2338.60"),
    )


@pytest.fixture
def store(tmp_path, clock):
    s = MarketStore(tmp_path / "market.duckdb", tmp_path / "parquet", clock)
    s.open()
    yield s
    s.close()


def log_lines(caplog, event: str) -> list:
    return [r for r in caplog.records if r.getMessage() == event]


# ------------------------------------------------------------------ 1 + 2: skip, and lose nothing
def test_a_flush_during_a_flush_returns_at_once_and_the_staged_ticks_survive(store, clock, caplog,
                                                                            monkeypatch):
    """With one flush held open inside ``_bulk_write``, the flush the next tick triggers returns at once
    rather than parking a thread on ``_flush_lock``; its ticks stay staged for the next cycle."""
    started, release = threading.Event(), threading.Event()
    real_bulk = store._bulk_write

    def slow_bulk(*args, **kwargs):
        started.set()
        release.wait(SLOW_FLUSH_MAX_S)
        return real_bulk(*args, **kwargs)

    monkeypatch.setattr(store, "_bulk_write", slow_bulk)
    store.stage_tick(_tick(clock.now()))
    slow = threading.Thread(target=store.flush_ticks, name="slow-flush", daemon=True)
    slow.start()
    assert started.wait(SLOW_FLUSH_MAX_S), "the first flush never entered the critical section"

    # a tick arrives mid-flush and trips the batch
    store.stage_tick(_tick(clock.now() + timedelta(seconds=1), vol=101))
    with caplog.at_level(logging.INFO, logger=STORE_LOGGER):
        t0 = time.monotonic()
        assert store.flush_ticks() == []
        elapsed = time.monotonic() - t0
        assert store.flush_ticks() == [] and store.flush_ticks() == []   # and again, and again

    assert elapsed < 1.0, f"the flush BLOCKED for {elapsed:.2f}s instead of skipping"
    assert store.tick_flush_skips == 3
    assert store.pending_tick_count == 1          # the second tick is staged, not lost, not written
    # skips are counted always but logged at most once a minute (else one line per tick in a burst)
    skipped = log_lines(caplog, "flush_skipped_in_flight")
    assert len(skipped) == 1
    assert skipped[0].skipped_total == 1 and skipped[0].pending_ticks == 1

    release.set()
    slow.join(SLOW_FLUSH_MAX_S * 2)
    assert not slow.is_alive()
    monkeypatch.undo()

    # --- the next cycle drains what the skip left staged ---------------------------------------
    files = store.flush_ticks()
    assert len(files) == 1 and store.pending_tick_count == 0
    assert [t.volume_traded for t in store.get_ticks("RELIANCE", clock.today())] == [100, 101]


def test_close_waits_for_an_in_flight_flush_instead_of_skipping_it(tmp_path, clock, monkeypatch):
    """``close`` is the one caller that may not skip (the connection is gone afterwards): it waits,
    bounded so shutdown cannot hang on a wedged flush."""
    store = MarketStore(tmp_path / "m.duckdb", tmp_path / "pq", clock)
    store.open()
    started, release = threading.Event(), threading.Event()
    real_bulk = store._bulk_write

    def slow_bulk(*args, **kwargs):
        started.set()
        release.wait(SLOW_FLUSH_MAX_S)
        return real_bulk(*args, **kwargs)

    monkeypatch.setattr(store, "_bulk_write", slow_bulk)
    store.stage_tick(_tick(clock.now()))
    slow = threading.Thread(target=store.flush_ticks, name="slow-flush", daemon=True)
    slow.start()
    assert started.wait(SLOW_FLUSH_MAX_S)
    store.stage_tick(_tick(clock.now() + timedelta(seconds=1), vol=101))

    def closer() -> None:
        store.close()

    closing = threading.Thread(target=closer, name="closer", daemon=True)
    closing.start()
    closing.join(0.5)
    assert closing.is_alive(), "close() skipped the in-flight flush instead of waiting for it"

    release.set()
    slow.join(SLOW_FLUSH_MAX_S * 2)
    closing.join(SLOW_FLUSH_MAX_S * 2)
    assert not closing.is_alive()
    assert store.pending_tick_count == 0          # both ticks made it to parquet before the close

    reopened = MarketStore(tmp_path / "m.duckdb", tmp_path / "pq", clock)
    reopened.open()
    try:
        assert [t.volume_traded for t in reopened.get_ticks("RELIANCE", clock.today())] == [100, 101]
    finally:
        reopened.close()


# ------------------------------------------------------------------ 3: which pool ran what
async def test_store_calls_run_on_mt_store_and_flushes_on_mt_flush(tmp_path, clock, monkeypatch):
    """Thread names prove the offloads landed on the pool they were routed to."""
    store = MarketStore(tmp_path / "m.duckdb", tmp_path / "pq", clock, max_buffered_ticks=1)
    store.open()
    try:
        assert store._store_executor is None and store._flush_executor is None   # created lazily
        assert (await store.arun(lambda: threading.current_thread().name)).startswith("mt-store")

        seen: list[str] = []
        real_ping = store.ping

        def spy_ping() -> bool:
            seen.append(threading.current_thread().name)
            return real_ping()

        monkeypatch.setattr(store, "ping", spy_ping)
        assert await store.aping() is True
        # the probe rides the pool the engine really uses: a private thread could say "free" while that path is starved
        assert seen[-1].startswith("mt-store")

        real_flush = store.flush_ticks

        def spy_flush(**kwargs):
            seen.append(threading.current_thread().name)
            return real_flush(**kwargs)

        monkeypatch.setattr(store, "flush_ticks", spy_flush)
        store.stage_tick(_tick(clock.now()))
        assert len(await store.aflush_ticks()) == 1
        assert seen[-1].startswith("mt-flush")

        # Separate pools; the flush pool is one thread wide (flushes are single-flight).
        assert store._pool() is not store._flush_pool()
        assert store._pool()._max_workers == _STORE_EXECUTOR_WORKERS
        assert store._flush_pool()._max_workers == 1
    finally:
        store.close()
    assert store._store_executor is None and store._flush_executor is None       # released by close


async def test_the_live_flush_path_skips_without_even_reaching_the_pool(tmp_path, clock):
    """``BarBuilder.on_tick_event`` awaits ``aflush_ticks`` once per due tick; a skip discovered only
    inside the worker would still queue each tick behind a long flush. So the skip is decided on the
    loop: no hop, no queue entry, and the flush pool is not even constructed."""
    store = MarketStore(tmp_path / "m.duckdb", tmp_path / "pq", clock, max_buffered_ticks=1)
    store.open()
    started, release = threading.Event(), threading.Event()
    real_locked = store._flush_lock.locked

    def hold_the_lock() -> None:
        with store._flush_lock:
            started.set()
            release.wait(SLOW_FLUSH_MAX_S)

    holder = threading.Thread(target=hold_the_lock, name="lock-holder", daemon=True)
    holder.start()
    try:
        assert started.wait(SLOW_FLUSH_MAX_S)
        store.stage_tick(_tick(clock.now()))

        assert await asyncio.wait_for(store.aflush_ticks(), 2.0) == []
        assert store.tick_flush_skips == 1
        assert store._flush_executor is None, "the skip still paid for a thread hop"
        assert store.pending_tick_count == 1              # staged, still due, nothing lost
        assert real_locked()                              # the flush really was in flight
    finally:
        release.set()
        holder.join(SLOW_FLUSH_MAX_S * 2)
        # with the lock free the next cycle goes through to parquet
        assert len(await store.aflush_ticks()) == 1
        store.close()


async def test_a_flush_queued_behind_one_that_drained_the_batch_writes_nothing(tmp_path, clock,
                                                                              monkeypatch):
    """``locked()`` cannot see a queued-but-unstarted flush, so a burst queues one more per tick event;
    a queued flush must re-check that the batch is still due once it holds the lock, else the queue
    runs back to back writing tiny batches."""
    store = MarketStore(tmp_path / "m.duckdb", tmp_path / "pq", clock, max_buffered_ticks=2)
    store.open()
    occupied, release = threading.Event(), threading.Event()

    def occupy_the_flush_thread() -> None:
        occupied.set()
        release.wait(SLOW_FLUSH_MAX_S)

    try:
        blocker = asyncio.get_running_loop().run_in_executor(store._flush_pool(), occupy_the_flush_thread)
        assert await asyncio.to_thread(occupied.wait, SLOW_FLUSH_MAX_S)
        store.stage_tick(_tick(clock.now()))
        store.stage_tick(_tick(clock.now() + timedelta(seconds=1), vol=101))    # due: the batch cap
        first = asyncio.ensure_future(store.aflush_ticks())
        second = asyncio.ensure_future(store.aflush_ticks())
        await asyncio.sleep(0)                                  # both queue behind the blocker
        real_flush_locked = store._flush_locked

        def flush_then_a_tick_arrives() -> list[Path]:
            written = real_flush_locked()
            store.stage_tick(_tick(clock.now() + timedelta(seconds=2), vol=102))
            return written

        monkeypatch.setattr(store, "_flush_locked", flush_then_a_tick_arrives)
        release.set()
        await blocker
        assert len(await first) == 1
        assert await second == []                               # one staged tick is not a due batch
        assert store.tick_flush_skips == 0                      # ...decided under the lock, not skipped
        assert store.pending_tick_count == 1                    # still staged for the next due flush
    finally:
        release.set()
        store.close()


# ------------------------------------------------------------------ 4: the isolation property
async def test_a_saturated_store_pool_cannot_starve_an_unrelated_to_thread(tmp_path, clock):
    """``SATURATING_CALLS`` blocked store calls exceed the default executor's whole width; with the store
    on its own pool an unrelated ``asyncio.to_thread`` does not notice.
    """
    store = MarketStore(tmp_path / "m.duckdb", tmp_path / "pq", clock)
    store.open()
    hold = threading.Event()

    def block() -> str:
        hold.wait(SLOW_FLUSH_MAX_S)
        return "store"

    jobs = [asyncio.create_task(store.arun(block)) for _ in range(SATURATING_CALLS)]
    try:
        await asyncio.sleep(0.1)                       # let the store pool pick the first ones up
        assert await asyncio.wait_for(asyncio.to_thread(lambda: "unrelated"), 2.0) == "unrelated"
        assert not any(j.done() for j in jobs)         # ...while the store pool was genuinely busy
    finally:
        hold.set()
        assert await asyncio.wait_for(asyncio.gather(*jobs), SLOW_FLUSH_MAX_S * 2) == (
            ["store"] * SATURATING_CALLS
        )
        store.close()


# ------------------------------------------------------------------ 5: the partition-dir cache
def test_partition_dirs_are_created_once_per_day_not_once_per_flush(store, clock, monkeypatch):
    """A steady-state flush must not re-prove already-created directories (inside ``_flush_lock``, where
    flush duration becomes caller pile-up). Cached per (date, symbol), dropped on date rollover."""
    made: list[str] = []
    real_mkdir = Path.mkdir

    def spy_mkdir(self, *args, **kwargs):
        made.append(self.as_posix())
        return real_mkdir(self, *args, **kwargs)

    monkeypatch.setattr(Path, "mkdir", spy_mkdir)

    def partitions_touched() -> set[str]:
        """The tick partitions the flush made a filesystem call for (a SET: ``mkdir(parents=True)``
        re-enters itself per path)."""
        return {p.rsplit("/ticks/", 1)[1] for p in made if "/symbol=" in p}

    today = clock.today().isoformat()
    for symbol in ("RELIANCE", "TCS"):
        store.stage_tick(_tick(clock.now(), symbol=symbol))
    assert len(store.flush_ticks()) == 2
    assert partitions_touched() == {                  # first pass of the day: created, as before
        f"date={today}/symbol=RELIANCE", f"date={today}/symbol=TCS",
    }

    made.clear()
    for symbol in ("RELIANCE", "TCS"):
        store.stage_tick(_tick(clock.now() + timedelta(seconds=1), symbol=symbol))
    assert len(store.flush_ticks()) == 2
    assert partitions_touched() == set()              # second pass: zero filesystem calls

    # date rollover drops the cache: tomorrow's partitions are new directories
    made.clear()
    tomorrow = clock.now() + timedelta(days=1)
    for symbol in ("RELIANCE", "TCS"):
        store.stage_tick(_tick(tomorrow, symbol=symbol))
    assert len(store.flush_ticks()) == 2
    assert partitions_touched() == {
        f"date={tomorrow.date().isoformat()}/symbol=RELIANCE",
        f"date={tomorrow.date().isoformat()}/symbol=TCS",
    }
    assert len(store.get_ticks("RELIANCE", tomorrow.date())) == 1


# ------------------------------------------------------------------ 6: the watchdog re-probe
# (StuckThenFreshStore + drain_all live in test_store_stall_watchdog with the other watchdog fixtures.)
async def test_a_pending_probe_no_longer_blocks_fresh_evidence(caplog):
    """A stuck probe must not be re-reported forever: four pending pulses start nothing (single-flight),
    the fifth abandons the stuck probe and asks again. The fresh probe answers, and both facts are
    logged: the store is usable NOW, and one worker went in and never came back.
    """
    at = Ticker(NOW)
    store = StuckThenFreshStore()
    mon = monitor(store, clock=Clock(time_source=at))

    try:
        with caplog.at_level(logging.INFO, logger="engine.ops.health"):
            for i in range(_STORE_REPROBE_EVERY + 1):
                at.at = NOW + timedelta(seconds=60 * i)
                await mon.check(check_skew=False)

        stalls = events(caplog, "store_stalled")
        assert len(stalls) == _STORE_REPROBE_EVERY
        assert [s.probe_pending for s in stalls] == [False] + [True] * (_STORE_REPROBE_EVERY - 1)
        assert [s.consecutive for s in stalls] == list(range(1, _STORE_REPROBE_EVERY + 1))
        assert all(s.abandoned == 0 for s in stalls)        # nothing abandoned while it was watched
        assert store.pings == 2, "the pending probe was never re-issued -- 08-25 all over again"

        # the fresh probe answered while the old one still hangs: recovered, AND anomalous
        anomalies = events(caplog, "store_probe_anomaly")
        assert len(anomalies) == 1
        assert anomalies[0].levelname == "INFO"
        assert anomalies[0].abandoned == 1
        assert anomalies[0].consecutive == _STORE_REPROBE_EVERY
        recovered = events(caplog, "store_stall_recovered")
        assert len(recovered) == 1 and recovered[0].abandoned == 1
        assert mon._store_stall_count == 0                  # treated as recovered
        assert len(events(caplog, "store_stall_stacks")) == 1    # still ONE dump per episode
    finally:
        store.gate.set()
        await drain_all(mon)


async def test_the_abandoned_count_drops_once_the_stuck_probe_finally_answers(caplog):
    """``abandoned`` counts threads currently swallowed, not a lifetime total."""
    at = Ticker(NOW)
    store = StuckThenFreshStore()
    mon = monitor(store, clock=Clock(time_source=at))

    try:
        for i in range(_STORE_REPROBE_EVERY + 1):
            at.at = NOW + timedelta(seconds=60 * i)
            await mon.check(check_skew=False)
        assert mon._outstanding_abandoned() == 1

        store.gate.set()                                    # the stuck ping comes back at last
        await drain_all(mon)
        caplog.clear()                                      # only the NEXT pulse is under test
        with caplog.at_level(logging.INFO, logger="engine.ops.health"):
            at.at = NOW + timedelta(seconds=60 * (_STORE_REPROBE_EVERY + 1))
            await mon.check(check_skew=False)

        assert mon._outstanding_abandoned() == 0
        assert events(caplog, "store_probe_anomaly") == []   # nothing anomalous left to report
        assert events(caplog, "store_stalled") == []
    finally:
        store.gate.set()
        await drain_all(mon)


def test_no_shared_executor_dispatch_remains_in_store_py():
    """store.py never dispatches through ``asyncio.to_thread``: a missed wrapper would silently re-open
    the shared-executor starvation path."""
    from pathlib import Path

    import engine.marketdata.store as store_mod

    source = Path(store_mod.__file__).read_text(encoding="utf-8")
    # call sites only: docstrings legitimately NAME asyncio.to_thread
    assert "asyncio.to_thread(" not in source
