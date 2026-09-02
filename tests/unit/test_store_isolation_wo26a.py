"""WO-26a (2026-08-25): the tick-flush starvation class, closed.

Three trading days died of the same mechanism. ``flush_ticks`` is called once per tick event whose
batch is due, it serialized whole flushes on ``_flush_lock``, and every wrapper on ``MarketStore``
offloaded via ``asyncio.to_thread`` -- the ONE default executor the whole process shares. So a flush
that ran long turned each further tick into another blocked worker: on 08-25 a stack dump caught ~20
of them stopped at that lock, the pool was gone, and for 4 h 24 min nothing else in the engine could
get a thread. The intelligence layer ran empty for the entire session -- zero candidates, zero
analyst calls -- because scan-context reads never got a worker. The health watchdog could not say so
either: its first probe never got a worker, and being strictly single-flight it then re-reported that
same 09:15 observation 264 times.

What is pinned here is the whole fix, in the order the failure unfolded:

* a flush that finds another in flight RETURNS -- it never queues (and the staged ticks survive it);
* store work runs on the store's OWN pools, ``mt-store`` for reads and ``mt-flush`` for the flush,
  so neither can starve the other and nothing outside can starve either;
* saturating the store pool leaves an unrelated ``asyncio.to_thread`` untouched -- the isolation
  property, stated as a test;
* a flush no longer re-proves ~200 partition directories into existence on every pass;
* and the watchdog gets FRESH evidence every fifth pending pulse, so "probe_pending=true,
  consecutive=264, nothing new for 4.4 h" is now unreachable.
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
from tests.unit.test_health_wo24 import NOW, Ticker, events, monitor

#: Every deliberately-slow flush in this file self-releases after this long. A regression must fail
#: the assertion, never hang the suite (there is no pytest-timeout here).
SLOW_FLUSH_MAX_S = 5.0

#: Enough blocked calls to saturate the DEFAULT executor too (its width is min(32, cpu+4)), which is
#: what makes the isolation test a real regression guard rather than a tautology: before WO-26a these
#: same calls WERE default-executor threads.
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
    """The pile-up, reproduced -- and refused.

    One flush is held open inside ``_bulk_write``; the flush the next tick triggers must come
    straight back rather than parking a thread on ``_flush_lock``. Its ticks are not dropped for
    that: they are still staged, and the next cycle writes them.
    """
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

    # A tick arrives mid-flush and trips the batch. Under the old code this call parked a worker
    # thread here for the whole duration of the flush in front of it.
    store.stage_tick(_tick(clock.now() + timedelta(seconds=1), vol=101))
    with caplog.at_level(logging.INFO, logger=STORE_LOGGER):
        t0 = time.monotonic()
        assert store.flush_ticks() == []
        elapsed = time.monotonic() - t0
        assert store.flush_ticks() == [] and store.flush_ticks() == []   # and again, and again

    assert elapsed < 1.0, f"the flush BLOCKED for {elapsed:.2f}s instead of skipping"
    assert store.tick_flush_skips == 3
    assert store.pending_tick_count == 1          # the second tick is staged, not lost, not written
    # Skips are the designed response to a burst, so they are counted always and logged at most once
    # a minute -- one line per skipped flush would be one line per tick during the storm itself.
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
    """The one caller that may not skip: after ``close`` the connection is gone, so a skipped batch
    would have nowhere left to go. The wait is bounded -- shutdown must not hang on a wedged flush --
    but it is a wait, and this is the call site the skip semantics had to be checked against."""
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
    """Thread names are the cheapest proof that the offloads landed where they were routed -- and
    the only proof that survives a refactor of how they get there."""
    store = MarketStore(tmp_path / "m.duckdb", tmp_path / "pq", clock)
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
        # The probe rides the pool the engine really uses: a private thread would answer "the lock
        # is free" while the path that matters was starved -- the one lie the watchdog must not tell.
        assert seen[-1].startswith("mt-store")

        real_flush = store.flush_ticks

        def spy_flush(**kwargs):
            seen.append(threading.current_thread().name)
            return real_flush(**kwargs)

        monkeypatch.setattr(store, "flush_ticks", spy_flush)
        store.stage_tick(_tick(clock.now()))
        assert len(await store.aflush_ticks()) == 1
        assert seen[-1].startswith("mt-flush")

        # Separate pools, and the flush pool is exactly one thread wide: flushes are single-flight
        # already, so a second flush thread could only ever wait on the first.
        assert store._pool() is not store._flush_pool()
        assert store._pool()._max_workers == _STORE_EXECUTOR_WORKERS
        assert store._flush_pool()._max_workers == 1
    finally:
        store.close()
    assert store._store_executor is None and store._flush_executor is None       # released by close


async def test_the_live_flush_path_skips_without_even_reaching_the_pool(tmp_path, clock):
    """``BarBuilder.on_tick_event`` awaits ``aflush_ticks`` once per due tick. If the skip were only
    discovered inside the worker, each of those ticks would still queue behind a flush that can run
    for seconds -- the pile-up in its last remaining form, now made of queue entries and tick-handler
    latency instead of threads. So the skip is decided on the loop: no hop, no queue entry, and the
    flush pool is not even constructed."""
    store = MarketStore(tmp_path / "m.duckdb", tmp_path / "pq", clock)
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
        # ...and the next cycle, with the lock free, goes all the way through to parquet.
        assert len(await store.aflush_ticks()) == 1
        store.close()


# ------------------------------------------------------------------ 4: the isolation property
async def test_a_saturated_store_pool_cannot_starve_an_unrelated_to_thread(tmp_path, clock):
    """08-25 in one assertion.

    ``SATURATING_CALLS`` blocked store calls is more than the default executor's whole width, so
    before WO-26a this is precisely the state the process was in when the intelligence layer went
    silent: every default-executor thread held by the store, nothing else able to get one. With the
    store on its own pool the unrelated offload does not even notice.
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
    """A steady-state flush used to spend ~200 filesystem round-trips re-proving directories that
    the flush 60 s earlier had already created -- inside ``_flush_lock``, which is where flush
    duration turns into caller pile-up. Cached per (date, symbol), dropped on date rollover."""
    made: list[str] = []
    real_mkdir = Path.mkdir

    def spy_mkdir(self, *args, **kwargs):
        made.append(self.as_posix())
        return real_mkdir(self, *args, **kwargs)

    monkeypatch.setattr(Path, "mkdir", spy_mkdir)

    def partitions_touched() -> set[str]:
        """The tick partitions the flush made a filesystem call for. A SET, because
        ``mkdir(parents=True)`` legitimately re-enters itself per path -- what is under test is
        which partitions were touched at all, not how pathlib gets there."""
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

    # Date rollover drops the cache -- tomorrow's partitions are new directories and must be made.
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
class StuckThenFreshStore:
    """The 08-25 shape: the FIRST ping goes into the pool and never comes out (its worker was gone),
    while the store itself is perfectly able to answer anyone who can get a thread. Under strict
    single-flight this store looks dead forever -- the only probe that could have said otherwise was
    the one that was never issued."""

    def __init__(self) -> None:
        self.gate = asyncio.Event()
        self.pings = 0

    async def aping(self) -> bool:
        self.pings += 1
        if self.pings == 1:
            await self.gate.wait()
        return True


async def drain_all(mon) -> None:
    """Let every probe the monitor is still holding -- current and abandoned -- finish, so the loop
    is left clean."""
    for probe in [mon._store_probe, *mon._store_abandoned]:
        if probe is not None:
            await asyncio.wait_for(asyncio.shield(probe), 5)


async def test_a_pending_probe_no_longer_blocks_fresh_evidence(caplog):
    """264 consecutive pulses of the same 09:15 observation must be unreachable.

    Four pending pulses still start nothing (single-flight is right for a short seizure); the fifth
    abandons the stuck probe and asks again. That fresh probe answers, which is the finding the old
    watchdog structurally could not produce -- and the two facts it leaves are both logged: the store
    is usable NOW, and one worker went in and never came back.
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

        # The fresh probe answered while the old one still hangs: recovered, AND anomalous.
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
    """``abandoned`` counts threads currently swallowed, not a lifetime total -- otherwise the number
    that says "this is still happening" would keep saying it long after it stopped."""
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
    """2026-09-02 review: five pre-WO-26a filings wrappers were missed by the to_thread->_off
    migration, silently re-opening the shared-executor starvation path. Pin the invariant the
    WO-26a docstrings claim ("never asyncio.to_thread") at the source level."""
    from pathlib import Path

    import engine.marketdata.store as store_mod

    source = Path(store_mod.__file__).read_text(encoding="utf-8")
    # Call sites only — the WO-26a docstrings legitimately NAME asyncio.to_thread while explaining
    # why it must never be used here.
    assert "asyncio.to_thread(" not in source
