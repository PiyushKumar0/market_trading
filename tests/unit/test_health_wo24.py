"""WO-24b-prime (2026-08-21): the store stall watchdog on the health pulse.

The 09:56 freeze wedged every store-touching path for 14 minutes and left TWO live hypotheses
standing, because nothing in the process was probing the resource they share. The health pulse,
however, kept beating through the whole episode -- so it is the host for a probe that would have
settled it: acquire the store lock, run ``SELECT 1``, and if that does not return, dump every
thread's stack and name whoever is holding the thing.

What is pinned here is the watchdog's whole contract, in the order it matters:

* it detects a hang (not merely a slow call) and says so at ERROR;
* it is SINGLE-FLIGHT -- a pulse that finds the previous probe still pending starts no new one,
  because queueing a second thread behind a seized lock is how a diagnostic becomes an incident;
* the stacks are dumped ONCE per stall episode and again for the next one;
* and none of it can raise into, or slow down, the pulse it rides on.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta

import pytest

from engine.core.clock import IST, Clock
from engine.core.config import load_settings
from engine.marketdata.store import MarketStore
from engine.ops.health import _STACK_DUMP_MAX_CHARS, HealthMonitor

NOW = datetime(2026, 6, 17, 11, 0, tzinfo=IST)

#: Short enough to keep the suite fast, long enough that a healthy stub is never called stalled.
FAST_PING_TIMEOUT_S = 0.05


class Ticker:
    """A movable time source -- ``ticker.at = ...`` advances the Clock built on it."""

    def __init__(self, at: datetime) -> None:
        self.at = at

    def __call__(self) -> datetime:
        return self.at


class HangingStore:
    """A store whose ping ACCEPTS the call and does not answer until released -- 09:56, in a stub."""

    def __init__(self) -> None:
        self.gate = asyncio.Event()
        self.pings = 0

    async def aping(self) -> bool:
        self.pings += 1
        await self.gate.wait()
        return True


class RaisingStore:
    """A store that answers with an ERROR. Not a stall: the lock was free, the statement failed."""

    def __init__(self) -> None:
        self.pings = 0

    async def aping(self) -> bool:
        self.pings += 1
        raise RuntimeError("connection is closed")


class BrokenStore:
    """``aping`` is not even callable as a coroutine -- a wiring bug, which must still not take down
    the pulse (the pulse surviving is the entire premise of hosting the watchdog here)."""

    aping = "not callable"


def events(caplog, event: str) -> list:
    return [r for r in caplog.records if r.getMessage() == event]


def monitor(store, *, clock: Clock) -> HealthMonitor:
    return HealthMonitor(
        clock, load_settings(), store=store, store_ping_timeout_s=FAST_PING_TIMEOUT_S
    )


async def drain_probe(mon: HealthMonitor) -> None:
    """Let an abandoned probe finish so the loop is left clean between phases."""
    probe = mon._store_probe
    if probe is not None:
        await asyncio.wait_for(asyncio.shield(probe), 5)


async def test_a_hung_store_ping_stalls_dumps_stacks_once_and_then_recovers(caplog):
    """The full episode: three stalled pulses, one stack dump, recovery, then a SECOND episode.

    ``pings`` is the load-bearing counter. It reaches 1 and stays there for all three pulses --
    single-flight -- because the pending probe IS the evidence and a second one would only add
    another thread to the queue behind whatever is stuck.
    """
    at = Ticker(NOW)
    store = HangingStore()
    mon = monitor(store, clock=Clock(time_source=at))

    try:
        with caplog.at_level(logging.INFO, logger="engine.ops.health"):
            # --- three pulses against a store that never answers ------------------------------
            for i in range(3):
                at.at = NOW + timedelta(seconds=60 * i)
                report = await mon.check(check_skew=False)
                assert report.healthy or report.problems is not None   # the pulse SURVIVED

            stalls = events(caplog, "store_stalled")
            assert len(stalls) == 3
            assert all(s.levelname == "ERROR" for s in stalls)
            assert [s.consecutive for s in stalls] == [1, 2, 3]
            # first pulse timed out on its own probe; the next two found it still pending
            assert [s.probe_pending for s in stalls] == [False, True, True]
            assert store.pings == 1                                    # single-flight, no pile-up
            # Nothing has ever succeeded, so there is no last-success age to fabricate.
            assert stalls[0].seconds_since_last_success is None
            assert stalls[2].stalled_s == pytest.approx(120.0)

            # --- the stacks: once per EPISODE, bounded, and actually useful --------------------
            dumps = events(caplog, "store_stall_stacks")
            assert len(dumps) == 1
            assert dumps[0].levelname == "ERROR"
            stacks = dumps[0].stacks
            assert isinstance(stacks, dict) and stacks
            assert dumps[0].threads >= 1
            # the dumping thread's own frame proves these are real, live stacks
            assert any("_dump_thread_stacks" in text for text in stacks.values())
            assert sum(len(text) for text in stacks.values()) <= _STACK_DUMP_MAX_CHARS

            # --- the store comes back --------------------------------------------------------
            store.gate.set()
            await drain_probe(mon)
            at.at = NOW + timedelta(seconds=300)
            await mon.check(check_skew=False)

            recovered = events(caplog, "store_stall_recovered")
            assert len(recovered) == 1
            assert recovered[0].consecutive == 3
            assert recovered[0].stalled_s == pytest.approx(300.0)
            assert store.pings == 2                     # a fresh probe, now that the old one is done
            assert len(events(caplog, "store_stalled")) == 3          # no new stall recorded

            # --- a SECOND, later episode dumps its own stacks ---------------------------------
            store.gate.clear()
            at.at = NOW + timedelta(seconds=360)
            await mon.check(check_skew=False)

            assert store.pings == 3
            assert len(events(caplog, "store_stall_stacks")) == 2
            latest = events(caplog, "store_stalled")[-1]
            assert latest.consecutive == 1              # the counter was reset by the recovery
            assert latest.seconds_since_last_success == pytest.approx(60.0)
    finally:
        store.gate.set()
        await drain_probe(mon)


async def test_a_store_that_answers_with_an_error_is_not_called_stalled(caplog):
    """A raising ping means the lock was free and the statement failed -- a different diagnosis, and
    one the stack dump would say nothing useful about. Warned, not ERRORed, and no dump."""
    store = RaisingStore()
    mon = monitor(store, clock=Clock(time_source=Ticker(NOW)))

    with caplog.at_level(logging.INFO, logger="engine.ops.health"):
        await mon.check(check_skew=False)

    failed = events(caplog, "store_ping_failed")
    assert len(failed) == 1
    assert failed[0].error_type == "RuntimeError"
    assert events(caplog, "store_stalled") == []
    assert events(caplog, "store_stall_stacks") == []
    assert store.pings == 1
    # The probe slot is cleared, so the next pulse probes again instead of reading a stale pending.
    assert mon._store_probe is None


async def test_a_broken_watchdog_never_takes_down_the_pulse(caplog):
    """The premise of hosting the probe on the health pulse is that the pulse keeps beating. A
    watchdog that can raise into its own host would invert exactly the property being bought."""
    clock = Clock(time_source=Ticker(NOW))
    baseline = await HealthMonitor(clock, load_settings()).check(check_skew=False)
    mon = monitor(BrokenStore(), clock=clock)

    with caplog.at_level(logging.INFO, logger="engine.ops.health"):
        report = await mon.check(check_skew=False)         # must not raise

    assert report.problems == baseline.problems
    assert report.healthy == baseline.healthy
    assert len(events(caplog, "store_watchdog_failed")) == 1


async def test_no_store_wired_means_no_probe_and_no_noise(caplog):
    """An unwired monitor (every existing test, and any partial deploy) is silent, not stalled."""
    mon = HealthMonitor(Clock(time_source=Ticker(NOW)), load_settings())

    with caplog.at_level(logging.INFO, logger="engine.ops.health"):
        await mon.check(check_skew=False)

    assert events(caplog, "store_stalled") == []
    assert events(caplog, "store_ping_failed") == []
    assert mon._store_probe is None


# ------------------------------------------------------------------ the live shape of the probe
async def test_market_store_ping_runs_against_a_real_duckdb(tmp_path, clock):
    """The probe is only worth anything if it exercises the real lock-and-execute path, so pin it
    against an actual DuckDB file rather than a stub. A CLOSED store raises -- which is the answer
    the watchdog reads as ``store_ping_failed``, deliberately distinct from a hang."""
    store = MarketStore(tmp_path / "market.duckdb", tmp_path / "pq", clock)
    store.open()
    try:
        assert store.ping() is True
        assert await store.aping() is True
    finally:
        store.close()

    with pytest.raises(RuntimeError):
        store.ping()
