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
* none of it can raise into, or slow down, the pulse it rides on;
* and (2026-09-03) a stall still there on the second pulse, or a probe still swallowed after a
  fresh one answered, is an owner-facing ``store_stalled`` problem -- in session only -- that ends
  when the store answers, with success OR with an error.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta

import pytest

from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.config import config_dir, load_settings
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


def paged_monitor(store, *, at: Ticker) -> tuple[HealthMonitor, list[tuple[str, str]]]:
    """A monitor with the owner alert wired and the calendar present (NOW is a trading Wednesday,
    in session) -- the shape under which a stall is allowed to page."""
    clock = Clock(time_source=at)
    alerts: list[tuple[str, str]] = []

    async def alert(severity: str, message: str) -> None:
        alerts.append((severity, message))

    mon = HealthMonitor(
        clock, load_settings(), store=store, store_ping_timeout_s=FAST_PING_TIMEOUT_S, alert=alert,
        calendar=NSECalendar(config_dir() / "calendar", clock, strict=False),
    )
    return mon, alerts


async def test_a_stall_that_survives_a_second_pulse_pages_the_owner_as_a_health_problem():
    """2026-09-03: the stall was log-only (ERROR lines + a stack dump) -- the 08-21 and 08-25 freezes
    were read off the log after the fact. One timed-out ping is an observation; the SAME stall still
    there on the next pulse is an incident, and it rides the WO-25b episode cadence: one page on the
    change, silence while unchanged (reminder after HEALTH_REPEAT_MIN), recovery announced once."""
    at = Ticker(NOW)
    store = HangingStore()
    mon, alerts = paged_monitor(store, at=at)
    try:
        report = await mon.check(check_skew=False)             # pulse 1: the probe times out
        assert "store_stalled" not in report.problems and alerts == []

        at.at = NOW + timedelta(seconds=60)
        report = await mon.check(check_skew=False)             # pulse 2: still pending -> incident
        assert "store_stalled" in report.problems
        assert "60s" in report.problem_details["store_stalled"]
        assert len(alerts) == 1 and "store_stalled" in alerts[0][1]
        # (the reminder / recovery cadence itself is _alert_problems' contract, pinned in
        #  test_health_monitor -- not re-pinned here)

        store.gate.set()                                       # the store answers
        await drain_probe(mon)
        at.at = NOW + timedelta(seconds=120)
        report = await mon.check(check_skew=False)             # pulse 3: a fresh probe succeeds
        assert "store_stalled" not in report.problems
    finally:
        store.gate.set()
        await drain_probe(mon)


async def test_a_stall_out_of_session_never_pages():
    """The 22:30 tick compaction holds the store lock for minutes on a healthy engine and never runs
    in session (WO-21), so the page is in-session only, exactly like feed_stale."""
    at = Ticker(NOW.replace(hour=22, minute=35))
    store = HangingStore()
    mon, alerts = paged_monitor(store, at=at)
    try:
        for i in range(3):
            at.at = NOW.replace(hour=22, minute=35) + timedelta(seconds=60 * i)
            report = await mon.check(check_skew=False)
            assert "store_stalled" not in report.problems
        assert alerts == []
    finally:
        store.gate.set()
        await drain_probe(mon)


class HangThenRaiseStore:
    """A store that stalls once and then answers every later ping with an ERROR -- the wedge killed
    the connection. Answering is not stalling, so the episode must END, not latch."""

    def __init__(self) -> None:
        self.gate = asyncio.Event()
        self.pings = 0

    async def aping(self) -> bool:
        self.pings += 1
        if self.pings == 1:
            await self.gate.wait()
            return True
        raise RuntimeError("connection is closed")


async def test_a_ping_that_raises_after_a_stall_ends_the_episode(caplog):
    """Review finding (2026-09-03): the count only ever reset on a SUCCESSFUL ping, so a store that
    started raising after two stalled pulses would have paged 'unanswered for Ns' forever."""
    at = Ticker(NOW)
    store = HangThenRaiseStore()
    mon, alerts = paged_monitor(store, at=at)
    try:
        await mon.check(check_skew=False)                      # pulse 1: times out
        at.at = NOW + timedelta(seconds=60)
        await mon.check(check_skew=False)                      # pulse 2: still pending -> page
        assert len(alerts) == 1

        store.gate.set()                                       # the first probe finally returns
        await drain_probe(mon)
        at.at = NOW + timedelta(seconds=120)
        with caplog.at_level(logging.WARNING, logger="engine.ops.health"):
            report = await mon.check(check_skew=False)         # pulse 3: a fresh probe RAISES
        assert events(caplog, "store_ping_failed")
        assert "store_stalled" not in report.problems
        assert len(alerts) == 2 and alerts[1][1].startswith("health recovered")
        assert mon._store_stall_count == 0 and mon._store_stall_since is None
    finally:
        store.gate.set()
        await drain_probe(mon)


async def test_a_swallowed_probe_keeps_the_problem_up_until_it_returns():
    """The WO-26a anomaly (08-25 shape): the re-probe on the 5th pending pulse gets a thread and
    answers while the first probe is still swallowed. That resets the stall count, but the pool is
    still losing threads -- so the problem stays up (no false 'all clear', no flap back and forth
    two pulses later) until the swallowed probe actually comes back."""
    at = Ticker(NOW)
    store = StuckThenFreshStore()
    mon, alerts = paged_monitor(store, at=at)
    try:
        for i in range(6):                                     # pulse 6 = the WO-26a re-probe
            at.at = NOW + timedelta(seconds=60 * i)
            report = await mon.check(check_skew=False)
        assert store.pings == 2 and mon._store_stall_count == 0     # fresh probe answered
        assert "store_stalled" in report.problems                   # ...but one is still swallowed
        assert "1 abandoned probe(s) still hanging" in report.problem_details["store_stalled"]
        assert len(alerts) == 1                                     # the pulse-2 page; no all-clear

        store.gate.set()                                       # the swallowed probe returns
        await drain_all(mon)
        at.at = NOW + timedelta(seconds=60 * 6)
        report = await mon.check(check_skew=False)
        assert "store_stalled" not in report.problems
        assert len(alerts) == 2 and alerts[1][1].startswith("health recovered")
    finally:
        store.gate.set()
        await drain_all(mon)


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
