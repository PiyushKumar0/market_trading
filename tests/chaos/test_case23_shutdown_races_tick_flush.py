"""§9.4 chaos case 23 — Shutdown races an in-flight tick flush (§2.6 teardown order / §3.2 hot-path
invariant 5).

Plan row (IMPLEMENTATION_PLAN.md §9.4, case 23), "Must hold":

    "with a due tick batch mid-flush on a background thread, a concurrent clean stop neither deadlocks
    (lock order: flush-serialization → store; close() flushes before taking the store lock) nor
    crashes: the flush completes or degrades gracefully (batch restaged + explicit
    tick_flush_skipped_store_closed log — never a swallowed traceback); event-bus handlers are drained
    before store close; ENGINE_STOPPED still emitted"

Composition (``tests/chaos/_lifecycle_rig.py``): the live tick path exactly as main.py wires it — a
real ``MarketStore`` (``from_settings``: 60 s / 2000-tick batching) with a real ``BarBuilder``
subscribed to ``"tick"`` on the real ``EventBus``; ticks arrive by fire-and-forget ``bus.publish`` (as
``TickerSupervisor`` publishes them), so the due flush runs through ``BarBuilder.on_tick_event`` →
``MarketStore.aflush_ticks`` on the store's ``mt-flush`` thread. The flush is held mid-flight inside
its batch write (the unit tests' ``_bulk_write`` seam) and the clean stop runs main.py's teardown
order: scheduler down → post-arm cancelled → ``bar_builder.flush_all`` → ticker stop →
``lifecycle.shutdown`` (ENGINE_STOPPED) → ``bus.drain`` → ``store.close()`` → conn close. The whole scenario runs on
its own event-loop thread joined with a hard bound, so a deadlock FAILS the test instead of hanging it.

Tests:

* ``…_completes_without_deadlock`` — the in-flight flush is released only once ``store.close()`` is
  waiting on it: no deadlock, no crash, the in-flight batch AND the tick staged behind it both reach
  parquet, no error/traceback logged, ENGINE_STOPPED once, state STOPPED. (passes)
* ``…_late_flush_after_close_restages_with_explicit_log`` — a size-due burst whose bus handlers only
  run after ``store.close()`` (the undrained-handler hazard): the orphaned flush restages the batch and
  logs ``tick_flush_skipped_store_closed``; nothing raises. (passes)
* ``…_bus_handlers_drained_before_store_close`` — the teardown drains the bus
  (``_SHUTDOWN_BUS_DRAIN_S``) before ``store.close()``, so no delivery is pending at close (CD-6,
  fixed 2026-09-24).
* ``…_flush_wedged_past_close_bound_degrades_gracefully`` — a flush still mid-flight when both
  bounds (the drain and ``close()``'s ``_CLOSE_FLUSH_WAIT_S``) expire restages its unwritten batch with
  ``tick_flush_skipped_store_closed`` instead of crashing (CD-7, fixed 2026-09-24).
"""

from __future__ import annotations

import asyncio
import logging
import threading
from datetime import date, datetime, timedelta
from decimal import Decimal

from engine.core.clock import IST
from engine.core.types import Tick
from engine.marketdata.store import MarketStore
from engine.notify.catalog import MessageKind
from tests.chaos._lifecycle_rig import EngineProcess, FakeTicker, RigEnv

WED = date(2026, 6, 17)
BOOT_AT = datetime(2026, 6, 17, 10, 5, tzinfo=IST)
#: Every deliberately-held flush self-releases after this long (belt and braces under RACE_BOUND_S).
SLOW_FLUSH_MAX_S = 8.0
#: Hard bound on a whole scenario (boot + race + teardown). close()'s own bounded wait is 15 s and is
#: never reached in the passing tests; a real deadlock would hold the scenario thread forever.
RACE_BOUND_S = 40.0
STORE_LOGGER = "engine"


def _tick(ts: datetime, vol: int) -> Tick:
    return Tick(
        instrument_token=738561, tradingsymbol="RELIANCE", ltp=Decimal("2338.55"), volume_traded=vol,
        exchange_ts=ts, avg_price=Decimal("2338.1234"), bid=Decimal("2338.50"), ask=Decimal("2338.60"),
    )


def _run_bounded(scenario) -> dict:
    """Run ``scenario()`` on a dedicated event-loop thread; a thread still alive after
    ``RACE_BOUND_S`` is a DEADLOCK and fails the test (the daemon thread is abandoned)."""
    out: dict = {}

    def target() -> None:
        try:
            out["result"] = asyncio.run(scenario())
        except BaseException as exc:  # noqa: BLE001 - re-raised on the test thread below
            out["error"] = exc

    th = threading.Thread(target=target, name="chaos-case23-loop", daemon=True)
    th.start()
    th.join(RACE_BOUND_S)
    assert not th.is_alive(), f"DEADLOCK: clean stop vs in-flight tick flush did not finish in {RACE_BOUND_S}s"
    if "error" in out:
        raise out["error"]
    return out["result"]


def _bus_deliveries(loop: asyncio.AbstractEventLoop) -> list[asyncio.Task]:
    """Pending ``EventBus.publish`` handler deliveries (fire-and-forget tasks on the loop)."""
    return [t for t in asyncio.all_tasks(loop)
            if not t.done() and getattr(t.get_coro(), "__qualname__", "") == "EventBus._deliver_one"]


def _hold_the_live_flush(store: MarketStore) -> tuple[threading.Event, threading.Event]:
    """Park the FIRST batch write made on the ``mt-flush`` thread (the live aflush path) until
    ``release`` is set. Returns (started, release)."""
    started, release = threading.Event(), threading.Event()
    real_bulk = store._bulk_write

    def held_bulk(*args, **kwargs):
        if threading.current_thread().name.startswith("mt-flush") and not started.is_set():
            started.set()
            release.wait(SLOW_FLUSH_MAX_S)
        return real_bulk(*args, **kwargs)

    store._bulk_write = held_bulk
    return started, release


async def _boot_and_start_in_flight_flush(env: RigEnv) -> tuple[EngineProcess, threading.Event]:
    """Boot at 10:05; at 10:06:05 a tick makes the batch due (60 s since the store's last flush) and
    its handler's flush parks mid-batch-write on mt-flush; a second tick is staged behind it."""
    env.at(BOOT_AT)
    proc = EngineProcess(env, bar_builder=True, ticker=FakeTicker())
    await proc.boot()
    started, release = _hold_the_live_flush(proc.store)
    env.at(BOOT_AT + timedelta(seconds=65))
    proc.bus.publish("tick", _tick(env.clock.now(), vol=100))        # fire-and-forget, as the supervisor
    loop = asyncio.get_running_loop()
    assert await loop.run_in_executor(None, started.wait, SLOW_FLUSH_MAX_S), "flush never went in flight"
    proc.bus.publish("tick", _tick(env.clock.now() + timedelta(seconds=1), vol=101))
    for _ in range(3):
        await asyncio.sleep(0)                                         # second tick staged (not due)
    assert proc.store.pending_tick_count == 1
    return proc, release


def _persisted_volumes(env: RigEnv) -> list[int]:
    reopened = MarketStore.from_settings(env.settings, env.clock)
    reopened.open()
    try:
        return [t.volume_traded for t in reopened.get_ticks("RELIANCE", WED)]
    finally:
        reopened.close()


def _tracebacks(caplog) -> list[str]:
    return [f"{r.name}:{r.getMessage()}" for r in caplog.records if r.exc_info or r.levelno >= logging.ERROR]


# --------------------------------------------------------------------------- the race: completes
def test_case23_clean_stop_during_in_flight_flush_completes_without_deadlock(tmp_path, monkeypatch, caplog):
    import tests.chaos._lifecycle_rig as rig_mod

    # The bus drain gives up first, so close() meets the in-flight flush — the race this test is about.
    monkeypatch.setattr(rig_mod, "_SHUTDOWN_BUS_DRAIN_S", 0.1)
    env = RigEnv(tmp_path, monkeypatch, start=BOOT_AT)

    async def scenario() -> dict:
        proc, release = await _boot_and_start_in_flight_flush(env)
        store, loop = proc.store, asyncio.get_running_loop()
        real_close = store.close
        seen: dict = {}

        def close_while_flush_in_flight() -> None:
            seen["flush_in_flight_at_close"] = store._flush_lock.locked()
            threading.Timer(0.2, release.set).start()   # the flush finishes WHILE close() waits on it
            real_close()

        store.close = close_while_flush_in_flight
        sent0 = len(env.sent)
        await proc.stop()                               # main.py teardown order
        for task in _bus_deliveries(loop):
            await asyncio.wait_for(task, timeout=SLOW_FLUSH_MAX_S)
        seen["stopped_msgs"] = [m for m in env.messages(since=sent0) if m.kind == MessageKind.ENGINE_STOPPED]
        seen["pending_after"] = store.pending_tick_count
        seen["flush_skips"] = store.tick_flush_skips
        return seen

    with caplog.at_level(logging.INFO, logger=STORE_LOGGER):
        seen = _run_bounded(scenario)

    assert seen["flush_in_flight_at_close"] is True, "the race was not exercised"
    assert _tracebacks(caplog) == [], "a crash/traceback during the stop-vs-flush race"
    assert [r for r in caplog.records if r.getMessage() == "tick_flush_skipped_store_closed"] == []
    assert seen["pending_after"] == 0                   # the staged tick was flushed by close()
    assert _persisted_volumes(env) == [100, 101]        # in-flight batch completed + the one behind it
    assert len(seen["stopped_msgs"]) == 1
    assert env.lifecycle_row()["state"] == "STOPPED"


# --------------------------------------------------------------------------- the race: degrades gracefully
def test_case23_late_flush_after_close_restages_with_explicit_log(tmp_path, monkeypatch, caplog):
    """A size-due tick burst whose bus handlers only get to run AFTER ``store.close()`` — the hazard an
    undrained bus leaves (see the drain DEFECT below). The orphaned flush must degrade gracefully:
    batch restaged, one explicit ``tick_flush_skipped_store_closed`` warning, nothing raises."""
    env = RigEnv(tmp_path, monkeypatch, start=BOOT_AT)

    async def scenario() -> dict:
        env.at(BOOT_AT)
        proc = EngineProcess(env, bar_builder=True, ticker=FakeTicker())
        await proc.boot()
        store, loop = proc.store, asyncio.get_running_loop()
        burst = store._max_buffered_ticks               # size-due on the last tick of the burst
        real_close = store.close

        def close_with_undelivered_burst() -> None:
            t0 = env.clock.now()
            for i in range(burst):                      # delivered on the NEXT loop turn: after close
                proc.bus.publish("tick", _tick(t0 + timedelta(milliseconds=i), vol=100 + i))
            real_close()

        store.close = close_with_undelivered_burst
        sent0 = len(env.sent)
        await proc.stop()
        for task in _bus_deliveries(loop):
            await asyncio.wait_for(task, timeout=SLOW_FLUSH_MAX_S)
        # A flush landing after close() resolves on the store's (re-created) mt-flush pool.
        for _ in range(200):
            if any(r.getMessage() == "tick_flush_skipped_store_closed" for r in caplog.records):
                break
            await asyncio.sleep(0.01)
        seen = {
            "pending_after": store.pending_tick_count, "burst": burst,
            "stopped_msgs": [m for m in env.messages(since=sent0) if m.kind == MessageKind.ENGINE_STOPPED],
        }
        real_close()                                    # hygiene: release the pool the late flush re-created
        return seen

    with caplog.at_level(logging.INFO, logger=STORE_LOGGER):
        seen = _run_bounded(scenario)

    restaged = [r for r in caplog.records if r.getMessage() == "tick_flush_skipped_store_closed"]
    assert len(restaged) == 1 and restaged[0].ticks == seen["burst"]
    assert seen["pending_after"] == seen["burst"]       # restaged, not silently dropped
    assert _tracebacks(caplog) == []                    # degraded, never crashed
    assert len(seen["stopped_msgs"]) == 1
    assert env.lifecycle_row()["state"] == "STOPPED"


# --------------------------------------------------------------------------- bus drained before close
def test_case23_bus_handlers_drained_before_store_close(tmp_path, monkeypatch):
    """CD-6 (fixed 2026-09-24): teardown now awaits ``bus.drain`` before ``store.close()``."""
    env = RigEnv(tmp_path, monkeypatch, start=BOOT_AT)

    async def scenario() -> list[str]:
        proc, release = await _boot_and_start_in_flight_flush(env)
        store, loop = proc.store, asyncio.get_running_loop()
        real_close = store.close
        pending_at_close: list[str] = []

        def spy_close() -> None:
            pending_at_close.extend(t.get_name() for t in _bus_deliveries(loop))
            release.set()
            real_close()

        store.close = spy_close
        await proc.stop()
        for task in _bus_deliveries(loop):
            await asyncio.wait_for(task, timeout=SLOW_FLUSH_MAX_S)
        return pending_at_close

    pending_at_close = _run_bounded(scenario)
    assert pending_at_close == [], f"bus handler deliveries still pending at store.close(): {pending_at_close}"


# --------------------------------------------------------------------------- wedged past the bound
def test_case23_flush_wedged_past_close_bound_degrades_gracefully(tmp_path, monkeypatch, caplog):
    """CD-7 (fixed 2026-09-24): the flush close() gave up on used to raise and lose its batch."""
    import engine.marketdata.store as store_mod
    import tests.chaos._lifecycle_rig as rig_mod

    monkeypatch.setattr(store_mod, "_CLOSE_FLUSH_WAIT_S", 0.3)   # the live bound is 15 s; same code path
    monkeypatch.setattr(rig_mod, "_SHUTDOWN_BUS_DRAIN_S", 0.3)   # the drain gives up on the wedge too
    env = RigEnv(tmp_path, monkeypatch, start=BOOT_AT)

    async def scenario() -> dict:
        proc, release = await _boot_and_start_in_flight_flush(env)
        store, loop = proc.store, asyncio.get_running_loop()
        real_close = store.close

        def close_gives_up_on_wedged_flush() -> None:
            real_close()                                # waits 0.3 s on the wedged flush, then closes
            release.set()                               # …and only then does the wedge clear

        store.close = close_gives_up_on_wedged_flush
        await proc.stop()
        for task in _bus_deliveries(loop):
            await asyncio.wait_for(task, timeout=SLOW_FLUSH_MAX_S)
        return {"pending_after": store.pending_tick_count}

    with caplog.at_level(logging.INFO, logger=STORE_LOGGER):
        seen = _run_bounded(scenario)

    restaged = [r for r in caplog.records if r.getMessage() == "tick_flush_skipped_store_closed"]
    assert _tracebacks(caplog) == [], _tracebacks(caplog)
    assert restaged and restaged[0].ticks >= 1, "the in-flight batch was neither written nor restaged"
    assert seen["pending_after"] == 2                   # in-flight batch restaged + the staged tick
