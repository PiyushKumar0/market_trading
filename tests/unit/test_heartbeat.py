"""HeartbeatWriter — the dedicated-thread liveness writer (§2.2 pinned mechanism).

The load-bearing property: the heartbeat keeps writing ``engine_lifecycle.last_alive_at`` while the
MAIN thread is busy (a long backfill/scan would starve an asyncio-loop-hosted heartbeat and
self-alarm the watchdog — the whole reason §2.2 pins a dedicated OS thread with its own sqlite3
connection). Uses the real migrated temp SQLite db from conftest.
"""

from __future__ import annotations

import sqlite3
import time as time_mod
from datetime import timedelta

from engine.core.clock import Clock
from engine.ops.heartbeat import HeartbeatWriter, pid_alive
from tests.conftest import FIXED_NOW


def _read_last_alive(db_path: str) -> str | None:
    c = sqlite3.connect(db_path)
    try:
        row = c.execute("SELECT last_alive_at FROM engine_lifecycle WHERE id=1").fetchone()
        return row[0] if row else None
    finally:
        c.close()


def test_heartbeat_writes_while_main_thread_is_busy(conn, db_path):
    """The thread must make progress while the main thread spins (GIL-bound busy work) — writes
    accumulate and last_alive_at lands in the row, advancing with the (test-advanced) clock."""
    ticks = {"n": 0}

    def advancing_now():
        ticks["n"] += 1
        return FIXED_NOW + timedelta(seconds=ticks["n"])

    hb = HeartbeatWriter(db_path, Clock(time_source=advancing_now), interval_s=0.02)
    hb.start()
    try:
        # Busy-spin the main thread (~0.4 s). A loop-hosted heartbeat would be starved by exactly
        # this pattern; the dedicated thread keeps beating through it (§2.2).
        t0 = time_mod.perf_counter()
        while time_mod.perf_counter() - t0 < 0.4:
            pass
        deadline = time_mod.perf_counter() + 2.0
        while hb.writes < 3 and time_mod.perf_counter() < deadline:
            time_mod.sleep(0.01)
    finally:
        hb.stop()
    assert hb.writes >= 3
    assert hb.last_error is None
    first = _read_last_alive(db_path)
    assert first is not None and first.startswith("2026-06-17")


def test_heartbeat_start_stop_idempotent(db_path, clock):
    hb = HeartbeatWriter(db_path, clock, interval_s=0.02)
    hb.start()
    hb.start()  # second start is a no-op, not a second thread
    assert hb.running is True
    hb.stop()
    assert hb.running is False
    hb.stop()  # idempotent
    assert hb.running is False


def test_heartbeat_survives_write_failure(tmp_path, clock):
    """A missing table (schema not migrated / disk trouble) must never crash the thread — the miss
    is logged and retried; the watchdog's down_stale_s absorbs it (§2.2)."""
    bare = tmp_path / "bare.db"
    sqlite3.connect(str(bare)).close()  # empty db: no engine_lifecycle table
    hb = HeartbeatWriter(bare, clock, interval_s=0.02)
    hb.start()
    try:
        deadline = time_mod.perf_counter() + 2.0
        while hb.last_error is None and time_mod.perf_counter() < deadline:
            time_mod.sleep(0.01)
        assert hb.running is True          # still alive despite failing writes
        assert hb.last_error is not None   # and the failure is observable
    finally:
        hb.stop()


def test_pid_alive_probe():
    """Own pid probes alive; an implausible pid probes dead (§2.6 step-0 single-instance guard)."""
    import os

    assert pid_alive(os.getpid()) is True
    assert pid_alive(None) is False
    assert pid_alive(-1) is False
    # A pid far beyond plausible allocation on this box (pids are multiples of 4 on Windows,
    # bounded well under this) — dead or inaccessible either way probes False.
    assert pid_alive(0x7FFFFFF0) is False
