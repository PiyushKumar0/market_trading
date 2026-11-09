"""Liveness heartbeat writer + process-liveness probe.

While the engine runs, ``engine_lifecycle.last_alive_at`` is refreshed every
``lifecycle.heartbeat_write_s`` (default 20 s) by a **dedicated OS thread doing a direct ``sqlite3``
UPDATE — NOT the asyncio loop**, so a busy loop (catch-up backfill, a long DuckDB scan) cannot starve
it and self-alarm the out-of-band watchdog. The thread owns its **own** sqlite3 connection (opened
inside the thread, never the loop's), started right after the atomic RUNNING commit and joined after
the STOPPED commit at shutdown.

A missed write is logged and retried next interval; it must never crash the engine (the watchdog's
``down_stale_s``, ~3× the interval, absorbs transient failures).

Also home to :func:`pid_alive` — the OS process-liveness probe for the boot-time single-instance guard
(prior ``state ∈ {RUNNING, STOPPING}`` + prior pid ALIVE ⇒ refuse to start). Windows uses ``ctypes``
``OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION)`` + ``GetExitCodeProcess``; POSIX uses
``os.kill(pid, 0)``. Caveat: pid reuse can rarely alias a dead engine to an unrelated live process (the
guard then refuses a start that manual inspection must clear). ``scripts/watchdog.py`` carries its own
copy of this probe — it may import only ``core.secrets``/``core.config``/httpx/stdlib, never
``engine.ops``.
"""

from __future__ import annotations

import os
import sqlite3
import sys
import threading
from pathlib import Path

from engine.core.clock import Clock
from engine.core.log import get_logger

_log = get_logger("engine.ops.heartbeat")

_STILL_ACTIVE = 259  # Windows GetExitCodeProcess sentinel for a running process


def pid_alive(pid: int | None) -> bool:
    """True iff ``pid`` names a live OS process (boot-time single-instance probe)."""
    if not pid or pid <= 0:
        return False
    if sys.platform == "win32":
        import ctypes

        process_query_limited_information = 0x1000
        kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
        handle = kernel32.OpenProcess(process_query_limited_information, False, int(pid))
        if not handle:
            return False  # no such process (or inaccessible — same service account in practice)
        try:
            code = ctypes.c_ulong()
            if not kernel32.GetExitCodeProcess(handle, ctypes.byref(code)):
                return False
            return code.value == _STILL_ACTIVE
        finally:
            kernel32.CloseHandle(handle)
    try:
        os.kill(int(pid), 0)
    except OSError:
        return False
    return True


class HeartbeatWriter:
    """The dedicated-thread ``engine_lifecycle.last_alive_at`` writer.

    Parameters
    ----------
    db_path:
        Path to ``state.db``. The thread opens its OWN connection against it (WAL mode set by the
        engine's main connection makes concurrent one-row UPDATEs safe).
    clock:
        The single source of "now" — ``Clock.now()`` is a pure call, safe off-loop.
    interval_s:
        Write cadence (``lifecycle.heartbeat_write_s``).
    """

    def __init__(self, db_path: str | Path, clock: Clock, interval_s: float = 20.0) -> None:
        self._db_path = str(db_path)
        self._clock = clock
        self._interval_s = float(interval_s)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.writes = 0          # observable write count (tests / HealthMonitor)
        self.last_error: str | None = None

    # ------------------------------------------------------------------ lifecycle
    def start(self) -> None:
        """Start the heartbeat thread (idempotent), right after the atomic RUNNING commit."""
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="mt-heartbeat", daemon=True)
        self._thread.start()
        _log.info("heartbeat_started", interval_s=self._interval_s, db=self._db_path)

    def stop(self, timeout_s: float = 10.0) -> None:
        """Signal the thread and join it (idempotent). Called after the STOPPED commit, so a long
        shutdown is never mistaken for a wedge by the watchdog."""
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout_s)
            if self._thread.is_alive():  # pragma: no cover - defensive; daemon thread dies with us
                _log.warning("heartbeat_join_timeout", timeout_s=timeout_s)
            self._thread = None
        _log.info("heartbeat_stopped", writes=self.writes)

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    # ------------------------------------------------------------------ thread body
    def _run(self) -> None:
        conn: sqlite3.Connection | None = None
        try:
            # isolation_level=None = autocommit: no implicit transaction is left open across beats.
            conn = sqlite3.connect(self._db_path, timeout=5.0, isolation_level=None)
            conn.execute("PRAGMA busy_timeout=5000")
            while True:
                self._write_once(conn)
                if self._stop.wait(self._interval_s):
                    break
        except sqlite3.Error as exc:  # pragma: no cover - open failure; watchdog surfaces the stale beat
            self.last_error = str(exc)
            _log.error("heartbeat_connection_failed", error=str(exc))
        finally:
            if conn is not None:
                conn.close()

    def _write_once(self, conn: sqlite3.Connection) -> None:
        try:
            conn.execute(
                "UPDATE engine_lifecycle SET last_alive_at=? WHERE id=1",
                (self._clock.now().isoformat(),),
            )
            self.writes += 1
            self.last_error = None
        except sqlite3.Error as exc:
            # Never crash the thread: a missed beat is absorbed by down_stale_s (~3× interval).
            self.last_error = str(exc)
            _log.warning("heartbeat_write_failed", error=str(exc))
