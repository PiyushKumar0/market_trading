"""Liveness heartbeat writer + process-liveness probe (§2.2/§4.2, pinned).

While the engine runs, ``engine_lifecycle.last_alive_at`` is refreshed every
``lifecycle.heartbeat_write_s`` (default 20 s) by a **dedicated OS thread doing a direct ``sqlite3``
UPDATE — NOT the asyncio loop** (§2.2). A legitimately busy loop (multi-day catch-up backfill, a long
DuckDB scan) therefore cannot starve the heartbeat and self-alarm the out-of-band watchdog. The thread
owns its **own** sqlite3 connection (opened inside the thread; never shared with the loop's
connection), started at §2.6 step-0 boot right after the atomic RUNNING commit and stopped with a
clean ``join`` after the STOPPED commit at shutdown.

A missed write is logged and retried on the next interval — the heartbeat must never crash the
engine; the watchdog's ``down_stale_s`` (~3× the write interval) absorbs transient failures.

Also home to :func:`pid_alive` — the OS process-liveness probe used by the §2.6 step-0
single-instance guard (prior ``state ∈ {RUNNING, STOPPING}`` + prior pid ALIVE ⇒ refuse to start).
On Windows this uses ``ctypes`` ``OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION)`` +
``GetExitCodeProcess`` (a pid whose process has exited — or was never ours — probes dead); the POSIX
fallback is ``os.kill(pid, 0)``. Documented caveats: pid reuse can rarely alias a dead engine to an
unrelated live process (the guard then refuses a start that manual inspection must clear), and
``STILL_ACTIVE`` (259) is Windows' sentinel exit code. ``scripts/watchdog.py`` carries its own copy of
this probe — it may import only ``core.secrets``/``core.config``/httpx/stdlib (§3.2.12), never
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
    """True iff ``pid`` names a live OS process (§2.6 step-0 single-instance probe)."""
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
    """The dedicated-thread ``engine_lifecycle.last_alive_at`` writer (§2.2 pinned mechanism).

    Parameters
    ----------
    db_path:
        Path to ``state.db``. The thread opens its OWN connection against it (WAL mode set by the
        engine's main connection makes concurrent one-row UPDATEs safe).
    clock:
        The single source of "now" (§3.2) — ``Clock.now()`` is a pure call, safe off-loop.
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
        """Start the heartbeat thread (idempotent). Called at §2.6 step 0, right after the atomic
        RUNNING commit — the boot commit itself wrote the first fresh ``last_alive_at``."""
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="mt-heartbeat", daemon=True)
        self._thread.start()
        _log.info("heartbeat_started", interval_s=self._interval_s, db=self._db_path)

    def stop(self, timeout_s: float = 10.0) -> None:
        """Signal the thread and join it cleanly (idempotent). Called after the STOPPED commit —
        keeping the heartbeat alive through STOPPING means a long shutdown guard is never mistaken
        for a wedge by the watchdog (§2.2)."""
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
            # Own connection, opened inside the thread — never the loop's (§2.2). Default
            # isolation_level autocommits are avoided: explicit commit per write.
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
