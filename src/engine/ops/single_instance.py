"""OS-level single-instance lock — the PRIMARY mutual-exclusion guard.

Two engine instances against the single-writer stores (sqlite ``state.db``, the DuckDB
:class:`~engine.marketdata.store.MarketStore`, the :8400 login socket, the Telegram long-poll) corrupt
them. The ``engine_lifecycle`` check in :meth:`engine.ops.lifecycle.SessionLifecycle.startup` is a
check-then-act on a row committed only deep into boot: an instance that wedges BEFORE ``startup`` is
invisible to it, and two simultaneous boots can both pass the read (TOCTOU).

This module is the real mutual exclusion: an exclusive **OS file lock** acquired at the very top of
``engine.ops.main.run`` — BEFORE any shared resource is touched. The kernel RELEASES it on ANY process
death (clean exit, crash, ``taskkill /F``, power loss), so unlike a pidfile there is no stale lock to
reap. A second boot that finds the lock held exits (``main`` returns 3).

The ``engine_lifecycle`` DB check stays as the SECONDARY guard: it drives ``crash_recovered`` detection
and the pid-alive semantics this lock has no need for.

Platform (detected via ``import msvcrt``, not a ``sys.platform`` string):

* Windows — ``msvcrt.locking(LK_NBLCK, 1)`` on byte 0, a MANDATORY byte-range lock: the region is
  unreadable AND unwritable by any OTHER handle. So the recorded pid lives at **offset 1**;
  :meth:`InstanceLock.holder_pid` reads from there so a refused instance can name the holder. The pid is
  written THROUGH the locking handle — the only handle allowed to write the locked region.
* POSIX (CI / tests) — ``fcntl.flock(LOCK_EX | LOCK_NB)``. Advisory, and conflicts across separate
  open file descriptions (a second handle in the same process is refused too).

GC HAZARD: the OS lock lives exactly as long as the file handle stays open. The handle is held on
``self._fh`` — **if that reference is ever dropped, GC closes the fd and SILENTLY releases the lock**.
``engine.ops.main.run`` keeps the ``InstanceLock`` in a local for the entire ``run`` body; never
reassign ``self._fh`` while locked, and never let the object go out of scope while the engine is up.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import IO

try:
    import msvcrt

    _HAVE_MSVCRT = True
except ImportError:  # POSIX (Linux/macOS CI) — advisory flock fallback keeps the tests portable
    import fcntl

    _HAVE_MSVCRT = False

from engine.core.log import get_logger

_log = get_logger("engine.ops.single_instance")

#: Byte 0 is the pure lock byte (mandatory-unreadable to other handles on Windows); the pid record
#: starts at offset 1 so a REFUSED acquirer can still read it for the diagnostics log.
_PID_OFFSET = 1


class InstanceLock:
    """Exclusive OS file lock enforcing a single engine instance (PRIMARY guard).

    One per ``<data_dir>/engine.lock``; same data dir == same platform instance. Non-blocking
    throughout — :meth:`acquire` never waits on a live holder, it refuses.
    """

    def __init__(self, path: Path) -> None:
        self._path = Path(path)
        # Held for the lock's LIFETIME (see the module GC-hazard note). ``None`` ⇒ not held.
        self._fh: IO[bytes] | None = None
        self._locked = False

    # ------------------------------------------------------------------ acquire / release
    def acquire(self) -> bool:
        """Non-blocking acquire. ``True`` ⇒ this object holds the lock (idempotent). ``False`` ⇒
        another process/handle owns it — this caller must not touch the single-writer stores."""
        if self._locked:
            return True
        self._path.parent.mkdir(parents=True, exist_ok=True)
        # r+b, create-if-missing, NEVER truncate on open: truncating a region another handle holds
        # locked fails on Windows, and we must not clobber a live holder's pid record.
        fh = os.fdopen(os.open(self._path, os.O_RDWR | os.O_CREAT, 0o644), "r+b")
        try:
            _lock_region(fh)
        except OSError:
            # Held elsewhere — refused. Close OUR handle so no fd leaks.
            _close_quietly(fh)
            return False
        self._fh = fh  # hold the handle for the lock's lifetime (GC-hazard: never drop this ref)
        self._locked = True
        self._write_pid()  # best-effort diagnostics; a write failure NEVER fails the acquisition
        return True

    def release(self) -> None:
        """Unlock + close (idempotent, never raises; safe on a never-acquired lock). Tidiness only: the
        kernel releases the OS lock on process death anyway."""
        fh = self._fh
        if fh is None:
            return
        try:
            _unlock_region(fh)
        except OSError:
            # Best-effort: the kernel releases the region on the close below.
            _log.debug("instance_lock_unlock_failed", path=str(self._path))
        _close_quietly(fh)
        self._fh = None
        self._locked = False

    def holder_pid(self) -> int | None:
        """Best-effort pid recorded in the lock file (refusal-log diagnostics). Re-opens READ-ONLY at
        offset 1 (byte 0 is the mandatory-locked byte), so it works whoever holds the lock. A
        missing/empty/garbled record ⇒ ``None``."""
        try:
            with open(self._path, "rb") as ro:
                ro.seek(_PID_OFFSET)
                raw = ro.read(64)
        except OSError:
            return None
        return _parse_pid(raw)

    # ------------------------------------------------------------------ internals
    def _write_pid(self) -> None:
        """Record ``os.getpid()`` at offset 1 (byte 0 stays a ``\\n`` placeholder lock byte), written
        THROUGH the locking handle. Best-effort: a failure never fails :meth:`acquire`."""
        fh = self._fh
        if fh is None:  # pragma: no cover - only called right after a successful acquire
            return
        try:
            fh.seek(0)
            fh.write(b"\n" + f"{os.getpid()}\n".encode("ascii"))
            fh.flush()
        except OSError:
            _log.debug("instance_lock_pid_write_failed", path=str(self._path))
            return
        try:
            # Drop stale trailing bytes from a longer prior pid record; the read path tolerates junk anyway.
            fh.truncate()
        except OSError:  # pragma: no cover - a platform that refuses truncate over a locked region
            pass


# --------------------------------------------------------------------------- platform lock primitives
def _lock_region(fh: IO[bytes]) -> None:
    """Take the exclusive, non-blocking lock on byte 0. Raises ``OSError`` when the region is held."""
    fh.seek(0)  # msvcrt.locking operates at the CURRENT file position — seek(0) FIRST
    if _HAVE_MSVCRT:
        msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
    else:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


def _unlock_region(fh: IO[bytes]) -> None:
    fh.seek(0)
    if _HAVE_MSVCRT:
        msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
    else:
        fcntl.flock(fh.fileno(), fcntl.LOCK_UN)


def _parse_pid(raw: bytes) -> int | None:
    text = raw.decode("ascii", "ignore").strip()
    if not text:
        return None
    try:
        return int(text.split()[0])  # first token; tolerates trailing junk from an un-truncated write
    except ValueError:
        return None


def _close_quietly(fh: IO[bytes]) -> None:
    try:
        fh.close()
    except OSError:  # pragma: no cover - defensive; a close failure must never propagate
        pass
