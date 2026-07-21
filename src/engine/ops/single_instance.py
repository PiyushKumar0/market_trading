"""OS-level single-instance lock — the §2.6 step-0 PRIMARY mutual-exclusion guard (O7/E4).

2026-07-21 incident: TWO engine instances ran concurrently against the single-writer stores (the
sqlite ``state.db``, the DuckDB :class:`~engine.marketdata.store.MarketStore`, the :8400 login
socket, the Telegram long-poll). The §2.6 step-0 guard in
:meth:`engine.ops.lifecycle.SessionLifecycle.startup` reads the ``engine_lifecycle`` row and refuses
when a prior RUNNING/STOPPING pid is still alive — but it is a **check-then-act on a DB row whose
RUNNING commit happens only deep into boot**. An instance that wedges BEFORE ``startup`` (the 10:35
boot did, inside the warm-up backfill) never wrote RUNNING and is invisible to that guard; and two
simultaneous boots can both pass the read (TOCTOU). Neither is real mutual exclusion.

This module IS the real one: an exclusive **OS file lock** acquired at the very top of
``engine.ops.main.run`` — BEFORE any shared resource is touched (sqlite connect, ``MarketStore.open``,
Telegram polling, the :8400 bind). The kernel holds the lock for the owning process and RELEASES it on
ANY process death — clean exit, crash, ``taskkill /F``, power loss — so, unlike a pidfile, there is NO
stale-lock problem to reap on the next boot. A second boot that finds the lock held exits
(``main`` returns 3) instead of corrupting the single-writer stores.

The ``engine_lifecycle`` DB check in ``lifecycle.startup`` STAYS as the SECONDARY, belt-and-suspenders
guard: it is what drives crash-recovered detection (``crash_recovered``) and carries the pid-alive
semantics this lock has no need for. This file lock is PRIMARY; that DB check is defence in depth.

Platform (detected via ``import msvcrt``, not a ``sys.platform`` string):

* Windows — ``msvcrt.locking(LK_NBLCK, 1)`` on byte 0. This is a MANDATORY byte-range lock: a locked
  region is unreadable AND unwritable by any OTHER handle (verified 2026-07-21). Because the lock byte
  is therefore off-limits to a refused acquirer, the recorded pid lives at **offset 1** (byte 0 is a
  pure lock byte); :meth:`InstanceLock.holder_pid` reads from offset 1 so a refused instance can still
  name the holder in its refusal log. The pid is written THROUGH the locking handle — the only handle
  allowed to write the locked region on Windows.
* POSIX (CI / tests) — ``fcntl.flock(LOCK_EX | LOCK_NB)``. Advisory, and conflicts across separate
  open file descriptions (so a second handle in the same process is refused too), keeping the unit +
  subprocess tests portable.

GC HAZARD: the OS lock lives exactly as long as the underlying file handle stays open. The handle is
held on ``self._fh`` for the lock's whole lifetime — **if that reference is ever dropped, GC closes
the fd and SILENTLY releases the lock**. ``engine.ops.main.run`` keeps the ``InstanceLock`` in a local
for the entire ``run`` body; never reassign ``self._fh`` while locked, and never let the object go
out of scope while the engine is up.
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
    """Exclusive OS file lock enforcing a single engine instance (§2.6 step-0 PRIMARY guard).

    One :class:`InstanceLock` per ``<data_dir>/engine.lock``. Same data dir == same platform instance
    (``settings.resolved_data_dir()`` already honours ``MT_DATA_DIR``); this invents no new path
    convention. Non-blocking throughout — :meth:`acquire` never waits on a live holder, it refuses.
    """

    def __init__(self, path: Path) -> None:
        self._path = Path(path)
        # Handle held here for the lock's LIFETIME — dropping this reference lets GC close the fd and
        # SILENTLY release the OS lock (see the module GC-hazard note). ``None`` ⇒ not currently held.
        self._fh: IO[bytes] | None = None
        self._locked = False

    # ------------------------------------------------------------------ acquire / release
    def acquire(self) -> bool:
        """Non-blocking acquire. ``True`` ⇒ this object holds the lock (idempotent: ``True`` again if
        already held by THIS object). ``False`` ⇒ another process/handle owns it — this caller must
        not touch the single-writer stores."""
        if self._locked:
            return True  # idempotent — this object already holds it; no second lock attempt
        self._path.parent.mkdir(parents=True, exist_ok=True)
        # r+b, create-if-missing, NEVER truncate on open: truncating a region another handle holds
        # locked fails on Windows, and we must not clobber a live holder's pid record.
        fh = os.fdopen(os.open(self._path, os.O_RDWR | os.O_CREAT, 0o644), "r+b")
        try:
            _lock_region(fh)
        except OSError:
            # Held by another process (or another handle in this process) — refused. Close OUR handle
            # so we never leak an fd; holder_pid() re-opens read-only to name the holder.
            _close_quietly(fh)
            return False
        self._fh = fh  # hold the handle for the lock's lifetime (GC-hazard: never drop this ref)
        self._locked = True
        self._write_pid()  # best-effort diagnostics; a write failure NEVER fails the acquisition
        return True

    def release(self) -> None:
        """Unlock + close (idempotent; guarded — never raises). Safe to call twice, and safe to call on
        a lock that was never acquired. Cosmetic tidiness only: every real exit path is already covered
        by the kernel releasing the OS lock on process death."""
        fh = self._fh
        if fh is None:
            return  # never acquired, or already released
        try:
            _unlock_region(fh)
        except OSError:
            # Best-effort: the kernel releases the region on the close below (and on process death
            # regardless), so a failed explicit unlock is never fatal.
            _log.debug("instance_lock_unlock_failed", path=str(self._path))
        _close_quietly(fh)
        self._fh = None
        self._locked = False

    def holder_pid(self) -> int | None:
        """Best-effort pid recorded in the lock file (diagnostics for the refusal log). Re-opens the
        file READ-ONLY and reads the pid record at offset 1 (byte 0 is the mandatory-locked lock byte,
        unreadable to any other handle on Windows), so it works whether WE hold the lock or another
        process does — and still works after a refusal. A missing/empty/garbled record ⇒ ``None``."""
        try:
            with open(self._path, "rb") as ro:
                ro.seek(_PID_OFFSET)
                raw = ro.read(64)
        except OSError:
            return None
        return _parse_pid(raw)

    # ------------------------------------------------------------------ internals
    def _write_pid(self) -> None:
        """Record ``os.getpid()`` for the refusal diagnostics. Byte 0 stays a pure lock byte (a ``\\n``
        placeholder); the pid record starts at offset 1 so a REFUSED acquirer can read it. Written
        THROUGH the locking handle (the only handle allowed to write the locked region on Windows).
        Wholly best-effort — any failure leaves the lock held with no recorded pid, never fails
        :meth:`acquire`."""
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
            # Drop any stale trailing bytes from a longer prior pid record. Verified to work through
            # the locking handle on Windows (2026-07-21); the read path tolerates junk regardless.
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
