"""OS-level single-instance lock (PRIMARY guard) — in-process semantics + subprocess proof.

The ``engine_lifecycle`` DB guard in ``lifecycle.startup`` is a TOCTOU check-then-act;
:class:`~engine.ops.single_instance.InstanceLock` (an exclusive OS file lock) is the real mutual
exclusion. Asserts in-process semantics (idempotent acquire/release, cross-handle refusal, pid
diagnostics) AND the cross-PROCESS guarantee via a spawned ``sys.executable``, including that the
KERNEL releases the lock on process death (no stale lock, the reason a file lock beats a pidfile).

Windows is the deployment platform and the ``msvcrt`` byte-range path is the point, so these tests do
NOT skip on Windows and avoid POSIX-only APIs.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import engine
from engine.ops.single_instance import InstanceLock

# Child program: try to acquire argv[1], print the verdict, and EXIT WITHOUT releasing, so a parent
# re-acquire proves the KERNEL released the lock. PYTHONPATH puts THIS process's ``src`` first, so the
# child imports the code under test, not the editable install's tree.
_CHILD = (
    "import sys\n"
    "from engine.ops.single_instance import InstanceLock\n"
    "lock = InstanceLock(sys.argv[1])\n"
    "print('RESULT=' + ('ACQUIRED' if lock.acquire() else 'REFUSED'))\n"
)
_SRC = str(Path(engine.__file__).parents[1])


def _spawn(lock_path) -> str:
    proc = subprocess.run(
        [sys.executable, "-c", _CHILD, str(lock_path)],
        env={**os.environ, "PYTHONPATH": _SRC}, capture_output=True, text=True, timeout=60,
    )
    assert proc.returncode == 0, f"child failed rc={proc.returncode}: {proc.stdout!r} {proc.stderr!r}"
    return proc.stdout


def test_acquire_creates_lockfile_and_records_pid(tmp_path) -> None:
    lock_path = tmp_path / "engine.lock"
    lock = InstanceLock(lock_path)
    try:
        assert lock.acquire() is True
        assert lock_path.exists()
        assert lock.holder_pid() == os.getpid()
    finally:
        lock.release()


def test_second_object_same_path_is_refused(tmp_path) -> None:
    lock_path = tmp_path / "engine.lock"
    first = InstanceLock(lock_path)
    second = InstanceLock(lock_path)
    try:
        assert first.acquire() is True
        # Windows byte-range lock and POSIX flock both refuse across handles, even in one process.
        assert second.acquire() is False
        # The recorded holder pid survives the refusal (offset 1).
        assert second.holder_pid() == os.getpid()
        assert first.holder_pid() == os.getpid()
        assert first.acquire() is True
    finally:
        first.release()
        second.release()


def test_release_then_reacquire_via_second_object(tmp_path) -> None:
    lock_path = tmp_path / "engine.lock"
    first = InstanceLock(lock_path)
    second = InstanceLock(lock_path)
    try:
        assert first.acquire() is True
        assert second.acquire() is False
        first.release()
        assert second.acquire() is True  # freed → the other object can now take it
    finally:
        first.release()
        second.release()


def test_acquire_and_release_are_idempotent(tmp_path) -> None:
    lock_path = tmp_path / "engine.lock"
    lock = InstanceLock(lock_path)
    assert lock.acquire() is True
    assert lock.acquire() is True  # same object, no double-lock attempt, no error
    lock.release()
    lock.release()  # second release is a guarded no-op
    # Releasing a lock that was never acquired is also a guarded no-op.
    InstanceLock(tmp_path / "never.lock").release()


def test_subprocess_is_refused_while_held_then_kernel_releases(tmp_path) -> None:
    lock_path = tmp_path / "engine.lock"
    parent = InstanceLock(lock_path)
    try:
        assert parent.acquire() is True
        assert "RESULT=REFUSED" in _spawn(lock_path)

        parent.release()
        # The next process acquires it and exits WITHOUT release(); the KERNEL frees the lock on
        # process death, so the parent re-acquires with no stale lock to reap.
        assert "RESULT=ACQUIRED" in _spawn(lock_path)
        assert parent.acquire() is True
    finally:
        parent.release()
