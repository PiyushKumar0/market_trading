"""OS-level single-instance lock (§2.6 step-0 PRIMARY guard) — in-process semantics + subprocess proof.

Closes the 2026-07-21 double-run follow-up. The ``engine_lifecycle`` DB guard in ``lifecycle.startup``
is a TOCTOU check-then-act an instance can slip past by wedging before the RUNNING commit;
:class:`~engine.ops.single_instance.InstanceLock` is the real mutual exclusion — an exclusive OS file
lock. These tests assert the in-process semantics (idempotent acquire/release, cross-handle refusal,
pid diagnostics) AND the actual cross-PROCESS guarantee via a spawned ``sys.executable`` that
constructs the SAME lock — including that the KERNEL releases it on process death (no stale lock, the
whole reason a file lock beats a pidfile).

Windows is the deployment platform and the ``msvcrt`` byte-range path is the POINT of this guard, so
these tests deliberately DO NOT skip on Windows and avoid POSIX-only APIs in the test body.
"""

from __future__ import annotations

import os
import subprocess
import sys

from engine.ops.single_instance import InstanceLock

# A tiny program a child interpreter runs: construct InstanceLock on argv[1], try to acquire, print the
# verdict, and EXIT WITHOUT releasing — so a subsequent parent re-acquire proves the KERNEL released
# the lock on process death. Importable because ``market-trading`` is installed in the same uv venv
# this test runs under (``sys.executable`` is that venv's python; run tests via ``uv run pytest``).
_CHILD = (
    "import sys\n"
    "from engine.ops.single_instance import InstanceLock\n"
    "lock = InstanceLock(sys.argv[1])\n"
    "print('RESULT=' + ('ACQUIRED' if lock.acquire() else 'REFUSED'))\n"
)


def _spawn(lock_path) -> str:
    proc = subprocess.run(
        [sys.executable, "-c", _CHILD, str(lock_path)],
        capture_output=True, text=True, timeout=60,
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
        # A second handle on the SAME path conflicts — Windows byte-range lock and POSIX
        # flock-on-a-new-open-file-description both refuse across handles, even in one process.
        assert second.acquire() is False
        # Diagnostics survive the refusal: the recorded holder pid is still readable (offset 1).
        assert second.holder_pid() == os.getpid()
        # The refusal left the real holder untouched, and re-acquire on it is idempotent.
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
        # A genuinely separate PROCESS is refused while the parent holds the lock (the real guarantee).
        assert "RESULT=REFUSED" in _spawn(lock_path)

        parent.release()
        # Freed: the next process acquires it. It exits WITHOUT calling release()...
        assert "RESULT=ACQUIRED" in _spawn(lock_path)
        # ...and because the KERNEL releases the lock on process death, the parent can re-acquire —
        # no stale lock to reap (the pidfile failure mode this design exists to avoid).
        assert parent.acquire() is True
    finally:
        parent.release()
