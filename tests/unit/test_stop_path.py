"""Reliable engine stop path (§2.6/§10.8, 2026-07-21 13:34 IST zombie).

The incident: Ctrl-C landed while ``lifecycle.startup`` was still running (no ``engine_ready`` yet).
Signal handlers were installed only AFTER startup completed, so the press hit Python's default SIGINT
handler → a raw ``KeyboardInterrupt`` at an arbitrary ``await`` (``main`` logged ``engine_interrupted``),
and the process then NEVER EXITED — pid 46332 lingered holding ``data/engine.lock`` until a manual
taskkill. These tests pin the three seams the fix adds:

  * :func:`engine.ops.main._make_stop_handler` — the COUNTED two-press handler (first press requests a
    graceful stop, a second forces a hard exit), exercised WITHOUT raising real signals.
  * :func:`engine.ops.main._hard_exit` — the ``os._exit`` backstop that a wedged non-daemon thread
    cannot block (the interpreter's own atexit executor-join would hang forever).
  * wiring order — signal installation precedes ``connect()`` so the WHOLE boot is interruptible.
"""

from __future__ import annotations

import asyncio
import inspect
import subprocess
import sys

import pytest

from engine.ops import main as opsmain


# --------------------------------------------------------------------------- counted stop handler
@pytest.mark.asyncio
async def test_stop_handler_first_requests_second_forces() -> None:
    """First call sets the stop event and does NOT force-exit; the second calls force_exit(130) exactly
    once. Uses a stubbed ``force_exit`` (records the code) so no real signal / process exit occurs."""
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()
    codes: list[int] = []
    handler = opsmain._make_stop_handler(loop, stop_event, force_exit=codes.append)

    # FIRST signal — graceful: wakes the event (via call_soon_threadsafe), no force-exit.
    handler()
    assert codes == []
    await asyncio.sleep(0)                       # drain the loop so call_soon_threadsafe lands
    assert stop_event.is_set() is True

    # SECOND signal — hard exit exactly once, with code 130.
    handler()
    assert codes == [130]


@pytest.mark.asyncio
async def test_stop_handler_never_raises_and_accepts_signal_args() -> None:
    """The handler serves BOTH the zero-arg add_signal_handler callback and the (signum, frame)
    signal.signal fallback, and never raises (a raising signal handler would surface at a random await)."""
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()
    codes: list[int] = []
    handler = opsmain._make_stop_handler(loop, stop_event, force_exit=codes.append)

    handler(2, None)                            # (signum, frame) shape — first press
    await asyncio.sleep(0)
    assert stop_event.is_set() is True and codes == []
    handler(2, None)                            # second press forces
    assert codes == [130]


# --------------------------------------------------------------------------- hard-exit backstop
def test_hard_exit_forces_exit_past_a_wedged_nondaemon_thread() -> None:
    """A NON-daemon thread blocked forever must NOT be able to zombify the process: ``_hard_exit`` uses
    ``os._exit`` (skips the interpreter's executor/thread joins). The subprocess must exit code 7 within
    a bounded time — a plain ``sys.exit``/return there would hang on the thread join indefinitely."""
    script = (
        "import threading, engine.ops.main as m\n"
        "block = threading.Event()\n"
        "threading.Thread(target=block.wait, daemon=False).start()\n"   # non-daemon, blocks forever
        "m._hard_exit(7)\n"
    )
    proc = subprocess.run([sys.executable, "-c", script], timeout=90, capture_output=True)
    assert proc.returncode == 7


# --------------------------------------------------------------------------- wiring order (regression)
def test_signal_handlers_installed_before_connect() -> None:
    """2026-07-21 regression guard: ``_install_signal_handlers`` must be wired BEFORE ``connect()`` (and
    after the instance-lock acquire) so a wedged boot is interruptible. A runtime assertion would need a
    full boot; a source-order check over ``run`` is the cheap, robust equivalent."""
    src = inspect.getsource(opsmain.run)
    i_lock = src.index("instance_lock.acquire")
    i_install = src.index("_install_signal_handlers(stop_event)")
    i_connect = src.index("conn = connect(")
    assert i_lock < i_install < i_connect
    # The late (post-startup) install site is GONE — the handler is installed exactly once, early.
    assert src.count("_install_signal_handlers(stop_event)") == 1


def test_install_signal_handlers_defaults_to_os_exit() -> None:
    """The production default force-exit is ``os._exit`` (the injection seam only exists for tests)."""
    sig = inspect.signature(opsmain._install_signal_handlers)
    assert sig.parameters["force_exit"].default is __import__("os")._exit
