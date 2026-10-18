"""Reliable engine stop path (§2.6/§10.8, 2026-07-21 13:34 IST zombie).

The incident: Ctrl-C landed while ``lifecycle.startup`` was still running (no ``engine_ready`` yet).
Signal handlers were installed only AFTER startup completed, so the press hit Python's default SIGINT
handler → a raw ``KeyboardInterrupt`` at an arbitrary ``await`` (``main`` logged ``engine_interrupted``),
and the process then NEVER EXITED — pid 46332 lingered holding ``data/engine.lock`` until a manual
taskkill. These tests pin the three seams the fix adds:

  * :func:`engine.ops.stop_path.make_stop_handler` — the COUNTED two-press handler (first press requests
    a graceful stop, a second forces a hard exit), exercised WITHOUT raising real signals.
  * :func:`engine.ops.stop_path.hard_exit` — the ``os._exit`` backstop that a wedged non-daemon thread
    cannot block (the interpreter's own atexit executor-join would hang forever).
  * wiring order — signal installation precedes the composition (``connect()``) so the WHOLE boot is
    interruptible.
"""

from __future__ import annotations

import asyncio
import inspect
import os
import subprocess
import sys
from pathlib import Path

import pytest

import engine
from engine.core.config import load_settings
from engine.ops import main as opsmain
from engine.ops import stop_path


# --------------------------------------------------------------------------- counted stop handler
class _FakeMonotonic:
    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t


@pytest.mark.asyncio
@pytest.mark.parametrize("args", [(), (2, None)], ids=["zero-arg", "signal-args"])
async def test_stop_handler_first_requests_second_forces(args: tuple) -> None:
    """First call sets the stop event and does NOT force-exit; a second call AFTER the repeat-grace
    window calls force_exit(130) exactly once. Uses a stubbed ``force_exit`` (records the code) so no
    real signal / process exit occurs.

    The handler serves BOTH the zero-arg add_signal_handler callback and the (signum, frame)
    signal.signal fallback, and never raises (a raising signal handler would surface at a random await)."""
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()
    codes: list[int] = []
    mono = _FakeMonotonic()
    handler = stop_path.make_stop_handler(loop, stop_event, force_exit=codes.append, monotonic=mono)

    # FIRST signal — graceful: wakes the event (via call_soon_threadsafe), no force-exit.
    handler(*args)
    assert codes == []
    await asyncio.sleep(0)                       # drain the loop so call_soon_threadsafe lands
    assert stop_event.is_set() is True and codes == []

    # SECOND signal past the grace window — hard exit exactly once, with code 130.
    mono.t += stop_path.REPEAT_SIGNAL_GRACE_S + 0.1
    handler(*args)
    assert codes == [130]


@pytest.mark.asyncio
async def test_stop_handler_ignores_repeat_signals_inside_grace_window() -> None:
    """2026-09-23: every `nssm stop` since 09-02 logged stop_forced ~0.85-1.4 s after stop_requested,
    mid shutdown-backup, so the clean stop was never committed. NSSM's console Ctrl-C reaches the whole
    console group more than once; repeats inside the grace window must NOT force-exit."""
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()
    codes: list[int] = []
    mono = _FakeMonotonic()
    handler = stop_path.make_stop_handler(loop, stop_event, force_exit=codes.append, monotonic=mono)

    handler(2, None)
    mono.t += 1.1                               # the observed NSSM repeat gap
    handler(2, None)
    mono.t += stop_path.REPEAT_SIGNAL_GRACE_S - 1.2   # still inside the window, measured from the FIRST
    handler(21, None)                           # a SIGBREAK repeat is treated the same
    await asyncio.sleep(0)
    assert stop_event.is_set() is True
    assert codes == []

    mono.t += 0.2                               # now past the window: a deliberate press still forces
    handler(2, None)
    assert codes == [130]


# --------------------------------------------------------------------------- hard-exit backstop
def test_hard_exit_forces_exit_past_a_wedged_nondaemon_thread() -> None:
    """A NON-daemon thread blocked forever must NOT be able to zombify the process: ``hard_exit`` uses
    ``os._exit`` (skips the interpreter's executor/thread joins). The subprocess must exit code 7 within
    a bounded time — a plain ``sys.exit``/return there would hang on the thread join indefinitely."""
    script = (
        "import threading, engine.ops.stop_path as m\n"
        "block = threading.Event()\n"
        "threading.Thread(target=block.wait, daemon=False).start()\n"   # non-daemon, blocks forever
        "m.hard_exit(7)\n"
    )
    # The child imports the engine under test, not whatever copy the interpreter has installed.
    env = {**os.environ, "PYTHONPATH": str(Path(engine.__file__).parents[1])}
    proc = subprocess.run([sys.executable, "-c", script], timeout=90, capture_output=True, env=env)
    assert proc.returncode == 7


# --------------------------------------------------------------------------- wiring order (regression)
class _Composed(Exception):
    """Ends ``run`` at the composition step."""


@pytest.mark.asyncio
async def test_signal_handlers_installed_before_connect(tmp_path, monkeypatch) -> None:
    """2026-07-21 regression guard: the signal handlers must be wired BEFORE ``connect()`` — i.e. before
    ``compose``, which opens every shared resource — and after the instance-lock acquire, so a wedged
    boot is interruptible. ``run`` is driven for real up to ``compose``, which ends it."""
    monkeypatch.setenv("MT_DATA_DIR", str(tmp_path / "data"))
    settings = load_settings()
    events: list[str] = []
    installed: list[asyncio.Event] = []
    composed: list[asyncio.Event] = []

    class _Lock:
        def __init__(self, path) -> None:
            assert path == settings.resolved_data_dir() / "engine.lock"

        def acquire(self) -> bool:
            events.append("lock")
            return True

    class _Secrets:   # run() constructs the real one; it must never be built in a test
        pass

    def _install(stop_event: asyncio.Event) -> None:
        events.append("signal_handlers")
        installed.append(stop_event)

    def _watchdog(state: dict):
        events.append("watchdog")

        async def _retired() -> None:
            return None

        return _retired()

    async def _compose(_settings, *, secrets, clock, stop_event, boot_state):
        events.append("compose")
        assert _settings is settings and isinstance(secrets, _Secrets)
        composed.append(stop_event)
        raise _Composed

    monkeypatch.setattr(opsmain, "load_settings", lambda: settings)
    monkeypatch.setattr(opsmain, "configure_logging", lambda **_kw: None)
    monkeypatch.setattr(opsmain, "InstanceLock", _Lock)
    monkeypatch.setattr(opsmain, "Secrets", _Secrets)
    monkeypatch.setattr(opsmain, "install_signal_handlers", _install)
    monkeypatch.setattr(opsmain, "boot_contract_watchdog", _watchdog)
    monkeypatch.setattr(opsmain, "compose", _compose)

    with pytest.raises(_Composed):
        await opsmain.run()
    await asyncio.sleep(0)                      # let the stand-in watchdog task finish

    assert events == ["lock", "signal_handlers", "watchdog", "compose"]
    assert installed[0] is composed[0]           # the event the handlers set is the one run idles on


def test_install_signal_handlers_defaults_to_os_exit() -> None:
    """The production default force-exit is ``os._exit`` (the injection seam only exists for tests)."""
    sig = inspect.signature(stop_path.install_signal_handlers)
    assert sig.parameters["force_exit"].default is os._exit
