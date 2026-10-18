"""The engine's stop path (2026-07-21 13:34 IST zombie): the counted signal handler and the ``os._exit``
backstop. Stdlib + ``engine.core.log`` only, so a process can import it without the composition root."""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import threading
import time
from collections.abc import Callable
from typing import NoReturn

from engine.core.log import get_logger

_log = get_logger("engine.ops.stop_path")

REPEAT_SIGNAL_GRACE_S = 10.0


def make_stop_handler(
    loop: asyncio.AbstractEventLoop, stop_event: asyncio.Event, force_exit: Callable[[int], object],
    *, monotonic: Callable[[], float] = time.monotonic,
) -> Callable[..., None]:
    """Build the COUNTED stop handler shared across every registered signal (extracted so the two-press
    semantics are unit-testable WITHOUT raising real signals — the returned handler is called directly).

    The same handler object is registered for SIGINT/SIGTERM/SIGBREAK, so its counter is shared across
    them (a SIGINT then a SIGTERM is still "second signal"). It accepts ``*_args`` so it serves BOTH the
    zero-arg :meth:`loop.add_signal_handler` callback and the ``(signum, frame)`` :func:`signal.signal`
    fallback. It never raises — a signal handler that raised would surface at an arbitrary ``await``.

    * FIRST signal: graceful — wake the idle ``await stop_event.wait()`` via
      :meth:`loop.call_soon_threadsafe` (the ``signal.signal`` fallback runs in the main thread OUTSIDE
      the loop callback context on Windows, so the threadsafe hand-off is the correct wake-up; the loop
      is captured at install time). Honoured at the next safe point.
    * REPEATS within ``REPEAT_SIGNAL_GRACE_S`` of the first: logged and ignored. A single ``nssm stop``
      delivers the console Ctrl-C more than once (2026-09-02..23: every stop logged ``stop_forced``
      0.85-1.4 s after ``stop_requested``, mid shutdown-backup, so the clean stop was never committed).
    * A signal AFTER the grace window: hard exit (the owner's deliberate second Ctrl-C for a wedged
      stop). State stays RUNNING (never committed STOPPED) so the next boot runs crash recovery — that is
      BY DESIGN; R3 protection is broker-resident, not process-local.
    """
    state: dict[str, float | int] = {"count": 0, "first_at": 0.0}

    def _handler(*args: object) -> None:
        state["count"] += 1
        signum = args[0] if args else None
        if state["count"] == 1:
            state["first_at"] = monotonic()
            _log.warning(
                "stop_requested",
                signum=signum,
                hint=f"graceful stop — honoured at the next safe point; a Ctrl-C more than "
                f"{REPEAT_SIGNAL_GRACE_S:.0f} s later forces exit",
            )
            try:
                loop.call_soon_threadsafe(stop_event.set)
            except RuntimeError:
                # Loop already closed (signal.signal fallback handlers are process-global and outlive
                # asyncio.run on Windows): a stray signal in the post-run teardown window has nothing to
                # wake — swallow so the "never raises" contract holds; hard_exit is already imminent.
                pass
            return
        since_first = monotonic() - state["first_at"]
        if since_first < REPEAT_SIGNAL_GRACE_S:
            _log.warning(
                "stop_signal_repeat_ignored",
                count=state["count"], signum=signum, since_first_s=round(since_first, 3),
            )
            return
        _log.critical(
            "stop_forced",
            count=state["count"], signum=signum, since_first_s=round(since_first, 3),
            hint="repeat signal past the grace window — engine exits hard; state stays RUNNING so the "
            "next boot runs crash recovery (R3 protection is broker-resident)",
        )
        try:
            logging.shutdown()
        except Exception:  # noqa: BLE001 - a hard-exit path must never raise out of a signal handler
            pass
        force_exit(130)

    return _handler


def install_signal_handlers(stop_event: asyncio.Event, *, force_exit: Callable[[int], object] = os._exit) -> None:
    """Install the graceful-stop signal handlers EARLY — before ``connect()``, so the WHOLE boot is
    covered (2026-07-21 13:34 IST: Ctrl-C during a wedged ``lifecycle.startup`` hit Python's default
    SIGINT handler → a raw ``KeyboardInterrupt`` at an arbitrary ``await`` with no ``lifecycle.shutdown``;
    the process then lingered holding ``data/engine.lock`` — a zombie that blocks every future start).

    ``force_exit`` is injectable so the second-press hard exit is unit-testable without killing the
    interpreter (default :func:`os._exit`). The counted handler is SHARED across all registered signals.
    """
    loop = asyncio.get_running_loop()
    handler = make_stop_handler(loop, stop_event, force_exit)

    signals = [signal.SIGINT, signal.SIGTERM]
    if hasattr(signal, "SIGBREAK"):   # Windows console ctrl-break / NSSM stop → CTRL_BREAK_EVENT
        signals.append(signal.SIGBREAK)
    for sig in signals:
        try:
            loop.add_signal_handler(sig, handler)   # POSIX: runs inside the loop
        except (NotImplementedError, RuntimeError):
            # Windows ProactorEventLoop has no add_signal_handler; fall back to signal.signal (the
            # handler accepts the (signum, frame) args and wakes the loop threadsafe from the main thread).
            try:
                signal.signal(sig, handler)
            except (ValueError, OSError):
                pass


def hard_exit(code: int) -> NoReturn:
    """Force process exit that the interpreter's own shutdown CANNOT (2026-07-21 13:34 IST zombie).

    After ``main()`` returns, interpreter shutdown joins non-daemon threads and ``concurrent.futures``'
    atexit hook joins executor workers WITHOUT a timeout. A single wedged worker (blocked DuckDB op, a
    long feature snapshot, a hung HTTP call) then hangs the process forever — still holding the §2.6
    instance lock and heartbeat, a zombie that blocks every future start until it is killed by hand
    (exactly what happened at 13:34). ``os._exit`` skips those joins entirely, so a wedged worker can
    never zombify the process. Runs ONLY from ``engine.ops.main.main()`` (never at import), so tests
    import this module freely without risk of exiting the test runner.
    """
    main_thread = threading.main_thread()
    stragglers = [t.name for t in threading.enumerate() if not t.daemon and t is not main_thread]
    if stragglers:
        _log.warning(
            "nondaemon_threads_at_exit", threads=stragglers,
            hint="abandoned non-daemon threads would block interpreter shutdown; forcing os._exit",
        )
    else:
        _log.info("exit_clean")
    try:
        logging.shutdown()
    except Exception:  # noqa: BLE001 - flushing the log handlers must never block the forced exit
        pass
    os._exit(code)
