"""The engine's stop path: the counted signal handler and the ``os._exit`` backstop. Stdlib +
``engine.core.log`` only, so a process can import it without the composition root."""

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
    """Build the COUNTED stop handler shared across every registered signal (the returned handler is
    callable directly, so the two-press semantics are testable without raising real signals).

    One handler object serves SIGINT/SIGTERM/SIGBREAK, so its counter is shared (a SIGINT then a
    SIGTERM is still "second signal"). It accepts ``*_args`` to serve BOTH the zero-arg
    :meth:`loop.add_signal_handler` callback and the ``(signum, frame)`` :func:`signal.signal`
    fallback. It never raises — a raise would surface at an arbitrary ``await``.

    * FIRST signal: graceful — wake the idle ``await stop_event.wait()`` via
      :meth:`loop.call_soon_threadsafe` (the ``signal.signal`` fallback runs in the main thread OUTSIDE
      the loop on Windows; the loop is captured at install time).
    * REPEATS within ``REPEAT_SIGNAL_GRACE_S`` of the first: logged and ignored. A single ``nssm stop``
      delivers the console Ctrl-C more than once.
    * A signal AFTER the grace window: hard exit (the owner's deliberate second Ctrl-C for a wedged
      stop). State stays RUNNING (never committed STOPPED) so the next boot runs crash recovery — BY
      DESIGN; protection is broker-resident, not process-local.
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
                # Loop already closed (signal.signal handlers outlive asyncio.run on Windows): nothing
                # to wake; swallow to keep the "never raises" contract.
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
            "next boot runs crash recovery (protection is broker-resident)",
        )
        try:
            logging.shutdown()
        except Exception:  # noqa: BLE001 - a hard-exit path must never raise out of a signal handler
            pass
        force_exit(130)

    return _handler


def install_signal_handlers(stop_event: asyncio.Event, *, force_exit: Callable[[int], object] = os._exit) -> None:
    """Install the graceful-stop signal handlers EARLY — before ``connect()``, so the WHOLE boot is
    covered (Python's default SIGINT handler would raise ``KeyboardInterrupt`` at an arbitrary ``await``
    with no ``lifecycle.shutdown``, leaving a zombie holding ``data/engine.lock``).

    ``force_exit`` is injectable so the hard exit is testable (default :func:`os._exit`).
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
    """Force process exit that the interpreter's own shutdown CANNOT.

    After ``main()`` returns, interpreter shutdown joins non-daemon threads and ``concurrent.futures``'
    atexit hook joins executor workers WITHOUT a timeout, so one wedged worker (blocked DuckDB op, hung
    HTTP call) leaves a zombie holding the instance lock and heartbeat. ``os._exit`` skips those joins.
    Runs ONLY from ``engine.ops.main.main()`` (never at import), so tests can import this module safely.
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
