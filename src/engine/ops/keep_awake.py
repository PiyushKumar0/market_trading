"""In-session OS keep-awake (2026-07-23 13:41 sleep/resume wedge).

While the NSE calendar reports a trading session open, this asserts the Windows
``SetThreadExecutionState(ES_CONTINUOUS | ES_SYSTEM_REQUIRED)`` flags so the OS does not *auto*-sleep
mid-session and freeze the tick feed — the 2026-07-23 13:41 feed wedge began with the PC sleeping while
the market was open. It is engaged on the session-open transition and released (``ES_CONTINUOUS`` alone,
which clears the system-required lock) on session close, driven by the periodic health loop.

Deliberate non-goals (documented so the boundary is not mistaken for a bug):

* **Display sleep is allowed** — we assert ``ES_SYSTEM_REQUIRED`` but NOT ``ES_DISPLAY_REQUIRED``, so
  the screen may still blank; only the *system* is kept awake.
* **Owner-initiated sleep is NOT fought** — ``SetThreadExecutionState`` cannot and must not block an
  explicit "Sleep" from the Start menu / lid-close / power button; it only prevents the idle-timeout
  auto-sleep. If the owner force-sleeps mid-session, the OS still sleeps; the WARMING-timeout respawn in
  :class:`~engine.broker.ticker_supervisor.TickerSupervisor` is the recovery for that (it fires on
  resume and re-spawns the wedged child).

Non-Windows platforms: every method is a guarded no-op. The ctypes call is injectable so tests can
record it without touching the real Win32 API.
"""

from __future__ import annotations

import sys
from collections.abc import Callable

from engine.core.log import get_logger

_log = get_logger("engine.ops.keep_awake")

#: Win32 EXECUTION_STATE flags (winbase.h). ES_CONTINUOUS makes the state persist until the next
#: ES_CONTINUOUS call (so a single assert holds across suspend/resume); ES_SYSTEM_REQUIRED forces the
#: system awake. ES_DISPLAY_REQUIRED is intentionally NOT used — the screen is allowed to sleep.
ES_CONTINUOUS = 0x80000000
ES_SYSTEM_REQUIRED = 0x00000001


class KeepAwake:
    """Idempotent in-session OS keep-awake toggle (Windows), driven by the periodic health loop.

    Parameters
    ----------
    enabled:
        ``settings.ticker.keep_awake_in_session`` — owner opt-out. Disabled ⇒ every call is a no-op.
    set_execution_state:
        Injectable ``SetThreadExecutionState`` stand-in ``(flags: int) -> None`` for tests. ``None``
        resolves the real ``ctypes.windll.kernel32.SetThreadExecutionState`` lazily on first use.
    """

    def __init__(
        self,
        *,
        enabled: bool = True,
        set_execution_state: Callable[[int], object] | None = None,
    ) -> None:
        self._enabled = enabled
        self._engaged = False
        self._is_windows = sys.platform.startswith("win")
        self._set_execution_state = set_execution_state

    def update(self, session_open: bool) -> None:
        """Engage keep-awake on the session-open transition, release it on session close.

        Idempotent and quiet: the ctypes call + one INFO line fire only on a state CHANGE, never every
        tick (no log spam). No-op when disabled or off-Windows. ``session_open`` is the same
        calendar/clock truth the tick-silence guard uses (the health loop supplies it)."""
        if not self._enabled or not self._is_windows:
            return
        if session_open and not self._engaged:
            self._apply(ES_CONTINUOUS | ES_SYSTEM_REQUIRED)
            self._engaged = True
            _log.info("keep_awake_engaged")
        elif not session_open and self._engaged:
            # Clear the system-required lock (ES_CONTINUOUS alone) — the OS may idle-sleep again.
            self._apply(ES_CONTINUOUS)
            self._engaged = False
            _log.info("keep_awake_released")

    def _apply(self, flags: int) -> None:
        fn = self._resolve()
        if fn is None:
            return
        try:
            fn(flags)
        except Exception:  # noqa: BLE001 - a keep-awake failure must never crash the health loop
            _log.exception("keep_awake_set_state_failed", flags=hex(flags))

    def _resolve(self) -> Callable[[int], object] | None:
        if self._set_execution_state is not None:
            return self._set_execution_state
        try:
            import ctypes

            return ctypes.windll.kernel32.SetThreadExecutionState  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001 - ctypes/kernel32 unavailable ⇒ silently degrade to no-op
            _log.exception("keep_awake_ctypes_unavailable")
            return None
