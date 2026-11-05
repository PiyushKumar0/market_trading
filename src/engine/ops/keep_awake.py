"""In-session OS keep-awake (2026-07-23 13:41 sleep/resume wedge).

While the NSE calendar reports a trading session open, this asserts the Windows
``SetThreadExecutionState(ES_CONTINUOUS | ES_SYSTEM_REQUIRED)`` flags so the OS does not *auto*-sleep
mid-session and freeze the tick feed — the 2026-07-23 13:41 feed wedge began with the PC sleeping while
the market was open. It is engaged on the session-open transition and released (``ES_CONTINUOUS`` alone,
which clears the system-required lock) on session close, driven by the periodic health loop.

Lapse check (2026-11-05): an engaged request has not always stayed in force. On 10-26, 10-28, 10-30, 11-02
and 11-05 the PC idle-slept on AC with the lid open while engaged, each time after an earlier in-session
sleep, while idle spans of up to 99 min held before any such sleep. So every pulse while engaged reads this
thread's flags (``SetThreadExecutionState(0)``, a pure query) and the system-wide execution state
(``CallNtPowerInformation(SystemExecutionState)``, no elevation needed). When either lacks
``ES_SYSTEM_REQUIRED`` it re-asserts the request and logs ``keep_awake_lapsed`` with what it saw; a
``keep_awake_state`` line every few pulses records both while they hold, so a sleep with the request in
force (Windows ignoring it rather than dropping it) is visible in the log too. One lapse stays invisible:
the system losing this request while its thread flags are intact and another process holds the bit —
telling those apart needs ``powercfg /requests``, which needs elevation.

Deliberate non-goals (documented so the boundary is not mistaken for a bug):

* **Display sleep is allowed** — we assert ``ES_SYSTEM_REQUIRED`` but NOT ``ES_DISPLAY_REQUIRED``, so
  the screen may still blank; only the *system* is kept awake.
* **Owner-initiated sleep is NOT fought** — ``SetThreadExecutionState`` cannot and must not block an
  explicit "Sleep" from the Start menu / lid-close / power button; it only prevents the idle-timeout
  auto-sleep. If the owner force-sleeps mid-session, the OS still sleeps; the WARMING-timeout respawn in
  :class:`~engine.broker.ticker_supervisor.TickerSupervisor` is the recovery for that (it fires on
  resume and re-spawns the wedged child).

Non-Windows platforms: every method is a guarded no-op. The ctypes calls are injectable so tests can
record them without touching the real Win32 API.
"""

from __future__ import annotations

import sys
from collections.abc import Callable

from engine.core.log import get_logger

_log = get_logger("engine.ops.keep_awake")

#: Win32 EXECUTION_STATE flags (winbase.h). ES_CONTINUOUS makes the state persist until the next
#: ES_CONTINUOUS call — in principle across suspend/resume too, which this host has not always honoured
#: (see the lapse check). ES_SYSTEM_REQUIRED forces the system awake. ES_DISPLAY_REQUIRED is
#: intentionally NOT used — the screen is allowed to sleep.
ES_CONTINUOUS = 0x80000000
ES_SYSTEM_REQUIRED = 0x00000001
#: ``POWER_INFORMATION_LEVEL.SystemExecutionState`` (powerbase.h): ``CallNtPowerInformation`` returns the
#: system-wide execution state, the union of every process's requests.
SYSTEM_EXECUTION_STATE = 16


class KeepAwake:
    """Idempotent in-session OS keep-awake toggle (Windows), driven by the periodic health loop.

    Every call must come from one thread: ``SetThreadExecutionState`` state belongs to the calling
    thread, so a release from another thread would leave the original request in force. The health loop
    runs on the event-loop thread.

    Parameters
    ----------
    enabled:
        ``settings.ticker.keep_awake_in_session`` — owner opt-out. Disabled ⇒ every call is a no-op.
    set_execution_state:
        Injectable ``SetThreadExecutionState`` stand-in ``(flags: int) -> previous state`` for tests;
        ``flags=0`` must leave the state unchanged (the query). ``None`` resolves the real Win32 call
        lazily on first use.
    get_system_state:
        Injectable stand-in ``() -> int`` for the system-wide execution-state read. ``None`` resolves the
        real ``CallNtPowerInformation`` read — unless ``set_execution_state`` is injected: a fake setter
        leaves nothing real to read back, so the lapse check is off.
    state_log_every:
        Engaged pulses between ``keep_awake_state`` lines (~5 min at the 60 s health pulse).
    """

    def __init__(
        self,
        *,
        enabled: bool = True,
        set_execution_state: Callable[[int], object] | None = None,
        get_system_state: Callable[[], int] | None = None,
        state_log_every: int = 5,
    ) -> None:
        self._enabled = enabled
        self._engaged = False
        self._is_windows = sys.platform.startswith("win")
        # Resolved lazily and independently: a failed read must never cost the keep-awake itself.
        self._setter = set_execution_state
        self._reader = get_system_state
        self._load_setter = set_execution_state is None
        self._load_reader = get_system_state is None and set_execution_state is None
        self._state_log_every = max(1, state_log_every)
        # Per-session lapse-check state, reset on engage.
        self._checking = False      # the engage read the system-wide state, so the pulses check
        self._own_visible: bool | None = None   # that read shows this process's request (None = unknown)
        self._pulses = 0            # checked pulses since the engage
        self._lapses = 0            # lapse episodes this session
        self._lapse_pulses = 0      # pulses the open episode has found the request missing (0 = none open)

    def update(self, session_open: bool) -> None:
        """Engage keep-awake on the session-open transition, check it each pulse, release it on close.

        Quiet: engage/release log only on a state CHANGE, a lapse warns once per episode, and
        ``keep_awake_state`` fires every ``state_log_every`` pulses. No-op when disabled or off-Windows.
        ``session_open`` is the same calendar/clock truth the tick-silence guard uses (the health loop
        supplies it)."""
        if not self._enabled or not self._is_windows:
            return
        if session_open and not self._engaged:
            self._engage()
        elif session_open:
            self._check()
        elif self._engaged:
            # Clear the system-required lock (ES_CONTINUOUS alone) — the OS may idle-sleep again.
            self._apply(ES_CONTINUOUS)
            self._engaged = False
            _log.info("keep_awake_released", lapses=self._lapses, lapsed_at_release=self._lapse_pulses > 0)

    def _engage(self) -> None:
        before = self._read_state()
        self._apply(ES_CONTINUOUS | ES_SYSTEM_REQUIRED)
        self._engaged = True
        self._pulses = self._lapses = self._lapse_pulses = 0
        state = self._read_state()
        # Whether the system-wide read can see this process's request decides what a missing bit means
        # later. Proven only when the bit appears with this assert; if another requester already held it,
        # it stays unknown and the read is trusted (a lost request costs a certain sleep, a needless
        # re-register at most a microsecond without one). If the bit is missing even now, the read is
        # blind to this process and only this thread's flags are checked.
        if state is None:
            self._own_visible = None
        elif not state & ES_SYSTEM_REQUIRED:
            self._own_visible = False
        elif before is not None and not before & ES_SYSTEM_REQUIRED:
            self._own_visible = True
        else:
            self._own_visible = None
        self._checking = state is not None
        _log.info(
            "keep_awake_engaged", system_state_before=_hex(before), system_state=_hex(state),
            own_visible=self._own_visible,
        )
        if self._own_visible is False:
            _log.warning("keep_awake_probe_blind", system_state=_hex(state))

    def _check(self) -> None:
        if not self._checking:
            return
        self._pulses += 1
        thread, state = self._apply(0), self._read_state()
        if self._in_force(thread, state):
            self._close_lapse(state)
            if self._pulses % self._state_log_every == 0:
                _log.info(
                    "keep_awake_state", thread_state=_hex(thread), system_state=_hex(state), lapses=self._lapses,
                )
            return
        # Lapsed: this thread's flags were cleared, or nothing holds ES_SYSTEM_REQUIRED system-wide. Either
        # way the reads say this process's request is not in force (see _engage on an unproven read), so
        # clearing it below does not drop a working lock. Re-assert with the same flags first; if that
        # does not bring it back, clear and set so the request is registered afresh.
        self._apply(ES_CONTINUOUS | ES_SYSTEM_REQUIRED)
        thread_after, state_after = self._apply(0), self._read_state()
        toggled = self._own_visible is not False and not self._in_force(thread_after, state_after)
        if toggled:
            self._apply(ES_CONTINUOUS)
            self._apply(ES_CONTINUOUS | ES_SYSTEM_REQUIRED)
            thread_after, state_after = self._apply(0), self._read_state()
        restored = self._in_force(thread_after, state_after)
        if self._lapse_pulses:
            self._lapse_pulses += 1
            if restored:
                self._close_lapse(state_after)
            return
        self._lapses += 1
        self._lapse_pulses = 0 if restored else 1
        _log.warning(
            "keep_awake_lapsed", thread_state=_hex(thread), system_state=_hex(state), toggled=toggled,
            thread_state_after=_hex(thread_after), system_state_after=_hex(state_after), restored=restored,
            own_visible=self._own_visible, lapses=self._lapses,
        )

    def _in_force(self, thread: int | None, system: int | None) -> bool:
        """This thread's flags carry ES_SYSTEM_REQUIRED and, unless the read is blind to this process,
        so does the system-wide state. An unknown reading (None) proves nothing either way."""
        if thread is not None and not thread & ES_SYSTEM_REQUIRED:
            return False
        return self._own_visible is False or system is None or bool(system & ES_SYSTEM_REQUIRED)

    def _close_lapse(self, state: int | None) -> None:
        if self._lapse_pulses:
            _log.info("keep_awake_restored", system_state=_hex(state), lapse_pulses=self._lapse_pulses)
            self._lapse_pulses = 0

    def _apply(self, flags: int) -> int | None:
        """Set this thread's execution state (``flags=0``: query it); return the previous state, or None
        if unknown or failed."""
        fn = self._resolve_setter()
        if fn is None:
            return None
        try:
            previous = fn(flags)
        except Exception:  # noqa: BLE001 - a keep-awake failure must never crash the health loop
            _log.exception("keep_awake_set_state_failed", flags=hex(flags))
            return None
        if previous == 0:
            # NULL is the documented failure return; a success always carries ES_CONTINUOUS here, a fresh
            # thread included (0x80000000, measured 2026-11-05). A failed query only loses the thread view,
            # so it stays at DEBUG — it would otherwise warn on every pulse.
            log = _log.debug if flags == 0 else _log.warning
            log("keep_awake_set_state_rejected", flags=hex(flags))
            return None
        return previous if isinstance(previous, int) else None

    def _read_state(self) -> int | None:
        """The system-wide execution state, or None when unavailable. Never raises."""
        fn = self._resolve_reader()
        if fn is None:
            return None
        try:
            return int(fn())
        except Exception as exc:  # noqa: BLE001 - a failed read only weakens this pulse's check
            _log.debug("keep_awake_state_read_failed", reason=str(exc))
            return None

    def _resolve_setter(self) -> Callable[[int], object] | None:
        if self._load_setter:
            self._load_setter = False
            self._setter = _load(_win32_set_execution_state)
        return self._setter

    def _resolve_reader(self) -> Callable[[], int] | None:
        if self._load_reader:
            self._load_reader = False
            self._reader = _load(_win32_system_state_reader)
        return self._reader


def _load[T](factory: Callable[[], T]) -> T | None:
    try:
        return factory()
    except Exception:  # noqa: BLE001 - ctypes/DLL unavailable ⇒ silently degrade to no-op
        _log.exception("keep_awake_ctypes_unavailable", call=factory.__name__)
        return None


def _win32_set_execution_state() -> Callable[[int], int]:
    """The real ``SetThreadExecutionState``.

    The explicit signature is load-bearing: ctypes' default c_int restype would turn a previous state of
    0x80000001 negative. It goes on a private ``WinDLL`` handle so the shared ``ctypes.windll`` function
    object stays untouched. ``ctypes`` is imported locally so this module stays importable off-Windows.
    """
    import ctypes
    from ctypes import wintypes

    set_state = ctypes.WinDLL("kernel32").SetThreadExecutionState
    set_state.restype = wintypes.DWORD
    set_state.argtypes = [wintypes.DWORD]
    return set_state


def _win32_system_state_reader() -> Callable[[], int]:
    """The real system-wide read: ``CallNtPowerInformation(SystemExecutionState)`` (powrprof)."""
    import ctypes
    from ctypes import wintypes

    call_info = ctypes.WinDLL("powrprof").CallNtPowerInformation
    call_info.restype = wintypes.LONG       # NTSTATUS
    call_info.argtypes = [
        ctypes.c_int, ctypes.c_void_p, wintypes.ULONG, ctypes.POINTER(wintypes.ULONG), wintypes.ULONG,
    ]

    def system_state() -> int:
        out = wintypes.ULONG(0)
        status = call_info(SYSTEM_EXECUTION_STATE, None, 0, ctypes.byref(out), ctypes.sizeof(out))
        if status != 0:
            raise OSError(f"CallNtPowerInformation(SystemExecutionState) failed: {status & 0xFFFFFFFF:#x}")
        return int(out.value)

    return system_state


def _hex(value: int | None) -> str | None:
    return None if value is None else f"{value:#010x}"
