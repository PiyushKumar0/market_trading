"""Current-process memory telemetry (Windows), driven by the periodic health loop.

Born from the 2026-08-17 crisis: the live engine's private commit ballooned to ~53 GB over ~18h
(machine at 96.5% commit, crisis restart required) and the leak could not be diagnosed because there
was exactly one snapshot (the crash) and no time series. This module gives :class:`~engine.ops.health.
HealthMonitor` a cheap per-pulse reading of the current process's memory footprint via the Win32
``GetProcessMemoryInfo`` API (``PROCESS_MEMORY_COUNTERS_EX``), so the ``process_memory`` log line can be
grepped into a curve after the fact — no new dependency (psutil is only a transitive dep of ipython in
``uv.lock``, not a project dependency; see ``pyproject.toml``).

Three counters are read, all in bytes, for the CURRENT process:

* ``PagefileUsage``      — private/commit bytes (what "96.5% commit" tracks; the field the crisis needed).
* ``WorkingSetSize``     — resident working-set bytes.
* ``PeakWorkingSetSize`` — the high-water mark of the working set since process start.

This is telemetry, not a check (§2.6 distinguishes the two): a read failure logs one DEBUG line and
returns ``None`` — it must never raise and must never affect the health verdict.

Non-Windows platforms: :meth:`ProcessMemoryReader.read` is a guarded no-op (returns ``None``), following
:mod:`engine.ops.keep_awake`'s platform-guarded ctypes pattern. The ctypes call is injectable so tests
can record/fake it without touching the real Win32 API.
"""

from __future__ import annotations

import sys
from collections.abc import Callable
from typing import NamedTuple

from engine.core.log import get_logger

_log = get_logger("engine.ops.process_memory")


class ProcessMemory(NamedTuple):
    """Current-process memory counters, in bytes (Win32 ``PROCESS_MEMORY_COUNTERS_EX``)."""

    private_bytes: int          # PagefileUsage — private/commit bytes
    working_set_bytes: int      # WorkingSetSize
    peak_working_set_bytes: int  # PeakWorkingSetSize


class ProcessMemoryReader:
    """Reads Win32 ``PROCESS_MEMORY_COUNTERS_EX`` for the current process.

    Parameters
    ----------
    get_process_memory_info:
        Injectable ``GetProcessMemoryInfo``-equivalent stand-in ``() -> ProcessMemory`` for tests —
        called with no arguments and expected to either return a :class:`ProcessMemory` or raise.
        ``None`` resolves the real Win32 call lazily on first use (production path).
    """

    def __init__(
        self,
        *,
        get_process_memory_info: Callable[[], ProcessMemory] | None = None,
    ) -> None:
        self._is_windows = sys.platform.startswith("win")
        self._get_process_memory_info = get_process_memory_info

    def read(self) -> ProcessMemory | None:
        """Return current-process memory counters, or ``None`` off-Windows / on read failure.

        Never raises (telemetry, not a check): a read failure logs one DEBUG line and returns ``None``.
        """
        if not self._is_windows:
            return None
        try:
            fn = self._resolve()
            if fn is None:
                return None
            return fn()
        except Exception as exc:  # noqa: BLE001 - a memory-read failure must never crash the health loop
            _log.debug("process_memory_read_failed", reason=str(exc))
            return None

    def _resolve(self) -> Callable[[], ProcessMemory] | None:
        if self._get_process_memory_info is not None:
            return self._get_process_memory_info
        try:
            return _read_via_win32
        except Exception:  # noqa: BLE001 - ctypes/psapi unavailable ⇒ silently degrade to no-op
            _log.debug("process_memory_ctypes_unavailable")
            return None


def _read_via_win32() -> ProcessMemory:
    """The real Win32 call: ``K32GetProcessMemoryInfo`` on the current process's pseudo-handle.

    Imports ``ctypes``/``ctypes.wintypes`` locally so this module stays importable (and its ``read()``
    no-op path exercisable) on non-Windows platforms — same discipline as
    :meth:`engine.ops.keep_awake.KeepAwake._resolve`.
    """
    import ctypes
    from ctypes import wintypes

    class _ProcessMemoryCountersEx(ctypes.Structure):
        _fields_ = [
            ("cb", wintypes.DWORD),
            ("PageFaultCount", wintypes.DWORD),
            ("PeakWorkingSetSize", ctypes.c_size_t),
            ("WorkingSetSize", ctypes.c_size_t),
            ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
            ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
            ("PagefileUsage", ctypes.c_size_t),
            ("PeakPagefileUsage", ctypes.c_size_t),
            ("PrivateUsage", ctypes.c_size_t),
        ]

    # Explicit argtypes/restype are required, not cosmetic: ctypes defaults an unset restype to
    # c_int (32-bit signed). GetCurrentProcess() returns a 64-bit pseudo-handle (-1); without a HANDLE
    # restype ctypes truncates it, GetProcessMemoryInfo is then called with a bogus handle, and the call
    # fails silently (returns FALSE, GetLastError()==0) on 64-bit Windows.
    kernel32 = ctypes.windll.kernel32
    kernel32.GetCurrentProcess.restype = wintypes.HANDLE
    kernel32.GetCurrentProcess.argtypes = []
    psapi = ctypes.windll.psapi
    psapi.GetProcessMemoryInfo.restype = wintypes.BOOL
    psapi.GetProcessMemoryInfo.argtypes = [
        wintypes.HANDLE, ctypes.POINTER(_ProcessMemoryCountersEx), wintypes.DWORD,
    ]

    counters = _ProcessMemoryCountersEx()
    counters.cb = ctypes.sizeof(_ProcessMemoryCountersEx)
    handle = kernel32.GetCurrentProcess()
    ok = psapi.GetProcessMemoryInfo(handle, ctypes.byref(counters), counters.cb)
    if not ok:
        raise OSError(f"GetProcessMemoryInfo failed: {ctypes.get_last_error()}")
    return ProcessMemory(
        private_bytes=int(counters.PagefileUsage),
        working_set_bytes=int(counters.WorkingSetSize),
        peak_working_set_bytes=int(counters.PeakWorkingSetSize),
    )
