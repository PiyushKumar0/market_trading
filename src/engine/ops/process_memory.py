"""Current-process memory telemetry (Windows), driven by the periodic health loop.

Gives :class:`~engine.ops.health.HealthMonitor` a cheap per-pulse reading of the current process's
memory via the Win32 ``GetProcessMemoryInfo`` API (``PROCESS_MEMORY_COUNTERS_EX``), so the
``process_memory`` log line can be grepped into a curve — no new dependency (psutil is not a project
dependency).

Three counters, all in bytes, for the CURRENT process:

* ``PagefileUsage``      — private/commit bytes (what machine commit pressure tracks).
* ``WorkingSetSize``     — resident working-set bytes.
* ``PeakWorkingSetSize`` — the working-set high-water mark since process start.

Telemetry, not a check: a read failure logs one DEBUG line and returns ``None`` — it must never raise
and must never affect the health verdict.

Non-Windows platforms: :meth:`ProcessMemoryReader.read` is a guarded no-op (returns ``None``). The
ctypes call is injectable so tests can fake it.
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
            return self._resolve()()
        except Exception as exc:  # noqa: BLE001 - a memory-read failure must never crash the health loop
            _log.debug("process_memory_read_failed", reason=str(exc))
            return None

    def _resolve(self) -> Callable[[], ProcessMemory]:
        return self._get_process_memory_info or _read_via_win32


def _read_via_win32() -> ProcessMemory:
    """The real Win32 call: ``K32GetProcessMemoryInfo`` on the current process's pseudo-handle.

    ``ctypes`` is imported locally so this module stays importable off-Windows.
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

    # Explicit argtypes/restype are required: the default c_int restype truncates the 64-bit
    # pseudo-handle from GetCurrentProcess(), and the call then fails silently on 64-bit Windows.
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
