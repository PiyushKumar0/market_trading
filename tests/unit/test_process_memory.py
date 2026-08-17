"""Process-memory telemetry (2026-08-17 unbounded-commit crisis: ~53 GB/18h, one snapshot, no curve).

``ProcessMemoryReader`` reads Win32 ``PROCESS_MEMORY_COUNTERS_EX`` for the current process. It is
telemetry, not a check: a read failure must log one DEBUG line, never raise, and never propagate.
"""

from __future__ import annotations

from engine.ops.process_memory import ProcessMemory, ProcessMemoryReader


# --------------------------------------------------------------------------- real Win32 read (this box)
def test_read_returns_plausible_positive_integers_on_windows():
    """On the real Win32 API (this dev/CI box is win32), all three counters are positive, and the
    working set never exceeds its own peak."""
    reader = ProcessMemoryReader()
    assert reader._is_windows is True  # sanity: this suite runs on win32

    mem = reader.read()

    assert mem is not None
    assert isinstance(mem, ProcessMemory)
    assert mem.private_bytes > 0
    assert mem.working_set_bytes > 0
    assert mem.peak_working_set_bytes > 0
    assert mem.peak_working_set_bytes >= mem.working_set_bytes


# --------------------------------------------------------------------------------- injected fake reads
def test_read_uses_injected_counters_when_provided():
    fake = ProcessMemory(private_bytes=123, working_set_bytes=456, peak_working_set_bytes=789)
    reader = ProcessMemoryReader(get_process_memory_info=lambda: fake)
    reader._is_windows = True

    mem = reader.read()

    assert mem == fake


def test_read_failure_returns_none_and_does_not_raise(caplog):
    def _boom() -> ProcessMemory:
        raise OSError("GetProcessMemoryInfo failed: 87")

    reader = ProcessMemoryReader(get_process_memory_info=_boom)
    reader._is_windows = True

    mem = reader.read()  # must not raise

    assert mem is None


def test_read_is_noop_off_windows():
    calls: list[int] = []
    reader = ProcessMemoryReader(get_process_memory_info=lambda: calls.append(1) or None)  # type: ignore[func-returns-value]
    reader._is_windows = False

    mem = reader.read()

    assert mem is None
    assert calls == []  # the injected reader is never even called off-Windows
