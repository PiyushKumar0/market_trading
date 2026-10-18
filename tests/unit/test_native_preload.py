"""The lazy native import-order guard (engine._preload): scikit-learn loads before the first numba import."""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from pathlib import Path

import pytest

import engine
from engine import _preload
from engine.ops.selftest import CheckStatus, SelfTest

_SRC = str(Path(engine.__file__).parents[1])

_CHILD = (
    "import sys\n"
    "import engine\n"
    "assert 'sklearn' not in sys.modules, 'import engine loaded sklearn eagerly'\n"
    "import numba\n"
    "mods = list(sys.modules)\n"
    "assert mods.index('sklearn') < mods.index('numba'), 'numba loaded before sklearn'\n"
    "assert engine._preload.PRELOADED\n"
    "print(engine.__file__)\n"
)


@pytest.mark.needs_heavy_deps
def test_guard_loads_sklearn_only_on_first_guarded_import() -> None:
    if importlib.util.find_spec("numba") is None or importlib.util.find_spec("sklearn") is None:
        pytest.skip("numba / scikit-learn not installed")
    proc = subprocess.run(
        [sys.executable, "-c", _CHILD],
        env={**os.environ, "PYTHONPATH": _SRC}, capture_output=True, text=True, timeout=120,
    )
    assert proc.returncode == 0, f"child failed rc={proc.returncode}: {proc.stdout!r} {proc.stderr!r}"
    assert Path(proc.stdout.strip()).is_relative_to(_SRC)          # the code under test, not another tree


def test_selftest_native_preload_passes_when_guard_armed() -> None:
    assert _preload.guard_armed()
    st = SelfTest(conn=None, clock=None, settings=None, secrets=None, protected_store=None,
                  kill_switch=None, mode_manager=None)
    assert st._check_native_preload().status is CheckStatus.PASS
