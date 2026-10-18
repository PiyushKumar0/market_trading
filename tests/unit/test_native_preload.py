"""The lazy native import-order guard (engine._preload): scikit-learn loads before a native package runs."""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
import types
from pathlib import Path
from types import SimpleNamespace

import pytest

import engine
from engine import _preload
from engine.ops.selftest import CheckStatus, SelfTest

_SRC = str(Path(engine.__file__).parents[1])

_CHILD = (
    "import importlib.util, sys\n"
    "import engine\n"
    "assert 'sklearn' not in sys.modules, 'import engine loaded sklearn eagerly'\n"
    "assert importlib.util.find_spec('numba') and 'sklearn' not in sys.modules, 'a probe loaded sklearn'\n"
    "import numba\n"
    "mods = list(sys.modules)\n"
    "assert mods.index('sklearn') < mods.index('numba'), 'numba loaded before sklearn'\n"
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


@pytest.mark.parametrize("name", ["numba", "llvmlite", "vectorbt", "cvxpy", "skfolio"])
def test_every_native_package_loads_through_the_sklearn_first_loader(name: str) -> None:
    """The five packages of the crash table: a fresh finder (``pending`` full, whatever this process has
    imported) hands each the sklearn-first loader. Locating a spec imports nothing."""
    spec = _preload._SklearnFirst().find_spec(name)
    if spec is None:
        pytest.skip(f"{name} not installed")
    assert isinstance(spec.loader, _preload._SklearnFirstLoader)


def test_a_broken_sklearn_fails_the_guarded_import_instead_of_skipping_the_guard(monkeypatch) -> None:
    """Installed but unimportable is not "absent": the guarded package must not run unguarded."""

    class _BrokenSklearn:
        def find_spec(self, fullname, path=None, target=None):
            return importlib.util.spec_from_loader(fullname, self) if fullname == "sklearn" else None

        def create_module(self, spec):
            return None

        def exec_module(self, module):
            raise ImportError("broken scikit-learn build")

    monkeypatch.delitem(sys.modules, "sklearn", raising=False)
    monkeypatch.setattr(sys, "meta_path", [_BrokenSklearn(), *sys.meta_path])
    ran: list[types.ModuleType] = []
    finder = _preload._SklearnFirst()
    module = types.ModuleType("numba")
    module.__spec__ = importlib.util.spec_from_loader("numba", loader=None)
    loader = _preload._SklearnFirstLoader(SimpleNamespace(exec_module=ran.append), finder)
    with pytest.raises(ImportError, match="broken scikit-learn build"):
        loader.exec_module(module)
    assert ran == [] and finder.pending                 # never ran, and the next import tries again


def test_selftest_native_preload_passes_when_guard_armed() -> None:
    assert _preload.guard_armed()
    st = SelfTest(conn=None, clock=None, settings=None, secrets=None, protected_store=None,
                  kill_switch=None, mode_manager=None)
    assert st._check_native_preload().status is CheckStatus.PASS
