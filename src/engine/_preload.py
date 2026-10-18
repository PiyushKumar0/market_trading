"""Native import-order guard — scikit-learn MUST load before numba / vectorbt / cvxpy (skfolio).

On Windows, importing **vectorbt** (→ numba → llvmlite/LLVM) and **cvxpy** (skfolio's convex-
optimization backend, native ``_cvxcore``) into the *same process* segfaults the interpreter with
``0xC0000005`` STATUS_ACCESS_VIOLATION. It is a native OpenMP runtime load-order conflict — NOT the
Intel-libiomp duplicate (``KMP_DUPLICATE_LIB_OK`` does not help), and NOT catchable by any Python
``try/except`` because a C-level access violation bypasses the interpreter entirely.

Importing **scikit-learn first** establishes the process-wide OpenMP runtime; numba/llvmlite and
cvxpy then coexist safely. Verified deterministically (3/3) on this env (numpy 2.4.6, scipy 1.17.1,
numba 0.65.1, llvmlite 0.47.0, sklearn 1.9.0, skfolio 0.20.1/cvxpy):

    sklearn -> vectorbt -> numba JIT -> cvxpy solve   ->  clean
    vectorbt -> cvxpy   (no sklearn preload)          ->  crash
    numba    -> cvxpy   (either order)                ->  crash
    numpy/scipy preload before cvxpy                  ->  still crash  (only sklearn fixes it)

The guard is lazy: ``engine/__init__`` imports this module, which installs a ``sys.meta_path`` finder.
For a :data:`GUARDED` package it returns the normal spec with :class:`_SklearnFirstLoader`, which imports
scikit-learn before the package's own code runs. So only processes that load the native stack (research /
backtest code) pay the ~5 s sklearn import, and a mere ``find_spec`` probe pays nothing. The import runs
while the package executes, under that package's module lock — never inside ``find_spec``, which CPython
calls under its GLOBAL import lock: every other thread's import would wait out the sklearn import, and one
mid-import of a module sklearn needs (numpy, scipy, joblib) would deadlock. A guarded package imported
before ``engine`` is out of reach; a scikit-learn that is installed but fails to import raises at the
first guarded import instead of letting the native stack load unguarded.

The Phase-0 install smoke test (``scripts/smoke_test.py``) independently validates the coexistence in
an isolated subprocess, and the startup self-test surfaces :func:`guard_armed` for operational visibility.
"""

from __future__ import annotations

import importlib.util
import sys
from importlib.machinery import ModuleSpec
from types import ModuleType
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from importlib.abc import Loader  # runtime import costs ~0.3 s (importlib.resources) per process

#: Top-level packages whose first import loads scikit-learn first.
GUARDED: frozenset[str] = frozenset({"numba", "llvmlite", "vectorbt", "cvxpy", "skfolio"})


class _SklearnFirst:
    """Meta-path finder: a :data:`GUARDED` package's spec as the other finders build it, with its loader
    wrapped. It stays on ``sys.meta_path`` for the life of the process (the import system iterates that
    list); once scikit-learn is in, ``pending`` is empty and each import costs one set lookup."""

    def __init__(self) -> None:
        self.pending = set(GUARDED)

    def find_spec(self, fullname: str, path: Any = None, target: Any = None) -> ModuleSpec | None:
        if fullname not in self.pending:
            return None
        for finder in sys.meta_path:
            if type(finder).__name__ == _SklearnFirst.__name__ or not hasattr(finder, "find_spec"):
                continue
            spec = finder.find_spec(fullname, path, target)
            if spec is not None:
                if spec.loader is not None:
                    spec.loader = cast("Loader", _SklearnFirstLoader(spec.loader, self))
                return spec
        return None


class _SklearnFirstLoader:
    """Imports scikit-learn, then runs the guarded package with the loader that found it."""

    def __init__(self, loader: Any, finder: _SklearnFirst) -> None:
        self._loader, self._finder = loader, finder

    def create_module(self, spec: ModuleSpec) -> ModuleType | None:
        return self._loader.create_module(spec)

    def exec_module(self, module: ModuleType) -> None:
        if self._finder.pending:
            if importlib.util.find_spec("sklearn") is not None:   # absent: a stripped env, nothing to order
                import sklearn  # noqa: F401 — imported for its SIDE EFFECT: the OpenMP runtime.
            self._finder.pending.clear()
        assert module.__spec__ is not None                         # the import system set it to our spec
        module.__loader__ = module.__spec__.loader = self._loader
        self._loader.exec_module(module)


def _installed() -> bool:
    return any(type(f).__name__ == _SklearnFirst.__name__ for f in sys.meta_path)


def guard_armed() -> bool:
    """The finder is installed and scikit-learn can be found (checked without importing it)."""
    return _installed() and importlib.util.find_spec("sklearn") is not None


if not _installed():
    sys.meta_path.insert(0, _SklearnFirst())
