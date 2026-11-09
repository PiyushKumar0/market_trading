"""Native import-order guard — scikit-learn MUST load before numba / vectorbt / cvxpy (skfolio).

On Windows, importing **vectorbt** (→ numba → llvmlite) and **cvxpy** (skfolio's backend, native
``_cvxcore``) into the same process segfaults it with ``0xC0000005``: a native OpenMP runtime load-order
conflict, not the Intel-libiomp duplicate (``KMP_DUPLICATE_LIB_OK`` does not help), and not catchable by
``try/except``. Importing **scikit-learn first** establishes the process-wide OpenMP runtime and the rest
coexist. numba→cvxpy crashes in either order; preloading numpy/scipy does not help, only sklearn does.

The guard is lazy: ``engine/__init__`` imports this module, which installs a ``sys.meta_path`` finder.
For a :data:`GUARDED` package it returns the normal spec with :class:`_SklearnFirstLoader`, which imports
scikit-learn before the package's own code runs, so only processes that load the native stack pay the ~5 s
import and a mere ``find_spec`` probe pays nothing. The import runs while the package executes, under that
package's module lock — never inside ``find_spec``, which CPython calls under its GLOBAL import lock: every
other thread's import would wait out the sklearn import, and one mid-import of a module sklearn needs
(numpy, scipy, joblib) would deadlock. A guarded package imported before ``engine`` is out of reach; a
scikit-learn that is installed but fails to import raises at the first guarded import instead of letting
the native stack load unguarded.

``scripts/smoke_test.py`` validates the coexistence in a subprocess; the startup self-test surfaces
:func:`guard_armed`.
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
