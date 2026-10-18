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

The guard is lazy: ``engine/__init__`` imports this module, which installs a ``sys.meta_path`` finder
that imports scikit-learn on the first import of a :data:`GUARDED` package and otherwise finds nothing,
so only processes that load the native stack (research / backtest code) pay the ~5 s sklearn import.
A guarded package already imported before ``engine`` is out of its reach.

The Phase-0 install smoke test (``scripts/smoke_test.py``) independently validates the coexistence in
an isolated subprocess, and the startup self-test surfaces :func:`guard_armed` for operational visibility.
"""

from __future__ import annotations

import importlib.util
import sys
import threading

#: Top-level packages whose first import triggers the scikit-learn preload.
GUARDED: frozenset[str] = frozenset({"numba", "llvmlite", "vectorbt", "cvxpy", "skfolio"})

#: ``True`` once the guard has imported scikit-learn (its OpenMP runtime is established).
PRELOADED: bool = False


class _SklearnFirst:
    """Meta-path finder that never finds a module; it imports scikit-learn before the first guarded one.

    It stays on ``sys.meta_path`` for the life of the process (removing it while the import system
    iterates that list is unsafe); once the preload ran, ``_pending`` is empty and each import costs
    one set lookup.
    """

    def __init__(self) -> None:
        self._pending = set(GUARDED)
        self._lock = threading.RLock()
        self._busy = False

    def find_spec(self, fullname: str, path: object = None, target: object = None) -> None:
        if fullname in self._pending:
            self._preload()
        return None

    def _preload(self) -> None:
        global PRELOADED
        # A concurrent first import waits here until sklearn is in; a re-entrant one (same thread,
        # from inside the sklearn import) passes straight through.
        with self._lock:
            if self._busy or not self._pending:
                return
            self._busy = True
            try:
                try:
                    import sklearn  # noqa: F401 — imported for its SIDE EFFECT: the OpenMP runtime.
                except ImportError:
                    pass  # stripped env without the ML stack: nothing to order
                else:
                    PRELOADED = True
                # Not reached when sklearn raised anything else: every guarded import keeps failing loudly.
                self._pending.clear()
            finally:
                self._busy = False


def _installed() -> _SklearnFirst | None:
    return next((f for f in sys.meta_path if type(f).__name__ == _SklearnFirst.__name__), None)


def guard_armed() -> bool:
    """The finder is installed and scikit-learn is importable (checked without importing it)."""
    return _installed() is not None and importlib.util.find_spec("sklearn") is not None


if _installed() is None:
    sys.meta_path.insert(0, _SklearnFirst())
