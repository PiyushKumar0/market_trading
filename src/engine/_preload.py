"""Native import-order guard — MUST establish before numba / vectorbt / cvxpy (skfolio) load.

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

``import sklearn`` is a no-op after the first time (module caching), so importing THIS module at the
very top of ``engine/__init__.py`` makes every ``import engine.*`` — the service (engine.ops.main),
tests, tools, notebooks — establish the safe order before any backtest (vectorbt) or portfolio-
optimization (skfolio) code loads its native dependencies. This is the single enforced chokepoint.

The Phase-0 install smoke test (``scripts/smoke_test.py``) independently validates the coexistence in
an isolated subprocess, and the startup self-test surfaces :data:`PRELOADED` for operational visibility.
"""

from __future__ import annotations

#: ``True`` once scikit-learn's OpenMP runtime has been established (the vectorbt+skfolio guard is
#: active); ``False`` only in a stripped environment without the ML/optimization stack. Read by the
#: startup self-test (engine.ops.selftest) so the gate output shows the guard is in force.
PRELOADED: bool = False

try:
    import sklearn  # noqa: F401 — imported for its SIDE EFFECT: load the OpenMP runtime FIRST.

    PRELOADED = True
except ImportError:
    # scikit-learn absent ⇒ a stripped env without the ML/optimization stack. Nothing that conflicts
    # (vectorbt / cvxpy / skfolio) can be present either, so there is nothing to guard against here.
    PRELOADED = False
