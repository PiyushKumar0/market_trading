"""Personal AI trading platform engine (NSE / Zerodha Kite Connect). See IMPLEMENTATION_PLAN.md. Three-tier separation (R1): intelligence proposes, risk disposes, oms executes."""

# Native import-order guard: installs an import hook that loads scikit-learn's OpenMP runtime before the
# first numba/llvmlite/vectorbt/cvxpy/skfolio import, or vectorbt + skfolio segfault the process on
# Windows (0xC0000005). Must be the first import so every `import engine.*` is covered. See engine._preload.
from engine import _preload as _preload  # noqa: F401,E402
