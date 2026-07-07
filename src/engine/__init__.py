"""Personal AI trading platform engine (NSE / Zerodha Kite Connect). See IMPLEMENTATION_PLAN.md. Three-tier separation (R1): intelligence proposes, risk disposes, oms executes."""

# Native import-order guard: establish scikit-learn's OpenMP runtime before ANY numba/vectorbt/cvxpy
# import loads, or vectorbt + skfolio segfault the process on Windows (0xC0000005). Must be the first
# import so every `import engine.*` inherits the safe order. See engine._preload for the full rationale.
from engine import _preload as _preload  # noqa: F401,E402
