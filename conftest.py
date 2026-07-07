"""Root pytest conftest.

Adds ``src/`` to ``sys.path`` so ``import engine...`` works without installing the package
(the dev machine pins 3.12 via uv, but tests are runnable on the system interpreter too).
The heavy LLM/broker/analytics deps are NOT required for the pure-Python test tiers; tests that
need them are marked ``needs_heavy_deps`` and skipped automatically when an import fails.
"""

from __future__ import annotations

import sys
from pathlib import Path

_SRC = Path(__file__).parent / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))
