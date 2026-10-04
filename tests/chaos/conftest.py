"""Plan §9.4 chaos suite. Every test under tests/chaos is marked ``chaos`` (``pytest -m chaos`` runs the
suite before a phase gate, §9.6). One file per case, ``test_case<NN>_<slug>.py``; the case number is the
§9.4 row, so runbooks/CHAOS_DRILLS.md can map drill -> test. Cases or sub-assertions that need Phase-3
components (ProtectionManager, GTTManager, Reconciler, SquareOffScheduler, AUTO routing — WO-P3-5/P3-6)
are skip-marked with ``PHASE3_GATED`` so the gap stays visible in every run instead of being absent.

Chaos tests never touch the live stores (data/state.db, data/market.duckdb), the engine's API port, or
the network: they build real components over the root conftest's migrated temp SQLite db and frozen
clock, and fake only the external boundary (Kite HTTP/websocket, the Claude SDK, Telegram).
"""

from __future__ import annotations

import pytest

PHASE3_GATED = "Phase-3-gated: needs WO-P3-5/P3-6 components (plan §8.4) — built with G2 + owner sign-off"


def pytest_collection_modifyitems(config, items):
    for item in items:
        if "tests" in item.path.parts and "chaos" in item.path.parts:
            item.add_marker(pytest.mark.chaos)
