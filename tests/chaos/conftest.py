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
#: The paper autopilot's classification of the PHASE3_GATED clauses it does not rewrite (plan Q4.5).
MIS_OUT_OF_PAPER_SCOPE = "MIS out of paper scope: paper trades CNC only (plan §1.3)"
REAL_BROKER_ONLY = "real-broker-only: paper never routes to a live broker (D8, plan §1.2)"
BROKER_RESIDENT_R3 = ("broker-resident R3: paper protection is the in-process PaperBroker's GTT book, and unobserved "
                      "time is the Q4.7 catch-up's (plan §1.7)")
PHASE4_LIFECYCLE_HOOKS = ("lifecycle hooks reserved for Phase 4: the paper reconcile is its own pass, never a "
                          "SessionLifecycle hook (plan §1.7, Q4.7)")


def pytest_collection_modifyitems(config, items):
    for item in items:
        if "tests" in item.path.parts and "chaos" in item.path.parts:
            item.add_marker(pytest.mark.chaos)
