"""Chaos suite. Every test under tests/chaos is marked ``chaos`` (``pytest -m chaos`` runs the
suite before a phase gate). One file per case, ``test_case<NN>_<slug>.py``; the case number is the
drill-catalog row, so runbooks/CHAOS_DRILLS.md can map drill -> test. Cases or sub-assertions that need live order-path
components (real AUTO routing and broker-resident protection) are skip-marked with ``LIVE_ORDER_PATH_GATED`` or
a classification below so the gap stays visible in every run instead of being absent.

Chaos tests never touch the live stores (data/state.db, data/market.duckdb), the engine's API port, or
the network: they build real components over the root conftest's migrated temp SQLite db and frozen
clock, and fake only the external boundary (Kite HTTP/websocket, the Claude SDK, Telegram).
"""

from __future__ import annotations

import pytest

LIVE_ORDER_PATH_GATED = "Gated on the live order path: needs real AUTO routing / broker-resident protection and owner sign-off"
#: The paper autopilot's classification of the LIVE_ORDER_PATH_GATED clauses it does not rewrite.
MIS_OUT_OF_PAPER_SCOPE = "MIS out of paper scope: paper trades CNC only"
REAL_BROKER_ONLY = "real-broker-only: paper never routes to a live broker"
BROKER_RESIDENT_PROTECTION = ("broker-resident protection: paper protection is the in-process PaperBroker's GTT book, and unobserved "
                      "time is the catch-up's")
REAL_SIDE_ONLY = ("real-side only: RECOMMEND places no broker-resident SL-M/GTT; the paper ProtectionManager "
                  "(oms/protection.py) protects paper positions, asserted in tests/replay/test_paper_e2e.py")
LIVE_AUTO_LIFECYCLE_HOOKS = ("lifecycle hooks reserved for live AUTO: the paper reconcile is its own pass, never a "
                             "SessionLifecycle hook")


def pytest_collection_modifyitems(config, items):
    for item in items:
        if "tests" in item.path.parts and "chaos" in item.path.parts:
            item.add_marker(pytest.mark.chaos)
