"""Tier 2 — deterministic risk.

Risk gate, limit table, exposure tracking, the mode/risk-state machines, and the sticky
kill switch. **Imports NOTHING from ``engine.intelligence``** — the tier separation is
structural, asserted by an import-graph test. Proposals reach this tier only as
schema-validated data.
"""

from __future__ import annotations

from engine.risk.kill import KillSwitch, KillSwitchEngaged
from engine.risk.mode import ModeManager

__all__ = ["KillSwitch", "KillSwitchEngaged", "ModeManager"]
