"""Tier 2 — deterministic risk (R1/R2/R5/R10).

Risk gate, limit table (§7.1), exposure tracking, the mode/risk-state machines (R5), and the sticky
kill switch (R10). **Imports NOTHING from ``engine.intelligence``** — the R1 tier separation is
structural, asserted by an import-graph test (§9.1). Proposals reach this tier only as
schema-validated data (§2.3).
"""

from __future__ import annotations

from engine.risk.kill import KillSwitch, KillSwitchEngaged
from engine.risk.mode import ModeManager

__all__ = ["KillSwitch", "KillSwitchEngaged", "ModeManager"]
