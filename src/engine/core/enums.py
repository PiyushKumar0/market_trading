"""Foundational enums shared across tiers (R1).

These live in ``core`` (the dependency-graph root) so both ``engine.risk`` (which owns the mode /
risk-state machines, R5) and ``engine.intelligence.schemas`` (whose ``GateVerdict`` records the mode
+ risk-state a verdict was issued under) can import them without a cross-tier dependency. Putting
them anywhere else would force ``risk`` to import ``intelligence`` or vice-versa, breaking R1.
"""

from __future__ import annotations

from enum import StrEnum


class Mode(StrEnum):
    """Operating mode (O2). Owner-controlled; the risk gate may only DOWNGRADE (§3.5.3)."""

    OFF = "OFF"
    RECOMMEND = "RECOMMEND"
    AUTO = "AUTO"


class Routing(StrEnum):
    """Order routing, orthogonal to Mode and valid ONLY in AUTO (§3.5.3)."""

    PAPER = "paper"
    LIVE = "live"


class RiskState(StrEnum):
    """Tier-2-controlled risk state (§3.5.3).

    Reached by direct per-cause entry edges, NOT a linear chain — most-restrictive-wins. Re-arm
    requires every latching cause cleared (risk-forced downgrades to CLOSE_ONLY/KILLED need explicit
    owner action, never a timer; R3/R5).
    """

    NORMAL = "NORMAL"
    FROZEN = "FROZEN"          # no new entries; manage/exit continue
    CLOSE_ONLY = "CLOSE_ONLY"  # manage/exit only; risk-forced; owner re-arm only
    KILLED = "KILLED"          # kill switch / cumulative floor; sticky; owner two-step reset only


class Actor(StrEnum):
    """Who originated a state change / order — for the audit chain (R8)."""

    OWNER = "owner"
    RISK_GATE = "risk_gate"
    SCHEDULER = "scheduler"
    KILL_SWITCH = "kill_switch"
    RECONCILER = "reconciler"
    LEARNER = "learner"
    SYSTEM = "system"


class DegradeTier(StrEnum):
    """Budget-governor degrade ladder (§5.6). Maps to a mode capability (D6)."""

    DG0 = "DG0"  # full operation
    DG1 = "DG1"  # trim context / cut heartbeats; analysis quality still full grade
    DG2 = "DG2"  # AUTO manage-only (no LLM-originated entries); RECOMMEND continues
    DG3 = "DG3"  # all intraday LLM off; sentiment off
    DG4 = "DG4"  # zero SDK calls (== LLM-unavailable safe state, D7)
