"""ORB / RSI(2) / trend / momentum baseline scanners (§6.1) + the scanner registry (§3.2.5).

Importing this package registers the four Phase-1 price baselines in ``SCANNER_REGISTRY``. The
Phase-3 ``cat`` scanner (§6.1 row 5, §2.7) will register here as a peer — same ``Scanner`` base,
its candidates carrying ``catalyst_ref`` and additionally capped by ``catalyst_guard.
max_catalyst_entries_day`` in the pre-screen (§3.2.5). No ``cat`` code ships in Phase 1 (§8.2).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from engine.strategy.scanners.base import SCANNER_REGISTRY, Scanner, params_from_envelope, register
from engine.strategy.scanners.momentum import MomentumScanner
from engine.strategy.scanners.orb import OrbScanner
from engine.strategy.scanners.rsi2 import Rsi2Scanner
from engine.strategy.scanners.trend import TrendScanner

__all__ = [
    "SCANNER_REGISTRY",
    "MomentumScanner",
    "OrbScanner",
    "Rsi2Scanner",
    "Scanner",
    "TrendScanner",
    "build_enabled_scanners",
    "params_from_envelope",
    "register",
]


def build_enabled_scanners(
    enabled: Sequence[str],
    params_by_id: Mapping[str, Mapping[str, float]] | None = None,
) -> list[Scanner]:
    """Instantiate the enabled scanners in the given order, with per-strategy §6.3 param overrides.

    ``enabled`` comes from settings (the integrator wires it); ``params_by_id`` maps
    ``strategy_id`` → bare param dict (see :func:`params_from_envelope` for envelope_state input).
    Unknown strategy ids fail loud — a typo must not silently disable a baseline.
    """
    unknown = [sid for sid in enabled if sid not in SCANNER_REGISTRY]
    if unknown:
        raise ValueError(f"unknown scanner id(s) {unknown}; registered: {sorted(SCANNER_REGISTRY)}")
    overrides = params_by_id or {}
    return [SCANNER_REGISTRY[sid](overrides.get(sid)) for sid in enabled]
