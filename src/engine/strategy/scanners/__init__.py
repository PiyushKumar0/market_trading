"""ORB / RSI(2) / trend / momentum baseline scanners (§6.1) + the scanner registry (§3.2.5).

Importing this package registers the four Phase-1 price baselines in ``SCANNER_REGISTRY``. The
``brk20``, ``ins`` and ``cat`` rules are deliberately NOT in it: they are EOD-batch rules with no
per-bar condition, so they implement no ``Scanner`` protocol and are swept + admitted directly by
the composition root (``prescreen.admit``), which binds them to the identical §3.2.5 dedupe/caps.

``cat`` (§2.7, v2 SHADOW as of the owner-directed 2026-08-18 amendment) is the one that changed
shape: the §6.1 row-5 design registered here as a per-bar peer with an intraday price/volume
confirmation, and that confirmation was RETIRED (WO-18). The rule now mirrors ``ins`` — a batch
translation of today's ``originating`` watchlist rows in :mod:`engine.strategy.scanners.cat`. Its
candidates carry ``catalyst_ref`` and are additionally capped by
``catalyst_guard.max_catalyst_entries_day`` in the pre-screen (§3.2.5), as always planned.
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
