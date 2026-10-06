"""Backtest evidence registry (``config/strategy_evidence.yaml``) and the card's evidence line.

The registry holds dotted paths into each strategy's backtest report JSON, never numbers: the line is
always read from the report, and registered edges stay in ``settings.yaml``.
"""

from __future__ import annotations

import json
from functools import cache
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict

from engine.core.config import load_yaml, repo_root
from engine.core.log import get_logger

_log = get_logger("engine.learning.evidence")
_warned: set[str] = set()


class StrategyEvidence(BaseModel):
    """``n``/``hit``/``median_net``/``cpcv`` are dotted paths into ``report``; ``hit`` and ``cpcv``
    resolve to fractions (``hit_rate_net``, ``fold_pass_fraction``)."""

    model_config = ConfigDict(extra="forbid")

    summary: str
    measured_exit: str
    report: str
    n: str
    hit: str
    median_net: str
    cpcv: str | None = None
    caveat: str | None = None


class EvidenceRegistry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    strategies: dict[str, StrategyEvidence]


@cache
def load_evidence(path: Path | None = None) -> EvidenceRegistry:
    return EvidenceRegistry.model_validate(
        load_yaml(path or repo_root() / "config" / "strategy_evidence.yaml")
    )


def _warn_once(msg: str, **kw: Any) -> None:
    if msg + repr(kw) not in _warned:
        _warned.add(msg + repr(kw))
        _log.warning(msg, **kw)


def _read(doc: Any, dotted: str) -> float | None:
    for part in dotted.split("."):
        if not isinstance(doc, dict) or part not in doc:
            return None
        doc = doc[part]
    return float(doc) if isinstance(doc, int | float) and not isinstance(doc, bool) else None


def evidence_line(
    strategy_id: str, reports_dir: Path, registry_path: Path | None = None
) -> str | None:
    """The card's backtest evidence line, or ``None`` when the strategy, report or every value is
    unavailable. A missing value drops only its own part. Never raises."""
    entry = load_evidence(registry_path).strategies.get(strategy_id)
    if entry is None:
        _warn_once("evidence_strategy_missing", strategy=strategy_id)
        return None
    try:
        doc = json.loads((reports_dir / entry.report).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        _warn_once("evidence_report_unreadable", strategy=strategy_id, error=str(exc))
        return None
    parts = []
    for label, key, fmt in (
        ("n", entry.n, "{:.0f}"),
        ("hit", entry.hit, "{:.1%}"),
        ("median net", entry.median_net, "{:.2f}%"),
        ("CPCV", entry.cpcv, "{:.1%} fold pass"),
    ):
        if key is None:
            continue
        value = _read(doc, key)
        if value is None:
            _warn_once("evidence_key_missing", strategy=strategy_id, key=key)
            continue
        parts.append(f"{label} {fmt.format(value)}")
    if not parts:
        return None
    line = f"backtest ({entry.measured_exit}): " + ", ".join(parts)
    return f"{line} — {entry.caveat}" if entry.caveat else line
