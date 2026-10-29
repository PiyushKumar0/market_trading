"""Research trial ledger and the multiplicity-aware registration bar (review §9 #8).

``fold_pass_min`` deflates by one sweep's grid size; the ledger (``config/research_trials.yaml``) counts every
hypothesis a family has tested. A new ``expected_edge_pct``, or a promotion out of shadow, cites a ledger entry,
passes CPCV at ``fold_pass_min(N)`` and has ``permutation_pvalue < registration_alpha(N)``, N = ``family_trials``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import yaml

from engine.core.config import config_dir

RESULTS = ("supported", "refuted", "null", "insufficient", "registered", "grandfathered")


@dataclass(frozen=True)
class Trial:
    id: str
    date: date
    family: str               # <topic>/<strategy>; a strategy's own entry edge is edge/<strategy_id>
    hypothesis: str
    population: str
    construct: str
    variants: int             # hypotheses this entry tested, not ledger rows
    result: str
    report: str


def _trial(row: dict[str, Any]) -> Trial:
    t = Trial(**{**row, "date": date.fromisoformat(str(row["date"]))})
    if t.date.isoformat() != str(row["date"]):
        raise ValueError(f"{t.id}: date {row['date']!r} is not YYYY-MM-DD")
    if t.result not in RESULTS:
        raise ValueError(f"{t.id}: result {t.result!r} not in {RESULTS}")
    if type(t.variants) is not int or t.variants < 1:
        raise ValueError(f"{t.id}: variants must be an int >= 1, got {t.variants!r}")
    return t


def load_trials(path: Path | None = None) -> list[Trial]:
    """Every ledger entry; a malformed row or a duplicate id raises."""
    p = path or config_dir() / "research_trials.yaml"
    trials = [_trial(row) for row in (yaml.safe_load(p.read_text(encoding="utf-8")) or {}).get("trials") or []]
    seen: set[str] = set()
    for t in trials:
        if t.id in seen:
            raise ValueError(f"duplicate trial id {t.id!r}")
        seen.add(t.id)
    return trials


def family_trials(trials: list[Trial], family: str) -> int:
    """N for the family: the hypotheses tested across its entries."""
    return sum(t.variants for t in trials if t.family == family)


def permutation_pvalue(sample, population, *, draws: int = 10_000, seed: int) -> float:
    """Share of ``draws`` same-size subsamples of ``population`` (without replacement) whose median is >= the
    sample's median. No +1 correction: a sample no draw matches reads exactly 0."""
    pop = np.asarray(population, dtype="float64")
    n = len(sample)
    if not 0 < n <= len(pop):
        raise ValueError(f"sample size {n} must be in 1..{len(pop)}")
    target = float(np.median(np.asarray(sample, dtype="float64")))
    rng = np.random.default_rng(seed)
    hits = sum(np.median(rng.choice(pop, n, replace=False)) >= target for _ in range(draws))
    return hits / draws


def registration_alpha(n_trials: int, alpha: float = 0.05) -> float:
    """Bonferroni over the family: ``alpha / max(1, N)``."""
    return alpha / max(1, n_trials)
