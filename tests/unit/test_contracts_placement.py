"""R1 contracts placement (§9.1): the Tier-1<->Tier-2 data contracts live in ``engine.core.contracts``,
not ``engine.intelligence.schemas``, so ``engine.risk``/``engine.oms`` can construct a ``GateVerdict``
without importing the LLM tier.

Asserts:
- ``engine.intelligence.schemas`` RE-EXPORTS the moved names (same object, not a copy) — a consumer that
  still does ``from engine.intelligence.schemas import GateVerdict`` gets the identical class engine.risk
  builds against;
- ``engine.strategy.cost_model`` (which needs ``CostBreakdown``) no longer imports ``engine.intelligence``
  at all, reusing the AST import-graph helpers from ``tests/unit/test_import_graph.py``.
"""

from __future__ import annotations

import pytest

from engine.core.config import repo_root
from tests.unit.test_import_graph import FORBIDDEN_PREFIX, _imports

MOVED_NAMES = [
    "_to_decimal",
    "DecimalStr",
    "ActionBase",
    "EnterAction",
    "ExitAction",
    "ModifyStopAction",
    "ModifyTargetAction",
    "CancelAction",
    "ActionProposal",
    "ActionProposalAdapter",
    "ACTION_MODELS",
    "CheckResult",
    "CostBreakdown",
    "GateVerdict",
    "Recommendation",
]


@pytest.mark.parametrize("name", MOVED_NAMES)
def test_schemas_reexports_are_identical_objects(name: str) -> None:
    import engine.core.contracts as contracts
    import engine.intelligence.schemas as schemas

    core_obj = getattr(contracts, name)
    schemas_obj = getattr(schemas, name)
    # `is` (not ==): engine.intelligence.schemas must re-export the SAME object engine.core.contracts
    # defines, not a redefined/copied lookalike — otherwise `isinstance` / discriminator identity across
    # the two import paths would silently diverge (e.g. a GateVerdict built by engine.risk failing an
    # isinstance check against the class imported via engine.intelligence.schemas).
    assert schemas_obj is core_obj, (
        f"engine.intelligence.schemas.{name} is not the same object as engine.core.contracts.{name}"
    )


def test_cost_model_no_longer_imports_intelligence() -> None:
    path = repo_root() / "src" / "engine" / "strategy" / "cost_model.py"
    imported = _imports(path)
    offenders = [name for name in imported if name == FORBIDDEN_PREFIX or name.startswith(FORBIDDEN_PREFIX + ".")]
    assert not offenders, f"engine.strategy.cost_model must not import {FORBIDDEN_PREFIX}: {offenders}"
