"""Tier-1 ↔ code schemas (R1): the action-object union (§3.3), GateVerdict (§3.4), Recommendation (§3.6).

The models themselves now live in :mod:`engine.core.contracts` (R1 import-graph guard, §9.1) — this
module RE-EXPORTS them unchanged and keeps the SDK-facing helpers that only Tier-1 needs.

These are the ONLY shape in which Tier-1 (Claude) output crosses into the deterministic tiers. The SDK
call uses :data:`ActionProposal` as its structured-output schema (D5/D7); anything that fails validation
after retries is dropped with an alert — NEVER parsed from prose (D7).

Three locked conventions (Phase-0 deliverables, §8.1):

1. **Decimal prices round-trip as STRINGS.** JSON Schema has no decimal type; serialising prices/ticks
   as floats corrupts them. :data:`DecimalStr` validates from str|int|float (via ``str()`` for floats,
   never binary-float artifacts) and serialises to a JSON string. The exported schema types these
   fields as ``"string"`` too.
2. **Temporal + identity fields are PLATFORM-STAMPED, never LLM-emitted.** ``proposal_id``, ``agent_id``,
   ``valid_until``, ``inputs_digest`` are overwritten post-parse by :func:`stamp_proposal` using
   ``Clock``-derived values — any value the model supplies is DISCARDED. The LLM never produces a
   date/time (§3.2 convention).
3. **Constraints the SDK structured-output layer may reject** (``Field(gt=0)`` / ``min_length`` /
   mandatory ``additionalProperties:false`` / no recursive schemas) are enforced CLIENT-SIDE by
   re-validating the model after parse — which is exactly what :func:`parse_and_stamp` does. See
   :data:`STRUCTURED_OUTPUT_NOTES`.
"""

from __future__ import annotations

from typing import Any

from pydantic import AwareDatetime

from engine.core.contracts import (
    ACTION_MODELS,
    ActionBase,
    ActionProposal,
    ActionProposalAdapter,
    CancelAction,
    CheckResult,
    CostBreakdown,
    DecimalStr,
    EnterAction,
    ExitAction,
    GateVerdict,
    ModifyStopAction,
    ModifyTargetAction,
    Recommendation,
    _to_decimal,
)

__all__ = [
    "ACTION_MODELS",
    "STRUCTURED_OUTPUT_NOTES",
    "ActionBase",
    "ActionProposal",
    "ActionProposalAdapter",
    "CancelAction",
    "CheckResult",
    "CostBreakdown",
    "DecimalStr",
    "EnterAction",
    "ExitAction",
    "GateVerdict",
    "ModifyStopAction",
    "ModifyTargetAction",
    "Recommendation",
    "action_proposal_json_schema",
    "parse_and_stamp",
]

# Notes recorded during the Phase-0 smoke test (§8.1): structured-output constraints to enforce
# client-side post-parse if the SDK schema layer rejects them in-schema. parse_and_stamp() re-validates
# through Pydantic, so these are enforced there regardless of what the wire schema allows.
STRUCTURED_OUTPUT_NOTES = {
    "decimal_serialization": "strings",
    "additionalProperties": "false (forbid extra keys; Pydantic extra='forbid')",
    "numeric_constraints_client_side": ["quantity>0", "confidence in [0,1]", "thesis len 20..600"],
    "no_recursive_schemas": True,
    "temporal_fields_platform_stamped": ["valid_until"],
    "identity_fields_platform_stamped": ["proposal_id", "agent_id", "inputs_digest"],
}


# --------------------------------------------------------------------------- helpers
def action_proposal_json_schema() -> dict[str, Any]:
    """JSON Schema for the action union — the SDK structured-output schema (D5/D7)."""
    return ActionProposalAdapter.json_schema()


def parse_and_stamp(
    raw: dict[str, Any] | str,
    *,
    proposal_id: str,
    agent_id: str,
    valid_until: AwareDatetime,
    inputs_digest: str,
) -> Any:
    """Validate raw LLM output into an :data:`ActionProposal` and OVERWRITE the platform-stamped fields.

    Any ``proposal_id`` / ``agent_id`` / ``valid_until`` / ``inputs_digest`` the model emitted is
    DISCARDED and replaced with the platform-supplied values (Clock-derived for ``valid_until``). This
    is also where the client-side constraint re-validation happens (§8.1) — a malformed quantity /
    confidence / thesis raises here, never reaching the gate.
    """
    model = ActionProposalAdapter.validate_json(raw) if isinstance(raw, str) else ActionProposalAdapter.validate_python(raw)
    stamped = model.model_copy(
        update={
            "proposal_id": proposal_id,
            "agent_id": agent_id,
            "valid_until": valid_until,
            "inputs_digest": inputs_digest,
        }
    )
    # RE-VALIDATE the fully-stamped model — model_copy(update=) does NOT run validators, so a naive
    # platform-supplied ``valid_until`` (or any bad stamped value) would otherwise slip past the
    # AwareDatetime / no-naive-datetime invariant (§9.1). Round-tripping through the adapter re-checks
    # every field and keeps the concrete discriminated subclass.
    return ActionProposalAdapter.validate_python(stamped.model_dump(mode="python"))
