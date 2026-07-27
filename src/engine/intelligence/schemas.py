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

import json
from typing import Annotated, Any, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, TypeAdapter, ValidationError

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
    "CLUSTER_EVENT_TYPES",
    "STRUCTURED_OUTPUT_NOTES",
    "ActionBase",
    "ActionProposal",
    "ActionProposalAdapter",
    "CancelAction",
    "CheckResult",
    "ClusterScore",
    "ClusterScoreBatch",
    "CostBreakdown",
    "DayPlan",
    "DayPlanCatalystFocus",
    "DayPlanFocus",
    "DecimalStr",
    "EnterAction",
    "ExitAction",
    "GateVerdict",
    "IntradayOutput",
    "IntradayOutputAdapter",
    "ModifyStopAction",
    "ModifyTargetAction",
    "NightlyReview",
    "NoActionOutput",
    "ParamSuggestion",
    "Recommendation",
    "TradeAttribution",
    "action_proposal_json_schema",
    "intraday_output_json_schema",
    "parse_and_stamp",
    "parse_cluster_scores",
    "parse_intraday",
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


# =========================================================================== Intraday Analyst (§5.2)
class NoActionOutput(BaseModel):
    """The Intraday Analyst's second legal output: an explicit refusal to act (§5.2 output schema).

    ``no_action`` is a first-class answer, not a failure — a heartbeat call (§5.2 trigger (c)) may
    emit NOTHING else. It carries no platform-stamped fields: nothing downstream of it can trade, so
    there is no proposal identity, TTL or audit digest to stamp.
    """

    model_config = ConfigDict(extra="forbid")

    action: Literal["no_action"]
    reason: str = Field(min_length=5)
    regime_note: str = ""       # optional regime read; feeds the NEXT context's stable block (§5.2)


#: §5.2 output schema: the §3.3 action union PLUS ``no_action``, discriminated on ``action``.
IntradayOutput = Annotated[
    EnterAction | ExitAction | ModifyStopAction | ModifyTargetAction | CancelAction | NoActionOutput,
    Field(discriminator="action"),
]

IntradayOutputAdapter: TypeAdapter[Any] = TypeAdapter(IntradayOutput)


def intraday_output_json_schema() -> dict[str, Any]:
    """JSON Schema for the §5.2 output union — the Intraday Analyst's structured-output schema."""
    return IntradayOutputAdapter.json_schema()


def parse_intraday(
    raw: dict[str, Any] | str,
    *,
    proposal_id: str,
    agent_id: str,
    valid_until: AwareDatetime,
    inputs_digest: str,
) -> Any:
    """Validate Intraday Analyst output into an ``ActionProposal`` (stamped) or :class:`NoActionOutput`.

    Actions go through :func:`parse_and_stamp` so the platform-stamped identity/temporal fields are
    overwritten exactly as they are on every other action path; ``no_action`` is plain-validated —
    stamping it would imply it can be acted on. A schema-invalid payload raises ``ValidationError``
    here and resolves to no-proposal + alert upstream (D7) — never re-parsed from prose.
    """
    model = IntradayOutputAdapter.validate_json(raw) if isinstance(raw, str) else IntradayOutputAdapter.validate_python(raw)
    if isinstance(model, NoActionOutput):
        return model
    return parse_and_stamp(
        model.model_dump(mode="python"),
        proposal_id=proposal_id,
        agent_id=agent_id,
        valid_until=valid_until,
        inputs_digest=inputs_digest,
    )


# =========================================================================== Pre-open Planner (§5.3)
class DayPlanFocus(BaseModel):
    """One focus symbol of the day plan. Advisory: the scanners still originate every signal."""

    model_config = ConfigDict(extra="forbid")

    symbol: str
    bias: Literal["long", "short", "avoid"]
    levels: str                 # free text — the planner NEVER emits binding numeric levels (§5.3)
    why: str


class DayPlanCatalystFocus(BaseModel):
    """Advisory commentary on ONE catalyst-watchlist entry (§5.3).

    The planner may highlight or deprioritize a watchlist symbol; it can neither add a symbol to the
    watchlist nor upgrade a ``context`` entry to ``originating``, and ``advisory_levels`` is prose —
    the binding origination levels stay the scanner's deterministic ones (§2.7 step 6).
    ``event_type`` is a free string (it echoes the watchlist entry): one odd advisory value must not
    void the whole day plan, unlike the cluster taxonomy below which is per-item droppable.
    """

    model_config = ConfigDict(extra="forbid")

    symbol: str
    event_type: str
    direction: Literal["long", "short", "neutral"]
    advisory_levels: str
    drift_note: str


class DayPlan(BaseModel):
    """The 08:50 day plan (§5.3) — one JSON row per trading day in ``day_plans``, fed into every
    intraday context's STABLE block (D8)."""

    model_config = ConfigDict(extra="forbid")

    regime: str
    focus: list[DayPlanFocus] = Field(default_factory=list, max_length=8)     # §5.3: 3–8 symbols
    catalyst_focus: list[DayPlanCatalystFocus] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    no_trade_today: bool = False


# =========================================================================== News Analyst (§5.4)
#: The CLOSED event taxonomy (§5.4). Out-of-enum ⇒ the cluster is DROPPED at parse + logged (D7).
CLUSTER_EVENT_TYPES: tuple[str, ...] = (
    "earnings_result",
    "earnings_guidance",
    "order_win",
    "capacity_expansion",
    "m_and_a",
    "mgmt_change",
    "regulatory_policy",
    "govt_program",
    "rating_change",
    "analyst_action",
    "legal_action",
    "dividend_corp_action",
    "macro_data",
    "global_market",
    "sector_policy",
    "disruption",
    "pump_promo_suspect",
    "other",
)


class ClusterScore(BaseModel):
    """One scored headline CLUSTER (§5.4 / §2.7 step 4) — one score per cluster, never per headline.

    ``entities`` are VERBATIM strings as they appear in the news; only the deterministic
    EntityResolver maps them to tradingsymbols (§2.7 step 3), never the LLM. Every score is
    UNTRUSTED evidence (§2.4).
    """

    model_config = ConfigDict(extra="forbid")

    cluster_id: str
    scope: Literal["market", "sector", "theme", "stock"]
    entities: list[str] = Field(default_factory=list)
    sectors: list[str] = Field(default_factory=list)
    themes: list[str] = Field(default_factory=list)     # from the in-prompt theme vocabulary
    sentiment: float = Field(ge=-1.0, le=1.0)
    materiality: float = Field(ge=0.0, le=1.0)          # rubric-anchored in the system prompt
    event_type: Literal[
        "earnings_result",
        "earnings_guidance",
        "order_win",
        "capacity_expansion",
        "m_and_a",
        "mgmt_change",
        "regulatory_policy",
        "govt_program",
        "rating_change",
        "analyst_action",
        "legal_action",
        "dividend_corp_action",
        "macro_data",
        "global_market",
        "sector_policy",
        "disruption",
        "pump_promo_suspect",
        "other",
    ]
    novelty: float = Field(ge=0.0, le=1.0)              # NEW information vs rehash of a scored story


class ClusterScoreBatch(BaseModel):
    """The News Analyst's batched output — up to 30 cluster scores per call (§5.4)."""

    model_config = ConfigDict(extra="forbid")

    scores: list[ClusterScore] = Field(default_factory=list)


def parse_cluster_scores(raw: dict[str, Any] | list[Any] | str) -> tuple[list[ClusterScore], list[str]]:
    """Validate a score batch PER ITEM: returns ``(valid_scores, dropped_cluster_ids)``.

    D7/§5.4: an out-of-enum ``event_type`` (or any other invalid field) drops THAT cluster and is
    logged — it never voids the batch, because one confabulated event type must not cost 29 good
    scores. A dropped cluster simply stays unscored, which excludes it from origination (§2.7
    fail-safe ladder). A cluster whose ``cluster_id`` is itself unusable is reported positionally.
    """
    payload: Any = json.loads(raw) if isinstance(raw, str) else raw
    if isinstance(payload, dict):
        items = payload.get("scores", [])
    else:
        items = payload
    if not isinstance(items, list):
        raise ValueError(f"cluster score batch is not a list of scores (got {type(items).__name__})")

    scores: list[ClusterScore] = []
    dropped: list[str] = []
    for index, item in enumerate(items):
        try:
            scores.append(ClusterScore.model_validate(item))
        except ValidationError:
            cid = item.get("cluster_id") if isinstance(item, dict) else None
            dropped.append(cid if isinstance(cid, str) and cid else f"#{index}")
    return scores, dropped


# =========================================================================== Nightly Reviewer (§5.5)
class ParamSuggestion(BaseModel):
    """ONE suggested envelope-parameter value (§5.5 output contract).

    A SUGGESTION, never a setting: the §6.4 validation pipeline and the owner decide (R4). The job
    that persists a review DROPS any suggestion whose ``parameter`` is not an ``envelope.yaml`` name
    or whose ``proposed_value`` falls outside that parameter's bounds — the model's suggestible set
    is the list it was shown in the context, and no downstream reader re-derives it.

    ``proposed_value`` is a float because every envelope parameter is a plain numeric knob (§6.3):
    no price, no money, so the decimal-as-string convention deliberately does not apply here.
    """

    model_config = ConfigDict(extra="forbid")

    parameter: str
    proposed_value: float
    evidence_refs: list[str] = Field(default_factory=list)   # entry_id / rec_id, never prose


class TradeAttribution(BaseModel):
    """Attribution of ONE closed learning-ledger row (§5.5).

    ``thesis_wrong`` and ``process_error`` are different failures: the first is the market
    disagreeing with a correctly-executed idea, the second is the platform or the owner mishandling
    it. Collapsing them would make the lessons unactionable, which is why the verdict is a closed
    enum rather than free text.
    """

    model_config = ConfigDict(extra="forbid")

    entry_id: str
    verdict: Literal["thesis_right", "thesis_wrong", "process_error", "unclear"]
    note: str = ""


class NightlyReview(BaseModel):
    """The Nightly Post-Trade Reviewer's output (§5.5) — one JSON row per trading day in
    ``nightly_reviews``, read by ``GET /config/params`` and the daily owner summary."""

    model_config = ConfigDict(extra="forbid")

    lessons: list[str] = Field(default_factory=list)
    param_suggestions: list[ParamSuggestion] = Field(default_factory=list)
    process_errors: list[str] = Field(default_factory=list)
    trade_attributions: list[TradeAttribution] = Field(default_factory=list)
    summary: str
