"""Tier-1 <-> Tier-2 data contracts (R1): the action-object union (§3.3), GateVerdict (§3.4),
Recommendation (§3.6).

Placed in ``engine.core`` (not ``engine.intelligence``) so ``engine.risk`` / ``engine.oms`` can
construct and consume these — ``GateVerdict`` above all — WITHOUT importing ``engine.intelligence``.
The deterministic tiers must never import the LLM tier (R1 import-graph guard, §9.1); the AST scan in
``tests/unit/test_import_graph.py`` enforces this structurally. ``engine.intelligence.schemas``
re-exports every name defined here unchanged (§8.1 locked conventions below still apply verbatim).

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
   :data:`STRUCTURED_OUTPUT_NOTES` (defined alongside :func:`parse_and_stamp` in
   ``engine.intelligence.schemas``, which still owns the SDK-facing helpers).
"""

from __future__ import annotations

from decimal import Decimal
from typing import Annotated, Any, Literal

from pydantic import (
    AwareDatetime,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    PlainSerializer,
    TypeAdapter,
    WithJsonSchema,
)

from engine.core.enums import Mode, RiskState


# --------------------------------------------------------------------------- Decimal-as-string
def _to_decimal(v: Any) -> Decimal:
    if isinstance(v, Decimal):
        return v
    if isinstance(v, float):
        return Decimal(str(v))   # str() first: never inherit binary-float artifacts into a price
    return Decimal(v)            # str | int


DecimalStr = Annotated[
    Decimal,
    BeforeValidator(_to_decimal),
    PlainSerializer(lambda v: str(v), return_type=str, when_used="json"),
    # LOCK the WIRE schema to "string" in BOTH modes (WithJsonSchema overrides pydantic's default
    # anyOf[number,string] for a BeforeValidator-annotated Decimal). Without this the exported SDK
    # structured-output schema would invite the model to emit a JSON number and corrupt a price/tick
    # (§8.1 decimal-as-string convention). The validator still accepts str|int|float defensively.
    WithJsonSchema({"type": "string", "description": "decimal price as a string (no float corruption)"}),
]


# --------------------------------------------------------------------------- action union (§3.3)
class ActionBase(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    proposal_id: str = ""        # PLATFORM-STAMPED (ULID). Default "" so the LLM may omit it.
    agent_id: str = ""           # PLATFORM-STAMPED: which Tier-1 agent produced it.
    thesis: str = Field(min_length=20, max_length=600)        # 3–5 lines
    confidence: float = Field(ge=0.0, le=1.0)
    # PLATFORM-STAMPED: Clock.now() + per-style TTL (§3.2.1). The LLM never emits a date/time; any value
    # it supplies is overwritten by stamp_proposal(). Optional on the wire so the model may omit it.
    valid_until: AwareDatetime | None = None     # tz-aware IST only (§9.1 no-naive-datetime invariant)
    inputs_digest: str = ""      # PLATFORM-STAMPED: hash of AssembledContext for audit/replay (R8)


class EnterAction(ActionBase):
    action: Literal["enter"]
    tradingsymbol: str
    exchange: Literal["NSE"]
    side: Literal["BUY", "SELL"]
    style: Literal["intraday", "swing", "position"]           # O3: intraday=MIS, else CNC
    entry_type: Literal["LIMIT", "MARKET"]
    entry_price: DecimalStr | None = None                     # required if LIMIT; gate checks sanity band
    stop_price: DecimalStr                                    # MANDATORY — no stopless proposals exist
    target_price: DecimalStr | None = None
    quantity: int = Field(gt=0)                               # gate may only shrink (R1)
    signal_id: str                                            # links to a live SignalPreScreen candidate
    strategy_id: str
    features_snapshot_id: str


class ExitAction(ActionBase):
    action: Literal["exit"]
    position_id: str
    exit_type: Literal["MARKET", "LIMIT"]
    limit_price: DecimalStr | None = None
    reason: Literal["thesis_invalidated", "target_neared", "risk_event", "time_stop", "other"]


class ModifyStopAction(ActionBase):
    action: Literal["modify-stop"]
    position_id: str
    new_stop: DecimalStr                                      # gate: tighten ⇒ auto; widen ⇒ owner approval


class ModifyTargetAction(ActionBase):
    action: Literal["modify-target"]
    position_id: str
    new_target: DecimalStr | None = None                     # None = remove target; extend ⇒ owner approval


class CancelAction(ActionBase):
    action: Literal["cancel"]
    order_id: str                                            # NON-PROTECTIVE orders only (R1/R3)


ActionProposal = Annotated[
    EnterAction | ExitAction | ModifyStopAction | ModifyTargetAction | CancelAction,
    Field(discriminator="action"),
]

ActionProposalAdapter: TypeAdapter[Any] = TypeAdapter(ActionProposal)

# The five concrete classes, keyed by their ``action`` literal (handy for tests + dispatch).
ACTION_MODELS = {
    "enter": EnterAction,
    "exit": ExitAction,
    "modify-stop": ModifyStopAction,
    "modify-target": ModifyTargetAction,
    "cancel": CancelAction,
}


# --------------------------------------------------------------------------- GateVerdict (§3.4)
class CheckResult(BaseModel):
    rule_id: str            # §7.1 id, e.g. "per_trade_risk", "circuit_proximity", "margin_buffer"
    passed: bool
    value: str
    limit: str
    headroom: str           # human-readable; ships in recommendations (R1)


class CostBreakdown(BaseModel):     # from CostModel (C2/C3)
    notional: DecimalStr
    total_cost: DecimalStr
    breakeven_pct: DecimalStr
    expected_edge_pct: DecimalStr
    edge_multiple: DecimalStr       # must be ≥ edge_multiple_min [tunable]
    components: dict[str, DecimalStr]   # brokerage, stt, txn, sebi, stamp, gst, dp, spread (2026-08-13)


class GateVerdict(BaseModel):
    verdict_id: str
    proposal_id: str
    verdict: Literal["approve", "shrink", "reject", "owner_approval_required"]
    original_qty: int | None = None
    approved_qty: int | None = None     # shrink: cost model re-run on approved_qty (R1)
    checks: list[CheckResult]           # every rule evaluated, pass or fail — full audit (R8)
    cost: CostBreakdown | None = None
    reasons: list[str] = Field(default_factory=list)
    mode: Mode
    risk_state: RiskState
    degrade_tier: str
    evaluated_at: AwareDatetime         # platform-stamped (Clock); tz-aware IST only (§9.1)


# --------------------------------------------------------------------------- Recommendation (§3.6)
class Recommendation(BaseModel):
    rec_id: str
    created_at: AwareDatetime           # tz-aware IST only (§9.1)
    valid_until: AwareDatetime
    kind: Literal["entry", "exit", "adjust"]
    instrument: str
    side: Literal["BUY", "SELL"]
    style: Literal["intraday", "swing", "position"]
    product: Literal["MIS", "CNC"]
    entry_zone: tuple[DecimalStr, DecimalStr]
    stop: DecimalStr
    targets: list[DecimalStr]
    qty: int
    notional: DecimalStr                # gate-approved size (R1)
    thesis: str
    confidence: float
    short_flag_higher_tail_risk: bool = False     # shorts marked per shorting policy (C8)
    gate: GateVerdict                   # verdict + per-rule headroom ship in payload (R1)
    cost: CostBreakdown                 # this trade's specific breakeven math (C3)
    manual_checklist: list[str]         # B7/R3 protective-order checklist for the human
