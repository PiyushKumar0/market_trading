"""Structured-output schema round-trip (Phase-0 deliverable, §8.1; temporal handling §9.1).

Asserts:
- each of the five action types validates back into its Pydantic model;
- Decimal price/stop/target fields round-trip as STRINGS (locked convention);
- platform-stamped temporal/identity fields (valid_until, proposal_id, agent_id, inputs_digest) are
  OVERWRITTEN post-parse — any LLM-supplied value is discarded;
- naive datetimes are rejected on persisted models (no-naive-datetime invariant);
- the action union exports as an SDK structured-output JSON schema with a discriminator.
"""

from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from engine.core.clock import IST
from engine.core.enums import Mode, RiskState
from engine.intelligence.schemas import (
    ACTION_MODELS,
    CheckResult,
    EnterAction,
    GateVerdict,
    action_proposal_json_schema,
    parse_and_stamp,
)

STAMP = dict(
    proposal_id="01J0PLATFORMULID",
    agent_id="intraday_analyst",
    valid_until=datetime(2026, 6, 17, 10, 5, tzinfo=IST),
    inputs_digest="sha256:deadbeef",
)

# Raw "LLM output" for each action type — WITHOUT platform-stamped fields (the model never emits them).
RAW_BY_ACTION = {
    "enter": {
        "action": "enter",
        "thesis": "ORB breakout above the 30-min opening range on strong relative volume.",
        "confidence": 0.62,
        "tradingsymbol": "RELIANCE",
        "exchange": "NSE",
        "side": "BUY",
        "style": "intraday",
        "entry_type": "LIMIT",
        "entry_price": "1402.50",
        "stop_price": "1390.00",
        "target_price": "1421.00",
        "quantity": 14,
        "signal_id": "sig-123",
        "strategy_id": "orb",
        "features_snapshot_id": "fs-9",
    },
    "exit": {
        "action": "exit",
        "thesis": "Thesis invalidated: price closed back inside the opening range.",
        "confidence": 0.7,
        "position_id": "pos-1",
        "exit_type": "MARKET",
        "reason": "thesis_invalidated",
    },
    "modify-stop": {
        "action": "modify-stop",
        "thesis": "Trail the stop up to breakeven after a favourable move of 1R.",
        "confidence": 0.66,
        "position_id": "pos-1",
        "new_stop": "1402.50",
    },
    "modify-target": {
        "action": "modify-target",
        "thesis": "Extend the target as momentum persists into the trade window close.",
        "confidence": 0.6,
        "position_id": "pos-1",
        "new_target": "1435.00",
    },
    "cancel": {
        "action": "cancel",
        "thesis": "Cancel the resting LIMIT entry — the setup is stale after the news print.",
        "confidence": 0.8,
        "order_id": "ord-1",
    },
}


@pytest.mark.parametrize("action", list(ACTION_MODELS))
def test_each_action_type_round_trips_and_is_stamped(action: str) -> None:
    raw = RAW_BY_ACTION[action]
    model = parse_and_stamp(raw, **STAMP)
    assert isinstance(model, ACTION_MODELS[action])
    assert model.action == action
    assert model.proposal_id == STAMP["proposal_id"]
    assert model.agent_id == STAMP["agent_id"]
    assert model.valid_until == STAMP["valid_until"]
    assert model.inputs_digest == STAMP["inputs_digest"]


def test_decimal_prices_round_trip_as_strings() -> None:
    model = parse_and_stamp(RAW_BY_ACTION["enter"], **STAMP)
    assert isinstance(model, EnterAction)
    assert model.entry_price == Decimal("1402.50")
    dumped = model.model_dump(mode="json")
    # Prices serialise to JSON STRINGS, never floats (locked convention, §8.1).
    assert isinstance(dumped["entry_price"], str)
    assert dumped["entry_price"] == "1402.50"
    assert isinstance(dumped["stop_price"], str)
    # Full JSON round-trip is byte-clean and re-parses.
    text = model.model_dump_json()
    reparsed = json.loads(text)
    assert reparsed["target_price"] == "1421.00"


def test_decimal_from_float_avoids_binary_artifacts() -> None:
    raw = {**RAW_BY_ACTION["enter"], "entry_price": 1402.5, "stop_price": 1390.1}
    model = parse_and_stamp(raw, **STAMP)
    assert model.entry_price == Decimal("1402.5")
    # 1390.1 as a float would be 1390.0999999...; str()-first conversion keeps it clean.
    assert model.stop_price == Decimal("1390.1")


def test_llm_supplied_temporal_and_identity_are_discarded() -> None:
    # The model tries to set its own ids + a date — all must be overwritten by the platform stamp.
    raw = {
        **RAW_BY_ACTION["enter"],
        "proposal_id": "LLM-INVENTED",
        "agent_id": "LLM-INVENTED",
        "valid_until": "1999-01-01T00:00:00+05:30",
        "inputs_digest": "LLM-INVENTED",
    }
    model = parse_and_stamp(raw, **STAMP)
    assert model.proposal_id == STAMP["proposal_id"]
    assert model.valid_until == STAMP["valid_until"]
    assert model.agent_id == STAMP["agent_id"]
    assert model.inputs_digest == STAMP["inputs_digest"]


def test_naive_datetime_rejected_on_persisted_models() -> None:
    naive = datetime(2026, 6, 17, 10, 5)  # no tzinfo
    with pytest.raises(ValidationError):
        GateVerdict(
            verdict_id="v1",
            proposal_id="p1",
            verdict="approve",
            checks=[CheckResult(rule_id="per_trade_risk", passed=True, value="0", limit="200", headroom="ok")],
            mode=Mode.RECOMMEND,
            risk_state=RiskState.NORMAL,
            degrade_tier="DG0",
            evaluated_at=naive,
        )


def test_extra_keys_forbidden() -> None:
    raw = {**RAW_BY_ACTION["enter"], "surprise_field": 1}
    with pytest.raises(ValidationError):
        parse_and_stamp(raw, **STAMP)


def test_constraints_enforced_client_side() -> None:
    # thesis too short
    with pytest.raises(ValidationError):
        parse_and_stamp({**RAW_BY_ACTION["enter"], "thesis": "short"}, **STAMP)
    # confidence out of range
    with pytest.raises(ValidationError):
        parse_and_stamp({**RAW_BY_ACTION["enter"], "confidence": 1.7}, **STAMP)
    # non-positive quantity
    with pytest.raises(ValidationError):
        parse_and_stamp({**RAW_BY_ACTION["enter"], "quantity": 0}, **STAMP)


def test_action_union_exports_json_schema_with_discriminator() -> None:
    schema = action_proposal_json_schema()
    assert isinstance(schema, dict)
    text = json.dumps(schema)
    assert "enter" in text and "discriminator" in text


def test_decimal_fields_typed_string_in_wire_schema() -> None:
    # The LLM-facing (SDK structured-output) schema must type prices as "string", never a JSON number,
    # so the model cannot emit a float that corrupts a price/tick (§8.1 decimal-as-string convention).
    schema = action_proposal_json_schema()
    props = schema["$defs"]["EnterAction"]["properties"]
    assert props["stop_price"].get("type") == "string"          # mandatory decimal
    entry = props["entry_price"]                                  # optional decimal (str | None)
    types = {v.get("type") for v in entry.get("anyOf", [entry])}
    assert "string" in types and "number" not in types


def test_naive_platform_valid_until_rejected_by_parse_and_stamp() -> None:
    # A naive platform-supplied valid_until must be caught by the post-stamp re-validation, not slip
    # through model_copy unchecked (§9.1 no-naive-datetime invariant).
    naive_stamp = {**STAMP, "valid_until": datetime(2026, 6, 17, 10, 5)}  # no tzinfo
    with pytest.raises(ValidationError):
        parse_and_stamp(RAW_BY_ACTION["enter"], **naive_stamp)
