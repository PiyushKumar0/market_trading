"""Structured-output schema round-trip.

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
    ExitAction,
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
    # Prices serialise to JSON STRINGS, never floats (locked convention).
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
    # so the model cannot emit a float that corrupts a price/tick (decimal-as-string convention).
    schema = action_proposal_json_schema()
    props = schema["$defs"]["EnterAction"]["properties"]
    assert props["stop_price"].get("type") == "string"          # mandatory decimal
    entry = props["entry_price"]                                  # optional decimal (str | None)
    types = {v.get("type") for v in entry.get("anyOf", [entry])}
    assert "string" in types and "number" not in types


def test_naive_platform_valid_until_rejected_by_parse_and_stamp() -> None:
    # A naive platform-supplied valid_until must be caught by the post-stamp re-validation, not slip
    # through model_copy unchecked (no-naive-datetime invariant).
    naive_stamp = {**STAMP, "valid_until": datetime(2026, 6, 17, 10, 5)}  # no tzinfo
    with pytest.raises(ValidationError):
        parse_and_stamp(RAW_BY_ACTION["enter"], **naive_stamp)


# --------------------------------------------------------------- guidance schema
def test_intraday_guidance_schema_is_flat_and_covers_the_union() -> None:
    """The runtime's output_format silently falls back to TEXT mode on any oneOf/anyOf union, so the
    knob gets a FLAT merge of the union. Fails when a union keyword sneaks back in or a variant grows
    a model-emitted field the guidance does not cover."""
    from engine.intelligence.schemas import NoActionOutput, intraday_guidance_json_schema

    g = intraday_guidance_json_schema()

    def walk(node) -> None:
        if isinstance(node, dict):
            for bad in ("oneOf", "anyOf", "allOf", "$ref", "discriminator"):
                assert bad not in node, f"union keyword {bad!r} disengages structured output"
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for v in node:
                walk(v)

    walk(g)
    assert set(g["properties"]["action"]["enum"]) == set(ACTION_MODELS) | {"no_action"}

    stamped = {"schema_version", "proposal_id", "agent_id", "valid_until", "inputs_digest"}
    for model in [*ACTION_MODELS.values(), NoActionOutput]:
        for field in model.model_fields:
            if field in stamped:
                continue
            assert field in g["properties"], f"{model.__name__}.{field} missing from guidance schema"


def test_no_action_accepts_the_fields_the_guidance_schema_advertises():
    """The FLAT guidance schema advertises ``thesis``/``confidence`` for EVERY action, so the model
    legitimately attaches them when declining: validation must accept what guidance invites. Foreign
    fields (e.g. a quantity) stay rejected: ``extra="forbid"`` keeps its teeth."""
    from engine.intelligence.schemas import NoActionOutput

    out = NoActionOutput.model_validate({
        "action": "no_action",
        "reason": "chop — the range has not resolved",
        "confidence": 0.35,
        "thesis": "no edge at this volume",
    })
    assert out.reason.startswith("chop")
    assert out.confidence == 0.35

    with pytest.raises(ValidationError):
        NoActionOutput.model_validate({
            "action": "no_action", "reason": "chop — no resolution", "quantity": 10,
        })
    with pytest.raises(ValidationError):
        NoActionOutput.model_validate({
            "action": "no_action", "reason": "chop — no resolution", "confidence": 1.7,
        })


# ------------------------------------------------- guidance-extras sanitizer
# The FLAT wire schema advertises every action's fields for EVERY action while the authoritative
# union forbids extras on each variant, and a retry re-emits the shape the schema keeps inviting.
# parse_intraday drops exactly the advertised-but-wrong-for-this-action keys before validating.


def _enter_no_action_stamp(model) -> None:
    """Every stamped action carries the platform values, never the model's."""
    assert model.proposal_id == STAMP["proposal_id"]
    assert model.agent_id == STAMP["agent_id"]
    assert model.valid_until == STAMP["valid_until"]
    assert model.inputs_digest == STAMP["inputs_digest"]


def test_no_action_with_enter_only_extras_parses() -> None:
    """The live no_action shape: the model attaches entry identity fields it was shown."""
    from engine.intelligence.schemas import NoActionOutput, parse_intraday

    out = parse_intraday(
        {
            "action": "no_action",
            "reason": "brk20 candidate failed the volume confirmation",
            "tradingsymbol": "ICICIAMC",
            "signal_id": "sig-123",
            "strategy_id": "brk20",
        },
        **STAMP,
    )
    assert isinstance(out, NoActionOutput)
    assert out.reason == "brk20 candidate failed the volume confirmation"


def test_enter_with_no_action_extras_parses_and_is_stamped() -> None:
    """A valid ``enter`` carrying no_action's ``reason`` parses."""
    from engine.intelligence.schemas import parse_intraday

    raw = {**RAW_BY_ACTION["enter"], "reason": "breakout confirmed on the 20-day high"}
    model = parse_intraday(raw, **STAMP)
    assert isinstance(model, EnterAction)
    assert model.tradingsymbol == "RELIANCE"
    assert model.quantity == 14
    _enter_no_action_stamp(model)


def test_sanitizer_does_not_admit_genuinely_foreign_keys() -> None:
    """A key the guidance schema never advertised is a confabulation and still dies schema_invalid."""
    from engine.intelligence.schemas import parse_intraday

    with pytest.raises(ValidationError):
        parse_intraday({**RAW_BY_ACTION["enter"], "frobnicate": 1}, **STAMP)


def test_sanitizer_does_not_mask_a_missing_required_field() -> None:
    """Dropping extras must not soften required-field enforcement: a stopless enter still dies."""
    from engine.intelligence.schemas import parse_intraday

    raw = {**RAW_BY_ACTION["enter"], "reason": "looks strong"}
    raw.pop("stop_price")
    with pytest.raises(ValidationError):
        parse_intraday(raw, **STAMP)


def test_no_action_with_price_extras_parses() -> None:
    """``limit_price``/``new_stop`` are advertised for every action and alien to no_action."""
    from engine.intelligence.schemas import NoActionOutput, parse_intraday

    out = parse_intraday(
        {
            "action": "no_action",
            "reason": "no candidate cleared the gate today",
            "limit_price": "101.25",
            "new_stop": "99.00",
        },
        **STAMP,
    )
    assert isinstance(out, NoActionOutput)


def test_sanitizer_handles_the_json_string_input_path() -> None:
    """The harness hands parse_output the raw JSON TEXT, so the string path must sanitize too."""
    from engine.intelligence.schemas import parse_intraday

    text = json.dumps({**RAW_BY_ACTION["enter"], "reason": "breakout confirmed"})
    model = parse_intraday(text, **STAMP)
    assert isinstance(model, EnterAction)
    _enter_no_action_stamp(model)


def test_unrecognised_action_sanitizes_nothing_and_still_fails() -> None:
    """An unknown/missing discriminator is not reshaped into validity — it fails as it always did."""
    from engine.intelligence.schemas import parse_intraday

    with pytest.raises(ValidationError):
        parse_intraday({"action": "levitate", "reason": "why not"}, **STAMP)
    with pytest.raises(ValidationError):
        parse_intraday({"reason": "no discriminator at all"}, **STAMP)


def test_sanitizer_logs_one_line_naming_what_it_dropped(caplog) -> None:
    """One structlog line per sanitized payload with the action and the sorted dropped keys (the mismatch stays visible)."""
    import logging

    from engine.intelligence.schemas import parse_intraday

    with caplog.at_level(logging.INFO, logger="engine.intelligence.schemas"):
        parse_intraday(
            {**RAW_BY_ACTION["enter"], "reason": "breakout", "exit_type": "MARKET"}, **STAMP
        )
    records = [r for r in caplog.records if r.getMessage() == "guidance_extras_dropped"]
    assert len(records) == 1
    assert records[0].action == "enter"
    assert records[0].dropped == ["exit_type", "reason"]


def test_clean_payloads_log_nothing(caplog) -> None:
    """No extras ⇒ no log line (the sanitizer is silent on the normal path)."""
    import logging

    from engine.intelligence.schemas import parse_intraday

    with caplog.at_level(logging.INFO, logger="engine.intelligence.schemas"):
        parse_intraday(RAW_BY_ACTION["enter"], **STAMP)
        parse_intraday({"action": "no_action", "reason": "nothing set up today"}, **STAMP)
    assert not [r for r in caplog.records if r.getMessage() == "guidance_extras_dropped"]


def test_platform_stamped_fields_are_not_dropped_by_the_sanitizer() -> None:
    """proposal_id/agent_id/valid_until/inputs_digest are not advertised, so the sanitizer leaves them
    for parse_and_stamp to overwrite."""
    from engine.intelligence.schemas import parse_intraday

    raw = {
        **RAW_BY_ACTION["enter"],
        "reason": "breakout",
        "proposal_id": "LLM-INVENTED",
        "agent_id": "LLM-INVENTED",
        "valid_until": "1999-01-01T00:00:00+05:30",
        "inputs_digest": "LLM-INVENTED",
    }
    _enter_no_action_stamp(parse_intraday(raw, **STAMP))


# --------------------------------------------- prose-overflow clamp
def test_overlong_advertised_thesis_is_clamped_not_fatal() -> None:
    """The wire schema advertises `thesis` as a bare string, so an advertised prose field overflowing
    the MATCHED model's declared max_length is clamped, not fatal: retries must converge and an exit
    decision must never die of verbosity."""
    from engine.intelligence.schemas import parse_intraday

    long_thesis = "Swing CNC long from 2668.80; " + ("invalidation detail " * 40)
    assert len(long_thesis) > 600
    model = parse_intraday({**RAW_BY_ACTION["exit"], "thesis": long_thesis}, **STAMP)
    assert model.thesis == long_thesis[:600]


def test_clamp_keys_off_the_matched_models_own_cap() -> None:
    """NoActionOutput.thesis is unconstrained, so the same over-long prose passes UNCLAMPED: the clamp
    reads the matched model's declared max_length, not a hardcoded number."""
    from engine.intelligence.schemas import NoActionOutput, parse_intraday

    long_thesis = "declining on regime grounds " * 40
    assert len(long_thesis) > 600
    out = parse_intraday(
        {"action": "no_action", "reason": "regime adverse", "thesis": long_thesis}, **STAMP
    )
    assert isinstance(out, NoActionOutput)
    assert out.thesis == long_thesis


def test_clamp_never_softens_min_length_or_numeric_constraints() -> None:
    """Truncation is the ONLY reshaping: a too-SHORT thesis is deficient content and still dies,
    and numeric constraint enforcement (confidence <= 1) is untouched."""
    from engine.intelligence.schemas import parse_intraday

    with pytest.raises(ValidationError):
        parse_intraday({**RAW_BY_ACTION["exit"], "thesis": "too short"}, **STAMP)
    with pytest.raises(ValidationError):
        parse_intraday({**RAW_BY_ACTION["exit"], "confidence": 1.7}, **STAMP)


def test_guidance_schema_does_not_advertise_a_thesis_cap() -> None:
    """The wire schema must NOT carry maxLength for `thesis`: advertised, it makes the runtime bounce
    verbose StructuredOutput inputs turn after turn (3-4x output tokens, a terminal max-turns
    sdk_error). Prose stays uncapped on the wire; the client-side clamp converges."""
    from engine.intelligence.schemas import intraday_guidance_json_schema

    props = intraday_guidance_json_schema()["properties"]
    assert "maxLength" not in props["thesis"]
    assert "maxLength" not in props["reason"]


def test_guidance_schema_has_no_regime_note() -> None:
    from engine.intelligence.schemas import NoActionOutput, intraday_guidance_json_schema

    assert "regime_note" not in intraday_guidance_json_schema()["properties"]
    assert "regime_note" not in NoActionOutput.model_fields


# ------------------------------------------------- exit-reason enum
# ``reason`` is advertised as free text (no_action's prose field) while ExitAction declares it a
# CLOSED Literal, so exit codes travel under their own advertised key ``exit_reason`` and the
# sanitizer renames it.

#: A live failing payload: prose ``reason`` beside a thesis, plus the entry-identity extras the flat
#: schema advertises for every action.
EXIT_2026_09_10 = {
    "action": "exit",
    "thesis": (
        "HDFCAMC swing long is six sessions past its stated horizon and the brk20 level it broke out "
        "from has been lost on closing basis; the reward basis is spent and what is left is pure "
        "overnight risk into a negative-breadth tape."
    ),
    "confidence": 0.72,
    "position_id": "pos-hdfcamc-1",
    "exit_type": "MARKET",
    "reason": "Holding horizon exceeded and the breakout level has failed on a closing basis.",
    "tradingsymbol": "HDFCAMC",
    "side": "SELL",
    "quantity": 3,
    "style": "swing",
}


def test_exit_reason_code_maps_onto_the_contract_literal() -> None:
    """With a code the code becomes ``reason`` and the thesis survives."""
    from engine.intelligence.schemas import parse_intraday

    model = parse_intraday({**EXIT_2026_09_10, "exit_reason": "risk_event"}, **STAMP)
    assert isinstance(model, ExitAction)
    assert model.reason == "risk_event"
    assert model.thesis == EXIT_2026_09_10["thesis"]     # prose reasoning preserved, not displaced
    assert model.position_id == "pos-hdfcamc-1"
    assert model.exit_type == "MARKET"
    assert "exit_reason" not in model.model_dump()       # advertised-but-foreign ⇒ dropped
    _enter_no_action_stamp(model)


def test_exit_with_a_code_and_no_reason_key_at_all_parses() -> None:
    """Code in ``exit_reason``, reasoning in ``thesis``, no ``reason`` key: nothing to displace."""
    from engine.intelligence.schemas import parse_intraday

    raw = {k: v for k, v in EXIT_2026_09_10.items() if k != "reason"}
    model = parse_intraday({**raw, "exit_reason": "target_neared"}, **STAMP)
    assert model.reason == "target_neared"
    assert model.thesis == EXIT_2026_09_10["thesis"]


@pytest.mark.parametrize(
    "override", [{}, {"exit_reason": "horizon_expired"}], ids=["no-exit-reason", "out-of-enum-code"]
)
def test_an_exit_without_a_valid_code_dies_on_reason_alone(override) -> None:
    """Absent a valid code, prose in ``reason`` is never guessed into one, and a confabulated code is
    dropped like any advertised extra. Either way the payload dies on ``reason`` alone: one
    literal_error at exit.reason."""
    from engine.intelligence.schemas import parse_intraday

    with pytest.raises(ValidationError) as err:
        parse_intraday({**EXIT_2026_09_10, **override}, **STAMP)
    errors = err.value.errors()
    assert len(errors) == 1
    assert errors[0]["type"] == "literal_error"
    assert errors[0]["loc"][-1] == "reason"


def test_displaced_prose_becomes_the_thesis_only_when_there_is_none() -> None:
    """The prose the code evicts from ``reason`` becomes the thesis only where none would be overwritten."""
    from engine.intelligence.schemas import parse_intraday

    no_thesis = {k: v for k, v in EXIT_2026_09_10.items() if k != "thesis"}
    model = parse_intraday({**no_thesis, "exit_reason": "time_stop"}, **STAMP)
    assert model.reason == "time_stop"
    assert model.thesis == EXIT_2026_09_10["reason"]

    blank = parse_intraday({**EXIT_2026_09_10, "thesis": "   ", "exit_reason": "time_stop"}, **STAMP)
    assert blank.thesis == EXIT_2026_09_10["reason"]


def test_displaced_prose_that_is_itself_a_code_is_not_promoted_to_thesis() -> None:
    """``reason`` == ``exit_reason`` is no prose: promoting the bare code would manufacture reasoning."""
    from engine.intelligence.schemas import parse_intraday

    raw = {k: v for k, v in EXIT_2026_09_10.items() if k != "thesis"}
    with pytest.raises(ValidationError) as err:
        parse_intraday({**raw, "reason": "risk_event", "exit_reason": "risk_event"}, **STAMP)
    assert [e["type"] for e in err.value.errors()] == ["missing"]    # thesis missing, not faked


def test_enter_carrying_exit_reason_drops_it_and_parses() -> None:
    """``exit_reason`` is advertised for every action; outside ``exit`` it is dropped (the mapping is exit-only)."""
    from engine.intelligence.schemas import parse_intraday

    model = parse_intraday({**RAW_BY_ACTION["enter"], "exit_reason": "risk_event"}, **STAMP)
    assert isinstance(model, EnterAction)
    assert model.tradingsymbol == "RELIANCE"
    assert "exit_reason" not in model.model_dump()
    _enter_no_action_stamp(model)


def test_no_action_carrying_exit_reason_drops_it_and_keeps_its_prose() -> None:
    """A no_action's ``reason`` IS its free prose and must never be overwritten by a stray code."""
    from engine.intelligence.schemas import NoActionOutput, parse_intraday

    out = parse_intraday(
        {
            "action": "no_action",
            "reason": "position is within tolerance; no adjustment warranted",
            "exit_reason": "risk_event",
        },
        **STAMP,
    )
    assert isinstance(out, NoActionOutput)
    assert out.reason == "position is within tolerance; no adjustment warranted"


def test_guidance_exit_reason_enum_equals_the_contract_literal() -> None:
    """The wire enum is derived from ExitAction's Literal; this fails if either is retyped. The five codes
    are also pinned: the sanitizer's rename and the position-event prompt share this vocabulary, so
    growing it is a three-place decision."""
    from typing import get_args

    from engine.core.contracts import ExitAction as ContractExitAction
    from engine.intelligence.schemas import EXIT_REASON_CODES, intraday_guidance_json_schema

    literal = get_args(ContractExitAction.model_fields["reason"].annotation)
    prop = intraday_guidance_json_schema()["properties"]["exit_reason"]
    assert tuple(prop["enum"]) == literal == tuple(EXIT_REASON_CODES)
    assert prop["type"] == "string"
    assert set(literal) == {
        "thesis_invalidated", "target_neared", "risk_event", "time_stop", "other",
    }


def test_exit_reason_mapping_is_logged_and_the_key_is_dropped(caplog) -> None:
    """The rename stays visible in the log, and the dropped line proves ``exit_reason`` leaves the payload."""
    import logging

    from engine.intelligence.schemas import parse_intraday

    with caplog.at_level(logging.INFO, logger="engine.intelligence.schemas"):
        parse_intraday({**EXIT_2026_09_10, "exit_reason": "risk_event"}, **STAMP)

    mapped = [r for r in caplog.records if r.getMessage() == "guidance_exit_reason_mapped"]
    assert len(mapped) == 1
    assert mapped[0].code == "risk_event"
    assert mapped[0].prose_to_thesis is False
    dropped = [r for r in caplog.records if r.getMessage() == "guidance_extras_dropped"]
    assert len(dropped) == 1
    assert "exit_reason" in dropped[0].dropped


def test_exit_without_a_code_logs_no_mapping(caplog) -> None:
    """No code ⇒ no rename and no log line."""
    import logging

    from engine.intelligence.schemas import parse_intraday

    with caplog.at_level(logging.INFO, logger="engine.intelligence.schemas"):
        with pytest.raises(ValidationError):
            parse_intraday(EXIT_2026_09_10, **STAMP)
        parse_intraday(RAW_BY_ACTION["exit"], **STAMP)
    assert not [r for r in caplog.records if r.getMessage() == "guidance_exit_reason_mapped"]


def test_mapped_exit_survives_the_json_string_path_and_the_prose_clamp() -> None:
    """The harness hands parse_output raw JSON TEXT, and a displaced prose ``reason`` gets the same
    600-char thesis clamp: the rename runs before the clamp."""
    from engine.intelligence.schemas import parse_intraday

    long_prose = "Holding horizon exceeded. " + ("level lost and breadth hostile " * 30)
    assert len(long_prose) > 600
    raw = {k: v for k, v in EXIT_2026_09_10.items() if k != "thesis"}
    text = json.dumps({**raw, "reason": long_prose, "exit_reason": "thesis_invalidated"})
    model = parse_intraday(text, **STAMP)
    assert model.reason == "thesis_invalidated"
    assert model.thesis == long_prose[:600]
