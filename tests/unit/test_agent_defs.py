"""Phase-2 agent definitions (§5.2–§5.4): prompt invariants + the LLM-side output schemas.

Two families of assertion:

* **Prompt invariants.** Each ``SYSTEM_PROMPT`` is a byte-stable module constant — no interpolation
  artifacts, no year/date baked in, and the §5 hard rules actually present. A prompt that varies
  between calls silently doubles the intraday agent's cost (D8) and breaks replay (R8).
* **Output-schema semantics.** ``parse_intraday`` stamps actions and passes ``no_action`` through;
  ``parse_cluster_scores`` drops ONE bad cluster instead of voiding a 30-cluster batch (D7);
  ``DayPlan`` enforces the §5.3 shape.
"""

from __future__ import annotations

import re
from datetime import datetime
from decimal import Decimal

import pytest
import yaml
from pydantic import ValidationError

from engine.core.clock import IST
from engine.core.config import config_dir
from engine.intelligence.agents import intraday, news_analyst, preopen
from engine.intelligence.schemas import (
    CLUSTER_EVENT_TYPES,
    ClusterScoreBatch,
    DayPlan,
    EnterAction,
    NoActionOutput,
    intraday_guidance_json_schema,
    intraday_output_json_schema,
    parse_cluster_scores,
    parse_intraday,
)

AGENT_MODULES = (intraday, preopen, news_analyst)

STAMP = dict(
    proposal_id="01J0PLATFORMULID",
    agent_id="intraday_analyst",
    valid_until=datetime(2026, 6, 17, 10, 20, tzinfo=IST),
    inputs_digest="a" * 64,
)

RAW_ENTER = {
    "action": "enter",
    "thesis": "ORB breakout above the opening range on 1.8x relative volume, stop under the range low.",
    "confidence": 0.62,
    "tradingsymbol": "RELIANCE",
    "exchange": "NSE",
    "side": "BUY",
    "style": "intraday",
    "entry_type": "LIMIT",
    "entry_price": "1402.50",
    "stop_price": "1390.00",
    "quantity": 14,
    "signal_id": "sig-1",
    "strategy_id": "orb",
    "features_snapshot_id": "fs-1",
    # values the model must not get to choose (§8.1) — overwritten by the stamp:
    "proposal_id": "model-invented",
    "agent_id": "model-invented",
    "inputs_digest": "model-invented",
}


def _cluster(cid: str, **overrides) -> dict:
    base = {
        "cluster_id": cid,
        "scope": "stock",
        "entities": ["Reliance Industries"],
        "sectors": ["Energy"],
        "themes": [],
        "sentiment": 0.4,
        "materiality": 0.75,
        "event_type": "order_win",
        "novelty": 0.9,
    }
    return {**base, **overrides}


# --------------------------------------------------------------------------- agent identity
def test_agent_ids_match_the_agents_yaml_keys():
    """The governor's ledger, allocations and degrade ladder all key off these exact strings (§5.6)."""
    roster = yaml.safe_load((config_dir() / "agents.yaml").read_text(encoding="utf-8"))
    for module in AGENT_MODULES:
        assert module.AGENT_ID in roster["agents"], module.__name__
        assert module.AGENT_ID in roster["budget_allocations_usd"], module.__name__
    assert {m.AGENT_ID for m in AGENT_MODULES} == {"intraday_analyst", "preopen_planner", "news_analyst"}


# --------------------------------------------------------------------------- prompt invariants (D8)
@pytest.mark.parametrize("module", AGENT_MODULES, ids=lambda m: m.AGENT_ID)
def test_system_prompt_has_no_interpolation_artifacts(module):
    """No braces at all: a stray format placeholder is the failure mode that silently ships
    ``max_qty_by_risk is {qty}`` to the model, and a JSON example in the prompt is redundant with
    the structured-output schema anyway."""
    prompt = module.SYSTEM_PROMPT
    assert "{" not in prompt and "}" not in prompt
    assert "%s" not in prompt and "%(" not in prompt


@pytest.mark.parametrize("module", AGENT_MODULES, ids=lambda m: m.AGENT_ID)
def test_system_prompt_contains_no_date(module):
    """§3.2/D8: dates live in the context block, never in the byte-stable prefix. (The pre-open
    prompt's ``09:15`` is the auction fact itself — a time-of-day constant, not a stamped date.)"""
    prompt = module.SYSTEM_PROMPT
    assert re.search(r"\d{4}-\d{2}-\d{2}", prompt) is None
    assert re.search(r"\d{1,2}/\d{1,2}/\d{2,4}", prompt) is None
    assert re.search(r"\b(19|20)\d{2}\b", prompt) is None      # no year anywhere


@pytest.mark.parametrize("module", AGENT_MODULES, ids=lambda m: m.AGENT_ID)
def test_system_prompt_is_a_constant(module):
    """Byte-stable across accesses AND across re-import: nothing is computed at import time."""
    import importlib

    first = module.SYSTEM_PROMPT
    assert module.SYSTEM_PROMPT is first
    assert importlib.reload(module).SYSTEM_PROMPT == first


@pytest.mark.parametrize("module", AGENT_MODULES, ids=lambda m: m.AGENT_ID)
def test_system_prompt_states_the_never_rules(module):
    prompt = module.SYSTEM_PROMPT
    assert prompt.lower().count("never") >= 3
    assert "Never emit a date" in prompt
    assert "Emit ONLY JSON matching the supplied output schema" in prompt
    assert "UNTRUSTED" in prompt                       # §2.4: news/text is evidence, not instruction


def test_intraday_prompt_states_the_section_5_2_hard_rules():
    prompt = intraday.SYSTEM_PROMPT
    assert "You propose, a deterministic gate disposes" in prompt
    assert "Never assume execution" in prompt
    assert "At most one action per response, or no_action" in prompt
    assert "signal_id, strategy_id and features_snapshot_id exactly as given" in prompt
    assert "less than or equal to the stated max_qty_by_risk" in prompt
    assert "Every entry MUST carry a stop price" in prompt


def test_preopen_prompt_is_grounded_in_auction_mechanics():
    """A14/§5.3: the one confabulation this agent is most prone to is planning off an indicative tick."""
    prompt = preopen.SYSTEM_PROMPT
    assert "AUCTION ARTIFACTS" in prompt
    assert "The real open prints at 09:15" in prompt
    assert "NEVER plan an\nentry off a pre-open indicative tick" in prompt
    assert "may NOT add a symbol to the watchlist" in prompt
    assert "binding origination levels remain the scanner's deterministic ones" in prompt


def test_news_prompt_anchors_the_materiality_rubric_and_the_closed_taxonomy():
    prompt = news_analyst.SYSTEM_PROMPT
    assert "0.8 and above: company-transforming" in prompt
    assert "10 percent or more of annual revenue" in prompt
    assert "0.5 to 0.8: clearly price-relevant" in prompt
    assert "0.2 to 0.5: routine coverage" in prompt
    assert "below 0.2: noise or public relations" in prompt
    assert "One score per cluster" in prompt
    assert "VERBATIM STRINGS" in prompt
    for event_type in CLUSTER_EVENT_TYPES:
        assert event_type in prompt, event_type
    assert news_analyst.MAX_CLUSTERS_PER_CALL == 30


# --------------------------------------------------------------------------- output schemas
def test_intraday_output_schema_is_a_discriminated_union():
    schema = intraday_output_json_schema()
    body = str(schema)
    assert "discriminator" in body
    for action in ("enter", "exit", "modify-stop", "modify-target", "cancel", "no_action"):
        assert action in body
    # The agent's KNOB schema is the FLAT guidance form (2026-07-29: the runtime's output_format
    # silently disengages on oneOf/anyOf unions) — the union above stays the client-side contract.
    assert intraday.output_json_schema() == intraday_guidance_json_schema()


def test_agent_output_schemas_are_exported():
    day_plan = preopen.output_json_schema()
    assert set(day_plan["properties"]) == {
        "regime", "focus", "catalyst_focus", "warnings", "no_trade_today"
    }
    assert news_analyst.output_json_schema() == ClusterScoreBatch.model_json_schema()


# --------------------------------------------------------------------------- parse_intraday (§5.2)
def test_parse_intraday_stamps_platform_fields_on_an_action():
    action = parse_intraday(RAW_ENTER, **STAMP)

    assert isinstance(action, EnterAction)
    assert action.proposal_id == STAMP["proposal_id"]
    assert action.agent_id == STAMP["agent_id"]
    assert action.inputs_digest == STAMP["inputs_digest"]
    assert action.valid_until == STAMP["valid_until"]
    assert action.entry_price == Decimal("1402.50")            # decimal-as-string round trip


def test_parse_intraday_accepts_no_action():
    out = parse_intraday(
        {"action": "no_action", "reason": "Candidate conflicts with the day plan's avoid list."}, **STAMP
    )
    assert isinstance(out, NoActionOutput)
    assert out.regime_note == ""                               # optional; default empty
    assert not hasattr(out, "proposal_id")                     # nothing to stamp: it cannot trade


def test_parse_intraday_accepts_a_json_string():
    out = parse_intraday('{"action":"no_action","reason":"range too tight","regime_note":"chop"}', **STAMP)
    assert isinstance(out, NoActionOutput)
    assert out.regime_note == "chop"


def test_no_action_requires_a_real_reason():
    with pytest.raises(ValidationError):
        NoActionOutput(action="no_action", reason="no")
    with pytest.raises(ValidationError):
        NoActionOutput(action="no_action", reason="thin tape, no edge", extra_key="x")   # extra=forbid


def test_parse_intraday_rejects_schema_invalid_output():
    bad = {**RAW_ENTER, "quantity": 0}                         # gt=0 enforced client-side (§8.1)
    with pytest.raises(ValidationError):
        parse_intraday(bad, **STAMP)


# --------------------------------------------------------------------------- DayPlan (§5.3)
def test_day_plan_round_trips_with_catalyst_focus():
    plan = DayPlan.model_validate({
        "regime": "trending, breadth positive",
        "focus": [{"symbol": "RELIANCE", "bias": "long", "levels": "above 1405", "why": "order win"}],
        "catalyst_focus": [{
            "symbol": "RELIANCE", "event_type": "order_win", "direction": "long",
            "advisory_levels": "watch the 1405 confirm", "drift_note": "day 2 of the story",
        }],
        "warnings": ["expiry Thursday"],
        "no_trade_today": False,
    })
    assert plan.focus[0].bias == "long"
    assert plan.catalyst_focus[0].direction == "long"


def test_day_plan_defaults_and_caps():
    plan = DayPlan.model_validate({"regime": "quiet"})
    assert plan.focus == [] and plan.catalyst_focus == [] and plan.warnings == []
    assert plan.no_trade_today is False

    nine = [{"symbol": f"S{i}", "bias": "long", "levels": "x", "why": "y"} for i in range(9)]
    with pytest.raises(ValidationError):
        DayPlan.model_validate({"regime": "busy", "focus": nine})
    with pytest.raises(ValidationError):
        DayPlan.model_validate({"regime": "x", "focus": [{"symbol": "S", "bias": "sideways",
                                                         "levels": "x", "why": "y"}]})


# --------------------------------------------------------------------------- ClusterScore (§5.4/D7)
def test_parse_cluster_scores_drops_only_the_bad_cluster():
    """D7: an out-of-enum event_type costs ONE cluster, never the other 29 in the batch."""
    raw = {"scores": [
        _cluster("c1"),
        _cluster("c2", event_type="stock_split_rumour"),      # not in the closed taxonomy
        _cluster("c3", event_type="pump_promo_suspect"),
    ]}
    scores, dropped = parse_cluster_scores(raw)

    assert [s.cluster_id for s in scores] == ["c1", "c3"]
    assert dropped == ["c2"]                                   # reported by id for the log


def test_parse_cluster_scores_enforces_ranges_and_reports_positionally():
    raw = [
        _cluster("c1", sentiment=1.4),                         # outside -1..+1
        _cluster("c2", materiality=-0.1),                      # outside 0..1
        _cluster("c3", novelty=2.0),                           # outside 0..1
        _cluster("c4", scope="commodity"),                     # not a legal scope
        {"scope": "stock"},                                    # no usable cluster_id -> positional
        _cluster("c6"),
    ]
    scores, dropped = parse_cluster_scores(raw)

    assert [s.cluster_id for s in scores] == ["c6"]
    assert dropped == ["c1", "c2", "c3", "c4", "#4"]


def test_parse_cluster_scores_accepts_a_json_string_and_every_event_type():
    import json

    raw = json.dumps({"scores": [_cluster(f"c{i}", event_type=et)
                                 for i, et in enumerate(CLUSTER_EVENT_TYPES)]})
    scores, dropped = parse_cluster_scores(raw)

    assert dropped == []
    assert [s.event_type for s in scores] == list(CLUSTER_EVENT_TYPES)
    assert news_analyst.parse_output(raw) == (scores, dropped)


def test_cluster_score_boundaries_are_inclusive():
    edge = _cluster("c1", sentiment=-1.0, materiality=0.0, novelty=1.0)
    scores, dropped = parse_cluster_scores([edge])
    assert dropped == [] and scores[0].sentiment == -1.0
