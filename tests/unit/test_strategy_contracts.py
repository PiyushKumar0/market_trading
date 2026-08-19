"""Per-strategy evaluation contracts (WO-20a): the registry itself.

The contract block is what the analyst judges a candidate *inside*, so the properties that matter
here are coverage (every shipped leg has a frame), honesty (every frame states its evidence status),
and the ``ins`` frame's specific anti-declines — the funnel autopsy found the platform's ONLY
validated leg declined for being below VWAP on the day its event fired.

Rendering (indentation, the ``strategy contract (<id>):`` header, the missing-contract warning) is
the context assembler's job and is tested in ``test_context_assembler.py``.
"""

from __future__ import annotations

import pytest

from engine.strategy.contracts import STRATEGY_CONTRACTS, UNKNOWN_CONTRACT, contract_text

#: Every §6.1 deterministic origination leg. A new scanner without a contract must fail HERE, at the
#: cheapest possible place, rather than reaching the analyst with an UNKNOWN frame in production.
EXPECTED_STRATEGIES = {"orb", "rsi2", "trend", "mom", "cat", "brk20", "ins"}


def test_every_shipped_strategy_has_exactly_one_contract():
    assert set(STRATEGY_CONTRACTS) == EXPECTED_STRATEGIES


@pytest.mark.parametrize("strategy_id", sorted(EXPECTED_STRATEGIES))
def test_each_contract_is_non_empty_and_states_its_evidence_status(strategy_id: str):
    """Evidence status is stated per leg BECAUSE it differs per leg: one is validated and six are
    exploratory. That is exactly why the claim could no longer live in the shared system prompt."""
    text = STRATEGY_CONTRACTS[strategy_id]
    assert text.strip(), strategy_id
    assert "evidence status" in text, strategy_id


def test_the_ins_contract_carries_its_three_load_bearing_clauses():
    """The 2026-08-19 HCLTECH decline in three assertions.

    ``ins`` is the only validated edge (WO-16), its validation used unconditional next-open entries
    with no price/volume/trend filter, and it ships ``target=None`` by design. The decline cited
    below-VWAP/downtrend/day-plan-avoid — i.e. the expected entry population for insider-buy
    crossings, which cluster in drawdowns — and "no target, so reward cannot be framed".
    """
    text = STRATEGY_CONTRACTS["ins"]
    assert "ONLY validated edge" in text
    assert "never invent one" in text
    assert "EXPECTED entry population" in text


def test_an_unregistered_strategy_id_gets_the_explicit_unknown_frame():
    """Total, never raising, and never silently empty: an unknown id is a deployment defect and the
    analyst is told so rather than left to infer a frame."""
    assert contract_text("nope") == "UNKNOWN (unregistered strategy_id — evaluate conservatively and flag it)"
    assert contract_text("nope") == UNKNOWN_CONTRACT


@pytest.mark.parametrize("strategy_id", sorted(EXPECTED_STRATEGIES))
def test_contract_text_returns_the_mapped_body_for_a_known_id(strategy_id: str):
    assert contract_text(strategy_id) == STRATEGY_CONTRACTS[strategy_id]
