"""Per-strategy evaluation contracts: the registry itself.

The contract block is what the analyst judges a candidate *inside*, so the properties that matter
are coverage (every shipped leg has a frame), honesty (every frame states its evidence status), and
the ``ins`` frame's specific anti-declines.

Rendering (indentation, the ``strategy contract (<id>):`` header, the missing-contract warning) is
the context assembler's job and is tested in ``test_context_assembler.py``.
"""

from __future__ import annotations

import pytest

from engine.strategy.contracts import STRATEGY_CONTRACTS, UNKNOWN_CONTRACT, contract_text

#: Every deterministic origination leg. A new scanner without a contract must fail HERE, not reach
#: the analyst with an UNKNOWN frame.
EXPECTED_STRATEGIES = {"orb", "rsi2", "trend", "mom", "cat", "brk20", "ins", "cat_reversal", "hi52"}


def test_every_shipped_strategy_has_exactly_one_contract():
    assert set(STRATEGY_CONTRACTS) == EXPECTED_STRATEGIES


@pytest.mark.parametrize("strategy_id", sorted(EXPECTED_STRATEGIES))
def test_each_contract_is_non_empty_and_states_its_evidence_status(strategy_id: str):
    """Evidence status differs per leg (validated / forward test / exploratory / shadow), so each frame states it."""
    text = STRATEGY_CONTRACTS[strategy_id]
    assert text.strip(), strategy_id
    assert "evidence status" in text, strategy_id


def test_the_ins_contract_carries_its_three_load_bearing_clauses():
    """``ins`` is the only validated edge, was validated on unconditional next-open entries with no
    price/volume/trend filter, and ships ``target=None`` by design. Below-VWAP/downtrend is its
    expected entry population (insider buys cluster in drawdowns), so none of that is a decline ground.
    """
    text = STRATEGY_CONTRACTS["ins"]
    assert "ONLY validated edge" in text
    assert "never invent one" in text
    assert "EXPECTED entry population" in text


def test_the_hi52_contract_carries_the_clauses_the_promotion_rests_on():
    """`hi52` is a FORWARD TEST (neither validated nor shadow); three clauses keep that honest.

    (1) It ships ``target=None`` by design (the drift was measured over the 20-session horizon, never
    predicted to a level), so a missing target is not a decline ground. (2) Approach shape is not one
    either: the rule itself gates on the smooth-approach and no-gap-day filters. (3) The decline
    grounds are stated EXHAUSTIVELY, as in `ins`; legs leaving them implicit leave the door open.
    """
    text = STRATEGY_CONTRACTS["hi52"]
    assert "target=None BY DESIGN" in text
    assert "never invent one" in text
    assert "approach shape is NOT a decline ground" in text
    assert "valid decline grounds, exhaustively" in text
    assert "FORWARD TEST" in text
    # Stale shadow-era claims must not survive a merge.
    assert "cannot become a recommendation" not in text
    assert "outside the gate-approvable watchlist" not in text


def test_an_unregistered_strategy_id_gets_the_explicit_unknown_frame():
    """Total, never raising, never silently empty: an unknown id is a deployment defect the analyst is told about."""
    assert contract_text("nope") == "UNKNOWN (unregistered strategy_id — evaluate conservatively and flag it)"
    assert contract_text("nope") == UNKNOWN_CONTRACT


@pytest.mark.parametrize("strategy_id", sorted(EXPECTED_STRATEGIES))
def test_contract_text_returns_the_mapped_body_for_a_known_id(strategy_id: str):
    assert contract_text(strategy_id) == STRATEGY_CONTRACTS[strategy_id]
