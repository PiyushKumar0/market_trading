"""Plan §9.4 cases whose every "Must hold" clause needs a Phase-3 component that does not exist yet.

Each placeholder names the case and the missing component, so ``pytest -m chaos -rs`` lists the whole
§9.4 table and the gap is visible. Replace a placeholder with a real ``test_case<NN>_<slug>.py`` when
its component ships (WO-P3-5 AUTO(paper) routing, WO-P3-6 R3 managers — plan §8.4).
"""

from __future__ import annotations

import pytest

from tests.chaos.conftest import PHASE3_GATED

_GATED = {
    "01_kill9_with_open_leveraged_mis": "ProtectionManager (resting SL-M), Reconciler adoption",
    "02a_restart_before_window_end": "SquareOffScheduler re-arm + 15:05-15:10 backstop",
    "02b_offline_across_window_end": "startup overdue-MIS square-off, broker_squareoff_offline reconcile",
    "07_lower_circuit_lock_mis_long": "band-lock detection, MIS->CNC conversion (R3)",
    "08_upper_circuit_lock_mis_short": "auction_settled pending state, reconciler T+1/T+2 tolerance",
    "09_index_circuit_breaker_halt": "market-halt detection + square-off recompute",
    "10_full_broker_outage": "post-outage full fill reconcile",
    "11_gtt_fired_rejected_or_unfilled": "GTTManager re-arm + gtt_failure_exit",
    "12_broker_rms_force_close": "broker_rms terminal state + protective-order cancel",
}


@pytest.mark.parametrize("case", sorted(_GATED))
def test_phase3_gated_case(case: str) -> None:
    pytest.skip(f"case {case}: {PHASE3_GATED}; missing: {_GATED[case]}")
