"""Plan §9.4 cases with no rewrite for the paper autopilot, each kept skipped with its reason (plan Q4.5).

Case 11 is rewritten for paper in ``test_case11_gtt_fired_rejected_or_unfilled.py``.
"""

from __future__ import annotations

import pytest

from tests.chaos.conftest import MIS_OUT_OF_PAPER_SCOPE, REAL_BROKER_ONLY

_KEPT = {
    "01_kill9_with_open_leveraged_mis": MIS_OUT_OF_PAPER_SCOPE,
    "02a_restart_before_window_end": MIS_OUT_OF_PAPER_SCOPE,
    "02b_offline_across_window_end": MIS_OUT_OF_PAPER_SCOPE,
    "07_lower_circuit_lock_mis_long": MIS_OUT_OF_PAPER_SCOPE,
    "08_upper_circuit_lock_mis_short": MIS_OUT_OF_PAPER_SCOPE,
    "09_index_circuit_breaker_halt": f"{MIS_OUT_OF_PAPER_SCOPE} (the halt's square-off recompute)",
    "10_full_broker_outage": REAL_BROKER_ONLY,
    "12_broker_rms_force_close": REAL_BROKER_ONLY,
}


@pytest.mark.parametrize("case", sorted(_KEPT))
def test_phase3_gated_case(case: str) -> None:
    pytest.skip(f"case {case}: {_KEPT[case]}")
