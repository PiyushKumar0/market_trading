"""Pool widening study (``edge/pool_widening``): the liquidity bands shared by the spread measurement and the study.

A name's band on a day comes from its median traded value (close x volume) over its last 20 sessions through that
day, ranked among the listed equities that have them:

- ``C0`` (control): the 500 most liquid names, a point-in-time stand-in for the NIFTY 500 core;
- ``W1``: past rank 500 with at least Rs 5 cr, today's engine floor;
- ``W2``: from Rs 2 cr to under Rs 5 cr, the lowered floor under test.

Names under Rs 2 cr have no band.
"""

from __future__ import annotations

CORE_RANK = 500
FLOOR_W1_INR = 50_000_000
FLOOR_W2_INR = 20_000_000
LIQ_SESSIONS = 20
LIQ_MIN_SESSIONS = 15

C0, W1, W2 = "C0_core_proxy", "W1_beyond_core_5cr", "W2_2cr_to_5cr"
BANDS = (C0, W1, W2)
WIDENING_BANDS = (W1, W2)


def band_of(rank: int, median_inr: float) -> str | None:
    """The band of a name with liquidity ``rank`` (1 = most liquid) and median traded value ``median_inr``."""
    if rank <= CORE_RANK:
        return C0
    if median_inr >= FLOOR_W1_INR:
        return W1
    if median_inr >= FLOOR_W2_INR:
        return W2
    return None
