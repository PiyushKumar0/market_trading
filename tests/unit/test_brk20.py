"""brk20 (§6.1 addendum, 2026-08-04) — 20d-high daily-close breakout, full eligible universe.

PINNED worked example: 20 flat sessions with high 100.0, then a fresh close above. All math is
hand-computed against the module's own DEFAULT_PARAMS-independent inputs (params passed
explicitly — owner envelope changes must never break these tests)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from engine.strategy.scanners import brk20
from engine.strategy.scanners.brk20 import DailyRow, scan_daily, sweep_daily

TODAY = date(2026, 8, 4)
P = {"lookback_days": 20, "vol_mult": 1.2, "rr_target": 2.0, "ex_skip_days": 10}


def _flat(n: int, high: float = 100.0, close: float = 99.0, volume: float = 1000.0) -> list[DailyRow]:
    return [DailyRow(high=high, close=close, volume=volume) for _ in range(n)]


def _series_with_breakout(breakout_volume: float = 2000.0) -> list[DailyRow]:
    """21 quiet sessions + yesterday closing at 103 above the 20d high of 100."""
    return _flat(21) + [DailyRow(high=103.5, close=103.0, volume=breakout_volume)]


def test_pinned_breakout_math():
    cand = scan_daily("BPCL", _series_with_breakout(), today=TODAY, params=P)
    assert cand is not None
    assert cand.strategy_id == "brk20"
    assert cand.side == "BUY" and cand.style == "swing"
    assert cand.raw_levels.entry == Decimal("103.00")
    assert cand.raw_levels.stop == Decimal("100.00")                       # the broken 20d high
    assert cand.raw_levels.target == Decimal("109.00")                     # 103 + 2 x (103-100)
    # score = min(1, 0.5 + 5 x (103/100 - 1)) = 0.65
    assert abs(cand.score - 0.65) < 1e-9


def test_volume_unconfirmed_breakout_refused():
    """The BPCL 2026-08-03 shape: genuine 20d-high close but ~0.8x average volume ⇒ refused.
    (Real numbers that day: close 329.95 > H20 321.90, volume 5.84M vs 7.32M avg.)"""
    assert scan_daily("BPCL", _series_with_breakout(breakout_volume=800.0), today=TODAY, params=P) is None
    # At exactly vol_mult x avg it fires (>= comparison).
    assert scan_daily("BPCL", _series_with_breakout(breakout_volume=1200.0), today=TODAY, params=P) is not None


def test_riding_above_the_band_is_not_a_fresh_cross():
    """Yesterday-1 already closed above ITS band ⇒ no re-fire while price rides the highs."""
    rows = _flat(21) + [
        DailyRow(high=103.5, close=103.0, volume=2000.0),   # the original breakout day…
        DailyRow(high=104.5, close=104.0, volume=2000.0),   # …and the day after, still above
    ]
    assert scan_daily("X", rows, today=TODAY, params=P) is None


def test_ex_date_inside_horizon_skips():
    rows = _series_with_breakout()
    assert scan_daily("X", rows, today=TODAY, upcoming_ex_dates=[date(2026, 8, 10)], params=P) is None
    # Outside the 10-calendar-day horizon: fires.
    assert scan_daily("X", rows, today=TODAY, upcoming_ex_dates=[date(2026, 9, 1)], params=P) is not None


def test_thin_history_and_non_breakout_fail_to_none():
    assert scan_daily("X", _flat(21), today=TODAY, params=P) is None          # no breakout
    assert scan_daily("X", _series_with_breakout()[-10:], today=TODAY, params=P) is None  # thin
    assert scan_daily("X", [], today=TODAY, params=P) is None


def test_sweep_orders_by_score_desc_then_symbol():
    strong = _flat(21) + [DailyRow(high=111.0, close=110.0, volume=2000.0)]   # +10% ⇒ score 1.0
    mild_a = _series_with_breakout()                                          # score 0.65
    mild_b = _series_with_breakout()
    out = sweep_daily(
        {"ZED": mild_b, "ALPHA": mild_a, "MID": strong}, today=TODAY, params=P
    )
    assert [c.symbol for c in out] == ["MID", "ALPHA", "ZED"]
    assert all(c.signal_id for c in out) and len({c.signal_id for c in out}) == 3


def test_default_params_are_the_envelope_defaults():
    assert brk20.DEFAULT_PARAMS == {
        "lookback_days": 20, "vol_mult": 1.2, "rr_target": 2.0, "ex_skip_days": 10,
    }
