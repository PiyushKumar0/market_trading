"""hi52 (2026-09-01 design) — 52-week-high-proximity continuation, SHADOW.

Fixtures use ``_fresh_cross_rows`` to build a hand-computable 22-row history: ``lead_n`` quiet
sessions at ``lead_close`` (below the 0.95 proximity band), one more session at ``prev_close``
(becomes ``y-1``), then ``y`` at ``close_y``. Every row's ``high`` is identical, so the windowed
52wk-high is the same constant regardless of which window (live vs. previous-session) it is drawn
from — that is what makes ``prox`` values hand-computable. Tests that only need to exercise the
``min_sessions`` length gate use the module's OWN ``DEFAULT_PARAMS`` (126), per the spec's literal
wording; every other test overrides ``lookback_sessions``/``min_sessions`` down to a small,
hand-computable size (mirrors brk20's tests overriding ``lookback_days`` for the same reason).
"""

from __future__ import annotations

import math
from datetime import date, timedelta
from decimal import Decimal

from engine.strategy.scanners import hi52
from engine.strategy.scanners.hi52 import DailyRow, diagnostics_for, scan_daily, sweep_daily

TODAY = date(2026, 9, 1)

# Small envelope override shared by most tests: 20-session lookback, 22-row minimum history — keeps
# fixtures at 22 rows instead of the real 126/252 defaults, without touching any gating semantics.
SMALL_P = {"lookback_sessions": 20, "min_sessions": 22}


def _flat_rows(n: int, high: float = 100.0, close: float = 94.0, volume: float = 1000.0) -> list[DailyRow]:
    return [DailyRow(high=high, close=close, volume=volume, open=close) for _ in range(n)]


def _fresh_cross_rows(
    close_y: float,
    *,
    prev_close: float = 94.0,
    lead_close: float = 94.0,
    high: float = 100.0,
    lead_n: int = 20,
    volume: float = 1000.0,
    volume_y: float = 1500.0,
) -> list[DailyRow]:
    """``lead_n`` quiet sessions at ``lead_close`` (prox 0.94, below the 0.95 band), one more at
    ``prev_close`` (this becomes ``y-1``), then ``y`` at ``close_y``. Total ``lead_n + 2`` rows."""
    rows = [DailyRow(high=high, close=lead_close, volume=volume, open=lead_close) for _ in range(lead_n)]
    rows.append(DailyRow(high=high, close=prev_close, volume=volume, open=prev_close))
    rows.append(DailyRow(high=high, close=close_y, volume=volume_y, open=close_y))
    return rows


# ============================================================ 1. fresh-cross fires
def test_fresh_cross_fires_with_correct_prox_stop_and_score():
    rows = _fresh_cross_rows(close_y=96.0)   # prox 0.96 vs prev 0.94 — a fresh cross of the 0.95 band
    cand = scan_daily("ALPHA", rows, today=TODAY, params=SMALL_P)
    assert cand is not None
    assert cand.strategy_id == "hi52"
    assert cand.side == "BUY" and cand.style == "swing"
    assert cand.raw_levels.entry == Decimal("96.00")            # close(y), tick-rounded
    assert cand.raw_levels.stop == Decimal("90.25")             # 96 x (1 - 0.06) = 90.24 -> tick 90.25
    assert cand.raw_levels.target is None                       # no fabricated target (shadow)
    assert abs(cand.score - 0.96) < 1e-9                        # score == prox, clamped to [0, 1]


# ============================================================ 2. parked-above does not re-fire
def test_parked_above_the_band_does_not_refire():
    # y-1 already at prox 0.96 (>= 0.95) on ITS OWN window -> not a fresh cross, even though y also
    # clears the band.
    rows = _fresh_cross_rows(close_y=96.0, prev_close=96.0)
    assert scan_daily("ALPHA", rows, today=TODAY, params=SMALL_P) is None


# ============================================================ 3. min_sessions gate (literal default)
def test_min_sessions_gate_on_the_envelope_default():
    assert hi52.DEFAULT_PARAMS["min_sessions"] == 126
    rows = _flat_rows(125)                                       # one short of the default 126
    assert scan_daily("X", rows, today=TODAY) is None


# ============================================================ 4. volume confirmation
def test_volume_unconfirmed_breakout_refused():
    p = {**SMALL_P, "vol_mult": 1.2}
    # mean of the 20 sessions before y is 1000; 1.2x that is 1200; y's volume of 1100 falls short.
    rows = _fresh_cross_rows(close_y=96.0, volume_y=1100.0)
    assert scan_daily("ALPHA", rows, today=TODAY, params=p) is None
    # At exactly vol_mult x avg it fires (>= comparison), all else unchanged.
    rows_ok = _fresh_cross_rows(close_y=96.0, volume_y=1200.0)
    assert scan_daily("ALPHA", rows_ok, today=TODAY, params=p) is not None


# ============================================================ 5. ex-date veto
def test_ex_date_inside_horizon_skips_and_is_counted():
    rows = _fresh_cross_rows(close_y=96.0)
    counts: dict[str, int] = {}
    assert scan_daily(
        "ALPHA", rows, today=TODAY, upcoming_ex_dates=[TODAY + timedelta(days=5)],
        params=SMALL_P, veto_counts=counts,
    ) is None
    assert counts == {hi52.VETO_EX_DATE_SKIP: 1}
    # Outside the 10-calendar-day horizon: fires, and the counter is untouched.
    assert scan_daily(
        "ALPHA", rows, today=TODAY, upcoming_ex_dates=[TODAY + timedelta(days=30)],
        params=SMALL_P, veto_counts=counts,
    ) is not None
    assert counts == {hi52.VETO_EX_DATE_SKIP: 1}


# ============================================================ 6. sweep_daily determinism
def test_sweep_orders_by_score_desc_then_symbol():
    strong = _fresh_cross_rows(close_y=99.0)     # prox 0.99 -> highest score
    tied_a = _fresh_cross_rows(close_y=96.0)     # prox 0.96
    tied_b = _fresh_cross_rows(close_y=96.0)     # prox 0.96, tied with tied_a
    out = sweep_daily(
        {"ZED": tied_b, "ALPHA": tied_a, "MID": strong}, today=TODAY, params=SMALL_P
    )
    assert [c.symbol for c in out] == ["MID", "ALPHA", "ZED"]     # score desc, then symbol asc on ties
    assert all(c.signal_id for c in out) and len({c.signal_id for c in out}) == 3


# ============================================================ 7. never-raise
def test_never_raises_on_malformed_short_or_empty_rows():
    # Length-gate refusals on ordinary/empty data.
    assert scan_daily("X", [], today=TODAY) is None
    assert scan_daily("X", _flat_rows(3), today=TODAY) is None

    # Full-length-enough (per SMALL_P) but structurally malformed rows must still fail to None.
    nan_close = _fresh_cross_rows(close_y=96.0)
    nan_close[-1] = nan_close[-1]._replace(close=math.nan)
    assert scan_daily("X", nan_close, today=TODAY, params=SMALL_P) is None

    inf_close = _fresh_cross_rows(close_y=96.0)
    inf_close[-2] = inf_close[-2]._replace(close=math.inf)     # corrupts the y-1 fresh-cross read
    assert scan_daily("X", inf_close, today=TODAY, params=SMALL_P) is None

    zero_high = _fresh_cross_rows(close_y=96.0, high=0.0)       # hi <= 0.0 everywhere
    assert scan_daily("X", zero_high, today=TODAY, params=SMALL_P) is None

    nan_vol_window = _fresh_cross_rows(close_y=96.0, volume=math.nan)   # NaN volumes in the mean
    assert scan_daily("X", nan_vol_window, today=TODAY, params=SMALL_P) is None

    nan_vol_y = _fresh_cross_rows(close_y=96.0, volume_y=math.nan)      # NaN volume on y itself
    assert scan_daily("X", nan_vol_y, today=TODAY, params=SMALL_P) is None

    negative_volumes = [r._replace(volume=-500.0) for r in _fresh_cross_rows(close_y=96.0)]
    # avg_vol (-500) < 0.0 trips the explicit non-negative guard directly -> refused, never a crash.
    assert scan_daily("X", negative_volumes, today=TODAY, params=SMALL_P) is None


# ============================================================ 8. smoothness diagnostics
def test_smoothness_diagnostics_present_and_correctly_computed():
    # 21 closes, alternating +5 / -2 from a base of 100 -> exactly 10 up-days and 10 down-days over
    # the 20 pairs, independently recomputed below (never reusing hi52's own arithmetic to check it).
    closes = [100.0]
    for i in range(20):
        closes.append(closes[-1] + (5.0 if i % 2 == 0 else -2.0))
    highs = [c + 1.0 for c in closes]     # keep high strictly above close everywhere
    rows = [DailyRow(high=h, close=c, volume=1000.0, open=c) for h, c in zip(highs, closes)]

    diag = diagnostics_for(rows, params={"lookback_sessions": 21})
    assert diag is not None

    pairs = list(zip(closes, closes[1:]))
    expected_up_frac = round(sum(1 for prev, cur in pairs if cur > prev) / len(pairs), 2)
    expected_max_move = round(max(abs(cur / prev - 1.0) for prev, cur in pairs), 4)
    expected_hi = max(highs)
    expected_prox = round(closes[-1] / expected_hi, 4)

    assert expected_up_frac == 0.50                 # sanity anchor: 10 up / 20 pairs, by construction
    assert diag.up_day_frac == expected_up_frac
    assert diag.max_day_move == expected_max_move
    assert diag.high_52wk == expected_hi
    assert diag.prox == expected_prox

    # Diagnostics are informational only: this history's prox (well under 0.95 given +1 headroom
    # every session) never fires a candidate, proving the two paths are independent.
    assert scan_daily("SMOOTH", rows, today=TODAY, params={"lookback_sessions": 21, "min_sessions": 21}) is None


# ============================================================ default-params regression guard
def test_default_params_match_the_envelope_spec():
    assert hi52.DEFAULT_PARAMS == {
        "proximity_min": 0.95,
        "lookback_sessions": 252,
        "min_sessions": 126,
        "vol_mult": 1.0,
        "ex_skip_days": 10,
        "hold_sessions": 20,
        "stop_pct": 6.0,
    }
