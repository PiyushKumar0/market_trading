"""hi52 (2026-09-01 design, v2 promoted 2026-09-12) — 52-week-high-proximity continuation.

Fixtures use ``_fresh_cross_rows`` to build a hand-computable 22-row history: ``lead_n`` sessions
climbing in ``lead_step`` increments to ``lead_close`` (below the 0.95 proximity band), one more
session at ``prev_close`` (becomes ``y-1``), then ``y`` at ``close_y``. Every row's ``high`` is
identical, so the windowed 52wk-high is the same constant regardless of which window (live vs.
previous-session) it is drawn from — that is what makes ``prox`` values hand-computable. Tests that
only need to exercise the ``min_sessions`` length gate use the module's OWN ``DEFAULT_PARAMS`` (126),
per the spec's literal wording; every other test overrides ``lookback_sessions``/``min_sessions``
down to a small, hand-computable size (mirrors brk20's tests overriding ``lookback_days`` for the
same reason).

The climbing lead is what the 2026-09-12 promotion added: the v2 smooth filter needs
``up_day_frac >= 0.55`` over the 20 sessions into the trigger, and the pre-promotion fixture's FLAT
lead scores 0.05 — so ``lead_step=0.0`` is now the file's canonical jumpy/unsmooth history and is
used as one on purpose (:func:`test_the_smooth_approach_filter_gates_and_is_counted`).
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
    lead_step: float = 0.2,
    volume: float = 1000.0,
    volume_y: float = 1500.0,
) -> list[DailyRow]:
    """``lead_n`` sessions climbing by ``lead_step`` to ``lead_close`` (prox 0.94, below the 0.95
    band), one more at ``prev_close`` (this becomes ``y-1``), then ``y`` at ``close_y``. Total
    ``lead_n + 2`` rows.

    The climb only has to make the approach SMOOTH for the v2 filter (up_day_frac 0.95, max_day_move
    0.0022 across the lead); it never touches ``prox``, which reads ``close`` against the constant
    ``high``. ``lead_step=0.0`` gives the flat, unsmooth lead v2 refuses.
    """
    rows = [
        DailyRow(
            high=high, close=lead_close - lead_step * (lead_n - 1 - i),
            volume=volume, open=lead_close - lead_step * (lead_n - 1 - i),
        )
        for i in range(lead_n)
    ]
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
    # 98.5, not 99: off a 94.00 prior close a +5.32% trigger day is a v2 GAP refusal, and this test
    # is about ordering, not about the filters (+4.79% clears both).
    strong = _fresh_cross_rows(close_y=98.5)     # prox 0.985 -> highest score
    tied_a = _fresh_cross_rows(close_y=96.0)     # prox 0.96
    tied_b = _fresh_cross_rows(close_y=96.0)     # prox 0.96, tied with tied_a
    out = sweep_daily(
        {"ZED": tied_b, "ALPHA": tied_a, "MID": strong}, today=TODAY, params=SMALL_P
    )
    assert [c.symbol for c in out] == ["MID", "ALPHA", "ZED"]     # score desc, then symbol asc on ties
    assert all(c.signal_id for c in out) and len({c.signal_id for c in out}) == 3


# ============================================================ 6b. unadjusted-history veto (2026-09-03)
def test_unadjusted_history_symbol_is_skipped_by_the_sweep_and_counted():
    """Stored daily history is never re-adjusted across an ex-date (Kite rows are adjusted at fetch
    time but seeded once; bhavcopy rows are raw), so a structural ex-date inside the lookback leaves
    phantom pre-ex highs: the sweep must not read that symbol at all until the window rolls past —
    and must count the refusal (§6.1 observability)."""
    counts: dict[str, int] = {}
    out = sweep_daily(
        {"RAW": _fresh_cross_rows(close_y=96.0), "IDX": _fresh_cross_rows(close_y=96.0)},
        today=TODAY, params=SMALL_P, unadjusted_symbols={"RAW"}, veto_counts=counts,
    )
    assert [c.symbol for c in out] == ["IDX"]
    assert counts == {hi52.VETO_UNADJUSTED_HISTORY: 1}


def test_unadjusted_history_picks_the_structural_kinds_only():
    rows = [
        {"symbol": "A", "kind": "bonus"},           # rescales the series -> vetoed
        {"symbol": "B", "kind": "dividend"},        # cash event, no rescale -> not vetoed
        {"symbol": "C", "kind": "buyback"},
        {"symbol": "D", "kind": "split"},           # incl. a consolidation (classify_purpose maps it here)
        {"symbol": "E", "kind": "rights"},
        {"symbol": "F", "kind": "demerger"},
        {"symbol": "G", "kind": "other"},           # AGM/EGM/unrecognised: not a rescale
    ]
    assert hi52.unadjusted_history(rows) == {"A", "D", "E", "F"}


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

    # diagnostics_for stays computable on rows no candidate can come from: this history's prox (well
    # under 0.95 given +1 headroom every session) fails the proximity test long before the v2 smooth
    # filter these same two numbers now feed.
    assert scan_daily("SMOOTH", rows, today=TODAY, params={"lookback_sessions": 21, "min_sessions": 21}) is None


# ============================================================ 9. v2 filters GATE (2026-09-12)
def test_the_smooth_approach_filter_gates_and_is_counted():
    """The pre-promotion fixture shape — a FLAT lead into one jump — is exactly what the v2 smooth
    filter exists to refuse: 1 up-day in 20 is up_day_frac 0.05, under the registered 0.55."""
    jumpy = _fresh_cross_rows(close_y=96.0, lead_step=0.0)
    counts: dict[str, int] = {}
    assert scan_daily("ALPHA", jumpy, today=TODAY, params=SMALL_P, veto_counts=counts) is None
    assert counts == {hi52.VETO_SMOOTH: 1}
    assert diagnostics_for(jumpy, params=SMALL_P).up_day_frac == 0.05   # the number it refused

    # The same cross with a smooth approach fires, and the counter is untouched.
    smooth = _fresh_cross_rows(close_y=96.0)
    assert diagnostics_for(smooth, params=SMALL_P).up_day_frac == 0.95
    assert scan_daily("ALPHA", smooth, today=TODAY, params=SMALL_P, veto_counts=counts) is not None
    assert counts == {hi52.VETO_SMOOTH: 1}


def test_the_gap_day_filter_gates_and_is_counted():
    """A +6.00% trigger session (94 -> 99.64) is smooth-clean (0.06 <= the 0.07 ceiling) but breaks
    the 5% no-gap-day bar — the ONLY band in which VETO_GAP can be booked."""
    counts: dict[str, int] = {}
    gappy = _fresh_cross_rows(close_y=99.64)
    assert diagnostics_for(gappy, params=SMALL_P).max_day_move == 0.06   # so smooth is NOT why
    assert scan_daily("ALPHA", gappy, today=TODAY, params=SMALL_P, veto_counts=counts) is None
    assert counts == {hi52.VETO_GAP: 1}

    # A 4% trigger move clears both v2 filters.
    assert scan_daily("ALPHA", _fresh_cross_rows(close_y=97.76), today=TODAY,
                      params=SMALL_P, veto_counts=counts) is not None
    assert counts == {hi52.VETO_GAP: 1}


def test_a_big_trigger_move_books_smooth_not_gap_first_match():
    """The counts PARTITION the v2 rejects, in the registration's own order, exactly as
    ``scripts/backtest_hi52.py`` tallies them: a +10% trigger day fails the 0.07 smooth ceiling AND
    the 0.05 gap bar, and first-match books it as smooth. Reading `gap` as "every gap day" would
    misread the funnel — the big gaps are in the smooth tally."""
    counts: dict[str, int] = {}
    rows = _fresh_cross_rows(close_y=99.0, prev_close=90.0, lead_close=90.0)
    assert diagnostics_for(rows, params=SMALL_P).max_day_move == 0.1    # 99/90 - 1, both bars broken
    assert scan_daily("ALPHA", rows, today=TODAY, params=SMALL_P, veto_counts=counts) is None
    assert counts == {hi52.VETO_SMOOTH: 1}


def test_v1_params_reproduce_the_pre_promotion_rule():
    """``V1_PARAMS`` is the registered v1 rule — DEFAULT_PARAMS with the three v2 thresholds
    neutralized — and exists for ``scripts/backtest_hi52.py --registration v1``, whose N=1 numbers
    must stay reproducible after the promotion made those thresholds gating. Both histories v2
    refused above fire again under it, and nothing is counted."""
    v1 = {**hi52.V1_PARAMS, **SMALL_P}
    counts: dict[str, int] = {}
    assert scan_daily("ALPHA", _fresh_cross_rows(close_y=96.0, lead_step=0.0), today=TODAY,
                      params=v1, veto_counts=counts) is not None
    assert scan_daily("ALPHA", _fresh_cross_rows(close_y=99.64), today=TODAY,
                      params=v1, veto_counts=counts) is not None
    assert counts == {}
    # Neutralized, never dropped: a missing key would merge back to its GATING default.
    assert set(hi52.V1_PARAMS) == set(hi52.DEFAULT_PARAMS)
    assert hi52.V2_FILTER_PARAMS == {"smooth_up_day_frac_min", "smooth_max_day_move", "gap_day_max"}
    assert {k: hi52.V1_PARAMS[k] for k in hi52.V2_FILTER_PARAMS} == {
        "smooth_up_day_frac_min": 0.0, "smooth_max_day_move": math.inf, "gap_day_max": math.inf,
    }
    # …and the neutralization is EXACT, which is the only reason V1_PARAMS exists: an INFINITE
    # ceiling turns its filter OFF rather than merely making it unfailable. A symbol whose close(y-1)
    # is 0 — a suspended session, which the archive contains — has no computable gap; v1 never looked
    # at rows[-2] and emitted it, so booking it as a gap veto on a `--registration v1` re-run would
    # silently change the population behind the published N=1 numbers.
    zero_prev = _fresh_cross_rows(close_y=96.0)
    zero_prev[-2] = zero_prev[-2]._replace(close=0.0)
    assert scan_daily("ALPHA", zero_prev, today=TODAY, params=v1) is not None
    # The LIVE rule, whose gap_day_max is finite, still FAILS CLOSED on the same history: the claim
    # is "the filter was SATISFIED at signal time", and an unknowable value did not satisfy it.
    gap_counts: dict[str, int] = {}
    gap_decides = {**SMALL_P, "smooth_max_day_move": 2.0}   # relaxed so the GAP clause is what rules
    assert scan_daily("ALPHA", zero_prev, today=TODAY, params=gap_decides,
                      veto_counts=gap_counts) is None
    assert gap_counts == {hi52.VETO_GAP: 1}
    # …and under the untouched live envelope it is refused one filter earlier, never emitted.
    smooth_counts: dict[str, int] = {}
    assert scan_daily("ALPHA", zero_prev, today=TODAY, params=SMALL_P,
                      veto_counts=smooth_counts) is None
    assert smooth_counts == {hi52.VETO_SMOOTH: 1}


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
        # v2, pre-registered 2026-09-09 from the 09-03 full-sample medians, gating since 09-12.
        # These three are the registration: a change here is a new pre-registration, never a tuning,
        # and it invalidates the backtest that justified the promotion.
        "smooth_up_day_frac_min": 0.55,
        "smooth_max_day_move": 0.07,
        "gap_day_max": 0.05,
    }
