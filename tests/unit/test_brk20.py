"""brk20 (§6.1 addendum, 2026-08-04) — 20d-high daily-close breakout, full eligible universe.

PINNED worked example: 20 flat sessions with high 100.0, then a fresh close above. All math is
hand-computed against the module's own DEFAULT_PARAMS-independent inputs (params passed
explicitly — owner envelope changes must never break these tests).

WO-19 (2026-08-19) added the overnight-gap stop-geometry floor. The legacy fixtures deliberately
carry a QUIET gap history (~0.05%/night ⇒ floor ~0.10% of entry) so the floor is never the thing
under test there and the pre-WO-19 geometry assertions stay field-exact; the floor's own fixtures
build their gap histories explicitly."""

from __future__ import annotations

import math
from datetime import date
from decimal import Decimal

from engine.strategy.scanners import brk20
from engine.strategy.scanners.brk20 import DailyRow, scan_daily, sweep_daily

TODAY = date(2026, 8, 4)
P = {"lookback_days": 20, "vol_mult": 1.2, "rr_target": 2.0, "ex_skip_days": 10}


def _flat(
    n: int,
    high: float = 100.0,
    close: float = 99.0,
    volume: float = 1000.0,
    open: float = 99.05,          # mirrors the DailyRow field name; ~0.05%/night = a quiet tape
) -> list[DailyRow]:
    return [DailyRow(high=high, close=close, volume=volume, open=open) for _ in range(n)]


def _series_with_breakout(breakout_volume: float = 2000.0) -> list[DailyRow]:
    """21 quiet sessions + yesterday closing at 103 above the 20d high of 100."""
    return _flat(21) + [DailyRow(high=103.5, close=103.0, volume=breakout_volume, open=99.05)]


def _gapped(rows: list[DailyRow], gap_pct: float) -> list[DailyRow]:
    """Rewrite every row's ``open`` so EACH session-pair gaps by exactly ``gap_pct`` percent.

    The window median is then ``gap_pct`` by construction and the WO-19 floor is 2 x that — no
    dependence on which pairs land inside the lookback, which is what makes the geometry fixtures
    hand-computable. ``rows[0].open`` is untouched: it is only ever a pair's ``prev``, and a
    ``prev`` contributes its CLOSE."""
    out = [rows[0]]
    for prev, cur in zip(rows, rows[1:]):
        out.append(cur._replace(open=prev.close * (1.0 + gap_pct / 100.0)))
    return out


def _reopen(rows: list[DailyRow], idxs: range, gap_pct: float) -> list[DailyRow]:
    """Re-gap only the pairs ENDING at ``idxs`` — used to place outliers in/outside the window."""
    out = list(rows)
    for i in idxs:
        out[i] = out[i]._replace(open=out[i - 1].close * (1.0 + gap_pct / 100.0))
    return out


def _breakout_at_risk_pct(risk_pct: float, *, gap_pct: float = 0.5) -> list[DailyRow]:
    """A fresh volume-confirmed breakout over the 100.0 level whose risk unit R is ``risk_pct`` of
    entry, on a history gapping ``gap_pct`` a night (default 0.5% ⇒ WO-19 floor = 1.0% of entry)."""
    close_y = 100.0 * (1.0 + risk_pct / 100.0)
    rows = _flat(21) + [DailyRow(high=close_y + 0.5, close=close_y, volume=2000.0, open=99.05)]
    return _gapped(rows, gap_pct)


def test_pinned_breakout_math():
    """WO-4 LIMIT-AT-LEVEL geometry: the plan is anchored on the BROKEN LEVEL (100), not on
    yesterday's unobtainable close (103), with the rule's own risk unit R = close(y) - H20 = 3
    translated down onto it."""
    cand = scan_daily("BPCL", _series_with_breakout(), today=TODAY, params=P)
    assert cand is not None
    assert cand.strategy_id == "brk20"
    assert cand.side == "BUY" and cand.style == "swing"
    assert cand.raw_levels.entry == Decimal("100.00")                      # the broken 20d high
    assert cand.raw_levels.stop == Decimal("97.00")                        # 100 - R, R = 103-100
    assert cand.raw_levels.target == Decimal("106.00")                     # 100 + 2 x (100-97)
    # score = min(1, 0.5 + 5 x (103/100 - 1)) = 0.65 — scored off the breakout MARGIN, so the
    # re-anchoring leaves it untouched.
    assert abs(cand.score - 0.65) < 1e-9


def test_entry_is_the_level_never_yesterdays_close():
    """WO-4/F5, the defect this rule existed to carry: ``entry`` must never be ``close(y)``.

    Swept over breakout margins so the assertion cannot pass by coincidence on one series, and the
    §7.1 ``levels_coherent`` shape (stop < entry < target) is asserted at every point."""
    for close in (100.5, 101.0, 103.0, 107.5, 110.0):
        rows = _flat(21) + [DailyRow(high=close + 0.5, close=close, volume=2000.0, open=99.05)]
        cand = scan_daily("X", rows, today=TODAY, params=P)
        assert cand is not None, close
        lv = cand.raw_levels
        assert lv.entry == Decimal("100.00"), close          # H20, for every breakout margin
        assert lv.entry != Decimal(str(close))
        # R preserved: entry - stop == close(y) - H20, and R:R == rr_target.
        assert lv.entry - lv.stop == Decimal(str(close)) - Decimal("100.00")
        assert lv.target - lv.entry == 2 * (lv.entry - lv.stop)
        assert lv.stop < lv.entry < lv.target                 # §7.1 levels_coherent shape


def test_breakout_margin_lost_to_tick_rounding_emits_nothing():
    """Degenerate guard: a margin smaller than one tick leaves entry == stop (no risk distance)."""
    rows = _flat(21) + [DailyRow(high=100.1, close=100.01, volume=2000.0, open=99.05)]
    assert scan_daily("X", rows, today=TODAY, params=P) is None


def test_volume_unconfirmed_breakout_refused():
    """The BPCL 2026-08-03 shape: genuine 20d-high close but ~0.8x average volume ⇒ refused.
    (Real numbers that day: close 329.95 > H20 321.90, volume 5.84M vs 7.32M avg.)"""
    assert scan_daily("BPCL", _series_with_breakout(breakout_volume=800.0), today=TODAY, params=P) is None
    # At exactly vol_mult x avg it fires (>= comparison).
    assert scan_daily("BPCL", _series_with_breakout(breakout_volume=1200.0), today=TODAY, params=P) is not None


def test_riding_above_the_band_is_not_a_fresh_cross():
    """Yesterday-1 already closed above ITS band ⇒ no re-fire while price rides the highs."""
    rows = _flat(21) + [
        DailyRow(high=103.5, close=103.0, volume=2000.0, open=99.05),   # the original breakout day…
        DailyRow(high=104.5, close=104.0, volume=2000.0, open=103.05),  # …and the day after, still above
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
    strong = _flat(21) + [DailyRow(high=111.0, close=110.0, volume=2000.0, open=99.05)]  # +10% ⇒ score 1.0
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


# ============================================================ WO-19 stop-geometry floor (2026-08-19)
def test_floor_knobs_are_structurally_outside_the_learnable_envelope():
    """The separation is the governance mechanism, not a convention: a learner shrinking the floor
    re-opens the IDEA/LENSKART degeneracy, so the knobs must not be reachable through the §6.3
    envelope dict at all."""
    assert brk20.FLOOR_PARAMS == {
        "gap_floor_mult": 2.0, "gap_lookback_sessions": 20, "gap_min_sessions": 10,
    }
    assert not (brk20.FLOOR_PARAMS.keys() & brk20.DEFAULT_PARAMS.keys())
    # …and passing them as strategy params must not move the floor (they are not envelope rows).
    vetoed = _breakout_at_risk_pct(0.61)
    assert scan_daily("MFSL", vetoed, today=TODAY, params={**P, "gap_floor_mult": 0.0}) is None


def test_generous_risk_on_healthy_gap_history_is_field_exact_unchanged():
    """(a) Regression: the pre-WO-19 candidate is reproduced FIELD-EXACT on a realistic 0.5%/night
    gap history — R = 3.00 = 3.0% of entry vs a 1.0% floor, so the floor is passed, not dodged."""
    rows = _gapped(_series_with_breakout(), 0.5)
    counts: dict[str, int] = {}
    cand = scan_daily("BPCL", rows, today=TODAY, params=P, veto_counts=counts)
    assert cand is not None
    assert cand.raw_levels.entry == Decimal("100.00")
    assert cand.raw_levels.stop == Decimal("97.00")
    assert cand.raw_levels.target == Decimal("106.00")
    assert abs(cand.score - 0.65) < 1e-9
    assert counts == {}


def test_live_2026_08_19_geometries():
    """(b) The four live geometries that motivated WO-19, on a 0.5%/night history ⇒ floor 1.0% of
    entry, which lands between the survivor and the three degenerates.

    BOSCHLTD-class R ~2.1% of entry ships; MFSL 0.61%, MCX 0.54% and LENSKART 0.17% — three of that
    day's four brk20 candidates — are vetoed as `gap_floor`. (IDEA 2026-08-17's two-tick R is the
    same shape, an order of magnitude further under.)"""
    counts: dict[str, int] = {}
    survivor = scan_daily(
        "BOSCHLTD", _breakout_at_risk_pct(2.1), today=TODAY, params=P, veto_counts=counts
    )
    assert survivor is not None
    assert survivor.raw_levels.entry - survivor.raw_levels.stop == Decimal("2.10")
    assert counts == {}

    for symbol, risk_pct, expected_r in (
        ("MFSL", 0.61, "0.60"), ("MCX", 0.54, "0.55"), ("LENSKART", 0.17, "0.15"),
    ):
        before = counts.get(brk20.VETO_GAP_FLOOR, 0)
        rows = _breakout_at_risk_pct(risk_pct)
        assert scan_daily(symbol, rows, today=TODAY, params=P, veto_counts=counts) is None, symbol
        assert counts[brk20.VETO_GAP_FLOOR] == before + 1, symbol
        # The tick-exact R the floor actually compared against (hand-computed off the 100.00 level).
        assert brk20.round_to_tick(rows[-1].close) - Decimal("100.00") == Decimal(expected_r), symbol
    assert counts == {brk20.VETO_GAP_FLOOR: 3}


def test_floor_unavailable_on_short_history():
    """(c1) A young listing: 9 session-pairs < gap_min_sessions(10) ⇒ no candidate. Reached with a
    short ``lookback_days`` because the rule's own length guard otherwise keeps history >= 22."""
    p = {**P, "lookback_days": 5}
    rows = _gapped(_flat(9) + [DailyRow(high=103.5, close=103.0, volume=2000.0, open=99.05)], 0.1)
    counts: dict[str, int] = {}
    assert scan_daily("NEWCO", rows, today=TODAY, params=p, veto_counts=counts) is None
    assert counts == {brk20.VETO_FLOOR_UNAVAILABLE: 1}
    # One more completed session (10 pairs) reaches the quorum and the same breakout ships.
    longer = _gapped(_flat(10) + [DailyRow(high=103.5, close=103.0, volume=2000.0, open=99.05)], 0.1)
    assert scan_daily("NEWCO", longer, today=TODAY, params=p, veto_counts=counts) is not None
    assert counts == {brk20.VETO_FLOOR_UNAVAILABLE: 1}


def test_floor_unavailable_on_dirty_rows():
    """(c2) Full-length history, but NaN opens and zero closes interleaved knock 12 of the window's
    20 pairs out — 8 valid < 10 ⇒ `floor_unavailable`, never a floor computed off the survivors.
    A pair is dropped by EITHER member: a NaN open kills its own pair, a <=0 close kills the pair
    it opens."""
    rows = _gapped(_series_with_breakout(), 0.5)
    for i in range(2, 8):
        rows[i] = rows[i]._replace(open=math.nan)      # kills the pair ending at i
    for i in range(8, 14):
        rows[i] = rows[i]._replace(close=0.0)          # kills the pair ending at i+1
    counts: dict[str, int] = {}
    assert scan_daily("DIRTY", rows, today=TODAY, params=P, veto_counts=counts) is None
    assert counts == {brk20.VETO_FLOOR_UNAVAILABLE: 1}
    # The SAME series is a live candidate once the dirt is gone — the veto is the data, not the rule.
    assert scan_daily("DIRTY", _gapped(_series_with_breakout(), 0.5), today=TODAY, params=P) is not None


def test_risk_exactly_at_the_floor_passes():
    """(d) Boundary: equality passes (the pinned comparison is R < floor ⇒ veto).

    Numbers chosen so the float gap is EXACT in binary — 100.78125/100.0 = 1.0078125 is
    representable, so the division is exact — which is what makes an equality boundary assertable
    at all: gap = 0.0078125/night, floor = 2 x that x entry(128.00) = 2.00, R = 130.00 - 128.00."""
    flat = _flat(21, high=128.0, close=100.0, open=100.78125)
    counts: dict[str, int] = {}
    at_floor = flat + [DailyRow(high=130.5, close=130.0, volume=2000.0, open=100.78125)]
    cand = scan_daily("X", at_floor, today=TODAY, params=P, veto_counts=counts)
    assert cand is not None
    assert cand.raw_levels.entry - cand.raw_levels.stop == Decimal("2.00")
    assert counts == {}
    # One tick under the floor vetoes.
    under = flat + [DailyRow(high=130.5, close=129.95, volume=2000.0, open=100.78125)]
    assert scan_daily("X", under, today=TODAY, params=P, veto_counts=counts) is None
    assert counts == {brk20.VETO_GAP_FLOOR: 1}


def test_floor_reads_only_the_last_20_pairs_ending_at_y():
    """(e) The gap window is the LAST gap_lookback_sessions(20) pairs ENDING AT y, not all history:
    the floor must track CURRENT overnight behaviour. (No-lookahead is structural — the API is only
    ever handed completed sessions <= y — so what is testable, and tested here, is the window.)

    45 sessions: 24 ancient 20%-gap nights then 20 quiet 0.5% ones. Over ALL 44 pairs the median
    would be 20% (24 of them outliers) ⇒ a 40% floor that vetoes everything; over the window it is
    0.5% ⇒ a 1.0% floor, and the 1.5% risk unit ships."""
    close_y = 101.5
    base = _gapped(_flat(44) + [DailyRow(high=102.0, close=close_y, volume=2000.0, open=99.05)], 0.5)
    outside = _reopen(base, range(1, 25), 20.0)      # window is rows[24:] ⇒ pairs ending at 25..44
    cand = scan_daily("X", outside, today=TODAY, params=P)
    assert cand is not None
    assert cand.raw_levels.entry - cand.raw_levels.stop == Decimal("1.50")
    # Move the identical outliers INSIDE the window and the same series vetoes — proving the window
    # placement is what mattered, not that outliers are ignored wherever they sit.
    counts: dict[str, int] = {}
    inside = _reopen(base, range(25, 45), 20.0)
    assert scan_daily("X", inside, today=TODAY, params=P, veto_counts=counts) is None
    assert counts == {brk20.VETO_GAP_FLOOR: 1}


def test_accumulator_defaults_to_none_and_scan_stays_pure():
    """(f) The observability seam is opt-in: with no accumulator the vetoes are silent, the verdicts
    are identical, and repeated calls cannot drift (no module-global counter)."""
    vetoed = _breakout_at_risk_pct(0.17)
    passing = _breakout_at_risk_pct(2.1)
    for _ in range(3):
        assert scan_daily("LENSKART", vetoed, today=TODAY, params=P) is None
        assert scan_daily("BOSCHLTD", passing, today=TODAY, params=P) is not None
    counts: dict[str, int] = {}
    assert scan_daily("LENSKART", vetoed, today=TODAY, params=P, veto_counts=counts) is None
    assert counts == {brk20.VETO_GAP_FLOOR: 1}


def test_sweep_threads_the_accumulator_and_keeps_its_ordering():
    """(g) sweep_daily's §9.6 ordering is untouched by the new argument, and one accumulator
    aggregates every symbol's veto classes — the counts ops/main.py logs per run."""
    dirty = _gapped(_series_with_breakout(), 0.5)
    for i in range(2, 8):
        dirty[i] = dirty[i]._replace(open=math.nan)
    for i in range(8, 14):
        dirty[i] = dirty[i]._replace(close=0.0)
    histories = {
        "ZED": _gapped(_series_with_breakout(), 0.5),        # score 0.65
        "ALPHA": _gapped(_series_with_breakout(), 0.5),      # score 0.65
        "MID": _gapped(_flat(21) + [DailyRow(high=111.0, close=110.0, volume=2000.0, open=99.05)], 0.5),
        "THIN": _breakout_at_risk_pct(0.54),
        "DIRTY": dirty,
    }
    counts: dict[str, int] = {}
    out = sweep_daily(histories, today=TODAY, params=P, veto_counts=counts)
    assert [c.symbol for c in out] == ["MID", "ALPHA", "ZED"]     # score desc, then symbol asc
    assert counts == {brk20.VETO_GAP_FLOOR: 1, brk20.VETO_FLOOR_UNAVAILABLE: 1}
    # Same inputs, no accumulator ⇒ same candidates.
    assert [c.symbol for c in sweep_daily(histories, today=TODAY, params=P)] == ["MID", "ALPHA", "ZED"]


# =================================== WO-19 floor — adversarial-review coverage-gap closures (2026-08-19)
# The review confirmed the production rule correct but found four test-coverage gaps in the fixtures
# above: every floor fixture is an UP gap only, every floor fixture is a CONSTANT gap history (median
# indistinguishable from mean), nothing proves veto_counts stays empty on non-floor refusals, and only
# 2 of the dirty-pair filter's 4 clauses are individually exercised. One test per gap, closed below.


def test_gap_floor_ignores_direction_up_and_down_gaps_produce_an_identical_floor():
    """GAP-1: ``gaps = |open_t/close_{t-1} - 1|`` is an ABS — sign must not matter — but no fixture
    above ever produces a DOWN gap (open_t < close_{t-1}); every ``_gapped``/``_breakout_at_risk_pct``
    call above uses a positive ``gap_pct``. Mirror the BOSCHLTD-survivor and LENSKART-vetoed shapes
    from ``test_live_2026_08_19_geometries`` onto histories where EVERY pair is a DOWN gap of
    identical magnitude, and assert the outcome — levels on the passing side, veto_counts on both —
    is IDENTICAL to the up-gap case.

    gap magnitude 0.5%/night either way ⇒ floor = 2.0 x 0.5% = 1.0% of entry = 1.00.
    """
    up_pass = _breakout_at_risk_pct(2.1, gap_pct=0.5)
    down_pass = _breakout_at_risk_pct(2.1, gap_pct=-0.5)
    # Confirm the mirror really is a down gap, not just claimed: same prior close (99.0), opposite
    # side, identical |gap| magnitude (0.5%).
    assert up_pass[5].open > up_pass[4].close                          # up gap: open above prior close
    assert down_pass[5].open < down_pass[4].close                      # down gap: open below prior close
    gap_up = abs(up_pass[5].open / up_pass[4].close - 1.0)
    gap_down = abs(down_pass[5].open / down_pass[4].close - 1.0)
    assert math.isclose(gap_up, gap_down, rel_tol=1e-12)               # same magnitude, mirrored sign

    cand_up = scan_daily("BOSCHLTD", up_pass, today=TODAY, params=P)
    cand_down = scan_daily("BOSCHLTD", down_pass, today=TODAY, params=P)
    assert cand_up is not None and cand_down is not None
    assert cand_down.raw_levels == cand_up.raw_levels                  # identical, not just both-shipped
    assert cand_down.score == cand_up.score
    # R = round_to_tick(102.10) - 100.00 = 2.10 vs floor 1.00 — ships on both signs.
    assert cand_up.raw_levels.entry == Decimal("100.00")
    assert cand_up.raw_levels.stop == Decimal("97.90")                 # 100.00 - 2.10
    assert cand_up.raw_levels.target == Decimal("104.20")              # 100.00 + 2 x 2.10

    counts_up: dict[str, int] = {}
    counts_down: dict[str, int] = {}
    up_thin = _breakout_at_risk_pct(0.17, gap_pct=0.5)
    down_thin = _breakout_at_risk_pct(0.17, gap_pct=-0.5)
    assert scan_daily("LENSKART", up_thin, today=TODAY, params=P, veto_counts=counts_up) is None
    assert scan_daily("LENSKART", down_thin, today=TODAY, params=P, veto_counts=counts_down) is None
    # R = round_to_tick(100.17) - 100.00 = 0.15 vs floor 1.00 — vetoed on both signs, identically.
    assert counts_up == counts_down == {brk20.VETO_GAP_FLOOR: 1}


def test_floor_uses_median_not_mean_and_resists_a_single_outlier():
    """GAP-2: every floor fixture above uses a CONSTANT gap history, so the median is indistinguishable
    from the mean/min/max. Build a HETEROGENEOUS gap window — 19 pairs at |gap| = 0.4%/night and ONE
    outlier pair at 8% (a fake split-like night), among the 20-pair window — where median and mean
    diverge sharply::

        median = 0.4%                    ⇒ floor = 2.0 x 0.4% = 0.8% of entry = 0.80
        mean   = (19x0.4 + 8) / 20 = 0.78% ⇒ a MEAN-based floor would be 2.0 x 0.78% = 1.56% of entry

    R = 1.00 (1.00% of entry, close(y) = 101.0) PASSES the true median floor (1.00 >= 0.80) but would
    FAIL a mean-based floor (1.00 < 1.56) — pinning both "median, not mean" and outlier-robustness (the
    lone corp-action-like night must not move the floor)."""
    base = _gapped(_flat(21) + [DailyRow(high=101.5, close=101.0, volume=2000.0, open=99.05)], 0.4)
    rows = _reopen(base, range(12, 13), 8.0)            # ONE outlier pair (cur index 12), |gap| = 8%
    counts: dict[str, int] = {}
    cand = scan_daily("SPLITCO", rows, today=TODAY, params=P, veto_counts=counts)
    assert cand is not None
    assert cand.raw_levels.entry == Decimal("100.00")                  # H20
    assert cand.raw_levels.stop == Decimal("99.00")                    # 100.00 - R, R = 101.00-100.00
    assert cand.raw_levels.target == Decimal("102.00")                 # 100.00 + 2 x 1.00
    assert abs(cand.score - 0.55) < 1e-9                # 0.5 + 5 x (101.0/100.0 - 1) = 0.55
    assert counts == {}


def test_veto_counts_only_count_otherwise_shippable_plans():
    """GAP-3: nothing above proves the accumulator counts only plans refused for NO other reason than
    the floor — it must stay EMPTY when a fixture is refused earlier in the pipeline. One fixture per
    earlier gate, each with a FRESH dict; none may add a `gap_floor`/`floor_unavailable` key."""
    # (a) volume-unconfirmed: genuine 20d-high close, ~0.8x average volume (the BPCL 2026-08-03 shape).
    counts_vol: dict[str, int] = {}
    assert scan_daily(
        "BPCL", _series_with_breakout(breakout_volume=800.0), today=TODAY, params=P, veto_counts=counts_vol
    ) is None
    assert counts_vol == {}

    # (b) not a breakout at all: close(y) = 99.0 <= H20 = 100.0 (22 flat rows: exactly clears the
    # lookback+2 thin-history guard so this fails at the breakout check, not the length check).
    counts_nobo: dict[str, int] = {}
    assert scan_daily("X", _flat(22), today=TODAY, params=P, veto_counts=counts_nobo) is None
    assert counts_nobo == {}

    # (c) tick-degenerate: H20 = 100.00, close(y) tick-rounds to 100.00 too ⇒ entry == stop.
    counts_tick: dict[str, int] = {}
    rows = _flat(21) + [DailyRow(high=100.1, close=100.01, volume=2000.0, open=99.05)]
    assert scan_daily("X", rows, today=TODAY, params=P, veto_counts=counts_tick) is None
    assert counts_tick == {}


def test_floor_unavailable_dirty_pair_clauses_individually_load_bearing():
    """GAP-4: ``_gap_floor_frac``'s skip condition has 4 clauses (``isfinite(prev.close)``,
    ``isfinite(cur.open)``, ``prev.close > 0.0``, ``cur.open > 0.0``); ``test_floor_unavailable_on_dirty_rows``
    only exercises ``isfinite(cur.open)`` (NaN opens) and ``prev.close > 0.0`` (zero closes). Exercise
    the other two IN ISOLATION — each alone must be able to knock the 20-pair window below
    ``gap_min_sessions`` (10)."""
    # (i) cur.open <= 0.0 on an otherwise-finite row: 11 opens zeroed (cur indices 2..12, one clause
    # only) leaves 20 - 11 = 9 < 10 valid pairs.
    zero_open = _gapped(_series_with_breakout(), 0.5)
    for i in range(2, 13):
        zero_open[i] = zero_open[i]._replace(open=0.0)
    counts_zero: dict[str, int] = {}
    assert scan_daily("ZEROOPEN", zero_open, today=TODAY, params=P, veto_counts=counts_zero) is None
    assert counts_zero == {brk20.VETO_FLOOR_UNAVAILABLE: 1}

    # (ii) non-finite prev.close (inf, not just NaN) with a finite cur.open: closes of rows 1..11 set
    # to inf kill the pairs THEY open as `prev` (cur indices 2..12) — same count, 9 < 10 valid pairs.
    inf_close = _gapped(_series_with_breakout(), 0.5)
    for i in range(1, 12):
        inf_close[i] = inf_close[i]._replace(close=math.inf)
    counts_inf: dict[str, int] = {}
    assert scan_daily("INFCLOSE", inf_close, today=TODAY, params=P, veto_counts=counts_inf) is None
    assert counts_inf == {brk20.VETO_FLOOR_UNAVAILABLE: 1}
