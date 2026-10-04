"""ValidationPipeline / fold_pass_min tests (E2 / §6.4 / §9.1).

Pure tier (no vectorbt): fold_pass_min exact boundaries, non-promotable without N, promotable iff
the CPCV fold-pass fraction ≥ fold_pass_min(N), anchored walk-forward split determinism, honest
negative-result rendering. The one skfolio-backed test (real CPCV purge/embargo no-overlap) is marked
``needs_heavy_deps``.

WO-3 (2026-08-13) adds two things pinned here:

* the **margin floor** — median PASSING-split expectancy ≥ ``cost_floor / MARGIN_FLOOR_DAYS`` per
  day, fail-closed when no cost floor is supplied — tested at the exact 12/15 = 80.0% boundary that
  four rsi2 runs sat on, with the recorded sub-floor margins (⇒ not promotable) and with margins that
  genuinely clear costs (⇒ promotable);
* the **winner-stability flag** — recorded on the report, never part of the verdict.

Every ``promotion_decision`` call therefore now passes a cost floor + a median: the rule fails closed
without them, which is itself asserted below.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from engine.core.clock import IST, Clock
from engine.learning import reports

# sweep.py's module top level is stdlib + pandas/numpy only (vectorbt is imported function-level),
# so the stamp constant is reachable from this pure tier without pulling the heavy deps in.
from engine.learning.sweep import MECHANICS_STAMP
from engine.learning.validate import (
    MARGIN_FLOOR_DAYS,
    CPCVFold,
    ParamSet,
    RealizedHold,
    ValidationPipeline,
    ValidationReport,
    WalkForwardFold,
    cpcv_splits,
    fold_pass_min,
    margin_floor_pct_per_day,
    promotion_decision,
    realized_hold_floor_cells,
    walk_forward_splits,
    winner_stability,
)

FIXED_NOW = datetime(2026, 6, 17, 18, 0, tzinfo=IST)

#: The CNC ₹20k round-trip friction after WO-2 = 0.2992% statutory fees + 0.0200% measured spread.
#: The WO-3 margin floor is this / MARGIN_FLOOR_DAYS = 0.01596 %/day.
CNC_COST_FLOOR_PCT = 0.3192
#: A margin that clears the floor comfortably (used wherever the test is about a DIFFERENT rule).
CLEARS = 0.05


def _decide(n, fraction, **kw):
    """promotion_decision with the WO-3 margin inputs defaulted to a clearly-clearing edge, so a
    test about (say) the drawdown gate is not silently answered by the margin floor."""
    kw.setdefault("median_passing_expectancy_pct", CLEARS)
    kw.setdefault("cost_floor_pct", CNC_COST_FLOOR_PCT)
    return promotion_decision(n, fraction, **kw)


@pytest.fixture
def clock() -> Clock:
    return Clock(time_source=lambda: FIXED_NOW)


# --------------------------------------------------------------------------- fold_pass_min boundaries
def test_fold_pass_min_exact_boundaries():
    # §9.1: 60% for N ≤ 10, 70% for 11 ≤ N ≤ 30, 80% for N > 30 — boundaries at 10/11/30/31.
    assert fold_pass_min(10) == 0.60
    assert fold_pass_min(11) == 0.70
    assert fold_pass_min(30) == 0.70
    assert fold_pass_min(31) == 0.80


def test_fold_pass_min_monotone_and_edges():
    assert fold_pass_min(0) == 0.60
    assert fold_pass_min(1) == 0.60
    assert fold_pass_min(1000) == 0.80
    # monotone non-decreasing
    prev = 0.0
    for n in range(0, 60):
        cur = fold_pass_min(n)
        assert cur >= prev
        prev = cur


def test_fold_pass_min_rejects_negative():
    with pytest.raises(ValueError):
        fold_pass_min(-1)


# --------------------------------------------------------------------------- promotion_decision rule
def test_promotion_absent_n_not_promotable():
    ok, reasons = _decide(None, 1.0)
    assert ok is False
    assert any("no cited N" in r or "N is ABSENT" in r for r in reasons)


def test_promotion_absent_folds_not_promotable():
    ok, reasons = _decide(10, None)
    assert ok is False
    assert any("no folds" in r.lower() or "CPCV produced no folds" in r for r in reasons)


def test_promotion_boundary_at_n10_and_n11():
    # N=10 requires 60%; N=11 requires 70%. A 60% fraction clears N=10, fails N=11.
    ok10, _ = _decide(10, 0.60)
    assert ok10 is True
    assert _decide(10, 0.59)[0] is False
    ok11, reasons11 = _decide(11, 0.60)
    assert ok11 is False
    assert any("fold_pass_min" in r for r in reasons11)


def test_promotion_boundary_at_n31():
    assert _decide(31, 0.80)[0] is True
    assert _decide(31, 0.79)[0] is False


def test_promotion_drawdown_gate():
    # Even a passing fold fraction is rejected when max DD exceeds 1.25× champion's (§6.4 step 2).
    ok, reasons = _decide(5, 1.0, max_dd_pct=20.0, champion_max_dd_pct=10.0)
    assert ok is False
    assert any("drawdown" in r.lower() for r in reasons)
    assert _decide(5, 1.0, max_dd_pct=12.0, champion_max_dd_pct=10.0)[0] is True


# --------------------------------------------------------------------------- WO-3 margin floor
def test_margin_floor_constant_and_derivation():
    """floor = cost_floor / 20 sessions — the one round trip a position must earn over the §7.1
    swing holding cap. On the post-WO-2 CNC surface that is 0.3192% / 20 = 0.01596 %/day."""
    assert MARGIN_FLOOR_DAYS == 20
    assert margin_floor_pct_per_day(CNC_COST_FLOOR_PCT) == pytest.approx(0.01596)
    assert margin_floor_pct_per_day(None) is None
    assert margin_floor_pct_per_day(CNC_COST_FLOOR_PCT, margin_floor_days=10) == pytest.approx(0.03192)
    with pytest.raises(ValueError):
        margin_floor_pct_per_day(CNC_COST_FLOOR_PCT, margin_floor_days=0)


def test_promotion_rejects_the_recorded_rsi2_boundary_pass():
    """THE case WO-3 exists for: 12/15 = exactly 80.0% (a strict-< pass at N>30) with the recorded
    rsi2 median passing split ~0.0006 %/day — positive, and ~27x below the cost floor."""
    n, fraction = 40, 12 / 15
    assert fraction == fold_pass_min(n)                       # sits EXACTLY on the bar
    ok, reasons = promotion_decision(
        n, fraction, median_passing_expectancy_pct=0.0006, cost_floor_pct=CNC_COST_FLOOR_PCT
    )
    assert ok is False
    assert not any("fold_pass_min" in r for r in reasons)      # the fold rule still passes it
    assert any("margin floor" in r for r in reasons)
    assert any("0.01596" in r for r in reasons)                # the floor is stated, not implied


def test_promotion_accepts_the_same_boundary_with_a_cost_clearing_margin():
    """Same 12/15 boundary, same N — but margins that genuinely clear costs ⇒ promotable."""
    n, fraction = 40, 12 / 15
    ok, reasons = promotion_decision(
        n, fraction, median_passing_expectancy_pct=0.02, cost_floor_pct=CNC_COST_FLOOR_PCT
    )
    assert ok is True and reasons == []


def test_margin_floor_boundary_is_exact_and_inclusive():
    """>= the floor passes; a hair below fails (the floor itself is a PASS, unlike fold_pass_min's
    strict-< comparison, which WO-3 deliberately left alone)."""
    floor = margin_floor_pct_per_day(CNC_COST_FLOOR_PCT)
    assert promotion_decision(
        40, 12 / 15, median_passing_expectancy_pct=floor, cost_floor_pct=CNC_COST_FLOOR_PCT
    )[0] is True
    assert promotion_decision(
        40, 12 / 15, median_passing_expectancy_pct=floor * 0.999, cost_floor_pct=CNC_COST_FLOOR_PCT
    )[0] is False


def test_promotion_fails_closed_without_a_cost_floor():
    """A margin floor that silently skips is no floor at all (same posture as a missing N)."""
    ok, reasons = promotion_decision(10, 1.0, median_passing_expectancy_pct=1.0)
    assert ok is False
    assert any("margin floor NOT EVALUATED" in r for r in reasons)


def test_promotion_fails_closed_without_any_passing_split():
    ok, reasons = promotion_decision(
        10, 1.0, median_passing_expectancy_pct=None, cost_floor_pct=CNC_COST_FLOOR_PCT
    )
    assert ok is False
    assert any("no passing CPCV splits" in r for r in reasons)


def test_fold_pass_fraction_comparison_stays_strict_less_than():
    """WO-3 explicitly did NOT move this boundary: fraction == fold_pass_min(N) still PASSES."""
    for n in (10, 11, 31, 40):
        assert _decide(n, fold_pass_min(n))[0] is True


# --------------------------------------------------------------------------- WO-3 winner stability
def test_winner_stability_flag_states():
    same = winner_stability({"a": 1.0, "b": 2.0}, {"a": 1.0, "b": 2.0},
                            grid_density="coarse", adjacent_density="medium")
    assert same.stable is True and same.differing_params == []
    assert "STABLE" in same.note()

    diff = winner_stability({"rsi_entry": 3.0, "rsi_exit": 10.0}, {"rsi_entry": 15.0, "rsi_exit": 2.0},
                            grid_density="medium", adjacent_density="fine")
    assert diff.stable is False
    assert diff.differing_params == ["rsi_entry", "rsi_exit"]
    assert "UNSTABLE" in diff.note()

    unknown = winner_stability({"a": 1.0}, None)
    assert unknown.stable is None
    assert "NOT ASSESSED" in unknown.note()

    # a density that adds an axis genuinely changed the winner
    added = winner_stability({"a": 1.0}, {"a": 1.0, "b": 3.0})
    assert added.stable is False and added.differing_params == ["b"]


def test_winner_instability_is_a_flag_not_an_auto_fail(clock):
    """The report records the instability; the verdict is unaffected (WO-3 (b))."""
    pipe = _pipeline(clock)
    ps = ParamSet(
        strategy_id="rsi2", params={"rsi_entry": 3.0}, trial_count_n=10,
        cost_floor_pct=CNC_COST_FLOOR_PCT, grid_density="coarse", adjacent_density="medium",
        adjacent_winner={"rsi_entry": 15.0},
    )
    report = pipe.validate_sync("rsi2", ps)
    assert report.winner_stability is not None
    assert report.winner_stability.stable is False
    assert report.winner_stability.differing_params == ["rsi_entry"]
    assert any("UNSTABLE" in note for note in report.notes)
    assert report.promotable is True                       # flag only — the verdict is unchanged
    assert not any("stab" in r.lower() for r in report.reasons)


# --------------------------------------------------------------------------- walk-forward determinism
def _daily_dates(start: date, days: int) -> list[date]:
    return [start + timedelta(days=i) for i in range(days)]


def test_walk_forward_anchored_and_deterministic():
    dates = _daily_dates(date(2024, 1, 1), 300)  # ~10 months
    a = walk_forward_splits(dates)
    b = walk_forward_splits(dates)
    assert a == b  # deterministic (§9.6)
    assert a, "expected at least one fold over 10 months with 6m train / 1m test"
    first = dates[0]
    for tr_s, tr_e, te_s, te_e in a:
        assert tr_s == first  # anchored: train always starts at the first observation
        assert tr_e == te_s   # train_end == test_start
        assert te_s < te_e    # non-empty exclusive test window
    # test windows roll forward monotonically
    starts = [te_s for _, _, te_s, _ in a]
    assert starts == sorted(starts)
    assert len(set(starts)) == len(starts)


def test_walk_forward_empty_below_one_train_window():
    dates = _daily_dates(date(2024, 1, 1), 30)  # < 6 months
    assert walk_forward_splits(dates) == []


# --------------------------------------------------------------------------- pipeline promotion end-to-end
class _FixedSplitter:
    """Deterministic CPCV splitter for the pure tier: 5 fixed folds, 3 positive / 2 negative."""

    def __call__(self, n_obs: int):
        pos = [np.array([0, 1, 2, 3]), np.array([4, 5, 6, 7]), np.array([8, 9, 10, 11])]
        neg = [np.array([20, 21, 22, 23]), np.array([24, 25, 26, 27])]
        splits = []
        for test in pos + neg:
            train = np.array([i for i in range(n_obs) if i not in set(test.tolist())])
            splits.append((train, test))
        return splits


def _returns_series() -> pd.Series:
    # first 20 daily returns +1%, last 20 −1% ⇒ the fixed splitter yields 3/5 = 60% fold pass.
    idx = [date(2024, 1, 1) + timedelta(days=i) for i in range(40)]
    vals = [0.01] * 20 + [-0.01] * 20
    return pd.Series(vals, index=idx, dtype="float64")


def _pipeline(clock, conn=None, reports_dir=None) -> ValidationPipeline:
    return ValidationPipeline(
        returns_provider=lambda sid, params: _returns_series(),
        clock=clock,
        conn=conn,
        reports_dir=reports_dir,
        splitter=_FixedSplitter(),
    )


def test_pipeline_not_promotable_without_n(clock):
    pipe = _pipeline(clock)
    ps = ParamSet(strategy_id="rsi2", params={"rsi_entry": 10.0}, trial_count_n=None)
    report = pipe.validate_sync("rsi2", ps)
    assert report.trial_count_n is None
    assert report.fold_pass_min is None
    assert report.promotable is False
    assert any("N is ABSENT" in r or "no cited N" in r for r in report.reasons)
    # the fraction was still computed (3/5) — absence of N, not of folds, is the blocker.
    assert report.cpcv_fold_pass_fraction == pytest.approx(0.6)


def test_pipeline_promotable_iff_fraction_meets_bar(clock):
    pipe = _pipeline(clock)
    # N=10 ⇒ bar 60%; observed 60% ⇒ promotable.
    r10 = pipe.validate_sync("rsi2", ParamSet(strategy_id="rsi2", params={}, trial_count_n=10))
    assert r10.fold_pass_min == 0.60
    assert r10.cpcv_fold_pass_fraction == pytest.approx(0.6)
    assert r10.promotable is True
    # N=11 ⇒ bar 70%; observed 60% ⇒ NOT promotable.
    r11 = pipe.validate_sync("rsi2", ParamSet(strategy_id="rsi2", params={}, trial_count_n=11))
    assert r11.fold_pass_min == 0.70
    assert r11.promotable is False


def _tiny_margin_series() -> pd.Series:
    """Same 3/5 fold-pass shape, but the winning folds earn ~0.0006%/day — the recorded rsi2
    magnitude: positive after costs, ~27x below the WO-3 margin floor."""
    idx = [date(2024, 1, 1) + timedelta(days=i) for i in range(40)]
    return pd.Series([0.000006] * 20 + [-0.01] * 20, index=idx, dtype="float64")


def test_pipeline_margin_floor_blocks_a_near_zero_margin_pass(clock):
    """End-to-end: the fold-pass rule says yes, the margin floor says no (WO-3 (a))."""
    pipe = ValidationPipeline(
        returns_provider=lambda sid, params: _tiny_margin_series(),
        clock=clock,
        splitter=_FixedSplitter(),
        cost_floor_provider=lambda _sid: CNC_COST_FLOOR_PCT,
    )
    report = pipe.validate_sync("rsi2", ParamSet(strategy_id="rsi2", params={}, trial_count_n=10))
    assert report.cpcv_fold_pass_fraction == pytest.approx(0.6)     # clears fold_pass_min(10)=60%
    assert report.cpcv_median_passing_expectancy_pct == pytest.approx(0.0006)
    assert report.cost_floor_pct == pytest.approx(CNC_COST_FLOOR_PCT)
    assert report.margin_floor_pct_per_day == pytest.approx(0.01596)
    assert report.promotable is False
    assert any("margin floor" in r for r in report.reasons)
    assert not any("fold_pass_min" in r for r in report.reasons)
    assert any("Margin floor (WO-3)" in note for note in report.notes)


def test_pipeline_derives_the_cost_floor_when_the_caller_supplies_none(clock):
    """No cost_floor_pct on the ParamSet and no provider ⇒ the pipeline derives one from the same
    CostModel the sweeps/gate use, so the floor is enforced rather than skipped."""
    pipe = _pipeline(clock)
    report = pipe.validate_sync("rsi2", ParamSet(strategy_id="rsi2", params={}, trial_count_n=10))
    assert report.cost_floor_pct is not None and report.cost_floor_pct > 0.3   # CNC fees + spread
    assert report.margin_floor_pct_per_day == pytest.approx(
        report.cost_floor_pct / MARGIN_FLOOR_DAYS
    )
    assert report.promotable is True            # +1%/day passing folds clear the floor easily


def test_pipeline_persists_param_set_and_artifacts(clock, conn, tmp_path):
    pipe = _pipeline(clock, conn=conn, reports_dir=tmp_path)
    ps = ParamSet(strategy_id="rsi2", params={"rsi_entry": 8.0}, trial_count_n=10)
    report = pipe.validate_sync("rsi2", ps)
    rows = conn.execute(
        "SELECT param_set_id, strategy_id, status, validation_report FROM param_sets"
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["strategy_id"] == "rsi2"
    assert rows[0]["status"] == "candidate"
    stored = json.loads(rows[0]["validation_report"])
    assert stored["trial_count_n"] == 10  # the report CITES N (§6.4 step 2)
    # artifacts written
    md = tmp_path / f"rsi2_{report.generated_at:%Y%m%dT%H%M%S}.md"
    js = tmp_path / f"rsi2_{report.generated_at:%Y%m%dT%H%M%S}.json"
    assert md.exists() and js.exists()


def test_report_artifacts_carry_the_wo3_flag_and_margin_floor(clock, tmp_path):
    """WO-3 acceptance: the re-issued reports SHOW the winner-stability flag and the margin floor —
    as prose in the md (via notes) and as structured fields in the json."""
    pipe = _pipeline(clock, reports_dir=tmp_path)
    ps = ParamSet(
        strategy_id="rsi2", params={"rsi_entry": 3.0}, trial_count_n=10,
        cost_floor_pct=CNC_COST_FLOOR_PCT, grid_density="coarse", adjacent_density="medium",
        adjacent_winner={"rsi_entry": 15.0},
    )
    report = pipe.validate_sync("rsi2", ps)
    md_text = (tmp_path / f"rsi2_{report.generated_at:%Y%m%dT%H%M%S}.md").read_text(encoding="utf-8")
    assert "Winner stability (WO-3): UNSTABLE" in md_text
    assert "Margin floor (WO-3)" in md_text and "0.01596%/day" in md_text

    data = json.loads((tmp_path / f"rsi2_{report.generated_at:%Y%m%dT%H%M%S}.json").read_text(encoding="utf-8"))
    assert data["winner_stability"]["stable"] is False
    assert data["winner_stability"]["differing_params"] == ["rsi_entry"]
    assert data["margin_floor_pct_per_day"] == pytest.approx(0.01596)
    assert data["cpcv_median_passing_expectancy_pct"] == pytest.approx(1.0)


def _positional_margin_series() -> pd.Series:
    """The recorded 2026-08-14 ``trend`` shape: passing splits ≈ 0.01%/day — BELOW the 20-session
    floor (0.01596), ABOVE the 120-session one (0.00266)."""
    idx = [date(2024, 1, 1) + timedelta(days=i) for i in range(40)]
    return pd.Series([0.0001] * 20 + [-0.01] * 20, index=idx, dtype="float64")


def _positional_pipeline(clock, *, margin_floor_days, reports_dir=None) -> ValidationPipeline:
    return ValidationPipeline(
        returns_provider=lambda sid, params: _positional_margin_series(),
        clock=clock,
        splitter=_FixedSplitter(),
        cost_floor_provider=lambda _sid: CNC_COST_FLOOR_PCT,
        reports_dir=reports_dir,
        margin_floor_days=margin_floor_days,
    )


def test_report_records_the_margin_floor_denominator_it_used(clock, tmp_path):
    """R2 (2026-09-12): the floor's DENOMINATOR is a protocol choice (20 sessions for a swing leg,
    the §7.1 120 for a positional one), so %/day alone does not make the artifact reproducible —
    the report must carry and PRINT the days. Same returns, two denominators, opposite verdicts."""
    ps = ParamSet(strategy_id="trend", params={}, trial_count_n=9)

    swing = _positional_pipeline(clock, margin_floor_days=MARGIN_FLOOR_DAYS)
    r20 = swing.validate_sync("trend", ps)
    assert r20.margin_floor_days == 20
    assert r20.cpcv_median_passing_expectancy_pct == pytest.approx(0.01)
    assert r20.promotable is False
    assert any("20 sessions" in reason for reason in r20.reasons)

    positional = _positional_pipeline(clock, margin_floor_days=120, reports_dir=tmp_path)
    r120 = positional.validate_sync("trend", ps)
    assert r120.margin_floor_days == 120
    assert r120.margin_floor_pct_per_day == pytest.approx(CNC_COST_FLOOR_PCT / 120)
    assert r120.promotable is True

    md = (tmp_path / f"trend_{r120.generated_at:%Y%m%dT%H%M%S}.md").read_text(encoding="utf-8")
    assert "/ 120 sessions" in md
    data = json.loads((tmp_path / f"trend_{r120.generated_at:%Y%m%dT%H%M%S}.json").read_text(encoding="utf-8"))
    assert data["margin_floor_days"] == 120


def test_default_margin_floor_days_is_recorded_unchanged(clock):
    report = _pipeline(clock).validate_sync(
        "rsi2", ParamSet(strategy_id="rsi2", params={}, trial_count_n=10)
    )
    assert report.margin_floor_days == MARGIN_FLOOR_DAYS == 20


def test_pipeline_rejects_strategy_id_mismatch(clock):
    pipe = _pipeline(clock)
    with pytest.raises(ValueError):
        pipe.validate_sync("orb", ParamSet(strategy_id="rsi2", params={}, trial_count_n=5))


# --------------------------------------------------------------------------- honest negative rendering
def _negative_report() -> ValidationReport:
    return ValidationReport(
        strategy_id="trend",
        param_set_id="01ABC",
        params={"adx_min": 20.0, "trail_atr_mult": 2.5},
        trial_count_n=15,
        fold_pass_min=0.70,
        data_start=date(2024, 1, 1),
        data_end=date(2024, 12, 31),
        n_obs=250,
        expectancy_pct=-0.42,
        total_return_pct=-8.1,
        max_drawdown_pct=17.0,
        walk_forward=[
            WalkForwardFold(
                fold=0, train_start=date(2024, 1, 1), train_end=date(2024, 7, 1),
                test_start=date(2024, 7, 1), test_end=date(2024, 8, 1), n_obs=21,
                expectancy_pct=-0.3, total_return_pct=-1.2,
            )
        ],
        cpcv=[CPCVFold(split=0, n_train_obs=200, n_test_obs=40, expectancy_pct=-0.5, passed=False)],
        cpcv_fold_pass_fraction=0.4,
        promotable=False,
        reasons=["CPCV fold-pass fraction 40.0% < fold_pass_min(N=15) = 70%"],
        generated_at=FIXED_NOW,
    )


def test_report_surfaces_negatives_prominently(tmp_path):
    report = _negative_report()
    md = reports.render_markdown(report)
    assert "NOT PROMOTABLE" in md
    assert "NEGATIVE" in md.upper()  # the honest negative-expectancy banner (C9)
    assert "fold_pass_min(N=15)" in md
    art = reports.write_report(report, tmp_path)
    assert art.markdown.exists() and art.json.exists()
    data = json.loads(art.json.read_text(encoding="utf-8"))
    assert data["trial_count_n"] == 15
    assert data["promotable"] is False


# --------------------------------------------------------------------------- CPCV purge/embargo (skfolio)
@pytest.mark.needs_heavy_deps
def test_cpcv_purge_embargo_no_overlap():
    pytest.importorskip("skfolio")
    purge, embargo = 5, 5
    # 120 obs / 6 folds ⇒ min_fold_size 20 > purge+embargo+1, so a valid purged CPCV forms.
    splits = cpcv_splits(120, purge=purge, embargo=embargo)
    assert splits, "expected CPCV splits for 120 observations"
    for train, test in splits:
        train_set = set(int(x) for x in train)
        test_sorted = np.sort(np.asarray(test, dtype=int))
        # break the (possibly multi-block) test index into contiguous blocks
        blocks: list[tuple[int, int]] = []
        lo = prev = int(test_sorted[0])
        for x in test_sorted[1:]:
            x = int(x)
            if x == prev + 1:
                prev = x
            else:
                blocks.append((lo, prev))
                lo = prev = x
        blocks.append((lo, prev))
        # no train index may fall inside [block_lo - purge, block_hi + embargo] (§9.1)
        for b_lo, b_hi in blocks:
            forbidden = set(range(b_lo - purge, b_hi + embargo + 1))
            assert train_set.isdisjoint(forbidden)
        # and train/test never overlap at all
        assert train_set.isdisjoint(set(int(x) for x in test_sorted))


# ================================================== R2 (2026-09-12) realized-hold reporting cells
#
# The WO-3 floor is cost_floor / <sessions>, and the denominator is a HORIZON. Both registered
# values are §7.1 CAPS (20 swing / 120 positional), and a CAP is an upper bound on the hold: the
# 2026-09-12 trend run passed at 3.89x the 120-session floor while its median trade was held 33
# sessions, where the same edge clears by 1.07x. These cells put the measured horizon in the
# artifact. They are REPORTING ONLY and the assertions below pin that: the verdict must not move.


def _hold(**kw) -> RealizedHold:
    base = dict(
        n_trades=303, n_closed=273, n_open=30,
        expectancy_per_trade_pct=3.626, expectancy_per_trade_closed_pct=1.841,
        mean_sessions=48.64, median_sessions=33.0, p90_sessions=121.0,
        mean_sessions_closed=45.05, median_sessions_closed=31.0, p90_sessions_closed=111.2,
    )
    base.update(kw)
    return RealizedHold(**base)


def test_realized_hold_cells_rebase_the_same_cost_floor_on_the_measured_horizon():
    cells = realized_hold_floor_cells(_hold(), CNC_COST_FLOOR_PCT, 0.010342)
    by_label = {c.label: c for c in cells}
    assert len(cells) == 4
    median_all = by_label["realized MEDIAN hold (all trades)"]
    mean_all = by_label["realized MEAN hold (all trades)"]
    # identical arithmetic to margin_floor_pct_per_day, only the denominator changes
    assert median_all.margin_floor_pct_per_day == pytest.approx(CNC_COST_FLOOR_PCT / 33.0)
    assert median_all.margin_floor_pct_per_day == pytest.approx(
        margin_floor_pct_per_day(CNC_COST_FLOOR_PCT, margin_floor_days=33)
    )
    # and the headroom the manager reads the verdict against: ~1.07x at the median, ~1.6x at the mean
    assert median_all.headroom_x == pytest.approx(1.07, abs=0.01)
    assert mean_all.headroom_x == pytest.approx(1.58, abs=0.01)
    # vs 3.89x at the 120-session §7.1 CAP the run was registered at
    assert 0.010342 / margin_floor_pct_per_day(CNC_COST_FLOOR_PCT, margin_floor_days=120) == (
        pytest.approx(3.89, abs=0.01)
    )


@pytest.mark.parametrize(
    "kw",
    [
        {"median_sessions": 0.0, "mean_sessions": 0.0},      # cost_floor/0 is not a per-day bar
        {"median_sessions": -3.0, "mean_sessions": -3.0},    # never observed; must not produce a cell
        {"median_sessions": None, "mean_sessions": None},    # no trades scored
    ],
)
def test_realized_hold_cells_skip_a_hold_that_cannot_denominate_a_floor(kw):
    cells = realized_hold_floor_cells(_hold(**kw), CNC_COST_FLOOR_PCT, 0.01)
    assert [c.label for c in cells] == [
        "realized MEDIAN hold (closed trades only)",
        "realized MEAN hold (closed trades only)",
    ]


def test_realized_hold_cells_are_total_on_missing_inputs():
    """A reporting path must never raise inside a validation run."""
    assert realized_hold_floor_cells(None, CNC_COST_FLOOR_PCT, 0.01) == []
    assert realized_hold_floor_cells(_hold(), None, 0.01) == []
    # no passing splits ⇒ cells still render, headroom is simply unknown
    cells = realized_hold_floor_cells(_hold(), CNC_COST_FLOOR_PCT, None)
    assert cells and all(c.headroom_x is None for c in cells)


def test_realized_hold_never_changes_the_promotion_verdict(clock):
    """The whole point: the bar stays at the REGISTERED denominator, however the hold came out.

    A verdict that moved with a horizon measured on the same run it is judging would make the
    promotion threshold a function of the result.
    """
    def _report(**extra):
        pipe = ValidationPipeline(
            returns_provider=lambda sid, params: _tiny_margin_series(),
            clock=clock,
            splitter=_FixedSplitter(),
            cost_floor_provider=lambda _sid: CNC_COST_FLOOR_PCT,
        )
        return pipe.validate_sync(
            "rsi2", ParamSet(strategy_id="rsi2", params={}, trial_count_n=10, **extra)
        )

    bare = _report()
    # a hold so short the floor at it is astronomically high, and one so long it is near zero
    short = _report(realized_hold=_hold(median_sessions=1.0, mean_sessions=1.0))
    forever = _report(realized_hold=_hold(median_sessions=5000.0, mean_sessions=5000.0))

    assert bare.promotable is short.promotable is forever.promotable is False
    assert short.reasons == forever.reasons == bare.reasons
    assert short.margin_floor_pct_per_day == forever.margin_floor_pct_per_day == pytest.approx(
        0.01596
    )
    assert short.margin_floor_days == forever.margin_floor_days == MARGIN_FLOOR_DAYS
    assert bare.realized_hold is None and bare.realized_hold_cells == []
    assert forever.realized_hold_cells, "the cells are still recorded, just never consulted"


def test_report_renders_the_hold_cells_the_open_split_and_the_survivorship_caveat(clock, tmp_path):
    pipe = ValidationPipeline(
        returns_provider=lambda sid, params: _returns_series(),
        clock=clock,
        splitter=_FixedSplitter(),
        cost_floor_provider=lambda _sid: CNC_COST_FLOOR_PCT,
        reports_dir=tmp_path,
        margin_floor_days=120,
    )
    report = pipe.validate_sync(
        "trend",
        ParamSet(
            strategy_id="trend", params={}, trial_count_n=9,
            realized_hold=_hold(), population_is_survivorship_tainted_proxy=True,
        ),
    )
    md = reports.render_markdown(report)

    assert "MEASURED holding period" in md
    assert "realized MEDIAN hold (all trades)" in md
    assert "still open at the window edge: 30" in md
    # WO-M: closed-only is the headline/ranked figure; all-trades is what is reported beside it
    assert "CLOSED round trips only (the sweep headline and ranking statistic" in md
    assert "SURVIVORSHIP" in md
    assert "120 sessions" in md          # the registered denominator is still what the verdict used
    # and it all travels in the machine-readable artifact, not only the prose
    art = reports.write_report(report, tmp_path)
    data = json.loads(art.json.read_text(encoding="utf-8"))
    assert data["population_is_survivorship_tainted_proxy"] is True
    assert data["realized_hold"]["median_sessions"] == 33.0
    assert data["realized_hold"]["n_open"] == 30
    assert len(data["realized_hold_cells"]) == 4


def test_the_sweep_mechanics_stamp_travels_with_the_verdict_and_never_moves_it(clock, tmp_path):
    """WO-M (2026-09-13): a verdict is only comparable with one produced under the same sweep
    mechanics, so the stamp is rendered and persisted with the report — and, like every other R2/WO-M
    reporting field, it is not an input to the promotion rule."""
    def _report(**extra):
        pipe = ValidationPipeline(
            returns_provider=lambda sid, params: _tiny_margin_series(),
            clock=clock,
            splitter=_FixedSplitter(),
            cost_floor_provider=lambda _sid: CNC_COST_FLOOR_PCT,
            reports_dir=tmp_path,
        )
        return pipe.validate_sync(
            "rsi2", ParamSet(strategy_id="rsi2", params={}, trial_count_n=10, **extra)
        )

    stamped = _report(sweep_mechanics=MECHANICS_STAMP)
    unstamped = _report()

    assert stamped.sweep_mechanics == MECHANICS_STAMP
    assert unstamped.sweep_mechanics is None
    assert stamped.promotable is unstamped.promotable
    assert stamped.reasons == unstamped.reasons

    assert MECHANICS_STAMP in reports.render_markdown(stamped)
    # An UNSTAMPED verdict must claim NOTHING about sweep mechanics. This pipeline also validates
    # returns that never came out of SweepRunner (the event-study harnesses build their own series
    # and charge their own fills/costs — scripts/validate_insider.py is one), and stamping those
    # "PRE-2026-09-13" would assert three sweep biases they structurally cannot have. Absence is
    # the tell; COMMANDS.md says what absence means on a verdict artifact.
    unstamped_md = reports.render_markdown(unstamped)
    assert "PRE-2026-09-13" not in unstamped_md
    assert "Sweep mechanics" not in unstamped_md
    art = reports.write_report(stamped, tmp_path)
    assert json.loads(art.json.read_text(encoding="utf-8"))["sweep_mechanics"] == MECHANICS_STAMP


def test_a_defaults_fallback_verdict_is_flagged_everywhere_and_still_moves_no_threshold(
    clock, tmp_path
):
    """WO-M follow-up: ``_rank_best`` can now return None for a whole grid (nothing closed a round
    trip), and the CLI then validates the §6.3 envelope DEFAULTS. That verdict must not RENDER like
    a grid-winner verdict — banner, note, and JSON — while changing no threshold, because gating on
    it would move the §6.4 promotion rule and no work order registers that."""
    def _report(**extra):
        pipe = ValidationPipeline(
            returns_provider=lambda sid, params: _tiny_margin_series(),
            clock=clock,
            splitter=_FixedSplitter(),
            cost_floor_provider=lambda _sid: CNC_COST_FLOOR_PCT,
            reports_dir=tmp_path,
        )
        return pipe.validate_sync(
            "rsi2", ParamSet(strategy_id="rsi2", params={}, trial_count_n=10, **extra)
        )

    defaulted = _report(params_are_grid_winner=False)
    ranked = _report(params_are_grid_winner=True)

    # reporting only: the verdict and every reason are identical either way
    assert defaulted.promotable is ranked.promotable
    assert defaulted.reasons == ranked.reasons

    md = reports.render_markdown(defaulted)
    assert "THESE PARAMS ARE NOT A GRID WINNER" in md
    assert any("PARAMS ARE NOT A GRID WINNER" in n for n in defaulted.notes)
    ranked_md = reports.render_markdown(ranked)
    assert "NOT A GRID WINNER" not in ranked_md
    assert not any("NOT A GRID WINNER" in n for n in ranked.notes)

    art = reports.write_report(defaulted, tmp_path)
    assert json.loads(art.json.read_text(encoding="utf-8"))["params_are_grid_winner"] is False
