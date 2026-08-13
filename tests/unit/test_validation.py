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
from engine.learning.validate import (
    MARGIN_FLOOR_DAYS,
    CPCVFold,
    ParamSet,
    ValidationPipeline,
    ValidationReport,
    WalkForwardFold,
    cpcv_splits,
    fold_pass_min,
    margin_floor_pct_per_day,
    promotion_decision,
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
