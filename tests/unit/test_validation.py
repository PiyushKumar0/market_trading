"""ValidationPipeline / fold_pass_min tests (E2 / §6.4 / §9.1).

Pure tier (no vectorbt): fold_pass_min exact boundaries, non-promotable without N, promotable iff
the CPCV fold-pass fraction ≥ fold_pass_min(N), anchored walk-forward split determinism, honest
negative-result rendering. The one skfolio-backed test (real CPCV purge/embargo no-overlap) is marked
``needs_heavy_deps``.
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
    CPCVFold,
    ParamSet,
    ValidationPipeline,
    ValidationReport,
    WalkForwardFold,
    cpcv_splits,
    fold_pass_min,
    promotion_decision,
    walk_forward_splits,
)

FIXED_NOW = datetime(2026, 6, 17, 18, 0, tzinfo=IST)


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
    ok, reasons = promotion_decision(None, 1.0)
    assert ok is False
    assert any("no cited N" in r or "N is ABSENT" in r for r in reasons)


def test_promotion_absent_folds_not_promotable():
    ok, reasons = promotion_decision(10, None)
    assert ok is False
    assert any("no folds" in r.lower() or "CPCV produced no folds" in r for r in reasons)


def test_promotion_boundary_at_n10_and_n11():
    # N=10 requires 60%; N=11 requires 70%. A 60% fraction clears N=10, fails N=11.
    ok10, _ = promotion_decision(10, 0.60)
    assert ok10 is True
    assert promotion_decision(10, 0.59)[0] is False
    ok11, reasons11 = promotion_decision(11, 0.60)
    assert ok11 is False
    assert any("fold_pass_min" in r for r in reasons11)


def test_promotion_boundary_at_n31():
    assert promotion_decision(31, 0.80)[0] is True
    assert promotion_decision(31, 0.79)[0] is False


def test_promotion_drawdown_gate():
    # Even a passing fold fraction is rejected when max DD exceeds 1.25× champion's (§6.4 step 2).
    ok, reasons = promotion_decision(5, 1.0, max_dd_pct=20.0, champion_max_dd_pct=10.0)
    assert ok is False
    assert any("drawdown" in r.lower() for r in reasons)
    assert promotion_decision(5, 1.0, max_dd_pct=12.0, champion_max_dd_pct=10.0)[0] is True


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
