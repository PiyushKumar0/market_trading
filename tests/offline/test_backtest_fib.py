"""scripts/backtest_fib.py: the pre-registered Fibonacci retracement study. Synthetic only."""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import numpy as np
import pytest

_ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("mt_backtest_fib", _ROOT / "scripts" / "backtest_fib.py")
fb = importlib.util.module_from_spec(_spec)
sys.modules["mt_backtest_fib"] = fb
_spec.loader.exec_module(fb)
rc, rf, bb = fb.rc, fb.rf, fb.bb

from engine.learning.multiplicity import family_trials, load_trials, permutation_pvalue  # noqa: E402
from engine.marketdata.store import DailyBar, MarketStore  # noqa: E402
from engine.strategy.cost_model import CostModel  # noqa: E402


def _weekdays(start: date, n: int) -> list[date]:
    out, d = [], start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


FLAT = (100, 100, 100, 100)


def _series(rows: list[tuple], sym: str = "SYM") -> bb.Series:
    o, h, lo, c = (np.array([float(r[j]) for r in rows]) for j in range(4))
    return bb.Series(sym, _weekdays(date(2025, 1, 6), len(rows)), o, h, lo, c, np.full(len(rows), 1000.0), [])


def _leg_rows(n: int = 130, top: float = 130.0, after: tuple = (125, 125, 125, 125)) -> list[tuple]:
    """Flat 100 through bar 69, the leg high H = ``top`` at bar 70 (L = 100, first at bar 11), ``after`` from 71."""
    rows = [FLAT] * n
    rows[70] = (100, top, 100, top)
    rows[71:] = [after] * (n - 71)
    return rows


def _legs(rows, structural=()) -> tuple[bb.Series, fb.Legs]:
    s = _series(rows)
    return s, fb.legs_for(s, list(structural))


# ----------------------------------------------------------------------------- the leg
def test_leg_is_the_60_session_high_over_the_59_session_low():
    _, legs = _legs(_leg_rows())
    assert fb.leg_as_of(legs, 75, 75) == fb.Leg(70, 11, 130.0, 100.0)


def test_h_is_the_earliest_of_equal_highs():
    rows = _leg_rows()
    rows[72] = (125, 130, 125, 125)
    _, legs = _legs(rows)
    assert fb.leg_as_of(legs, 75, 75).h == 70


@pytest.mark.parametrize("bar, L, lo", [(10, 100.0, 11), (11, 90.0, 11)])   # h-60 outside, h-59 inside
def test_l_window_is_the_59_sessions_before_h(bar, L, lo):
    rows = _leg_rows()
    rows[bar] = (100, 100, 90, 100)
    _, legs = _legs(rows)
    leg = fb.leg_as_of(legs, 75, 75)
    assert (leg.L, leg.lo) == (L, lo)


@pytest.mark.parametrize("top, qualifies", [(114.99, False), (115.0, True)])
def test_the_15pct_boundary(top, qualifies):
    _, legs = _legs(_leg_rows(top=top, after=(110, 110, 110, 110)))
    assert (fb.leg_as_of(legs, 75, 75) is not None) is qualifies


def test_h_needs_60_sessions_of_history():
    rows = [FLAT] * 100
    rows[59] = (100, 130, 100, 130)
    _, legs = _legs(rows)
    assert all(fb.leg_as_of(legs, y, y) is None for y in range(59, 100))


def test_leg_reads_no_bar_after_y():
    rows = _leg_rows()
    s, legs = _legs(rows)
    y = 75
    before = (fb.leg_as_of(legs, y, y), legs.h[y], legs.lo[y], legs.H[y], legs.L[y], legs.geom[y])
    for i in range(y + 1, len(rows)):
        rows[i] = (1, 500, 1, 1)
    s2, legs2 = _legs(rows)
    after = (fb.leg_as_of(legs2, y, y), legs2.h[y], legs2.lo[y], legs2.H[y], legs2.L[y], legs2.geom[y])
    assert before == after and before[0] is not None
    assert legs.new_high[: y + 1].tolist() == legs2.new_high[: y + 1].tolist()


@pytest.mark.parametrize("ex_bar, qualifies", [(11, True), (12, False), (76, False), (77, True), (5, True)])
def test_ex_date_veto_runs_after_l_through_k(ex_bar, qualifies):
    s = _series(_leg_rows())
    legs = fb.legs_for(s, [s.dates[ex_bar]])
    assert (fb.leg_as_of(legs, 75, 76) is not None) is qualifies


@pytest.mark.parametrize("bad_bar, qualifies", [(10, True), (11, False), (76, False), (77, True)])
def test_every_bar_from_l_through_k_is_usable(bad_bar, qualifies):
    rows = _leg_rows()
    rows[bad_bar] = (100, 100, 100, float("nan"))
    _, legs = _legs(rows)
    assert (fb.leg_as_of(legs, 75, 76) is not None) is qualifies


def test_level_is_rounded_to_the_tick():
    leg = fb.Leg(70, 11, 130.0, 100.0)
    assert [fb.level(leg, d) for d in fb.FIB] == [118.55, 115.0, 111.45]      # 118.54, 115, 111.46


# ----------------------------------------------------------------------------- F1 orders and fills
def _f1(rows, atr=None, depths=(0.382,)):
    s = _series(rows)
    counts: dict = {}
    out = fb.f1_events("SYM", s, fb.legs_for(s, []), np.ones(len(s)) if atr is None else atr, counts, depths)
    return out, counts


def _fills(out, d=0.382):
    return [(f.event.signal_idx, f.event.fill_idx, f.event.fill_px) for f in out[d]]


def test_touch_fills_at_the_level():
    rows = _leg_rows()
    rows[75] = (124, 125, 118, 120)
    out, counts = _f1(rows, depths=(0.382, 0.44))
    assert _fills(out) == [(74, 75, Decimal("118.55"))] and out[0.44] == []
    assert out[0.382][0].leg == fb.Leg(70, 11, 130.0, 100.0) and counts[0.382] == {"touches": 1}


def test_gap_through_fills_at_the_open():
    rows = _leg_rows()
    rows[75] = (117, 117, 116.5, 117)
    out, _ = _f1(rows, depths=(0.382, 0.44))
    assert _fills(out) == [(74, 75, Decimal("117.00"))]
    assert _fills(out, 0.44) == [(74, 75, Decimal("116.80"))]


def test_one_fill_per_leg_and_depth_and_an_earlier_touch_blocks():
    rows = _leg_rows()
    rows[75] = rows[78] = (124, 125, 118, 120)
    out, _ = _f1(rows)
    assert _fills(out) == [(74, 75, Decimal("118.55"))]
    atr = np.ones(len(rows))
    atr[74] = np.nan                                           # the first touch is not admitted
    out, counts = _f1(rows, atr=atr)
    assert out[0.382] == [] and counts[0.382] == {"touches": 1, "no_atr14": 1}


@pytest.mark.parametrize("t, filled", [(90, True), (91, False)])     # t - h = 20 / 21
def test_the_order_rests_while_t_minus_h_is_at_most_20(t, filled):
    rows = _leg_rows()
    rows[t] = (124, 125, 118, 120)
    out, _ = _f1(rows)
    assert bool(out[0.382]) is filled


def test_a_new_high_starts_a_new_leg_that_may_fill():
    rows = _leg_rows()
    rows[75] = (124, 125, 118, 120)
    rows[80] = (125, 140, 125, 140)
    rows[81:] = [(135, 135, 135, 135)] * (len(rows) - 81)
    rows[83] = (135, 135, 124, 130)                            # 140 - 0.382 x 40 = 124.72 -> 124.70
    out, _ = _f1(rows)
    assert _fills(out) == [(74, 75, Decimal("118.55")), (82, 83, Decimal("124.70"))]
    assert out[0.382][1].leg == fb.Leg(80, 21, 140.0, 100.0)


def test_admission_needs_the_t_plus_20_bars():
    rows = _leg_rows(n=94)
    rows[75] = (124, 125, 118, 120)
    out, counts = _f1(rows)
    assert out[0.382] == [] and counts[0.382]["short_forward_window"] == 1


def _pullback(rows):
    s = _series(rows)
    counts: dict = {}
    return fb.pullback_day_events("SYM", s, fb.legs_for(s, []), np.ones(len(s)), counts), counts


def test_any_pullback_day_takes_every_y_within_19_sessions_of_h():
    out, counts = _pullback(_leg_rows())
    assert [(f.event.signal_idx, f.event.fill_idx) for f in out] == [(y, y + 1) for y in range(70, 90)]
    assert {f.event.fill_px for f in out} == {Decimal("125.00")} and counts == {"days": 20}


def test_any_pullback_day_stops_at_a_low_below_l():
    rows = _leg_rows()
    rows[75] = (125, 125, 99.95, 125)
    out, _ = _pullback(rows)
    assert [f.event.signal_idx for f in out] == list(range(70, 75))


def test_any_pullback_day_allows_a_low_at_l():
    rows = _leg_rows()
    rows[75] = (125, 125, 100, 125)
    out, _ = _pullback(rows)
    assert len(out) == 20


# ----------------------------------------------------------------------------- F1 benchmark and permutation
def test_benchmark_members_need_both_bars_and_no_ex_date_in_fill_to_exit():
    rows = [FLAT] * 5
    mk = {"A": 110, "B": 90, "C": 120, "D": 80, "E": 120, "F": 100}
    series = {}
    for sym, exit_close in mk.items():
        r = list(rows)
        r[3] = (100, 100, 100, exit_close)
        series[sym] = _series(r, sym)
    d = series["A"].dates
    series["C"] = bb.Series("C", d[:3], *(a[:3] for a in (series["C"].open, series["C"].high, series["C"].low,
                                                          series["C"].close, series["C"].volume)), [])
    structural = {"B": [d[0]], "D": [d[3]], "E": [d[4]], "F": [d[0] - timedelta(days=1)]}
    bench = fb.Bench(series, structural)
    got, n = bench(d[0], d[3])                                 # B ex on fill, C no exit bar, D ex on exit
    assert n == 3 and got == pytest.approx((10.0 + 20.0 + 0.0) / 3)
    assert bench(d[0], d[3]) == (got, n)


def test_permutation_block_matches_permutation_pvalue_and_needs_a_pool():
    sample, pool = [0.5, 1.0, 1.5], [0.1, 0.2, 0.3, 0.4, 1.2, 2.0]
    assert fb.permutation_block(sample, pool)["p"] == permutation_pvalue(sample, pool, draws=10_000, seed=20261104)
    assert fb.permutation_block(sample, pool[:2])["p"] is None
    assert fb.permutation_block([], pool)["p"] is None


def test_study_f1_pools_the_two_neighbours():
    series = {}
    for i in range(12):
        rows = _leg_rows()
        rows[75] = (124, 125, 105, 120)                          # every depth to 78.6% fills on bar 75
        rows[94] = (125, 125, 125, 100 + 3 * i)                  # T+20 exit close
        series[f"S{i:02d}"] = _series(rows, f"S{i:02d}")
    legs = {k: fb.legs_for(s, []) for k, s in series.items()}
    cost = rc.cost_pct(CostModel.from_config())
    out = fb.study_f1(series, legs, {}, "synthetic", cost)
    nets = {}
    for d in fb.DEPTHS:
        evs = [f.event for k, s in series.items()
               for f in fb.f1_events(k, s, legs[k], rc._atr(s), {}, (d,))[d]]
        nets[d] = [t.net for _, t in rf.measure(rc.Population("x", "x", 12, evs, {}, {}), cost)]
    assert all(len(v) == 12 for v in nets.values())
    for d, (a, b) in fb.NEIGHBOURS.items():
        assert out["permutation"][fb.key(d)]["p"] == permutation_pvalue(nets[d], nets[a] + nets[b], draws=10_000,
                                                                        seed=20261104)
        assert out["permutation"][fb.key(d)]["pool"] == [fb.key(a), fb.key(b)]
    assert out["decision"] == {fb.key(d): rf.INSUFFICIENT for d in fb.FIB}
    assert out["benchmark"]["d38.2"]["n"] == 12 and out["classic"]["d38.2"]["n"] == 12
    assert set(out["cohorts"]) == {fb.key(d) for d in fb.DEPTHS} | {fb.ANY_DAY}


def _r1(n=30, promotable=True, med=0.1, ci_lo=0.01, p=0.0166) -> dict:
    k = "d38.2"
    return {"cohorts": {k: {"n": n, "median_net": med}}, "cpcv": {k: {"promotable": promotable}},
            "benchmark": {k: {"bootstrap": {"ci_lo": ci_lo}}}, "permutation": {k: {"p": p}}, "alpha": fb.F1_ALPHA}


@pytest.mark.parametrize("kw, decision", [
    ({}, rf.SUPPORTED),
    ({"n": 29}, rf.INSUFFICIENT),
    ({"promotable": False}, rf.NOT_SUPPORTED),
    ({"med": 0.0}, rf.NOT_SUPPORTED),
    ({"ci_lo": 0.0}, rf.NOT_SUPPORTED),
    ({"ci_lo": None}, rf.NOT_SUPPORTED),
    ({"p": None}, rf.NOT_SUPPORTED),
    ({"p": 0.05 / 3}, rf.NOT_SUPPORTED),
])
def test_decide_f1_boundaries(kw, decision):
    assert fb.decide_f1(_r1(**kw), "d38.2") == decision


# ----------------------------------------------------------------------------- F2
def _f2(rows, structural=()):
    s = _series(rows)
    counts: dict = {}
    out = fb.f2_legs("SYM", s, fb.legs_for(s, list(structural)), counts)
    return {o.h: o for o in out}, counts


def _pull(n=140, low_at=None):
    rows = _leg_rows(n=n, after=(120, 125, 118, 120))
    for i, low in (low_at or {}).items():
        rows[i] = (120, 125, low, 120)
    return rows


def test_f2_completed_leg_ratio():
    rows = _pull(low_at={75: 115})
    rows[80] = (125, 131, 125, 131)
    legs, _ = _f2(rows)
    assert (legs[70].kind, legs[70].r) == (fb.COMPLETED, 0.5)


def test_f2_low_on_the_completion_bar_does_not_fail_the_leg():
    rows = _pull(low_at={75: 115})
    rows[80] = (125, 131, 99, 131)
    legs, _ = _f2(rows)
    assert legs[70].kind == fb.COMPLETED


def test_f2_failed_incomplete_censored_skipped():
    failed = _pull(low_at={75: 99.95})
    failed[80] = (125, 131, 125, 131)
    assert _f2(failed)[0][70].kind == fb.FAILED
    assert _f2(_pull())[0][70].kind == fb.INCOMPLETE
    assert _f2(_pull(n=130))[0][70].kind == fb.CENSORED                  # ends before h + 60
    skipped = _pull()
    skipped[71] = (125, 131, 125, 131)
    legs, counts = _f2(skipped)
    assert (legs[70].kind, legs[70].r) == (fb.SKIPPED, None) and counts[fb.SKIPPED] == 1


def test_f2_completion_must_come_within_60_sessions():
    rows = _pull(n=200)
    rows[131] = (125, 131, 125, 131)                                     # h + 61
    assert _f2(rows)[0][70].kind == fb.INCOMPLETE
    rows[130] = (125, 131, 125, 131)                                     # h + 60
    assert _f2(rows)[0][70].kind == fb.COMPLETED


def test_f2_needs_a_strict_new_high():
    rows = _pull()
    rows[65] = (100, 130, 100, 100)
    legs, _ = _f2(rows)
    assert 70 not in legs and 65 in legs


def test_f2_ex_date_through_the_last_scanned_session_disqualifies():
    s = _series(_pull(low_at={75: 99.95}))
    for bar, kept in ((75, False), (76, True)):
        counts: dict = {}
        got = {o.h for o in fb.f2_legs("SYM", s, fb.legs_for(s, [s.dates[bar]]), counts)}
        assert (70 in got) is kept
    assert counts == {fb.FAILED: 1}


def test_f2_window_edges_are_half_open_and_tick_exact():
    r = np.array([0.372, 0.391999, 0.392, 0.352, 0.351999, 0.412, 0.411999])
    assert fb.centre_counts(r, 0.382) == {"obs": 2, "shoulders": 3, "expected": 1.5, "ratio": 2 / 1.5}
    rows = _leg_rows(n=140, after=(120, 125, 119, 120))
    rows[75] = (120, 125, 118.84, 120)                                 # (130 - 118.84) / 30 = 0.372 exactly
    rows[80] = (125, 131, 125, 131)
    leg = _f2(rows)[0][70]
    assert leg.r == 0.372 and fb.centre_counts(np.array([leg.r]), 0.382)["obs"] == 1


def test_f2_pooled_ratio_and_placebo():
    r = np.array([0.38, 0.5, 0.505, 0.62, 0.36, 0.40, 0.48, 0.64, 0.44])
    fib = fb.pooled(r, fb.FIB)
    assert (fib["obs"], fib["expected"], fib["ratio"]) == (4, 2.0, 2.0)
    assert fb.pooled(r, fb.CONTROLS) == {"obs": 1, "expected": 0.0, "ratio": None}     # no placebo shoulders
    plc = fb.pooled(np.append(r, [0.42, 0.46]), fb.CONTROLS)                         # 0.44's shoulders
    assert (plc["obs"], plc["expected"], plc["ratio"]) == (1, 1.0, 1.0)


def test_f2_bootstrap_is_deterministic_and_clusters_on_h_dates():
    days = _weekdays(date(2025, 1, 6), 40)
    rng = np.random.default_rng(1)
    r = np.round(rng.uniform(0.3, 0.7, 200), 3)
    dates = [days[i % 40] for i in range(200)]
    a, b = fb.bootstrap_f2(dates, r), fb.bootstrap_f2(dates, r)
    assert a == b and a["n_clusters"] == 40 and a["reps"] == 2000 and a["seed"] == 20261104
    assert a["ci_lo"] <= fb.pooled(r, fb.FIB)["ratio"] <= a["ci_hi"]
    other = fb.bootstrap_f2(dates, r, seed=1)
    assert (other["ci_lo"], other["ci_hi"]) != (a["ci_lo"], a["ci_hi"])
    assert fb.bootstrap_f2([], np.array([]))["ci_lo"] is None


def test_f2_histogram_bins():
    h = fb.histogram(np.array([0.0, 0.29, 0.999, 1.0]))
    assert len(h) == 100 and h[0] == 1 and h[29] == 1 and h[99] == 2


def _r2(obs=100, ratio=1.10, ci_lo=1.01, placebo=1.0) -> dict:
    return {"pooled": {"fib": {"obs": obs, "ratio": ratio}, "placebo": {"ratio": placebo}},
            "bootstrap": {"ci_lo": ci_lo}}


@pytest.mark.parametrize("kw, decision", [
    ({}, rf.SUPPORTED),
    ({"obs": 99}, rf.INSUFFICIENT),
    ({"ratio": 1.0999}, rf.NOT_SUPPORTED),
    ({"ratio": 110 / 100}, rf.SUPPORTED),
    ({"ci_lo": 1.0}, rf.NOT_SUPPORTED),
    ({"ci_lo": None}, rf.NOT_SUPPORTED),
    ({"placebo": 1.10}, rf.NOT_SUPPORTED),                               # placebo tie
    ({"placebo": None}, rf.NOT_SUPPORTED),
    ({"ratio": None}, rf.NOT_SUPPORTED),
])
def test_decide_f2_boundaries(kw, decision):
    assert fb.decide_f2(_r2(**kw)) == decision


# ----------------------------------------------------------------------------- F3
def _conf_series(fill_bar_high: float = 106) -> bb.Series:
    rows = [(90, 90, 85, 90)] * 120                                    # L = 85
    rows[70] = (90, 115, 90, 115)                                      # H = 115: the 50% level is 100.00
    rows[71:] = [(105, 106, 98, 105)] * 49
    rows[75] = (101, fill_bar_high, 98, 105)
    return _series(rows)


@pytest.mark.parametrize("fill, inside", [("101.00", True), ("101.01", False), ("99.00", True), ("98.99", False)])
def test_confluence_tolerance_is_1pct_inclusive(fill, inside):
    s = _conf_series()
    ev = rc.Event("SYM", s, 74, 75, Decimal(fill), 1.0)
    assert fb.confluent(ev, fb.legs_for(s, [])) is inside


def test_confluence_reads_the_leg_as_of_the_session_before_the_fill():
    s = _conf_series(fill_bar_high=200)                                 # a new high on the fill bar
    ev = rc.Event("SYM", s, 74, 75, Decimal("100.00"), 1.0)
    assert fb.confluent(ev, fb.legs_for(s, [])) is True
    assert fb.confluent(ev, fb.legs_for(s, [s.dates[75]])) is None    # ex-date on the fill session


def _r3(n=30, promotable=True, med=0.3, full=0.2, p=0.0083) -> dict:
    return {"cohorts": {fb.CONFLUENCE: {"n": n, "median_net": med}, rf.FULL: {"n": 100, "median_net": full}},
            "cpcv": {fb.CONFLUENCE: {"promotable": promotable}}, "permutation": {"p": p},
            "alpha": 0.05 / 6}


@pytest.mark.parametrize("kw, decision", [
    ({}, rf.SUPPORTED),
    ({"n": 29}, rf.INSUFFICIENT),
    ({"promotable": False}, rf.NOT_SUPPORTED),
    ({"med": 0.2}, rf.NOT_SUPPORTED),                                   # not above FULL
    ({"full": -0.1, "med": 0.0}, rf.NOT_SUPPORTED),                     # not above 0
    ({"p": 0.05 / 6}, rf.NOT_SUPPORTED),
    ({"p": None}, rf.NOT_SUPPORTED),
])
def test_decide_f3_boundaries(kw, decision):
    assert fb.decide_f3(_r3(**kw)) == decision


def test_study_f3_splits_confluence_from_full():
    s = _conf_series()
    pop = rc.Population("ins", "synthetic", 1, [rc.Event("SYM", s, 74, 75, Decimal(px), 1.0)
                                                for px in ("101.00", "101.01")], {}, {})
    r = fb.study_f3(pop, rc.cost_pct(CostModel.from_config()))
    assert {k: c["events"] for k, c in r["cohorts"].items()} == {rf.FULL: 2, fb.CONFLUENCE: 1}
    assert (r["family"], r["family_n"], r["alpha"], r["decision"]) == ("selection_filter/ins", 1, 0.05,
                                                                        rf.INSUFFICIENT)


# ----------------------------------------------------------------------------- F4
def _f4_series(touch_bar: int = 27, low: float = 108.0, open_: float = 110.0) -> bb.Series:
    rows = [(100, 100, 95, 100)] * 40
    rows[4] = (100, 100, 50, 100)                                      # y-21: outside L20's window
    rows[10] = (100, 100, 90, 100)                                     # L20 = 90
    rows[25] = (100, 110, 50, 108)                                     # y: its own low is not in L20's window
    rows[26] = (112, 120, 119, 118)                                    # H_t from y+2 on is 120
    rows[27:31] = [(121, 121, 119, 120)] * 4
    rows[touch_bar] = (open_, 121, low, 109)
    return _series(rows)


def _sig(s, y=25) -> bb.Signal:
    return bb.Signal("SYM", y, s.dates[y], 100.0, 100.0, 0.08, 1.0)


def test_f4_level_follows_the_highest_high_since_y():
    s = _f4_series()                                                   # 120 - 0.382 x 30 = 108.54 -> 108.55
    assert fb.fib_fill(0.382)(s, _sig(s)) == (27, 108.55)              # a fixed H = 110 would rest at 102.35


def test_f4_gap_through_fills_at_the_open():
    s = _f4_series(open_=105.0, low=104.0)
    assert fb.fib_fill(0.382)(s, _sig(s)) == (27, 105.0)


@pytest.mark.parametrize("touch_bar, filled", [(30, True), (31, False)])   # y+5 / y+6
def test_f4_window_is_y_plus_1_to_y_plus_5(touch_bar, filled):
    s = _f4_series(touch_bar=touch_bar)
    assert (fb.fib_fill(0.382)(s, _sig(s)) is not None) is filled


def test_f4_fill_slot_in_brk20_population(tmp_path, clock):
    conn = rc.open_snapshot(_brk20_snapshot(tmp_path, clock))
    try:
        base = rc.brk20_population(conn, END)
        same = rc.brk20_population(conn, END, fill=lambda s, sig: bb.fill_for(s, sig, bb.VARIANT_V2_N5))
        seen = []
        moved = rc.brk20_population(conn, END, fill=lambda s, sig: seen.append(sig.idx) or (sig.idx + 3, 99.5))
        none = rc.brk20_population(conn, END, fill=lambda s, sig: None)
    finally:
        conn.close()
    def rows(pop):
        return [(e.symbol, e.signal_idx, e.fill_idx, e.fill_px, e.atr, e.levels) for e in pop.events]

    assert [r[:4] for r in rows(base)] == [("BRKA", 100, 101, Decimal("100.00"))]
    assert rows(base) == rows(same) and base.counts == same.counts
    assert seen == [100] and [(e.fill_idx, e.fill_px) for e in moved.events] == [(103, Decimal("99.50"))]
    assert none.events == [] and none.counts["unfilled"] == 1


def _r4(n=30, promotable=True, med=0.3, base=0.2) -> dict:
    return {"cohorts": {"d38.2": {"n": n, "median_net": med}, fb.BASE: {"n": 100, "median_net": base}},
            "cpcv": {"d38.2": {"promotable": promotable}}}


@pytest.mark.parametrize("kw, decision", [
    ({}, rf.SUPPORTED),
    ({"n": 29}, rf.INSUFFICIENT),
    ({"promotable": False}, rf.NOT_SUPPORTED),
    ({"med": 0.2}, rf.NOT_SUPPORTED),
    ({"base": -0.1, "med": 0.0}, rf.NOT_SUPPORTED),
    ({"base": None}, rf.NOT_SUPPORTED),
])
def test_decide_f4_boundaries(kw, decision):
    assert fb.decide_f4(_r4(**kw), "d38.2") == decision


@pytest.mark.parametrize("decisions, meds, won", [
    ({"d38.2": rf.SUPPORTED, "d50": rf.SUPPORTED, "d61.8": rf.NOT_SUPPORTED}, (0.4, 0.5, 0.9), "d50"),
    ({"d38.2": rf.SUPPORTED, "d50": rf.SUPPORTED, "d61.8": rf.INSUFFICIENT}, (0.5, 0.5, 0.9), fb.WINNER_TIE),
    ({"d38.2": rf.NOT_SUPPORTED, "d50": rf.NOT_SUPPORTED, "d61.8": rf.INSUFFICIENT}, (0.5, 0.5, 0.9), fb.NO_WINNER),
])
def test_f4_winner_is_the_highest_supported_median(decisions, meds, won):
    r = {"decision": decisions, "cohorts": {k: {"median_net": m} for k, m in zip(decisions, meds, strict=True)}}
    assert fb.winner(r) == won


def test_f4_matched_view_pairs_on_symbol_and_signal_date():
    s = _series([FLAT] * 40)
    t = [rc.Trade("SYM", s.dates[i], s.dates[i + 1], "time", 1.0, float(i)) for i in range(4)]
    ev = [rc.Event("SYM", s, i, i + 1, Decimal("100"), 1.0) for i in range(4)]
    a = [(ev[0], t[0]), (ev[1], t[1]), (ev[2], None)]
    b = [(ev[1], t[1]), (ev[2], t[2]), (ev[3], t[3])]
    assert fb.matched(a, b) == {"n": 1, "median_net": 1.0, "median_net_V2-5": 1.0}


# ----------------------------------------------------------------------------- registration
def test_family_n_matches_the_ledger():
    trials = load_trials()
    assert family_trials(trials, "edge/fib_pullback") == fb.F1_N == 3
    assert family_trials(trials, "price_structure/fib") == fb.F2_N == 1
    for s, n in fb.F3_N.items():
        assert family_trials(trials, f"selection_filter/{s}") == n
    assert family_trials(trials, "entry_mechanism/brk20") == fb.F4_N == 6


def test_the_registered_constants():
    assert (fb.REGISTERED, fb.SEED, fb.DRAWS, fb.BOOT_REPS) == (date(2026, 11, 4), 20261104, 10_000, 2000)
    assert (fb.LEG_WINDOW, fb.LOW_WINDOW, fb.MIN_LEG, fb.PULLBACK_MAX, fb.COMPLETE_MAX) == (60, 59, 0.15, 20, 60)
    assert (fb.F4_WINDOW, fb.L20_LOOKBACK, fb.T20, fb.F1_ALPHA) == (5, 20, 19, 0.05 / 3)
    assert set(fb.NEIGHBOURS) == set(fb.FIB) and {x for v in fb.NEIGHBOURS.values() for x in v} == set(fb.CONTROLS)


# ----------------------------------------------------------------------------- snapshots + CLI
DATES = _weekdays(date(2026, 1, 5), 190)
END = DATES[-1]


def _daily(sym: str, rows: list[tuple], dates=DATES) -> list[DailyBar]:
    return [DailyBar(symbol=sym, d=dates[i], open=Decimal(str(r[0])), high=Decimal(str(r[1])),
                     low=Decimal(str(r[2])), close=Decimal(str(r[3])), volume=r[4] if len(r) > 4 else 1000,
                     src="bhavcopy") for i, r in enumerate(rows)]


def _brk20_snapshot(tmp_path, clock, extra: dict[str, list[tuple]] | None = None) -> Path:
    rows = [(100, 100, 99, 100, 1000)] * len(DATES)
    rows[100] = (100, 104, 100, 104, 2000)                    # fresh 20-day breakout on 2x volume
    rows[101] = (103, 105, 99, 101, 1000)                     # touches the 100.00 level: V2-5 fill
    bars = {"BRKA": rows, **(extra or {})}
    db = tmp_path / "research" / f"market_{END}.duckdb"
    store = MarketStore(db, tmp_path / "parquet", clock).open()
    try:
        store.upsert_bars_1d([b for sym, r in bars.items() for b in _daily(sym, r)])
        store.upsert_universe_daily([{"d": rc.BRK20_UNIVERSE_AS_OF, "symbol": sym, "included": True}
                                     for sym in bars])
    finally:
        store.close()
    return db


def _population(strategy: str) -> rc.Population:
    c = 100.0 + np.random.default_rng(0).uniform(-10.0, 10.0, 60)
    s = bb.Series("SYM", DATES[:60], c, c, c, c, np.full(60, 1000.0), [])
    return rc.Population(strategy, "synthetic", 1, [rc.Event("SYM", s, i, i + 1, Decimal("100.00"), 1.0)
                                                    for i in (5, 10, 15, 20)], {"unfilled": 1}, {})


def test_cli_refuses_until_registered(tmp_path, clock, monkeypatch, capsys):
    monkeypatch.setattr(rc, "market_hours_refusal", lambda: None)
    monkeypatch.setattr(fb, "REGISTERED", None)
    assert fb.main(["--db", str(_brk20_snapshot(tmp_path, clock)), "--end", str(END)]) == 2
    assert "not registered" in capsys.readouterr().err


def test_cli_writes_the_report(tmp_path, clock, monkeypatch, capsys):
    leg = _leg_rows(n=len(DATES))
    leg[75] = (124, 125, 105, 120)
    db = _brk20_snapshot(tmp_path, clock, {"LEGA": leg})
    monkeypatch.setattr(rc, "market_hours_refusal", lambda: None)
    monkeypatch.setattr(rf, "build_population", lambda s, *a: _population(s))
    monkeypatch.setattr(rc, "brk20_population", lambda conn, end, **kw: _population("brk20"))
    out = tmp_path / "reports" / "fib.json"
    assert fb.main(["--db", str(db), "--end", str(END), "--out", str(out)]) == 0
    doc = json.loads(out.read_text(encoding="utf-8"))
    assert set(doc) == {"meta", "f1", "f2", "f3", "f4"}
    assert doc["meta"]["family_n"]["entry_mechanism/brk20"] == 6 and doc["meta"]["seed"] == 20261104
    assert doc["f1"]["population"]["n_symbols"] == 2 and doc["f1"]["cohorts"]["d38.2"]["n"] == 1
    assert doc["f2"]["counts"]["censored"] + doc["f2"]["counts"]["incomplete"] >= 1
    assert list(doc["f3"]) == ["brk20", "hi52", "ins"] and doc["f4"]["population"]["d50"]["fill_rate"] == 0.8
    assert doc["f4"]["winner"] == fb.NO_WINNER
    text = capsys.readouterr().out
    assert text.isascii() and "SURVIVORSHIP-TAINTED" in text and "touch is not a guaranteed fill" in text
    assert text.index("GEOMETRY FIRST") < text.index("DECISION")
    assert "DECISION edge/fib_pullback d38.2: insufficient" in text and "DECISION price_structure/fib:" in text
    assert out.with_suffix(".md").exists()
