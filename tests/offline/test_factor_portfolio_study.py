"""``scripts/factor_portfolio_study.py``: clean returns, timing, universe, the book, costs, bootstrap and verdict.

Every bar is synthetic; nothing reads a research snapshot.
"""

from __future__ import annotations

import importlib.util
import json
import math
import sys
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

_SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))
_spec = importlib.util.spec_from_file_location("mt_factor_portfolio_study", _SCRIPTS / "factor_portfolio_study.py")
fps = importlib.util.module_from_spec(_spec)
sys.modules["mt_factor_portfolio_study"] = fps
_spec.loader.exec_module(fps)

import duckdb  # noqa: E402,I001  after the script module, which imports engine first
from engine.core.clock import IST  # noqa: E402
from engine.strategy.cost_model import CostModel  # noqa: E402

AFTER_HOURS = datetime(2026, 11, 12, 20, 0, tzinfo=IST)


def _weekdays(start: date, n: int) -> list[date]:
    out, d = [], start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


def _frame(dates: list[date], series: dict[str, dict]) -> pd.DataFrame:
    """``series``: symbol -> {"open", "close", optional "high", "volume", "absent" (session indices)}; NIFTY 50 is
    added flat at 20,000 on every date unless given."""
    series = {"NIFTY 50": {"open": 20_000.0, "close": 20_000.0}, **series}
    rows = []
    for sym, s in series.items():
        n = len(dates)
        o = np.broadcast_to(np.asarray(s["open"], dtype=float), (n,))
        c = np.broadcast_to(np.asarray(s["close"], dtype=float), (n,))
        h = np.broadcast_to(np.asarray(s.get("high", np.maximum(o, c)), dtype=float), (n,))
        v = np.broadcast_to(np.asarray(s.get("volume", 100_000.0), dtype=float), (n,))
        absent = set(s.get("absent", ()))
        rows += [(sym, d, o[i], h[i], c[i], v[i]) for i, d in enumerate(dates) if i not in absent]
    return pd.DataFrame(rows, columns=["symbol", "d", "open", "high", "close", "volume"])


def _panel(dates, series, structural=None, etf=frozenset()):
    return fps.build_panel(_frame(dates, series), structural or {}, etf)


def _fm(panel, f, ranked, universe=None, invest=True):
    cols = np.array([panel.col[s] for s in ranked], dtype=np.int64)
    uni = cols if universe is None else np.array([panel.col[s] for s in universe], dtype=np.int64)
    return fps.Formation(f, f + 1, uni, cols, invest)


def _free(v: float) -> float:
    return 0.0


@pytest.fixture(scope="module")
def cm() -> CostModel:
    return CostModel.from_config()


def _random_market(n_sessions: int, n_stocks: int, seed: int = 3) -> tuple[list[date], dict[str, dict]]:
    rng = np.random.default_rng(seed)
    dates = _weekdays(date(2023, 1, 2), n_sessions)
    series: dict[str, dict] = {}
    for i in range(n_stocks):
        c = 100.0 * np.exp(np.cumsum(rng.normal(0.0004 * (i % 5), 0.01 + 0.002 * (i % 7), n_sessions)))
        o = c * np.exp(rng.normal(0, 0.003, n_sessions))
        series[f"S{i:02d}"] = {"open": o, "close": c, "high": np.maximum(o, c) * 1.01,
                               "volume": rng.integers(50_000, 500_000, n_sessions).astype(float)}
    return dates, series


# ----------------------------------------------------------------------------- clean returns
def test_structural_ex_date_and_unexplained_jump_are_zeroed():
    dates = _weekdays(date(2024, 1, 1), 10)
    split = np.array([100.0] * 5 + [50.0] * 5)
    jump = np.array([100.0] * 6 + [150.0] * 4)
    up = np.array([100.0] * 6 + [120.0] * 4)
    late = np.array([100.0] * 7 + [50.0] * 3)          # ex-date on session 6, no bar that day
    p = _panel(dates, {"A": {"open": split, "close": split}, "B": {"open": jump, "close": jump},
                       "C": {"open": up, "close": up}, "D": {"open": late, "close": late, "absent": [6]}},
               {"A": [dates[5]], "D": [dates[6]]})
    a, b, c, d = (p.col[s] for s in "ABCD")
    assert p.R[5, a] == 0.0 and p.on_zeroed[5, a] and p.ON[5, a] == 1.0
    assert p.R[6, b] == 0.0 and p.on_zeroed[6, b]
    assert p.R[6, c] == pytest.approx(0.2) and not p.on_zeroed[6, c]
    assert np.isnan(p.R[6, d]) and p.R[7, d] == 0.0                # the next bar spans the ex-date
    assert p.counts["zeroed_structural"] == 2 and p.counts["zeroed_jump"] == 1
    assert np.isnan(p.R[0, a])                                      # first bar: no return


def test_bars_off_the_index_calendar_are_dropped_and_listed():
    dates = _weekdays(date(2024, 1, 1), 10)
    frame = _frame(dates, {"A": {"open": 100.0, "close": 100.0}})
    extra = pd.DataFrame([("A", date(2024, 1, 6), 300.0, 300.0, 300.0, 1.0),
                          ("A", date(2023, 12, 29), 300.0, 300.0, 300.0, 1.0)], columns=frame.columns)
    p = fps.build_panel(pd.concat([frame, extra], ignore_index=True), {}, frozenset())
    assert p.n_sessions == 10 and p.off_calendar_inside == [date(2024, 1, 6)]
    assert p.counts["off_calendar_rows"] == 2 and p.counts["off_calendar_dates_inside_range"] == 1
    assert np.nanmax(p.close[:, p.col["A"]]) == 100.0


def test_split_inside_a_hold_leaves_value_unchanged():
    dates = _weekdays(date(2024, 1, 1), 10)
    split = np.array([100.0] * 5 + [50.0] * 5)
    p = _panel(dates, {"A": {"open": split, "close": split}}, {"A": [dates[5]]})
    sim = fps.simulate(p, [_fm(p, 1, ["A"]), _fm(p, 7, [], universe=[])], n=1, cost=_free, capital=1000.0)
    assert sim.close_equity[4] == pytest.approx(1000.0)
    assert sim.close_equity[5] == pytest.approx(1000.0)            # ex-date: price halves, value does not
    assert sim.cash[8] == pytest.approx(1000.0)                    # sold at 50 after the split: no loss
    assert sim.equity_before[1] == pytest.approx(1000.0)


def test_session_without_stock_bars_leaves_values_and_equity_unchanged(cm):
    dates = _weekdays(date(2024, 1, 1), 10)
    px = np.array([100.0] * 7 + [110.0] * 3)
    p = _panel(dates, {"A": {"open": px, "close": px, "absent": [6]}, "B": {"open": 50.0, "close": 50.0,
                                                                         "absent": [6]}})
    assert p.present[6, p.col["NIFTY 50"]] and not p.present[6, p.stock].any()
    sim = fps.simulate(p, [_fm(p, 1, ["A", "B"])], n=2, cost=lambda v: fps.round_trip_cost(cm, v))
    assert sim.close_equity[6] == sim.close_equity[5]
    assert sim.cash[6] == sim.cash[5]
    a = p.col["A"]
    assert p.R[7, a] == pytest.approx(0.1)                           # against session 5's close
    qty = next(t[3] for t in sim.trades if t[1] == a and t[2] == "buy")
    v7 = qty * 110.0
    v_b = next(t[3] * t[4] for t in sim.trades if t[1] == p.col["B"])
    assert sim.close_equity[7] == pytest.approx(sim.cash[7] + v7 - fps.round_trip_cost(cm, v7)
                                                + v_b - fps.round_trip_cost(cm, v_b))


# ----------------------------------------------------------------------------- the book
def test_hold_band_keeps_rank_2n_and_sells_2n_plus_1():
    dates = _weekdays(date(2024, 1, 1), 10)
    p = _panel(dates, {s: {"open": 100.0, "close": 100.0} for s in "ABCDEF"})
    fms = [_fm(p, 1, list("ABCDEF")), _fm(p, 4, list("CDEABF"))]     # n=2: A at rank 4 = 2n, B at 5
    sim = fps.simulate(p, fms, n=2, cost=_free, capital=1000.0)
    col = {v: k for k, v in p.col.items()}
    second = [(col[t[1]], t[2]) for t in sim.trades if t[0] == 5]
    assert second == [("B", "sell"), ("C", "buy")]
    assert sim.counts["sold_out_of_band"] == 1 and sim.held_after == [2, 2]


def test_holding_that_left_universe_or_is_unranked_is_sold():
    dates = _weekdays(date(2024, 1, 1), 10)
    p = _panel(dates, {s: {"open": 100.0, "close": 100.0} for s in "ABC"})
    fms = [_fm(p, 1, ["A", "B"]), _fm(p, 4, ["C"], universe=["B", "C"])]
    sim = fps.simulate(p, fms, n=2, cost=_free, capital=1000.0)
    assert sim.counts["sold_left_universe"] == 1 and sim.counts["sold_unranked"] == 1


def test_whole_shares_skip_a_price_above_the_allocation():
    dates = _weekdays(date(2024, 1, 1), 6)
    p = _panel(dates, {"A": {"open": 600.0, "close": 600.0}, "B": {"open": 100.0, "close": 100.0},
                       "C": {"open": 100.0, "close": 100.0}})
    sim = fps.simulate(p, [_fm(p, 1, ["A", "B", "C"])], n=2, cost=_free, capital=1000.0)
    col = {v: k for k, v in p.col.items()}
    assert [(col[t[1]], t[3]) for t in sim.trades] == [("B", 5), ("C", 5)]   # allocation 500 each
    assert sim.counts["skipped_buy_price"] == 1
    assert sim.cash[2] == pytest.approx(0.0)


def test_buy_without_a_bar_is_skipped_and_buys_stop_at_the_band():
    dates = _weekdays(date(2024, 1, 1), 6)
    p = _panel(dates, {"A": {"open": 100.0, "close": 100.0, "absent": [2]},
                       **{s: {"open": 100.0, "close": 100.0} for s in "BCDE"}})
    sim = fps.simulate(p, [_fm(p, 1, list("ABCDE"))], n=2, cost=_free, capital=1000.0)
    assert sim.counts["skipped_buy_no_bar"] == 1 and sim.buys == [2]
    sim = fps.simulate(p, [_fm(p, 1, list("ABCDE"))], n=1, cost=_free, capital=50.0)
    assert sim.buys == [0] and sim.counts["short_book_formations"] == 1   # nothing buyable within rank 2
    assert sim.counts["skipped_buy_no_bar"] == 1 and sim.counts["skipped_buy_price"] == 1


def test_deferred_sale_fills_at_next_open_or_closes_after_five_sessions():
    dates = _weekdays(date(2024, 1, 1), 16)
    px = np.array([100.0] * 6 + [120.0] * 10)
    p = _panel(dates, {"A": {"open": px, "close": 100.0, "absent": [5, 6]},
                       "B": {"open": 100.0, "close": 100.0, "absent": list(range(5, 16))}})
    fms = [_fm(p, 1, ["A", "B"]), _fm(p, 4, [], universe=[])]
    sim = fps.simulate(p, fms, n=2, cost=_free, capital=1000.0)
    a, b = p.col["A"], p.col["B"]
    sells = {t[1]: t for t in sim.trades if t[2] == "sell"}
    assert sells[a][0] == 7 and sells[a][3] == pytest.approx(500.0 * 1.2)   # next bar's open
    assert sells[b][0] == 9 and sells[b][3] == pytest.approx(500.0)         # sessions 5..9 without a bar
    assert sim.counts["deferred_sales"] == 2 and sim.counts["deferred_sales_filled"] == 1
    assert sim.counts["deferred_sales_closed_at_last_value"] == 1
    assert sim.equity_before[1] == pytest.approx(1000.0)


def test_costs_liquidation_equity_and_one_round_trip(cm):
    dates = _weekdays(date(2024, 1, 1), 8)
    p = _panel(dates, {"A": {"open": 100.0, "close": 100.0}})
    sim = fps.simulate(p, [_fm(p, 1, ["A"]), _fm(p, 4, [], universe=[])], n=1,
                       cost=lambda v: fps.round_trip_cost(cm, v), capital=120_000.0)
    rt = float(cm.round_trip(Decimal("120000.00"), "CNC").total_cost)
    assert rt > 0
    assert sim.close_equity[2] == pytest.approx(120_000.0 - rt, abs=1e-9)   # lower by exactly one round trip
    assert sim.equity_before[1] == pytest.approx(120_000.0 - rt, abs=1e-9)
    assert sim.cash[5] == pytest.approx(120_000.0 - rt, abs=1e-9)           # buy -> sell, flat: one round trip
    assert sim.realized_cost[5] == pytest.approx(rt, abs=1e-9)
    assert fps.round_trip_cost(cm, 0.0) == 0.0


def test_period_rows_net_and_benchmark_are_open_to_open():
    dates = _weekdays(date(2024, 1, 1), 10)
    o = np.array([100.0] * 2 + [100.0, 104.0, 104.0, 110.0] + [110.0] * 4)
    c = np.array([100.0] * 2 + [102.0, 104.0, 105.0, 111.0] + [110.0] * 4)
    p = _panel(dates, {"A": {"open": o, "close": c}, "B": {"open": 50.0, "close": 50.0}})
    fms = [_fm(p, 1, ["A"], universe=["A", "B"]), _fm(p, 4, ["A"], universe=["A", "B"])]
    bench = fps.benchmarks(p, {1: fms[0].universe, 4: fms[1].universe})
    sim = fps.simulate(p, fms, n=1, cost=_free, capital=1000.0)
    rows = fps.period_rows(p, fms, sim, bench)
    assert len(rows) == 1
    # A: open(2) 100 -> close(2) 102 -> close(3) 104 -> close(4) 105 -> open(5) 110: +10%; B flat.
    assert rows[0]["benchmark"] == pytest.approx(0.05)
    assert rows[0]["net"] == pytest.approx(0.10)
    assert rows[0]["e"] == pytest.approx(0.05)


# ----------------------------------------------------------------------------- timing + universe + scores
def test_no_lookahead_scores_universe_and_fills_use_open_r():
    dates, series = _random_market(300, 30)
    series["LATE"] = {"open": 100.0, "close": 100.0, "absent": list(range(60))}
    f = fps.formation_indices(_panel(dates, series))[0]
    assert f >= fps.WINDOW and (dates[f + 1].month != dates[f].month)
    # 199 bars in the 252 sessions to f: out; a bar on f+1 must not bring it in.
    series["EDGE"] = {"open": 100.0, "close": 100.0, "volume": 10**9, "absent": list(range(f - 250, f - 197))}
    p1 = _panel(dates, series)
    rng = np.random.default_rng(9)
    changed = {}
    for sym, s in series.items():
        s2 = dict(s)
        for key in ("open", "close", "high", "volume"):
            arr = np.array(np.broadcast_to(np.asarray(s.get(key, 100_000.0 if key == "volume" else s["close"]),
                                                      dtype=float), (len(dates),)))
            arr[f + 1:] *= rng.uniform(0.5, 1.5, len(dates) - f - 1)
            s2[key] = arr
        changed[sym] = s2
    changed["EDGE"]["absent"] = [*series["EDGE"]["absent"], *range(f + 1, f + 6)]
    p2 = _panel(dates, changed, {"S00": [dates[f + 1]]})
    u1, _ = fps.universe_at(p1, f, 50)
    u2, _ = fps.universe_at(p2, f, 50)
    np.testing.assert_array_equal(u1, u2)
    assert p1.col["LATE"] not in set(u1.tolist())                     # first bar after f - 252
    assert p1.col["EDGE"] not in set(u1.tolist())
    for construct in ("momentum", "lowvol", "composite", "mom6_1", "mom12_1_raw", "mom20", "hi52"):
        s1, s2 = fps.scores(p1, f, u1, construct), fps.scores(p2, f, u2, construct)
        np.testing.assert_array_equal(s1, s2)
        np.testing.assert_array_equal(fps.rank_order(u1, s1), fps.rank_order(u2, s2))
    for p in (p1, p2):
        fm = fps.build_formations(p, {f: u1}, "momentum")[0]
        sim = fps.simulate(p, [fm], n=5, cost=_free, capital=100_000.0)
        buys = [t for t in sim.trades if t[2] == "buy"]
        assert len(buys) == 5 and all(t[0] == f + 1 for t in buys)
        assert all(t[4] == p.open[f + 1, t[1]] and t[3] == math.floor(20_000.0 / t[4]) for t in buys)


def test_universe_name_rules_and_liquidity_order():
    dates, series = _random_market(300, 6)
    for sym in ("XYZ-BE", "GOLDBEES", "FUNDX"):
        series[sym] = {"open": 100.0, "close": 100.0, "volume": 10**9}
    series["THIN"] = {"open": 100.0, "close": 100.0, "volume": 10**9, "absent": list(range(100, 160))}
    p = _panel(dates, series, etf=frozenset({"FUNDX"}))
    f = fps.formation_indices(p)[0]
    u, funnel = fps.universe_at(p, f, 3)
    names = [p.symbols[j] for j in u]
    assert not {"XYZ-BE", "GOLDBEES", "FUNDX", "THIN", "NIFTY 50"} & set(names)
    assert funnel["index_row"] == 1 and funnel["etf"] == 2 and funnel["series_suffix"] == 1
    assert funnel["under_200_bars_in_252"] == 1 and funnel["eligible"] == 6 and len(u) == 3
    liq = [np.nanmedian(p.close[f - 125:f + 1, j] * p.volume[f - 125:f + 1, j]) for j in u]
    assert liq == sorted(liq, reverse=True)


def test_scores_definitions_on_a_known_series():
    dates, series = _random_market(300, 4)
    p = _panel(dates, series)
    f = fps.formation_indices(p)[0]
    cols = np.arange(len(p.symbols))[p.stock]
    j = int(cols[0])
    r = p.R[:, j]
    sd = np.nanstd(r[f - 251:f + 1], ddof=1) * math.sqrt(252)
    mom = np.prod(1 + r[f - 251:f - 20]) - 1
    k = list(cols).index(j)
    assert fps.scores(p, f, cols, "lowvol")[k] == pytest.approx(-sd)
    assert fps.scores(p, f, cols, "momentum")[k] == pytest.approx(mom / sd)
    assert mom == pytest.approx(p.close[f - 21, j] / p.close[f - 252, j] - 1)
    assert fps.scores(p, f, cols, "mom20")[k] == pytest.approx(p.close[f, j] / p.close[f - 20, j] - 1)
    comp = fps.scores(p, f, cols, "composite")
    zm = fps.zscore(fps.scores(p, f, cols, "momentum"))
    zv = fps.zscore(fps.scores(p, f, cols, "lowvol"))
    np.testing.assert_allclose(comp, (zm + zv) / 2)
    p2 = _panel(dates, series, {p.symbols[j]: [dates[f - 10]]})
    assert np.isnan(fps.scores(p2, f, cols, "hi52")[k])               # ex-date inside the 252 sessions


def test_zscore_clips_and_rank_breaks_ties_by_symbol():
    x = np.array([0.0] * 20 + [100.0, np.nan])
    z = fps.zscore(x)
    assert z[20] == 3.0 and np.isnan(z[21])
    order = fps.rank_order(np.array([4, 2, 7, 1]), np.array([1.0, 2.0, 1.0, np.nan]))
    assert order.tolist() == [2, 4, 7]


def test_trend_overlay_sells_everything_and_stays_in_cash():
    dates = _weekdays(date(2024, 1, 1), 230)
    idx = np.concatenate([np.full(215, 20_000.0), np.full(15, 15_000.0)])
    p = _panel(dates, {"NIFTY 50": {"open": idx, "close": idx}, "A": {"open": 100.0, "close": 100.0}})
    assert fps.trend_on(p, 210) is False                               # flat: close == mean is not above
    assert fps.trend_on(p, 150) is True                                # fewer than 200 closes
    sim = fps.simulate(p, [_fm(p, 150, ["A"]), _fm(p, 220, ["A"], invest=False)], n=1, cost=_free,
                       capital=1000.0)
    assert sim.counts["sold_trend_overlay"] == 1 and sim.buys == [1, 0] and sim.cash[229] == pytest.approx(1000.0)


# ----------------------------------------------------------------------------- bootstrap + verdict
def test_bootstrap_is_deterministic_and_calibrated():
    rng = np.random.default_rng(1)
    half = rng.normal(0, 0.02, 20)
    sym = rng.permutation(np.concatenate([half, -half]))
    assert fps.bootstrap_p(sym) == fps.bootstrap_p(sym)
    assert 0.4 < fps.bootstrap_p(sym) < 0.6
    assert fps.bootstrap_p(rng.normal(0.02, 0.02, 40)) < 0.01
    assert fps.bootstrap_p(sym, seed=1) != fps.bootstrap_p(sym, seed=2)
    assert fps.bootstrap_p([]) is None


def test_verdict_labels():
    rng = np.random.default_rng(5)
    good = list(0.02 + rng.uniform(0, 0.01, 40))
    v = fps.verdict(good, [0.01] * 40)
    assert v["label"] == fps.SUPPORTED and all(r["pass"] for r in v["criteria"])
    assert fps.verdict(list(-0.01 + rng.normal(0, 0.001, 40)), [0.01] * 40)["label"] == fps.REFUTED
    assert fps.verdict(good[:20], [0.01] * 20)["label"] == fps.INSUFFICIENT
    skew = [-0.01, 0.05] * 9 + [-0.01, -0.01] + [-0.01, 0.05] * 10     # mean > 0 in both halves, median < 0
    v = fps.verdict(skew, [0.01] * 40)
    assert v["label"] == fps.NULL and [r["id"] for r in v["criteria"] if not r["pass"]] == [3]
    assert fps.verdict(good, [-0.01] * 40)["label"] == fps.NULL        # mean net <= 0 fails criterion 4
    halves = [0.03] * 20 + [-0.001] * 20
    v = fps.verdict(halves, [0.01] * 40)
    assert v["values"]["mean_e_second_half"] < 0 and v["label"] == fps.NULL
    v = fps.verdict(list(range(1, 40)), [0.01] * 39)
    assert v["values"]["mean_e_first_half"] == pytest.approx(np.mean(range(1, 20)))   # 19 earlier, 20 later
    assert v["values"]["mean_e_without_best_3"] == pytest.approx(np.mean(range(1, 37)))


# ----------------------------------------------------------------------------- CLI end to end
def _write_db(path: Path, dates: list[date], series: dict[str, dict], corp: list[tuple]) -> None:
    frame = _frame(dates, series)
    frame["low"] = frame[["open", "close"]].min(axis=1)
    frame["volume"] = frame["volume"].astype("int64")
    frame["src"] = "synthetic"
    con = duckdb.connect(str(path))
    try:
        con.register("f", frame)
        con.execute("CREATE TABLE bars_1d AS SELECT symbol, CAST(d AS DATE) AS d, CAST(open AS DECIMAL(18,4)) AS open, "
                    "CAST(high AS DECIMAL(18,4)) AS high, CAST(low AS DECIMAL(18,4)) AS low, "
                    "CAST(close AS DECIMAL(18,4)) AS close, volume, src FROM f")
        con.execute("CREATE TABLE corp_actions (symbol VARCHAR, ex_date DATE, kind VARCHAR, ratio DOUBLE, "
                    "amount DOUBLE)")
        for row in corp:
            con.execute("INSERT INTO corp_actions VALUES (?, ?, ?, ?, ?)", row)
    finally:
        con.close()


@pytest.fixture(scope="module")
def synthetic_db(tmp_path_factory) -> tuple[Path, Path, list[date]]:
    root = tmp_path_factory.mktemp("fps")
    dates, series = _random_market(420, 40, seed=11)
    series["XBEES"] = {"open": 10.0, "close": 10.0, "volume": 10**8}
    series["NIFTYBEES"] = {"open": 200.0, "close": 200.0}
    for s in series.values():
        s["absent"] = [300]                                            # a session with no stock bars
    db = root / "synthetic.duckdb"
    _write_db(db, dates, series, [("S01", dates[350], "split", 2.0, None), ("S02", dates[340], "dividend", None, 5.0)])
    etf = root / "etf.csv"
    etf.write_text("Symbol,Underlying Asset\nNIFTYBEES,Nifty 50\n", encoding="utf-8")
    return db, etf, dates


def test_counts_only_prints_no_returns(synthetic_db, capsys, monkeypatch):
    db, etf, dates = synthetic_db
    monkeypatch.setattr(fps, "THIN_SESSION_STOCKS", 10)

    def boom(*a, **k):
        raise AssertionError("counts-only computed an outcome")

    for name in ("simulate", "scores", "path_growth", "benchmark", "run_study", "bootstrap_p", "verdict"):
        monkeypatch.setattr(fps, name, boom)
    rc_ = fps.main(["--db", str(db), "--etf-list", str(etf), "--counts-only"], now=datetime(2026, 11, 12, 10, 0,
                                                                                           tzinfo=IST))
    out = capsys.readouterr().out
    assert rc_ == 0
    assert "COUNTS ONLY" in out and "%" not in out
    for word in ("net", "benchmark", "verdict", "mean", "excess", "e="):
        assert word not in out.lower().replace("no returns", "")
    assert f"thin sessions (< 10 stocks with a bar): 1 ['{dates[300]}']" in out
    assert "zeroed_structural" in out and "zeroed_jump" in out
    assert "last formation funnel" in out


def test_full_run_writes_reports(synthetic_db, tmp_path, capsys):
    db, etf, dates = synthetic_db
    out = tmp_path / "fp.json"
    rc_ = fps.main(["--db", str(db), "--etf-list", str(etf), "--out", str(out)], now=AFTER_HOURS)
    assert rc_ == 0
    doc = json.loads(out.read_text(encoding="utf-8"))
    assert out.with_suffix(".md").exists()
    assert set(doc["constructs"]) == set(fps.REGISTERED)
    for c in fps.REGISTERED:
        b = doc["constructs"][c]
        assert b["verdict"]["label"] == fps.INSUFFICIENT
        assert len(b["verdict"]["criteria"]) == 7
        assert b["periods"] and all(p["sample"] == fps.HISTORY for p in b["periods"])
        assert b["verdict"]["values"]["periods"] == len(b["periods"])
        assert b["summary"]["worst_drawdown"] <= 0
    d = doc["diagnostics"]
    assert d["label"] == fps.DECIDES_NOTHING
    assert len(d["runs"]) == 3 * len(fps.DIAG_N) + 3 + 4
    assert "benchmark trend overlay" in d["trend_overlay"]
    assert set(d["random_draw_percentile"]) == set(fps.REGISTERED)
    assert doc["meta"]["db"] == str(db) and doc["meta"]["alpha"] == pytest.approx(0.05 / 3)
    assert "VERDICT INSUFFICIENT" in capsys.readouterr().out


def test_as_of_drops_later_bars(synthetic_db, tmp_path):
    db, etf, dates = synthetic_db
    out = tmp_path / "asof.json"
    assert fps.main(["--db", str(db), "--etf-list", str(etf), "--out", str(out), "--as-of", str(dates[380])],
                    now=AFTER_HOURS) == 0
    doc = json.loads(out.read_text(encoding="utf-8"))
    assert doc["meta"]["sessions"]["last"] == str(dates[380])


def test_full_run_refuses_in_market_hours(synthetic_db, capsys):
    db, etf, _ = synthetic_db
    assert fps.main(["--db", str(db), "--etf-list", str(etf)], now=datetime(2026, 11, 12, 10, 0, tzinfo=IST)) == 2
    assert "market hours" in capsys.readouterr().err
