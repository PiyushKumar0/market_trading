"""``scripts/pool_widening_study.py`` and ``scripts/pool_widening_common.py``: bands, benchmark, walk and verdict.

Every series is synthetic; nothing reads a research snapshot.
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import numpy as np
import pytest

_SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))
_spec = importlib.util.spec_from_file_location("mt_pool_widening_study", _SCRIPTS / "pool_widening_study.py")
pws = importlib.util.module_from_spec(_spec)
sys.modules["mt_pool_widening_study"] = pws
_spec.loader.exec_module(pws)

import pool_widening_common as pw  # noqa: E402
from engine.core.clock import IST  # noqa: E402

rc, hv = pws.rc, pws.hv


def _weekdays(start: date, n: int) -> list[date]:
    out, d = [], start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


DATES = _weekdays(date(2024, 1, 1), 60)


def _series(sym: str, *, price: float = 100.0, vol: float = 1_000_000.0, n: int = 60,
            closes: dict[int, float] | None = None, dates: list[date] | None = None) -> pws.Series:
    o, h, lo, c = (np.full(n, price) for _ in range(4))
    h = h * 1.01
    lo = lo * 0.99
    for i, px in (closes or {}).items():
        c[i] = px
        h[i] = max(h[i], px)
    return pws.Series(sym, (dates or DATES)[:n], o, h, lo, c, np.full(n, vol), [])


@pytest.fixture
def spreads(monkeypatch):
    monkeypatch.setattr(pws, "BAND_SPREAD_PCT", {pw.C0: Decimal("0.04"), pw.W1: Decimal("0.06"),
                                                 pw.W2: Decimal("0.11")})
    monkeypatch.setattr(pws, "BAND_SPREAD_P75_PCT", {pw.C0: Decimal("0.05"), pw.W1: Decimal("0.09"),
                                                     pw.W2: Decimal("0.14")})


# ----------------------------------------------------------------------------- bands
def test_band_edges():
    assert pw.band_of(1, 1.0) == pw.C0
    assert pw.band_of(500, 1.0) == pw.C0
    assert pw.band_of(501, 50_000_000) == pw.W1
    assert pw.band_of(501, 49_999_999.99) == pw.W2
    assert pw.band_of(9999, 20_000_000) == pw.W2
    assert pw.band_of(9999, 19_999_999.99) is None


def test_asof_liquidity_matches_hvol_and_looks_strictly_back():
    rng = np.random.default_rng(7)
    s = _series("A")
    s.close[:] = 100 + rng.normal(0, 3, len(s))
    s.volume[:] = rng.integers(1, 10**6, len(s)).astype(float)
    got = pws.asof_liquidity(s)
    np.testing.assert_array_equal(got, hv.features(s).liquidity)
    assert np.isnan(got[19]) and np.isfinite(got[20])
    tv = np.round(s.close * s.volume, 2)
    assert got[20] == pytest.approx(float(np.median(tv[0:20])))
    assert got[35] == pytest.approx(float(np.median(tv[15:35])))   # sessions 15..34, not 35


def test_band_row_ranks_by_liquidity_with_symbol_tiebreak(monkeypatch):
    monkeypatch.setattr(pw, "CORE_RANK", 1)
    liq = np.array([3e7, 9e7, 9e7, 1e7, np.nan])
    valid = np.isfinite(liq)
    band, rank = pws.band_row(liq, valid)
    assert rank.tolist() == [3, 1, 2, 4, 0]                      # columns 1 and 2 tie; column 1 sorts first
    assert band.tolist() == [pws.BAND_CODE[pw.W2], pws.BAND_CODE[pw.C0], pws.BAND_CODE[pw.W1],
                             pws.NO_BAND, pws.NO_BAND]


def test_grid_bands_only_stocks_trading_that_day(monkeypatch):
    monkeypatch.setattr(pw, "CORE_RANK", 1)
    big = _series("BIG", vol=2_000_000.0)                         # Rs 20 cr a day
    mid = _series("MID", vol=600_000.0)                           # Rs 6 cr
    gap = _series("GAP", vol=600_000.0, n=59, dates=[d for d in DATES if d != DATES[40]])   # no bar on DATES[40]
    g = pws.Grid.build({"BIG": big, "MID": mid, "GAP": gap}, {})
    assert g.band_at("BIG", DATES[19])[0] == pws.NO_BAND          # only 19 prior sessions
    assert g.band_at("BIG", DATES[25])[:2] == (pws.BAND_CODE[pw.C0], 1)
    assert g.band_at("GAP", DATES[25])[:2] == (pws.BAND_CODE[pw.W1], 2)    # ties MID; GAP sorts first
    assert g.band_at("MID", DATES[25])[:2] == (pws.BAND_CODE[pw.W1], 3)
    assert g.band_at("GAP", DATES[40])[0] == pws.NO_BAND                    # no bar that day: not ranked
    assert g.band_at("MID", DATES[40])[1] == 2
    assert g.band_at("GAP", DATES[41])[1] == 2 and g.band_at("MID", DATES[41])[1] == 3


def test_benchmark_is_band_matched_and_drops_ex_dates(monkeypatch):
    monkeypatch.setattr(pw, "CORE_RANK", 3)
    a = _series("A", closes={45: 110.0})
    b = _series("B", closes={45: 100.0})
    c = _series("C", closes={45: 130.0})
    d = _series("D", vol=300_000.0, closes={45: 200.0})          # Rs 3 cr but ranked 4th with CORE_RANK 3 -> W2
    g = pws.Grid.build({"A": a, "B": b, "C": c, "D": d}, {"C": [DATES[30]]})
    mean, n = g.benchmark(pws.BAND_CODE[pw.C0], DATES[25], DATES[26], DATES[45])
    assert n == 2 and mean == pytest.approx(5.0)                 # C has an ex-date inside [26, 45]; D is W2
    mean_w2, n_w2 = g.benchmark(pws.BAND_CODE[pw.W2], DATES[25], DATES[26], DATES[45])
    assert n_w2 == 1 and mean_w2 == pytest.approx(100.0)


# ----------------------------------------------------------------------------- walk
def test_walk_time_only_net_and_excess(monkeypatch, spreads):
    monkeypatch.setattr(pw, "CORE_RANK", 2)
    a = _series("A", closes={45: 110.0})
    b = _series("B", closes={45: 100.0})
    g = pws.Grid.build({"A": a, "B": b}, {})
    ev = rc.Event("A", a, 25, 26, rc.px(a.open[26]), 1.0)
    cand = pws.Candidate("ins", ev, pw.C0, 1, 1e8)
    counts: dict[str, int] = {}
    fees = Decimal("0.30")
    (t,) = pws.walk_all([cand], g, {}, fees, counts)
    assert t.exit_date == DATES[26 + 19]
    assert t.gross == pytest.approx(10.0)
    assert t.net == pytest.approx(10.0 - 0.34)
    assert t.benchmark == pytest.approx(5.0) and t.bench_n == 2
    assert t.excess == pytest.approx(5.0)
    brk = pws.Candidate("brk20", ev, pw.C0, 1, 1e8)
    (t2,) = pws.walk_all([brk], g, {}, fees, counts)
    assert t2.exit_date == DATES[26 + 20]                         # brk20 exits close(fill + 20)


def test_walk_voids_a_structural_ex_date_in_the_hold(spreads, monkeypatch):
    monkeypatch.setattr(pw, "CORE_RANK", 2)
    a = _series("A")
    g = pws.Grid.build({"A": a}, {})
    ev = rc.Event("A", a, 25, 26, rc.px(a.open[26]), 1.0)
    counts: dict[str, int] = {}
    assert pws.walk_all([pws.Candidate("ins", ev, pw.C0, 1, 1e8)], g, {"A": [DATES[30]]}, Decimal("0.3"),
                        counts) == []
    assert counts == {"void_ca": 1}


def test_locked_fill_session_is_unfillable():
    s = _series("A")
    s.high[10] = s.low[10] = 105.0
    counts: dict[str, int] = {}
    assert pws._fillable(s, 10, counts) is False and counts == {"locked_fill_session": 1}
    assert pws._fillable(s, 11, counts) is True


def test_window_gate_counts():
    counts: dict[str, int] = {}
    assert pws._in_window(pws.SIGNALS_FROM - timedelta(days=1), counts) is False
    assert pws._in_window(pws.SIGNALS_FROM, counts) is True
    assert counts == {"before_window": 1, "signals_in_window": 1}


# ----------------------------------------------------------------------------- verdict
PASSING = {
    "n": 150, "median_gross": 1.0, "cost_pct": 0.35, "mean_net": 0.8, "t_net": 2.5,
    "mean_excess": 0.5, "excess_ci_lo": 0.1, "excess_ci_hi": 0.9, "excess_degenerate": False,
    "cpcv_promotable": True, "top1_dropped_mean_net": 0.4, "held_out_mean_excess": 0.2,
}


def test_verdict_widens_only_when_all_seven_pass():
    assert pws.verdict(PASSING)["verdict"] == pws.WIDEN


@pytest.mark.parametrize("change, failed", [
    ({"median_gross": 0.35}, 2),
    ({"t_net": 1.9}, 3),
    ({"mean_net": 0.0}, 3),
    ({"excess_ci_lo": -0.01}, 4),
    ({"excess_degenerate": True}, 4),
    ({"cpcv_promotable": False}, 5),
    ({"top1_dropped_mean_net": -0.1}, 6),
    ({"held_out_mean_excess": None}, 7),
    ({"t_net": float("nan")}, 3),
])
def test_verdict_each_failure_blocks_widening(change, failed):
    v = pws.verdict({**PASSING, **change})
    assert v["verdict"] == pws.NOT_SHOWN
    assert [r["id"] for r in v["rows"] if not r["pass"]] == [failed]


def test_verdict_refuted_and_insufficient():
    assert pws.verdict({**PASSING, "mean_excess": 0.0})["verdict"] == pws.REFUTED
    assert pws.verdict({**PASSING, "mean_excess": -0.3, "excess_ci_lo": -0.5})["verdict"] == pws.REFUTED
    assert pws.verdict({**PASSING, "n": 99})["verdict"] == pws.INSUFFICIENT
    assert pws.verdict({"n": 0})["verdict"] == pws.INSUFFICIENT


def test_measure_cell_on_no_trades_is_insufficient(spreads):
    m = pws.measure_cell([], 0.34)
    assert m["n"] == 0 and m["mean_net"] is None and m["cpcv_promotable"] is False
    assert pws.verdict(m)["verdict"] == pws.INSUFFICIENT


# ----------------------------------------------------------------------------- refusals
def test_run_refuses_without_recorded_spreads(monkeypatch):
    monkeypatch.setattr(pws, "BAND_SPREAD_PCT", {})
    with pytest.raises(rc.Refusal, match="band spreads"):
        pws.run_study(None, db_path=Path("x.duckdb"), etf=None)


def test_main_refuses_in_market_hours(tmp_path, capsys):
    rc_ = pws.main(["--db", str(tmp_path / "x.duckdb")], now=datetime(2026, 11, 11, 10, 0, tzinfo=IST))
    assert rc_ == 2
    assert "market hours" in capsys.readouterr().err
