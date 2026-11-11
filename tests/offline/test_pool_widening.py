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


def test_registered_constants():
    assert pws.BAND_SPREAD_PCT == {pw.C0: Decimal("0.04"), pw.W1: Decimal("0.07"), pw.W2: Decimal("0.11")}
    assert pws.BAND_SPREAD_P75_PCT == {pw.C0: Decimal("0.05"), pw.W1: Decimal("0.09"), pw.W2: Decimal("0.14")}
    assert pws.INS_FLOOR_INR == 10_000_000
    assert pws.FILL_WINDOW_SESSIONS == 5
    assert pws.EXIT_SESSIONS == {"brk20": 20, "ins": 19}
    assert pws.LEGS == ("brk20", "ins") and pws.FAMILY_N == 4
    assert pws.STRUCTURAL_SUPPLEMENT == {"AMIORG": (date(2025, 4, 25),)}


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


def test_benchmark_runs_open_entry_to_close_exit_with_the_band_of_y(monkeypatch):
    monkeypatch.setattr(pw, "CORE_RANK", 3)
    a = _series("A", closes={45: 110.0})
    a.open[26] = 50.0                                             # entry open differs from every close
    b = _series("B")
    late = _series("L", n=59, dates=[d for d in DATES if d != DATES[25]])   # no bar on y: not banded that day
    late.close[44] = 300.0                                        # its DATES[45] bar
    g = pws.Grid.build({"A": a, "B": b, "L": late}, {})
    assert g.band_at("L", DATES[26])[0] == pws.BAND_CODE[pw.C0]   # banded on the entry day, not on y
    mean, n = g.benchmark(pws.BAND_CODE[pw.C0], DATES[25], DATES[26], DATES[45])
    assert n == 2 and mean == pytest.approx((120.0 + 0.0) / 2)    # A: 110 / open 50; close(entry) would give 10


# ----------------------------------------------------------------------------- market sessions
def _gappy(sym: str, missing: set[int], **kw) -> pws.Series:
    keep = [d for i, d in enumerate(DATES) if i not in missing]
    return _series(sym, n=len(keep), dates=keep, **kw)


def _sig(s: pws.Series, idx: int, level: float):
    return pws.bb.Signal(s.symbol, idx, s.dates[idx], level, level, 0.01, 0.01)


def test_limit_fill_matches_fill_for_without_gaps_and_counts_market_sessions_with_them():
    full = _series("F")
    full.low[:] = 100.0
    full.low[31] = 95.0                                           # touch on y + 6 only
    g = pws.Grid.build({"F": full}, {})
    for level, want in ((96.0, None), (99.5, None)):
        assert pws.limit_fill(g, full, _sig(full, 25, level)) == pws.bb.fill_for(full, _sig(full, 25, level),
                                                                                  pws.bb.VARIANT_LIMIT_5) == want
    full.low[30] = 95.0                                           # touch on y + 5: fills
    assert pws.limit_fill(g, full, _sig(full, 25, 96.0)) == (30, 96.0)
    assert pws.bb.fill_for(full, _sig(full, 25, 96.0), pws.bb.VARIANT_LIMIT_5) == (30, 96.0)
    gap = _gappy("G", {27, 28})                                   # out of EQ on y + 2 and y + 3
    gap.low[:] = 100.0
    gap.low[28] = 95.0                                            # own index 28 is DATES[30], y + 5: fills
    gap.low[29] = 95.0                                            # DATES[31], y + 6
    g2 = pws.Grid.build({"F": full, "G": gap}, {})
    assert pws.limit_fill(g2, gap, _sig(gap, 25, 96.0)) == (28, 96.0)
    gap.low[28] = 100.0
    assert pws.limit_fill(g2, gap, _sig(gap, 25, 96.0)) is None   # own bars would still be inside 5
    assert pws.bb.fill_for(gap, _sig(gap, 25, 96.0), pws.bb.VARIANT_LIMIT_5) == (29, 96.0)


def test_exit_bar_counts_market_sessions_and_moves_late():
    full = _series("F")
    gap = _gappy("G", {45, 46, 47})                               # out of EQ DATES[45..47]
    g = pws.Grid.build({"F": full, "G": gap}, {})
    assert pws.exit_bar(g, full, 26, 19) == (45, 0)
    assert pws.exit_bar(g, gap, 26, 10) == (36, 0)                # before the hole: own index = grid index
    x, late = pws.exit_bar(g, gap, 26, 19)                        # due DATES[45]; next own bar is DATES[48]
    assert gap.dates[x] == DATES[48] and late == 3
    short = _series("S", n=40)
    g3 = pws.Grid.build({"F": full, "S": short}, {})
    assert pws.exit_bar(g3, short, 26, 19) is None                # no bar from DATES[45] on


class _Store:
    def get_insider_trades(self, symbol):
        return []


def test_ins_events_use_full_history_and_need_a_bar_on_the_next_session(monkeypatch):
    monkeypatch.setattr(pw, "CORE_RANK", 5)
    monkeypatch.setattr(pws.rc, "store_reader", lambda conn: _Store())
    seen: list[int] = []

    def events(sessions, filings, threshold):
        seen.append(len(sessions))
        assert threshold == pws.INS_FLOOR_INR
        return [25]

    monkeypatch.setattr(pws.es, "insider_buy_events", events)
    full = _series("F")
    gap = _gappy("G", {26})                                       # own index 25 is DATES[25]; next bar DATES[27]
    g = pws.Grid.build({"F": full, "G": gap}, {})
    cands, counts = pws.ins_candidates(None, {"F": full, "G": gap}, g, {})
    assert seen == [60, 59]                                       # every session of each stock, not a cut window
    (c,) = cands
    assert c.event.symbol == "F" and c.event.fill_idx == 26 and float(c.event.fill_px) == full.open[26]
    assert c.exit_date == DATES[45] and c.band == pw.C0
    assert counts == {"signals_in_window": 2, "no_fill_session": 1}


def _cand(s: pws.Series, sig: int, exit_idx: int, leg: str = "ins") -> pws.Candidate:
    return pws.Candidate(leg, rc.Event(s.symbol, s, sig, sig + 1, rc.px(100.0), 1.0), pw.C0, 1, 1e8,
                         exit_idx=exit_idx)


def test_one_position_per_symbol_blocks_until_the_exit_date():
    a, b = _series("A"), _series("B")
    first, inside, on_exit, after = _cand(a, 5, 25), _cand(a, 20, 40), _cand(a, 25, 45), _cand(a, 30, 50)
    other = _cand(b, 20, 40)
    counts: dict[str, int] = {}
    kept = pws.one_position_per_symbol([after, inside, other, on_exit, first], counts)
    assert kept == [first, on_exit, other]                        # a dropped signal never extends the block
    assert counts == {"held_overlap": 2}


def test_voided_matches_the_walk():
    a = _series("A")
    c = _cand(a, 25, 45)
    assert pws.voided(c, {"A": [DATES[45]]}) and not pws.voided(c, {"A": [DATES[26]]})
    assert not pws.voided(c, {"A": [DATES[46]]}) and not pws.voided(c, {})


# ----------------------------------------------------------------------------- walk
def test_walk_time_only_net_and_excess(monkeypatch):
    monkeypatch.setattr(pw, "CORE_RANK", 2)
    a = _series("A", closes={45: 110.0})
    b = _series("B", closes={45: 100.0})
    g = pws.Grid.build({"A": a, "B": b}, {})
    ev = rc.Event("A", a, 25, 26, rc.px(a.open[26]), 1.0)
    x, _ = pws.exit_bar(g, a, 26, pws.EXIT_SESSIONS["ins"])
    cand = pws.Candidate("ins", ev, pw.C0, 1, 1e8, exit_idx=x)
    counts: dict[str, int] = {}
    fees = Decimal("0.30")
    (t,) = pws.walk_all([cand], g, {}, fees, counts)
    assert t.exit_date == DATES[26 + 19]
    assert t.gross == pytest.approx(10.0)
    assert t.net == pytest.approx(10.0 - 0.34)
    assert t.benchmark == pytest.approx(5.0) and t.bench_n == 2
    assert t.excess == pytest.approx(5.0)
    x2, _ = pws.exit_bar(g, a, 26, pws.EXIT_SESSIONS["brk20"])
    (t2,) = pws.walk_all([pws.Candidate("brk20", ev, pw.C0, 1, 1e8, exit_idx=x2)], g, {}, fees, counts)
    assert t2.exit_date == DATES[26 + 20]                         # brk20 exits close(fill + 20)


def test_walk_exits_at_the_late_bar(monkeypatch):
    monkeypatch.setattr(pw, "CORE_RANK", 2)
    full = _series("F")
    gap = _gappy("G", {45, 46})                                   # own index 45 is DATES[47]
    g = pws.Grid.build({"F": full, "G": gap}, {})
    x, late = pws.exit_bar(g, gap, 26, 19)
    ev = rc.Event("G", gap, 25, 26, rc.px(gap.open[26]), 1.0)
    (t,) = pws.walk_all([pws.Candidate("ins", ev, pw.W1, 2, 1e8, exit_idx=x, exit_late=late)], g, {},
                        Decimal("0.30"), {})
    assert t.exit_date == DATES[47] and t.exit_late == 2 and t.gross == pytest.approx(0.0)
    gap.close[45] = 120.0
    (t,) = pws.walk_all([pws.Candidate("ins", ev, pw.W1, 2, 1e8, exit_idx=x, exit_late=late)], g, {},
                        Decimal("0.30"), {})
    assert t.gross == pytest.approx(20.0)


def test_walk_voids_a_structural_ex_date_in_the_hold(monkeypatch):
    monkeypatch.setattr(pw, "CORE_RANK", 2)
    a = _series("A")
    g = pws.Grid.build({"A": a}, {})
    ev = rc.Event("A", a, 25, 26, rc.px(a.open[26]), 1.0)
    counts: dict[str, int] = {}
    assert pws.walk_all([pws.Candidate("ins", ev, pw.C0, 1, 1e8, exit_idx=45)], g, {"A": [DATES[30]]},
                        Decimal("0.3"), counts) == []
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
    # too few trades outranks a negative excess
    assert pws.verdict({**PASSING, "n": 99, "mean_excess": -0.3, "excess_ci_lo": -0.5})["verdict"] == pws.INSUFFICIENT


def test_verdict_n_boundary():
    assert pws.verdict({**PASSING, "n": 100})["verdict"] == pws.WIDEN
    assert [r["pass"] for r in pws.verdict({**PASSING, "n": 99})["rows"]][0] is False


def test_measure_cell_on_no_trades_is_insufficient():
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
