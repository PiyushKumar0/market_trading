"""scripts/backtest_hvol.py - the pre-registered hvol (abnormal-volume swing drift) harness.

Synthetic series only (no snapshot): a flat 100.00 / 1,000,000-share base where index ``Y`` = 70 is
the one engineered spike (VR exactly 3.0). Each signal condition is flipped in isolation against
that base, the lookahead pins perturb every bar on or after ``Y + 1``, and the benchmark,
bootstrap and promotion table are checked on hand-built inputs whose answers are known.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from dataclasses import replace
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import numpy as np
import pytest

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "backtest_hvol.py"
_spec = importlib.util.spec_from_file_location("mt_backtest_hvol", _SCRIPT)
hv = importlib.util.module_from_spec(_spec)
sys.modules["mt_backtest_hvol"] = hv
_spec.loader.exec_module(hv)

from engine.core.clock import IST  # noqa: E402
from engine.marketdata.store import DailyBar, MarketStore  # noqa: E402

N = 100
Y = 70
BASE_VOL = 1_000_000.0
OUTSIDE_HOURS = datetime(2026, 10, 7, 20, 0, tzinfo=IST)


def _weekdays(start: date, n: int) -> list[date]:
    out, d = [], start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


DATES = _weekdays(date(2024, 1, 1), 140)


def _series(n: int = N, *, price: float = 100.0, vol: float = BASE_VOL, sym: str = "SYM",
            spike_at: int | None = Y, spike_mult: float = 3.0) -> hv.Series:
    o, h, lo, c = (np.full(n, price) for _ in range(4))
    v = np.full(n, vol)
    if spike_at is not None:
        v[spike_at] = vol * spike_mult
    return hv.Series(sym, DATES[:n], o, h, lo, c, v, [])


def _clone(s: hv.Series) -> hv.Series:
    return hv.Series(s.symbol, list(s.dates), s.open.copy(), s.high.copy(), s.low.copy(),
                     s.close.copy(), s.volume.copy(), [])


def _fires(s: hv.Series, y: int = Y, structural=(), rule=None) -> bool:
    return bool(hv.signal_mask(s, structural, rule or hv.PRIMARY)[y])


# ----------------------------------------------------------------------------- base case
def test_base_case_fires_only_on_the_spike():
    s = _series()
    assert np.flatnonzero(hv.signal_mask(s, ())).tolist() == [Y]


# ----------------------------------------------------------------------------- lookahead pins
@pytest.mark.parametrize("field", ["open", "high", "low", "close", "volume"])
@pytest.mark.parametrize("mult", [0.0, 0.5, 10.0])
@pytest.mark.parametrize("spike_mult, expected", [(3.0, True), (2.99, False)])
def test_bars_on_or_after_y_plus_1_never_change_y(field, mult, spike_mult, expected):
    base = _series(spike_mult=spike_mult)
    assert _fires(base) is expected
    for j in range(Y + 1, N):
        s = _clone(base)
        getattr(s, field)[j] *= mult
        assert _fires(s) is expected, (field, j, mult)
    s = _clone(base)
    for arr in (s.open, s.high, s.low, s.close, s.volume):
        arr[Y + 1:] *= mult
    assert _fires(s) is expected


def test_truncating_the_future_does_not_change_y_without_ex_dates():
    s = _series()
    cut = hv.Series(s.symbol, s.dates[:Y + 1], s.open[:Y + 1], s.high[:Y + 1], s.low[:Y + 1],
                    s.close[:Y + 1], s.volume[:Y + 1], [])
    assert _fires(cut) is True


def test_volume_on_y_changes_y():
    s = _series()
    s.volume[Y] = BASE_VOL
    assert _fires(s) is False


def test_close_on_y_changes_y():
    s = _series()
    s.high[Y] = s.close[Y] = 100.01      # above max(high, y-20..y-1) = 100.00: a breakout
    assert _fires(s) is False


def test_baseline_median_excludes_y():
    # y-50 = 1e6; y-49..y-1 = 24 x 1e6 + 25 x 2e6 -> strict median 1.5e6, VR(y) = 4.5e6/1.5e6 = 3.0.
    # A window that included y (y-49..y) would read median 2e6 and VR 2.25.
    s = _series(spike_at=None)
    window = list(range(Y - 49, Y))
    for j in window[::2][:25]:
        s.volume[j] = 2_000_000.0
    assert sum(s.volume[Y - 50:Y] == 2_000_000.0) == 25 and s.volume[Y - 50] == BASE_VOL
    s.volume[Y] = 4_500_000.0
    assert _fires(s) is True


def test_liquidity_floor_excludes_y():
    # Traded value: y-20 low, y-19..y-1 = 9 high + 10 low -> strict median = low (4e7, illiquid).
    # A window y-19..y would add the spike day's huge value and read the 5e7 midpoint (liquid).
    s = _series(vol=400_000.0, spike_at=None)
    for j in range(Y - 19, Y - 10):
        s.volume[j] = 600_000.0
    s.volume[Y] = 1_500_000.0
    assert _fires(s) is False
    s.volume[Y - 20] = 600_000.0            # the strict window is now 10 high + 10 low: 5e7, liquid
    assert _fires(s) is True


# ----------------------------------------------------------------------------- each condition flips
@pytest.mark.parametrize("mult, expected", [(2.99, False), (3.0, True)])
def test_vr_threshold(mult, expected):
    assert _fires(_series(spike_mult=mult)) is expected


def test_breakout_blocks_primary_but_not_the_breakout_inclusive_variant():
    s = _series()
    s.high[Y] = s.close[Y] = 100.01
    assert _fires(s) is False
    assert _fires(s, rule=hv.BREAKOUT_INCLUSIVE) is True
    s.high[Y] = 100.01
    s.close[Y] = 100.00                     # close == max prior high: NOT a breakout (<=)
    assert _fires(s) is True


@pytest.mark.parametrize("earlier, expected", [(Y - 20, False), (Y - 21, True), (Y - 1, False)])
def test_freshness(earlier, expected):
    s = _series()
    s.volume[earlier] = 3 * BASE_VOL
    assert _fires(s) is expected


def test_freshness_counts_breakout_spikes_too():
    s = _series()
    s.volume[Y - 5] = 3 * BASE_VOL
    s.high[Y - 5] = s.close[Y - 5] = 101.0  # the earlier spike was a breakout: still blocks
    s.high[Y] = 101.0                       # keep y itself a non-breakout
    assert _fires(s) is False


@pytest.mark.parametrize("offset, expected", [(-50, False), (-51, True), (21, False), (22, True),
                                              (0, False), (1, False)])
def test_structural_ex_date_window(offset, expected):
    assert _fires(_series(), structural=[DATES[Y + offset]]) is expected


def test_structural_window_end_falls_back_to_unbounded_when_y_plus_21_is_missing():
    s = _series(n=Y + 21)                    # last index Y+20: session y+21 does not exist
    assert _fires(s, structural=[DATES[Y + 30]]) is False
    assert _fires(s, structural=[DATES[Y - 51]]) is True


@pytest.mark.parametrize("spike_at, expected", [(59, False), (60, True)])
def test_needs_60_prior_sessions(spike_at, expected):
    s = _series(spike_at=spike_at)
    assert _fires(s, y=spike_at) is expected


@pytest.mark.parametrize("zeros, expected", [(11, False), (10, True)])
def test_needs_40_positive_baseline_sessions(zeros, expected):
    s = _series()
    s.volume[Y - 50:Y - 50 + zeros] = 0.0   # all inside y-50..y-21: liquidity window untouched
    assert _fires(s) is expected


@pytest.mark.parametrize("close_y, expected", [(9.99, False), (10.0, True)])
def test_min_close(close_y, expected):
    s = _series(price=10.0, vol=10_000_000.0)
    s.close[Y] = close_y
    assert _fires(s) is expected


@pytest.mark.parametrize("vol, expected", [(499_999.0, False), (500_000.0, True)])
def test_liquidity_floor(vol, expected):
    assert _fires(_series(vol=vol)) is expected     # 100.00 x 500,000 = Rs 5cr exactly


def test_variant_thresholds():
    assert _fires(_series(spike_mult=2.0), rule=hv.VR_2) is True
    assert _fires(_series(spike_mult=4.99), rule=hv.VR_5) is False
    assert _fires(_series(spike_mult=5.0), rule=hv.VR_5) is True
    s = _series(spike_mult=2.0)
    s.volume[Y - 10] = 2 * BASE_VOL                 # the variant's spike defines its own cluster
    assert _fires(s, rule=hv.VR_2) is False


def test_primary_rule_is_the_registered_one():
    assert hv.PRIMARY == hv.Rule("primary", 3.0, 3.0, True)
    assert hv.RULES == (hv.PRIMARY, hv.VR_2, hv.VR_5, hv.BREAKOUT_INCLUSIVE)
    assert (hv.BASELINE_SESSIONS, hv.MIN_POSITIVE_BASELINE, hv.MIN_PRIOR_SESSIONS) == (50, 40, 60)
    assert (hv.LIQUIDITY_SESSIONS, hv.LIQUIDITY_FLOOR_INR, hv.MIN_CLOSE_INR) == (20, 50_000_000.0, 10.0)
    assert (hv.BOOTSTRAP_REPS, hv.BOOTSTRAP_SEED, hv.TRIAL_COUNT_N) == (2000, 20261009, 1)
    assert hv.HORIZONS == (5, 10, 20) and hv.WORST_BOOK_NOTIONAL == Decimal("6000")


# ----------------------------------------------------------------------------- returns
def test_returns_close_k_over_open_t_plus_1():
    s = _series(n=N)
    s.open[Y + 1] = 100.0
    s.close[Y + 5], s.close[Y + 10], s.close[Y + 20] = 105.0, 110.0, 80.0
    s.close[Y + 1] = 999.0                   # never read: entry is the OPEN of y+1
    r = hv.event_returns(s, Y)
    assert r == pytest.approx({5: 5.0, 10: 10.0, 20: -20.0})


@pytest.mark.parametrize("n, measured", [(Y + 20, False), (Y + 21, True)])
def test_event_short_of_t_plus_20_is_dropped_whole(n, measured):
    assert (hv.event_returns(_series(n=n), Y) is not None) is measured


# ----------------------------------------------------------------------------- benchmark
def _bench_series(sym: str, *, entry: float, exit_: float, vol: float = BASE_VOL,
                  drop: int | None = None) -> hv.Series:
    s = _series(sym=sym, spike_at=None, vol=vol)
    s.open[Y + 1] = entry
    s.close[Y + 5] = exit_
    if drop is not None:
        keep = [i for i in range(N) if i != drop]
        s = hv.Series(sym, [s.dates[i] for i in keep], s.open[keep], s.high[keep], s.low[keep],
                      s.close[keep], s.volume[keep], [])
    return s


def test_benchmark_is_the_equal_weight_mean_of_eligible_names():
    series = {
        "A": _bench_series("A", entry=100.0, exit_=110.0),     # +10
        "B": _bench_series("B", entry=50.0, exit_=48.0),       # -4
        "C": _bench_series("C", entry=200.0, exit_=200.0),     # 0
        "D": _bench_series("D", entry=100.0, exit_=150.0),     # structural ex-date in window
        "E": _bench_series("E", entry=100.0, exit_=200.0, vol=100.0),   # illiquid
        "F": _bench_series("F", entry=100.0, exit_=300.0, drop=Y + 5),  # no bar on the exit date
        "G": _bench_series("G", entry=100.0, exit_=400.0, vol=100.0),   # liquid as of y+1, not as of y
        "H": _bench_series("H", entry=100.0, exit_=500.0),     # ex-date ON the entry date
    }
    # G: 10 high-value days y-9 .. y. As of y (y-20 .. y-1: 9 high, 11 low) the median is low;
    # as of y+1 (y-19 .. y: 10 and 10) it is (1e4 + 1e8) / 2 >= Rs 5cr. The floor is read ON y.
    series["G"].volume[Y - 9:] = BASE_VOL
    structural = {"D": [DATES[Y + 3]], "H": [DATES[Y + 1]]}
    cs = hv.CrossSection.build(series, structural)
    g = cs.symbols.index("G")
    assert cs.liquidity[cs.pos[DATES[Y]], g] < hv.LIQUIDITY_FLOOR_INR <= cs.liquidity[cs.pos[DATES[Y + 1]], g]
    mean, n = cs.benchmark(DATES[Y], DATES[Y + 1], DATES[Y + 5])
    assert n == 3
    assert mean == pytest.approx(2.0)


# ----------------------------------------------------------------------------- bootstrap
def test_bootstrap_is_deterministic_under_the_seed():
    rng = np.random.default_rng(1)
    dates = [DATES[i % 30] for i in range(300)]
    cols = {"net": rng.normal(0.5, 3, 300).tolist(), "excess": rng.normal(0.1, 3, 300).tolist()}
    a = hv.clustered_bootstrap(dates, cols)
    b = hv.clustered_bootstrap(dates, cols)
    assert a == b
    assert a["net"]["mean"] == pytest.approx(float(np.mean(cols["net"])))
    assert a["net"]["n_clusters"] == 30 and a["net"]["degenerate"] is False
    assert a["net"]["ci_lo"] < a["net"]["mean"] < a["net"]["ci_hi"]
    assert a["net"]["t"] == pytest.approx(a["net"]["mean"] / a["net"]["se"])
    assert hv.clustered_bootstrap(dates, cols, seed=7)["net"]["se"] != a["net"]["se"]


def test_bootstrap_single_date_is_degenerate_not_a_crash():
    out = hv.clustered_bootstrap([DATES[0]] * 5, {"net": [1.0, 2.0, 3.0, 4.0, 5.0]})["net"]
    assert out["degenerate"] is True and out["t"] is None
    assert out["ci_lo"] == pytest.approx(3.0) and out["ci_hi"] == pytest.approx(3.0)


def test_bootstrap_empty():
    out = hv.clustered_bootstrap([], {"net": []})["net"]
    assert out["mean"] is None and out["t"] is None and out["degenerate"] is True


def test_top_tail_dropped_mean():
    vals = [float(v) for v in range(1, 201)]          # 200 values: ceil(1%) = 2 dropped (200, 199)
    assert hv.top_tail_dropped_mean(vals) == pytest.approx(np.mean(vals[:198]))
    assert hv.top_tail_dropped_mean([5.0, -1.0]) == pytest.approx(-1.0)   # ceil: 1 of 2 dropped
    assert hv.top_tail_dropped_mean([5.0]) is None
    assert hv.top_tail_dropped_mean([]) is None


# ----------------------------------------------------------------------------- promotion table
PASSING = {
    "n": 250, "median_gross": 1.0, "cost_pct": 0.4, "mean_net": 0.8, "t_net": 2.5,
    "mean_excess": 0.5, "excess_ci_lo": 0.1, "excess_ci_hi": 0.9, "excess_degenerate": False,
    "cpcv_promotable": True, "top1_dropped_mean_net": 0.3,
    "deployable_mean_net": 0.2, "deployable_mean_excess": 0.1, "held_out_mean_excess": 0.05,
}


def test_promotion_all_pass():
    t = hv.promotion_table(PASSING)
    assert [r["id"] for r in t["rows"]] == [1, 2, 3, 4, 5, 6, 7, 8]
    assert all(r["pass"] for r in t["rows"])
    assert t["verdict"] == hv.VERDICT_PROMOTABLE == "PROMOTABLE (candidate only, §8.6 applies)"


@pytest.mark.parametrize("crit, overrides", [
    (1, {"n": 199}),
    (2, {"median_gross": 0.4}),
    (3, {"mean_net": -0.1}),
    (3, {"t_net": 2.0}),
    (3, {"t_net": None}),
    (4, {"mean_excess": 0.0}),
    (4, {"excess_ci_lo": -0.01}),
    (4, {"excess_degenerate": True}),
    (5, {"cpcv_promotable": False}),
    (6, {"top1_dropped_mean_net": 0.0}),
    (7, {"deployable_mean_net": -0.01}),
    (7, {"deployable_mean_excess": None}),
    (8, {"held_out_mean_excess": 0.0}),
])
def test_promotion_each_criterion_fails_alone(crit, overrides):
    t = hv.promotion_table({**PASSING, **overrides})
    assert [r["id"] for r in t["rows"] if not r["pass"]] == [crit]
    assert t["verdict"] == hv.VERDICT_REFUTED == "REFUTED"


# ----------------------------------------------------------------------------- ETFs (Amendment A1)
ETF_HEADER = "Symbol,Underlying Asset,SecurityName,DateofListing,MarketLot,ISINNumber,FaceValue,ETF Underlying,Underlying Key"


def _etf_csv(tmp_path: Path, symbols=("SLVR",), name: str = "etf.csv") -> Path:
    p = tmp_path / name
    rows = [f" {s.lower()} ,Silver,X,01-Jan-20,1,INF000000000,1,COMMODITY,Silver" for s in symbols]
    p.write_text("\n".join([ETF_HEADER, *rows]) + "\n", encoding="utf-8")
    return p


def test_is_etf_list_and_pattern():
    etf = {"SLVR"}
    assert hv.is_etf("SLVR", etf) and hv.is_etf(" slvr ", etf)
    for sym in ("GOLDBEES", "NIFTYIETF", "MAFANGETF", "goldbees"):
        assert hv.is_etf(sym, set()), sym
    for sym in ("RELIANCE", "BEESX", "ETFX", "BETA", "NIFTY 50"):
        assert not hv.is_etf(sym, etf), sym


def test_load_etf_list_strips_uppercases_and_hashes(tmp_path):
    import hashlib
    p = _etf_csv(tmp_path, ("SLVR", "goldx"))
    lst = hv.load_etf_list(p)
    assert lst.symbols == frozenset({"SLVR", "GOLDX"})
    assert lst.sha256 == hashlib.sha256(p.read_bytes()).hexdigest()


@pytest.mark.parametrize("content", [None, "", "Name,Underlying\nX,Y\n", ETF_HEADER + "\n"])
def test_load_etf_list_refuses_missing_or_unparseable(tmp_path, content):
    p = tmp_path / "etf.csv"
    if content is not None:
        p.write_text(content, encoding="utf-8")
    with pytest.raises(hv.rc.Refusal, match="ETF list"):
        hv.load_etf_list(p)


def test_missing_etf_list_refuses_with_exit_2(tmp_path, capsys):
    rc = hv.main(["--db", str(tmp_path / "market_x.duckdb"), "--out-dir", str(tmp_path / "out"),
                  "--etf-list", str(tmp_path / "nope.csv")], now=OUTSIDE_HOURS)
    assert rc == 2 and "ETF list" in capsys.readouterr().err
    assert not (tmp_path / "out").exists()


def test_etfs_are_absent_from_events_and_from_the_benchmark(tmp_path):
    def spiked(sym: str, exit_: float) -> hv.Series:
        s = _series(sym=sym)                      # spikes at Y: a signal if it were in the population
        s.open[Y + 1] = 100.0
        s.close[Y + 5] = exit_
        return s

    series = {"A": spiked("A", 110.0), "B": spiked("B", 96.0), "C": spiked("C", 100.0),
              "SLVR": spiked("SLVR", 200.0), "GOLDBEES": spiked("GOLDBEES", 300.0)}
    etf = hv.load_etf_list(_etf_csv(tmp_path))
    with_etfs = hv.CrossSection.build(series, {})
    assert with_etfs.benchmark(DATES[Y], DATES[Y + 1], DATES[Y + 5]) == (pytest.approx(61.2), 5)
    kept, counts = hv.exclude_etfs(series, etf)
    assert sorted(kept) == ["A", "B", "C"] and counts == {"via_list": 1, "via_pattern_only": 1}
    cs = hv.CrossSection.build(kept, {})
    assert cs.benchmark(DATES[Y], DATES[Y + 1], DATES[Y + 5]) == (pytest.approx(2.0), 3)
    assert "SLVR" not in cs.symbols and "GOLDBEES" not in cs.symbols


# ----------------------------------------------------------------------------- NIFTY 500 source (A1)
def _membership_root(tmp_path: Path, core: str | None) -> Path:
    (tmp_path / "config" / "universe").mkdir(parents=True)
    (tmp_path / "config" / "universe" / "nifty500_seed.csv").write_text("Symbol\nSEEDA\n", encoding="utf-8")
    (tmp_path / "config" / "universe" / "niftytotalmarket_seed.csv").write_text("Symbol\nSEEDA\nTMB\n",
                                                                                encoding="utf-8")
    if core is not None:
        (tmp_path / "data" / "universe").mkdir(parents=True)
        (tmp_path / "data" / "universe" / "core_index_cached.csv").write_text(core, encoding="utf-8")
    return tmp_path


@pytest.mark.parametrize("core, expected, source", [
    ("Company Name,Symbol,Series\nX,COREA,EQ\n", {"COREA"}, "core_index_cached.csv"),
    (None, {"SEEDA"}, "nifty500_seed.csv"),               # absent -> seed
    ("Symbol\n", {"SEEDA"}, "nifty500_seed.csv"),         # parses empty -> seed
    ("Name\nCOREA\n", {"SEEDA"}, "nifty500_seed.csv"),    # unparseable (no Symbol column) -> seed
])
def test_nifty500_reads_the_core_cache_then_falls_back_to_the_seed(tmp_path, core, expected, source):
    n500, tm, seeds = hv.load_membership(_membership_root(tmp_path, core))
    assert n500 == expected and tm == {"SEEDA", "TMB"}
    assert seeds["nifty500"].endswith(source) and seeds["total_market"].endswith("niftytotalmarket_seed.csv")


# ----------------------------------------------------------------------------- refusals
def test_live_store_is_refused(tmp_path, capsys):
    rc = hv.main(["--db", str(tmp_path / "market.duckdb"), "--out-dir", str(tmp_path / "out"),
                  "--etf-list", str(_etf_csv(tmp_path))], now=OUTSIDE_HOURS)
    assert rc == 2 and "live store" in capsys.readouterr().err
    assert not (tmp_path / "out").exists()


@pytest.mark.parametrize("now, refused", [
    (datetime(2026, 10, 7, 10, 0, tzinfo=IST), True),
    (datetime(2026, 10, 7, 9, 14, tzinfo=IST), False),
    (datetime(2026, 10, 7, 15, 30, tzinfo=IST), False),
    (datetime(2026, 10, 2, 10, 0, tzinfo=IST), False),     # Gandhi Jayanti holiday
])
def test_market_hours_refusal(tmp_path, capsys, now, refused):
    # A missing snapshot: outside market hours the run reaches open_snapshot and refuses THAT.
    rc = hv.main(["--db", str(tmp_path / "market_x.duckdb"), "--out-dir", str(tmp_path / "out"),
                  "--etf-list", str(_etf_csv(tmp_path))], now=now)
    err = capsys.readouterr().err
    assert rc == 2
    assert ("market hours" in err) is refused
    assert not (tmp_path / "out").exists()


# ----------------------------------------------------------------------------- end to end
def _bars(sym: str, s: hv.Series, src_at: dict[int, str] | None = None) -> list[DailyBar]:
    src_at = src_at or {}
    return [DailyBar(symbol=sym, d=s.dates[i], open=Decimal(f"{s.open[i]:.2f}"), high=Decimal(f"{s.high[i]:.2f}"),
                     low=Decimal(f"{s.low[i]:.2f}"), close=Decimal(f"{s.close[i]:.2f}"), volume=int(s.volume[i]),
                     src=src_at.get(i, "bhavcopy")) for i in range(len(s))]


def test_end_to_end_on_a_synthetic_snapshot(tmp_path, clock, capsys):
    spk = _series(sym="SPK")
    spk.open[Y + 1] = 100.0
    spk.high[Y + 20] = spk.close[Y + 20] = 102.0         # T+20 gross +2.00%
    flat = _series(sym="FLT", spike_at=None)
    split = _series(sym="SPL")                             # same spike, vetoed by a split ex-date
    etfs = []
    for sym in ("SLVR", "GOLDBEES"):                       # on the list / pattern only: spike, +100% at T+20
        e = _series(sym=sym)
        e.high[Y + 20] = e.close[Y + 20] = 200.0
        etfs += _bars(sym, e)
    db = tmp_path / "research" / "market_2024-05-20.duckdb"
    store = MarketStore(db, tmp_path / "parquet", clock).open()
    try:
        store.upsert_bars_1d(_bars("SPK", spk, {Y: "kite_official"}) + _bars("FLT", flat)
                             + _bars("SPL", split) + _bars("NIFTY 50", _series(sym="NIFTY 50")) + etfs)
        store.upsert_corp_actions([{"symbol": "SPL", "ex_date": DATES[Y + 10], "kind": "split"}])
        store.upsert_earnings_calendar([
            {"symbol": "FLT", "event_date": DATES[5], "kind": "results"},
            {"symbol": "SPK", "event_date": DATES[Y + 1], "kind": "results"},
        ])
    finally:
        store.close()
    out = tmp_path / "reports"
    etf_csv = _etf_csv(tmp_path)
    assert hv.main(["--db", str(db), "--out-dir", str(out), "--etf-list", str(etf_csv)], now=OUTSIDE_HOURS) == 0
    stamp = datetime.now(IST).date().isoformat()
    doc = json.loads((out / f"backtest_hvol_{stamp}.json").read_text(encoding="utf-8"))
    md = (out / f"backtest_hvol_{stamp}.md").read_text(encoding="utf-8")

    assert doc["meta"]["n_symbols"] == 3                   # NIFTY 50 and both ETFs excluded
    import hashlib
    etf_meta = doc["meta"]["etf_exclusion"]
    assert (etf_meta["via_list"], etf_meta["via_pattern_only"]) == (1, 1)
    assert etf_meta["sha256"] == hashlib.sha256(etf_csv.read_bytes()).hexdigest()
    assert "ETFs excluded (Amendment A1, 2026-10-09): 1 via list + 1 via pattern" in md
    assert etf_meta["sha256"] in md and "ETFs excluded (Amendment A1, 2026-10-09)" in hv.__doc__
    assert "NIFTY 500 list" in md
    assert doc["meta"]["trial_count_n"] == 1 and doc["meta"]["registered_population"] is True
    prim = doc["primary"]["horizons"]["20"]
    assert prim["n"] == 1
    assert prim["mean_gross"] == pytest.approx(2.0)
    assert prim["mean_benchmark"] == pytest.approx(1.0)    # SPK +2 and FLT 0; SPL vetoed for its split
    assert prim["mean_excess"] == pytest.approx(1.0)
    splits = doc["splits"]
    assert splits["data_source_switch"]["cells"]["yes"]["horizons"]["20"]["n"] == 1
    assert splits["earnings_within_2_sessions"]["cells"]["yes"]["horizons"]["20"]["n"] == 1
    assert splits["entry_gap_gt_5pct"]["cells"]["no"]["horizons"]["20"]["n"] == 1
    assert splits["index_class_current_membership"]["label"] == hv.rc.SURVIVORSHIP_LABEL
    assert doc["promotion_cells"]["deployable_total_market_current"]["label"] == hv.rc.SURVIVORSHIP_LABEL
    assert doc["variants"]["breakout_inclusive"]["horizons"]["20"]["n"] == 1
    assert doc["primary"]["funnel"]["no_structural_ex"] == 1        # SPL's spike vetoed by its split
    assert doc["promotion"]["verdict"] == hv.VERDICT_REFUTED   # n = 1 < 200
    assert md.index("GEOMETRY:") < md.index("PRIMARY CELL") < md.index("PROMOTION")
    assert "VERDICT: REFUTED" in md and "§8.6" in doc["meta"]["verdict_promotable_text"]
    assert capsys.readouterr().out.isascii()


def test_rule_dataclass_is_frozen():
    with pytest.raises(AttributeError):
        hv.PRIMARY.vr_min = 2.0  # type: ignore[misc]
    assert replace(hv.PRIMARY, name="x").vr_min == 3.0
