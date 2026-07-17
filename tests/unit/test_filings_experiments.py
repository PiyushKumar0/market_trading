"""Pre-registered filings experiments E1-E3 (``scripts/filings_experiments.py``).

PURE flag/slice builder tests (cluster-active window edges, results T+1/T+2 incl. after-hours shift,
adverse-pledge 90d window, person-category value-share dominance, liquidity terciles, crossing
contributors) + one tmp-store glue smoke per experiment (tiny synthetic frames/filings -> runs
end-to-end with honest empty/populated sections). The real ``data/market.duckdb`` is NEVER opened;
every fixture is synthetic on a temp MarketStore.
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from engine.core.clock import IST
from engine.core.types import Bar
from engine.marketdata.store import DailyBar, MarketStore
from engine.strategy.cost_model import CostModel

# Load the loose script by path (same pattern as test_event_study_filings.py).
_FE_PATH = Path(__file__).resolve().parents[2] / "scripts" / "filings_experiments.py"
_spec = importlib.util.spec_from_file_location("mt_filings_experiments", _FE_PATH)
fe = importlib.util.module_from_spec(_spec)
sys.modules["mt_filings_experiments"] = fe
_spec.loader.exec_module(fe)


def _sessions(n: int, start: date = date(2026, 1, 5)) -> list[date]:
    return [start + timedelta(days=i) for i in range(n)]


def _bdt(d: date, hh: int, mm: int = 0) -> datetime:
    return datetime(d.year, d.month, d.day, hh, mm, tzinfo=IST)


# =========================================================================== cluster-active window edges
def test_cluster_active_dates_excludes_crossing_and_marks_forward_five():
    sess = _sessions(15)
    active = fe.cluster_active_dates(sess, [3], forward=5)
    assert active == {sess[4], sess[5], sess[6], sess[7], sess[8]}
    assert sess[3] not in active                     # crossing session itself is NOT active (PIT)


def test_cluster_active_dates_clamps_at_series_end():
    sess = _sessions(15)                             # indices 0..14
    active = fe.cluster_active_dates(sess, [13], forward=5)
    assert active == {sess[14]}                      # 14 valid; 15/16/17/18 clamped away
    assert fe.cluster_active_dates(sess, [14], forward=5) == set()   # last session: nothing after


def test_cluster_active_dates_unions_multiple_events():
    sess = _sessions(20)
    active = fe.cluster_active_dates(sess, [2, 3], forward=2)
    assert active == {sess[3], sess[4], sess[5]}     # {3,4} U {4,5}


# =========================================================================== results T+1/T+2 boundary
def test_results_active_dates_t1_t2_and_after_hours_shift():
    sess = [date(2026, 1, 8), date(2026, 1, 9), date(2026, 1, 12), date(2026, 1, 13), date(2026, 1, 14)]
    # broadcast at/before 15:30 on sess[0] -> knowable session 0 -> active {1, 2}
    assert fe.results_active_dates(sess, [_bdt(sess[0], 10)]) == {sess[1], sess[2]}
    # after-hours on sess[0] -> knowable session 1 (skips nothing here) -> active {2, 3}
    assert fe.results_active_dates(sess, [_bdt(sess[0], 16)]) == {sess[2], sess[3]}
    # 15:30 sharp is AT the close (not after-hours) -> knowable same session
    assert fe.results_active_dates(sess, [_bdt(sess[0], 15, 30)]) == {sess[1], sess[2]}


def test_results_active_dates_clamps_and_skips_none():
    sess = _sessions(5)
    # knowable session 3 -> {4}; the T+2 offset (5) is clamped off the end
    assert fe.results_active_dates(sess, [_bdt(sess[3], 10)]) == {sess[4]}
    assert fe.results_active_dates(sess, [None]) == set()


# =========================================================================== adverse-pledge 90d window
def test_pledge_increase_active_90d_window_edges():
    sess = [date(2026, 1, 1) + timedelta(days=i) for i in range(200)]
    inc = [{"broadcast_dt": _bdt(date(2026, 1, 10), 12), "direction": "increase"}]
    assert fe.pledge_increase_active(date(2026, 4, 10), sess, inc, 90) is True   # exactly 90 cal days
    assert fe.pledge_increase_active(date(2026, 4, 11), sess, inc, 90) is False  # 91 days -> outside
    assert fe.pledge_increase_active(date(2026, 1, 10), sess, inc, 90) is True   # same day, knowable


def test_pledge_increase_active_not_knowable_before_broadcast():
    sess = [date(2026, 1, 1) + timedelta(days=i) for i in range(60)]
    inc = [{"broadcast_dt": _bdt(date(2026, 1, 20), 12), "direction": "increase"}]
    assert fe.pledge_increase_active(date(2026, 1, 5), sess, inc, 90) is False    # entry precedes broadcast
    # after-hours broadcast on the entry date is NOT knowable at that session's close
    inc2 = [{"broadcast_dt": _bdt(date(2026, 1, 20), 16), "direction": "increase"}]
    assert fe.pledge_increase_active(date(2026, 1, 20), sess, inc2, 90) is False


# =========================================================================== person-category dominance
def test_person_category_group_mapping():
    assert fe.person_category_group("Promoters") == fe.CATEGORY_GROUP_PROMOTER
    assert fe.person_category_group("Promoter Group") == fe.CATEGORY_GROUP_PROMOTER
    assert fe.person_category_group("Director") == fe.CATEGORY_GROUP_DIRECTOR_KMP
    assert fe.person_category_group("Key Managerial Personnel") == fe.CATEGORY_GROUP_DIRECTOR_KMP
    assert fe.person_category_group("KMP") == fe.CATEGORY_GROUP_DIRECTOR_KMP
    assert fe.person_category_group("Employees") == fe.CATEGORY_GROUP_OTHER
    assert fe.person_category_group("Designated Person") == fe.CATEGORY_GROUP_OTHER
    assert fe.person_category_group(None) == fe.CATEGORY_GROUP_OTHER


def test_dominant_category_is_by_value_share():
    filings = [
        {"person_category": "Promoters", "value": Decimal("5000000")},
        {"person_category": "Director", "value": Decimal("9000000")},   # largest single group
        {"person_category": "Employees", "value": Decimal("1000000")},
    ]
    assert fe.dominant_category(filings) == fe.CATEGORY_GROUP_DIRECTOR_KMP
    # split promoter across two rows so its SHARE (10M) beats the director's 9M
    filings2 = filings + [{"person_category": "Promoter Group", "value": Decimal("5000000")}]
    assert fe.dominant_category(filings2) == fe.CATEGORY_GROUP_PROMOTER
    assert fe.dominant_category([]) == "unknown"


def test_contributing_filings_window_and_side():
    sess = [date(2026, 1, 1) + timedelta(days=i) for i in range(30)]
    filings = [
        {"txn_type": "Buy", "acq_mode": "Market", "value": Decimal("5000000"),
         "broadcast_dt": _bdt(sess[5], 10), "person_category": "Promoters"},
        {"txn_type": "Buy", "acq_mode": "Market", "value": Decimal("6000000"),
         "broadcast_dt": _bdt(sess[8], 10), "person_category": "Director"},
        {"txn_type": "Sell", "acq_mode": "Market", "value": Decimal("9990000"),   # sell -> excluded
         "broadcast_dt": _bdt(sess[8], 10), "person_category": "Promoters"},
        {"txn_type": "Buy", "acq_mode": "ESOP", "value": Decimal("9990000"),      # ESOP -> excluded
         "broadcast_dt": _bdt(sess[8], 10), "person_category": "Employees"},
    ]
    contrib = fe.contributing_filings(sess, filings, 8)      # window [0..8] at crossing 8
    assert len(contrib) == 2                                  # the two open-market buys only
    # trailing window is 10 sessions (lo = crossing-9): moving the crossing later drops early buys.
    assert len(fe.contributing_filings(sess, filings, 16)) == 1   # window [7..16]: only the sess[8] buy
    assert len(fe.contributing_filings(sess, filings, 18)) == 0   # window [9..18]: neither buy remains


# =========================================================================== liquidity terciles
def test_liquidity_tercile_labels_partition_is_monotone():
    vals = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0]
    labels, bounds = fe.liquidity_tercile_labels(vals)
    assert bounds is not None
    lows = [v for v, lab in zip(vals, labels, strict=True) if lab == "low"]
    mids = [v for v, lab in zip(vals, labels, strict=True) if lab == "mid"]
    highs = [v for v, lab in zip(vals, labels, strict=True) if lab == "high"]
    assert lows and mids and highs                           # all three terciles populated
    assert max(lows) <= min(mids)                            # monotone, boundary-consistent
    assert max(mids) <= min(highs)


def test_liquidity_tercile_labels_too_few_and_nan():
    labels, bounds = fe.liquidity_tercile_labels([float("nan"), 1.0])
    assert bounds is None                                    # < 3 usable -> not terciled
    assert labels == ["unknown", "all"]


def test_year_slice_canonical_buckets():
    assert fe.year_slice(date(2023, 8, 1)) == "2023H2"
    assert fe.year_slice(date(2023, 6, 30)) == "2023H1"
    assert fe.year_slice(date(2024, 5, 5)) == "2024"
    assert fe.year_slice(date(2025, 12, 31)) == "2025"
    assert fe.year_slice(date(2026, 6, 30)) == "2026H1"
    assert fe.year_slice(date(2026, 7, 1)) == "2026H2"


# =========================================================================== tmp-store glue smokes
def _store(tmp_path, clock) -> MarketStore:
    return MarketStore(tmp_path / "market.duckdb", tmp_path / "parquet", clock).open()


def _daily(sym: str, d: date, close: float) -> DailyBar:
    px = Decimal(str(close))
    return DailyBar(symbol=sym, d=d, open=px, high=px + 1, low=px - 1, close=px, volume=100_000)


def _min_bars(sym: str, day: date, n: int = 40, base: float = 100.0) -> list[Bar]:
    t0 = datetime(day.year, day.month, day.day, 9, 15, tzinfo=IST)
    out: list[Bar] = []
    for i in range(n):
        px = Decimal(str(round(base + (i % 3) * 0.1, 2)))
        out.append(Bar(
            symbol=sym, ts_minute=t0 + timedelta(minutes=i), open=px, high=px + Decimal("0.5"),
            low=px - Decimal("0.5"), close=px, volume=1000,
            auction_open=Decimal(str(base)) if i == 0 else None,
        ))
    return out


def test_e3_populated_insider_leg_slices(tmp_path, clock):
    store = _store(tmp_path, clock)
    try:
        base = date(2026, 1, 1)
        dates = [base + timedelta(days=i) for i in range(60)]
        store.upsert_bars_1d([_daily("AAA", d, 100 + i) for i, d in enumerate(dates)])
        store.upsert_insider_trades([{
            "id": "buy-1", "symbol": "AAA", "txn_type": "Buy", "acq_mode": "Market Purchase",
            "value": Decimal("12000000"), "person_category": "Promoters",
            "broadcast_dt": _bdt(dates[25], 10),
        }])
        cm = CostModel.from_config()
        res = fe.run_e3(store, cm, clock, ["AAA"], base, dates[-1],
                        insider_min_value_inr=10_000_000)
        assert res["n_events"] == 1
        # slice (a): entry 2026-01-26 -> 2026H1 bucket
        assert "2026H1" in res["slices"]["by_year"]
        assert res["slices"]["by_year"]["2026H1"][20]["n"] == 1
        # slice (b): the single contributing filing is a promoter -> promoter bucket
        assert fe.CATEGORY_GROUP_PROMOTER in res["slices"]["by_category"]
        # slice (c): a single event cannot be terciled -> honest single 'all' bucket
        assert "all" in res["slices"]["by_liquidity"]
        md = fe.render_e3(res)
        assert "insider_net_buy robustness slices" in md and "T+20" in md
    finally:
        store.close()


def test_e3_empty_is_honest(tmp_path, clock):
    store = _store(tmp_path, clock)
    try:
        base = date(2026, 1, 1)
        dates = [base + timedelta(days=i) for i in range(60)]
        store.upsert_bars_1d([_daily("AAA", d, 100 + i) for i, d in enumerate(dates)])
        cm = CostModel.from_config()
        res = fe.run_e3(store, cm, clock, ["AAA"], base, dates[-1], insider_min_value_inr=10_000_000)
        assert res["n_events"] == 0
        md = fe.render_e3(res)
        assert "NO insider_net_buy crossings" in md            # honest whole-study empty (C9)
    finally:
        store.close()


@pytest.mark.needs_heavy_deps
def test_e1_runs_end_to_end_honest_empty(tmp_path, clock):
    pytest.importorskip("vectorbt")
    store = _store(tmp_path, clock)
    try:
        days = [date(2026, 3, 2), date(2026, 3, 3), date(2026, 3, 4)]
        for d in days:
            store.insert_bars_1m(_min_bars("AAA", d))
        # daily bars + one insider buy so the cohort-A flag set is non-empty (glue exercised)
        dl = [date(2026, 1, 1) + timedelta(days=i) for i in range(70)]
        store.upsert_bars_1d([_daily("AAA", d, 100 + i) for i, d in enumerate(dl)])
        store.upsert_insider_trades([{
            "id": "b1", "symbol": "AAA", "txn_type": "Buy", "acq_mode": "Market",
            "value": Decimal("12000000"), "person_category": "Promoters",
            "broadcast_dt": _bdt(dl[30], 10),
        }])
        cm = CostModel.from_config()
        res = fe.run_e1(store, cm, clock, ["AAA"], days[0], days[-1], insider_min_value_inr=10_000_000)
        assert res["experiment"] == "e1"
        assert isinstance(res["n_trades_total"], int)
        assert set(res["cohorts"]) == {"A_insider_buy_cluster", "B_results_t1_t2", "C_unconditioned"}
        assert res["round_trip_pct"] > 0.0                     # a real MIS round trip was modelled
        md = fe.render_e1(res)
        assert "Cohort stats" in md and "Discriminator" in md
        if res["n_trades_total"] == 0:
            assert "honest n=0" in md                          # empty cohorts reported honestly (C9)
    finally:
        store.close()


@pytest.mark.needs_heavy_deps
def test_e2_runs_end_to_end_honest_empty(tmp_path, clock):
    pytest.importorskip("vectorbt")
    store = _store(tmp_path, clock)
    try:
        base = date(2025, 1, 1)
        dates = [base + timedelta(days=i) for i in range(80)]
        store.upsert_bars_1d([_daily("AAA", d, 100 + (i % 7)) for i, d in enumerate(dates)])
        store.upsert_bars_1d([_daily("NIFTY 50", d, 20000 + i) for i, d in enumerate(dates)])
        # exercise the adverse SELL-cluster + pledge-increase flag paths
        store.upsert_insider_trades([{
            "id": "s1", "symbol": "AAA", "txn_type": "Sell", "acq_mode": "Market",
            "value": Decimal("15000000"), "person_category": "Promoters",
            "broadcast_dt": _bdt(dates[20], 10),
        }])
        store.upsert_shp_quarterly([
            {"symbol": "AAA", "qtr_end": date(2024, 12, 31), "category": "Promoter & Promoter Group",
             "pledged_pct": 10.0, "broadcast_dt": _bdt(date(2025, 1, 15), 12)},
            {"symbol": "AAA", "qtr_end": date(2025, 3, 31), "category": "Promoter & Promoter Group",
             "pledged_pct": 18.0, "broadcast_dt": _bdt(date(2025, 4, 15), 12)},
        ])
        cm = CostModel.from_config()
        res = fe.run_e2(store, cm, clock, ["AAA"], dates[0], dates[-1],
                        insider_min_value_inr=10_000_000, pledge_delta_min_pct=5.0)
        assert res["experiment"] == "e2"
        assert res["regime_applied"] is True                   # NIFTY 50 frame present
        assert set(res["cohorts"]) == {"ADVERSE", "FAVORABLE", "NEUTRAL"}
        md = fe.render_e2(res)
        assert "Cohort stats" in md and "Discriminator" in md
        if res["n_trades_total"] == 0:
            assert "honest n=0" in md
    finally:
        store.close()
