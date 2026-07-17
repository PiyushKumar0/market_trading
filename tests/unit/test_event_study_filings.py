"""§2.8.4 filings legs of ``scripts/event_study.py`` — PURE event-derivation + measurement, plus a
tmp-store glue smoke (the real ``data/market.duckdb`` is never touched; every fixture is synthetic).

Covers the spec's required cases: trailing-10-session threshold crossing + re-arm; acq_mode exclusions;
after-hours broadcast shifting T to the next session; zero-reaction drop; pledge cohort split;
gross-vs-net arithmetic (net = gross - one CNC round trip). The store-reading glue is exercised on a
temp MarketStore for the empty-table (honest n=0) and populated-insider paths.
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from engine.core.clock import IST
from engine.marketdata.store import DailyBar, MarketStore
from engine.strategy.cost_model import CostModel

# Load the loose script by path (same pattern as test_watchdog.py).
_ES_PATH = Path(__file__).resolve().parents[2] / "scripts" / "event_study.py"
_spec = importlib.util.spec_from_file_location("mt_event_study", _ES_PATH)
es = importlib.util.module_from_spec(_spec)
sys.modules["mt_event_study"] = es
_spec.loader.exec_module(es)


def _sessions(n: int, start: date = date(2026, 1, 5)) -> list[date]:
    return [start + timedelta(days=i) for i in range(n)]


def _bdt(d: date, hh: int, mm: int = 0) -> datetime:
    return datetime(d.year, d.month, d.day, hh, mm, tzinfo=IST)


# =========================================================================== acq_mode exclusions
def test_is_open_market_buy_taxonomy():
    # a plain market Buy is open-market; a blank/None mode on a Buy defaults to open-market
    assert es.is_open_market_buy("Buy", "Market Purchase") is True
    assert es.is_open_market_buy("Buy", "") is True
    assert es.is_open_market_buy("Buy", None) is True
    assert es.is_open_market_buy("buy", "market purchase") is True          # case-insensitive txn_type
    # a Sell is never a buy
    assert es.is_open_market_buy("Sell", "Market Purchase") is False
    # every §2.8.2 non-market mode is excluded (case-insensitive substring)
    for mode in ("ESOP", "ESOPs", "ESOP Allotment", "Gift", "Inter-se Transfer",
                 "Inter se Transfer", "Pledge Invocation", "Preferential Offer",
                 "Preferential Allotment", "Rights", "Bonus", "gift  "):
        assert es.is_open_market_buy("Buy", mode) is False, mode


def test_insider_events_exclude_non_market_even_if_huge():
    sess = _sessions(30)
    esop = [{"txn_type": "Buy", "acq_mode": "ESOP Allotment", "value": Decimal("10000000000"),
             "broadcast_dt": _bdt(sess[5], 10)}]
    assert es.insider_buy_events(sess, esop, 10_000_000) == []        # excluded ⇒ never crosses
    market = [{"txn_type": "Buy", "acq_mode": "Market Purchase", "value": Decimal("20000000"),
               "broadcast_dt": _bdt(sess[5], 10)}]
    assert es.insider_buy_events(sess, market, 10_000_000) == [5]     # included ⇒ crosses at its session


# =========================================================================== after-hours PIT entry
def test_entry_session_index_after_hours_and_gaps():
    # Thu, Fri, Mon, Tue (a weekend gap between index 1 and 2).
    sess = [date(2026, 1, 8), date(2026, 1, 9), date(2026, 1, 12), date(2026, 1, 13)]
    assert es.after_hours(_bdt(sess[0], 16)) is True
    assert es.after_hours(_bdt(sess[0], 15, 30)) is False             # 15:30 sharp is AT the close
    assert es.entry_session_index(sess, _bdt(sess[0], 10)) == 0       # before close ⇒ same session
    assert es.entry_session_index(sess, _bdt(sess[1], 16)) == 2       # after close ⇒ NEXT session (skips weekend)
    assert es.entry_session_index(sess, _bdt(date(2026, 1, 10), 10)) == 2   # Sat filing ⇒ next Mon
    assert es.entry_session_index(sess, _bdt(sess[3], 16)) is None    # after the last bar ⇒ unmeasurable


def test_insider_after_hours_shifts_entry_to_next_session():
    sess = _sessions(30)
    # one crossing filing broadcast AFTER close on session[5] ⇒ entry at session[6], not [5].
    filings = [{"txn_type": "Buy", "acq_mode": "Market", "value": Decimal("15000000"),
                "broadcast_dt": _bdt(sess[5], 16)}]
    assert es.insider_buy_events(sess, filings, 10_000_000) == [6]


# =========================================================================== trailing crossing + rearm
def test_insider_trailing_crossing_and_rearm():
    sess = _sessions(30)
    filings = [
        {"txn_type": "Buy", "acq_mode": "Market", "value": Decimal("6000000"), "broadcast_dt": _bdt(sess[5], 10)},
        {"txn_type": "Buy", "acq_mode": "Market", "value": Decimal("6000000"), "broadcast_dt": _bdt(sess[6], 10)},
        # after both age out of the 10-session window the sum falls below threshold (re-arm); a later
        # single filing >= threshold then crosses again.
        {"txn_type": "Buy", "acq_mode": "Market", "value": Decimal("15000000"), "broadcast_dt": _bdt(sess[18], 10)},
    ]
    # first crossing at 6 (60L+60L=120L>=100L), disarm; sum stays >=thr on 6 but does NOT re-fire;
    # both age out by session 16 ⇒ re-arm; new crossing at 18.
    assert es.insider_buy_events(sess, filings, 10_000_000) == [6, 18]


def test_insider_single_big_filing_fires_once_only():
    sess = _sessions(30)
    filings = [{"txn_type": "Buy", "acq_mode": "Market", "value": Decimal("50000000"), "broadcast_dt": _bdt(sess[4], 10)}]
    # crosses at 4; stays in the trailing window for sessions 4..13 but must fire exactly once.
    assert es.insider_buy_events(sess, filings, 10_000_000) == [4]


# =========================================================================== pledge cohort split
def _prow(cat, qtr_end, pledged_pct, bdt):
    return {"category": cat, "qtr_end": qtr_end, "pledged_pct": pledged_pct, "broadcast_dt": bdt}


def test_pledge_delta_cohort_split_and_threshold():
    rows = [
        _prow("(A) Promoter & Promoter Group", date(2025, 3, 31), 10.0, _bdt(date(2025, 4, 20), 12)),
        _prow("(A) Promoter & Promoter Group", date(2025, 6, 30), 16.0, _bdt(date(2025, 7, 20), 12)),   # +6 increase
        _prow("(A) Promoter & Promoter Group", date(2025, 9, 30), 18.0, _bdt(date(2025, 10, 20), 12)),  # +2 sub-threshold
        _prow("(A) Promoter & Promoter Group", date(2025, 12, 31), 8.0, _bdt(date(2026, 1, 20), 12)),   # -10 decrease
        _prow("(B) Public", date(2025, 6, 30), 99.0, _bdt(date(2025, 7, 20), 12)),                      # non-promoter, ignored
    ]
    events = es.pledge_delta_events(rows, 5.0)
    assert [(e["direction"], e["qtr_end"]) for e in events] == [
        ("increase", date(2025, 6, 30)),
        ("decrease", date(2025, 12, 31)),
    ]
    inc = events[0]
    assert inc["broadcast_dt"] == _bdt(date(2025, 7, 20), 12)    # event time = LATER quarter's broadcast (PIT)
    assert round(inc["delta"], 4) == 6.0 and round(events[1]["delta"], 4) == -10.0


def test_pledge_delta_skips_missing_pledged_pct():
    rows = [
        _prow("(A) Promoter", date(2025, 3, 31), None, _bdt(date(2025, 4, 20), 12)),
        _prow("(A) Promoter", date(2025, 6, 30), 20.0, _bdt(date(2025, 7, 20), 12)),
    ]
    assert es.pledge_delta_events(rows, 5.0) == []       # None on one side ⇒ no comparable delta


# =========================================================================== zero-reaction drop
def _row(d, o, hi, lo, c, v):
    return es._Row(d, float(o), float(hi), float(lo), float(c), float(v))


def _rows_seq(closes, opens=None, base=date(2026, 2, 2)):
    opens = opens or closes
    return [_row(base + timedelta(days=i), opens[i], max(opens[i], closes[i]) + 1,
                 min(opens[i], closes[i]) - 1, closes[i], 1000) for i in range(len(closes))]


def test_measure_event_zero_reaction_dropped():
    rows = _rows_seq([100] * 10, [100] * 10)             # close == open everywhere ⇒ reaction 0
    assert es.measure_event(rows, 2, symbol="X", kind="results_filing", cost_pct=0.1) is None


def test_measure_event_reaction_sign_and_net():
    closes = [100, 100, 102, 104, 106, 108, 110, 112, 114, 116]
    opens = [100, 100, 100, 104, 106, 108, 110, 112, 114, 116]   # at idx 2: open 100 < close 102 ⇒ +1
    rows = _rows_seq(closes, opens)
    obs = es.measure_event(rows, 2, symbol="X", kind="results_filing", cost_pct=0.25)
    assert obs is not None and obs.reaction_sign == 1
    for k in es.HORIZONS:
        assert abs(obs.pead_net[k] - (obs.pead_gross[k] - 0.25)) < 1e-9   # net = gross - round trip


# =========================================================================== gross-vs-net arithmetic
def test_measure_directional_gross_minus_cost_is_net():
    closes = [100 + i for i in range(30)]
    rows = _rows_seq(closes)
    cost = 0.37
    obs = es.measure_directional(rows, 3, symbol="X", kind="insider_buy",
                                 horizons=es.INSIDER_HORIZONS, cost_pct=cost)
    assert obs is not None
    base = rows[3].close
    for k in es.INSIDER_HORIZONS:
        expected_gross = (rows[3 + k].close / base - 1.0) * 100.0
        assert abs(obs.gross[k] - expected_gross) < 1e-9
        assert abs(obs.net[k] - (obs.gross[k] - cost)) < 1e-9


def test_measure_directional_insufficient_forward_bars():
    rows = _rows_seq([100 + i for i in range(15)])       # < idx+20 ⇒ T+20 unmeasurable
    assert es.measure_directional(rows, 0, symbol="X", kind="insider_buy",
                                  horizons=es.INSIDER_HORIZONS, cost_pct=0.1) is None


# =========================================================================== aggregate + render
def test_aggregate_and_render_report_gross_and_net_columns():
    closes = [100 + i for i in range(30)]
    rows = _rows_seq(closes)
    d_obs = [es.measure_directional(rows, 3, symbol="X", kind="insider_buy",
                                    horizons=es.INSIDER_HORIZONS, cost_pct=0.1)]
    agg = es.aggregate([], [], [o for o in d_obs if o])
    meta = {"generated_at": "2026-07-16T00:00:00+05:30", "n_symbols": 1, "start": "2026-01-01",
            "end": "2026-03-01", "cost_pct": 0.1, "reference_notional": "20000",
            "skip_filings": False, "insider_min_value_inr": 10_000_000, "pledge_delta_min_pct": 5.0}
    md = es.render_markdown(agg, meta)
    assert "mean gross %" in md and "mean net %" in md               # both columns present
    assert "Insider-buy leg" in md and "T+20" in md
    # empty filings legs degrade to an honest n=0 note
    assert "shp_quarterly empty" in md and "results_filings empty" in md


def test_render_skip_filings_hides_filings_sections():
    agg = es.aggregate([], [], [])
    meta = {"generated_at": "x", "n_symbols": 0, "start": "a", "end": "b", "cost_pct": 0.1,
            "reference_notional": "20000", "skip_filings": True,
            "insider_min_value_inr": 10_000_000, "pledge_delta_min_pct": 5.0}
    md = es.render_markdown(agg, meta)
    assert "NO EVENTS DETECTED" in md                                # n_events 0 short-circuit unchanged
    assert "Insider-buy leg" not in md


# =========================================================================== tmp-store glue smoke
def _daily(sym, d, close):
    px = Decimal(str(close))
    return DailyBar(symbol=sym, d=d, open=px, high=px + 1, low=px - 1, close=px, volume=1000)


def _store(tmp_path, clock):
    return MarketStore(tmp_path / "market.duckdb", tmp_path / "parquet", clock).open()


def test_run_study_empty_filings_tables_degrade_to_honest_empty(tmp_path, clock):
    store = _store(tmp_path, clock)
    try:
        import datetime as _dt
        base = date(2026, 1, 1)
        bars = [_daily("AAA", base + _dt.timedelta(days=i), 100 + i) for i in range(60)]
        store.upsert_bars_1d(bars)
        cm = CostModel.from_config()
        pead, results, directional, meta = es.run_study(
            store, cm, ["AAA"], base, base + _dt.timedelta(days=59), today=date(2026, 6, 1)
        )
        assert results == [] and directional == []                  # empty filings tables ⇒ no events
        agg = es.aggregate(pead, results, directional)
        assert agg["legs"]["insider_buy"]["n_events"] == 0
        assert agg["n_events"] == 0                                  # nothing anywhere ⇒ honest whole-study empty
        md = es.render_markdown(agg, meta)
        assert "NO EVENTS DETECTED" in md                            # unchanged C9 short-circuit, no crash
    finally:
        store.close()


def test_run_study_populated_insider_leg_produces_events(tmp_path, clock):
    store = _store(tmp_path, clock)
    try:
        import datetime as _dt
        base = date(2026, 1, 1)
        dates = [base + _dt.timedelta(days=i) for i in range(60)]
        store.upsert_bars_1d([_daily("AAA", d, 100 + i) for i, d in enumerate(dates)])
        # one open-market buy above the ₹1cr floor, knowable at close of dates[25] ⇒ entry idx 25.
        store.upsert_insider_trades([{
            "id": "buy-1", "symbol": "AAA", "txn_type": "Buy", "acq_mode": "Market Purchase",
            "value": Decimal("12000000"), "broadcast_dt": _bdt(dates[25], 10),
        }])
        cm = CostModel.from_config()
        pead, results, directional, meta = es.run_study(
            store, cm, ["AAA"], base, dates[-1], today=date(2026, 6, 1)
        )
        insider = [o for o in directional if o.kind == "insider_buy"]
        assert len(insider) == 1 and insider[0].event_date == dates[25]
        assert results == []                                        # results_filings + earnings both empty
        # net = gross - one CNC round trip, per horizon
        for k in es.INSIDER_HORIZONS:
            assert abs(insider[0].net[k] - (insider[0].gross[k] - meta["cost_pct"])) < 1e-9
        agg = es.aggregate(pead, results, directional)
        assert agg["legs"]["insider_buy"]["n_events"] == 1
        # the populated insider leg renders its table WHILE the empty legs show honest n=0 notes
        md = es.render_markdown(agg, meta)
        assert "Insider-buy leg" in md and "T+20" in md
        assert "shp_quarterly empty" in md and "results_filings empty" in md
    finally:
        store.close()
