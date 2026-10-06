"""scripts/backtest_regime_filter.py (C2) + scripts/backtest_selection_filters.py (C3), plan-2026-10-06 Q5.4/Q5.5."""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path

import numpy as np
import pytest

_ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location(
    "mt_backtest_selection_filters", _ROOT / "scripts" / "backtest_selection_filters.py")
c3 = importlib.util.module_from_spec(_spec)
sys.modules["mt_backtest_selection_filters"] = c3
_spec.loader.exec_module(c3)
c2, rc = c3.rf, c3.rc

from engine.core.clock import IST  # noqa: E402
from engine.marketdata.store import DailyBar, MarketStore  # noqa: E402
from engine.strategy.cost_model import CostModel  # noqa: E402


def _weekdays(start: date, n: int) -> list[date]:
    out, d = [], start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


def _series(sym: str, dates: list[date], close) -> rc.bb.Series:
    c = np.asarray(close, dtype="float64")
    return rc.bb.Series(sym, dates, c, c, c, c, np.full(len(c), 1000.0), [])


# ----------------------------------------------------------------------------- C2 regime predicate
SAWTOOTH = [100.0 + (i % 20) for i in range(80)]   # SMA50 identical 20 sessions apart, close above it


@pytest.mark.parametrize("closes, on", [
    (np.arange(1000.0, 1070.0), True),
    (np.arange(1000.0, 1069.0), None),                                   # < 50 + 20 closes
    (np.full(70, 1000.0), False),                                        # close == SMA50
    (np.r_[np.arange(1000.0, 1069.0), 1000.0], False),                   # close below SMA50
    (np.array(SAWTOOTH), False),                                         # SMA50 flat: strict rising
    (None, None),                                                        # no index bar on y
])
def test_regime_predicate(closes, on):
    assert c2.regime_on(closes) is on


def test_index_closes_are_point_in_time():
    dates = _weekdays(date(2026, 9, 1), 5)
    idx = _series(c2.INDEX_SYMBOL, dates, [1.0, 2.0, 3.0, 4.0, 5.0])
    assert c2.closes_through(idx, dates[2]).tolist() == [1.0, 2.0, 3.0]
    assert c2.closes_through(idx, date(2026, 9, 6)) is None             # Sunday: no bar on y


# ----------------------------------------------------------------------------- C3 filters at their boundaries
def _bars(close, *, high=None, low=None, volume=None, atr=None, index=None) -> c3.Bars:
    c = np.asarray(close, dtype="float64")
    return c3.Bars(c, c if high is None else high, c if low is None else low,
                   np.full(len(c), 1000.0) if volume is None else np.asarray(volume, dtype="float64"),
                   np.full(len(c), 1.0) if atr is None else np.asarray(atr), index)


def _rs(stock_end: float, index_end: float | None) -> c3.Bars:
    c = np.full(64, 105.0)
    c[0], c[-1] = 100.0, stock_end
    idx = None if index_end is None else np.r_[1000.0, np.full(62, 1050.0), index_end]
    return _bars(c, index=idx)


def _template(dip: float, spike: float, n: int = 260) -> c3.Bars:
    c = np.linspace(97.0, 120.0, n)
    low, high = c.copy(), c.copy()
    low[-100], high[-50] = dip, spike
    return _bars(c, high=high, low=low)


def _acc(moves: list[int], vol_up: list[bool]) -> c3.Bars:
    close = np.r_[100.0, 100.0 + np.cumsum(moves)]
    volume = np.r_[1000.0, 1000.0 + np.cumsum([1 if u else -1 for u in vol_up])]
    return _bars(close, volume=volume)


ALT = [1, -1] * 12


@pytest.mark.parametrize("f, bars, passed", [
    (c3.rs63, _rs(110.0, 1100.0), False),                                # returns equal: not > 0
    (c3.rs63, _rs(110.0, 1099.0), True),
    (c3.rs63, _rs(110.0, None), None),
    (c3.trend_template, _template(96.0, 160.0), True),                   # close == 1.25 x low == 0.75 x high
    (c3.trend_template, _template(96.01, 160.0), False),
    (c3.trend_template, _template(96.0, 160.01), False),
    (c3.trend_template, _template(96.0, 160.0, n=251), None),
    (c3.atr_contraction, _bars(np.ones(61), atr=[1.25] * 60 + [1.0]), True),
    (c3.atr_contraction, _bars(np.ones(61), atr=[1.25] * 60 + [1.0000001]), False),
    (c3.atr_contraction, _bars(np.ones(61), atr=[np.nan] + [1.25] * 59 + [1.0]), None),
    (c3.stage2, _bars([100.0] * 169 + [100.01]), True),
    (c3.stage2, _bars([100.0] * 170), False),                            # close == SMA150, SMA150 flat
    (c3.stage2, _bars([100.0] * 168 + [100.01]), None),
    (c3.acc_dist, _acc(ALT + [1], [True] * 25), True),                   # 13 - 12
    (c3.acc_dist, _acc(ALT + [0], [True] * 25), False),                  # 12 - 12
    (c3.acc_dist, _acc(ALT + [1], [True] * 24 + [False]), False),        # the up-day on lighter volume
    (c3.acc_dist, _acc(ALT, [True] * 24), None),
])
def test_selection_filter_boundaries(f, bars, passed):
    assert f(bars) is passed


def test_family_n_is_the_filter_count():
    assert len(c3.FILTERS) == c3.FAMILY_N


# ----------------------------------------------------------------------------- matched cohorts
def _index_closes(n: int) -> list[float]:
    return [1000.0 + i if i < 90 else 1089.0 - 5 * (i - 89) for i in range(n)]


DATES_120 = _weekdays(date(2026, 3, 2), 120)


@pytest.mark.parametrize("mod", [c2, c3])
def test_cohorts_split_the_measured_events_never_regenerate(mod, monkeypatch):
    s = _series("SYM", DATES_120, [100.0] * 120)
    events = [rc.Event("SYM", s, i, i + 1, Decimal("100.00"), 1.0) for i in (75, 98, 10)]  # ON, OFF, undefined
    pop = rc.Population("hi52", "synthetic", 1, events, {}, {})
    walked, real = [], rc.walk
    monkeypatch.setattr(rc, "walk", lambda ev, **kw: walked.append(ev) or real(ev, **kw))
    r = mod.study(pop, _series(c2.INDEX_SYMBOL, DATES_120, _index_closes(120)), CostModel.from_config())
    assert walked == events
    events_by_cohort = {k: c["events"] for k, c in r["cohorts"].items()}
    assert events_by_cohort.pop(c2.FULL) == 3
    if mod is c2:
        assert events_by_cohort == {c2.ON: 1, c2.OFF: 1} and r["population"]["regime_undefined"] == 1
        assert r["decision"] == c2.INSUFFICIENT
    else:
        assert set(events_by_cohort) == set(c3.FILTERS) and max(events_by_cohort.values()) <= 3
        assert set(r["decision"].values()) == {c2.INSUFFICIENT}


# ----------------------------------------------------------------------------- smoke run
DATES = _weekdays(date(2026, 1, 5), 190)
END = DATES[-1]


def _daily(sym: str, rows: list[tuple]) -> list[DailyBar]:
    return [DailyBar(symbol=sym, d=DATES[i], open=Decimal(str(o)), high=Decimal(str(h)), low=Decimal(str(lo)),
                     close=Decimal(str(c)), volume=v, src="bhavcopy") for i, (o, h, lo, c, v) in enumerate(rows)]


def _brk20_rows() -> list[tuple]:
    rows = [(100, 100, 99, 100, 1000)] * len(DATES)
    rows[100] = (100, 104, 100, 104, 2000)                    # fresh 20-day breakout on 2x volume
    rows[101] = (103, 105, 99, 101, 1000)                     # touches the 100.00 level: V2-5 fill
    return rows


def _hi52_rows() -> list[tuple]:
    rows = [(200, 200, 150, 150, 1000)] + [(150, 150, 150, 150, 1000)] * 130
    prev = 150.0
    for k in range(1, 21):                                    # smooth climb; crosses 0.95 x 200 at k = 20
        c = round(150 * 1.0125 ** k, 2)
        rows.append((prev, c, prev, c, 1000))
        prev = c
    return rows + [(prev, prev, prev, prev, 1000)] * (len(DATES) - 151)


def _seed(tmp_path: Path, clock) -> Path:
    db = tmp_path / "research" / f"market_{END}.duckdb"
    store = MarketStore(db, tmp_path / "parquet", clock).open()
    try:
        store.upsert_bars_1d(
            _daily("BRKA", _brk20_rows()) + _daily("HIA", _hi52_rows())
            + _daily("INSA", [(100, 101, 99, 100, 1000)] * len(DATES))
            + _daily("ZZZ", [(100, 100, 100, 100, 1000)] * len(DATES))
            + _daily(c2.INDEX_SYMBOL, [(c, c, c, c, 0) for c in range(1000, 1000 + len(DATES))])
        )
        store.upsert_universe_daily([
            {"d": d, "symbol": sym, "included": True, "exclusion_reasons": None, "median_traded_value": Decimal("1e8")}
            for d, syms in ((rc.BRK20_UNIVERSE_AS_OF, ("BRKA", "ZZZ")), (END, ("INSA", "ZZZ"))) for sym in syms
        ])
        store.upsert_insider_trades([{
            "id": "buy-1", "symbol": "INSA", "txn_type": "Buy", "acq_mode": "Market Purchase",
            "value": Decimal("20000000"), "broadcast_dt": datetime.combine(DATES[160], time(11), IST),
        }])
    finally:
        store.close()
    (tmp_path / "universe").mkdir()
    (tmp_path / "universe" / "index_cached.csv").write_text("Symbol\nHIA\nZZZ\n", encoding="utf-8")
    return db


@pytest.mark.parametrize("mod", [c2, c3])
def test_max_symbols_smoke_run(mod, tmp_path, clock, monkeypatch, capsys):
    monkeypatch.setattr(rc, "market_hours_refusal", lambda: None)
    db = _seed(tmp_path, clock)
    out = tmp_path / "reports" / "study.json"
    assert mod.main(["--db", str(db), "--max-symbols", "1", "--end", str(END), "--out", str(out)]) == 0
    doc = json.loads(out.read_text(encoding="utf-8"))
    assert list(doc["strategies"]) == list(mod.STRATEGIES)
    for block in doc["strategies"].values():
        assert (block["population"]["n_symbols"], block["population"]["n_events"]) == (1, 1)
        assert block["cohorts"][c2.FULL]["n"] == 1
        if mod is c2:                                         # the index rises throughout: every event is ON
            assert (block["cohorts"][c2.ON]["n"], block["cohorts"][c2.OFF]["n"]) == (1, 0)
            assert block["decision"] == c2.INSUFFICIENT
    assert doc["meta"]["family_n"] == mod.FAMILY_N and doc["meta"]["registered_population"] is False
    assert out.with_suffix(".md").exists()
    text = capsys.readouterr().out
    assert text.isascii() and text.index("GEOMETRY FIRST") < text.index("CPCV")
