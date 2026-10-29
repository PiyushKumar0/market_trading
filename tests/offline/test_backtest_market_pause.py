"""scripts/backtest_market_pause.py: the pre-registered market-wide pause study (review §9 #1). Synthetic only."""

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
_spec = importlib.util.spec_from_file_location("mt_backtest_market_pause", _ROOT / "scripts" / "backtest_market_pause.py")
mp = importlib.util.module_from_spec(_spec)
sys.modules["mt_backtest_market_pause"] = mp
_spec.loader.exec_module(mp)
rc, rf = mp.rc, mp.rf
rs = rc._load_script("research_snapshot")

from engine.learning.multiplicity import permutation_pvalue  # noqa: E402
from engine.learning.validate import fold_pass_min  # noqa: E402
from engine.marketdata.store import DailyBar, MarketStore  # noqa: E402
from engine.strategy.cost_model import CostModel  # noqa: E402


def _weekdays(start: date, n: int) -> list[date]:
    out, d = [], start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


D = _weekdays(date(2026, 3, 2), 30)
CLOSES = {"AAA": {D[0]: 100, D[1]: 110, D[2]: 110}, "BBB": {D[0]: 100, D[1]: 126, D[2]: 126},
          "CCC": {D[0]: 100, D[1]: 80, D[2]: 88}, "OUT": {D[0]: 100, D[1]: 200, D[2]: 200}}


def _snapshot(tmp_path, clock, closes=CLOSES, splits=(("CCC", D[1]),)) -> Path:
    """A research snapshot built the way scripts/research_snapshot.py builds the real one."""
    src = tmp_path / "live" / "src.duckdb"
    store = MarketStore(src, tmp_path / "parquet", clock).open()
    try:
        store.upsert_bars_1d([DailyBar(symbol=s, d=d, open=Decimal(c), high=Decimal(c), low=Decimal(c),
                                       close=Decimal(c), volume=1000, src="bhavcopy")
                              for s, cs in closes.items() for d, c in cs.items()])
        store.upsert_corp_actions([{"symbol": s, "ex_date": d, "kind": "split"} for s, d in splits])
    finally:
        store.close()
    db = tmp_path / "research" / "market_2026-03-04.duckdb"
    rs.snapshot(src, db)
    (tmp_path / "universe").mkdir()
    (tmp_path / "universe" / "core_index_cached.csv").write_text("Symbol\nAAA\nBBB\nCCC\n", encoding="utf-8")
    return db


def _core_ew(db: Path, end: date) -> dict:
    conn = rc.open_snapshot(db)
    try:
        return rc.core_ew(conn, db, end)[0]
    finally:
        conn.close()


def test_ew_drops_moves_over_25pct_and_structural_days(tmp_path, clock):
    daily = _core_ew(_snapshot(tmp_path, clock), D[2])
    assert daily[D[0]] is None
    assert daily[D[1]] == pytest.approx(0.10)             # BBB +26% dropped, CCC's -20% split day skipped, OUT not core
    assert daily[D[2]] == pytest.approx((0.0 + 0.0 + 0.10) / 3)


def test_ew_grid_skips_a_muhurat_session(tmp_path, clock):
    mon, muhurat, thu = date(2025, 10, 20), date(2025, 10, 21), date(2025, 10, 23)   # config/calendar/2025.yaml
    daily = _core_ew(_snapshot(tmp_path, clock, {"AAA": {mon: 100, muhurat: 105, thu: 110}}, ()), thu)
    assert list(daily) == [mon, thu] and daily[thu] == pytest.approx(0.10)


def test_ew20_compounds_the_20_sessions_ending_at_y():
    daily = {d: 0.0 for d in D}
    daily[D[0]] = None
    daily[D[4]] = daily[D[25]] = -0.5                    # 21st session back, and after y: outside
    daily[D[5]] = 0.01                                    # 20th session back: inside
    table = mp.ew20_table(daily)
    assert table[D[24]] == pytest.approx(0.01)
    assert table.get(D[19]) is None                       # its window reaches the None first session
    assert table.get(D[18]) is None                       # fewer than 20 sessions


def _r(trade_n=30, pause_n=30, promotable=True, trade_med=0.5, full_med=0.2, pause_med=0.0, p=0.0249) -> dict:
    return {"cohorts": {mp.TRADE: {"n": trade_n, "median_net": trade_med}, mp.PAUSE: {"n": pause_n, "median_net": pause_med},
                        rf.FULL: {"n": trade_n + pause_n, "median_net": full_med}},
            "cpcv": {mp.TRADE: {"promotable": promotable}}, "permutation": {"p": p}}


@pytest.mark.parametrize("kw, decision", [
    ({}, rf.SUPPORTED),
    ({"trade_n": 29}, rf.INSUFFICIENT),
    ({"pause_n": 29}, rf.INSUFFICIENT),
    ({"promotable": False}, rf.NOT_SUPPORTED),
    ({"trade_med": 0.2}, rf.NOT_SUPPORTED),                           # not above FULL
    ({"full_med": -0.1, "trade_med": 0.0}, rf.NOT_SUPPORTED),         # not above 0
    ({"pause_med": 0.01}, rf.NOT_SUPPORTED),
    ({"p": 0.025}, rf.NOT_SUPPORTED),
])
def test_decide_boundaries(kw, decision):
    assert mp.decide(_r(**kw)) == decision


def test_the_registered_family_bar():
    assert (mp.FAMILY_N, fold_pass_min(mp.FAMILY_N), mp.ALPHA) == (2, 0.60, 0.025)
    assert (mp.SEED, mp.DRAWS, mp.STRATEGIES) == (20261029, 10_000, ("hi52", "ins"))


DATES_60 = _weekdays(date(2026, 3, 2), 60)


def _population(strategy: str, idx=(5, 10, 15, 20)) -> rc.Population:
    c = 100.0 + np.random.default_rng(0).uniform(-10.0, 10.0, 60)          # distinct T+20 nets per event
    s = rc.bb.Series("SYM", DATES_60, c, c, c, c, np.full(60, 1000.0), [])
    return rc.Population(strategy, "synthetic", 1, [rc.Event("SYM", s, i, i + 1, Decimal("100.00"), 1.0) for i in idx],
                         {}, {})


def test_study_splits_the_measured_events_by_ew20_at_the_signal_session():
    table = {DATES_60[5]: 0.0, DATES_60[10]: 0.02, DATES_60[15]: -0.01}    # DATES_60[20]: undefined
    r = mp.study(_population("hi52"), table, CostModel.from_config())
    assert {k: c["events"] for k, c in r["cohorts"].items()} == {rf.FULL: 4, mp.TRADE: 2, mp.PAUSE: 1}
    assert r["population"]["ew20_undefined"] == 1 and r["family"] == "regime_filter/hi52"
    assert r["decision"] == rf.INSUFFICIENT


def test_permutation_compares_trade_with_full_at_the_registered_seed():
    pop = _population("hi52", idx=range(1, 39))
    table = {DATES_60[i]: 0.01 if i % 3 else -0.01 for i in range(1, 39)}
    cost_model = CostModel.from_config()
    r = mp.study(pop, table, cost_model)
    nets = [t.net for _, t in rf.measure(pop, rc.cost_pct(cost_model))]
    trade = [n for i, n in zip(range(1, 39), nets, strict=True) if i % 3]
    assert len(set(nets)) == len(nets)
    assert r["permutation"]["p"] == permutation_pvalue(trade, nets, draws=10_000, seed=20261029)


def test_cli_refuses_until_registered(tmp_path, clock, monkeypatch, capsys):
    monkeypatch.setattr(rc, "market_hours_refusal", lambda: None)
    assert mp.REGISTERED is None
    assert mp.main(["--db", str(_snapshot(tmp_path, clock)), "--end", str(D[2])]) == 2
    assert "not registered" in capsys.readouterr().err


def test_cli_writes_the_report(tmp_path, clock, monkeypatch, capsys):
    db = _snapshot(tmp_path, clock)
    monkeypatch.setattr(rc, "market_hours_refusal", lambda: None)
    monkeypatch.setattr(mp, "REGISTERED", date(2026, 10, 29))
    monkeypatch.setattr(rf, "build_population", lambda s, *a: _population(s))
    out = tmp_path / "reports" / "pause.json"
    assert mp.main(["--db", str(db), "--end", str(D[2]), "--out", str(out)]) == 0
    doc = json.loads(out.read_text(encoding="utf-8"))
    assert list(doc["strategies"]) == ["hi52", "ins"] and doc["meta"]["family_n"] == 2
    text = capsys.readouterr().out
    assert text.isascii() and "SURVIVORSHIP-TAINTED" in text and "DECISION regime_filter/ins: insufficient" in text
