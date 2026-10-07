"""scripts/research_common.py + scripts/backtest_exit_geometry.py (plan-2026-10-06 Q5.3, C1)."""

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
_spec = importlib.util.spec_from_file_location("mt_backtest_exit_geometry", _ROOT / "scripts" / "backtest_exit_geometry.py")
c1 = importlib.util.module_from_spec(_spec)
sys.modules["mt_backtest_exit_geometry"] = c1
_spec.loader.exec_module(c1)
rc = c1.rc

from engine.core.clock import IST  # noqa: E402
from engine.marketdata.store import DailyBar, MarketStore  # noqa: E402
from engine.strategy.types import RawLevels  # noqa: E402

SHIPPED, TIME, CAT = c1.ARM_SHIPPED, c1.ARM_TIME, c1.ARM_CATASTROPHE


def _weekdays(start: date, n: int) -> list[date]:
    out, d = [], start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


# ----------------------------------------------------------------------------- arm definitions
def _event(overrides: dict[int, tuple], *, atr: float = 2.0, levels: RawLevels | None = None) -> rc.Event:
    """Signal at 0, fill at 1 for 100.00; every bar 100 flat unless overridden as (o, h, l, c)."""
    n = 25
    bars = [overrides.get(i, (100, 100, 100, 100)) for i in range(n)]
    o, h, lo, c = (np.array([float(b[j]) for b in bars]) for j in range(4))
    s = rc.bb.Series("SYM", _weekdays(date(2026, 3, 2), n), o, h, lo, c, np.full(n, 1000.0), [])
    return rc.Event("SYM", s, 0, 1, Decimal("100.00"), atr, levels)


BRK_LEVELS = RawLevels(entry=Decimal("125.00"), stop=Decimal("120.00"), target=Decimal("135.00"))


@pytest.mark.parametrize("strategy, arm, overrides, horizon, reason, exit_idx, exit_px", [
    ("hi52", SHIPPED, {3: (99, 99, 93, 95)}, 19, "stop", 3, "94.00"),
    ("hi52", SHIPPED, {3: (90, 91, 89, 90)}, 19, "stop", 3, "90.00"),            # gap through: the open
    ("hi52", SHIPPED, {1: (100, 100, 80, 100)}, 19, "time", 20, "100.00"),       # fill session not evaluated
    ("ins", SHIPPED, {3: (99, 99, 94, 95)}, 19, "stop", 3, "95.00"),
    ("hi52", TIME, {3: (99, 99, 50, 95), 20: (100, 101, 100, 101)}, 19, "time", 20, "101.00"),
    ("ins", CAT, {3: (97, 98, 94, 96)}, 19, "stop", 3, "95.00"),                 # 100 - 2.5 x ATR 2.0
    ("brk20", SHIPPED, {3: (101, 109, 100, 108)}, 19, "target", 3, "108.00"),    # 135/125 of the fill
    ("brk20", SHIPPED, {3: (110, 112, 109, 111)}, 19, "target", 3, "110.00"),    # gap through: the open
    ("brk20", SHIPPED, {3: (100, 110, 95, 100)}, 19, "stop", 3, "96.00"),        # both touched: stop
    ("brk20", TIME, {21: (100, 103, 100, 103)}, 20, "time", 21, "103.00"),       # registered close(fill+20)
])
def test_arm_exits(strategy, arm, overrides, horizon, reason, exit_idx, exit_px):
    ev = _event(overrides, levels=BRK_LEVELS)
    stop, target = c1.arm_levels(arm, strategy, ev)
    o = rc.walk(ev, stop=stop, target=target, horizon=horizon, cost_pct=Decimal("0.3"), ex_dates=())
    assert (o.status, o.reason, o.exit_d, o.exit_px) == ("exit", reason, ev.series.dates[exit_idx], Decimal(exit_px))
    assert o.net_pct == (Decimal(exit_px) - 100) - Decimal("0.3")


def test_ex_date_inside_the_hold_voids():
    ev = _event({})
    o = rc.walk(ev, stop=None, target=None, horizon=19, cost_pct=Decimal(0), ex_dates=[ev.series.dates[5]])
    assert o.status == "void_ca"


@pytest.mark.parametrize("prom_i, med_i, prom_iii, med_iii, outcome", [
    (True, 1.0, True, 2.0, CAT),
    (True, 1.0, False, 2.0, SHIPPED),
    (True, 0.0, True, -1.0, c1.NEITHER),       # CPCV passes but median net <= 0
    (False, 1.0, False, 2.0, c1.NEITHER),
])
def test_decision_rule(prom_i, med_i, prom_iii, med_iii, outcome):
    cpcv = {SHIPPED: {"promotable": prom_i}, CAT: {"promotable": prom_iii}}
    d = c1.decide(cpcv, {SHIPPED: med_i, TIME: 1.5, CAT: med_iii})
    assert d["outcome"] == outcome
    assert d["cost_of_stop_pp"] == {"ii_minus_i": round(1.5 - med_i, 4), "ii_minus_iii": round(1.5 - med_iii, 4)}


# ----------------------------------------------------------------------------- refusals
@pytest.mark.parametrize("now, refused", [
    (datetime(2026, 10, 7, 10, 0, tzinfo=IST), True),
    (datetime(2026, 10, 7, 9, 14, tzinfo=IST), False),
    (datetime(2026, 10, 7, 15, 30, tzinfo=IST), False),
    (datetime(2026, 10, 2, 10, 0, tzinfo=IST), False),   # Gandhi Jayanti holiday
])
def test_market_hours_refusal(now, refused):
    assert (rc.market_hours_refusal(now) is not None) is refused


def test_live_store_is_refused(tmp_path):
    with pytest.raises(rc.Refusal, match="live store"):
        rc.open_snapshot(tmp_path / "market.duckdb")


# ----------------------------------------------------------------------------- smoke run
DATES = _weekdays(date(2026, 1, 5), 190)
END = DATES[-1]


def _bars(sym: str, rows: list[tuple]) -> list[DailyBar]:
    return [DailyBar(symbol=sym, d=DATES[i], open=Decimal(str(o)), high=Decimal(str(h)), low=Decimal(str(lo)),
                     close=Decimal(str(c)), volume=v, src="bhavcopy") for i, (o, h, lo, c, v) in enumerate(rows)]


def _brk20_rows() -> list[tuple]:
    rows = [(100, 100, 99, 100, 1000)] * len(DATES)
    rows[100] = (100, 104, 100, 104, 2000)                    # fresh 20-day breakout on 2x volume
    rows[101] = (103, 105, 99, 101, 1000)                     # touches the 100.00 level: V2-5 fill
    return rows


def _hi52_rows() -> list[tuple]:
    rows = [(200, 200, 150, 150, 1000)] + [(150, 150, 150, 150, 1000)] * (len(DATES) - 1)
    prev = 150.0
    for k in range(1, 21):                                    # smooth climb; crosses 0.95 x 200 at k = 20
        c = round(150 * 1.0125 ** k, 2)
        rows[130 + k] = (prev, c, prev, c, 1000)
        prev = c
    return rows[:151] + [(prev, prev, prev, prev, 1000)] * (len(DATES) - 151)


def _seed(tmp_path: Path, clock) -> Path:
    db = tmp_path / "research" / f"market_{END}.duckdb"
    store = MarketStore(db, tmp_path / "parquet", clock).open()
    try:
        store.upsert_bars_1d(
            _bars("BRKA", _brk20_rows()) + _bars("HIA", _hi52_rows())
            + _bars("INSA", [(100, 101, 99, 100, 1000)] * len(DATES))
            + _bars("ZZZ", [(100, 100, 100, 100, 1000)] * len(DATES))
        )
        store.upsert_universe_daily([
            {"d": d, "symbol": sym, "included": True, "exclusion_reasons": None,
             "median_traded_value": Decimal("1e8")}
            for d, syms in ((rc.BRK20_UNIVERSE_AS_OF, ("BRKA", "ZZZ")), (END, ("INSA", "ZZZ")))
            for sym in syms
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


def test_max_symbols_smoke_run(tmp_path, clock, monkeypatch, capsys):
    monkeypatch.setattr(rc, "market_hours_refusal", lambda: None)
    monkeypatch.setattr(rc, "INS_REGISTERED_WINDOW", (DATES[0], END))
    db = _seed(tmp_path, clock)
    out = tmp_path / "reports" / "c1.json"
    assert c1.main(["--db", str(db), "--max-symbols", "1", "--end", str(END), "--out", str(out)]) == 0
    doc = json.loads(out.read_text(encoding="utf-8"))
    for name in c1.STRATEGIES:
        block = doc["strategies"][name]
        assert (block["population"]["n_symbols"], block["population"]["n_events"]) == (1, 1)
        assert all(block["arms"][a][c1.DECISION_HORIZON]["n"] == 1 for a in c1.ARMS)
    assert "close(fill+20)" in doc["strategies"]["brk20"]["arms"][TIME]
    assert doc["meta"]["population_is_survivorship_tainted_proxy"] is True
    assert doc["meta"]["registered_population"] is False
    assert out.with_suffix(".md").exists()
    text = capsys.readouterr().out
    assert text.isascii() and text.index("GEOMETRY FIRST") < text.index("CPCV")


# ----------------------------------------------------------------------------- baseline reproduction
_SNAPSHOTS = sorted((_ROOT / "data" / "research").glob("market_*.duckdb"))
_REGISTERED = _ROOT / "data" / "reports" / "backtest_brk20_2026-09-12.json"


@pytest.mark.skipif(not (_SNAPSHOTS and _REGISTERED.exists()), reason="no research snapshot or registered report")
def test_brk20_reference_arm_reproduces_the_registered_baseline():
    """Arm (ii) at close(fill+20) re-measures V2_limit_at_H20_N5 at horizon 20 (2026-09-12 report).

    The sets differ only at the edges: --end 2026-08-31 drops roughly the last 8 sessions of
    admissible fills (~1% of n), and C1 excludes exit_sim void_ca trades the registered run kept
    (~0.5%). Removing ~1.5% of the trades on a return-agnostic basis moves n by that much and the
    median by a few hundredths of a point, so n within 3% and median net within 0.10 pp; a larger
    gap means the population or the walk drifted from the registered construct.
    """
    reg = json.loads(_REGISTERED.read_text(encoding="utf-8"))
    cell = reg["variants"]["V2_limit_at_H20_N5"]["cells"]["all"]["horizons"]["20"]
    conn = rc.open_snapshot(_SNAPSHOTS[-1])
    try:
        doc = c1.run(conn, _SNAPSHOTS[-1], date(2026, 8, 31), strategies=("brk20",))
    finally:
        conn.close()
    got = doc["strategies"]["brk20"]["arms"][TIME]["close(fill+20)"]
    cost_shift = doc["meta"]["cost_round_trip_pct"] - reg["meta"]["cost_round_trip_pct"]
    assert abs(got["n"] / cell["n"] - 1) <= 0.03
    assert abs(got["median_net"] + cost_shift - cell["median_net"]) <= 0.10
