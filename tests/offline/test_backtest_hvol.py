"""scripts/backtest_hvol.py - smoke test of the refuted hvol (abnormal-volume swing drift) harness.

A synthetic research snapshot: flat 100.00 / 1,000,000-share series where index ``Y`` = 70 is the one
engineered spike (VR exactly 3.0), run end to end through ``main``.
"""

from __future__ import annotations

import importlib.util
import json
import sys
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


ETF_HEADER = "Symbol,Underlying Asset,SecurityName,DateofListing,MarketLot,ISINNumber,FaceValue,ETF Underlying,Underlying Key"


def _etf_csv(tmp_path: Path, symbols=("SLVR",), name: str = "etf.csv") -> Path:
    p = tmp_path / name
    rows = [f" {s.lower()} ,Silver,X,01-Jan-20,1,INF000000000,1,COMMODITY,Silver" for s in symbols]
    p.write_text("\n".join([ETF_HEADER, *rows]) + "\n", encoding="utf-8")
    return p


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
