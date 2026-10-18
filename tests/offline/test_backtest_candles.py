"""scripts/backtest_candles.py - smoke test of the refuted candle battery's CLI on synthetic 1m bars.

The tape is a temp :class:`MarketStore` holding ONE session of 09:15..15:29 one-minute bars for the
cast below, flat filler names ``FILL000..FILL159`` (enough to clear ``session_min_symbols`` without
overriding the pre-registered PARAMS) and 20 prior daily bars per measured symbol. The test checks the
JSON document's conventions and shape, the ``R1|E1`` cell, and the ASCII report.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]


def _load(name: str, rel: str):
    spec = importlib.util.spec_from_file_location(name, _ROOT / rel)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


bt = _load("mt_backtest_candles", "scripts/backtest_candles.py")

from engine.core.clock import IST  # noqa: E402
from engine.core.types import Bar  # noqa: E402
from engine.marketdata.store import DailyBar, MarketStore  # noqa: E402

D = date(2026, 3, 2)                      # a Monday; one session is the whole fixture
N_BUCKETS = 75                            # 09:15..15:29 = 375 one-minute bars = 75 five-minute bars
FLAT100 = (100.0, 100.0, 100.0, 100.0, 1000)
N_FILLERS = 160


# --------------------------------------------------------------------------- tape construction
def _bar(sym: str, mfo: int, o: float, h: float, low: float, c: float, vol: int) -> Bar:
    total = 9 * 60 + 15 + mfo
    return Bar(
        symbol=sym,
        ts_minute=datetime.combine(D, time(total // 60, total % 60), tzinfo=IST),
        open=Decimal(str(round(o, 2))), high=Decimal(str(round(h, 2))),
        low=Decimal(str(round(low, 2))), close=Decimal(str(round(c, 2))),
        volume=int(vol), src="kite_official",
    )


def _bucket(sym: str, bk: int, spec, minutes=(0, 1, 2, 3, 4)) -> list[Bar]:
    """Five 1m bars whose 5m aggregate is EXACTLY ``spec`` = (open, high, low, close, volume).

    The high is printed in minute 1, the low in minute 2 and the close in minute 4, so the aggregate
    is (first open, max high, min low, last close, sum volume) by construction.
    """
    o, h, low, c, vol = spec
    per = int(vol) // 5
    shape = [
        (o, o, o, o),
        (o, h, o, o),
        (o, o, low, o),
        (o, o, o, o),
        (o, max(o, c), min(o, c), c),
    ]
    return [_bar(sym, bk * 5 + i, *shape[i], per) for i in minutes]


def _session(sym: str, segments, *, gappy_bk: int | None = None) -> list[Bar]:
    specs: dict[int, tuple] = {}
    for lo, hi, spec in segments:
        for bk in range(lo, hi + 1):
            specs[bk] = spec
    bars: list[Bar] = []
    for bk in range(N_BUCKETS):
        spec = specs.get(bk)
        if spec is None:
            continue
        bars += _bucket(sym, bk, spec, minutes=(0, 1) if bk == gappy_bk else (0, 1, 2, 3, 4))
    return bars


CAST: dict[str, list] = {
    # The R1|E1 cell books exactly two trades, both entered at 102.00: R1_POS out at the E1 target
    # 106.00 (the 107 print) and TWICE once (one trade per symbol per day). R1_NEG (1.9x volume) and
    # SHORTDAY (under the medvol20 floor) must not add a third. The R2..R5 pairs populate the other
    # cells, whose outcomes the smoke test does not assert.
    "R1_POS": [(0, 9, FLAT100), (10, 10, (100.0, 102.0, 100.0, 102.0, 2000)),
               (11, 11, (102.0, 102.0, 102.0, 102.0, 1000)),
               (12, 12, (102.0, 107.0, 102.0, 103.0, 1000)),
               (13, 13, (103.0, 103.0, 101.0, 101.0, 1000)),
               (14, 74, (100.5, 100.5, 100.5, 100.5, 1000))],
    "R1_NEG": [(0, 9, FLAT100), (10, 10, (100.0, 102.0, 100.0, 102.0, 1900)),
               (11, 74, (102.0, 102.0, 102.0, 102.0, 1000))],
    "R2_POS": [(0, 7, FLAT100), (8, 8, (100.1, 101.0, 100.1, 101.0, 1000)),
               (9, 9, (101.0, 102.0, 101.0, 102.0, 1000)),
               (10, 10, (102.0, 103.0, 102.0, 103.0, 1600)),
               (11, 72, (103.0, 103.0, 103.0, 103.0, 1000)),
               (73, 74, (200.0, 200.0, 200.0, 200.0, 1000))],
    "R2_NEG": [(0, 7, FLAT100), (8, 8, (100.1, 101.0, 100.1, 101.0, 1000)),
               (9, 9, (101.0, 102.0, 101.0, 102.0, 1000)),
               (10, 10, (102.0, 103.0, 102.0, 103.0, 1400)),
               (11, 74, (103.0, 103.0, 103.0, 103.0, 1000))],
    "R3_POS": [(0, 5, FLAT100), (6, 6, (100.0, 106.0, 100.0, 105.5, 1000)),
               (7, 7, (105.5, 105.5, 105.0, 105.0, 1000)),
               (8, 8, (105.0, 105.0, 104.0, 104.0, 1000)),
               (9, 9, (104.0, 104.0, 103.0, 103.0, 1000)),
               (10, 10, (103.0, 105.0, 103.0, 104.5, 1300)),
               (11, 11, (104.5, 104.5, 104.5, 104.5, 1000)),
               (12, 12, (104.5, 104.5, 102.5, 103.0, 1000)),
               (13, 74, (102.0, 102.0, 102.0, 102.0, 1000))],
    "R3_NEG": [(0, 5, FLAT100), (6, 6, (100.0, 106.0, 100.0, 105.5, 1000)),
               (7, 7, (105.5, 105.5, 105.0, 105.0, 1000)),
               (8, 8, (105.0, 105.0, 104.0, 104.0, 1000)),
               (9, 9, (104.0, 104.0, 103.0, 103.0, 1000)),
               (10, 10, (103.0, 105.0, 103.0, 104.5, 1100)),
               (11, 74, (104.5, 104.5, 104.5, 104.5, 1000))],
    "R4_POS": [(0, 3, FLAT100), (4, 9, (101.0, 101.0, 101.0, 101.0, 1000)),
               (10, 10, (101.0, 101.2, 100.6, 101.2, 1000)),
               (11, 74, (101.2, 101.2, 101.2, 101.2, 1000))],
    "R4_NEG": [(0, 3, FLAT100), (4, 9, (101.0, 101.0, 101.0, 101.0, 1000)),
               (10, 10, (101.0, 101.2, 101.0, 101.2, 1000)),
               (11, 74, (101.2, 101.2, 101.2, 101.2, 1000))],
    "R5_POS": [(0, 3, FLAT100), (4, 9, (99.0, 99.0, 99.0, 99.0, 1000)),
               (10, 10, (99.0, 100.5, 99.0, 100.5, 1600)),
               (11, 74, (100.5, 100.5, 100.5, 100.5, 1000))],
    "R5_NEG": [(0, 3, FLAT100), (4, 9, (99.0, 99.0, 99.0, 99.0, 1000)),
               (10, 10, (99.0, 100.5, 99.0, 100.5, 1400)),
               (11, 74, (100.5, 100.5, 100.5, 100.5, 1000))],
    "TWICE": [(0, 9, FLAT100), (10, 10, (100.0, 102.0, 100.0, 102.0, 2000)),
              (11, 19, (102.0, 102.0, 102.0, 102.0, 1000)),
              (20, 20, (102.0, 104.0, 102.0, 104.0, 3000)),
              (21, 74, (104.0, 104.0, 104.0, 104.0, 1000))],
    "SHORTDAY": [(6, 9, FLAT100), (10, 10, (100.0, 102.0, 100.0, 102.0, 2000)),
                 (11, 20, (102.0, 102.0, 102.0, 102.0, 1000))],
    "GAPPY": [(0, 74, FLAT100)],
}


def _tape() -> list[Bar]:
    bars: list[Bar] = []
    for sym, segs in CAST.items():
        bars += _session(sym, segs, gappy_bk=3 if sym == "GAPPY" else None)
    for i in range(N_FILLERS):
        bars += _session(f"FILL{i:03d}", [(0, 74, FLAT100)])
    return bars


# --------------------------------------------------------------------------- the DAILY tape (R3)
#: Constant per-symbol daily bars around a close of 100.00, so every true range is the plain
#: high-low and ATR% is the range itself: the six measured symbols carry ATR% 1..6.
DAILY_ATR_PCT: dict[str, float] = {
    "R1_POS": 1.0, "R2_POS": 2.0, "R3_POS": 3.0, "R4_POS": 4.0, "R5_POS": 5.0, "TWICE": 6.0,
}
N_DAILY_SESSIONS = 20                      # more than the 14-session lookback


def _weekdays_before(n: int, before: date) -> list[date]:
    out: list[date] = []
    d = before - timedelta(days=1)
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d -= timedelta(days=1)
    return sorted(out)


def _daily_bar(sym: str, d: date, high: float, low: float, close: float) -> DailyBar:
    return DailyBar(
        symbol=sym, d=d, open=Decimal(str(round(close, 2))), high=Decimal(str(round(high, 2))),
        low=Decimal(str(round(low, 2))), close=Decimal(str(round(close, 2))), volume=1000,
        src="kite_official",
    )


def _daily_tape() -> list[DailyBar]:
    """20 prior daily sessions per measured symbol, plus a deliberately absurd SIGNAL-DAY bar."""
    bars: list[DailyBar] = []
    for sym, pct in DAILY_ATR_PCT.items():
        for d in _weekdays_before(N_DAILY_SESSIONS, D):
            bars.append(_daily_bar(sym, d, 100.0 + pct / 2.0, 100.0 - pct / 2.0, 100.0))
        bars.append(_daily_bar(sym, D, 150.0, 50.0, 100.0))
    return bars


@pytest.fixture(scope="module")
def module_clock():
    from engine.core.clock import Clock
    return Clock()


@pytest.fixture(scope="module")
def db_file(tmp_path_factory, module_clock) -> Path:
    """A temp market.duckdb holding the synthetic tape, CLOSED so the study can attach read-only."""
    root = tmp_path_factory.mktemp("candles")
    path = root / "market.duckdb"
    store = MarketStore(path, root / "parquet", module_clock).open()
    try:
        tape = _tape()
        for i in range(0, len(tape), 20000):
            store.insert_bars_1m(tape[i: i + 20000])
        store.upsert_bars_1d(_daily_tape())
    finally:
        store.close()
    return path


def test_main_writes_json_and_prints_an_ascii_table(db_file, tmp_path, capsys):
    out = tmp_path / "results" / "candles.json"
    rc = bt.main(["--db", str(db_file), "--out", str(out), "--start", str(D), "--end", str(D)])
    assert rc == 0
    doc = json.loads(out.read_text(encoding="utf-8"))
    assert doc["meta"]["product"] == "MIS"
    assert "OPEN of the 1m bar at signal.last_minute + 1" in doc["meta"]["entry_convention"]
    assert "next 1m OPEN" in doc["meta"]["exit_conventions"]["E2"]
    assert len(doc["results"]) == 10
    assert doc["results"]["R1|E1"]["example_trades"]                  # the audit sample is on record
    # R1 books R1_POS (E1 target 106.00) and TWICE (its 104.00 squareoff), both entered at 102.00.
    r1 = doc["results"]["R1|E1"]["splits"][bt.SPLIT_ALL]
    assert r1["n"] == 2
    assert r1["mean_gross_pct"] == pytest.approx(((106.0 + 104.0) / 2 / 102.0 - 1.0) * 100.0, abs=1e-3)

    text = capsys.readouterr().out
    assert text.isascii()                                             # the Windows console is cp1252
    assert text.index("STEP 1 - SAMPLE SHAPE") < text.index("STEP 2 - RESULTS")
    assert "promotable=True in NO rule x exit x split cell." in text
    assert str(out) in text
