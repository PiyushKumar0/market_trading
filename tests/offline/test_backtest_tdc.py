"""scripts/backtest_tdc.py - smoke test of the refuted tdc harness on synthetic 1m bars.

The synthetic tape is a temp :class:`MarketStore` holding 30 sessions x a handful of engineered
symbols, each session a full 09:15..15:29 run of one-minute bars. The last session (``SIGNAL_DAY``)
is the one under test; the 29 before it give ``rel_volume_tod`` its 20 prior sessions at a known
constant volume. On ``SIGNAL_DAY`` only ``WINNER`` (flat at 102.00 after entry, squared off at 15:15)
and ``STOPPER`` (loses VWAP: the 11:05 close triggers, the 11:06 open of 95.00 fills) clear every
gate; ``THINVOL``, ``LAGGARD``, ``DIPPER`` and ``NOHIST`` each fail one. ``NIFTY 50`` is the index.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "backtest_tdc.py"
_spec = importlib.util.spec_from_file_location("mt_backtest_tdc", _SCRIPT)
bt = importlib.util.module_from_spec(_spec)
sys.modules["mt_backtest_tdc"] = bt
_spec.loader.exec_module(bt)

from engine.core.clock import IST  # noqa: E402
from engine.core.types import Bar  # noqa: E402
from engine.marketdata.store import DailyBar, MarketStore  # noqa: E402

N_SESSIONS = 30
START = date(2026, 3, 2)                 # a Monday
OPEN_T = time(9, 15)
CLOSE_T = time(15, 29)                   # ts_minute is the bar's minute START -> 375 bars/session
BASE_VOL = 1000                          # every prior session's per-minute volume
INDEX = "NIFTY 50"


def _sessions(n: int, start: date = START) -> list[date]:
    out: list[date] = []
    d = start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


SESSIONS = _sessions(N_SESSIONS)
SIGNAL_DAY = SESSIONS[-1]


def _minutes() -> list[time]:
    out: list[time] = []
    t = datetime.combine(date(2000, 1, 1), OPEN_T)
    end = datetime.combine(date(2000, 1, 1), CLOSE_T)
    while t <= end:
        out.append(t.time())
        t += timedelta(minutes=1)
    return out


MINUTES = _minutes()
assert len(MINUTES) == 375


def _bar(sym: str, d: date, t: time, px: float, vol: int, *, o: float | None = None) -> Bar:
    """A flat one-minute bar at ``px`` (so typical price == close == VWAP contribution == px)."""
    op = px if o is None else o
    return Bar(
        symbol=sym,
        ts_minute=datetime.combine(d, t, tzinfo=IST),
        open=Decimal(str(round(op, 2))),
        high=Decimal(str(round(max(op, px), 2))),
        low=Decimal(str(round(min(op, px), 2))),
        close=Decimal(str(round(px, 2))),
        volume=vol,
        src="kite_official",
    )


def _prior_session(sym: str, d: date, px: float = 100.0, vol: int = BASE_VOL) -> list[Bar]:
    """A flat, boring session: every bar at ``px`` with constant volume (an exact RVOL denominator)."""
    return [_bar(sym, d, t, px, vol) for t in MINUTES]


# --------------------------------------------------------------------------- the signal-day paths
def _signal_session(
    sym: str,
    *,
    ret_pct: float,
    vol_mult: float,
    or_high_px: float = 100.5,
    dip_at: time | None = None,
    post_entry: dict[time, tuple[float, float]] | None = None,
) -> list[Bar]:
    """One engineered signal-day tape.

    09:15 opens at 100.00; the opening range (09:15..09:29) tops out at ``or_high_px``; the tape then
    settles at 100.20 until 10:44, and from 10:45 sits at ``close_T`` so the T bar's close gives
    exactly ``ret_pct``. ``dip_at`` drops one bar inside the 15-bar acceptance window below the
    running VWAP. ``post_entry`` overrides ``(open, close)`` for specific minutes after T.
    """
    close_t = 100.0 * (1.0 + ret_pct / 100.0)
    vol = int(BASE_VOL * vol_mult)
    bars: list[Bar] = []
    for t in MINUTES:
        if t < time(9, 30):
            px = or_high_px if t == time(9, 20) else 100.0
        elif t < time(10, 45):
            px = 100.2
        else:
            px = close_t
        o = px
        if post_entry and t in post_entry:
            o, px = post_entry[t]
        if dip_at is not None and t == dip_at:
            px = 99.0                                  # below the running VWAP for that one minute
            o = 99.0
        bars.append(_bar(sym, SIGNAL_DAY, t, px, vol, o=o))
    return bars


#: STOPPER's post-entry path. Entry is the 11:01 OPEN = 102.00. The 11:05 bar CLOSES at 99.00 (well
#: under VWAP(T) * 0.9975) and the 11:06 bar OPENS at 95.00, the fill.
STOPPER_PATH: dict[time, tuple[float, float]] = {
    time(11, 5): (102.0, 99.0),
    time(11, 6): (95.0, 95.0),
}
STOPPER_PATH.update({t: (95.0, 95.0) for t in MINUTES if t > time(11, 6)})


def _tape() -> list[Bar]:
    bars: list[Bar] = []
    cast = ["WINNER", "STOPPER", "THINVOL", "LAGGARD", "DIPPER"]
    for sym in cast:
        for d in SESSIONS[:-1]:
            bars += _prior_session(sym, d)
    for d in SESSIONS[-9:-1]:                          # NOHIST has only 8 prior sessions
        bars += _prior_session("NOHIST", d)
    for d in SESSIONS[:-1]:
        bars += _prior_session(INDEX, d, px=20000.0, vol=0)

    bars += _signal_session("WINNER", ret_pct=2.0, vol_mult=2.0,
                            post_entry={t: (102.0, 102.0) for t in MINUTES if t > time(11, 0)})
    bars += _signal_session("STOPPER", ret_pct=2.0, vol_mult=2.0, post_entry=STOPPER_PATH)
    bars += _signal_session("THINVOL", ret_pct=2.0, vol_mult=1.0,
                            post_entry={t: (102.0, 102.0) for t in MINUTES if t > time(11, 0)})
    bars += _signal_session("LAGGARD", ret_pct=1.2, vol_mult=2.0,
                            post_entry={t: (101.2, 101.2) for t in MINUTES if t > time(11, 0)})
    bars += _signal_session("DIPPER", ret_pct=2.0, vol_mult=2.0, dip_at=time(10, 50),
                            post_entry={t: (102.0, 102.0) for t in MINUTES if t > time(11, 0)})
    bars += _signal_session("NOHIST", ret_pct=2.0, vol_mult=2.0,
                            post_entry={t: (102.0, 102.0) for t in MINUTES if t > time(11, 0)})

    # the index: +0.50% from open at T, so (iii) demands a stock return >= +1.50%
    for t in MINUTES:
        px = 20000.0 if t < time(9, 30) else (20100.0 if t <= time(11, 0) else 20100.0)
        bars.append(_bar(INDEX, SIGNAL_DAY, t, px, 0))
    return bars


# --------------------------------------------------------------------------- the DAILY tape (R3)
#: Constant per-symbol daily bars, so every true range is the plain high-low.
DAILY_OHLC: dict[str, tuple[float, float, float]] = {      # symbol -> (high, low, close)
    "WINNER": (102.0, 100.0, 101.0),
    "STOPPER": (105.0, 100.0, 102.0),
}


def _daily_bar(sym: str, d: date, high: float, low: float, close: float) -> DailyBar:
    return DailyBar(
        symbol=sym, d=d, open=Decimal(str(round(close, 2))), high=Decimal(str(round(high, 2))),
        low=Decimal(str(round(low, 2))), close=Decimal(str(round(close, 2))), volume=1000,
        src="kite_official",
    )


def _daily_tape() -> list[DailyBar]:
    """One daily bar per prior session, plus a deliberately absurd SIGNAL-DAY bar."""
    bars: list[DailyBar] = []
    for sym, (high, low, close) in DAILY_OHLC.items():
        bars += [_daily_bar(sym, d, high, low, close) for d in SESSIONS[:-1]]
        bars.append(_daily_bar(sym, SIGNAL_DAY, 200.0, 100.0, 150.0))
    return bars


@pytest.fixture(scope="module")
def db_file(tmp_path_factory, module_clock) -> Path:
    """A temp market.duckdb holding the synthetic tape, CLOSED so the study can attach read-only."""
    root = tmp_path_factory.mktemp("tdc")
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


@pytest.fixture(scope="module")
def module_clock():
    from engine.core.clock import Clock
    return Clock()


def test_main_writes_json_and_prints_an_ascii_table(db_file, tmp_path, capsys):
    out = tmp_path / "results" / "tdc.json"
    rc = bt.main(["--db", str(db_file), "--out", str(out),
                  "--start", str(SESSIONS[0]), "--end", str(SESSIONS[-1])])
    assert rc == 0
    doc = json.loads(out.read_text(encoding="utf-8"))
    assert doc["meta"]["product"] == "MIS"
    assert "OPEN of the bar immediately after T" in doc["meta"]["entry_convention"]
    assert "FOLLOWING bar OPEN" in doc["meta"]["exit_convention"]
    assert doc["coverage"]["bars_1m_by_year"]                        # the per-year shape is on record
    # H1 books WINNER (102.00 in, 102.00 out) and STOPPER (102.00 in, 95.00 out at the 11:06 open).
    h1 = doc["results"]["H1"]["splits"][bt.SPLIT_ALL]
    assert h1["n"] == 2
    assert h1["median_gross_pct"] == pytest.approx((95.0 / 102.0 - 1.0) * 100.0 / 2, abs=1e-3)

    text = capsys.readouterr().out
    assert text.isascii()                                            # the Windows console is cp1252
    assert text.index("STEP 1 - SAMPLE SHAPE") < text.index("STEP 2 - RESULTS")
    assert "promotable=True in NO variant x split cell." in text
    assert str(out) in text
