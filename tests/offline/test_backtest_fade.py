"""scripts/backtest_fade.py - smoke test of the refuted fade (SHORT) harness on synthetic bars.

The synthetic tape is a temp :class:`MarketStore` holding 30 sessions of one-minute bars over an
engineered cast. The last session (``SIGNAL_DAY``) is the one under test; the 29 before it give
``rel_volume_tod`` its 20 prior sessions at a known constant volume. Every F1 name shares one morning
skeleton - the 09:15 open at 100.00, one 100.50 print at 09:20 so the OPENING RANGE HIGH IS 100.50,
and a 101.00 stretch that is above it - so the cast differs only in the clause it exercises.

The F1 cast on ``SIGNAL_DAY`` (decision bar 11:00, entry 11:01):

* ``FADER``     - the fade that triggers, held to the 15:15 squareoff at 98.00, a +2.00% gross SHORT.
* ``FAILFADE``  - same signal, then the fade FAILS: the 11:05 bar closes at 101.00 and the 11:06
  bar opens at 104.00, the fill: -4.00% gross.
* ``STOPPOKE``  - same signal; the 11:03 bar's HIGH pokes to 103.00 while its close stays at 100.00,
  so under the registered CLOSE trigger it squares off at 99.00: +1.00% gross.
* ``NOBREAK``, ``STILLUP``, ``THINVOL``, ``LATEBREAK``, ``EXDATED`` - each fails one F1 clause.
* ``OUTSIDER``  - FADER's exact path but ``exclusion_reasons == ['not_in_index']``: never scanned.

The F2 cast (decision bar 10:00, entry 10:01; prior daily close 100.00 for all of them):
``GAPPER`` and ``GAPFAIL`` trigger; ``SMALLGAP`` and ``GAPHOLD`` do not.

``NIFTY 50`` is -0.30% from its open at 10:00 and +0.50% at 11:00.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "backtest_fade.py"
_spec = importlib.util.spec_from_file_location("mt_backtest_fade", _SCRIPT)
bt = importlib.util.module_from_spec(_spec)
sys.modules["mt_backtest_fade"] = bt
_spec.loader.exec_module(bt)

from engine.core.clock import IST  # noqa: E402
from engine.core.types import Bar  # noqa: E402
from engine.marketdata.store import DailyBar, MarketStore  # noqa: E402

N_SESSIONS = 30
START = date(2026, 3, 2)                 # a Monday
OPEN_T = time(9, 15)
CLOSE_T = time(15, 29)                   # ts_minute is the bar's minute START -> 375 bars/session
BASE_VOL = 1000
INDEX = "NIFTY 50"
OR_HIGH = 100.5                          # the 09:20 print; every F1 name shares it
PRIOR_CLOSE = 100.0


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
PRIOR_DAY = SESSIONS[-2]
EX_DATE = SIGNAL_DAY - timedelta(days=20)          # inside the 35-day veto window


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

T11 = time(11, 0)
T10 = time(10, 0)


def _bar(sym: str, d: date, t: time, o: float, h: float, lo: float, c: float, vol: int) -> Bar:
    return Bar(
        symbol=sym,
        ts_minute=datetime.combine(d, t, tzinfo=IST),
        open=Decimal(str(round(o, 2))),
        high=Decimal(str(round(max(h, o, c), 2))),
        low=Decimal(str(round(min(lo, o, c), 2))),
        close=Decimal(str(round(c, 2))),
        volume=vol,
        src="kite_official",
    )


def _session(
    sym: str,
    d: date,
    price,
    vol: int,
    overrides: dict[time, tuple[float, float, float, float]] | None = None,
) -> list[Bar]:
    """One session: a flat bar at ``price(t)`` each minute, except the ``overrides`` minutes."""
    bars: list[Bar] = []
    for t in MINUTES:
        if overrides and t in overrides:
            o, h, lo, c = overrides[t]
        else:
            p = float(price(t))
            o = h = lo = c = p
        bars.append(_bar(sym, d, t, o, h, lo, c, vol))
    return bars


def _flat(px: float):
    return lambda _t: px


# --------------------------------------------------------------------------- the F1 morning shapes
def _f1_price(morning: float, late: float, afternoon: float, squareoff_px: float):
    """09:15-09:29 the shared opening range (OR high 100.50), then ``morning`` until 10:30, ``late``
    until 11:00, ``afternoon`` until 15:14, ``squareoff_px`` at 15:15 and 90.00 after it (a price the
    15:15 exit must never reach)."""
    def price(t: time) -> float:
        if t < time(9, 30):
            return OR_HIGH if t == time(9, 20) else 100.0
        if t < time(10, 30):
            return morning
        if t <= T11:
            return late
        if t < time(15, 15):
            return afternoon
        return squareoff_px if t == time(15, 15) else 90.0
    return price


FADER_PRICE = _f1_price(101.0, 100.0, 100.0, 98.0)

#: FAILFADE: the 11:05 bar CLOSES at 101.00 - above the OR high (100.50), and NOT above the session
#: high (101.00), so it is the FADE-FAILURE exit and not the stop - and the 11:06 bar OPENS at 104.00.
FAILFADE_OVERRIDES: dict[time, tuple[float, float, float, float]] = {
    time(11, 5): (100.0, 101.0, 100.0, 101.0),
}


def _failfade_price(t: time) -> float:
    if t > time(11, 5):
        return 104.0
    return FADER_PRICE(t)


#: STOPPOKE: the 11:03 bar's HIGH reaches 103.00 (above the 101.00 session high) while its CLOSE
#: stays at 100.00 (below the 100.50 OR high). Nothing fires under the close trigger.
STOPPOKE_OVERRIDES: dict[time, tuple[float, float, float, float]] = {
    time(11, 3): (100.0, 103.0, 100.0, 100.0),
}
STOPPOKE_PRICE = _f1_price(101.0, 100.0, 100.0, 99.0)


def _latebreak_price(t: time) -> float:
    if t < time(9, 30):
        return OR_HIGH if t == time(9, 20) else 100.0
    if t < time(10, 30):
        return 100.0                                   # nothing above the OR high inside the window
    if t < time(10, 45):
        return 101.0                                   # the break, but too late to count
    if t <= T11:
        return 100.0
    return 98.0 if t == time(15, 15) else (100.0 if t < time(15, 15) else 90.0)


# --------------------------------------------------------------------------- the F2 shapes
#: GAPPER/GAPFAIL/GAPHOLD open at 103.00 against a 100.00 prior close (+3.0%). The 09:15 bar's HIGH
#: is 103.50, which is the session high at 10:00 and therefore the stop; the 10:00 bar's HIGH is
#: 102.50, which is the fade-failure level.
GAP_OPEN_BAR = (103.0, 103.5, 103.0, 103.2)
GAP_T_BAR = (102.5, 102.5, 102.0, 102.0)


def _gap_price(after_entry: float, squareoff_px: float):
    def price(t: time) -> float:
        if t < T10:
            return 102.8
        if t < time(15, 15):
            return after_entry
        return squareoff_px if t == time(15, 15) else 90.0
    return price


def _gapper_session(sym: str) -> list[Bar]:
    return _session(
        sym, SIGNAL_DAY, _gap_price(101.0, 100.0), BASE_VOL,
        overrides={OPEN_T: GAP_OPEN_BAR, T10: GAP_T_BAR, time(10, 1): (102.0, 102.0, 102.0, 102.0)},
    )


def _gapfail_session(sym: str) -> list[Bar]:
    def price(t: time) -> float:
        if t < T10:
            return 102.8
        if t <= time(10, 4):
            return 102.0
        if t < time(15, 15):
            return 106.0
        return 100.0 if t == time(15, 15) else 90.0
    return _session(
        sym, SIGNAL_DAY, price, BASE_VOL,
        overrides={
            OPEN_T: GAP_OPEN_BAR,
            T10: GAP_T_BAR,
            time(10, 1): (102.0, 102.0, 102.0, 102.0),
            time(10, 5): (102.0, 103.0, 102.0, 103.0),      # closes above the 10:00 bar high
            time(10, 6): (106.0, 106.0, 106.0, 106.0),      # ... and fills four rupees away
        },
    )


def _gaphold_session(sym: str) -> list[Bar]:
    def price(t: time) -> float:
        if t < time(15, 15):
            return 104.0
        return 104.0 if t == time(15, 15) else 90.0
    return _session(sym, SIGNAL_DAY, price, BASE_VOL, overrides={OPEN_T: GAP_OPEN_BAR})


def _index_price(t: time) -> float:
    if t < time(9, 30):
        return 20000.0
    return 19940.0 if t <= T10 else 20100.0            # -0.30% at 10:00, +0.50% at 11:00


# --------------------------------------------------------------------------- the tape
F1_CAST = ("FADER", "FAILFADE", "STOPPOKE", "NOBREAK", "STILLUP", "THINVOL", "LATEBREAK",
           "EXDATED", "OUTSIDER")
F2_CAST = ("GAPPER", "GAPFAIL", "SMALLGAP", "GAPHOLD")

#: ``(high, low)`` of every daily bar, so ATR(14,1d) is exactly ``high - low``.
DAILY_RANGE: dict[str, float] = {
    "FADER": 1.0, "FAILFADE": 2.0, "STOPPOKE": 3.0, "GAPPER": 2.0, "GAPFAIL": 3.0,
}

UNIVERSE_EXCLUSIONS: dict[str, list[str]] = {
    "THINVOL": ["watchlist_cap"],                      # cap-only rows ARE eligible
    "OUTSIDER": ["not_in_index"],                      # out-of-index rows are NOT
}


def _tape() -> list[Bar]:
    bars: list[Bar] = []
    for sym in F1_CAST:
        for d in SESSIONS[:-1]:
            bars += _session(sym, d, _flat(100.0), BASE_VOL)
    for sym in F2_CAST:
        bars += _session(sym, PRIOR_DAY, _flat(100.0), BASE_VOL)
    for d in SESSIONS[:-1]:
        bars += _session(INDEX, d, _flat(20000.0), 0)

    two_x = BASE_VOL * 2
    bars += _session("FADER", SIGNAL_DAY, FADER_PRICE, two_x)
    bars += _session("EXDATED", SIGNAL_DAY, FADER_PRICE, two_x)
    bars += _session("OUTSIDER", SIGNAL_DAY, FADER_PRICE, two_x)
    bars += _session("FAILFADE", SIGNAL_DAY, _failfade_price, two_x, overrides=FAILFADE_OVERRIDES)
    bars += _session("STOPPOKE", SIGNAL_DAY, STOPPOKE_PRICE, two_x, overrides=STOPPOKE_OVERRIDES)
    bars += _session("THINVOL", SIGNAL_DAY, FADER_PRICE, BASE_VOL)
    bars += _session("NOBREAK", SIGNAL_DAY, _f1_price(100.0, 100.0, 100.0, 98.0), two_x)
    bars += _session("STILLUP", SIGNAL_DAY, _f1_price(101.0, 101.0, 101.0, 98.0), two_x)
    bars += _session("LATEBREAK", SIGNAL_DAY, _latebreak_price, two_x)

    bars += _gapper_session("GAPPER")
    bars += _gapfail_session("GAPFAIL")
    bars += _session("SMALLGAP", SIGNAL_DAY, _flat(100.5), BASE_VOL,
                     overrides={OPEN_T: (101.0, 101.0, 101.0, 101.0)})
    bars += _gaphold_session("GAPHOLD")
    bars += _session(INDEX, SIGNAL_DAY, _index_price, 0)
    return bars


def _daily() -> list[DailyBar]:
    out: list[DailyBar] = []
    for sym in (*F1_CAST, *F2_CAST):
        rng = DAILY_RANGE.get(sym, 1.0)
        for d in SESSIONS:
            out.append(DailyBar(
                symbol=sym, d=d,
                open=Decimal("100.00"),
                high=Decimal(str(round(PRIOR_CLOSE + rng / 2, 2))),
                low=Decimal(str(round(PRIOR_CLOSE - rng / 2, 2))),
                close=Decimal("100.00"), volume=100000, src="bhavcopy",
            ))
    return out


def _universe_rows() -> list[dict]:
    return [
        {"d": SIGNAL_DAY, "symbol": sym, "included": not UNIVERSE_EXCLUSIONS.get(sym),
         "exclusion_reasons": UNIVERSE_EXCLUSIONS.get(sym),
         "median_traded_value": Decimal("50000000.00")}
        for sym in (*F1_CAST, *F2_CAST)
    ]


@pytest.fixture(scope="module")
def module_clock():
    from engine.core.clock import Clock
    return Clock()


@pytest.fixture(scope="module")
def db_file(tmp_path_factory, module_clock) -> Path:
    """A temp market.duckdb holding the synthetic tape, CLOSED so the study can attach read-only."""
    root = tmp_path_factory.mktemp("fade")
    path = root / "market.duckdb"
    store = MarketStore(path, root / "parquet", module_clock).open()
    try:
        bars = _tape()
        for i in range(0, len(bars), 20000):
            store.insert_bars_1m(bars[i: i + 20000])
        store.upsert_bars_1d(_daily())
        store.upsert_corp_actions([{
            "symbol": "EXDATED", "ex_date": EX_DATE, "kind": "bonus", "ratio": "1:1",
            "amount": None, "source": "test", "recorded_at": module_clock.now(),
        }])
        store.upsert_universe_daily(_universe_rows())
    finally:
        store.close()
    return path


def test_main_writes_json_and_markdown_and_prints_an_ascii_table(db_file, tmp_path, capsys):
    out = tmp_path / "results" / "fade.json"
    rc = bt.main(["--db", str(db_file), "--out", str(out),
                  "--start", str(SESSIONS[0]), "--end", str(SESSIONS[-1])])
    assert rc == 0
    doc = json.loads(out.read_text(encoding="utf-8"))
    assert doc["meta"]["product"] == "MIS"
    assert doc["meta"]["research_id"] == "R4"
    assert "SHORT at the OPEN of the bar immediately after T" in doc["meta"]["entry_convention"]
    assert "FOLLOWING bar OPEN" in doc["meta"]["exit_convention"]
    assert doc["coverage"]["bars_1m_by_year"]
    # F1 books FADER +2.00%, FAILFADE -4.00% and STOPPOKE +1.00% gross; F2 books GAPPER and GAPFAIL.
    f1 = doc["results"]["F1"]["splits"][bt.SPLIT_ALL]
    assert f1["n"] == 3
    assert f1["median_gross_pct"] == pytest.approx(1.0, abs=1e-3)
    assert doc["results"]["F2"]["splits"][bt.SPLIT_ALL]["n"] == 2

    md = out.with_suffix(".md").read_text(encoding="utf-8")
    assert "Outcome: **REFUTED**" in md
    assert "Geometry first" in md

    text = capsys.readouterr().out
    assert text.isascii()                                      # the Windows console is cp1252
    assert text.index("STEP 1 - SAMPLE SHAPE") < text.index("STEP 3 - GEOMETRY FIRST")
    assert text.index("STEP 3 - GEOMETRY FIRST") < text.index("STEP 4 - RESULTS")
    assert "promotable=True in NO REGISTERED rule x split cell" in text
    assert str(out) in text
