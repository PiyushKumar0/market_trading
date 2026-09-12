"""scripts/backtest_fade.py - the pre-registered fade (SHORT) backtest harness, on synthetic bars.

Every fixture is hand-computed so each assertion has an arithmetic answer, not a regression blob.

The synthetic tape is a temp :class:`MarketStore` holding 30 sessions of one-minute bars over an
engineered cast. The last session (``SIGNAL_DAY``) is the one under test; the 29 before it exist so
``rel_volume_tod`` has the 20 prior sessions its median needs, and their volumes are a known constant
so that median is exact. Every F1 name shares one morning skeleton - the 09:15 open at 100.00, one
100.50 print at 09:20 so the OPENING RANGE HIGH IS 100.50, and a 101.00 stretch that is above it -
so the cast differs only in the clause under test.

The F1 cast on ``SIGNAL_DAY`` (decision bar 11:00, entry 11:01):

* ``FADER``     - breaks the OR high at 09:30, back below it at 11:00 on rvol 2.0: the fade that
  triggers, held to the 15:15 squareoff at 98.00, a +2.00% gross SHORT.
* ``FAILFADE``  - same signal, then the fade FAILS: the 11:05 bar closes at 101.00 (above the OR
  high) and the 11:06 bar opens at 104.00. A same-bar fill books -1.00%; the correct following-open
  fill books -4.00%, so the two cannot be confused.
* ``STOPPOKE``  - same signal, then the 11:03 bar's HIGH pokes to 103.00 while its close stays at
  100.00. Under the registered CLOSE trigger nothing fires and it squares off at +1.00%; under the
  reporting-only ``_stop_on_1m_high`` cell the session-high stop (101.00) binds at -1.00%. One
  symbol, two execution models, opposite signs.
* ``NOBREAK``   - never closes above the OR high: no signal.
* ``STILLUP``   - broke and is STILL above the OR high at 11:00: no signal (this is a continuation,
  not a failed breakout).
* ``THINVOL``   - FADER's exact path at 1.0x volume: fails participation alone.
* ``LATEBREAK`` - breaks at 10:30, i.e. OUTSIDE the ``[09:30, 10:30)`` window: no signal.
* ``EXDATED``   - FADER's exact path plus a corp action 20 days back: vetoed by the ex-date window.
* ``OUTSIDER``  - FADER's exact path but ``exclusion_reasons == ['not_in_index']``: out of the
  eligible population, so it is never even scanned.

The F2 cast (decision bar 10:00, entry 10:01; prior daily close 100.00 for all of them):

* ``GAPPER``   - opens +3.0% and the 10:00 close is back below the session open: the gap fade that
  triggers, held to the 15:15 squareoff at 100.00.
* ``GAPFAIL``  - same signal, then the 10:05 close clears the 10:00 bar's HIGH and the 10:06 bar
  opens at 106.00. Under the reporting-only ``F2_1515_only`` cell that exit does not exist and the
  same trade is held to 15:15 - the sign flips.
* ``SMALLGAP`` - opens +1.0%: under the gap floor.
* ``GAPHOLD``  - opens +3.0% but the 10:00 close is ABOVE the session open: no fade.

``NIFTY 50`` is -0.30% from its open at 10:00 and +0.50% at 11:00, so the F2 trades land in the
index-DOWN cell and the F1 trades in the index-UP cell and the split is exercised in both directions.
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
from engine.strategy.cost_model import CostModel  # noqa: E402

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
#: high (101.00), so it is the FADE-FAILURE exit and not the stop - and the 11:06 bar OPENS at
#: 104.00. Three rupees between the triggering close and the actual fill.
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

#: ``(high, low)`` of every daily bar, so ATR(14,1d) is exactly ``high - low`` and the tercile cells
#: are hand-known: FADER 1%, FAILFADE 2%, STOPPOKE 3% of a 100.00 close.
DAILY_RANGE: dict[str, float] = {
    "FADER": 1.0, "FAILFADE": 2.0, "STOPPOKE": 3.0, "GAPPER": 2.0, "GAPFAIL": 3.0,
}

UNIVERSE_EXCLUSIONS: dict[str, list[str]] = {
    "THINVOL": ["watchlist_cap"],                      # cap-only rows ARE eligible
    "OUTSIDER": ["not_in_index"],                      # out-of-index rows are NOT
}
ELIGIBLE = (set(F1_CAST) | set(F2_CAST)) - {"OUTSIDER"}


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


def _seed(path: Path, parquet: Path, clock, *, universe: bool = True, tape: bool = True) -> Path:
    """A temp market.duckdb holding the synthetic tape, CLOSED so the study can attach read-only."""
    store = MarketStore(path, parquet, clock).open()
    try:
        if tape:
            bars = _tape()
            for i in range(0, len(bars), 20000):
                store.insert_bars_1m(bars[i: i + 20000])
            store.upsert_bars_1d(_daily())
            store.upsert_corp_actions([{
                "symbol": "EXDATED", "ex_date": EX_DATE, "kind": "bonus", "ratio": "1:1",
                "amount": None, "source": "test", "recorded_at": clock.now(),
            }])
        if universe:
            store.upsert_universe_daily(_universe_rows())
    finally:
        store.close()
    return path


@pytest.fixture(scope="module")
def db_file(tmp_path_factory, module_clock) -> Path:
    root = tmp_path_factory.mktemp("fade")
    return _seed(root / "market.duckdb", root / "parquet", module_clock)


@pytest.fixture(scope="module")
def study(db_file):
    conn = bt.open_readonly(db_file)
    try:
        return bt.run_study(
            conn, start=SESSIONS[0], end=SESSIONS[-1], cost_model=CostModel.from_config(),
        )
    finally:
        conn.close()


def _by_symbol(trades, rule: str) -> dict[str, bt.Trade]:
    return {t.symbol: t for t in trades[rule]}


def _cost(entry_px: float) -> float:
    return bt.trade_cost_pct(CostModel.from_config(), bt.PARAMS, entry_px)


# ============================================================ 0. pre-registration discipline
def test_one_params_dict_two_registered_rules_and_no_signal_knob_on_the_cli():
    """PARAMS is the single pre-registered set; the CLI cannot turn two rules into a grid."""
    assert bt.PARAMS["or_start"] == "09:15"
    assert bt.PARAMS["or_end_exclusive"] == "09:30"
    assert bt.PARAMS["break_window_end_exclusive"] == "10:30"
    assert bt.PARAMS["f1_rvol_min"] == 1.5
    assert bt.PARAMS["f2_gap_min_pct"] == 2.0
    assert bt.PARAMS["rvol_lookback_sessions"] == 20
    assert bt.PARAMS["rvol_min_valid_sessions"] == 10
    assert bt.PARAMS["squareoff_time"] == "15:15"
    assert bt.PARAMS["product"] == "MIS"
    assert bt.PARAMS["promote_min_n"] == 200
    assert bt.PARAMS["promote_min_t"] == 2.0
    assert bt.PARAMS["promote_min_cpcv_positive_share"] == 0.60

    assert bt.PRE_REGISTERED_RULES == ("F1", "F2")
    assert bt.TRIAL_COUNT_N == 2
    assert {n for n, s in bt.RULES.items() if s.registered} == {"F1", "F2"}
    assert set(bt.RULES) == {
        "F1", "F2", "F1_T1200", "F2_1515_only", "F1_stop_on_1m_high", "F2_stop_on_1m_high"
    }
    assert bt.RULES["F1"].decision_time == "11:00"
    assert bt.RULES["F2"].decision_time == "10:00"
    assert bt.RULES["F1"].exit_level == bt.EXIT_OR_HIGH
    assert bt.RULES["F2"].exit_level == bt.EXIT_T_BAR_HIGH
    assert bt.RULES["F2_1515_only"].exit_level is None and bt.RULES["F2_1515_only"].stop is None

    flags = {a.option_strings[0] for a in bt.build_parser()._actions if a.option_strings}
    assert not (flags & {"--rvol-min", "--gap", "--decision-time", "--stop", "--grid", "--rule"})


def test_every_rule_is_a_short_and_the_doc_says_shorts_stay_gated(study):
    doc, trades = study
    assert doc["meta"]["side"] == "SHORT"
    assert doc["meta"]["shorts_gated_for_auto_by_1_4_9"] is True
    assert doc["meta"]["parameter_sweep_run"] is False
    assert doc["meta"]["rule_selection_performed"] is False
    assert doc["meta"]["population_is_survivorship_tainted_proxy"] is True
    assert all(t.side == "SHORT" for ts in trades.values() for t in ts)


# ============================================================ 1. the T-bar feature computation
def test_or_high_comes_from_completed_bars_only(db_file):
    """FADER trades at 101.00 from 09:30; the OR high is the 09:20 print (100.50), not that."""
    conn = bt.open_readonly(db_file)
    try:
        f = bt.load_features(conn, SESSIONS[0], SESSIONS[-1], decision_time="11:00")
    finally:
        conn.close()
    f["d"] = [x.date() if hasattr(x, "date") else x for x in f["d"]]
    row = f[(f["symbol"] == "FADER") & (f["d"] == SIGNAL_DAY)].iloc[0]
    assert float(row.open_0915) == pytest.approx(100.0)
    assert float(row.or_high) == pytest.approx(OR_HIGH)        # NOT 101.00
    assert float(row.max_close_break) == pytest.approx(101.0)   # the [09:30, 10:30) break
    assert float(row.close_t) == pytest.approx(100.0)
    assert float(row.sess_high_t) == pytest.approx(101.0)       # the stop level
    assert int(row.n_t) == 1


def test_the_T_bar_snapshot_cannot_see_the_post_T_tape(db_file):
    """FAILFADE runs to 104.00 after 11:05 and FADER stays at 100.00; their pre-T rows are equal."""
    conn = bt.open_readonly(db_file)
    try:
        f = bt.load_features(conn, SESSIONS[0], SESSIONS[-1], decision_time="11:00")
    finally:
        conn.close()
    f["d"] = [x.date() if hasattr(x, "date") else x for x in f["d"]]
    a = f[(f["symbol"] == "FADER") & (f["d"] == SIGNAL_DAY)].iloc[0]
    b = f[(f["symbol"] == "FAILFADE") & (f["d"] == SIGNAL_DAY)].iloc[0]
    for col in ("or_high", "max_close_break", "close_t", "sess_high_t", "cumvol_t"):
        assert float(getattr(b, col)) == pytest.approx(float(getattr(a, col)))


# ============================================================ 2. F1 selection
def test_F1_books_exactly_the_three_qualifying_symbols(study):
    """The whole F1 cast in one assertion: only these three clear every clause."""
    _doc, trades = study
    assert set(_by_symbol(trades, "F1")) == {"FADER", "FAILFADE", "STOPPOKE"}
    assert {t.d for t in trades["F1"]} == {SIGNAL_DAY}


def test_F1_rejects_each_clause_for_its_own_reason(study):
    doc, _trades = study
    vetoes = doc["diagnostics"]["by_rule"]["F1"]["vetoes"]
    assert vetoes["no_breakout_before_window_end"] >= 2       # NOBREAK and LATEBREAK
    assert vetoes["still_above_or_high_at_T"] >= 1            # STILLUP
    assert vetoes["rvol_below_min"] >= 1                      # THINVOL
    assert vetoes["ex_date_window"] >= 1                      # EXDATED


def test_an_out_of_index_symbol_is_never_scanned(study):
    """OUTSIDER shares FADER's tape byte for byte; only its universe row differs."""
    doc, trades = study
    assert "OUTSIDER" not in _by_symbol(trades, "F1")
    assert doc["meta"]["n_symbols_measured"] == len(ELIGIBLE)
    assert "THINVOL" in ELIGIBLE                              # a cap-only row IS eligible


def test_rvol_is_the_tdc_definition_and_the_thin_denominator_is_skipped(study):
    doc, trades = study
    assert _by_symbol(trades, "F1")["FADER"].rvol == pytest.approx(2.0, abs=1e-9)
    # the F2 cast carries one prior session only, so its rvol window is below the 10-session floor
    assert doc["diagnostics"]["by_decision_time"]["11:00"][
        "rvol_insufficient_history_symbol_days"] > 0


# ============================================================ 3. F1 execution
def test_F1_enters_short_at_the_open_of_the_bar_after_T_and_squares_off_at_1515(study):
    """FADER: entry 100.00 at 11:01, exit at the 15:15 CLOSE of 98.00 - the tape runs to 15:29."""
    _doc, trades = study
    t = _by_symbol(trades, "F1")["FADER"]
    assert t.entry_px == pytest.approx(100.0)
    assert t.exit_reason == bt.EXIT_SQUAREOFF
    assert t.exit_px == pytest.approx(98.0)                   # never the 90.00 printed after 15:15
    assert t.gross_pct == pytest.approx(2.0, abs=1e-9)        # a SHORT profits when price falls
    assert t.net_pct == pytest.approx(2.0 - _cost(100.0), abs=1e-9)


def test_the_fade_failure_exit_fills_at_the_FOLLOWING_bar_open(study):
    """FAILFADE's 11:05 bar closes at 101.00 (the trigger) and the 11:06 bar opens at 104.00.

    A same-bar fill would book (1 - 101/100) = -1.00%. The correct fill books -4.00%.
    """
    _doc, trades = study
    t = _by_symbol(trades, "F1")["FAILFADE"]
    assert t.exit_reason == bt.EXIT_FADE_FAILED
    assert t.exit_px == pytest.approx(104.0)
    assert t.gross_pct == pytest.approx(-4.0, abs=1e-9)
    assert t.gross_pct != pytest.approx(-1.0, abs=1e-3)


def test_the_exit_level_is_the_OR_high_and_the_stop_is_the_session_high(study):
    _doc, trades = study
    t = _by_symbol(trades, "F1")["FAILFADE"]
    assert t.exit_level == pytest.approx(OR_HIGH)
    assert t.stop_level == pytest.approx(101.0)


def test_the_stop_is_dominated_under_the_close_trigger_and_the_diagnostics_prove_it(study):
    """Every registered rule: stop >= exit level by construction, so the stop never fires alone."""
    doc, trades = study
    for name in ("F1", "F2"):
        dg = doc["diagnostics"]["by_rule"][name]
        assert dg["n_exits_stop_first"] == 0
        assert dg["n_signals_with_stop_below_exit_level"] == 0
        assert all(t.stop_level >= t.exit_level for t in trades[name])


def test_the_intrabar_high_stop_binds_exactly_where_the_close_trigger_does_not(study):
    """STOPPOKE: +1.00% squared off under F1, -1.00% stopped under the conservatism cell.

    The 11:03 bar's high (103.00) clears the 101.00 session high while its close (100.00) stays
    under the 100.50 OR high, so the two execution models must disagree on this symbol and agree
    everywhere else.
    """
    _doc, trades = study
    closed = _by_symbol(trades, "F1")["STOPPOKE"]
    poked = _by_symbol(trades, "F1_stop_on_1m_high")["STOPPOKE"]
    assert closed.exit_reason == bt.EXIT_SQUAREOFF
    assert closed.gross_pct == pytest.approx(1.0, abs=1e-9)
    assert poked.exit_reason == bt.EXIT_STOP_INTRABAR
    assert poked.exit_px == pytest.approx(101.0)              # max(stop, that bar's open)
    assert poked.gross_pct == pytest.approx(-1.0, abs=1e-9)
    # the other two trades are untouched by the execution change
    assert _by_symbol(trades, "F1_stop_on_1m_high")["FADER"].exit_reason == bt.EXIT_SQUAREOFF
    assert _by_symbol(trades, "F1_stop_on_1m_high")["FAILFADE"].exit_px == pytest.approx(104.0)


def test_the_reporting_only_1200_cell_moves_the_decision_bar_and_nothing_else(study):
    """At 12:00 FAILFADE is above its OR high again, so the same rule sees two names, not three."""
    _doc, trades = study
    assert set(_by_symbol(trades, "F1_T1200")) == {"FADER", "STOPPOKE"}
    assert _by_symbol(trades, "F1_T1200")["FADER"].entry_px == pytest.approx(100.0)


# ============================================================ 4. F2
def test_F2_books_exactly_the_two_gap_fades(study):
    _doc, trades = study
    assert set(_by_symbol(trades, "F2")) == {"GAPPER", "GAPFAIL"}


def test_F2_rejects_a_small_gap_and_a_gap_that_held(study):
    doc, _trades = study
    vetoes = doc["diagnostics"]["by_rule"]["F2"]["vetoes"]
    assert vetoes["gap_below_min"] >= 1                        # SMALLGAP at +1.0%
    assert vetoes["not_below_session_open_at_T"] >= 1          # GAPHOLD


def test_F2_measures_the_gap_against_the_prior_daily_close_and_enters_at_1001(study):
    _doc, trades = study
    t = _by_symbol(trades, "F2")["GAPPER"]
    assert t.gap_pct == pytest.approx(3.0, abs=1e-9)           # 103.00 vs a 100.00 prior close
    assert t.entry_px == pytest.approx(102.0)                  # the 10:01 OPEN
    assert t.exit_reason == bt.EXIT_SQUAREOFF
    assert t.exit_px == pytest.approx(100.0)
    assert t.gross_pct == pytest.approx((1.0 - 100.0 / 102.0) * 100.0, abs=1e-9)


def test_F2s_failure_level_is_the_T_bar_high(study):
    """GAPFAIL's 10:05 close (103.00) clears the 10:00 bar's high (102.50); the fill is 10:06's open."""
    _doc, trades = study
    t = _by_symbol(trades, "F2")["GAPFAIL"]
    assert t.exit_level == pytest.approx(102.5)
    assert t.stop_level == pytest.approx(103.5)
    assert t.exit_reason == bt.EXIT_FADE_FAILED
    assert t.exit_px == pytest.approx(106.0)
    assert t.gross_pct == pytest.approx((1.0 - 106.0 / 102.0) * 100.0, abs=1e-9)


def test_the_1515_only_cell_removes_both_the_failure_exit_and_the_stop(study):
    """Same entry, no intraday exit: GAPFAIL's sign flips because it is held to the squareoff."""
    _doc, trades = study
    held = _by_symbol(trades, "F2_1515_only")["GAPFAIL"]
    assert held.exit_reason == bt.EXIT_SQUAREOFF
    assert held.exit_px == pytest.approx(100.0)
    assert held.gross_pct == pytest.approx((1.0 - 100.0 / 102.0) * 100.0, abs=1e-9)
    assert held.gross_pct > 0 > _by_symbol(trades, "F2")["GAPFAIL"].gross_pct


# ============================================================ 5. the short model in isolation
def test_short_return_is_profit_over_the_entry_notional():
    assert bt.short_gross_pct(100.0, 98.0) == pytest.approx(2.0)
    assert bt.short_gross_pct(100.0, 102.0) == pytest.approx(-2.0)
    # not the (entry/exit - 1) reading, which would book +2.0408% on the same trade
    assert bt.short_gross_pct(100.0, 98.0) != pytest.approx((100.0 / 98.0 - 1.0) * 100.0, abs=1e-4)


def test_a_missing_entry_bar_is_a_skipped_trade_never_a_fill_at_another_price():
    assert bt.simulate_short(
        ["11:02", "11:03"], [100.0] * 2, [100.0] * 2, [100.0] * 2,
        entry_time="11:01", squareoff_time="15:15", exit_level=100.5, stop_level=101.0,
        stop_trigger=bt.TRIGGER_CLOSE,
    ) is None


def test_the_squareoff_uses_the_1515_bar_close_and_ignores_the_later_tape():
    times = [f"{h:02d}:{m:02d}" for h in (11, 15) for m in range(60)]
    opens = [100.0] * len(times)
    highs = [100.0] * len(times)
    closes = [100.0] * len(times)
    for i, t in enumerate(times):
        if t == "15:15":
            closes[i] = 95.0
        elif t > "15:15":
            closes[i] = opens[i] = highs[i] = 1.0          # must never be reachable
    got = bt.simulate_short(times, opens, highs, closes, entry_time="11:01",
                            squareoff_time="15:15", exit_level=200.0, stop_level=300.0,
                            stop_trigger=bt.TRIGGER_CLOSE)
    assert got == (100.0, 95.0, bt.EXIT_SQUAREOFF)


def test_the_failure_trigger_is_strict_and_fills_one_bar_later():
    times = ["11:01", "11:02", "11:03", "15:15"]
    opens = [100.0, 100.0, 108.0, 80.0]
    highs = [100.0, 100.5, 108.0, 80.0]
    at_level = bt.simulate_short(times, opens, highs, [100.0, 100.5, 100.0, 100.0],
                                 entry_time="11:01", squareoff_time="15:15", exit_level=100.5,
                                 stop_level=101.0, stop_trigger=bt.TRIGGER_CLOSE)
    assert at_level[2] == bt.EXIT_SQUAREOFF            # a close EQUAL to the level does not fire
    above = bt.simulate_short(times, opens, highs, [100.0, 100.6, 100.0, 100.0],
                              entry_time="11:01", squareoff_time="15:15", exit_level=100.5,
                              stop_level=101.0, stop_trigger=bt.TRIGGER_CLOSE)
    assert above == (100.0, 108.0, bt.EXIT_FADE_FAILED)   # the 11:03 OPEN, not the 11:02 close


def test_the_intrabar_stop_fills_at_the_stop_or_the_bar_open_whichever_is_worse():
    times = ["11:01", "11:02", "15:15"]
    # the bar opens BELOW the stop and pokes through it: the fill is the stop
    got = bt.simulate_short(times, [100.0, 100.0, 99.0], [100.0, 101.5, 99.0], [100.0] * 3,
                            entry_time="11:01", squareoff_time="15:15", exit_level=100.5,
                            stop_level=101.0, stop_trigger=bt.TRIGGER_HIGH)
    assert got == (100.0, 101.0, bt.EXIT_STOP_INTRABAR)
    # the bar GAPS through the stop: a short cannot be bought back at a price the tape skipped
    got = bt.simulate_short(times, [100.0, 102.0, 99.0], [100.0, 102.5, 99.0], [100.0] * 3,
                            entry_time="11:01", squareoff_time="15:15", exit_level=100.5,
                            stop_level=101.0, stop_trigger=bt.TRIGGER_HIGH)
    assert got == (100.0, 102.0, bt.EXIT_STOP_INTRABAR)
    # ... and a stop touched on the squareoff bar itself is a stop, not a squareoff
    got = bt.simulate_short(times, [100.0, 100.0, 99.0], [100.0, 100.0, 101.5], [100.0] * 3,
                            entry_time="11:01", squareoff_time="15:15", exit_level=100.5,
                            stop_level=101.0, stop_trigger=bt.TRIGGER_HIGH)
    assert got == (100.0, 101.0, bt.EXIT_STOP_INTRABAR)


def test_a_corrupt_exit_price_drops_the_trade_instead_of_booking_a_100_percent_short_win():
    """A zero buy-back price reads as +100% on a SHORT, so it is a drop, never a fill."""
    times = ["11:01", "11:02", "11:03", "15:15"]
    assert bt.simulate_short(times, [100.0, 100.0, 0.0, 80.0], [100.0] * 4,
                             [100.0, 106.0, 100.0, 100.0], entry_time="11:01",
                             squareoff_time="15:15", exit_level=100.5, stop_level=101.0,
                             stop_trigger=bt.TRIGGER_CLOSE) is None
    assert bt.simulate_short(times, [100.0] * 4, [100.0] * 4, [100.0, 100.0, 100.0, -1.0],
                             entry_time="11:01", squareoff_time="15:15", exit_level=200.0,
                             stop_level=300.0, stop_trigger=bt.TRIGGER_CLOSE) is None


def test_the_close_triggered_stop_is_reachable_only_when_it_is_below_the_failure_level():
    """The branch exists and is correct; the registered rules can never reach it (stop >= level)."""
    times = ["11:01", "11:02", "11:03", "15:15"]
    got = bt.simulate_short(times, [100.0, 100.0, 107.0, 80.0], [100.0] * 4,
                            [100.0, 106.0, 100.0, 100.0], entry_time="11:01",
                            squareoff_time="15:15", exit_level=None, stop_level=105.0,
                            stop_trigger=bt.TRIGGER_CLOSE)
    assert got == (100.0, 107.0, bt.EXIT_STOP_NEXT_OPEN)


# ============================================================ 6. costs
def test_cost_is_one_mis_round_trip_plus_one_tick_of_slippage_each_side():
    cm = CostModel.from_config()
    entry = 102.0
    rt = float(cm.breakeven_pct(Decimal(bt.PARAMS["reference_notional_inr"]), "MIS"))
    tick = float(Decimal(bt.PARAMS["tick_size_inr"]))
    assert bt.trade_cost_pct(cm, bt.PARAMS, entry) == pytest.approx(
        rt + 2.0 * tick / entry * 100.0, abs=1e-12
    )
    assert bt.PARAMS["product"] == "MIS"                       # a short leg is never delivery
    assert rt < float(cm.breakeven_pct(Decimal(bt.PARAMS["reference_notional_inr"]), "CNC"))


def test_net_is_gross_minus_exactly_one_round_trip_per_trade(study):
    _doc, trades = study
    for rule_trades in trades.values():
        for t in rule_trades:
            assert t.net_pct == pytest.approx(t.gross_pct - t.cost_pct, abs=1e-12)
            assert t.cost_pct == pytest.approx(_cost(t.entry_px), abs=1e-12)


# ============================================================ 7. vetoes and guards in isolation
def test_the_ex_date_window_is_inclusive_at_both_ends_and_ignores_the_future():
    d = date(2026, 6, 30)
    ex = {"X": [d - timedelta(days=35)], "Y": [d - timedelta(days=36)],
          "Z": [d], "F": [d + timedelta(days=1)]}
    assert bt.has_ex_date_within(ex, "X", d, 35) is True
    assert bt.has_ex_date_within(ex, "Y", d, 35) is False
    assert bt.has_ex_date_within(ex, "Z", d, 35) is True
    assert bt.has_ex_date_within(ex, "F", d, 35) is False       # an ex-date ahead of d is not in it
    assert bt.has_ex_date_within(ex, "MISSING", d, 35) is False


def _gap_features(open_px: float = 103.0, close_t: float = 102.0) -> dict:
    f = bt.DayFeatures(SIGNAL_DAY)
    f.symbols.append("G")
    f.open_0915["G"] = open_px
    f.or_high["G"] = 103.5
    f.max_close_break["G"] = float("nan")
    f.close_t["G"] = close_t
    f.high_t["G"] = 102.5
    f.sess_high_t["G"] = 103.5
    f.ret_pct["G"] = 0.0
    return {SIGNAL_DAY: f}


def _f2_spec() -> bt.RuleSpec:
    return bt.RULES["F2"]


def test_a_stale_prior_close_is_a_suspension_not_a_gap():
    daily = {"G": ([SIGNAL_DAY - timedelta(days=30)], [100.0], [100.0], [100.0])}
    picks, veto = bt.select_rule(_f2_spec(), _gap_features(), params=bt.PARAMS, ex_dates={},
                                 daily=daily, prior_1m={})
    assert picks == {} and veto["stale_prior_close"] == 1


def test_a_prior_close_the_two_sources_disagree_on_is_a_unit_mismatch():
    prev = SIGNAL_DAY - timedelta(days=1)
    daily = {"G": ([prev], [100.0], [100.0], [100.0])}
    picks, veto = bt.select_rule(_f2_spec(), _gap_features(), params=bt.PARAMS, ex_dates={},
                                 daily=daily, prior_1m={("G", prev): 130.0})
    assert picks == {} and veto["prior_close_source_disagreement"] == 1
    # inside the tolerance the same day is a signal, and the missing cross-check is counted not fatal
    picks, veto = bt.select_rule(_f2_spec(), _gap_features(), params=bt.PARAMS, ex_dates={},
                                 daily=daily, prior_1m={("G", prev): 100.5})
    assert list(picks) == [SIGNAL_DAY]
    picks, veto = bt.select_rule(_f2_spec(), _gap_features(), params=bt.PARAMS, ex_dates={},
                                 daily=daily, prior_1m={})
    assert list(picks) == [SIGNAL_DAY] and veto["prior_close_crosscheck_unavailable"] == 1


def test_a_symbol_with_no_daily_history_cannot_form_a_gap():
    _picks, veto = bt.select_rule(_f2_spec(), _gap_features(), params=bt.PARAMS, ex_dates={},
                                  daily={}, prior_1m={})
    assert veto["no_daily_history"] == 1


def test_run_study_refuses_a_breakout_rule_whose_decision_bar_precedes_the_break_window(db_file):
    """Moving T below 10:30 would silently redefine "broke before 10:30" - that is a different rule."""
    bad = {"F1_bad": bt.RuleSpec("F1_bad", "F1", bt.KIND_FAILED_BREAKOUT, "10:00", bt.EXIT_OR_HIGH,
                                 bt.STOP_SESSION_HIGH, bt.TRIGGER_CLOSE, True, "bad")}
    conn = bt.open_readonly(db_file)
    try:
        with pytest.raises(ValueError, match="precedes the break-window end"):
            bt.run_study(conn, start=SESSIONS[0], end=SESSIONS[-1],
                         cost_model=CostModel.from_config(), rules=bad)
    finally:
        conn.close()


# ============================================================ 8. splits, metrics, verdict
def test_the_atr_tercile_is_r3s_prior_14_session_daily_statistic(study):
    """FADER 1%, FAILFADE 2%, STOPPOKE 3% of a 100.00 close - one name per cell."""
    doc, trades = study
    by_sym = _by_symbol(trades, "F1")
    assert by_sym["FADER"].atr_pct == pytest.approx(1.0, abs=1e-9)
    assert by_sym["FAILFADE"].atr_pct == pytest.approx(2.0, abs=1e-9)
    assert by_sym["STOPPOKE"].atr_pct == pytest.approx(3.0, abs=1e-9)
    assert by_sym["FADER"].atr_tercile == bt.ATR_LOW
    assert by_sym["FAILFADE"].atr_tercile == bt.ATR_MID
    assert by_sym["STOPPOKE"].atr_tercile == bt.ATR_HIGH
    cells = doc["results"]["F1"]["splits"]
    assert sum(cells[c]["n"] for c in bt.ATR_CELLS) == cells[bt.SPLIT_ALL]["n"]


def test_the_atr_window_ends_the_session_before_the_signal_and_needs_a_full_14():
    """The signal day's own daily bar is never in its own label, and a short history is unclassified."""
    days = _sessions(20)
    # a calm history, then a violent bar ON the signal day: the label must not see it
    highs = [101.0] * 19 + [300.0]
    lows = [99.0] * 19 + [1.0]
    closes = [100.0] * 20
    daily = {"CALM": (days, highs, lows, closes)}
    assert bt.atr_pct_at(daily, "CALM", days[-1], period=14) == pytest.approx(2.0, abs=1e-9)
    # exactly 14 priors is enough; 13 is not
    assert bt.atr_pct_at({"C": (days[:14], highs[:14], lows[:14], closes[:14])}, "C", days[14],
                         period=14) == pytest.approx(2.0, abs=1e-9)
    assert bt.atr_pct_at({"C": (days[:13], highs[:13], lows[:13], closes[:13])}, "C", days[13],
                         period=14) is None
    assert bt.atr_pct_at({}, "MISSING", days[-1], period=14) is None


def test_tercile_cuts_need_three_values_and_refuse_a_degenerate_population():
    assert bt.tercile_cuts([1.0, 2.0]) is None
    assert bt.tercile_cuts([5.0, 5.0, 5.0, 5.0]) is None
    cuts = bt.tercile_cuts([1.0, 2.0, 3.0])
    assert bt.tercile_of(1.0, cuts) == bt.ATR_LOW
    assert bt.tercile_of(3.0, cuts) == bt.ATR_HIGH
    # unclassifiable values are LABELLED, never dropped, so the four cells always partition
    assert bt.tercile_of(None, cuts) == bt.ATR_UNCLASSIFIED
    assert bt.tercile_of(1.0, None) == bt.ATR_UNCLASSIFIED


def test_the_atr_cuts_are_taken_once_per_rule_family(study):
    """A symbol-day sits in the same ATR cell under every variant of its rule (R3's posture)."""
    doc, trades = study
    f1_cuts = doc["diagnostics"]["by_rule"]["F1"]["atr_tercile_cuts_pct"]
    for name in ("F1_T1200", "F1_stop_on_1m_high"):
        assert doc["diagnostics"]["by_rule"][name]["atr_tercile_cuts_pct"] == f1_cuts
        assert doc["diagnostics"]["by_rule"][name]["atr_family"] == "F1"
    cell = {t.symbol: t.atr_tercile for t in trades["F1"]}
    for name in ("F1_T1200", "F1_stop_on_1m_high"):
        for t in trades[name]:
            assert t.atr_tercile == cell[t.symbol]


def test_the_index_regime_split_is_taken_at_the_rules_own_decision_bar(study):
    """The index is -0.30% at 10:00 and +0.50% at 11:00, so F2 is down-tape and F1 is up-tape."""
    doc, trades = study
    assert all(t.index_ret_pct == pytest.approx(0.5, abs=1e-9) for t in trades["F1"])
    assert all(t.index_ret_pct == pytest.approx(-0.3, abs=1e-9) for t in trades["F2"])
    assert all(t.index_is_real for t in trades["F1"])
    f1 = doc["results"]["F1"]["splits"]
    assert f1[bt.SPLIT_INDEX_UP]["n"] == 3 and f1[bt.SPLIT_INDEX_DOWN]["n"] == 0
    f2 = doc["results"]["F2"]["splits"]
    assert f2[bt.SPLIT_INDEX_DOWN]["n"] == 2 and f2[bt.SPLIT_INDEX_UP]["n"] == 0
    assert f1[bt.SPLIT_REAL_INDEX]["n"] == 3


def test_splits_partition_the_trade_set(study):
    doc, _trades = study
    for name, block in doc["results"].items():
        cells = block["splits"]
        n = cells[bt.SPLIT_ALL]["n"]
        assert cells[bt.SPLIT_INDEX_UP]["n"] + cells[bt.SPLIT_INDEX_DOWN]["n"] == n, name
        assert sum(v["n"] for k, v in cells.items() if k.startswith("year_")) == n, name


def test_promotable_is_the_tdc_boolean_and_a_three_trade_cell_never_clears_it(study):
    doc, _trades = study
    s = doc["results"]["F1"]["splits"][bt.SPLIT_ALL]
    for key in ("n", "mean_net_pct", "median_net_pct", "win_rate", "t_stat", "promotable"):
        assert key in s
    assert s["n"] == 3 and s["promotable"] is False            # the n >= 200 floor alone is decisive
    assert doc["meta"]["promotion_rule"].startswith("mean net % > 0 AND t > 2 AND n >= 200")


def test_the_verdict_is_a_refutation_unless_a_REGISTERED_cell_clears_the_boolean(study):
    doc, _trades = study
    assert doc["verdict"]["outcome"] == "REFUTED"
    assert doc["verdict"]["registered_promotable_cells"] == []
    assert set(doc["verdict"]["pooled_registered"]) == {"F1", "F2"}
    assert set(doc["verdict"]["geometry_first"]) == {"F1", "F2"}


# ============================================================ 9. the CLI: report, JSON, refusals
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

    md = out.with_suffix(".md").read_text(encoding="utf-8")
    assert "Outcome: **REFUTED**" in md
    assert "Geometry first" in md

    text = capsys.readouterr().out
    assert text.isascii()                                      # the Windows console is cp1252
    assert text.index("STEP 1 - SAMPLE SHAPE") < text.index("STEP 3 - GEOMETRY FIRST")
    assert text.index("STEP 3 - GEOMETRY FIRST") < text.index("STEP 4 - RESULTS")
    assert "promotable=True in NO REGISTERED rule x split cell" in text
    assert str(out) in text


def test_refuses_a_missing_database_file(tmp_path, capsys):
    rc = bt.main(["--db", str(tmp_path / "nope.duckdb"), "--out", str(tmp_path / "x.json")])
    assert rc == 2
    err = capsys.readouterr().err
    assert "REFUSING TO RUN" in err and "no such database file" in err
    assert not (tmp_path / "x.json").exists()


def test_refuses_a_locked_database_file(db_file, tmp_path, module_clock, capsys):
    """A running engine holds market.duckdb read-write; DuckDB then refuses a read-only attach."""
    holder = MarketStore(db_file, tmp_path / "parquet_lock", module_clock).open()
    try:
        rc = bt.main(["--db", str(db_file), "--out", str(tmp_path / "y.json")])
    finally:
        holder.close()
    assert rc == 2
    err = capsys.readouterr().err
    assert "REFUSING TO RUN" in err and "cannot open read-only" in err and "mt-engine" in err
    assert not (tmp_path / "y.json").exists()


def test_refuses_an_empty_universe_rather_than_measuring_a_different_population(
    tmp_path, module_clock, capsys
):
    db = _seed(tmp_path / "bare.duckdb", tmp_path / "parquet_bare", module_clock,
               universe=False, tape=False)
    rc = bt.main(["--db", str(db), "--out", str(tmp_path / "z.json")])
    assert rc == 2
    err = capsys.readouterr().err
    assert "no eligible universe" in err
    assert not (tmp_path / "z.json").exists()
