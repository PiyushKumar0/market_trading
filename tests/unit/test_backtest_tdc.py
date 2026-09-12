"""scripts/backtest_tdc.py - the pre-registered tdc backtest harness, on synthetic 1m bars.

Every fixture is hand-computed so each assertion has an arithmetic answer, not a regression blob.

The synthetic tape is a temp :class:`MarketStore` holding 30 sessions x a handful of engineered
symbols, each session a full 09:15..15:29 run of one-minute bars. The last session (``SIGNAL_DAY``)
is the one under test; the 29 before it exist so ``rel_volume_tod`` has the 20 prior sessions its
median needs, and their volumes are set to a known constant so that median is exact.

The cast on ``SIGNAL_DAY``:

* ``WINNER``  - passes (i), (ii) and (iii); climbs after entry and is squared off at 15:15.
* ``STOPPER`` - passes selection, then loses VWAP: the tripwire for "trigger on a close, fill at the
  FOLLOWING open". Its triggering bar closes at a price that would flatter the trade and the next
  bar opens far below it, so a same-bar fill and the correct fill differ by a wide, checkable margin.
* ``THINVOL`` - identical price path to WINNER but only ``rvol`` 1.0: fails (ii), never selected.
* ``LAGGARD`` - up on the day but by less than the index + 1%: fails (iii), never selected.
* ``DIPPER``  - closes one of the 15 pre-T bars below its running VWAP: fails the acceptance test
  alone, and is the assertion that the 15-close window is really being read.
* ``NOHIST``  - appears only on the last 9 sessions, so its ``rel_volume_tod`` window holds 8 prior
  sessions: below the 10-session floor, so it is SKIPPED and counted, never admitted on a thin
  denominator.
* ``NIFTY 50`` - the index, up +0.50% from open at T, so (iii)'s bar is +1.50%.
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
from engine.strategy.cost_model import CostModel  # noqa: E402

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
    settles at 100.20 (above the running VWAP and above the OR high when ``or_high_px`` is 100.10)
    until 10:44, and from 10:45 sits at ``close_T`` so the T bar's close gives exactly ``ret_pct``.
    ``dip_at`` drops one bar inside the 15-bar acceptance window below the running VWAP.
    ``post_entry`` overrides ``(open, close)`` for specific minutes after T.
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
#: under VWAP(T) * 0.9975) and the 11:06 bar OPENS at 95.00 - a 4-rupee gap between the price that
#: triggers the exit and the price the exit actually fills at. A same-bar fill would book
#: 99.00/102.00; the correct fill books 95.00/102.00, and the test pins the latter.
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
#: Constant per-symbol daily bars, so every true range is the plain high-low, Wilder ATR(14) IS that
#: range and ATR% is exact: WINNER 2.00/101.00 = 1.9802%, STOPPER 5.00/102.00 = 4.9020%.
DAILY_OHLC: dict[str, tuple[float, float, float]] = {      # symbol -> (high, low, close)
    "WINNER": (102.0, 100.0, 101.0),
    "STOPPER": (105.0, 100.0, 102.0),
}
WINNER_ATR_PCT = 2.0 / 101.0 * 100.0
STOPPER_ATR_PCT = 5.0 / 102.0 * 100.0


def _daily_bar(sym: str, d: date, high: float, low: float, close: float) -> DailyBar:
    return DailyBar(
        symbol=sym, d=d, open=Decimal(str(round(close, 2))), high=Decimal(str(round(high, 2))),
        low=Decimal(str(round(low, 2))), close=Decimal(str(round(close, 2))), volume=1000,
        src="kite_official",
    )


def _daily_tape() -> list[DailyBar]:
    """One daily bar per prior session, plus a deliberately absurd SIGNAL-DAY bar.

    The signal day's own daily bar is 100.00 wide; it is strictly AFTER the ATR window, so if the
    window ever reached it WINNER's ATR% would read ~66 instead of ~1.98.
    """
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


def _run(db: Path, variants: dict | None = None):
    conn = bt.open_readonly(db)
    try:
        return bt.run_study(
            conn,
            start=SESSIONS[0],
            end=SESSIONS[-1],
            cost_model=CostModel.from_config(),
            variants=variants,
        )
    finally:
        conn.close()


@pytest.fixture(scope="module")
def study(db_file):
    return _run(db_file)


def _trades(trades, variant: str) -> dict[str, object]:
    return {t.symbol: t for t in trades[variant]}


def _cost(entry_px: float) -> float:
    return bt.trade_cost_pct(CostModel.from_config(), bt.PARAMS, entry_px)


# ============================================================ 0. pre-registration discipline
def test_one_params_dict_one_variants_dict_and_no_signal_knob_on_the_cli():
    """PARAMS is the single pre-registered set; the CLI cannot turn one rule into a grid."""
    assert bt.PARAMS["decision_time"] == "11:00"
    assert bt.PARAMS["rvol_min"] == 1.5
    assert bt.PARAMS["rvol_lookback_sessions"] == 20
    assert bt.PARAMS["rvol_min_valid_sessions"] == 10
    assert bt.PARAMS["ret_from_open_min_pct"] == 1.0
    assert bt.PARAMS["rs_over_index_min_pct"] == 1.0
    assert bt.PARAMS["top_n_per_day"] == 5
    assert bt.PARAMS["stop_vwap_pct"] == 0.25
    assert bt.PARAMS["squareoff_time"] == "15:15"
    assert bt.PARAMS["product"] == "MIS"
    assert bt.PRE_REGISTERED_HYPOTHESIS == "H1"
    # the plan's stated robustness list, verbatim, and nothing else
    assert set(bt.VARIANTS) == {
        "H1", "B", "T_1030", "T_1200", "RVOL_1.2", "RVOL_2.0", "STOP_0.5", "C_sector"
    }
    assert bt.VARIANTS["H1"] == {}
    assert bt.VARIANTS["B"] == {"stop_vwap_pct": None}
    flags = {a.option_strings[0] for a in bt.build_parser()._actions if a.option_strings}
    assert not (flags & {"--rvol-min", "--stop", "--decision-time", "--top-n", "--grid"})


# ============================================================ 1. the T-bar feature computation
def test_or_high_and_session_vwap_are_computed_from_bars_at_or_before_T(db_file):
    """OR high = max high over 09:15..09:29; VWAP(T) = cumulative typical-price VWAP to the T bar."""
    conn = bt.open_readonly(db_file)
    try:
        f = bt.load_features(conn, SESSIONS[0], SESSIONS[-1], decision_time="11:00")
    finally:
        conn.close()
    f["d"] = [x.date() if hasattr(x, "date") else x for x in f["d"]]
    row = f[(f["symbol"] == "WINNER") & (f["d"] == SIGNAL_DAY)].iloc[0]

    assert float(row.open_0915) == pytest.approx(100.0)
    assert float(row.or_high) == pytest.approx(100.5)      # the 09:20 spike, inside the 09:15-09:29 range
    assert float(row.close_t) == pytest.approx(102.0)

    # VWAP(T) by hand: constant per-bar volume, so it is the MEAN typical price over 09:15..11:00.
    # 09:15..09:29 = 15 bars (one at 100.5, fourteen at 100.0); 09:30..10:44 = 75 bars at 100.2;
    # 10:45..11:00 = 16 bars at 102.0. Flat bars => typical price == close.
    expected = (14 * 100.0 + 100.5 + 75 * 100.2 + 16 * 102.0) / 106.0
    assert float(row.vwap_t) == pytest.approx(expected, abs=1e-9)

    # the T-bar snapshot must not see the post-T tape: WINNER trades at 102.00 all afternoon and
    # STOPPER collapses to 95.00, yet both carry the same pre-T VWAP.
    other = f[(f["symbol"] == "STOPPER") & (f["d"] == SIGNAL_DAY)].iloc[0]
    assert float(other.vwap_t) == pytest.approx(float(row.vwap_t), abs=1e-9)


def test_the_acceptance_window_is_the_15_bars_before_T_and_shifts_with_T():
    assert bt.acceptance_window("11:00", 15) == ("10:45", "10:59")
    assert bt.acceptance_window("10:30", 15) == ("10:15", "10:29")
    assert bt.acceptance_window("12:00", 15) == ("11:45", "11:59")


def test_a_single_close_below_running_vwap_fails_the_acceptance_test(db_file):
    """DIPPER differs from WINNER in exactly one of the 15 pre-T bars, and that is disqualifying."""
    conn = bt.open_readonly(db_file)
    try:
        f = bt.load_features(conn, SESSIONS[0], SESSIONS[-1], decision_time="11:00")
    finally:
        conn.close()
    f["d"] = [x.date() if hasattr(x, "date") else x for x in f["d"]]
    win = f[(f["symbol"] == "WINNER") & (f["d"] == SIGNAL_DAY)].iloc[0]
    dip = f[(f["symbol"] == "DIPPER") & (f["d"] == SIGNAL_DAY)].iloc[0]
    assert int(win.n_acc) == int(dip.n_acc) == 15          # both have the full window of bars
    assert int(win.n_acc_ok) == 15                          # all 15 closes above the running VWAP
    assert int(dip.n_acc_ok) == 14                          # the 10:50 dip breaks exactly one


def test_acceptance_failure_removes_the_symbol_from_selection(study):
    _doc, trades = study
    assert "DIPPER" not in _trades(trades, "H1")


def test_exactly_the_two_qualifying_symbols_are_booked(study):
    """The whole cast in one assertion: only WINNER and STOPPER clear (i), (ii) and (iii).

    Without this, every "X not in booked" test above would also pass on an empty trade list.
    """
    _doc, trades = study
    assert set(_trades(trades, "H1")) == {"WINNER", "STOPPER"}
    assert {t.d for t in trades["H1"]} == {SIGNAL_DAY}


# ============================================================ 2. rel_volume_tod
def test_rvol_median_uses_only_prior_sessions_and_needs_ten_valid_ones():
    """The median is over the 20 sessions BEFORE d; fewer than 10 usable priors is a SKIP."""
    sessions = _sessions(25)
    cum = {
        "FULL": {d: 100.0 for d in sessions},
        "SPIKE": {d: (100.0 if i < 24 else 999.0) for i, d in enumerate(sessions)},
        "SHORT": {d: 100.0 for d in sessions[-9:]},        # only 8 priors on the last session
    }
    med, skipped = rvol = bt.rvol_medians(cum, sessions, lookback=20, min_valid=10)

    last = sessions[-1]
    assert med["FULL"][last] == pytest.approx(100.0)
    # SPIKE's own 999 is on the LAST session and must not enter its own denominator
    assert med["SPIKE"][last] == pytest.approx(100.0)
    assert last not in med["SHORT"]                        # 8 < 10 -> skipped, not thinned
    assert skipped >= 9                                     # every SHORT day lacks 10 priors
    assert rvol[1] == skipped


def test_rvol_gate_rejects_a_symbol_at_one_times_normal_volume(study):
    """THINVOL's price path is WINNER's; only its volume differs, and (ii) is what stops it."""
    _doc, trades = study
    booked = _trades(trades, "H1")
    assert "THINVOL" not in booked
    assert booked["WINNER"].rvol == pytest.approx(2.0, abs=1e-6)   # 2x the constant prior median


def test_a_symbol_without_ten_prior_sessions_is_skipped_and_counted(study):
    doc, trades = study
    assert "NOHIST" not in _trades(trades, "H1")
    assert doc["diagnostics"]["11:00"]["rvol_insufficient_history_symbol_days"] > 0


# ============================================================ 3. the relative-strength gate
def test_rs_gate_needs_one_percent_over_the_index_not_just_one_percent(study):
    """LAGGARD is +1.20% from open - clear of the absolute +1.0% floor, short of index+1.0%."""
    _doc, trades = study
    booked = _trades(trades, "H1")
    assert "LAGGARD" not in booked
    w = booked["WINNER"]
    assert w.index_ret_pct == pytest.approx(0.5, abs=1e-6)          # the index is +0.50% at T
    assert w.rs_pct == pytest.approx(1.5, abs=1e-6)                 # +2.00% - +0.50%
    assert w.index_is_real is True


def test_selection_ranks_by_relative_strength_and_caps_at_top_n():
    """Ranking is (return - index return) descending, ties by symbol; the cap is top_n_per_day."""
    f = bt.DayFeatures(SIGNAL_DAY)
    for sym, ret in [("A", 5.0), ("B", 4.0), ("C", 3.0), ("D", 2.5), ("E", 2.4), ("F", 2.3)]:
        f.symbols.append(sym)
        f.ret_pct[sym] = ret
        f.accepted[sym] = True
        f.rvol[sym] = 2.0
    picks = bt.select_day(f, index_ret_pct=0.5, eligible=sorted(f.symbols), params=bt.PARAMS)
    assert picks == ["A", "B", "C", "D", "E"]                        # F is the 6th, and dropped


# ============================================================ 4. entry and the two exits
def test_entry_is_the_open_of_the_bar_after_T(study):
    """WINNER's T bar CLOSES at 102.00 and the 11:01 bar OPENS at 102.00 - the fill is the open."""
    _doc, trades = study
    w = _trades(trades, "H1")["WINNER"]
    assert w.entry_px == pytest.approx(102.0)
    assert w.exit_reason == "squareoff_1515"
    assert w.exit_px == pytest.approx(102.0)                         # flat all afternoon
    assert w.gross_pct == pytest.approx(0.0, abs=1e-9)
    assert w.net_pct == pytest.approx(-_cost(102.0), abs=1e-9)       # pays the round trip, earns nothing


def test_vwap_loss_exit_fills_at_the_FOLLOWING_bar_open_not_the_triggering_close(study):
    """STOPPER's 11:05 bar closes at 99.00 (the trigger) and the 11:06 bar opens at 95.00 (the fill).

    A same-bar fill would book 99.00/102.00 - 1 = -2.94%. The correct fill books
    95.00/102.00 - 1 = -6.86%. The two differ by four rupees, so this cannot pass by accident.
    """
    _doc, trades = study
    s = _trades(trades, "H1")["STOPPER"]
    assert s.exit_reason == "vwap_loss_next_open"
    assert s.entry_px == pytest.approx(102.0)
    assert s.exit_px == pytest.approx(95.0)                          # the FOLLOWING bar's open
    assert s.gross_pct == pytest.approx((95.0 / 102.0 - 1.0) * 100.0, abs=1e-9)
    same_bar = (99.0 / 102.0 - 1.0) * 100.0
    assert s.gross_pct != pytest.approx(same_bar, abs=1e-3)


def test_variant_B_removes_the_stop_and_holds_to_the_1515_squareoff(study):
    """Same selection as H1; STOPPER now rides the collapse to the 15:15 close instead of exiting."""
    _doc, trades = study
    h1 = _trades(trades, "H1")
    b = _trades(trades, "B")
    assert set(h1) == set(b)                                          # identical selection
    assert all(t.exit_reason == "squareoff_1515" for t in trades["B"])
    assert b["STOPPER"].exit_px == pytest.approx(95.0)                # the 15:15 bar's CLOSE
    assert b["WINNER"].exit_px == pytest.approx(h1["WINNER"].exit_px)


def test_the_1515_squareoff_uses_the_1515_bar_close_and_ignores_the_later_tape():
    """The tape runs to 15:29; the exit is the 15:15 bar's close, not the session's last price."""
    times = [f"{h:02d}:{m:02d}" for h in (11, 15) for m in range(0, 60)]
    opens = [100.0] * len(times)
    closes = [100.0] * len(times)
    for i, t in enumerate(times):
        if t == "15:15":
            closes[i] = 110.0                                         # the squareoff price
        elif t > "15:15":
            closes[i] = 200.0                                         # must never be reachable
            opens[i] = 200.0
    got = bt.simulate_exit(times, opens, closes, vwap_t=100.0, stop_pct=None,
                           entry_time="11:01", squareoff_time="15:15")
    assert got == (100.0, 110.0, "squareoff_1515")


def test_a_missing_entry_bar_is_a_skipped_trade_never_a_fill_at_another_price():
    times = ["11:02", "11:03"]
    assert bt.simulate_exit(times, [100.0, 100.0], [100.0, 100.0], vwap_t=100.0, stop_pct=None,
                            entry_time="11:01", squareoff_time="15:15") is None


def test_the_stop_threshold_is_vwap_times_one_minus_the_pct():
    """0.25% under a VWAP(T) of 100.00 is 99.75: a 99.80 close does not trigger, 99.70 does."""
    times = ["11:01", "11:02", "11:03", "15:15"]
    opens = [100.0, 100.0, 90.0, 80.0]
    assert bt.simulate_exit(times, opens, [100.0, 99.80, 100.0, 100.0], vwap_t=100.0,
                            stop_pct=0.25, entry_time="11:01",
                            squareoff_time="15:15")[2] == "squareoff_1515"
    got = bt.simulate_exit(times, opens, [100.0, 99.70, 100.0, 100.0], vwap_t=100.0,
                           stop_pct=0.25, entry_time="11:01", squareoff_time="15:15")
    assert got == (100.0, 90.0, "vwap_loss_next_open")               # fills at the 11:03 OPEN


# ============================================================ 5. cost application
def test_cost_is_one_mis_round_trip_plus_one_tick_of_slippage_each_side():
    cm = CostModel.from_config()
    entry = 102.0
    rt = float(cm.breakeven_pct(Decimal(bt.PARAMS["reference_notional_inr"]), "MIS"))
    tick = float(Decimal(bt.PARAMS["tick_size_inr"]))
    assert bt.trade_cost_pct(cm, bt.PARAMS, entry) == pytest.approx(
        rt + 2.0 * tick / entry * 100.0, abs=1e-12
    )
    # MIS, not the dearer delivery surface
    assert bt.PARAMS["product"] == "MIS"
    assert rt < float(cm.breakeven_pct(Decimal(bt.PARAMS["reference_notional_inr"]), "CNC"))
    # slippage is price-relative: a cheap stock pays a bigger percentage for the same tick
    assert bt.trade_cost_pct(cm, bt.PARAMS, 50.0) > bt.trade_cost_pct(cm, bt.PARAMS, 5000.0)


def test_net_is_gross_minus_exactly_one_round_trip_per_trade(study):
    _doc, trades = study
    for t in trades["H1"]:
        assert t.net_pct == pytest.approx(t.gross_pct - t.cost_pct, abs=1e-12)
        assert t.cost_pct == pytest.approx(_cost(t.entry_px), abs=1e-12)


# ============================================================ 6. the splits
def test_breadth_split_partitions_on_the_share_above_OR_high_at_T():
    p = {**bt.PARAMS, "breadth_trend_day_min": 0.5}
    mk = lambda b: bt.Trade(  # noqa: E731
        variant="H1", symbol="X", d=SIGNAL_DAY, entry_px=100.0, exit_px=100.0,
        exit_reason="squareoff_1515", gross_pct=0.0, cost_pct=0.0, net_pct=0.0,
        rs_pct=1.0, rvol=2.0, index_ret_pct=0.5, index_is_real=True, breadth=b,
        catalyst_at_T=False, catalyst_split_covered=False,
    )
    trades = [mk(0.49), mk(0.50), mk(0.80)]
    cells = bt.split_cells(trades, p)
    assert [t.breadth for t in cells[bt.SPLIT_BREADTH_HIGH]] == [0.50, 0.80]   # >= is inclusive
    assert [t.breadth for t in cells[bt.SPLIT_BREADTH_LOW]] == [0.49]
    assert len(cells[bt.SPLIT_BREADTH_HIGH]) + len(cells[bt.SPLIT_BREADTH_LOW]) == len(trades)


def test_index_regime_split_partitions_on_the_index_return_at_T():
    p = dict(bt.PARAMS)
    mk = lambda r: bt.Trade(  # noqa: E731
        variant="H1", symbol="X", d=SIGNAL_DAY, entry_px=100.0, exit_px=100.0,
        exit_reason="squareoff_1515", gross_pct=0.0, cost_pct=0.0, net_pct=0.0,
        rs_pct=1.0, rvol=2.0, index_ret_pct=r, index_is_real=True, breadth=0.2,
        catalyst_at_T=False, catalyst_split_covered=False,
    )
    cells = bt.split_cells([mk(-0.1), mk(0.0), mk(0.3)], p)
    assert len(cells[bt.SPLIT_INDEX_UP]) == 2                        # 0.0 counts as "up" (>= 0)
    assert len(cells[bt.SPLIT_INDEX_DOWN]) == 1


def test_catalyst_cells_only_hold_trades_inside_the_feeds_coverage():
    p = dict(bt.PARAMS)
    mk = lambda cov, flag: bt.Trade(  # noqa: E731
        variant="H1", symbol="X", d=SIGNAL_DAY, entry_px=100.0, exit_px=100.0,
        exit_reason="squareoff_1515", gross_pct=0.0, cost_pct=0.0, net_pct=0.0,
        rs_pct=1.0, rvol=2.0, index_ret_pct=0.5, index_is_real=True, breadth=0.2,
        catalyst_at_T=flag, catalyst_split_covered=cov,
    )
    cells = bt.split_cells([mk(False, False), mk(True, True), mk(True, False)], p)
    assert len(cells[bt.SPLIT_CATALYST_TRUE]) == 1
    assert len(cells[bt.SPLIT_CATALYST_FALSE]) == 1                  # the uncovered trade is in neither
    assert len(cells[bt.SPLIT_ALL]) == 3


def test_catalyst_flag_window_runs_from_the_prior_sessions_1500_to_T():
    sessions = [date(2026, 8, 31), date(2026, 9, 1)]
    d = sessions[1]
    in_window = datetime.combine(sessions[0], time(18, 0), tzinfo=IST)     # overnight
    too_old = datetime.combine(sessions[0], time(14, 0), tzinfo=IST)       # before 15:00
    too_late = datetime.combine(d, time(11, 30), tzinfo=IST)               # after T
    assert bt._catalyst_at("X", d, "11:00", set(), {"X": [in_window]}, sessions) is True
    assert bt._catalyst_at("X", d, "11:00", set(), {"X": [too_old]}, sessions) is False
    assert bt._catalyst_at("X", d, "11:00", set(), {"X": [too_late]}, sessions) is False
    assert bt._catalyst_at("X", d, "11:00", {(d, "X")}, {}, sessions) is True   # the watchlist source


# ============================================================ 7. metrics + the plan's boolean
def test_promotable_is_the_plans_four_part_boolean_and_nothing_else():
    p = {**bt.PARAMS, "promote_min_n": 3, "promote_min_t": 2.0,
         "promote_min_cpcv_positive_share": 0.6}
    mk = lambda i, net: bt.Trade(  # noqa: E731
        variant="H1", symbol="X", d=SESSIONS[i % len(SESSIONS)], entry_px=100.0, exit_px=100.0,
        exit_reason="squareoff_1515", gross_pct=net, cost_pct=0.0, net_pct=net,
        rs_pct=1.0, rvol=2.0, index_ret_pct=0.5, index_is_real=True, breadth=0.2,
        catalyst_at_T=False, catalyst_split_covered=False,
    )
    losers = bt.metrics([mk(i, -1.0 + (i % 3) * 0.1) for i in range(20)], p)
    assert losers["mean_net_pct"] < 0 and losers["promotable"] is False
    thin = bt.metrics([mk(i, 1.0 + (i % 3) * 0.1) for i in range(2)], {**p, "promote_min_n": 200})
    assert thin["n"] == 2 and thin["promotable"] is False             # n floor alone is decisive


def test_metrics_report_every_required_field(study):
    doc, _trades = study
    s = doc["results"]["H1"]["splits"][bt.SPLIT_ALL]
    for key in ("n", "mean_net_pct", "median_net_pct", "win_rate", "t_stat", "promotable"):
        assert key in s
    assert "positive_share" in s["cpcv"]
    assert doc["meta"]["parameter_sweep_run"] is False
    assert doc["meta"]["variant_selection_performed"] is False


def test_cpcv_degrades_honestly_on_a_one_session_population(study):
    doc, _trades = study
    cv = doc["results"]["H1"]["splits"][bt.SPLIT_ALL]["cpcv"]
    assert cv["n_obs_sessions"] == 1                                  # one signal day in the fixture
    assert cv["n_splits"] == 0
    assert cv["positive_share"] is None
    assert doc["results"]["H1"]["splits"][bt.SPLIT_ALL]["promotable"] is False


def test_purged_kfold_fallback_honours_purge_and_embargo():
    splits = bt._purged_kfold_splits(120, n_folds=6, purge=5, embargo=5)
    assert len(splits) == 6
    for train, test in splits:
        lo, hi = int(test.min()), int(test.max())
        assert not set(train) & set(test)
        assert all(j < lo - 5 or j > hi + 5 for j in train)


# ============================================================ 8. the CLI: report, JSON, refusal
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

    text = capsys.readouterr().out
    assert text.isascii()                                            # the Windows console is cp1252
    assert text.index("STEP 1 - SAMPLE SHAPE") < text.index("STEP 2 - RESULTS")
    assert "promotable=True in NO variant x split cell." in text
    assert str(out) in text


def test_refuses_a_missing_database_file(tmp_path, capsys):
    rc = bt.main(["--db", str(tmp_path / "nope.duckdb"), "--out", str(tmp_path / "x.json")])
    assert rc == 2
    err = capsys.readouterr().err
    assert "REFUSING TO RUN" in err and "no such database file" in err
    assert not (tmp_path / "x.json").exists()


def test_refuses_a_locked_database_file(db_file, tmp_path, module_clock, capsys):
    """A running engine holds market.duckdb read-write; DuckDB then refuses a read-only attach."""
    holder = MarketStore(db_file, tmp_path / "parquet2", module_clock).open()
    try:
        rc = bt.main(["--db", str(db_file), "--out", str(tmp_path / "y.json")])
    finally:
        holder.close()
    assert rc == 2
    err = capsys.readouterr().err
    assert "REFUSING TO RUN" in err and "cannot open read-only" in err and "mt-engine" in err
    assert not (tmp_path / "y.json").exists()


# ============================================================ 9. the ATR% tercile split (R3)
def _ladder(n: int = 15, step: float = 3.0, half_range: float = 1.0):
    """``n`` daily sessions before ``SIGNAL_DAY`` whose closes climb by ``step``.

    Every TR after the first is the gap term ``step + half_range``; the first bar of the ATR WINDOW
    has no prior close inside the window, so its TR is the plain high-low ``2 * half_range``. With
    the default 15 sessions the window is the last 14, so session 0 is read by nothing.
    """
    days = _sessions(n, SESSIONS[0])
    closes = [100.0 + step * i for i in range(n)]
    return (days, [c + half_range for c in closes], [c - half_range for c in closes], closes)


def test_atr14_is_the_mean_true_range_of_the_14_sessions_strictly_before_the_signal_day():
    """TRs by hand: one plain high-low of 2.00 then thirteen gap TRs of 4.00 -> ATR = 54/14."""
    days, highs, lows, closes = _ladder()
    expected = ((2.0 + 13 * 4.0) / 14.0) / closes[-1] * 100.0
    assert closes[-1] == 142.0
    assert bt.atr_pct_at((days, highs, lows, closes), SIGNAL_DAY, lookback=14) == pytest.approx(
        expected, abs=1e-12
    )
    # PINNED READING: the window is EXACTLY 14 sessions, so the 15th session back is read by nothing.
    assert bt.atr_pct_at((days[1:], highs[1:], lows[1:], closes[1:]), SIGNAL_DAY,
                         lookback=14) == pytest.approx(expected, abs=1e-12)
    # and a bar ON the signal day cannot move it by a basis point
    assert bt.atr_pct_at(([*days, SIGNAL_DAY], [*highs, 200.0], [*lows, 1.0], [*closes, 100.0]),
                         SIGNAL_DAY, lookback=14) == pytest.approx(expected, abs=1e-12)


def test_atr_needs_a_full_lookback_of_prior_sessions_and_a_positive_prior_close():
    days, highs, lows, closes = _ladder(n=13)
    assert bt.atr_pct_at((days, highs, lows, closes), SIGNAL_DAY, lookback=14) is None
    assert bt.atr_pct_at(None, SIGNAL_DAY, lookback=14) is None
    days, highs, lows, closes = _ladder()
    assert bt.atr_pct_at((days, highs, lows, [*closes[:-1], 0.0]), SIGNAL_DAY, lookback=14) is None


def test_tercile_cuts_and_assignment_partition_the_population():
    """Six values 1..6: the exclusive quantiles land in (2,3) and (4,5), so the cells are 2/2/2."""
    vals = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
    cuts = bt.atr_tercile_cuts(vals)
    assert 2.0 <= cuts[0] < 3.0 and 4.0 <= cuts[1] < 5.0
    assert [bt.atr_tercile_of(v, cuts) for v in vals] == [
        bt.SPLIT_ATR_LOW, bt.SPLIT_ATR_LOW, bt.SPLIT_ATR_MID,
        bt.SPLIT_ATR_MID, bt.SPLIT_ATR_HIGH, bt.SPLIT_ATR_HIGH,
    ]
    assert bt.atr_tercile_cuts([1.0, 2.0]) is None                    # fewer than three: no cut
    assert bt.atr_tercile_of(3.0, None) is None                       # and nothing is classified
    assert bt.atr_tercile_of(None, cuts) is None
    assert bt.atr_tercile_of(float("nan"), cuts) is None


def test_stamp_atr_terciles_labels_every_trade_and_keeps_the_unclassified_ones():
    mk = lambda sym: bt.Trade(  # noqa: E731
        variant="H1", symbol=sym, d=SIGNAL_DAY, entry_px=100.0, exit_px=100.0,
        exit_reason="squareoff_1515", gross_pct=0.0, cost_pct=0.0, net_pct=0.0,
        rs_pct=1.0, rvol=2.0, index_ret_pct=0.5, index_is_real=True, breadth=0.2,
        catalyst_at_T=False, catalyst_split_covered=False,
    )
    trades = [mk(s) for s in ("A", "B", "C", "D", "E")]
    atr = {
        ("A", SIGNAL_DAY): 1.0, ("B", SIGNAL_DAY): 2.0, ("C", SIGNAL_DAY): 3.0,
        ("D", SIGNAL_DAY): 9.0, ("E", SIGNAL_DAY): None,
    }
    block = bt.stamp_atr_terciles([trades], atr)
    assert block["n_symbol_days"] == 5 and block["n_symbol_days_without_atr"] == 1
    assert [t.atr_cell for t in trades[:4]] == [
        bt.SPLIT_ATR_LOW, bt.SPLIT_ATR_MID, bt.SPLIT_ATR_MID, bt.SPLIT_ATR_HIGH
    ]
    assert trades[4].atr_cell is None and trades[4].atr_pct is None
    assert sum(block["n_trades_by_cell"].values()) == len(trades)
    cells = bt.split_cells(trades, bt.PARAMS)
    assert sum(len(cells[c]) for c in bt.ATR_REPORT_CELLS) == len(cells[bt.SPLIT_ALL])


def test_the_study_stamps_each_trade_with_its_daily_atr_pct_from_bars_1d(study):
    """End to end: bars_1d is read, and the signal day's own 100-wide daily bar is not in the window."""
    doc, trades = study
    booked = _trades(trades, "H1")
    assert booked["WINNER"].atr_pct == pytest.approx(WINNER_ATR_PCT, abs=1e-9)
    assert booked["STOPPER"].atr_pct == pytest.approx(STOPPER_ATR_PCT, abs=1e-9)
    ab = doc["coverage"]["atr_pct_tercile_split"]
    assert ab["source_table"] == "bars_1d" and ab["descriptive_not_tradeable"] is True
    # only WINNER and STOPPER carry daily bars; the robustness variants reach a symbol that has none,
    # and it is COUNTED rather than silently dropped.
    assert ab["n_symbol_days_with_atr"] == 2
    assert ab["n_symbol_days_without_atr"] == ab["n_symbol_days"] - 2 >= 1
    # two known values is fewer than three, so NO cut exists: every trade stays unclassified and
    # NOTHING is dropped - the residue cell is what keeps the four cells a partition.
    assert ab["cuts"] is None
    assert all(t.atr_cell is None for v in trades.values() for t in v)
    assert ab["n_trades_by_cell"][bt.SPLIT_ATR_UNCLASSIFIED] == sum(len(v) for v in trades.values())


def test_the_four_atr_cells_partition_every_variants_trades(study):
    doc, _trades = study
    for block in doc["results"].values():
        total = block["splits"][bt.SPLIT_ALL]["n"]
        assert sum(block["splits"][c]["n"] for c in bt.ATR_REPORT_CELLS) == total


def test_the_report_prints_the_atr_block_with_gross_and_labels_it_descriptive(study):
    doc, _trades = study
    text = bt.render_text(doc)
    assert text.isascii()                                             # the Windows console is cp1252
    assert "STEP 2B - ATR% TERCILE SPLIT" in text
    assert text.index("STEP 2B") < text.index("STEP 3 - PROMOTABLE")
    assert all(cell in text for cell in bt.ATR_REPORT_CELLS)
    assert any("DESCRIPTIVE, NOT TRADEABLE" in n for n in doc["notes"])
    assert any("ATR% SURVIVORSHIP" in n for n in doc["notes"])
    s = doc["results"]["H1"]["splits"][bt.SPLIT_ALL]
    assert s["median_gross_pct"] is not None and s["median_net_pct"] is not None
