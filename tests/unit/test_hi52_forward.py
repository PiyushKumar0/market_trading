"""``engine.learning.hi52_forward`` (the rec card's forward-test line) and
``engine.strategy.hi52_edge.sizing_notional``. The verdict script's own report and rule are pinned in
``tests/offline/test_hi52_forward_verdict.py``."""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from engine.core.db import connect
from engine.learning.hi52_forward import (
    MIN_SIGNALS,
    PROMOTION_DATE,
    VERDICT_HORIZON,
    Series,
    Signal,
    load_signal_rows,
    measure_all,
    measure_signal,
)
from engine.strategy import hi52_edge

COST_PCT = 0.5
START = PROMOTION_DATE               # Monday 2026-09-14


def _sessions(n: int, *, start: date = START, holidays: frozenset[date] = frozenset()) -> list[date]:
    """``n`` ascending trading sessions from ``start``: weekdays minus ``holidays``."""
    out: list[date] = []
    d = start
    while len(out) < n:
        if d.weekday() < 5 and d not in holidays:
            out.append(d)
        d += timedelta(days=1)
    return out


def _series(
    symbol: str,
    *,
    t20_close: float,
    t10_close: float = 100.0,
    t5_close: float = 100.0,
    holidays: frozenset[date] = frozenset(),
) -> Series:
    """A 21-session series whose signal day is index 0: the entry is ``open[0]`` and T+k is
    ``close[k-1]``; every other bar is inert (100.00)."""
    n = 21
    dates = _sessions(n, holidays=holidays)
    opens = [100.0] * n
    closes = [100.0] * n
    closes[4] = t5_close
    closes[9] = t10_close
    closes[19] = t20_close
    return Series(symbol=symbol, dates=dates, open=opens, close=closes)


def test_the_verdict_rule_is_exactly_as_pre_registered():
    assert MIN_SIGNALS == 20 and VERDICT_HORIZON == 20


def test_the_entry_is_the_journal_day_s_own_open_and_exits_count_sessions_across_a_holiday_gap():
    """Entry is the journal day's OWN open — the backtest's anchor — and T+k is the close of the
    k-th session of the HOLD, never calendar days (2026-09-12 manager decision).

    `d` is the session AFTER the trigger `y`, so `open(d)` IS `backtest_hi52`'s `open(y+1)` and
    `close(d + k-1)` IS its `close(y+k)`: the forward test measures the same quantity the registered
    edge was measured as. The fixture puts a Tuesday-to-Friday exchange holiday immediately after
    the signal day, so the T+20 exit lands 31 calendar days out; calendar arithmetic would book the
    wrong bar.
    """
    holidays = frozenset({date(2026, 9, 15), date(2026, 9, 16), date(2026, 9, 17), date(2026, 9, 18)})
    series = _series("GAPPY", t20_close=130.0, t10_close=110.0, t5_close=105.0, holidays=holidays)
    assert series.dates[0] == date(2026, 9, 14)
    assert series.dates[1] == date(2026, 9, 21)           # the next session, 7 calendar days on
    m = measure_signal(series, date(2026, 9, 14), cost_pct=COST_PCT)
    assert m is not None
    assert m.entry_date == m.signal_date == date(2026, 9, 14)   # same session, no day-1 discard
    assert m.entry_px == 100.0
    assert series.dates[19] == date(2026, 10, 15)
    assert (series.dates[19] - m.signal_date).days == 31   # 20 SESSIONS of hold, 31 calendar days
    assert m.gross[20] == pytest.approx(30.0, abs=1e-9)   # close[19]/open[0] - 1
    assert m.net[20] == pytest.approx(30.0 - COST_PCT, abs=1e-9)
    assert m.gross[10] == pytest.approx(10.0, abs=1e-9)   # the T+10 cell reads close[9]
    assert m.gross[5] == pytest.approx(5.0, abs=1e-9)     # …and T+5 reads close[4]


def test_the_horizon_indexing_is_the_backtest_s_own_k_session_hold():
    """The anchor change is only worth anything if the HORIZON matches too: `hi52`'s registered edge
    is `close(y+20)/open(y+1)`, a 20-session hold. Measured off
    `d = y+1` that is `close[19]`, not `close[20]` — one bar of drift would quietly make the kill
    criterion test a 21-session rule."""
    dates = _sessions(21)
    # A series that moves ONLY on the 21st session: a T+20 that read close[20] would see +50%.
    series = Series("EXACT", dates, [100.0] * 21, [100.0] * 20 + [150.0])
    m = measure_signal(series, dates[0], cost_pct=COST_PCT)
    assert m is not None and m.gross[20] == pytest.approx(0.0, abs=1e-9)
    # …and a series that moves only on the 20th session is exactly what T+20 must see.
    series = Series("EXACT2", dates, [100.0] * 21, [100.0] * 19 + [150.0, 100.0])
    m = measure_signal(series, dates[0], cost_pct=COST_PCT)
    assert m is not None and m.gross[20] == pytest.approx(50.0, abs=1e-9)


def test_horizons_are_measured_on_their_own_event_sets():
    """A signal old enough for T+10 but not T+20 has a T+10 cell ONLY."""
    young = Series(symbol="YOUNG", dates=_sessions(12),
                   open=[100.0] * 12, close=[100.0] * 9 + [105.0, 100.0, 100.0])
    m = measure_signal(young, young.dates[0], cost_pct=COST_PCT)
    assert m is not None
    assert set(m.net) == {5, 10}                          # T+20 is not reached, not zero


def test_unmeasurable_signals_are_counted_by_REASON_never_silently_dropped():
    """A bhavcopy that has not landed yet (``d`` past the last stored session) must not read like a
    store hole (``d`` missing from inside the stored range)."""
    good = _series("GOOD", t20_close=103.0)
    signals = [
        Signal(START, "GOOD"),
        Signal(START, "NOBARS"),                          # no series at all
        Signal(date(2026, 9, 19), "GOOD"),                # a Saturday INSIDE the range: a hole
        Signal(good.dates[-1] + timedelta(days=3), "GOOD"),   # today: no bhavcopy row YET
    ]
    trades, skipped = measure_all(signals, {"GOOD": good}, cost_pct=COST_PCT)
    assert [t.symbol for t in trades] == ["GOOD"]
    assert skipped == {"no_bars": 1, "unanchored": 1, "no_entry_yet": 1, "bad_entry_bar": 0}


def test_a_signal_on_the_last_stored_session_is_entered_but_reaches_no_horizon():
    """A signal journalled on the last stored session HAS an entry — a measured trade with an empty
    cell set, not a skip."""
    good = _series("GOOD", t20_close=103.0)
    trades, skipped = measure_all([Signal(good.dates[-1], "GOOD")], {"GOOD": good},
                                  cost_pct=COST_PCT)
    assert len(trades) == 1 and trades[0].net == {} and trades[0].gross == {}
    assert skipped == {"no_bars": 0, "unanchored": 0, "no_entry_yet": 0, "bad_entry_bar": 0}


def test_malformed_bars_fail_to_no_measurement_never_to_a_crash():
    dates = _sessions(21)
    bad_entry = Series("BADENTRY", dates, [0.0] + [100.0] * 20, [100.0] * 21)
    assert measure_signal(bad_entry, dates[0], cost_pct=COST_PCT) is None
    _, skipped = measure_all([Signal(dates[0], "BADENTRY")], {"BADENTRY": bad_entry},
                             cost_pct=COST_PCT)
    assert skipped["bad_entry_bar"] == 1                  # a zero open is a bar defect, not an age
    bad_exit = Series("BADEXIT", dates, [100.0] * 21,
                      [100.0] * 19 + [float("nan"), 100.0])
    m = measure_signal(bad_exit, dates[0], cost_pct=COST_PCT)
    assert m is not None and 20 not in m.net              # that cell drops; T+10 survives
    assert 10 in m.net


def _state_db(tmp_path: Path, rows: list[tuple[str, str, str]]) -> Path:
    """A state.db with the real ``prescreen_day_slots`` shape and ``rows`` of (d, symbol, sid)."""
    path = tmp_path / "state.db"
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE prescreen_day_slots (d TEXT NOT NULL, symbol TEXT NOT NULL, "
        "strategy_id TEXT NOT NULL, published_at TEXT NOT NULL, evaluated INTEGER NOT NULL "
        "DEFAULT 1, score REAL, forwarded INTEGER NOT NULL DEFAULT 0, "
        "PRIMARY KEY (d, symbol, strategy_id))"
    )
    conn.executemany(
        "INSERT INTO prescreen_day_slots (d, symbol, strategy_id, published_at, evaluated, "
        "score, forwarded) VALUES (?, ?, ?, '2026-09-14T09:15:00+05:30', 1, 0.97, 0)", rows,
    )
    conn.commit()
    conn.close()
    return path


def test_the_population_is_hi52_rows_from_the_promotion_day_onward(tmp_path):
    """Every published hi52 row in the window — and nothing from before the promotion (the SHADOW
    population, a different rule) and nothing from another strategy."""
    path = _state_db(tmp_path, [
        ("2026-09-11", "OLDSHADOW", "hi52"),              # pre-promotion: a different rule
        ("2026-09-14", "BHEL", "hi52"),
        ("2026-09-14", "ABB", "hi52"),
        ("2026-09-14", "TCS", "brk20"),                   # another strategy
        ("2026-09-15", "BHEL", "hi52"),
        ("2026-12-01", "LATER", "hi52"),                  # beyond as_of
        ("2026-09-1x", "CORRUPT", "hi52"),                # sorts inside the window, will not parse:
                                                          # skipped, never a crash
    ])
    conn = connect(path)                                  # the engine's own (Row-factory) connection
    try:
        rows = load_signal_rows(conn, start=PROMOTION_DATE, as_of=date(2026, 9, 30))
    finally:
        conn.close()
    assert rows == [
        Signal(date(2026, 9, 14), "ABB"),
        Signal(date(2026, 9, 14), "BHEL"),
        Signal(date(2026, 9, 15), "BHEL"),
    ]


def test_the_strategy_keyword_selects_another_strategy_s_rows_and_defaults_to_hi52(tmp_path):
    """``scripts/cat_shadow_verdict.py`` reads ``cat``'s population through the same loader; the
    default keeps every hi52 caller on hi52."""
    path = _state_db(tmp_path, [
        ("2026-09-14", "BHEL", "hi52"), ("2026-09-14", "TCS", "cat"), ("2026-09-15", "ABB", "cat"),
    ])
    conn = connect(path)
    try:
        window = {"start": PROMOTION_DATE, "as_of": date(2026, 9, 30)}
        assert load_signal_rows(conn, **window) == [Signal(date(2026, 9, 14), "BHEL")]
        assert load_signal_rows(conn, **window, strategy_id="cat") == [
            Signal(date(2026, 9, 14), "TCS"), Signal(date(2026, 9, 15), "ABB")]
    finally:
        conn.close()


def test_the_sized_notional_is_the_7_1_cap_and_nothing_chosen_by_hand():
    """§7.1 caps a swing position at `swing_position_pct`% of equity charged at `overnight_gap_mult`
    × the stop distance, so the notional is `equity × 2% / (2.5 × 6%)` = equity/7.5 — the price
    cancels out of `qty_max × price`. Three readings of the same formula, at the three equities the
    promotion's paperwork quotes."""
    at = lambda eq: hi52_edge.sizing_notional(  # noqa: E731
        Decimal(eq), position_pct=Decimal("2.0"), gap_mult=Decimal("2.5"), stop_pct=Decimal("6.0"))
    assert at("40000") == pytest.approx(Decimal("5333.33"), abs=Decimal("0.01"))
    assert at("39256.65") == pytest.approx(Decimal("5234.22"), abs=Decimal("0.01"))
    assert at("36000") == Decimal("4800")                 # the equity_floor_rung, the worst book
    # A wider stop SHRINKS the notional and RAISES the floor — the half of the derivation that a
    # stop change must move, which is why STOP_PCT is read from the scanner and never re-typed.
    assert at("36000") > hi52_edge.sizing_notional(
        Decimal("36000"), position_pct=Decimal("2.0"), gap_mult=Decimal("2.5"),
        stop_pct=Decimal("12.0"))
    assert hi52_edge.STOP_PCT == Decimal("6.0")
    for bad in ({"equity": Decimal("0")}, {"stop_pct": Decimal("0")}, {"gap_mult": Decimal("-1")}):
        kwargs = {"equity": Decimal("36000"), "position_pct": Decimal("2.0"),
                  "gap_mult": Decimal("2.5"), "stop_pct": Decimal("6.0"), **bad}
        with pytest.raises(ValueError):                   # a tiny notional is a spurious DEMOTE
            hi52_edge.sizing_notional(kwargs.pop("equity"), **kwargs)
