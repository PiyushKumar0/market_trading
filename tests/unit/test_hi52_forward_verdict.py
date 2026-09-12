"""scripts/hi52_forward_verdict.py — the mechanical kill criterion for the 2026-09-12 promotion.

The script's whole job is to answer ONE pre-registered question on the promoted population, so these
tests are about the answer being mechanical: a DEMOTE case, a HOLD case, an INSUFFICIENT case, the
two boundaries the rule is written on (``n >= 20``, ``median <= 0``, ``hit < 50%``), and the session
arithmetic — the journal day's OWN open as the entry (the backtest's anchor, 2026-09-12) and T+k
exits counted in SESSIONS of the hold, over a synthetic bars fixture whose calendar deliberately
contains a holiday gap.

Costs come from the repo's own CostModel at the script's reference notional, never a frozen
constant: a costs.yaml re-scrape must move the expected net with the model rather than break these
tests for the wrong reason. The registered edge is pinned to the SAME derivation for the same
reason — see ``test_the_registered_edge_is_this_script_s_derivation``.
"""

from __future__ import annotations

import importlib.util
import sqlite3
import sys
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "hi52_forward_verdict.py"
_spec = importlib.util.spec_from_file_location("mt_hi52_forward_verdict", _SCRIPT)
fv = importlib.util.module_from_spec(_spec)
sys.modules["mt_hi52_forward_verdict"] = fv
_spec.loader.exec_module(fv)

from engine.strategy.cost_model import CostModel  # noqa: E402

COST_PCT = float(CostModel.from_config().breakeven_pct(fv.REFERENCE_NOTIONAL, fv.PRODUCT))
#: The live §7.1 table, by repo root rather than by CWD — the derivation reads the real file.
_LIMITS = fv.repo_root() / "config" / "limits.yaml"
START = fv.PROMOTION_DATE               # Monday 2026-09-14


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
    entry_open: float = 100.0,
    n: int = 21,
    holidays: frozenset[date] = frozenset(),
) -> fv.Series:
    """A 21-session series whose signal day is index 0: the entry is ``open[0]`` (the journal day's
    OWN open) and T+k is ``close[k-1]`` — the k-th session of the hold, the entry session counting
    as the first. So T+5 is ``close[4]``, T+10 ``close[9]``, T+20 ``close[19]``; index 20 is a spare
    session that no horizon reads, and every other bar is inert (100.00)."""
    dates = _sessions(n, holidays=holidays)
    opens = [100.0] * n
    closes = [100.0] * n
    opens[0] = entry_open
    closes[4] = t5_close
    closes[9] = t10_close
    closes[19] = t20_close
    return fv.Series(symbol=symbol, dates=dates, open=opens, close=closes)


def _population(n_signals: int, *, t20_close: float) -> tuple[list, dict[str, fv.Series]]:
    """``n_signals`` one-per-symbol signals, all on the same day, all with the same T+20 outcome."""
    signals = [fv.Signal(START, f"SYM{i:02d}") for i in range(n_signals)]
    series = {s.symbol: _series(s.symbol, t20_close=t20_close) for s in signals}
    return signals, series


def _report(signals, series, *, as_of: date = date(2026, 12, 31)) -> dict:
    return fv.build_report(signals, series, cost_pct=COST_PCT, as_of=as_of, start=START)


# ============================================================ 1. DEMOTE
def test_a_flat_population_of_twenty_signals_demotes():
    """20 signals that go NOWHERE gross are a losing rule after one round trip: the median net is
    the cost itself, negative, and nothing hits. Both clauses of the rule fire."""
    doc = _report(*_population(20, t20_close=100.0))
    cell = doc["horizons"]["20"]
    assert cell["n"] == 20
    assert cell["median_net"] == pytest.approx(-COST_PCT, abs=1e-4)
    assert cell["hit_rate"] == 0.0
    assert doc["verdict"] == fv.VERDICT_DEMOTE
    assert "median net" in doc["reason"] and "hit rate" in doc["reason"]
    # The report tells the operator what demotion IS — both edits, not one.
    text = fv.render(doc)
    assert "NO_EDGE_SHADOW_STRATEGIES" in text and "expected_edge_pct" in text


# ============================================================ 2. HOLD
def test_twenty_signals_drifting_three_percent_hold():
    doc = _report(*_population(20, t20_close=103.0))
    cell = doc["horizons"]["20"]
    assert cell["n"] == 20
    assert cell["median_gross"] == pytest.approx(3.0, abs=1e-4)
    assert cell["median_net"] == pytest.approx(3.0 - COST_PCT, abs=1e-4)
    assert cell["hit_rate"] == 1.0
    assert doc["verdict"] == fv.VERDICT_HOLD


# ============================================================ 3. INSUFFICIENT
def test_nineteen_signals_are_insufficient_however_bad_they_look():
    """Below the pre-registered n the rule has NOTHING to say — and says that, rather than HOLD.
    "Not yet refuted" and "no evidence" must never read the same."""
    doc = _report(*_population(19, t20_close=90.0))
    assert doc["horizons"]["20"]["n"] == 19
    assert doc["horizons"]["20"]["median_net"] < 0        # it looks terrible…
    assert doc["verdict"] == fv.VERDICT_INSUFFICIENT      # …and that is not a verdict yet
    assert "19 measured" in doc["reason"]
    assert "VALIDATION, not income" in fv.render(doc)


# ============================================================ 4. the rule's own boundaries
def test_the_verdict_rule_is_exactly_as_pre_registered():
    """The three comparisons, at their boundaries, on the pure function — no bars, no stores."""
    assert fv.MIN_SIGNALS == 20 and fv.VERDICT_HORIZON == 20
    at = lambda n, med, hit: fv.verdict({"20": {"n": n, "median_net": med, "hit_rate": hit}})[0]  # noqa: E731
    assert at(19, -9.0, 0.0) == fv.VERDICT_INSUFFICIENT   # n < 20 wins over everything
    assert at(20, 0.0, 0.9) == fv.VERDICT_DEMOTE          # median <= 0 (zero is a failure)
    assert at(20, 0.0001, 0.9) == fv.VERDICT_HOLD
    assert at(20, 1.0, 0.4999) == fv.VERDICT_DEMOTE       # hit < 50%
    assert at(20, 1.0, 0.50) == fv.VERDICT_HOLD           # exactly 50% passes
    # A cell that cannot be summarised at all fails to the SAFE side, never to HOLD.
    assert fv.verdict({"20": {"n": 25, "median_net": None, "hit_rate": None}})[0] == fv.VERDICT_DEMOTE
    assert fv.verdict({})[0] == fv.VERDICT_INSUFFICIENT


def test_a_half_hitting_population_with_a_positive_median_holds():
    """Boundary population: 10 flat (net = -cost, a miss) and 10 at +3%. Hit rate is exactly 50%
    and the median is positive, so BOTH clauses are satisfied and the rule HOLDs."""
    flat_s, flat_series = _population(10, t20_close=100.0)
    up_s, up_series = _population(10, t20_close=103.0)
    up_s = [fv.Signal(s.d, s.symbol + "X") for s in up_s]
    up_series = {s.symbol: _series(s.symbol, t20_close=103.0) for s in up_s}
    doc = _report(flat_s + up_s, {**flat_series, **up_series})
    cell = doc["horizons"]["20"]
    assert (cell["n"], cell["hit_rate"]) == (20, 0.5)
    assert cell["median_net"] > 0
    assert doc["verdict"] == fv.VERDICT_HOLD


# ============================================================ 5. session arithmetic
def test_the_entry_is_the_journal_day_s_own_open_and_exits_count_sessions_across_a_holiday_gap():
    """Entry is the journal day's OWN open — the backtest's anchor — and T+k is the close of the
    k-th session of the HOLD, never calendar days (2026-09-12 manager decision).

    `d` is the session AFTER the trigger `y`, so `open(d)` IS `backtest_hi52`'s `open(y+1)` and
    `close(d + k-1)` IS its `close(y+k)`: the forward test measures the same quantity the registered
    edge was measured as. The fixture puts a Tuesday-to-Friday exchange holiday immediately after
    the signal day, so the T+20 exit lands 32 calendar days out; calendar arithmetic would book the
    wrong bar.
    """
    holidays = frozenset({date(2026, 9, 15), date(2026, 9, 16), date(2026, 9, 17), date(2026, 9, 18)})
    series = _series("GAPPY", t20_close=130.0, t10_close=110.0, t5_close=105.0, holidays=holidays)
    assert series.dates[0] == date(2026, 9, 14)
    assert series.dates[1] == date(2026, 9, 21)           # the next session, 7 calendar days on
    m = fv.measure_signal(series, date(2026, 9, 14), cost_pct=COST_PCT)
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
    is `close(y+20)/open(y+1)`, a 20-session hold. Measured off `d = y+1` that is `close[19]`, not
    `close[20]` — one bar of drift would quietly make the kill criterion test a 21-session rule."""
    dates = _sessions(21)
    # A series that moves ONLY on the 21st session: a T+20 that read close[20] would see +50%.
    series = fv.Series("EXACT", dates, [100.0] * 21, [100.0] * 20 + [150.0])
    m = fv.measure_signal(series, dates[0], cost_pct=COST_PCT)
    assert m is not None and m.gross[20] == pytest.approx(0.0, abs=1e-9)
    # …and a series that moves only on the 20th session is exactly what T+20 must see.
    series = fv.Series("EXACT2", dates, [100.0] * 21, [100.0] * 19 + [150.0, 100.0])
    m = fv.measure_signal(series, dates[0], cost_pct=COST_PCT)
    assert m is not None and m.gross[20] == pytest.approx(50.0, abs=1e-9)


def test_horizons_are_measured_on_their_own_event_sets():
    """A signal old enough for T+10 but not T+20 counts at T+10 ONLY — a forward test reads as its
    data arrives, and pretending the cells share a population would misstate both."""
    young = fv.Series(symbol="YOUNG", dates=_sessions(12),
                      open=[100.0] * 12, close=[100.0] * 9 + [105.0, 100.0, 100.0])
    m = fv.measure_signal(young, young.dates[0], cost_pct=COST_PCT)
    assert m is not None
    assert set(m.net) == {5, 10}                          # T+20 is not reached, not zero
    cells = fv.summarize([m])
    assert cells["10"]["n"] == 1 and cells["20"]["n"] == 0
    assert fv.verdict(cells)[0] == fv.VERDICT_INSUFFICIENT


def test_unmeasurable_signals_are_counted_by_REASON_never_silently_dropped():
    """An ``n`` that shrank because the newest signal's bhavcopy has not landed yet (the ordinary
    state of a forward test on the morning it runs) must not read like one that shrank because the
    store has a hole in it — and with the entry at ``open(d)`` the two are told apart by WHERE ``d``
    sits: past the last stored session, or missing from inside the stored range."""
    good = _series("GOOD", t20_close=103.0)
    signals = [
        fv.Signal(START, "GOOD"),
        fv.Signal(START, "NOBARS"),                       # no series at all
        fv.Signal(date(2026, 9, 19), "GOOD"),             # a Saturday INSIDE the range: a hole
        fv.Signal(good.dates[-1] + timedelta(days=3), "GOOD"),   # today: no bhavcopy row YET
    ]
    trades, skipped = fv.measure_all(signals, {"GOOD": good}, cost_pct=COST_PCT)
    assert [t.symbol for t in trades] == ["GOOD"]
    assert skipped == {"no_bars": 1, "unanchored": 1, "no_entry_yet": 1, "bad_entry_bar": 0}
    doc = _report(signals, {"GOOD": good})
    assert doc["population"] == {
        "signals_journalled": 4, "signals_measured": 1, "skipped": skipped,
        "symbols": ["GOOD", "NOBARS"],
    }


def test_a_signal_on_the_last_stored_session_is_entered_but_reaches_no_horizon():
    """The anchor moved to ``open(d)``, so a signal journalled on the last stored session HAS an
    entry — it is a measured trade with an empty cell set, not a skip. It must count in neither the
    verdict cell nor the skip tally, or the two would disagree about the same row."""
    good = _series("GOOD", t20_close=103.0)
    trades, skipped = fv.measure_all([fv.Signal(good.dates[-1], "GOOD")], {"GOOD": good},
                                     cost_pct=COST_PCT)
    assert len(trades) == 1 and trades[0].net == {} and trades[0].gross == {}
    assert skipped == {"no_bars": 0, "unanchored": 0, "no_entry_yet": 0, "bad_entry_bar": 0}
    assert fv.summarize(trades)["20"]["n"] == 0


def test_malformed_bars_fail_to_no_measurement_never_to_a_crash():
    dates = _sessions(21)
    bad_entry = fv.Series("BADENTRY", dates, [0.0] + [100.0] * 20, [100.0] * 21)
    assert fv.measure_signal(bad_entry, dates[0], cost_pct=COST_PCT) is None
    _, skipped = fv.measure_all([fv.Signal(dates[0], "BADENTRY")], {"BADENTRY": bad_entry},
                                cost_pct=COST_PCT)
    assert skipped["bad_entry_bar"] == 1                  # a zero open is a bar defect, not an age
    bad_exit = fv.Series("BADEXIT", dates, [100.0] * 21,
                         [100.0] * 19 + [float("nan"), 100.0])
    m = fv.measure_signal(bad_exit, dates[0], cost_pct=COST_PCT)
    assert m is not None and 20 not in m.net              # that cell drops; T+10 survives
    assert 10 in m.net


# ============================================================ 6. the journal read
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
    """Every published hi52 row in the window, whatever the analyst budget then did with it — and
    nothing from before the promotion (that is the SHADOW population, a different rule) and nothing
    from another strategy."""
    path = _state_db(tmp_path, [
        ("2026-09-11", "OLDSHADOW", "hi52"),              # pre-promotion: a different rule
        ("2026-09-14", "BHEL", "hi52"),
        ("2026-09-14", "ABB", "hi52"),
        ("2026-09-14", "TCS", "brk20"),                   # another strategy
        ("2026-09-15", "BHEL", "hi52"),
        ("2026-12-01", "LATER", "hi52"),                  # beyond --as-of
        ("not-a-date", "CORRUPT", "hi52"),                # a corrupt row is skipped, never a crash
    ])
    conn = fv.open_state_db(path)
    try:
        rows = fv.load_signal_rows(conn, start=fv.PROMOTION_DATE, as_of=date(2026, 9, 30))
    finally:
        conn.close()
    assert rows == [
        fv.Signal(date(2026, 9, 14), "ABB"),
        fv.Signal(date(2026, 9, 14), "BHEL"),
        fv.Signal(date(2026, 9, 15), "BHEL"),
    ]


def test_a_store_that_cannot_be_read_is_not_a_verdict(tmp_path, capsys):
    """A missing store reports UNAVAILABLE and still exits 0 — an INSUFFICIENT there would put a
    verdict on evidence nobody looked at."""
    rc = fv.main(["--state-db", str(tmp_path / "absent.db"), "--db", str(tmp_path / "absent.duckdb")])
    out = capsys.readouterr().out
    assert rc == 0
    assert fv.VERDICT_UNAVAILABLE in out
    assert fv.VERDICT_INSUFFICIENT not in out


# ============================================================ 7. the ONE cost derivation
def test_the_sized_notional_is_the_7_1_cap_and_nothing_chosen_by_hand():
    """§7.1 caps a swing position at `swing_position_pct`% of equity charged at `overnight_gap_mult`
    × the stop distance, so the notional is `equity × 2% / (2.5 × 6%)` = equity/7.5 — the price
    cancels out of `qty_max × price`. Three readings of the same formula, at the three equities the
    promotion's paperwork quotes."""
    at = lambda eq: fv.sizing_notional(  # noqa: E731
        Decimal(eq), position_pct=Decimal("2.0"), gap_mult=Decimal("2.5"), stop_pct=Decimal("6.0"))
    assert at("40000") == pytest.approx(Decimal("5333.33"), abs=Decimal("0.01"))
    assert at("39256.65") == pytest.approx(Decimal("5234.22"), abs=Decimal("0.01"))
    assert at("36000") == Decimal("4800")                 # the equity_floor_rung, the worst book
    # A wider stop SHRINKS the notional and RAISES the floor — the half of the derivation that a
    # stop change must move, which is why STOP_PCT is read from the scanner and never re-typed.
    assert at("36000") > fv.sizing_notional(
        Decimal("36000"), position_pct=Decimal("2.0"), gap_mult=Decimal("2.5"),
        stop_pct=Decimal("12.0"))
    assert fv.STOP_PCT == Decimal("6.0")
    for bad in ({"equity": Decimal("0")}, {"stop_pct": Decimal("0")}, {"gap_mult": Decimal("-1")}):
        kwargs = {"equity": Decimal("36000"), "position_pct": Decimal("2.0"),
                  "gap_mult": Decimal("2.5"), "stop_pct": Decimal("6.0"), **bad}
        with pytest.raises(ValueError):                   # a tiny notional is a spurious DEMOTE
            fv.sizing_notional(kwargs.pop("equity"), **kwargs)


def test_the_registered_edge_is_this_script_s_derivation():
    """The registered edge and the kill criterion's cost floor must come from ONE derivation or they
    drift apart silently — a stale `expected_edge_pct` is an edge the arithmetic no longer supports,
    read on every candidate for months. So `settings.yaml` carries only the OUTPUT of
    `registered_edge_pct()`, and this test is the pin: a change to limits.yaml, to hi52's stop or to
    the cost tables fails HERE instead of leaving the gate multiplying against a number nobody
    re-derived."""
    from engine.core.config import load_settings

    derived = fv.registered_edge_pct()
    assert derived == Decimal("1.47")
    assert Decimal(str(load_settings().hi52.expected_edge_pct)) == derived
    # …and it IS gross-minus-floor, rounded DOWN: rounding an edge UP is the one direction that can
    # buy a candidate a C3 pass it did not earn.
    floor = fv.cost_floor_pct(Decimal("4800"))
    assert floor == pytest.approx(Decimal("0.561875"), abs=Decimal("1e-6"))
    assert fv.MEASURED_T20_MEDIAN_GROSS_PCT - floor > derived          # 1.4717 -> 1.47
    assert fv.MEASURED_T20_MEDIAN_GROSS_PCT - floor - derived < Decimal("0.01")


def test_the_registration_equity_is_the_go_flat_rung_and_falls_back_loudly(tmp_path):
    """Derived at the WORST equity the risk table still lets a position be opened at (the
    `equity_floor_rung`), because the cost floor RISES as the book shrinks: deriving at the largest
    permitted book would register the flattering edge for exactly the drawdown it fails in."""
    eq, src = fv.load_registration_equity(_LIMITS)
    assert eq == Decimal("36000.0") and "equity_floor_rung" in src
    # An unreadable or implausible table falls back to the pinned rung and SAYS so — never to a
    # bigger book, which would quietly lower the floor and raise the registered edge.
    missing = tmp_path / "nope.yaml"
    eq2, src2 = fv.load_registration_equity(missing)
    assert eq2 == Decimal("36000.0") and "pinned default" in src2
    (tmp_path / "bad.yaml").write_text(
        "capital_base_inr: 40000\nlimits:\n  equity_floor_rung:\n    equity_pct_of_base: 0.0\n",
        encoding="utf-8")
    eq3, src3 = fv.load_registration_equity(tmp_path / "bad.yaml")
    assert eq3 == Decimal("36000.0") and "pinned default" in src3


def test_the_notional_override_is_shouted_not_whispered(tmp_path):
    """`--notional` exists for what-ifs and is exactly the defect the derivation removed, so a report
    that used it must say OVERRIDE in the numbers a reader quotes."""
    cost, sizing = fv.resolve_cost_floor(
        limits_path=_LIMITS, equity_override=None, min_equity=None,
        notional_override=Decimal("8000"))
    assert cost == pytest.approx(0.4343, abs=1e-3)        # the ₹8,000 floor the review refuted
    assert sizing["override"] is True and sizing["derivation"].startswith("OVERRIDE")
    doc = _report(*_population(20, t20_close=103.0))
    doc["meta"]["sizing"] = sizing
    doc["meta"]["reference_notional_inr"] = sizing["notional_inr"]
    assert "[OVERRIDE]" in fv.render(doc)
    # …and the derived path says nothing of the sort, and names the equity it used.
    cost2, sizing2 = fv.resolve_cost_floor(
        limits_path=_LIMITS, equity_override=Decimal("36000"), min_equity=None)
    assert sizing2["override"] is False and Decimal(sizing2["notional_inr"]) == Decimal("4800")
    assert cost2 > cost                                   # the real floor is HIGHER than ₹8,000's
    assert "[OVERRIDE]" not in fv.render(_report(*_population(20, t20_close=103.0)))


def test_t5_and_t10_are_printed_as_diagnostics_and_never_vote():
    """T+5 is printed because the manager asked to see it; the v2 study measured it DEAD, so the
    promoted rule makes no claim there. A cell nobody pre-registered must be unquotable as an
    outcome — it is labelled on its own row and the verdict reads T+20 alone."""
    assert fv.HORIZONS == (5, 10, 20) and fv.VERDICT_HORIZON == 20
    assert fv.DIAGNOSTIC_HORIZONS == (5, 10)
    # A population that is TERRIBLE at T+5/T+10 and fine at T+20 still HOLDs: only T+20 votes.
    signals = [fv.Signal(START, f"SYM{i:02d}") for i in range(20)]
    series = {s.symbol: _series(s.symbol, t20_close=103.0, t10_close=80.0, t5_close=80.0)
              for s in signals}
    doc = _report(signals, series)
    assert doc["horizons"]["5"]["median_net"] < 0 and doc["horizons"]["10"]["median_net"] < 0
    assert doc["verdict"] == fv.VERDICT_HOLD
    text = fv.render(doc)
    for line in text.splitlines():
        if line.strip().startswith(("T+5", "T+10")):
            assert "diagnostic only" in line
        if line.strip().startswith("T+20"):
            assert "diagnostic" not in line


def test_the_script_is_read_only_by_construction():
    """No INSERT/UPDATE/DELETE/CREATE anywhere, and the DuckDB attach is read_only — the engine's
    stores are never written by a report."""
    src = _SCRIPT.read_text(encoding="utf-8")
    assert "read_only=True" in src and "mode=ro" in src
    # Case-SENSITIVE: SQL is written uppercase throughout this repo, and the prose above talks about
    # deleting a settings key — a case-folded scan would flag that sentence instead of a statement.
    for stmt in ("INSERT INTO", "UPDATE ", "DELETE FROM", "CREATE TABLE", "DROP "):
        assert stmt not in src, stmt
