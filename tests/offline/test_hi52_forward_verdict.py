"""scripts/hi52_forward_verdict.py — the mechanical kill criterion for the hi52 promotion.

Pins the pre-registered rule: DEMOTE, HOLD and INSUFFICIENT cases and the boundaries it is written on
(``n >= 20``, ``median <= 0``, ``hit < 50%``). Session arithmetic, the journal read and the sizing
notional are production code pinned in ``tests/unit/test_hi52_forward.py``.

Costs come from the repo's CostModel at the script's reference notional, not a frozen constant; the
registered edge is pinned to the same derivation in
``tests/unit/test_hi52_edge.py::test_the_registered_edge_is_this_script_s_derivation``.
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

from engine.core.config import config_dir  # noqa: E402
from engine.strategy.cost_model import CostModel  # noqa: E402

COST_PCT = float(CostModel.from_config().breakeven_pct(fv.REFERENCE_NOTIONAL, fv.PRODUCT))
#: The risk-limits table from the pinned test config (conftest: registration capital, the owner's dashboard capital change).
_LIMITS = config_dir() / "limits.yaml"
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
    """A 21-session series whose signal day is index 0: entry is ``open[0]`` (the journal day's own open)
    and T+k is ``close[k-1]`` (the entry session counts as the first), so T+5 is ``close[4]``, T+10
    ``close[9]``, T+20 ``close[19]``; index 20 is a spare and every other bar is inert (100.00)."""
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
    """20 signals flat gross lose one round trip: the median net is -cost, nothing hits, both clauses fire."""
    doc = _report(*_population(20, t20_close=100.0))
    cell = doc["horizons"]["20"]
    assert cell["n"] == 20
    assert cell["median_net"] == pytest.approx(-COST_PCT, abs=1e-4)
    assert cell["hit_rate"] == 0.0
    assert doc["verdict"] == fv.VERDICT_DEMOTE
    assert "median net" in doc["reason"] and "hit rate" in doc["reason"]
    # The report names both edits demotion requires.
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
    """Below the registered n the rule says INSUFFICIENT, never HOLD ("not yet refuted" is not "no evidence")."""
    doc = _report(*_population(19, t20_close=90.0))
    assert doc["horizons"]["20"]["n"] == 19
    assert doc["horizons"]["20"]["median_net"] < 0        # it looks terrible…
    assert doc["verdict"] == fv.VERDICT_INSUFFICIENT      # …and that is not a verdict yet
    assert "19 measured" in doc["reason"]
    assert "VALIDATION, not income" in fv.render(doc)


# ============================================================ 4. the rule's own boundaries
def test_the_verdict_rule_is_exactly_as_pre_registered():
    """The three comparisons at their boundaries, on the pure function."""
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
    """10 flat (a miss) and 10 at +3%: hit rate is exactly 50% and the median positive, so the rule HOLDs."""
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
def test_horizons_are_measured_on_their_own_event_sets():
    """A signal old enough for T+10 but not T+20 counts at T+10 only; the cells do not share a population."""
    young = fv.Series(symbol="YOUNG", dates=_sessions(12),
                      open=[100.0] * 12, close=[100.0] * 9 + [105.0, 100.0, 100.0])
    m = fv.measure_signal(young, young.dates[0], cost_pct=COST_PCT)
    cells = fv.summarize([m])
    assert cells["10"]["n"] == 1 and cells["20"]["n"] == 0
    assert fv.verdict(cells)[0] == fv.VERDICT_INSUFFICIENT


def test_unmeasurable_signals_are_counted_by_REASON_never_silently_dropped():
    """An ``n`` shrunk by a not-yet-landed bhavcopy must not read like a store hole: they are told apart by
    whether ``d`` is past the last stored session or missing from inside the stored range."""
    good = _series("GOOD", t20_close=103.0)
    signals = [
        fv.Signal(START, "GOOD"),
        fv.Signal(START, "NOBARS"),                       # no series at all
        fv.Signal(date(2026, 9, 19), "GOOD"),             # a Saturday INSIDE the range: a hole
        fv.Signal(good.dates[-1] + timedelta(days=3), "GOOD"),   # today: no bhavcopy row YET
    ]
    _, skipped = fv.measure_all(signals, {"GOOD": good}, cost_pct=COST_PCT)
    doc = _report(signals, {"GOOD": good})
    assert doc["population"] == {
        "signals_journalled": 4, "signals_measured": 1, "skipped": skipped,
        "symbols": ["GOOD", "NOBARS"],
    }


def test_a_signal_on_the_last_stored_session_is_entered_but_reaches_no_horizon():
    """Entry is ``open(d)``, so a signal on the last stored session is a measured trade with an empty cell
    set, not a skip; it counts in neither the verdict cell nor the skip tally."""
    good = _series("GOOD", t20_close=103.0)
    trades, _ = fv.measure_all([fv.Signal(good.dates[-1], "GOOD")], {"GOOD": good},
                               cost_pct=COST_PCT)
    assert fv.summarize(trades)["20"]["n"] == 0


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
    """The journal is read through the script's read-only connection; row selection is pinned in ``tests/unit/test_hi52_forward.py``."""
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
    assert len(rows) == 3


def test_a_store_that_cannot_be_read_is_not_a_verdict(tmp_path, capsys):
    """A missing store reports UNAVAILABLE and exits 0; INSUFFICIENT would put a verdict on unread evidence."""
    rc = fv.main(["--state-db", str(tmp_path / "absent.db"), "--db", str(tmp_path / "absent.duckdb")])
    out = capsys.readouterr().out
    assert rc == 0
    assert fv.VERDICT_UNAVAILABLE in out
    assert fv.VERDICT_INSUFFICIENT not in out


# ============================================================ 7. the ONE cost derivation
def test_the_registration_equity_is_the_go_flat_rung_and_falls_back_loudly(tmp_path):
    """Derived at the worst equity the risk table still opens positions at (the `equity_floor_rung`),
    because the cost floor rises as the book shrinks; a larger book would register a flattering edge."""
    eq, src = fv.load_registration_equity(_LIMITS)
    assert eq == Decimal("36000.0") and "equity_floor_rung" in src
    # An unreadable or implausible table falls back to the pinned rung and says so, never to a bigger book.
    missing = tmp_path / "nope.yaml"
    eq2, src2 = fv.load_registration_equity(missing)
    assert eq2 == Decimal("36000.0") and "pinned default" in src2
    (tmp_path / "bad.yaml").write_text(
        "capital_base_inr: 40000\nlimits:\n  equity_floor_rung:\n    equity_pct_of_base: 0.0\n",
        encoding="utf-8")
    eq3, src3 = fv.load_registration_equity(tmp_path / "bad.yaml")
    assert eq3 == Decimal("36000.0") and "pinned default" in src3


def test_the_notional_override_is_shouted_not_whispered(tmp_path):
    """`--notional` is for what-ifs; a report that used it must say OVERRIDE."""
    cost, sizing = fv.resolve_cost_floor(
        limits_path=_LIMITS, equity_override=None, min_equity=None,
        notional_override=Decimal("8000"))
    assert cost == pytest.approx(float(fv.cost_floor_pct(Decimal("8000"))))
    assert sizing["override"] is True and sizing["derivation"].startswith("OVERRIDE")
    doc = _report(*_population(20, t20_close=103.0))
    doc["meta"]["sizing"] = sizing
    doc["meta"]["reference_notional_inr"] = sizing["notional_inr"]
    assert "[OVERRIDE]" in fv.render(doc)
    # The derived path carries no OVERRIDE tag.
    cost2, sizing2 = fv.resolve_cost_floor(
        limits_path=_LIMITS, equity_override=Decimal("36000"), min_equity=None)
    # Rs 36,000 x 2% / (2.0 x 6%) = Rs 6,000
    assert sizing2["override"] is False and Decimal(sizing2["notional_inr"]) == Decimal("6000")
    # Another rule's stop (cat's 5%, scripts/cat_shadow_verdict.py) moves the notional: Rs 7,200.
    assert Decimal(fv.resolve_cost_floor(
        limits_path=_LIMITS, equity_override=Decimal("36000"), min_equity=None,
        stop_pct=Decimal("5.0"))[1]["notional_inr"]) == Decimal("7200")
    assert cost2 > cost                                   # a smaller notional costs more
    assert "[OVERRIDE]" not in fv.render(_report(*_population(20, t20_close=103.0)))


def test_t5_and_t10_are_printed_as_diagnostics_and_never_vote():
    """T+5 and T+10 are labelled diagnostic-only; the verdict reads T+20 alone."""
    assert fv.HORIZONS == (5, 10, 20) and fv.VERDICT_HORIZON == 20
    assert fv.DIAGNOSTIC_HORIZONS == (5, 10)
    # Terrible at T+5/T+10 and fine at T+20 still HOLDs.
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
    """No write statements, and the DuckDB attach is read_only."""
    src = _SCRIPT.read_text(encoding="utf-8")
    assert "read_only=True" in src and "mode=ro" in src
    # Case-sensitive: SQL is uppercase here, and prose mentions deleting a settings key.
    for stmt in ("INSERT INTO", "UPDATE ", "DELETE FROM", "CREATE TABLE", "DROP "):
        assert stmt not in src, stmt
