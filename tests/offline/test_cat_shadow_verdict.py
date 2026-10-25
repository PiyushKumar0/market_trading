"""scripts/cat_shadow_verdict.py — the pre-registered retire-or-review rule for the ``cat`` v2 shadow.

The rule's boundaries are tested on the pure functions (:func:`verdict`, :func:`fixed_cohort`,
:func:`margin_threshold_pct`, :func:`segment_of`); the seam — a migrated ``state.db`` and a real
``MarketStore`` DuckDB file read through the script's own ``main`` — is tested once end to end,
including the ``--as-of`` bound. Entry/exit arithmetic is ``engine.learning.hi52_forward``'s and is
pinned in ``tests/unit/test_hi52_forward.py``.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "cat_shadow_verdict.py"
_spec = importlib.util.spec_from_file_location("mt_cat_shadow_verdict", _SCRIPT)
cv = importlib.util.module_from_spec(_spec)
sys.modules["mt_cat_shadow_verdict"] = cv
_spec.loader.exec_module(cv)

from engine.core.calendar import NSECalendar  # noqa: E402
from engine.core.clock import IST, Clock  # noqa: E402
from engine.core.config import config_dir  # noqa: E402
from engine.core.db import connect  # noqa: E402
from engine.core.migrations import apply_migrations  # noqa: E402
from engine.learning.hi52_forward import Measured, Series, Signal, load_signal_rows  # noqa: E402
from engine.learning.validate import MARGIN_FLOOR_DAYS  # noqa: E402
from engine.marketdata.store import DailyBar, MarketStore  # noqa: E402
from engine.strategy.hi52_edge import cost_floor_pct  # noqa: E402

_LIMITS = config_dir() / "limits.yaml"
COST = 0.5                                   # pure-function tests: a round, readable floor
FIRST = date(2026, 8, 31)                    # first session of the restarted clock (08-29 is a Sat)


@pytest.fixture(scope="module")
def cal() -> NSECalendar:
    return NSECalendar(config_dir() / "calendar", Clock(), include_special=False)


def _sessions(cal: NSECalendar, n: int, start: date = FIRST) -> list[date]:
    out, d = [], start
    while len(out) < n:
        if cal.is_trading_day(d):
            out.append(d)
        d += timedelta(days=1)
    return out


def _series(cal: NSECalendar, symbol: str, *, start: date, n: int = 25, t10: float = 100.0,
            t20: float = 100.0) -> Series:
    """``n`` calendar sessions from ``start`` (index 0 = the signal day): open 100 everywhere,
    T+10 = close[9], T+20 = close[19], every other close 100."""
    dates = _sessions(cal, n, start)
    closes = [100.0] * n
    if n > 9:
        closes[9] = t10
    if n > 19:
        closes[19] = t20
    return Series(symbol, dates, [100.0] * n, closes)


def _member(i: int, net10: float | None, net20: float | None, status: str = cv.MEASURED,
            d: date = FIRST) -> cv.Assessed:
    net = {k: v for k, v in ((10, net10), (20, net20)) if v is not None}
    trade = Measured(f"S{i:02d}", d, d, 100.0, gross=dict(net), net=dict(net))
    return cv.Assessed(Signal(d, f"S{i:02d}"), status, "" if status == cv.MEASURED else status,
                       trade, {})


def _decide(cohort, *, sessions: int = 40, admitted: int | None = None) -> tuple[str, str]:
    return cv.verdict(sessions=sessions, admitted=len(cohort) if admitted is None else admitted,
                      cohort=cohort, threshold_10=cv.margin_threshold_pct(COST, 10),
                      threshold_20=cv.margin_threshold_pct(COST, 20))


# ============================================================ 1. the WO-3 thresholds
def test_the_margin_threshold_is_one_round_trip_per_twenty_sessions_of_hold():
    """WO-3: net >= cost / 20 per day, so T+20 needs one full round trip and T+10 half of one."""
    assert MARGIN_FLOOR_DAYS == 20                       # the pre-registered denominator
    assert cv.HORIZONS == (10, 20)                       # no T+5 cell: nothing else is registered
    assert cv.margin_threshold_pct(0.5, 20) == pytest.approx(0.5)
    assert cv.margin_threshold_pct(0.5, 10) == pytest.approx(0.25)
    assert cv.margin_threshold_pct(0.4769, 10) == pytest.approx(0.23845)


# ============================================================ 2. the fixed cohort
def test_the_cohort_is_the_first_twenty_by_day_then_symbol():
    late = [Signal(date(2026, 9, 30), f"Z{i}") for i in range(5)]
    early = [Signal(date(2026, 9, 1), f"S{i:02d}") for i in range(19, -1, -1)]   # reverse order
    cohort = cv.fixed_cohort(late + early)
    assert [s.symbol for s in cohort] == [f"S{i:02d}" for i in range(20)]
    # Same day: the symbol orders it, so a 21st signal on the 20th's day but sorting after it is out.
    tie = early + [Signal(date(2026, 9, 1), "ZZZ")]
    assert Signal(date(2026, 9, 1), "ZZZ") not in cv.fixed_cohort(tie)


def test_a_late_maturing_early_signal_holds_the_verdict_and_is_never_replaced(cal):
    """Signal #1 has a bar missing mid-hold, so its T+20 lands a session after everyone else's.
    The 21st..25th signals are fully measured and STILL never enter: the verdict waits for #1."""
    d0 = FIRST
    signals = [Signal(d0, f"S{i:02d}") for i in range(20)]
    later = [Signal(_sessions(cal, 2)[1], f"T{i:02d}") for i in range(5)]
    series = {s.symbol: _series(cal, s.symbol, start=s.d, n=30, t10=110.0, t20=110.0)
              for s in signals[1:] + later}
    full = _series(cal, "S00", start=d0, n=30, t10=110.0, t20=110.0)
    # The session at index 5 is missing (a halt): S00's 20th held session is full.dates[20].
    halted = Series("S00", full.dates[:5] + full.dates[6:], full.open[:5] + full.open[6:],
                    full.close[:5] + full.close[6:])
    series["S00"] = halted
    as_of = full.dates[19]                    # everyone else's T+20 session (later: full.dates[20])
    doc = cv.build_report(signals + later, series, cost_pct=COST, as_of=as_of, start=d0,
                          calendar=cal)
    members = doc["cohort"]["members"]
    assert [m["symbol"] for m in members] == [f"S{i:02d}" for i in range(20)]
    assert members[0]["status"] != cv.MEASURED and "20" not in members[0]["net"]
    assert all(m["status"] == cv.MEASURED for m in members[1:])
    assert doc["verdict"] == cv.VERDICT_INSUFFICIENT
    assert "cohort members" in doc["reason"]
    assert doc["population"]["signals_admitted"] == 25
    # One session later S00's own T+20 bar has landed: the SAME 20 form the verdict, and the five
    # later signals (measured all along) still never entered.
    doc = cv.build_report(signals + later, series, cost_pct=COST, as_of=full.dates[20], start=d0,
                          calendar=cal)
    assert [m["symbol"] for m in doc["cohort"]["members"]] == [f"S{i:02d}" for i in range(20)]
    assert doc["cohort"]["measured"] == 20
    assert doc["verdict"] == cv.VERDICT_INSUFFICIENT and "sessions, rule needs 30" in doc["reason"]
    doc = cv.build_report(signals + later, series, cost_pct=COST, as_of=full.dates[29], start=d0,
                          calendar=cal)
    assert doc["verdict"] == cv.VERDICT_OWNER_REVIEW
    assert [m["symbol"] for m in doc["cohort"]["members"]] == [f"S{i:02d}" for i in range(20)]


def test_a_pending_cohort_member_is_listed_and_holds_the_verdict(cal):
    sessions = _sessions(cal, 45)
    signals = [Signal(sessions[0], f"S{i:02d}") for i in range(19)] + [Signal(sessions[30], "LAST")]
    series = {s.symbol: _series(cal, s.symbol, start=s.d, n=40 if s.symbol != "LAST" else 10,
                                t10=110.0, t20=110.0) for s in signals}
    doc = cv.build_report(signals, series, cost_pct=COST, as_of=sessions[39], start=FIRST,
                          calendar=cal)
    assert doc["verdict"] == cv.VERDICT_INSUFFICIENT
    assert doc["cohort"]["pending"] and doc["cohort"]["pending"][0].endswith(
        f"due {cal.add_sessions(sessions[30], 19)}")
    assert doc["cohort"]["holes"] == []


# ============================================================ 3. RETIRE / OWNER REVIEW
def test_retire_when_both_medians_are_at_or_below_the_threshold_equality_retires():
    thr10, thr20 = cv.margin_threshold_pct(COST, 10), cv.margin_threshold_pct(COST, 20)
    assert _decide([_member(i, thr10, thr20) for i in range(20)])[0] == cv.VERDICT_RETIRE
    assert _decide([_member(i, -1.0, -2.0) for i in range(20)])[0] == cv.VERDICT_RETIRE
    # The MEDIAN decides, not the mean: 11 losers and 9 big winners is still a retire.
    cohort = [_member(i, -1.0, -1.0) for i in range(11)] + [_member(i, 9.0, 9.0) for i in range(11, 20)]
    assert _decide(cohort)[0] == cv.VERDICT_RETIRE


def test_owner_review_when_either_horizon_clears():
    thr10, thr20 = cv.margin_threshold_pct(COST, 10), cv.margin_threshold_pct(COST, 20)
    eps = 1e-6
    v, reason = _decide([_member(i, thr10 + eps, thr20) for i in range(20)])
    assert v == cv.VERDICT_OWNER_REVIEW and "clears at T+10" in reason
    v, reason = _decide([_member(i, thr10, thr20 + eps) for i in range(20)])
    assert v == cv.VERDICT_OWNER_REVIEW and "clears at T+20" in reason
    v, reason = _decide([_member(i, 3.0, 3.0) for i in range(20)])
    assert v == cv.VERDICT_OWNER_REVIEW and "T+10 and T+20" in reason


# ============================================================ 4. INSUFFICIENT, per binding condition
def test_insufficient_names_each_binding_condition():
    good = [_member(i, 3.0, 3.0) for i in range(20)]
    v, reason = _decide(good, sessions=29)
    assert v == cv.VERDICT_INSUFFICIENT and "29 sessions, rule needs 30" in reason
    v, reason = _decide(good[:19])
    assert v == cv.VERDICT_INSUFFICIENT and "19 admitted signals, rule needs 20" in reason
    pending = good[:19] + [_member(19, 3.0, None, status=cv.PENDING)]
    v, reason = _decide(pending)
    assert v == cv.VERDICT_INSUFFICIENT and "1 of 20 cohort members not yet measured" in reason
    hole = good[:19] + [cv.Assessed(Signal(FIRST, "HOLE"), cv.HOLE, "no_bars", None, {})]
    v, reason = _decide(hole)
    assert v == cv.VERDICT_INSUFFICIENT and "store hole" in reason
    # Admitted >= 20 counts every journalled row, however many the cohort has measured.
    assert _decide(good, admitted=25)[0] == cv.VERDICT_OWNER_REVIEW


def test_a_store_hole_in_the_cohort_holds_the_verdict_and_is_listed(cal):
    """"Fix the data, never skip the row": a symbol with no bars is a hole, and the 21st signal does
    not take its place."""
    sessions = _sessions(cal, 40)
    signals = [Signal(sessions[0], f"S{i:02d}") for i in range(21)]
    series = {s.symbol: _series(cal, s.symbol, start=s.d, n=40, t10=90.0, t20=90.0)
              for s in signals if s.symbol != "S03"}
    doc = cv.build_report(signals, series, cost_pct=COST, as_of=sessions[-1], start=FIRST,
                          calendar=cal)
    assert doc["verdict"] == cv.VERDICT_INSUFFICIENT
    assert doc["cohort"]["holes"] == [f"{sessions[0]} S03: no_bars: no bars_1d rows for this "
                                      "symbol in the window (delisted?)"]
    assert "S03" in cv.render(doc) and "HOLES" in cv.render(doc)


def test_a_full_measured_cohort_retires_end_to_end_on_pure_inputs(cal):
    sessions = _sessions(cal, 40)
    signals = [Signal(sessions[0], f"S{i:02d}") for i in range(20)]
    series = {s.symbol: _series(cal, s.symbol, start=s.d, n=40) for s in signals}   # flat
    doc = cv.build_report(signals, series, cost_pct=COST, as_of=sessions[-1], start=FIRST,
                          calendar=cal)
    assert doc["population"]["sessions"] >= 30
    assert doc["verdict"] == cv.VERDICT_RETIRE                 # net = -cost at both horizons
    text = cv.render(doc)
    assert ("=> the pre-registered rule retires cat origination; this report changes nothing "
            "(retiring is an owner-visible change citing it)") in text
    rule = next(line for line in text.splitlines() if line.startswith("RULE"))
    assert "20 non-void, non-duplicate" in rule
    for line in text.splitlines():
        if line.startswith(("running", "pre-O15", "O15 +", "Sonnet-5.5", "excess vs EW")):
            assert "decides nothing" in line


# ============================================================ 5. the clock-restart exclusion
def test_signals_admitted_08_18_through_08_28_are_excluded_whatever_from_says(cal):
    rows = [Signal(date(2026, 8, 17), "BEFORE"), Signal(date(2026, 8, 18), "X1"),
            Signal(date(2026, 8, 28), "X2"), Signal(date(2026, 8, 31), "KEPT")]
    kept, n_excluded = cv.apply_exclusion(rows)
    assert [s.symbol for s in kept] == ["BEFORE", "KEPT"] and n_excluded == 2
    doc = cv.build_report(rows, {}, cost_pct=COST, as_of=date(2026, 9, 30),
                          start=date(2026, 8, 18), calendar=cal)
    assert doc["population"]["signals_excluded_clock_restart"] == 2
    assert [m["symbol"] for m in doc["cohort"]["members"]] == ["KEPT"]
    assert doc["meta"]["registered_window"] is False
    assert "NON-REGISTERED WINDOW" in cv.render(doc)


# ============================================================ 6. segments
def test_segment_assignment_at_both_boundaries():
    s = date(2026, 10, 6)
    assert cv.segment_of(date(2026, 9, 6), scorer_switch=s) == "pre_o15"
    assert cv.segment_of(date(2026, 9, 7), scorer_switch=s) == "o15_sonnet5"
    assert cv.segment_of(date(2026, 10, 5), scorer_switch=s) == "o15_sonnet5"
    assert cv.segment_of(s, scorer_switch=s) == "sonnet55"
    assert cv.O15_FIRST_SESSION == date(2026, 9, 7) and "WORKLOG 2026-09-04" in cv.O15_PROVENANCE


def _clusters(rows: list[tuple[str, datetime | None]]):
    con = duckdb.connect()
    con.execute("CREATE TABLE news_clusters (cluster_id TEXT, scored_at TIMESTAMPTZ, "
                "scorer_model TEXT)")
    for i, (model, ts) in enumerate(rows):
        con.execute("INSERT INTO news_clusters VALUES (?, ?, ?)", [f"c{i}", ts, model])
    return lambda sql, params: con.execute(sql, params).fetchall()


def test_the_scorer_switch_is_the_session_after_the_first_sonnet_5_5_score_in_IST(cal):
    """20:00 UTC on Sun 10-04 is 01:30 IST on Mon 10-05: the IST date is 10-05, so S is Tue 10-06.
    A UTC reading would say 10-04 and put S on 10-05 — a session early."""
    fetch = _clusters([
        ("sonnet-5", datetime(2026, 9, 20, 4, 0, tzinfo=UTC)),
        ("sonnet-5.5", datetime(2026, 10, 4, 20, 0, tzinfo=UTC)),
        ("sonnet-5.5", datetime(2026, 10, 9, 4, 0, tzinfo=UTC)),
    ])
    s, src = cv.scorer_switch_session(fetch, cal, as_of=date(2026, 10, 26))
    assert s == date(2026, 10, 6) and "FALLBACK" not in src and "2026-10-05" in src
    # --as-of bounds the lookup: before the first sonnet-5.5 row there is no switch to see.
    s, src = cv.scorer_switch_session(fetch, cal, as_of=date(2026, 10, 4))
    assert s == cv.SCORER_SWITCH_FALLBACK and src.startswith("PINNED FALLBACK (no news_clusters")


def test_an_unreadable_scorer_column_uses_the_pinned_fallback_and_says_so(cal):
    con = duckdb.connect()
    con.execute("CREATE TABLE news_clusters (cluster_id TEXT)")
    s, src = cv.scorer_switch_session(lambda q, p: con.execute(q, p).fetchall(), cal,
                                      as_of=date(2026, 10, 26))
    assert s == date(2026, 10, 5) and src.startswith("PINNED FALLBACK (BinderException")


# ============================================================ 7. EW excess
def test_the_ew_excess_cell_omits_a_window_with_a_missing_day_and_counts_it(cal):
    sessions = _sessions(cal, 25)
    a = _series(cal, "A", start=sessions[0], n=21, t10=102.0, t20=104.0)
    b = _series(cal, "B", start=sessions[1], n=20, t10=102.0, t20=104.0)
    daily = {d: 0.001 for d in sessions}
    del daily[sessions[20]]                     # inside B's T+20 window only (A exits at [19])
    doc = cv.build_report([Signal(sessions[0], "A"), Signal(sessions[1], "B")], {"A": a, "B": b},
                          cost_pct=COST, as_of=sessions[24], start=FIRST, calendar=cal, daily=daily)
    ex = doc["diagnostics"]["excess_vs_ew"]
    assert (ex["20"]["n"], ex["20"]["omitted_missing_ew_day"]) == (1, 1)
    assert (ex["10"]["n"], ex["10"]["omitted_missing_ew_day"]) == (2, 0)
    # A: net 4 - 0.5; benchmark compounds 19 sessions (fill+1 .. exit) at 0.1%.
    assert ex["20"]["median_excess"] == pytest.approx(3.5 - (1.001 ** 19 - 1) * 100, abs=1e-4)


# ============================================================ 7b. VOID: an unadjusted corp action in the hold
def _void_population(cal, *, n_signals: int = 21):
    sessions = _sessions(cal, 40)
    signals = [Signal(sessions[0], f"S{i:02d}") for i in range(n_signals)]
    series = {s.symbol: _series(cal, s.symbol, start=s.d, n=40, t10=103.0, t20=103.0)
              for s in signals}
    # S03 "crashes" 50% at its T+20 — the unadjusted bars of a 1:1 bonus, not a loss.
    series["S03"] = _series(cal, "S03", start=sessions[0], n=40, t10=50.0, t20=50.0)
    return sessions, signals, series


def test_an_unadjusted_ex_date_inside_the_hold_voids_and_the_21st_signal_enters_the_cohort(cal):
    """exit_sim's rule: an UNADJUSTED_KINDS ex-date x with d < x <= the last measured session voids
    the observation (stored bars are never re-adjusted, so the window holds two units)."""
    sessions, signals, series = _void_population(cal)
    doc = cv.build_report(signals, series, cost_pct=COST, as_of=sessions[-1], start=FIRST,
                          calendar=cal, ex_dates={"S03": [(sessions[5], "bonus")]})
    members = [m["symbol"] for m in doc["cohort"]["members"]]
    assert "S03" not in members and members[-1] == "S20" and len(members) == 20
    assert doc["population"]["status"][cv.VOID] == 1
    assert doc["population"]["signals_admitted"] == 21
    assert doc["voids"] == [f"{sessions[0]} S03: void_ca: bonus ex-date {sessions[5]}"]
    assert doc["verdict"] == cv.VERDICT_OWNER_REVIEW
    # Excluded from every cell: all 20 remaining nets are +3% - cost, so the void's -50% is absent.
    run = doc["diagnostics"]["running"]
    assert run["20"]["n"] == 20 and run["20"]["mean_net"] == pytest.approx(3.0 - COST, abs=1e-4)
    assert doc["diagnostics"]["segments"]["pre_o15"]["signals"] == 20
    assert doc["diagnostics"]["excess_vs_ew"]["20"]["omitted_missing_ew_day"] == 20   # no EW given
    text = cv.render(doc)
    assert "VOID" in text and f"bonus ex-date {sessions[5]}" in text and "void 1" in text


def test_an_ex_date_on_d_after_the_exit_or_of_another_kind_does_not_void(cal):
    sessions, signals, series = _void_population(cal)
    ex = {"S03": [(sessions[0], "split"),         # on d: open(d) is already in the new unit
                  (sessions[20], "bonus"),        # after the T+20 exit session (sessions[19])
                  (sessions[5], "dividend")]}     # not a structural kind
    doc = cv.build_report(signals, series, cost_pct=COST, as_of=sessions[-1], start=FIRST,
                          calendar=cal, ex_dates=ex)
    assert doc["population"]["status"][cv.VOID] == 0
    assert "S03" in [m["symbol"] for m in doc["cohort"]["members"]]


def test_an_unmatured_signal_voids_on_an_ex_date_up_to_its_last_stored_session(cal):
    """Before T+20 the window is measured through the symbol's last stored session; the ex-date at
    the boundary voids, one session past it is not read as inside."""
    sessions = _sessions(cal, 40)
    young = Series("Y", sessions[25:40], [100.0] * 15, [100.0] * 15)     # T+20 not reached
    sig = [Signal(sessions[25], "Y")]
    doc = cv.build_report(sig, {"Y": young}, cost_pct=COST, as_of=sessions[39], start=FIRST,
                          calendar=cal, ex_dates={"Y": [(sessions[39], "rights")]})
    assert doc["population"]["status"][cv.VOID] == 1
    doc = cv.build_report(sig, {"Y": young}, cost_pct=COST, as_of=sessions[38], start=FIRST,
                          calendar=cal, ex_dates={"Y": [(sessions[39], "rights")]})
    assert doc["population"]["status"][cv.VOID] == 0          # x after the last stored session


def test_voids_shrinking_the_cohort_below_twenty_hold_the_verdict(cal):
    sessions, signals, series = _void_population(cal, n_signals=20)
    doc = cv.build_report(signals, series, cost_pct=COST, as_of=sessions[-1], start=FIRST,
                          calendar=cal, ex_dates={"S03": [(sessions[5], "bonus")]})
    assert doc["verdict"] == cv.VERDICT_INSUFFICIENT
    assert "19 of 20 cohort places filled by non-void, non-duplicate signals" in doc["reason"]


def test_corp_actions_are_read_bounded_by_as_of_and_by_kind(tmp_path, clock):
    market = tmp_path / "market.duckdb"
    store = MarketStore(market, tmp_path / "parquet", clock).open()
    try:
        store.upsert_corp_actions([
            {"symbol": s, "ex_date": x, "kind": k, "ratio": None, "amount": None, "source": "t",
             "recorded_at": clock.now()}
            for s, x, k in [("ABB", date(2026, 9, 10), "bonus"), ("ABB", date(2026, 9, 11), "dividend"),
                            ("ABB", date(2026, 10, 30), "split"), ("TCS", date(2026, 9, 15), "demerger"),
                            ("OTHER", date(2026, 9, 15), "split"), ("ABB", date(2026, 8, 20), "split")]
        ])
    finally:
        store.close()
    con = duckdb.connect(str(market), read_only=True)
    try:
        got = cv.load_ex_dates(lambda q, p: con.execute(q, p).fetchall(), ["ABB", "TCS"],
                               start=date(2026, 8, 29), as_of=date(2026, 10, 26))
    finally:
        con.close()
    assert got == {"ABB": [(date(2026, 9, 10), "bonus")], "TCS": [(date(2026, 9, 15), "demerger")]}


# ============================================================ 8. costs
def test_the_cost_floor_is_the_7_1_cap_at_cat_s_own_stop():
    stop, src = cv.load_stop_pct()
    assert stop == Decimal("5.0") and src == "settings.yaml cat.stop_pct"
    cost, sizing = cv.cat_cost_floor(limits_path=_LIMITS, equity_override=Decimal("36000"),
                                     min_equity=None, stop_pct=stop, stop_source=src)
    # Rs 36,000 x 2% / (2.0 x 5%) = Rs 7,200
    assert Decimal(sizing["notional_inr"]) == Decimal("7200")
    assert sizing["stop_pct"] == "5.0" and sizing["stop_source"] == src
    assert "5.0% stop" in sizing["derivation"]
    assert cost == pytest.approx(float(cost_floor_pct(Decimal("7200"))))
    # No snapshot: the equity_floor_rung (Rs 36,000 at the pinned test limits), stated.
    _, s2 = cv.cat_cost_floor(limits_path=_LIMITS, equity_override=None, min_equity=None,
                              stop_pct=stop)
    assert "equity_floor_rung" in s2["equity_source"] and s2["override"] is False
    assert Decimal(s2["notional_inr"]) == Decimal("7200")
    _, s3 = cv.cat_cost_floor(limits_path=_LIMITS, equity_override=None, min_equity=None,
                              notional_override=Decimal("8000"), stop_pct=stop)
    assert s3["override"] is True and s3["derivation"].startswith("OVERRIDE")


# ============================================================ 9. stores
def test_an_unreadable_store_is_unavailable_not_a_verdict(tmp_path, capsys):
    rc = cv.main(["--state-db", str(tmp_path / "absent.db"), "--db", str(tmp_path / "absent.duckdb")])
    out = capsys.readouterr().out
    assert rc == 0 and cv.VERDICT_UNAVAILABLE in out and cv.VERDICT_INSUFFICIENT not in out
    state = tmp_path / "state.db"
    conn = connect(state)
    apply_migrations(conn)
    conn.close()
    rc = cv.main(["--state-db", str(state), "--db", str(tmp_path / "absent.duckdb")])
    out = capsys.readouterr().out
    assert rc == 0 and cv.VERDICT_UNAVAILABLE in out and "market.duckdb" in out


def _hi52_and_cat_rows(path: Path) -> None:
    conn = connect(path)
    apply_migrations(conn)
    conn.executemany(
        "INSERT INTO prescreen_day_slots (d, symbol, strategy_id, published_at) VALUES (?, ?, ?, ?)",
        [("2026-09-15", "BHEL", "hi52", "x"), ("2026-09-15", "ABB", "cat", "x"),
         ("2026-09-16", "TCS", "cat", "x")])
    conn.commit()
    conn.close()


def test_load_signal_rows_defaults_to_hi52_and_the_keyword_selects_cat(tmp_path):
    path = tmp_path / "state.db"
    _hi52_and_cat_rows(path)
    conn = connect(path)
    try:
        assert load_signal_rows(conn, start=date(2026, 9, 1), as_of=date(2026, 9, 30)) == [
            Signal(date(2026, 9, 15), "BHEL")]
        assert load_signal_rows(conn, start=date(2026, 9, 1), as_of=date(2026, 9, 30),
                                strategy_id=cv.STRATEGY_ID) == [
            Signal(date(2026, 9, 15), "ABB"), Signal(date(2026, 9, 16), "TCS")]
    finally:
        conn.close()
    assert cv.STRATEGY_ID == "cat"


def _stores(tmp_path: Path, cal: NSECalendar, clock) -> tuple[Path, Path, list[date]]:
    """A migrated state.db and a real MarketStore DuckDB: 20 cat signals on the first session,
    one hi52 row, one cat row late in the window; bars +1% at T+10 and T+20; EW 0.1%/session; one
    sonnet-5.5 cluster scored 2026-10-04 20:00 UTC (IST 10-05). S00 is renamed S00N at sessions[10]
    WITHOUT a rekey (its later bars are stored under S00N). Every cat row has its originating
    watchlist row: the S rows age 1, LATE age 0 with its cluster first seen 08:00 IST on d."""
    sessions = _sessions(cal, 45)
    state = tmp_path / "state.db"
    conn = connect(state)
    apply_migrations(conn)
    rows = [(sessions[0].isoformat(), f"S{i:02d}", "cat", "x") for i in range(20)]
    rows += [(sessions[0].isoformat(), "HI", "hi52", "x"), (sessions[40].isoformat(), "LATE", "cat", "x")]
    conn.executemany("INSERT INTO prescreen_day_slots (d, symbol, strategy_id, published_at) "
                     "VALUES (?, ?, ?, ?)", rows)
    conn.executemany("INSERT INTO universe_ew_returns (d, ret, n) VALUES (?, ?, 1)",
                     [(d.isoformat(), 0.001) for d in sessions])
    conn.execute("INSERT INTO equity_snapshots (at, equity) VALUES (?, ?)",
                 (f"{sessions[3].isoformat()}T13:00:00+05:30", "38000"))
    conn.commit()
    conn.close()
    market = tmp_path / "market.duckdb"
    store = MarketStore(market, tmp_path / "parquet", clock).open()
    try:
        bars = []
        for sym in [f"S{i:02d}" for i in range(20)] + ["LATE"]:
            for j, d in enumerate(sessions):
                c = Decimal("101") if j in (9, 19) else Decimal("100")
                stored_as = "S00N" if sym == "S00" and j >= 10 else sym
                bars.append(DailyBar(symbol=stored_as, d=d, open=Decimal("100"), high=c, low=c,
                                     close=c, volume=1, src="bhavcopy"))
        store.upsert_bars_1d(bars)
        store.upsert_instruments_daily([
            {"d": d, "instrument_token": tok, "tradingsymbol": sym, "exchange": "NSE",
             "segment": "NSE", "instrument_type": "EQ", "tick_size": Decimal("0.05"), "lot_size": 1}
            for d, tok, sym in [(sessions[9], 1, "RELIANCE"), (sessions[9], 77, "S00"),
                                (sessions[10], 1, "RELIANCE"), (sessions[10], 77, "S00N")]
        ])
        store.replace_catalyst_watchlist(sessions[0], [
            {"symbol": f"S{i:02d}", "grade": "originating", "direction": "long",
             "event_type": "order_win", "cluster_refs": [f"k{i:02d}"], "event_age_sessions": 1}
            for i in range(20)])
        store.replace_catalyst_watchlist(sessions[40], [
            {"symbol": "LATE", "grade": "originating", "direction": "long", "event_type": "order_win",
             "cluster_refs": ["kl"], "event_age_sessions": 0}])
        seen = datetime.combine(sessions[40], datetime.min.time(), tzinfo=IST) + timedelta(hours=8)
        store.upsert_news_clusters([{
            "cluster_id": "kl", "representative": "r", "source_domains": ["a.com"],
            "first_seen": seen, "last_seen": seen}])
        # After S05's T+20 exit (sessions[19]) and after the early --as-of: voids nothing.
        store.upsert_corp_actions([{"symbol": "S05", "ex_date": sessions[30], "kind": "bonus",
                                    "ratio": None, "amount": None, "source": "t",
                                    "recorded_at": clock.now()}])
        ts = datetime(2026, 10, 4, 20, 0, tzinfo=UTC)
        store.upsert_news_clusters([{
            "cluster_id": "c1", "representative": "r", "source_domains": ["a.com"],
            "first_seen": ts, "last_seen": ts, "scored_at": ts, "scorer_model": "sonnet-5.5"}])
    finally:
        store.close()
    return state, market, sessions


def test_as_of_bounds_both_stores_end_to_end(tmp_path, cal, clock, capsys):
    """Through the script's own main: at an --as-of before the cohort's T+20 session no T+20 bar is
    read (the bars exist in the store), the late row and the scorer switch are invisible; at a later
    --as-of the same stores produce a formed verdict."""
    state, market, sessions = _stores(tmp_path, cal, clock)
    early = sessions[18]                                       # T+20 is sessions[19]
    out_json = tmp_path / "early.json"
    assert cv.main(["--state-db", str(state), "--db", str(market), "--as-of", early.isoformat(),
                    "--limits", str(_LIMITS), "--json", str(out_json)]) == 0
    doc = json.loads(out_json.read_text(encoding="utf-8"))
    assert doc["verdict"] == cv.VERDICT_INSUFFICIENT
    assert doc["population"]["signals_admitted"] == 20         # LATE is after as_of; HI is hi52
    assert doc["meta"]["stored_through"] == early.isoformat()
    assert all(m["status"] == cv.PENDING and "20" not in m["net"] for m in doc["cohort"]["members"])
    assert doc["diagnostics"]["running"]["20"]["n"] == 0
    assert doc["meta"]["segment_bounds"]["scorer_switch_provenance"].startswith("PINNED FALLBACK")
    assert doc["meta"]["sizing"]["equity_inr"] == "38000"      # the snapshot inside the window

    late = sessions[44]
    out_json2 = tmp_path / "late.json"
    assert cv.main(["--state-db", str(state), "--db", str(market), "--as-of", late.isoformat(),
                    "--limits", str(_LIMITS), "--json", str(out_json2)]) == 0
    capsys.readouterr()
    doc2 = json.loads(out_json2.read_text(encoding="utf-8"))
    assert doc2["population"]["signals_admitted"] == 21 and doc2["population"]["sessions"] >= 30
    assert [m["symbol"] for m in doc2["cohort"]["members"]] == [f"S{i:02d}" for i in range(20)]
    cost = doc2["meta"]["cost_round_trip_pct"]
    assert doc2["cohort"]["members"][0]["net"]["20"] == pytest.approx(1.0 - cost, abs=1e-4)
    # +1% gross clears cost x 10/20 and cost x 20/20 at this notional: owner review, not retire.
    assert cost < 0.66
    assert doc2["verdict"] == cv.VERDICT_OWNER_REVIEW
    assert doc2["population"]["status"][cv.VOID] == 0 and doc2["voids"] == []
    # S00's post-rename bars are under S00N (no rekey): followed, merged, measured, and named.
    assert doc2["cohort"]["members"][0]["status"] == cv.MEASURED
    assert doc2["renamed"] == ["S00 (now S00N)"] and "S00 (now S00N)" in cv.render(doc2)
    assert doc2["population"]["status"][cv.DUPLICATE] == 0
    sb = doc2["meta"]["segment_bounds"]
    assert sb["scorer_switch_session"] == "2026-10-06" and "FALLBACK" not in sb["scorer_switch_provenance"]
    assert doc2["diagnostics"]["excess_vs_ew"]["20"]["n"] == 20
    assert doc2["diagnostics"]["segments"]["pre_o15"]["signals"] == 20
    assert doc2["diagnostics"]["segments"]["sonnet55"]["signals"] == 1      # LATE
    assert doc2["meta"]["sizing"]["equity_inr"] == "38000"


# ============================================================ 10. the void window is the hold
def test_a_hole_is_not_voided_by_an_ex_date_after_its_would_be_t20(cal):
    """Reviewer repro: S03 has no bar on d (and S04 a bad T+20 close); a bonus AFTER the would-be
    T+20 must not void them — the hole stays, and the verdict stays INSUFFICIENT."""
    sessions, signals, series = _void_population(cal)
    s03 = series["S03"]
    series["S03"] = Series("S03", s03.dates[1:], s03.open[1:], s03.close[1:])    # no bar on d
    s04 = _series(cal, "S04", start=sessions[0], n=40, t10=103.0, t20=103.0)
    s04.close[19] = float("nan")                                                # bad T+20 close
    series["S04"] = s04
    late_bonus = {"S03": [(sessions[25], "bonus")], "S04": [(sessions[25], "bonus")]}
    doc = cv.build_report(signals, series, cost_pct=COST, as_of=sessions[-1], start=FIRST,
                          calendar=cal, ex_dates=late_bonus)
    assert doc["population"]["status"][cv.VOID] == 0
    assert {h.split(": ")[0] for h in doc["cohort"]["holes"]} == {f"{sessions[0]} S03",
                                                                  f"{sessions[0]} S04"}
    assert doc["verdict"] == cv.VERDICT_INSUFFICIENT
    # …while an ex-date INSIDE the would-be hold still voids the hole.
    inside = {"S03": [(sessions[5], "bonus")]}
    doc = cv.build_report(signals, series, cost_pct=COST, as_of=sessions[-1], start=FIRST,
                          calendar=cal, ex_dates=inside)
    assert doc["voids"] == [f"{sessions[0]} S03: void_ca: bonus ex-date {sessions[5]}"]


# ============================================================ 11. renames
def test_a_renamed_symbol_measures_whether_or_not_it_was_rekeyed(cal):
    """Journalled under OLD, renamed to NEW at sessions[10]. Rekeyed: every bar is under NEW. Not
    rekeyed: bars before the rename under OLD, after it under NEW. Both merge to one series."""
    sessions = _sessions(cal, 40)
    full = _series(cal, "X", start=sessions[0], n=40, t10=104.0, t20=108.0)

    def part(sym, lo, hi):
        return Series(sym, full.dates[lo:hi], full.open[lo:hi], full.close[lo:hi])

    sig = [Signal(sessions[0], "OLD")]
    for series in ({"NEW": part("NEW", 0, 40)},                               # rekeyed
                   {"OLD": part("OLD", 0, 10), "NEW": part("NEW", 10, 40)}):  # not rekeyed
        doc = cv.build_report(sig, series, cost_pct=COST, as_of=sessions[-1], start=FIRST,
                              calendar=cal, renames={"OLD": "NEW"})
        m = doc["cohort"]["members"][0]
        assert m["status"] == cv.MEASURED and m["net"]["20"] == pytest.approx(8.0 - COST)
        assert doc["renamed"] == ["OLD (now NEW)"]
    # NEW's row wins on a date both names carry (rekey's rule).
    both = {"OLD": part("OLD", 0, 40), "NEW": part("NEW", 0, 40)}
    both["OLD"].close[19] = 50.0
    doc = cv.build_report(sig, both, cost_pct=COST, as_of=sessions[-1], start=FIRST,
                          calendar=cal, renames={"OLD": "NEW"})
    assert doc["cohort"]["members"][0]["net"]["20"] == pytest.approx(8.0 - COST)
    # Corp actions follow the rename too.
    doc = cv.build_report(sig, {"NEW": part("NEW", 0, 40)}, cost_pct=COST, as_of=sessions[-1],
                          start=FIRST, calendar=cal, renames={"OLD": "NEW"},
                          ex_dates={"NEW": [(sessions[5], "split")]})
    assert doc["population"]["status"][cv.VOID] == 1


# ============================================================ 12. entry timing (lookahead)
def _orig(age: int, *refs: str) -> list:
    return [cv.OrigRow("e", tuple(refs), age)]


def test_a_story_first_seen_after_the_open_enters_at_the_next_session(cal):
    """A post-open digest can grade an age-0 cluster first seen 09:40 on d; open(d) would book the
    announcement move. The entry moves to the next session's open, and the hold counts from there."""
    sessions = _sessions(cal, 45)
    d = sessions[0]
    s = _series(cal, "A", start=d, n=45)
    s.open[1] = 110.0                         # the announcement gap, booked only by an open(d) entry
    s.close[20] = 121.0                       # T+20 from the NEXT session = close[1 + 19]
    at = lambda h, m: datetime.combine(d, datetime.min.time(), tzinfo=IST) + timedelta(hours=h, minutes=m)  # noqa: E731
    doc = cv.build_report([Signal(d, "A")], {"A": s}, cost_pct=COST, as_of=sessions[-1],
                          start=FIRST, calendar=cal, origination={(d, "A"): _orig(0, "c1")},
                          first_seen={"c1": at(9, 40)})
    m = doc["cohort"]["members"][0]
    assert m["entry"] == sessions[1].isoformat() and m["status"] == cv.MEASURED
    assert m["net"]["20"] == pytest.approx((121.0 / 110.0 - 1) * 100 - COST)
    # Before 09:15 on d (or an age-1 row, which predates d by definition): open(d).
    for orig, seen in ((_orig(0, "c1"), {"c1": at(9, 14)}), (_orig(1, "zz"), {})):
        doc = cv.build_report([Signal(d, "A")], {"A": s}, cost_pct=COST, as_of=sessions[-1],
                              start=FIRST, calendar=cal, origination={(d, "A"): orig},
                              first_seen=seen)
        assert doc["cohort"]["members"][0]["entry"] == d.isoformat()


def test_a_delayed_entry_starts_the_void_window_and_the_benchmark_at_the_entry_session(cal):
    sessions = _sessions(cal, 45)
    d = sessions[0]
    s = _series(cal, "A", start=d, n=45)
    late = datetime.combine(d, datetime.min.time(), tzinfo=IST) + timedelta(hours=11)
    kw = dict(cost_pct=COST, as_of=sessions[-1], start=FIRST, calendar=cal,
              origination={(d, "A"): _orig(0, "c1")}, first_seen={"c1": late})
    # An ex-date ON the entry session (d+1) is already in the new unit: no void.
    doc = cv.build_report([Signal(d, "A")], {"A": s}, ex_dates={"A": [(sessions[1], "bonus")]}, **kw)
    assert doc["population"]["status"][cv.VOID] == 0
    # The benchmark compounds entry+1 .. exit: 19 sessions of 0.1% from sessions[2] to sessions[20].
    daily = {x: 0.001 for x in sessions[2:21]}
    doc = cv.build_report([Signal(d, "A")], {"A": s}, daily=daily, **kw)
    assert doc["diagnostics"]["excess_vs_ew"]["20"]["n"] == 1


def test_no_originating_row_or_an_unreadable_first_seen_is_a_hole(cal):
    sessions = _sessions(cal, 40)
    d = sessions[0]
    s = {"A": _series(cal, "A", start=d, n=40)}
    doc = cv.build_report([Signal(d, "A")], s, cost_pct=COST, as_of=sessions[-1], start=FIRST,
                          calendar=cal, origination={}, first_seen={})
    assert doc["cohort"]["holes"] == [f"{d} A: no originating catalyst_watchlist row"]
    doc = cv.build_report([Signal(d, "A")], s, cost_pct=COST, as_of=sessions[-1], start=FIRST,
                          calendar=cal, origination={(d, "A"): _orig(0, "gone")}, first_seen={})
    assert doc["cohort"]["holes"][0].startswith(f"{d} A: story first_seen unreadable")


# ============================================================ 13. same-story duplicates
def test_an_age0_then_age1_origination_of_one_story_is_a_duplicate(cal):
    """First seen on d before d's digest: age 0 on d, age 1 on d+1, both originate. The second
    shares a cluster with the first: DUPLICATE, out of the cohort, listed with the signal it repeats."""
    sessions = _sessions(cal, 45)
    signals = [Signal(sessions[0], f"S{i:02d}") for i in range(20)] + [Signal(sessions[1], "S03")]
    series = {s.symbol: _series(cal, s.symbol, start=sessions[0], n=45, t10=103.0, t20=103.0)
              for s in signals}
    early = datetime.combine(sessions[0], datetime.min.time(), tzinfo=IST) + timedelta(hours=7)
    orig = {(s.d, s.symbol): _orig(1, f"k{s.symbol}") for s in signals}
    orig[(sessions[0], "S03")] = _orig(0, "story")
    orig[(sessions[1], "S03")] = _orig(1, "story", "followup")
    doc = cv.build_report(signals, series, cost_pct=COST, as_of=sessions[-1], start=FIRST,
                          calendar=cal, origination=orig, first_seen={"story": early})
    assert doc["population"]["status"][cv.DUPLICATE] == 1
    assert doc["duplicates"] == [f"{sessions[1]} S03: duplicate of {sessions[0]} S03 "
                                 "(shared cluster story)"]
    assert len(doc["cohort"]["members"]) == 20
    assert (sessions[1].isoformat(), "S03") not in {(m["d"], m["symbol"]) for m in doc["cohort"]["members"]}
    assert doc["diagnostics"]["running"]["20"]["n"] == 20


def test_consecutive_rows_from_disjoint_clusters_stay_separate_signals(cal):
    """Same symbol, same event_type, consecutive days, no shared cluster: two stories by the
    registered letter, both counted."""
    sessions = _sessions(cal, 45)
    signals = [Signal(sessions[0], "A"), Signal(sessions[1], "A")]
    series = {"A": _series(cal, "A", start=sessions[0], n=45)}
    orig = {(sessions[0], "A"): _orig(1, "c1"), (sessions[1], "A"): _orig(1, "c2")}
    doc = cv.build_report(signals, series, cost_pct=COST, as_of=sessions[-1], start=FIRST,
                          calendar=cal, origination=orig, first_seen={})
    assert doc["population"]["status"][cv.DUPLICATE] == 0 and doc["duplicates"] == []
    assert len(doc["cohort"]["members"]) == 2


def test_the_script_is_read_only_by_construction():
    """No write statement anywhere; both stores open through the hi52 verdict's read-only openers
    (``mode=ro`` URI, ``read_only=True``)."""
    src = _SCRIPT.read_text(encoding="utf-8")
    assert "h52v.open_state_db(" in src and "h52v.open_market_db(" in src
    opener_src = (_SCRIPT.parent / "hi52_forward_verdict.py").read_text(encoding="utf-8")
    assert "read_only=True" in opener_src and "mode=ro" in opener_src
    for stmt in ("INSERT INTO", "UPDATE ", "DELETE FROM", "CREATE TABLE", "DROP "):
        assert stmt not in src, stmt
