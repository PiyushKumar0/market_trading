"""scripts/no_stop_verdict.py — the C5 no-stop forward-test decision rule and its diagnostics."""

from __future__ import annotations

import importlib.util
import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.config import config_dir
from engine.learning.exit_sim import SimBar

_SPEC = importlib.util.spec_from_file_location(
    "no_stop_verdict", Path(__file__).resolve().parents[2] / "scripts" / "no_stop_verdict.py"
)
nsv = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(nsv)


@pytest.fixture(scope="module")
def calendar() -> NSECalendar:
    return NSECalendar(config_dir() / "calendar", Clock(), include_special=False)


def _add(conn, rec_id, strategy="hi52", status="closed", kind="entry", net_pct=-3.0,
         net_t20=-1.0, excess_t20=0.5, fill_d="2026-09-10", exit_reason="stop",
         net_t5=None, net_t10=None) -> None:
    payload = {"instrument": "X"} if kind is None else {"kind": kind, "instrument": "X"}
    conn.execute("INSERT INTO recommendations (rec_id, payload, delivered_at) VALUES (?, ?, ?)",
                 (rec_id, json.dumps(payload), "2026-09-10T10:00:00+05:30"))
    conn.execute(
        "INSERT INTO rec_outcomes (rec_id, strategy_id, status, fill_d, exit_reason, net_pct, "
        "net_t5, net_t10, net_t20, excess_t20, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (rec_id, strategy, status, fill_d, exit_reason, net_pct, net_t5, net_t10, net_t20,
         excess_t20, "2026-10-14T23:00:00+05:30"),
    )


def _rows(n, d, e):
    """n eligible rows whose paired difference is d and excess is e."""
    return [{"status": "closed", "net_pct": -3.0, "net_t20": -3.0 + d, "excess_t20": e,
             "strategy_id": "hi52", "fill_d": "2026-09-10", "exit_reason": "stop"} for _ in range(n)]


def test_decision_rule_labels():
    assert nsv.decision_block(_rows(19, 2.0, 1.0))["label"] == nsv.LABEL_INSUFFICIENT
    assert nsv.decision_block(_rows(20, 2.0, 1.0))["label"] == nsv.LABEL_SUPPORTED
    assert nsv.decision_block(_rows(20, 0.0, 1.0))["label"] == nsv.LABEL_STOP_OK     # D <= 0
    assert nsv.decision_block(_rows(20, -1.0, 1.0))["label"] == nsv.LABEL_STOP_OK
    assert nsv.decision_block(_rows(20, 2.0, 0.0))["label"] == nsv.LABEL_NO_EDGE     # E <= 0
    assert nsv.decision_block(_rows(20, 2.0, -1.0))["label"] == nsv.LABEL_NO_EDGE


def test_population_filtering(conn):
    _add(conn, "ok1")
    _add(conn, "ok_nokind", kind=None)                  # a missing kind counts as entry
    _add(conn, "other", strategy="orb")                 # other strategy: not in the population
    _add(conn, "exit_rec", kind="exit")                 # not an entry rec
    _add(conn, "void", status="void_ca", net_pct=None, net_t20=None, excess_t20=None)
    _add(conn, "unf", status="unfilled", net_pct=None, net_t20=None, excess_t20=None)
    _add(conn, "uns", status="unscorable", net_pct=None, net_t20=None, excess_t20=None)
    _add(conn, "open1", status="open", net_pct=None, net_t20=None, excess_t20=None)
    _add(conn, "t20null", net_t20=None, excess_t20=None)  # closed but T+20 not yet scorable
    eligible, counts = nsv.split_population(nsv.load_population(conn))
    assert sorted(r["rec_id"] for r in eligible) == ["ok1", "ok_nokind"]
    assert (counts["population"], counts["eligible"], counts["void_ca"], counts["unfilled"],
            counts["unscorable"], counts["pending"]) == (7, 2, 1, 1, 1, 2)


def test_paired_difference_uses_same_rec(conn):
    _add(conn, "a", net_pct=-5.0, net_t20=-1.0)
    _add(conn, "b", net_pct=2.0, net_t20=1.5)
    eligible, _ = nsv.split_population(nsv.load_population(conn))
    by_id = {r["rec_id"]: d for r, d in zip(eligible, nsv.paired(eligible), strict=True)}
    assert by_id == {"a": 4.0, "b": -0.5}


def test_sign_test_hand_checked():
    # 4 positive, 1 negative, 1 zero (ignored): n=5, k=4 -> 2*(C(5,0)+C(5,1))/32 = 0.375
    r = nsv.sign_test_p([1, 2, 3, 4, -1, 0])
    assert r == {"n_nonzero": 5, "n_positive": 4, "p": 0.375}
    assert nsv.sign_test_p([1, 1, 1, 1])["p"] == pytest.approx(2 / 16)
    assert nsv.sign_test_p([1, -1])["p"] == 1.0                  # capped at 1
    assert nsv.sign_test_p([0, 0])["p"] is None


def test_bootstrap_deterministic_and_bounded():
    e, d = [1.0, 2.0, 3.0, 4.0], [-1.0, 0.0, 1.0, 5.0]
    a = nsv.bootstrap_ci(e, d, resamples=500)
    assert a == nsv.bootstrap_ci(e, d, resamples=500)
    assert 1.0 <= a["E"][0] <= a["E"][1] <= 4.0
    assert -1.0 <= a["D"][0] <= a["D"][1] <= 5.0
    same = nsv.bootstrap_ci([2.0] * 5, [3.0] * 5, resamples=200)
    assert same["E"] == [2.0, 2.0] and same["D"] == [3.0, 3.0]
    assert nsv.bootstrap_ci([], [])["E"] is None


def test_confirmation_slice_and_per_strategy(conn, calendar):
    _add(conn, "old", fill_d="2026-10-01")
    _add(conn, "new", strategy="ins", fill_d="2026-10-16", net_pct=-4.0, net_t20=1.0)
    out = nsv.build_signal_level(nsv.load_population(conn), {}, calendar)
    assert out["confirmation_slice"]["n"] == 1
    assert out["confirmation_slice"]["D"] == 5.0
    assert out["diagnostics"]["per_strategy"]["ins"]["n"] == 1
    assert out["decision"] == nsv.LABEL_INSUFFICIENT
    assert "label" not in out["confirmation_slice"] and "label" not in out["running"]


def test_counterfactual_walk_stop_exit():
    t0 = datetime(2026, 10, 13, 10, 10, tzinfo=IST)
    bars = [
        SimBar(datetime(2026, 10, 13, 10, 11, tzinfo=IST), Decimal(100), Decimal(101), Decimal(99), Decimal(100)),
        SimBar(datetime(2026, 10, 13, 10, 12, tzinfo=IST), Decimal(100), Decimal(100), Decimal(94), Decimal(95)),
        SimBar(date(2026, 10, 14), Decimal(95), Decimal(120), Decimal(95), Decimal(118)),
    ]
    o = nsv.counterfactual_walk(
        bars, Decimal(100), t0, stop=Decimal(96), target=None, horizon=date(2026, 11, 10),
        end=date(2026, 10, 14), cost_pct=Decimal(0),
    )
    assert (o.status, o.reason, o.exit_px) == ("exit", "stop", Decimal(96))
    assert o.net_pct == Decimal("-4")
    # without a breach the same walk stays open to the horizon
    calm = nsv.counterfactual_walk(
        bars[:1] + bars[2:], Decimal(100), t0, stop=Decimal(90), target=None,
        horizon=date(2026, 11, 10), end=date(2026, 10, 14), cost_pct=Decimal(0),
    )
    assert calm.status == "open"


def test_portfolio_without_exit_policy_column(conn, calendar):
    conn.execute("ALTER TABLE positions RENAME TO positions_old")
    conn.execute("CREATE TABLE positions (position_id TEXT, is_paper INTEGER)")
    out = nsv.build_portfolio(conn, {}, calendar, date(2026, 10, 15))
    assert out["counts"]["time_only"] == 0 and out["note"]


def test_portfolio_time_only_position(conn, calendar):
    conn.execute(
        "INSERT INTO positions (position_id, symbol, qty, avg_entry, state, is_paper, origin, "
        "close_reason, opened_at, closed_at, realized_pnl, costs, exit_policy) "
        "VALUES ('p1','X',10,'100','CLOSED',1,'platform','risk_flatten',"
        "'2026-10-17T10:10:00+05:30','2026-10-19T11:00:00+05:30','-50','10','time_only')"
    )
    conn.execute(
        "INSERT INTO proposals (proposal_id, agent_id, action, payload, inputs_digest, created_at) "
        "VALUES ('pr','a','enter','{}','d','t')"
    )
    conn.execute(
        "INSERT INTO verdicts (verdict_id, proposal_id, verdict, payload, evaluated_at, is_paper) "
        "VALUES ('v','pr','reject',?,'2026-10-17T10:00:00+05:30',1)",
        (json.dumps({"reasons": ["max_open_positions: total 4+0+1 (limit 4)"]}),),
    )
    out = nsv.build_portfolio(conn, {}, calendar, date(2026, 10, 15))
    assert out["counts"]["closed"] == 1 and out["counts"]["closed_without_costs"] == 0
    assert out["net_pct"]["median"] == pytest.approx(-6.0)        # (-50 - 10) / 1000
    assert out["refused_full_book"] == 1


def _eligible(n, d=1.0, e=1.0, start="2026-09-01"):
    return [{"rec_id": f"r{i:03d}", "status": "closed", "net_pct": -3.0, "net_t20": -3.0 + d,
             "excess_t20": e, "strategy_id": "hi52", "fill_d": start, "exit_reason": "stop",
             "net_t5": None, "net_t10": None}
            for i in range(n)]


def test_looks_fixed_and_look2_only_at_40():
    rows = _eligible(25)
    first = nsv.registered_looks(rows)
    assert first["look2"] is None and first["look1"]["n"] == 20
    # rows beyond the first 20 (here strongly negative) never change look 1
    extra = [{**r, "rec_id": f"z{i}", "fill_d": "2026-10-01", "net_t20": -30.0, "excess_t20": -9.0}
             for i, r in enumerate(_eligible(15))]
    later = nsv.registered_looks(rows + extra)
    assert later["look1"] == first["look1"]
    assert later["look2"]["n"] == 40
    assert nsv.registered_looks(_eligible(19)) == {"look1": None, "look2": None}


def test_looks_ordered_by_fill_date_then_rec_id(calendar):
    late = [{**r, "rec_id": f"late{i}", "fill_d": "2026-09-20", "net_t20": -50.0}
            for i, r in enumerate(_eligible(5))]
    out = nsv.build_signal_level(late + _eligible(20), {}, calendar, date(2026, 12, 1))
    assert out["looks"]["look1"]["D"] == 1.0          # the five late rows fall outside the first 20
    assert out["decision"] == nsv.LABEL_SUPPORTED
    assert out["running"]["n"] == 25


def test_pending_split(conn, calendar):
    _add(conn, "past_due", fill_d="2026-08-03", net_t20=None, excess_t20=None)
    _add(conn, "awaiting", fill_d="2026-10-10", net_t20=None, excess_t20=None)
    _add(conn, "open_nofill", status="open", fill_d=None, net_pct=None, net_t20=None, excess_t20=None)
    _add(conn, "open_old", status="open", fill_d="2026-08-03", net_pct=None, net_t20=-1.0,
         excess_t20=0.1)
    _, counts = nsv.split_population(nsv.load_population(conn), calendar, date(2026, 10, 15))
    assert (counts["awaiting_t20"], counts["t20_past_due"], counts["awaiting_shipped_exit"]) == (2, 1, 1)
    assert counts["pending"] == 4


def test_horizon_cells_exclude_final_statuses(calendar):
    rows = [
        {"status": "closed", "fill_d": "2026-09-01", "net_t5": -1.0, "net_pct": -2.0},
        {"status": "void_ca", "fill_d": "2026-09-01", "net_t5": 5.0, "net_pct": None},
    ]
    assert nsv.horizon_diagnostics(rows, 5, {}, calendar)["net"]["n"] == 1


def _position(conn, pid, reason, basis):
    conn.execute(
        "INSERT INTO positions (position_id, symbol, qty, avg_entry, state, is_paper, origin, "
        "close_reason, close_basis, opened_at, closed_at, realized_pnl, costs, exit_policy) "
        "VALUES (?,'X',10,'100','CLOSED',1,'platform',?,?,'2026-10-17T10:10:00+05:30',"
        "'2026-10-19T11:00:00+05:30','-50','10','time_only')", (pid, reason, basis),
    )


def test_floor_cause_filter_and_flatten_split(conn, calendar):
    _position(conn, "a", "risk_flatten", "equity_floor")
    _position(conn, "b", "risk_flatten", "paper_reset")
    for cause, at in [("floor_weekly_drawdown", "2026-10-17T09:00:00+05:30"),
                      ("floor_cumulative_floor", "2026-10-10T09:00:00+05:30"),   # before policy start
                      ("some_other_halt", "2026-10-17T09:00:00+05:30")]:
        conn.execute("INSERT INTO paper_halts (cause, rung, set_at) VALUES (?, 'r', ?)", (cause, at))
    out = nsv.build_portfolio(conn, {}, calendar, date(2026, 10, 20))
    assert out["floor_events"] == {"floor_halts_latched_distinct_causes": 1,
                                   "risk_flatten_equity_floor": 1, "risk_flatten_paper_reset": 1}


def test_voids_and_resets_excluded_from_cells(conn, calendar):
    _position(conn, "ok", "time_stop", None)
    _position(conn, "v", "void", None)
    _position(conn, "r", "risk_flatten", "paper_reset")
    out = nsv.build_portfolio(conn, {}, calendar, date(2026, 10, 20))
    assert out["net_pct"]["n"] == 1
    assert (out["counts"]["closed"], out["counts"]["excluded_void"],
            out["counts"]["excluded_paper_reset"]) == (3, 1, 1)


def test_daily_only_fallback_marks_row():
    duckdb = pytest.importorskip("duckdb")
    m = duckdb.connect()
    m.execute("CREATE TABLE bars_1d (symbol TEXT, d DATE, open DECIMAL(12,2), high DECIMAL(12,2), "
              "low DECIMAL(12,2), close DECIMAL(12,2))")
    m.execute("INSERT INTO bars_1d VALUES ('X','2026-10-13',100,101,99,100),"
              "('X','2026-10-14',100,100,94,95)")
    opened = datetime(2026, 10, 13, 10, 10, tzinfo=IST)
    bars, kind = nsv.load_walk_bars(m, "X", opened, date(2026, 10, 15))
    assert kind == "daily_only" and len(bars) == 2
    o = nsv.counterfactual_walk(bars, Decimal(100), opened, stop=Decimal(96), target=None,
                                horizon=None, end=date(2026, 10, 15), cost_pct=Decimal(0))
    assert o.reason == "stop"      # the fill day's daily bar is skipped; the next day breaches
