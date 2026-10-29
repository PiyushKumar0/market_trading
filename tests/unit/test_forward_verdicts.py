"""Pre-registered forward verdicts (T4): fixed-cohort looks, the kill and analyst rules, the loader, the CLI."""

from __future__ import annotations

import importlib.util
import json
import re
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path

import pytest

from engine.learning import forward_verdicts as fv
from engine.learning.forward_verdicts import (
    Bucket,
    C2Arms,
    Cell,
    CellLook,
    ExcludedKey,
    Look,
    SignalRow,
    Stats,
)

D0 = date(2026, 6, 1)
R = D0
_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "forward_verdicts.py"


def _r(i=0, *, symbol=None, strategy_id="ins", construct="rule", status="closed", fill=0, net=1.0,
       decision=None, conf=None, key=("v1", "m")) -> SignalRow:
    d = D0 + timedelta(days=i)
    net = net if status == "closed" else None
    return SignalRow(d, symbol or f"S{i:03d}", strategy_id, construct, status,
                     None if fill is None else d + timedelta(days=fill), None, net, net, net, None,
                     decision, conf, *key)


def _kill(nets, registered_from=R):
    return fv.kill_verdict(fv.kill_looks([_r(i, net=n) for i, n in enumerate(nets)]),
                           registered_from=registered_from)[0]


def _cells(*diffs, final=True):
    return [Cell(D0 + timedelta(days=i), "hi52", 1, 1, final, x) for i, x in enumerate(diffs)]


def _analyst(cells, registered_from=R):
    return fv.analyst_verdict(fv.analyst_looks(cells), registered_from=registered_from)[0]


# ------------------------------------------------------------------ kill looks
def test_late_finisher_never_reorders_a_look():
    rows = [_r(0, status="open"), *(_r(i) for i in range(1, 25))]
    assert fv.kill_looks(rows)[0].n_closed == 19
    assert fv.look_holders(rows, 20) == [rows[0]]


def test_look_waits_for_every_member():
    assert not fv.kill_looks([*(_r(i) for i in range(19)), _r(19, status="open")])[0].formed


def test_void_is_final_and_excluded():
    rows = [*(_r(i) for i in range(19)), _r(19, status="void_ca")]
    assert fv.kill_looks(rows)[0] == Look(20, True, 19, 1, 1.0, 1.0, 1.0, None)


def test_members_sharing_a_fill_day_are_ordered_by_symbol():
    rows = [*(_r(i) for i in range(19)), _r(25, symbol="A", fill=5), _r(20, symbol="Z", fill=10, status="open")]
    assert fv.kill_looks(rows)[0].formed


def test_an_unfilled_open_row_dated_by_the_last_fill_holds_the_look():
    rows = [*(_r(i) for i in range(20)), _r(5, symbol="LATE", status="open", fill=None)]
    assert fv.look_holders(rows, 20) == [rows[-1]]
    assert fv.kill_looks([*rows[:20], _r(30, status="open", fill=None)])[0].formed


def test_the_holder_cutoff_includes_the_last_fill_day():
    rows = [*(_r(i) for i in range(20)), _r(19, symbol="ON", status="open", fill=None)]
    assert fv.look_holders(rows, 20) == [rows[-1]]


def test_demote_at_median_exactly_zero():
    assert _kill([0.0] * 20) == fv.DEMOTE


def test_hold_above_zero():
    assert _kill([0.01] * 20) == fv.HOLD


def test_demote_reads_the_median_not_the_mean():
    assert _kill([-0.1] * 11 + [5.0] * 9) == fv.DEMOTE


def test_look2_supersedes_look1():
    looks = fv.kill_looks([*(_r(i, net=1.0) for i in range(20)), *(_r(i, net=-1.0) for i in range(20, 40))])
    assert fv.kill_verdict(looks[:1], registered_from=R)[0] == fv.HOLD
    assert fv.kill_verdict(looks, registered_from=R)[0] == fv.DEMOTE


def test_insufficient_below_20_filled():
    assert _kill([1.0] * 19) == fv.INSUFFICIENT


def test_unregistered_without_registered_from():
    assert _kill([1.0] * 20, registered_from=None) == fv.UNREGISTERED
    assert _analyst(_cells(*[1.0] * 30), registered_from=None) == fv.UNREGISTERED


# ------------------------------------------------------------------ analyst value
def test_cell_membership_from_decisions_not_outcomes():
    rows = [_r(0, decision="enter", status="open"), _r(0, symbol="B", decision="no_action", status="open"),
            _r(1, decision="enter"), _r(2), _r(2, symbol="B")]
    assert fv.analyst_cells(rows) == [Cell(D0, "ins", 1, 1, False, None)]


def test_a_cell_arm_is_valued_by_its_mean():
    rows = [_r(0, decision="enter", net=0.0), _r(0, symbol="B", decision="enter", net=0.0),
            _r(0, symbol="C", decision="enter", net=3.0), _r(0, symbol="D", decision="no_action", net=0.0)]
    assert fv.analyst_cells(rows)[0].diff == 1.0


def test_cell_waits_for_its_late_row():
    rows = [_r(0, decision="enter"), _r(0, symbol="B", decision="no_action", status="open")]
    assert not fv.analyst_cells(rows)[0].final


def test_unusable_cell_reported_not_averaged():
    rows = [_r(0, decision="enter", net=2.0), _r(0, symbol="B", decision="no_action", status="void_ca"),
            _r(1, decision="enter", net=1.0), _r(1, symbol="B", decision="no_action", net=0.0)]
    assert fv.analyst_looks(fv.analyst_cells(rows), sizes=(2,)) == [CellLook(2, True, 2, 2, 1, 1.0)]


def test_advisory_at_mean_exactly_zero():
    assert _analyst(_cells(*[0.0] * 30)) == fv.ADVISORY


def test_keep_above_zero():
    assert _analyst(_cells(*[0.2] * 30)) == fv.KEEP


def test_advisory_reads_the_mean_not_the_median():
    assert _analyst(_cells(*[0.1] * 16, *[-1.0] * 14)) == fv.ADVISORY


def test_analyst_look2_supersedes_look1():
    assert _analyst(_cells(*[1.0] * 30)) == fv.KEEP
    assert _analyst(_cells(*[1.0] * 30, *[-2.0] * 30)) == fv.ADVISORY


def test_analyst_insufficient_below_30_final_cells():
    assert _analyst(_cells(*[1.0] * 29)) == fv.INSUFFICIENT


def test_the_registered_key_is_the_first_decision_by_day_then_symbol():
    rows = [_r(1, decision="enter", key=("v2", "m")), _r(0, symbol="B", decision="enter", key=("v3", "m")),
            _r(0, symbol="A", decision="no_action", key=("v1", "m"))]
    assert fv.registered_keys(rows) == {"ins": ("v1", "m")}


def test_the_population_freezes_on_the_first_other_key():
    rows = [_r(0, decision="enter"), _r(1, decision="enter", key=("v2", "m")), _r(2, decision="enter")]
    assert fv.split_by_key(rows, {"ins": ("v1", "m")}) == ([rows[0]], rows[1:])


def test_cell_holders_are_the_open_decided_rows_of_the_first_cells():
    rows = [_r(0, decision="enter", status="open"), _r(0, symbol="B", decision="no_action"),
            _r(1, decision="enter", status="open"), _r(1, symbol="B", decision="no_action")]
    assert fv.cell_holders(rows, fv.analyst_cells(rows), 1) == [rows[0]]


# ------------------------------------------------------------------ diagnostics
def test_confidence_buckets_split_closed_nets_by_decision():
    assert fv.confidence_buckets([
        _r(0, decision="enter", conf=0.55, net=2.0), _r(1, decision="enter", conf=0.6, net=0.0),
        _r(2, decision="no_action", conf=0.5, net=-1.0), _r(3, decision="enter", conf=0.8, status="open"),
    ]) == [Bucket("<0.55", "no_action", Stats(1, -1.0, -1.0, 0.0)), Bucket("0.55-0.65", "enter", Stats(2, 1.0, 1.0, 0.5)),
           Bucket(">=0.75", "enter", Stats(0, None, None, None))]


def test_c2_arms_compare_admitted_with_down_blocked_and_count_other_reasons_in_neither():
    blocked = [replace(_r(0, construct="rule_blocked", net=-2.0), reason="down"),
               replace(_r(1, construct="rule_blocked", net=5.0), reason="undefined"),
               replace(_r(2, construct="rule_blocked", net=5.0), reason="index_bar_missing")]
    arms = fv.c2_arms([_r(0, net=1.0), _r(1, status="open")], blocked)
    assert arms == C2Arms(Stats(1, 1.0, 1.0, 1.0), Stats(1, -2.0, -2.0, 0.0), 2)


# ------------------------------------------------------------------ loader and readings
def _slot(conn, d, symbol, strategy_id="ins", *, decision=None, call_id=None):
    conn.execute("INSERT INTO prescreen_day_slots (d, symbol, strategy_id, published_at, analyst_decision, "
                 "analyst_confidence, analyst_call_id) VALUES (?, ?, ?, 't', ?, 0.6, ?)",
                 (d.isoformat(), symbol, strategy_id, decision, call_id))


def _call(conn, call_id, prompt_version, model):
    conn.execute("INSERT INTO agent_calls (call_id, agent_id, model, ok, at, prompt_version) "
                 "VALUES (?, 'intraday', ?, 1, 't', ?)", (call_id, model, prompt_version))


def _outcome(conn, d, symbol, construct, status, *, strategy_id="ins", fill_d=None, exit_d=None, net=None):
    conn.execute("INSERT INTO signal_outcomes (d, symbol, strategy_id, construct, status, fill_d, exit_d, net_t5, "
                 "net_t10, net_t20, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 't')",
                 (d.isoformat(), symbol, strategy_id, construct, status, fill_d and fill_d.isoformat(),
                  exit_d and exit_d.isoformat(), net, net, net))


def _day(i):
    return D0 + timedelta(days=i)


def _load(conn, construct="rule", as_of=None, strategies=("ins",)):
    return fv.load_rows(conn, strategies=strategies, construct=construct, start=D0, as_of=as_of or _day(9))


def test_load_rows_joins_the_outcome_and_the_call_and_an_unscored_row_reads_open(conn):
    _call(conn, "c1", "v1", "m")
    _slot(conn, _day(0), "A", decision="enter", call_id="c1")
    _outcome(conn, _day(0), "A", "next_open", "closed", fill_d=_day(1), exit_d=_day(5), net=2.0)
    _slot(conn, _day(0), "B")
    assert _load(conn, "next_open") == [
        SignalRow(_day(0), "A", "ins", "next_open", "closed", _day(1), _day(5), 2.0, 2.0, 2.0, None,
                  "enter", 0.6, "v1", "m"),
        SignalRow(_day(0), "B", "ins", "next_open", "open", None, None, None, None, None, None,
                  None, 0.6, None, None)]


def test_as_of_bounds_d_fill_and_exit(conn):
    _slot(conn, _day(0), "A")
    _outcome(conn, _day(0), "A", "rule", "closed", fill_d=_day(0), exit_d=_day(5), net=2.0)
    _slot(conn, _day(0), "B")
    _outcome(conn, _day(0), "B", "rule", "closed", fill_d=_day(4), exit_d=_day(8), net=1.0)
    _slot(conn, _day(4), "C")
    assert [(r.symbol, r.status, r.fill_d, r.exit_d, r.net_t20) for r in _load(conn, as_of=_day(3))] == [
        ("A", "open", _day(0), None, None), ("B", "open", None, None, None)]


def test_as_of_is_inclusive(conn):
    _slot(conn, _day(0), "A")
    _outcome(conn, _day(0), "A", "rule", "open", fill_d=_day(3))
    _slot(conn, _day(0), "B")
    _outcome(conn, _day(0), "B", "rule", "closed", fill_d=_day(0), exit_d=_day(3), net=1.0)
    a, b = _load(conn, as_of=_day(3))
    assert (a.fill_d, b.status, b.net_t20) == (_day(3), "closed", 1.0)


def test_load_rows_rule_takes_brk20_first_publications_only(conn):
    conn.execute("INSERT INTO brk20_resting_levels (symbol, signal_d, entry, stop, score, expires_d, created_at, "
                 "updated_at) VALUES ('A', ?, '90', '85', 1.0, ?, 't', 't')", (_day(0).isoformat(), _day(6).isoformat()))
    _slot(conn, _day(0), "A", "brk20")
    _slot(conn, _day(2), "A", "brk20")
    assert [r.d for r in _load(conn, strategies=("brk20",))] == [_day(0)]
    assert [r.d for r in _load(conn, "next_open", strategies=("brk20",))] == [_day(0), _day(2)]


def test_load_rows_rule_blocked_reads_the_blocked_table(conn):
    conn.execute("INSERT INTO brk20_regime_blocked (symbol, signal_d, entry, stop, score, reason, created_at) "
                 "VALUES ('A', ?, '90', '85', 1.0, 'down', 't')", (_day(0).isoformat(),))
    _outcome(conn, _day(0), "A", "rule_blocked", "unfilled", strategy_id="brk20")
    assert _load(conn, "rule_blocked", strategies=("brk20",)) == [
        SignalRow(_day(0), "A", "brk20", "rule_blocked", "unfilled", None, None, None, None, None, None,
                  reason="down")]
    assert _load(conn, "rule_blocked") == []


def test_a_reading_loads_its_population_from_registered_from(conn):
    _slot(conn, _day(-1), "A")
    _slot(conn, _day(0), "B")
    assert fv.kill_reading(conn, "ins", as_of=_day(9), registered_from=R).n_rows == 1
    assert fv.kill_reading(conn, "ins", as_of=_day(9), registered_from=None).n_rows == 2


def test_other_prompt_or_model_excluded_and_counted(conn):
    for call_id, pv, model in (("c1", "v1", "m"), ("c2", "v2", "m"), ("c3", "v1", "m2")):
        _call(conn, call_id, pv, model)
    for symbol, call_id in (("A", "c1"), ("B", "c2"), ("C", "c3")):
        _slot(conn, D0, symbol, decision="enter", call_id=call_id)
    reading = fv.analyst_reading(conn, as_of=_day(9), registered_from=R)
    assert (reading.n_kept, reading.excluded) == (1, (ExcludedKey("ins", "v1", "m2", 1), ExcludedKey("ins", "v2", "m", 1)))


# ------------------------------------------------------------------ CLI
@pytest.fixture(scope="module")
def cli():
    spec = importlib.util.spec_from_file_location("mt_forward_verdicts", _SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _main(cli, rule, db, *extra):
    return cli.main(["--rule", rule, "--db", str(db), "--as-of", "2026-07-31", *extra])


@pytest.mark.parametrize("rule", ["brk20", "ins", "analyst"])
def test_cli_prints_the_verdict_line_first(cli, conn, db_path, capsys, monkeypatch, rule):
    monkeypatch.setattr(fv, "REGISTERED_FROM", R)
    assert _main(cli, rule, db_path) == 0
    assert capsys.readouterr().out.startswith(f"VERDICT {rule}: {fv.INSUFFICIENT}")


def test_cli_prints_look_progress(cli, conn, db_path, capsys):
    _main(cli, "analyst", db_path)
    assert "\nlook 1: 0 of 30 final, interim, decides nothing;" in capsys.readouterr().out


def test_cli_names_a_filled_row_holding_the_look(cli, conn, db_path, capsys):
    _slot(conn, D0, "HOLE")
    _outcome(conn, D0, "HOLE", "rule", "open", fill_d=D0)
    _main(cli, "ins", db_path)
    out = capsys.readouterr().out
    assert re.search(r"HOLE +d=2026-06-01 open +fill=2026-06-01 .*T\+20 2026-06-\d\d, [1-9]\d* sessions past", out)


def test_cli_reports_an_unreadable_store_and_exits_0(cli, tmp_path, capsys):
    assert _main(cli, "ins", tmp_path / "missing.db") == 0
    assert capsys.readouterr().out.startswith("VERDICT ins: UNAVAILABLE (")


def test_cli_writes_the_reading_as_json(cli, conn, db_path, tmp_path, capsys):
    _main(cli, "ins", db_path, "--json", str(tmp_path / "r.json"))
    doc = json.loads((tmp_path / "r.json").read_text(encoding="utf-8"))
    assert (doc["verdict"], doc["as_of"], doc["looks"][0]["size"]) == (fv.UNREGISTERED, "2026-07-31", 20)
