"""Pre-registered forward verdicts (T4): fixed-cohort looks, the kill and analyst rules, the loader, the CLI."""

from __future__ import annotations

import importlib.util
import re
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path

import pytest

from engine.learning import forward_verdicts as fv
from engine.learning.forward_verdicts import C2Arms, Cell, CellLook, Look, SignalRow, Stats

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


# ------------------------------------------------------------------ kill looks
def test_look_forms_when_every_member_is_final_and_a_void_carries_no_return():
    rows = [_r(i) for i in range(19)]
    assert not fv.kill_looks([*rows, _r(19, status="open")])[0].formed
    assert fv.kill_looks([*rows, _r(19, status="void_ca")])[0] == Look(20, True, 19, 1, 1.0, 1.0, 1.0, None)


def test_cohort_is_cut_by_fill_order_not_finishing_order():
    rows = [_r(0, status="open"), *(_r(i) for i in range(1, 25))]
    look = fv.kill_looks(rows)[0]
    assert not look.formed and look.n_closed == 19
    assert fv.look_holders(rows, 20) == [rows[0]]


def test_an_unfilled_open_row_dated_by_the_last_fill_holds_the_look():
    rows = [*(_r(i) for i in range(20)), _r(5, symbol="LATE", status="open", fill=None)]
    assert not fv.kill_looks(rows)[0].formed
    assert fv.look_holders(rows, 20) == [rows[-1]]
    assert fv.kill_looks([*rows[:20], _r(30, status="open", fill=None)])[0].formed


@pytest.mark.parametrize(("net", "label"), [(0.0, fv.DEMOTE), (-0.5, fv.DEMOTE), (0.01, fv.HOLD)])
def test_kill_verdict_reads_the_formed_look_median(net, label):
    assert fv.kill_verdict(fv.kill_looks([_r(i, net=net) for i in range(20)]), registered_from=R)[0] == label


def test_look_two_supersedes_look_one():
    looks = fv.kill_looks([*(_r(i, net=1.0) for i in range(20)), *(_r(i, net=-1.0) for i in range(20, 40))])
    assert fv.kill_verdict(looks[:1], registered_from=R)[0] == fv.HOLD
    assert fv.kill_verdict(looks, registered_from=R)[0] == fv.DEMOTE


def test_kill_verdict_is_insufficient_before_a_look_and_unregistered_without_r():
    assert fv.kill_verdict(fv.kill_looks([_r(0)]), registered_from=R)[0] == fv.INSUFFICIENT
    assert fv.kill_verdict(fv.kill_looks([_r(i) for i in range(20)]), registered_from=None)[0] == fv.UNREGISTERED


# ------------------------------------------------------------------ analyst value
def test_a_cell_needs_both_arms_and_diffs_the_closed_means():
    rows = [_r(0, decision="enter", net=3.0), _r(0, symbol="B", decision="enter", net=1.0),
            _r(0, symbol="C", decision="no_action", net=0.5), _r(1, decision="enter"), _r(2)]
    assert fv.analyst_cells(rows) == [Cell(D0, "ins", 2, 1, True, 1.5)]


def test_a_cell_is_final_only_when_every_decided_row_is():
    rows = [_r(0, decision="enter"), _r(0, symbol="B", decision="no_action", status="open")]
    assert not fv.analyst_cells(rows)[0].final


def test_an_arm_without_a_closed_outcome_leaves_the_cell_unvalued_and_unaveraged():
    rows = [_r(0, decision="enter", net=2.0), _r(0, symbol="B", decision="no_action", status="void_ca"),
            _r(1, decision="enter", net=1.0), _r(1, symbol="B", decision="no_action", net=0.0)]
    cells = fv.analyst_cells(rows)
    assert [c.diff for c in cells] == [None, 1.0]
    assert fv.analyst_looks(cells, sizes=(2,)) == [CellLook(2, True, 2, 2, 1, 1.0)]


def _cells(diff, *, final=True, n=30):
    return [Cell(D0 + timedelta(days=i), "hi52", 1, 1, final, diff) for i in range(n)]


@pytest.mark.parametrize(("diff", "label"), [(0.0, fv.ADVISORY), (-0.1, fv.ADVISORY), (0.2, fv.KEEP)])
def test_analyst_verdict_reads_the_formed_look_mean(diff, label):
    assert fv.analyst_verdict(fv.analyst_looks(_cells(diff)), registered_from=R)[0] == label


def test_analyst_verdict_is_insufficient_until_thirty_final_cells_and_unregistered_without_r():
    held = [*_cells(1.0, n=29), *_cells(1.0, final=False, n=1)]
    assert fv.analyst_verdict(fv.analyst_looks(held), registered_from=R)[0] == fv.INSUFFICIENT
    assert fv.analyst_verdict(fv.analyst_looks(_cells(1.0)), registered_from=None)[0] == fv.UNREGISTERED


def test_the_registered_key_is_the_first_stamped_decision_and_other_keys_are_excluded():
    rows = [_r(1, decision="enter", key=("v2", "m")), _r(0, decision="no_action", key=(None, "m")),
            _r(2, decision="enter"), _r(0, symbol="X", strategy_id="brk20", decision="enter"), _r(3)]
    keys = fv.registered_keys(rows)
    assert keys == {"ins": ("v2", "m"), "brk20": ("v1", "m")}
    assert fv.split_by_key(rows, keys) == ([rows[0], rows[3]], [rows[1], rows[2]])


def test_cell_holders_are_the_open_decided_rows_of_the_first_cells():
    rows = [_r(0, decision="enter", status="open"), _r(0, symbol="B", decision="no_action"),
            _r(1, decision="enter", status="open"), _r(1, symbol="B", decision="no_action")]
    assert fv.cell_holders(rows, fv.analyst_cells(rows), 1) == [rows[0]]


# ------------------------------------------------------------------ diagnostics
def test_confidence_buckets_split_closed_nets_by_decision():
    b = fv.confidence_buckets([
        _r(0, decision="enter", conf=0.55, net=2.0), _r(1, decision="enter", conf=0.6, net=0.0),
        _r(2, decision="no_action", conf=0.5, net=-1.0), _r(3, decision="enter", conf=0.8, status="open")])
    assert b == {("<0.55", "no_action"): Stats(1, -1.0, -1.0, 0.0), ("0.55-0.65", "enter"): Stats(2, 1.0, 1.0, 0.5),
                 (">=0.75", "enter"): Stats(0, None, None, None)}


def test_c2_arms_compare_admitted_with_down_blocked_and_count_other_reasons_in_neither():
    blocked = [replace(_r(0, construct="rule_blocked", net=-2.0), reason="down"),
               replace(_r(1, construct="rule_blocked", net=5.0), reason="undefined"),
               replace(_r(2, construct="rule_blocked", net=5.0), reason="index_bar_missing")]
    arms = fv.c2_arms([_r(0, net=1.0), _r(1, status="open")], blocked)
    assert arms == C2Arms(Stats(1, 1.0, 1.0, 1.0), Stats(1, -2.0, -2.0, 0.0), 2)


# ------------------------------------------------------------------ loader
def _slot(conn, d, symbol, strategy_id="ins", *, decision=None, call_id=None):
    conn.execute("INSERT INTO prescreen_day_slots (d, symbol, strategy_id, published_at, analyst_decision, "
                 "analyst_confidence, analyst_call_id) VALUES (?, ?, ?, 't', ?, 0.6, ?)",
                 (d.isoformat(), symbol, strategy_id, decision, call_id))


def _outcome(conn, d, symbol, construct, status, *, strategy_id="ins", fill_d=None, exit_d=None, net=None):
    conn.execute("INSERT INTO signal_outcomes (d, symbol, strategy_id, construct, status, fill_d, exit_d, net_t5, "
                 "net_t10, net_t20, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 't')",
                 (d.isoformat(), symbol, strategy_id, construct, status, fill_d and fill_d.isoformat(),
                  exit_d and exit_d.isoformat(), net, net, net))


def _day(i):
    return D0 + timedelta(days=i)


def _load(conn, construct="rule", as_of=None, strategies=("ins",)):
    as_of = as_of or _day(9)
    return fv.load_rows(conn, strategies=strategies, construct=construct, start=D0, as_of=as_of)


def test_load_rows_joins_the_outcome_and_the_call_and_an_unscored_row_reads_open(conn):
    conn.execute("INSERT INTO agent_calls (call_id, agent_id, model, ok, at, prompt_version) "
                 "VALUES ('c1', 'intraday', 'm', 1, 't', 'v1')")
    _slot(conn, _day(0), "A", decision="enter", call_id="c1")
    _outcome(conn, _day(0), "A", "next_open", "closed", fill_d=_day(1), exit_d=_day(5), net=2.0)
    _slot(conn, _day(0), "B")
    _slot(conn, _day(10), "C")
    assert _load(conn, "next_open") == [
        SignalRow(_day(0), "A", "ins", "next_open", "closed", _day(1), _day(5), 2.0, 2.0, 2.0, None,
                  "enter", 0.6, "v1", "m"),
        SignalRow(_day(0), "B", "ins", "next_open", "open", None, None, None, None, None, None,
                  None, 0.6, None, None)]


def test_load_rows_reads_a_later_fill_as_unfilled_and_a_later_exit_as_open(conn):
    _slot(conn, _day(0), "A")
    _outcome(conn, _day(0), "A", "rule", "closed", fill_d=_day(0), exit_d=_day(5), net=2.0)
    _slot(conn, _day(0), "B")
    _outcome(conn, _day(0), "B", "rule", "closed", fill_d=_day(4), exit_d=_day(8), net=1.0)
    a, b = _load(conn, as_of=_day(3))
    assert (a.status, a.fill_d, a.exit_d, a.net_t20) == ("open", _day(0), None, None)
    assert (b.status, b.fill_d, b.exit_d, b.net_t20) == ("open", None, None, None)


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


# ------------------------------------------------------------------ CLI
@pytest.fixture(scope="module")
def cli():
    spec = importlib.util.spec_from_file_location("mt_forward_verdicts", _SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.mark.parametrize("rule", ["brk20", "ins", "analyst"])
def test_cli_prints_the_verdict_line_first(cli, conn, db_path, capsys, monkeypatch, rule):
    monkeypatch.setattr(fv, "REGISTERED_FROM", R)
    assert cli.main(["--rule", rule, "--state-db", db_path, "--as-of", "2026-07-31"]) == 0
    assert capsys.readouterr().out.startswith(f"VERDICT {rule}: {fv.INSUFFICIENT}")


def test_cli_names_a_filled_row_holding_the_look(cli, conn, db_path, capsys):
    _slot(conn, D0, "HOLE")
    _outcome(conn, D0, "HOLE", "rule", "open", fill_d=D0)
    assert cli.main(["--rule", "ins", "--state-db", db_path, "--as-of", "2026-07-31"]) == 0
    out = capsys.readouterr().out
    assert out.startswith(f"VERDICT ins: {fv.UNREGISTERED}")
    assert re.search(r"HOLE +d=2026-06-01 open +fill=2026-06-01 .*T\+20 2026-06-\d\d, [1-9]\d* sessions past", out)
