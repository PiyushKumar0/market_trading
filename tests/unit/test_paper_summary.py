"""``paper_summary``: the epoch predicate, curve, book and totals behind GET /paper."""

from __future__ import annotations

import json
from decimal import Decimal

from engine.ops.paper_control import PaperControl
from engine.ops.paper_summary import PaperView, paper_summary
from tests.unit.test_scorecard import seed_paper

_VIEW = PaperView(
    prep_ready=lambda: True, entry_guard=lambda: None,
    mark={"AAA": Decimal("110.5")}.get, counters=lambda: {"voids": 1},
)
# at, equity, realized_pnl, open_mtm
_SNAPSHOTS = [
    ("2026-10-04T15:00:00+05:30", "19900.00", "-100.00", "0"),     # before the epoch
    ("2026-10-05T10:00:00+05:30", "20010.00", "0", "10.00"),
    ("2026-10-05T15:00:00+05:30", "20040.00", "0", "40.00"),
    ("2026-10-06T14:00:00+05:30", "20150.50", "100.50", "50.00"),
]


def _seed(conn) -> None:
    seed_paper(conn)
    for pid, symbol, side, qty, avg, opened in [
        ("p0", "AAA", "BUY", 10, "100.00", "2026-10-06T10:00:00+05:30"),
        ("p1", "BBB", "SELL", 5, "200.00", "2026-10-06T11:00:00+05:30"),
        ("p4", "OLD", "BUY", 1, "50.00", "2026-10-01T10:00:00+05:30"),     # opened before the epoch
    ]:
        conn.execute("UPDATE positions SET symbol=?, side=?, qty=?, avg_entry=?, opened_at=? WHERE position_id=?",
                     (symbol, side, qty, avg, opened, pid))
    for at, equity, realized, mtm in _SNAPSHOTS:
        conn.execute("INSERT INTO paper_equity_snapshots (at, equity, realized_pnl, open_mtm) VALUES (?, ?, ?, ?)",
                     (at, equity, realized, mtm))
    conn.execute(
        "INSERT INTO proposals (proposal_id, agent_id, action, payload, inputs_digest, created_at) "
        "VALUES ('pr1', 'a', 'enter', ?, 'd', '2026-10-06T09:30:00+05:30')",
        (json.dumps({"tradingsymbol": "CCC", "strategy_id": "brk20"}),),
    )
    for oid, state, proposal in [("o1", "ACKED", "pr1"), ("o2", "FILLED", None)]:
        conn.execute(
            "INSERT INTO orders (order_id, proposal_id, role, is_paper, state, product, side, qty, created_at) "
            "VALUES (?, ?, 'entry', 1, ?, 'CNC', 'BUY', 3, '2026-10-06T09:31:00+05:30')", (oid, proposal, state))


def _summary(conn, clock, view=_VIEW, subsystem_enabled=True) -> dict:
    return paper_summary(conn, PaperControl(conn, clock), view=view, subsystem_enabled=subsystem_enabled)


def test_summary_counts_only_the_current_epoch(conn, clock) -> None:
    _seed(conn)
    s = _summary(conn, clock)
    assert s["built"] and s["prep_ready"] is True and s["entry_guard"] is None
    assert s["equity"] == {
        "at": "2026-10-06T14:00:00+05:30", "equity": "20150.50", "realized_pnl": "100.50", "open_mtm": "50.00",
        "day_mtm": None, "positions_open": None, "pnl": "150.50", "capital_base": "20000.00",
        "pnl_pct": "0.75", "realized_pnl_pct": "0.50", "open_mtm_pct": "0.25", "day_mtm_pct": None,
    }
    assert [(c["d"], c["equity"]) for c in s["curve"]] == [("2026-10-05", "20040.00"), ("2026-10-06", "20150.50")]
    assert [(p["symbol"], p["mark"], p["unrealized"]) for p in s["positions"]] == [
        ("AAA", "110.5", "105.00"), ("BBB", None, None)]
    assert [p["unrealized_pct"] for p in s["positions"]] == ["10.50", None]        # 105 / (10 x 100)
    assert s["unmarked"] == ["BBB"]
    assert [(o["order_id"], o["symbol"], o["strategy_id"]) for o in s["orders"]] == [("o1", "CCC", "brk20")]
    assert [c["net_pnl"] for c in s["closed"][:2]] == ["999.00", "-40.25"]       # newest first
    assert sorted(c["net_pnl"] for c in s["closed"]) == ["-10.00", "-40.25", "100.50", "999.00"]
    assert s["totals"] == {"closed": 3, "wins": 1, "voids": 1, "void_net": "999.00", "void_net_pct": "5.00"}
    assert s["counters"] == {"voids": 1}


def test_closed_rows_carry_the_net_percent_of_their_own_cost_basis(conn, clock) -> None:
    _seed(conn)
    conn.execute("UPDATE learning_ledger SET qty = 10, entry_px = '100' WHERE entry_id = 'e1'")   # net -40.25
    by_net = {c["net_pnl"]: c["net_pnl_pct"] for c in _summary(conn, clock)["closed"]}
    assert by_net["-40.25"] == "-4.03"                        # -40.25 / 1000, half-up away from zero
    assert by_net["100.50"] is None and by_net["999.00"] is None      # NULL qty / entry_px: no basis


def test_no_equity_row_leaves_the_void_percent_empty(conn, clock) -> None:
    _seed(conn)
    conn.execute("DELETE FROM paper_equity_snapshots")
    s = _summary(conn, clock)
    assert s["equity"] is None
    assert s["totals"]["void_net"] == "999.00" and s["totals"]["void_net_pct"] is None


def test_first_epoch_counts_all_paper_history(conn, clock) -> None:
    _seed(conn)
    conn.execute("UPDATE paper_state SET epoch_started_at = NULL")
    s = _summary(conn, clock)
    assert [c["d"] for c in s["curve"]] == ["2026-10-04", "2026-10-05", "2026-10-06"]
    assert [p["symbol"] for p in s["positions"]] == ["OLD", "AAA", "BBB"]
    assert s["totals"] == {"closed": 4, "wins": 2, "voids": 1, "void_net": "999.00", "void_net_pct": "5.00"}


def test_positions_carry_the_exit_policy_and_the_summary_the_no_stop_list(conn, clock) -> None:
    _seed(conn)
    conn.execute("UPDATE positions SET exit_policy = 'time_only', stop = NULL, target = NULL, "
                 "protection_state = 'TIME_EXIT_ONLY' WHERE position_id = 'p0'")
    s = paper_summary(conn, PaperControl(conn, clock), view=_VIEW, subsystem_enabled=True,
                      no_stop_strategies=("hi52", "brk20"))
    assert [(p["symbol"], p["exit_policy"], p["stop"]) for p in s["positions"]][0] == ("AAA", "time_only", None)
    assert [p["exit_policy"] for p in s["positions"]][1:] == [None]
    assert s["no_stop_strategies"] == ["hi52", "brk20"]
    assert _summary(conn, clock)["no_stop_strategies"] == []


# at, equity, realized_pnl, open_mtm, day_mtm: stored at a 40,000 base, P&L +100 then +200
_BASE_40K = [
    ("2026-10-05T15:00:00+05:30", "40100.00", "60.00", "40.00", "100.00"),
    ("2026-10-06T15:00:00+05:30", "40200.00", "150.00", "50.00", "30.00"),
]


def _seed_40k(conn, rows=_BASE_40K) -> None:
    seed_paper(conn)
    for row in rows:
        conn.execute("INSERT INTO paper_equity_snapshots (at, equity, realized_pnl, open_mtm, day_mtm) "
                     "VALUES (?, ?, ?, ?, ?)", row)


def _live(conn, clock, capital_base) -> dict:
    return paper_summary(conn, PaperControl(conn, clock), view=_VIEW, subsystem_enabled=True,
                         capital_base=capital_base)


def test_the_live_base_reprices_the_equity_and_the_curve_with_no_cliff(conn, clock) -> None:
    """O19 §7: history in P&L space. A 40k → 60k change shows every point at today's base."""
    _seed_40k(conn)
    s = _live(conn, clock, Decimal("60000"))
    assert s["equity"] == {
        "at": "2026-10-06T15:00:00+05:30", "equity": "60200.00", "realized_pnl": "150.00", "open_mtm": "50.00",
        "day_mtm": "30.00", "positions_open": None, "pnl": "200.00", "capital_base": "60000.00",
        "pnl_pct": "0.33", "realized_pnl_pct": "0.25", "open_mtm_pct": "0.08", "day_mtm_pct": "0.05",
    }
    assert [c["equity"] for c in s["curve"]] == ["60100.00", "60200.00"]
    assert s["totals"]["void_net"] == "999.00" and s["totals"]["void_net_pct"] == "1.67"   # 999 / 60,000


def test_no_live_base_is_exactly_the_stored_view(conn, clock) -> None:
    _seed_40k(conn)
    s = _live(conn, clock, None)
    assert json.dumps(s, sort_keys=True) == json.dumps(_summary(conn, clock), sort_keys=True)
    assert s["equity"]["capital_base"] == "40000.00" and s["equity"]["equity"] == "40200.00"
    assert [c["equity"] for c in s["curve"]] == ["40100.00", "40200.00"]
    assert s["equity"]["pnl_pct"] == "0.50" and s["totals"]["void_net_pct"] == "2.50"     # 999 / 40,000


def test_a_null_pnl_row_falls_back_to_its_stored_equity(conn, clock) -> None:
    _seed_40k(conn, [
        ("2026-10-05T15:00:00+05:30", "40100.00", "60.00", None, None),
        ("2026-10-06T15:00:00+05:30", "40200.00", "", "50.00", None),
    ])
    s = _live(conn, clock, Decimal("60000"))
    assert [c["equity"] for c in s["curve"]] == ["40100.00", "40200.00"]
    # The latest row too: its stored equity, and its P&L is equity − the live base (O19 §3.1).
    assert (s["equity"]["equity"], s["equity"]["capital_base"], s["equity"]["pnl"]) == (
        "40200.00", "60000.00", "-19800.00")


def test_the_void_percent_uses_the_live_base_even_before_the_first_snapshot(conn, clock) -> None:
    _seed_40k(conn, [])
    s = _live(conn, clock, Decimal("60000"))
    assert (s["equity"], s["curve"]) == (None, [])
    assert s["totals"]["void_net_pct"] == "1.67"


def test_not_built_with_nothing_stored(conn, clock) -> None:
    s = _summary(conn, clock, view=None, subsystem_enabled=False)
    assert s["built"] is False and s["prep_ready"] is None and s["entry_guard"] == "not built"
    assert (s["equity"], s["counters"], s["subsystem_enabled"]) == (None, None, False)
    assert [s[k] for k in ("unmarked", "halts", "curve", "positions", "orders", "closed")] == [[]] * 6
    assert s["totals"] == {"closed": 0, "wins": 0, "voids": 0, "void_net": None, "void_net_pct": None}
