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
    }
    assert [(c["d"], c["equity"]) for c in s["curve"]] == [("2026-10-05", "20040.00"), ("2026-10-06", "20150.50")]
    assert [(p["symbol"], p["mark"], p["unrealized"]) for p in s["positions"]] == [
        ("AAA", "110.5", "105.00"), ("BBB", None, None)]
    assert s["unmarked"] == ["BBB"]
    assert [(o["order_id"], o["symbol"], o["strategy_id"]) for o in s["orders"]] == [("o1", "CCC", "brk20")]
    assert [c["net_pnl"] for c in s["closed"][:2]] == ["999.00", "-40.25"]       # newest first
    assert sorted(c["net_pnl"] for c in s["closed"]) == ["-10.00", "-40.25", "100.50", "999.00"]
    assert s["totals"] == {"closed": 3, "wins": 1, "voids": 1, "void_net": "999.00"}
    assert s["counters"] == {"voids": 1}


def test_first_epoch_counts_all_paper_history(conn, clock) -> None:
    _seed(conn)
    conn.execute("UPDATE paper_state SET epoch_started_at = NULL")
    s = _summary(conn, clock)
    assert [c["d"] for c in s["curve"]] == ["2026-10-04", "2026-10-05", "2026-10-06"]
    assert [p["symbol"] for p in s["positions"]] == ["OLD", "AAA", "BBB"]
    assert s["totals"] == {"closed": 4, "wins": 2, "voids": 1, "void_net": "999.00"}


def test_not_built_with_nothing_stored(conn, clock) -> None:
    s = _summary(conn, clock, view=None, subsystem_enabled=False)
    assert s["built"] is False and s["prep_ready"] is None and s["entry_guard"] == "not built"
    assert (s["equity"], s["counters"], s["subsystem_enabled"]) == (None, None, False)
    assert [s[k] for k in ("unmarked", "halts", "curve", "positions", "orders", "closed")] == [[]] * 6
    assert s["totals"] == {"closed": 0, "wins": 0, "voids": 0, "void_net": None}
