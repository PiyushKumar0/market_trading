"""Hindsight scorecard aggregation (plan Q2.4)."""

from __future__ import annotations

import json

from engine.ops.scorecard import LABEL, scorecard

# rec_id, kind, human_action, skip_reason, strategy, status, fill_basis, net, net_t20, excess
_ROWS = [
    ("a", "entry", "taken", None, "hi52", "closed", "1m_post_delivery", 2.0, 3.0, 1.0),
    ("b", "entry", "dismissed", "price", "hi52", "closed", "daily", -1.0, 0.5, -2.0),
    ("c", "entry", "expired", None, "hi52", "open", "daily", None, None, None),
    ("d", "entry", None, None, "hi52", "unfilled", None, None, None, None),
    ("e", "entry", "closed", None, "hi52", "unscorable", None, None, None, None),
    ("f", "entry", "taken", None, "hi52", "void_ca", "daily", 9.0, 9.0, 9.0),
    ("g", "exit", "taken", None, "hi52", "closed", "daily", 9.0, 9.0, 9.0),
    ("h", "entry", "dismissed", "trust", None, None, None, None, None, None),
]


def _seed(conn) -> None:
    for rec_id, kind, action, skip, sid, status, basis, net, t20, exc in _ROWS:
        conn.execute(
            "INSERT INTO recommendations (rec_id, payload, delivered_at, human_action, skip_reason) "
            "VALUES (?, ?, '2026-10-01T10:00:00+05:30', ?, ?)",
            (rec_id, json.dumps({"kind": kind}), action, skip),
        )
        if status:
            conn.execute(
                "INSERT INTO rec_outcomes (rec_id, strategy_id, status, fill_basis, net_pct, net_t20, "
                "excess_pct, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, 'x')",
                (rec_id, sid, status, basis, net, t20, exc),
            )


def test_scorecard_aggregates_per_strategy_excluding_void_and_exit_recs(conn) -> None:
    _seed(conn)
    out = scorecard(conn)
    assert out["label"] == LABEL
    assert set(out["bench"]) == {"time", "intrasession"}
    assert list(out["strategies"]) == ["hi52", "unattributed"]
    hi52 = out["strategies"]["hi52"]["recs"]
    assert hi52 == {
        "n": 5, "filled": 3, "closed": 2, "hit_rate": 0.5, "median_net": 0.5, "mean_net": 0.5,
        "net_t20": 1.75, "mean_excess": -0.5,
        "actions": {"taken": 2, "dismissed": 1, "expired": 1, "open": 1},
        "skip_reasons": {"price": 1}, "daily_basis": 2, "unscorable": 1,
    }
    un = out["strategies"]["unattributed"]["recs"]
    assert (un["n"], un["closed"], un["hit_rate"], un["skip_reasons"]) == (1, 0, None, {"trust": 1})


def test_unscored_recs_take_the_payload_then_ledger_strategy(conn) -> None:
    for rec_id, payload in (("p", {"kind": "entry", "strategy_id": "cat"}), ("l", {"kind": "entry"}),
                            ("n", {"kind": "entry"})):
        conn.execute(
            "INSERT INTO recommendations (rec_id, payload, delivered_at) VALUES (?, ?, 'x')",
            (rec_id, json.dumps(payload)),
        )
    conn.execute("INSERT INTO learning_ledger (entry_id, rec_id, strategy_id) VALUES ('e1', 'l', 'brk20')")
    assert {k: v["recs"]["n"] for k, v in scorecard(conn)["strategies"].items()} == {
        "brk20": 1, "cat": 1, "unattributed": 1,
    }


def test_scorecard_empty_and_paper_stub_shape(conn) -> None:
    assert scorecard(conn)["strategies"] == {}
    _seed(conn)
    assert scorecard(conn)["strategies"]["hi52"]["paper"] == {
        "closed": 0, "hit_rate": None, "net": None, "open": 0,
    }
