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


def test_scorecard_without_paper_activity_has_zeroed_paper_halves(conn) -> None:
    assert scorecard(conn)["strategies"] == {}
    _seed(conn)
    assert scorecard(conn)["strategies"]["hi52"]["paper"] == {
        "closed": 0, "hit_rate": None, "net": None, "open": 0,
    }


# is_paper, strategy, outcome_label, net_pnl, closed_at
_LEDGER = [
    (1, "hi52", "win", "100.50", "2026-10-06T15:00:00+05:30"),
    (1, "hi52", "loss", "-40.25", "2026-10-06T15:05:00+05:30"),
    (1, "hi52", "void", "999.00", "2026-10-06T15:10:00+05:30"),
    (1, "hi52", "win", "500.00", "2026-10-01T15:00:00+05:30"),      # before the epoch
    (0, "hi52", "win", "700.00", "2026-10-06T15:00:00+05:30"),      # a real trade
    (1, "paperonly", "loss", "-10.00", "2026-10-06T15:00:00+05:30"),
]
# state, is_paper, origin, strategy
_POSITIONS = [
    ("OPEN", 1, "platform", "hi52"), ("PENDING_EXIT", 1, "platform", "hi52"),
    ("CLOSED", 1, "platform", "hi52"), ("OPEN", 0, "recommended", "hi52"),
    ("OPEN", 1, "platform", None),
]


def seed_paper(conn) -> None:
    conn.execute("INSERT INTO paper_state (id, epoch_started_at) VALUES (1, '2026-10-05T09:00:00+05:30')")
    for i, (paper, sid, label, net, closed) in enumerate(_LEDGER):
        conn.execute(
            "INSERT INTO learning_ledger (entry_id, is_paper, strategy_id, outcome_label, net_pnl, closed_at) "
            "VALUES (?, ?, ?, ?, ?, ?)", (f"e{i}", paper, sid, label, net, closed),
        )
    for i, (state, paper, origin, sid) in enumerate(_POSITIONS):
        conn.execute(
            "INSERT INTO positions (position_id, symbol, state, is_paper, origin, strategy_id) "
            "VALUES (?, 'X', ?, ?, ?, ?)", (f"p{i}", state, paper, origin, sid),
        )


def test_paper_half_counts_this_epochs_paper_trades_and_excludes_voids(conn) -> None:
    _seed(conn)
    seed_paper(conn)
    strategies = scorecard(conn)["strategies"]
    assert strategies["hi52"]["paper"] == {"closed": 2, "hit_rate": 0.5, "net": 60.25, "open": 2}
    assert strategies["paperonly"]["paper"] == {"closed": 1, "hit_rate": 0.0, "net": -10.0, "open": 0}
    assert strategies["unattributed"]["paper"] == {"closed": 0, "hit_rate": None, "net": None, "open": 1}
    assert strategies["paperonly"]["recs"]["n"] == 0
    assert strategies["hi52"]["recs"]["n"] == 5          # paper rows never reach the recs half
