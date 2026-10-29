"""Hindsight scorecard aggregation (plan Q2.4)."""

from __future__ import annotations

import json

import pytest

from engine.ops.scorecard import LABEL, scorecard, symbol_history_line

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
        "closed": 0, "hit_rate": None, "net": None, "net_pct": None, "open": 0,
    }


# is_paper, strategy, outcome_label, net_pnl, closed_at
_LEDGER = [
    (1, "hi52", "win", "100.50", "2026-10-06T15:00:00+05:30"),
    (1, "hi52", "loss", "-40.25", "2026-10-06T15:05:00+05:30"),
    (1, "hi52", "void", "999.00", "2026-10-06T15:10:00+05:30"),
    (1, "hi52", "win", "500.00", "2026-10-01T15:00:00+05:30"),      # before the epoch: still on the card
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


def test_label_is_the_literal_em_dash_text() -> None:
    assert LABEL == "hindsight on official bars — not platform equity"
    assert "Ã" not in LABEL


def test_paper_half_keeps_pre_epoch_paper_trades_and_excludes_voids(conn) -> None:
    _seed(conn)
    seed_paper(conn)
    strategies = scorecard(conn)["strategies"]
    assert strategies["hi52"]["paper"] == {
        "closed": 3, "hit_rate": 2 / 3, "net": 560.25, "net_pct": None, "open": 2}      # no qty / entry_px stored
    assert strategies["paperonly"]["paper"] == {
        "closed": 1, "hit_rate": 0.0, "net": -10.0, "net_pct": None, "open": 0}
    assert strategies["unattributed"]["paper"] == {
        "closed": 0, "hit_rate": None, "net": None, "net_pct": None, "open": 1}
    assert strategies["paperonly"]["recs"]["n"] == 0
    assert strategies["hi52"]["recs"]["n"] == 5          # paper rows never reach the recs half


def _rec(conn, rec_id, day, *, symbol="BHEL", kind="entry", strategy="hi52", payload_strategy=None,
         outcome_strategy=None, status=None, delivered=True, **outcome):
    """``strategy`` is the ledger's; ``payload_strategy`` outranks it, ``outcome_strategy`` both."""
    payload = {"kind": kind, "instrument": symbol, "strategy_id": payload_strategy}
    conn.execute(
        "INSERT INTO recommendations (rec_id, payload, delivered_at) VALUES (?, ?, ?)",
        (rec_id, json.dumps(payload), f"{day}T10:00:00+05:30" if delivered else None),
    )
    conn.execute("INSERT INTO learning_ledger (entry_id, rec_id, strategy_id) VALUES (?, ?, ?)",
                 (f"e-{rec_id}", rec_id, strategy))
    if status:
        conn.execute("INSERT INTO rec_outcomes (rec_id, strategy_id, status, net_pct, exit_reason, updated_at) "
                     "VALUES (?, ?, ?, ?, ?, 'x')",
                     (rec_id, outcome_strategy, status, outcome.get("net"), outcome.get("reason")))


def test_history_line_lists_earlier_entry_recs_newest_first(conn) -> None:
    _rec(conn, "a", "2026-08-27", strategy="ledger", payload_strategy="hi52", status="open")
    _rec(conn, "b", "2026-09-10", strategy="ledger", payload_strategy="payload", outcome_strategy="brk20",
         status="closed", net=-4.1234, reason="stop")
    _rec(conn, "x", "2026-09-11", kind="exit", status="closed", net=9.0, reason="time")
    _rec(conn, "o", "2026-09-12", symbol="TCS", status="open")
    _rec(conn, "u", "2026-09-13", status="open", delivered=False)
    assert symbol_history_line(conn, "BHEL") == (
        "same-ticker history (hindsight): 2026-09-10 brk20 closed -4.12% (stop); 2026-08-27 hi52 open")


def test_history_line_caps_at_the_limit(conn) -> None:
    for i in range(1, 5):
        _rec(conn, f"r{i}", f"2026-09-0{i}", status="open")
    assert symbol_history_line(conn, "BHEL").count("hi52 open") == 3
    assert "2026-09-01" not in symbol_history_line(conn, "BHEL")


@pytest.mark.parametrize(("status", "word"), [
    ("unfilled", "did not fill"), ("void_ca", "voided (corporate action)"),
    ("unscorable", "not scored"), (None, "not scored"),
])
def test_history_line_status_words(conn, status, word) -> None:
    _rec(conn, "a", "2026-09-10", status=status)
    assert symbol_history_line(conn, "BHEL").endswith(f"2026-09-10 hi52 {word}")


def test_history_line_without_earlier_recs(conn) -> None:
    assert symbol_history_line(conn, "BHEL") == "same-ticker history: no earlier recommendation on BHEL"


def test_paper_net_pct_is_net_over_the_summed_cost_basis_never_a_partial_one(conn) -> None:
    _seed(conn)
    seed_paper(conn)
    for entry_id, qty, px in [("e0", 10, "100"), ("e1", 5, "200"), ("e3", 2, "50")]:    # bases 1000, 1000, 100
        conn.execute("UPDATE learning_ledger SET qty = ?, entry_px = ? WHERE entry_id = ?", (qty, px, entry_id))
    paper = scorecard(conn)["strategies"]["hi52"]["paper"]
    assert paper["net"] == 560.25 and paper["net_pct"] == 26.68      # 560.25 / 2100 = 26.678...
    conn.execute("UPDATE learning_ledger SET entry_px = NULL WHERE entry_id = 'e3'")
    assert scorecard(conn)["strategies"]["hi52"]["paper"]["net_pct"] is None
