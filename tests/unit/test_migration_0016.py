"""0016_paper_book: new tables/columns, entry-order unique index, upgrade from a populated 0015 db."""

from __future__ import annotations

import sqlite3

import pytest

from engine.core import migrations
from engine.core.db import connect
from engine.core.migrations import apply_migrations


@pytest.fixture
def conn(db_path):
    c = connect(db_path)
    apply_migrations(c)
    yield c
    c.close()


def _order(conn, oid, verdict_id, role="entry"):
    conn.execute(
        "INSERT INTO orders (order_id, verdict_id, role, state, product) VALUES (?, ?, ?, 'NEW', 'CNC')",
        (oid, verdict_id, role),
    )


def test_paper_state_single_row_and_defaults(conn):
    conn.execute("INSERT INTO paper_state (id) VALUES (1)")
    row = conn.execute("SELECT * FROM paper_state").fetchone()
    assert row["enabled"] == 0 and row["changed_at"] is None and row["reset_requested_at"] is None
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("INSERT INTO paper_state (id) VALUES (2)")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("INSERT INTO paper_state (id) VALUES (1)")


def test_paper_equity_snapshots_mirrors_equity_snapshots(conn):
    def cols(t):
        return [(r["name"], r["type"], r["notnull"], r["pk"]) for r in conn.execute(f"PRAGMA table_info({t})")]
    assert cols("paper_equity_snapshots") == cols("equity_snapshots")
    ins = "INSERT INTO paper_equity_snapshots (at, equity) VALUES (?, ?)"
    conn.execute(ins, ("t1", "100000"))
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(ins, ("t1", "1"))
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(ins, ("t2", None))


def test_paper_halts_pk_and_not_null(conn):
    ins = "INSERT INTO paper_halts (cause, rung, set_at) VALUES (?, ?, ?)"
    conn.execute(ins, ("dd", "halt", "t"))
    row = conn.execute("SELECT latched, cleared_at FROM paper_halts").fetchone()
    assert row["latched"] == 0 and row["cleared_at"] is None
    for args in (("dd", "halt", "t"), ("x", None, "t"), ("y", "halt", None)):
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(ins, args)


def test_entry_order_unique_per_verdict(conn):
    _order(conn, "o1", "v1")
    with pytest.raises(sqlite3.IntegrityError):
        _order(conn, "o2", "v1")
    _order(conn, "o3", "v1", role="protective_sl")
    _order(conn, "o4", "v1", role="exit")
    _order(conn, "o5", "v2")


def test_new_columns_and_defaults(conn):
    conn.execute("INSERT INTO gtts (gtt_id, position_id, state) VALUES (900001, 'p', 'active')")
    conn.execute("INSERT INTO positions (position_id, symbol, state, origin) VALUES ('p', 'X', 'OPEN', 'platform')")
    g = conn.execute("SELECT * FROM gtts").fetchone()
    assert g["is_paper"] == 0
    assert all(g[c] is None for c in ("symbol", "side", "product", "qty", "stop_limit", "target_limit",
                                      "last_price", "created_at"))
    p = conn.execute("SELECT * FROM positions").fetchone()
    assert p["strategy_id"] is None and p["exit_session"] is None and p["close_basis"] is None


def test_upgrade_from_populated_0015(db_path, monkeypatch):
    c = connect(db_path)
    try:
        full = migrations.discover()
        monkeypatch.setattr(migrations, "discover", lambda: [p for p in full if p.name < "0016"])
        assert "0016_paper_book.sql" not in apply_migrations(c)
        c.execute("INSERT INTO proposals (proposal_id, agent_id, action, payload, inputs_digest, created_at) "
                  "VALUES ('pr', 'a', 'enter', '{}', 'd', 't')")
        c.execute("INSERT INTO verdicts (verdict_id, proposal_id, verdict, payload, evaluated_at) "
                  "VALUES ('v1', 'pr', 'approve', '{}', 't')")
        _order(c, "o1", "v1")
        c.execute("INSERT INTO positions (position_id, symbol, state, origin, stop) "
                  "VALUES ('p', 'X', 'OPEN', 'platform', '95')")
        c.execute("INSERT INTO gtts (gtt_id, position_id, state, trigger_low, trigger_high) "
                  "VALUES (7, 'p', 'active', '95', '110')")
        c.commit()

        monkeypatch.setattr(migrations, "discover", lambda: full)
        assert apply_migrations(c) == ["0016_paper_book.sql"]

        assert c.execute("SELECT verdict, is_paper FROM verdicts").fetchone()[:] == ("approve", 0)
        assert c.execute("SELECT order_id, role FROM orders").fetchone()[:] == ("o1", "entry")
        assert c.execute("SELECT stop, strategy_id, exit_session, close_basis FROM positions"
                         ).fetchone()[:] == ("95", None, None, None)
        assert c.execute("SELECT gtt_id, trigger_low, trigger_high, is_paper, symbol FROM gtts"
                         ).fetchone()[:] == (7, "95", "110", 0, None)
    finally:
        c.close()


def test_pre_0016_queries_still_run(conn):
    conn.execute("INSERT INTO gtts (gtt_id, position_id, state, trigger_low, trigger_high, last_verified_at, "
                 "ex_date_adjusted_for) VALUES (1, 'p', 'active', '1', '2', 't', NULL)")
    conn.execute("INSERT INTO positions (position_id, symbol, state, origin) VALUES ('p', 'X', 'OPEN', 'external')")
    _order(conn, "o1", "v1")
    assert conn.execute("SELECT gtt_id, position_id, state, trigger_low, trigger_high, last_verified_at, "
                        "ex_date_adjusted_for FROM gtts").fetchone()
    assert conn.execute("SELECT position_id, symbol, is_paper, close_reason FROM positions").fetchone()
    assert conn.execute("SELECT order_id, verdict_id, role, is_paper FROM orders").fetchone()
    assert conn.execute("SELECT verdict_id, proposal_id, verdict, payload, evaluated_at FROM verdicts").fetchall() == []
