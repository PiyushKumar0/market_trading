"""0015_rec_feedback: new tables/columns, upgrade from a populated 0014 db, pre-0015 queries still run."""

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


def _position(conn, pid, state):
    conn.execute(
        "INSERT INTO positions (position_id, symbol, state, origin) VALUES (?, 'X', ?, 'recommended')",
        (pid, state),
    )


def test_rec_outcomes_upsert_and_checks(conn):
    sql = ("INSERT INTO rec_outcomes (rec_id, status, fill_basis, updated_at) VALUES ('r1', ?, ?, 't') "
           "ON CONFLICT(rec_id) DO UPDATE SET status=excluded.status")
    conn.execute(sql, ("open", "daily"))
    conn.execute(sql, ("closed", "daily"))
    assert conn.execute("SELECT status FROM rec_outcomes").fetchall()[0]["status"] == "closed"
    for status, basis in (("bogus", "daily"), ("open", "bogus"), (None, None)):
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(sql.replace("r1", "r2"), (status, basis))


def test_universe_ew_returns_upsert_and_not_null(conn):
    ins = ("INSERT INTO universe_ew_returns (d, ret, n) VALUES ('2026-10-01', ?, ?) "
           "ON CONFLICT(d) DO UPDATE SET ret=excluded.ret, n=excluded.n")
    conn.execute(ins, (0.5, 10))
    conn.execute(ins, (None, 3))
    assert [tuple(r) for r in conn.execute("SELECT d, ret, n FROM universe_ew_returns")] == [
        ("2026-10-01", None, 3)]
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(ins, (0.1, None))


def test_new_columns_defaults_and_checks(conn):
    _position(conn, "p1", "PENDING_ENTRY")
    conn.execute("INSERT INTO recommendations (rec_id) VALUES ('r1')")
    p = conn.execute("SELECT * FROM positions").fetchone()
    r = conn.execute("SELECT * FROM recommendations").fetchone()
    assert p["protection_reminders"] == 0 and p["owner_protected_at"] is None
    assert r["skip_reason"] is None and r["skip_reason_at"] is None and r["reminder_sent_at"] is None
    conn.execute("UPDATE recommendations SET skip_reason='trust'")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("UPDATE recommendations SET skip_reason='bogus'")


def test_notifications_dedupe_key_unique_when_set(conn):
    ins = ("INSERT INTO notifications (notification_id, created_at, title, body, dedupe_key) "
           "VALUES (?, 't', 'T', 'B', ?)")
    conn.execute(ins, ("n1", None))
    conn.execute(ins, ("n2", None))
    conn.execute(ins, ("n3", "k"))
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(ins, ("n4", "k"))


def test_upgrade_from_populated_0014(db_path, monkeypatch):
    c = connect(db_path)
    try:
        full = migrations.discover()
        monkeypatch.setattr(migrations, "discover", lambda: [p for p in full if p.name < "0015"])
        assert "0015_rec_feedback.sql" not in apply_migrations(c)
        _position(c, "open", "OPEN")
        _position(c, "closed", "CLOSED")
        c.execute("INSERT INTO recommendations (rec_id, payload) VALUES ('r1', '{}')")
        c.execute("INSERT INTO notifications (notification_id, created_at, title, body) "
                  "VALUES ('n1', 't', 'T', 'B')")
        c.commit()

        monkeypatch.setattr(migrations, "discover", lambda: [p for p in full if p.name < "0016"])
        assert apply_migrations(c) == ["0015_rec_feedback.sql"]

        reminders = {r["position_id"]: r["protection_reminders"]
                     for r in c.execute("SELECT position_id, protection_reminders FROM positions")}
        assert reminders == {"open": 2, "closed": 0}
        assert c.execute("SELECT payload, skip_reason FROM recommendations").fetchone()[:] == ("{}", None)
        assert c.execute("SELECT status, dedupe_key FROM notifications").fetchone()[:] == ("pending", None)
    finally:
        c.close()


def test_pre_0015_queries_still_run(conn):
    conn.execute("INSERT INTO recommendations (rec_id, payload, delivered_at) VALUES ('r1', '{}', 't')")
    conn.execute("INSERT INTO notifications (notification_id, created_at, kind, severity, title, body) "
                 "VALUES ('n1', 't', 'k', 'info', 'T', 'B')")
    conn.execute("INSERT INTO positions (position_id, symbol, state, origin) VALUES ('p', 'X', 'OPEN', 'external')")
    assert conn.execute("SELECT rec_id, payload, human_action, outcome FROM recommendations").fetchone()
    assert conn.execute("SELECT notification_id, status, attempts FROM notifications").fetchone()
    assert conn.execute("SELECT position_id, protection_state, avg_entry FROM positions").fetchone()
