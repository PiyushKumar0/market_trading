"""core.db.transaction(): every way out of the block leaves the shared connection outside a transaction.

The defect this pins (parked in the O19 build, fixed 2026-10-18): a COMMIT that failed was raised from the
``else`` branch with no ROLLBACK, and SQLite keeps the transaction open after such a COMMIT. Every later
``BEGIN`` on the shared connection then failed, the kill switch's persist included.
"""

from __future__ import annotations

import logging
import sqlite3

import pytest

from engine.core.db import connect, transaction


def _fk_conn(tmp_path) -> sqlite3.Connection:
    """A deferred foreign key is checked at COMMIT: a real COMMIT failure that leaves the transaction open."""
    conn = connect(tmp_path / "t.db")
    conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
    conn.execute(
        "CREATE TABLE child (id INTEGER PRIMARY KEY, "
        "parent_id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)"
    )
    return conn


def _count(conn: sqlite3.Connection, table: str) -> int:
    return conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


def test_a_block_commits(tmp_path):
    conn = _fk_conn(tmp_path)
    with transaction(conn):
        conn.execute("INSERT INTO parent (id) VALUES (1)")
    assert conn.in_transaction is False
    assert _count(conn, "parent") == 1


def test_an_error_in_the_block_rolls_back_and_propagates(tmp_path):
    conn = _fk_conn(tmp_path)
    with pytest.raises(ValueError, match="boom"):
        with transaction(conn):
            conn.execute("INSERT INTO parent (id) VALUES (1)")
            raise ValueError("boom")
    assert conn.in_transaction is False
    assert _count(conn, "parent") == 0


def test_a_failed_commit_rolls_back_and_leaves_the_connection_usable(tmp_path):
    conn = _fk_conn(tmp_path)
    with pytest.raises(sqlite3.IntegrityError):
        with transaction(conn):
            conn.execute("INSERT INTO parent (id) VALUES (7)")
            conn.execute("INSERT INTO child (id, parent_id) VALUES (1, 99)")   # violation found at COMMIT
    assert conn.in_transaction is False
    assert (_count(conn, "parent"), _count(conn, "child")) == (0, 0)      # nothing half-published
    with transaction(conn):                                                 # the next persist works
        conn.execute("INSERT INTO parent (id) VALUES (1)")
    assert _count(conn, "parent") == 1


def test_an_error_after_sqlite_already_rolled_back_is_not_masked(tmp_path):
    conn = _fk_conn(tmp_path)
    with pytest.raises(ValueError, match="the real error"):
        with transaction(conn):
            conn.execute("ROLLBACK")     # as SQLite does by itself after a full disk or an I/O error
            raise ValueError("the real error")
    assert conn.in_transaction is False


def test_a_nested_call_fails_at_begin_and_leaves_the_outer_transaction_alone(tmp_path):
    conn = _fk_conn(tmp_path)
    with transaction(conn):
        conn.execute("INSERT INTO parent (id) VALUES (1)")
        with pytest.raises(sqlite3.OperationalError):
            with transaction(conn):              # BEGIN inside a transaction fails before the try
                conn.execute("INSERT INTO parent (id) VALUES (2)")
        assert conn.in_transaction is True        # the outer one was not rolled back
    assert [r[0] for r in conn.execute("SELECT id FROM parent")] == [1]


def test_a_closed_connection_does_not_mask_the_original_error(tmp_path):
    conn = _fk_conn(tmp_path)
    with pytest.raises(ValueError, match="original"):
        with transaction(conn):
            conn.close()
            raise ValueError("original")


class _RollbackFails:
    """A connection whose ROLLBACK fails, delegating everything else to a real one."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    @property
    def in_transaction(self) -> bool:
        return self._conn.in_transaction

    def execute(self, sql: str, *args):
        if sql == "ROLLBACK":
            raise sqlite3.OperationalError("disk I/O error")
        return self._conn.execute(sql, *args)


def test_a_failing_rollback_is_logged_critical_and_the_original_error_propagates(tmp_path, caplog):
    conn = _fk_conn(tmp_path)
    with caplog.at_level(logging.CRITICAL, logger="engine.core.db"):
        with pytest.raises(ValueError, match="boom"):
            with transaction(_RollbackFails(conn)):
                raise ValueError("boom")
    assert "transaction_rollback_failed" in [r.getMessage() for r in caplog.records]
    conn.execute("ROLLBACK")             # the real connection: clean up what the fake refused
