"""SQLite connection helper for the transactional state store (E4, §4.1).

One writer (the engine), WAL mode for crash-safe single-box durability. This is a thin helper, not
an ORM — the platform's SQL is hand-written and lives next to each owner (migrations, mode, kill, ...).
A private addition to ``core`` per the plan's "implementing agent may add private helpers" allowance
(§3.2).
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from engine.core.log import get_logger

_log = get_logger(__name__)


def connect(db_path: str | Path) -> sqlite3.Connection:
    """Open a SQLite connection in WAL mode with sane pragmas and a Row factory.

    ``check_same_thread=False`` because the asyncio engine touches the connection from the loop
    thread and (rarely) executor threads; the engine serialises writes itself (single writer, §4.1).
    """
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), check_same_thread=False, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")   # WAL + NORMAL is crash-safe and fast on one box
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Explicit transaction (``isolation_level=None`` means autocommit otherwise).

    Used where a state change must be persisted atomically BEFORE any side effect (the kill switch
    persists ``KILLED`` before acting, R10; the OMS persists every transition before side effects).

    A COMMIT that fails while SQLite keeps the transaction open (a deferred constraint, statements
    still in progress; under WAL a lock surfaces at the block's first write instead) rolls back too
    and re-raises. Left open on the shared connection, every later ``BEGIN`` would fail, the kill
    switch's persist included, and a later COMMIT would publish this block's writes after the caller
    was told they failed. An error after which SQLite rolled back by itself (a full disk, an I/O
    error) propagates as itself rather than as "cannot rollback - no transaction is active".

    ``BEGIN`` stays outside the ``try``: a nested call fails there and leaves the outer transaction
    alone.
    """
    conn.execute("BEGIN")
    try:
        yield conn
        conn.execute("COMMIT")
    except BaseException:
        _rollback(conn)
        raise


def _rollback(conn: sqlite3.Connection) -> None:
    """Roll back the open transaction without masking the error being raised. SQLite may already
    have rolled back (a full disk or an I/O error), and a ROLLBACK then fails "no transaction is
    active"; the block may also have ended the transaction itself, or the connection be closed."""
    try:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
    except sqlite3.Error as exc:
        _log.critical("transaction_rollback_failed", error=repr(exc),
                      hint="the shared connection may be left inside an open transaction")


def fetch_one(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> sqlite3.Row | None:
    cur = conn.execute(sql, params)
    return cur.fetchone()


def fetch_all(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
    cur = conn.execute(sql, params)
    return cur.fetchall()
