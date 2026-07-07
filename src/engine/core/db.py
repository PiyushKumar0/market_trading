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
    """
    conn.execute("BEGIN")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


def fetch_one(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> sqlite3.Row | None:
    cur = conn.execute(sql, params)
    return cur.fetchone()


def fetch_all(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
    cur = conn.execute(sql, params)
    return cur.fetchall()
