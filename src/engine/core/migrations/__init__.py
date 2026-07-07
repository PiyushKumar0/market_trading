"""SQLite migrations runner (§4.2).

Numbered ``NNNN_name.sql`` files in this directory are applied in order, each exactly once, recorded
in a ``schema_migrations`` table. Idempotent: re-running applies only new files. Run with::

    python -m engine.core.migrations            # uses settings.yaml sqlite path
    python -m engine.core.migrations <path.db>  # explicit path
"""

from __future__ import annotations

import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from engine.core.db import connect
from engine.core.log import configure_logging, get_logger

_log = get_logger("engine.core.migrations")
_MIGRATIONS_DIR = Path(__file__).parent


def _ensure_tracking_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            filename   TEXT PRIMARY KEY,
            applied_at TEXT NOT NULL
        )
        """
    )


def _applied(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT filename FROM schema_migrations").fetchall()
    return {r["filename"] for r in rows}


def discover() -> list[Path]:
    return sorted(_MIGRATIONS_DIR.glob("[0-9]*.sql"))


def apply_migrations(conn: sqlite3.Connection) -> list[str]:
    """Apply all pending migration files. Returns the filenames applied this run."""
    _ensure_tracking_table(conn)
    done = _applied(conn)
    applied_now: list[str] = []
    for path in discover():
        if path.name in done:
            continue
        sql = path.read_text(encoding="utf-8")
        # Each migration is atomic. ``executescript`` ignores Python-side transaction control (it
        # implicitly commits first and runs the script verbatim), so the BEGIN/COMMIT and the
        # tracking-row insert must live INSIDE the script. filename + timestamp are fully controlled
        # by us (single quotes escaped defensively) — no injection surface.
        applied_ts = datetime.now(ZoneInfo("Asia/Kolkata")).isoformat()
        fname = path.name.replace("'", "''")
        ts = applied_ts.replace("'", "''")
        script = (
            "BEGIN;\n"
            f"{sql}\n"
            f"INSERT INTO schema_migrations (filename, applied_at) VALUES ('{fname}', '{ts}');\n"
            "COMMIT;\n"
        )
        try:
            conn.executescript(script)
        except BaseException:
            conn.rollback()
            _log.exception("migration_failed", filename=path.name)
            raise
        applied_now.append(path.name)
        _log.info("migration_applied", filename=path.name)
    return applied_now


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    args = argv if argv is not None else sys.argv[1:]
    if args:
        db_path = Path(args[0])
    else:
        from engine.core.config import load_settings

        db_path = load_settings().sqlite_path()
    conn = connect(db_path)
    try:
        applied = apply_migrations(conn)
    finally:
        conn.close()
    if applied:
        _log.info("migrations_complete", count=len(applied), db=str(db_path))
    else:
        _log.info("migrations_up_to_date", db=str(db_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
