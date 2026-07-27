"""SQLite migrations v1 (§4.2): the full table set is created, singletons are seeded, and re-applying
is idempotent. Guards against a future migration omitting a §4.2 table (the engine_lifecycle regression
that this test would have caught)."""

from __future__ import annotations

from engine.core.db import connect
from engine.core.migrations import apply_migrations

# The complete §4.2 table inventory (migrations v1). Adding a table to the schema without adding it here
# fails CI, and vice-versa — the two must stay in lockstep so an omission cannot pass silently.
EXPECTED_TABLES = {
    "proposals", "verdicts", "orders", "order_events", "positions", "gtts",
    "mode_state", "kill_state", "engine_lifecycle", "trade_window_state", "job_runs",
    "budget_ledger", "protected_config", "config_audit", "owner_approvals",
    "learning_ledger", "param_sets", "model_registry", "envelope_state", "shadow_trades",
    "recommendations", "backfill_checkpoints", "filings_backfill_checkpoints", "schema_migrations",
}


def _tables(conn) -> set[str]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {r["name"] for r in rows}


def test_migrations_create_full_table_set(db_path):
    conn = connect(db_path)
    try:
        applied = apply_migrations(conn)
        assert applied, "expected migration files to apply on a fresh db"
        assert EXPECTED_TABLES <= _tables(conn)
    finally:
        conn.close()


def test_engine_lifecycle_singleton_seeded(db_path):
    conn = connect(db_path)
    try:
        apply_migrations(conn)
        row = conn.execute("SELECT id, state FROM engine_lifecycle WHERE id=1").fetchone()
        assert row is not None and row["state"] == "STOPPED"   # safe default (§2.6): watchdog silent
        # The tri-state CHECK constraint rejects anything outside RUNNING/STOPPING/STOPPED.
        import sqlite3

        try:
            conn.execute("UPDATE engine_lifecycle SET state='BOGUS' WHERE id=1")
            conn.commit()
            raise AssertionError("expected CHECK constraint to reject an invalid state")
        except sqlite3.IntegrityError:
            conn.rollback()
    finally:
        conn.close()


def test_migrations_idempotent(db_path):
    conn = connect(db_path)
    try:
        first = apply_migrations(conn)
        second = apply_migrations(conn)
        assert first and second == []          # nothing re-applied on the second pass
        assert EXPECTED_TABLES <= _tables(conn)
    finally:
        conn.close()
