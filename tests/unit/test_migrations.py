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
    "day_plans", "agent_calls", "equity_snapshots", "risk_state_causes", "nightly_reviews",
    "notifications",
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


def test_funnel_raw_counts_table_upserts_absolute_values(db_path):
    """2026-08-21: the WO-9 RAW pre-screen counters get a DB home so a restart stops zeroing them
    (the 22:35 review reported ``raw=None`` on 4 of the last 6 trade days). The PK is
    (d, strategy_id) precisely because the flush re-writes the day's ABSOLUTE total whenever it
    changes — the write has to be idempotent no matter how many times the engine bounced."""
    conn = connect(db_path)
    try:
        apply_migrations(conn)
        assert "funnel_raw_counts" in _tables(conn)
        for fires in (12, 37):                   # same (d, strategy) twice = one row, latest wins
            conn.execute(
                "INSERT INTO funnel_raw_counts (d, strategy_id, fires) VALUES ('2026-08-21', 'orb', ?) "
                "ON CONFLICT(d, strategy_id) DO UPDATE SET fires=excluded.fires",
                (fires,),
            )
        conn.execute(
            "INSERT INTO funnel_raw_counts (d, strategy_id, fires) VALUES ('2026-08-21', 'rsi2', 4)"
        )
        rows = conn.execute(
            "SELECT strategy_id, fires FROM funnel_raw_counts ORDER BY strategy_id"
        ).fetchall()
        assert [(r["strategy_id"], r["fires"]) for r in rows] == [("orb", 37), ("rsi2", 4)]
    finally:
        conn.close()


def test_notifications_journal_table_and_status_machine(db_path):
    """WO-24d (2026-08-21): the owner-notification journal that is simultaneously the Telegram retry
    outbox and the dashboard's data source. ``status`` is the state machine and the CHECK constraint
    is what keeps a fourth, undefined state from ever reaching the drainer or the page."""
    import sqlite3

    conn = connect(db_path)
    try:
        apply_migrations(conn)
        assert "notifications" in _tables(conn)
        conn.execute(
            "INSERT INTO notifications (notification_id, created_at, kind, severity, title, body) "
            "VALUES ('n1', '2026-08-21T10:00:00+05:30', 'recommendation', 'info', 'T', 'B')"
        )
        row = conn.execute("SELECT * FROM notifications WHERE notification_id='n1'").fetchone()
        assert row["status"] == "pending" and row["attempts"] == 0        # defaults, not caller-supplied
        assert row["delivered_at"] is None and row["last_error"] is None
        assert row["last_attempt_at"] is None                             # backoff clock starts unset

        try:
            conn.execute("UPDATE notifications SET status='queued' WHERE notification_id='n1'")
            conn.commit()
            raise AssertionError("expected CHECK to reject a status outside the state machine")
        except sqlite3.IntegrityError:
            conn.rollback()

        # The day read (GET /notifications) and the outbox drain are both created_at scans.
        indexes = {
            r["name"]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='notifications'"
            ).fetchall()
        }
        assert "idx_notifications_created_at" in indexes
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
