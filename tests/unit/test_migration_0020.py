"""0020_signal_outcomes: two new tables; upgrade from a populated 0019 db."""

from __future__ import annotations

import sqlite3

import pytest

from engine.core import migrations
from engine.core.db import connect
from engine.core.migrations import apply_migrations


def test_upgrade_from_populated_0019_adds_both_tables_and_their_checks(db_path, monkeypatch):
    c = connect(db_path)
    try:
        full = migrations.discover()
        monkeypatch.setattr(migrations, "discover", lambda: [p for p in full if p.name < "0020"])
        apply_migrations(c)
        c.execute("INSERT INTO prescreen_day_slots (d, symbol, strategy_id, published_at) "
                  "VALUES ('2026-10-28', 'X', 'hi52', '2026-10-28T10:00:00+05:30')")
        c.commit()

        monkeypatch.setattr(migrations, "discover", lambda: full)
        assert "0020_signal_outcomes.sql" in apply_migrations(c)

        assert tuple(c.execute("SELECT d, symbol, strategy_id FROM prescreen_day_slots").fetchone()) == (
            "2026-10-28", "X", "hi52")
        c.execute("INSERT INTO signal_outcomes (d, symbol, strategy_id, construct, status, updated_at) "
                  "VALUES ('2026-10-28', 'X', 'hi52', 'rule', 'open', 't')")
        c.execute("INSERT INTO brk20_regime_blocked (symbol, signal_d, entry, score, reason, created_at) "
                  "VALUES ('X', '2026-10-28', '90', 1.0, 'down', 't')")
        for column, value in (("construct", "market"), ("status", "pending")):
            with pytest.raises(sqlite3.IntegrityError):
                c.execute(f"UPDATE signal_outcomes SET {column} = ?", (value,))
    finally:
        c.close()
