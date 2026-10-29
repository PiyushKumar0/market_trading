"""0021_notification_buttons: an additive, nullable column; upgrade from a populated 0020 db."""

from __future__ import annotations

from engine.core import migrations
from engine.core.db import connect
from engine.core.migrations import apply_migrations


def test_upgrade_from_populated_0020_keeps_every_notification_without_buttons(db_path, monkeypatch):
    c = connect(db_path)
    try:
        full = migrations.discover()
        monkeypatch.setattr(migrations, "discover", lambda: [p for p in full if p.name < "0021"])
        apply_migrations(c)
        c.execute("INSERT INTO notifications (notification_id, created_at, title, body) "
                  "VALUES ('n1', '2026-10-29T10:00:00+05:30', 'T', 'B')")
        c.commit()

        monkeypatch.setattr(migrations, "discover", lambda: full)
        assert "0021_notification_buttons.sql" in apply_migrations(c)

        row = c.execute("SELECT notification_id, status, buttons FROM notifications").fetchone()
        assert tuple(row) == ("n1", "pending", None)
    finally:
        c.close()
