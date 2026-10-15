"""0017_paper_exit_policy: an additive, nullable column; upgrade from a populated 0016 db (D14)."""

from __future__ import annotations

from engine.core import migrations
from engine.core.db import connect
from engine.core.migrations import apply_migrations


def test_upgrade_from_populated_0016_keeps_every_position_on_its_levels(db_path, monkeypatch):
    c = connect(db_path)
    try:
        full = migrations.discover()
        monkeypatch.setattr(migrations, "discover", lambda: [p for p in full if p.name < "0017"])
        apply_migrations(c)
        c.execute("INSERT INTO positions (position_id, symbol, state, origin, stop, target, is_paper) "
                  "VALUES ('paper', 'X', 'OPEN', 'platform', '95', '110', 1)")
        c.execute("INSERT INTO positions (position_id, symbol, state, origin, stop, is_paper) "
                  "VALUES ('real', 'Y', 'OPEN', 'recommended', '40', 0)")
        c.commit()

        monkeypatch.setattr(migrations, "discover", lambda: full)
        assert "0017_paper_exit_policy.sql" in apply_migrations(c)

        rows = c.execute("SELECT position_id, stop, target, exit_policy FROM positions ORDER BY position_id").fetchall()
        assert [tuple(r) for r in rows] == [("paper", "95", "110", None), ("real", "40", None, None)]
    finally:
        c.close()
