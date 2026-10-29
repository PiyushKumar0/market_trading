"""0018_prescreen_analyst_decision: additive nullable columns; upgrade from a populated 0017 db."""

from __future__ import annotations

from engine.core import migrations
from engine.core.db import connect
from engine.core.migrations import apply_migrations


def test_upgrade_from_populated_0017_keeps_every_slot_with_no_decision(db_path, monkeypatch):
    c = connect(db_path)
    try:
        full = migrations.discover()
        monkeypatch.setattr(migrations, "discover", lambda: [p for p in full if p.name < "0018"])
        apply_migrations(c)
        c.execute("INSERT INTO prescreen_day_slots (d, symbol, strategy_id, published_at, evaluated, forwarded) "
                  "VALUES ('2026-10-28', 'X', 'orb', '2026-10-28T10:00:00+05:30', 1, 1)")
        c.commit()

        monkeypatch.setattr(migrations, "discover", lambda: full)
        assert "0018_prescreen_analyst_decision.sql" in apply_migrations(c)

        row = c.execute("SELECT symbol, forwarded, analyst_decision, analyst_confidence, analyst_call_id, "
                        "proposal_id FROM prescreen_day_slots").fetchone()
        assert tuple(row) == ("X", 1, None, None, None, None)
    finally:
        c.close()
