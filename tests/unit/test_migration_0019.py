"""0019_agent_calls_prompt_version: an additive, nullable column; upgrade from a populated 0018 db."""

from __future__ import annotations

from engine.core import migrations
from engine.core.db import connect
from engine.core.migrations import apply_migrations


def test_upgrade_from_populated_0018_keeps_every_call_with_no_prompt_version(db_path, monkeypatch):
    c = connect(db_path)
    try:
        full = migrations.discover()
        monkeypatch.setattr(migrations, "discover", lambda: [p for p in full if p.name < "0019"])
        apply_migrations(c)
        c.execute("INSERT INTO agent_calls (call_id, agent_id, trigger, model, inputs_digest, context_gz, "
                  "output_json, ok, at) VALUES ('c1', 'intraday_analyst', 'signal_candidate', 'sonnet-5.5', "
                  "'d', x'00', '{}', 1, '2026-10-28T10:00:00+05:30')")
        c.commit()

        monkeypatch.setattr(migrations, "discover", lambda: full)
        assert "0019_agent_calls_prompt_version.sql" in apply_migrations(c)

        row = c.execute("SELECT call_id, ok, prompt_version FROM agent_calls").fetchone()
        assert tuple(row) == ("c1", 1, None)
    finally:
        c.close()
