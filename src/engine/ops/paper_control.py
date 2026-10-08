"""Paper autopilot on/off and reset request over the single ``paper_state`` row (plan Q4.0).

The row is created disabled on first use. Reset is only recorded here; Q4.8 carries it out.
"""

from __future__ import annotations

import json
import sqlite3
from typing import Any

from engine.core.clock import Clock
from engine.core.db import transaction


class PaperControl:
    def __init__(self, conn: sqlite3.Connection, clock: Clock) -> None:
        self._conn = conn
        self._clock = clock
        conn.execute("INSERT OR IGNORE INTO paper_state (id, enabled) VALUES (1, 0)")

    def enabled(self) -> bool:
        return bool(self._conn.execute("SELECT enabled FROM paper_state WHERE id = 1").fetchone()[0])

    def set_enabled(self, on: bool, actor: str, *, via: str) -> bool:
        """Writes and audits only a change: nightly ``paper_line`` keys on ``changed_at``."""
        now = self._clock.now().isoformat()
        with transaction(self._conn):
            changed = self._conn.execute(
                "UPDATE paper_state SET enabled = ?, changed_at = ?, changed_by = ? "
                "WHERE id = 1 AND enabled != ?",
                (int(on), now, actor, int(on)),
            ).rowcount > 0
            if changed:
                self._conn.execute(
                    "INSERT INTO config_audit (name, diff, actor, at) VALUES (?, ?, ?, ?)",
                    ("paper_state", json.dumps({"enabled": on, "via": via}), actor, now),
                )
        return changed

    def request_reset(self, actor: str) -> None:
        now = self._clock.now().isoformat()
        self._conn.execute(
            "UPDATE paper_state SET reset_requested_at = ?, changed_at = ?, changed_by = ? WHERE id = 1",
            (now, now, actor),
        )

    def state(self) -> dict[str, Any]:
        row = self._conn.execute(
            "SELECT enabled, changed_at, changed_by, epoch_started_at, reset_requested_at "
            "FROM paper_state WHERE id = 1"
        ).fetchone()
        return {**dict(row), "enabled": bool(row["enabled"])}

    def active_halts(self) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT cause, rung, set_at, latched FROM paper_halts WHERE cleared_at IS NULL ORDER BY set_at"
        ).fetchall()
        return [dict(r) for r in rows]
