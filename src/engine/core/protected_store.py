"""Protected store: integrity-verified limits / envelope config (R4, §2.4).

``config/limits.yaml`` (§7.1) and ``config/envelope.yaml`` (§6.3) are loaded by the risk gate ONLY if
their SHA-256 matches the signature recorded in the SQLite ``protected_config`` table — which is
written SOLELY by the owner-confirmed change flow (Telegram two-step / dashboard with token). The
learning system has read-only logical access and NO code path that writes these files (R4).

This module raises :class:`IntegrityError` on any mismatch; it does NOT decide the consequence. The
single integrity-failure rule (§2.4) — startup-with-flat-book ⇒ FROZEN+alert, runtime-or-with-live-book
⇒ kill — is applied by the caller (self-test / gate), tested in §9.1.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

import yaml

from engine.core.clock import Clock
from engine.core.db import transaction
from engine.core.enums import Actor
from engine.core.types import OwnerConfirmation

PROTECTED_NAMES = ("limits.yaml", "envelope.yaml")


class IntegrityError(RuntimeError):
    """Raised when a protected config's on-disk hash does not match its signature record (R4)."""


class UnauthorizedUpdate(PermissionError):
    """Raised when a protected-config update is attempted without an owner confirmation (R4/R10)."""


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class ProtectedStore:
    """Loads + verifies the protected config files; the sole writer of ``protected_config``."""

    def __init__(self, config_dir: str | Path, conn: sqlite3.Connection, clock: Clock) -> None:
        self._dir = Path(config_dir)
        self._conn = conn
        self._clock = clock

    # ----------------------------------------------------------------- read
    def _path(self, name: str) -> Path:
        if name not in PROTECTED_NAMES:
            raise ValueError(f"{name!r} is not a protected config (expected one of {PROTECTED_NAMES})")
        return self._dir / name

    def _signature(self, name: str) -> str | None:
        row = self._conn.execute(
            "SELECT sha256 FROM protected_config WHERE name=?", (name,)
        ).fetchone()
        return row["sha256"] if row else None

    def is_registered(self, name: str) -> bool:
        return self._signature(name) is not None

    def verify(self, name: str) -> bool:
        """Non-raising integrity check (used by the startup self-test, D11)."""
        try:
            self.load_verified(name)
            return True
        except (IntegrityError, FileNotFoundError):
            return False

    def load_verified(self, name: str) -> dict:
        """Load ``name`` only if its SHA-256 matches the recorded signature; else raise (R4)."""
        path = self._path(name)
        raw = path.read_bytes()
        actual = sha256_bytes(raw)
        recorded = self._signature(name)
        if recorded is None:
            raise IntegrityError(f"{name} is not registered in protected_config (unregistered, §2.4)")
        if actual != recorded:
            raise IntegrityError(
                f"{name} hash mismatch: on-disk {actual[:12]}… != recorded {recorded[:12]}… (R4)"
            )
        return yaml.safe_load(raw.decode("utf-8")) or {}

    # ----------------------------------------------------------------- write (owner-only)
    def owner_update(self, name: str, content: str, confirmation: OwnerConfirmation) -> None:
        """Owner-confirmed change flow: write ``content`` to ``name`` and re-sign it (R4/R10).

        The ONLY path that writes ``protected_config``. Requires an authenticated owner confirmation;
        the learner/Tier-1 can never produce one (R4). Appends a ``config_audit`` row.
        """
        if not confirmation.confirmed or confirmation.actor != Actor.OWNER:
            raise UnauthorizedUpdate(
                "protected-config update requires an authenticated OWNER confirmation (R4/R10)"
            )
        path = self._path(name)
        # Validate it parses as YAML before committing the change.
        yaml.safe_load(content)
        encoded = content.encode("utf-8")
        new_hash = sha256_bytes(encoded)
        prev = self._signature(name)
        now = self._clock.now().isoformat()
        # write_bytes (not write_text): text mode translates "\n"->"\r\n" on Windows, which would make
        # the on-disk bytes differ from the hashed bytes. Byte-exact write keeps load_verified consistent.
        path.write_bytes(encoded)
        with transaction(self._conn):
            self._conn.execute(
                """
                INSERT INTO protected_config (name, sha256, updated_by, updated_at, content_snapshot)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    sha256=excluded.sha256,
                    updated_by=excluded.updated_by,
                    updated_at=excluded.updated_at,
                    content_snapshot=excluded.content_snapshot
                """,
                (name, new_hash, confirmation.actor.value, now, content),
            )
            self._conn.execute(
                "INSERT INTO config_audit (name, diff, actor, at) VALUES (?, ?, ?, ?)",
                (
                    name,
                    json.dumps({"prev_sha256": prev, "new_sha256": new_hash, "note": confirmation.note}),
                    confirmation.actor.value,
                    now,
                ),
            )

    def register_initial(self, name: str, confirmation: OwnerConfirmation) -> None:
        """First-run seeding: register the CURRENT on-disk file's hash (owner action).

        Convenience over :meth:`owner_update` for the unchanged starter files. The owner runs this once
        (e.g. ``scripts/seed_protected_config.py``) after reviewing the shipped limits/envelope.
        """
        content = self._path(name).read_text(encoding="utf-8")
        self.owner_update(name, content, confirmation)
