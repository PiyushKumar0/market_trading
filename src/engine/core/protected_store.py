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
import os
import sqlite3
from pathlib import Path

import yaml

from engine.core.clock import Clock
from engine.core.db import transaction
from engine.core.enums import Actor
from engine.core.log import get_logger
from engine.core.types import OwnerConfirmation

PROTECTED_NAMES = ("limits.yaml", "envelope.yaml")

_log = get_logger(__name__)


class IntegrityError(RuntimeError):
    """Raised when a protected config's on-disk hash does not match its signature record (R4)."""


class UnauthorizedUpdate(PermissionError):
    """Raised when a protected-config update is attempted without an owner confirmation (R4/R10)."""


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _write_synced(path: Path, data: bytes) -> None:
    """Write ``data`` byte-exact and fsync it, so a later ``os.replace`` never moves in a short file."""
    with open(path, "wb") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())


def _unlink_quietly(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except OSError:
        _log.warning("protected_config.tmp_not_removed", path=str(path))


def _tmp_path(path: Path, kind: str = "") -> Path:
    """Per-process temp name beside ``path``: writers in other processes never share a temp file."""
    return path.with_name(f".{path.name}.{os.getpid()}{kind}.tmp")


_KEEP = object()  # restore target: the file already matches the recorded signature
_UNREAD = object()  # current bytes could not be read


def _put_bytes(path: Path, target: bytes | None) -> None:
    """Make ``path`` hold ``target`` (absent if ``None``) via a synced temp + ``os.replace``."""
    restore_tmp = _tmp_path(path, ".restore")
    try:
        current: object
        try:
            current = path.read_bytes()
        except FileNotFoundError:
            current = None
        except OSError:
            current = _UNREAD  # equal to no target, so the write still happens
        if current == target:
            return
        if target is None:
            path.unlink(missing_ok=True)
            return
        _write_synced(restore_tmp, target)
        os.replace(restore_tmp, path)
    except BaseException:
        _unlink_quietly(restore_tmp)
        _log.critical(
            "protected_config.restore_failed",
            name=path.name,
            detail="on-disk bytes do not match the signature; the next load fails closed",
        )
        raise


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

    def read_verified_bytes(self, name: str) -> tuple[bytes, str]:
        """Return ``name``'s raw bytes and their SHA-256, only if it matches the signature; else raise.

        The single read a protected-config edit is built from: pass the returned hash to
        :meth:`owner_update` as ``expected_sha256`` so the write is a compare-and-swap on it.
        """
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
        return raw, actual

    def load_verified(self, name: str) -> dict:
        """Load ``name`` only if its SHA-256 matches the recorded signature; else raise (R4)."""
        raw, _ = self.read_verified_bytes(name)
        return yaml.safe_load(raw.decode("utf-8")) or {}

    # ----------------------------------------------------------------- write (owner-only)
    def owner_update(
        self,
        name: str,
        content: str,
        confirmation: OwnerConfirmation,
        *,
        expected_sha256: str | None = None,
    ) -> None:
        """Owner-confirmed change flow: write ``content`` to ``name`` and re-sign it (R4/R10).

        The ONLY path that writes ``protected_config``. Requires an authenticated owner confirmation;
        the learner/Tier-1 can never produce one (R4). Appends a ``config_audit`` row.

        With ``expected_sha256`` (the hash :meth:`read_verified_bytes` returned) the re-sign is a
        compare-and-swap: if the signature or the on-disk bytes changed since that read, raise
        :class:`IntegrityError` and touch nothing. The new bytes go to a per-process temp file that is
        moved into place as the last step before COMMIT; on any exception the transaction is rolled
        back if still open and the file is set to match the recorded signature (see :meth:`_restore`),
        so every exit leaves the file and its signature matched, except two cases: a hard crash
        between the move and COMMIT, and a failed restore (logged CRITICAL). Both fail closed at the
        next load (§2.4).
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
        if expected_sha256 is not None and prev != expected_sha256:
            raise IntegrityError(
                f"{name} signature changed since it was read: recorded "
                f"{prev[:12] + '…' if prev else 'none'} != expected {expected_sha256[:12]}… (R4)"
            )
        try:
            old_bytes: bytes | None = path.read_bytes()
        except FileNotFoundError:
            old_bytes = None
        if expected_sha256 is not None and (old_bytes is None or sha256_bytes(old_bytes) != expected_sha256):
            # The signature still matches the read, but the file does not: an unsigned edit landed in
            # between. Overwriting it would silently discard that edit.
            raise IntegrityError(
                f"{name} on-disk bytes changed since it was read (unsigned edit); not overwriting (R4)"
            )
        now = self._clock.now().isoformat()
        tmp = _tmp_path(path)
        began_outside = self._conn.in_transaction
        try:
            # Byte-exact write (not text mode): text mode translates "\n"->"\r\n" on Windows, which
            # would make the on-disk bytes differ from the hashed bytes.
            _write_synced(tmp, encoded)
            with transaction(self._conn):
                if expected_sha256 is None:
                    self._conn.execute(
                        """
                        INSERT INTO protected_config
                            (name, sha256, updated_by, updated_at, content_snapshot)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(name) DO UPDATE SET
                            sha256=excluded.sha256,
                            updated_by=excluded.updated_by,
                            updated_at=excluded.updated_at,
                            content_snapshot=excluded.content_snapshot
                        """,
                        (name, new_hash, confirmation.actor.value, now, content),
                    )
                else:
                    cur = self._conn.execute(
                        "UPDATE protected_config SET sha256=?, updated_by=?, updated_at=?, "
                        "content_snapshot=? WHERE name=? AND sha256=?",
                        (new_hash, confirmation.actor.value, now, content, name, expected_sha256),
                    )
                    if cur.rowcount != 1:
                        raise IntegrityError(
                            f"{name} signature changed since it was read: compare-and-swap on "
                            f"{expected_sha256[:12]}… matched {cur.rowcount} rows (R4)"
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
                os.replace(tmp, path)  # must stay the last statement: COMMIT follows immediately
        except BaseException:
            try:
                # transaction() does not ROLLBACK when COMMIT itself fails, and SQLite then keeps the
                # transaction open; a later COMMIT on this shared connection would then record the new
                # signature over the restored old bytes.
                if not began_outside and self._conn.in_transaction:
                    try:
                        self._conn.execute("ROLLBACK")
                    except Exception:
                        _log.exception("protected_config.rollback_failed", name=name)
            finally:
                # Runs however the rollback ends, a BaseException during it included.
                self._restore(name, path, tmp, old_bytes, encoded, durable=self._txn_closed(began_outside))
            raise

    def _txn_closed(self, began_outside: bool) -> bool:
        """True when this call's transaction is no longer open, so the recorded signature is durable."""
        if began_outside:
            return True  # our BEGIN failed: nothing of ours was written or swapped
        try:
            return not self._conn.in_transaction
        except Exception:
            return False

    def _restore(
        self,
        name: str,
        path: Path,
        tmp: Path,
        old_bytes: bytes | None,
        new_bytes: bytes,
        *,
        durable: bool,
    ) -> None:
        """After a failed update, leave ``path`` matching the recorded signature; remove the temp files.

        The target comes from durable state, not from the exception's timing: a COMMIT that landed
        before the exception keeps the new bytes, and a file another writer committed and signed is
        left alone. When that state is unknowable, or records neither the old nor the new bytes, the
        old bytes go back and the event is logged CRITICAL. A temp file that cannot be removed is only
        logged, so it never stops the restore or masks the exception that caused it.
        """
        try:
            target, verified = self._restore_target(name, path, old_bytes, new_bytes, durable=durable)
            if not verified:
                _log.critical(
                    "protected_config.restore_unverified",
                    name=name,
                    detail="recorded signature unknown or matches neither old nor new bytes; old bytes restored",
                )
            if target is not _KEEP:
                _put_bytes(path, target)  # type: ignore[arg-type]
        finally:
            _unlink_quietly(tmp)

    def _restore_target(
        self, name: str, path: Path, old_bytes: bytes | None, new_bytes: bytes, *, durable: bool
    ) -> tuple[object, bool]:
        """``(target, verified)``; target is ``_KEEP``, the bytes to put back, or ``None`` (absent)."""
        if not durable:
            return old_bytes, False
        try:
            recorded = self._signature(name)
            try:
                current: bytes | None = path.read_bytes()
            except FileNotFoundError:
                current = None
        except Exception:
            return old_bytes, False
        if recorded is None:
            return old_bytes, True  # nothing signed: back to the prior unregistered state
        if current is not None and sha256_bytes(current) == recorded:
            return _KEEP, True
        if old_bytes is not None and sha256_bytes(old_bytes) == recorded:
            return old_bytes, True
        if sha256_bytes(new_bytes) == recorded:
            return new_bytes, True
        return old_bytes, False

    def register_initial(self, name: str, confirmation: OwnerConfirmation) -> None:
        """First-run seeding: register the CURRENT on-disk file's hash (owner action).

        Convenience over :meth:`owner_update` for the unchanged starter files. The owner runs this once
        (e.g. ``scripts/seed_protected_config.py``) after reviewing the shipped limits/envelope.
        """
        content = self._path(name).read_text(encoding="utf-8")
        self.owner_update(name, content, confirmation)
