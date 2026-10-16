"""Protected-store integrity (R4, §2.4). Tamper detection + owner-only write path."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from engine.core.enums import Actor
from engine.core.protected_store import IntegrityError, ProtectedStore, UnauthorizedUpdate, sha256_bytes
from engine.core.types import OwnerConfirmation

OWNER_OK = OwnerConfirmation(actor=Actor.OWNER, confirmed=True, note="two-step")


@pytest.fixture
def store(tmp_path, conn, clock):
    cfg = tmp_path / "config"
    cfg.mkdir()
    (cfg / "limits.yaml").write_text("schema_version: 1\nlimits: {}\n", encoding="utf-8")
    (cfg / "envelope.yaml").write_text("schema_version: 1\nparameters: {}\n", encoding="utf-8")
    return ProtectedStore(cfg, conn, clock)


def test_unregistered_load_raises(store):
    with pytest.raises(IntegrityError):
        store.load_verified("limits.yaml")
    assert store.verify("limits.yaml") is False


def test_register_then_load_ok(store):
    store.register_initial("limits.yaml", OWNER_OK)
    data = store.load_verified("limits.yaml")
    assert data["schema_version"] == 1
    assert store.verify("limits.yaml") is True


def test_tamper_detected(store, tmp_path):
    store.register_initial("limits.yaml", OWNER_OK)
    # Tamper with the file out-of-band (the learner-tamper scenario, §2.4).
    (tmp_path / "config" / "limits.yaml").write_text("schema_version: 1\nlimits: {hacked: true}\n", encoding="utf-8")
    with pytest.raises(IntegrityError):
        store.load_verified("limits.yaml")
    assert store.verify("limits.yaml") is False


def test_owner_update_requires_confirmation(store):
    store.register_initial("limits.yaml", OWNER_OK)
    with pytest.raises(UnauthorizedUpdate):
        store.owner_update("limits.yaml", "schema_version: 1\n", OwnerConfirmation(actor=Actor.OWNER, confirmed=False))
    with pytest.raises(UnauthorizedUpdate):
        store.owner_update("limits.yaml", "schema_version: 1\n", OwnerConfirmation(actor=Actor.LEARNER, confirmed=True))


def test_owner_update_resigns_and_audits(store, conn):
    store.register_initial("limits.yaml", OWNER_OK)
    new_content = "schema_version: 1\nlimits:\n  max_new_trades_day:\n    count: 4\n"
    store.owner_update("limits.yaml", new_content, OWNER_OK)
    # Loads cleanly under the new hash.
    data = store.load_verified("limits.yaml")
    assert data["limits"]["max_new_trades_day"]["count"] == 4
    # An audit row exists (register + update => >=2).
    rows = conn.execute("SELECT COUNT(*) AS n FROM config_audit WHERE name='limits.yaml'").fetchone()
    assert rows["n"] >= 2


def test_non_protected_name_rejected(store):
    with pytest.raises(ValueError):
        store.load_verified("settings.yaml")


# --------------------------------------------------------------------- O19: read_verified_bytes + CAS write
NEW_CONTENT = "schema_version: 1\nlimits:\n  max_new_trades_day:\n    count: 4\n"


def _limits(tmp_path):
    return tmp_path / "config" / "limits.yaml"


def _state(store, conn, path):
    """Everything a failed owner_update must leave untouched."""
    audits = conn.execute("SELECT COUNT(*) AS n FROM config_audit WHERE name=?", (path.name,)).fetchone()["n"]
    data = path.read_bytes() if path.exists() else None
    return data, store._signature(path.name), audits


def _no_tmp_left(path):
    return not list(path.parent.glob("." + path.name + "*.tmp"))


def _fail_first_replace(monkeypatch, exc, *, after_swap):
    """Patch ``os.replace`` so only its FIRST call fails; the restore path then uses the real one."""
    import engine.core.protected_store as ps

    real_replace = ps.os.replace
    calls = []

    def replace(src, dst):
        calls.append((src, dst))
        if len(calls) == 1:
            if after_swap:
                real_replace(src, dst)
            raise exc
        real_replace(src, dst)

    monkeypatch.setattr(ps.os, "replace", replace)
    return calls


def test_read_verified_bytes_matches_the_signature(store, tmp_path):
    store.register_initial("limits.yaml", OWNER_OK)
    raw, sha = store.read_verified_bytes("limits.yaml")
    assert raw == _limits(tmp_path).read_bytes()
    assert sha == store._signature("limits.yaml") == sha256_bytes(raw)


def test_read_verified_bytes_refuses_an_unsigned_edit(store, tmp_path):
    store.register_initial("limits.yaml", OWNER_OK)
    path = _limits(tmp_path)
    path.write_bytes(path.read_bytes() + b"#")
    with pytest.raises(IntegrityError, match="hash mismatch"):
        store.read_verified_bytes("limits.yaml")


def test_read_verified_bytes_refuses_an_unregistered_file(store):
    with pytest.raises(IntegrityError, match="not registered"):
        store.read_verified_bytes("limits.yaml")


def test_cas_refuses_a_changed_signature(store, conn, tmp_path):
    store.register_initial("limits.yaml", OWNER_OK)
    path = _limits(tmp_path)
    before = _state(store, conn, path)
    with pytest.raises(IntegrityError, match="signature changed since it was read"):
        store.owner_update("limits.yaml", NEW_CONTENT, OWNER_OK, expected_sha256="0" * 64)
    assert _state(store, conn, path) == before
    assert _no_tmp_left(path)


def test_cas_refuses_an_unregistered_file(store, conn, tmp_path):
    path = _limits(tmp_path)
    before = _state(store, conn, path)
    with pytest.raises(IntegrityError, match="signature changed since it was read"):
        store.owner_update("limits.yaml", NEW_CONTENT, OWNER_OK, expected_sha256="0" * 64)
    assert _state(store, conn, path) == before


def test_cas_refuses_a_signature_that_changes_after_the_precheck(store, conn, tmp_path, monkeypatch):
    # Another writer re-signs between the pre-check and the UPDATE: the WHERE sha256=? guard must catch it.
    store.register_initial("limits.yaml", OWNER_OK)
    path = _limits(tmp_path)
    _, stale = store.read_verified_bytes("limits.yaml")
    conn.execute("UPDATE protected_config SET sha256=? WHERE name='limits.yaml'", ("f" * 64,))
    monkeypatch.setattr(store, "_signature", lambda name: stale)
    before = _state(store, conn, path)
    with pytest.raises(IntegrityError, match="signature changed since it was read"):
        store.owner_update("limits.yaml", NEW_CONTENT, OWNER_OK, expected_sha256=stale)
    assert _state(store, conn, path) == before
    assert _no_tmp_left(path)


def test_cas_refuses_an_unsigned_edit_made_since_the_read(store, conn, tmp_path):
    # The signature still matches what the caller read, but the bytes on disk do not: overwriting them
    # would silently discard someone's edit, so the write is refused.
    store.register_initial("limits.yaml", OWNER_OK)
    path = _limits(tmp_path)
    _, sha = store.read_verified_bytes("limits.yaml")
    path.write_bytes(path.read_bytes() + b"# owner edit in progress\n")
    before = _state(store, conn, path)
    with pytest.raises(IntegrityError, match="changed since it was read"):
        store.owner_update("limits.yaml", NEW_CONTENT, OWNER_OK, expected_sha256=sha)
    assert _state(store, conn, path) == before
    assert _no_tmp_left(path)


def test_cas_success_resigns_and_audits(store, conn, tmp_path):
    store.register_initial("limits.yaml", OWNER_OK)
    path = _limits(tmp_path)
    _, sha = store.read_verified_bytes("limits.yaml")
    audits_before = _state(store, conn, path)[2]
    store.owner_update("limits.yaml", NEW_CONTENT, OWNER_OK, expected_sha256=sha)
    assert store.load_verified("limits.yaml")["limits"]["max_new_trades_day"]["count"] == 4
    assert path.read_bytes() == NEW_CONTENT.encode("utf-8")
    row = conn.execute(
        "SELECT sha256, content_snapshot FROM protected_config WHERE name='limits.yaml'"
    ).fetchone()
    assert row["sha256"] == sha256_bytes(NEW_CONTENT.encode("utf-8"))
    assert row["content_snapshot"] == NEW_CONTENT
    audits = conn.execute(
        "SELECT diff FROM config_audit WHERE name='limits.yaml' ORDER BY id"
    ).fetchall()
    assert len(audits) == audits_before + 1
    diff = json.loads(audits[-1]["diff"])
    assert diff == {"prev_sha256": sha, "new_sha256": row["sha256"], "note": OWNER_OK.note}
    assert _no_tmp_left(path)


@pytest.mark.parametrize("exc", [OSError("disk full"), KeyboardInterrupt()], ids=["oserror", "interrupt"])
def test_a_failure_inside_the_transaction_restores_the_old_bytes(store, conn, tmp_path, monkeypatch, exc):
    store.register_initial("limits.yaml", OWNER_OK)
    path = _limits(tmp_path)
    before = _state(store, conn, path)
    calls = _fail_first_replace(monkeypatch, exc, after_swap=True)  # after the swap, before COMMIT
    with pytest.raises(type(exc)):
        store.owner_update("limits.yaml", NEW_CONTENT, OWNER_OK)
    assert len(calls) == 2  # the swap, then the restore
    assert _state(store, conn, path) == before
    assert store.verify("limits.yaml") is True
    assert _no_tmp_left(path)


def test_a_cas_failure_after_the_swap_restores_the_old_bytes(store, conn, tmp_path, monkeypatch):
    store.register_initial("limits.yaml", OWNER_OK)
    path = _limits(tmp_path)
    _, sha = store.read_verified_bytes("limits.yaml")
    before = _state(store, conn, path)
    _fail_first_replace(monkeypatch, OSError("disk full"), after_swap=True)
    with pytest.raises(OSError):
        store.owner_update("limits.yaml", NEW_CONTENT, OWNER_OK, expected_sha256=sha)
    assert _state(store, conn, path) == before
    assert store.verify("limits.yaml") is True
    assert _no_tmp_left(path)


def test_a_failed_swap_leaves_the_old_bytes_and_no_tmp(store, conn, tmp_path, monkeypatch):
    # os.replace itself fails (e.g. the file is held open on Windows): nothing moved, nothing to restore.
    store.register_initial("limits.yaml", OWNER_OK)
    path = _limits(tmp_path)
    before = _state(store, conn, path)
    calls = _fail_first_replace(monkeypatch, PermissionError("in use"), after_swap=False)
    with pytest.raises(PermissionError):
        store.owner_update("limits.yaml", NEW_CONTENT, OWNER_OK)
    assert len(calls) == 1  # bytes already equal the old bytes, so no restore swap
    assert _state(store, conn, path) == before
    assert _no_tmp_left(path)


def test_a_failure_with_no_old_file_removes_the_new_file(store, conn, tmp_path, monkeypatch):
    path = tmp_path / "config" / "envelope.yaml"
    path.unlink()
    _fail_first_replace(monkeypatch, OSError("disk full"), after_swap=True)
    with pytest.raises(OSError):
        store.owner_update("envelope.yaml", "schema_version: 1\n", OWNER_OK)
    assert not path.exists()
    assert store._signature("envelope.yaml") is None
    assert _no_tmp_left(path)


class _CommitFails:
    """Connection proxy whose COMMIT fails: SQLite then keeps the transaction open (SQLITE_BUSY)."""

    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql, *args):
        if sql.strip().upper() == "COMMIT":
            raise sqlite3.OperationalError("database is locked")
        return self._conn.execute(sql, *args)

    def __getattr__(self, name):
        return getattr(self._conn, name)


def test_a_failed_commit_rolls_back_and_restores_the_old_bytes(tmp_path, conn, clock):
    cfg = tmp_path / "config"
    cfg.mkdir()
    path = cfg / "limits.yaml"
    path.write_text("schema_version: 1\nlimits: {}\n", encoding="utf-8")
    ProtectedStore(cfg, conn, clock).register_initial("limits.yaml", OWNER_OK)
    store = ProtectedStore(cfg, _CommitFails(conn), clock)
    before = _state(store, conn, path)
    with pytest.raises(sqlite3.OperationalError):
        store.owner_update("limits.yaml", NEW_CONTENT, OWNER_OK)
    assert not conn.in_transaction  # the open transaction was rolled back, not left for a later COMMIT
    assert _state(store, conn, path) == before
    assert _no_tmp_left(path)


def test_a_restore_failure_propagates_and_leaves_the_mismatch_detectable(store, conn, tmp_path, monkeypatch):
    # If the restore itself fails the file and signature disagree; load_verified must then refuse (fail
    # closed), and no restore temp file may linger.
    import engine.core.protected_store as ps

    store.register_initial("limits.yaml", OWNER_OK)
    path = _limits(tmp_path)
    real_replace = ps.os.replace
    calls = []

    def replace(src, dst):
        calls.append(src)
        if len(calls) == 1:
            real_replace(src, dst)
            raise OSError("disk full")
        raise OSError("still full")

    monkeypatch.setattr(ps.os, "replace", replace)
    with pytest.raises(OSError, match="still full") as info:
        store.owner_update("limits.yaml", NEW_CONTENT, OWNER_OK)
    assert "disk full" in str(info.value.__context__)
    assert store.verify("limits.yaml") is False
    assert _no_tmp_left(path)


# Fix round 1: the restore target comes from durable state (design §4 invariant), temp names carry the pid.
class _CommitThenInterrupt:
    """COMMIT really commits, then a KeyboardInterrupt lands (a SIGINT pending during COMMIT)."""

    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql, *args):
        result = self._conn.execute(sql, *args)
        if sql.strip().upper() == "COMMIT":
            raise KeyboardInterrupt
        return result

    def __getattr__(self, name):
        return getattr(self._conn, name)


class _CommitFailsThenRollbackInterrupted:
    """COMMIT fails leaving the transaction open; the ROLLBACK runs, then a KeyboardInterrupt lands."""

    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql, *args):
        verb = sql.strip().upper()
        if verb == "COMMIT":
            raise sqlite3.OperationalError("database is locked")
        result = self._conn.execute(sql, *args)
        if verb == "ROLLBACK":
            raise KeyboardInterrupt
        return result

    def __getattr__(self, name):
        return getattr(self._conn, name)


class _CommitAndRollbackFail:
    """COMMIT fails and ROLLBACK fails without running: the transaction stays open, durable state unknown."""

    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql, *args):
        if sql.strip().upper() in ("COMMIT", "ROLLBACK"):
            raise sqlite3.OperationalError("database is locked")
        return self._conn.execute(sql, *args)

    def __getattr__(self, name):
        return getattr(self._conn, name)


def _proxied_store(tmp_path, conn, clock, proxy_cls):
    cfg = tmp_path / "config"
    cfg.mkdir()
    path = cfg / "limits.yaml"
    path.write_bytes(b"schema_version: 1\nlimits: {}\n")
    plain = ProtectedStore(cfg, conn, clock)
    plain.register_initial("limits.yaml", OWNER_OK)
    return plain, ProtectedStore(cfg, proxy_cls(conn), clock), path


def test_an_interrupt_after_a_landed_commit_keeps_the_new_bytes(tmp_path, conn, clock):
    # Review probe A: the signature committed, so the new bytes must stay (restoring the old ones would
    # leave old bytes under the new signature).
    plain, store, path = _proxied_store(tmp_path, conn, clock, _CommitThenInterrupt)
    _, sha = plain.read_verified_bytes("limits.yaml")
    with pytest.raises(KeyboardInterrupt):
        store.owner_update("limits.yaml", NEW_CONTENT, OWNER_OK, expected_sha256=sha)
    assert path.read_bytes() == NEW_CONTENT.encode("utf-8")
    assert plain._signature("limits.yaml") == sha256_bytes(NEW_CONTENT.encode("utf-8"))
    assert plain.verify("limits.yaml") is True
    assert _no_tmp_left(path)


def test_an_interrupt_during_the_rollback_still_restores_the_old_bytes(tmp_path, conn, clock):
    # Review probe B: a BaseException raised by the ROLLBACK must not skip the restore.
    plain, store, path = _proxied_store(tmp_path, conn, clock, _CommitFailsThenRollbackInterrupted)
    before = _state(plain, conn, path)
    with pytest.raises(KeyboardInterrupt):
        store.owner_update("limits.yaml", NEW_CONTENT, OWNER_OK)
    assert not conn.in_transaction
    assert _state(plain, conn, path) == before
    assert plain.verify("limits.yaml") is True
    assert _no_tmp_left(path)


def test_unknown_durable_state_falls_back_to_the_old_bytes_and_logs_critical(tmp_path, conn, clock, caplog):
    plain, store, path = _proxied_store(tmp_path, conn, clock, _CommitAndRollbackFail)
    old = path.read_bytes()
    with caplog.at_level("CRITICAL", logger="engine.core.protected_store"):
        with pytest.raises(sqlite3.OperationalError):
            store.owner_update("limits.yaml", NEW_CONTENT, OWNER_OK)
    assert path.read_bytes() == old
    assert "protected_config.restore_unverified" in caplog.messages
    assert _no_tmp_left(path)
    conn.execute("ROLLBACK")  # what the engine's next failure handling would see: the old signature
    assert plain.verify("limits.yaml") is True


def test_an_in_transaction_cas_refusal_leaves_another_writers_signed_file(store, conn, tmp_path, monkeypatch):
    # Another process commits and signs its own bytes after our pre-checks, before our UPDATE. Our CAS
    # refuses, and the restore must not put our old bytes back over that signed file.
    import engine.core.protected_store as ps

    store.register_initial("limits.yaml", OWNER_OK)
    path = _limits(tmp_path)
    _, sha = store.read_verified_bytes("limits.yaml")
    theirs = b"schema_version: 1\nlimits: {theirs: 1}\n"
    real_write = ps._write_synced

    def other_writer_lands_first(target, data):
        if not target.name.endswith(".restore.tmp") and path.read_bytes() != theirs:
            path.write_bytes(theirs)
            conn.execute("UPDATE protected_config SET sha256=? WHERE name='limits.yaml'", (sha256_bytes(theirs),))
        real_write(target, data)

    monkeypatch.setattr(ps, "_write_synced", other_writer_lands_first)
    with pytest.raises(IntegrityError, match="signature changed since it was read"):
        store.owner_update("limits.yaml", NEW_CONTENT, OWNER_OK, expected_sha256=sha)
    assert path.read_bytes() == theirs
    assert store.verify("limits.yaml") is True
    assert _no_tmp_left(path)


def test_the_temp_names_carry_the_pid(store, tmp_path, monkeypatch):
    import engine.core.protected_store as ps

    store.register_initial("limits.yaml", OWNER_OK)
    real_replace = ps.os.replace
    sources = []

    def replace(src, dst):
        sources.append(Path(src).name)
        real_replace(src, dst)
        if len(sources) == 1:
            raise OSError("disk full")  # after the swap: forces the restore's own temp file

    monkeypatch.setattr(ps.os, "replace", replace)
    with pytest.raises(OSError):
        store.owner_update("limits.yaml", NEW_CONTENT, OWNER_OK)
    pid = ps.os.getpid()
    assert sources == [f".limits.yaml.{pid}.tmp", f".limits.yaml.{pid}.restore.tmp"]
    assert _no_tmp_left(_limits(tmp_path))


def test_seed_path_keeps_lf_bytes_and_leaves_no_tmp(store, tmp_path):
    path = _limits(tmp_path)
    original = b"schema_version: 1\nlimits: {}  # O1 provenance\n"
    path.write_bytes(original)
    store.register_initial("limits.yaml", OWNER_OK)
    assert path.read_bytes() == original
    assert store._signature("limits.yaml") == sha256_bytes(original)
    store.register_initial("limits.yaml", OWNER_OK)  # --reseed of an unchanged file
    assert path.read_bytes() == original
    assert _no_tmp_left(path)


def test_the_shipped_limits_file_round_trips_through_a_cas_write(store, tmp_path):
    # Seam: the real limits.yaml (a tmp copy) read verified, decoded, and written back by CAS keeps its
    # exact bytes, including any non-ASCII provenance comments.
    shipped = Path(__file__).resolve().parents[2] / "config" / "limits.yaml"
    path = _limits(tmp_path)
    path.write_bytes(shipped.read_bytes())
    store.register_initial("limits.yaml", OWNER_OK)
    raw, sha = store.read_verified_bytes("limits.yaml")
    store.owner_update("limits.yaml", raw.decode("utf-8"), OWNER_OK, expected_sha256=sha)
    assert path.read_bytes() == raw
    assert store.read_verified_bytes("limits.yaml") == (raw, sha)


def test_seed_path_still_normalises_crlf_to_lf(store, tmp_path):
    # register_initial reads text mode, so a CRLF file is re-written LF and the LF bytes are signed.
    path = _limits(tmp_path)
    path.write_bytes(b"schema_version: 1\r\nlimits: {}\r\n")
    store.register_initial("limits.yaml", OWNER_OK)
    assert path.read_bytes() == b"schema_version: 1\nlimits: {}\n"
    assert store.verify("limits.yaml") is True
