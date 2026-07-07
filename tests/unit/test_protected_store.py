"""Protected-store integrity (R4, §2.4). Tamper detection + owner-only write path."""

from __future__ import annotations

import pytest

from engine.core.enums import Actor
from engine.core.protected_store import IntegrityError, ProtectedStore, UnauthorizedUpdate
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
