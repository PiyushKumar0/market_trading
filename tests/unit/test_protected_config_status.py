"""scripts/protected_config_status.py: the O19 recovery path for a limits.yaml reverted by git under a
newer signature. It may only ever put back the exact bytes the signature covers."""

from __future__ import annotations

import importlib.util
import shutil
from pathlib import Path

import pytest

from engine.core.clock import Clock
from engine.core.config import config_dir
from engine.core.enums import Actor
from engine.core.protected_store import ProtectedStore
from engine.core.types import OwnerConfirmation

_SPEC = importlib.util.spec_from_file_location(
    "protected_config_status", Path(__file__).resolve().parents[2] / "scripts" / "protected_config_status.py"
)
pcs = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(pcs)

OWNER = OwnerConfirmation(actor=Actor.OWNER, confirmed=True, note="test")


@pytest.fixture
def signed(tmp_path, conn):
    cfg = tmp_path / "config"
    cfg.mkdir()
    for name in pcs.NAMES:
        shutil.copyfile(config_dir() / name, cfg / name)
    store = ProtectedStore(cfg, conn, Clock())
    for name in pcs.NAMES:
        store.register_initial(name, OWNER)
    return cfg, store


def test_status_reports_every_file_matching_after_registration(signed, conn):
    cfg, _ = signed
    rows = pcs.status(conn, cfg)
    assert [r["name"] for r in rows] == list(pcs.NAMES)
    assert all(r["recorded"] == r["snapshot"] == r["on_disk"] for r in rows)


def test_a_git_revert_under_a_newer_signature_is_detected_and_restored(signed, conn):
    """A dashboard change re-signs NEW bytes; a git checkout then puts the OLD bytes back on disk."""
    cfg, store = signed
    path = cfg / "limits.yaml"
    old = path.read_bytes()
    new = old.replace(b"capital_base_inr: 40000", b"capital_base_inr: 60000", 1)
    assert new != old
    store.owner_update("limits.yaml", new.decode("utf-8"), OWNER)
    path.write_bytes(old)                                    # the git revert
    row = next(r for r in pcs.status(conn, cfg) if r["name"] == "limits.yaml")
    assert row["on_disk"] != row["recorded"] == row["snapshot"]

    assert "restored" in pcs.restore(conn, cfg, "limits.yaml")
    assert path.read_bytes() == new
    store.load_verified("limits.yaml")                       # the boot's check now passes
    assert "nothing written" in pcs.restore(conn, cfg, "limits.yaml")
    assert not list(cfg.glob(".limits.yaml*.tmp"))


def test_restore_refuses_a_snapshot_that_does_not_match_the_signature(signed, conn):
    cfg, _ = signed
    conn.execute("UPDATE protected_config SET content_snapshot = 'tampered: 1' WHERE name = 'limits.yaml'")
    before = (cfg / "limits.yaml").read_bytes()
    with pytest.raises(RuntimeError, match="does not hash"):
        pcs.restore(conn, cfg, "limits.yaml")
    assert (cfg / "limits.yaml").read_bytes() == before


def test_restore_refuses_an_unprotected_name(signed, conn):
    cfg, _ = signed
    with pytest.raises(ValueError):
        pcs.restore(conn, cfg, "settings.yaml")
