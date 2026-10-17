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


def test_restore_refuses_a_missing_protected_config_row(signed, conn):
    cfg, _ = signed
    conn.execute("DELETE FROM protected_config WHERE name = 'limits.yaml'")
    with pytest.raises(RuntimeError, match="no signed snapshot"):
        pcs.restore(conn, cfg, "limits.yaml")


def _argv(cfg, db_path, *extra):
    return ["--state-db", str(db_path), "--config-dir", str(cfg), *extra]


def test_main_exit_codes_0_1_and_2(signed, conn, db_path, capsys):
    cfg, _ = signed
    conn.commit()
    assert pcs.main(_argv(cfg, db_path)) == 0                       # every file matches
    (cfg / "limits.yaml").write_bytes(b"capital_base_inr: 1\n")
    assert pcs.main(_argv(cfg, db_path)) == 1                       # mismatch
    capsys.readouterr()
    assert pcs.main(_argv(cfg, db_path, "--restore", "settings.yaml")) == 2   # refusal
    err = capsys.readouterr().err
    assert err.startswith("error: ") and err.count("\n") == 1
    assert pcs.main(_argv(cfg, db_path, "--restore", "limits.yaml")) == 0
    assert pcs.main(_argv(cfg, db_path)) == 0


def test_restore_with_a_missing_state_db_errors_and_creates_nothing(signed, tmp_path, capsys):
    cfg, _ = signed
    missing = tmp_path / "nowhere" / "state.db"
    assert pcs.main(_argv(cfg, missing, "--restore", "limits.yaml")) == 2
    assert not missing.exists() and not missing.parent.exists()
    missing = tmp_path / "state_missing.db"
    assert pcs.main(_argv(cfg, missing, "--restore", "limits.yaml")) == 2
    assert not missing.exists()
    assert "error:" in capsys.readouterr().err


def test_a_failed_replace_leaves_no_tmp_and_the_target_unchanged(signed, conn, monkeypatch):
    cfg, store = signed
    path = cfg / "limits.yaml"
    old = path.read_bytes()
    store.owner_update("limits.yaml", old.decode("utf-8").replace(
        "capital_base_inr: 40000", "capital_base_inr: 60000", 1), OWNER)
    path.write_bytes(old)

    def boom(src, dst):
        raise OSError("replace refused")

    monkeypatch.setattr(pcs.os, "replace", boom)
    with pytest.raises(OSError, match="replace refused"):
        pcs.restore(conn, cfg, "limits.yaml")
    assert path.read_bytes() == old
    assert not list(cfg.glob(".limits.yaml*.tmp"))
