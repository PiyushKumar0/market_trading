"""Retention of state snapshots / NSSM service logs (§10.5) and the stderr level split (core.log)."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

import pytest

from engine.core import log as core_log
from engine.core.clock import IST
from engine.ops.main import _prune_retained_files

NOW = datetime(2026, 10, 4, 21, 0, tzinfo=IST)


def snapshot(backups, at: datetime):
    p = backups / f"state-{at:%Y%m%dT%H%M%S}.db"
    p.write_bytes(b"")
    return p


def service_log(logs, name: str, age_days: float):
    p = logs / name
    p.write_text("x")
    t = (NOW - timedelta(days=age_days)).timestamp()
    os.utime(p, (t, t))
    return p


@pytest.fixture
def dirs(tmp_path):
    backups, logs = tmp_path / "backups", tmp_path / "logs"
    backups.mkdir()
    logs.mkdir()
    return backups, logs


def test_keeps_recent_snapshots_and_one_per_week_then_drops_past_90_days(dirs):
    backups, logs = dirs
    recent = [snapshot(backups, NOW - timedelta(days=d, hours=h)) for d in (0, 5, 13) for h in (0, 12)]
    # Two snapshots in one ISO week 30+ days back: only the newer survives.
    week_new = snapshot(backups, datetime(2026, 9, 3, 21, 0, tzinfo=IST))     # Thu, week 36
    week_old = snapshot(backups, datetime(2026, 9, 1, 21, 0, tzinfo=IST))     # Tue, week 36
    ancient = snapshot(backups, NOW - timedelta(days=120))

    report = _prune_retained_files(backups, logs, NOW)

    assert all(p.exists() for p in recent)
    assert week_new.exists() and not week_old.exists()
    assert not ancient.exists()
    assert report == {"snapshots_deleted": 2, "service_logs_deleted": 0}


def test_newest_snapshot_survives_even_when_ancient(dirs):
    backups, logs = dirs
    only = snapshot(backups, NOW - timedelta(days=200))
    older = snapshot(backups, NOW - timedelta(days=300))

    _prune_retained_files(backups, logs, NOW)

    assert only.exists() and not older.exists()


def test_unparseable_and_foreign_files_are_left_alone(dirs):
    backups, logs = dirs
    odd = backups / "state-manual-copy.db"
    odd.write_bytes(b"")
    foreign = backups / "market_pre_x.duckdb"
    foreign.write_bytes(b"")

    _prune_retained_files(backups, logs, NOW)

    assert odd.exists() and foreign.exists()


def test_rotated_service_logs_past_90_days_go_but_live_logs_stay(dirs):
    backups, logs = dirs
    old = service_log(logs, "service.err-20260601T000000.000.log", 100)
    young = service_log(logs, "service.out-20260920T000000.000.log", 10)
    live = service_log(logs, "service.err.log", 100)          # the file NSSM is writing
    engine = service_log(logs, "engine.log.2026-06-01", 100)  # core.log's own rotation

    report = _prune_retained_files(backups, logs, NOW)

    assert not old.exists()
    assert young.exists() and live.exists() and engine.exists()
    assert report["service_logs_deleted"] == 1


@pytest.fixture
def fresh_root_logging(monkeypatch):
    root = logging.getLogger()
    saved_handlers, saved_level = list(root.handlers), root.level
    monkeypatch.setattr(core_log, "_CONFIGURED", False)
    yield root
    for h in list(root.handlers):
        root.removeHandler(h)
        h.close()
    for h in saved_handlers:
        root.addHandler(h)
    root.setLevel(saved_level)


def stderr_handler(root):
    return next(h for h in root.handlers if type(h) is logging.StreamHandler)


def test_stderr_is_warning_only_when_a_log_file_exists(fresh_root_logging, tmp_path):
    core_log.configure_logging(logs_dir=tmp_path)
    assert stderr_handler(fresh_root_logging).level == logging.WARNING


def test_stderr_keeps_everything_without_a_log_file(fresh_root_logging):
    core_log.configure_logging()
    assert stderr_handler(fresh_root_logging).level == logging.NOTSET
