"""Retention of state snapshots / NSSM service logs (§10.5) and the stderr level split (core.log)."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

import pytest

from engine.core import log as core_log
from engine.core.clock import IST
from engine.ops.main import _prune_market_snapshots, _prune_retained_files

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


# --------------------------------------------------------------------------- DuckDB exports (§10.5)
def market_export(backups, at: datetime, *, tmp: bool = False):
    p = backups / f"market-{at:%Y%m%dT%H%M%S}{'.tmp' if tmp else ''}"
    p.mkdir()
    (p / "bars_1m.parquet").write_bytes(b"x")
    return p


def date_(month: int, day: int):
    return datetime(2026, month, day).date()


def test_market_snapshots_keep_newest_and_weekly_then_drop(dirs):
    backups, logs = dirs
    # NOW is Sun 2026-10-04 (ISO week 40). Nightly exports Mon-Fri for six weeks, 22:00.
    days = [NOW.date() - timedelta(days=d) for d in range(1, 43)]
    exports = {d: market_export(backups, datetime(d.year, d.month, d.day, 22, 0, tzinfo=IST))
               for d in days if d.weekday() < 5}
    stale_tmp = market_export(backups, datetime(2026, 9, 30, 22, 0, 30, tzinfo=IST), tmp=True)
    foreign_dir = backups / "market-manual"
    foreign_dir.mkdir()
    foreign_tmp = backups / "market-scratch.tmp"
    foreign_tmp.mkdir()
    state = snapshot(backups, NOW - timedelta(days=1))

    report = _prune_market_snapshots(backups, NOW)

    kept = sorted(d for d, p in exports.items() if p.exists())
    newest3 = sorted(exports)[-3:]                  # Wed 09-30, Thu 10-01, Fri 10-02 (week 40)
    assert newest3 == [date_(9, 30), date_(10, 1), date_(10, 2)]
    # Plus the newest of each older ISO week inside 28 days: Fri 09-25 (39), 09-18 (38), 09-11 (37).
    # Week 40's own older days (Mon/Tue) are covered by the newest three; week 36 is past 28 days.
    assert kept == [date_(9, 11), date_(9, 18), date_(9, 25), *newest3]
    assert report == {"market_snapshots_deleted": len(exports) - 6, "market_tmp_deleted": 1}
    assert not stale_tmp.exists()
    assert foreign_dir.exists() and foreign_tmp.exists() and state.exists()


def test_newest_market_snapshot_survives_even_when_ancient(dirs):
    backups, _logs = dirs
    only = market_export(backups, NOW - timedelta(days=200))

    report = _prune_market_snapshots(backups, NOW)

    assert only.exists() and report == {"market_snapshots_deleted": 0, "market_tmp_deleted": 0}


def test_market_prune_leaves_state_snapshot_pruning_alone(dirs):
    """The two prunes share data/backups and never touch each other's files."""
    backups, logs = dirs
    exports = [market_export(backups, NOW - timedelta(days=d)) for d in (200, 300)]
    old_state = snapshot(backups, NOW - timedelta(days=120))
    new_state = snapshot(backups, NOW)

    _prune_market_snapshots(backups, NOW)
    assert old_state.exists() and new_state.exists()
    report = _prune_retained_files(backups, logs, NOW)

    assert report["snapshots_deleted"] == 1 and not old_state.exists() and new_state.exists()
    assert all(p.exists() for p in exports)      # two exports: both inside the newest three


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
