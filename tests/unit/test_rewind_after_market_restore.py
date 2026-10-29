"""scripts/rewind_after_market_restore.py: rewinding state.db after a market.duckdb restore (§10.5).

Scratch ``state.db`` built with the repo's migrations (the ``conn`` fixture's recipe); the script is
loaded with the loose-script mechanism of tests/unit/test_backfill_bhavcopy.py.

Timeline: export ``market-20261012T220012`` (Mon 22:00:12 IST) ⇒ S = 2026-10-12, inclusive; the run
happens on Thu 2026-10-15. 10-09 is the previous trading day (Fri); 10-10/10-11 are the weekend.
"""

from __future__ import annotations

import asyncio
import importlib.util
import inspect
import sqlite3
import sys
from datetime import date, datetime, time
from pathlib import Path

import pytest

from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.config import Settings, config_dir
from engine.core.db import connect
from engine.core.migrations import apply_migrations
from engine.marketdata.backfill import BackfillJob
from engine.ops.jobs import CatchUpRunner, JobClass, JobRegistry, JobSpec
from engine.ops.single_instance import InstanceLock

_SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "rewind_after_market_restore.py"
_spec = importlib.util.spec_from_file_location("mt_rewind_after_market_restore", _SCRIPT_PATH)
mod = importlib.util.module_from_spec(_spec)
sys.modules["mt_rewind_after_market_restore"] = mod
_spec.loader.exec_module(mod)

EXPORT = "market-20261012T220012"
S = date(2026, 10, 12)
TODAY = date(2026, 10, 15)

#: (job_id, run_for_date, status, last_success_at) — the date-keyed jobs with DuckDB output, the ones
#: whose output survives the restore, and one run-latest job.
_JOB_ROWS = [
    ("bhavcopy", "2026-10-09", "success", "2026-10-09T18:01:00+05:30"),
    ("bhavcopy", "2026-10-12", "success", "2026-10-12T18:01:00+05:30"),
    ("bhavcopy", "2026-10-13", "success", "2026-10-13T18:01:00+05:30"),
    ("bhavcopy", "2026-10-14", "success", "2026-10-14T18:01:00+05:30"),
    ("daily_bars", "2026-10-09", "success", "2026-10-09T18:06:00+05:30"),
    ("daily_bars", "2026-10-12", "success", "2026-10-12T18:06:00+05:30"),
    ("daily_bars", "2026-10-14", "success", "2026-10-14T18:06:00+05:30"),
    ("deals", "2026-10-14", "failed", None),
    ("features_daily", "2026-10-13", "success", "2026-10-13T20:46:00+05:30"),
    ("bar_reconcile", "2026-10-13", "success", "2026-10-13T15:55:00+05:30"),
    ("filings_pit", "2026-10-13", "success", "2026-10-13T18:36:00+05:30"),
    ("filings_pit_fresh", "2026-10-13", "success", "2026-10-13T19:01:00+05:30"),
    ("filings_results", "2026-10-13", "success", "2026-10-13T18:46:00+05:30"),
    ("ins_crossings", "2026-10-13", "success", "2026-10-13T19:16:00+05:30"),
    ("tick_compact", "2026-10-13", "success", "2026-10-13T22:31:00+05:30"),
    ("nightly_review", "2026-10-13", "success", "2026-10-13T21:01:00+05:30"),
    ("rec_outcomes", "2026-10-13", "success", "2026-10-13T18:20:00+05:30"),
    ("signal_outcomes", "2026-10-13", "success", "2026-10-13T18:41:00+05:30"),
    ("universe_build", "2026-10-14", "success", "2026-10-14T08:31:00+05:30"),
]

_CHECKPOINTS = [
    ("RELIANCE", "day", "2026-10-14"),
    ("NIFTY 50", "day", "2026-10-14"),
    ("INDIA VIX", "day", "2026-10-12"),
    ("TCS", "day", "2026-10-09"),
    ("RELIANCE", "minute", "2026-10-14"),
    ("INFY", "minute", "2026-10-08"),
]


# --------------------------------------------------------------------------- fixtures
@pytest.fixture
def env(tmp_path):
    data = tmp_path / "data"
    cfg = tmp_path / "config"
    cfg.mkdir()
    (cfg / "settings.yaml").write_text("{}\n", encoding="utf-8")
    (data / "backups" / EXPORT).mkdir(parents=True)
    c = connect(data / "state.db")
    apply_migrations(c)
    c.executemany(
        "INSERT INTO job_runs (job_id, run_for_date, last_success_at, last_attempt_at, status) "
        "VALUES (?, ?, ?, ?, ?)",
        [(j, d, at, at or "2026-10-14T20:31:00+05:30", st) for j, d, st, at in _JOB_ROWS],
    )
    c.executemany(
        "INSERT INTO backfill_checkpoints (symbol, interval, through_date) VALUES (?, ?, ?)",
        _CHECKPOINTS,
    )
    c.close()
    return data, cfg


def _run(env, *extra: str) -> int:
    data, cfg = env
    return mod.main([*extra, "--data-dir", str(data), "--config-dir", str(cfg)], today=TODAY)


def _dump(data: Path) -> list[str]:
    c = sqlite3.connect(data / "state.db")
    try:
        return list(c.iterdump())
    finally:
        c.close()


def _job_rows(data: Path) -> set[tuple[str, str]]:
    c = sqlite3.connect(data / "state.db")
    try:
        return set(c.execute("SELECT job_id, run_for_date FROM job_runs").fetchall())
    finally:
        c.close()


def _checkpoints(data: Path) -> dict[tuple[str, str], str]:
    c = sqlite3.connect(data / "state.db")
    try:
        rows = c.execute("SELECT symbol, interval, through_date FROM backfill_checkpoints").fetchall()
    finally:
        c.close()
    return {(s, i): t for s, i, t in rows}


def _set_engine_state(data: Path, state: str) -> None:
    c = sqlite3.connect(data / "state.db")
    try:
        c.execute("UPDATE engine_lifecycle SET state=?, pid=? WHERE id=1", (state, 999_999))
        c.commit()
    finally:
        c.close()


def _lifecycle(data: Path) -> list[tuple]:
    c = sqlite3.connect(data / "state.db")
    try:
        return c.execute("SELECT * FROM engine_lifecycle").fetchall()
    finally:
        c.close()


def _add_job_rows(data: Path, rows: list[tuple[str, str, str, str | None]]) -> None:
    c = sqlite3.connect(data / "state.db")
    try:
        c.executemany(
            "INSERT INTO job_runs (job_id, run_for_date, last_success_at, last_attempt_at, status) "
            "VALUES (?, ?, ?, ?, ?)",
            [(j, d, at, at or "2026-10-15T08:16:00+05:30", st) for j, d, st, at in rows],
        )
        c.commit()
    finally:
        c.close()


# --------------------------------------------------------------------------- stamp parsing
@pytest.mark.parametrize("arg", [
    "market-20261012T220012",
    r"data\backups\market-20261012T220012",
    "data/backups/market-20261012T220012/",
    r"C:\x\backups\market-20261012T220012" + "\\",
])
def test_export_stamp_parses_name_or_path(arg):
    assert mod.parse_export_stamp(arg) == datetime(2026, 10, 12, 22, 0, 12, tzinfo=IST)


@pytest.mark.parametrize("arg", [
    "market-20261012T220012.tmp",     # an unfinished export (the drill ignores *.tmp too)
    "market-20261012",
    "market-2026-10-12T22:00:12",
    "state-20261012T220012",
    "market-20261399T220012",         # not a date
    "",
])
def test_export_stamp_rejects_anything_else(arg):
    with pytest.raises(ValueError):
        mod.parse_export_stamp(arg)


# --------------------------------------------------------------------------- dry run
def test_dry_run_changes_nothing(env, capsys):
    data, _ = env
    before = _dump(data)
    assert _run(env, "--export", EXPORT) == 0
    assert _dump(data) == before
    out = capsys.readouterr().out
    assert "DRY RUN" in out
    assert "S = 2026-10-12" in out
    assert "bhavcopy: 2026-10-12, 2026-10-13, 2026-10-14" in out
    assert "day 2026-10-14 -> 2026-10-11 (2): NIFTY 50, RELIANCE" in out
    assert "kept: deals 2026-10-14 (failed)" in out
    assert "--apply" in out


def test_failed_row_is_kept_and_still_replayed(env):
    """Keeping a failed row loses nothing: the real catch-up still scans it (include_failed)."""
    data, _ = env
    assert _run(env, "--export", EXPORT, "--apply") == 0
    c = connect(data / "state.db")
    try:
        row = c.execute(
            "SELECT status FROM job_runs WHERE job_id='deals' AND run_for_date='2026-10-14'"
        ).fetchone()
    finally:
        c.close()
    assert row["status"] == "failed"
    assert date(2026, 10, 14) in _missed_after_rewind(data, "deals", time(20, 30))


def _missed_after_rewind(data: Path, job_id: str, at: time) -> list[date]:
    """``CatchUpRunner._missed_days`` for one date-keyed job at Thu 10:00, over the real state.db."""
    now = datetime(2026, 10, 15, 10, 0, tzinfo=IST)
    clock = Clock(time_source=lambda: now)
    calendar = NSECalendar(config_dir() / "calendar", clock, strict=False)

    async def _noop(d: date) -> None:
        return None

    spec = JobSpec(job_id=job_id, job_class=JobClass.DATE_KEYED, at=at, run=_noop)
    reg = JobRegistry()
    reg.register(spec)
    c = connect(data / "state.db")
    try:
        return CatchUpRunner(c, clock, calendar, reg)._missed_days(spec, now, None, include_failed=True)
    finally:
        c.close()


def test_dry_run_is_allowed_while_the_engine_runs(env, capsys):
    data, _ = env
    _set_engine_state(data, "RUNNING")
    before = _dump(data)
    assert _run(env, "--export", EXPORT) == 0
    assert _dump(data) == before
    out = capsys.readouterr().out
    assert "--apply would be refused" in out
    assert "--engine-crashed" in out


# --------------------------------------------------------------------------- apply
def test_apply_deletes_exactly_the_replayable_job_rows(env):
    data, _ = env
    assert _run(env, "--export", EXPORT, "--apply") == 0
    assert _job_rows(data) == {
        ("bhavcopy", "2026-10-09"),            # before S: kept
        ("daily_bars", "2026-10-09"),
        ("deals", "2026-10-14"),               # failed: the catch-up already retries it; streak kept
        ("ins_crossings", "2026-10-13"),       # state.db output, never lost
        ("tick_compact", "2026-10-13"),        # parquet output, outside market.duckdb
        ("nightly_review", "2026-10-13"),      # LLM; state.db output
        ("rec_outcomes", "2026-10-13"),        # state.db output
        ("signal_outcomes", "2026-10-13"),     # state.db output
        ("universe_build", "2026-10-14"),      # run-latest: not a per-day replay
    }


def test_apply_rewinds_checkpoints_for_every_symbol_including_the_index(env):
    data, _ = env
    assert _run(env, "--export", EXPORT, "--apply") == 0
    assert _checkpoints(data) == {
        ("RELIANCE", "day"): "2026-10-11",
        ("NIFTY 50", "day"): "2026-10-11",
        ("INDIA VIX", "day"): "2026-10-11",   # through_date == S is rewound too (S is inclusive)
        ("TCS", "day"): "2026-10-09",         # behind S: untouched
        ("RELIANCE", "minute"): "2026-10-11",
        ("INFY", "minute"): "2026-10-08",
    }


def test_since_gives_the_same_rewind_as_the_export(env):
    data, _ = env
    assert _run(env, "--since", "2026-10-12", "--apply") == 0
    assert ("bhavcopy", "2026-10-12") not in _job_rows(data)
    assert _checkpoints(data)[("NIFTY 50", "day")] == "2026-10-11"


def test_apply_writes_a_pre_rewind_copy_once(env):
    data, _ = env
    before = _dump(data)
    assert _run(env, "--export", EXPORT, "--apply") == 0
    copies = sorted(data.glob("state.db.pre-rewind-*"))
    assert len(copies) == 1
    c = sqlite3.connect(copies[0])
    try:
        assert list(c.iterdump()) == before
    finally:
        c.close()
    assert _run(env, "--export", EXPORT, "--apply") == 0     # nothing to change: no second copy
    assert sorted(data.glob("state.db.pre-rewind-*")) == copies


def test_apply_is_idempotent(env, capsys):
    data, _ = env
    assert _run(env, "--export", EXPORT, "--apply") == 0
    after_first = _dump(data)
    capsys.readouterr()
    assert _run(env, "--export", EXPORT, "--apply") == 0
    assert _dump(data) == after_first
    assert "nothing to change" in capsys.readouterr().out


def test_success_recorded_after_the_export_pulls_s_back(env, capsys):
    """An export taken before an earlier day's catch-up finished lacks that day's data: a success
    recorded after the stamp moves S back to its run_for_date."""
    data, _ = env
    c = sqlite3.connect(data / "state.db")
    c.execute(
        "UPDATE job_runs SET last_success_at='2026-10-12T23:10:00+05:30' "
        "WHERE job_id='bhavcopy' AND run_for_date='2026-10-09'"
    )
    c.commit()
    c.close()
    assert _run(env, "--export", EXPORT, "--apply") == 0
    assert "S = 2026-10-09" in capsys.readouterr().out
    rows = _job_rows(data)
    assert ("bhavcopy", "2026-10-09") not in rows
    assert ("daily_bars", "2026-10-09") not in rows
    assert _checkpoints(data)[("TCS", "day")] == "2026-10-08"


# --------------------------------------------------------------------------- today's snapshot jobs
@pytest.mark.parametrize("success_job, failed_job", [
    ("universe_build", "instruments"),
    ("instruments", "universe_build"),
])
def test_same_day_s_deletes_todays_snapshot_success_rows(env, capsys, success_job, failed_job):
    """S = today: today's universe_daily / instruments_daily are not in the restored file, so the
    success row goes and the boot catch-up rebuilds them; a failed row is kept (already retried)."""
    data, _ = env
    _add_job_rows(data, [
        (success_job, "2026-10-15", "success", "2026-10-15T08:31:00+05:30"),
        (failed_job, "2026-10-15", "failed", None),
    ])
    assert _run(env, "--since", "2026-10-15") == 0
    out = capsys.readouterr().out
    assert f"{success_job} 2026-10-15" in out
    assert ("instruments is SAFETY_CRITICAL" in out) == (success_job == "instruments")
    assert f"kept: {failed_job} 2026-10-15 (failed)" in out
    assert _run(env, "--since", "2026-10-15", "--apply") == 0
    rows = _job_rows(data)
    assert (success_job, "2026-10-15") not in rows
    assert (failed_job, "2026-10-15") in rows
    assert ("universe_build", "2026-10-14") in rows      # an earlier day: run-latest never rebuilds it


def test_s_before_today_touches_only_todays_snapshot_rows(env):
    """S before today: the S..yesterday snapshot rows stay (nothing rebuilds a past day); today's
    success rows go, because an export stamped before today lacks today's 08:15/08:30 builds."""
    data, _ = env
    _add_job_rows(data, [
        ("universe_build", "2026-10-15", "success", "2026-10-15T08:31:00+05:30"),
        ("instruments", "2026-10-14", "success", "2026-10-14T08:16:00+05:30"),
    ])
    assert _run(env, "--export", EXPORT, "--apply") == 0
    rows = _job_rows(data)
    assert ("universe_build", "2026-10-15") not in rows
    assert ("universe_build", "2026-10-14") in rows
    assert ("instruments", "2026-10-14") in rows


def test_export_taken_after_todays_builds_keeps_todays_snapshot_rows(env, capsys):
    """An export stamped today after 08:31 holds today's universe_daily: rebuilding it after the
    close would rewrite the universe the day traded on."""
    data, _ = env
    (data / "backups" / "market-20261015T220005").mkdir()
    _add_job_rows(data, [("universe_build", "2026-10-15", "success", "2026-10-15T08:31:00+05:30")])
    assert _run(env, "--export", "market-20261015T220005", "--apply") == 0
    assert ("universe_build", "2026-10-15") in _job_rows(data)
    assert "kept: universe_build 2026-10-15 — recorded before the export" in capsys.readouterr().out


def test_catch_up_rebuilds_the_universe_today_after_the_rewind(env):
    """The real CatchUpRunner reads the rewound job_runs: universe_build (run-latest) is due today."""
    data, _ = env
    _add_job_rows(data, [("universe_build", "2026-10-15", "success", "2026-10-15T08:31:00+05:30")])
    assert _run(env, "--since", "2026-10-15", "--apply") == 0
    now = datetime(2026, 10, 15, 10, 0, tzinfo=IST)
    clock = Clock(time_source=lambda: now)
    calendar = NSECalendar(config_dir() / "calendar", clock, strict=False)

    async def _noop() -> None:
        return None

    spec = JobSpec(job_id="universe_build", job_class=JobClass.RUN_LATEST, at=time(8, 30), run=_noop)
    reg = JobRegistry()
    reg.register(spec)
    c = connect(data / "state.db")
    try:
        assert CatchUpRunner(c, clock, calendar, reg)._missed_days(spec, now, None) == [TODAY]
    finally:
        c.close()


def test_instruments_is_stale_again_in_session_after_the_rewind(env):
    """The real freshness predicate: in session, today's instruments run is due and unrecorded, so
    the boot re-runs it and entries stay frozen until it succeeds (fail closed)."""
    data, _ = env
    _add_job_rows(data, [("instruments", "2026-10-15", "success", "2026-10-15T08:16:00+05:30")])
    assert _run(env, "--since", "2026-10-15", "--apply") == 0
    now = datetime(2026, 10, 15, 10, 0, tzinfo=IST)
    clock = Clock(time_source=lambda: now)
    calendar = NSECalendar(config_dir() / "calendar", clock, strict=False)

    async def _noop() -> None:
        return None

    reg = JobRegistry()
    reg.register(JobSpec(job_id="instruments", job_class=JobClass.SAFETY_CRITICAL, at=time(8, 15),
                         run=_noop))
    c = connect(data / "state.db")
    try:
        assert CatchUpRunner(c, clock, calendar, reg).stale_safety_jobs(now) == ["instruments"]
    finally:
        c.close()


# --------------------------------------------------------------------------- refusals
@pytest.mark.parametrize("state", ["RUNNING", "STOPPING"])
def test_apply_refused_unless_engine_stopped(env, capsys, state):
    data, _ = env
    _set_engine_state(data, state)
    before = _dump(data)
    assert _run(env, "--export", EXPORT, "--apply") == 2
    assert _dump(data) == before
    assert not list(data.glob("state.db.pre-rewind-*"))
    err = capsys.readouterr().err
    assert f"engine_lifecycle.state is {state}" in err
    assert "--engine-crashed" in err


@pytest.mark.parametrize("state", ["RUNNING", "STOPPING"])
def test_engine_crashed_applies_when_the_lock_is_free(env, state):
    """A crash leaves RUNNING/STOPPING behind; a free engine.lock proves no engine is alive."""
    data, _ = env
    _set_engine_state(data, state)
    lifecycle = _lifecycle(data)
    assert _run(env, "--export", EXPORT, "--apply", "--engine-crashed") == 0
    assert ("bhavcopy", "2026-10-12") not in _job_rows(data)
    assert _checkpoints(data)[("NIFTY 50", "day")] == "2026-10-11"
    assert _lifecycle(data) == lifecycle        # the next boot still sees the crash and recovers
    lock = InstanceLock(data / "engine.lock")   # released once the transaction is done
    try:
        assert lock.acquire()
    finally:
        lock.release()


def test_engine_crashed_refused_while_the_engine_lock_is_held(env, capsys):
    data, _ = env
    _set_engine_state(data, "RUNNING")
    lock = InstanceLock(data / "engine.lock")
    assert lock.acquire()
    try:
        before = _dump(data)
        assert _run(env, "--export", EXPORT, "--apply", "--engine-crashed") == 2
        assert _dump(data) == before
        assert not list(data.glob("state.db.pre-rewind-*"))
        err = capsys.readouterr().err
        assert "engine.lock" in err
        assert "engine is running" in err
    finally:
        lock.release()


def test_engine_crashed_still_needs_a_lifecycle_row(env, capsys):
    """The flag widens STOPPED to the two crash states only, never to an unknown state."""
    data, _ = env
    c = sqlite3.connect(data / "state.db")
    c.execute("DELETE FROM engine_lifecycle")
    c.commit()
    c.close()
    before = _dump(data)
    assert _run(env, "--export", EXPORT, "--apply", "--engine-crashed") == 2
    assert _dump(data) == before
    assert "engine_lifecycle.state is None" in capsys.readouterr().err


def test_apply_refused_while_the_engine_lock_is_held(env, capsys):
    data, _ = env
    lock = InstanceLock(data / "engine.lock")
    assert lock.acquire()
    try:
        before = _dump(data)
        assert _run(env, "--export", EXPORT, "--apply") == 2
        assert _dump(data) == before
        assert "engine.lock" in capsys.readouterr().err
    finally:
        lock.release()


def test_missing_export_dir_is_refused(env, capsys):
    assert _run(env, "--export", "market-20261013T220000") == 2
    assert "not found" in capsys.readouterr().err


def test_future_since_is_refused(env):
    assert _run(env, "--since", "2026-10-16") == 2


def test_missing_state_db_is_refused_and_not_created(tmp_path):
    cfg = tmp_path / "config"
    cfg.mkdir()
    (cfg / "settings.yaml").write_text("{}\n", encoding="utf-8")
    data = tmp_path / "data"
    rc = mod.main(["--since", "2026-10-12", "--data-dir", str(data), "--config-dir", str(cfg)],
                  today=TODAY)
    assert rc == 2
    assert not (data / "state.db").exists()


def test_lookback_matches_the_catch_up_runner_default():
    default = inspect.signature(CatchUpRunner.__init__).parameters["max_lookback_days"].default
    assert mod.CATCHUP_LOOKBACK_DAYS == default


# --------------------------------------------------------------------------- seams: real consumers
def test_catch_up_replays_from_s_after_the_rewind(env):
    """The real CatchUpRunner reads the rewound job_runs: bhavcopy is due again from S."""
    data, _ = env
    assert _run(env, "--export", EXPORT, "--apply") == 0
    missed = _missed_after_rewind(data, "bhavcopy", time(18, 0))
    assert missed == [date(2026, 10, 12), date(2026, 10, 13), date(2026, 10, 14)]


class _RecordingKite:
    def __init__(self) -> None:
        self.calls: list[tuple[int, datetime, datetime, str]] = []

    async def historical(self, token, frm, to, interval):
        self.calls.append((token, frm, to, interval))
        return []


def test_backfill_refetches_the_index_from_s_after_the_rewind(env):
    """The real BackfillJob reads the rewound checkpoint: NIFTY 50 is fetched from S again."""
    data, _ = env
    assert _run(env, "--export", EXPORT, "--apply") == 0
    clock = Clock(time_source=lambda: datetime(2026, 10, 15, 19, 0, tzinfo=IST))
    kite = _RecordingKite()
    c = connect(data / "state.db")
    try:
        job = BackfillJob(None, kite, clock, Settings(), c, lambda _s: 256265)
        asyncio.run(job.run(["NIFTY 50"], "day", date(2024, 10, 15), date(2026, 10, 14)))
    finally:
        c.close()
    assert [(frm.date(), to.date()) for _t, frm, to, _i in kite.calls] == [(S, date(2026, 10, 14))]
