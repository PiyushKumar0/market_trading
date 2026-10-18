"""§10.5 DuckDB leg (2026-10-11): ``MarketStore.export_snapshot`` and the ``market_backup`` lane job.

Nothing backed up ``market.duckdb`` (5.96 GB, 25 tables, several irreplaceable: LLM-scored news
clusters, feature snapshots the learning ledger references, point-in-time universe history). Windows
cannot copy the open file, so the backup is an ``EXPORT DATABASE`` on a cursor of the live store —
no store lock — written to a tmp dir and renamed into place once complete; ``IMPORT DATABASE``
restores it with its primary keys.
"""

from __future__ import annotations

import logging
import threading
import time as time_mod
from collections import namedtuple
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from types import SimpleNamespace

import duckdb
import pytest

from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.config import config_dir
from engine.core.types import Bar
from engine.marketdata import store as store_mod
from engine.marketdata.store import DailyBar, MarketStore
from engine.ops import main as opsmain
from engine.ops.jobs import JOB_MARKET_BACKUP, CatchUpRunner, CatchUpScope, JobClass, JobRegistry, JobSpec

WED = date(2026, 6, 17)                                   # a regular trading day
IN_SESSION = datetime(2026, 6, 17, 10, 0, tzinfo=IST)
NIGHT = datetime(2026, 6, 17, 22, 0, tzinfo=IST)


class MovableClock(Clock):
    def __init__(self, at: datetime) -> None:
        self.at = at
        super().__init__(time_source=lambda: self.at)


@pytest.fixture
def store(tmp_path):
    s = MarketStore(tmp_path / "market.duckdb", tmp_path / "parquet", MovableClock(NIGHT))
    s.open()
    yield s
    s.close()


def _seed(store: MarketStore) -> None:
    t0 = datetime(2026, 6, 16, 9, 15, tzinfo=IST)
    store.insert_bars_1m([
        Bar(symbol=sym, ts_minute=t0 + timedelta(minutes=i), open=Decimal("100.05"), high=Decimal("101"),
            low=Decimal("99.5"), close=Decimal("100.5"), volume=10 + i)
        for sym in ("RELIANCE", "TCS") for i in range(5)
    ])
    store.upsert_bars_1d([
        DailyBar(symbol="RELIANCE", d=date(2026, 6, 16), open=Decimal("100"), high=Decimal("102"),
                 low=Decimal("99"), close=Decimal("101.25"), volume=12345),
    ])


def _counts(con, tables) -> dict[str, int]:
    return {t: con.execute(f'SELECT count(*) FROM "{t}"').fetchone()[0] for t in tables}


def _tables(con) -> list[str]:
    return sorted(r[0] for r in con.execute(
        "SELECT table_name FROM duckdb_tables() WHERE schema_name = 'main' AND NOT temporary"
    ).fetchall())


def _pk_count(con) -> int:
    return con.execute(
        "SELECT count(*) FROM duckdb_constraints() WHERE constraint_type = 'PRIMARY KEY'"
    ).fetchone()[0]


def _big_table(store: MarketStore, rows: int = 10_000_000) -> None:
    """An extra table big enough that an export is still running when the test interrupts it."""
    store._require_con().execute(
        f"CREATE TABLE zz_big AS SELECT i AS a, random() AS b, md5(i::TEXT) AS c FROM range({rows}) t(i)"
    )


def _wait_for_export(store: MarketStore) -> None:
    deadline = time_mod.monotonic() + 10
    while store._export_cursor is None:
        assert time_mod.monotonic() < deadline, "export never started"
        time_mod.sleep(0.01)
    time_mod.sleep(0.3)                                    # inside the EXPORT statement


# --------------------------------------------------------------------------- export_snapshot
def test_export_snapshot_round_trips_via_import(store, tmp_path) -> None:
    _seed(store)
    dest = tmp_path / "backups" / "market-20260617T220000"
    dest.parent.mkdir()

    report = store.export_snapshot(dest)

    assert (dest / "schema.sql").is_file() and (dest / "load.sql").is_file()
    assert not dest.with_name(dest.name + ".tmp").exists()
    src = store._require_con()
    tables = _tables(src)
    assert report["tables"] == len(tables) and report["bytes"] > 0
    # The real consumer of the artefact: IMPORT DATABASE into a fresh file, from the RENAMED dir
    # (load.sql names the tmp path; IMPORT resolves files against the directory it is given).
    restored = duckdb.connect(str(tmp_path / "restored.duckdb"))
    try:
        restored.execute(f"IMPORT DATABASE '{dest.as_posix()}'")
        assert _tables(restored) == tables
        assert _counts(restored, tables) == _counts(src, tables)
        assert _counts(restored, ["bars_1m", "bars_1d"]) == {"bars_1m": 10, "bars_1d": 1}
        assert _pk_count(restored) == _pk_count(src) > 0
    finally:
        restored.close()


def test_export_snapshot_does_not_take_store_lock(store, tmp_path) -> None:
    _seed(store)
    dest = tmp_path / "market-20260617T220000"
    done: dict = {}

    def run() -> None:
        done["report"] = store.export_snapshot(dest)

    with store._lock:                                      # every store writer would queue here
        worker = threading.Thread(target=run)
        worker.start()
        worker.join(timeout=30)
        assert not worker.is_alive(), "the export waited on MarketStore._lock"
    assert done["report"]["tables"] > 0 and dest.is_dir()


def test_export_snapshot_reflects_committed_rows_only_at_its_start(store, tmp_path) -> None:
    _seed(store)
    first = tmp_path / "market-a"
    store.export_snapshot(first)
    store.upsert_bars_1d([
        DailyBar(symbol="TCS", d=date(2026, 6, 16), open=Decimal("1"), high=Decimal("1"),
                 low=Decimal("1"), close=Decimal("1"), volume=1),
    ])
    second = tmp_path / "market-b"
    store.export_snapshot(second)

    def exported(d) -> int:
        # DuckDB sanitizes the file name (bars_1d -> bars__d.parquet): take it from load.sql.
        line = next(ln for ln in (d / "load.sql").read_text().splitlines() if ln.startswith("COPY bars_1d "))
        name = store_mod._EXPORT_COPY_LINE.match(line).group(2).rsplit("/", 1)[-1]
        return duckdb.sql(f"SELECT count(*) FROM '{(d / name).as_posix()}'").fetchone()[0]

    assert (exported(first), exported(second)) == (1, 2)


def test_failed_export_leaves_no_final_dir(store, tmp_path, monkeypatch) -> None:
    def incomplete(directory, tables):
        assert directory.name == "market-x.tmp" and directory.is_dir()   # the export did land in tmp
        raise RuntimeError("snapshot export incomplete: no data file for ['bars_1m']")

    monkeypatch.setattr(store_mod, "_verify_export", incomplete)
    dest = tmp_path / "market-x"

    with pytest.raises(RuntimeError, match="incomplete"):
        store.export_snapshot(dest)

    assert not dest.exists() and not (tmp_path / "market-x.tmp").exists()
    assert store._export_cursor is None                    # the next export is not refused
    monkeypatch.undo()
    store.export_snapshot(dest)
    assert dest.is_dir()


def test_export_snapshot_refuses_an_existing_destination(store, tmp_path) -> None:
    dest = tmp_path / "market-x"
    dest.mkdir()
    with pytest.raises(FileExistsError):
        store.export_snapshot(dest)


def test_interrupt_ends_an_export_and_removes_its_tmp(store, tmp_path) -> None:
    _big_table(store)
    dest = tmp_path / "market-x"
    outcome: dict = {}

    def run() -> None:
        try:
            store.export_snapshot(dest)
            outcome["ok"] = True
        except duckdb.InterruptException as exc:
            outcome["interrupted"] = exc

    worker = threading.Thread(target=run)
    worker.start()
    _wait_for_export(store)
    assert store.interrupt_export(wait_s=10) is True
    worker.join(timeout=10)

    assert "interrupted" in outcome, outcome
    assert not dest.exists() and not (tmp_path / "market-x.tmp").exists()
    assert store.ping() is True                            # the store's own connection is untouched


def test_close_interrupts_an_in_flight_export_instead_of_waiting_for_it(tmp_path) -> None:
    """Closing the parent connection waits for a cursor's EXPORT to finish (measured): a ~45 s export
    on the stop path would outlast NSSM's 30 s grace. close() interrupts it first."""
    s = MarketStore(tmp_path / "market.duckdb", tmp_path / "parquet", MovableClock(NIGHT)).open()
    _big_table(s)
    dest = tmp_path / "market-x"
    outcome: dict = {}

    def run() -> None:
        try:
            s.export_snapshot(dest)
            outcome["ok"] = True
        except duckdb.Error as exc:
            outcome["error"] = exc

    worker = threading.Thread(target=run)
    worker.start()
    _wait_for_export(s)
    started = time_mod.monotonic()
    s.close()
    closed_in = time_mod.monotonic() - started
    worker.join(timeout=10)

    assert "error" in outcome, outcome
    assert closed_in < store_mod._CLOSE_EXPORT_WAIT_S + 2
    assert not dest.exists() and not (tmp_path / "market-x.tmp").exists()
    with pytest.raises(RuntimeError, match="closing"):
        s.export_snapshot(tmp_path / "market-y")


def test_a_second_concurrent_export_is_refused(store, tmp_path) -> None:
    _big_table(store, rows=5_000_000)
    worker = threading.Thread(target=lambda: store.export_snapshot(tmp_path / "market-a"))
    worker.start()
    _wait_for_export(store)
    try:
        with pytest.raises(RuntimeError, match="in flight"):
            store.export_snapshot(tmp_path / "market-b")
    finally:
        worker.join(timeout=60)
    assert (tmp_path / "market-a").is_dir() and not (tmp_path / "market-b").exists()


# --------------------------------------------------------------------------- the market_backup job
class FakeStore:
    def __init__(self) -> None:
        self.exports: list = []

    def export_snapshot(self, dest):
        dest.mkdir()
        (dest / "bars_1m.parquet").write_bytes(b"x" * 100)
        self.exports.append(dest)
        return {"tables": 1, "bytes": 100}

    async def arun(self, fn, *args):
        return fn(*args)


def _settings(tmp_path) -> SimpleNamespace:
    return SimpleNamespace(backups_dir=lambda: tmp_path / "backups", duckdb_path=lambda: tmp_path / "market.duckdb")


def _calendar(clock) -> NSECalendar:
    return NSECalendar(config_dir() / "calendar", clock, strict=False)


@pytest.mark.asyncio
async def test_job_refuses_inside_session_window(tmp_path) -> None:
    clock = MovableClock(IN_SESSION)
    calendar = _calendar(clock)
    fake = FakeStore()

    result = await opsmain._market_backup(fake, _settings(tmp_path), clock, calendar)
    assert result.unfinished is True and fake.exports == []

    clock.at = NIGHT
    result = await opsmain._market_backup(fake, _settings(tmp_path), clock, calendar)
    assert result.ok is True and result.unfinished is False
    assert [p.name for p in fake.exports] == ["market-20260617T220000"]


@pytest.mark.asyncio
async def test_an_unfinished_run_records_no_watermark_in_the_lane_runner(tmp_path, conn) -> None:
    """``CatchUpRunner._run_latest`` honours ``unfinished`` like the date-keyed and scheduled runners:
    a refused export leaves the day missed for the first lane pass after the window."""
    clock = MovableClock(datetime(2026, 6, 18, 8, 50, tzinfo=IST))   # Thu, inside the window
    calendar = _calendar(clock)
    fake = FakeStore()
    reg = JobRegistry()
    reg.register(JobSpec(JOB_MARKET_BACKUP, JobClass.RUN_LATEST, time(22, 0),
                         lambda: opsmain._market_backup(fake, _settings(tmp_path), clock, calendar), order=95))
    runner = CatchUpRunner(conn, clock, calendar, reg)
    runner.record_run(JOB_MARKET_BACKUP, date(2026, 6, 16))           # Wed 22:00 is the missed fire

    result = await runner.catch_up()
    assert fake.exports == [] and result.jobs_caught_up == [] and result.jobs_failed == []
    assert runner.was_run(JOB_MARKET_BACKUP, WED) is False

    clock.at = datetime(2026, 6, 18, 16, 0, tzinfo=IST)               # after the window
    result = await runner.catch_up()
    assert len(fake.exports) == 1 and result.jobs_caught_up == [f"{JOB_MARKET_BACKUP}:2026-06-17"]


@pytest.mark.parametrize(
    ("when", "starts"),
    [
        (datetime(2026, 6, 17, 5, 59, tzinfo=IST), True),        # Wed small hours
        (datetime(2026, 6, 17, 6, 0, tzinfo=IST), False),        # early hydration owns the disk
        (datetime(2026, 6, 17, 8, 30, tzinfo=IST), False),       # boot pass / pre-open chain
        (datetime(2026, 6, 17, 8, 44, 30, tzinfo=IST), False),   # would run into the 08:45 window
        (datetime(2026, 6, 17, 15, 45, tzinfo=IST), False),      # inclusive upper bound
        (datetime(2026, 6, 17, 15, 46, tzinfo=IST), True),
        (datetime(2026, 6, 20, 7, 0, tzinfo=IST), True),         # Saturday: no session to protect
        (datetime(2026, 6, 20, 11, 0, tzinfo=IST), True),
    ],
)
@pytest.mark.asyncio
async def test_export_never_starts_in_a_trading_day_morning_or_session(tmp_path, when, starts) -> None:
    clock = MovableClock(when)
    fake = FakeStore()

    result = await opsmain._market_backup(fake, _settings(tmp_path), clock, _calendar(clock))

    assert (result.unfinished, len(fake.exports)) == ((False, 1) if starts else (True, 0))


class FailingStore(FakeStore):
    def __init__(self) -> None:
        super().__init__()
        self.fail = True
        self.attempts: list = []

    def export_snapshot(self, dest):
        self.attempts.append(dest.name)
        if self.fail:
            raise RuntimeError("snapshot export incomplete: no data file for ['bars_1m']")
        return super().export_snapshot(dest)


@pytest.mark.asyncio
async def test_a_failed_export_is_not_retried_for_six_hours(tmp_path) -> None:
    clock = MovableClock(datetime(2026, 6, 20, 22, 0, tzinfo=IST))      # Saturday: no day window
    calendar = _calendar(clock)
    store = FailingStore()
    retry = opsmain.MarketBackupRetry()

    async def run():
        return await opsmain._market_backup(store, _settings(tmp_path), clock, calendar, retry=retry)

    with pytest.raises(RuntimeError, match="incomplete"):
        await run()
    assert retry.failed_at == datetime(2026, 6, 20, 22, 0, tzinfo=IST)

    for at in (datetime(2026, 6, 20, 22, 30, tzinfo=IST), datetime(2026, 6, 21, 3, 59, 59, tzinfo=IST)):
        clock.at = at
        assert (await run()).unfinished is True
    assert len(store.attempts) == 1

    clock.at = datetime(2026, 6, 21, 4, 0, tzinfo=IST)                   # six hours after the failure
    store.fail = False
    result = await run()
    assert result.ok is True and result.unfinished is False and len(store.attempts) == 2
    assert retry.failed_at is None
    clock.at = datetime(2026, 6, 21, 4, 30, tzinfo=IST)                  # a success clears the hold
    assert (await run()).unfinished is False and len(store.attempts) == 3


@pytest.mark.asyncio
async def test_a_restart_allows_one_fresh_export_attempt(tmp_path) -> None:
    clock = MovableClock(datetime(2026, 6, 20, 22, 0, tzinfo=IST))
    calendar = _calendar(clock)
    store = FailingStore()
    for retry in (opsmain.MarketBackupRetry(), opsmain.MarketBackupRetry()):   # a new process's state
        with pytest.raises(RuntimeError):
            await opsmain._market_backup(store, _settings(tmp_path), clock, calendar, retry=retry)
        clock.at += timedelta(minutes=5)
    assert len(store.attempts) == 2


@pytest.mark.asyncio
async def test_a_stuck_export_failure_is_neither_retried_nor_reported_every_sweep(tmp_path, conn) -> None:
    """Through the lane's real runner: the first failure records ``failed`` and reports once; the
    sweeps inside the hold export nothing, record nothing, report nothing; the retry after it
    fails again quietly (unchanged failure set); the first pass after the next hold succeeds."""
    clock = MovableClock(datetime(2026, 6, 19, 22, 0, tzinfo=IST))      # Fri 22:00
    calendar = _calendar(clock)
    store = FailingStore()
    retry = opsmain.MarketBackupRetry()
    sent: list = []

    async def notify(msg) -> None:
        sent.append(msg)

    reg = JobRegistry()
    reg.register(JobSpec(
        JOB_MARKET_BACKUP, JobClass.RUN_LATEST, time(22, 0),
        lambda: opsmain._market_backup(store, _settings(tmp_path), clock, calendar, retry=retry), order=95))
    runner = CatchUpRunner(conn, clock, calendar, reg, notify=notify)
    runner.record_run(JOB_MARKET_BACKUP, date(2026, 6, 18))
    fri = date(2026, 6, 19)

    await runner.catch_up(scope=CatchUpScope.ALL)
    assert len(store.attempts) == 1 and len(sent) == 1
    assert sent[0].data["jobs_failed"] == [f"{JOB_MARKET_BACKUP}:{fri.isoformat()}"]

    clock.at += timedelta(minutes=30)
    while clock.at < datetime(2026, 6, 20, 4, 0, tzinfo=IST):           # every 30-min sweep
        result = await runner.catch_up(scope=CatchUpScope.ALL)
        assert result.jobs_failed == [] and result.jobs_caught_up == []
        clock.at += timedelta(minutes=30)
    assert len(store.attempts) == 1 and len(sent) == 1
    status = conn.execute("SELECT status FROM job_runs WHERE job_id=? AND run_for_date=?",
                          (JOB_MARKET_BACKUP, fri.isoformat())).fetchone()["status"]
    assert status == "failed" and runner.was_run(JOB_MARKET_BACKUP, fri) is False

    await runner.catch_up(scope=CatchUpScope.ALL)                        # Sat 04:00: the retry
    assert len(store.attempts) == 2 and len(sent) == 1

    store.fail = False
    clock.at = datetime(2026, 6, 20, 10, 0, tzinfo=IST)                  # Sat: no day window
    result = await runner.catch_up(scope=CatchUpScope.ALL)
    assert len(store.attempts) == 3 and result.jobs_caught_up == [f"{JOB_MARKET_BACKUP}:{fri.isoformat()}"]
    assert runner.was_run(JOB_MARKET_BACKUP, fri) is True and len(sent) == 2


@pytest.mark.asyncio
async def test_job_skips_when_disk_is_low(tmp_path, monkeypatch, caplog) -> None:
    clock = MovableClock(NIGHT)
    calendar = _calendar(clock)
    backups = tmp_path / "backups"
    newest = backups / "market-20260616T220000"
    newest.mkdir(parents=True)
    (newest / "bars_1m.parquet").write_bytes(b"x" * 1000)
    older = backups / "market-20260615T220000"
    older.mkdir()
    (older / "bars_1m.parquet").write_bytes(b"x" * 5000)            # the NEWEST export sets the floor
    usage = namedtuple("usage", "total used free")
    free = {"bytes": 2999}
    monkeypatch.setattr(opsmain.shutil, "disk_usage", lambda _p: usage(10**12, 0, free["bytes"]))
    fake = FakeStore()

    with caplog.at_level(logging.WARNING, logger="engine.ops.main"):
        result = await opsmain._market_backup(fake, _settings(tmp_path), clock, calendar)

    assert result.ok is False and fake.exports == []
    low = [r for r in caplog.records if r.getMessage() == "market_backup_skipped_low_disk"]
    assert len(low) == 1 and (low[0].free_bytes, low[0].needed_bytes) == (2999, 3000)

    free["bytes"] = 3000
    result = await opsmain._market_backup(fake, _settings(tmp_path), clock, calendar)
    assert result.ok is True and len(fake.exports) == 1


@pytest.mark.asyncio
async def test_first_export_needs_the_database_size_free(tmp_path, monkeypatch) -> None:
    clock = MovableClock(NIGHT)
    (tmp_path / "market.duckdb").write_bytes(b"x" * 4096)
    usage = namedtuple("usage", "total used free")
    monkeypatch.setattr(opsmain.shutil, "disk_usage", lambda _p: usage(10**12, 0, 4095))
    fake = FakeStore()

    result = await opsmain._market_backup(fake, _settings(tmp_path), clock, _calendar(clock))

    assert result.ok is False and fake.exports == []


@pytest.mark.asyncio
async def test_job_on_a_real_store_writes_logs_and_prunes(tmp_path, caplog) -> None:
    """Producer and consumer at their seam: the job drives the real export, the next run's disk
    floor reads the export it wrote, and retention keeps the newest."""
    clock = MovableClock(NIGHT)
    real = MarketStore(tmp_path / "market.duckdb", tmp_path / "parquet", clock).open()
    try:
        _seed(real)
        settings = _settings(tmp_path)
        backups = settings.backups_dir()
        backups.mkdir()
        stale = backups / "market-20260601T220000.tmp"
        stale.mkdir()
        with caplog.at_level(logging.INFO, logger="engine.ops.main"):
            first = await opsmain._market_backup(real, settings, clock, _calendar(clock))
        written = [r for r in caplog.records if r.getMessage() == "market_backup_written"]
        assert len(written) == 1 and written[0].bytes == first.bytes > 0
        assert written[0].duration_s >= 0 and written[0].tables > 20
        assert not stale.exists()

        free, needed = opsmain._market_backup_space(backups, settings.duckdb_path())
        assert needed == opsmain.MARKET_BACKUP_FREE_FACTOR * first.bytes

        clock.at = NIGHT + timedelta(days=1)
        second = await opsmain._market_backup(real, settings, clock, _calendar(clock))
        assert sorted(p.name for p in backups.iterdir()) == [
            "market-20260617T220000", "market-20260618T220000"]
        assert second.path.endswith("market-20260618T220000")
    finally:
        real.close()
