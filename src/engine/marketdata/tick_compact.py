"""Tick-partition compaction — the nightly small-file collapse.

The live tick writer flushes every ``flush_interval_s`` or ``max_buffered_ticks``, writing ONE
parquet file per (date, symbol) present in each batch (``MarketStore.flush_ticks``) — thousands of
small files per symbol-day. Readers glob ``ticks/date=…/symbol=…/*.parquet``, so fragments and a
compacted file are *equivalent* to every reader: the cost is read amplification and inode count,
never correctness. This module collapses each symbol-day to ONE file:

    write ``.<name>.tmp`` (invisible to the readers' ``*.parquet`` glob) and fsync it
      → ``os.replace`` it onto its final name (atomic within the partition dir)
      → unlink the exact fragment list that was compacted

Ordering is deliberate. A crash after the swap leaves compacted+fragments (readers see duplicate
rows until the next run repairs it — recoverable); the reverse order would lose rows outright. The
fsync keeps a power loss from leaving the swapped-in name on data the OS never wrote while the
fragments it replaced are already gone.

Fragments beside a compacted file are reconciled, never left to fail the day. Rows do arrive after
a compaction, because this job closes a date at midnight and the writer does not: every ticker
(re)connect re-sends each instrument's last state stamped with its last session (by the weekend
with volume, average price and depth zeroed, so no copy of a stored row), a processing backlog can
carry a session's ticks past midnight, and a flush can outrun ``job_tick_compact``'s pre-flush.
The rows :data:`_LATE_ROWS` finds are merged in by the same write-then-swap and the fragments
unlinked; when it finds none, the fragments are only unlinked. A row on both sides is kept as many
times as the side holding more copies holds it, never the sum: an interrupted unlink's leftovers
are already inside and are not doubled — nor is a late copy of a stored row, which the 12 stored
columns cannot tell from a genuine repeat. A crash between a merge's swap and its unlink resolves
to that delete on the next run. Two ``compact-`` files are refused: this job writes one
deterministic name, so which one holds the symbol-day is unknowable.

Idempotence is per symbol-day: one file (or none) ⇒ nothing to do. A re-run, a sweep retry and a
date-keyed catch-up replay are no-ops on days already compacted, and a partial failure (one bad
symbol) is resumable.

The job runs on its own in-memory DuckDB connection in a worker thread: the live ``MarketStore``
connection is the bar/tick write path (the heartbeat invariant), and a full day's compaction must
never queue behind — or in front of — it.

SINGLE-FLIGHT: the post-arm one-shot and the scheduler's ``_scheduled_runner`` both reach this job
directly, unaware of each other. Racing runs are not a correctness bug (the loser fails at READ
time, before writing anything) but are error noise, so :func:`compact_ticks` is single-flight via
a module-level ``threading.Lock`` (same process, off the event loop via ``asyncio.to_thread``; no
cross-process coordination is needed). The loser returns immediately with
``skipped_in_flight=True`` and ``ok=True``: the in-flight run owns the day's outcome, so a skip
must never sink the watermark.
"""

from __future__ import annotations

import os
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import duckdb

from engine.core.log import get_logger

_log = get_logger("engine.marketdata.tick_compact")

#: Marker prefix on a compacted file — distinguishes "the output" from "a writer fragment" when an
#: interrupted run leaves both behind. Fragments are ULID-named by ``MarketStore.flush_ticks``.
COMPACT_PREFIX = "compact-"

#: The compacted file's name within a symbol-day partition. Deterministic (not ULID-suffixed) so a
#: second compaction of the same symbol-day overwrites in place instead of accreting outputs.
COMPACT_NAME = f"{COMPACT_PREFIX}ticks.parquet"

#: Staging name for the write-then-swap. The leading dot + ``.tmp`` suffix keeps it out of both the
#: readers' ``*.parquet`` glob and this module's own fragment enumeration.
_TMP_NAME = f".{COMPACT_NAME}.tmp"

#: Single-flight guard: non-blocking so a losing caller returns immediately instead of queueing
#: behind a run that may still be draining a backlog. Module-level because both callers reach
#: :func:`compact_ticks` as a bare function.
_COMPACT_LOCK = threading.Lock()

#: Substring DuckDB's ``read_parquet`` raises when a path in its file list no longer exists — a
#: fragment a CONCURRENT compaction run unlinked out from under this one's glob. Narrower than
#: "any ``duckdb.IOException``" on purpose: a permission error or a corrupt file must stay at ERROR.
_MISSING_FILE_MARKER = "No files found"

#: Smallest structurally-possible parquet file: the 4-byte PAR1 magic at both ends plus the 4-byte
#: footer length. Anything under this is a truncated write, not data.
_MIN_PARQUET_BYTES = 12

#: Hard ceiling on the compaction connection's DuckDB memory. DuckDB's default (~80% of RAM) is
#: reachable by EXCEPT checks and ORDER BY COPYs over thousands of tiny fragments; past this limit
#: it spills to :data:`_SPILL_DIRNAME`. A maintenance job may be slow; it must never compete with
#: the machine for memory.
_MEMORY_LIMIT = "4GB"

#: DuckDB spill directory for the bounded connection, created inside ``ticks/``. The leading dot
#: keeps it out of the date-partition enumeration (``date=`` prefix check) and every reader's
#: ``*.parquet`` glob; DuckDB owns the contents.
_SPILL_DIRNAME = ".compact_spill"


def _open_connection(ticks_root: Path) -> duckdb.DuckDBPyConnection:
    """One BOUNDED connection: ``memory_limit`` + disk spill, insertion order off (every consumer
    counts rows or ORDER BYs explicitly). Opened per DATE partition by the caller — closing between
    dates hands allocator retention back across a long backlog drain."""
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{_MEMORY_LIMIT}'")
    spill = ticks_root / _SPILL_DIRNAME
    spill.mkdir(exist_ok=True)
    con.execute(f"SET temp_directory='{spill.as_posix()}'")
    con.execute("SET preserve_insertion_order=false")
    return con


@dataclass
class TickCompactionResult:
    """One run's outcome — ok-bearing so the watermark verdict sees a degraded run.

    ``ok=False`` (a symbol-day that failed, or holds two compacted files) records a FAILED watermark,
    and the next sweep retries: the symbol-days that DID compact are no-ops on the retry.
    ``symbol_days_compacted`` and ``rows_written`` count a merge of late rows (module docstring) as
    one symbol-day and the rows of the file it rewrote; ``symbol_days_merged`` counts those alone.
    """

    ok: bool = True
    dates: list[str] = field(default_factory=list)          # date partitions examined
    symbol_days_compacted: int = 0
    symbol_days_merged: int = 0                             # of those, late-row merges
    fragments_removed: int = 0
    rows_written: int = 0
    skipped_current_date: str | None = None                 # the date left alone (writer still live)
    failures: list[str] = field(default_factory=list)       # "YYYY-MM-DD/SYMBOL: reason"
    budget_exhausted: bool = False                          # stopped on max_symbol_days, more remains
    skipped_in_flight: bool = False                         # another run already held the lock
    stopped: bool = False                                   # the caller's ``stop`` fired, more remains

    @property
    def unfinished(self) -> bool:
        """The run did not do the day's work: skipped for an in-flight run, or stopped by the caller.
        Its runner records no watermark, so the day stays missed and a later pass replays it."""
        return self.skipped_in_flight or self.stopped


def compact_ticks(
    parquet_root: Path | str, *, upto: date, today: date,
    max_dates: int = 40, max_symbol_days: int = 400, stop: Callable[[], bool] | None = None,
) -> TickCompactionResult:
    """Compact every tick date partition ``<= upto`` and strictly BEFORE ``today``, oldest first.

    ``today`` is skipped unconditionally: the tick writer is still appending to it (and late/post-
    close prints land there), so its fragment list is not a closed set. ``upto`` is the date-keyed
    job's ``run_for`` day — a catch-up replay of an older slot therefore compacts only what that
    slot could have seen, and the next slot picks up the rest.

    BOUNDED: ``max_symbol_days`` stops a run once it has done that much; oldest-first order plus
    per-symbol-day idempotence makes progress monotone, so a standing backlog drains over
    successive nightly runs. ``max_dates`` bounds the partition scan itself (ticks retain 30 days).

    SINGLE-FLIGHT (module docstring): a non-blocking ``threading.Lock`` acquire. The loser never
    touches the store or filesystem — it returns immediately with ``skipped_in_flight=True`` on an
    otherwise-default (``ok=True``) result, deliberately watermark-neutral.

    ``stop`` is polled before every symbol-day; once it returns True the run ends there with
    ``stopped=True``. The engine passes "the session window has begun or the engine is stopping":
    a run is otherwise unbounded in wall-clock (a host that sleeps mid-run resumes it in-session),
    and a worker thread still compacting at shutdown holds the process past NSSM's stop grace.

    Synchronous and blocking (DuckDB + filesystem): call it via ``asyncio.to_thread``.
    """
    if not _COMPACT_LOCK.acquire(blocking=False):
        _log.info(
            "tick_compaction_skipped_in_flight", upto=upto.isoformat(), today=today.isoformat(),
            thread=threading.current_thread().name,
        )
        return TickCompactionResult(skipped_in_flight=True)
    try:
        return _compact_ticks_locked(
            parquet_root, upto=upto, today=today, max_dates=max_dates, max_symbol_days=max_symbol_days,
            stop=stop,
        )
    finally:
        _COMPACT_LOCK.release()


def _compact_ticks_locked(
    parquet_root: Path | str, *, upto: date, today: date, max_dates: int, max_symbol_days: int,
    stop: Callable[[], bool] | None,
) -> TickCompactionResult:
    """The actual scan-and-compact body, run under :data:`_COMPACT_LOCK` by :func:`compact_ticks`."""
    result = TickCompactionResult()
    ticks_root = Path(parquet_root) / "ticks"
    if not ticks_root.is_dir():
        return result

    day_dirs: list[tuple[date, Path]] = []
    for p in sorted(ticks_root.iterdir()):
        if not p.is_dir() or not p.name.startswith("date="):
            continue
        try:
            d = date.fromisoformat(p.name[len("date="):])
        except ValueError:
            _log.warning("tick_compaction_unparsable_partition", partition=p.name)
            continue
        if d >= today:
            if d == today:    # the writer owns today's partition (and any future-dated one)
                result.skipped_current_date = d.isoformat()
            continue
        if d > upto:
            continue
        day_dirs.append((d, p))

    for d, day_dir in day_dirs[-max_dates:]:
        result.dates.append(d.isoformat())
        # Per-DATE connection: see _open_connection.
        con = _open_connection(ticks_root)
        try:
            for sym_dir in sorted(p for p in day_dir.iterdir() if p.is_dir()):
                if result.symbol_days_compacted >= max_symbol_days:
                    result.budget_exhausted = True
                    _log.info("tick_compaction_budget_exhausted", stopped_at=f"{d.isoformat()}/{sym_dir.name}",
                              symbol_days=result.symbol_days_compacted, note="resumes on the next run")
                    return _done(result)
                if stop is not None and stop():
                    result.stopped = True
                    _log.info("tick_compaction_stopped", stopped_at=f"{d.isoformat()}/{sym_dir.name}",
                              symbol_days=result.symbol_days_compacted, note="resumes on the next run")
                    return _done(result)
                _compact_symbol_day(con, d, sym_dir, result)
                if result.symbol_days_compacted and result.symbol_days_compacted % 50 == 0:
                    _log.info("tick_compaction_progress", date=d.isoformat(),
                              symbol_days=result.symbol_days_compacted, rows=result.rows_written)
        finally:
            con.close()
    return _done(result)


def _done(result: TickCompactionResult) -> TickCompactionResult:
    _log.info(
        "tick_compaction_done", dates=result.dates, symbol_days=result.symbol_days_compacted,
        merged=result.symbol_days_merged, fragments_removed=result.fragments_removed, rows=result.rows_written,
        skipped_current_date=result.skipped_current_date, failures=result.failures,
        budget_exhausted=result.budget_exhausted, stopped=result.stopped, ok=result.ok,
    )
    return result


def _compact_symbol_day(
    con: duckdb.DuckDBPyConnection, d: date, sym_dir: Path, result: TickCompactionResult
) -> None:
    """Collapse one ``symbol=<SYM>`` partition to a single parquet file (idempotent, atomic swap)."""
    files = sorted(p for p in sym_dir.glob("*.parquet") if p.is_file())
    if len(files) <= 1:
        _cleanup_stale_tmp(sym_dir)
        return                                    # already one file (or empty) — nothing to compact

    existing = [p for p in files if p.name.startswith(COMPACT_PREFIX)]
    fragments = [p for p in files if not p.name.startswith(COMPACT_PREFIX)]
    label = f"{d.isoformat()}/{sym_dir.name.removeprefix('symbol=')}"
    try:
        # Corrupt-fragment containment: truncated writes leave 0-byte fragments, and DuckDB's
        # read_parquet fails the WHOLE partition on one. Fragments under _MIN_PARQUET_BYTES are
        # quarantined and the partition compacts from the survivors. A failing quarantine move
        # falls to the generic handler below and degrades this partition only.
        corrupt = [p for p in fragments if p.stat().st_size < _MIN_PARQUET_BYTES]
        if corrupt:
            qdir = sym_dir.parent.parent.parent / "quarantine"
            qdir.mkdir(exist_ok=True)
            for p in corrupt:
                p.replace(
                    qdir / f"{d.isoformat()}_{sym_dir.name.removeprefix('symbol=')}_{p.name}"
                )
            _log.warning("tick_compaction_fragments_quarantined", partition=label,
                         files=[p.name for p in corrupt], quarantine=str(qdir))
            fragments = [p for p in fragments if p not in corrupt]
        if len(existing) > 1:
            result.ok = False
            result.failures.append(f"{label}: more than one compacted file, left alone")
            _log.warning("tick_compaction_ambiguous", partition=label,
                         compacted=len(existing), fragments=len(fragments))
            return
        if not fragments:
            return                            # nothing readable left beside the compacted file, if any
        tmp = sym_dir / _TMP_NAME
        if existing:
            _reconcile_late_fragments(con, label, existing[0], fragments, tmp, result)
            return

        tmp.unlink(missing_ok=True)
        rows = _write_compacted(con, fragments, tmp)
        _swap_in(tmp, sym_dir / COMPACT_NAME)
        for f in fragments:                       # exactly the files that were read into it
            f.unlink()
        result.symbol_days_compacted += 1
        result.fragments_removed += len(fragments)
        result.rows_written += rows
    except duckdb.IOException as exc:
        if _MISSING_FILE_MARKER in str(exc):
            # A concurrent run unlinked these fragments between this run's glob and DuckDB's read.
            # It fails here, before writing anything, and the symbol-day is a no-op next run.
            # Benign: WARNING, no failure recorded.
            _log.warning("tick_compaction_fragments_vanished", partition=label, error=str(exc))
            return
        result.ok = False
        result.failures.append(f"{label}: {type(exc).__name__}: {exc}")
        _log.exception("tick_compaction_symbol_day_failed", partition=label)
    except Exception as exc:  # noqa: BLE001 - one bad symbol-day degrades the run, never kills it
        result.ok = False
        result.failures.append(f"{label}: {type(exc).__name__}: {exc}")
        _log.exception("tick_compaction_symbol_day_failed", partition=label)


def _write_compacted(con: duckdb.DuckDBPyConnection, fragments: list[Path], out: Path) -> int:
    """Write ``fragments`` (an explicit file list, never a glob — the glob would also pick up files
    this run is not going to delete) into ``out``, ordered by ``exchange_ts``. Returns the row count."""
    paths = [p.as_posix() for p in fragments]
    rows = con.execute("SELECT count(*) FROM read_parquet($files)", {"files": paths}).fetchall()[0][0]
    con.execute(
        f"COPY (SELECT * FROM read_parquet($files) ORDER BY exchange_ts) TO '{out.as_posix()}' "
        "(FORMAT PARQUET)",
        {"files": paths},
    )
    return int(rows)


def _reconcile_late_fragments(
    con: duckdb.DuckDBPyConnection, label: str, compacted: Path, fragments: list[Path], tmp: Path,
    result: TickCompactionResult,
) -> None:
    """Fragments beside the symbol-day's compacted file (module docstring): merge in the rows it
    lacks — none, for an interrupted unlink's leftovers — then unlink the fragments."""
    added = _missing_rows(con, fragments, compacted)
    rows = 0
    if added:
        tmp.unlink(missing_ok=True)
        rows = _write_merged(con, compacted, fragments, tmp)
        _swap_in(tmp, compacted)
    for f in fragments:                           # every row of each is now in ``compacted``
        f.unlink()
    result.fragments_removed += len(fragments)
    if not added:
        _log.info("tick_compaction_recovered", partition=label, fragments=len(fragments))
        return
    result.symbol_days_compacted += 1
    result.symbol_days_merged += 1
    result.rows_written += rows
    _log.info("tick_compaction_late_rows_merged", partition=label, fragments=len(fragments),
              rows_added=added)


def _swap_in(tmp: Path, dest: Path) -> None:
    """``os.replace`` ``tmp`` onto ``dest`` once its bytes are on disk: the caller unlinks fragments
    next, and they must not go while the only copy of their rows is still in the OS cache."""
    with tmp.open("r+b") as fh:                   # FlushFileBuffers needs a writable handle on Windows
        os.fsync(fh.fileno())
    os.replace(tmp, dest)                         # atomic within the partition dir


#: The fragment rows a compacted file does not already hold, as a multiset (``EXCEPT ALL``): a row
#: the fragments hold n times and the compacted file m times yields max(n - m, 0) copies, so a merge
#: keeps max(n, m) — never n + m, which would double an interrupted unlink's leftovers.
_LATE_ROWS = "SELECT * FROM read_parquet($frags) EXCEPT ALL SELECT * FROM read_parquet($compacted)"


def _missing_rows(con: duckdb.DuckDBPyConnection, fragments: list[Path], compacted: Path) -> int:
    """How many rows :data:`_LATE_ROWS` finds: 0 for an interrupted unlink's leftovers, which is what
    makes deleting them safe rather than hopeful."""
    return int(con.execute(
        f"SELECT count(*) FROM ({_LATE_ROWS})",
        {"frags": [p.as_posix() for p in fragments], "compacted": [compacted.as_posix()]},
    ).fetchall()[0][0])


def _write_merged(
    con: duckdb.DuckDBPyConnection, compacted: Path, fragments: list[Path], out: Path
) -> int:
    """Write ``compacted`` plus the fragment rows it lacks (:data:`_LATE_ROWS`) into ``out``, ordered
    by ``exchange_ts``. Returns the row count."""
    params = {"frags": [p.as_posix() for p in fragments], "compacted": [compacted.as_posix()]}
    merged = f"SELECT * FROM read_parquet($compacted) UNION ALL SELECT * FROM ({_LATE_ROWS})"
    rows = con.execute(f"SELECT count(*) FROM ({merged})", params).fetchall()[0][0]
    con.execute(
        f"COPY (SELECT * FROM ({merged}) ORDER BY exchange_ts) TO '{out.as_posix()}' (FORMAT PARQUET)",
        params,
    )
    return int(rows)


def _cleanup_stale_tmp(sym_dir: Path) -> None:
    """Drop a staging file orphaned by a crash BEFORE its swap (it holds no rows the fragments
    don't). Never touches ``*.parquet`` — a stale tmp is invisible to readers either way."""
    tmp = sym_dir / _TMP_NAME
    if tmp.exists():
        tmp.unlink(missing_ok=True)
        _log.info("tick_compaction_stale_tmp_removed", partition=sym_dir.name)
