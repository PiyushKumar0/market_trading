"""Tick-partition compaction (§4.3 storage, WO-7) — the nightly small-file collapse.

The live tick writer flushes every ``flush_interval_s`` (60 s default since WO-7) or ``max_buffered_ticks``, writing
ONE parquet file per (date, symbol) present in each batch (``MarketStore.flush_ticks``). Over a
session that is ~7.5 K files per symbol-day and 752,150 files / 1.37 GB for a single day (≈1.9 KB
per file, audit F9). Readers glob ``ticks/date=…/symbol=…/*.parquet``, so fragments and a compacted
file are *equivalent* to every reader — the pathology is pure read amplification and inode count,
never correctness. This module collapses each symbol-day to ONE file:

    write ``.<name>.tmp`` (invisible to the readers' ``*.parquet`` glob)
      → ``os.replace`` it onto its final name (atomic within the partition dir)
      → unlink the exact fragment list that was compacted

Ordering is deliberate. A crash after the swap leaves compacted+fragments (readers see duplicate
rows until the next run repairs it — recoverable); the reverse order would lose rows outright. The
repair path is explicit: fragments coexisting with a compacted file can only be leftovers of an
interrupted unlink (the writer never appends to a PAST date, and this job never touches the current
one), which :func:`_absorbed` verifies by row-set containment before deleting them. A symbol-day
that cannot be verified is left ALONE and reported not-ok — never merged on a guess.

Idempotence is per symbol-day: one file (or none) ⇒ nothing to do. That makes a re-run, a sweep
retry and a date-keyed catch-up replay all no-ops on days already compacted, and makes a partial
failure (one bad symbol) resumable — the successful symbols simply do not come back.

The job runs on its own in-memory DuckDB connection in a worker thread: the live ``MarketStore``
connection is the bar/tick write path (§2.2 heartbeat invariant), and a full day's compaction must
never queue behind — or in front of — it.

SINGLE-FLIGHT (2026-08-14): WO-15's ``CatchUpRunner`` guards catch-up-vs-catch-up replay, but the
post-arm one-shot and the scheduler's ``_scheduled_runner`` both reach this job directly — neither
knows about the other. Two concurrent runs racing the same partitions is not a correctness bug (the
loser always fails at READ time, before writing anything — the winner's output is unaffected) but it
is 78-errors-a-night noise, so :func:`compact_ticks` itself is single-flight via a module-level
``threading.Lock`` (both invocations land in the same engine process, off the event loop via
``asyncio.to_thread``, so a thread lock is sufficient — no cross-process coordination exists or is
needed here). The loser returns immediately with ``skipped_in_flight=True`` and ``ok=True``: the
in-flight run owns the day's outcome, so a skip must never sink the watermark.
"""

from __future__ import annotations

import os
import threading
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

#: Single-flight guard (2026-08-14): non-blocking so a losing caller returns immediately instead of
#: queueing behind a run that may still be draining a 30-day backlog. Module-level because both
#: callers reach :func:`compact_ticks` as a bare function, not through a shared object.
_COMPACT_LOCK = threading.Lock()

#: Substring DuckDB's ``read_parquet`` raises when a path in its file list no longer exists — the
#: signature of a fragment a CONCURRENT compaction run already unlinked out from under this one's
#: glob (2026-08-14). Narrower than "any ``duckdb.IOException``" on purpose: a permission error or a
#: truly corrupt file is a real problem and must stay at ERROR, not be swallowed by this benign case.
_MISSING_FILE_MARKER = "No files found"

#: Hard ceiling on the compaction connection's DuckDB memory (2026-08-18, the 53 GB incident of
#: 2026-08-17). DuckDB's DEFAULT limit is ~80% of RAM (~25 GB here), and this job's shape — EXCEPT
#: containment checks and ORDER BY COPYs over thousands of tiny fragments, for up to
#: ``max_symbol_days`` partitions — will actually get there: telemetry recorded 2.2→10.6 GB in the
#: first 35 minutes of the 2026-08-18 22:30 run. Past this limit DuckDB spills to
#: :data:`_SPILL_DIRNAME` instead of committing RAM. A maintenance job may be slow; it may never
#: again compete with the machine for memory.
_MEMORY_LIMIT = "4GB"

#: DuckDB spill directory for the bounded connection, created inside ``ticks/``. The leading dot
#: keeps it out of the date-partition enumeration (``date=`` prefix check) and every reader's
#: ``*.parquet`` glob; DuckDB owns the contents.
_SPILL_DIRNAME = ".compact_spill"


def _open_connection(ticks_root: Path) -> duckdb.DuckDBPyConnection:
    """One BOUNDED connection (2026-08-18): ``memory_limit`` + disk spill, insertion order off
    (every consumer here either counts rows or ORDER BYs explicitly). Opened per DATE partition by
    the caller — closing between dates hands allocator retention back instead of compounding it
    across a multi-hour backlog drain."""
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{_MEMORY_LIMIT}'")
    spill = ticks_root / _SPILL_DIRNAME
    spill.mkdir(exist_ok=True)
    con.execute(f"SET temp_directory='{spill.as_posix()}'")
    con.execute("SET preserve_insertion_order=false")
    return con


@dataclass
class TickCompactionResult:
    """One run's outcome — ok-bearing so the §2.6 watermark verdict sees a degraded run (E5).

    ``ok=False`` (a symbol-day that failed or could not be verified) records a FAILED watermark, and
    the next sweep retries: the symbol-days that DID compact are no-ops on the retry.
    """

    ok: bool = True
    dates: list[str] = field(default_factory=list)          # date partitions examined
    symbol_days_compacted: int = 0
    fragments_removed: int = 0
    rows_written: int = 0
    skipped_current_date: str | None = None                 # the date left alone (writer still live)
    failures: list[str] = field(default_factory=list)       # "YYYY-MM-DD/SYMBOL: reason"
    budget_exhausted: bool = False                          # stopped on max_symbol_days, more remains
    skipped_in_flight: bool = False                         # 2026-08-14: another run already held the lock


def compact_ticks(
    parquet_root: Path | str, *, upto: date, today: date,
    max_dates: int = 40, max_symbol_days: int = 400,
) -> TickCompactionResult:
    """Compact every tick date partition ``<= upto`` and strictly BEFORE ``today``, oldest first.

    ``today`` is skipped unconditionally: the tick writer is still appending to it (and late/post-
    close prints land there), so its fragment list is not a closed set. ``upto`` is the date-keyed
    job's ``run_for`` day — a catch-up replay of an older slot therefore compacts only what that
    slot could have seen, and the next slot picks up the rest.

    BOUNDED + OBSERVABLE (the §2.6 filed proposal, applied here because the FIRST run faces the
    whole standing backlog — up to 30 retained days × ~200 symbols × ~7.5 K files each, hours of
    pure filesystem work). ``max_symbol_days`` stops a run once it has done that much; oldest-first
    order plus per-symbol-day idempotence makes progress monotone, so the backlog drains over
    successive nightly runs instead of one unbounded pass. ``max_dates`` bounds the partition scan
    itself (ticks retain 30 days, §4.5 — a bigger backlog means retention is broken, not this).

    SINGLE-FLIGHT (2026-08-14, module docstring): a non-blocking ``threading.Lock`` acquire. The
    loser never touches the store or filesystem — it returns immediately with
    ``skipped_in_flight=True`` on an otherwise-default (``ok=True``) result, which is deliberately
    watermark-neutral: the in-flight run owns whatever this day's real outcome turns out to be.

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
        )
    finally:
        _COMPACT_LOCK.release()


def _compact_ticks_locked(
    parquet_root: Path | str, *, upto: date, today: date, max_dates: int, max_symbol_days: int,
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
        # Per-DATE connection (2026-08-18): the 53 GB incident was one unbounded connection held
        # across the whole backlog drain — bounding (see _open_connection) caps the working set,
        # and reconnecting per date releases what the allocator would otherwise retain for hours.
        con = _open_connection(ticks_root)
        try:
            for sym_dir in sorted(p for p in day_dir.iterdir() if p.is_dir()):
                if result.symbol_days_compacted >= max_symbol_days:
                    result.budget_exhausted = True
                    _log.info("tick_compaction_budget_exhausted", stopped_at=f"{d.isoformat()}/{sym_dir.name}",
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
        fragments_removed=result.fragments_removed, rows=result.rows_written,
        skipped_current_date=result.skipped_current_date, failures=result.failures,
        budget_exhausted=result.budget_exhausted, ok=result.ok,
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
        if existing:
            # Interrupted unlink (the only way this state is reachable: a past date's fragments can
            # no longer grow). Delete the leftovers ONLY if their rows are provably already inside
            # the compacted file — otherwise leave the partition untouched and report.
            if len(existing) > 1 or not _absorbed(con, fragments, existing[0]):
                result.ok = False
                result.failures.append(f"{label}: unverifiable compacted+fragment mix, left alone")
                _log.warning("tick_compaction_ambiguous", partition=label,
                             compacted=len(existing), fragments=len(fragments))
                return
            for f in fragments:
                f.unlink()
            result.fragments_removed += len(fragments)
            _log.info("tick_compaction_recovered", partition=label, fragments=len(fragments))
            return

        tmp = sym_dir / _TMP_NAME
        tmp.unlink(missing_ok=True)
        rows = _write_compacted(con, fragments, tmp)
        os.replace(tmp, sym_dir / COMPACT_NAME)   # atomic within the partition dir
        for f in fragments:                       # exactly the files that were read into it
            f.unlink()
        result.symbol_days_compacted += 1
        result.fragments_removed += len(fragments)
        result.rows_written += rows
    except duckdb.IOException as exc:
        if _MISSING_FILE_MARKER in str(exc):
            # A concurrent compaction run can unlink these EXACT fragments between this run's glob
            # (above) and DuckDB's read of them (2026-08-14 live: the post-arm one-shot and the
            # 22:30 scheduled slot both reached this job before :data:`_COMPACT_LOCK` existed). This
            # loser always fails HERE, at read time, before writing anything — the winner's
            # partition is already correct, and this symbol-day resolves to a no-op on the very next
            # run (idempotent, module docstring). Known and benign: WARNING with no traceback, no
            # failure recorded, the run moves on to the next symbol-day.
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
    rows = con.execute("SELECT count(*) FROM read_parquet($files)", {"files": paths}).fetchone()[0]
    con.execute(
        f"COPY (SELECT * FROM read_parquet($files) ORDER BY exchange_ts) TO '{out.as_posix()}' "
        "(FORMAT PARQUET)",
        {"files": paths},
    )
    return int(rows)


def _absorbed(con: duckdb.DuckDBPyConnection, fragments: list[Path], compacted: Path) -> bool:
    """True when every fragment row is already present in ``compacted`` (and it is no smaller) —
    the containment check that makes deleting an interrupted run's leftovers safe rather than
    hopeful."""
    paths = [p.as_posix() for p in fragments]
    missing = con.execute(
        "SELECT count(*) FROM (SELECT * FROM read_parquet($frags) "
        "EXCEPT SELECT * FROM read_parquet($compacted))",
        {"frags": paths, "compacted": [compacted.as_posix()]},
    ).fetchone()[0]
    if missing:
        return False
    n_frag = con.execute("SELECT count(*) FROM read_parquet($frags)", {"frags": paths}).fetchone()[0]
    n_comp = con.execute(
        "SELECT count(*) FROM read_parquet($compacted)", {"compacted": [compacted.as_posix()]}
    ).fetchone()[0]
    return int(n_comp) >= int(n_frag)


def _cleanup_stale_tmp(sym_dir: Path) -> None:
    """Drop a staging file orphaned by a crash BEFORE its swap (it holds no rows the fragments
    don't). Never touches ``*.parquet`` — a stale tmp is invisible to readers either way."""
    tmp = sym_dir / _TMP_NAME
    if tmp.exists():
        tmp.unlink(missing_ok=True)
        _log.info("tick_compaction_stale_tmp_removed", partition=sym_dir.name)
