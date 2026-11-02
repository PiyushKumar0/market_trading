"""Tick-partition compaction (§4.3 storage, WO-7).

Fixtures are written by the REAL tick writer (``MarketStore.flush_ticks``) so the fragments under
test have the exact shape the live engine produces — ULID-named one-file-per-flush partitions —
and are read back through the REAL reader (``MarketStore.get_ticks``, which globs ``*.parquet``),
which is what makes "row-identical before/after" a statement about the engine rather than about
DuckDB. Acceptance (WO-7): N fragments → 1 file, rows identical, re-run is a no-op, and the CURRENT
trading date is never touched (the writer is still appending to it).
"""

from __future__ import annotations

import logging
import os
import shutil
import threading
from datetime import date, datetime, timedelta
from decimal import Decimal

import duckdb
import pytest

from engine.core.clock import IST, Clock
from engine.core.types import Tick
from engine.marketdata import tick_compact as tick_compact_module
from engine.marketdata.bar_builder import BarBuilder
from engine.marketdata.store import MarketStore
from engine.marketdata.tick_compact import (
    COMPACT_NAME,
    COMPACT_PREFIX,
    TickCompactionResult,
    compact_ticks,
)

TODAY = date(2026, 6, 17)          # conftest's frozen clock day
YESTERDAY = date(2026, 6, 16)


@pytest.fixture
def store(tmp_path, clock):
    s = MarketStore(tmp_path / "market.duckdb", tmp_path / "parquet", clock)
    s.open()
    yield s
    s.close()


def _tick(ts: datetime, *, symbol: str = "RELIANCE", ltp: str = "2338.55", vol: int = 100) -> Tick:
    return Tick(
        instrument_token=738561, tradingsymbol=symbol, ltp=Decimal(ltp), volume_traded=vol,
        exchange_ts=ts, avg_price=Decimal("2338.1234"), bid=Decimal("2338.50"), ask=Decimal("2338.60"),
    )


def _write_fragments(
    store: MarketStore, d: date, *, symbol: str = "RELIANCE", n: int = 5, start: int = 0
) -> None:
    """``n`` separate flushes ⇒ ``n`` fragment files in that symbol-day (the live pathology: ~7.5 K
    of these per symbol-day, 752,150 files for one session — audit F9). ``start`` offsets the tick
    timestamps/volumes so a later batch carries genuinely new rows, not repeats."""
    base = datetime.combine(d, datetime.min.time(), tzinfo=IST) + timedelta(hours=10)
    for i in range(start, start + n):
        store.buffer_tick(_tick(base + timedelta(seconds=i), symbol=symbol, vol=i + 1))
        store.flush_ticks()


def _files(store: MarketStore, d: date, symbol: str = "RELIANCE") -> list:
    part = store._parquet_root / "ticks" / f"date={d.isoformat()}" / f"symbol={symbol}"
    return sorted(part.glob("*.parquet"))


def _rows(store: MarketStore, d: date, symbol: str = "RELIANCE") -> list[Tick]:
    """The symbol-day through the real reader, in an order that does not depend on how rows that
    share an ``exchange_ts`` happen to come back (an echo shares its stamp with the last print)."""
    return sorted(store.get_ticks(symbol, d), key=lambda t: (t.exchange_ts, t.volume_traded))


def test_fragments_collapse_to_one_file_with_identical_rows(store, tmp_path):
    _write_fragments(store, YESTERDAY, n=5)
    before = store.get_ticks("RELIANCE", YESTERDAY)
    assert len(_files(store, YESTERDAY)) == 5 and len(before) == 5

    result = compact_ticks(tmp_path / "parquet", upto=TODAY, today=TODAY)

    files = _files(store, YESTERDAY)
    assert [f.name for f in files] == [COMPACT_NAME]          # ONE file per symbol-day
    assert store.get_ticks("RELIANCE", YESTERDAY) == before   # row-identical through the real reader
    assert result.ok is True
    assert (result.symbol_days_compacted, result.fragments_removed, result.rows_written) == (1, 5, 5)
    assert result.dates == [YESTERDAY.isoformat()]


def test_a_zero_byte_fragment_is_quarantined_and_the_survivors_still_compact(store, tmp_path):
    """2026-08-26 containment — the FOURTH manual quarantine of this class became the last: a
    truncated (0-byte) fragment used to fail the WHOLE partition via DuckDB's read_parquet
    ("too small to be a Parquet file"); it is now moved to the quarantine dir exactly the way the
    manual ritual did it, and the partition compacts from the survivors with ok=True."""
    _write_fragments(store, YESTERDAY, n=3)
    before = store.get_ticks("RELIANCE", YESTERDAY)   # BEFORE planting: the reader's glob would
    part = store._parquet_root / "ticks" / f"date={YESTERDAY.isoformat()}" / "symbol=RELIANCE"
    corrupt = part / "01ZZZZZZZZZZZZZZZZZZZZZZZZ.parquet"
    corrupt.write_bytes(b"")                          # choke on the 0-byte file exactly like DuckDB

    result = compact_ticks(tmp_path / "parquet", upto=TODAY, today=TODAY)

    assert result.ok is True and result.failures == []
    assert [f.name for f in _files(store, YESTERDAY)] == [COMPACT_NAME]
    assert store.get_ticks("RELIANCE", YESTERDAY) == before
    qdir = store._parquet_root / "quarantine"
    quarantined = list(qdir.glob("*.parquet"))
    assert [q.name for q in quarantined] == [
        f"{YESTERDAY.isoformat()}_RELIANCE_{corrupt.name}"
    ]
    assert quarantined[0].stat().st_size == 0
    assert not corrupt.exists()


def test_a_zero_byte_fragment_beside_a_compacted_file_is_quarantined_and_the_day_stays_ok(
    store, tmp_path
):
    """Quarantine can leave the compacted file with no fragment beside it: nothing is left to
    reconcile, so the symbol-day resolves clean."""
    _write_fragments(store, YESTERDAY, n=3)
    compact_ticks(tmp_path / "parquet", upto=TODAY, today=TODAY)
    rows = _rows(store, YESTERDAY)
    corrupt = _files(store, YESTERDAY)[0].parent / "01ZZZZZZZZZZZZZZZZZZZZZZZZ.parquet"
    corrupt.write_bytes(b"")

    result = compact_ticks(tmp_path / "parquet", upto=TODAY, today=TODAY)

    assert result.ok is True and result.failures == []
    assert [f.name for f in _files(store, YESTERDAY)] == [COMPACT_NAME]
    assert _rows(store, YESTERDAY) == rows
    assert not corrupt.exists()


def test_rerun_on_a_compacted_day_is_a_no_op(store, tmp_path):
    _write_fragments(store, YESTERDAY, n=4)
    compact_ticks(tmp_path / "parquet", upto=TODAY, today=TODAY)
    rows = store.get_ticks("RELIANCE", YESTERDAY)
    stamp = _files(store, YESTERDAY)[0].stat().st_mtime_ns

    again = compact_ticks(tmp_path / "parquet", upto=TODAY, today=TODAY)

    assert again.ok is True
    assert (again.symbol_days_compacted, again.fragments_removed) == (0, 0)   # nothing rewritten
    assert _files(store, YESTERDAY)[0].stat().st_mtime_ns == stamp            # not even touched
    assert store.get_ticks("RELIANCE", YESTERDAY) == rows


def test_current_trading_date_is_never_compacted(store, tmp_path):
    """The writer is still appending to today's partition — its fragment list is not a closed set."""
    _write_fragments(store, TODAY, n=3)
    _write_fragments(store, YESTERDAY, n=3)

    result = compact_ticks(tmp_path / "parquet", upto=TODAY, today=TODAY)

    assert len(_files(store, TODAY)) == 3                     # untouched
    assert [f.name for f in _files(store, YESTERDAY)] == [COMPACT_NAME]
    assert result.skipped_current_date == TODAY.isoformat()
    assert result.dates == [YESTERDAY.isoformat()]


def test_every_symbol_in_a_date_partition_is_compacted_independently(store, tmp_path):
    _write_fragments(store, YESTERDAY, symbol="RELIANCE", n=3)
    _write_fragments(store, YESTERDAY, symbol="TCS", n=2)
    before = {s: store.get_ticks(s, YESTERDAY) for s in ("RELIANCE", "TCS")}

    result = compact_ticks(tmp_path / "parquet", upto=TODAY, today=TODAY)

    for symbol in ("RELIANCE", "TCS"):
        assert [f.name for f in _files(store, YESTERDAY, symbol)] == [COMPACT_NAME]
        assert store.get_ticks(symbol, YESTERDAY) == before[symbol]
    assert (result.symbol_days_compacted, result.fragments_removed) == (2, 5)


def test_upto_bounds_the_run_to_the_date_keyed_slot(store, tmp_path):
    """The date-keyed replay of an older slot compacts only what that slot could have seen; the
    later slots pick up the rest (and are no-ops on what this run already did)."""
    older = date(2026, 6, 12)
    _write_fragments(store, older, n=2)
    _write_fragments(store, YESTERDAY, n=2)

    first = compact_ticks(tmp_path / "parquet", upto=older, today=TODAY)
    assert first.dates == [older.isoformat()]
    assert [f.name for f in _files(store, older)] == [COMPACT_NAME]
    assert len(_files(store, YESTERDAY)) == 2                 # out of this slot's range

    second = compact_ticks(tmp_path / "parquet", upto=TODAY, today=TODAY)
    assert second.symbol_days_compacted == 1                  # only YESTERDAY had work left
    assert [f.name for f in _files(store, YESTERDAY)] == [COMPACT_NAME]


def test_interrupted_unlink_is_repaired_not_duplicated(store, tmp_path):
    """Crash window (the deliberate ordering: swap first, unlink second — the reverse would lose
    rows): fragments left beside a compacted file are absorbed leftovers and get deleted, never
    merged back in (which would duplicate every row in the symbol-day)."""
    _write_fragments(store, YESTERDAY, n=3)
    rows = store.get_ticks("RELIANCE", YESTERDAY)
    fragments = {f: f.read_bytes() for f in _files(store, YESTERDAY)}
    compact_ticks(tmp_path / "parquet", upto=TODAY, today=TODAY)
    for path, blob in fragments.items():                      # replay the interrupted unlink
        path.write_bytes(blob)
    assert len(_files(store, YESTERDAY)) == 4

    result = compact_ticks(tmp_path / "parquet", upto=TODAY, today=TODAY)

    assert result.ok is True
    assert [f.name for f in _files(store, YESTERDAY)] == [COMPACT_NAME]
    assert store.get_ticks("RELIANCE", YESTERDAY) == rows     # no duplicated rows
    assert result.fragments_removed == 3


def test_late_rows_beside_a_compacted_file_are_merged_in(store, tmp_path):
    """2026-10-26..11-02, the live shape: a ticker reconnect's snapshot echo (the session's last
    stamp, volume zeroed by the weekend) written into a partition AFTER its compaction. Reported
    "unverifiable" and left alone, it failed the symbol-day on every run — 601 of them, the watermark
    never cleared and retention stopped. The row the compacted file lacks is merged in, and nothing
    it already holds is doubled."""
    _write_fragments(store, YESTERDAY, n=3)
    compact_ticks(tmp_path / "parquet", upto=TODAY, today=TODAY)
    last = store.get_ticks("RELIANCE", YESTERDAY)[-1]
    store.buffer_tick(_tick(last.exchange_ts, vol=0))          # the echo: same stamp, not a duplicate
    store.flush_ticks()
    before = _rows(store, YESTERDAY)
    assert len(_files(store, YESTERDAY)) == 2 and len(before) == 4

    result = compact_ticks(tmp_path / "parquet", upto=TODAY, today=TODAY)

    assert result.ok is True and result.failures == []
    assert [f.name for f in _files(store, YESTERDAY)] == [COMPACT_NAME]
    assert _rows(store, YESTERDAY) == before
    assert (result.symbol_days_merged, result.fragments_removed, result.rows_written) == (1, 1, 4)


def test_a_merge_interrupted_before_its_unlink_is_repaired_not_duplicated(store, tmp_path):
    """A merge swaps first and unlinks second, like a first compaction, so it has the same crash
    window: the late fragment left beside the merged file is now inside it, and the next run
    deletes it instead of merging it a second time."""
    _write_fragments(store, YESTERDAY, n=3)
    compact_ticks(tmp_path / "parquet", upto=TODAY, today=TODAY)
    _write_fragments(store, YESTERDAY, n=1, start=10)         # late, genuinely new rows
    (late,) = [f for f in _files(store, YESTERDAY) if f.name != COMPACT_NAME]
    blob = late.read_bytes()
    compact_ticks(tmp_path / "parquet", upto=TODAY, today=TODAY)
    merged = _rows(store, YESTERDAY)
    late.write_bytes(blob)                                    # replay the interrupted unlink

    result = compact_ticks(tmp_path / "parquet", upto=TODAY, today=TODAY)

    assert result.ok is True
    assert [f.name for f in _files(store, YESTERDAY)] == [COMPACT_NAME]
    assert _rows(store, YESTERDAY) == merged and len(merged) == 4    # no duplicated rows
    assert (result.symbol_days_merged, result.fragments_removed) == (0, 1)


def test_leftovers_and_late_rows_in_one_run_are_merged_without_doubling_the_leftovers(store, tmp_path):
    """Both kinds of fragment beside one compacted file at once: an interrupted first compaction's
    leftovers (already inside it) and a late write (not). Only the late row is added."""
    _write_fragments(store, YESTERDAY, n=3)
    leftovers = {f: f.read_bytes() for f in _files(store, YESTERDAY)}
    compact_ticks(tmp_path / "parquet", upto=TODAY, today=TODAY)
    for path, blob in leftovers.items():                      # replay the interrupted unlink
        path.write_bytes(blob)
    _write_fragments(store, YESTERDAY, n=1, start=10)         # and a late, genuinely new row
    assert len(_files(store, YESTERDAY)) == 5

    result = compact_ticks(tmp_path / "parquet", upto=TODAY, today=TODAY)

    assert result.ok is True
    assert [f.name for f in _files(store, YESTERDAY)] == [COMPACT_NAME]
    assert [t.volume_traded for t in _rows(store, YESTERDAY)] == [1, 2, 3, 11]
    assert (result.symbol_days_merged, result.fragments_removed) == (1, 4)


def test_a_row_on_both_sides_is_kept_as_often_as_the_side_with_more_copies(store, tmp_path):
    """Multiset, not set: a late fragment holding a stored row twice leaves it twice — plain EXCEPT
    would find nothing to add and drop the second copy — and never three times, which appending the
    whole fragment would."""
    _write_fragments(store, YESTERDAY, n=3)
    compact_ticks(tmp_path / "parquet", upto=TODAY, today=TODAY)
    last = store.get_ticks("RELIANCE", YESTERDAY)[-1]
    store.buffer_tick(last)
    store.buffer_tick(last)
    store.flush_ticks()                                       # one fragment: the stored row, twice

    result = compact_ticks(tmp_path / "parquet", upto=TODAY, today=TODAY)

    assert result.ok is True
    assert [f.name for f in _files(store, YESTERDAY)] == [COMPACT_NAME]
    assert [t.volume_traded for t in _rows(store, YESTERDAY)] == [1, 2, 3, 3]
    assert (result.symbol_days_merged, result.rows_written) == (1, 4)


def test_a_failed_swap_loses_nothing_and_the_retry_converges(store, tmp_path, monkeypatch):
    """On Windows ``os.replace`` onto a file another process holds open fails (scripts/replay_day.py
    reads compacted files): the symbol-day reports not-ok with the compacted file and the late
    fragment both intact, and the next run completes the merge."""
    _write_fragments(store, YESTERDAY, n=3)
    compact_ticks(tmp_path / "parquet", upto=TODAY, today=TODAY)
    _write_fragments(store, YESTERDAY, n=1, start=10)
    before = _rows(store, YESTERDAY)
    real_replace = os.replace
    calls: list[object] = []

    def held_open_once(src, dst):
        calls.append(dst)
        if len(calls) == 1:
            raise PermissionError(13, "The process cannot access the file because it is being used")
        return real_replace(src, dst)

    monkeypatch.setattr(tick_compact_module.os, "replace", held_open_once)

    first = compact_ticks(tmp_path / "parquet", upto=TODAY, today=TODAY)
    assert first.ok is False and "PermissionError" in first.failures[0]
    assert len(_files(store, YESTERDAY)) == 2 and _rows(store, YESTERDAY) == before

    second = compact_ticks(tmp_path / "parquet", upto=TODAY, today=TODAY)
    assert second.ok is True and second.symbol_days_merged == 1
    assert [f.name for f in _files(store, YESTERDAY)] == [COMPACT_NAME]
    assert _rows(store, YESTERDAY) == before


def test_two_compacted_files_are_left_alone_and_reported(store, tmp_path):
    """The one shape still refused: this job writes a single deterministic name, so two
    ``compact-`` files are not its output and which one holds the symbol-day is unknowable — the
    partition is left untouched and the run reports not-ok."""
    _write_fragments(store, YESTERDAY, n=3)
    compact_ticks(tmp_path / "parquet", upto=TODAY, today=TODAY)
    part = _files(store, YESTERDAY)[0].parent
    shutil.copyfile(part / COMPACT_NAME, part / f"{COMPACT_PREFIX}copy.parquet")
    _write_fragments(store, YESTERDAY, n=1, start=10)
    names = [f.name for f in _files(store, YESTERDAY)]

    result = compact_ticks(tmp_path / "parquet", upto=TODAY, today=TODAY)

    assert result.ok is False
    assert result.failures and "more than one compacted file" in result.failures[0]
    assert [f.name for f in _files(store, YESTERDAY)] == names    # untouched


def test_a_reconnect_echo_written_after_compaction_is_merged_by_the_next_run(tmp_path):
    """The seam that failed live, through the REAL live write path (BarBuilder → MarketStore): a
    session's partition compacted after midnight (the host slept through 22:30), then a reconnect
    re-sends the last state, still stamped with that session and its volume zeroed. The writer
    persists it beside the compacted file, and the next run must merge it and finish clean."""
    wall = {"now": datetime(2026, 6, 16, 15, 29, 50, tzinfo=IST)}
    clock = Clock(time_source=lambda: wall["now"])
    s = MarketStore(tmp_path / "market.duckdb", tmp_path / "parquet", clock)
    s.open()
    try:
        bb = BarBuilder(s, clock)
        for i in range(3):
            wall["now"] = datetime(2026, 6, 16, 15, 29, 50 + i, tzinfo=IST)
            bb.on_tick(_tick(wall["now"], vol=100 + i))
            s.flush_ticks()
        wall["now"] = datetime(2026, 6, 17, 0, 30, tzinfo=IST)
        assert compact_ticks(tmp_path / "parquet", upto=YESTERDAY, today=TODAY).symbol_days_compacted == 1

        wall["now"] = datetime(2026, 6, 17, 2, 22, tzinfo=IST)
        bb.on_tick(_tick(datetime(2026, 6, 16, 17, 34, 39, tzinfo=IST), vol=0))
        s.flush_ticks()
        assert len(_files(s, YESTERDAY)) == 2                 # the echo landed after the compaction

        again = compact_ticks(tmp_path / "parquet", upto=YESTERDAY, today=TODAY)
        assert again.ok is True and again.failures == []
        assert (again.symbol_days_merged, again.fragments_removed) == (1, 1)
        assert [f.name for f in _files(s, YESTERDAY)] == [COMPACT_NAME]
        assert [t.volume_traded for t in _rows(s, YESTERDAY)] == [100, 101, 102, 0]
    finally:
        s.close()


def test_missing_tick_root_is_a_clean_no_op(tmp_path):
    result = compact_ticks(tmp_path / "parquet", upto=TODAY, today=TODAY)
    assert (result.ok, result.dates, result.symbol_days_compacted) == (True, [], 0)


def test_skipped_result_defaults_are_watermark_neutral() -> None:
    """``engine.ops.jobs._job_result_ok`` reads a job result's ``.ok`` — a skip must leave it True
    (and touch nothing else) so the in-flight run owns the day's real watermark outcome, not this
    no-op (2026-08-14)."""
    result = TickCompactionResult(skipped_in_flight=True)
    assert result.ok is True
    assert (result.dates, result.symbol_days_compacted, result.failures) == ([], 0, [])


def test_two_concurrent_calls_exactly_one_does_the_work(store, tmp_path, monkeypatch) -> None:
    """2026-08-14 live: the post-arm one-shot and the 22:30 scheduled slot both reached
    ``compact_ticks`` for the same partitions (WO-15's ``CatchUpRunner`` single-flight does not
    cover this path), producing 78 read-time failures. The module-level lock makes this
    single-flight: exactly one caller does the work, the other returns the skip shape without ever
    touching the store or the filesystem."""
    _write_fragments(store, YESTERDAY, n=3)
    entered = threading.Event()
    release = threading.Event()
    original = tick_compact_module._compact_ticks_locked

    def slow_locked(*args, **kwargs):
        entered.set()
        assert release.wait(timeout=5), "test setup: release was never signalled"
        return original(*args, **kwargs)

    monkeypatch.setattr(tick_compact_module, "_compact_ticks_locked", slow_locked)

    results: dict[str, TickCompactionResult] = {}

    def call_a() -> None:
        results["a"] = compact_ticks(tmp_path / "parquet", upto=TODAY, today=TODAY)

    def call_b() -> None:
        assert entered.wait(timeout=5), "test setup: A never reached the lock"
        results["b"] = compact_ticks(tmp_path / "parquet", upto=TODAY, today=TODAY)
        release.set()                       # let A proceed only after B has observed the skip

    ta, tb = threading.Thread(target=call_a), threading.Thread(target=call_b)
    ta.start()
    tb.start()
    ta.join(timeout=5)
    tb.join(timeout=5)

    a, b = results["a"], results["b"]
    assert b.skipped_in_flight is True
    assert b.ok is True                                          # watermark-neutral
    assert (b.dates, b.symbol_days_compacted, b.fragments_removed) == ([], 0, 0)
    assert a.skipped_in_flight is False
    assert (a.symbol_days_compacted, a.fragments_removed) == (1, 3)   # only A did the work
    assert [f.name for f in _files(store, YESTERDAY)] == [COMPACT_NAME]


def test_vanished_fragments_log_a_warning_without_a_traceback_and_the_run_continues(
    store, tmp_path, monkeypatch, caplog
) -> None:
    """2026-08-14: a concurrent compactor can unlink these exact fragments between this run's glob
    and DuckDB's read of them — known and benign now, so it is a one-line WARNING (no traceback),
    ``result.ok`` stays True, and the run still reaches the NEXT symbol-day rather than stopping."""
    _write_fragments(store, YESTERDAY, symbol="AAA", n=3)        # will "vanish" underneath this run
    _write_fragments(store, YESTERDAY, symbol="BBB", n=2)        # must still compact normally
    real_write_compacted = tick_compact_module._write_compacted

    def flaky_write_compacted(con, fragments, out):
        if any("AAA" in p.as_posix() for p in fragments):
            raise duckdb.IOException(
                f'IO Error: No files found that match the pattern "{fragments[0].as_posix()}"'
            )
        return real_write_compacted(con, fragments, out)

    monkeypatch.setattr(tick_compact_module, "_write_compacted", flaky_write_compacted)

    with caplog.at_level(logging.WARNING, logger="engine.marketdata.tick_compact"):
        result = compact_ticks(tmp_path / "parquet", upto=TODAY, today=TODAY)

    warnings = [r for r in caplog.records if r.getMessage() == "tick_compaction_fragments_vanished"]
    assert len(warnings) == 1
    assert warnings[0].levelname == "WARNING"
    assert warnings[0].exc_info is None                          # no traceback

    assert result.ok is True                                     # known-benign, never a failed watermark
    assert result.failures == []
    assert len(_files(store, YESTERDAY, "AAA")) == 3              # untouched: nothing written or deleted
    assert [f.name for f in _files(store, YESTERDAY, "BBB")] == [COMPACT_NAME]   # the run continued
    assert result.symbol_days_compacted == 1


def test_a_run_is_bounded_and_resumes_where_it_stopped(store, tmp_path):
    """Bounded + observable (the §2.6 filed proposal): the FIRST run faces the whole standing
    backlog, so a run does at most ``max_symbol_days`` and the rest drains on the next one —
    oldest-first plus per-symbol-day idempotence makes the progress monotone."""
    for symbol in ("AAA", "BBB", "CCC"):
        _write_fragments(store, YESTERDAY, symbol=symbol, n=2)

    first = compact_ticks(tmp_path / "parquet", upto=TODAY, today=TODAY, max_symbol_days=2)
    assert (first.symbol_days_compacted, first.budget_exhausted, first.ok) == (2, True, True)
    assert [f.name for f in _files(store, YESTERDAY, "AAA")] == [COMPACT_NAME]
    assert len(_files(store, YESTERDAY, "CCC")) == 2                  # not reached this run

    second = compact_ticks(tmp_path / "parquet", upto=TODAY, today=TODAY, max_symbol_days=2)
    assert (second.symbol_days_compacted, second.budget_exhausted) == (1, False)
    assert all([f.name for f in _files(store, YESTERDAY, s)] == [COMPACT_NAME]
               for s in ("AAA", "BBB", "CCC"))


def test_a_run_ends_before_the_next_symbol_day_once_stop_fires(store, tmp_path):
    """2026-09-29: ``stop`` (the WO-21 window, or a stop signal) ends a run between symbol-days —
    never inside one — and the result is unfinished, so its runner records no watermark and a later
    pass resumes it."""
    for symbol in ("AAA", "BBB", "CCC"):
        _write_fragments(store, YESTERDAY, symbol=symbol, n=2)
    polls: list[int] = []

    def stop() -> bool:
        polls.append(1)
        return len(polls) > 1                          # let the first symbol-day through, then stop

    first = compact_ticks(tmp_path / "parquet", upto=TODAY, today=TODAY, stop=stop)
    assert (first.symbol_days_compacted, first.stopped, first.unfinished, first.ok) == (1, True, True, True)
    assert [f.name for f in _files(store, YESTERDAY, "AAA")] == [COMPACT_NAME]
    assert len(_files(store, YESTERDAY, "BBB")) == 2   # not started: no half-compacted partition

    second = compact_ticks(tmp_path / "parquet", upto=TODAY, today=TODAY, stop=lambda: False)
    assert (second.symbol_days_compacted, second.stopped, second.unfinished) == (2, False, False)


def test_compaction_connection_is_memory_bounded(store, tmp_path):
    """2026-08-18 (the 53 GB incident): the compaction connection must carry an explicit DuckDB
    memory_limit and a spill temp_directory inside ticks/, and the dot-named spill dir must be
    invisible to the date-partition enumeration (a run with it present still succeeds)."""
    ticks_root = tmp_path / "ticks"
    ticks_root.mkdir(parents=True, exist_ok=True)
    con = tick_compact_module._open_connection(ticks_root)
    try:
        # DuckDB normalizes '4GB' to a GiB rendering — assert the bound numerically, not textually:
        # anything at or under 4 GiB and far from the ~80%-of-RAM default proves the SET applied.
        limit = con.execute("SELECT current_setting('memory_limit')").fetchone()[0]
        value, unit = limit.split()
        assert unit in ("GiB", "GB"), limit
        assert float(value) <= 4.0, limit
        tmpdir = con.execute("SELECT current_setting('temp_directory')").fetchone()[0]
        assert tick_compact_module._SPILL_DIRNAME in tmpdir
    finally:
        con.close()
    assert (ticks_root / tick_compact_module._SPILL_DIRNAME).is_dir()
    result = compact_ticks(tmp_path, upto=date(2026, 8, 14), today=date(2026, 8, 18))
    assert result.ok
    assert result.dates == []  # the spill dir is not a date partition and breaks nothing
