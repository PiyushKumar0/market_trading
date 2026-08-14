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
import threading
from datetime import date, datetime, timedelta
from decimal import Decimal

import duckdb
import pytest

from engine.core.clock import IST
from engine.core.types import Tick
from engine.marketdata import tick_compact as tick_compact_module
from engine.marketdata.store import MarketStore
from engine.marketdata.tick_compact import COMPACT_NAME, TickCompactionResult, compact_ticks

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


def test_unverifiable_mix_is_left_alone_and_reported(store, tmp_path):
    """A fragment carrying rows the compacted file does NOT have is unreachable by design (the
    writer never appends to a past date). If it happens anyway, the partition is left untouched and
    the run reports not-ok — a failed watermark and a human, never a guessed merge."""
    _write_fragments(store, YESTERDAY, n=3)
    compact_ticks(tmp_path / "parquet", upto=TODAY, today=TODAY)
    _write_fragments(store, YESTERDAY, n=1, start=10)         # a NEW, unabsorbed fragment
    before = store.get_ticks("RELIANCE", YESTERDAY)

    result = compact_ticks(tmp_path / "parquet", upto=TODAY, today=TODAY)

    assert result.ok is False
    assert result.failures and "unverifiable" in result.failures[0]
    assert len(_files(store, YESTERDAY)) == 2                 # untouched: nothing deleted, nothing merged
    assert store.get_ticks("RELIANCE", YESTERDAY) == before


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
