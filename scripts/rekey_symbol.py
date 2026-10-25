#!/usr/bin/env python
"""Move a renamed symbol's stored history to its new symbol (NSE rename; 2026-10-23 ICICIPRULI→ICICILIFE).

    python scripts/rekey_symbol.py OLD NEW [--apply]

Kite keeps the instrument token across an NSE rename and the universe follows it
(``MarketStore.get_symbol_renames``), but every row keyed by symbol stays under the old one, so
daily-lookback rules read the new symbol as a fresh listing. This moves the company's data: in
market.duckdb, ``MarketStore.rekey_symbol`` (bars, features, filings, corporate actions, news symbols,
aliases, the ISIN map, ...); in state.db, :data:`STATE_TABLES` (backfill progress, brk20 resting levels,
pending ``ins`` entries). Where the new symbol already holds a row with the same key, its row wins.
Records of what happened under the old name stay: universe_daily, instruments_daily, the logs, positions
and the journals.

Refused (exit 2) unless the instrument snapshots show OLD→NEW as a rename and ``<data>/engine.lock`` is
free; the lock is held for the whole run, so no engine boots mid-move. ``--apply`` is also refused while a
live position (PENDING_ENTRY / OPEN / PENDING_EXIT) names OLD, since moving order state is the owner's
call; while OLD has a BSE insider row inside the fresh feed's window, since the feed would re-fetch it
under NEW with a new content-hash id, a duplicate of the moved copy that the twin check cannot see yet;
and when the rename came with a trading gap (``MarketStore.rename_trading_gap``; HEG→HEGAM was
suspended 10 sessions around its demerger), since the moved history would carry the hole and the warm-up
would refuse NEW for a gapped window instead of treating it as a fresh listing.

Default is a dry run: the same statements run in transactions that are rolled back, so the counts printed
are the ones ``--apply`` commits. market.duckdb cannot be opened while the engine holds it, so the dry run
needs the engine stopped too; rehearse on a copy with ``MT_DATA_DIR`` pointing at it. ``--apply`` first
writes the rows the move can change to ``<backups>/rekey-OLD-NEW-<stamp>/``: ``<table>.parquet`` per
market.duckdb table and a ``state.db`` copy. Undo, engine stopped: delete the rows naming OLD or NEW from
each table, insert the parquet back, and put the state.db copy in place. Each table is verified before its
transaction ends (no row names OLD; NEW holds what it held plus what moved) and a mismatch rolls back.
market.duckdb commits before state.db; if the state.db step fails, re-run (the first part then moves
nothing). A second run moves nothing.

Exit codes: 0 = dry run printed or applied; 2 = refused or error.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

_REPO_SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if _REPO_SRC not in sys.path:  # pragma: no cover - loose-script shim
    sys.path.insert(0, _REPO_SRC)

import duckdb  # noqa: E402

import engine  # noqa: E402,F401  native import-order guard (sklearn before numba/vectorbt/cvxpy)
from engine.core.clock import IST, Clock  # noqa: E402
from engine.core.config import Settings, load_settings  # noqa: E402
from engine.core.db import connect  # noqa: E402
from engine.datafeeds.filings_pit_fresh import BSE_SOURCE, FRESH_WINDOW_DAYS  # noqa: E402
from engine.marketdata.store import MarketStore, RekeyCount, SymbolRename  # noqa: E402
from engine.oms.state import PositionState  # noqa: E402
from engine.ops.single_instance import InstanceLock  # noqa: E402

#: state.db tables keyed by symbol that move with the company: its backfill progress and pending
#: strategy state. Positions, GTTs and the journals are records and stay.
STATE_TABLES: tuple[str, ...] = ("backfill_checkpoints", "brk20_resting_levels", "ins_pending")

LIVE_POSITION_STATES: tuple[str, ...] = (
    PositionState.PENDING_ENTRY.value, PositionState.OPEN.value, PositionState.PENDING_EXIT.value,
)

#: get_symbol_renames window for the pair check: every stored snapshot.
RENAME_LOOKBACK_DAYS = 3650


def rekey_state(conn: sqlite3.Connection, old: str, new: str, *, apply: bool) -> list[RekeyCount]:
    """:data:`STATE_TABLES` rows of ``old`` moved to ``new`` in one transaction (``new``'s row wins a key
    collision), each table verified before it ends; rolled back unless ``apply``."""
    counts: list[RekeyCount] = []
    conn.execute("BEGIN IMMEDIATE")
    try:
        for table in STATE_TABLES:
            others = [r[1] for r in conn.execute(f'PRAGMA table_info("{table}")') if r[5] and r[1] != "symbol"]
            same_key = "".join(f' AND n."{k}" = {table}."{k}"' for k in others)

            def naming(symbol: str, table: str = table) -> int:
                return conn.execute(f"SELECT count(*) FROM {table} WHERE symbol = ?", (symbol,)).fetchone()[0]

            old_rows, new_rows = naming(old), naming(new)
            dropped = conn.execute(
                f"DELETE FROM {table} WHERE symbol = ? AND EXISTS "
                f"(SELECT 1 FROM {table} AS n WHERE n.symbol = ?{same_key})",
                (old, new),
            ).rowcount
            moved = conn.execute(f"UPDATE {table} SET symbol = ? WHERE symbol = ?", (new, old)).rowcount
            still_old, now_new = naming(old), naming(new)
            if still_old or now_new != new_rows + moved:
                raise RuntimeError(
                    f"{table}.symbol: {still_old} row(s) still name {old} and {now_new} name {new}, "
                    f"expected 0 and {new_rows} + {moved} moved"
                )
            counts.append(RekeyCount(table=table, column="symbol", old_rows=old_rows, new_rows=new_rows,
                                     moved=moved, dropped=dropped))
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    conn.execute("COMMIT" if apply else "ROLLBACK")
    return counts


def blockers(store: MarketStore, conn: sqlite3.Connection, old: str, new: str, today: date) -> list[str]:
    """Why ``--apply`` must wait (module docstring); empty when it may run."""
    out: list[str] = []
    marks = ", ".join("?" for _ in LIVE_POSITION_STATES)
    live = conn.execute(
        f"SELECT count(*) FROM positions WHERE symbol = ? AND state IN ({marks})", (old, *LIVE_POSITION_STATES)
    ).fetchone()[0]
    if live:
        out.append(f"{live} live position(s) name {old}: settle or move them first (moving order state is "
                   "the owner's call)")
    gap = store.rename_trading_gap(old, new)
    if gap:
        out.append(f"{gap} market session(s) between {old}'s last daily bar and {new}'s first have no bar under "
                   f"either symbol (a suspension, e.g. around a scheme of arrangement). The moved history would "
                   f"carry that hole, so the warm-up would refuse {new} for a gapped 200-session window instead "
                   f"of treating it as a fresh listing; leave {old}'s history where it is")
    last = store.latest_insider_broadcast(BSE_SOURCE, symbol=old)
    if last is not None and last.date() >= today - timedelta(days=FRESH_WINDOW_DAYS):
        retry = last.date() + timedelta(days=FRESH_WINDOW_DAYS + 1)
        out.append(f"{old}'s newest BSE insider row ({last:%Y-%m-%d %H:%M}) is inside the {FRESH_WINDOW_DAYS}-day "
                   f"fresh-feed window, so the feed would re-fetch it under {new} as a duplicate. "
                   f"Re-run on or after {retry.isoformat()}")
    return out


def _report(rename: SymbolRename, counts: list[RekeyCount], left: dict[str, int], *, apply: bool,
            undo: Path | None, waits: list[str]) -> None:
    head = "APPLIED" if apply else "DRY RUN (rolled back, nothing changed)"
    print(f"{head}: {rename.old_symbol} -> {rename.new_symbol} (Kite token {rename.instrument_token}, "
          f"{rename.new_symbol} since {rename.effective_d.isoformat()})\n")
    print(f"{'column':<38}{'old':>9}{'new':>9}{'moved':>9}{'dropped':>9}")
    for c in counts:
        print(f"{c.table + '.' + c.column:<38}{c.old_rows:>9}{c.new_rows:>9}{c.moved:>9}{c.dropped:>9}")
    named = ", ".join(f"{column} {n}" for column, n in left.items() if n)
    print(f"\nleft naming {rename.old_symbol} (records): {named or 'none'}")
    if left.get("theme_map.symbols"):
        print(f"theme_map is rewritten weekly from config/themes.yaml: rename {rename.old_symbol} there "
              "(owner-approved file).")
    if undo is not None:
        print(f"\nundo copy: {undo}")
    for wait in waits:
        print(f"\nNOTE: --apply would be refused: {wait}")


def _run(settings: Settings, old: str, new: str, today: date, *, apply: bool) -> int:
    store = MarketStore(settings.duckdb_path(), settings.parquet_dir(), Clock()).open()
    try:
        conn = connect(settings.sqlite_path())
        try:
            rename = next(
                (r for r in store.get_symbol_renames(today, lookback_days=RENAME_LOOKBACK_DAYS)
                 if (r.old_symbol, r.new_symbol) == (old, new)),
                None,
            )
            if rename is None:
                print(f"rekey: refused: the instrument snapshots show no rename {old} -> {new} (a Kite token "
                      f"that carried {old} and carries {new} in the latest snapshot)", file=sys.stderr)
                return 2
            waits = blockers(store, conn, old, new, today)
            if apply and waits:
                for wait in waits:
                    print(f"rekey: refused: {wait}", file=sys.stderr)
                return 2
            undo = settings.backups_dir() / f"rekey-{old}-{new}-{datetime.now(IST):%Y%m%dT%H%M%S}" if apply else None
            counts, left = store.rekey_symbol(old, new, apply=apply, undo_dir=undo)
            if undo is not None:
                copy = sqlite3.connect(undo / "state.db")
                try:
                    conn.backup(copy)
                finally:
                    copy.close()
            try:
                counts += rekey_state(conn, old, new, apply=apply)
            except (sqlite3.Error, RuntimeError):
                if apply:
                    print("rekey: market.duckdb is committed but state.db is unchanged: re-run to finish",
                          file=sys.stderr)
                raise
        finally:
            conn.close()
    finally:
        store.close()
    _report(rename, counts, left, apply=apply, undo=undo, waits=waits)
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Move a renamed symbol's stored history to its new symbol.")
    parser.add_argument("old", help="the retired symbol, e.g. ICICIPRULI")
    parser.add_argument("new", help="its current symbol, e.g. ICICILIFE")
    parser.add_argument("--apply", action="store_true", help="commit (engine stopped); default is a dry run")
    return parser


def main(argv: list[str] | None = None, *, today: date | None = None) -> int:
    args = _build_parser().parse_args(argv)
    old, new = args.old.strip().upper(), args.new.strip().upper()
    if not old or not new or old == new:
        print(f"rekey: need two different symbols, got {args.old!r} and {args.new!r}", file=sys.stderr)
        return 2
    settings = load_settings()
    for path in (settings.duckdb_path(), settings.sqlite_path()):
        if not path.is_file():
            print(f"rekey: {path} not found (MT_DATA_DIR={os.environ.get('MT_DATA_DIR')})", file=sys.stderr)
            return 2
    lock_path = settings.resolved_data_dir() / "engine.lock"
    lock = InstanceLock(lock_path)
    if not lock.acquire():
        print(f"rekey: refused: {lock_path} is held (pid {lock.holder_pid()}): the engine is running. "
              "Stop mt-engine first.", file=sys.stderr)
        return 2
    try:
        return _run(settings, old, new, today or Clock().today(), apply=args.apply)
    except (duckdb.Error, sqlite3.Error, RuntimeError) as exc:
        print(f"rekey: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2
    finally:
        lock.release()


if __name__ == "__main__":
    raise SystemExit(main())
