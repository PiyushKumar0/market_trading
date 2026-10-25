"""Moving a renamed symbol's history: ``MarketStore.rekey_symbol`` and ``scripts/rekey_symbol.py``
(2026-10-23 NSE rename ICICIPRULI→ICICILIFE; Kite kept the token, every symbol-keyed row did not move)."""

from __future__ import annotations

import importlib.util
import sqlite3
import sys
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

from engine.core.clock import IST
from engine.core.config import load_settings
from engine.core.db import connect
from engine.core.migrations import apply_migrations
from engine.marketdata.store import _REKEY_COLUMNS, _REKEY_LEFT, MarketStore, RekeyCount
from engine.ops.single_instance import InstanceLock
from tests.conftest import FIXED_NOW

_SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "rekey_symbol.py"
_spec = importlib.util.spec_from_file_location("mt_rekey_symbol", _SCRIPT_PATH)
script = importlib.util.module_from_spec(_spec)
sys.modules["mt_rekey_symbol"] = script
_spec.loader.exec_module(script)

OLD, NEW = "OLDCO", "NEWCO"
D = FIXED_NOW.date()
T = FIXED_NOW

#: One row naming OLD in every column the re-key moves...
_MOVED_ROWS: dict[str, dict] = {
    "bars_1d": {"symbol": OLD, "d": D, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1},
    "bars_1m": {"symbol": OLD, "ts_minute": T, "open": 1, "high": 1, "low": 1, "close": 1,
                "volume": 1, "src": "self"},
    "bars_1m_no_trade": {"symbol": OLD, "ts_minute": T, "confirmed_at": T, "src": "kite_empty"},
    "features_daily": {"d": D, "symbol": OLD, "feature_set_version": 1, "features": "{}"},
    "feature_snapshots": {"snapshot_id": "s1", "symbol": OLD, "ts": T, "feature_set_version": 1,
                          "features": "{}"},
    "corp_actions": {"symbol": OLD, "ex_date": D, "kind": "split"},
    "earnings_calendar": {"symbol": OLD, "event_date": D},
    "flagged_instrument_days": {"symbol": OLD, "d": D, "reason": "bulk_deal"},
    "sector_map": {"as_of": D, "symbol": OLD, "sector": "FINANCIAL SERVICES"},
    "catalyst_watchlist": {"entry_id": "e1", "d": D, "symbol": OLD, "grade": "context"},
    "symbol_isin": {"symbol": OLD, "isin": "INE726G01019", "as_of": D, "ingested_at": T},
    "insider_trades": {"id": "i1", "symbol": OLD, "ingested_at": T},
    "shp_quarterly": {"symbol": OLD, "qtr_end": D, "category": "promoter", "ingested_at": T},
    "results_filings": {"symbol": OLD, "period_end": D, "consolidated": True, "ingested_at": T},
    "entity_aliases": {"alias": "old co", "tradingsymbol": OLD},
    "news_clusters": {"cluster_id": "c1", "representative": "Old Co results", "source_domains": ["a.in"],
                      "first_seen": T, "last_seen": T, "symbols": [OLD]},
    "sentiment_agg": {"scope": "symbol", "scope_key": OLD, "as_of": T, "value": 0.5},
}
#: ...and in every column it leaves.
_LEFT_ROWS: dict[str, dict] = {
    "instruments_daily": {"d": D, "instrument_token": 77, "tradingsymbol": OLD},
    "universe_daily": {"d": D, "symbol": OLD, "included": True},
    "reconcile_log": {"d": D, "symbol": OLD, "ran_at": T},
    "corrections_log": {"symbol": OLD, "minute": T, "tick_ts": T, "logged_at": T},
    "unresolved_entities": {"entity_text": "old co", "reason": "out_of_universe",
                            "candidate_symbols": [OLD], "logged_at": T},
    "theme_map": {"theme": "insurance", "keywords": ["insurer"], "symbols": [OLD]},
}


@pytest.fixture
def store(tmp_path, clock):
    s = MarketStore(tmp_path / "market.duckdb", tmp_path / "parquet", clock).open()
    yield s
    s.close()


def insert(store: MarketStore, table: str, *rows: dict) -> None:
    for row in rows:
        cols = ", ".join(f'"{c}"' for c in row)
        store._execute(f"INSERT INTO {table} ({cols}) VALUES ({', '.join('?' for _ in row)})", list(row.values()))


def naming(store: MarketStore, table: str, column: str, symbol: str) -> int:
    array = store._fetchall(
        "SELECT data_type FROM information_schema.columns WHERE table_name = ? AND column_name = ?",
        [table, column],
    )[0][0].endswith("[]")
    match = f'list_contains("{column}", ?)' if array else f'"{column}" = ?'
    return store._fetchall(f"SELECT count(*) FROM {table} WHERE {match}", [symbol])[0][0]


def by_table(counts: list[RekeyCount]) -> dict[str, RekeyCount]:
    return {c.table: c for c in counts}


# --------------------------------------------------------------------------- MarketStore.rekey_symbol
def test_every_symbol_column_is_classified(store):
    """A new table naming a symbol must be declared moved or left — never silently skipped."""
    found = {
        (t, c) for t, c in store._fetchall(
            "SELECT table_name, column_name FROM information_schema.columns "
            "WHERE table_catalog = current_database() AND column_name ILIKE '%symbol%'"
        )
    }
    moved = {(t, c) for t, c, _ in _REKEY_COLUMNS}
    assert found <= moved | set(_REKEY_LEFT)
    assert not moved & set(_REKEY_LEFT)
    for table, column in moved | set(_REKEY_LEFT):
        assert store._fetchall(
            "SELECT count(*) FROM information_schema.columns WHERE table_name = ? AND column_name = ?",
            [table, column],
        )[0][0] == 1, (table, column)
    assert set(_MOVED_ROWS) == {t for t, _ in moved} and set(_LEFT_ROWS) == {t for t, _ in _REKEY_LEFT}


def test_rekey_moves_the_company_data_and_leaves_the_records(store):
    for table, row in {**_MOVED_ROWS, **_LEFT_ROWS}.items():
        insert(store, table, row)

    counts, left = store.rekey_symbol(OLD, NEW, apply=True)

    assert counts == [
        RekeyCount(table=t, column=c, old_rows=1, new_rows=0, moved=1, dropped=0) for t, c, _ in _REKEY_COLUMNS
    ]
    for table, column, _ in _REKEY_COLUMNS:
        assert (naming(store, table, column, OLD), naming(store, table, column, NEW)) == (0, 1), table
    assert left == {f"{t}.{c}": 1 for t, c in _REKEY_LEFT}
    for table, column in _REKEY_LEFT:
        assert (naming(store, table, column, OLD), naming(store, table, column, NEW)) == (1, 0), table


def test_a_row_the_new_symbol_already_holds_wins(store):
    insert(store, "bars_1d",
           {"symbol": OLD, "d": D - timedelta(days=2), "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1},
           {"symbol": OLD, "d": D - timedelta(days=1), "open": 2, "high": 2, "low": 2, "close": 2, "volume": 2},
           {"symbol": NEW, "d": D - timedelta(days=1), "open": 9, "high": 9, "low": 9, "close": 9, "volume": 9,
            "src": "bhavcopy"})
    insert(store, "symbol_isin",
           {"symbol": OLD, "isin": "OLDISIN", "as_of": D, "ingested_at": T},
           {"symbol": NEW, "isin": "NEWISIN", "as_of": D, "ingested_at": T})
    insert(store, "entity_aliases",
           {"alias": "old co", "tradingsymbol": OLD}, {"alias": "old co", "tradingsymbol": NEW},
           {"alias": "old company", "tradingsymbol": OLD})

    counts = by_table(store.rekey_symbol(OLD, NEW, apply=True)[0])

    assert counts["bars_1d"].model_dump(include={"moved", "dropped"}) == {"moved": 1, "dropped": 1}
    assert store._fetchall(
        "SELECT d, close, src FROM bars_1d WHERE symbol = ? ORDER BY d", [NEW]
    ) == [(D - timedelta(days=2), Decimal("1.00"), "kite_official"), (D - timedelta(days=1), Decimal("9.00"), "bhavcopy")]
    assert store._fetchall("SELECT symbol, isin FROM symbol_isin") == [(NEW, "NEWISIN")]
    assert sorted(store._fetchall("SELECT alias, tradingsymbol FROM entity_aliases")) == [
        ("old co", NEW), ("old company", NEW),
    ]


def test_array_symbols_are_renamed_in_place_without_duplicating(store):
    base = {"representative": "r", "source_domains": ["a.in"], "first_seen": T, "last_seen": T}
    insert(store, "news_clusters",
           {"cluster_id": "c1", "symbols": [OLD, "OTHER"], **base},
           {"cluster_id": "c2", "symbols": [OLD, NEW], **base},
           {"cluster_id": "c3", "symbols": ["OTHER"], **base},
           {"cluster_id": "c4", "symbols": None, **base})

    counts = by_table(store.rekey_symbol(OLD, NEW, apply=True)[0])

    assert counts["news_clusters"] == RekeyCount(
        table="news_clusters", column="symbols", old_rows=2, new_rows=1, moved=1, dropped=1)
    assert store._fetchall("SELECT cluster_id, symbols FROM news_clusters ORDER BY cluster_id") == [
        ("c1", [NEW, "OTHER"]), ("c2", [NEW]), ("c3", ["OTHER"]), ("c4", None),
    ]


def test_insider_rows_keep_their_id_and_a_same_source_twin_is_dropped(store):
    """The content-hash id covers the symbol and cannot be rebuilt from stored values, so a twin is
    found on the hash's other inputs, within one source — an NSE row never twins a BSE row."""
    bdt = datetime(2026, 6, 10, 18, 5, tzinfo=IST)
    trade = {"broadcast_dt": bdt, "qty": 10, "value": Decimal("100"), "ingested_at": T}
    insert(store, "insider_trades",
           {"id": "bse:o1", "symbol": OLD, "person_name": None, "txn_type": None, **trade},
           {"id": "bse:n1", "symbol": NEW, "person_name": None, "txn_type": None, **trade},
           {"id": "o2", "symbol": OLD, "person_name": "P", "txn_type": "Buy", **trade},
           {"id": "bse:n2", "symbol": NEW, "person_name": "P", "txn_type": "Buy", **trade},
           {"id": "bse:o3", "symbol": OLD, "person_name": "Q", "txn_type": "Buy", **{**trade, "value": Decimal("101")}},
           {"id": "bse:n3", "symbol": NEW, "person_name": "Q", "txn_type": "Buy", **trade})

    counts = by_table(store.rekey_symbol(OLD, NEW, apply=True)[0])

    assert counts["insider_trades"].model_dump(include={"old_rows", "new_rows", "moved", "dropped"}) == {
        "old_rows": 3, "new_rows": 3, "moved": 2, "dropped": 1}
    assert store._fetchall("SELECT id FROM insider_trades WHERE symbol = ? ORDER BY id", [NEW]) == [
        ("bse:n1",), ("bse:n2",), ("bse:n3",), ("bse:o3",), ("o2",),
    ]


def test_sentiment_moves_symbol_scope_rows_only(store):
    insert(store, "sentiment_agg",
           {"scope": "symbol", "scope_key": OLD, "as_of": T, "value": 0.5},
           {"scope": "theme", "scope_key": OLD, "as_of": T, "value": 0.1})

    counts = by_table(store.rekey_symbol(OLD, NEW, apply=True)[0])

    assert counts["sentiment_agg"].old_rows == 1 and counts["sentiment_agg"].moved == 1
    assert sorted(store._fetchall("SELECT scope, scope_key FROM sentiment_agg")) == [
        ("symbol", NEW), ("theme", OLD),
    ]


def test_dry_run_rolls_back_and_reports_what_an_apply_commits(store):
    for table, row in _MOVED_ROWS.items():
        insert(store, table, row)
    insert(store, "bars_1d", {"symbol": NEW, "d": D, "open": 9, "high": 9, "low": 9, "close": 9, "volume": 9})

    dry = store.rekey_symbol(OLD, NEW, apply=False)

    for table, column, _ in _REKEY_COLUMNS:
        assert naming(store, table, column, OLD) == 1, table
    assert store.rekey_symbol(OLD, NEW, apply=True) == dry
    assert by_table(store.rekey_symbol(OLD, NEW, apply=True)[0])["bars_1d"].model_dump(
        include={"old_rows", "moved", "dropped"}) == {"old_rows": 0, "moved": 0, "dropped": 0}


def test_a_failure_mid_move_rolls_every_table_back(store, monkeypatch):
    for table, row in _MOVED_ROWS.items():
        insert(store, table, row)
    real = MarketStore._rekey_column
    failing = _REKEY_COLUMNS[3][0]

    def boom(con, table, *args):
        if table == failing:
            raise RuntimeError("injected")
        return real(con, table, *args)

    monkeypatch.setattr(MarketStore, "_rekey_column", staticmethod(boom))

    with pytest.raises(RuntimeError, match="injected"):
        store.rekey_symbol(OLD, NEW, apply=True)
    for table, column, _ in _REKEY_COLUMNS:
        assert (naming(store, table, column, OLD), naming(store, table, column, NEW)) == (1, 0), table


def test_undo_copy_holds_every_row_the_move_can_change(store, tmp_path):
    insert(store, "bars_1d",
           {"symbol": OLD, "d": D, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1},
           {"symbol": NEW, "d": D - timedelta(days=1), "open": 9, "high": 9, "low": 9, "close": 9, "volume": 9},
           {"symbol": "OTHER", "d": D, "open": 5, "high": 5, "low": 5, "close": 5, "volume": 5})
    undo = tmp_path / "undo"

    store.rekey_symbol(OLD, NEW, apply=True, undo_dir=undo)

    assert sorted(f.name for f in undo.iterdir()) == sorted(f"{t}.parquet" for t, _, _ in _REKEY_COLUMNS)
    assert sorted(duckdb.sql(f"SELECT symbol, d FROM '{undo / 'bars_1d.parquet'}'").fetchall()) == [
        (NEW, D - timedelta(days=1)), (OLD, D),
    ]


def test_same_symbol_is_refused(store):
    with pytest.raises(ValueError):
        store.rekey_symbol(OLD, OLD, apply=False)


# --------------------------------------------------------------------------- scripts/rekey_symbol.py
@pytest.fixture
def env(tmp_path, monkeypatch, clock):
    """A data dir with the rename in the instrument snapshots, history under OLD and a migrated state.db."""
    data = tmp_path / "data"
    monkeypatch.setenv("MT_DATA_DIR", str(data))
    settings = load_settings()
    s = MarketStore(settings.duckdb_path(), settings.parquet_dir(), clock).open()
    eq = {"exchange": "NSE", "segment": "NSE", "instrument_type": "EQ"}
    s.upsert_instruments_daily([
        {"d": D - timedelta(days=2), "instrument_token": 77, "tradingsymbol": OLD, **eq},
        {"d": D - timedelta(days=1), "instrument_token": 77, "tradingsymbol": NEW, **eq},
    ])
    insert(s, "bars_1d", {"symbol": OLD, "d": D - timedelta(days=3), "open": 1, "high": 1, "low": 1,
                          "close": 1, "volume": 1})
    s.close()
    c = connect(settings.sqlite_path())
    apply_migrations(c)
    c.executemany("INSERT INTO backfill_checkpoints (symbol, interval, through_date) VALUES (?, ?, ?)",
                  [(OLD, "day", "2026-06-10"), (OLD, "minute", "2026-06-10"), (NEW, "day", "2026-06-16")])
    c.execute(
        "INSERT INTO ins_pending (for_session, symbol, crossing_session, trailing_value, reference_close, "
        "created_at) VALUES ('2026-06-18', ?, '2026-06-16', '100', '10', '2026-06-16T19:00:00+05:30')", (OLD,))
    c.close()
    return settings


def run(*argv: str) -> int:
    return script.main(list(argv), today=D)


def state_rows(settings, sql: str) -> list[tuple]:
    c = sqlite3.connect(settings.sqlite_path())
    try:
        return c.execute(sql).fetchall()
    finally:
        c.close()


def market_rows(settings, sql: str) -> list[tuple]:
    con = duckdb.connect(str(settings.duckdb_path()), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def test_script_dry_run_changes_nothing(env, capsys):
    assert run(OLD, NEW) == 0

    assert "DRY RUN" in capsys.readouterr().out
    assert market_rows(env, "SELECT symbol FROM bars_1d") == [(OLD,)]
    assert sorted(state_rows(env, "SELECT symbol, interval FROM backfill_checkpoints")) == [
        (NEW, "day"), (OLD, "day"), (OLD, "minute")]
    assert not env.backups_dir().exists()


def test_script_apply_moves_both_stores_and_writes_the_undo(env):
    assert run(OLD, NEW, "--apply") == 0

    assert market_rows(env, "SELECT symbol FROM bars_1d") == [(NEW,)]
    # NEW's own day checkpoint wins; OLD's minute checkpoint moves.
    assert sorted(state_rows(env, "SELECT symbol, interval, through_date FROM backfill_checkpoints")) == [
        (NEW, "day", "2026-06-16"), (NEW, "minute", "2026-06-10")]
    assert state_rows(env, "SELECT symbol FROM ins_pending") == [(NEW,)]
    (undo,) = env.backups_dir().glob(f"rekey-{OLD}-{NEW}-*")
    assert (undo / "bars_1d.parquet").is_file()
    copy = sqlite3.connect(undo / "state.db")
    try:
        assert copy.execute("SELECT count(*) FROM backfill_checkpoints WHERE symbol = ?", (OLD,)).fetchone() == (2,)
    finally:
        copy.close()
    assert run(OLD, NEW, "--apply") == 0          # a second run moves nothing


def test_script_refuses_a_pair_the_snapshots_do_not_show(env, capsys):
    assert run(NEW, OLD, "--apply") == 2
    assert "no rename" in capsys.readouterr().err
    assert market_rows(env, "SELECT symbol FROM bars_1d") == [(OLD,)]


def test_script_refuses_while_the_engine_lock_is_held(env, capsys):
    lock = InstanceLock(env.resolved_data_dir() / "engine.lock")
    assert lock.acquire()
    try:
        assert run(OLD, NEW) == 2
    finally:
        lock.release()
    assert "engine is running" in capsys.readouterr().err


def test_script_apply_refuses_a_live_position_on_the_old_symbol(env, capsys):
    c = sqlite3.connect(env.sqlite_path())
    c.execute("INSERT INTO positions (position_id, symbol, state, origin) VALUES ('p1', ?, 'OPEN', 'recommended')",
              (OLD,))
    c.commit()
    c.close()

    assert run(OLD, NEW, "--apply") == 2
    assert "live position" in capsys.readouterr().err
    assert market_rows(env, "SELECT symbol FROM bars_1d") == [(OLD,)]


def test_script_apply_refuses_a_rename_with_a_trading_gap(env, capsys, clock):
    """HEG→HEGAM (2026-09-22) came with a 10-session suspension around its demerger: moved history would
    carry that hole, and the warm-up would refuse the new symbol for a gapped window instead of treating
    it as a fresh listing."""
    s = MarketStore(env.duckdb_path(), env.parquet_dir(), clock).open()
    bar = {"open": 1, "high": 1, "low": 1, "close": 1, "volume": 1}
    insert(s, "bars_1d", {"symbol": "OTHER", "d": D - timedelta(days=2), **bar},
           {"symbol": NEW, "d": D - timedelta(days=1), **bar})
    assert s.rename_trading_gap(OLD, NEW) == 1
    assert s.rename_trading_gap(OLD, "NOSUCH") == 0
    s.close()

    assert run(OLD, NEW, "--apply") == 2
    assert "1 market session" in capsys.readouterr().err
    assert market_rows(env, "SELECT symbol FROM bars_1d WHERE symbol = 'OLDCO'") == [(OLD,)]


def test_script_apply_refuses_inside_the_fresh_insider_window(env, capsys, clock):
    s = MarketStore(env.duckdb_path(), env.parquet_dir(), clock).open()
    insert(s, "insider_trades", {"id": "bse:x", "symbol": OLD, "broadcast_dt": T - timedelta(days=1),
                                 "ingested_at": T})
    s.close()

    assert run(OLD, NEW, "--apply") == 2
    err = capsys.readouterr().err
    assert "fresh" in err and (D + timedelta(days=3)).isoformat() in err
    assert market_rows(env, "SELECT symbol FROM bars_1d") == [(OLD,)]
