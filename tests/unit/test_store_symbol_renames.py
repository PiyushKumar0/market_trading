"""MarketStore.get_symbol_renames — NSE equity renames derived from ``instruments_daily`` token
continuity (2026-10-23 ICICIPRULI→ICICILIFE: Kite kept token 4774913 while the NSE index lists and
the Zerodha MIS file still named the retired symbol)."""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pytest

from engine.marketdata.store import MarketStore, SymbolRename
from tests.conftest import FIXED_NOW

D = FIXED_NOW.date()


@pytest.fixture
def store(tmp_path, clock):
    s = MarketStore(tmp_path / "market.duckdb", tmp_path / "parquet", clock).open()
    yield s
    s.close()


def inst(d: date, token: int, symbol: str, *, exchange: str = "NSE", segment: str = "NSE",
         instrument_type: str = "EQ") -> dict:
    return {
        "d": d, "instrument_token": token, "tradingsymbol": symbol, "exchange": exchange,
        "segment": segment, "instrument_type": instrument_type, "tick_size": Decimal("0.05"),
        "lot_size": 1,
    }


def snapshots(store: MarketStore, days: dict[date, list[tuple[int, str]]]) -> None:
    """One NSE-EQ snapshot per day; a fixed bystander token keeps every listed day non-empty."""
    rows = []
    for d, pairs in days.items():
        rows.append(inst(d, 1, "RELIANCE"))
        rows.extend(inst(d, token, symbol) for token, symbol in pairs)
    store.upsert_instruments_daily(rows)


def test_retired_symbol_maps_to_its_tokens_current_symbol(store):
    snapshots(store, {D - timedelta(days=1): [(4774913, "ICICIPRULI")],
                      D: [(4774913, "ICICILIFE")]})

    assert store.get_symbol_renames(D) == [
        SymbolRename(old_symbol="ICICIPRULI", new_symbol="ICICILIFE", instrument_token=4774913,
                     effective_d=D),
    ]


def test_rename_across_a_token_absence_gap(store):
    """HEG→HEGAM (2026-09-22) kept token 342017 across a 17-day suspension: a day-over-day diff
    misses it; the windowed derivation does not. effective_d is the first new-symbol day."""
    snapshots(store, {D - timedelta(days=3): [(342017, "HEG")],
                      D - timedelta(days=2): [],
                      D - timedelta(days=1): [(342017, "HEGAM")],
                      D: [(342017, "HEGAM")]})

    assert store.get_symbol_renames(D) == [
        SymbolRename(old_symbol="HEG", new_symbol="HEGAM", instrument_token=342017,
                     effective_d=D - timedelta(days=1)),
    ]


def test_series_move_gets_a_new_token_and_is_not_a_rename(store):
    snapshots(store, {D - timedelta(days=1): [(3531521, "PARSVNATH-BE")],
                      D: [(196293633, "PARSVNATH-BZ")]})

    assert store.get_symbol_renames(D) == []


def test_reused_symbol_is_never_remapped(store):
    """The retired symbol names a DIFFERENT live token today ⇒ it is a live name, not an alias."""
    snapshots(store, {D - timedelta(days=1): [(77, "OLDCO")],
                      D: [(77, "NEWCO"), (88, "OLDCO")]})

    assert store.get_symbol_renames(D) == []


def test_chained_renames_resolve_to_the_current_symbol(store):
    snapshots(store, {D - timedelta(days=2): [(77, "AAA")],
                      D - timedelta(days=1): [(77, "BBB")],
                      D: [(77, "CCC")]})

    renames = store.get_symbol_renames(D)

    assert {(r.old_symbol, r.new_symbol) for r in renames} == {("AAA", "CCC"), ("BBB", "CCC")}
    assert {r.effective_d for r in renames} == {D}


def test_token_gone_from_the_latest_snapshot_maps_nothing(store):
    """ASHIKA→ASHIKAG then delisted from NSE: no live token, so nothing to map to."""
    snapshots(store, {D - timedelta(days=2): [(77, "ASHIKA")],
                      D - timedelta(days=1): [(77, "ASHIKAG")],
                      D: []})

    assert store.get_symbol_renames(D) == []


def test_symbol_that_named_two_live_tokens_is_ambiguous_and_dropped(store):
    snapshots(store, {D - timedelta(days=2): [(1001, "XCO")],
                      D - timedelta(days=1): [(1001, "YCO1"), (1002, "XCO")],
                      D: [(1001, "YCO1"), (1002, "YCO2")]})

    assert store.get_symbol_renames(D) == []


def test_only_nse_cash_equity_rows_count(store):
    store.upsert_instruments_daily([
        inst(D - timedelta(days=1), 1, "RELIANCE"), inst(D, 1, "RELIANCE"),
        inst(D - timedelta(days=1), 500, "OLDB", exchange="BSE", segment="BSE"),
        inst(D, 500, "NEWB", exchange="BSE", segment="BSE"),
        inst(D - timedelta(days=1), 600, "OLDFUT", segment="NFO-FUT", instrument_type="FUT"),
        inst(D, 600, "NEWFUT", segment="NFO-FUT", instrument_type="FUT"),
    ])

    assert store.get_symbol_renames(D) == []


def test_lookback_window_and_as_of_bound_the_scan(store):
    snapshots(store, {D - timedelta(days=30): [(77, "OLDCO")],
                      D - timedelta(days=29): [(77, "NEWCO")],
                      D: [(77, "NEWCO")],
                      D + timedelta(days=1): [(77, "NEWER")]})

    # The rename's old sighting is outside a 10-day window ⇒ invisible.
    assert store.get_symbol_renames(D, lookback_days=10) == []
    # Snapshots after as_of are ignored: on D the token is still NEWCO.
    assert [(r.old_symbol, r.new_symbol) for r in store.get_symbol_renames(D)] == [("OLDCO", "NEWCO")]
