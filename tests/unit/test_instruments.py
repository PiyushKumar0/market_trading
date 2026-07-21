"""InstrumentStore index-token seam + tradable indexing (§3.2.2, A2/A10/C7).

Kite's full dump carries the NSE ``INDICES`` rows (``NIFTY 50`` token 256265, ``INDIA VIX`` token
264969) with ``tick_size=0``/``lot_size=0`` — they cannot be a tradable :class:`Instrument`
(``tick_size=Field(gt=0)``), so :meth:`InstrumentStore.refresh` must harvest them into a SEPARATE
token map instead of silently dropping them (the A2 backfill_unknown_token bug). These tests pin:
the token-only resolution path (``token_for_symbol`` / ``symbol_for_token`` fall back to it), the
fail-closed guarantee for the tradable seam (``by_symbol`` / ``round_to_tick`` still raise for an
index), the atomic replace-snapshot semantics of a re-``refresh`` / ``seed``, and that both a sync-
and an async-returning ``instruments()`` are awaited defensively.
"""

from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal

import pytest

from engine.broker.instruments import Instrument, InstrumentStore, UnknownInstrument
from engine.marketdata.store import _TABLE_SPEC, MarketStore

# An F&O underlying (NFO future): is_fno is DERIVED True on refresh, and must round-trip verbatim
# through snapshot_rows -> hydrate (which reads the stored ``fno`` column, not the heuristic).
NIFTY_FUT_ROW = {
    "tradingsymbol": "NIFTY26JANFUT", "instrument_token": 12345678, "exchange": "NFO",
    "segment": "NFO-FUT", "tick_size": 0.05, "lot_size": 50, "instrument_type": "FUT",
}
SNAPSHOT_DAY = date(2026, 6, 17)

# One valid EQ row, the two INDICES rows (tick/lot 0), and a genuinely malformed row (no token) that
# must be counted as skipped and resolve to None everywhere — mirroring the live Kite dump shape.
RELIANCE_ROW = {
    "tradingsymbol": "RELIANCE", "instrument_token": 408065, "exchange": "NSE",
    "segment": "NSE", "tick_size": 0.05, "lot_size": 1, "instrument_type": "EQ",
}
NIFTY50_ROW = {
    "tradingsymbol": "NIFTY 50", "instrument_token": 256265, "exchange": "NSE",
    "segment": "INDICES", "tick_size": 0, "lot_size": 0, "instrument_type": "EQ",
}
INDIA_VIX_ROW = {
    "tradingsymbol": "INDIA VIX", "instrument_token": 264969, "exchange": "NSE",
    "segment": "INDICES", "tick_size": 0, "lot_size": 0, "instrument_type": "EQ",
}
MALFORMED_ROW = {  # tradable segment, no instrument_token → int(None) raises → skipped
    "tradingsymbol": "BROKEN", "exchange": "NSE", "segment": "NSE",
    "tick_size": 0.05, "lot_size": 1, "instrument_type": "EQ",
}
# A legitimately tradable currency-derivative future with a sub-₹0.01 tick (₹0.0025): refresh accepts it
# (0.0025 > 0), but a DECIMAL(10,2) store column truncated it to 0.00 so hydrate rejected it — the
# 2026-07-21 8072-skip mechanism the widened DECIMAL(18,6) column fixes.
CDS_FUT_ROW = {
    "tradingsymbol": "USDINR26JANFUT", "instrument_token": 111111, "exchange": "CDS",
    "segment": "CDS-FUT", "tick_size": 0.0025, "lot_size": 1, "instrument_type": "FUT",
}


class FakeKite:
    """Duck-typed KiteClient surface: ``instruments()`` returns a canned dump (sync)."""

    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows
        self.calls = 0

    def instruments(self) -> list[dict]:
        self.calls += 1
        return list(self._rows)


class FakeAsyncKite(FakeKite):
    """Same surface, but ``instruments()`` is a coroutine (the ``hasattr(raw, '__await__')`` path)."""

    async def instruments(self) -> list[dict]:  # type: ignore[override]
        self.calls += 1
        return list(self._rows)


# ------------------------------------------------------------------ refresh: EQ + index + malformed
async def test_refresh_indexes_eq_and_routes_indices_to_token_seam(clock):
    store = InstrumentStore(clock)
    count = await store.refresh(FakeKite([RELIANCE_ROW, NIFTY50_ROW, INDIA_VIX_ROW, MALFORMED_ROW]))

    # Only the one EQ row is a tradable Instrument; the two indices are NOT counted as skipped, and
    # the malformed row IS (so it never lands in either map).
    assert count == 1

    # Index rows resolve through the token-only seam, both directions.
    assert store.token_for_symbol("NIFTY 50") == 256265
    assert store.token_for_symbol("INDIA VIX") == 264969
    assert store.symbol_for_token(256265) == "NIFTY 50"
    assert store.symbol_for_token(264969) == "INDIA VIX"

    # But an index is never priced/sized/routed — the tradable seam stays fail-closed (A2).
    with pytest.raises(UnknownInstrument):
        store.by_symbol("NIFTY 50")
    with pytest.raises(UnknownInstrument):
        store.round_to_tick("NIFTY 50", Decimal("19850.03"))
    assert store.is_fno("NIFTY 50") is False

    # The EQ row still resolves everywhere.
    assert store.token_for_symbol("RELIANCE") == 408065
    assert store.symbol_for_token(408065) == "RELIANCE"
    assert store.by_symbol("RELIANCE").tick_size == Decimal("0.05")
    assert store.round_to_tick("RELIANCE", Decimal("100.03")) == Decimal("100.05")

    # The malformed row resolves to None / raises (skipped, in neither map).
    assert store.token_for_symbol("BROKEN") is None
    with pytest.raises(UnknownInstrument):
        store.by_symbol("BROKEN")


# ------------------------------------------------------------------ atomic replace-snapshot on re-refresh
async def test_second_refresh_clears_stale_index_entries(clock):
    store = InstrumentStore(clock)
    await store.refresh(FakeKite([RELIANCE_ROW, NIFTY50_ROW, INDIA_VIX_ROW]))
    assert store.token_for_symbol("NIFTY 50") == 256265

    # A different dump: NIFTY 50 / INDIA VIX are gone, a new index appears.
    nifty_bank = {
        "tradingsymbol": "NIFTY BANK", "instrument_token": 260105, "exchange": "NSE",
        "segment": "INDICES", "tick_size": 0, "lot_size": 0, "instrument_type": "EQ",
    }
    await store.refresh(FakeKite([RELIANCE_ROW, nifty_bank]))

    assert store.token_for_symbol("NIFTY 50") is None        # stale forward entry gone
    assert store.token_for_symbol("INDIA VIX") is None
    assert store.symbol_for_token(256265) is None            # stale reverse entry gone
    assert store.symbol_for_token(264969) is None
    assert store.token_for_symbol("NIFTY BANK") == 260105    # new index resolves
    assert store.symbol_for_token(260105) == "NIFTY BANK"


# ------------------------------------------------------------------ seed(): replace-snapshot for indices
async def test_seed_index_tokens_resolve_and_are_cleared_when_omitted(clock):
    store = InstrumentStore(clock)
    reliance = Instrument(
        tradingsymbol="RELIANCE", instrument_token=408065, exchange="NSE", segment="NSE",
        tick_size=Decimal("0.05"), lot_size=1, instrument_type="EQ",
    )

    # seed(..., index_tokens=...) → the index seam resolves both directions.
    store.seed([reliance], index_tokens={"NIFTY 50": 256265})
    assert store.token_for_symbol("NIFTY 50") == 256265
    assert store.symbol_for_token(256265) == "NIFTY 50"
    assert store.token_for_symbol("RELIANCE") == 408065

    # seed() WITHOUT index_tokens clears the prior index maps (replace-snapshot semantics).
    store.seed([reliance])
    assert store.token_for_symbol("NIFTY 50") is None
    assert store.symbol_for_token(256265) is None
    assert store.token_for_symbol("RELIANCE") == 408065      # tradable seam still intact


# ------------------------------------------------------------------ sync vs async instruments() guard
@pytest.mark.parametrize("kite_cls", [FakeKite, FakeAsyncKite])
async def test_refresh_awaits_sync_and_async_instruments(clock, kite_cls):
    store = InstrumentStore(clock)
    kite = kite_cls([RELIANCE_ROW, NIFTY50_ROW])
    count = await store.refresh(kite)
    assert kite.calls == 1
    assert count == 1
    assert store.token_for_symbol("NIFTY 50") == 256265
    assert store.by_symbol("RELIANCE").lot_size == 1


# ------------------------------------------------------------------ malformed index row is skipped
async def test_malformed_index_row_is_skipped_not_indexed(clock):
    # An INDICES row missing its token, and one missing its symbol: both must be skipped (not routed
    # into the token seam), while a well-formed sibling index still resolves.
    no_token = {"tradingsymbol": "NIFTY IT", "exchange": "NSE", "segment": "INDICES",
                "tick_size": 0, "lot_size": 0, "instrument_type": "EQ"}
    no_symbol = {"instrument_token": 999999, "exchange": "NSE", "segment": "INDICES",
                 "tick_size": 0, "lot_size": 0, "instrument_type": "EQ"}
    store = InstrumentStore(clock)
    count = await store.refresh(FakeKite([no_token, no_symbol, NIFTY50_ROW]))

    assert count == 0                                        # no tradable rows
    assert store.token_for_symbol("NIFTY IT") is None        # missing-token index dropped
    assert store.symbol_for_token(999999) is None            # missing-symbol index dropped
    assert store.token_for_symbol("NIFTY 50") == 256265      # well-formed index still resolves


# ================================================================== F1/F2 persistence + hydration
# ------------------------------------------------------------------ snapshot_rows column-exactness
async def test_snapshot_rows_columns_match_table_spec(clock):
    """The emitted dicts must carry EXACTLY the store's pinned ``instruments_daily`` columns — a DDL
    column add/rename then fails here instead of silently dropping data at upsert time (F1)."""
    spec_cols, _pk = _TABLE_SPEC["instruments_daily"]
    # The store spec is the single source of truth; the store-side pinned tuple must track it.
    assert InstrumentStore._SNAPSHOT_COLUMNS == spec_cols

    store = InstrumentStore(clock)
    await store.refresh(FakeKite([RELIANCE_ROW, NIFTY_FUT_ROW, NIFTY50_ROW, INDIA_VIX_ROW]))
    rows = store.snapshot_rows(SNAPSHOT_DAY)

    assert len(rows) == 4                                     # 2 tradable + 2 index tokens
    for row in rows:
        assert set(row) == set(spec_cols)                    # no unknown/missing keys (upsert would raise)
        assert row["d"] == SNAPSHOT_DAY
    by_sym = {r["tradingsymbol"]: r for r in rows}
    # Tradable rows carry the dump's own fields; the A8 surveillance/MIS join is left NULL (its own job).
    assert by_sym["RELIANCE"]["tick_size"] == Decimal("0.05")
    assert by_sym["RELIANCE"]["fno"] is False
    assert by_sym["RELIANCE"]["surveillance"] is None and by_sym["RELIANCE"]["mis_leverage"] is None
    assert by_sym["NIFTY26JANFUT"]["fno"] is True            # F&O membership preserved for the store
    # Index rows are representable within the spec: segment discriminator + null tick/lot.
    assert by_sym["NIFTY 50"]["segment"] == "INDICES"
    assert by_sym["NIFTY 50"]["instrument_type"] == "INDEX"
    assert by_sym["NIFTY 50"]["tick_size"] is None and by_sym["NIFTY 50"]["lot_size"] is None
    assert by_sym["NIFTY 50"]["instrument_token"] == 256265


# ------------------------------------------------------------------ snapshot -> hydrate round-trip
async def test_snapshot_hydrate_round_trip_is_identical(clock):
    """refresh -> snapshot_rows -> hydrate rebuilds an identical token map (tradable + index, both
    directions), with is_fno read verbatim from the stored column (F2)."""
    src = InstrumentStore(clock)
    await src.refresh(FakeKite([RELIANCE_ROW, NIFTY_FUT_ROW, NIFTY50_ROW, INDIA_VIX_ROW]))
    rows = src.snapshot_rows(SNAPSHOT_DAY)

    dst = InstrumentStore(clock)
    assert dst.is_empty is True
    loaded = dst.hydrate(rows)

    assert loaded == 2                                        # two tradable instruments
    assert dst.is_empty is False
    assert dst.hydrated is True                               # provenance flag flipped
    assert dst.index_count == 2

    # Tradable seam identical, both directions + the load-bearing metadata.
    for sym, tok in [("RELIANCE", 408065), ("NIFTY26JANFUT", 12345678)]:
        assert dst.token_for_symbol(sym) == src.token_for_symbol(sym) == tok
        assert dst.symbol_for_token(tok) == sym
        assert dst.by_symbol(sym).tick_size == src.by_symbol(sym).tick_size
        assert dst.is_fno(sym) == src.is_fno(sym)
    assert dst.is_fno("NIFTY26JANFUT") is True               # stored fno honoured (not re-derived)

    # Index seam identical, both directions; still fail-closed on the tradable seam (A2).
    assert dst.token_for_symbol("NIFTY 50") == 256265
    assert dst.symbol_for_token(264969) == "INDIA VIX"
    with pytest.raises(UnknownInstrument):
        dst.by_symbol("NIFTY 50")


# ------------------------------------------------------------------ hydrate skips malformed rows
def _stored_row(**over) -> dict:
    """A well-formed persisted ``instruments_daily`` row (RELIANCE), overridable per-field."""
    row = {
        "d": SNAPSHOT_DAY, "instrument_token": 408065, "tradingsymbol": "RELIANCE", "name": None,
        "exchange": "NSE", "segment": "NSE", "instrument_type": "EQ", "tick_size": Decimal("0.05"),
        "lot_size": 1, "mis_leverage": None, "mis_eligible": None, "surveillance": None,
        "fno": False, "extra": None,
    }
    row.update(over)
    return row


async def test_hydrate_skips_and_counts_malformed_rows(clock):
    """A malformed stored row (missing token, NULL/zero tick, missing index symbol) is skipped and
    counted, never aborting the hydrate — the one good tradable + one good index still resolve (F2)."""
    good_index = {"d": SNAPSHOT_DAY, "instrument_token": 256265, "tradingsymbol": "NIFTY 50",
                  "segment": "INDICES", "instrument_type": "INDEX", "tick_size": None, "lot_size": None,
                  "fno": False}
    rows = [
        _stored_row(),                                       # OK tradable
        _stored_row(tradingsymbol="NOTOK", instrument_token=None),   # int(None) -> TypeError
        _stored_row(tradingsymbol="ZEROTICK", instrument_token=1, tick_size=Decimal("0")),  # gt=0 fails
        _stored_row(tradingsymbol="NULLTICK", instrument_token=2, tick_size=None),  # Decimal('None') fails
        {"d": SNAPSHOT_DAY, "instrument_token": 9, "segment": "INDICES", "tick_size": None},  # index, no symbol
        good_index,
    ]
    store = InstrumentStore(clock)
    loaded = store.hydrate(rows)

    assert loaded == 1                                        # only the one good tradable row
    assert store.token_for_symbol("RELIANCE") == 408065
    assert store.token_for_symbol("NIFTY 50") == 256265      # good index resolves
    assert store.index_count == 1                            # the symbol-less index row was skipped
    for bad in ("NOTOK", "ZEROTICK", "NULLTICK"):
        assert store.token_for_symbol(bad) is None


# ================================================================== F1/F2 round-trip through the REAL store
@pytest.fixture
def market_store(tmp_path, clock):
    """A hermetic tmp DuckDB store (never data/market.duckdb) — the real upsert/get_latest path the F2
    cold-start hydrate uses, so the DECIMAL round-trip is exercised, not just the in-memory dict path."""
    s = MarketStore(tmp_path / "market.duckdb", tmp_path / "parquet", clock)
    s.open()
    yield s
    s.close()


async def test_store_round_trip_is_lossless_including_subpaisa(market_store, clock, caplog):
    """2026-07-21 lossless-hydrate: the FULL round-trip refresh → snapshot_rows → upsert → get_latest →
    hydrate through a real DuckDB store must reconstruct EXACTLY what refresh loaded — indices (tick 0),
    a normal equity, AND a sub-₹0.01 tradable — with zero spurious skips and index_count preserved.
    Fails on the old DECIMAL(10,2) column (the sub-paisa tick truncates to 0.00 → hydrate rejects it)."""
    src = InstrumentStore(clock)
    await src.refresh(FakeKite([RELIANCE_ROW, NIFTY50_ROW, INDIA_VIX_ROW, CDS_FUT_ROW]))
    market_store.upsert_instruments_daily(src.snapshot_rows(clock.today()))
    latest = market_store.get_latest_instruments_daily()
    assert latest is not None
    _d, stored = latest

    dst = InstrumentStore(clock)
    with caplog.at_level(logging.WARNING, logger="engine.broker.instruments"):
        loaded = dst.hydrate(stored)

    # Zero spurious skips: both tradables refresh built (RELIANCE + the sub-paisa future) survived.
    assert loaded == 2
    assert dst.index_count == src.index_count == 2
    assert not [r for r in caplog.records if r.getMessage() == "instrument.hydrate_row_skipped"]

    # NIFTY 50 / India VIX resolve pre-login (the regime tokens), both directions.
    assert dst.token_for_symbol("NIFTY 50") == 256265
    assert dst.token_for_symbol("INDIA VIX") == 264969
    assert dst.symbol_for_token(256265) == "NIFTY 50"

    # Normal equity intact; the tradable seam still fail-closed for indices (A2).
    assert dst.token_for_symbol("RELIANCE") == 408065
    assert dst.by_symbol("RELIANCE").tick_size == Decimal("0.05")
    with pytest.raises(UnknownInstrument):
        dst.by_symbol("NIFTY 50")

    # The sub-₹0.01 tradable survived with its EXACT tick (DECIMAL(18,6) column, not truncated to 0.00).
    assert dst.token_for_symbol("USDINR26JANFUT") == 111111
    assert dst.by_symbol("USDINR26JANFUT").tick_size == Decimal("0.0025")


async def test_store_round_trip_still_rejects_corrupt_equity(market_store, clock, caplog):
    """The A10 tick_size>0 invariant is NOT weakened: a persisted EQUITY row whose tick is genuinely 0
    (corrupt data, not an index) is still skipped + warned on hydrate — even though a full round-trip
    otherwise produces no skips."""
    src = InstrumentStore(clock)
    await src.refresh(FakeKite([RELIANCE_ROW, NIFTY50_ROW]))
    rows = src.snapshot_rows(clock.today())
    # A corrupt tradable EQUITY row (equity segment/type, but tick 0) alongside the clean snapshot.
    rows.append({
        "d": clock.today(), "instrument_token": 999001, "tradingsymbol": "CORRUPTEQ", "name": None,
        "exchange": "NSE", "segment": "NSE", "instrument_type": "EQ", "tick_size": Decimal("0"),
        "lot_size": 1, "mis_leverage": None, "mis_eligible": None, "surveillance": None,
        "fno": False, "extra": None,
    })
    market_store.upsert_instruments_daily(rows)
    latest = market_store.get_latest_instruments_daily()
    assert latest is not None
    _d, stored = latest

    dst = InstrumentStore(clock)
    with caplog.at_level(logging.WARNING, logger="engine.broker.instruments"):
        loaded = dst.hydrate(stored)

    assert loaded == 1                                    # only RELIANCE; the corrupt EQ was rejected
    assert dst.token_for_symbol("CORRUPTEQ") is None      # never entered the tradable map
    assert dst.token_for_symbol("NIFTY 50") == 256265     # the index seam is unaffected
    assert [r for r in caplog.records if r.getMessage() == "instrument.hydrate_row_skipped"]
