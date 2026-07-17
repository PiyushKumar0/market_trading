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

from decimal import Decimal

import pytest

from engine.broker.instruments import Instrument, InstrumentStore, UnknownInstrument

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
