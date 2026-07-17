"""Daily NSE instruments dump + per-symbol tick/lot/token store (§3.2.2, A10/A8/C7).

The ``InstrumentStore`` is refreshed once per trading day at ~08:15 from the full Kite instruments
dump (``instruments_daily`` snapshot, §4.3). It is the single authority for per-symbol metadata that
the rest of the platform reads — never re-derived inline:

- ``tick_size`` (A10): the exchange-published price-band tick for each instrument. NSE tick sizes are
  price-banded, so this is a PER-INSTRUMENT value taken from the dump, NOT a flat ₹0.05 assumption.
  :meth:`round_to_tick` quantises a price to that instrument's tick grid and is the ONLY sanctioned
  way to snap a price (used by the gate and OMS — never inline math, §3.2.2/§6/§C of the plan).
- ``lot_size`` / ``instrument_token`` / ``segment``: order sizing, ticker subscription, routing.
- F&O membership (C7): :meth:`is_fno` backs the dynamic-circuit-band rule (MIS candidates must be on
  the F&O list because F&O names get the wider ±10–20% dynamic band rather than a fixed ±x% band).
- Index rows (NSE ``INDICES`` segment — e.g. ``NIFTY 50`` token 256265, ``INDIA VIX`` token 264969) are
  non-tradable and carry ``tick_size=0``/``lot_size=0``, so they cannot be an :class:`Instrument`
  (which requires a positive tick, A10). They get a SEPARATE token-only seam: :meth:`refresh` harvests
  ``tradingsymbol -> instrument_token`` into ``_index_tokens`` (+ reverse), and
  :meth:`token_for_symbol` / :meth:`symbol_for_token` fall back to it so daily-bar backfill can resolve
  a regime symbol's token (A2). :meth:`by_symbol` / :meth:`round_to_tick` / :meth:`is_fno` still
  raise/deny for indices — you never price, size, or route an index, so fail-closed there is correct.

Phase 0 ships the in-memory store, the indexing in :meth:`refresh`, and the load-bearing
:meth:`round_to_tick`. DuckDB persistence of the daily snapshot (A8 surveillance/leverage join, §4.3)
is a Phase-1 TODO. This module talks only to ``core`` + the injected ``kite_client``; it never imports
``engine.intelligence`` (R1).
"""

from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from engine.core.clock import Clock
from engine.core.log import get_logger

_log = get_logger("engine.broker.instruments")

# Kite reports F&O-bearing underlyings via the NFO exchange / FUT|OPT instrument types; an NSE-equity
# row whose tradingsymbol also appears in those segments is treated as F&O-listed (C7). Phase 0 keeps a
# conservative explicit flag on each ``Instrument`` (populated by :meth:`refresh`); the full NFO-join is
# a Phase-1 concern alongside the DuckDB snapshot.
_FNO_EXCHANGES = frozenset({"NFO", "BFO", "CDS", "MCX"})
_FNO_INSTRUMENT_TYPES = frozenset({"FUT", "CE", "PE"})


class UnknownInstrument(KeyError):
    """Raised by :meth:`InstrumentStore.by_symbol` when a tradingsymbol is not in today's dump.

    Subclasses ``KeyError`` (per the §3.2.2 contract) so existing ``except KeyError`` callers keep
    working, while giving the gate/OMS a precise type to catch. An unknown instrument is a hard stop:
    the platform must never size, price, or route an order for a symbol it has no tick/lot for.
    """


class Instrument(BaseModel):
    """One row of the daily instruments dump (the fields the platform actually consumes).

    ``tick_size`` is a ``Decimal`` (price), per the money/price convention; it is the price-banded
    exchange tick for THIS instrument (A10), not a global constant.
    """

    model_config = ConfigDict(frozen=True)

    tradingsymbol: str
    instrument_token: int
    exchange: str
    segment: str
    tick_size: Decimal = Field(gt=0)            # A10 — per-instrument, price-banded; must be positive
    lot_size: int = Field(gt=0)
    instrument_type: str                        # "EQ" | "FUT" | "CE" | "PE" | ...
    is_fno: bool = False                        # C7 — F&O-listed underlying (dynamic band membership)


class InstrumentStore:
    """In-memory index of today's instruments, keyed by ``tradingsymbol`` (§3.2.2, A10/A8/C7).

    Construct once per process with the shared :class:`~engine.core.clock.Clock`; call
    :meth:`refresh` during the 08:15 daily job (or startup catch-up) to (re)load the dump. Tests use
    :meth:`seed` to load a fixed list without a live Kite client.
    """

    def __init__(self, clock: Clock) -> None:
        self._clock = clock
        self._by_symbol: dict[str, Instrument] = {}
        self._by_token: dict[int, str] = {}   # reverse index: instrument_token -> tradingsymbol (ticker)
        # A2 — non-tradable INDICES seam: tradingsymbol -> token (+ reverse), kept OUT of the tradable
        # maps so by_symbol/round_to_tick/is_fno still fail-closed for indices.
        self._index_tokens: dict[str, int] = {}
        self._index_by_token: dict[int, str] = {}
        self._refreshed_at = None  # tz-aware IST datetime of the last successful refresh (None until)

    async def refresh(self, kite_client: Any) -> int:
        """Pull the full instruments dump via ``kite_client`` and index it. Returns the row count.

        Phase-0 skeleton: fetch the dump, build :class:`Instrument` rows, and index by
        ``tradingsymbol``. ``kite_client.instruments()`` is the ``KiteClient``/pykiteconnect surface
        (sync or async — both are awaited defensively). The build replaces the prior snapshot
        atomically (a failed refresh leaves the previous day's store intact rather than half-loaded) —
        the tradable AND index maps are swapped in together at the end for that reason.

        Index rows (``segment`` contains ``INDICES`` — e.g. ``NIFTY 50``/``INDIA VIX``) are harvested
        into a SEPARATE token map rather than built as tradable :class:`Instrument`s (their
        ``tick_size=0`` would fail the ``gt=0`` model). They are NOT counted as malformed/skipped; only
        an index row missing its symbol or token is skipped like any other bad row (A2).

        TODO(Phase 1): persist the snapshot to the DuckDB ``instruments_daily`` table (one row-set per
        trading day, §4.3) and join the Zerodha MIS-leverage + NSE surveillance files (A8) so
        ``is_fno`` (C7) and per-stock leverage come from the real NFO/margins join rather than the
        per-row heuristic below.
        """
        raw = kite_client.instruments()
        if hasattr(raw, "__await__"):
            raw = await raw

        indexed: dict[str, Instrument] = {}
        index_tokens: dict[str, int] = {}
        index_by_token: dict[int, str] = {}
        skipped = 0
        for row in raw:
            # Detect index rows BEFORE building an Instrument: they carry tick_size=0 and would be
            # swallowed by the ValidationError branch below, so route them to the token-only seam (A2).
            segment = str(self._row_get(row, "segment", "") or "").upper()
            if "INDICES" in segment:
                try:
                    symbol = str(self._row_get(row, "tradingsymbol") or "")
                    token = int(self._row_get(row, "instrument_token"))
                except (KeyError, ValueError, TypeError) as exc:
                    skipped += 1
                    _log.warning("instrument.row_skipped", error=str(exc))
                    continue
                if not symbol:
                    skipped += 1
                    _log.warning("instrument.row_skipped", error="index row missing tradingsymbol")
                    continue
                index_tokens[symbol] = token
                index_by_token[token] = symbol
                continue
            try:
                instrument = self._row_to_instrument(row)
            except (KeyError, ValueError, TypeError) as exc:
                skipped += 1
                _log.warning("instrument.row_skipped", error=str(exc))
                continue
            indexed[instrument.tradingsymbol] = instrument

        # Atomic swap: tradable + index maps replace the prior snapshot together, so a raise anywhere
        # above leaves the previous day's store fully intact (never half of one dump + half of another).
        self._by_symbol = indexed
        self._by_token = {ins.instrument_token: sym for sym, ins in indexed.items()}
        self._index_tokens = index_tokens
        self._index_by_token = index_by_token
        self._refreshed_at = self._clock.now()
        _log.info(
            "instruments.refreshed",
            count=len(indexed),
            skipped=skipped,
            indices=len(index_tokens),
            at=self._refreshed_at.isoformat(),
        )
        return len(indexed)

    def seed(
        self, instruments: list[Instrument], *, index_tokens: dict[str, int] | None = None
    ) -> int:
        """Load instruments from a list (unit-test / replay helper). Returns the tradable count loaded.

        Replaces the current snapshot — including the index seam: ``index_tokens`` (``tradingsymbol ->
        instrument_token``, A2) replaces ``_index_tokens`` (+ reverse), and passing it as ``None``/
        omitting it clears any prior index entries (replace-snapshot semantics, mirroring
        :meth:`refresh`). Does not touch the clock-stamped ``_refreshed_at`` semantics of a real
        :meth:`refresh` beyond recording that a load happened, so tests stay deterministic.
        """
        self._by_symbol = {ins.tradingsymbol: ins for ins in instruments}
        self._by_token = {ins.instrument_token: ins.tradingsymbol for ins in instruments}
        idx = dict(index_tokens or {})
        self._index_tokens = idx
        self._index_by_token = {tok: sym for sym, tok in idx.items()}
        self._refreshed_at = self._clock.now()
        return len(self._by_symbol)

    def by_symbol(self, tradingsymbol: str) -> Instrument:
        """Return the :class:`Instrument` for ``tradingsymbol``.

        Raises :class:`UnknownInstrument` (a ``KeyError`` subclass) if the symbol is not in today's
        dump — callers must treat this as a hard stop, never as "assume defaults".
        """
        try:
            return self._by_symbol[tradingsymbol]
        except KeyError as exc:
            raise UnknownInstrument(tradingsymbol) from exc

    def round_to_tick(self, symbol: str, price: Decimal) -> Decimal:
        """Quantise ``price`` to the nearest multiple of ``symbol``'s tick size (A10).

        Load-bearing: the gate and OMS price every order through here so a banded tick (e.g. ₹0.01,
        ₹0.05, ₹0.10) is honoured instead of a hard-coded ₹0.05. The price is snapped to the tick
        GRID with ``ROUND_HALF_UP`` (ties round up to the next tick), and the result is returned at the
        tick's own scale (e.g. tick ₹0.05 ⇒ two decimals) so it is broker-acceptable as-is.

        Raises :class:`UnknownInstrument` if the symbol is unknown (no tick to round to).
        """
        tick = self.by_symbol(symbol).tick_size
        price = Decimal(price)
        # Snap to the integer number of ticks (half-up), then scale back onto the price grid and
        # re-quantise to the tick's scale so trailing precision matches the tick exactly.
        steps = (price / tick).quantize(Decimal(1), rounding=ROUND_HALF_UP)
        snapped = steps * tick
        return snapped.quantize(tick, rounding=ROUND_HALF_UP)

    def token_for_symbol(self, symbol: str) -> int | None:
        """``tradingsymbol -> instrument_token`` (the ticker-subscription / backfill resolver seam).

        Resolves tradable instruments first, then falls back to the non-tradable index token map (A2)
        so a regime symbol like ``NIFTY 50``/``INDIA VIX`` resolves for daily-bar backfill even though
        it has no tradable tick. Returns ``None`` for an unknown symbol (never guesses a token) so
        callers such as :class:`~engine.marketdata.backfill.BackfillJob` can report that symbol failed
        rather than request candles for the wrong instrument (§3.2.3).
        """
        instrument = self._by_symbol.get(symbol)
        if instrument is not None:
            return instrument.instrument_token
        return self._index_tokens.get(symbol)

    def symbol_for_token(self, token: int) -> str | None:
        """``instrument_token -> tradingsymbol`` reverse index (the ticker frame → symbol mapping).

        Tradable reverse index first, then the index reverse map (A2). Returns ``None`` for a token not
        in today's dump so the ``TickerSupervisor`` drops an unrecognised frame rather than mislabelling
        it (A3/§3.2.2).
        """
        symbol = self._by_token.get(token)
        if symbol is not None:
            return symbol
        return self._index_by_token.get(token)

    def is_fno(self, symbol: str) -> bool:
        """True if ``symbol`` is F&O-listed (C7 — dynamic-band membership).

        Returns ``False`` for an unknown symbol (conservative: a name we have no record of is treated
        as non-F&O, so the dynamic-band MIS-eligibility check fails closed rather than open).
        """
        instrument = self._by_symbol.get(symbol)
        return bool(instrument and instrument.is_fno)

    # -- internals -----------------------------------------------------------------------------

    @staticmethod
    def _row_get(row: Any, key: str, default: Any = None) -> Any:
        """One tolerant field read from a Kite dump row (dict-shaped or attribute-shaped).

        Mirrors the accessor built inline in :meth:`_row_to_instrument`; used by :meth:`refresh` to
        sniff a row's ``segment`` (index detection) before committing to the tradable model path.
        """
        if isinstance(row, dict):
            return row.get(key, default)
        return getattr(row, key, default)

    @staticmethod
    def _row_to_instrument(row: Any) -> Instrument:
        """Map one Kite dump row (dict or object) to an :class:`Instrument`.

        Tolerant of dict-shaped (pykiteconnect ``instruments()``) and attribute-shaped rows.
        """
        get = row.get if isinstance(row, dict) else (lambda k, d=None: getattr(row, k, d))

        exchange = str(get("exchange", "") or "")
        instrument_type = str(get("instrument_type", "") or "")
        is_fno = exchange in _FNO_EXCHANGES or instrument_type in _FNO_INSTRUMENT_TYPES

        return Instrument(
            tradingsymbol=str(get("tradingsymbol")),
            instrument_token=int(get("instrument_token")),
            exchange=exchange,
            segment=str(get("segment", "") or ""),
            tick_size=Decimal(str(get("tick_size"))),
            lot_size=int(get("lot_size") or 1),
            instrument_type=instrument_type,
            is_fno=is_fno,
        )
