"""Daily NSE instruments dump + per-symbol tick/lot/token store.

The ``InstrumentStore`` is refreshed once per trading day at ~08:15 from the full Kite instruments dump
(``instruments_daily`` snapshot). It is the single authority for per-symbol metadata — never re-derived
inline:

- ``tick_size``: PER-INSTRUMENT and price-banded, taken from the dump, NOT a flat ₹0.05.
  :meth:`round_to_tick` is the ONLY sanctioned way to snap a price (gate and OMS).
- ``lot_size`` / ``instrument_token`` / ``segment``: order sizing, ticker subscription, routing.
- F&O membership: :meth:`is_fno` backs the dynamic-circuit-band rule (MIS candidates must be F&O names,
  which get the wider ±10–20% dynamic band).
- Index rows (NSE ``INDICES``, e.g. ``NIFTY 50``, ``INDIA VIX``) carry ``tick_size=0``/``lot_size=0`` and
  cannot be an :class:`Instrument` (positive tick required). They live in a SEPARATE token-only seam
  (``_index_tokens`` + reverse) that :meth:`token_for_symbol` / :meth:`symbol_for_token` fall back to, so
  daily-bar backfill can resolve a regime symbol. :meth:`by_symbol` / :meth:`round_to_tick` /
  :meth:`is_fno` still raise/deny for indices: you never price, size, or route an index.

The store is in-memory, rebuilt each boot. A restart AFTER the 08:15 ``instruments`` job (whose ``job_runs``
watermark makes catch-up skip it) would leave every token lookup empty, so :meth:`snapshot_rows` renders the
dump as ``instruments_daily`` rows for the job to persist and :meth:`hydrate` rebuilds the index from the
latest stored snapshot at startup, pre-login (recovery never waits on a Kite session). This module talks
only to ``core`` + the injected ``kite_client``.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from engine.core.clock import Clock
from engine.core.log import get_logger

_log = get_logger("engine.broker.instruments")

# Derivative rows (these exchanges, FUT|CE|PE types) carry their underlying's tradingsymbol in ``name``;
# an NSE equity whose tradingsymbol is such a ``name`` is F&O-listed (the join :meth:`InstrumentStore.refresh`
# stamps on ``Instrument.is_fno``).
_FNO_EXCHANGES = frozenset({"NFO", "BFO", "CDS", "MCX"})
_FNO_INSTRUMENT_TYPES = frozenset({"FUT", "CE", "PE"})


class UnknownInstrument(KeyError):
    """Raised by :meth:`InstrumentStore.by_symbol` when a tradingsymbol is not in today's dump.

    A ``KeyError`` subclass so ``except KeyError`` callers keep working. A hard stop: never size, price, or
    route an order for a symbol with no tick/lot.
    """


class Instrument(BaseModel):
    """One row of the daily instruments dump (the fields the platform consumes).

    ``tick_size`` is a ``Decimal``: the price-banded exchange tick for THIS instrument.
    """

    model_config = ConfigDict(frozen=True)

    tradingsymbol: str
    instrument_token: int
    exchange: str
    segment: str
    tick_size: Decimal = Field(gt=0)            # per-instrument, price-banded; must be positive
    lot_size: int = Field(gt=0)
    instrument_type: str                        # "EQ" | "FUT" | "CE" | "PE" | ...
    is_fno: bool = False                        # F&O-listed underlying (dynamic band membership)
    name: str | None = None                     # company name from the dump — the alias-seed source


class InstrumentStore:
    """In-memory index of today's instruments, keyed by ``tradingsymbol``.

    :meth:`refresh` loads the dump (08:15 job or startup catch-up); tests use :meth:`seed`.
    """

    def __init__(self, clock: Clock) -> None:
        self._clock = clock
        self._by_symbol: dict[str, Instrument] = {}
        self._by_token: dict[int, str] = {}   # reverse index: instrument_token -> tradingsymbol (ticker)
        # Non-tradable INDICES seam: tradingsymbol -> token (+ reverse), kept OUT of the tradable
        # maps so by_symbol/round_to_tick/is_fno still fail-closed for indices.
        self._index_tokens: dict[str, int] = {}
        self._index_by_token: dict[int, str] = {}
        self._refreshed_at: datetime | None = None  # tz-aware IST datetime of last refresh/hydrate (None until)
        self._hydrated = False     # True only when the live snapshot came from hydrate() (provenance)

    async def refresh(self, kite_client: Any) -> int:
        """Pull the full instruments dump via ``kite_client`` and index it. Returns the row count.

        ``kite_client.instruments()`` may be sync or async (both awaited defensively). The tradable AND index
        maps are swapped in together at the end, so a failed refresh leaves the previous store intact.

        Index rows (``segment`` contains ``INDICES``) go to the separate token map (``tick_size=0`` would
        fail the ``gt=0`` model) and are not counted as skipped; only one missing its symbol or token is.

        The 08:15 job persists the snapshot via :meth:`snapshot_rows` so a restart can :meth:`hydrate`.
        TODO: no job writes ``instruments_daily.mis_leverage``/``mis_eligible``/``surveillance``; those
        restrictions are enforced by the universe build's exclusions.
        """
        raw = kite_client.instruments()
        if hasattr(raw, "__await__"):
            raw = await raw
        raw = list(raw)

        # NFO→underlying join: a derivative row's ``name`` is its underlying's tradingsymbol; collect those
        # first, then flag the matching NSE equities (the rows the platform actually looks up).
        fno_underlyings: set[str] = set()
        for row in raw:
            exchange = str(self._row_get(row, "exchange", "") or "")
            itype = str(self._row_get(row, "instrument_type", "") or "")
            if exchange in _FNO_EXCHANGES and itype in _FNO_INSTRUMENT_TYPES:
                name = str(self._row_get(row, "name", "") or "").strip()
                if name:
                    fno_underlyings.add(name)

        indexed: dict[str, Instrument] = {}
        index_tokens: dict[str, int] = {}
        index_by_token: dict[int, str] = {}
        skipped = 0
        for row in raw:
            # Index rows first: tick_size=0 would otherwise be skipped as a bad row below.
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
                instrument = self._row_to_instrument(row, fno_underlyings)
            except (KeyError, ValueError, TypeError) as exc:
                skipped += 1
                _log.warning("instrument.row_skipped", error=str(exc))
                continue
            indexed[instrument.tradingsymbol] = instrument

        # Atomic swap: a raise above leaves the previous store intact (never half of two dumps).
        self._by_symbol = indexed
        self._by_token = {ins.instrument_token: sym for sym, ins in indexed.items()}
        self._index_tokens = index_tokens
        self._index_by_token = index_by_token
        self._refreshed_at = self._clock.now()
        self._hydrated = False   # a live dump supersedes a cold-start hydrate
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

        Replace-snapshot semantics like :meth:`refresh`, including the index seam: ``index_tokens``
        (``tradingsymbol -> instrument_token``) replaces ``_index_tokens``; ``None`` clears it.
        """
        self._by_symbol = {ins.tradingsymbol: ins for ins in instruments}
        self._by_token = {ins.instrument_token: ins.tradingsymbol for ins in instruments}
        idx = dict(index_tokens or {})
        self._index_tokens = idx
        self._index_by_token = {tok: sym for sym, tok in idx.items()}
        self._refreshed_at = self._clock.now()
        self._hydrated = False
        return len(self._by_symbol)

    # -- persistence / cold-start hydration --------------------------------------
    #: The ``instruments_daily`` column order (store ``_TABLE_SPEC``); a test asserts it against the spec so
    #: a DDL column change fails loudly instead of silently dropping data.
    _SNAPSHOT_COLUMNS: tuple[str, ...] = (
        "d", "instrument_token", "tradingsymbol", "name", "exchange", "segment", "instrument_type",
        "tick_size", "lot_size", "mis_leverage", "mis_eligible", "surveillance", "fno", "extra",
    )

    def snapshot_rows(self, d: date) -> list[dict[str, Any]]:
        """Render today's dump as ``instruments_daily`` rows for day ``d``.

        One dict per tradable :class:`Instrument` PLUS one per index token, each with exactly the
        :attr:`_SNAPSHOT_COLUMNS` keys. ``mis_leverage``/``mis_eligible``/``surveillance``/``extra`` stay
        ``NULL`` (only the universe build's exclusions use those lists). ``name`` persists verbatim: it is
        the alias-seed source.

        ``tick_size`` is emitted verbatim as a ``Decimal``; the ``DECIMAL(18,6)`` column keeps a sub-₹0.01
        tick (₹0.0025 derivatives) from truncating to 0.00 and failing :meth:`hydrate`.

        Index rows get ``segment='INDICES'`` and ``instrument_type='INDEX'`` (what :meth:`hydrate` routes
        on), NULL ``tick_size``/``lot_size`` and ``fno=False``.
        """
        rows: list[dict[str, Any]] = []
        for ins in self._by_symbol.values():
            rows.append({
                "d": d,
                "instrument_token": ins.instrument_token,
                "tradingsymbol": ins.tradingsymbol,
                "name": ins.name,
                "exchange": ins.exchange,
                "segment": ins.segment,
                "instrument_type": ins.instrument_type,
                "tick_size": ins.tick_size,
                "lot_size": ins.lot_size,
                "mis_leverage": None,
                "mis_eligible": None,
                "surveillance": None,
                "fno": ins.is_fno,
                "extra": None,
            })
        for symbol, token in self._index_tokens.items():
            rows.append({
                "d": d,
                "instrument_token": token,
                "tradingsymbol": symbol,
                "name": None,
                "exchange": None,
                "segment": "INDICES",
                "instrument_type": "INDEX",
                "tick_size": None,
                "lot_size": None,
                "mis_leverage": None,
                "mis_eligible": None,
                "surveillance": None,
                "fno": False,
                "extra": None,
            })
        return rows

    def hydrate(self, rows: list[dict[str, Any]]) -> int:
        """Rebuild the in-memory index from persisted ``instruments_daily`` rows.

        The cold-start inverse of :meth:`snapshot_rows`: ``INDICES``/``INDEX`` rows go to the token seam,
        the rest to tradable :class:`Instrument`s, and all four maps swap in atomically like :meth:`refresh`.
        A malformed stored row (missing token, ``NULL``/zero tick, bad type) is skipped and counted, never
        aborting the hydrate. Sets :attr:`hydrated`. Returns the tradable row count loaded.

        The round-trip (``refresh`` → ``snapshot_rows`` → store → ``hydrate``) must reconstruct exactly what
        ``refresh`` loaded, with zero spurious skips; that depends on the store column not truncating a
        sub-₹0.01 tick. A genuinely non-positive stored tick still fails the ``gt=0`` model and is
        skip-counted: that invariant is deliberately not weakened.

        ``is_fno`` is read from the stored ``fno`` column verbatim, not re-derived: the F&O join needs the
        whole dump, which a stored equity row cannot reproduce.
        """
        indexed: dict[str, Instrument] = {}
        index_tokens: dict[str, int] = {}
        index_by_token: dict[int, str] = {}
        skipped = 0
        for row in rows:
            # Index rows go to the token seam before building an Instrument. EITHER discriminator
            # (``segment='INDICES'`` or ``instrument_type='INDEX'``) matches, so one drifted field still
            # round-trips; a corrupt EQUITY row never carries ``INDEX`` and is rejected below.
            segment = str(self._row_get(row, "segment", "") or "").upper()
            instrument_type = str(self._row_get(row, "instrument_type", "") or "").upper()
            if "INDICES" in segment or instrument_type == "INDEX":
                try:
                    symbol = str(self._row_get(row, "tradingsymbol") or "")
                    token = int(self._row_get(row, "instrument_token"))
                except (KeyError, ValueError, TypeError) as exc:
                    skipped += 1
                    _log.warning("instrument.hydrate_row_skipped", error=str(exc))
                    continue
                if not symbol:
                    skipped += 1
                    _log.warning("instrument.hydrate_row_skipped", error="index row missing tradingsymbol")
                    continue
                index_tokens[symbol] = token
                index_by_token[token] = symbol
                continue
            try:
                instrument = self._stored_row_to_instrument(row)
            except (KeyError, ValueError, TypeError, InvalidOperation) as exc:
                skipped += 1
                _log.warning("instrument.hydrate_row_skipped", error=str(exc))
                continue
            indexed[instrument.tradingsymbol] = instrument

        self._by_symbol = indexed
        self._by_token = {ins.instrument_token: sym for sym, ins in indexed.items()}
        self._index_tokens = index_tokens
        self._index_by_token = index_by_token
        self._refreshed_at = self._clock.now()
        self._hydrated = True
        _log.info(
            "instruments.hydrated",
            count=len(indexed),
            skipped=skipped,
            indices=len(index_tokens),
            at=self._refreshed_at.isoformat(),
        )
        return len(indexed)

    @property
    def is_empty(self) -> bool:
        """True when no dump is loaded (no tradable rows AND no index tokens) — the hydrate trigger."""
        return not self._by_symbol and not self._index_tokens

    @property
    def hydrated(self) -> bool:
        """True when the current snapshot came from :meth:`hydrate` rather than a live :meth:`refresh`;
        cleared by any subsequent refresh/seed."""
        return self._hydrated

    @property
    def index_count(self) -> int:
        """Number of resolvable non-tradable index tokens (for the startup report line)."""
        return len(self._index_tokens)

    def by_symbol(self, tradingsymbol: str) -> Instrument:
        """Return the :class:`Instrument` for ``tradingsymbol``.

        Raises :class:`UnknownInstrument` if the symbol is not in today's dump: a hard stop, never
        "assume defaults".
        """
        try:
            return self._by_symbol[tradingsymbol]
        except KeyError as exc:
            raise UnknownInstrument(tradingsymbol) from exc

    def round_to_tick(self, symbol: str, price: Decimal) -> Decimal:
        """Quantise ``price`` to the nearest multiple of ``symbol``'s tick size.

        The gate and OMS price every order through here so a banded tick is honoured. Snaps to the tick
        GRID with ``ROUND_HALF_UP`` (ties round up) and returns at the tick's own scale (tick ₹0.05 ⇒ two
        decimals), broker-acceptable as-is. Raises :class:`UnknownInstrument` for an unknown symbol.
        """
        tick = self.by_symbol(symbol).tick_size
        price = Decimal(price)
        steps = (price / tick).quantize(Decimal(1), rounding=ROUND_HALF_UP)
        snapped = steps * tick
        return snapped.quantize(tick, rounding=ROUND_HALF_UP)

    def token_for_symbol(self, symbol: str) -> int | None:
        """``tradingsymbol -> instrument_token`` (the ticker-subscription / backfill resolver seam).

        Tradables first, then the index token map (so ``NIFTY 50``/``INDIA VIX`` resolve for backfill).
        ``None`` for an unknown symbol (never guesses), so a caller reports that symbol failed rather than
        request candles for the wrong instrument.
        """
        instrument = self._by_symbol.get(symbol)
        if instrument is not None:
            return instrument.instrument_token
        return self._index_tokens.get(symbol)

    def symbol_for_token(self, token: int) -> str | None:
        """``instrument_token -> tradingsymbol`` reverse index (the ticker frame → symbol mapping).

        Tradables first, then the index map. ``None`` for an unknown token so the ``TickerSupervisor`` drops
        the frame rather than mislabel it.
        """
        symbol = self._by_token.get(token)
        if symbol is not None:
            return symbol
        return self._index_by_token.get(token)

    def is_fno(self, symbol: str) -> bool:
        """True if ``symbol`` is F&O-listed (dynamic-band membership).

        ``False`` for an unknown symbol, so the dynamic-band MIS-eligibility check fails closed.
        """
        instrument = self._by_symbol.get(symbol)
        return bool(instrument and instrument.is_fno)

    # -- internals -----------------------------------------------------------------------------

    @staticmethod
    def _row_get(row: Any, key: str, default: Any = None) -> Any:
        """One tolerant field read from a Kite dump row (dict- or attribute-shaped)."""
        if isinstance(row, dict):
            return row.get(key, default)
        return getattr(row, key, default)

    @staticmethod
    def _row_to_instrument(row: Any, fno_underlyings: frozenset[str] | set[str] = frozenset()) -> Instrument:
        """Map one Kite dump row (dict or object) to an :class:`Instrument`.

        ``fno_underlyings`` is the join input from :meth:`refresh`: an NSE equity whose tradingsymbol is
        among the derivative rows' ``name`` values is F&O-listed.
        """
        get = row.get if isinstance(row, dict) else (lambda k, d=None: getattr(row, k, d))

        exchange = str(get("exchange", "") or "")
        instrument_type = str(get("instrument_type", "") or "")
        tradingsymbol = str(get("tradingsymbol"))
        # A derivative row is F&O by its own shape; an EQUITY only via the underlying-name join.
        is_fno = (
            exchange in _FNO_EXCHANGES
            or instrument_type in _FNO_INSTRUMENT_TYPES
            or tradingsymbol in fno_underlyings
        )

        return Instrument(
            tradingsymbol=tradingsymbol,
            instrument_token=int(get("instrument_token")),
            exchange=exchange,
            segment=str(get("segment", "") or ""),
            tick_size=Decimal(str(get("tick_size"))),
            lot_size=int(get("lot_size") or 1),
            instrument_type=instrument_type,
            is_fno=is_fno,
            name=(str(get("name")).strip() or None) if get("name") else None,
        )

    @staticmethod
    def _stored_row_to_instrument(row: dict[str, Any]) -> Instrument:
        """Rebuild an :class:`Instrument` from a persisted ``instruments_daily`` row (:meth:`hydrate`).

        Reads the stored ``fno`` verbatim (unlike the live path, which derives it). A missing/NULL/zero
        token, tick or lot raises so :meth:`hydrate` skip-and-counts the row.
        """
        get = row.get if isinstance(row, dict) else (lambda k, d=None: getattr(row, k, d))
        return Instrument(
            tradingsymbol=str(get("tradingsymbol")),
            instrument_token=int(get("instrument_token")),
            exchange=str(get("exchange", "") or ""),
            segment=str(get("segment", "") or ""),
            tick_size=Decimal(str(get("tick_size"))),
            lot_size=int(get("lot_size")),
            instrument_type=str(get("instrument_type", "") or ""),
            is_fno=bool(get("fno")),
            name=(str(get("name")).strip() or None) if get("name") else None,
        )
