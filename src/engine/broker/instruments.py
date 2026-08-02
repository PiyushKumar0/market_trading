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

The store is in-memory: a process holds today's dump only after a :meth:`refresh` (or a cold-start
:meth:`hydrate`). Because the map is rebuilt from scratch each boot, an engine restart AFTER the
08:15 ``instruments`` job — whose ``job_runs`` watermark makes the catch-up runner skip it — would
otherwise leave every token lookup empty (the cold-start ``unknown_token`` storm). Two seams close
that gap (§4.3): :meth:`snapshot_rows` renders the current dump as ``instruments_daily`` rows the
08:15 job persists, and :meth:`hydrate` rebuilds the in-memory index from the latest stored snapshot
at startup — pre-login, so recovery never waits on a Kite session. This module talks only to ``core``
+ the injected ``kite_client``; it never imports ``engine.intelligence`` (R1).
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
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
    name: str | None = None                     # company name from the dump — the §3.2.4 alias-seed
                                                # source (dropped until 2026-08-03: empty-alias G1 finding)


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
        self._refreshed_at: datetime | None = None  # tz-aware IST datetime of last refresh/hydrate (None until)
        self._hydrated = False     # True only when the live snapshot came from hydrate() (provenance)

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

        The daily snapshot is persisted to the DuckDB ``instruments_daily`` table by the 08:15 job
        (via :meth:`snapshot_rows`) so a later restart can :meth:`hydrate` the token map pre-login
        (§4.3). TODO(Phase 1): join the Zerodha MIS-leverage + NSE surveillance files (A8) so
        ``is_fno`` (C7), ``surveillance`` and per-stock leverage come from the real NFO/margins join
        rather than the per-row heuristic below (that join is surveillance's own job — snapshot_rows
        leaves those columns at their spec defaults).
        """
        raw = kite_client.instruments()
        if hasattr(raw, "__await__"):
            raw = await raw
        raw = list(raw)

        # C7 NFO→underlying join (2026-07-31: the per-row heuristic marked only the DERIVATIVE rows
        # is_fno, never the NSE equity the platform actually looks up — mis_candidates was 0 every
        # day and the gate structurally rejected every MIS proposal). A derivative row's ``name`` is
        # its underlying's tradingsymbol; collect them first, then flag matching equities.
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
                instrument = self._row_to_instrument(row, fno_underlyings)
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
        self._hydrated = False   # a live dump supersedes any prior cold-start hydrate (provenance)
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
        self._hydrated = False
        return len(self._by_symbol)

    # -- persistence / cold-start hydration (§4.3, F1/F2) --------------------------------------
    #: The ``instruments_daily`` column order (§4.3 DDL / store ``_TABLE_SPEC``). Pinned here so
    #: :meth:`snapshot_rows` emits exactly these keys; the test asserts it against the store spec so a
    #: DDL column add/rename fails loudly instead of silently dropping data.
    _SNAPSHOT_COLUMNS: tuple[str, ...] = (
        "d", "instrument_token", "tradingsymbol", "name", "exchange", "segment", "instrument_type",
        "tick_size", "lot_size", "mis_leverage", "mis_eligible", "surveillance", "fno", "extra",
    )

    def snapshot_rows(self, d: date) -> list[dict[str, Any]]:
        """Render today's dump as ``instruments_daily`` rows for day ``d`` (§4.3, F1).

        One dict per tradable :class:`Instrument` PLUS one per index token (the non-tradable
        ``INDICES`` seam), each carrying exactly the :attr:`_SNAPSHOT_COLUMNS` keys so the store's
        pinned-column upsert accepts them. The A8 surveillance/MIS-leverage join is surveillance's own
        job, so ``mis_leverage``/``mis_eligible``/``surveillance``/``extra`` are left at the table's
        ``NULL`` default here — this writer persists only what the dump itself carries. ``name`` IS
        dump-carried and persists verbatim: it is the §3.2.4 alias-seed source (2026-08-03 G1 finding —
        a NULL name column left ``entity_aliases`` empty and every headline unresolved).

        Each tradable ``tick_size`` is emitted verbatim (a ``Decimal``); the store column is
        ``DECIMAL(18,6)`` so a sub-₹0.01 tick (₹0.0025 currency/commodity derivatives) survives the
        round-trip instead of truncating to 0.00 and being rejected on :meth:`hydrate` (2026-07-21).

        Index rows are representable within the DDL (``tick_size``/``lot_size`` are nullable): they
        get ``segment='INDICES'`` (the discriminator :meth:`hydrate`/:meth:`refresh` route on) and
        ``instrument_type='INDEX'``, with ``tick_size``/``lot_size`` NULL (an index is never priced or
        sized, A2) and ``fno=False`` (an index is not an F&O underlying, C7).
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
        """Rebuild the in-memory index from persisted ``instruments_daily`` rows (§4.3, F2).

        The cold-start inverse of :meth:`snapshot_rows`: classify the stored rows the way :meth:`refresh`
        classifies live-dump rows (``INDICES``/``INDEX`` → the non-tradable token seam, everything else →
        tradable :class:`Instrument`s) and swap all four maps in atomically, exactly like :meth:`refresh`
        — a raise mid-build leaves the prior (empty) store intact. A malformed stored row (missing token,
        ``NULL``/zero tick, bad type) is skipped and counted, never aborting the hydrate; the count is
        logged. Sets the :attr:`hydrated` provenance flag so the startup report can say the token map is a
        stored snapshot, not a live dump. Returns the tradable row count loaded.

        2026-07-21 (lossless round-trip): a full round-trip (``refresh`` → ``snapshot_rows`` → store →
        ``get_latest`` → ``hydrate``) must reconstruct EXACTLY what ``refresh`` loaded — same token map,
        same :attr:`index_count`, zero spurious skips. The prerequisite lives in the store: ``refresh``
        builds an :class:`Instrument` straight from the live dump (so a sub-₹0.01 tick like ₹0.0025 is a
        valid ``tick_size>0`` row), but ``hydrate`` rebuilds from the PERSISTED tick — which the
        ``instruments_daily.tick_size`` column must not have truncated to 0.00 (the widened
        ``DECIMAL(18,6)`` column, §4.3, is what keeps this lossless). A row whose stored tick is genuinely
        non-positive (corrupt EQUITY data) still fails the ``gt=0`` model and is skip-counted here — that
        A10 invariant is deliberately NOT weakened.

        Unlike :meth:`refresh`, ``is_fno`` is read from the stored ``fno`` column verbatim (not
        re-derived) so hydrate stays a faithful inverse even once F&O membership comes from the A8 NFO
        join rather than the exchange/type heuristic.
        """
        indexed: dict[str, Instrument] = {}
        index_tokens: dict[str, int] = {}
        index_by_token: dict[int, str] = {}
        skipped = 0
        for row in rows:
            # Route non-tradable INDEX rows to the token-only seam BEFORE constructing an Instrument —
            # symmetric with the discriminators :meth:`snapshot_rows` writes (BOTH ``segment='INDICES'``
            # AND ``instrument_type='INDEX'``, with a NULL tick). Recognising EITHER hardens the round-trip
            # against a snapshot where one field drifted (2026-07-21); a genuine tradable never carries
            # ``instrument_type='INDEX'``, so a corrupt EQUITY row still falls through to the tradable
            # path below and is rejected (the A10 ``tick_size>0`` invariant stays intact).
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
        """True when no dump is loaded (no tradable rows AND no index tokens) — the F2 hydrate trigger."""
        return not self._by_symbol and not self._index_tokens

    @property
    def hydrated(self) -> bool:
        """Provenance: True when the current snapshot came from :meth:`hydrate` (a stored dump at cold
        start) rather than a live :meth:`refresh`. Cleared by any subsequent refresh/seed."""
        return self._hydrated

    @property
    def index_count(self) -> int:
        """Number of resolvable non-tradable index tokens (the A2 ``INDICES`` seam) — for the startup
        report line, where a hydrated dump reports ``count`` tradables + ``indices`` regime tokens."""
        return len(self._index_tokens)

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
    def _row_to_instrument(row: Any, fno_underlyings: frozenset[str] | set[str] = frozenset()) -> Instrument:
        """Map one Kite dump row (dict or object) to an :class:`Instrument`.

        Tolerant of dict-shaped (pykiteconnect ``instruments()``) and attribute-shaped rows.
        ``fno_underlyings`` is the C7 join input from :meth:`refresh`: an NSE equity whose
        tradingsymbol appears among the derivative rows' ``name`` values is F&O-listed.
        """
        get = row.get if isinstance(row, dict) else (lambda k, d=None: getattr(row, k, d))

        exchange = str(get("exchange", "") or "")
        instrument_type = str(get("instrument_type", "") or "")
        tradingsymbol = str(get("tradingsymbol"))
        # A derivative row is F&O by its own shape; an EQUITY is F&O-listed when its tradingsymbol
        # is among the derivative rows' underlying names (the C7 join — 2026-07-31; the shape-only
        # heuristic left every NSE equity is_fno=False and mis_candidates empty forever).
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

        Distinct from :meth:`_row_to_instrument` (the live-dump path that DERIVES ``is_fno`` from the
        exchange/type heuristic): a stored snapshot already carries the authoritative ``fno`` value, so
        read it verbatim. Any missing/NULL/zero required field (token, tick, lot) raises through the
        tolerant accessor + the ``Instrument`` model (``tick_size``/``lot_size`` ``gt=0``) so
        :meth:`hydrate` skip-and-counts it.
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
