"""Daily tradeable-universe build (§3.2.4 ``UniverseBuilder``, 08:30 — A8/C7/E5).

The PINNED universe rule (§3.2.4, zero latitude):

    NIFTY200 ∩ MIS-eligible (Zerodha leverage file) ∩ NOT in GSM/ASM/T2T/ESM (A8)
    ∩ median 20d traded value ≥ ₹5cr [``data.min_median_traded_value_inr``, tunable];
    mis_candidates ⊆ F&O list (C7); active intraday watchlist capped at 50
    [``data.universe_max_watchlist``, tunable] — capacity is not the binding constraint; focus is.

Every NIFTY200 symbol gets a ``universe_daily`` row with its per-symbol exclusion reasons
(auditable, §4.3). ``included=True`` means "in today's ACTIVE watchlist" (≤ cap): symbols that pass
every rule but fall past the cap are persisted ``included=False`` with reason ``watchlist_cap`` so
the audit trail explains exactly why an otherwise-eligible name is out.

NIFTY200 membership is best-effort (E5): download the NSE indices constituents CSV
(``settings.universe.nifty200_source_url``, [VERIFY Phase-1]) → on failure fall back to the runtime
cached copy under ``data/universe/`` → then to the committed seed
``config/universe/nifty200_seed.csv`` (owner-refreshed). Non-download provenance alerts but never
blocks the build.

Resolved ambiguities (documented, plan-silent):
- Median traded value uses ``close × volume`` over the last up-to-20 ``bars_1d`` rows strictly
  BEFORE the build date (built pre-open; day ``d`` has no bar yet). A symbol with NO daily bars
  cannot confirm liquidity ⇒ excluded (``no_liquidity_data`` — conservative, never assume liquid).
- The watchlist cap keeps the TOP-``N`` eligible symbols by median traded value (desc, symbol asc
  tie-break) — deterministic and liquidity-first.

Dependencies: ``core``, ``broker`` (InstrumentStore, C7), ``marketdata`` (MarketStore single-writer,
convention 12), sibling ``universe`` ingests. Never raises into the scheduler (E5).
"""

from __future__ import annotations

import csv
import io
import statistics
from collections.abc import Awaitable, Callable
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Literal

import httpx
from pydantic import BaseModel, ConfigDict, Field

from engine.broker.instruments import InstrumentStore
from engine.core.clock import Clock
from engine.core.config import Settings, repo_root
from engine.core.log import get_logger
from engine.marketdata.store import MarketStore
from engine.notify.catalog import CatalogMessage, MessageKind
from engine.universe.leverage import MisLeverageIngest
from engine.universe.surveillance import SurveillanceIngest

_log = get_logger("engine.universe.builder")

# ---------------------------------------------------------------- exclusion reasons (auditable, §4.3)
EXCL_NOT_MIS = "not_mis_eligible"                     # absent/1× in the Zerodha leverage file
EXCL_GSM = "surveillance_gsm"                         # A8
EXCL_ASM = "surveillance_asm"
EXCL_T2T = "surveillance_t2t"
EXCL_ESM = "surveillance_esm"
EXCL_LOW_VALUE = "below_min_traded_value"             # median 20d traded value < threshold
EXCL_NO_DATA = "no_liquidity_data"                    # no bars_1d history ⇒ liquidity unconfirmable
EXCL_CAP = "watchlist_cap"                            # eligible, but past the top-N focus cap

#: Calendar-day lookback that comfortably contains 20 TRADING days (holidays/weekends margin).
_TRADED_VALUE_LOOKBACK_DAYS = 45
_TRADED_VALUE_BARS = 20                               # §3.2.4: "median 20d traded value"
_PAISE = Decimal("0.01")

NotifySink = Callable[[CatalogMessage], Awaitable[None]]

Nifty200Source = Literal["download", "cache", "seed", "none"]


class Universe(BaseModel):
    """One day's resolved universe (§3.2.4 ``build`` output; persisted as ``universe_daily``).

    ``symbols`` is the ACTIVE intraday watchlist (≤ cap, sorted); ``mis_candidates`` ⊆ ``symbols``
    ∩ F&O list (C7). ``eligible`` is the pre-cap rule-passing set (audit). ``exclusions`` maps every
    excluded NIFTY200 symbol to its reasons. ``degraded=True`` when any input was reused/stale
    (NIFTY200 not freshly downloaded, leverage file cached, or a surveillance source down).
    """

    model_config = ConfigDict(frozen=True)

    d: date
    symbols: tuple[str, ...] = ()
    mis_candidates: tuple[str, ...] = ()
    eligible: tuple[str, ...] = ()
    exclusions: dict[str, tuple[str, ...]] = Field(default_factory=dict)
    median_traded_value: dict[str, Decimal] = Field(default_factory=dict)
    nifty200_source: Nifty200Source = "none"
    degraded: bool = False


def parse_index_constituents_csv(text: str) -> list[str]:
    """Symbols from an NSE index-constituents CSV (``Company Name,Industry,Symbol,Series,ISIN``).

    Defensive (E5): ``#``-prefixed comment lines are skipped (the committed seed carries an owner
    note), the ``Symbol`` column is located case-insensitively, and a ``Series`` column (if present)
    filters to EQ. Order-preserving, de-duplicated, uppercased.
    """
    lines = [ln for ln in text.splitlines() if ln.strip() and not ln.lstrip().startswith("#")]
    reader = csv.DictReader(io.StringIO("\n".join(lines)))
    norm = {(name or "").strip().lower(): name for name in (reader.fieldnames or [])}
    sym_col = norm.get("symbol")
    if sym_col is None:
        raise ValueError(f"no Symbol column in index CSV: {reader.fieldnames}")
    series_col = norm.get("series")
    out: list[str] = []
    seen: set[str] = set()
    for row in reader:
        if series_col is not None:
            series = (row.get(series_col) or "").strip().upper()
            if series and series != "EQ":
                continue
        symbol = (row.get(sym_col) or "").strip().upper()
        if symbol and symbol not in seen:
            seen.add(symbol)
            out.append(symbol)
    return out


class UniverseBuilder:
    """§3.2.4 ``UniverseBuilder`` — runs 08:30 daily; persists the ``universe_daily`` snapshot.

    Parameters
    ----------
    settings:
        Typed config — thresholds (``data.*``), NIFTY200 source URL + seed path (``universe.*``).
    store:
        Single-writer :class:`MarketStore` (convention 12) — ``bars_1d`` reads + ``universe_daily``
        writes, all executor-offloaded via ``store.arun`` (convention 4).
    instruments:
        :class:`InstrumentStore` — the C7 F&O-membership authority (``is_fno``).
    leverage / surveillance:
        The sibling ingests; :meth:`build` reads their ``current()`` snapshot (scheduler refreshes
        them at 08:15/08:20 before the 08:30 build; on catch-up ``current()`` self-refreshes).
    clock:
        Single source of "now" (§3.2).
    http:
        Injected ``httpx.AsyncClient`` for the NIFTY200 download (convention 11, E5).
    notify:
        Optional owner-alert sink for degraded inputs / build failure.
    """

    def __init__(
        self,
        settings: Settings,
        store: MarketStore,
        instruments: InstrumentStore,
        leverage: MisLeverageIngest,
        surveillance: SurveillanceIngest,
        clock: Clock,
        http: httpx.AsyncClient,
        *,
        notify: NotifySink | None = None,
        request_timeout_s: float = 15.0,
    ) -> None:
        self._settings = settings
        self._store = store
        self._instruments = instruments
        self._leverage = leverage
        self._surveillance = surveillance
        self._clock = clock
        self._http = http
        self._notify = notify
        self._timeout = float(request_timeout_s)
        seed = Path(settings.universe.nifty200_seed_path)
        self._seed_path = seed if seed.is_absolute() else repo_root() / seed
        self._cache_path = settings.resolved_data_dir() / "universe" / "nifty200_cached.csv"

    async def build(self, d: date) -> Universe:
        """Build + persist day ``d``'s universe. NEVER raises into the scheduler (E5): a total
        failure alerts and returns an empty, degraded :class:`Universe` (fail closed — no universe,
        no new entries; risk-reducing paths are elsewhere and unaffected, R3)."""
        try:
            return await self._build(d)
        except Exception as exc:  # noqa: BLE001 - E5: degrade + alert, never raise into the scheduler
            _log.exception("universe_build_failed", d=d.isoformat())
            await self._alert(
                title="Universe build failed",
                body=f"Build for {d.isoformat()} failed ({type(exc).__name__}: {exc}). "
                "No universe persisted — entries have no watchlist today (fail closed).",
                severity="critical",
                data={"job_id": "universe_build", "d": d.isoformat()},
            )
            return Universe(d=d, nifty200_source="none", degraded=True)

    # ------------------------------------------------------------------ core
    async def _build(self, d: date) -> Universe:
        leverage = await self._leverage.current()
        surveillance = await self._surveillance.current()
        nifty200, source = await self._load_nifty200()
        if not nifty200:
            raise ValueError("NIFTY200 membership unavailable from download, cache, and seed")

        medians = await self._store.arun(self._median_traded_values, nifty200, d)
        min_value = Decimal(self._settings.data.min_median_traded_value_inr)

        exclusions: dict[str, list[str]] = {}
        eligible: list[str] = []
        for symbol in nifty200:
            reasons: list[str] = []
            if not leverage.is_mis_eligible(symbol):
                reasons.append(EXCL_NOT_MIS)
            reasons.extend(surveillance.reasons_for(symbol))
            median = medians.get(symbol)
            if median is None:
                reasons.append(EXCL_NO_DATA)
            elif median < min_value:
                reasons.append(EXCL_LOW_VALUE)
            if reasons:
                exclusions[symbol] = reasons
            else:
                eligible.append(symbol)

        # Focus cap (§3.2.4): top-N eligible by median traded value desc, symbol asc tie-break.
        cap = int(self._settings.data.universe_max_watchlist)
        ranked = sorted(eligible, key=lambda s: (-medians[s], s))
        watchlist = sorted(ranked[:cap])
        for symbol in ranked[cap:]:
            exclusions[symbol] = [EXCL_CAP]

        # mis_candidates ⊆ F&O list (C7): dynamic circuit band required for MIS names.
        mis_candidates = tuple(s for s in watchlist if self._instruments.is_fno(s))

        included = set(watchlist)
        mis_set = set(mis_candidates)
        rows = [
            {
                "d": d,
                "symbol": symbol,
                "included": symbol in included,
                "mis_candidate": symbol in mis_set,
                "exclusion_reasons": exclusions.get(symbol, []),
                "median_traded_value": (
                    medians[symbol].quantize(_PAISE) if symbol in medians else None
                ),
            }
            for symbol in nifty200
        ]
        await self._store.arun(self._store.upsert_universe_daily, rows)

        degraded = source != "download" or leverage.degraded or bool(surveillance.degraded_sources)
        universe = Universe(
            d=d,
            symbols=tuple(watchlist),
            mis_candidates=mis_candidates,
            eligible=tuple(sorted(eligible)),
            exclusions={s: tuple(r) for s, r in exclusions.items()},
            median_traded_value={s: v.quantize(_PAISE) for s, v in medians.items()},
            nifty200_source=source,
            degraded=degraded,
        )
        _log.info(
            "universe_built",
            d=d.isoformat(),
            nifty200=len(nifty200),
            eligible=len(eligible),
            watchlist=len(watchlist),
            mis_candidates=len(mis_candidates),
            source=source,
            degraded=degraded,
        )
        return universe

    # ------------------------------------------------------------------ NIFTY200 membership (E5)
    async def _load_nifty200(self) -> tuple[list[str], Nifty200Source]:
        """Download → runtime cache → committed seed (E5 ladder). Alerts when not freshly downloaded."""
        url = self._settings.universe.nifty200_source_url
        try:
            resp = await self._http.get(url, timeout=self._timeout)
            resp.raise_for_status()
            symbols = parse_index_constituents_csv(resp.text)
            if not symbols:
                raise ValueError("downloaded NIFTY200 CSV parsed to zero symbols")
            self._write_cache(resp.text)
            return symbols, "download"
        except Exception as exc:  # noqa: BLE001 - E5: fall down the ladder, never raise
            _log.warning("nifty200_download_failed", url=url, error=f"{type(exc).__name__}: {exc}")

        for path, source in ((self._cache_path, "cache"), (self._seed_path, "seed")):
            try:
                if path.exists():
                    symbols = parse_index_constituents_csv(path.read_text(encoding="utf-8"))
                    if symbols:
                        await self._alert(
                            title="NIFTY200 download failed — using fallback",
                            body=f"NSE constituents download failed; using the {source} copy "
                            f"({len(symbols)} symbols). Membership may be stale (E5).",
                            severity="warning",
                            data={"job_id": "universe_build", "fallback": source},
                        )
                        return symbols, source  # type: ignore[return-value]
            except (OSError, ValueError):
                _log.exception("nifty200_fallback_unreadable", path=str(path))
        return [], "none"

    def _write_cache(self, text: str) -> None:
        try:
            self._cache_path.parent.mkdir(parents=True, exist_ok=True)
            self._cache_path.write_text(text, encoding="utf-8")
        except OSError:
            _log.exception("nifty200_cache_write_failed", path=str(self._cache_path))

    # ------------------------------------------------------------------ liquidity filter
    def _median_traded_values(self, symbols: list[str], d: date) -> dict[str, Decimal]:
        """Median of ``close × volume`` over the last ≤20 daily bars strictly before ``d``.

        Sync (runs under ``store.arun``). Symbols with no history are absent from the result —
        the caller excludes them (``no_liquidity_data``)."""
        start = d - timedelta(days=_TRADED_VALUE_LOOKBACK_DAYS)
        end = d - timedelta(days=1)
        out: dict[str, Decimal] = {}
        for symbol in symbols:
            bars = self._store.get_bars_1d(symbol, start, end)
            if not bars:
                continue
            window = bars[-_TRADED_VALUE_BARS:]
            out[symbol] = statistics.median([bar.close * bar.volume for bar in window])
        return out

    # ------------------------------------------------------------------ alerts
    async def _alert(self, *, title: str, body: str, severity: str, data: dict) -> None:
        if self._notify is None:
            return
        msg = CatalogMessage(
            kind=MessageKind.DATA_FRESHNESS_FROZEN,  # closest catalog kind (closed catalog here)
            title=title,
            body=body,
            severity=severity,  # type: ignore[arg-type]
            data=data,
        )
        try:
            await self._notify(msg)
        except Exception:  # noqa: BLE001 - best-effort alert; a failed send never propagates
            _log.exception("universe_notify_failed")
