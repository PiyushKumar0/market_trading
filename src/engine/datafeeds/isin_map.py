"""Symbol → ISIN → BSE scrip-code map → ``symbol_isin`` (§2.8 utility; O14, E5).

Builds the stable cross-exchange join key (§2.8.1: symbols change, ISINs survive renames). Three
layers, cheapest/most-authoritative first:

1. **NIFTY-constituents ISIN (offline, authoritative).** The universe CSV the repo already caches
   carries an ``ISIN Code`` column that ``engine.universe.builder.parse_index_constituents_csv``
   parses-and-DROPS. This module re-reads that cached CSV (download-cache → committed seed ladder;
   ``builder.py`` is left untouched per the brief) and keeps the ISIN.
2. **Announcements ``sm_isin`` fallback.** For symbols absent from the CSV, one NSE
   ``corporate-announcements`` page carries ``symbol`` + ``sm_isin`` (probe-verified) — a best-effort
   supplement, never load-bearing.
3. **BSE BULK scrip master via ``ListofScripData/w``** (:data:`BSE_SCRIP_MASTER_URL`) — the whole
   Active-Equity ISIN→scrip-code list in ONE request, parsed by :func:`parse_scrip_master`. It
   reproduced all 199 codes the old per-symbol ``PeerSmartSearch`` resolver had found (2026-09-12);
   an ISIN it lacks is NSE-only or not a live BSE scrip. BSE 404s masquerade as 200 +
   ``error_Bse.html`` ⇒ fetched through :func:`engine.core.bse_http.bse_get`. The §2.8 fresh-insider
   feed reads the same master to build its own scrip→symbol map every run.

The job runs daily (``isin_map``, before ``filings_shp``, which fetches only mapped symbols): run by
hand only, the map stayed at the 200 symbols of 2026-07-17 after the universe grew to NIFTY 500.
Defensive throughout; a failed network layer degrades to fewer mappings, never raises.
"""

from __future__ import annotations

import csv
import io
import json
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict

from engine.core.bse_http import bse_get
from engine.core.clock import Clock
from engine.core.config import Settings, repo_root
from engine.core.log import get_logger
from engine.core.nse_http import nse_get
from engine.marketdata.store import MarketStore
from engine.notify.catalog import CatalogMessage, MessageKind

_log = get_logger("engine.datafeeds.isin_map")

#: NSE announcements page carrying ``symbol`` + ``sm_isin`` (probe-verified). [VERIFY Phase-1].
NSE_ANNOUNCEMENTS_URL = "https://www.nseindia.com/api/corporate-announcements?index=equities"

#: BSE BULK scrip master — the whole Active-Equity list (``SCRIP_CD`` + ``ISIN_NUMBER``) in ONE
#: request. Probe-verified 2026-09-12: 5,004 rows / 5,003 distinct ISINs, no ISIN carrying two
#: different codes.
BSE_SCRIP_MASTER_URL = (
    "https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w"
    "?Group=&Scripcode=&industry=&segment=Equity&status=Active"
)

#: Floor on the master's read timeout: the response is ~1.7 MB where the per-scrip BSE surfaces are a
#: few hundred KB, so it must not inherit a per-surface timeout tuned for them.
SCRIP_MASTER_TIMEOUT_S = 45.0

NotifySink = Callable[[CatalogMessage], Awaitable[None]]


class IsinMapResult(BaseModel):
    """One build's outcome (never an exception, E5)."""

    model_config = ConfigDict(frozen=True)

    ok: bool
    degraded: bool = False
    symbols: int = 0
    with_isin: int = 0
    scrip_resolved: int = 0
    rows_written: int = 0
    reason: str | None = None


# --------------------------------------------------------------------------- pure parsers
def parse_constituents_isin(text: str) -> dict[str, str]:
    """NIFTY index-constituents CSV → ``{symbol: isin}`` (the ISIN column ``builder.py`` drops).

    Defensive (E5): ``#``-comment lines skipped, ``Symbol`` + ``ISIN Code`` columns located
    case-insensitively, ``Series`` (if present) filtered to EQ. Uppercased symbols, deduped.
    """
    lines = [ln for ln in text.splitlines() if ln.strip() and not ln.lstrip().startswith("#")]
    reader = csv.DictReader(io.StringIO("\n".join(lines)))
    norm = {(name or "").strip().lower(): name for name in (reader.fieldnames or [])}
    sym_col = norm.get("symbol")
    isin_col = norm.get("isin code") or norm.get("isin")
    if sym_col is None or isin_col is None:
        return {}
    series_col = norm.get("series")
    out: dict[str, str] = {}
    for row in reader:
        if series_col is not None:
            series = (row.get(series_col) or "").strip().upper()
            if series and series != "EQ":
                continue
        symbol = (row.get(sym_col) or "").strip().upper()
        isin = (row.get(isin_col) or "").strip().upper()
        if symbol and isin and symbol not in out:
            out[symbol] = isin
    return out


def parse_announcements_isin(payload: Any) -> dict[str, str]:
    """NSE announcements JSON → ``{symbol: sm_isin}`` (best-effort fallback, probe-verified fields)."""
    rows = payload if isinstance(payload, list) else []
    if isinstance(payload, dict):
        for key in ("data", "rows", "records"):
            if isinstance(payload.get(key), list):
                rows = payload[key]
                break
    out: dict[str, str] = {}
    for raw in rows:
        if not isinstance(raw, dict):
            continue
        keys = {str(k).lower(): v for k, v in raw.items()}
        symbol = str(keys.get("symbol") or "").strip().upper()
        isin = str(keys.get("sm_isin") or keys.get("isin") or "").strip().upper()
        if symbol and isin and symbol not in out:
            out[symbol] = isin
    return out


def parse_scrip_master(payload: Any) -> dict[str, str]:
    """BSE bulk scrip master → ``{isin: bse_scrip_code}`` (defensive; probe-verified field names).

    The capture is a BARE LIST of scrip dicts (no ``Table`` envelope); the usual wrapper keys are
    still tolerated in case BSE wraps it later. A row missing either an ISIN or a code is dropped,
    and the FIRST code seen for an ISIN wins — in the 2026-09-12 capture no ISIN carried two
    different codes, so the tie-break is a determinism guarantee rather than a real choice.
    """
    rows: list[Any] = payload if isinstance(payload, list) else []
    if isinstance(payload, dict):
        for key in ("Table", "data", "rows", "records"):
            if isinstance(payload.get(key), list):
                rows = payload[key]
                break
    out: dict[str, str] = {}
    for raw in rows:
        if not isinstance(raw, dict):
            continue
        keys = {str(k).lower(): v for k, v in raw.items()}
        isin = str(keys.get("isin_number") or keys.get("isin") or "").strip().upper()
        code = str(keys.get("scrip_cd") or keys.get("scripcode") or "").strip()
        if isin and code and isin not in out:
            out[isin] = code
    return out


def load_constituents_isin(settings: Settings) -> dict[str, str]:
    """Read the cached index-constituents CSV (download-cache → committed seed ladder) for ISINs.

    Mirrors ``UniverseBuilder``'s cache/seed paths (``builder.py`` untouched) — both renamed by O15
    (2026-09-04) when the index became config (NIFTY 500); returns ``{}`` if neither is readable
    (the caller then relies on the announcements fallback)."""
    cache = settings.resolved_data_dir() / "universe" / "index_cached.csv"
    seed_rel = Path(settings.universe.index_seed_path)
    seed = seed_rel if seed_rel.is_absolute() else repo_root() / seed_rel
    for path in (cache, seed):
        try:
            if path.exists():
                mapping = parse_constituents_isin(path.read_text(encoding="utf-8"))
                if mapping:
                    return mapping
        except (OSError, ValueError):
            _log.warning("isin_map_csv_unreadable", path=str(path))
    return {}


class IsinMapJob:
    """§2.8 — build/refresh ``symbol_isin`` (CSV ISIN + announcements fallback + BSE bulk master)."""

    def __init__(
        self,
        settings: Settings,
        store: MarketStore,
        clock: Clock,
        http: httpx.AsyncClient,
        *,
        notify: NotifySink | None = None,
        request_timeout_s: float = 20.0,
    ) -> None:
        self._settings = settings
        self._store = store
        self._clock = clock
        self._http = http
        self._notify = notify
        self._timeout = float(request_timeout_s)
        #: One alert per failing streak (the job is run-latest and retried by the catch-up sweep).
        self._alerted = False

    async def run(self, symbols: list[str] | None = None) -> IsinMapResult:
        """Build ``symbol_isin`` for ``symbols`` (uppercased), default every index constituent.
        ``ok`` is False — and the owner alerted once per streak — when a symbol still lacks a BSE
        scrip code because the master could not be read. Never raises (E5)."""
        try:
            result = await self._run(symbols)
        except Exception as exc:  # noqa: BLE001 - E5: degrade + alert, never raise
            reason = f"{type(exc).__name__}: {exc}"
            _log.exception("isin_map_build_failed")
            result = IsinMapResult(ok=False, degraded=True, reason=reason)
        if result.ok:
            self._alerted = False
        elif not self._alerted:
            await self._alert(result.reason or "unknown")
            self._alerted = True
        return result

    async def _run(self, symbols: list[str] | None) -> IsinMapResult:
        isin_by_symbol = load_constituents_isin(self._settings)
        degraded = not isin_by_symbol
        wanted = [s.strip().upper() for s in symbols if s.strip()] if symbols is not None else sorted(isin_by_symbol)

        missing = [s for s in wanted if s not in isin_by_symbol]
        if missing:
            try:
                resp = await nse_get(self._http, NSE_ANNOUNCEMENTS_URL, timeout=self._timeout)
                fallback = parse_announcements_isin(json.loads(resp.content))
                for symbol in missing:
                    if symbol in fallback:
                        isin_by_symbol[symbol] = fallback[symbol]
            except Exception as exc:  # noqa: BLE001 - fallback is best-effort; degrade, never raise
                degraded = True
                _log.warning("isin_map_announcements_failed", error=f"{type(exc).__name__}: {exc}")

        # A stored code wins: a split changes the ISIN, never the BSE scrip code.
        existing = await self._store.asymbol_isin_map()
        needs_code = [
            s for s in wanted
            if isin_by_symbol.get(s) and not (existing.get(s) or {}).get("bse_scrip_code")
        ]
        master: dict[str, str] = {}
        master_error: str | None = None
        if needs_code:
            try:
                resp = await bse_get(
                    self._http, BSE_SCRIP_MASTER_URL, timeout=max(self._timeout, SCRIP_MASTER_TIMEOUT_S)
                )
                master = parse_scrip_master(json.loads(resp.content))
            except Exception as exc:  # noqa: BLE001 - degrade to the stored codes, never raise
                master_error = f"BSE scrip master: {type(exc).__name__}: {exc}"
                _log.warning("isin_map_master_failed", error=master_error)

        as_of = self._clock.today()
        rows: list[dict[str, Any]] = []
        with_isin = scrip_resolved = 0
        for symbol in wanted:
            isin = isin_by_symbol.get(symbol)
            if not isin:
                continue
            with_isin += 1
            code = (existing.get(symbol) or {}).get("bse_scrip_code") or master.get(isin)
            if code:
                scrip_resolved += 1
            rows.append({"symbol": symbol, "isin": isin, "bse_scrip_code": code, "as_of": as_of})

        written = await self._store.arun(self._store.upsert_symbol_isin, rows) if rows else 0
        _log.info(
            "isin_map_built",
            symbols=len(wanted), with_isin=with_isin, scrip_resolved=scrip_resolved, written=written,
        )
        return IsinMapResult(
            ok=master_error is None, degraded=degraded or master_error is not None,
            symbols=len(wanted), with_isin=with_isin, scrip_resolved=scrip_resolved,
            rows_written=written, reason=master_error,
        )

    async def _alert(self, reason: str) -> None:
        if self._notify is None:
            return
        msg = CatalogMessage(
            kind=MessageKind.DATA_FRESHNESS_FROZEN,
            title="ISIN map build degraded",
            body=(
                f"symbol_isin build degraded: {reason}. Existing mappings remain; filings_shp skips "
                "symbols without a BSE scrip code (§2.8/E5). Not entry-blocking."
            ),
            severity="warning",
            data={"job_id": "isin_map", "reason": reason},
        )
        try:
            await self._notify(msg)
        except Exception:  # noqa: BLE001 - best-effort alert; a failed send never propagates
            _log.exception("isin_map_notify_failed")
