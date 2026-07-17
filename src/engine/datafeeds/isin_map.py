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
3. **BSE scrip code via ``PeerSmartSearch/w``.** Resolves an ISIN to its BSE scrip code EXACTLY
   (probe-verified), returning HTML ``<li>`` rows (regex-parsed, not JSON) — the ``filings_shp`` job's
   required lookup. Resolved once and cached; only symbols still missing a code are queried, ≥1.5 s
   apart (§2.8). BSE 404s masquerade as 200 + ``error_Bse.html`` ⇒ funnel through
   :func:`engine.core.bse_http.bse_get` (JSON-parse health check).

Not a scheduled job (the backfill seed / an owner refresh invokes it). Defensive throughout; a failed
network layer degrades to fewer mappings, never raises.
"""

from __future__ import annotations

import asyncio
import csv
import io
import json
import re
from collections.abc import Awaitable, Callable
from datetime import date
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

#: BSE ISIN→scrip-code resolver (returns HTML ``<li>`` rows, not JSON). [VERIFY Phase-1].
BSE_PEER_SEARCH_URL = "https://api.bseindia.com/BseIndiaAPI/api/PeerSmartSearch/w?Type=SS&text={text}"

#: BSE per-request spacing (§2.8: ≥1.5 s observed safe). Module indirection so tests skip the wait.
_BSE_SPACING_S = 1.5

#: 12-char ISIN token (2-letter country + 9 alphanumeric + 1 check digit), e.g. INE002A01018.
_ISIN_RE = re.compile(r"\b([A-Z]{2}[A-Z0-9]{9}\d)\b")

#: BSE PeerSmartSearch ``<li>`` — the scrip code + long name are the first two ``liclick`` args.
_LICLICK_RE = re.compile(r"liclick\('(\d+)','([^']*)'\)")
_SPAN_RE = re.compile(r"<span>(.*?)</span>", re.DOTALL)

#: Injectable sleep so tests skip the ≥1.5 s BSE spacing (monkeypatch this attribute).
_sleep = asyncio.sleep

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


def parse_peersmartsearch(html: str) -> list[dict[str, str]]:
    """BSE PeerSmartSearch HTML → ``[{scrip_code, symbol, isin, name}]`` (regex, probe-verified shape).

    Each ``<li>`` carries ``liclick('<scrip_code>','<long name>')`` and a ``<span>`` of
    ``<symbol> <ISIN> <scrip_code>`` (``&nbsp;``-separated, some tokens ``<strong>``-wrapped when they
    matched the query). Tag-stripped, the span's first token is the symbol and its ISIN-shaped token
    the ISIN.
    """
    out: list[dict[str, str]] = []
    for chunk in re.split(r"(?=<li)", html):
        m_code = _LICLICK_RE.search(chunk)
        if m_code is None:
            continue
        scrip_code, name = m_code.group(1), m_code.group(2).strip()
        m_span = _SPAN_RE.search(chunk)
        span_text = re.sub(r"<[^>]+>", "", m_span.group(1)) if m_span else ""
        span_text = span_text.replace("&nbsp;", " ")
        tokens = [t for t in span_text.split() if t.strip()]
        symbol = tokens[0].upper() if tokens else ""
        isin_match = _ISIN_RE.search(span_text)
        isin = isin_match.group(1) if isin_match else ""
        out.append({"scrip_code": scrip_code, "symbol": symbol, "isin": isin, "name": name})
    return out


def scrip_for_isin(entries: list[dict[str, str]], isin: str) -> str | None:
    """The scrip code of the entry whose ISIN matches ``isin`` exactly (the resolver's answer, §2.8)."""
    for entry in entries:
        if entry.get("isin") == isin and entry.get("scrip_code"):
            return entry["scrip_code"]
    return None


def load_constituents_isin(settings: Settings) -> dict[str, str]:
    """Read the cached NIFTY-constituents CSV (download-cache → committed seed ladder) for ISINs.

    Mirrors ``UniverseBuilder``'s cache/seed paths (``builder.py`` untouched); returns ``{}`` if
    neither is readable (the caller then relies on the announcements fallback)."""
    cache = settings.resolved_data_dir() / "universe" / "nifty200_cached.csv"
    seed_rel = Path(settings.universe.nifty200_seed_path)
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
    """§2.8 utility — build/refresh ``symbol_isin`` (CSV ISIN + announcements fallback + BSE scrip)."""

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

    async def run(self, symbols: list[str], *, resolve_scrip: bool = True) -> IsinMapResult:
        """Build ``symbol_isin`` for ``symbols`` (uppercased). ``resolve_scrip`` gates the BSE
        PeerSmartSearch leg (skipped ⇒ ISIN-only rows, scrip codes filled on a later run). Never
        raises (E5)."""
        try:
            return await self._run(symbols, resolve_scrip=resolve_scrip)
        except Exception as exc:  # noqa: BLE001 - E5: degrade + alert, never raise
            reason = f"{type(exc).__name__}: {exc}"
            _log.exception("isin_map_build_failed")
            await self._alert(reason)
            return IsinMapResult(ok=False, degraded=True, reason=reason)

    async def _run(self, symbols: list[str], *, resolve_scrip: bool) -> IsinMapResult:
        wanted = [s.strip().upper() for s in symbols if s.strip()]
        isin_by_symbol = load_constituents_isin(self._settings)
        degraded = not isin_by_symbol

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

        # Reuse already-resolved BSE scrip codes so a refresh never re-queries a mapped symbol (§2.8).
        existing = await self._store.asymbol_isin_map()
        as_of = self._clock.today()
        rows: list[dict[str, Any]] = []
        with_isin = scrip_resolved = 0
        first_bse = True
        for symbol in wanted:
            isin = isin_by_symbol.get(symbol)
            if not isin:
                continue
            with_isin += 1
            code = (existing.get(symbol) or {}).get("bse_scrip_code")
            if resolve_scrip and not code:
                code = await self._resolve_scrip(symbol, isin, first=first_bse)
                first_bse = False
            if code:
                scrip_resolved += 1
            rows.append(
                {"symbol": symbol, "isin": isin, "bse_scrip_code": code, "as_of": as_of}
            )

        written = await self._store.arun(self._store.upsert_symbol_isin, rows) if rows else 0
        _log.info(
            "isin_map_built",
            symbols=len(wanted), with_isin=with_isin, scrip_resolved=scrip_resolved, written=written,
        )
        return IsinMapResult(
            ok=True, degraded=degraded, symbols=len(wanted), with_isin=with_isin,
            scrip_resolved=scrip_resolved, rows_written=written,
        )

    async def _resolve_scrip(self, symbol: str, isin: str, *, first: bool) -> str | None:
        """One PeerSmartSearch lookup, ≥1.5 s after the previous BSE call (§2.8). Degrades to None.

        PeerSmartSearch is served ``application/json`` but the body is the ``<li>`` HTML wrapped as a
        JSON STRING (probe-verified: ``r.json()`` returns a str) — so ``bse_get``'s JSON-parse health
        check passes (the error_Bse.html page is NOT valid JSON) and we ``json.loads`` to unwrap the
        HTML before the regex parse."""
        try:
            if not first:
                await _sleep(_BSE_SPACING_S)
            resp = await bse_get(
                self._http, BSE_PEER_SEARCH_URL.format(text=isin), timeout=self._timeout
            )
            payload = json.loads(resp.content)
            html = payload if isinstance(payload, str) else resp.text
            return scrip_for_isin(parse_peersmartsearch(html), isin)
        except Exception as exc:  # noqa: BLE001 - E5: a failed resolve leaves the code NULL, never raises
            _log.warning("isin_map_scrip_failed", symbol=symbol, isin=isin, error=f"{type(exc).__name__}: {exc}")
            return None

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
