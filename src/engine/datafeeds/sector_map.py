"""Weekly sector map + theme map refresh (§4.4 job 13, Sunday — R1, E5 best-effort).

``sector_map`` is the DETERMINISTIC source for the §7.1 ``per_sector_exposure`` gate input and the
§2.7 sector fan-out — the Kite instruments dump carries no sector field, so sector membership comes
from the NSE sectoral-index constituent lists (Bank, IT, Pharma, FMCG, Auto, Metal, Energy, Realty,
PSU Bank, Financial Services). A symbol in no list maps to sector ``UNCLASSIFIED`` (capped at 1 open
position by the gate [conservative], §4.4 job 13).

Classification is FIRST-WINS over the pinned :data:`SECTOR_SOURCES` order (most-specific first:
PSU Bank ⊂ Bank ⊂ Financial Services) so a symbol in overlapping indices lands in exactly one
sector, deterministically — ``sector_map`` is one row per (as_of, symbol).

Failure model (E5): each index list is a best-effort scrape under its OWN guard with a frozen
fallback copy (runtime JSON cache under ``data/``, survives restarts — "reuse yesterday"). A failed
source reuses its cached list + alert; if NOTHING (fresh or cached) classifies, no new snapshot is
written at all so ``get_sector_map`` keeps serving the previous weekly snapshot. Never raises into
the scheduler.

The same job refreshes ``theme_map`` from the ``config/themes.yaml`` seed — rows are written
VERBATIM (owner-approved additions only: the weekly researcher SUGGESTS, the owner edits the YAML,
§5.5/§6.3 — nothing is ever auto-added here).

A small owner-curated supplement, ``config/sector_overrides.yaml`` (same §5.5/§6.3 convention as
``themes.yaml``), folds names the scraped indices structurally exclude (e.g. AMCs are tagged
Industry=Financial Services by NSE but are not constituents of the real Nifty Financial Services
index) into their natural sector. Applied AFTER the index-scrape classification as a pure
supplement — ``mapping.setdefault``, so a symbol already classified by a real index is never
touched — on top of BOTH a fresh scrape and a reused frozen-cache fallback. Same E5 guarantee as
everything else here: a malformed/missing override file degrades to "no overrides this run" and
alerts, never breaks sector classification. Sector NAMES in the file are also validated against
:data:`SECTOR_SOURCES` at merge time (``_load_overrides``) — an unknown name is dropped (its
symbols stay UNCLASSIFIED) and alerted rather than minting a phantom sector bucket the exposure
gate never groups on.
"""

from __future__ import annotations

import csv
import io
import json
from collections.abc import Awaitable, Callable, Iterable
from datetime import date
from pathlib import Path
from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict

from engine.core.clock import Clock
from engine.core.config import config_dir, load_yaml
from engine.core.log import get_logger
from engine.marketdata.store import MarketStore
from engine.notify.catalog import CatalogMessage, MessageKind

_log = get_logger("engine.datafeeds.sector_map")

#: Sector for symbols in no sectoral index — the gate caps it at 1 open position (§4.4 job 13).
UNCLASSIFIED = "UNCLASSIFIED"

#: NSE sectoral-index constituent CSVs (archives host — same format as the NIFTY200 list:
#: ``Company Name,Industry,Symbol,Series,ISIN Code``). All [VERIFY Phase-1] — NSE moves these.
#: ORDER IS LOAD-BEARING: classification is first-wins, most-specific index first (PSU Bank before
#: Bank before Financial Services), so overlapping memberships resolve deterministically.
SECTOR_SOURCES: tuple[tuple[str, str], ...] = (
    ("PSU_BANK", "https://archives.nseindia.com/content/indices/ind_niftypsubanklist.csv"),
    ("BANK", "https://archives.nseindia.com/content/indices/ind_niftybanklist.csv"),
    ("FINANCIAL_SERVICES", "https://archives.nseindia.com/content/indices/ind_niftyfinancelist.csv"),
    ("IT", "https://archives.nseindia.com/content/indices/ind_niftyitlist.csv"),
    ("PHARMA", "https://archives.nseindia.com/content/indices/ind_niftypharmalist.csv"),
    ("FMCG", "https://archives.nseindia.com/content/indices/ind_niftyfmcglist.csv"),
    ("AUTO", "https://archives.nseindia.com/content/indices/ind_niftyautolist.csv"),
    ("METAL", "https://archives.nseindia.com/content/indices/ind_niftymetallist.csv"),
    ("ENERGY", "https://archives.nseindia.com/content/indices/ind_niftyenergylist.csv"),
    ("REALTY", "https://archives.nseindia.com/content/indices/ind_niftyrealtylist.csv"),
)

_NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0 Safari/537.36",
    "Accept": "text/csv, text/plain, */*",
    "Referer": "https://www.nseindia.com/",
}

NotifySink = Callable[[CatalogMessage], Awaitable[None]]


class SectorMapResult(BaseModel):
    """One run's outcome (never an exception, E5). ``ok`` = a sector snapshot was written;
    ``themes_ok`` = the theme_map seed refresh succeeded (independent of the sector part)."""

    model_config = ConfigDict(frozen=True)

    as_of: date
    ok: bool
    themes_ok: bool = False
    degraded_sources: tuple[str, ...] = ()
    rows_written: int = 0
    classified: int = 0
    unclassified: int = 0
    themes_written: int = 0
    reason: str | None = None


def parse_constituents_csv(text: str) -> list[str]:
    """Symbols from an NSE index-constituents CSV (``Company Name,Industry,Symbol,Series,ISIN``).

    Defensive (E5), same conventions as the NIFTY200 parser in ``engine.universe.builder`` (kept
    local — no universe→datafeeds import edge): ``#`` comment lines skipped, ``Symbol`` column
    located case-insensitively, a ``Series`` column (if present) filters to EQ. Order-preserving,
    de-duplicated, uppercased.
    """
    lines = [ln for ln in text.splitlines() if ln.strip() and not ln.lstrip().startswith("#")]
    reader = csv.DictReader(io.StringIO("\n".join(lines)))
    norm = {(name or "").strip().lower(): name for name in (reader.fieldnames or [])}
    sym_col = norm.get("symbol")
    if sym_col is None:
        raise ValueError(f"no Symbol column in constituents CSV: {reader.fieldnames}")
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


def load_theme_seed(path: str | Path) -> list[dict[str, Any]]:
    """``config/themes.yaml`` → ``theme_map`` row dicts, VERBATIM (owner-approved content only).

    Shape: ``themes: {name: {keywords: [...], symbols: [...]}}``. Nothing is inferred or added —
    the symbols lists stay exactly as the owner approved them (§5.5 platform-suggests-owner-sets).
    """
    raw = load_yaml(path)
    themes = raw.get("themes") or {}
    if not isinstance(themes, dict):
        raise ValueError("themes.yaml: 'themes' must be a mapping")
    rows: list[dict[str, Any]] = []
    for name, spec in themes.items():
        spec = spec if isinstance(spec, dict) else {}
        rows.append(
            {
                "theme": str(name),
                "keywords": [str(k) for k in (spec.get("keywords") or [])],
                "symbols": [str(s) for s in (spec.get("symbols") or [])],
            }
        )
    return rows


def load_sector_overrides(path: str | Path) -> dict[str, str]:
    """``config/sector_overrides.yaml`` → ``{symbol: sector}``, owner-curated supplement.

    Shape: ``overrides: {sector_name: [symbol, ...]}``. Same §5.5/§6.3 owner-approval convention
    as :func:`load_theme_seed` (platform suggests, owner edits the YAML). Flattened to one sector
    per symbol here for the caller's merge; if a symbol is listed under two sectors in the file
    itself, the later one wins (dict overwrite) — the file is small and owner-curated, so this is
    a YAML-authoring mistake, not a runtime concern.

    Raises (never caught here — E5 degrade-and-alert is the caller's job, matching
    ``_refresh_themes``'s handling of :func:`load_theme_seed`) on a missing file or malformed
    schema so the caller can tell "no overrides configured / broken" apart from "empty file".
    """
    raw = load_yaml(path)
    overrides = raw.get("overrides") or {}
    if not isinstance(overrides, dict):
        raise ValueError("sector_overrides.yaml: 'overrides' must be a mapping")
    out: dict[str, str] = {}
    for sector, symbols in overrides.items():
        sector_name = str(sector).strip().upper()
        if not sector_name:
            continue
        if not isinstance(symbols, list):
            raise ValueError(f"sector_overrides.yaml: '{sector}' symbols must be a list")
        for sym in symbols:
            symbol = str(sym).strip().upper()
            if symbol:
                out[symbol] = sector_name
    return out


class SectorMapJob:
    """§4.4 job 13 — weekly ``sector_map`` snapshot + ``theme_map`` seed refresh (R1, E5).

    Parameters
    ----------
    store:
        Single-writer :class:`MarketStore` (convention 12); writes are executor-offloaded.
    clock:
        Single source of "now" (§3.2) — stamps ``theme_map.updated_at`` and the cache entries.
    http:
        Injected ``httpx.AsyncClient`` (convention 11). Owned by the caller; never closed here.
    cache_path:
        Runtime last-good JSON cache (e.g. ``data/datafeeds/sector_lists.json``), one entry per
        sector — the frozen fallback copy reused per-source on failure, surviving restarts.
    themes_path:
        The theme seed YAML; defaults to ``config/themes.yaml`` under the configured config dir.
    overrides_path:
        The owner-curated sector-override YAML; defaults to ``config/sector_overrides.yaml``
        under the configured config dir.
    notify:
        Optional owner-alert sink; degraded sources / a skipped snapshot alert through it.
    """

    def __init__(
        self,
        store: MarketStore,
        clock: Clock,
        http: httpx.AsyncClient,
        cache_path: str | Path,
        *,
        themes_path: str | Path | None = None,
        overrides_path: str | Path | None = None,
        notify: NotifySink | None = None,
        request_timeout_s: float = 20.0,
    ) -> None:
        self._store = store
        self._clock = clock
        self._http = http
        self._cache_path = Path(cache_path)
        self._themes_path = Path(themes_path) if themes_path is not None else config_dir() / "themes.yaml"
        self._overrides_path = (
            Path(overrides_path) if overrides_path is not None else config_dir() / "sector_overrides.yaml"
        )
        self._notify = notify
        self._timeout = float(request_timeout_s)
        #: Per-``d`` (as_of) alert dedup (2026-08-13, mirrors bhavcopy): guards all THREE alert sites
        #: below (job-failed, no-data, degraded-frozen-copies) — the last of which can fire on an
        #: otherwise ``ok=True`` run, so the dedup is keyed at each ``_alert`` call site, not on ``ok``.
        #: The theme-seed alert in ``_refresh_themes`` has no ``d`` to key on and is NOT deduped here
        #: (see that method — it also never drives ``ok``/the watermark, only ``themes_ok``).
        self._alerted: set[date] = set()

    async def run(self, d: date, *, universe_symbols: Iterable[str] | None = None) -> SectorMapResult:
        """Build + persist the ``as_of=d`` sector snapshot and refresh ``theme_map``.

        ``universe_symbols`` (typically today's NIFTY200/watchlist) get explicit ``UNCLASSIFIED``
        rows when no index claims them, so the snapshot is total over the tradeable set. Idempotent
        run-latest (§2.6): the (as_of, symbol) upsert makes a re-run harmless. Never raises (E5).
        """
        try:
            return await self._run(d, universe_symbols)
        except Exception as exc:  # noqa: BLE001 - E5: degrade + alert, never raise into the scheduler
            reason = f"{type(exc).__name__}: {exc}"
            _log.exception("sector_map_job_failed", d=d.isoformat())
            if d not in self._alerted:  # dedup: don't storm on every retry of a still-failing day
                await self._alert(
                    title="Sector-map job failed",
                    body=f"Weekly sector_map/theme_map refresh failed: {reason}. Previous snapshot "
                    "remains the latest (per_sector_exposure keeps last week's map, R1/E5).",
                    severity="critical",
                    data={"job_id": "sector_map", "d": d.isoformat(), "reason": reason},
                )
                self._alerted.add(d)
            return SectorMapResult(as_of=d, ok=False, reason=reason)

    # ------------------------------------------------------------------ core
    async def _run(self, d: date, universe_symbols: Iterable[str] | None) -> SectorMapResult:
        cache = self._load_cache()
        degraded: list[str] = []
        mapping: dict[str, str] = {}
        for sector, url in SECTOR_SOURCES:
            try:
                resp = await self._http.get(url, headers=_NSE_HEADERS, timeout=self._timeout)
                resp.raise_for_status()
                symbols = parse_constituents_csv(resp.text)
                if not symbols:
                    raise ValueError("constituents CSV parsed to zero symbols")
                cache[sector] = {"as_of": d.isoformat(), "symbols": symbols}
            except Exception as exc:  # noqa: BLE001 - E5: per-source degrade, reuse frozen copy
                symbols = [str(s) for s in (cache.get(sector, {}).get("symbols") or [])]
                _log.warning(
                    "sector_source_failed",
                    sector=sector,
                    error=f"{type(exc).__name__}: {exc}",
                    reused=len(symbols),
                )
                degraded.append(sector)
            for symbol in symbols:
                mapping.setdefault(symbol, sector)   # first-wins: SECTOR_SOURCES order is pinned
        self._save_cache(cache)

        themes_ok, themes_written = await self._refresh_themes()

        if not mapping:
            # Nothing classifies at all (every source down AND no frozen copy): writing a snapshot
            # of only-UNCLASSIFIED rows would clobber the previous good map — keep it instead.
            if d not in self._alerted:  # dedup: don't storm on every retry of a still-failing day
                await self._alert(
                    title="Sector map has NO data — snapshot skipped",
                    body="Every sectoral-index source failed and no frozen fallback exists. The "
                    "previous sector_map snapshot remains the latest (R1/E5).",
                    severity="critical",
                    data={"job_id": "sector_map", "d": d.isoformat(), "degraded_sources": degraded},
                )
                self._alerted.add(d)
            return SectorMapResult(
                as_of=d, ok=False, themes_ok=themes_ok, degraded_sources=tuple(degraded),
                themes_written=themes_written, reason="no sector data (all sources failed, no cache)",
            )

        # Owner-curated supplement (config/sector_overrides.yaml) — see the module docstring for the
        # placement, the setdefault semantics and the E5 degrade rule. Entries are validated against
        # the known-good SECTOR_SOURCES sector names here (not in load_sector_overrides, which only
        # validates YAML shape) — an unknown name must never mint a phantom sector bucket that
        # per_sector_exposure never groups on (see _load_overrides).
        overrides = await self._load_overrides(valid_sectors=frozenset(s for s, _ in SECTOR_SOURCES))
        for symbol, sector in overrides.items():
            mapping.setdefault(symbol, sector)

        extra = sorted(
            {str(s).strip().upper() for s in (universe_symbols or []) if str(s).strip()} - set(mapping)
        )
        rows = [{"symbol": s, "sector": sec} for s, sec in sorted(mapping.items())]
        rows += [{"symbol": s, "sector": UNCLASSIFIED} for s in extra]
        written = await self._store.arun(self._store.upsert_sector_map, d, rows)

        if degraded:
            if d not in self._alerted:  # dedup: don't storm on every retry of a still-degraded day
                await self._alert(
                    title="Sector map degraded — frozen copies reused",
                    body=f"Sectoral-index source(s) failed: {', '.join(degraded)}. Cached constituent "
                    "lists reused where available; membership may be stale (E5).",
                    severity="warning",
                    data={"job_id": "sector_map", "d": d.isoformat(), "degraded_sources": degraded},
                )
                self._alerted.add(d)
        else:
            self._alerted.discard(d)  # a fully clean run for d re-arms the alert for a later streak
        _log.info(
            "sector_map_written",
            as_of=d.isoformat(),
            classified=len(mapping),
            unclassified=len(extra),
            degraded=degraded,
            themes=themes_written,
            overrides=len(overrides),
        )
        return SectorMapResult(
            as_of=d,
            ok=True,
            themes_ok=themes_ok,
            degraded_sources=tuple(degraded),
            rows_written=written,
            classified=len(mapping),
            unclassified=len(extra),
            themes_written=themes_written,
        )

    # ------------------------------------------------------------------ theme_map (seed, verbatim)
    async def _refresh_themes(self) -> tuple[bool, int]:
        try:
            rows = load_theme_seed(self._themes_path)
        except Exception as exc:  # noqa: BLE001 - E5: theme seed failure never blocks the sector part
            reason = f"{type(exc).__name__}: {exc}"
            _log.warning("theme_seed_unreadable", path=str(self._themes_path), error=reason)
            await self._alert(
                title="Theme-map seed unreadable",
                body=f"config/themes.yaml could not be loaded ({reason}). Existing theme_map rows "
                "remain in force (E5).",
                severity="warning",
                data={"job_id": "sector_map", "reason": reason},
            )
            return False, 0
        now = self._clock.now()
        stamped = [{**row, "updated_at": now} for row in rows]
        written = await self._store.arun(self._store.upsert_theme_map, stamped)
        return True, written

    # ------------------------------------------------------------------ sector overrides (owner supplement)
    async def _load_overrides(self, *, valid_sectors: frozenset[str]) -> dict[str, str]:
        """``config/sector_overrides.yaml`` supplement — same E5 shape as ``_refresh_themes``: a
        missing or malformed file degrades to "no overrides this run" (never blocks the
        index-scrape classification in ``_run``), alerted but non-fatal.

        ``load_sector_overrides`` validates YAML shape only, not sector names — an entry naming a
        sector outside ``valid_sectors`` (a typo, e.g. 'FINANCIALSERVICES') is dropped here rather
        than merged: left in, it would create a phantom one-symbol sector bucket that
        ``per_sector_exposure`` never groups on, so the symbol's notional escapes the cap silently
        instead of landing in UNCLASSIFIED (the conservative fallback). Valid entries in the same
        file are unaffected.
        """
        try:
            overrides = load_sector_overrides(self._overrides_path)
        except Exception as exc:  # noqa: BLE001 - E5: override failure never blocks classification
            reason = f"{type(exc).__name__}: {exc}"
            _log.warning("sector_overrides_unreadable", path=str(self._overrides_path), error=reason)
            await self._alert(
                title="Sector overrides file unreadable",
                body=f"config/sector_overrides.yaml could not be loaded ({reason}). Proceeding with "
                "index-scrape classification only this run — overrides are never load-bearing (E5).",
                severity="warning",
                data={"job_id": "sector_map", "reason": reason},
            )
            return {}

        unknown = sorted({sector for sector in overrides.values() if sector not in valid_sectors})
        if not unknown:
            return overrides
        _log.warning("sector_overrides_unknown_sector", sectors=unknown)
        await self._alert(
            title="Sector overrides reference unknown sector name(s)",
            body=f"config/sector_overrides.yaml names sector(s) not in SECTOR_SOURCES: "
            f"{', '.join(unknown)}. Those entries are skipped this run — affected symbols stay "
            "UNCLASSIFIED (conservative) rather than form an uncapped phantom bucket (E5). Other "
            "entries in the file still apply.",
            severity="warning",
            data={"job_id": "sector_map", "unknown_sectors": unknown},
        )
        return {sym: sector for sym, sector in overrides.items() if sector in valid_sectors}

    # ------------------------------------------------------------------ cache (frozen fallback, E5)
    def _load_cache(self) -> dict[str, dict[str, Any]]:
        if not self._cache_path.exists():
            return {}
        try:
            raw = json.loads(self._cache_path.read_text(encoding="utf-8"))
            return raw if isinstance(raw, dict) else {}
        except (OSError, ValueError):
            _log.exception("sector_cache_unreadable", path=str(self._cache_path))
            return {}

    def _save_cache(self, cache: dict[str, dict[str, Any]]) -> None:
        try:
            self._cache_path.parent.mkdir(parents=True, exist_ok=True)
            self._cache_path.write_text(json.dumps(cache), encoding="utf-8")
        except OSError:  # not fatal — next failure just has an older frozen copy
            _log.exception("sector_cache_write_failed", path=str(self._cache_path))

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
            _log.exception("sector_map_notify_failed")
