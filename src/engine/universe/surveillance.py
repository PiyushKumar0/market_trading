"""Surveillance-list ingest — GSM/ASM/T2T/ESM + unsolicited-SMS lists (§3.2.4 / §4.4 job 5, A8).

``SurveillanceIngest.refresh()`` (08:20 daily, §10.1) pulls the exchange surveillance lists that the
§3.2.4 universe rule excludes ("not in GSM/ASM/T2T/ESM") plus the unsolicited-SMS pump-suspect list
(consumed by scanners/gate context, not the universe rule). All sources are best-effort scrapes of
NSE pages/APIs with the plan's ``[likely]`` anti-bot caveat (E5): injected ``httpx.AsyncClient``,
[VERIFY]-marked URLs, defensive parsing.

Failure model (§4.4 job 5, pinned): on a per-source failure, REUSE yesterday's list for that source
(runtime JSON cache under ``data/``, survives restarts) + alert + expose the affected symbols via
``SurveillanceLists.unconfirmed_symbols`` — the Tier-2 gate refuses NEW entries in any symbol whose
surveillance status cannot be confirmed (conservative default; the gate consumes this in Phase 2 —
seam documented on :class:`SurveillanceLists`). A failed source can therefore never *shrink* the
exclusion set: yesterday's flagged symbols stay excluded, and they are additionally unconfirmed.

Held-position migration (A8): "a held position migrated INTO a surveillance list" must alert
immediately. TODO(Phase 2): the positions module does not exist yet — the diff seam is
:meth:`SurveillanceLists.new_entries` (today's flagged minus a previous snapshot's), which the
position-holding caller intersects with held symbols and alerts on.
"""

from __future__ import annotations

import csv
import io
import json
from collections.abc import Awaitable, Callable
from datetime import date
from pathlib import Path
from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict, Field

from engine.core.clock import Clock
from engine.core.log import get_logger
from engine.core.nse_http import nse_get
from engine.notify.catalog import CatalogMessage, MessageKind

_log = get_logger("engine.universe.surveillance")

# --------------------------------------------------------------------------- sources ([VERIFY Phase-1])
# NSE JSON report endpoints ([likely] anti-bot: cookie/UA-gated — best-effort per plan §4.4 job 5).
NSE_GSM_URL = "https://www.nseindia.com/api/reportGSM"                # [VERIFY Phase-1]
NSE_ASM_URL = "https://www.nseindia.com/api/reportASM"                # [VERIFY Phase-1]
NSE_ESM_URL = "https://www.nseindia.com/api/reportESM"                # [VERIFY Phase-1]
#: T2T from the listed-securities master (archives host, less bot-gated): SERIES BE/BZ = trade-to-trade.
NSE_T2T_URL = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"   # [VERIFY Phase-1]
#: Unsolicited-SMS watch list (pump-promo suspects). [VERIFY Phase-1] — page/API moves; anti-bot [likely].
NSE_SMS_URL = "https://www.nseindia.com/api/unsolicited-sms"          # [VERIFY Phase-1]

#: T2T series codes in the securities master (BE = rolling T2T, BZ = T2T + other restrictions).
_T2T_SERIES = frozenset({"BE", "BZ"})

NotifySink = Callable[[CatalogMessage], Awaitable[None]]

#: Universe-rule sources (§3.2.4 pins these four) in canonical order; ``sms`` is carried separately.
SURVEILLANCE_KEYS: tuple[str, ...] = ("gsm", "asm", "t2t", "esm")


class SurveillanceLists(BaseModel):
    """One refresh's surveillance snapshot (A8) — the §3.2.4 universe-rule exclusion input.

    ``unconfirmed_symbols`` is the Phase-2 gate seam: symbols whose CURRENT status could not be
    confirmed because their source failed and yesterday's list was reused — the gate refuses NEW
    entries there (conservative default, §4.4 job 5). ``degraded_sources`` names the failed sources
    so the gate can choose an even broader stance if everything is down.
    """

    model_config = ConfigDict(frozen=True)

    as_of: date
    gsm: frozenset[str] = Field(default_factory=frozenset)
    asm: frozenset[str] = Field(default_factory=frozenset)
    t2t: frozenset[str] = Field(default_factory=frozenset)
    esm: frozenset[str] = Field(default_factory=frozenset)
    sms: frozenset[str] = Field(default_factory=frozenset)
    degraded_sources: tuple[str, ...] = ()
    unconfirmed_symbols: frozenset[str] = Field(default_factory=frozenset)

    def flagged(self) -> frozenset[str]:
        """Union of the four universe-rule lists (GSM/ASM/T2T/ESM — not SMS, §3.2.4)."""
        return frozenset(self.gsm | self.asm | self.t2t | self.esm)

    def reasons_for(self, symbol: str) -> list[str]:
        """Per-list exclusion reasons for ``symbol`` (auditable ``universe_daily`` strings, §4.3)."""
        sym = symbol.upper()
        return [f"surveillance_{key}" for key in SURVEILLANCE_KEYS if sym in getattr(self, key)]

    def new_entries(self, previous: SurveillanceLists) -> frozenset[str]:
        """Symbols newly flagged vs ``previous`` — the A8 held-position-migration diff input
        (TODO(Phase 2): intersect with held symbols and alert immediately)."""
        return frozenset(self.flagged() - previous.flagged())


def _symbols_from_json(payload: Any) -> set[str]:
    """Recursively collect ``"symbol"`` values from an arbitrarily nested NSE JSON payload.

    The NSE report endpoints wrap rows differently per list (``{"data": [...]}``,
    ``{"longterm": {"data": [...]}, "shortterm": {...}}``, plain arrays…) — walking the whole tree
    for ``symbol`` keys is robust to all of them (parse-defensively, E5).
    """
    found: set[str] = set()

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                if str(key).lower() == "symbol" and isinstance(value, str) and value.strip():
                    found.add(value.strip().upper())
                else:
                    walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(payload)
    return found


def _t2t_from_csv(text: str) -> set[str]:
    """T2T symbols = securities-master rows whose SERIES is BE/BZ (defensive column lookup)."""
    reader = csv.DictReader(io.StringIO(text))
    norm = {(name or "").strip().lower(): name for name in (reader.fieldnames or [])}
    sym_col, series_col = norm.get("symbol"), norm.get("series")
    if sym_col is None or series_col is None:
        raise ValueError(f"EQUITY_L.csv missing SYMBOL/SERIES columns: {reader.fieldnames}")
    out: set[str] = set()
    for row in reader:
        series = (row.get(series_col) or "").strip().upper()
        symbol = (row.get(sym_col) or "").strip().upper()
        if symbol and series in _T2T_SERIES:
            out.add(symbol)
    return out


class SurveillanceIngest:
    """§3.2.4 ``SurveillanceIngest`` — best-effort surveillance-list refresh with reuse-yesterday (E5).

    Parameters
    ----------
    clock:
        Single source of "now" (§3.2) — stamps ``as_of`` and the cache entries.
    http:
        Injected ``httpx.AsyncClient`` (convention 11). Owned by the caller; never closed here.
    cache_path:
        Runtime last-good JSON cache (e.g. ``data/universe/surveillance.json``), one entry per
        source — the "yesterday's lists" reused per-source on failure, surviving restarts.
    notify:
        Optional owner-alert sink; any degraded source alerts through it (critical — A8 safety).
    """

    def __init__(
        self,
        clock: Clock,
        http: httpx.AsyncClient,
        cache_path: str | Path,
        *,
        notify: NotifySink | None = None,
        request_timeout_s: float = 15.0,
    ) -> None:
        self._clock = clock
        self._http = http
        self._cache_path = Path(cache_path)
        self._notify = notify
        self._timeout = float(request_timeout_s)
        self._current: SurveillanceLists | None = None

    async def current(self) -> SurveillanceLists:
        """The latest refreshed lists, refreshing once if never refreshed this process (§2.6)."""
        if self._current is None:
            return await self.refresh()
        return self._current

    async def refresh(self) -> SurveillanceLists:
        """Fetch every source under its own guard; failed sources reuse yesterday + alert (§4.4 job 5).

        Never raises into the caller's loop (E5). Returns (and retains for :meth:`current`) the
        snapshot with per-source results, degraded-source names, and the unconfirmed-symbol set.
        """
        today = self._clock.today()
        cache = self._load_cache()
        results: dict[str, frozenset[str]] = {}
        degraded: list[str] = []
        unconfirmed: set[str] = set()

        for key, coro in (
            ("gsm", self._fetch_json_symbols(NSE_GSM_URL)),
            ("asm", self._fetch_json_symbols(NSE_ASM_URL)),
            ("t2t", self._fetch_t2t()),
            ("esm", self._fetch_json_symbols(NSE_ESM_URL)),
            ("sms", self._fetch_json_symbols(NSE_SMS_URL)),
        ):
            try:
                symbols = await coro
                results[key] = frozenset(symbols)
                cache[key] = {"as_of": today.isoformat(), "symbols": sorted(symbols)}
            except Exception as exc:  # noqa: BLE001 - E5: per-source degrade, never raise
                previous = frozenset(cache.get(key, {}).get("symbols") or [])
                _log.warning(
                    "surveillance_source_failed",
                    source=key,
                    error=f"{type(exc).__name__}: {exc}",
                    reused=len(previous),
                )
                results[key] = previous
                degraded.append(key)
                unconfirmed |= previous

        self._save_cache(cache)
        lists = SurveillanceLists(
            as_of=today,
            gsm=results["gsm"],
            asm=results["asm"],
            t2t=results["t2t"],
            esm=results["esm"],
            sms=results["sms"],
            degraded_sources=tuple(degraded),
            unconfirmed_symbols=frozenset(unconfirmed),
        )
        self._current = lists
        _log.info(
            "surveillance_refreshed",
            gsm=len(lists.gsm), asm=len(lists.asm), t2t=len(lists.t2t), esm=len(lists.esm),
            sms=len(lists.sms), degraded=degraded,
        )
        if degraded:
            await self._alert(degraded=degraded, unconfirmed=len(unconfirmed))
        return lists

    # ------------------------------------------------------------------ fetchers
    async def _fetch_json_symbols(self, url: str) -> set[str]:
        # www.nseindia.com API host: cookie priming + browser headers + bounded retry (A3, E5).
        resp = await nse_get(self._http, url, timeout=self._timeout)
        return _symbols_from_json(json.loads(resp.content))

    async def _fetch_t2t(self) -> set[str]:
        # nsearchives host: browser headers + retry, no priming (no cookie gate on archives, A4).
        resp = await nse_get(self._http, NSE_T2T_URL, timeout=self._timeout)
        return _t2t_from_csv(resp.text)

    # ------------------------------------------------------------------ cache (reuse-yesterday, E5)
    def _load_cache(self) -> dict[str, dict[str, Any]]:
        if not self._cache_path.exists():
            return {}
        try:
            raw = json.loads(self._cache_path.read_text(encoding="utf-8"))
            return raw if isinstance(raw, dict) else {}
        except (OSError, ValueError):
            _log.exception("surveillance_cache_unreadable", path=str(self._cache_path))
            return {}

    def _save_cache(self, cache: dict[str, dict[str, Any]]) -> None:
        try:
            self._cache_path.parent.mkdir(parents=True, exist_ok=True)
            self._cache_path.write_text(json.dumps(cache), encoding="utf-8")
        except OSError:  # not fatal — next failure just has an older fallback
            _log.exception("surveillance_cache_write_failed", path=str(self._cache_path))

    async def _alert(self, *, degraded: list[str], unconfirmed: int) -> None:
        if self._notify is None:
            return
        msg = CatalogMessage(
            # Closest catalog kind for "safety-critical daily list not fresh" (§2.6 step 5 names
            # surveillance in the safety set); the freeze itself is the gate/catch-up's decision.
            kind=MessageKind.DATA_FRESHNESS_FROZEN,
            title="Surveillance lists degraded — reusing yesterday",
            body=(
                f"Source(s) failed: {', '.join(degraded)}. Yesterday's lists reused; "
                f"{unconfirmed} symbol(s) unconfirmed — gate refuses NEW entries there (A8/E5)."
            ),
            severity="critical",
            data={"job_id": "surveillance", "degraded_sources": degraded, "unconfirmed": unconfirmed},
        )
        try:
            await self._notify(msg)
        except Exception:  # noqa: BLE001 - best-effort alert; a failed send never propagates
            _log.exception("surveillance_notify_failed")
