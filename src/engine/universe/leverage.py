"""Zerodha MIS margins file → per-stock intraday leverage + MIS eligibility (§3.2.4 / §4.4 job 4, A8).

The Zerodha equity-margins file is the authority for which NSE stocks can be traded MIS and at what
intraday leverage (the "MIS-eligible (Zerodha leverage file)" leg of the §3.2.4 universe rule). This
module fetches and parses it; the :class:`~engine.universe.builder.UniverseBuilder` consumes the
resulting :class:`MisLeverageMap`, and the 08:15 instruments job joins it into ``instruments_daily``
(``mis_leverage`` / ``mis_eligible`` columns, §4.3 — wiring seam, not done here).

Best-effort source (E5, convention 11): injected ``httpx.AsyncClient``, [VERIFY]-marked URL, defensive
parsing. On fetch/parse failure the last good file is reused from a runtime JSON cache under ``data/``
("reuse yesterday" — survives restarts) with ``degraded=True`` + an owner alert; with no cache at all
the map is EMPTY, which fails CLOSED: every symbol reads as not-MIS-eligible and the universe rule
excludes it (A8 conservative default). A leverage-file failure can therefore never widen the universe.
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict, Field

from engine.core.clock import IST, Clock
from engine.core.log import get_logger
from engine.notify.catalog import CatalogMessage, MessageKind

_log = get_logger("engine.universe.leverage")

#: Kite Connect public equity-margins endpoint (tradingsymbol + ``mis_multiplier``/``mis_margin``
#: per stock). [VERIFY Phase-1] — endpoint + row shape re-checked at the G1 gate; parsing below is
#: defensive against both the array form and a ``{"data": [...]}`` wrapper.
KITE_MIS_MARGINS_URL = "https://api.kite.trade/margins/equity"

#: A stock is MIS-eligible iff it appears in the margins file with computed leverage strictly > 1×
#: (a 1×/absent entry means no intraday margin product — CNC only). [VERIFY Phase-1] with the live file.
MIS_ELIGIBLE_MIN_LEVERAGE = 1.0

NotifySink = Callable[[CatalogMessage], Awaitable[None]]


class MisLeverageMap(BaseModel):
    """Per-stock MIS leverage snapshot (the §3.2.4 universe-rule input).

    ``leverages`` maps ``tradingsymbol -> leverage multiple`` (e.g. 5.0 = 20% MIS margin). Leverage
    is a ratio, not money — ``float`` is correct (mirrors ``instruments_daily.mis_leverage DOUBLE``).
    ``degraded=True`` means this snapshot was reused from the runtime cache after a fetch failure.
    """

    model_config = ConfigDict(frozen=True)

    as_of: datetime                                   # tz-aware IST fetch time (Clock-stamped)
    degraded: bool = False
    leverages: dict[str, float] = Field(default_factory=dict)

    def leverage(self, symbol: str) -> float | None:
        return self.leverages.get(symbol.upper())

    def is_mis_eligible(self, symbol: str) -> bool:
        """A8 fail-closed: unknown symbol ⇒ not eligible (never "assume leveraged")."""
        lev = self.leverages.get(symbol.upper())
        return lev is not None and lev > MIS_ELIGIBLE_MIN_LEVERAGE


def _row_leverage(row: dict[str, Any]) -> float | None:
    """Leverage multiple from one margins row: prefer ``mis_multiplier``; else 100/``mis_margin``."""
    keys = {str(k).lower(): v for k, v in row.items()}
    mult = keys.get("mis_multiplier")
    try:
        if mult is not None and float(mult) > 0:
            return float(mult)
    except (TypeError, ValueError):
        pass
    margin = keys.get("mis_margin")
    try:
        if margin is not None and float(margin) > 0:
            return 100.0 / float(margin)
    except (TypeError, ValueError):
        pass
    return None


def parse_margins_payload(payload: Any) -> dict[str, float]:
    """Defensively parse the margins JSON (array or ``{"data": [...]}``) → ``symbol -> leverage``.

    Rows without a tradingsymbol or a usable multiplier/margin are skipped, never fatal (E5).
    """
    rows = payload
    if isinstance(payload, dict):
        rows = payload.get("data") or payload.get("margins") or []
    if not isinstance(rows, list):
        return {}
    out: dict[str, float] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        keys = {str(k).lower(): v for k, v in row.items()}
        symbol = str(keys.get("tradingsymbol") or keys.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        lev = _row_leverage(row)
        if lev is not None:
            out[symbol] = lev
    return out


class MisLeverageIngest:
    """Fetch + cache the Zerodha MIS margins file (E5 best-effort; reuse-yesterday on failure).

    Parameters
    ----------
    clock:
        Single source of "now" (§3.2) — stamps ``as_of`` and the cache.
    http:
        Injected ``httpx.AsyncClient`` (convention 11). Owned by the caller; never closed here.
    cache_path:
        Runtime last-good JSON cache (e.g. ``data/universe/mis_margins.json``) — the
        "yesterday's data" reused on failure, surviving restarts.
    notify:
        Optional owner-alert sink (``ops`` wires the Telegram send); degradations alert through it.
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
        self._current: MisLeverageMap | None = None

    async def current(self) -> MisLeverageMap:
        """The latest refreshed map, refreshing once if never refreshed this process (§2.6 catch-up)."""
        if self._current is None:
            return await self.refresh()
        return self._current

    async def refresh(self) -> MisLeverageMap:
        """Fetch + parse the margins file; on ANY failure reuse the cached last-good copy + alert.

        Never raises into the caller's loop (E5). An empty degraded map (no cache either) fails
        CLOSED: no symbol is MIS-eligible until the file is reachable again.
        """
        try:
            resp = await self._http.get(KITE_MIS_MARGINS_URL, timeout=self._timeout)
            resp.raise_for_status()
            leverages = parse_margins_payload(json.loads(resp.content))
            if not leverages:
                raise ValueError("margins payload parsed to zero usable rows")
            now = self._clock.now()
            self._write_cache(now, leverages)
            self._current = MisLeverageMap(as_of=now, degraded=False, leverages=leverages)
            _log.info("mis_margins_refreshed", symbols=len(leverages))
            return self._current
        except Exception as exc:  # noqa: BLE001 - E5: best-effort feed, degrade + alert, never raise
            _log.warning("mis_margins_fetch_failed", error=f"{type(exc).__name__}: {exc}")
            self._current = self._from_cache(reason=str(exc))
            await self._alert(
                reason=f"MIS margins fetch failed ({type(exc).__name__}); "
                + ("reusing last good file" if self._current.leverages else "NO cached file — failing closed"),
            )
            return self._current

    # ------------------------------------------------------------------ cache (reuse-yesterday, E5)
    def _write_cache(self, as_of: datetime, leverages: dict[str, float]) -> None:
        try:
            self._cache_path.parent.mkdir(parents=True, exist_ok=True)
            self._cache_path.write_text(
                json.dumps({"as_of": as_of.isoformat(), "leverages": leverages}), encoding="utf-8"
            )
        except OSError:  # cache write failure is not fatal — next failure just has no fallback
            _log.exception("mis_margins_cache_write_failed", path=str(self._cache_path))

    def _from_cache(self, *, reason: str) -> MisLeverageMap:
        if self._cache_path.exists():
            try:
                raw = json.loads(self._cache_path.read_text(encoding="utf-8"))
                as_of = datetime.fromisoformat(raw["as_of"]).astimezone(IST)
                leverages = {str(k).upper(): float(v) for k, v in dict(raw["leverages"]).items()}
                _log.warning("mis_margins_reusing_cache", as_of=as_of.isoformat(), symbols=len(leverages))
                return MisLeverageMap(as_of=as_of, degraded=True, leverages=leverages)
            except (OSError, ValueError, KeyError, TypeError):
                _log.exception("mis_margins_cache_unreadable", path=str(self._cache_path))
        # No usable cache: empty map ⇒ every symbol not-MIS-eligible (fail closed, A8).
        return MisLeverageMap(as_of=self._clock.now(), degraded=True, leverages={})

    async def _alert(self, *, reason: str) -> None:
        if self._notify is None:
            return
        msg = CatalogMessage(
            # Closest catalog kind for "daily data job not fresh" (the catalog is closed here;
            # a dedicated FEED_DEGRADED kind is an integrator follow-up).
            kind=MessageKind.DATA_FRESHNESS_FROZEN,
            title="MIS leverage file degraded",
            body=f"{reason}. Universe MIS-eligibility fails closed until refreshed (A8/E5).",
            severity="warning",
            data={"job_id": "mis_margins", "reason": reason},
        )
        try:
            await self._notify(msg)
        except Exception:  # noqa: BLE001 - best-effort alert; a failed send never propagates
            _log.exception("mis_margins_notify_failed")
