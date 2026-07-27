"""NSE bulk/block deals → ``flagged_instrument_days`` (§4.4 job 9, 18:45 — E5 best-effort).

A day with a bulk or block deal in a symbol distorts its volume profile: scanners must suppress
volume-breakout signals on flagged (symbol, day)s ("fake-volume-breakout suppression", §4 spec /
§4.3 ``flagged_instrument_days``). This job pulls the day's bulk-deal and block-deal lists from the
NSE historical-deals APIs (date-ranged, so the §2.6 date-keyed catch-up can re-run any missed
trading day) and writes one flagged row per (symbol, day, reason).

Failure model (E5): each source (bulk, block) runs under its OWN guard — one failing never blocks
the other. A failed source alerts + leaves the table's existing rows in force (reuse yesterday; the
date-keyed catch-up retries the missed day). Rows whose own trade-date parses to a DIFFERENT day
are dropped (wrong window — never mis-key a flag). Never raises into the scheduler.
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from datetime import date, datetime
from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict

from engine.core.clock import Clock
from engine.core.log import get_logger
from engine.core.nse_http import nse_get
from engine.marketdata.store import MarketStore
from engine.notify.catalog import CatalogMessage, MessageKind

_log = get_logger("engine.datafeeds.deals")

#: NSE historical bulk/block-deal APIs (single-day window ``from=to=d``). [VERIFY Phase-1];
#: anti-bot [likely] — the api host is cookie/UA-gated like the other NSE JSON endpoints.
NSE_BULK_DEALS_URL_TEMPLATE = (
    "https://www.nseindia.com/api/historical/bulk-deals?from={d:%d-%m-%Y}&to={d:%d-%m-%Y}"
)
NSE_BLOCK_DEALS_URL_TEMPLATE = (
    "https://www.nseindia.com/api/historical/block-deals?from={d:%d-%m-%Y}&to={d:%d-%m-%Y}"
)

#: ``flagged_instrument_days.reason`` vocabulary (PK component, §4.3).
REASON_BULK = "bulk_deal"
REASON_BLOCK = "block_deal"

# Case-insensitive field aliases across the NSE bulk (BD_*) / block (BP_*) row shapes.
_SYMBOL_ALIASES = ("symbol", "bd_symbol", "bp_symbol", "block_symbol", "td_symbol")
_DATE_ALIASES = ("date", "bd_dt_date", "bp_dt_date", "deal_date", "trade_date", "mtimestamp")
_CLIENT_ALIASES = ("client", "client_name", "bd_client_name", "bp_client_name")
_QTY_ALIASES = ("qty", "quantity", "qty_shares", "bd_qty_trd", "bp_qty")
_PRICE_ALIASES = ("price", "trade_price", "watp", "wap", "bd_tp_watp", "bp_wap")

NotifySink = Callable[[CatalogMessage], Awaitable[None]]


class DealsResult(BaseModel):
    """One run's outcome (never an exception, E5). ``ok`` = at least one source ingested;
    ``degraded`` = at least one source failed (its existing rows stay in force)."""

    model_config = ConfigDict(frozen=True)

    d: date
    ok: bool
    degraded: bool = False
    failed_sources: tuple[str, ...] = ()
    rows_parsed: int = 0
    rows_written: int = 0
    reason: str | None = None


def _rows_of(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    if isinstance(payload, dict):
        for key in ("data", "rows", "records"):
            value = payload.get(key)
            if isinstance(value, list):
                return [r for r in value if isinstance(r, dict)]
    return []


def _parse_date(raw: str) -> date | None:
    raw = raw.strip()
    for fmt in ("%d-%b-%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def _first(keys: dict[str, Any], aliases: tuple[str, ...]) -> str:
    for alias in aliases:
        value = str(keys.get(alias) or "").strip()
        if value:
            return value
    return ""


def parse_deals(payload: Any, d: date, reason: str) -> list[dict[str, Any]]:
    """NSE deals JSON → ``flagged_instrument_days`` row dicts for day ``d`` (defensive, E5).

    A row whose own trade-date parses to a DIFFERENT day is dropped (wrong window). ``details``
    carries the audit context (client/qty/price) as a compact JSON string — the flag itself is the
    load-bearing bit; details are never parsed downstream.
    """
    rows: list[dict[str, Any]] = []
    skipped = 0
    for raw in _rows_of(payload):
        keys = {str(k).lower(): v for k, v in raw.items()}
        symbol = _first(keys, _SYMBOL_ALIASES).upper()
        if not symbol:
            skipped += 1
            continue
        date_raw = _first(keys, _DATE_ALIASES)
        if date_raw:
            parsed = _parse_date(date_raw)
            if parsed is not None and parsed != d:
                continue  # another day's deal — never mis-key a flag
        details = {
            k: v
            for k, v in (
                ("client", _first(keys, _CLIENT_ALIASES)),
                ("qty", _first(keys, _QTY_ALIASES)),
                ("price", _first(keys, _PRICE_ALIASES)),
            )
            if v
        }
        rows.append(
            {
                "symbol": symbol,
                "d": d,
                "reason": reason,
                "details": json.dumps(details) if details else None,
            }
        )
    if skipped:
        _log.warning("deals_malformed_rows", reason=reason, skipped=skipped)
    return rows


class DealsJob:
    """§4.4 job 9 — bulk/block deals → ``flagged_instrument_days`` (volume-breakout suppression)."""

    def __init__(
        self,
        store: MarketStore,
        clock: Clock,
        http: httpx.AsyncClient,
        *,
        notify: NotifySink | None = None,
        request_timeout_s: float = 20.0,
    ) -> None:
        self._store = store
        self._clock = clock
        self._http = http
        self._notify = notify
        self._timeout = float(request_timeout_s)

    async def run(self, d: date) -> DealsResult:
        """Fetch bulk + block deals for day ``d`` under per-source guards and upsert the flags
        (idempotent on (symbol, d, reason)). Never raises into the scheduler (E5)."""
        all_rows: list[dict[str, Any]] = []
        failed: list[str] = []
        reasons: list[str] = []
        for source, template, reason in (
            ("bulk", NSE_BULK_DEALS_URL_TEMPLATE, REASON_BULK),
            ("block", NSE_BLOCK_DEALS_URL_TEMPLATE, REASON_BLOCK),
        ):
            url = template.format(d=d)
            try:
                resp = await nse_get(self._http, url, timeout=self._timeout)
                all_rows.extend(parse_deals(json.loads(resp.content), d, reason))
            except Exception as exc:  # noqa: BLE001 - E5: per-source degrade, never raise
                detail = f"{source}: {type(exc).__name__}: {exc}"
                _log.warning("deals_fetch_failed", d=d.isoformat(), source=source, error=detail)
                failed.append(source)
                reasons.append(detail)

        written = 0
        if all_rows:
            written = await self._store.arun(self._store.upsert_flagged_instrument_days, all_rows)
        if failed:
            await self._alert(d, failed, "; ".join(reasons))
        result = DealsResult(
            d=d,
            ok=len(failed) < 2,
            degraded=bool(failed),
            failed_sources=tuple(failed),
            rows_parsed=len(all_rows),
            rows_written=written,
            reason="; ".join(reasons) or None,
        )
        _log.info(
            "deals_ingested", d=d.isoformat(), parsed=len(all_rows), written=written, failed=failed,
        )
        return result

    async def _alert(self, d: date, failed: list[str], reason: str) -> None:
        if self._notify is None:
            return
        msg = CatalogMessage(
            kind=MessageKind.DATA_FRESHNESS_FROZEN,  # closest catalog kind (closed catalog here)
            title="Bulk/block deals feed degraded",
            body=(
                f"Deals source(s) failed for {d.isoformat()}: {', '.join(failed)} ({reason}). "
                "Existing flags remain; volume-breakout suppression may miss today's deals until "
                "the date-keyed catch-up re-runs (E5)."
            ),
            severity="warning",
            data={"job_id": "deals", "d": d.isoformat(), "failed_sources": failed, "reason": reason},
        )
        try:
            await self._notify(msg)
        except Exception:  # noqa: BLE001 - best-effort alert; a failed send never propagates
            _log.exception("deals_notify_failed")
