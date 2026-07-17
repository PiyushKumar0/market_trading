"""NSE corporate-actions feed → ``corp_actions`` (§4.4 job 7, 18:15 — A12, E5 best-effort).

Pulls upcoming/recent corporate actions (ex-dates for dividends, splits, bonuses, rights, …) from
the NSE corporates API and persists them to the DuckDB ``corp_actions`` table (§4.3). DATA ONLY in
Phase 1: the consumers land later — ``GTTManager.adjust_for_ex_date`` (A12, Phase 3), ledger
attribution tags, and ``NSECalendar.ex_dates`` (the §2.6 startup corp-action scan reads this table).

Parsing is defensive (E5): the row list is located under common wrapper keys, per-row field names
are matched case-insensitively, the free-text ``purpose`` ("Dividend - Rs 5 Per Share",
"Bonus 1:1", "Face Value Split…") is classified by keyword with ratio/amount extracted by regex —
an unclassifiable purpose is kept as ``kind='other'`` (recorded, never guessed into a GTT
adjustment). On fetch failure: alert + reuse yesterday's rows (the table simply keeps what it has;
the §2.6 catch-up retries). Never raises into the scheduler.
"""

from __future__ import annotations

import json
import re
from collections.abc import Awaitable, Callable
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict

from engine.core.clock import Clock
from engine.core.log import get_logger
from engine.core.nse_http import nse_get
from engine.marketdata.store import MarketStore
from engine.notify.catalog import CatalogMessage, MessageKind

_log = get_logger("engine.datafeeds.corp_actions")

#: NSE corporates corporate-actions API (equities segment). [VERIFY Phase-1]; anti-bot [likely].
NSE_CORP_ACTIONS_URL = "https://www.nseindia.com/api/corporates-corporate-actions?index=equities"

#: purpose-keyword → kind, checked IN ORDER (first match wins; bonus/split before dividend so a
#: compound purpose classifies by its structural action, which is what GTT adjustment cares about).
_KIND_KEYWORDS: tuple[tuple[str, str], ...] = (
    ("bonus", "bonus"),
    ("split", "split"),
    ("sub-division", "split"),
    ("subdivision", "split"),
    ("rights", "rights"),
    ("buyback", "buyback"),
    ("buy back", "buyback"),
    ("dividend", "dividend"),
)

_RATIO_RE = re.compile(r"(\d+)\s*:\s*(\d+)")
_AMOUNT_RE = re.compile(r"(?:rs\.?|inr|₹)\s*([0-9]+(?:\.[0-9]+)?)", re.IGNORECASE)

NotifySink = Callable[[CatalogMessage], Awaitable[None]]


class CorpActionsResult(BaseModel):
    """One run's outcome (never an exception, E5)."""

    model_config = ConfigDict(frozen=True)

    ok: bool
    degraded: bool = False
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


def classify_purpose(purpose: str) -> tuple[str, str | None, Decimal | None]:
    """Free-text purpose → ``(kind, ratio, amount)`` (deterministic keyword/regex, never a guess).

    ``ratio`` is the first ``a:b`` in the text (bonus/split/rights terms); ``amount`` is the first
    rupee figure and only kept for dividends (a split's "Rs 10/- to Rs 2/-" is a face value, not a
    payout). Unrecognized ⇒ ``('other', None, None)`` — recorded for the audit trail (A12).
    """
    lowered = purpose.lower()
    kind = "other"
    for keyword, mapped in _KIND_KEYWORDS:
        if keyword in lowered:
            kind = mapped
            break
    ratio_match = _RATIO_RE.search(purpose)
    ratio = f"{ratio_match.group(1)}:{ratio_match.group(2)}" if ratio_match else None
    amount: Decimal | None = None
    if kind == "dividend":
        amount_match = _AMOUNT_RE.search(purpose)
        if amount_match:
            try:
                amount = Decimal(amount_match.group(1))
            except InvalidOperation:
                amount = None
    return kind, ratio, amount


def parse_corp_actions(payload: Any) -> list[dict[str, Any]]:
    """NSE corporate-actions JSON → ``corp_actions`` row dicts (defensive, skip-and-count)."""
    rows: list[dict[str, Any]] = []
    skipped = 0
    for raw in _rows_of(payload):
        keys = {str(k).lower(): v for k, v in raw.items()}
        symbol = str(keys.get("symbol") or "").strip().upper()
        ex_raw = str(keys.get("exdate") or keys.get("exdt") or keys.get("ex_date") or "").strip()
        purpose = str(keys.get("purpose") or keys.get("subject") or "").strip()
        ex_date = _parse_date(ex_raw) if ex_raw else None
        if not symbol or ex_date is None:
            skipped += 1
            continue
        kind, ratio, amount = classify_purpose(purpose)
        rows.append(
            {
                "symbol": symbol,
                "ex_date": ex_date,
                "kind": kind,
                "ratio": ratio,
                "amount": amount,
                "source": "nse",
            }
        )
    if skipped:
        _log.warning("corp_actions_malformed_rows", skipped=skipped)
    return rows


class CorpActionsJob:
    """§4.4 job 7 — NSE corporate actions → ``corp_actions`` (A12 data; consumers in Phase 3)."""

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

    async def run(self, d: date) -> CorpActionsResult:
        """Fetch + upsert corporate actions (idempotent on (symbol, ex_date, kind)). ``d`` is the
        run day (audit only — the feed is forward-looking). Never raises into the scheduler (E5)."""
        try:
            resp = await nse_get(self._http, NSE_CORP_ACTIONS_URL, timeout=self._timeout)
            rows = parse_corp_actions(json.loads(resp.content))
        except Exception as exc:  # noqa: BLE001 - E5: degrade + alert, never raise
            reason = f"{type(exc).__name__}: {exc}"
            _log.warning("corp_actions_fetch_failed", d=d.isoformat(), error=reason)
            await self._alert(d, reason)
            return CorpActionsResult(ok=False, degraded=True, reason=reason)

        now = self._clock.now()
        stamped = [{**row, "recorded_at": now} for row in rows]
        written = await self._store.arun(self._store.upsert_corp_actions, stamped)
        _log.info("corp_actions_ingested", d=d.isoformat(), parsed=len(rows), written=written)
        return CorpActionsResult(ok=True, rows_parsed=len(rows), rows_written=written)

    async def _alert(self, d: date, reason: str) -> None:
        if self._notify is None:
            return
        msg = CatalogMessage(
            # Corp actions are in the §2.6 safety/deadline-critical set (GTT ex-date adjustment, A12).
            kind=MessageKind.DATA_FRESHNESS_FROZEN,
            title="Corporate-actions feed degraded",
            body=(
                f"NSE corporate-actions fetch failed on {d.isoformat()}: {reason}. Existing "
                "corp_actions rows remain in force; catch-up retries (A12/E5)."
            ),
            severity="critical",
            data={"job_id": "corp_actions", "d": d.isoformat(), "reason": reason},
        )
        try:
            await self._notify(msg)
        except Exception:  # noqa: BLE001 - best-effort alert; a failed send never propagates
            _log.exception("corp_actions_notify_failed")
