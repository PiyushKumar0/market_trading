"""NSE results/board-meeting calendar → ``earnings_calendar`` (§4.4 job 8, 18:30 — R2, E5).

Pulls upcoming results / board-meeting dates from the NSE event-calendar API and persists them to
the DuckDB ``earnings_calendar`` table (§4.3). Two consumers, both later phases (DATA ONLY here):

1. **R2 no-trade windows** — "no entry in a symbol on its results day" (§7.1 ``no_trade_windows``,
   day T only), checked at gate time per instrument. The table is in the §2.6 step-5
   safety/deadline-critical catch-up set: it must run or verify before entries open.
2. **§6.1 ``cat`` T+1 PEAD eligibility (O13)** — the digest reads results dates to grade the T+1
   earnings-continuation catalyst.

Parsing is defensive (E5): the row list is located under common wrapper keys, field names are
matched case-insensitively with aliases, the free-text ``purpose`` is classified by keyword
(``results`` vs ``board_meeting`` — a board meeting that mentions results IS a results event for
R2). On fetch failure: alert + reuse yesterday's rows (the table keeps what it has; the §2.6
catch-up retries). Never raises into the scheduler.
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

_log = get_logger("engine.datafeeds.earnings_calendar")

#: NSE corporate event-calendar API (results/board-meeting dates). [VERIFY Phase-1]; anti-bot [likely].
NSE_EVENT_CALENDAR_URL = "https://www.nseindia.com/api/event-calendar"


def event_calendar_range_url(frm: date, to: date) -> str:
    """Event-calendar URL for an explicit ``[frm, to]`` window (§2.8 filings_results historical leg).

    The default :data:`NSE_EVENT_CALENDAR_URL` returns only the forward-looking cross-section; the
    ``from_date``/``to_date`` (DD-MM-YYYY) variant returns the PAST board-meeting/results dates too —
    the historical results-date source the §2.7 event study lacked (630 rows for Jan-2023, verified
    §2.8). The provider's existing job contract (:meth:`EarningsCalendarJob.run`) is unchanged; this
    is the added historical leg only."""
    return (
        "https://www.nseindia.com/api/event-calendar?index=equities"
        f"&from_date={frm:%d-%m-%Y}&to_date={to:%d-%m-%Y}"
    )

#: ``earnings_calendar.kind`` vocabulary (PK component, §4.3). ``results`` drives the R2 no-trade
#: window + O13 PEAD; ``board_meeting`` is recorded for completeness (audit / future rules).
KIND_RESULTS = "results"
KIND_BOARD_MEETING = "board_meeting"

NotifySink = Callable[[CatalogMessage], Awaitable[None]]


class EarningsCalendarResult(BaseModel):
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


def classify_event(purpose: str) -> str:
    """Free-text purpose → kind. "result" anywhere ⇒ ``results`` (R2 binds on results DAYS — a
    board meeting "to consider financial results" IS a results event); everything else is a plain
    ``board_meeting``. Deterministic keyword rule, never a guess."""
    return KIND_RESULTS if "result" in purpose.lower() else KIND_BOARD_MEETING


def parse_event_calendar(payload: Any) -> list[dict[str, Any]]:
    """NSE event-calendar JSON → ``earnings_calendar`` row dicts (defensive, skip-and-count).

    Field aliases (case-insensitive): symbol; date ∈ {date, bm_date, meetingdate, event_date,
    boardmeetingdate}; purpose ∈ {purpose, bm_purpose, subject, bm_desc, desc}.
    """
    rows: list[dict[str, Any]] = []
    skipped = 0
    for raw in _rows_of(payload):
        keys = {str(k).lower(): v for k, v in raw.items()}
        symbol = str(keys.get("symbol") or "").strip().upper()
        date_raw = ""
        for alias in ("date", "bm_date", "meetingdate", "event_date", "boardmeetingdate"):
            date_raw = str(keys.get(alias) or "").strip()
            if date_raw:
                break
        purpose = ""
        for alias in ("purpose", "bm_purpose", "subject", "bm_desc", "desc"):
            purpose = str(keys.get(alias) or "").strip()
            if purpose:
                break
        event_date = _parse_date(date_raw) if date_raw else None
        if not symbol or event_date is None:
            skipped += 1
            continue
        rows.append(
            {
                "symbol": symbol,
                "event_date": event_date,
                "kind": classify_event(purpose),
                "source": "nse",
            }
        )
    if skipped:
        _log.warning("earnings_calendar_malformed_rows", skipped=skipped)
    return rows


class EarningsCalendarJob:
    """§4.4 job 8 — NSE event calendar → ``earnings_calendar`` (R2 no-trade windows + O13 PEAD)."""

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

    async def run(self, d: date) -> EarningsCalendarResult:
        """Fetch + upsert results/board-meeting dates (idempotent on (symbol, event_date, kind)).
        ``d`` is the run day (audit only — the feed is forward-looking). Never raises (E5)."""
        try:
            resp = await nse_get(self._http, NSE_EVENT_CALENDAR_URL, timeout=self._timeout)
            rows = parse_event_calendar(json.loads(resp.content))
        except Exception as exc:  # noqa: BLE001 - E5: degrade + alert, never raise
            reason = f"{type(exc).__name__}: {exc}"
            _log.warning("earnings_calendar_fetch_failed", d=d.isoformat(), error=reason)
            await self._alert(d, reason)
            return EarningsCalendarResult(ok=False, degraded=True, reason=reason)

        now = self._clock.now()
        stamped = [{**row, "recorded_at": now} for row in rows]
        written = await self._store.arun(self._store.upsert_earnings_calendar, stamped)
        _log.info("earnings_calendar_ingested", d=d.isoformat(), parsed=len(rows), written=written)
        return EarningsCalendarResult(ok=True, rows_parsed=len(rows), rows_written=written)

    async def run_range(self, frm: date, to: date) -> EarningsCalendarResult:
        """Ingest the event calendar over an explicit ``[frm, to]`` window into the SAME
        ``earnings_calendar`` table (§2.8 filings_results historical leg). Same defensive parse +
        idempotent upsert as :meth:`run`; the forward-looking daily job contract is untouched. Never
        raises (E5) — a failure degrades and the caller (filings_results) decides how to alert."""
        url = event_calendar_range_url(frm, to)
        try:
            resp = await nse_get(self._http, url, timeout=self._timeout)
            rows = parse_event_calendar(json.loads(resp.content))
        except Exception as exc:  # noqa: BLE001 - E5: degrade, never raise
            reason = f"{type(exc).__name__}: {exc}"
            _log.warning(
                "earnings_calendar_range_failed", frm=frm.isoformat(), to=to.isoformat(), error=reason
            )
            return EarningsCalendarResult(ok=False, degraded=True, reason=reason)
        now = self._clock.now()
        stamped = [{**row, "recorded_at": now} for row in rows]
        written = await self._store.arun(self._store.upsert_earnings_calendar, stamped)
        _log.info(
            "earnings_calendar_range_ingested",
            frm=frm.isoformat(), to=to.isoformat(), parsed=len(rows), written=written,
        )
        return EarningsCalendarResult(ok=True, rows_parsed=len(rows), rows_written=written)

    async def _alert(self, d: date, reason: str) -> None:
        if self._notify is None:
            return
        msg = CatalogMessage(
            # Earnings calendar is in the §2.6 safety/deadline-critical set (R2 no-trade windows):
            # a stale calendar must be verified before entries open, else FROZEN-for-entries.
            kind=MessageKind.DATA_FRESHNESS_FROZEN,
            title="Earnings-calendar feed degraded",
            body=(
                f"NSE event-calendar fetch failed on {d.isoformat()}: {reason}. Existing "
                "earnings_calendar rows remain in force; catch-up retries (R2/E5)."
            ),
            severity="critical",
            data={"job_id": "earnings_calendar", "d": d.isoformat(), "reason": reason},
        )
        try:
            await self._notify(msg)
        except Exception:  # noqa: BLE001 - best-effort alert; a failed send never propagates
            _log.exception("earnings_calendar_notify_failed")
