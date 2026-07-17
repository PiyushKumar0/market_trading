"""NSE results filings → ``results_filings`` + historical board-meeting dates (§2.8 job
``filings_results``, ~18:45 — O14, E5).

Two legs under their OWN guards (one failing never blocks the other — the §4.4 job-9 deals pattern):

1. **Results filings** — ``corporates-financial-results?period=Quarterly`` filing METADATA (period,
   audited/consolidated flags, ``broadCastDate``/``exchdisstime``, XBRL link) → ``results_filings``.
   Line items (revenue/PAT/EPS) are NULL in stage 1 and populated in stage 2 once ``results-comparision``
   / XBRL parsing is verified (§2.8.1/§2.8.4). The date filter keys off the BROADCAST date (correct for
   point-in-time — a 2023-broadcast filing for FY2021 was observed live; period labels lie, §2.8 rule i).
2. **Board-meeting dates (past + future)** — merged into the existing ``earnings_calendar`` table via
   the provider's historical leg (:meth:`EarningsCalendarJob.run_range`), the historical results-date
   source the §2.7 event study lacked. The provider's forward-looking daily job contract is untouched.

DATA ONLY in stage 1 (no decision path touched). Both legs' incremental window keys off the latest
stored ``results_filings.broadcast_dt`` watermark (deep 3y history is the backfill's job). Defensive
parse (skip-and-count), idempotent upsert, never raises into the scheduler (E5).
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from datetime import date, datetime, timedelta
from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict

from engine.core.clock import IST, Clock
from engine.core.log import get_logger
from engine.core.nse_http import nse_get
from engine.datafeeds.earnings_calendar import EarningsCalendarJob
from engine.marketdata.store import MarketStore
from engine.notify.catalog import CatalogMessage, MessageKind

_log = get_logger("engine.datafeeds.filings_results")

#: NSE quarterly financial-results API (equities). [VERIFY Phase-1]; anti-bot [likely].
NSE_RESULTS_URL = "https://www.nseindia.com/api/corporates-financial-results"

#: The event-calendar historical leg's daily window around the run day — a modest past leg captures
#: just-announced board meetings; the forward leg captures the near-term results calendar. The deep
#: multi-year event-calendar history is the backfill's job, not the nightly incremental (§2.8).
EVENT_CALENDAR_PAST_DAYS = 7
EVENT_CALENDAR_FUTURE_DAYS = 45

NotifySink = Callable[[CatalogMessage], Awaitable[None]]


def results_url(frm: date, to: date) -> str:
    """Quarterly-results URL for the explicit broadcast-date window ``[frm, to]`` (DD-MM-YYYY, §2.8)."""
    return (
        f"{NSE_RESULTS_URL}?index=equities&period=Quarterly"
        f"&from_date={frm:%d-%m-%Y}&to_date={to:%d-%m-%Y}"
    )


class FilingsResultsResult(BaseModel):
    """One run's outcome (never an exception, E5). ``ok`` = the results leg ingested; ``degraded`` =
    at least one leg failed (its existing rows stay in force, the catch-up retries)."""

    model_config = ConfigDict(frozen=True)

    ok: bool
    degraded: bool = False
    failed_legs: tuple[str, ...] = ()
    frm: date | None = None
    to: date | None = None
    results_parsed: int = 0
    results_written: int = 0
    events_written: int = 0
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


def _clean(raw: Any) -> str:
    return str(raw or "").strip()


def _parse_date(raw: Any) -> date | None:
    s = _clean(raw)
    for fmt in ("%d-%b-%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _parse_dt(raw: Any) -> datetime | None:
    s = _clean(raw)
    for fmt in ("%d-%b-%Y %H:%M:%S", "%d-%b-%Y %H:%M", "%d-%b-%Y"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=IST)
        except ValueError:
            continue
    return None


def _is_consolidated(raw: Any) -> bool:
    """"Consolidated" → True, "Non-Consolidated" → False (both stored; consolidated preferred, §2.8)."""
    low = _clean(raw).lower()
    return "consolidated" in low and not low.startswith("non")


def _is_audited(raw: Any) -> bool | None:
    """"Audited" → True, "Un-Audited" → False, blank → None (unknown, not a guess)."""
    low = _clean(raw).lower()
    if not low:
        return None
    if low.startswith("un"):
        return False
    return "audited" in low


def parse_results(payload: Any) -> list[dict[str, Any]]:
    """NSE financial-results JSON → ``results_filings`` row dicts (defensive, skip-and-count).

    Field map (probe-verified, §2.8): symbol; toDate→period_end (the quarter end — the PK component);
    consolidated flag; audited flag; broadCastDate→broadcast_dt (the point-in-time key); exchdisstime→
    exchdiss_dt; xbrl. Line items (revenue/pat/eps) are NULL in stage 1. A row without a symbol or an
    unparseable period_end is skipped (never key a filing on a missing period).
    """
    rows: list[dict[str, Any]] = []
    skipped = 0
    for raw in _rows_of(payload):
        keys = {str(k).lower(): v for k, v in raw.items()}
        symbol = _clean(keys.get("symbol")).upper()
        period_end = _parse_date(keys.get("todate"))
        if not symbol or period_end is None:
            skipped += 1
            continue
        rows.append(
            {
                "symbol": symbol,
                "period_end": period_end,
                "consolidated": _is_consolidated(keys.get("consolidated")),
                "audited": _is_audited(keys.get("audited")),
                "broadcast_dt": _parse_dt(keys.get("broadcastdate")),
                "exchdiss_dt": _parse_dt(keys.get("exchdisstime")),
                "xbrl": _clean(keys.get("xbrl")) or None,
                "revenue": None,   # stage-2 line items (§2.8.4)
                "pat": None,
                "eps": None,
            }
        )
    if skipped:
        _log.warning("filings_results_malformed_rows", skipped=skipped)
    return rows


class FilingsResultsJob:
    """§2.8 job ``filings_results`` — results filings + historical board-meeting dates (date-keyed, E5).

    ``earnings`` is the existing :class:`EarningsCalendarJob` provider; this job calls its historical
    leg (``run_range``) so the board-meeting dates land in the SAME ``earnings_calendar`` table without
    changing that provider's daily forward-looking job contract (§2.8). ``earnings=None`` runs the
    results leg only.
    """

    def __init__(
        self,
        store: MarketStore,
        clock: Clock,
        http: httpx.AsyncClient,
        *,
        earnings: EarningsCalendarJob | None = None,
        notify: NotifySink | None = None,
        request_timeout_s: float = 20.0,
    ) -> None:
        self._store = store
        self._clock = clock
        self._http = http
        self._earnings = earnings
        self._notify = notify
        self._timeout = float(request_timeout_s)

    async def run(self, d: date) -> FilingsResultsResult:
        """Ingest results filings over ``[watermark → d]`` and (if wired) the event calendar over the
        surrounding window, under per-leg guards. ``d`` is the run day. Never raises (E5)."""
        watermark = await self._store.alatest_results_broadcast()
        frm = min(watermark.date(), d) if watermark is not None else d
        failed: list[str] = []
        reasons: list[str] = []

        results_parsed = results_written = 0
        try:
            resp = await nse_get(self._http, results_url(frm, d), timeout=self._timeout)
            parsed = parse_results(json.loads(resp.content))
            results_parsed = len(parsed)
            results_written = await self._store.arun(self._store.upsert_results_filings, parsed)
        except Exception as exc:  # noqa: BLE001 - E5: per-leg degrade, never raise
            detail = f"results: {type(exc).__name__}: {exc}"
            _log.warning("filings_results_fetch_failed", d=d.isoformat(), error=detail)
            failed.append("results")
            reasons.append(detail)

        events_written = 0
        if self._earnings is not None:
            ec = await self._earnings.run_range(
                d - timedelta(days=EVENT_CALENDAR_PAST_DAYS),
                d + timedelta(days=EVENT_CALENDAR_FUTURE_DAYS),
            )
            events_written = ec.rows_written
            if not ec.ok:
                failed.append("event_calendar")
                reasons.append(f"event_calendar: {ec.reason}")

        if failed:
            await self._alert(d, failed, "; ".join(reasons))
        result = FilingsResultsResult(
            ok="results" not in failed,
            degraded=bool(failed),
            failed_legs=tuple(failed),
            frm=frm,
            to=d,
            results_parsed=results_parsed,
            results_written=results_written,
            events_written=events_written,
            reason="; ".join(reasons) or None,
        )
        _log.info(
            "filings_results_ingested", frm=frm.isoformat(), to=d.isoformat(),
            results=results_written, events=events_written, failed=failed,
        )
        return result

    async def _alert(self, d: date, failed: list[str], reason: str) -> None:
        if self._notify is None:
            return
        msg = CatalogMessage(
            # Filings are NEVER load-bearing (§2.8 rule iii): features/risk-context, not safety —
            # warning severity, not entry-blocking (contrast the earnings-calendar SAFETY job, R2).
            kind=MessageKind.DATA_FRESHNESS_FROZEN,
            title="Results-filings feed degraded",
            body=(
                f"Results-filings leg(s) failed for {d.isoformat()}: {', '.join(failed)} ({reason}). "
                "Existing rows remain; the date-keyed catch-up retries (§2.8/E5). Not entry-blocking."
            ),
            severity="warning",
            data={"job_id": "filings_results", "d": d.isoformat(), "failed_legs": failed, "reason": reason},
        )
        try:
            await self._notify(msg)
        except Exception:  # noqa: BLE001 - best-effort alert; a failed send never propagates
            _log.exception("filings_results_notify_failed")
