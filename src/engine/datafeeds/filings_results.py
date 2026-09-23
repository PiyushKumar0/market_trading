"""NSE results filings → ``results_filings`` + historical board-meeting dates (§2.8 job
``filings_results``, ~18:45 — O14, E5), and their quarterly line items (job ``results_line_items``).

``filings_results`` runs three legs under their OWN guards (one failing never blocks another — the
§4.4 job-9 deals pattern):

1. **Results filings** — ``corporates-financial-results?period=Quarterly`` filing METADATA (period,
   audited/consolidated flags, ``broadCastDate``/``exchdisstime``, XBRL link) → ``results_filings``.
   This endpoint's newest period is the Dec-2024 quarter; it now only carries late filings of old
   periods. The date filter keys off the BROADCAST date (correct for point-in-time — a
   2023-broadcast filing for FY2021 was observed live; period labels lie, §2.8 rule i).
2. **Integrated filings** — SEBI's Integrated Filing (Financials) listing, where every result from
   the Mar-2025 quarter on is filed (probe-verified 2026-09-24): same metadata, same table.
3. **Board-meeting dates (past + future)** — merged into the existing ``earnings_calendar`` table via
   the provider's historical leg (:meth:`EarningsCalendarJob.run_range`), the historical results-date
   source the §2.7 event study lacked. The provider's forward-looking daily job contract is untouched.

The listing legs' incremental window keys off the latest stored ``results_filings.broadcast_dt``
watermark (deep history is the backfill's job). A run whose newest stored period is older than
:data:`RESULTS_STALE_AFTER_DAYS` is degraded + alerted: a listing that returns nothing new still
"succeeds", which is how the switch to Integrated Filing went unnoticed for 18 months.

``results_line_items`` (§2.8.4 stage 2) fetches each pending filing's XBRL and stores its quarterly
``revenue``/``pat``, a bounded batch per run. Defensive parse (skip-and-count), idempotent upsert,
never raises into the scheduler (E5).
"""

from __future__ import annotations

import asyncio
import json
import xml.etree.ElementTree as ET
from collections.abc import Awaitable, Callable
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import quote

import httpx
from pydantic import BaseModel, ConfigDict

from engine.core.clock import IST, Clock
from engine.core.log import get_logger
from engine.core.nse_http import nse_get
from engine.datafeeds.earnings_calendar import EarningsCalendarJob
from engine.datafeeds.filings_pit import PIT_PACE_S
from engine.marketdata.store import MarketStore
from engine.notify.catalog import CatalogMessage, MessageKind

_log = get_logger("engine.datafeeds.filings_results")

#: NSE quarterly financial-results API (equities). [VERIFY Phase-1]; anti-bot [likely].
NSE_RESULTS_URL = "https://www.nseindia.com/api/corporates-financial-results"

#: SEBI Integrated Filing (Financials) listing — the home of every result since the Mar-2025 quarter.
NSE_INTEGRATED_RESULTS_URL = "https://www.nseindia.com/api/integrated-filing-results"
INTEGRATED_RESULTS_TYPE = "Integrated Filing- Financials"
INTEGRATED_PAGE_SIZE = 500

#: Results are due within 45 days of a quarter end (60 for Q4) and early filers land within weeks, so
#: a healthy feed's newest period is never much more than ~105 days old; beyond this it is starved.
RESULTS_STALE_AFTER_DAYS = 150

#: ``results_line_items``: XBRL fetches per run (paced at :data:`PIT_PACE_S`) and the period horizon
#: (five quarters plus the filing lag — what a trailing-year revenue needs).
LINE_ITEMS_PER_RUN = 300
LINE_ITEMS_LOOKBACK_DAYS = 450

#: Quarterly line items, first tag present wins (probe-verified 2026-09-24 on INDAS, NBFC_INDAS,
#: NONINDAS, BANKING, LI and GI filings). "Revenue" is the issuer's operating scale: revenue from
#: operations; an insurer's gross premium; a bank's total income.
_REVENUE_TAGS = ("RevenueFromOperations", "GrossPremiumIncome", "GrossPremiumsWritten", "Income")
_PROFIT_TAGS = (
    "ProfitLossForPeriod", "ProfitLossForThePeriod", "ProfitLossAfterTaxAndExtraordinaryItems",
    "ProfitLossAfterTax",
)

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
    """One run's outcome (never an exception, E5). ``ok`` = both results listings ingested;
    ``degraded`` = a leg failed (its existing rows stay in force, the catch-up retries) or the stored
    periods are stale (``failed_legs`` then names ``stale``)."""

    model_config = ConfigDict(frozen=True)

    ok: bool
    degraded: bool = False
    failed_legs: tuple[str, ...] = ()
    frm: date | None = None
    to: date | None = None
    results_parsed: int = 0
    results_written: int = 0
    integrated_written: int = 0
    events_written: int = 0
    newest_period: date | None = None
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


_sleep = asyncio.sleep   # indirection so tests collapse the pacing


def integrated_results_url(frm: date, to: date, page: int) -> str:
    """Integrated Filing (Financials) listing for the broadcast window ``[frm, to]``, 1-based ``page``."""
    return (
        f"{NSE_INTEGRATED_RESULTS_URL}?index=equities&type={quote(INTEGRATED_RESULTS_TYPE)}"
        f"&from_date={frm:%d-%m-%Y}&to_date={to:%d-%m-%Y}&page={page}&size={INTEGRATED_PAGE_SIZE}"
    )


def parse_integrated_results(payload: Any) -> list[dict[str, Any]]:
    """Integrated Filing listing JSON → ``results_filings`` row dicts (defensive, skip-and-count).

    ORIGINAL filings only. A revision carries no broadcast time and restates a period already broadcast:
    keying its figures to the original's time would be lookahead, and re-keying the period to the
    revision's time would move a known event. ``broadcast_Date`` is the point-in-time key, falling back
    to ``creation_Date`` (the dissemination stamp) when an original lacks it."""
    rows: list[dict[str, Any]] = []
    skipped = revisions = 0
    for raw in _rows_of(payload):
        keys = {str(k).lower(): v for k, v in raw.items()}
        if _clean(keys.get("type_sub")).lower() != "original":
            revisions += 1
            continue
        symbol = _clean(keys.get("symbol")).upper()
        period_end = _parse_date(keys.get("qe_date"))
        if not symbol or period_end is None:
            skipped += 1
            continue
        created = _parse_dt(keys.get("creation_date"))
        rows.append(
            {
                "symbol": symbol,
                "period_end": period_end,
                "consolidated": _is_consolidated(keys.get("consolidated")),
                "audited": _is_audited(keys.get("audited")),
                "broadcast_dt": _parse_dt(keys.get("broadcast_date")) or created,
                "exchdiss_dt": created,
                "xbrl": _clean(keys.get("xbrl")) or None,
            }
        )
    if skipped or revisions:
        _log.info("filings_integrated_rows_skipped", malformed=skipped, revisions=revisions)
    return rows


async def fetch_integrated_results(
    http: httpx.AsyncClient, frm: date, to: date, *, timeout: float
) -> list[dict[str, Any]]:
    """Every original Integrated Filing (Financials) row broadcast in ``[frm, to]``, walking pages."""
    rows: list[dict[str, Any]] = []
    page = 1
    while True:
        resp = await nse_get(http, integrated_results_url(frm, to, page), timeout=timeout)
        payload = json.loads(resp.content)
        rows.extend(parse_integrated_results(payload))
        total = int(payload.get("totalCount") or 0) if isinstance(payload, dict) else 0
        if not _rows_of(payload) or page * INTEGRATED_PAGE_SIZE >= total:
            return rows
        page += 1
        await _sleep(PIT_PACE_S)


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _first_amount(facts: dict[str, str | None], tags: tuple[str, ...]) -> Decimal | None:
    for tag in tags:
        text = facts.get(tag)
        if text and text.strip():
            try:
                return Decimal(text.strip())
            except InvalidOperation:
                return None
    return None


def parse_xbrl_line_items(content: bytes | str, period_end: date) -> tuple[Decimal | None, Decimal | None]:
    """``(revenue, pat)`` in ₹ for the QUARTER ending ``period_end`` from one results XBRL instance.

    The quarter is the undimensioned context ending on ``period_end`` with the LATEST start: a Q4
    filing also reports the full year under a context with the same end date, and dimensioned
    (segment/scenario) contexts carry breakdowns, not totals. ``None`` where the tag is absent.
    Raises :class:`xml.etree.ElementTree.ParseError` on a malformed document."""
    root = ET.fromstring(content)
    quarter: tuple[str, date] | None = None
    for ctx in root:
        if _local(ctx.tag) != "context":
            continue
        parts = {_local(e.tag): (e.text or "").strip() for e in ctx.iter()}
        if "segment" in parts or "scenario" in parts:
            continue
        try:
            start = date.fromisoformat(parts.get("startDate", ""))
            end = date.fromisoformat(parts.get("endDate", ""))
        except ValueError:
            continue   # instant contexts carry no period
        if end == period_end and (quarter is None or start > quarter[1]):
            quarter = (ctx.get("id") or "", start)
    if quarter is None:
        return None, None
    facts: dict[str, str | None] = {}
    for el in root:
        if el.get("contextRef") == quarter[0]:
            facts.setdefault(_local(el.tag), el.text)
    return _first_amount(facts, _REVENUE_TAGS), _first_amount(facts, _PROFIT_TAGS)


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
        #: Per-date alert dedup (2026-08-13, mirrors bhavcopy): keyed on ``d``; ``_alert`` can fire on a
        #: run where only the event_calendar leg failed (``ok`` stays True — ``ok="results" not in
        #: failed``), so the dedup guards the call site, not the ``ok`` field. Discarded whenever a run
        #: for ``d`` has zero failed legs (re-arms for a later streak).
        self._alerted: set[date] = set()

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

        integrated_written = 0
        try:
            rows = await fetch_integrated_results(self._http, frm, d, timeout=self._timeout)
            integrated_written = await self._store.arun(self._store.upsert_results_filings, rows)
        except Exception as exc:  # noqa: BLE001 - E5: per-leg degrade, never raise
            detail = f"integrated: {type(exc).__name__}: {exc}"
            _log.warning("filings_results_fetch_failed", d=d.isoformat(), error=detail)
            failed.append("integrated")
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

        newest = await self._store.arun(self._store.latest_results_period)
        if newest is not None and (d - newest).days > RESULTS_STALE_AFTER_DAYS:
            failed.append("stale")
            reasons.append(f"stale: newest stored period {newest.isoformat()} is {(d - newest).days} days old")

        if failed:
            if d not in self._alerted:  # dedup: don't storm on every retry of a still-failing day
                await self._alert(d, failed, "; ".join(reasons))
                self._alerted.add(d)
        else:
            self._alerted.discard(d)  # a clean run for d re-arms the alert for a later streak
        result = FilingsResultsResult(
            ok="results" not in failed and "integrated" not in failed,
            degraded=bool(failed),
            failed_legs=tuple(failed),
            frm=frm,
            to=d,
            results_parsed=results_parsed,
            results_written=results_written,
            integrated_written=integrated_written,
            events_written=events_written,
            newest_period=newest,
            reason="; ".join(reasons) or None,
        )
        _log.info(
            "filings_results_ingested", frm=frm.isoformat(), to=d.isoformat(),
            results=results_written, integrated=integrated_written, events=events_written,
            newest_period=newest.isoformat() if newest else None, failed=failed,
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
                f"Results-filings problem(s) for {d.isoformat()}: {', '.join(failed)} ({reason}). "
                "Existing rows remain; a failed leg is retried by the date-keyed catch-up (§2.8/E5), "
                "a stale feed is not — its source needs a look. Not entry-blocking."
            ),
            severity="warning",
            data={"job_id": "filings_results", "d": d.isoformat(), "failed_legs": failed, "reason": reason},
        )
        try:
            await self._notify(msg)
        except Exception:  # noqa: BLE001 - best-effort alert; a failed send never propagates
            _log.exception("filings_results_notify_failed")


class ResultsLineItemsResult(BaseModel):
    """One ``results_line_items`` run. ``ok`` is always True: a filing that failed to fetch stays
    pending for the next scheduled run, and a failed run must not trigger 30-min catch-up retries."""

    model_config = ConfigDict(frozen=True)

    ok: bool = True
    pending: int = 0
    filled: int = 0
    no_revenue: int = 0
    failed: int = 0


class ResultsLineItemsJob:
    """§2.8.4 stage 2 — quarterly revenue/PAT from each pending filing's XBRL, for the batch universe
    (the symbols the news resolver can attach), at most ``per_run`` fetches per run, newest first."""

    def __init__(
        self,
        store: MarketStore,
        clock: Clock,
        http: httpx.AsyncClient,
        *,
        per_run: int = LINE_ITEMS_PER_RUN,
        request_timeout_s: float = 20.0,
    ) -> None:
        self._store = store
        self._clock = clock
        self._http = http
        self._per_run = int(per_run)
        self._timeout = float(request_timeout_s)

    async def run(self) -> ResultsLineItemsResult:
        today = self._clock.today()
        symbols = await self._store.arun(self._store.get_batch_universe_symbols, today)
        if not symbols:
            _log.info("results_line_items_no_universe", d=today.isoformat())
            return ResultsLineItemsResult()
        pending = await self._store.arun(
            self._store.results_line_item_candidates, symbols,
            since=today - timedelta(days=LINE_ITEMS_LOOKBACK_DAYS), limit=self._per_run,
        )
        filled = no_revenue = failed = 0
        for i, row in enumerate(pending):
            if i:
                await _sleep(PIT_PACE_S)
            try:
                resp = await nse_get(self._http, row["xbrl"], timeout=self._timeout)
            except Exception as exc:  # noqa: BLE001 - transient: stays pending for the next run
                failed += 1
                _log.warning("results_line_items_fetch_failed", symbol=row["symbol"],
                             period_end=str(row["period_end"]), error=f"{type(exc).__name__}: {exc}")
                continue
            try:
                revenue, pat = parse_xbrl_line_items(resp.content, row["period_end"])
            except ET.ParseError:
                # Permanent: stamped with no values so a malformed file is never re-fetched.
                revenue = pat = None
                _log.warning("results_line_items_malformed_xbrl", symbol=row["symbol"], xbrl=row["xbrl"])
            await self._store.arun(
                self._store.set_results_line_items, [{**row, "revenue": revenue, "pat": pat}]
            )
            if revenue is None:
                no_revenue += 1
            else:
                filled += 1
        result = ResultsLineItemsResult(
            pending=len(pending), filled=filled, no_revenue=no_revenue, failed=failed
        )
        _log.info("results_line_items_run", **result.model_dump())
        return result
