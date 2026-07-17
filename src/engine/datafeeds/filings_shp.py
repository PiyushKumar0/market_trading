"""Shareholding pattern (SHP) + pledge → ``shp_quarterly`` (§2.8 job ``filings_shp``, ~18:50 — O14, E5).

Daily incremental (RUN_LATEST, never entry-blocking): scan the NSE ``corporate-share-holdings-master``
cross-section for submissions with ``broadcastDate`` newer than the stored watermark; for each new
(symbol, quarter) fetch the BSE SHP detail stack — the SEBI-format quarterly tables that carry the
per-category **pledged/encumbered** + locked shares NSE's master does not — and upsert one row per
category into ``shp_quarterly`` (§2.8.1). The BSE detail is two calls (``SHPQNewFormat/w`` quarter
index → ``CorporatesSHPSecuritybeta/w`` detail per ``qtrid``), paced ≥1.5 s apart (observed safe).

The BSE scrip code comes from ``symbol_isin`` (seeded by ``isin_map`` / the backfill). A symbol with
no scrip-code mapping is **skipped-and-counted**, never a failure — the feed degrades to less data,
never raises (§2.8 rule iii, news-equal E5). A per-symbol BSE fetch failure (incl. the documented
``error_Bse.html``-as-200 quirk via :func:`engine.core.bse_http.bse_get`) degrades that symbol only.

DATA ONLY in stage 1 (no decision path touched).

**BSE SHP detail shape — LIVE-VERIFIED 2026-07-16** (the original Phase-1 probe had guessed the WRONG
endpoints — ``ShareholdingPattern/w`` / ``ShareholdingPatternGetData/w`` — which returned
``error_Bse.html``; the §2.8-named endpoints were re-probed live during this build):
  * ``SHPQNewFormat/w?scripcode=`` → ``{"Table": [{yr, qtrid (float), qtr ("June 2026"),
    filing_date_time, XbrlFile, ...}]}`` — the filed-quarter index. ``qtrid`` is a float (``130.0``);
    the quarter is a "<Month> <Year>" LABEL, not a date, so we derive the quarter-END date from it.
  * ``CorporatesSHPSecuritybeta/w?scripcode=&qtrid=`` → a 7-result-set envelope; the per-category
    SEBI table is in **``Table1``** (``Fld_ShortName`` category, ``Fld_TotalNoOfShares``,
    ``Fld_TotalPercentageOf_A_B_C2`` %, ``Fld_PledgeEncumberedNoOfShares``/``…Percentage`` pledge,
    ``Fld_NoOfLockedInShares``, ``Fld_NoOfShareHolders``). The parsers below key off these verified
    names with defensive fallbacks. The NSE-master leg (new-submission detection) is probe-verified.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from datetime import date, datetime, timedelta
from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict

from engine.core.bse_http import bse_get
from engine.core.clock import IST, Clock
from engine.core.log import get_logger
from engine.core.nse_http import nse_get
from engine.marketdata.store import MarketStore
from engine.notify.catalog import CatalogMessage, MessageKind

_log = get_logger("engine.datafeeds.filings_shp")

#: NSE SHP daily-incremental master cross-section (equities). [VERIFY Phase-1]; probe-verified list shape.
NSE_SHP_MASTER_URL = "https://www.nseindia.com/api/corporate-share-holdings-master?index=equities"

#: BSE SHP detail stack (§2.8). [VERIFY Phase-1 — shape UNVERIFIED, see module docstring].
BSE_SHP_QUARTER_INDEX_URL = "https://api.bseindia.com/BseIndiaAPI/api/SHPQNewFormat/w?scripcode={code}"
BSE_SHP_DETAIL_URL = (
    "https://api.bseindia.com/BseIndiaAPI/api/CorporatesSHPSecuritybeta/w?scripcode={code}&qtrid={qtrid}"
)

#: BSE per-request spacing (§2.8: ≥1.5 s observed safe). Module indirection so tests skip the wait.
_BSE_SPACING_S = 1.5
_sleep = asyncio.sleep

NotifySink = Callable[[CatalogMessage], Awaitable[None]]

SOURCE_BSE = "bse"
SOURCE_NSE = "nse"


class FilingsShpResult(BaseModel):
    """One run's outcome (never an exception, E5)."""

    model_config = ConfigDict(frozen=True)

    ok: bool
    degraded: bool = False
    submissions_seen: int = 0
    new_submissions: int = 0
    symbols_upserted: int = 0
    rows_written: int = 0
    skipped_no_scrip: int = 0
    failed_symbols: int = 0
    reason: str | None = None


# --------------------------------------------------------------------------- shared defensive helpers
def _clean(raw: Any) -> str:
    return str(raw or "").strip()


def _parse_date(raw: Any) -> date | None:
    s = _clean(raw)
    for fmt in ("%d-%b-%Y", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _parse_dt(raw: Any) -> datetime | None:
    s = _clean(raw)
    for fmt in (
        "%d-%b-%Y %H:%M:%S", "%d-%b-%Y %H:%M", "%d-%b-%Y",
        "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S",
    ):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=IST)
        except ValueError:
            continue
    return None


def _first(keys: dict[str, Any], aliases: tuple[str, ...]) -> Any:
    for alias in aliases:
        if alias in keys and _clean(keys[alias]):
            return keys[alias]
    return None


def _int(raw: Any) -> int | None:
    s = _clean(raw).replace(",", "")
    if not s or s in ("-", "NA"):
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def _flt(raw: Any) -> float | None:
    s = _clean(raw).replace(",", "")
    if not s or s in ("-", "NA"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _bse_table(payload: Any, key: str = "Table") -> list[dict[str, Any]]:
    """Extract a BSE ASP.NET result set (``Table``/``Table1`` envelope OR a top-level list)."""
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    if isinstance(payload, dict):
        value = payload.get(key)
        if isinstance(value, list):
            return [r for r in value if isinstance(r, dict)]
    return []


# --------------------------------------------------------------------------- NSE master (VERIFIED shape)
class ShpSubmission(BaseModel):
    """One NSE SHP-master submission (the daily new-filing detection unit, §2.8)."""

    model_config = ConfigDict(frozen=True)

    symbol: str
    isin: str | None
    qtr_end: date
    broadcast_dt: datetime
    revised: bool


def parse_shp_master(payload: Any) -> list[ShpSubmission]:
    """NSE SHP-master JSON → submissions (defensive, skip-and-count). Probe-verified field names:
    symbol; isin; ``date``→qtr_end; ``broadcastDate``→broadcast_dt; ``revisedData`` Y/N→revised."""
    subs: list[ShpSubmission] = []
    skipped = 0
    rows = payload if isinstance(payload, list) else _bse_table(payload)
    for raw in rows:
        if not isinstance(raw, dict):
            skipped += 1
            continue
        keys = {str(k).lower(): v for k, v in raw.items()}
        symbol = _clean(keys.get("symbol")).upper()
        qtr_end = _parse_date(keys.get("date"))
        broadcast_dt = _parse_dt(keys.get("broadcastdate"))
        if not symbol or qtr_end is None or broadcast_dt is None:
            skipped += 1
            continue
        subs.append(
            ShpSubmission(
                symbol=symbol,
                isin=_clean(keys.get("isin")) or None,
                qtr_end=qtr_end,
                broadcast_dt=broadcast_dt,
                revised=_clean(keys.get("reviseddata")).upper() == "Y",
            )
        )
    if skipped:
        _log.warning("filings_shp_master_malformed_rows", skipped=skipped)
    return subs


# --------------------------------------------------------------------------- BSE detail (LIVE-VERIFIED)
# Real field names (live 2026-07-16); defensive fallbacks kept for resilience to BSE tweaks.
_QTRID_ALIASES = ("qtrid", "qtr_id", "fld_quarterid", "quarterid", "qid")
_QTR_LABEL_ALIASES = ("qtr", "fld_qtrname", "fld_quartername", "fld_enddate", "quarter", "quarterdate", "date")
_CATEGORY_ALIASES = ("fld_shortname", "fld_shortcatg", "fld_category", "category", "categoryname")
_HOLDERS_ALIASES = ("fld_noofshareholders", "noofshareholders", "holders")
_SHARES_ALIASES = ("fld_totalnoofshares", "totalnoofshares", "noofshares", "shares")
_PCT_ALIASES = ("fld_totalpercentageof_a_b_c2", "fld_totalvotingrightspercent", "percentage", "pct")
_PLEDGED_SH_ALIASES = ("fld_pledgeencumberednoofshares", "fld_totalencumberednoofshares", "pledgedshares")
_PLEDGED_PCT_ALIASES = ("fld_pledgeencumberedpercentage", "fld_totalencumberedpercentage", "pledgedpct")
_LOCKED_ALIASES = ("fld_nooflockedinshares", "lockedshares", "noofshareslocked")

#: The detail category rows live in ``Table1`` (``Table``/``Table4`` are single meta rows); a fallback
#: scan covers a BSE result-set reshuffle without a code change.
_DETAIL_ROW_KEYS = ("Table1", "Table5", "Table")

#: The DECLARATION row (result set ``Table``) carries the quarter's disclosure timestamp
#: ``Fld_AuthoriseDate`` — the point-in-time key (§2.8 rule i) for backfill-path rows, where no
#: NSE-master broadcast is available. Alias fallbacks defensive like every other field.
_AUTHORISE_ALIASES = ("fld_authorisedate", "authorisedate", "fld_authorizedate")
_DECLARATION_ROW_KEYS = ("Table", "Table4", "Table2")


def declaration_broadcast_dt(payload: Any) -> datetime | None:
    """The detail payload's own ``Fld_AuthoriseDate`` (IST) — None when absent, NEVER invented."""
    for key in _DECLARATION_ROW_KEYS:
        for raw in _bse_table(payload, key):
            keys = {str(k).lower(): v for k, v in raw.items()}
            dt = _parse_dt(_first(keys, _AUTHORISE_ALIASES))
            if dt is not None:
                return dt
    return None


def _norm_qtrid(raw: Any) -> str | None:
    """BSE ``qtrid`` is a float (``130.0``) but the URL wants ``130`` — normalize to a bare int string."""
    s = _clean(raw)
    if not s:
        return None
    try:
        return str(int(float(s)))
    except ValueError:
        return s


def _month_end(d: date) -> date:
    nxt = date(d.year + (d.month == 12), (d.month % 12) + 1, 1)
    return nxt - timedelta(days=1)


def _quarter_end(raw: Any) -> date | None:
    """Quarter LABEL/date → the quarter-END date. "30 Jun 2026"/ISO → that date (already month-end);
    "June 2026"/"Jun-26" → the month-end (so it matches the SHP-master's ``date`` exactly)."""
    s = _clean(raw)
    for fmt in ("%d %b %Y", "%d-%b-%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    for fmt in ("%B %Y", "%b %Y", "%b-%y", "%B-%Y", "%b-%Y"):
        try:
            return _month_end(datetime.strptime(s, fmt).date())
        except ValueError:
            continue
    return None


def parse_shp_quarter_index(payload: Any) -> list[dict[str, Any]]:
    """BSE ``SHPQNewFormat/w`` → ``[{qtrid, qtr_end}]`` (live-verified: ``Table`` of ``qtrid``+``qtr``)."""
    out: list[dict[str, Any]] = []
    for raw in _bse_table(payload, "Table"):
        keys = {str(k).lower(): v for k, v in raw.items()}
        qtrid = _norm_qtrid(_first(keys, _QTRID_ALIASES))
        qtr_end = _quarter_end(_first(keys, _QTR_LABEL_ALIASES))
        if qtrid is None or qtr_end is None:
            continue
        out.append({"qtrid": qtrid, "qtr_end": qtr_end})
    return out


def qtrid_for(quarter_index: list[dict[str, Any]], qtr_end: date) -> str | None:
    """The ``qtrid`` whose quarter-end matches ``qtr_end`` (the BSE detail-fetch key, §2.8)."""
    for entry in quarter_index:
        if entry.get("qtr_end") == qtr_end:
            return entry.get("qtrid")
    return None


def _detail_rows(payload: Any) -> list[dict[str, Any]]:
    """The category result set (``Table1``; ``Table``/``Table4`` are meta) — the first with categories."""
    for key in _DETAIL_ROW_KEYS:
        rows = _bse_table(payload, key)
        if rows and any(
            _first({str(k).lower(): v for k, v in r.items()}, _CATEGORY_ALIASES) for r in rows
        ):
            return rows
    return []


def parse_shp_detail(
    payload: Any, *, symbol: str, qtr_end: date, broadcast_dt: datetime | None, revised: bool
) -> list[dict[str, Any]]:
    """BSE ``CorporatesSHPSecuritybeta/w`` → ``shp_quarterly`` rows, one per category (live-verified:
    the ``Table1`` SEBI table). A row without a category label is skipped. ``source='bse'``.

    ``broadcast_dt=None`` (the backfill path — quarter loops have no NSE-master timestamp) falls
    back to the payload's OWN declaration-row ``Fld_AuthoriseDate`` (§2.8 rule i); still-None rows
    are stored with NULL broadcast_dt and excluded by point-in-time consumers — never invented."""
    if broadcast_dt is None:
        broadcast_dt = declaration_broadcast_dt(payload)
    rows: list[dict[str, Any]] = []
    for raw in _detail_rows(payload):
        keys = {str(k).lower(): v for k, v in raw.items()}
        category = _clean(_first(keys, _CATEGORY_ALIASES))
        if not category:
            continue
        rows.append(
            {
                "symbol": symbol,
                "qtr_end": qtr_end,
                "category": category,
                "holders": _int(_first(keys, _HOLDERS_ALIASES)),
                "shares": _int(_first(keys, _SHARES_ALIASES)),
                "pct": _flt(_first(keys, _PCT_ALIASES)),
                "pledged_shares": _int(_first(keys, _PLEDGED_SH_ALIASES)),
                "pledged_pct": _flt(_first(keys, _PLEDGED_PCT_ALIASES)),
                "locked_shares": _int(_first(keys, _LOCKED_ALIASES)),
                "broadcast_dt": broadcast_dt,
                "source": SOURCE_BSE,
                "revised": revised,
            }
        )
    return rows


class FilingsShpJob:
    """§2.8 job ``filings_shp`` — SHP + pledge history → ``shp_quarterly`` (run-latest, E5)."""

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

    async def run(self) -> FilingsShpResult:
        """Scan the NSE SHP-master for new submissions and fetch the BSE detail stack for each.
        Run-latest (§2.6): a single pass covers the gap. Never raises into the scheduler (E5)."""
        try:
            resp = await nse_get(self._http, NSE_SHP_MASTER_URL, timeout=self._timeout)
            submissions = parse_shp_master(json.loads(resp.content))
        except Exception as exc:  # noqa: BLE001 - E5: degrade + alert, never raise
            reason = f"master: {type(exc).__name__}: {exc}"
            _log.warning("filings_shp_master_failed", error=reason)
            await self._alert(reason)
            return FilingsShpResult(ok=False, degraded=True, reason=reason)

        watermark = await self._store.alatest_shp_broadcast()
        new = [s for s in submissions if watermark is None or s.broadcast_dt > watermark]
        isin_map = await self._store.asymbol_isin_map()

        skipped_no_scrip = failed = symbols_upserted = rows_written = 0
        bse_calls = 0
        for sub in new:
            mapping = isin_map.get(sub.symbol)
            code = _clean(mapping.get("bse_scrip_code")) if mapping else ""
            if not code:
                skipped_no_scrip += 1
                continue
            try:
                if bse_calls:
                    await _sleep(_BSE_SPACING_S)
                idx_resp = await bse_get(
                    self._http, BSE_SHP_QUARTER_INDEX_URL.format(code=code), timeout=self._timeout
                )
                bse_calls += 1
                quarter_index = parse_shp_quarter_index(json.loads(idx_resp.content))
                qtrid = qtrid_for(quarter_index, sub.qtr_end)
                if qtrid is None:
                    _log.info(
                        "filings_shp_quarter_not_found", symbol=sub.symbol, qtr_end=sub.qtr_end.isoformat()
                    )
                    continue
                await _sleep(_BSE_SPACING_S)
                detail_resp = await bse_get(
                    self._http,
                    BSE_SHP_DETAIL_URL.format(code=code, qtrid=qtrid),
                    timeout=self._timeout,
                )
                bse_calls += 1
                rows = parse_shp_detail(
                    json.loads(detail_resp.content),
                    symbol=sub.symbol, qtr_end=sub.qtr_end,
                    broadcast_dt=sub.broadcast_dt, revised=sub.revised,
                )
            except Exception as exc:  # noqa: BLE001 - E5: degrade THIS symbol only, never raise
                failed += 1
                _log.warning(
                    "filings_shp_detail_failed", symbol=sub.symbol, error=f"{type(exc).__name__}: {exc}"
                )
                continue
            if rows:
                written = await self._store.arun(self._store.upsert_shp_quarterly, rows)
                rows_written += written
                symbols_upserted += 1

        degraded = bool(failed or skipped_no_scrip)
        if degraded and (failed or skipped_no_scrip):
            await self._alert(
                f"{failed} symbol(s) failed BSE detail fetch, {skipped_no_scrip} skipped (no scrip code)"
            )
        result = FilingsShpResult(
            ok=True,
            degraded=degraded,
            submissions_seen=len(submissions),
            new_submissions=len(new),
            symbols_upserted=symbols_upserted,
            rows_written=rows_written,
            skipped_no_scrip=skipped_no_scrip,
            failed_symbols=failed,
        )
        _log.info(
            "filings_shp_ingested",
            seen=len(submissions), new=len(new), symbols=symbols_upserted,
            rows=rows_written, skipped_no_scrip=skipped_no_scrip, failed=failed,
        )
        return result

    async def _alert(self, reason: str) -> None:
        if self._notify is None:
            return
        msg = CatalogMessage(
            # Filings are NEVER load-bearing (§2.8 rule iii): warning, not entry-blocking.
            kind=MessageKind.DATA_FRESHNESS_FROZEN,
            title="Shareholding-pattern (SHP) feed degraded",
            body=(
                f"SHP ingest degraded: {reason}. Existing shp_quarterly rows remain; the run-latest "
                "catch-up retries (§2.8/E5). Not entry-blocking."
            ),
            severity="warning",
            data={"job_id": "filings_shp", "reason": reason},
        )
        try:
            await self._notify(msg)
        except Exception:  # noqa: BLE001 - best-effort alert; a failed send never propagates
            _log.exception("filings_shp_notify_failed")
