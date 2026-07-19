"""BSE FRESH insider disclosures -> ``insider_trades`` (§2.8 job ``filings_pit_fresh``, 19:00 - stage 3).

The NSE PIT feed (:mod:`engine.datafeeds.filings_pit`) is structurally embargoed ~70 days (re-verified
2026-07-19: boundary 2026-05-10), so it is the deep HISTORICAL backbone but useless for SAME-DAY
origination. BSE ``getCorp_Regulation_ng/w`` (found 2026-07-19 via browser network capture, plan §2.8
source table) serves the SAME structured PIT rows fresh on the disclosure day, with a full
``Fld_CreateDate`` broadcast timestamp. This feed pulls those rows into the SAME ``insider_trades``
table the NSE feed writes, DATA ONLY (no decision path touched — the §2.8.2 event typing / §2.7
catalyst wiring stay behind §8.6; this feed only makes the fresh rows AVAILABLE).

**Two-surface fetch (probe-verified, plan §2.8):**
  * ``Isdefault=1`` -> the rolling ~100-row / ~4-week LATEST view (one call, no date filter) — the
    daily-pull surface.
  * ``Isdefault=2&fromDT=YYYYMMDD&ToDate=YYYYMMDD`` -> a date-filtered belt-and-braces narrow sweep
    over ``[d-3, d]``. This surface is HARD-CAPPED at ~25 rows/call and a wide range silently returns
    ``{}`` (no pagination). So: if the window call returns >= 25 rows (cap likely hit), the window is
    SUBDIVIDED per-day and each day refetched. A per-day call that STILL returns 25 is logged (a
    single market-day with >25 equity disclosures across the whole market — rare; unrecoverable
    without pagination the endpoint does not offer).

The two surfaces overlap heavily; rows are deduped on the content-hash ``id`` before the upsert.

**Source tagging — id PREFIX, not a column (decided by store shape, §2.8.1):** ``insider_trades`` has
NO ``source`` column and :meth:`MarketStore.init_schema` is ``CREATE TABLE IF NOT EXISTS`` only — there
is NO ALTER migration path, so adding a column to the DDL would NOT migrate the already-created live
table and would break the NSE feed's insert. BSE rows are therefore tagged by an ``id`` PREFIX
(``bse:``): NSE ids stay bare (byte-identical to the existing convention — back-compatible), BSE ids
never collide with NSE ids, and the downstream :mod:`engine.datafeeds.filings_events` recovers the
source from the prefix. See that module's ``row_source``.

**Equity filter (probe deviation, documented):** the brief said keep ``Fld_SecurityTypeName == 'Equity'``
rows, but the probe shows the equity value is overwhelmingly ``'Equity Shares'`` ('Equity' is 2/102 in
the Isdefault=1 capture) with non-equity ``'Any other instrument'`` / ``'Debentures'`` mixed in. A
literal ``== 'Equity'`` would drop ~98% of the real equity rows, so the filter is a case-insensitive
``startswith('equity')`` (keeps 'Equity' AND 'Equity Shares'; drops 'Any other instrument',
'Debentures', ...). Non-equity rows are skipped-and-counted.

Symbol resolution: ``Fld_ScripCode`` -> our symbol via the ``symbol_isin`` reverse map
(:meth:`MarketStore.bse_scrip_symbol_map`). BSE serves the WHOLE market, so an out-of-universe scrip is
EXPECTED and skipped-and-counted, never a failure. Parsing is defensive (E5) and the job never raises
into the scheduler; a fetch failure degrades + alerts (warning — filings are never entry-blocking,
§2.8 rule iii) and leaves existing rows in force for the date-keyed catch-up.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict

from engine.core.bse_http import bse_get
from engine.core.clock import IST, Clock
from engine.core.log import get_logger
from engine.datafeeds.filings_pit import insider_id
from engine.marketdata.store import MarketStore
from engine.notify.catalog import CatalogMessage, MessageKind

_log = get_logger("engine.datafeeds.filings_pit_fresh")

#: BSE fresh-insider endpoint (§2.8 source table; found 2026-07-19). [VERIFY Phase-1] — browser capture.
BSE_FRESH_URL = "https://api.bseindia.com/BseIndiaAPI/api/getCorp_Regulation_ng/w"

#: The Isdefault=2 date-filtered surface caps at ~25 rows/call (probe-verified: two narrow captures
#: returned EXACTLY 25). A window returning >= this is assumed truncated and subdivided per-day.
BSE_ROW_CAP = 25

#: Belt-and-braces narrow-sweep lookback (days) behind the run day for the Isdefault=2 surface.
FRESH_WINDOW_DAYS = 3

#: Source tag folded into the content-hash id (see module docstring). NSE rows stay bare. The prefix
#: is PUBLIC so :mod:`engine.datafeeds.filings_events` can recover the source from a row's id.
BSE_SOURCE = "bse"
BSE_ID_PREFIX = "bse:"

#: Fld_TransactionType -> canonical txn_type. Unmapped values pass through verbatim (the probe also
#: carries 'Pledge' / 'Pledge Released' rows the brief's Acquisition/Disposal/Revoke map omits — kept
#: as-is so is_open_market_buy (which only matches 'Buy') correctly ignores them downstream).
_TXN_TYPE_MAP = {"acquisition": "Buy", "disposal": "Sell", "revoke": "Revoke"}

NotifySink = Callable[[CatalogMessage], Awaitable[None]]


def fresh_url(*, scrip: str = "", regulation: str = "", frm: str = "", to: str = "", isdefault: int = 1) -> str:
    """BSE fresh-insider URL. ``frm``/``to`` are ``YYYYMMDD`` (empty for the Isdefault=1 rolling view)."""
    return (
        f"{BSE_FRESH_URL}?scripCode={scrip}&Regulation={regulation}"
        f"&fromDT={frm}&ToDate={to}&Isdefault={isdefault}"
    )


def bse_insider_id(
    symbol: str, person_name: str, broadcast_dt: datetime | None,
    txn_type: str, qty: int | None, value: Decimal | None,
) -> str:
    """Source-tagged content-hash id: the filings_pit :func:`insider_id` hash with a ``bse:`` prefix so
    BSE rows never collide with the NSE feed's bare ids and the source is recoverable from the id."""
    return BSE_ID_PREFIX + insider_id(symbol, person_name, broadcast_dt, txn_type, qty, value)


# --------------------------------------------------------------------------- defensive helpers
def _clean(raw: Any) -> str:
    return str(raw if raw is not None else "").strip()


def _parse_date(raw: Any) -> date | None:
    """BSE dates are ISO with a zeroed time (``2026-06-22T00:00:00``); plain forms kept as fallbacks."""
    s = _clean(raw)
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%d-%b-%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _parse_dt(raw: Any) -> datetime | None:
    """``Fld_CreateDate`` broadcast timestamp -> tz-aware IST (§3.2 stdlib parse). ISO with optional
    fractional seconds (``2026-06-23T20:30:11.843``); never invented when unparseable."""
    s = _clean(raw)
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%d-%b-%Y %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=IST)
        except ValueError:
            continue
    return None


def _dec(raw: Any) -> Decimal | None:
    s = _clean(raw).replace(",", "")
    if not s or s in ("-", "NA"):
        return None
    try:
        return Decimal(s)
    except InvalidOperation:
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


def _norm_scrip(raw: Any) -> str:
    """Bare-int-string scrip code (mirrors ``store._norm_scrip_code``) so the payload code matches the
    ``symbol_isin`` reverse map key regardless of int/str/``'.0'`` formatting."""
    s = _clean(raw)
    if not s:
        return ""
    try:
        return str(int(float(s)))
    except ValueError:
        return s


def _is_equity(sec_type_name: Any) -> bool:
    """Equity-instrument filter: case-insensitive ``startswith('equity')`` — keeps 'Equity' AND the
    dominant 'Equity Shares', drops 'Any other instrument' / 'Debentures' / warrants (see docstring)."""
    return _clean(sec_type_name).lower().startswith("equity")


def _map_txn_type(raw: Any) -> str:
    s = _clean(raw)
    return _TXN_TYPE_MAP.get(s.lower(), s)


def _rows_of(payload: Any) -> list[dict[str, Any]]:
    """Rows under the BSE ``Table`` envelope (or a bare list); ``{}`` / ``{"Table": []}`` -> ``[]``."""
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    if isinstance(payload, dict):
        for key in ("Table", "data", "rows", "records"):
            value = payload.get(key)
            if isinstance(value, list):
                return [r for r in value if isinstance(r, dict)]
    return []


@dataclass(frozen=True)
class PitFreshParse:
    """One payload's parse outcome: ``insider_trades`` rows + skip tallies (all skips EXPECTED, E5).

    ``raw_rows`` is the pre-filter Table length — the cap detector keys off it (>= ``BSE_ROW_CAP`` ⇒
    the window is truncated and must be subdivided)."""

    rows: list[dict[str, Any]]
    raw_rows: int
    skipped_non_equity: int
    skipped_unmapped_scrip: int
    skipped_no_broadcast: int


def parse_pit_fresh(payload: Any, scrip_map: Mapping[str, str]) -> PitFreshParse:
    """BSE ``getCorp_Regulation_ng/w`` JSON -> ``insider_trades`` rows (defensive, skip-and-count).

    Field map (probe-verified, §2.8): Fld_PromoterName->person_name; Fld_PersonCatgName->person_category;
    ModeOfAquisation->acq_mode; Fld_TransactionType Acquisition/Disposal/Revoke->Buy/Sell/Revoke (others
    pass through); Fld_SecurityNo->qty; Fld_SecurityValue->value; Fld_PercentofShareholdingPre/Post->
    before/after %; Fld_FromDate/ToDate->txn window; Fld_DateIntimation->intim_dt; **Fld_CreateDate->
    broadcast_dt** (the point-in-time key); xbrlurl->xbrl. Resolution needs ``scrip_map`` (the id keys on
    the resolved symbol), so it is folded in here — one place for the equity/scrip/timestamp skip-count.

    Skip precedence (a row failing several counts under the first): non-equity, then unmapped scrip,
    then unparseable ``Fld_CreateDate`` (never mis-key a point-in-time row)."""
    raw = _rows_of(payload)
    rows: list[dict[str, Any]] = []
    skipped_non_equity = skipped_unmapped = skipped_no_key = 0
    for r in raw:
        keys = {str(k).lower(): v for k, v in r.items()}
        if not _is_equity(keys.get("fld_securitytypename")):
            skipped_non_equity += 1
            continue
        symbol = scrip_map.get(_norm_scrip(keys.get("fld_scripcode")))
        if not symbol:
            skipped_unmapped += 1
            continue
        broadcast_dt = _parse_dt(keys.get("fld_createdate"))
        if broadcast_dt is None:
            skipped_no_key += 1
            continue
        person_name = _clean(keys.get("fld_promotername")) or None
        txn_type = _map_txn_type(keys.get("fld_transactiontype"))
        qty = _int(keys.get("fld_securityno"))
        value = _dec(keys.get("fld_securityvalue"))
        rows.append(
            {
                "id": bse_insider_id(symbol, person_name or "", broadcast_dt, txn_type, qty, value),
                "symbol": symbol,
                "person_name": person_name,
                "person_category": _clean(keys.get("fld_personcatgname")) or None,
                "acq_mode": _clean(keys.get("modeofaquisation")) or None,
                "txn_type": txn_type or None,
                "qty": qty,
                "value": value,
                "before_pct": _flt(keys.get("fld_percentofshareholdingpre")),
                "after_pct": _flt(keys.get("fld_percentofshareholdingpost")),
                "txn_from": _parse_date(keys.get("fld_fromdate")),
                "txn_to": _parse_date(keys.get("fld_todate")),
                "intim_dt": _parse_date(keys.get("fld_dateintimation")),
                "broadcast_dt": broadcast_dt,
                "xbrl": _clean(keys.get("xbrlurl")) or None,
            }
        )
    if skipped_no_key:
        _log.warning("filings_pit_fresh_no_broadcast", skipped=skipped_no_key)
    return PitFreshParse(
        rows=rows,
        raw_rows=len(raw),
        skipped_non_equity=skipped_non_equity,
        skipped_unmapped_scrip=skipped_unmapped,
        skipped_no_broadcast=skipped_no_key,
    )


class FilingsPitFreshResult(BaseModel):
    """One run's outcome (never an exception, E5). ``ok`` = at least one surface ingested;
    ``degraded`` = any surface/per-day fetch failed (existing rows stay in force)."""

    model_config = ConfigDict(frozen=True)

    d: date
    ok: bool
    degraded: bool = False
    failed_sources: tuple[str, ...] = ()
    rows_parsed: int = 0
    rows_written: int = 0
    windows_subdivided: int = 0
    skipped_non_equity: int = 0
    skipped_unmapped_scrip: int = 0
    skipped_no_broadcast: int = 0
    reason: str | None = None


class FilingsPitFreshJob:
    """§2.8 job ``filings_pit_fresh`` — BSE fresh insider disclosures -> ``insider_trades`` (date-keyed,
    19:00 IST, E5 never entry-blocking). Two surfaces (Isdefault=1 rolling + Isdefault=2 narrow sweep
    with per-day subdivision on the ~25-row cap); rows deduped on the content-hash id, upserted."""

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

    async def run(self, d: date) -> FilingsPitFreshResult:
        """Fetch both surfaces for run day ``d``, dedupe on id, upsert. Never raises (E5)."""
        scrip_map = await self._store.abse_scrip_symbol_map()
        by_id: dict[str, dict[str, Any]] = {}
        failed: list[str] = []
        reasons: list[str] = []
        subdivided = 0
        skips = [0, 0, 0]  # non_equity, unmapped, no_broadcast

        def absorb(parse: PitFreshParse) -> None:
            for row in parse.rows:
                by_id[row["id"]] = row
            skips[0] += parse.skipped_non_equity
            skips[1] += parse.skipped_unmapped_scrip
            skips[2] += parse.skipped_no_broadcast

        # Surface 1: Isdefault=1 rolling latest view (one call).
        parse, err = await self._fetch_parse(fresh_url(isdefault=1), scrip_map)
        if err is not None:
            failed.append("isdefault1")
            reasons.append(err)
        else:
            absorb(parse)

        # Surface 2: Isdefault=2 narrow window [d-3, d]; subdivide per-day on the ~25-row cap.
        frm = d - timedelta(days=FRESH_WINDOW_DAYS)
        parse, err = await self._fetch_parse(
            fresh_url(isdefault=2, frm=frm.strftime("%Y%m%d"), to=d.strftime("%Y%m%d")), scrip_map
        )
        if err is not None:
            failed.append("isdefault2")
            reasons.append(err)
        elif parse.raw_rows >= BSE_ROW_CAP:
            subdivided += 1
            _log.info("filings_pit_fresh_window_capped", frm=frm.isoformat(), to=d.isoformat(), raw=parse.raw_rows)
            day = frm
            while day <= d:
                key = day.strftime("%Y%m%d")
                pday, errd = await self._fetch_parse(fresh_url(isdefault=2, frm=key, to=key), scrip_map)
                if errd is not None:
                    reasons.append(errd)  # partial: a per-day gap, not a whole-surface failure
                else:
                    absorb(pday)
                    if pday.raw_rows >= BSE_ROW_CAP:
                        _log.warning("filings_pit_fresh_day_still_capped", d=day.isoformat(), raw=pday.raw_rows)
                day += timedelta(days=1)
        else:
            absorb(parse)

        rows = list(by_id.values())
        written = await self._store.arun(self._store.upsert_insider_trades, rows) if rows else 0
        degraded = bool(failed) or bool(reasons)
        if degraded:
            await self._alert(d, failed, reasons)
        result = FilingsPitFreshResult(
            d=d,
            ok=len(failed) < 2,
            degraded=degraded,
            failed_sources=tuple(failed),
            rows_parsed=len(rows),
            rows_written=written,
            windows_subdivided=subdivided,
            skipped_non_equity=skips[0],
            skipped_unmapped_scrip=skips[1],
            skipped_no_broadcast=skips[2],
            reason="; ".join(reasons) or None,
        )
        _log.info(
            "filings_pit_fresh_ingested", d=d.isoformat(), parsed=len(rows), written=written,
            subdivided=subdivided, skipped_non_equity=skips[0], skipped_unmapped=skips[1], failed=failed,
        )
        return result

    async def _fetch_parse(
        self, url: str, scrip_map: Mapping[str, str]
    ) -> tuple[PitFreshParse, str | None]:
        """GET + parse one URL under its own E5 guard. Returns ``(parse, None)`` or an empty parse +
        an error string; never raises."""
        try:
            resp = await bse_get(self._http, url, timeout=self._timeout)
            return parse_pit_fresh(json.loads(resp.content), scrip_map), None
        except Exception as exc:  # noqa: BLE001 - E5: degrade this surface, never raise
            detail = f"{type(exc).__name__}: {exc}"
            _log.warning("filings_pit_fresh_fetch_failed", url=url, error=detail)
            return PitFreshParse([], 0, 0, 0, 0), detail

    async def _alert(self, d: date, failed: list[str], reasons: list[str]) -> None:
        if self._notify is None:
            return
        msg = CatalogMessage(
            # Filings are NEVER load-bearing (§2.8 rule iii): a fresh-events feed, warning severity.
            kind=MessageKind.DATA_FRESHNESS_FROZEN,
            title="Fresh insider (BSE) feed degraded",
            body=(
                f"BSE fresh-insider fetch degraded on {d.isoformat()}: "
                f"failed={failed or 'none'} ({'; '.join(reasons)}). Existing insider_trades rows remain "
                "in force; the date-keyed catch-up retries the missed day (§2.8/E5). Not entry-blocking."
            ),
            severity="warning",
            data={"job_id": "filings_pit_fresh", "d": d.isoformat(), "failed_sources": failed},
        )
        try:
            await self._notify(msg)
        except Exception:  # noqa: BLE001 - best-effort alert; a failed send never propagates
            _log.exception("filings_pit_fresh_notify_failed")


# --------------------------------------------------------------------------- one-shot runner (--once)
async def _run_once(d: date | None = None) -> FilingsPitFreshResult:
    from engine.core.config import load_settings

    settings = load_settings()
    clock = Clock()
    store = MarketStore.from_settings(settings, clock).open()
    run_day = d or clock.today()
    async with httpx.AsyncClient() as http:
        try:
            return await FilingsPitFreshJob(store, clock, http).run(run_day)
        finally:
            store.close()


def main(argv: list[str] | None = None) -> int:
    from engine.core.log import configure_logging

    configure_logging()
    parser = argparse.ArgumentParser(description="§2.8 BSE fresh-insider feed -> insider_trades.")
    parser.add_argument("--once", action="store_true", help="run one ingest now against the live store")
    parser.add_argument(
        "--date", default=None, type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        help="run day YYYY-MM-DD (default: today IST)",
    )
    args = parser.parse_args(argv)
    if not args.once:
        parser.error("nothing to do -- pass --once to run one ingest")
    result = asyncio.run(_run_once(args.date))
    # ASCII only (this prints to a Windows console that may be cp1252).
    print(
        f"filings_pit_fresh d={result.d} ok={result.ok} degraded={result.degraded} "
        f"parsed={result.rows_parsed} written={result.rows_written} subdivided={result.windows_subdivided} "
        f"skipped(non_equity={result.skipped_non_equity} unmapped={result.skipped_unmapped_scrip} "
        f"no_broadcast={result.skipped_no_broadcast})"
    )
    if result.reason:
        print(f"  degraded_reason: {result.reason}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
