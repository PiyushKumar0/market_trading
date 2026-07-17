"""NSE PIT insider trades → ``insider_trades`` (§2.8 job ``filings_pit``, ~18:35 — O14, E5).

Pulls structured insider/PIT disclosures from the NSE ``corporates-pit`` API and persists them to the
DuckDB ``insider_trades`` table (§2.8.1). DATA ONLY in stage 1 — no decision path is touched (the
§2.8.2 event typing / §2.7 catalyst wiring is a later stage, gated on the §2.8.4 event study).

**Point-in-time discipline (§2.8 rule i):** every row carries the exchange broadcast timestamp
(``broadcast_dt``, minute-granular ``date`` field) — every downstream as-of join keys on THAT, never
the ``txn_from``/``txn_to`` period. The PK is a sha256 content hash of
(symbol, person_name, broadcast_dt, txn_type, qty, value) so an amended/duplicate broadcast of the
same transaction collapses to one row (latest wins) and a genuinely different transaction never
collides (§2.8 edge cases).

**Endpoint quirk (probe-verified):** ``corporates-pit`` returns an EMPTY ``data`` list unless BOTH
``from_date`` and ``to_date`` (DD-MM-YYYY) are supplied — so this feed ALWAYS passes an explicit
window. The daily job's window is ``[watermark → run-day]`` where the watermark is the latest stored
``broadcast_dt`` (a fresh store fetches just the run-day; the deep 3y history is the backfill's job).

Parsing is defensive (E5): the row list is under the ``data`` wrapper key, field names are matched
case-insensitively with aliases, and a row missing a symbol OR an unparseable broadcast timestamp is
skipped-and-counted. On fetch failure: alert + leave existing rows in force (the §2.6 date-keyed
catch-up retries the missed day). Never raises into the scheduler.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Awaitable, Callable
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict

from engine.core.clock import IST, Clock
from engine.core.log import get_logger
from engine.core.nse_http import nse_get
from engine.marketdata.store import MarketStore
from engine.notify.catalog import CatalogMessage, MessageKind

_log = get_logger("engine.datafeeds.filings_pit")

#: NSE PIT insider-trades API (equities). [VERIFY Phase-1]; anti-bot [likely] (cookie-gated www host).
NSE_PIT_URL = "https://www.nseindia.com/api/corporates-pit"

NotifySink = Callable[[CatalogMessage], Awaitable[None]]


def pit_url(frm: date, to: date, *, symbol: str | None = None) -> str:
    """PIT URL for the explicit ``[frm, to]`` window (DD-MM-YYYY; the endpoint is EMPTY without it)."""
    url = f"{NSE_PIT_URL}?index=equities&from_date={frm:%d-%m-%Y}&to_date={to:%d-%m-%Y}"
    if symbol:
        url += f"&symbol={symbol}"
    return url


class FilingsPitResult(BaseModel):
    """One run's outcome (never an exception, E5)."""

    model_config = ConfigDict(frozen=True)

    ok: bool
    degraded: bool = False
    frm: date | None = None
    to: date | None = None
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
    """Exchange broadcast timestamp → tz-aware IST datetime (stdlib parse, §3.2 — never the LLM)."""
    s = _clean(raw)
    for fmt in ("%d-%b-%Y %H:%M:%S", "%d-%b-%Y %H:%M", "%d-%b-%Y"):
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
    d = _dec(raw)
    return int(d) if d is not None else None


def _flt(raw: Any) -> float | None:
    s = _clean(raw).replace(",", "")
    if not s or s == "-":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _first_present(*values: Any) -> Any:
    """First non-None value — NOT ``or`` chaining (a legit ``Decimal(0)``/``0`` is falsy but present,
    e.g. a gift's ``secVal='0'`` is a real zero-consideration value, not 'missing')."""
    for value in values:
        if value is not None:
            return value
    return None


def insider_id(
    symbol: str, person_name: str, broadcast_dt: datetime | None,
    txn_type: str, qty: int | None, value: Decimal | None,
) -> str:
    """Content-hash PK: sha256 of (symbol, person_name, broadcast_dt, txn_type, qty, value) (§2.8.1).

    Stable across re-broadcasts of the SAME disclosure (dedupe) and distinct across genuinely
    different transactions. ``broadcast_dt`` is rendered ISO (minute-granular) — the point-in-time key.
    """
    parts = [
        symbol,
        person_name,
        broadcast_dt.isoformat() if broadcast_dt is not None else "",
        txn_type,
        "" if qty is None else str(qty),
        "" if value is None else str(value),
    ]
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def parse_pit(payload: Any) -> list[dict[str, Any]]:
    """NSE PIT JSON → ``insider_trades`` row dicts (defensive, skip-and-count).

    Field map (probe-verified, §2.8): symbol; acqName→person_name; personCategory→person_category;
    acqMode→acq_mode; tdpTransactionType→txn_type; secAcq→qty (securities acquired/disposed);
    secVal→value; befAcqSharesPer/afterAcqSharesPer→before/after %; acqfromDt/acqtoDt→txn window;
    intimDt→intim_dt; date→broadcast_dt (the point-in-time timestamp); xbrl. A row without a symbol
    or a parseable broadcast timestamp is skipped (never mis-key a point-in-time row).
    """
    rows: list[dict[str, Any]] = []
    skipped = 0
    for raw in _rows_of(payload):
        keys = {str(k).lower(): v for k, v in raw.items()}
        symbol = _clean(keys.get("symbol")).upper()
        broadcast_dt = _parse_dt(keys.get("date"))
        if not symbol or broadcast_dt is None:
            skipped += 1
            continue
        person_name = _clean(keys.get("acqname"))
        txn_type = _clean(keys.get("tdptransactiontype"))
        qty = _first_present(
            _int(keys.get("secacq")), _int(keys.get("buyquantity")), _int(keys.get("sellquantity"))
        )
        value = _first_present(
            _dec(keys.get("secval")), _dec(keys.get("buyvalue")), _dec(keys.get("sellvalue"))
        )
        rows.append(
            {
                "id": insider_id(symbol, person_name, broadcast_dt, txn_type, qty, value),
                "symbol": symbol,
                "person_name": person_name or None,
                "person_category": _clean(keys.get("personcategory")) or None,
                "acq_mode": _clean(keys.get("acqmode")) or None,
                "txn_type": txn_type or None,
                "qty": qty,
                "value": value,
                "before_pct": _flt(keys.get("befacqsharesper")),
                "after_pct": _flt(keys.get("afteracqsharesper")),
                "txn_from": _parse_date(keys.get("acqfromdt")),
                "txn_to": _parse_date(keys.get("acqtodt")),
                "intim_dt": _parse_date(keys.get("intimdt")),
                "broadcast_dt": broadcast_dt,
                "xbrl": _clean(keys.get("xbrl")) or None,
            }
        )
    if skipped:
        _log.warning("filings_pit_malformed_rows", skipped=skipped)
    return rows


class FilingsPitJob:
    """§2.8 job ``filings_pit`` — NSE PIT insider trades → ``insider_trades`` (date-keyed, E5)."""

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

    async def run(self, d: date) -> FilingsPitResult:
        """Fetch + upsert PIT filings over ``[watermark → d]`` (idempotent on the content-hash id).
        ``d`` is the run day (§2.6 date-keyed). Never raises into the scheduler (E5)."""
        watermark = await self._store.alatest_insider_broadcast()
        frm = min(watermark.date(), d) if watermark is not None else d
        try:
            resp = await nse_get(self._http, pit_url(frm, d), timeout=self._timeout)
            rows = parse_pit(json.loads(resp.content))
        except Exception as exc:  # noqa: BLE001 - E5: degrade + alert, never raise
            reason = f"{type(exc).__name__}: {exc}"
            _log.warning("filings_pit_fetch_failed", d=d.isoformat(), error=reason)
            await self._alert(d, reason)
            return FilingsPitResult(ok=False, degraded=True, frm=frm, to=d, reason=reason)

        written = await self._store.arun(self._store.upsert_insider_trades, rows)
        _log.info(
            "filings_pit_ingested", frm=frm.isoformat(), to=d.isoformat(),
            parsed=len(rows), written=written,
        )
        return FilingsPitResult(
            ok=True, frm=frm, to=d, rows_parsed=len(rows), rows_written=written
        )

    async def _alert(self, d: date, reason: str) -> None:
        if self._notify is None:
            return
        msg = CatalogMessage(
            # Filings are NEVER load-bearing (§2.8 rule iii, news-equal E5): a features/risk-context
            # feed, not a safety-critical one — closest closed-catalog kind, warning severity.
            kind=MessageKind.DATA_FRESHNESS_FROZEN,
            title="Insider-trades (PIT) feed degraded",
            body=(
                f"NSE PIT fetch failed on {d.isoformat()}: {reason}. Existing insider_trades rows "
                "remain in force; the date-keyed catch-up retries (§2.8/E5). Not entry-blocking."
            ),
            severity="warning",
            data={"job_id": "filings_pit", "d": d.isoformat(), "reason": reason},
        )
        try:
            await self._notify(msg)
        except Exception:  # noqa: BLE001 - best-effort alert; a failed send never propagates
            _log.exception("filings_pit_notify_failed")
