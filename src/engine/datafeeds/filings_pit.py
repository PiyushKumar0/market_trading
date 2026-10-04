"""NSE insider trades → ``insider_trades`` (§2.8 job ``filings_pit``, ~18:35 — O14, E5).

**Two NSE routes.** Insider (PIT) disclosures moved to SEBI's PIT V2.0 XBRL filings on 2026-05-03 (found
2026-09-24 by capturing NSE's own insider-trading page). The old ``corporates-pit`` route
(:func:`pit_url`, :func:`parse_pit`) holds the history up to 2026-05-02 and only
``scripts/backfill_filings.py`` still walks it. The daily job reads ``corporates-pit-gg``
(:func:`pit_gg_url`): one listing row per FILING, with its trades in the filing's XBRL (one context per
disclosure, :func:`parse_pit_xbrl`). Only index constituents' XBRL is fetched, the same universe the
BSE fresh feed resolves to.

**Point-in-time discipline (§2.8 rule i):** ``broadcast_dt`` is the exchange dissemination time (the
listing's received time when that is missing; the two differ by at most 5 s), and every downstream
as-of join keys on it, never the ``txn_from``/``txn_to`` period. The PK is a sha256 content hash of
(symbol, person_name, broadcast_dt, txn_type, qty, value), so a re-ingested filing never duplicates.

**Both exchanges carry the same filing** (2026-09-24: all 19 NSE disclosures of 09-22/23 matched a BSE
fresh-feed row field for field, BSE 20 s to 3 min earlier). Each source's rows are stored as they
come; :meth:`MarketStore.get_insider_trades` pairs the two copies of a trade and keeps the earlier.

**Window.** Both routes return nothing without an explicit ``from_date``/``to_date`` (DD-MM-YYYY)
window. The daily job's window is ``[watermark - PIT_RETRY_DAYS → run-day]`` where the watermark is
the latest stored ``broadcast_dt`` **of this source's rows only** (§2.8.5, 2026-09-13 — the BSE fresh
feed writes into the same table every day and a whole-table watermark collapsed this window to
``[d-1, d]``), floored at :data:`MAX_WINDOW_DAYS` behind the run day and walked in
:data:`PIT_WINDOW_DAYS` chunks. A filing whose XBRL link already has stored rows is not fetched again.
History older than the floor is the backfill's job
(``scripts/backfill_filings.py seed --from <day> --skip-results --skip-integrated --skip-shp``).

**Failure model (E5).** A listing row without a symbol, broadcast time or XBRL link is skipped and
counted. A filing whose XBRL fails (fetch, parse or store) is skipped and counted, and fetched again on
later runs while it stays inside the retry margin, so one bad filing never stalls the rest. A failed
listing, or a run in which every XBRL fetch failed, alerts and returns a degraded result for the §2.6
date-keyed catch-up to retry. Never raises into the scheduler.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import xml.etree.ElementTree as ET
from collections.abc import Awaitable, Callable, Collection
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict

from engine.core.clock import IST, Clock
from engine.core.config import Settings
from engine.core.log import get_logger
from engine.core.nse_http import nse_get
from engine.datafeeds.isin_map import load_constituents_isin
from engine.marketdata.store import MarketStore
from engine.notify.catalog import CatalogMessage, MessageKind

_log = get_logger("engine.datafeeds.filings_pit")

#: Old NSE PIT insider-trades route: broadcasts up to 2026-05-02 only (history, backfill script).
NSE_PIT_URL = "https://www.nseindia.com/api/corporates-pit"

#: NSE PIT V2.0 filing listing (equities) — every insider filing from :data:`PIT_GG_FIRST_DAY` on.
NSE_PIT_GG_URL = "https://www.nseindia.com/api/corporates-pit-gg"
PIT_GG_FIRST_DAY = date(2026, 5, 3)

#: Days before the watermark each run lists again. A filing whose XBRL failed (not yet on the archive
#: host, a transient error, a malformed file) is fetched again on every run inside this margin; one
#: failure never stalls the filings behind it.
PIT_RETRY_DAYS = 7

#: This job's source tag in ``insider_trades`` — the bare-id rows (``filings_pit_fresh`` writes the
#: ``bse:``-tagged ones). Canonical: ``filings_events.SOURCE_NSE`` imports this directly — filings_events
#: already depends on this module transitively (filings_events -> filings_pit_fresh -> filings_pit), so
#: a direct import adds no new edge and cannot cycle.
NSE_SOURCE = "nse"

#: Widest span (days) ONE request asks for — the unit ``scripts/backfill_filings.py`` already walks
#: this SAME endpoint in (imported there as ``_NSE_WINDOW_DAYS``, §2.8 observed safe; scripts/ can
#: import src/, so the value is shared, not duplicated). NSE has never been observed to REFUSE a wider
#: window — the 2026-09-12 probe answered 01-05 → 12-09 (134 days) and its 3 rows came from the START
#: of that span, which argues against truncation without proving it — but a silent truncation to the
#: newest slice would lift the watermark past rows never served, and that hole has no alarm: the
#: floor below only fires on what it refuses to ASK for. So the daily job never asks for more than
#: the backfill does.
PIT_WINDOW_DAYS = 31

#: Listing requests ONE run may issue, hence the window-START floor (§2.8.5, 2026-09-13). The window
#: opens at the per-SOURCE NSE watermark, which widens by a day for every day the feed stores nothing —
#: unbounded without a floor. Window bounds are INCLUSIVE, so N chunks reach
#: ``N * PIT_WINDOW_DAYS - 1`` days behind the run day. A residue older than the floor is an owner-run
#: ``scripts/backfill_filings.py seed --from <watermark day> --skip-results --skip-integrated
#: --skip-shp``, announced by ``filings_pit_window_clamped``.
MAX_WINDOWS_PER_RUN = 6
MAX_WINDOW_DAYS = PIT_WINDOW_DAYS * MAX_WINDOWS_PER_RUN - 1

#: ≥1.5 s between consecutive requests to the cookie-gated www host (§2.8 observed safe). Public:
#: ``scripts/backfill_filings.py`` imports it (with :func:`pit_windows` and :data:`PIT_WINDOW_DAYS`)
#: to walk the same endpoint. ``_sleep`` is a module-level indirection so tests observe the pacing
#: without waiting (the ``engine.core.nse_http`` idiom).
PIT_PACE_S = 1.5
_sleep = asyncio.sleep

NotifySink = Callable[[CatalogMessage], Awaitable[None]]


def pit_url(frm: date, to: date, *, symbol: str | None = None) -> str:
    """Old-route PIT URL for the explicit ``[frm, to]`` window (DD-MM-YYYY; EMPTY without it)."""
    url = f"{NSE_PIT_URL}?index=equities&from_date={frm:%d-%m-%Y}&to_date={to:%d-%m-%Y}"
    if symbol:
        url += f"&symbol={symbol}"
    return url


def pit_gg_url(frm: date, to: date) -> str:
    """PIT V2.0 filing listing for filings broadcast in ``[frm, to]`` (DD-MM-YYYY)."""
    return f"{NSE_PIT_GG_URL}?index=equities&from_date={frm:%d-%m-%Y}&to_date={to:%d-%m-%Y}"


def pit_windows(frm: date, to: date, span_days: int = PIT_WINDOW_DAYS) -> list[tuple[date, date]]:
    """Ascending ≤``span_days`` windows covering ``[frm, to]`` inclusive (empty if ``frm > to``).

    Canonical implementation — ``scripts/backfill_filings.py`` imports this rather than keeping its
    own copy (scripts/ can import src/; the reverse cannot, which is why this lives here and not
    there). The two walk the SAME endpoint in the same unit, so a change here is a change for both.
    """
    out: list[tuple[date, date]] = []
    cur = frm
    while cur <= to:
        end = min(cur + timedelta(days=span_days - 1), to)
        out.append((cur, end))
        cur = end + timedelta(days=1)
    return out


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
    """Stringify + strip. Deliberately treats every falsy raw value (``None``, ``0``, ``""``, ``False``)
    as missing — NOT the same as ``filings_pit_fresh``'s own ``_clean``, which preserves a numeric ``0``
    (BSE's ``secVal='0'`` is a real zero-consideration value there); the two must not be merged without
    an explicit data decision on whether the NSE feed's numeric fields can legitimately carry a bare 0."""
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
        v = Decimal(s)
    except InvalidOperation:
        return None
    # A bare ``NaN`` (or ``Infinity``) token survives ``json.loads`` and constructs a Decimal fine,
    # but every ORDERING comparison on it raises InvalidOperation (int(Decimal('Infinity')) raises
    # OverflowError) — a raise that :func:`_int` and any downstream `v <= 0` would take out of the E5
    # guard. Non-finite is "no consideration" (folded in from filings_pit_fresh, 2026-09-23).
    return v if v.is_finite() else None


def _int(raw: Any) -> int | None:
    d = _dec(raw)
    return int(d) if d is not None else None


def _flt(raw: Any) -> float | None:
    s = _clean(raw).replace(",", "")
    if not s or s in ("-", "NA"):
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


@dataclass(frozen=True)
class PitFiling:
    """One ``corporates-pit-gg`` listing row: a filing whose trades are in its XBRL."""

    symbol: str
    broadcast_dt: datetime
    xbrl: str


def parse_pit_filings(payload: Any) -> list[PitFiling]:
    """PIT V2.0 listing JSON → filings, oldest first. A row without a symbol, a parseable broadcast
    time or an XBRL link is skipped and counted."""
    filings: list[PitFiling] = []
    skipped = 0
    for raw in _rows_of(payload):
        keys = {str(k).lower(): v for k, v in raw.items()}
        symbol = _clean(keys.get("symbol")).upper()
        broadcast_dt = _parse_dt(keys.get("exchdisstime")) or _parse_dt(keys.get("broadcastdatetime"))
        xbrl = _clean(keys.get("xmlfilename"))
        if not symbol or broadcast_dt is None or not xbrl:
            skipped += 1
            continue
        filings.append(PitFiling(symbol=symbol, broadcast_dt=broadcast_dt, xbrl=xbrl))
    if skipped:
        _log.warning("filings_pit_malformed_rows", skipped=skipped)
    return sorted(filings, key=lambda f: f.broadcast_dt)


def _pct(raw: Any) -> float | None:
    """PIT V2.0 holdings are fractions (0.4745 = 47.45%); stored as percent like the other feeds."""
    d = _dec(raw)
    return float(d * 100) if d is not None else None


def parse_pit_xbrl(content: bytes | str, filing: PitFiling) -> tuple[list[dict[str, Any]], int]:
    """One PIT V2.0 XBRL filing → (``insider_trades`` rows for its equity disclosures, count of
    non-equity disclosures skipped). Raises :class:`xml.etree.ElementTree.ParseError` on a malformed
    document."""
    facts_by_ctx: dict[str, dict[str, str]] = {}
    for el in ET.fromstring(content):
        ctx = el.get("contextRef")
        if ctx:
            facts_by_ctx.setdefault(ctx, {})[el.tag.rsplit("}", 1)[-1]] = (el.text or "").strip()
    rows: list[dict[str, Any]] = []
    non_equity = 0
    for facts in facts_by_ctx.values():
        if "NameOfThePerson" not in facts and "SecuritiesAcquiredOrDisposedTransactionType" not in facts:
            continue  # the filing-level context
        if not facts.get("TypeOfInstrument", "").lower().startswith("equity"):
            non_equity += 1
            continue
        person_name = facts.get("NameOfThePerson", "")
        txn_type = facts.get("SecuritiesAcquiredOrDisposedTransactionType", "")
        qty = _int(facts.get("SecuritiesAcquiredOrDisposedNumberOfSecurity"))
        value = _dec(facts.get("SecuritiesAcquiredOrDisposedValueOfSecurity"))
        rows.append(
            {
                "id": insider_id(filing.symbol, person_name, filing.broadcast_dt, txn_type, qty, value),
                "symbol": filing.symbol,
                "person_name": person_name or None,
                "person_category": facts.get("CategoryOfPerson") or None,
                "acq_mode": facts.get("ModeOfAcquisitionOrDisposal") or None,
                "txn_type": txn_type or None,
                "qty": qty,
                "value": value,
                "before_pct": _pct(
                    facts.get("SecuritiesHeldPriorToAcquisitionOrDisposalPercentageOfShareholding")
                ),
                "after_pct": _pct(  # the taxonomy's own spelling: "Acquistion"
                    facts.get("SecuritiesHeldPostAcquistionOrDisposalPercentageOfShareholding")
                ),
                "txn_from": _parse_date(
                    facts.get("DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyFromDate")
                ),
                "txn_to": _parse_date(
                    facts.get("DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyToDate")
                ),
                "intim_dt": _parse_date(facts.get("DateOfIntimationToCompany")),
                "broadcast_dt": filing.broadcast_dt,
                "xbrl": filing.xbrl,
            }
        )
    return rows, non_equity


@dataclass
class PitIngest:
    """Running tallies of an ingest — mutable, so a caller still holds them when the ingest raises."""

    filings: int = 0
    out_of_universe: int = 0
    already_ingested: int = 0
    xbrl_fetched: int = 0
    xbrl_failed: int = 0
    non_equity: int = 0
    rows: int = 0
    written: int = 0


async def ingest_pit_window(
    http: httpx.AsyncClient, store: MarketStore, frm: date, to: date, symbols: Collection[str],
    stats: PitIngest, *, done: Collection[str], timeout: float,
) -> None:
    """List the PIT V2.0 filings broadcast in ``[frm, to]`` and upsert each constituent filing's trades,
    oldest first. A filing whose XBRL link is in ``done`` (already stored) is not fetched again. A
    filing that fails is skipped and counted, and the listing continues; a failed listing raises."""
    resp = await nse_get(http, pit_gg_url(frm, to), timeout=timeout)
    for filing in parse_pit_filings(json.loads(resp.content)):
        stats.filings += 1
        if filing.symbol not in symbols:
            stats.out_of_universe += 1
            continue
        if filing.xbrl in done:
            stats.already_ingested += 1
            continue
        await _sleep(PIT_PACE_S)
        try:
            xml = await nse_get(http, filing.xbrl, timeout=timeout)
            rows, non_equity = parse_pit_xbrl(xml.content, filing)
            written = await store.arun(store.upsert_insider_trades, rows) if rows else 0
        except Exception as exc:  # noqa: BLE001 - one filing never stops the rest; retried next run
            stats.xbrl_failed += 1
            _log.warning("filings_pit_xbrl_failed", symbol=filing.symbol, xbrl=filing.xbrl,
                         error=f"{type(exc).__name__}: {exc}")
            continue
        stats.xbrl_fetched += 1
        stats.non_equity += non_equity
        stats.rows += len(rows)
        stats.written += written


class FilingsPitJob:
    """§2.8 job ``filings_pit`` — NSE PIT V2.0 insider trades → ``insider_trades`` (date-keyed, E5)."""

    def __init__(
        self,
        store: MarketStore,
        clock: Clock,
        http: httpx.AsyncClient,
        *,
        settings: Settings,
        notify: NotifySink | None = None,
        request_timeout_s: float = 20.0,
    ) -> None:
        self._store = store
        self._clock = clock
        self._http = http
        #: Locates the index-constituents CSV: only constituents' filings are fetched.
        self._settings = settings
        self._notify = notify
        self._timeout = float(request_timeout_s)
        #: Per-date alert dedup (2026-08-13, mirrors bhavcopy): with the watermark fix forwarding this
        #: job's ``ok`` through the composition root, the 30-min sweeps genuinely re-run a still-failing
        #: day — alert once per failing streak, not on every attempt.
        self._alerted: set[date] = set()
        #: Window-floor latch: WARNING on the TRANSITION into clamping, not once per run and not once
        #: per replayed date (a boot catch-up replays one run per missed day through THIS instance,
        #: and while the feed stores nothing the condition holds on every run). Cleared the moment the
        #: watermark is back inside the cap — a latching cause needs a symmetric clear (2026-09-01
        #: catchup_safety_jobs lesson) or the alarm never re-arms.
        self._clamped = False

    def _window_start(self, watermark: datetime | None, d: date) -> date:
        """Window start: the NSE watermark, capped at ``d``, less :data:`PIT_RETRY_DAYS`, floored at
        ``d - MAX_WINDOW_DAYS``.

        The CAP is what keeps a catch-up replaying an OLD day from inverting the window when stored
        rows run ahead of ``d`` — :func:`pit_windows` yields NOTHING for an inverted span, which would
        green that day's watermark on zero requests. The FLOOR bounds the run's request count.
        """
        frm = min(watermark.date(), d) if watermark is not None else d
        floor = d - timedelta(days=MAX_WINDOW_DAYS)
        if frm >= floor:
            self._clamped = False
            return max(frm - timedelta(days=PIT_RETRY_DAYS), floor)
        # Rows older than `floor` and newer than the watermark are NOT requested by this run, and no
        # later run reaches them either: a successful run lifts the watermark past the gap. Recovery
        # is `scripts/backfill_filings.py seed --from <watermark day> --skip-results --skip-integrated
        # --skip-shp`.
        log = _log.info if self._clamped else _log.warning
        log(
            "filings_pit_window_clamped", watermark=frm.isoformat(), frm=floor.isoformat(),
            to=d.isoformat(), uncovered_days=(floor - frm).days, cap_days=MAX_WINDOW_DAYS,
        )
        self._clamped = True
        return floor

    async def run(self, d: date) -> FilingsPitResult:
        """Ingest the PIT V2.0 filings broadcast over ``[frm → d]`` in ≤:data:`PIT_WINDOW_DAYS`
        chunks (:meth:`_window_start`), skipping filings already stored. Idempotent on the
        content-hash id; ``d`` is the run day (§2.6 date-keyed). Never raises into the scheduler (E5)
        — the constituents and watermark reads are inside the guard too, so they degrade like a
        fetch failure."""
        # `frm` stands at the run day until the watermark is read, so a store fault reports the
        # degenerate window it never got to widen; `windows_done=0` on the warning is what separates
        # "the watermark read failed" from "the first fetch failed".
        frm = d
        win: tuple[date, date] | None = None
        windows = 0
        stats = PitIngest()
        try:
            symbols = load_constituents_isin(self._settings).keys()
            if not symbols:
                raise RuntimeError("the index-constituents CSV gave no symbols")
            watermark = await self._store.alatest_insider_broadcast(source=NSE_SOURCE)
            frm = self._window_start(watermark, d)
            done = await self._store.arun(
                self._store.nse_insider_xbrls, datetime.combine(frm, datetime.min.time(), tzinfo=IST)
            )
            for win in pit_windows(frm, d):
                if windows:
                    await _sleep(PIT_PACE_S)  # never burst the cookie-gated www host (§2.8)
                await ingest_pit_window(
                    self._http, self._store, *win, symbols, stats, done=done, timeout=self._timeout
                )
                windows += 1
            if stats.xbrl_failed and not stats.xbrl_fetched:
                raise RuntimeError(f"all {stats.xbrl_failed} XBRL fetches failed")
        except Exception as exc:  # noqa: BLE001 - E5: degrade + alert, never raise
            reason = f"{type(exc).__name__}: {exc}"
            _log.warning(
                "filings_pit_fetch_failed", d=d.isoformat(), error=reason,
                window=f"{win[0].isoformat()}..{win[1].isoformat()}" if win is not None else None,
                windows_done=windows, **asdict(stats),
            )
            if d not in self._alerted:  # dedup: don't storm on every retry of a still-failing day
                await self._alert(d, reason)
                self._alerted.add(d)
            return FilingsPitResult(
                ok=False, degraded=True, frm=frm, to=d,
                rows_parsed=stats.rows, rows_written=stats.written, reason=reason,
            )

        self._alerted.discard(d)  # a success for d re-arms the alert (dedup is per failing streak)
        _log.info(
            "filings_pit_ingested", frm=frm.isoformat(), to=d.isoformat(), windows=windows, **asdict(stats)
        )
        return FilingsPitResult(
            ok=True, degraded=stats.xbrl_failed > 0, frm=frm, to=d,
            rows_parsed=stats.rows, rows_written=stats.written,
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
