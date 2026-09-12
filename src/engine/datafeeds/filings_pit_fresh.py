"""BSE FRESH insider disclosures -> ``insider_trades`` (§2.8 job ``filings_pit_fresh``, 19:00 - stage 3).

The NSE PIT feed (:mod:`engine.datafeeds.filings_pit`) is structurally embargoed ~70 days (re-verified
2026-07-19: boundary 2026-05-10), so it is the deep HISTORICAL backbone but useless for SAME-DAY
origination. BSE ``getCorp_Regulation_ng/w`` (found 2026-07-19 via browser network capture, plan §2.8
source table) serves the SAME structured PIT rows fresh on the disclosure day, with a full
``Fld_CreateDate`` broadcast timestamp. This feed pulls those rows into the SAME ``insider_trades``
table the NSE feed writes, DATA ONLY (no decision path touched — the §2.8.2 event typing / §2.7
catalyst wiring stay behind §8.6; this feed only makes the fresh rows AVAILABLE).

**Two-surface fetch (re-measured 2026-09-12, plan §2.8 source table + §2.8.5):**
  * ``Isdefault=1`` -> the LATEST view (one call, no date filter) — the daily-pull surface, and the
    one that CARRIES THE DAY. It is a SAME-DAY view, NOT the "rolling ~100-row / ~4-week" window
    first recorded on 2026-07-19: the 09-12 capture was 124 rows / 72 distinct scrips with every
    ``Fld_CreateDate`` stamped the run day (one 06-23 straggler aside). A day this surface misses is
    therefore NOT recoverable from it later.
  * ``Isdefault=2&fromDT=YYYYMMDD&ToDate=YYYYMMDD`` -> a date-filtered narrow sweep over ``[d-3, d]``.
    Because Isdefault=1 is same-day only, this is the ONLY missed-day recovery path (the date-keyed
    catch-up's whole purpose), not belt-and-braces. It is HARD-CAPPED at ~25 rows/call and a wide
    range silently returns ``{}`` (no pagination). So: if the window call returns >= 25 rows (cap
    likely hit), the window is SUBDIVIDED per-day and each day refetched. A per-day call that STILL
    returns 25 is logged — and that is NOT rare: ``filings_pit_fresh_day_still_capped`` has fired on
    3-4 per-day calls EVERY trading day since the warning shipped 2026-08-13. The cap therefore
    BOUNDS what a catch-up can recover, and the bound is real, not theoretical (§2.8.5).

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

Symbol resolution: ``Fld_ScripCode`` -> our symbol. BSE serves the WHOLE market, so an
out-of-universe scrip is EXPECTED and skipped-and-counted, never a failure. Parsing is defensive (E5)
and the job never raises into the scheduler; a fetch failure degrades + alerts (warning — filings are
never entry-blocking, §2.8 rule iii) and leaves existing rows in force for the date-keyed catch-up.

**The resolution map is rebuilt EVERY run (2026-09-12 starvation fix, plan §2.8).** It used to be the
``symbol_isin`` reverse map alone (:meth:`MarketStore.bse_scrip_symbol_map`) — a table nothing
refreshes: ``isin_map`` is not a scheduled job, so the map stayed at the 200 symbols the 2026-07-17
backfill seeded while O15 widened the eligible universe to 480 on 09-04. 281 of 480 eligible issuers
therefore had NO scrip code and every BSE row they filed was dropped as an unmapped scrip (measured
2026-09-11: 257 of ~298 fetched rows), which is why the §6.1 ``ins`` rule saw 22 issuers. So this feed
now builds its own map each run: the cached index-constituents CSV gives symbol->ISIN
(:func:`~engine.datafeeds.isin_map.load_constituents_isin`) and ONE BSE bulk-master call gives
ISIN->scrip code (:data:`~engine.datafeeds.isin_map.BSE_SCRIP_MASTER_URL`); the stored codes are
unioned in and WIN any collision (they are the PeerSmartSearch resolutions the map was built from —
verified 199/199 identical to the master on 2026-09-12). Measured coverage after the join: 478 of 480
eligible symbols (the 2 misses, BSE and CDSL, are not BSE-listed at all).

A master leg that contributes nothing — it raised, or it answered 200 with a shape
:func:`~engine.datafeeds.isin_map.parse_scrip_master` cannot read — silently reverts the feed to that
pre-fix coverage, which is the exact failure that went unnoticed for two months. So it is NOT left to
the §6.1 ``ins_feed_coverage_low`` alarm: that alarm is computed over the 120-day CORPUS and will sit
well above its floor once this fix has widened the corpus, so it cannot see the regression. The map
stage owns its own detection: ``filings_pit_fresh_scrip_map_degraded`` on every degraded run, the
per-stage counts on the run line, and ONE owner alert on the TRANSITION into degradation (latched,
cleared by the first healthy map — a 20-day catch-up replay therefore alerts once, not 20 times).
The run still returns ``ok=True``: rows keep flowing at the old coverage, and the ``Isdefault=2``
``[d-3, d]`` window re-covers a degraded day on the next three runs — cap-bounded, so partially —
where sinking the watermark would add an unbounded retry loop against the same BSE host that is
already failing. If a future measurement shows the cap swallowing most of that recovery, revisit the
trade (``ok=False`` here is safe: the upsert is idempotent on the content-hash id).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict

from engine.core.bse_http import bse_get
from engine.core.clock import IST, Clock
from engine.core.config import Settings
from engine.core.log import get_logger
from engine.datafeeds.filings_pit import insider_id
from engine.datafeeds.insider_crossings import is_open_market_buy
from engine.datafeeds.isin_map import (
    BSE_SCRIP_MASTER_URL,
    load_constituents_isin,
    parse_scrip_master,
)
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

#: Floor on the scrip-master read timeout: that response is ~1.7 MB (5,004 rows) where the insider
#: surfaces are a few hundred KB, so it must not inherit a per-surface timeout tuned for them.
SCRIP_MASTER_TIMEOUT_S = 45.0

#: Quiet period after a FAILED scrip-master fetch before another is attempted. This job is DATE_KEYED,
#: so a boot after a multi-day stop replays one run per missed day back-to-back; without this a BSE
#: outage would be met with one 1.7 MB attempt (45 s x 3 tries) per replayed day, back-to-back, with
#: no spacing — the shape that makes BSE answer with ``error_Bse.html`` in the first place. Short
#: enough that the 19:00 scheduled run still retries hours after a morning catch-up burst.
SCRIP_MASTER_RETRY_COOLDOWN_S = 900.0

#: The scrip-map COVERAGE FLOOR (2026-09-12 review): the share of index constituents that must end
#: up with a correct scrip code (from either leg) for the map to count as healthy. Measured 478/480
#: on 2026-09-12; a bulk master that BSE truncates (the documented `{}` / ~25-row shapes of its
#: sibling surfaces) can still parse to hundreds of pairs, so a presence check (`not master`) would
#: certify a map that covers a tenth of the universe as healthy — the two-month-silent shape again.
MAP_COVERAGE_FLOOR_PCT = 0.90
#: ...with an ABSOLUTE allowance: the constituents that can never resolve (NSE-only names and dead
#: scrips — BSE, CDSL, DUMMYHEG on 2026-09-12) are a handful regardless of universe size, so the floor
#: tolerates the larger of 10% of the universe and this many names. Without it a small universe (or
#: a test fixture) with one legitimate NSE-only name would read as truncated.
MAP_UNRESOLVED_ALLOWANCE = 5

#: Source tag folded into the content-hash id (see module docstring). NSE rows stay bare. The prefix
#: is PUBLIC so :mod:`engine.datafeeds.filings_events` can recover the source from a row's id.
BSE_SOURCE = "bse"
BSE_ID_PREFIX = "bse:"

#: Fld_TransactionType -> canonical txn_type. Unmapped values pass through verbatim (the probe also
#: carries 'Pledge' / 'Pledge Released' rows the brief's Acquisition/Disposal/Revoke map omits — kept
#: as-is so is_open_market_buy (which only matches 'Buy') correctly ignores them downstream).
_TXN_TYPE_MAP = {"acquisition": "Buy", "disposal": "Sell", "revoke": "Revoke"}

#: ``ScripMap.cause`` -> the sentence the owner alert carries. Each leg sits on a different host with
#: a different fix, so the alert must name the one that failed rather than point at BSE by default.
_MAP_CAUSE_DETAIL = {
    "symbol_isin_read": "the stored symbol_isin read failed (duckdb unreadable or locked)",
    "constituents_csv": (
        "the cached index-constituents CSV gave no symbol->ISIN rows (the download cache AND the "
        "committed seed were unreadable or empty), so the join had nothing to run against"
    ),
    "bulk_master": (
        "the BSE bulk scrip master fetch failed or was cooling down after an earlier failure "
        "({symbols} constituents were waiting on it)"
    ),
    "bulk_master_empty": (
        "the BSE bulk scrip master answered but parsed to nothing (master_rows={master_rows} over "
        "{symbols} constituents) - SCRIP_CD/ISIN_NUMBER have most likely been renamed"
    ),
    "map_coverage_low": (
        "the rebuilt map covers only {covered} of {symbols} constituents (floor {floor_pct}% or "
        "{allowance} unresolved names, whichever is looser; master_rows={master_rows}) - the bulk "
        "master or the constituents CSV came back truncated"
    ),
}

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
        v = Decimal(s)
    except InvalidOperation:
        return None
    # A bare ``NaN`` token survives ``json.loads`` and constructs a Decimal fine, but every ORDERING
    # comparison on it raises InvalidOperation — the ins-eligible tally (and insider_cluster_events'
    # `v <= 0`) would take that raise out of the E5 guard. Non-finite is "no consideration".
    return v if v.is_finite() else None


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
class ScripMap:
    """One run's ``scrip_code -> symbol`` resolution map, with the coverage it was built from.

    ``symbols``, ``unresolved`` and ``shadowed`` are counted over the INDEX-CONSTITUENT CSV (499
    names), not the eligible universe: ``unresolved`` is the constituents that got no code from
    either source — 3 on 2026-09-12, all of them NSE-only or dead scrips, which is the expected
    residue, not a defect.

    ``master_rows`` is the ISIN->code relation the BULK MASTER itself yielded and ``added_codes`` the
    subset of it that was new to the map: they are separate numbers because a healthy master can
    legitimately add nothing (every constituent already stored), while ``master_rows == 0`` with
    constituents present means the leg is BROKEN — the field-rename failure mode that would silently
    revert this feed to its pre-fix coverage.

    ``shadowed`` counts constituents the master DID code but whose code a STORED row already holds
    for a DIFFERENT symbol. Stored-wins is the right precedence, but the loser must not vanish: a
    stale ``symbol_isin`` row (that table has no refresh job) shadows the correct symbol permanently,
    and every filing by that issuer is then written under a symbol §6.1 ``ins`` drops as
    out-of-universe. A non-zero count is the ONE signal that separates "a stale stored row" from
    "the market filed nothing" — without it the run line would certify full resolution while the
    issuer starved. Two CONSTITUENTS sharing one ISIN (a dual listing, a rename window) are a
    different thing and are counted apart as ``duplicate_isin`` — they say nothing about the store.

    ``covered`` is the number of constituents that ended up with THEIR OWN code from either leg —
    the one number that measures what the rebuild contributes. ``degraded`` is a FLOOR on it
    (:data:`MAP_COVERAGE_FLOOR_PCT`, loosened by :data:`MAP_UNRESOLVED_ALLOWANCE` names for the
    NSE-only residue), not a presence bit: a master that parsed to nothing AND a
    master that parsed to a truncated few hundred pairs both leave the feed at the pre-2026-09-12
    coverage, and only the first of those is ``not master``. ``cause`` names WHICH leg (a key of
    :data:`_MAP_CAUSE_DETAIL`, ``""`` when healthy) — the legs live on different hosts with
    different fixes, and a zero count alone cannot tell them apart.
    """

    by_scrip: dict[str, str]
    stored_codes: int
    master_rows: int
    added_codes: int
    symbols: int
    unresolved: int
    shadowed: int
    degraded: bool
    cause: str = ""
    covered: int = 0
    duplicate_isin: int = 0


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
    # Per-stage funnel (2026-09-12, plan §2.8): ``rows_fetched`` and ``rows_resolved`` are PER-FETCH
    # SUMS over the overlapping surfaces, exactly like the three skip tallies above, so the stages
    # close: fetched = non_equity + unmapped + no_broadcast + resolved. ``rows_parsed`` is the count
    # AFTER the content-hash dedupe, which is why it is smaller than ``rows_resolved``.
    rows_fetched: int = 0
    rows_resolved: int = 0
    rows_ins_eligible: int = 0   # ...of the deduped rows, those the §6.1 `ins` rule can ACT on
    scrip_codes: int = 0
    scrip_stored_codes: int = 0       # ...of them, from the unrefreshed `symbol_isin` table
    scrip_master_rows: int = 0        # ISIN->code pairs the BULK master yielded (0 ⇒ that leg broke)
    scrip_symbols_unresolved: int = 0
    scrip_shadowed: int = 0           # constituents whose code a DIFFERENT stored symbol already held
    scrip_added_codes: int = 0        # codes the master join contributed on top of the stored map
    scrip_covered: int = 0            # constituents holding their own code from either leg
    scrip_map_degraded: bool = False


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
        settings: Settings,
        notify: NotifySink | None = None,
        request_timeout_s: float = 20.0,
    ) -> None:
        self._store = store
        self._clock = clock
        self._http = http
        #: Required, not optional: the scrip map's symbol->ISIN half comes from the cached index CSV
        #: this locates. An optional settings would let a wiring slip silently halve the feed's
        #: issuer coverage — which is the exact failure this job is being fixed for.
        self._settings = settings
        self._notify = notify
        self._timeout = float(request_timeout_s)
        #: Per-date alert dedup (2026-08-13, mirrors bhavcopy): keyed on ``d``; ``_alert`` can fire on a
        #: ``degraded`` run that still has ``ok=True`` (a per-day subdivision gap with no full-surface
        #: failure), so the dedup guards the call site, not the ``ok`` field. Discarded whenever a run
        #: for ``d`` is fully clean (re-arms for a later streak).
        self._alerted: set[date] = set()
        #: Today's bulk ISIN->scrip master, fetched ONCE per IST day and reused across runs. The job
        #: is DATE_KEYED and a boot after a multi-day stop replays one run per missed day through
        #: THIS instance; the master is a today-view of BSE's active list, identical on every
        #: replayed date, so a 30-day replay must cost one 1.7 MB fetch, not thirty.
        self._master: tuple[date, dict[str, str]] | None = None
        #: Suppress-until stamp after a FAILED master fetch (see SCRIP_MASTER_RETRY_COOLDOWN_S).
        self._master_retry_after: datetime | None = None
        #: Map-degradation latch: the owner is alerted on the TRANSITION into degradation, not once
        #: per run or per replayed date. Cleared by the first healthy map — a latching cause needs a
        #: symmetric clear (2026-09-01 catchup_safety_jobs lesson) or the alarm never re-arms.
        self._map_degraded = False

    async def run(self, d: date) -> FilingsPitFreshResult:
        """Fetch both surfaces for run day ``d``, dedupe on id, upsert. Never raises (E5)."""
        smap = await self._scrip_map()
        scrip_map = smap.by_scrip
        by_id: dict[str, dict[str, Any]] = {}
        failed: list[str] = []
        reasons: list[str] = []
        subdivided = 0
        # Every surface's parse, kept so the run's stage tallies are summed in ONE place rather than
        # accumulated by hand at each absorb site (the funnel has to close arithmetically).
        absorbed: list[PitFreshParse] = []

        def absorb(parse: PitFreshParse) -> None:
            absorbed.append(parse)
            for row in parse.rows:
                by_id[row["id"]] = row

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
        fetched = sum(p.raw_rows for p in absorbed)
        resolved = sum(len(p.rows) for p in absorbed)
        non_equity = sum(p.skipped_non_equity for p in absorbed)
        unmapped = sum(p.skipped_unmapped_scrip for p in absorbed)
        no_broadcast = sum(p.skipped_no_broadcast for p in absorbed)
        # The §6.1 `ins` rule only ever aggregates open-market BUYs, so "rows ingested" overstates
        # what the rule can act on by ~an order of magnitude. Counted on the DEDUPED rows and with
        # the rule's own predicate — never a local copy of the taxonomy, which would drift. The
        # value test mirrors what insider_cluster_events does AFTER the predicate passes (it skips
        # `value is None` and `value <= 0`): BSE serves blank / '-' / 'NA' / '0.00' considerations,
        # and a counter that includes them would read "6 eligible" on a day the rule can act on none
        # — overstating on exactly the shape that would cause a silent starvation.
        ins_eligible = sum(
            1 for r in rows
            if is_open_market_buy(r["txn_type"], r["acq_mode"])
            and r["value"] is not None and r["value"] > 0
        )
        degraded = bool(failed) or bool(reasons)
        if degraded:
            if d not in self._alerted:  # dedup: don't storm on every retry of a still-failing day
                await self._alert(d, failed, reasons)
                self._alerted.add(d)
        else:
            self._alerted.discard(d)  # a clean run for d re-arms the alert for a later streak
        result = FilingsPitFreshResult(
            d=d,
            ok=len(failed) < 2,
            degraded=degraded,
            failed_sources=tuple(failed),
            rows_parsed=len(rows),
            rows_written=written,
            windows_subdivided=subdivided,
            skipped_non_equity=non_equity,
            skipped_unmapped_scrip=unmapped,
            skipped_no_broadcast=no_broadcast,
            reason="; ".join(reasons) or None,
            rows_fetched=fetched,
            rows_resolved=resolved,
            rows_ins_eligible=ins_eligible,
            scrip_codes=len(scrip_map),
            scrip_stored_codes=smap.stored_codes,
            scrip_master_rows=smap.master_rows,
            scrip_symbols_unresolved=smap.unresolved,
            scrip_shadowed=smap.shadowed,
            scrip_added_codes=smap.added_codes,
            scrip_covered=smap.covered,
            scrip_map_degraded=smap.degraded,
        )
        # THE feed-side starvation line (plan §2.8, 2026-09-12): every stage of the funnel on one
        # line, so "the market filed nothing we care about" and "we dropped it at stage N" are
        # distinguishable without a DB query. fetched = non_equity + unmapped + no_broadcast +
        # resolved (per-fetch sums over overlapping surfaces); parsed/written/ins_eligible are after
        # the id dedupe; the scrip_* fields are the resolution map the run was able to build.
        _log.info(
            "filings_pit_fresh_ingested", d=d.isoformat(), fetched=fetched, parsed=len(rows),
            written=written, subdivided=subdivided, skipped_non_equity=non_equity,
            skipped_unmapped=unmapped, skipped_no_broadcast=no_broadcast, resolved=resolved,
            ins_eligible=ins_eligible, scrip_codes=len(scrip_map), scrip_stored=smap.stored_codes,
            scrip_master=smap.master_rows, scrip_added=smap.added_codes, scrip_symbols=smap.symbols,
            scrip_unresolved=smap.unresolved, scrip_shadowed=smap.shadowed,
            scrip_covered=smap.covered, scrip_duplicate_isin=smap.duplicate_isin,
            scrip_map_degraded=smap.degraded, failed=failed,
        )
        await self._report_scrip_map(d, smap)
        return result

    async def _report_scrip_map(self, d: date, smap: ScripMap) -> None:
        """Own the map stage's failure modes instead of leaving them to §6.1 ``ins_feed_coverage_low``
        (that alarm reads the 120-day CORPUS and goes quiet once this fix widens it, so it cannot see
        a later regression of THIS stage).

        A degraded map warns on every run and alerts ONCE on the transition into degradation; the
        latch clears on the first healthy map, so a 20-day catch-up replay produces one alert and a
        recovery re-arms it. Shadowed codes warn without alerting: the feed is still ingesting, but
        some issuers are being written under a stale symbol §6.1 ``ins`` will drop.
        """
        if smap.shadowed:
            _log.warning(
                "filings_pit_fresh_scrip_codes_shadowed", d=d.isoformat(), shadowed=smap.shadowed,
                stored=smap.stored_codes, symbols=smap.symbols,
            )
        if smap.duplicate_isin:
            # Two constituents on one ISIN: a CSV fact (dual listing / rename window), not a stale
            # store — its own line so the shadowed count keeps its one meaning.
            _log.info("filings_pit_fresh_duplicate_isin", d=d.isoformat(),
                      duplicate_isin=smap.duplicate_isin, symbols=smap.symbols)
        if not smap.degraded:
            self._map_degraded = False   # symmetric clear: the alarm must re-arm after a recovery
            return
        _log.warning(
            "filings_pit_fresh_scrip_map_degraded", d=d.isoformat(), cause=smap.cause,
            codes=len(smap.by_scrip), stored=smap.stored_codes, master=smap.master_rows,
            symbols=smap.symbols, covered=smap.covered,
        )
        if self._map_degraded:
            return
        self._map_degraded = True
        # Name WHICH leg failed: the causes have different fixes (a locked store, a missing universe
        # cache, an unreachable/renamed BSE endpoint), and an alert that pointed at BSE for a missing
        # CSV would send the reader to the wrong host.
        detail = _MAP_CAUSE_DETAIL.get(smap.cause, smap.cause or "unknown").format(
            master_rows=smap.master_rows, symbols=smap.symbols, covered=smap.covered,
            floor_pct=int(MAP_COVERAGE_FLOOR_PCT * 100), allowance=MAP_UNRESOLVED_ALLOWANCE,
        )
        await self._notify_msg(CatalogMessage(
            # Filings are NEVER load-bearing (§2.8 rule iii) ⇒ warning, like the fetch-degraded alert.
            kind=MessageKind.DATA_FRESHNESS_FROZEN,
            title="Fresh insider (BSE) feed: scrip map degraded",
            body=(
                f"filings_pit_fresh could not rebuild its scrip->symbol map on {d.isoformat()}: "
                f"{detail}. The feed has fallen back to the {smap.stored_codes} stored symbol_isin "
                "codes, which is the coverage that starved §6.1 `ins` to 22 of 480 issuers before "
                "2026-09-12 (plan §2.8.5). Rows keep ingesting and nothing is entry-blocking. One "
                "alert per degradation streak — a healthy map re-arms it."
            ),
            severity="warning",
            data={"job_id": "filings_pit_fresh", "d": d.isoformat(), "cause": smap.cause,
                  "scrip_codes": len(smap.by_scrip), "scrip_master_rows": smap.master_rows,
                  "scrip_symbols": smap.symbols},
        ))

    async def _scrip_map(self) -> ScripMap:
        """Build this run's ``scrip_code -> symbol`` map: stored codes ∪ (index CSV ⋈ BSE master).

        Never raises (E5): every leg — the store read included — is inside the guard, and a failure
        degrades to whatever half is in hand. Precedence is STORED-WINS on a scrip key: those codes
        were resolved per-symbol by ``PeerSmartSearch`` and verified identical to the master's
        (199/199, 2026-09-12), so a disagreement would be a master surprise, and the verified source
        must not be silently overwritten by it — but the loser is COUNTED (``shadowed``), because the
        other reading of a disagreement is a stale ``symbol_isin`` row shadowing the right symbol.
        """
        stored: Mapping[str, str] = {}
        isin_by_symbol: Mapping[str, str] = {}
        # Which leg is in flight, so a degraded map names the half that failed rather than being
        # inferred from a zero count (`symbols == 0` is produced by BOTH a missing CSV and a store
        # read that raised before the CSV was reached — different hosts, different fixes).
        leg = "symbol_isin_read"
        try:
            # The store read and the CSV read are inside the guard too: duckdb can fail on a locked
            # file, and a corrupt cache raises shapes load_constituents_isin does not catch
            # (csv.Error). This job's contract is degrade-never-raise, so nothing above may escape.
            stored = await self._store.abse_scrip_symbol_map()
            leg = "constituents_csv"
            isin_by_symbol = load_constituents_isin(self._settings)
            if not isin_by_symbol:
                # No symbol->ISIN half ⇒ nothing to join the master to; do not spend the request.
                _log.warning("filings_pit_fresh_constituents_missing")
                return ScripMap(
                    by_scrip=dict(stored), stored_codes=len(stored), master_rows=0, added_codes=0,
                    symbols=0, unresolved=0, shadowed=0, degraded=True, cause=leg,
                )
            leg = "bulk_master"
            master = await self._bulk_master()
        except Exception as exc:  # noqa: BLE001 - E5: degrade to the stored map, never raise
            _log.warning(
                # Not ``..._master_failed``: this guard now also covers the store read and the CSV,
                # and ``leg`` is what says which one. A name that always said "master" would send
                # the reader to BSE for a locked duckdb file.
                "filings_pit_fresh_scrip_map_failed", leg=leg, error=f"{type(exc).__name__}: {exc}"
            )
            return ScripMap(
                by_scrip=dict(stored), stored_codes=len(stored), master_rows=0, added_codes=0,
                symbols=len(isin_by_symbol), unresolved=0, shadowed=0, degraded=True, cause=leg,
            )

        by_scrip = dict(stored)
        held_symbols = set(stored.values())
        added = unresolved = shadowed = duplicate_isin = covered = 0
        for symbol in sorted(isin_by_symbol):  # sorted: a collision resolves the same way every run
            code = _norm_scrip(master.get(isin_by_symbol[symbol]))
            if not code:
                # An index symbol the BSE master does not list is NSE-only or not a live scrip at
                # all (2026-09-12: BSE, CDSL, DUMMYHEG of 499) — not a resolution failure, since it
                # can never file a BSE disclosure in the first place. A stored code still covers it.
                if symbol in held_symbols:
                    covered += 1
                else:
                    unresolved += 1
                continue
            held = by_scrip.get(code)
            if held is None:
                by_scrip[code] = symbol
                added += 1
                covered += 1
            elif held == symbol:
                covered += 1
            elif code in stored:
                # Stored wins, but the loss is not silent: see ScripMap.shadowed.
                shadowed += 1
            else:
                # The holder was added by THIS loop: two constituents on one ISIN (2026-09-12
                # review) — a CSV fact, never a stale-store signal, so it must not read as shadowed.
                duplicate_isin += 1
        # A master that yielded NO usable pairs while constituents were present is a BROKEN leg (a
        # renamed SCRIP_CD/ISIN_NUMBER answers 200 and parses to {}), not an empty market; a master
        # that parsed to a TRUNCATED few hundred pairs is the same regression through a door the
        # presence bit cannot see. Both revert the feed toward the pre-2026-09-12 coverage, so the
        # detector is a FLOOR on covered constituents (MAP_COVERAGE_FLOOR_PCT), not `not master`.
        missing = len(isin_by_symbol) - covered
        tolerated = max(
            math.ceil((1.0 - MAP_COVERAGE_FLOOR_PCT) * len(isin_by_symbol)), MAP_UNRESOLVED_ALLOWANCE
        )
        if not master:
            cause = "bulk_master_empty"
        elif missing > tolerated:
            cause = "map_coverage_low"
        else:
            cause = ""
        return ScripMap(
            by_scrip=by_scrip, stored_codes=len(stored), master_rows=len(master), added_codes=added,
            symbols=len(isin_by_symbol), unresolved=unresolved, shadowed=shadowed,
            degraded=bool(cause), cause=cause, covered=covered, duplicate_isin=duplicate_isin,
        )

    async def _bulk_master(self) -> dict[str, str]:
        """Today's ``ISIN -> bse_scrip_code`` relation from the BSE bulk master — at most ONE fetch
        per IST day, and not again inside :data:`SCRIP_MASTER_RETRY_COOLDOWN_S` of a failure.

        Keyed on ``clock.today()``, not on the run day: the master is a live view of BSE's active
        list, identical for every date a catch-up replays, and a fresh key after midnight keeps the
        rebuild-every-day property the coverage fix rests on. Raises on a failed (or cooled-down)
        fetch — the caller's E5 guard turns that into a degraded map."""
        today = self._clock.today()
        if self._master is not None and self._master[0] == today:
            return self._master[1]
        now = self._clock.now()
        if self._master_retry_after is not None and now < self._master_retry_after:
            raise RuntimeError(f"scrip-master fetch cooling down until {self._master_retry_after.isoformat()}")
        try:
            resp = await bse_get(
                self._http, BSE_SCRIP_MASTER_URL, timeout=max(self._timeout, SCRIP_MASTER_TIMEOUT_S)
            )
            master = parse_scrip_master(json.loads(resp.content))
        except Exception:
            self._master_retry_after = now + timedelta(seconds=SCRIP_MASTER_RETRY_COOLDOWN_S)
            raise
        if not master:
            # A 200 that parsed to nothing is NOT memoised for the day (2026-09-12 review): BSE's
            # sibling surfaces are documented to answer `{}` / `[]` transiently, and a day-long memo
            # of that would pin the feed at the pre-fix coverage through the 19:00 run that follows
            # a healthy BSE. It takes the failure COOLDOWN instead — the pacing the memo was
            # reaching for — and the caller reads it as `bulk_master_empty`, degraded.
            self._master_retry_after = now + timedelta(seconds=SCRIP_MASTER_RETRY_COOLDOWN_S)
            return master
        self._master = (today, master)
        self._master_retry_after = None
        return master

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
        await self._notify_msg(msg)

    async def _notify_msg(self, msg: CatalogMessage) -> None:
        """Best-effort send: no sink wired, or a failing sink, never propagates out of a run (E5)."""
        if self._notify is None:
            return
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
            return await FilingsPitFreshJob(store, clock, http, settings=settings).run(run_day)
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
        f"fetched={result.rows_fetched} resolved={result.rows_resolved} parsed={result.rows_parsed} "
        f"written={result.rows_written} ins_eligible={result.rows_ins_eligible} "
        f"subdivided={result.windows_subdivided} "
        f"skipped(non_equity={result.skipped_non_equity} unmapped={result.skipped_unmapped_scrip} "
        f"no_broadcast={result.skipped_no_broadcast})"
    )
    print(
        f"  scrip_map codes={result.scrip_codes} stored={result.scrip_stored_codes} "
        f"master_rows={result.scrip_master_rows} unresolved_symbols={result.scrip_symbols_unresolved} "
        f"shadowed={result.scrip_shadowed} degraded={result.scrip_map_degraded}"
    )
    if result.reason:
        print(f"  degraded_reason: {result.reason}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
