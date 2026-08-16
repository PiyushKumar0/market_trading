"""§2.8.2/§2.8.4 insider-crossing primitives — THE one definition, shared by the study and live.

These are the pure functions the §2.8.4 stage-2 study validated and that WO-16 (2026-08-14) re-ran
under corrected mechanics (disclosure-date anchoring, next-open fills, spread-inclusive costs):
T+10 **+0.7297%** / T+20 **+1.5797%** net — the only recorded edge that survived, and therefore the
evidence the §6.1 ``ins`` strategy leg is promoted from.

**Why this module exists (the §6.1 `ins` addendum's one-definition rule).** Until 2026-08-17 these
functions lived in ``scripts/event_study.py`` and :mod:`engine.datafeeds.filings_events` reached them
by path-loading that loose script at import time — a shim whose own docstring flagged that engine code
must not import a script at runtime once the library went live. ``ins`` makes it live: an EOD job now
computes crossings on the live decision path. So the primitives are PROMOTED here (dependency
inverted) and ``scripts/event_study.py`` imports them back. Both the validated study and the live
``ins_crossings`` job therefore execute the SAME bytes — a copy would drift, and a drifted live rule
would be trading a population the evidence never measured.

Nothing in this module was changed in the move: the code below is the stage-2/WO-16 text verbatim.
Anyone editing it is editing the validated rule and must re-run the study (§2.8.4 / WO-3 margin
floor), not just the unit tests.

**Standing WO-16 caveats (carried, never hidden — plan §6.1 `ins` addendum):**

* Live crossings are computable only from the **BSE fresh feed** (live since 2026-07-19, ~13-18
  in-universe rows/day); the NSE PIT feed's ~70-day content embargo makes it historical-only. The
  live-reachable event population is therefore NOT proven identical to the backtested one.
* The CPCV pass is boundary-exact (60.0% of folds against a 60% bar — one fold from failure).
* The survivorship / index-membership bound is uncorrectable with stored data.

``ins`` is the best-evidenced leg the platform has. It is not a proven money-printer.
"""

from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal

from engine.core.clock import IST

INSIDER_TRAILING_SESSIONS = 10          # §2.8.2 trailing-session window for the insider BUY value sum
MARKET_CLOSE_IST = time(15, 30)         # a filing broadcast after this is NOT knowable at that close

# §2.8.2 taxonomy — acq_mode values that are NOT open-market purchases and are excluded from the
# insider_net_buy aggregation. Case-insensitive SUBSTRING match (defensive: NSE varies the exact
# label — 'ESOP' / 'ESOPs' / 'ESOP Allotment' all contain 'esop'; 'Inter-se Transfer' vs 'Inter se
# Transfer'; 'Preferential Offer' vs 'Preferential Allotment' — the substrings below cover the family).
INSIDER_ACQ_MODE_EXCLUSIONS = (
    "esop", "gift", "inter-se", "inter se", "pledge invocation",
    "preferential", "rights", "bonus",
)


def to_ist(dt: datetime) -> datetime:
    """Coerce to tz-aware IST. Store rows are TIMESTAMPTZ (already IST); a naive dt is assumed IST.

    (Was ``event_study._to_ist``; made public in the 2026-08-17 promotion — ``scripts/filings_experiments.py``
    consumes it, so it is part of this module's contract, not an internal helper.)"""
    return dt.astimezone(IST) if dt.tzinfo is not None else dt.replace(tzinfo=IST)


# ------------------------------------------------------------------ §2.8.4 point-in-time entry mapping
def after_hours(broadcast_dt: datetime) -> bool:
    """A broadcast strictly after 15:30 IST was NOT knowable at that session's close (§2.8.4 PIT).

    **Midnight guard (WO-16).** Exchanges do not broadcast at 00:00:00, so an exact-midnight stamp
    means the TIME IS UNKNOWN, not that the filing was public before dawn: it is what the feeds'
    date-only parse fallbacks produce (``filings_pit._parse_dt``'s ``'%d-%b-%Y'`` branch,
    ``filings_pit_fresh._parse_dt``'s ``'%Y-%m-%d'`` branch) and what ``scripts/event_study.py``
    synthesises for date-only ``earnings_calendar`` fallback rows. Treating it as intraday would let
    a filing that was actually disseminated at 20:39 (the shape of every real PIT row — see
    ``tests/unit/fixtures/filings_pit.json``) enter at that same session's close: a one-session
    lookahead. So an unknown time is treated as AFTER hours — the conservative direction.
    """
    t = to_ist(broadcast_dt).time()
    if t == time(0, 0):
        return True
    return t > MARKET_CLOSE_IST


def entry_session_index(sessions: list[date], broadcast_dt: datetime) -> int | None:
    """Index into ascending ``sessions`` of the first session at whose CLOSE ``broadcast_dt`` was
    knowable (§2.8.4). After 15:30 IST ⇒ first session strictly AFTER the broadcast date; at/before
    15:30 ⇒ first session on/after it. ``None`` if no such session is in the series (filing past the
    last bar). This is the single PIT entry rule shared by the insider-buy, results and pledge legs.
    """
    bdate = to_ist(broadcast_dt).date()
    late = after_hours(broadcast_dt)
    for i, d in enumerate(sessions):
        if (d > bdate) if late else (d >= bdate):
            return i
    return None


def is_open_market_buy(txn_type: object, acq_mode: object) -> bool:
    """True iff an ``insider_trades`` row is an OPEN-MARKET buy (§2.8.2): ``txn_type`` == 'Buy' AND
    ``acq_mode`` is not one of the non-market modes (:data:`INSIDER_ACQ_MODE_EXCLUSIONS`). The acq_mode
    test is a case-insensitive substring; a blank/None acq_mode on a Buy defaults to open-market
    (a market purchase often carries no explicit mode label — defensive toward inclusion)."""
    if str(txn_type or "").strip().lower() != "buy":
        return False
    mode = str(acq_mode or "").strip().lower()
    return not any(excl in mode for excl in INSIDER_ACQ_MODE_EXCLUSIONS)


def is_open_market_sell(txn_type: object, acq_mode: object) -> bool:
    """Mirror of :func:`is_open_market_buy` for the SELL side (the filings_experiments E2 adverse-context
    leg). True iff ``txn_type`` == 'Sell' AND ``acq_mode`` is not one of the non-market modes
    (:data:`INSIDER_ACQ_MODE_EXCLUSIONS`). The SAME §2.8.2 exclusion taxonomy applies, so an
    ESOP-exercise SELL (acq_mode contains 'esop') is deliberately NOT counted as an open-market sell —
    an insider disposing of just-exercised ESOP stock is not the bearish open-market signal the adverse
    cohort is after. A blank/None acq_mode on a Sell defaults to open-market (symmetry with the buy
    side)."""
    if str(txn_type or "").strip().lower() != "sell":
        return False
    mode = str(acq_mode or "").strip().lower()
    return not any(excl in mode for excl in INSIDER_ACQ_MODE_EXCLUSIONS)


def insider_cluster_events(
    sessions: list[date], filings: list[dict], threshold: object, predicate=is_open_market_buy
) -> list[int]:
    """Session indices at which the trailing-``INSIDER_TRAILING_SESSIONS`` sum of the value of filings
    matching ``predicate`` first crosses ≥ ``threshold``, re-arming only after the trailing sum falls
    back below (one event per crossing). Generalizes the transaction SIDE via ``predicate``:
    :func:`is_open_market_buy` for the BUY cluster (the §2.8.4 ``insider_net_buy`` proxy),
    :func:`is_open_market_sell` for the SELL cluster (E2 adverse context).

    Pure. ``sessions`` = ascending session dates; ``filings`` = plain row dicts (``txn_type``,
    ``acq_mode``, ``value``, ``broadcast_dt``). Each eligible filing's value lands on its point-in-time
    knowable session (:func:`entry_session_index`); the crossing session is the entry T (close_T).
    Only the absolute ₹ floor is applied here — the §2.8.2 value/20d-ADV floor is deliberately NOT part
    of this proxy (see the module report's ambiguity note).

    **Re-arm state is PATH-DEPENDENT from ``sessions[0]``** (``armed`` starts True): a caller that
    passes a TRUNCATED session window can therefore re-detect a crossing the full-history study
    considered already-armed-down. The live ``ins_crossings`` job runs a deliberately long window for
    exactly this reason — see :data:`engine.datafeeds.ins_crossings.LOOKBACK_DAYS`.
    """
    thr = Decimal(str(threshold))
    per_session = [Decimal("0")] * len(sessions)
    for f in filings:
        if not predicate(f.get("txn_type"), f.get("acq_mode")):
            continue
        value = f.get("value")
        bdt = f.get("broadcast_dt")
        if value is None or bdt is None:
            continue
        v = Decimal(str(value))
        if v <= 0:
            continue
        si = entry_session_index(sessions, bdt)
        if si is None:
            continue
        per_session[si] += v
    events: list[int] = []
    armed = True
    for i in range(len(sessions)):
        lo = max(0, i - INSIDER_TRAILING_SESSIONS + 1)
        trailing = sum(per_session[lo:i + 1], Decimal("0"))
        if armed and trailing >= thr:
            events.append(i)
            armed = False
        elif not armed and trailing < thr:
            armed = True
    return events


def insider_buy_events(sessions: list[date], filings: list[dict], threshold: object) -> list[int]:
    """Session indices T at which the trailing-``INSIDER_TRAILING_SESSIONS`` sum of open-market insider
    BUY value first crosses ≥ ``threshold`` (§2.8.2/§2.8.4). Re-arms only after the trailing sum falls
    back below the threshold, so one event per crossing. Thin wrapper over
    :func:`insider_cluster_events` with the open-market BUY predicate (kept as the pinned §2.8.4 API)."""
    return insider_cluster_events(sessions, filings, threshold, is_open_market_buy)


__all__ = [
    "INSIDER_ACQ_MODE_EXCLUSIONS",
    "INSIDER_TRAILING_SESSIONS",
    "MARKET_CLOSE_IST",
    "after_hours",
    "entry_session_index",
    "insider_buy_events",
    "insider_cluster_events",
    "is_open_market_buy",
    "is_open_market_sell",
    "to_ist",
]
