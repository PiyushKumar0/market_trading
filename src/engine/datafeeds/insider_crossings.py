"""Insider-crossing primitives — THE one definition, shared by the study and live.

These are the pure functions the stage-2 study validated (disclosure-date anchoring, next-open
fills, spread-inclusive costs): T+10 **+0.7297%** / T+20 **+1.5797%** net — the only recorded edge
that survived, and the evidence the ``ins`` strategy leg is promoted from.

**One-definition rule.** ``scripts/event_study.py`` imports these back, so the validated study and
the live ``ins_crossings`` job execute the SAME bytes — a copy would drift, and a drifted live rule
would trade a population the evidence never measured. The code is the validated stage-2 text:
anyone editing it is editing the validated rule and must re-run the study, not just the unit tests.

**Standing caveats (carried, never hidden):**

* Live crossings come from the **BSE fresh feed** (~13-18 in-universe rows/day) and NSE's PIT V2.0
  route (the old NSE route's ~70-day content embargo made NSE historical-only). The live-reachable
  event population is NOT proven identical to the backtested one.
* The CPCV pass is boundary-exact (60.0% of folds against a 60% bar — one fold from failure).
* The survivorship / index-membership bound is uncorrectable with stored data.

``ins`` is the best-evidenced leg the platform has. It is not a proven money-printer.
"""

from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal

from engine.core.clock import IST

INSIDER_TRAILING_SESSIONS = 10          # trailing-session window for the insider BUY value sum
MARKET_CLOSE_IST = time(15, 30)         # a filing broadcast after this is NOT knowable at that close

# acq_mode values that are NOT open-market purchases, excluded from insider_net_buy. Case-insensitive
# SUBSTRING match because NSE varies the exact label ('ESOP' / 'ESOP Allotment', 'Inter-se' / 'Inter se').
INSIDER_ACQ_MODE_EXCLUSIONS = (
    "esop", "gift", "inter-se", "inter se", "pledge invocation",
    "preferential", "rights", "bonus",
)


def to_ist(dt: datetime) -> datetime:
    """Coerce to tz-aware IST. Store rows are TIMESTAMPTZ (already IST); a naive dt is assumed IST.

    Public: ``scripts/filings_experiments.py`` consumes it."""
    return dt.astimezone(IST) if dt.tzinfo is not None else dt.replace(tzinfo=IST)


# ------------------------------------------------------------------ point-in-time entry mapping
def after_hours(broadcast_dt: datetime) -> bool:
    """A broadcast strictly after 15:30 IST was NOT knowable at that session's close (PIT).

    **Midnight guard.** Exchanges do not broadcast at 00:00:00, so an exact-midnight stamp means the
    TIME IS UNKNOWN (the feeds' date-only parse fallbacks and ``scripts/event_study.py``'s date-only
    ``earnings_calendar`` rows produce it). Treating it as intraday would let a filing disseminated
    at 20:39 enter at that same session's close: a one-session lookahead. An unknown time is
    treated as AFTER hours — the conservative direction.
    """
    t = to_ist(broadcast_dt).time()
    if t == time(0, 0):
        return True
    return t > MARKET_CLOSE_IST


def entry_session_index(sessions: list[date], broadcast_dt: datetime) -> int | None:
    """Index into ascending ``sessions`` of the first session at whose CLOSE ``broadcast_dt`` was
    knowable. After 15:30 IST ⇒ first session strictly AFTER the broadcast date; at/before
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
    """True iff an ``insider_trades`` row is an OPEN-MARKET buy: ``txn_type`` == 'Buy' AND
    ``acq_mode`` is not one of the non-market modes (:data:`INSIDER_ACQ_MODE_EXCLUSIONS`). The acq_mode
    test is a case-insensitive substring; a blank/None acq_mode on a Buy defaults to open-market
    (a market purchase often carries no explicit mode label — defensive toward inclusion)."""
    if str(txn_type or "").strip().lower() != "buy":
        return False
    mode = str(acq_mode or "").strip().lower()
    return not any(excl in mode for excl in INSIDER_ACQ_MODE_EXCLUSIONS)


def is_open_market_sell(txn_type: object, acq_mode: object) -> bool:
    """Mirror of :func:`is_open_market_buy` for the SELL side (RSI2 catalyst-veto adverse context).

    The SAME exclusions apply: an ESOP-exercise SELL is deliberately NOT an open-market sell, and a
    blank/None acq_mode defaults to open-market."""
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
    :func:`is_open_market_buy` for the BUY cluster (the ``insider_net_buy`` proxy),
    :func:`is_open_market_sell` for the SELL cluster (RSI2 catalyst-veto adverse context).

    Pure. ``sessions`` = ascending session dates; ``filings`` = plain row dicts (``txn_type``,
    ``acq_mode``, ``value``, ``broadcast_dt``). Each eligible filing's value lands on its point-in-time
    knowable session (:func:`entry_session_index`); the crossing session is the entry T (close_T).
    Only the absolute ₹ floor is applied; the value/20d-ADV floor is deliberately NOT part of this proxy.

    **Re-arm state is PATH-DEPENDENT from ``sessions[0]``** (``armed`` starts True): a TRUNCATED
    session window can re-detect a crossing the full-history study considered already-armed-down. The
    live job runs a long window for this reason — see
    :data:`engine.datafeeds.ins_crossings.LOOKBACK_DAYS`.
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
    BUY value first crosses ≥ ``threshold``. Re-arms only after the trailing sum falls
    back below the threshold, so one event per crossing. Thin wrapper over
    :func:`insider_cluster_events` with the open-market BUY predicate (kept as the pinned API)."""
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
