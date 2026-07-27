"""``FilingsEventBuilder`` (§2.8.2) — PURE mapping of stored ``insider_trades`` rows to typed candidate
events. NO store writes, no catalyst-watchlist writing, no origination (all deferred to Phase 2's
``CatalystDigestJob`` behind the §8.6 owner gate). In Phase 1 this library is consumed ONLY by tests
and ``scripts/validate_insider.py``.

Scope (stage 3): the ONE event type §2.8.4 cleared — ``insider_net_buy`` (the trailing-session
open-market promoter/director BUY cluster, stage-2 verdict PASSED: T+10 net +0.75%, T+20 +1.61%,
n=110). ``pledge_delta`` / ``results_filing`` are NOT built here (stage-2 verdicts INCONCLUSIVE /
FAILED-as-origination — plan §2.8.4).

**Reuse, don't duplicate (plan §2.8.2 / brief):** the point-in-time entry mapping
(:func:`entry_session_index`), the open-market predicate (:func:`is_open_market_buy` + the pinned
``INSIDER_ACQ_MODE_EXCLUSIONS`` taxonomy) and the trailing-window re-arming crossing logic
(:func:`insider_cluster_events`) are the SAME functions ``scripts/event_study.py`` validated in
stage 2 — imported here VERBATIM (via the repo's established load-loose-script-by-path pattern, the
same one ``scripts/filings_experiments.py`` and ``tests/unit/test_event_study_filings.py`` use) so the
Phase-1 event set is byte-identical to the validated rule. The absolute-₹ floor is applied; the
§2.8.2 value/20d-ADV floor is deliberately NOT part of this proxy (matching the validated leg — see
event_study's ``insider_cluster_events`` note).

> Phase-2 note: when this library is wired into the live ``CatalystDigestJob``, the shared primitives
> should be promoted into the engine package (dependency inverted) rather than path-loaded from
> ``scripts/`` — engine code should not import a loose script at runtime. Flagged, not done here (a
> refactor of the stage-2 script + its 3 test consumers is out of this stage's scope).

**Cross-source dedup (§2.8 edge case):** the SAME disclosure surfaces first on the BSE fresh feed
(``source='bse'``, id-prefixed) and again ~70 days later on the NSE PIT structured feed
(``source='nse'``, bare id). Before clustering, a BSE row whose ``(symbol, txn_from, qty)`` already
appears from NSE is dropped — the NSE structured row supersedes the interim fresh one. Source is
recovered from the id prefix (:func:`row_source`); NSE never drops.
"""

from __future__ import annotations

import importlib.util
import sys
from collections import Counter, defaultdict
from collections.abc import Sequence
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from engine.datafeeds.filings_pit_fresh import BSE_ID_PREFIX, BSE_SOURCE

# --- reuse the stage-2 VALIDATED pure functions from scripts/event_study.py (import, never duplicate) ---
_ES_MODNAME = "mt_event_study"
if _ES_MODNAME in sys.modules:  # already loaded (e.g. by the event-study tests) — reuse the one instance
    _es = sys.modules[_ES_MODNAME]
else:  # pragma: no cover - path-load shim (exercised, but coverage of the branch depends on load order)
    _ES_PATH = Path(__file__).resolve().parents[3] / "scripts" / "event_study.py"
    _spec = importlib.util.spec_from_file_location(_ES_MODNAME, _ES_PATH)
    _es = importlib.util.module_from_spec(_spec)
    sys.modules[_ES_MODNAME] = _es
    _spec.loader.exec_module(_es)

is_open_market_buy = _es.is_open_market_buy
insider_cluster_events = _es.insider_cluster_events
entry_session_index = _es.entry_session_index
INSIDER_TRAILING_SESSIONS: int = _es.INSIDER_TRAILING_SESSIONS

SOURCE_NSE = "nse"


def row_source(row: dict[str, Any]) -> str:
    """The source of an ``insider_trades``-shaped row: an explicit ``source``/``_source`` key if
    present, else inferred from the id prefix (``bse:`` -> ``'bse'``, bare -> ``'nse'``)."""
    explicit = row.get("source") or row.get("_source")
    if explicit:
        return str(explicit).strip().lower()
    return BSE_SOURCE if str(row.get("id") or "").startswith(BSE_ID_PREFIX) else SOURCE_NSE


def _dedup_key(row: dict[str, Any]) -> tuple[str, Any, Any]:
    return (str(row.get("symbol") or "").upper(), row.get("txn_from"), row.get("qty"))


def dedup_cross_source(filings: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    """Drop BSE rows superseded by an NSE row on ``(symbol, txn_from, qty)`` (§2.8 edge case). NSE rows
    are never dropped; a BSE row with no NSE counterpart is kept. Order preserved."""
    nse_keys = {_dedup_key(f) for f in filings if row_source(f) == SOURCE_NSE}
    out: list[dict[str, Any]] = []
    for f in filings:
        if row_source(f) == BSE_SOURCE and _dedup_key(f) in nse_keys:
            continue
        out.append(f)
    return out


def insider_net_buy(
    filings: Sequence[dict[str, Any]],
    sessions: Sequence[date],
    *,
    min_value_inr: float | int | Decimal,
) -> list[dict[str, Any]]:
    """Typed ``insider_net_buy`` candidate events over ``insider_trades``-shaped ``filings`` and an
    ascending ``sessions`` calendar (§2.8.2). One event dict per trailing-window crossing::

        {symbol, event_session, broadcast_dt, trailing_value, contributing_filings_n,
         person_category_dominant}

    The crossing SET comes verbatim from :func:`insider_cluster_events` (the validated rule); the
    trailing_value / contributing / dominant-category fields are metadata computed around it. Cross-
    source dedup is applied first. Events are sorted by (symbol, event_session)."""
    deduped = dedup_cross_source(filings)
    by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for f in deduped:
        sym = str(f.get("symbol") or "").upper()
        if sym:
            by_symbol[sym].append(f)

    events: list[dict[str, Any]] = []
    for sym, sym_filings in by_symbol.items():
        crossings = insider_cluster_events(sessions, sym_filings, min_value_inr, is_open_market_buy)
        if not crossings:
            continue
        # Bucket the ELIGIBLE (open-market buy, value>0, knowable) filings by point-in-time session so
        # each crossing can report the value/count/dominant-category that produced it.
        buckets: dict[int, list[tuple[dict[str, Any], Decimal, datetime]]] = defaultdict(list)
        for f in sym_filings:
            if not is_open_market_buy(f.get("txn_type"), f.get("acq_mode")):
                continue
            value, bdt = f.get("value"), f.get("broadcast_dt")
            if value is None or bdt is None:
                continue
            dv = Decimal(str(value))
            if dv <= 0:
                continue
            si = entry_session_index(sessions, bdt)
            if si is not None:
                buckets[si].append((f, dv, bdt))

        for i in crossings:
            lo = max(0, i - INSIDER_TRAILING_SESSIONS + 1)
            contributing = [t for s in range(lo, i + 1) for t in buckets.get(s, [])]
            trailing_value = sum((t[1] for t in contributing), Decimal("0"))
            # The event's point-in-time stamp is the latest broadcast among the filings that landed ON
            # the crossing session (the ones that pushed the trailing sum over); fall back to the
            # latest contributing filing if none map exactly to i (defensive — a crossing implies one).
            on_i = buckets.get(i, [])
            broadcast_dt = max((t[2] for t in on_i), default=None) or max(
                (t[2] for t in contributing), default=None
            )
            cats = [str(t[0].get("person_category") or "").strip() for t in contributing]
            cats = [c for c in cats if c]
            events.append(
                {
                    "symbol": sym,
                    "event_session": sessions[i],
                    "broadcast_dt": broadcast_dt,
                    "trailing_value": trailing_value,
                    "contributing_filings_n": len(contributing),
                    "person_category_dominant": Counter(cats).most_common(1)[0][0] if cats else None,
                }
            )
    events.sort(key=lambda e: (e["symbol"], e["event_session"]))
    return events


class FilingsEventBuilder:
    """§2.8.2 event builder (PURE — no store writes). Stage 3 exposes only ``insider_net_buy`` (the
    one event type §2.8.4 cleared). ``insider_min_value_inr`` is the absolute-₹ floor
    (``settings.filings.insider_min_value_inr``, ₹1cr default)."""

    def __init__(self, *, insider_min_value_inr: float | int | Decimal) -> None:
        self._min_value_inr = insider_min_value_inr

    def insider_net_buy(
        self, filings: Sequence[dict[str, Any]], sessions: Sequence[date]
    ) -> list[dict[str, Any]]:
        """Typed ``insider_net_buy`` events (see the module-level :func:`insider_net_buy`)."""
        return insider_net_buy(filings, sessions, min_value_inr=self._min_value_inr)


__all__ = [
    "FilingsEventBuilder",
    "INSIDER_TRAILING_SESSIONS",
    "dedup_cross_source",
    "insider_net_buy",
    "is_open_market_buy",
    "row_source",
]
