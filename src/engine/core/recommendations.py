"""Shared recommendation-lifecycle predicate (§3.6).

Lives in ``core`` rather than ``ops.pipeline`` — home of the canonical consumer,
``RecommendationBook.expire_stale`` — because the other two consumers sit BELOW ``ops`` in the import
graph: the ``/taken`` ticker resolver (``notify.telegram``) and the gate's pending-entry check
(``risk.gate``). Neither package currently imports ``engine.ops`` at runtime, and ``ops.pipeline``
itself imports from ``notify`` — so having ``notify``/``risk`` reach back UP into ``ops.pipeline``
would be a real layering violation, not just an awkward import. ``core`` sits beneath all three.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from datetime import datetime
from typing import Any


def parse_valid_until(raw: Any) -> datetime | None:
    """A recommendation's ``payload.valid_until`` as a tz-aware datetime, or ``None``.

    ``None`` covers three cases every caller here treats identically: absent, unparseable, and naive.
    A naive timestamp is itself a bug upstream (§3.2 requires tz-aware IST throughout) and is treated
    as "cannot tell" rather than risked in a comparison against an aware ``now``.
    """
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(str(raw))
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo is not None else None


def recommendation_expired(valid_until: Any, now: datetime) -> bool:
    """Has this recommendation's ``valid_until`` strictly passed?

    False whenever :func:`parse_valid_until` returns ``None`` (absent/naive/unparseable) — the caller
    leaves such a row alone rather than expiring it: two surfaces disagreeing about which
    recommendations exist would be worse than either rule alone. The comparison is strict ``<``, so a
    recommendation stays live right up to its stated instant.

    Canonical use is ``RecommendationBook.expire_stale`` (``ops.pipeline``) and the ``/taken`` ticker
    resolver (``notify.telegram``), both of which call this directly. The gate's pending-entry check
    (``risk.gate``) wants the *opposite* direction ("still pending") but is NOT simply
    ``not recommendation_expired(...)`` — that would silently flip absent/naive/unparseable rows from
    "excluded" to "included", which is not what its existing behaviour does. It calls
    :func:`parse_valid_until` directly instead.
    """
    parsed = parse_valid_until(valid_until)
    return parsed is not None and parsed < now


def pending_entry_symbols(rows: Iterable[Mapping[str, Any]], now: datetime) -> frozenset[str]:
    """Symbols carrying an UNEXPIRED, UNACTIONED entry recommendation (§3.6).

    ``rows`` are ``recommendations`` table rows (anything supporting ``row["payload"]`` /
    ``row["human_action"]`` — a ``sqlite3.Row`` in practice). Shared by two callers that must agree
    exactly: the gate's own ``GateContext.pending_rec_symbols`` (``risk.gate``, for the O16
    sector/correlation-room math) and the brk20 retest re-arm screen (``ops.main``), which must
    refuse what the gate would refuse (``per_stock_exposure: already held or pending`` is a HARD
    reason, uncurable by shrinking) BEFORE a §3.2.5 day slot and an analyst call are spent rather
    than after.

    A row with ANY ``human_action`` (taken / expired / dismissed / closed) is skipped here
    regardless of whether the caller's own SQL already narrowed to unactioned rows — one caller
    pre-filters in SQL (a 60 s pulse against a table that only grows), the other does not, and the
    guard living here too means the two can never disagree on that point. Shares
    :func:`parse_valid_until` with the expiry predicate rather than negating it: an
    absent/naive/unparseable ``valid_until`` stays EXCLUDED here, exactly as it does there —
    negating :func:`recommendation_expired` would silently flip such a row to "pending".
    """
    out: set[str] = set()
    for row in rows:
        if row["human_action"]:
            continue                       # taken / expired / dismissed / closed ⇒ not pending
        try:
            data = json.loads(row["payload"] or "{}")
        except (TypeError, ValueError):
            continue
        if not isinstance(data, dict) or data.get("kind") != "entry":
            continue
        valid_until = parse_valid_until(data.get("valid_until"))
        if valid_until is not None and valid_until > now and data.get("instrument"):
            out.add(str(data["instrument"]))
    return frozenset(out)
