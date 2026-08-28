"""Shared recommendation-lifecycle predicate (§3.6).

Lives in ``core`` rather than ``ops.pipeline`` — home of the canonical consumer,
``RecommendationBook.expire_stale`` — because the other two consumers sit BELOW ``ops`` in the import
graph: the ``/taken`` ticker resolver (``notify.telegram``) and the gate's pending-entry check
(``risk.gate``). Neither package currently imports ``engine.ops`` at runtime, and ``ops.pipeline``
itself imports from ``notify`` — so having ``notify``/``risk`` reach back UP into ``ops.pipeline``
would be a real layering violation, not just an awkward import. ``core`` sits beneath all three.
"""

from __future__ import annotations

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
