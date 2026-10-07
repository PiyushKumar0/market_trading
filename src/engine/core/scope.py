"""Real/paper row scope (plan §1.4, Q3.2): the ONLY source of the scope predicate.

Paper rows live in the real tables with ``is_paper=1`` and, on ``positions``, ``origin='platform'``.
Every reader of a mixed table states its scope through :func:`scope_sql`; a test fails on the literal
origin list anywhere else in code, so a reader cannot quietly drift out of scope.
"""

from __future__ import annotations

from typing import Literal

Scope = Literal["real", "paper"]

#: Position states that still hold exposure. A paper exit works in ``PENDING_EXIT`` (Q4.6); a real
#: position in RECOMMEND is only ever ``OPEN``.
HELD_STATES_SQL: dict[Scope, str] = {
    "real": "state='OPEN'",
    "paper": "state IN ('OPEN','PENDING_EXIT')",
}


def scope_sql(scope: Scope, alias: str | None = None, *, has_origin: bool = False) -> str:
    """SQL predicate selecting ``scope``'s rows. ``has_origin`` adds the ``positions.origin`` leg:
    real = the platform's and the owner's confirmed recommendations (O5 excludes ``external``)."""
    col = f"{alias}." if alias else ""
    if scope == "real":
        sql = f"COALESCE({col}is_paper, 0) = 0"
        return f"{sql} AND {col}origin IN ('platform','recommended')" if has_origin else sql
    if scope == "paper":
        sql = f"{col}is_paper = 1"
        return f"{sql} AND {col}origin = 'platform'" if has_origin else sql
    raise ValueError(f"unknown scope {scope!r}")
