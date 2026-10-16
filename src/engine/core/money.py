"""Percent strings for owner surfaces: the caller states the basis, this module only divides."""

from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal
from typing import Any

_CENT = Decimal("0.01")


def pct_of(amount: Any, basis: Any) -> str | None:
    """``amount`` as a 2-dp percent of ``abs(basis)``; None when either is missing or the basis is 0."""
    if amount is None or basis is None or amount == "" or basis == "":
        return None
    denom = abs(Decimal(str(basis)))
    if denom == 0:
        return None
    pct = (Decimal(str(amount)) / denom * 100).quantize(_CENT, ROUND_HALF_UP)
    return str(abs(pct)) if pct == 0 else str(pct)
