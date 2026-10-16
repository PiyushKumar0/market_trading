"""``pct_of``: the percent strings the owner surfaces print next to a rupee figure."""

from __future__ import annotations

from decimal import Decimal

import pytest

from engine.core.money import pct_of


@pytest.mark.parametrize(("amount", "basis", "expected"), [
    ("150.50", "20000", "0.75"),
    (Decimal("-250"), Decimal("20000"), "-1.25"),
    ("0", "20000", "0.00"),
    ("-0.001", "100", "0.00"),                 # rounds to negative zero: never printed as "-0.00"
    ("5", "-100", "5.00"),                     # the basis is its magnitude
    ("-5", "-100", "-5.00"),
    ("1", "3", "33.33"),
    ("2", "3", "66.67"),
    ("1", "800", "0.13"),
    ("1", "8", "12.50"),
    ("0.5", "100", "0.50"),
    ("0.005", "100", "0.01"),                  # half-up, not banker's
    (10, 40000, "0.03"),
])
def test_pct_of_values(amount, basis, expected) -> None:
    assert pct_of(amount, basis) == expected


@pytest.mark.parametrize(("amount", "basis"), [
    ("10", "0"), ("10", 0), ("10", None), ("10", ""), (None, "100"), ("", "100"), (None, None),
])
def test_pct_of_is_none_without_an_amount_or_a_basis(amount, basis) -> None:
    assert pct_of(amount, basis) is None
