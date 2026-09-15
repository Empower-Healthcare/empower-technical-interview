"""Pricing rule boundaries. Expected values are computed by hand from the rules
in the README, not from the implementation."""

import pytest

from app import pricing


@pytest.mark.parametrize(
    ("weight_grams", "expected_cents"),
    [
        (1, 600),  # minimum: 500 + 1 started kg
        (999, 600),
        (1000, 600),  # exactly one kilogram is still one started kilogram
        (1001, 700),  # first gram over one kilogram starts the second
        (1500, 700),  # README example
        (2000, 700),
        (29_999, 3500),
        (30_000, 3500),  # maximum: 500 + 30 * 100
    ],
)
def test_standard_price(weight_grams: int, expected_cents: int) -> None:
    money = pricing.quote(weight_grams)
    assert money == pricing.Money(amount_cents=expected_cents, currency="USD")


@pytest.mark.parametrize("weight_grams", [0, -1, 30_001, 1_000_000])
def test_out_of_range_weight_is_rejected(weight_grams: int) -> None:
    with pytest.raises(pricing.InvalidWeight):
        pricing.quote(weight_grams)
