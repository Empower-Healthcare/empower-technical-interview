"""Pricing rules. Pure functions; no I/O, no clock, no configuration.

All rules are fictional and exist only for this exercise.
"""

from dataclasses import dataclass

MIN_WEIGHT_GRAMS = 1
MAX_WEIGHT_GRAMS = 30_000

BASE_CENTS = 500
PER_STARTED_KG_CENTS = 100
CURRENCY = "USD"


class InvalidWeight(ValueError):
    """weight_grams is outside [MIN_WEIGHT_GRAMS, MAX_WEIGHT_GRAMS]."""


@dataclass(frozen=True, slots=True)
class Money:
    amount_cents: int
    currency: str


def validate_weight(weight_grams: int) -> int:
    if not MIN_WEIGHT_GRAMS <= weight_grams <= MAX_WEIGHT_GRAMS:
        raise InvalidWeight(
            f"weight_grams must be between {MIN_WEIGHT_GRAMS} and {MAX_WEIGHT_GRAMS}, got {weight_grams}"
        )
    return weight_grams


def started_kilograms(weight_grams: int) -> int:
    """1..1000 g is 1 started kg, 1001..2000 g is 2, and so on."""
    return -(-weight_grams // 1000)


def quote(weight_grams: int) -> Money:
    """Standard delivery: 500 cents plus 100 cents per started kilogram.

    Example: 1,500 g -> 500 + 2 * 100 = 700 cents.
    """
    validate_weight(weight_grams)
    return Money(
        amount_cents=BASE_CENTS + PER_STARTED_KG_CENTS * started_kilograms(weight_grams),
        currency=CURRENCY,
    )
