"""Pricing rules. Pure functions; no I/O, no clock, no configuration.

All rules are fictional and exist only for this exercise.
"""

from dataclasses import dataclass
from enum import Enum

MIN_WEIGHT_GRAMS = 1
MAX_WEIGHT_GRAMS = 30_000

BASE_CENTS = 500
PER_STARTED_KG_CENTS = 100
EXPRESS_SURCHARGE_CENTS = 500
CURRENCY = "USD"


class DeliverySpeed(str, Enum):
    """Wire values of delivery_speed. Keep in sync with internal/pricing/client.go."""

    STANDARD = "standard"
    EXPRESS = "express"


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


def quote(weight_grams: int, speed: DeliverySpeed) -> Money:
    """Standard delivery: 500 cents plus 100 cents per started kilogram.
    Express delivery: standard price plus 500 cents.

    Example: 1,500 g -> 500 + 2 * 100 = 700 cents, or 1,200 cents express.
    """
    validate_weight(weight_grams)
    amount_cents = BASE_CENTS + PER_STARTED_KG_CENTS * started_kilograms(weight_grams)
    if speed is DeliverySpeed.EXPRESS:
        amount_cents += EXPRESS_SURCHARGE_CENTS
    return Money(amount_cents=amount_cents, currency=CURRENCY)
