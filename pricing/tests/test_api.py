"""HTTP contract of the pricing service as the Go client sees it."""

import logging

import pytest
from fastapi.testclient import TestClient

from app.main import Metrics, Settings, create_app


@pytest.fixture
def client() -> TestClient:
    settings = Settings(artificial_delay_ms=0, log_level="WARNING")
    return TestClient(create_app(settings, Metrics.new(), logging.getLogger("pricing-test")))


def test_quote_returns_integer_cents_and_currency(client: TestClient) -> None:
    resp = client.post("/quote", json={"weight_grams": 1500}, headers={"X-Correlation-ID": "req_test_1"})
    assert resp.status_code == 200
    assert resp.json() == {"amount_cents": 700, "currency": "USD"}
    assert resp.headers["X-Correlation-ID"] == "req_test_1"


@pytest.mark.parametrize(
    ("body", "expected_cents"),
    [
        ({"weight_grams": 1500}, 700),  # field absent: standard, unchanged behaviour
        ({"weight_grams": 1500, "delivery_speed": "standard"}, 700),
        ({"weight_grams": 1500, "delivery_speed": "express"}, 1200),
    ],
)
def test_quote_prices_delivery_speed(client: TestClient, body: dict, expected_cents: int) -> None:
    resp = client.post("/quote", json=body)
    assert resp.status_code == 200
    assert resp.json() == {"amount_cents": expected_cents, "currency": "USD"}


@pytest.mark.parametrize(
    "delivery_speed",
    ["overnight", "EXPRESS", "DELIVERY_SPEED_EXPRESS", "", None, 2],
)
def test_unsupported_delivery_speed_is_422(client: TestClient, delivery_speed: object) -> None:
    resp = client.post("/quote", json={"weight_grams": 1500, "delivery_speed": delivery_speed})
    assert resp.status_code == 422
    assert any(err["loc"] == ["body", "delivery_speed"] for err in resp.json()["detail"])


def test_quote_generates_correlation_id_when_missing(client: TestClient) -> None:
    resp = client.post("/quote", json={"weight_grams": 1})
    assert resp.status_code == 200
    assert resp.headers["X-Correlation-ID"].startswith("req_pricing_")


@pytest.mark.parametrize("weight_grams", [0, 30_001])
def test_out_of_range_weight_is_422(client: TestClient, weight_grams: int) -> None:
    resp = client.post("/quote", json={"weight_grams": weight_grams})
    assert resp.status_code == 422
    assert "weight_grams" in resp.json()["detail"]


@pytest.mark.parametrize("body", [{}, {"weight_grams": "heavy"}, {"weight_grams": 1.5}])
def test_malformed_body_is_422(client: TestClient, body: dict) -> None:
    assert client.post("/quote", json=body).status_code == 422


def test_metrics_count_outcomes(client: TestClient) -> None:
    client.post("/quote", json={"weight_grams": 1500})
    client.post("/quote", json={"weight_grams": 0})
    text = client.get("/metrics").text
    assert 'pricing_quotes_total{outcome="ok"} 1.0' in text
    assert 'pricing_quotes_total{outcome="invalid"} 1.0' in text


def test_body_validation_errors_count_as_invalid(client: TestClient) -> None:
    client.post("/quote", json={"weight_grams": 1500, "delivery_speed": "overnight"})
    client.post("/quote", json={"weight_grams": "heavy"})
    assert 'pricing_quotes_total{outcome="invalid"} 2.0' in client.get("/metrics").text


def test_healthz(client: TestClient) -> None:
    assert client.get("/healthz").json() == {"status": "ok"}


def test_settings_reject_negative_delay() -> None:
    with pytest.raises(ValueError):
        Settings.from_env({"PRICING_ARTIFICIAL_DELAY_MS": "-1"})
