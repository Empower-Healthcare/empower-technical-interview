"""ParcelLab pricing service.

POST /quote  {"weight_grams": 1500}  ->  {"amount_cents": 700, "currency": "USD"}
GET  /healthz                          ->  {"status": "ok"}
GET  /metrics                          ->  Prometheus text format

The Go shipment service is the only client. It sends X-Correlation-ID; every
log line for that request carries the same value so a request can be followed
across both services.

Local simplification: plaintext HTTP inside the Compose network.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, AsyncIterator

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, Counter, Histogram, generate_latest
from pydantic import BaseModel, Field

from app import pricing

CORRELATION_HEADER = "X-Correlation-ID"


@dataclass(frozen=True, slots=True)
class Settings:
    """Process configuration, read once at startup."""

    # Failure-scenario knob: hold every quote for this long so the Go
    # service's bounded pricing deadline can be demonstrated. 0 in normal use.
    artificial_delay_ms: int
    log_level: str

    @staticmethod
    def from_env(env: dict[str, str]) -> "Settings":
        delay = int(env.get("PRICING_ARTIFICIAL_DELAY_MS", "0"))
        if delay < 0:
            raise ValueError(f"PRICING_ARTIFICIAL_DELAY_MS must be >= 0, got {delay}")
        return Settings(artificial_delay_ms=delay, log_level=env.get("LOG_LEVEL", "INFO").upper())


class JSONLogFormatter(logging.Formatter):
    """One JSON object per line, matching the Go services' slog output."""

    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, Any] = {
            "time": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "service": "pricing",
            "msg": record.getMessage(),
        }
        entry.update(getattr(record, "fields", {}))
        return json.dumps(entry, separators=(",", ":"))


def configure_logging(level: str) -> logging.Logger:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JSONLogFormatter())
    logger = logging.getLogger("pricing")
    logger.handlers = [handler]
    logger.setLevel(level)
    logger.propagate = False
    return logger


@dataclass(frozen=True, slots=True)
class Metrics:
    registry: CollectorRegistry
    quotes: Counter
    duration: Histogram

    @staticmethod
    def new() -> "Metrics":
        registry = CollectorRegistry()
        return Metrics(
            registry=registry,
            quotes=Counter(
                "pricing_quotes_total",
                "Quote requests by outcome (ok, invalid).",
                ["outcome"],
                registry=registry,
            ),
            duration=Histogram(
                "pricing_quote_duration_seconds",
                "Time spent answering POST /quote.",
                registry=registry,
            ),
        )


class QuoteRequest(BaseModel):
    weight_grams: int = Field(description="Parcel weight in grams, 1..30000.")
    # Absent means standard. Any other value, null included, fails with 422.
    delivery_speed: pricing.DeliverySpeed = Field(
        default=pricing.DeliverySpeed.STANDARD,
        description="standard or express; defaults to standard.",
    )


class QuoteResponse(BaseModel):
    amount_cents: int
    currency: str


def create_app(settings: Settings, metrics: Metrics, logger: logging.Logger) -> FastAPI:
    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        logger.info(
            "pricing service started",
            extra={"fields": {"artificial_delay_ms": settings.artificial_delay_ms}},
        )
        yield
        logger.info("pricing service stopped")

    app = FastAPI(title="ParcelLab pricing", lifespan=lifespan, docs_url=None, redoc_url=None)

    @app.middleware("http")
    async def correlate(request: Request, call_next):  # type: ignore[no-untyped-def]
        # The Go service always sends one; generate a fallback so every log line has a value.
        request.state.correlation_id = request.headers.get(CORRELATION_HEADER) or f"req_pricing_{time.time_ns():x}"
        response: Response = await call_next(request)
        response.headers[CORRELATION_HEADER] = request.state.correlation_id
        return response

    @app.exception_handler(pricing.InvalidWeight)
    async def invalid_weight(request: Request, exc: pricing.InvalidWeight) -> JSONResponse:
        metrics.quotes.labels(outcome="invalid").inc()
        logger.warning(
            "quote rejected",
            extra={"fields": {"correlation_id": request.state.correlation_id, "error": str(exc)}},
        )
        return JSONResponse(status_code=422, content={"detail": str(exc)})

    @app.post("/quote", response_model=QuoteResponse)
    async def post_quote(body: QuoteRequest, request: Request) -> QuoteResponse:
        started = time.perf_counter()
        if settings.artificial_delay_ms:
            await asyncio.sleep(settings.artificial_delay_ms / 1000)
        money = pricing.quote(body.weight_grams, body.delivery_speed)
        metrics.quotes.labels(outcome="ok").inc()
        metrics.duration.observe(time.perf_counter() - started)
        logger.info(
            "quote priced",
            extra={
                "fields": {
                    "correlation_id": request.state.correlation_id,
                    "weight_grams": body.weight_grams,
                    "delivery_speed": body.delivery_speed.value,
                    "amount_cents": money.amount_cents,
                    "currency": money.currency,
                }
            },
        )
        return QuoteResponse(amount_cents=money.amount_cents, currency=money.currency)

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/metrics")
    async def prometheus_metrics() -> Response:
        return Response(generate_latest(metrics.registry), media_type=CONTENT_TYPE_LATEST)

    return app


def default_app() -> FastAPI:
    """Entry point for uvicorn: `uvicorn app.main:app`."""
    settings = Settings.from_env(dict(os.environ))
    return create_app(settings, Metrics.new(), configure_logging(settings.log_level))


app = default_app()
