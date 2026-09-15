// Package consumer processes shipment.created.v1 events from Kafka and records
// one dispatch per shipment.
//
// Per record, in order:
//  1. decode + validate the payload
//  2. one PostgreSQL transaction: insert processed_events row, insert dispatch
//  3. commit the Kafka offset
//
// A crash between 2 and 3 causes a redelivery, which step 2 recognises as a
// duplicate (the event_id is already in processed_events). This is
// at-least-once delivery made idempotent, not exactly-once processing.
package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/empower-healthcare/parcellab/internal/correlation"
	"github.com/empower-healthcare/parcellab/internal/domain"
	"github.com/empower-healthcare/parcellab/internal/events"
	"github.com/empower-healthcare/parcellab/internal/storage"
)

// Outcome classifies what happened to one record. It is the only label on the
// consumer's outcome counter, so the set of values must stay small.
type Outcome string

const (
	OutcomeProcessed   Outcome = "processed"   // dispatch created; offset may be committed
	OutcomeDuplicate   Outcome = "duplicate"   // event_id already processed; offset may be committed
	OutcomeMalformed   Outcome = "malformed"   // undecodable payload; offset must NOT be committed
	OutcomeUnsupported Outcome = "unsupported" // unknown type/version; offset must NOT be committed
	OutcomeConflict    Outcome = "conflict"    // different event for an already dispatched shipment; offset must NOT be committed
	OutcomeFailed      Outcome = "failed"      // storage error after bounded retries; offset must NOT be committed
)

// Metrics are the consumer's Prometheus series.
type Metrics struct {
	events   *prometheus.CounterVec
	duration prometheus.Histogram
	retries  prometheus.Counter
}

// NewMetrics creates and registers the consumer metrics.
func NewMetrics(reg prometheus.Registerer) Metrics {
	m := Metrics{
		events: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "parcellab_consumer_events_total",
			Help: "Records handled by the dispatch consumer, by outcome (processed, duplicate, malformed, unsupported, conflict, failed).",
		}, []string{"outcome"}),
		duration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "parcellab_consumer_processing_duration_seconds",
			Help:    "Time to handle one record, including storage retries.",
			Buckets: prometheus.DefBuckets,
		}),
		retries: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "parcellab_consumer_storage_retries_total",
			Help: "Transient storage failures that were retried.",
		}),
	}
	reg.MustRegister(m.events, m.duration, m.retries)
	return m
}

// DispatchStore is the persistence dependency (storage.Store in the running stack).
type DispatchStore interface {
	RecordDispatch(ctx context.Context, req storage.DispatchRequest) (domain.Dispatch, bool, error)
}

// RetryPolicy bounds retries of transient storage failures. Waits grow
// linearly: Backoff, 2*Backoff, ...
type RetryPolicy struct {
	MaxAttempts int
	Backoff     time.Duration
}

// Validate rejects policies that would skip the store call or wait negatively.
func (p RetryPolicy) Validate() error {
	if p.MaxAttempts < 1 {
		return fmt.Errorf("max attempts %d: must be at least 1", p.MaxAttempts)
	}
	if p.Backoff < 0 {
		return fmt.Errorf("backoff %s: must not be negative", p.Backoff)
	}
	return nil
}

// MaxWait is the total time the policy can spend sleeping between attempts.
func (p RetryPolicy) MaxWait() time.Duration {
	n := time.Duration(p.MaxAttempts - 1)
	return p.Backoff * n * (n + 1) / 2
}

// Record identifies one Kafka record for logging.
type Record struct {
	Partition int32
	Offset    int64
	Value     []byte
}

// Handler turns one record into at most one dispatch.
type Handler struct {
	store   DispatchStore
	metrics Metrics
	logger  *slog.Logger
	retry   RetryPolicy
}

// NewHandler returns a Handler.
func NewHandler(store DispatchStore, metrics Metrics, logger *slog.Logger, retry RetryPolicy) *Handler {
	return &Handler{store: store, metrics: metrics, logger: logger, retry: retry}
}

// Handle processes a record. A nil error means the caller may commit the
// offset. A non-nil error means it must not: the record was malformed,
// unsupported, conflicting, or storage stayed unavailable. The caller decides
// what a non-commit means; this project's consumer stops, so the failure is
// visible as lag and restarts rather than being skipped.
func (h *Handler) Handle(ctx context.Context, rec Record) (Outcome, error) {
	start := time.Now()
	outcome, err := h.handle(ctx, rec)
	h.metrics.events.WithLabelValues(string(outcome)).Inc()
	h.metrics.duration.Observe(time.Since(start).Seconds())
	return outcome, err
}

func (h *Handler) handle(ctx context.Context, rec Record) (Outcome, error) {
	evt, err := events.Decode(rec.Value)
	if err != nil {
		outcome := decodeOutcome(err)
		h.logger.ErrorContext(ctx, "rejecting event",
			"outcome", outcome, "partition", rec.Partition, "offset", rec.Offset, "error", err.Error())
		return outcome, err
	}

	// From here on every log line carries the event's correlation ID.
	ctx = correlation.WithID(ctx, evt.CorrelationID)
	logger := h.logger.With(
		"event_id", evt.EventID, "shipment_id", evt.ShipmentID,
		"partition", rec.Partition, "offset", rec.Offset,
	)

	req := storage.DispatchRequest{
		EventID:     evt.EventID,
		ShipmentID:  evt.ShipmentID,
		AmountCents: evt.Data.AmountCents,
		Currency:    evt.Data.Currency,
	}

	for attempt := 1; ; attempt++ {
		dispatch, duplicate, err := h.store.RecordDispatch(ctx, req)
		switch {
		case err == nil && duplicate:
			logger.WarnContext(ctx, "duplicate event ignored; dispatch already exists")
			return OutcomeDuplicate, nil
		case err == nil:
			logger.InfoContext(ctx, "dispatch recorded",
				"dispatch_id", dispatch.ID, "amount_cents", req.AmountCents, "currency", req.Currency)
			return OutcomeProcessed, nil
		case errors.Is(err, storage.ErrDispatchConflict):
			logger.ErrorContext(ctx, "conflicting dispatch", "error", err.Error())
			return OutcomeConflict, err
		case ctx.Err() != nil:
			// Report both: why we stopped (ctx) and what storage last said.
			return OutcomeFailed, fmt.Errorf("record dispatch: %w: %w", ctx.Err(), err)
		case attempt >= h.retry.MaxAttempts:
			logger.ErrorContext(ctx, "storage failure; giving up", "attempts", attempt, "error", err.Error())
			return OutcomeFailed, fmt.Errorf("record dispatch after %d attempts: %w", attempt, err)
		}

		h.metrics.retries.Inc()
		logger.WarnContext(ctx, "storage failure; retrying", "attempt", attempt, "error", err.Error())
		if err := sleep(ctx, time.Duration(attempt)*h.retry.Backoff); err != nil {
			return OutcomeFailed, err
		}
	}
}

func decodeOutcome(err error) Outcome {
	if errors.Is(err, events.ErrUnsupported) {
		return OutcomeUnsupported
	}
	return OutcomeMalformed
}

func sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
