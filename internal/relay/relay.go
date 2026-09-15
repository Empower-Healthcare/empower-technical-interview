// Package relay polls the transactional outbox and publishes pending events to
// Kafka. It is the only component that both reads outbox_events and talks to
// the broker, which is what lets CreateShipment succeed while Kafka is down.
package relay

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/empower-healthcare/parcellab/internal/broker"
	"github.com/empower-healthcare/parcellab/internal/correlation"
	"github.com/empower-healthcare/parcellab/internal/domain"
	"github.com/empower-healthcare/parcellab/internal/events"
	"github.com/empower-healthcare/parcellab/internal/storage"
)

// Metrics are the relay's Prometheus series.
type Metrics struct {
	published    prometheus.Counter
	failures     prometheus.Counter
	pending      prometheus.Gauge
	pollDuration prometheus.Histogram
}

// NewMetrics creates and registers the relay metrics.
func NewMetrics(reg prometheus.Registerer) Metrics {
	m := Metrics{
		published: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "parcellab_outbox_published_total",
			Help: "Outbox events acknowledged by Kafka and marked published.",
		}),
		failures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "parcellab_outbox_publish_failures_total",
			Help: "Outbox publish attempts that failed (the event stays pending).",
		}),
		pending: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "parcellab_outbox_pending_events",
			Help: "Outbox events not yet published, as of the last poll.",
		}),
		pollDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "parcellab_outbox_poll_duration_seconds",
			Help:    "Time spent per relay poll, including publishing.",
			Buckets: prometheus.DefBuckets,
		}),
	}
	reg.MustRegister(m.published, m.failures, m.pending, m.pollDuration)
	return m
}

// Publisher is the broker dependency.
type Publisher interface {
	Publish(ctx context.Context, msg broker.Message) error
}

// Outbox is the storage dependency.
type Outbox interface {
	ProcessPending(ctx context.Context, limit int, publish storage.PublishFunc) (storage.PublishResult, error)
	CountPending(ctx context.Context) (int, error)
}

// Config tunes the poll loop.
type Config struct {
	PollInterval time.Duration // idle wait between polls; also the initial backoff
	BatchSize    int           // rows locked per poll
}

// Validate rejects values that would make Run busy-loop or claim nothing.
func (c Config) Validate() error {
	if c.PollInterval <= 0 {
		return fmt.Errorf("poll interval %s: must be positive", c.PollInterval)
	}
	if c.BatchSize <= 0 {
		return fmt.Errorf("batch size %d: must be positive", c.BatchSize)
	}
	return nil
}

// Relay runs the poll loop.
type Relay struct {
	outbox    Outbox
	publisher Publisher
	metrics   Metrics
	logger    *slog.Logger
	cfg       Config
}

// New creates a Relay. cfg must have passed Validate.
func New(outbox Outbox, publisher Publisher, metrics Metrics, logger *slog.Logger, cfg Config) *Relay {
	return &Relay{outbox: outbox, publisher: publisher, metrics: metrics, logger: logger, cfg: cfg}
}

// Run polls until ctx is cancelled. Publish failures back off exponentially
// (capped at 10x the poll interval) because they almost always mean the broker
// is unavailable; the pending rows are simply retried later.
func (r *Relay) Run(ctx context.Context) error {
	backoff := r.cfg.PollInterval
	maxBackoff := 10 * r.cfg.PollInterval

	for {
		result, err := r.poll(ctx)
		if ctx.Err() != nil {
			return nil
		}

		var wait time.Duration
		switch {
		case err != nil:
			// Storage failure: nothing was claimed or marked.
			r.metrics.failures.Inc()
			r.logger.ErrorContext(ctx, "outbox poll failed", "error", err.Error(), "retry_in", backoff.String())
			wait, backoff = backoff, min(backoff*2, maxBackoff)
		case result.Failed != "":
			// Broker failure: successes were marked, the failed event stays pending.
			r.metrics.failures.Inc()
			r.logger.ErrorContext(ctx, "outbox publish failed; event stays pending",
				"event_id", result.Failed, "error", result.Err.Error(),
				"published_before_failure", len(result.Published), "retry_in", backoff.String())
			wait, backoff = backoff, min(backoff*2, maxBackoff)
		case len(result.Published) > 0:
			r.logger.InfoContext(ctx, "outbox events published", "count", len(result.Published))
			wait, backoff = 0, r.cfg.PollInterval // drain quickly while there is a backlog
		default:
			wait, backoff = r.cfg.PollInterval, r.cfg.PollInterval
		}

		if err := sleep(ctx, wait); err != nil {
			return nil
		}
	}
}

func (r *Relay) poll(ctx context.Context) (storage.PublishResult, error) {
	// Sample before claiming: a poll can block for the whole publish timeout
	// while the broker is down, and that is exactly when the backlog matters.
	ObserveBacklog(ctx, r.outbox, r.metrics)
	start := time.Now()
	result, err := r.outbox.ProcessPending(ctx, r.cfg.BatchSize, r.publishBatch)
	r.metrics.pollDuration.Observe(time.Since(start).Seconds())
	r.metrics.published.Add(float64(len(result.Published)))
	return result, err
}

// ObserveBacklog samples the pending-event count into the gauge. The relay
// calls it before every poll; the binary also calls it while waiting for Kafka
// so the backlog stays visible during exactly the outage that grows it.
func ObserveBacklog(ctx context.Context, outbox Outbox, metrics Metrics) {
	if n, err := outbox.CountPending(ctx); err == nil {
		metrics.pending.Set(float64(n))
	}
}

// publishBatch is the storage.PublishFunc: publish in order, stop at the first failure.
func (r *Relay) publishBatch(ctx context.Context, batch []storage.OutboxEvent) storage.PublishResult {
	result := storage.PublishResult{Published: make([]domain.EventID, 0, len(batch))}
	for _, evt := range batch {
		msg, err := MessageFor(evt)
		if err != nil {
			result.Failed, result.Err = evt.EventID, err
			return result
		}
		// The event's own correlation ID travels with the publish and its log line.
		evtCtx := correlation.WithID(ctx, msg.Headers[events.HeaderCorrelationID])
		if err := r.publisher.Publish(evtCtx, msg); err != nil {
			result.Failed, result.Err = evt.EventID, err
			return result
		}
		r.logger.InfoContext(evtCtx, "outbox event acknowledged by kafka",
			"event_id", evt.EventID, "shipment_id", evt.ShipmentID, "topic", msg.Topic, "attempt", evt.Attempts+1)
		result.Published = append(result.Published, evt.EventID)
	}
	return result
}

// MessageFor converts an outbox row into the Kafka message: keyed by shipment
// ID so all events for one shipment share a partition and keep their order,
// with the correlation ID and schema metadata as headers. The payload is
// decoded only to lift those header values out of it.
func MessageFor(evt storage.OutboxEvent) (broker.Message, error) {
	payload, err := events.Decode(evt.Payload)
	if err != nil {
		return broker.Message{}, fmt.Errorf("outbox event %s: %w", evt.EventID, err)
	}
	return broker.Message{
		Topic: events.Topic,
		Key:   []byte(evt.ShipmentID),
		Value: evt.Payload,
		Headers: map[string]string{
			events.HeaderEventType:     payload.EventType,
			events.HeaderSchemaVersion: strconv.Itoa(payload.SchemaVersion),
			events.HeaderCorrelationID: payload.CorrelationID,
		},
	}, nil
}

func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
