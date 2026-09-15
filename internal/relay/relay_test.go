package relay

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/empower-healthcare/parcellab/internal/broker"
	"github.com/empower-healthcare/parcellab/internal/domain"
	"github.com/empower-healthcare/parcellab/internal/events"
	"github.com/empower-healthcare/parcellab/internal/storage"
)

func outboxEvent(eventID domain.EventID, shipmentID domain.ShipmentID, correlationID string) storage.OutboxEvent {
	payload, err := events.Encode(events.ShipmentCreated{
		EventID:       eventID,
		EventType:     events.TypeShipmentCreated,
		SchemaVersion: events.SchemaVersion,
		ShipmentID:    shipmentID,
		OccurredAt:    time.Date(2026, 9, 15, 16, 0, 0, 0, time.UTC),
		CorrelationID: correlationID,
		Data:          events.ShipmentCreatedData{WeightGrams: 1500, AmountCents: 700, Currency: "USD"},
	})
	if err != nil {
		panic(err)
	}
	return storage.OutboxEvent{EventID: eventID, ShipmentID: shipmentID, EventType: events.TypeShipmentCreated, Payload: payload}
}

func TestMessageFor(t *testing.T) {
	evt := outboxEvent("evt_1", "shp_1", "req_1")

	msg, err := MessageFor(evt)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Topic != events.Topic {
		t.Fatalf("topic = %q, want %q", msg.Topic, events.Topic)
	}
	if string(msg.Key) != "shp_1" {
		t.Fatalf("key = %q, want the shipment id so a shipment's events share a partition", msg.Key)
	}
	if string(msg.Value) != string(evt.Payload) {
		t.Fatalf("value must be the stored payload verbatim")
	}
	wantHeaders := map[string]string{
		events.HeaderEventType:     "shipment.created",
		events.HeaderSchemaVersion: "1",
		events.HeaderCorrelationID: "req_1",
	}
	for k, want := range wantHeaders {
		if got := msg.Headers[k]; got != want {
			t.Fatalf("header %s = %q, want %q", k, got, want)
		}
	}
}

func TestMessageForRejectsUndecodablePayload(t *testing.T) {
	_, err := MessageFor(storage.OutboxEvent{EventID: "evt_bad", ShipmentID: "shp_1", Payload: []byte(`{`)})
	if !errors.Is(err, events.ErrMalformed) {
		t.Fatalf("want ErrMalformed, got %v", err)
	}
}

// fakePublisher fails every message whose event ID is in failOn.
type fakePublisher struct {
	failOn    map[domain.EventID]error
	published []broker.Message
}

func (f *fakePublisher) Publish(_ context.Context, msg broker.Message) error {
	var payload events.ShipmentCreated
	payload, _ = events.Decode(msg.Value)
	if err, ok := f.failOn[payload.EventID]; ok {
		return err
	}
	f.published = append(f.published, msg)
	return nil
}

func TestPublishBatchStopsAtFirstFailure(t *testing.T) {
	brokerDown := errors.New("broker unreachable")
	cases := []struct {
		name          string
		batch         []storage.OutboxEvent
		failOn        map[domain.EventID]error
		wantPublished []domain.EventID
		wantFailed    domain.EventID
	}{
		{
			name:          "all succeed",
			batch:         []storage.OutboxEvent{outboxEvent("evt_1", "shp_1", "req_1"), outboxEvent("evt_2", "shp_2", "req_2")},
			wantPublished: []domain.EventID{"evt_1", "evt_2"},
		},
		{
			name:          "second fails, third is not attempted",
			batch:         []storage.OutboxEvent{outboxEvent("evt_1", "shp_1", "req_1"), outboxEvent("evt_2", "shp_2", "req_2"), outboxEvent("evt_3", "shp_3", "req_3")},
			failOn:        map[domain.EventID]error{"evt_2": brokerDown},
			wantPublished: []domain.EventID{"evt_1"},
			wantFailed:    "evt_2",
		},
		{
			name:          "undecodable row fails without reaching the broker",
			batch:         []storage.OutboxEvent{{EventID: "evt_bad", ShipmentID: "shp_1", Payload: []byte(`{`)}, outboxEvent("evt_2", "shp_2", "req_2")},
			wantPublished: []domain.EventID{},
			wantFailed:    "evt_bad",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(sub *testing.T) {
			pub := &fakePublisher{failOn: tc.failOn}
			r := New(nil, pub, NewMetrics(prometheus.NewRegistry()), slog.New(slog.DiscardHandler), Config{})

			result := r.publishBatch(sub.Context(), tc.batch)

			if len(result.Published) != len(tc.wantPublished) {
				sub.Fatalf("published = %v, want %v", result.Published, tc.wantPublished)
			}
			for i := range tc.wantPublished {
				if result.Published[i] != tc.wantPublished[i] {
					sub.Fatalf("published = %v, want %v", result.Published, tc.wantPublished)
				}
			}
			if result.Failed != tc.wantFailed {
				sub.Fatalf("failed = %q, want %q", result.Failed, tc.wantFailed)
			}
			if (result.Err != nil) != (tc.wantFailed != "") {
				sub.Fatalf("err = %v, inconsistent with failed = %q", result.Err, result.Failed)
			}
			if len(pub.published) != len(tc.wantPublished) {
				sub.Fatalf("broker received %d messages, want %d", len(pub.published), len(tc.wantPublished))
			}
		})
	}
}

// fakeOutbox serves one fixed batch and applies the relay's result to it.
// Run calls it from its own goroutine, so tests observe progress through
// drained instead of reading pending directly.
type fakeOutbox struct {
	pending []storage.OutboxEvent
	err     error
	drained chan struct{} // closed once pending is empty; may be nil
}

func (f *fakeOutbox) ProcessPending(ctx context.Context, limit int, publish storage.PublishFunc) (storage.PublishResult, error) {
	if f.err != nil {
		return storage.PublishResult{}, f.err
	}
	defer func() {
		if len(f.pending) == 0 && f.drained != nil {
			close(f.drained)
			f.drained = nil
		}
	}()
	batch := f.pending[:min(limit, len(f.pending))]
	result := publish(ctx, batch)
	remaining := f.pending[:0:0]
	for _, evt := range f.pending {
		published := false
		for _, id := range result.Published {
			published = published || id == evt.EventID
		}
		if !published {
			remaining = append(remaining, evt)
		}
	}
	f.pending = remaining
	return result, nil
}

func (f *fakeOutbox) CountPending(context.Context) (int, error) { return len(f.pending), nil }

func TestRunDrainsBacklogAndReportsMetrics(t *testing.T) {
	drained := make(chan struct{})
	outbox := &fakeOutbox{
		pending: []storage.OutboxEvent{
			outboxEvent("evt_1", "shp_1", "req_1"),
			outboxEvent("evt_2", "shp_2", "req_2"),
			outboxEvent("evt_3", "shp_3", "req_3"),
		},
		drained: drained,
	}
	pub := &fakePublisher{}
	metrics := NewMetrics(prometheus.NewRegistry())
	r := New(outbox, pub, metrics, slog.New(slog.DiscardHandler), Config{PollInterval: time.Millisecond, BatchSize: 2})

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- r.Run(ctx) }()

	select {
	case <-drained:
	case <-ctx.Done():
		t.Fatal("relay did not drain the backlog before the deadline")
	}
	cancel()
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}

	if len(pub.published) != 3 {
		t.Fatalf("broker received %d messages, want 3", len(pub.published))
	}
	if n := testutil.ToFloat64(metrics.published); n != 3 {
		t.Fatalf("published counter = %v, want 3", n)
	}
	if n := testutil.ToFloat64(metrics.pending); n != 0 {
		t.Fatalf("pending gauge = %v, want 0", n)
	}
}

func TestRunCountsFailuresAndKeepsGoing(t *testing.T) {
	outbox := &fakeOutbox{err: errors.New("postgres down")}
	metrics := NewMetrics(prometheus.NewRegistry())
	r := New(outbox, &fakePublisher{}, metrics, slog.New(slog.DiscardHandler), Config{PollInterval: time.Millisecond, BatchSize: 1})

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	if err := r.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if n := testutil.ToFloat64(metrics.failures); n < 2 {
		t.Fatalf("failures counter = %v, want repeated retries", n)
	}
}
