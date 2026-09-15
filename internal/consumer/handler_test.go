package consumer

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/empower-healthcare/parcellab/internal/domain"
	"github.com/empower-healthcare/parcellab/internal/storage"
)

const validPayload = `{"event_id":"evt_1","event_type":"shipment.created","schema_version":1,` +
	`"shipment_id":"shp_1","occurred_at":"2026-09-15T16:00:00Z","correlation_id":"req_1",` +
	`"data":{"weight_grams":1500,"amount_cents":700,"currency":"USD"}}`

// memoryStore mimics the database's uniqueness rules in memory.
type memoryStore struct {
	processed map[domain.EventID]bool
	dispatch  map[domain.ShipmentID]domain.EventID
	failures  int // transient errors to return before succeeding
	calls     int
}

func newMemoryStore() *memoryStore {
	return &memoryStore{processed: map[domain.EventID]bool{}, dispatch: map[domain.ShipmentID]domain.EventID{}}
}

func (m *memoryStore) RecordDispatch(_ context.Context, req storage.DispatchRequest) (domain.Dispatch, bool, error) {
	m.calls++
	if m.failures > 0 {
		m.failures--
		return domain.Dispatch{}, false, errors.New("connection refused")
	}
	if m.processed[req.EventID] {
		return domain.Dispatch{}, true, nil
	}
	if other, ok := m.dispatch[req.ShipmentID]; ok && other != req.EventID {
		return domain.Dispatch{}, false, storage.ErrDispatchConflict
	}
	m.processed[req.EventID] = true
	m.dispatch[req.ShipmentID] = req.EventID
	return domain.Dispatch{Status: domain.DispatchDispatched, ID: "dsp_1", EventID: req.EventID}, false, nil
}

func newHandler(store DispatchStore, maxAttempts int) (*Handler, Metrics) {
	metrics := NewMetrics(prometheus.NewRegistry())
	retry := RetryPolicy{MaxAttempts: maxAttempts, Backoff: time.Microsecond}
	return NewHandler(store, metrics, slog.New(slog.DiscardHandler), retry), metrics
}

func record(payload string) Record {
	return Record{Partition: 0, Offset: 1, Value: []byte(payload)}
}

func TestHandleProcessesThenDeduplicates(t *testing.T) {
	store := newMemoryStore()
	h, metrics := newHandler(store, 3)

	outcome, err := h.Handle(t.Context(), record(validPayload))
	if err != nil || outcome != OutcomeProcessed {
		t.Fatalf("first delivery: want processed, got %s / %v", outcome, err)
	}

	// Redelivery of the same event ID (relay crash, replay helper, offset not committed).
	outcome, err = h.Handle(t.Context(), record(validPayload))
	if err != nil || outcome != OutcomeDuplicate {
		t.Fatalf("redelivery: want duplicate with nil error (commit offset), got %s / %v", outcome, err)
	}

	if len(store.dispatch) != 1 {
		t.Fatalf("exactly one dispatch expected, got %d", len(store.dispatch))
	}
	if n := testutil.ToFloat64(metrics.events.WithLabelValues(string(OutcomeDuplicate))); n != 1 {
		t.Fatalf("duplicate counter = %v, want 1", n)
	}
}

func TestHandleRejectsBadPayloadsWithoutTouchingStorage(t *testing.T) {
	cases := []struct {
		name        string
		payload     string
		wantOutcome Outcome
	}{
		{
			name:        "malformed",
			payload:     `{"event_id":`,
			wantOutcome: OutcomeMalformed,
		},
		{
			name:        "unsupported schema version",
			payload:     `{"event_id":"evt_2","event_type":"shipment.created","schema_version":9,"shipment_id":"shp_2","data":{"currency":"USD"}}`,
			wantOutcome: OutcomeUnsupported,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(sub *testing.T) {
			store := newMemoryStore()
			h, _ := newHandler(store, 3)

			outcome, err := h.Handle(sub.Context(), record(tc.payload))
			if err == nil {
				sub.Fatal("bad payloads must return an error so the offset is not committed")
			}
			if outcome != tc.wantOutcome {
				sub.Fatalf("outcome = %s, want %s", outcome, tc.wantOutcome)
			}
			if store.calls != 0 {
				sub.Fatalf("storage must not be called for rejected payloads, got %d calls", store.calls)
			}
		})
	}
}

func TestHandleRetriesTransientStorageErrors(t *testing.T) {
	cases := []struct {
		name        string
		failures    int
		maxAttempts int
		wantOutcome Outcome
		wantErr     bool
		wantCalls   int
	}{
		{name: "recovers within budget", failures: 1, maxAttempts: 3, wantOutcome: OutcomeProcessed, wantErr: false, wantCalls: 2},
		{name: "recovers on last attempt", failures: 2, maxAttempts: 3, wantOutcome: OutcomeProcessed, wantErr: false, wantCalls: 3},
		{name: "gives up after max attempts", failures: 10, maxAttempts: 3, wantOutcome: OutcomeFailed, wantErr: true, wantCalls: 3},
		{name: "single attempt never retries", failures: 1, maxAttempts: 1, wantOutcome: OutcomeFailed, wantErr: true, wantCalls: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(sub *testing.T) {
			store := newMemoryStore()
			store.failures = tc.failures
			h, metrics := newHandler(store, tc.maxAttempts)

			outcome, err := h.Handle(sub.Context(), record(validPayload))
			if (err != nil) != tc.wantErr || outcome != tc.wantOutcome {
				sub.Fatalf("got %s / %v, want %s / err=%v", outcome, err, tc.wantOutcome, tc.wantErr)
			}
			if store.calls != tc.wantCalls {
				sub.Fatalf("storage calls = %d, want %d", store.calls, tc.wantCalls)
			}
			wantRetries := float64(tc.wantCalls - 1)
			if n := testutil.ToFloat64(metrics.retries); n != wantRetries {
				sub.Fatalf("retries counter = %v, want %v", n, wantRetries)
			}
		})
	}
}

func TestHandleStopsRetryingWhenContextEnds(t *testing.T) {
	store := newMemoryStore()
	store.failures = 100
	h, _ := newHandler(store, 100)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	outcome, err := h.Handle(ctx, record(validPayload))
	if !errors.Is(err, context.Canceled) || outcome != OutcomeFailed {
		t.Fatalf("got %s / %v, want failed / context.Canceled", outcome, err)
	}
	if store.calls != 1 {
		t.Fatalf("storage calls = %d, want 1 (no retries after cancellation)", store.calls)
	}
}

func TestHandleReportsConflictingEventForSameShipment(t *testing.T) {
	store := newMemoryStore()
	h, _ := newHandler(store, 3)
	if _, err := h.Handle(t.Context(), record(validPayload)); err != nil {
		t.Fatal(err)
	}

	// A second, distinct event for the same shipment: valid on its own, but
	// the shipment already has a dispatch from evt_1.
	other := `{"event_id":"evt_OTHER","event_type":"shipment.created","schema_version":1,` +
		`"shipment_id":"shp_1","occurred_at":"2026-09-15T16:00:01Z","correlation_id":"req_2",` +
		`"data":{"weight_grams":1500,"amount_cents":700,"currency":"USD"}}`
	outcome, err := h.Handle(t.Context(), Record{Partition: 0, Offset: 2, Value: []byte(other)})
	if !errors.Is(err, storage.ErrDispatchConflict) || outcome != OutcomeConflict {
		t.Fatalf("got %s / %v, want conflict", outcome, err)
	}
	if store.calls != 2 {
		t.Fatalf("conflicts must not be retried, got %d calls", store.calls)
	}
}
