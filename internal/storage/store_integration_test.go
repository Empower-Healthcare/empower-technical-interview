package storage

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/empower-healthcare/parcellab/internal/domain"
	"github.com/empower-healthcare/parcellab/internal/events"
)

// These tests need a real PostgreSQL because the guarantees under test
// (transaction atomicity, unique constraints under concurrency, SKIP LOCKED)
// are database behaviour, not Go behaviour. They run against the Compose
// database when TEST_DATABASE_URL is set, e.g.
//
//	TEST_DATABASE_URL=postgres://parcellab:parcellab_dev_password@127.0.0.1:5432/parcellab?sslmode=disable go test ./internal/storage
//
// and are skipped otherwise so `make test` works without the stack.

func testStore(t *testing.T) (*Store, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping PostgreSQL integration test")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	pool, err := Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := Migrate(ctx, pool, slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return New(pool), pool
}

func testShipment() (domain.Shipment, OutboxEvent) {
	shipment := domain.Shipment{
		ID:          domain.NewShipmentID(),
		WeightGrams: 1500,
		Price:       domain.Money{AmountCents: 700, Currency: "USD"},
		CreatedAt:   time.Now().UTC().Truncate(time.Microsecond),
	}
	evt := events.NewShipmentCreated(shipment, "req_storage_test")
	payload, err := events.Encode(evt)
	if err != nil {
		panic(err)
	}
	return shipment, OutboxEvent{EventID: evt.EventID, ShipmentID: shipment.ID, EventType: evt.EventType, Payload: payload}
}

func TestCreateShipmentIsAtomic(t *testing.T) {
	store, pool := testStore(t)
	ctx := t.Context()

	// A duplicate event_id makes the second INSERT fail; the shipment INSERT
	// that already ran in the same transaction must be rolled back with it.
	first, evt := testShipment()
	if err := store.CreateShipment(ctx, first, evt); err != nil {
		t.Fatalf("first create: %v", err)
	}
	second, _ := testShipment()
	evt.ShipmentID = second.ID
	if err := store.CreateShipment(ctx, second, evt); err == nil {
		t.Fatal("second create with a reused event_id must fail")
	}

	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM shipments WHERE shipment_id = $1`, string(second.ID)).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("shipment %s was stored although its outbox insert failed", second.ID)
	}
}

func TestRecordDispatchConcurrentSameEventCreatesOneDispatch(t *testing.T) {
	store, pool := testStore(t)
	ctx := t.Context()

	shipment, evt := testShipment()
	if err := store.CreateShipment(ctx, shipment, evt); err != nil {
		t.Fatal(err)
	}
	req := DispatchRequest{EventID: evt.EventID, ShipmentID: shipment.ID, AmountCents: 700, Currency: "USD"}

	// Many consumers race on the same event ID (redelivery to several
	// instances). Exactly one may create the dispatch; the rest must see a
	// duplicate, and none may fail.
	const workers = 8
	results := make([]struct {
		duplicate bool
		err       error
	}, workers)
	var start, done sync.WaitGroup
	start.Add(1)
	for i := range workers {
		done.Add(1)
		go func() {
			defer done.Done()
			start.Wait()
			_, results[i].duplicate, results[i].err = store.RecordDispatch(ctx, req)
		}()
	}
	start.Done()
	done.Wait()

	created := 0
	for i, r := range results {
		if r.err != nil {
			t.Errorf("worker %d: %v", i, r.err)
		}
		if !r.duplicate {
			created++
		}
	}
	if created != 1 {
		t.Fatalf("%d workers reported creating the dispatch, want exactly 1", created)
	}

	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM dispatches WHERE shipment_id = $1`, string(shipment.ID)).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("%d dispatch rows for %s, want 1", n, shipment.ID)
	}
}

func TestRecordDispatchSecondEventForSameShipmentConflicts(t *testing.T) {
	store, _ := testStore(t)
	ctx := t.Context()

	shipment, evt := testShipment()
	if err := store.CreateShipment(ctx, shipment, evt); err != nil {
		t.Fatal(err)
	}
	if _, dup, err := store.RecordDispatch(ctx, DispatchRequest{EventID: evt.EventID, ShipmentID: shipment.ID, AmountCents: 700, Currency: "USD"}); err != nil || dup {
		t.Fatalf("first dispatch: dup=%v err=%v", dup, err)
	}

	// A different event ID for the same shipment is not a duplicate; the
	// UNIQUE (shipment_id) constraint turns it into a conflict, and the
	// processed_events insert rolls back with it.
	other := DispatchRequest{EventID: domain.NewEventID(), ShipmentID: shipment.ID, AmountCents: 700, Currency: "USD"}
	_, _, err := store.RecordDispatch(ctx, other)
	if !errors.Is(err, ErrDispatchConflict) {
		t.Fatalf("got %v, want ErrDispatchConflict", err)
	}
	// Retrying the conflicting event must still be a conflict, not a duplicate:
	// nothing about it was recorded.
	if _, dup, err := store.RecordDispatch(ctx, other); !errors.Is(err, ErrDispatchConflict) || dup {
		t.Fatalf("retry: dup=%v err=%v, want conflict", dup, err)
	}
}

func TestProcessPendingLeavesFailedEventPendingAndMarksPublished(t *testing.T) {
	store, pool := testStore(t)
	ctx := t.Context()

	shipment, evt := testShipment()
	if err := store.CreateShipment(ctx, shipment, evt); err != nil {
		t.Fatal(err)
	}

	// The publish function decides which events succeed. Other tests' pending
	// rows may be in the batch, so we only make statements about our own.
	brokerDown := errors.New("broker down")
	fail := func(_ context.Context, batch []OutboxEvent) PublishResult {
		return PublishResult{Failed: batch[0].EventID, Err: brokerDown}
	}
	result, err := store.ProcessPending(ctx, 100, fail)
	if err != nil {
		t.Fatal(err)
	}
	if result.Failed == "" || !errors.Is(result.Err, brokerDown) {
		t.Fatalf("expected the publish failure to be reported, got %+v", result)
	}

	var attempts int
	var lastErr *string
	var publishedAt *time.Time
	if err := pool.QueryRow(ctx, `SELECT attempts, last_error, published_at FROM outbox_events WHERE event_id = $1`, string(evt.EventID)).
		Scan(&attempts, &lastErr, &publishedAt); err != nil {
		t.Fatal(err)
	}
	if publishedAt != nil {
		t.Fatal("failed event must stay pending")
	}
	if result.Failed == evt.EventID && (attempts != 1 || lastErr == nil) {
		t.Fatalf("failed event: attempts=%d last_error=%v, want 1 and non-null", attempts, lastErr)
	}

	ok := func(_ context.Context, batch []OutboxEvent) PublishResult {
		r := PublishResult{}
		for _, e := range batch {
			r.Published = append(r.Published, e.EventID)
		}
		return r
	}
	if _, err := store.ProcessPending(ctx, 100, ok); err != nil {
		t.Fatal(err)
	}
	view, err := store.GetShipment(ctx, shipment.ID)
	if err != nil {
		t.Fatal(err)
	}
	if view.Outbox != domain.OutboxPublished {
		t.Fatalf("outbox status %v, want published", view.Outbox)
	}
}
