// Package storage is the PostgreSQL persistence layer shared by the shipment
// service, the outbox relay and the dispatch consumer. Every consistency
// boundary in the system (shipment + outbox event, processed event + dispatch)
// is a method on Store that runs in one transaction.
package storage

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/empower-healthcare/parcellab/internal/domain"
)

// ErrDispatchConflict is returned when a *different* event ID tries to create
// a dispatch for a shipment that already has one. This never happens on the
// happy path; it indicates a producer bug rather than a redelivery.
var ErrDispatchConflict = errors.New("shipment already dispatched by another event")

// ErrUnavailable wraps failures to reach PostgreSQL (connection refused, pool
// acquire timeout, broken connection). Callers map it to a retryable status;
// every other database error is unexpected and stays unclassified.
var ErrUnavailable = errors.New("database unavailable")

const pgUniqueViolation = "23505"

// classify wraps connectivity failures in ErrUnavailable and leaves everything
// else (including context errors and constraint violations) unchanged.
func classify(err error) error {
	if err == nil {
		return nil
	}
	var (
		connectErr *pgconn.ConnectError
		netErr     net.Error
	)
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return err
	case errors.As(err, &connectErr), errors.As(err, &netErr), pgconn.SafeToRetry(err):
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	default:
		return err
	}
}

// Open connects to PostgreSQL and verifies the connection.
func Open(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	cfg.MaxConns = 8
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return pool, nil
}

// Store wraps a connection pool.
type Store struct {
	pool *pgxpool.Pool
}

// New returns a Store.
func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// Ping is used by readiness checks.
func (s *Store) Ping(ctx context.Context) error { return classify(s.pool.Ping(ctx)) }

// OutboxEvent is a row in outbox_events. Payload is the JSON published to Kafka.
type OutboxEvent struct {
	EventID    domain.EventID
	ShipmentID domain.ShipmentID
	EventType  string
	Payload    []byte
	CreatedAt  time.Time
	Attempts   int
}

// CreateShipment inserts the shipment and its outbox event in one transaction,
// so either both exist or neither does. Kafka is not involved here: the
// synchronous request path ends when this transaction commits.
func (s *Store) CreateShipment(ctx context.Context, shipment domain.Shipment, evt OutboxEvent) error {
	return classify(pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			INSERT INTO shipments (shipment_id, weight_grams, amount_cents, currency, created_at)
			VALUES ($1, $2, $3, $4, $5)`,
			string(shipment.ID), shipment.WeightGrams, shipment.Price.AmountCents, shipment.Price.Currency, shipment.CreatedAt,
		); err != nil {
			return fmt.Errorf("insert shipment: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO outbox_events (event_id, shipment_id, event_type, payload, created_at)
			VALUES ($1, $2, $3, $4, $5)`,
			string(evt.EventID), string(evt.ShipmentID), evt.EventType, evt.Payload, shipment.CreatedAt,
		); err != nil {
			return fmt.Errorf("insert outbox event: %w", err)
		}
		return nil
	}))
}

// shipmentRow is the flat result of the GetShipment query; nullable columns
// are pointers only here, at the database boundary.
type shipmentRow struct {
	shipmentID   string
	weightGrams  int32
	amountCents  int64
	currency     string
	createdAt    time.Time
	publishedAt  *time.Time
	dispatchID   *string
	eventID      *string
	dispatchedAt *time.Time
}

func (r shipmentRow) view() domain.ShipmentView {
	v := domain.ShipmentView{
		Shipment: domain.Shipment{
			ID:          domain.ShipmentID(r.shipmentID),
			WeightGrams: r.weightGrams,
			Price:       domain.Money{AmountCents: r.amountCents, Currency: r.currency},
			CreatedAt:   r.createdAt,
		},
		Outbox:   domain.OutboxPending,
		Dispatch: domain.Dispatch{Status: domain.DispatchPending},
	}
	if r.publishedAt != nil {
		v.Outbox = domain.OutboxPublished
	}
	if r.dispatchID != nil {
		v.Dispatch = domain.Dispatch{
			Status:       domain.DispatchDispatched,
			ID:           domain.DispatchID(*r.dispatchID),
			EventID:      domain.EventID(*r.eventID),
			DispatchedAt: *r.dispatchedAt,
		}
	}
	return v
}

// GetShipment loads a shipment with its outbox and dispatch state.
func (s *Store) GetShipment(ctx context.Context, id domain.ShipmentID) (domain.ShipmentView, error) {
	var r shipmentRow
	err := s.pool.QueryRow(ctx, `
		SELECT s.shipment_id, s.weight_grams, s.amount_cents, s.currency, s.created_at,
		       o.published_at,
		       d.dispatch_id, d.event_id, d.dispatched_at
		FROM shipments s
		LEFT JOIN outbox_events o ON o.shipment_id = s.shipment_id AND o.event_type = 'shipment.created'
		LEFT JOIN dispatches    d ON d.shipment_id = s.shipment_id
		WHERE s.shipment_id = $1`, string(id),
	).Scan(
		&r.shipmentID, &r.weightGrams, &r.amountCents, &r.currency, &r.createdAt,
		&r.publishedAt,
		&r.dispatchID, &r.eventID, &r.dispatchedAt,
	)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return domain.ShipmentView{}, fmt.Errorf("shipment %s: %w", id, domain.ErrNotFound)
	case err != nil:
		return domain.ShipmentView{}, fmt.Errorf("query shipment: %w", classify(err))
	}
	return r.view(), nil
}

// PublishResult reports what a relay did with a claimed batch: which events the
// broker acknowledged and, if publishing stopped early, which event failed.
type PublishResult struct {
	Published []domain.EventID
	Failed    domain.EventID // zero when every event was published
	Err       error          // the failure for Failed, nil otherwise
}

// PublishFunc publishes a claimed batch in order and stops at the first failure.
type PublishFunc func(ctx context.Context, batch []OutboxEvent) PublishResult

// ProcessPending claims up to limit unpublished events with FOR UPDATE SKIP
// LOCKED, hands the batch to publish, then records the result in the same
// transaction: acknowledged events get published_at, a failed event gets its
// attempts/last_error updated and stays pending. Several relay instances can
// run concurrently; the row locks stop them from claiming the same events.
//
// Delivery is at-least-once: if the process dies after the broker acknowledged
// an event but before this transaction commits, the row stays pending and is
// published again with the same event_id. The consumer deduplicates on that ID.
func (s *Store) ProcessPending(ctx context.Context, limit int, publish PublishFunc) (PublishResult, error) {
	var result PublishResult
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		batch, err := claimPending(ctx, tx, limit)
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			return nil
		}

		result = publish(ctx, batch)

		if len(result.Published) > 0 {
			if _, err := tx.Exec(ctx, `
				UPDATE outbox_events
				SET published_at = now(), attempts = attempts + 1, last_error = NULL
				WHERE event_id = ANY($1)`, eventIDStrings(result.Published)); err != nil {
				return fmt.Errorf("mark published: %w", err)
			}
		}
		if result.Failed != "" {
			if _, err := tx.Exec(ctx, `
				UPDATE outbox_events
				SET attempts = attempts + 1, last_error = $2
				WHERE event_id = $1`, string(result.Failed), result.Err.Error()); err != nil {
				return fmt.Errorf("record publish failure: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return PublishResult{}, classify(err)
	}
	return result, nil
}

func claimPending(ctx context.Context, tx pgx.Tx, limit int) ([]OutboxEvent, error) {
	rows, err := tx.Query(ctx, `
		SELECT event_id, shipment_id, event_type, payload, created_at, attempts
		FROM outbox_events
		WHERE published_at IS NULL
		ORDER BY created_at
		LIMIT $1
		FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return nil, fmt.Errorf("select pending: %w", err)
	}
	batch, err := pgx.CollectRows(rows, scanOutboxEvent)
	if err != nil {
		return nil, fmt.Errorf("scan pending: %w", err)
	}
	return batch, nil
}

func scanOutboxEvent(row pgx.CollectableRow) (OutboxEvent, error) {
	var (
		e                   OutboxEvent
		eventID, shipmentID string
	)
	err := row.Scan(&eventID, &shipmentID, &e.EventType, &e.Payload, &e.CreatedAt, &e.Attempts)
	e.EventID, e.ShipmentID = domain.EventID(eventID), domain.ShipmentID(shipmentID)
	return e, err
}

func eventIDStrings(ids []domain.EventID) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = string(id)
	}
	return out
}

// CountPending returns the outbox backlog, exported as a gauge by the relay.
func (s *Store) CountPending(ctx context.Context) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE published_at IS NULL`).Scan(&n)
	return n, classify(err)
}

// GetOutboxEvent loads one event by ID, published or not. Used by the replay helper.
func (s *Store) GetOutboxEvent(ctx context.Context, id domain.EventID) (OutboxEvent, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT event_id, shipment_id, event_type, payload, created_at, attempts
		FROM outbox_events WHERE event_id = $1`, string(id))
	if err != nil {
		return OutboxEvent{}, classify(err)
	}
	e, err := pgx.CollectExactlyOneRow(rows, scanOutboxEvent)
	if errors.Is(err, pgx.ErrNoRows) {
		return OutboxEvent{}, fmt.Errorf("outbox event %s: %w", id, domain.ErrNotFound)
	}
	return e, classify(err)
}

// DispatchRequest is what the consumer asks the store to persist.
type DispatchRequest struct {
	EventID     domain.EventID
	ShipmentID  domain.ShipmentID
	AmountCents int64
	Currency    string
}

// RecordDispatch marks the event processed and creates the dispatch in one
// transaction. When the event ID is already in processed_events nothing is
// written and the returned Dispatch has Status == DispatchPending with the
// duplicate flag set.
//
// The consumer commits its Kafka offset only after this returns, so a crash
// between the database commit and the offset commit leads to a redelivery that
// is recognised here as a duplicate.
func (s *Store) RecordDispatch(ctx context.Context, req DispatchRequest) (dispatch domain.Dispatch, duplicate bool, err error) {
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			INSERT INTO processed_events (event_id) VALUES ($1)
			ON CONFLICT (event_id) DO NOTHING`, string(req.EventID))
		if err != nil {
			return fmt.Errorf("insert processed_event: %w", err)
		}
		if tag.RowsAffected() == 0 {
			duplicate = true
			return nil
		}

		dispatch = domain.Dispatch{
			Status:  domain.DispatchDispatched,
			ID:      domain.NewDispatchID(),
			EventID: req.EventID,
		}
		err = tx.QueryRow(ctx, `
			INSERT INTO dispatches (dispatch_id, shipment_id, event_id, amount_cents, currency)
			VALUES ($1, $2, $3, $4, $5)
			RETURNING dispatched_at`,
			string(dispatch.ID), string(req.ShipmentID), string(req.EventID), req.AmountCents, req.Currency,
		).Scan(&dispatch.DispatchedAt)
		if isUniqueViolation(err) {
			return fmt.Errorf("%w: shipment %s, event %s", ErrDispatchConflict, req.ShipmentID, req.EventID)
		}
		if err != nil {
			return fmt.Errorf("insert dispatch: %w", err)
		}
		return nil
	})
	if err != nil {
		return domain.Dispatch{}, false, classify(err)
	}
	return dispatch, duplicate, nil
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation
}
