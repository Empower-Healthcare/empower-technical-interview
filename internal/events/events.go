// Package events defines the shipment.created.v1 message contract that flows
// from the outbox through Kafka to the dispatch consumer.
package events

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/empower-healthcare/parcellab/internal/domain"
)

const (
	// Topic is the Kafka topic for shipment-created events. The version suffix
	// is part of the name so an incompatible payload can move to a new topic.
	Topic = "shipment.created.v1"

	// TypeShipmentCreated is the event_type carried in the payload.
	TypeShipmentCreated = "shipment.created"

	// SchemaVersion is the payload schema the consumer understands.
	SchemaVersion = 1
)

// Kafka header names carried alongside the payload.
const (
	HeaderCorrelationID = "correlation_id"
	HeaderEventType     = "event_type"
	HeaderSchemaVersion = "schema_version"
)

// ErrMalformed marks payloads that cannot be decoded or lack required fields.
var ErrMalformed = errors.New("malformed event")

// ErrUnsupported marks well-formed payloads with a type or schema version the
// consumer does not handle.
var ErrUnsupported = errors.New("unsupported event")

// ShipmentCreated is the JSON payload stored in outbox_events.payload and
// published to Kafka verbatim.
type ShipmentCreated struct {
	EventID       domain.EventID      `json:"event_id"`
	EventType     string              `json:"event_type"`
	SchemaVersion int                 `json:"schema_version"`
	ShipmentID    domain.ShipmentID   `json:"shipment_id"`
	OccurredAt    time.Time           `json:"occurred_at"`
	CorrelationID string              `json:"correlation_id"`
	Data          ShipmentCreatedData `json:"data"`
}

// ShipmentCreatedData carries the priced parcel. The amount is whatever the
// pricing service returned at creation time, so pricing changes (for example a
// new delivery speed) appear here without a schema change.
type ShipmentCreatedData struct {
	WeightGrams int32  `json:"weight_grams"`
	AmountCents int64  `json:"amount_cents"`
	Currency    string `json:"currency"`
}

// NewShipmentCreated builds the event for a freshly created shipment.
func NewShipmentCreated(s domain.Shipment, correlationID string) ShipmentCreated {
	return ShipmentCreated{
		EventID:       domain.NewEventID(),
		EventType:     TypeShipmentCreated,
		SchemaVersion: SchemaVersion,
		ShipmentID:    s.ID,
		OccurredAt:    s.CreatedAt.UTC(),
		CorrelationID: correlationID,
		Data: ShipmentCreatedData{
			WeightGrams: s.WeightGrams,
			AmountCents: s.Price.AmountCents,
			Currency:    s.Price.Currency,
		},
	}
}

// Encode serializes the event payload.
func Encode(e ShipmentCreated) ([]byte, error) {
	return json.Marshal(e)
}

// wireEvent is the JSON boundary shape. Every field is a pointer so Decode
// can tell "absent or null" from a legitimate zero value; nothing outside this
// file sees the pointers.
type wireEvent struct {
	EventID       *string    `json:"event_id"`
	EventType     *string    `json:"event_type"`
	SchemaVersion *int       `json:"schema_version"`
	ShipmentID    *string    `json:"shipment_id"`
	OccurredAt    *time.Time `json:"occurred_at"`
	CorrelationID *string    `json:"correlation_id"`
	Data          *struct {
		WeightGrams *int32  `json:"weight_grams"`
		AmountCents *int64  `json:"amount_cents"`
		Currency    *string `json:"currency"`
	} `json:"data"`
}

// Decode parses and validates a payload. Every field of the v1 contract is
// required; a missing or null field is ErrMalformed, as is a value that
// violates the domain rules (weight range, non-negative amount, currency
// code). A well-formed payload with a type or schema version the consumer does
// not handle is ErrUnsupported. Type and version are checked first so a future
// schema is reported as unsupported even if its required fields differ.
func Decode(payload []byte) (ShipmentCreated, error) {
	var w wireEvent
	if err := json.Unmarshal(payload, &w); err != nil {
		return ShipmentCreated{}, fmt.Errorf("%w: %w", ErrMalformed, err)
	}

	switch {
	case w.EventType == nil:
		return ShipmentCreated{}, missing("event_type")
	case *w.EventType != TypeShipmentCreated:
		return ShipmentCreated{}, fmt.Errorf("%w: event_type %q", ErrUnsupported, *w.EventType)
	case w.SchemaVersion == nil:
		return ShipmentCreated{}, missing("schema_version")
	case *w.SchemaVersion != SchemaVersion:
		return ShipmentCreated{}, fmt.Errorf("%w: schema_version %d", ErrUnsupported, *w.SchemaVersion)
	}

	switch {
	case w.EventID == nil || *w.EventID == "":
		return ShipmentCreated{}, missing("event_id")
	case w.ShipmentID == nil || *w.ShipmentID == "":
		return ShipmentCreated{}, missing("shipment_id")
	case w.OccurredAt == nil:
		return ShipmentCreated{}, missing("occurred_at")
	case w.CorrelationID == nil:
		return ShipmentCreated{}, missing("correlation_id")
	case w.Data == nil:
		return ShipmentCreated{}, missing("data")
	case w.Data.WeightGrams == nil:
		return ShipmentCreated{}, missing("data.weight_grams")
	case w.Data.AmountCents == nil:
		return ShipmentCreated{}, missing("data.amount_cents")
	case w.Data.Currency == nil:
		return ShipmentCreated{}, missing("data.currency")
	}

	if err := domain.ValidateWeight(*w.Data.WeightGrams); err != nil {
		return ShipmentCreated{}, fmt.Errorf("%w: data.weight_grams: %w", ErrMalformed, err)
	}
	money := domain.Money{AmountCents: *w.Data.AmountCents, Currency: *w.Data.Currency}
	if err := domain.ValidateMoney(money); err != nil {
		return ShipmentCreated{}, fmt.Errorf("%w: data: %w", ErrMalformed, err)
	}

	return ShipmentCreated{
		EventID:       domain.EventID(*w.EventID),
		EventType:     *w.EventType,
		SchemaVersion: *w.SchemaVersion,
		ShipmentID:    domain.ShipmentID(*w.ShipmentID),
		OccurredAt:    *w.OccurredAt,
		CorrelationID: *w.CorrelationID,
		Data: ShipmentCreatedData{
			WeightGrams: *w.Data.WeightGrams,
			AmountCents: money.AmountCents,
			Currency:    money.Currency,
		},
	}, nil
}

func missing(field string) error {
	return fmt.Errorf("%w: missing %s", ErrMalformed, field)
}
