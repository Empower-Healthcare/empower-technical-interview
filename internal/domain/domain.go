// Package domain holds the ParcelLab business types and rules shared by the
// gRPC service, the outbox relay and the dispatch consumer.
//
// Everything here is fictional and exists only for the interview exercise.
package domain

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

const (
	// MinWeightGrams and MaxWeightGrams bound the parcel weights the platform accepts.
	MinWeightGrams int32 = 1
	MaxWeightGrams int32 = 30_000
)

// ErrInvalidWeight is returned for weights outside [MinWeightGrams, MaxWeightGrams].
var ErrInvalidWeight = errors.New("weight_grams must be between 1 and 30000")

// ErrNotFound is returned when a shipment or event does not exist.
var ErrNotFound = errors.New("not found")

// ValidateWeight checks the shared weight rule. The pricing service enforces
// the same bound; validating here avoids a downstream call for obvious input errors.
func ValidateWeight(weightGrams int32) error {
	if weightGrams < MinWeightGrams || weightGrams > MaxWeightGrams {
		return fmt.Errorf("%w: got %d", ErrInvalidWeight, weightGrams)
	}
	return nil
}

// Identifiers are distinct types so a shipment ID cannot be passed where an
// event ID is expected. Their string form is "<prefix>_<16 hex chars>".
type (
	ShipmentID string
	EventID    string
	DispatchID string
)

// NewShipmentID, NewEventID and NewDispatchID return fresh random identifiers.
func NewShipmentID() ShipmentID { return ShipmentID(newID("shp")) }
func NewEventID() EventID       { return EventID(newID("evt")) }
func NewDispatchID() DispatchID { return DispatchID(newID("dsp")) }

// NewCorrelationID returns a request-scoped correlation ID such as "req_9f86d081884c7d65".
func NewCorrelationID() string { return newID("req") }

func newID(prefix string) string {
	var b [8]byte
	rand.Read(b[:]) // crypto/rand.Read never returns an error as of Go 1.24
	return prefix + "_" + hex.EncodeToString(b[:])
}

// Money is an integer amount in the smallest currency unit.
type Money struct {
	AmountCents int64
	Currency    string
}

// ErrInvalidMoney is returned for a negative amount or a currency that is not
// a three-letter uppercase ISO 4217 code.
var ErrInvalidMoney = errors.New("invalid money")

// ValidateMoney checks the invariants every persisted or published amount must
// hold. Zero is a valid amount; the pricing rules decide whether it can occur.
func ValidateMoney(m Money) error {
	if m.AmountCents < 0 {
		return fmt.Errorf("%w: amount_cents %d is negative", ErrInvalidMoney, m.AmountCents)
	}
	if !isCurrencyCode(m.Currency) {
		return fmt.Errorf("%w: currency %q is not a 3-letter uppercase code", ErrInvalidMoney, m.Currency)
	}
	return nil
}

func isCurrencyCode(s string) bool {
	if len(s) != 3 {
		return false
	}
	for i := range 3 {
		if s[i] < 'A' || s[i] > 'Z' {
			return false
		}
	}
	return true
}

// Shipment is the persisted shipment record.
type Shipment struct {
	ID          ShipmentID
	WeightGrams int32
	Price       Money
	CreatedAt   time.Time
}

// OutboxStatus is the publication state of a shipment's outbox event.
type OutboxStatus uint8

const (
	OutboxPending   OutboxStatus = iota + 1 // stored, not yet acknowledged by Kafka
	OutboxPublished                         // acknowledged by Kafka and marked published by the relay
)

// DispatchStatus is the asynchronous processing state of a shipment.
type DispatchStatus uint8

const (
	DispatchPending    DispatchStatus = iota + 1 // no dispatch row yet; the event has not been consumed
	DispatchDispatched                           // the consumer persisted exactly one dispatch row
)

// Dispatch is the record the consumer creates once per shipment. When Status
// is DispatchPending the remaining fields are zero.
type Dispatch struct {
	Status       DispatchStatus
	ID           DispatchID
	EventID      EventID
	DispatchedAt time.Time
}

// ShipmentView is what GetShipment returns: the shipment plus the state of its
// asynchronous processing.
type ShipmentView struct {
	Shipment Shipment
	Outbox   OutboxStatus
	Dispatch Dispatch
}
