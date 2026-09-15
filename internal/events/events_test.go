package events

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/empower-healthcare/parcellab/internal/domain"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	shipment := domain.Shipment{
		ID:          "shp_fictional_001",
		WeightGrams: 1500,
		Price:       domain.Money{AmountCents: 700, Currency: "USD"},
		CreatedAt:   time.Date(2026, 9, 15, 16, 0, 0, 0, time.UTC),
	}
	want := NewShipmentCreated(shipment, "req_fictional_001")

	payload, err := Encode(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Decode(payload)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got != want {
		t.Fatalf("round trip mismatch:\n got %+v\nwant %+v", got, want)
	}
	if got.EventType != TypeShipmentCreated || got.SchemaVersion != SchemaVersion {
		t.Fatalf("unexpected type/version: %s/%d", got.EventType, got.SchemaVersion)
	}
}

// validPayload is the v1 contract with every field present. Each Decode case
// below mutates exactly one field of it so the failing field is unambiguous.
func validPayload() map[string]any {
	return map[string]any{
		"event_id":       "evt_1",
		"event_type":     "shipment.created",
		"schema_version": 1,
		"shipment_id":    "shp_1",
		"occurred_at":    "2026-09-15T16:00:00Z",
		"correlation_id": "req_1",
		"data": map[string]any{
			"weight_grams": 1500,
			"amount_cents": 700,
			"currency":     "USD",
		},
	}
}

func TestDecode(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(p map[string]any) // nil leaves the valid payload untouched
		wantErr error
	}{
		{name: "valid", mutate: nil, wantErr: nil},
		{name: "zero amount is valid", mutate: func(p map[string]any) { data(p)["amount_cents"] = 0 }, wantErr: nil},
		{name: "unknown type", mutate: func(p map[string]any) { p["event_type"] = "shipment.cancelled" }, wantErr: ErrUnsupported},
		{name: "future schema", mutate: func(p map[string]any) { p["schema_version"] = 2 }, wantErr: ErrUnsupported},
		{name: "future schema wins over missing data", mutate: func(p map[string]any) { p["schema_version"] = 2; delete(p, "data") }, wantErr: ErrUnsupported},
		{name: "missing event_type", mutate: func(p map[string]any) { delete(p, "event_type") }, wantErr: ErrMalformed},
		{name: "missing schema_version", mutate: func(p map[string]any) { delete(p, "schema_version") }, wantErr: ErrMalformed},
		{name: "missing event_id", mutate: func(p map[string]any) { delete(p, "event_id") }, wantErr: ErrMalformed},
		{name: "empty event_id", mutate: func(p map[string]any) { p["event_id"] = "" }, wantErr: ErrMalformed},
		{name: "missing shipment_id", mutate: func(p map[string]any) { delete(p, "shipment_id") }, wantErr: ErrMalformed},
		{name: "missing occurred_at", mutate: func(p map[string]any) { delete(p, "occurred_at") }, wantErr: ErrMalformed},
		{name: "missing correlation_id", mutate: func(p map[string]any) { delete(p, "correlation_id") }, wantErr: ErrMalformed},
		{name: "missing data", mutate: func(p map[string]any) { delete(p, "data") }, wantErr: ErrMalformed},
		{name: "missing weight", mutate: func(p map[string]any) { delete(data(p), "weight_grams") }, wantErr: ErrMalformed},
		{name: "missing amount", mutate: func(p map[string]any) { delete(data(p), "amount_cents") }, wantErr: ErrMalformed},
		{name: "null amount", mutate: func(p map[string]any) { data(p)["amount_cents"] = nil }, wantErr: ErrMalformed},
		{name: "negative amount", mutate: func(p map[string]any) { data(p)["amount_cents"] = -1 }, wantErr: ErrMalformed},
		{name: "missing currency", mutate: func(p map[string]any) { delete(data(p), "currency") }, wantErr: ErrMalformed},
		{name: "lowercase currency", mutate: func(p map[string]any) { data(p)["currency"] = "usd" }, wantErr: ErrMalformed},
		{name: "weight out of range", mutate: func(p map[string]any) { data(p)["weight_grams"] = 30_001 }, wantErr: ErrMalformed},
		{name: "wrong amount type", mutate: func(p map[string]any) { data(p)["amount_cents"] = "700" }, wantErr: ErrMalformed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(sub *testing.T) {
			p := validPayload()
			if tc.mutate != nil {
				tc.mutate(p)
			}
			payload, err := json.Marshal(p)
			if err != nil {
				sub.Fatal(err)
			}
			got, err := Decode(payload)
			if !errors.Is(err, tc.wantErr) {
				sub.Fatalf("Decode error = %v, want %v", err, tc.wantErr)
			}
			if err == nil && (got.EventID != "evt_1" || got.ShipmentID != "shp_1" || got.Data.WeightGrams != 1500) {
				sub.Fatalf("decoded fields lost: %+v", got)
			}
		})
	}
}

func TestDecodeRejectsNonJSON(t *testing.T) {
	if _, err := Decode([]byte(`{"event_id":`)); !errors.Is(err, ErrMalformed) {
		t.Fatalf("Decode error = %v, want ErrMalformed", err)
	}
}

func data(p map[string]any) map[string]any { return p["data"].(map[string]any) }
