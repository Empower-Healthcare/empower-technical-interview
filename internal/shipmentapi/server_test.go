package shipmentapi

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	parcellabv1 "github.com/empower-healthcare/parcellab/gen/parcellab/v1"
	"github.com/empower-healthcare/parcellab/internal/correlation"
	"github.com/empower-healthcare/parcellab/internal/domain"
	"github.com/empower-healthcare/parcellab/internal/events"
	"github.com/empower-healthcare/parcellab/internal/pricing"
	"github.com/empower-healthcare/parcellab/internal/storage"
)

// fixedNow is the clock every test server runs on.
func fixedNow() time.Time { return time.Date(2026, 9, 15, 16, 0, 0, 0, time.UTC) }

// usd700 is the price of the 1,500 g example.
func usd700() domain.Money { return domain.Money{AmountCents: 700, Currency: "USD"} }

type fakePricer struct {
	money     domain.Money
	err       error
	calls     int
	lastSpeed domain.DeliverySpeed
}

func (f *fakePricer) Quote(_ context.Context, _ int32, speed domain.DeliverySpeed) (domain.Money, error) {
	f.calls++
	f.lastSpeed = speed
	return f.money, f.err
}

// fakeStore records what the service asked it to persist.
type fakeStore struct {
	shipments map[domain.ShipmentID]domain.ShipmentView
	outbox    []storage.OutboxEvent
	err       error
}

func newFakeStore() *fakeStore {
	return &fakeStore{shipments: map[domain.ShipmentID]domain.ShipmentView{}}
}

func (f *fakeStore) CreateShipment(_ context.Context, s domain.Shipment, evt storage.OutboxEvent) error {
	if f.err != nil {
		return f.err
	}
	f.shipments[s.ID] = domain.ShipmentView{Shipment: s, Outbox: domain.OutboxPending, Dispatch: domain.Dispatch{Status: domain.DispatchPending}}
	f.outbox = append(f.outbox, evt)
	return nil
}

func (f *fakeStore) GetShipment(_ context.Context, id domain.ShipmentID) (domain.ShipmentView, error) {
	if f.err != nil {
		return domain.ShipmentView{}, f.err
	}
	v, ok := f.shipments[id]
	if !ok {
		return domain.ShipmentView{}, domain.ErrNotFound
	}
	return v, nil
}

func newTestServer(pricer Pricer, store Store) *Server {
	return New(pricer, store, slog.New(slog.DiscardHandler), fixedNow)
}

func TestGetQuoteReturnsPricingResult(t *testing.T) {
	srv := newTestServer(&fakePricer{money: usd700()}, newFakeStore())

	resp, err := srv.GetQuote(t.Context(), &parcellabv1.GetQuoteRequest{WeightGrams: 1500})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetPrice().GetAmountCents() != 700 || resp.GetPrice().GetCurrency() != "USD" {
		t.Fatalf("unexpected price %v", resp.GetPrice())
	}
}

func TestInvalidWeightIsRejectedBeforePricing(t *testing.T) {
	cases := []struct {
		name        string
		weightGrams int32
	}{
		{name: "zero (proto default)", weightGrams: 0},
		{name: "negative", weightGrams: -5},
		{name: "above maximum", weightGrams: 30_001},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(sub *testing.T) {
			pricer := &fakePricer{money: usd700()}
			store := newFakeStore()
			srv := newTestServer(pricer, store)

			_, quoteErr := srv.GetQuote(sub.Context(), &parcellabv1.GetQuoteRequest{WeightGrams: tc.weightGrams})
			_, createErr := srv.CreateShipment(sub.Context(), &parcellabv1.CreateShipmentRequest{WeightGrams: tc.weightGrams})
			for _, err := range []error{quoteErr, createErr} {
				if status.Code(err) != codes.InvalidArgument {
					sub.Fatalf("want InvalidArgument, got %v", err)
				}
			}
			if pricer.calls != 0 {
				sub.Fatalf("pricing must not be called for invalid input, got %d calls", pricer.calls)
			}
			if len(store.outbox) != 0 {
				sub.Fatalf("nothing may be stored for invalid input")
			}
		})
	}
}

// TestDeliverySpeedMapping: an unset field (proto3 zero value) is priced as standard.
func TestDeliverySpeedMapping(t *testing.T) {
	cases := []struct {
		name  string
		speed parcellabv1.DeliverySpeed
		want  domain.DeliverySpeed
	}{
		{name: "unset (zero value) is standard", speed: parcellabv1.DeliverySpeed_DELIVERY_SPEED_UNSPECIFIED, want: domain.DeliveryStandard},
		{name: "standard", speed: parcellabv1.DeliverySpeed_DELIVERY_SPEED_STANDARD, want: domain.DeliveryStandard},
		{name: "express", speed: parcellabv1.DeliverySpeed_DELIVERY_SPEED_EXPRESS, want: domain.DeliveryExpress},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(sub *testing.T) {
			pricer := &fakePricer{money: usd700()}
			srv := newTestServer(pricer, newFakeStore())

			if _, err := srv.GetQuote(sub.Context(), &parcellabv1.GetQuoteRequest{WeightGrams: 1500, DeliverySpeed: tc.speed}); err != nil {
				sub.Fatal(err)
			}
			if pricer.lastSpeed != tc.want {
				sub.Fatalf("GetQuote priced speed %d, want %d", pricer.lastSpeed, tc.want)
			}

			pricer.lastSpeed = 0
			if _, err := srv.CreateShipment(sub.Context(), &parcellabv1.CreateShipmentRequest{WeightGrams: 1500, DeliverySpeed: tc.speed}); err != nil {
				sub.Fatal(err)
			}
			if pricer.lastSpeed != tc.want {
				sub.Fatalf("CreateShipment priced speed %d, want %d", pricer.lastSpeed, tc.want)
			}
		})
	}
}

// TestUnsupportedDeliverySpeedIsRejectedBeforePricing: proto3 enums are open, so
// an unnamed number can arrive. It fails like a bad weight: no call, no row.
func TestUnsupportedDeliverySpeedIsRejectedBeforePricing(t *testing.T) {
	for _, speed := range []parcellabv1.DeliverySpeed{3, 99, -1} {
		t.Run(speed.String(), func(sub *testing.T) {
			pricer := &fakePricer{money: usd700()}
			store := newFakeStore()
			srv := newTestServer(pricer, store)

			_, quoteErr := srv.GetQuote(sub.Context(), &parcellabv1.GetQuoteRequest{WeightGrams: 1500, DeliverySpeed: speed})
			_, createErr := srv.CreateShipment(sub.Context(), &parcellabv1.CreateShipmentRequest{WeightGrams: 1500, DeliverySpeed: speed})
			for _, err := range []error{quoteErr, createErr} {
				if status.Code(err) != codes.InvalidArgument {
					sub.Fatalf("want InvalidArgument, got %v", err)
				}
			}
			if pricer.calls != 0 {
				sub.Fatalf("pricing must not be called for an unsupported speed, got %d calls", pricer.calls)
			}
			if len(store.outbox) != 0 {
				sub.Fatalf("nothing may be stored for an unsupported speed")
			}
		})
	}
}

func TestToStatus(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		wantCode codes.Code
		wantMsg  string // "" means "do not check"
	}{
		{name: "invalid weight", err: domain.ValidateWeight(0), wantCode: codes.InvalidArgument},
		{name: "invalid delivery speed", err: domain.ErrInvalidDeliverySpeed, wantCode: codes.InvalidArgument},
		{name: "not found", err: domain.ErrNotFound, wantCode: codes.NotFound},
		{name: "pricing rejected", err: pricing.ErrRejected, wantCode: codes.InvalidArgument},
		{name: "pricing unavailable", err: pricing.ErrUnavailable, wantCode: codes.Unavailable},
		{name: "pricing timeout", err: pricing.ErrTimeout, wantCode: codes.DeadlineExceeded},
		{name: "storage unavailable", err: storage.ErrUnavailable, wantCode: codes.Unavailable},
		{name: "caller deadline", err: context.DeadlineExceeded, wantCode: codes.DeadlineExceeded},
		{name: "caller cancelled", err: context.Canceled, wantCode: codes.Canceled},
		{name: "wrapped unavailable", err: errors.Join(errors.New("ctx"), pricing.ErrUnavailable), wantCode: codes.Unavailable},
		{name: "existing status passes through", err: status.Error(codes.PermissionDenied, "nope"), wantCode: codes.PermissionDenied, wantMsg: "nope"},
		{name: "unknown error is internal and redacted", err: errors.New("disk on fire"), wantCode: codes.Internal, wantMsg: "internal error"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(sub *testing.T) {
			got := status.Convert(toStatus(tc.err))
			if got.Code() != tc.wantCode {
				sub.Fatalf("toStatus(%v) code = %v, want %v", tc.err, got.Code(), tc.wantCode)
			}
			if tc.wantMsg != "" && got.Message() != tc.wantMsg {
				sub.Fatalf("toStatus(%v) message = %q, want %q", tc.err, got.Message(), tc.wantMsg)
			}
		})
	}
}

func TestCreateShipmentStoresShipmentAndEventTogether(t *testing.T) {
	store := newFakeStore()
	srv := newTestServer(&fakePricer{money: usd700()}, store)
	ctx := correlation.WithID(t.Context(), "req_test_42")

	resp, err := srv.CreateShipment(ctx, &parcellabv1.CreateShipmentRequest{WeightGrams: 1500})
	if err != nil {
		t.Fatal(err)
	}
	got := resp.GetShipment()
	if got.GetPrice().GetAmountCents() != 700 || got.GetWeightGrams() != 1500 {
		t.Fatalf("unexpected shipment %v", got)
	}
	if !got.GetCreatedAt().AsTime().Equal(fixedNow()) {
		t.Fatalf("created_at = %v, want %v", got.GetCreatedAt().AsTime(), fixedNow())
	}
	if got.GetOutboxStatus() != parcellabv1.OutboxStatus_OUTBOX_STATUS_PENDING {
		t.Fatalf("new shipment should report a pending outbox event, got %v", got.GetOutboxStatus())
	}
	if got.GetDispatch().GetStatus() != parcellabv1.DispatchStatus_DISPATCH_STATUS_PENDING {
		t.Fatalf("new shipment should report a pending dispatch, got %v", got.GetDispatch())
	}

	if len(store.outbox) != 1 {
		t.Fatalf("expected exactly one outbox event, got %d", len(store.outbox))
	}
	row := store.outbox[0]
	if string(row.ShipmentID) != got.GetShipmentId() || row.EventType != events.TypeShipmentCreated {
		t.Fatalf("outbox row does not reference the shipment: %+v", row)
	}
	payload, err := events.Decode(row.Payload)
	if err != nil {
		t.Fatalf("stored payload must be a valid event: %v", err)
	}
	want := events.ShipmentCreated{
		EventID:       row.EventID,
		EventType:     events.TypeShipmentCreated,
		SchemaVersion: events.SchemaVersion,
		ShipmentID:    row.ShipmentID,
		OccurredAt:    fixedNow(),
		CorrelationID: "req_test_42",
		Data:          events.ShipmentCreatedData{WeightGrams: 1500, AmountCents: 700, Currency: "USD"},
	}
	if payload != want {
		t.Fatalf("payload mismatch:\n got %+v\nwant %+v", payload, want)
	}
}

// TestCreateShipmentExpressStoresExpressPrice: amount_cents already carries the
// surcharge, so no new column or event field is needed.
func TestCreateShipmentExpressStoresExpressPrice(t *testing.T) {
	store := newFakeStore()
	srv := newTestServer(&fakePricer{money: domain.Money{AmountCents: 1200, Currency: "USD"}}, store)

	resp, err := srv.CreateShipment(t.Context(), &parcellabv1.CreateShipmentRequest{
		WeightGrams:   1500,
		DeliverySpeed: parcellabv1.DeliverySpeed_DELIVERY_SPEED_EXPRESS,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := resp.GetShipment().GetPrice().GetAmountCents(); got != 1200 {
		t.Fatalf("response amount_cents = %d, want 1200", got)
	}
	if got := store.shipments[domain.ShipmentID(resp.GetShipment().GetShipmentId())].Shipment.Price.AmountCents; got != 1200 {
		t.Fatalf("stored amount_cents = %d, want 1200", got)
	}
	payload, err := events.Decode(store.outbox[0].Payload)
	if err != nil {
		t.Fatal(err)
	}
	if payload.Data.AmountCents != 1200 {
		t.Fatalf("event amount_cents = %d, want 1200", payload.Data.AmountCents)
	}
}

func TestCreateShipmentFailures(t *testing.T) {
	cases := []struct {
		name     string
		pricer   *fakePricer
		storeErr error
		wantCode codes.Code
	}{
		{name: "pricing unavailable", pricer: &fakePricer{err: pricing.ErrUnavailable}, wantCode: codes.Unavailable},
		{name: "pricing timeout", pricer: &fakePricer{err: pricing.ErrTimeout}, wantCode: codes.DeadlineExceeded},
		{name: "storage failure", pricer: &fakePricer{money: usd700()}, storeErr: errors.New("constraint violated"), wantCode: codes.Internal},
		{name: "storage unavailable", pricer: &fakePricer{money: usd700()}, storeErr: storage.ErrUnavailable, wantCode: codes.Unavailable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(sub *testing.T) {
			store := newFakeStore()
			store.err = tc.storeErr
			srv := newTestServer(tc.pricer, store)

			_, err := srv.CreateShipment(sub.Context(), &parcellabv1.CreateShipmentRequest{WeightGrams: 1500})
			if status.Code(err) != tc.wantCode {
				sub.Fatalf("want %v, got %v", tc.wantCode, err)
			}
			if len(store.outbox) != 0 {
				sub.Fatalf("nothing may be stored when the request fails")
			}
		})
	}
}

func TestGetShipment(t *testing.T) {
	dispatchedAt := fixedNow().Add(5 * time.Second)
	store := newFakeStore()
	store.shipments["shp_1"] = domain.ShipmentView{
		Shipment: domain.Shipment{ID: "shp_1", WeightGrams: 1500, Price: usd700(), CreatedAt: fixedNow()},
		Outbox:   domain.OutboxPublished,
		Dispatch: domain.Dispatch{Status: domain.DispatchDispatched, ID: "dsp_1", EventID: "evt_1", DispatchedAt: dispatchedAt},
	}
	srv := newTestServer(&fakePricer{}, store)

	cases := []struct {
		name       string
		shipmentID string
		wantCode   codes.Code
		want       *parcellabv1.Dispatch
	}{
		{
			name:       "dispatched shipment",
			shipmentID: "shp_1",
			wantCode:   codes.OK,
			want: &parcellabv1.Dispatch{
				Status:     parcellabv1.DispatchStatus_DISPATCH_STATUS_DISPATCHED,
				DispatchId: "dsp_1",
				EventId:    "evt_1",
			},
		},
		{name: "missing shipment", shipmentID: "shp_nope", wantCode: codes.NotFound},
		{name: "empty id", shipmentID: "", wantCode: codes.InvalidArgument},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(sub *testing.T) {
			resp, err := srv.GetShipment(sub.Context(), &parcellabv1.GetShipmentRequest{ShipmentId: tc.shipmentID})
			if status.Code(err) != tc.wantCode {
				sub.Fatalf("want %v, got %v", tc.wantCode, err)
			}
			if tc.wantCode != codes.OK {
				return
			}
			s := resp.GetShipment()
			if s.GetOutboxStatus() != parcellabv1.OutboxStatus_OUTBOX_STATUS_PUBLISHED {
				sub.Fatalf("outbox status = %v, want PUBLISHED", s.GetOutboxStatus())
			}
			d := s.GetDispatch()
			if d.GetStatus() != tc.want.GetStatus() || d.GetDispatchId() != tc.want.GetDispatchId() || d.GetEventId() != tc.want.GetEventId() {
				sub.Fatalf("dispatch = %v, want %v", d, tc.want)
			}
			if !d.GetDispatchedAt().AsTime().Equal(dispatchedAt) {
				sub.Fatalf("dispatched_at = %v, want %v", d.GetDispatchedAt().AsTime(), dispatchedAt)
			}
		})
	}
}
