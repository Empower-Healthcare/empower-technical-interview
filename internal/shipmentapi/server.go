// Package shipmentapi implements the parcellab.v1.ShipmentService gRPC API.
package shipmentapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	parcellabv1 "github.com/empower-healthcare/parcellab/gen/parcellab/v1"
	"github.com/empower-healthcare/parcellab/internal/correlation"
	"github.com/empower-healthcare/parcellab/internal/domain"
	"github.com/empower-healthcare/parcellab/internal/events"
	"github.com/empower-healthcare/parcellab/internal/pricing"
	"github.com/empower-healthcare/parcellab/internal/storage"
)

// Pricer is the downstream pricing dependency (the Python service in the
// running stack, a fake in tests).
type Pricer interface {
	Quote(ctx context.Context, weightGrams int32) (domain.Money, error)
}

// Store is the persistence dependency.
type Store interface {
	CreateShipment(ctx context.Context, shipment domain.Shipment, evt storage.OutboxEvent) error
	GetShipment(ctx context.Context, id domain.ShipmentID) (domain.ShipmentView, error)
}

// Server implements parcellabv1.ShipmentServiceServer.
type Server struct {
	parcellabv1.UnimplementedShipmentServiceServer

	pricer Pricer
	store  Store
	logger *slog.Logger
	now    func() time.Time
}

// New wires the service. now supplies shipment creation timestamps; pass
// time.Now outside tests.
func New(pricer Pricer, store Store, logger *slog.Logger, now func() time.Time) *Server {
	return &Server{pricer: pricer, store: store, logger: logger, now: now}
}

// GetQuote validates the parcel and prices it via the pricing service.
func (s *Server) GetQuote(ctx context.Context, req *parcellabv1.GetQuoteRequest) (*parcellabv1.GetQuoteResponse, error) {
	price, err := s.quote(ctx, req.GetWeightGrams())
	if err != nil {
		return nil, s.fail(ctx, err)
	}
	return &parcellabv1.GetQuoteResponse{Price: toProtoMoney(price)}, nil
}

// CreateShipment prices the parcel, then stores the shipment and its outbox
// event in one transaction. Returning does not mean the event has reached
// Kafka; that is the relay's job.
func (s *Server) CreateShipment(ctx context.Context, req *parcellabv1.CreateShipmentRequest) (*parcellabv1.CreateShipmentResponse, error) {
	price, err := s.quote(ctx, req.GetWeightGrams())
	if err != nil {
		return nil, s.fail(ctx, err)
	}

	shipment := domain.Shipment{
		ID:          domain.NewShipmentID(),
		WeightGrams: req.GetWeightGrams(),
		Price:       price,
		CreatedAt:   s.now().UTC(),
	}
	outbox, err := newOutboxEvent(shipment, correlation.FromContext(ctx))
	if err != nil {
		return nil, s.fail(ctx, err)
	}
	if err := s.store.CreateShipment(ctx, shipment, outbox); err != nil {
		return nil, s.fail(ctx, err)
	}

	s.logger.InfoContext(ctx, "shipment created",
		"shipment_id", shipment.ID,
		"event_id", outbox.EventID,
		"amount_cents", price.AmountCents,
		"currency", price.Currency,
	)
	view := domain.ShipmentView{
		Shipment: shipment,
		Outbox:   domain.OutboxPending,
		Dispatch: domain.Dispatch{Status: domain.DispatchPending},
	}
	return &parcellabv1.CreateShipmentResponse{Shipment: toProtoShipment(view)}, nil
}

// GetShipment returns the shipment with its outbox and dispatch state.
func (s *Server) GetShipment(ctx context.Context, req *parcellabv1.GetShipmentRequest) (*parcellabv1.GetShipmentResponse, error) {
	if req.GetShipmentId() == "" {
		return nil, status.Error(codes.InvalidArgument, "shipment_id is required")
	}
	view, err := s.store.GetShipment(ctx, domain.ShipmentID(req.GetShipmentId()))
	if err != nil {
		return nil, s.fail(ctx, err)
	}
	return &parcellabv1.GetShipmentResponse{Shipment: toProtoShipment(view)}, nil
}

// quote validates locally before spending a downstream call.
func (s *Server) quote(ctx context.Context, weightGrams int32) (domain.Money, error) {
	if err := domain.ValidateWeight(weightGrams); err != nil {
		return domain.Money{}, err
	}
	return s.pricer.Quote(ctx, weightGrams)
}

// newOutboxEvent builds the shipment.created event and its outbox row.
func newOutboxEvent(shipment domain.Shipment, correlationID string) (storage.OutboxEvent, error) {
	evt := events.NewShipmentCreated(shipment, correlationID)
	payload, err := events.Encode(evt)
	if err != nil {
		return storage.OutboxEvent{}, fmt.Errorf("encode event: %w", err)
	}
	return storage.OutboxEvent{
		EventID:    evt.EventID,
		ShipmentID: shipment.ID,
		EventType:  evt.EventType,
		Payload:    payload,
	}, nil
}

// fail converts an application error into the gRPC status returned to the
// caller. Errors that map to Internal are logged here with their cause, because
// the caller only sees "internal error" and the interceptor only sees the status.
func (s *Server) fail(ctx context.Context, err error) error {
	st := toStatus(err)
	if status.Code(st) == codes.Internal {
		s.logger.ErrorContext(ctx, "internal error", "error", err.Error())
	}
	return st
}

// toStatus maps application errors to gRPC statuses.
//
// Context errors are checked before dependency errors so a call the caller
// abandoned reports Canceled/DeadlineExceeded rather than a dependency failure.
// Anything unrecognised (constraint violations, bugs) becomes Internal without
// leaking detail to callers.
func toStatus(err error) error {
	if _, ok := status.FromError(err); ok {
		return err
	}
	switch {
	case errors.Is(err, domain.ErrInvalidWeight), errors.Is(err, pricing.ErrRejected):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, domain.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, pricing.ErrTimeout), errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, pricing.ErrUnavailable), errors.Is(err, storage.ErrUnavailable):
		return status.Error(codes.Unavailable, err.Error())
	default:
		return status.Error(codes.Internal, "internal error")
	}
}

func toProtoMoney(m domain.Money) *parcellabv1.Money {
	return &parcellabv1.Money{AmountCents: m.AmountCents, Currency: m.Currency}
}

func toProtoShipment(v domain.ShipmentView) *parcellabv1.Shipment {
	return &parcellabv1.Shipment{
		ShipmentId:   string(v.Shipment.ID),
		WeightGrams:  v.Shipment.WeightGrams,
		Price:        toProtoMoney(v.Shipment.Price),
		CreatedAt:    timestamppb.New(v.Shipment.CreatedAt),
		OutboxStatus: toProtoOutboxStatus(v.Outbox),
		Dispatch:     toProtoDispatch(v.Dispatch),
	}
}

func toProtoOutboxStatus(s domain.OutboxStatus) parcellabv1.OutboxStatus {
	switch s {
	case domain.OutboxPending:
		return parcellabv1.OutboxStatus_OUTBOX_STATUS_PENDING
	case domain.OutboxPublished:
		return parcellabv1.OutboxStatus_OUTBOX_STATUS_PUBLISHED
	default:
		return parcellabv1.OutboxStatus_OUTBOX_STATUS_UNSPECIFIED
	}
}

func toProtoDispatch(d domain.Dispatch) *parcellabv1.Dispatch {
	switch d.Status {
	case domain.DispatchDispatched:
		return &parcellabv1.Dispatch{
			Status:       parcellabv1.DispatchStatus_DISPATCH_STATUS_DISPATCHED,
			DispatchId:   string(d.ID),
			EventId:      string(d.EventID),
			DispatchedAt: timestamppb.New(d.DispatchedAt),
		}
	case domain.DispatchPending:
		return &parcellabv1.Dispatch{Status: parcellabv1.DispatchStatus_DISPATCH_STATUS_PENDING}
	default:
		return &parcellabv1.Dispatch{Status: parcellabv1.DispatchStatus_DISPATCH_STATUS_UNSPECIFIED}
	}
}
