// Package correlation propagates a per-request correlation ID across gRPC
// metadata, HTTP headers, the outbox payload and consumer logs.
//
// The ID travels in the context. The structured logger (observability.NewLogger)
// reads it from there, so code never has to add it to log lines by hand.
//
// A correlation ID is a log-joining aid, not distributed tracing: there are no
// spans, timings or parent/child relationships.
package correlation

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/empower-healthcare/parcellab/internal/domain"
)

const (
	// MetadataKey is the incoming/outgoing gRPC metadata key.
	MetadataKey = "x-correlation-id"
	// HTTPHeader is used on the HTTP/JSON call to the pricing service.
	HTTPHeader = "X-Correlation-ID"
	// LogKey is the attribute name on structured log lines.
	LogKey = "correlation_id"
)

type ctxKey struct{}

// WithID stores id in the context.
func WithID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

// FromContext returns the correlation ID, or "" when none was set.
func FromContext(ctx context.Context) string {
	id, _ := ctx.Value(ctxKey{}).(string)
	return id
}

// UnaryServerInterceptor reads x-correlation-id from incoming metadata (or
// generates a req_... ID), stores it in the context and echoes it back to the
// caller as a response header.
func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		id := fromIncomingMetadata(ctx)
		if id == "" {
			id = domain.NewCorrelationID()
		}
		_ = grpc.SetHeader(ctx, metadata.Pairs(MetadataKey, id))
		return handler(WithID(ctx, id), req)
	}
}

func fromIncomingMetadata(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	if vals := md.Get(MetadataKey); len(vals) > 0 {
		return vals[0]
	}
	return ""
}
