package shipmentapi

import (
	"context"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Metrics are the gRPC server's Prometheus series. Labels are bounded (method
// name, status code); shipment and correlation IDs stay in logs.
type Metrics struct {
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

// NewMetrics creates and registers the server metrics.
func NewMetrics(reg prometheus.Registerer) Metrics {
	m := Metrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "parcellab_grpc_requests_total",
			Help: "gRPC requests handled by the shipment service, by method and status code.",
		}, []string{"method", "code"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "parcellab_grpc_request_duration_seconds",
			Help:    "gRPC request latency by method.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method"}),
	}
	reg.MustRegister(m.requests, m.duration)
	return m
}

// Interceptor records metrics and writes one structured log line per request.
// The correlation ID is added to the line by the logger from ctx.
func Interceptor(metrics Metrics, logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		elapsed := time.Since(start)
		code := status.Code(err)

		metrics.requests.WithLabelValues(info.FullMethod, code.String()).Inc()
		metrics.duration.WithLabelValues(info.FullMethod).Observe(elapsed.Seconds())

		attrs := []any{"method", info.FullMethod, "code", code.String(), "duration_ms", elapsed.Milliseconds()}
		if err != nil {
			attrs = append(attrs, "error", err.Error())
		}
		logger.Log(ctx, logLevel(code), "rpc", attrs...)
		return resp, err
	}
}

// logLevel: caller mistakes are warnings, dependency and server failures are errors.
func logLevel(code codes.Code) slog.Level {
	switch code {
	case codes.OK:
		return slog.LevelInfo
	case codes.InvalidArgument, codes.NotFound, codes.Canceled:
		return slog.LevelWarn
	default:
		return slog.LevelError
	}
}
