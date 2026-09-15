// Package observability provides the structured, context-aware logger and the
// management HTTP server (health + Prometheus metrics) shared by every Go binary.
//
// The management endpoint is operational HTTP only; the business API is gRPC.
package observability

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/empower-healthcare/parcellab/internal/correlation"
)

// NewLogger returns a JSON logger that tags every line with the service name
// and, when the *Context logging methods are used, the correlation ID carried
// by the context.
func NewLogger(service string, w io.Writer, level slog.Level) *slog.Logger {
	var handler slog.Handler = slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level})
	handler = correlationHandler{Handler: handler}
	return slog.New(handler).With("service", service)
}

// correlationHandler adds correlation_id from the context to every record.
type correlationHandler struct{ slog.Handler }

func (h correlationHandler) Handle(ctx context.Context, rec slog.Record) error {
	if id := correlation.FromContext(ctx); id != "" {
		rec.AddAttrs(slog.String(correlation.LogKey, id))
	}
	return h.Handler.Handle(ctx, rec)
}

func (h correlationHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return correlationHandler{Handler: h.Handler.WithAttrs(attrs)}
}

func (h correlationHandler) WithGroup(name string) slog.Handler {
	return correlationHandler{Handler: h.Handler.WithGroup(name)}
}

// NewRegistry returns a Prometheus registry pre-loaded with the standard Go
// runtime and process collectors. Components register their own metrics on it.
func NewRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return reg
}

// ReadinessCheck reports whether the process can serve traffic (for example,
// whether PostgreSQL is reachable).
type ReadinessCheck func(ctx context.Context) error

// NewManagementServer serves:
//
//	GET /healthz  liveness: the process is up
//	GET /readyz   readiness: runs the supplied check (503 on failure)
//	GET /metrics  Prometheus exposition of reg
//
// Request contexts derive from base, so handlers stop when the process does.
func NewManagementServer(base context.Context, addr string, ready ReadinessCheck, reg *prometheus.Registry) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		w.Header().Set("Content-Type", "text/plain")
		if err := ready(ctx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, "not ready: "+err.Error()+"\n")
			return
		}
		_, _ = io.WriteString(w, "ready\n")
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}))

	return &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return base },
	}
}
