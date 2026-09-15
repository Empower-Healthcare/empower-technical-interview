// Package app is the shared bootstrap for the Go binaries: signal-aware root
// context, logger, metrics registry, environment reader, database connection
// and management server.
//
// Each binary is a `run(ctx, app) error` function; Main calls it and turns its
// error into the process exit status. This keeps os.Exit out of the binaries
// so their deferred cleanup (pools, clients, servers) always runs.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/empower-healthcare/parcellab/internal/config"
	"github.com/empower-healthcare/parcellab/internal/observability"
	"github.com/empower-healthcare/parcellab/internal/storage"
)

// App holds what every binary needs before it does anything useful.
type App struct {
	Name     string
	Logger   *slog.Logger
	Registry *prometheus.Registry
	Env      *config.Env
}

// Main runs a binary: builds the App and a root context cancelled on
// SIGINT/SIGTERM, calls run, and exits 1 if run returns an error. It never
// returns normally before run has returned, so run's defers always complete.
func Main(name string, run func(ctx context.Context, a App) error) {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	env := config.NewEnv()
	a := App{
		Name:     name,
		Logger:   observability.NewLogger(name, os.Stdout, logLevel(env.String("LOG_LEVEL", "info"))),
		Registry: observability.NewRegistry(),
		Env:      env,
	}
	if err := run(ctx, a); err != nil {
		a.Logger.ErrorContext(ctx, name+" failed", "error", err.Error())
		stop()
		os.Exit(1)
	}
}

func logLevel(name string) slog.Level {
	var level slog.Level
	if err := level.UnmarshalText([]byte(name)); err != nil {
		return slog.LevelInfo
	}
	return level
}

// Retry calls fn until it succeeds or timeout passes; see config.Retry.
func (a App) Retry(ctx context.Context, what string, timeout time.Duration, fn func(ctx context.Context) error) error {
	return config.Retry(ctx, a.Logger, what, timeout, fn)
}

// OpenStore connects to PostgreSQL at dsn, retrying for up to a minute while
// the database boots. The caller owns the returned pool.
func (a App) OpenStore(ctx context.Context, dsn string) (*storage.Store, *pgxpool.Pool, error) {
	var pool *pgxpool.Pool
	err := a.Retry(ctx, "postgres", time.Minute, func(ctx context.Context) error {
		p, err := storage.Open(ctx, dsn)
		if err != nil {
			return err
		}
		pool = p
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return storage.New(pool), pool, nil
}

// Management is the running health/metrics HTTP server.
type Management struct {
	srv    *http.Server
	failed chan error
}

// ListenManagement binds addr now (so a port clash fails startup) and serves
// health and metrics in the background.
func (a App) ListenManagement(ctx context.Context, addr string, ready observability.ReadinessCheck) (*Management, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen management %s: %w", addr, err)
	}
	m := &Management{
		srv:    observability.NewManagementServer(ctx, addr, ready, a.Registry),
		failed: make(chan error, 1),
	}
	go func() {
		if err := m.srv.Serve(lis); !errors.Is(err, http.ErrServerClosed) {
			m.failed <- fmt.Errorf("management server: %w", err)
		}
	}()
	a.Logger.InfoContext(ctx, "management HTTP listening", "addr", lis.Addr().String())
	return m, nil
}

// Failed receives once if the server stops for any reason other than Shutdown.
// Binaries select on it next to their main work so a dead metrics endpoint
// ends the process instead of going unnoticed.
func (m *Management) Failed() <-chan error { return m.failed }

// Shutdown stops accepting connections and waits for in-flight requests until
// ctx expires.
func (m *Management) Shutdown(ctx context.Context) error {
	return m.srv.Shutdown(ctx)
}

// ShutdownContext returns a bounded context for cleanup that keeps the values
// (correlation IDs, etc.) of ctx but ignores its cancellation.
func ShutdownContext(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), d)
}
