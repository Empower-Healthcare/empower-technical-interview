// Package config reads environment variables into per-binary config structs.
//
// Usage:
//
//	env := config.NewEnv()
//	cfg := serviceConfig{
//		Addr:    env.String("GRPC_ADDR", ":50051"),
//		Timeout: env.Duration("PRICING_TIMEOUT", 2*time.Second),
//	}
//	if err := env.Err(); err != nil { ... }
//
// Every reader records parse failures instead of failing fast, so one error
// message lists everything that is wrong with the environment.
package config

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// DefaultDatabaseURL is the Compose development credential. It is not a
// production secret; every value in it is also visible in docker-compose.yml.
const DefaultDatabaseURL = "postgres://parcellab:parcellab_dev_password@postgres:5432/parcellab?sslmode=disable"

// Env reads variables and accumulates parse errors.
type Env struct {
	lookup func(string) (string, bool)
	errs   []error
}

// NewEnv reads from the process environment.
func NewEnv() *Env { return &Env{lookup: os.LookupEnv} }

// NewEnvFrom reads from a map; used in tests.
func NewEnvFrom(values map[string]string) *Env {
	return &Env{lookup: func(k string) (string, bool) { v, ok := values[k]; return v, ok }}
}

// String returns the variable or def when unset or empty.
func (e *Env) String(key, def string) string {
	if v, ok := e.lookup(key); ok && v != "" {
		return v
	}
	return def
}

// Int returns the variable parsed as an int, or def.
func (e *Env) Int(key string, def int) int {
	v, ok := e.lookup(key)
	if !ok || v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s=%q: %w", key, v, err))
		return def
	}
	return n
}

// Duration returns the variable parsed with time.ParseDuration, or def.
func (e *Env) Duration(key string, def time.Duration) time.Duration {
	v, ok := e.lookup(key)
	if !ok || v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s=%q: %w", key, v, err))
		return def
	}
	return d
}

// Brokers returns KAFKA_BROKERS split on commas.
func (e *Env) Brokers() []string {
	return strings.Split(e.String("KAFKA_BROKERS", "kafka:9092"), ",")
}

// DatabaseURL returns DATABASE_URL or the Compose default.
func (e *Env) DatabaseURL() string {
	return e.String("DATABASE_URL", DefaultDatabaseURL)
}

// Err returns every parse failure seen so far, joined.
func (e *Env) Err() error { return errors.Join(e.errs...) }

// Retry calls fn until it succeeds, ctx is cancelled, or timeout passes,
// logging each failure. Binaries use it at startup so they can outlive a
// dependency that is still booting or temporarily down.
func Retry(ctx context.Context, logger *slog.Logger, what string, timeout time.Duration, fn func(ctx context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	wait := 500 * time.Millisecond
	for {
		attemptCtx, attemptCancel := context.WithTimeout(ctx, 5*time.Second)
		err := fn(attemptCtx)
		attemptCancel()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return gaveUp(what, ctx.Err(), err)
		}
		logger.WarnContext(ctx, "dependency not ready; retrying", "dependency", what, "error", err.Error(), "retry_in", wait.String())

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return gaveUp(what, ctx.Err(), err)
		case <-timer.C:
		}
		wait = min(wait*2, 5*time.Second)
	}
}

// gaveUp reports both why Retry stopped (cancelled or timed out) and the last
// dependency failure, so errors.Is works for either.
func gaveUp(what string, stop, last error) error {
	return fmt.Errorf("%s: gave up (%w): last error: %w", what, stop, last)
}
