// dispatch-consumer reads shipment.created.v1 from Kafka and records one
// dispatch per shipment, deduplicating on event ID.
//
// Management HTTP (health + metrics) on :8080 by default. The process exits
// non-zero when it meets a record it cannot process (malformed, unsupported,
// conflicting, or storage still failing after retries). It does not skip such
// records; Compose restarts it and the lag stays visible in Kafka UI.
package main

import (
	"context"
	"time"

	"github.com/empower-healthcare/parcellab/internal/app"
	"github.com/empower-healthcare/parcellab/internal/broker"
	"github.com/empower-healthcare/parcellab/internal/consumer"
)

const (
	shutdownGrace = 5 * time.Second
	// kafkaWait is how long the consumer keeps retrying the broker at startup.
	kafkaWait = 24 * time.Hour
)

type consumerConfig struct {
	MgmtAddr    string
	DatabaseURL string
	Consumer    consumer.Config
}

func main() { app.Main("dispatch-consumer", run) }

func run(ctx context.Context, a app.App) error {
	cfg := consumerConfig{
		MgmtAddr:    a.Env.String("MGMT_ADDR", ":8080"),
		DatabaseURL: a.Env.DatabaseURL(),
		Consumer: consumer.Config{
			Brokers:       a.Env.Brokers(),
			RecordTimeout: a.Env.Duration("CONSUMER_RECORD_TIMEOUT", 15*time.Second),
			Retry: consumer.RetryPolicy{
				MaxAttempts: a.Env.Int("CONSUMER_MAX_ATTEMPTS", 5),
				Backoff:     a.Env.Duration("CONSUMER_BACKOFF", 500*time.Millisecond),
			},
		},
	}
	if err := a.Env.Err(); err != nil {
		return err
	}
	if err := cfg.Consumer.Validate(); err != nil {
		return err
	}

	store, pool, err := a.OpenStore(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	metrics := consumer.NewMetrics(a.Registry)
	mgmt, err := a.ListenManagement(ctx, cfg.MgmtAddr, store.Ping)
	if err != nil {
		return err
	}
	defer func() {
		stopCtx, cancel := app.ShutdownContext(ctx, shutdownGrace)
		defer cancel()
		if err := mgmt.Shutdown(stopCtx); err != nil {
			a.Logger.WarnContext(ctx, "management shutdown", "error", err.Error())
		}
		a.Logger.InfoContext(ctx, "stopped")
	}()

	err = a.Retry(ctx, "kafka", kafkaWait, func(ctx context.Context) error {
		return broker.Ping(ctx, cfg.Consumer.Brokers)
	})
	if err != nil {
		return err
	}

	handler := consumer.NewHandler(store, metrics, a.Logger, cfg.Consumer.Retry)
	a.Logger.InfoContext(ctx, "consumer started",
		"brokers", cfg.Consumer.Brokers, "record_timeout", cfg.Consumer.RecordTimeout.String(),
		"max_attempts", cfg.Consumer.Retry.MaxAttempts, "backoff", cfg.Consumer.Retry.Backoff.String())

	runCtx, stopRun := context.WithCancel(ctx)
	defer stopRun()
	runErr := make(chan error, 1)
	go func() { runErr <- consumer.Run(runCtx, cfg.Consumer, handler, a.Logger) }()
	select {
	case err := <-runErr:
		return err
	case err := <-mgmt.Failed():
		// Stop the loop before the deferred pool close takes the store away.
		stopRun()
		<-runErr
		return err
	}
}
