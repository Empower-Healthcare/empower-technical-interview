// outbox-relay polls outbox_events and publishes pending rows to Kafka.
//
// Management HTTP (health + metrics) on :8080 by default.
package main

import (
	"context"
	"time"

	"github.com/empower-healthcare/parcellab/internal/app"
	"github.com/empower-healthcare/parcellab/internal/broker"
	"github.com/empower-healthcare/parcellab/internal/relay"
)

const (
	shutdownGrace = 5 * time.Second
	// kafkaWait is how long the relay keeps retrying the broker at startup.
	// Shipment creation does not depend on the relay, so it may wait through a
	// long outage rather than crash-loop.
	kafkaWait = 24 * time.Hour
)

type relayConfig struct {
	MgmtAddr    string
	DatabaseURL string
	Publisher   broker.PublisherConfig
	Relay       relay.Config
}

func (c relayConfig) Validate() error {
	if err := c.Publisher.Validate(); err != nil {
		return err
	}
	return c.Relay.Validate()
}

func main() { app.Main("outbox-relay", run) }

func run(ctx context.Context, a app.App) error {
	cfg := relayConfig{
		MgmtAddr:    a.Env.String("MGMT_ADDR", ":8080"),
		DatabaseURL: a.Env.DatabaseURL(),
		Publisher: broker.PublisherConfig{
			Brokers:         a.Env.Brokers(),
			DeliveryTimeout: a.Env.Duration("RELAY_PUBLISH_TIMEOUT", 10*time.Second),
		},
		Relay: relay.Config{
			PollInterval: a.Env.Duration("RELAY_POLL_INTERVAL", 500*time.Millisecond),
			BatchSize:    a.Env.Int("RELAY_BATCH_SIZE", 100),
		},
	}
	if err := a.Env.Err(); err != nil {
		return err
	}
	if err := cfg.Validate(); err != nil {
		return err
	}

	store, pool, err := a.OpenStore(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	// Metrics are registered and served before Kafka is reachable so the
	// outbox backlog stays visible in Prometheus during a broker outage.
	metrics := relay.NewMetrics(a.Registry)
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

	// Each connect attempt also samples the backlog; attempts are at most 5s
	// apart, so the gauge stays current while we wait for the broker.
	var publisher *broker.Publisher
	err = a.Retry(ctx, "kafka", kafkaWait, func(ctx context.Context) error {
		relay.ObserveBacklog(ctx, store, metrics)
		p, err := broker.NewPublisher(ctx, cfg.Publisher)
		if err != nil {
			return err
		}
		publisher = p
		return nil
	})
	if err != nil {
		return err
	}
	defer publisher.Close()

	a.Logger.InfoContext(ctx, "relay started",
		"brokers", cfg.Publisher.Brokers, "publish_timeout", cfg.Publisher.DeliveryTimeout.String(),
		"poll_interval", cfg.Relay.PollInterval.String(), "batch_size", cfg.Relay.BatchSize)

	runCtx, stopRun := context.WithCancel(ctx)
	defer stopRun()
	runErr := make(chan error, 1)
	go func() {
		runErr <- relay.New(store, publisher, metrics, a.Logger, cfg.Relay).Run(runCtx)
	}()
	select {
	case err := <-runErr:
		return err
	case err := <-mgmt.Failed():
		// Stop the loop before the deferred Close calls take its dependencies away.
		stopRun()
		<-runErr
		return err
	}
}
