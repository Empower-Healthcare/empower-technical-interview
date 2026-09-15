// replay republishes one existing outbox event to Kafka with its original
// event ID, key and payload. It exists to demonstrate duplicate delivery: the
// consumer must recognise the event ID and must not create a second dispatch.
//
//	replay <event_id>
//
// The outbox row is not modified; this is a deliberate duplicate, not a retry.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/empower-healthcare/parcellab/internal/app"
	"github.com/empower-healthcare/parcellab/internal/broker"
	"github.com/empower-healthcare/parcellab/internal/domain"
	"github.com/empower-healthcare/parcellab/internal/relay"
)

func main() {
	if len(os.Args) != 2 || os.Args[1] == "" {
		fmt.Fprintln(os.Stderr, "usage: replay <event_id>")
		os.Exit(2)
	}
	eventID := domain.EventID(os.Args[1])
	app.Main("replay", func(ctx context.Context, a app.App) error { return run(ctx, a, eventID) })
}

func run(ctx context.Context, a app.App, eventID domain.EventID) error {
	dsn := a.Env.DatabaseURL()
	pubCfg := broker.PublisherConfig{
		Brokers:         a.Env.Brokers(),
		DeliveryTimeout: a.Env.Duration("RELAY_PUBLISH_TIMEOUT", 10*time.Second),
	}
	if err := errors.Join(a.Env.Err(), pubCfg.Validate()); err != nil {
		return err
	}

	store, pool, err := a.OpenStore(ctx, dsn)
	if err != nil {
		return err
	}
	defer pool.Close()

	evt, err := store.GetOutboxEvent(ctx, eventID)
	if err != nil {
		return fmt.Errorf("load event: %w", err)
	}
	msg, err := relay.MessageFor(evt)
	if err != nil {
		return fmt.Errorf("event is not publishable: %w", err)
	}

	publisher, err := broker.NewPublisher(ctx, pubCfg)
	if err != nil {
		return err
	}
	defer publisher.Close()

	start := time.Now()
	if err := publisher.Publish(ctx, msg); err != nil {
		return err
	}
	a.Logger.InfoContext(ctx, "event replayed",
		"event_id", evt.EventID, "shipment_id", evt.ShipmentID, "topic", msg.Topic, "ack_ms", time.Since(start).Milliseconds())
	fmt.Printf("replayed %s (shipment %s) to %s; the consumer should log a duplicate and create no new dispatch\n",
		evt.EventID, evt.ShipmentID, msg.Topic)
	return nil
}
