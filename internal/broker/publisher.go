// Package broker wraps the Kafka client used by the outbox relay, the replay
// helper and the dispatch consumer.
//
// Local simplification: connections are PLAINTEXT to a single broker. A real
// deployment would use TLS/SASL and replication factor >= 3.
package broker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Message is one record to publish: plain data, independent of the client library.
type Message struct {
	Topic   string
	Key     []byte
	Value   []byte
	Headers map[string]string
}

// Ping verifies that at least one broker answers. Binaries call it (with
// retries) before starting long-running work so a slow broker start does not
// turn into a crash loop.
func Ping(ctx context.Context, brokers []string) error {
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return fmt.Errorf("create kafka client: %w", err)
	}
	defer client.Close()
	if err := client.Ping(ctx); err != nil {
		return fmt.Errorf("ping kafka: %w", err)
	}
	return nil
}

// PublisherConfig configures a Publisher.
type PublisherConfig struct {
	Brokers []string
	// DeliveryTimeout bounds every Publish call, including the client's
	// internal retries. It must be positive: an unbounded produce would hold
	// the relay's outbox transaction (and its row locks) open for as long as
	// the broker is unreachable.
	DeliveryTimeout time.Duration
}

// Validate reports configuration that would make the Publisher misbehave.
func (c PublisherConfig) Validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("brokers: at least one required")
	}
	if c.DeliveryTimeout <= 0 {
		return fmt.Errorf("delivery timeout %s: must be positive", c.DeliveryTimeout)
	}
	return nil
}

// Publisher produces messages to Kafka and waits for acknowledgment.
type Publisher struct {
	client  *kgo.Client
	timeout time.Duration
}

// NewPublisher connects to the brokers and verifies they answer.
//
// Writes wait for all in-sync replicas and the producer is idempotent, so the
// client's own retries never duplicate a record. Delivery is time-bounded:
// when the timeout passes or ctx is cancelled while a produce is in flight the
// call fails even though the broker may have written the record. The relay
// then leaves the outbox row pending and publishes it again, so the same
// event_id can appear twice on the topic. That is the at-least-once boundary
// the consumer's deduplication exists for.
func NewPublisher(ctx context.Context, cfg PublisherConfig) (*Publisher, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("publisher config: %w", err)
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.NoCompression()),
		kgo.RecordDeliveryTimeout(cfg.DeliveryTimeout),
		kgo.AllowIdempotentProduceCancellation(),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}
	if err := client.Ping(ctx); err != nil {
		client.Close()
		return nil, fmt.Errorf("ping kafka: %w", err)
	}
	return &Publisher{client: client, timeout: cfg.DeliveryTimeout}, nil
}

// Publish sends one message and returns once the broker has acknowledged it,
// or with an error when it has not within the delivery timeout (or ctx ends).
func (p *Publisher) Publish(ctx context.Context, msg Message) error {
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	record := &kgo.Record{Topic: msg.Topic, Key: msg.Key, Value: msg.Value}
	for k, v := range msg.Headers {
		record.Headers = append(record.Headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
	}
	if err := p.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("produce to %s: %w", msg.Topic, err)
	}
	return nil
}

// Close closes the client. Any produce still in flight fails with an error;
// callers publish only from Publish, which has already returned by then.
func (p *Publisher) Close() { p.client.Close() }
