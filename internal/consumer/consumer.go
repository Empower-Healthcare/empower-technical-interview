package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/empower-healthcare/parcellab/internal/events"
)

// GroupID is the consumer group. Kafka UI shows its offsets and lag.
const GroupID = "dispatch-consumer"

// ErrStopped is returned when a record could not be handled and the consumer
// refuses to move past it.
var ErrStopped = errors.New("consumer stopped on unprocessable record")

// Timing constants shared between the client options and Config.Validate.
const (
	// rebalanceTimeout is how long the group coordinator waits for this member
	// to rejoin after a rebalance starts. Because rebalances are blocked while
	// a record is being handled, one record's worst case (handle + commit) must
	// finish well inside it, or the member is kicked and its partitions are
	// reassigned while it is still writing.
	rebalanceTimeout = 60 * time.Second
	// commitTimeout bounds the offset commit that follows a successful handle.
	commitTimeout = 10 * time.Second
	// maxRecordTimeout keeps handle + commit under half the rebalance timeout.
	maxRecordTimeout = rebalanceTimeout/2 - commitTimeout
)

// Config configures Run.
type Config struct {
	Brokers []string
	Retry   RetryPolicy
	// RecordTimeout bounds the handling of one record, including storage
	// retries. It is derived from the process context, so shutdown still
	// interrupts a record early.
	RecordTimeout time.Duration
}

// Validate checks the values once at startup so the loop needs no guards.
func (c Config) Validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("brokers: at least one required")
	}
	if err := c.Retry.Validate(); err != nil {
		return fmt.Errorf("retry: %w", err)
	}
	switch {
	case c.RecordTimeout <= 0:
		return fmt.Errorf("record timeout %s: must be positive", c.RecordTimeout)
	case c.RecordTimeout > maxRecordTimeout:
		return fmt.Errorf("record timeout %s: must not exceed %s so a stuck record cannot outlive the %s rebalance timeout",
			c.RecordTimeout, maxRecordTimeout, rebalanceTimeout)
	case c.Retry.MaxWait() >= c.RecordTimeout:
		return fmt.Errorf("retry policy can wait %s, which is not inside the record timeout %s", c.Retry.MaxWait(), c.RecordTimeout)
	}
	return nil
}

// Run consumes shipment.created.v1 until ctx is cancelled or a record cannot
// be handled. Records are processed one at a time in partition order, and the
// offset for each record is committed only after Handler.Handle returns nil.
//
// Auto-commit is disabled on purpose: with it enabled the client could commit
// an offset past a record whose database transaction has not finished.
// Rebalances are blocked between poll and commit for the same reason.
func Run(ctx context.Context, cfg Config, handler *Handler, logger *slog.Logger) error {
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("consumer config: %w", err)
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(GroupID),
		kgo.ConsumeTopics(events.Topic),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.RebalanceTimeout(rebalanceTimeout),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return fmt.Errorf("create kafka client: %w", err)
	}
	defer client.Close()

	if err := client.Ping(ctx); err != nil {
		return fmt.Errorf("ping kafka: %w", err)
	}
	logger.InfoContext(ctx, "consuming", "topic", events.Topic, "group", GroupID)

	for ctx.Err() == nil {
		// One record per poll: the poll-to-commit window is exactly one
		// record's work, which is what Config.Validate bounded.
		fetches := client.PollRecords(ctx, 1)
		if fetches.IsClientClosed() {
			return nil
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			logger.ErrorContext(ctx, "fetch error", "topic", topic, "partition", partition, "error", err.Error())
		})

		err := processFetches(ctx, cfg.RecordTimeout, client, fetches, handler)
		client.AllowRebalance()
		if err != nil && ctx.Err() == nil {
			return err
		}
	}
	return nil
}

// processFetches handles each record and commits its offset before moving to
// the next. It returns on the first record that cannot be handled, leaving
// that record's offset uncommitted.
func processFetches(ctx context.Context, recordTimeout time.Duration, client *kgo.Client, fetches kgo.Fetches, handler *Handler) error {
	for iter := fetches.RecordIter(); !iter.Done(); {
		rec := iter.Next()
		if err := processRecord(ctx, recordTimeout, client, rec, handler); err != nil {
			return err
		}
	}
	return nil
}

func processRecord(ctx context.Context, recordTimeout time.Duration, client *kgo.Client, rec *kgo.Record, handler *Handler) error {
	handleCtx, cancelHandle := context.WithTimeout(ctx, recordTimeout)
	defer cancelHandle()
	outcome, err := handler.Handle(handleCtx, Record{Partition: rec.Partition, Offset: rec.Offset, Value: rec.Value})
	if err != nil {
		return fmt.Errorf("%w: partition %d offset %d outcome %s: %w", ErrStopped, rec.Partition, rec.Offset, outcome, err)
	}

	// The database transaction is committed; now it is safe to move the offset.
	// If this commit fails the dispatch exists but the offset does not, and the
	// redelivery after a restart is handled as a duplicate.
	commitCtx, cancelCommit := context.WithTimeout(ctx, commitTimeout)
	defer cancelCommit()
	if err := client.CommitRecords(commitCtx, rec); err != nil {
		return fmt.Errorf("commit offset partition %d offset %d: %w", rec.Partition, rec.Offset, err)
	}
	return nil
}
