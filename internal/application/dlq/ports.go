package dlq

import (
	"context"
	"errors"
	"time"
)

var (
	ErrInvalidDLQID    = errors.New("invalid dlq id")
	ErrInvalidDLQInput = errors.New("invalid dlq input")
)

// Message is a failed stream message to be dead-lettered.
type Message struct {
	Stream          string
	ConsumerGroup   string
	StreamMessageID string
	EventID         *string
	EventType       string
	AggregateType   string
	AggregateID     string
	CorrelationID   string
	Payload         []byte
	FailureClass    FailureClass
	LastError       string
	Stack           string
	Attempts        int
	ConsumerName    string
	EventCreatedAt  *time.Time
	FirstFailedAt   time.Time
}

// Writer persists dead-lettered messages. Inserting the same
// (stream, consumer group, stream message ID) twice is a no-op.
type Writer interface {
	Insert(ctx context.Context, msg Message) error
}

// Replayer re-publishes a dead-lettered message to its consumer group.
type Replayer interface {
	Replay(ctx context.Context, id string) (outboxEventID string, err error)
}

// ReplaySource is what a replay needs from a dead-lettered message.
type ReplaySource struct {
	ConsumerGroup string
	EventID       string
	EventType     string
	AggregateType string
	AggregateID   string
	Payload       []byte
	// Replayable is false when the envelope was corrupt or a replayed field was altered to fit storage.
	Replayable bool
}

// ReplayEvent is the outbox event that re-publishes a dead-lettered message to one consumer group.
type ReplayEvent struct {
	ID              string
	ReplayOfEventID string
	TargetGroup     string
	EventType       string
	AggregateType   string
	AggregateID     string
	Payload         []byte
}

// Repository reads and transitions dead-lettered messages.
type Repository interface {
	// GetReplaySource returns ErrDLQNotFound when no message has this id and ErrInvalidDLQID when id is malformed.
	GetReplaySource(ctx context.Context, id string) (ReplaySource, error)
	// MarkReplayed moves the message from DEAD to REPLAYED. It reports false when the message was not DEAD.
	MarkReplayed(ctx context.Context, id, replayOutboxID string) (bool, error)
}

// OutboxWriter writes replay events to the transactional outbox.
type OutboxWriter interface {
	CreateReplayEvent(ctx context.Context, e ReplayEvent) error
}

// TxRepos are the repositories bound to one transaction.
type TxRepos struct {
	DLQ    Repository
	Outbox OutboxWriter
}

// TxRunner runs fn in one transaction, committing only if fn returns nil.
type TxRunner interface {
	WithinTx(ctx context.Context, fn func(repos TxRepos) error) error
}
