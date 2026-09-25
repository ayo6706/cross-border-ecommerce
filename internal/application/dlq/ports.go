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

type Writer interface {
	Insert(ctx context.Context, msg Message) error
}

type Replayer interface {
	Replay(ctx context.Context, id string) (outboxEventID string, err error)
}
