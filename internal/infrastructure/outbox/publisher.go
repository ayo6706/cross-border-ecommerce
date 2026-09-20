package outbox

import (
	"context"
	"time"
)

type Event struct {
	ID            string
	AggregateType string
	AggregateID   string
	EventType     string
	Payload       []byte
	Status        string
	RetryCount    int
	CreatedAt     time.Time
	ProcessedAt   *time.Time
}

type Publisher interface {
	Publish(ctx context.Context, event Event) error
}
