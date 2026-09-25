package messaging

import (
	"context"
	"errors"
	"time"
)

// ErrBrokerUnavailable signals a transient infrastructure/network failure or broker capacity exhaustion (e.g. OOM/loading).
var ErrBrokerUnavailable = errors.New("broker unavailable")

// Message represents an event envelope received from an external stream broker.
type Message struct {
	StreamID      string
	Stream        string
	EventID       string
	AggregateType string
	AggregateID   string
	EventType     string
	Payload       []byte
	CreatedAt     time.Time
}

// Handler defines the function signature for consuming stream messages.
// A nil error return acknowledges the message (XACK); a non-nil error leaves the message unacknowledged for redelivery.
type Handler func(ctx context.Context, msg Message) error
