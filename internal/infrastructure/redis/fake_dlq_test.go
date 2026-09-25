package redis

import (
	"context"
	"sync"

	"github.com/ayo6706/cross-border-ecommerce/internal/application/dlq"
)

// memoryDLQStore is the in-memory dlq.Writer for consumer tests. Like dlq_messages it ignores a
// second insert of the same (stream, consumer group, stream message ID).
type memoryDLQStore struct {
	mu       sync.Mutex
	messages []dlq.Message
}

func (s *memoryDLQStore) Insert(_ context.Context, msg dlq.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range s.messages {
		if m.Stream == msg.Stream && m.ConsumerGroup == msg.ConsumerGroup && m.StreamMessageID == msg.StreamMessageID {
			return nil
		}
	}
	s.messages = append(s.messages, msg)
	return nil
}

// Messages returns a copy of the dead-lettered messages.
func (s *memoryDLQStore) Messages() []dlq.Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]dlq.Message(nil), s.messages...)
}
