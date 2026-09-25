package redis

import (
	"testing"
	"time"

	appOutbox "github.com/ayo6706/cross-border-ecommerce/internal/application/outbox"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecodeMessage_RoundTrip(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 9, 25, 10, 0, 0, 123456000, time.UTC)
	evt := appOutbox.Event{
		ID:            "evt-12345",
		AggregateType: "product",
		AggregateID:   "prod-999",
		EventType:     "product.changed",
		Payload:       []byte(`{"name":"test product","price":"10.00"}`),
		CreatedAt:     now,
	}

	encoded := EncodeEvent(evt)
	assert.Equal(t, "evt-12345", encoded[FieldEventID])
	assert.Equal(t, "product", encoded[FieldAggregateType])
	assert.Equal(t, "prod-999", encoded[FieldAggregateID])
	assert.Equal(t, "product.changed", encoded[FieldEventType])
	assert.Equal(t, evt.Payload, encoded[FieldPayload])
	assert.Equal(t, now.Format(time.RFC3339Nano), encoded[FieldCreatedAt])

	raw := goredis.XMessage{
		ID:     "1695636000000-0",
		Values: encoded,
	}

	msg, err := DecodeMessage("product.changed", raw)
	require.NoError(t, err)
	assert.Equal(t, "1695636000000-0", msg.StreamID)
	assert.Equal(t, "product.changed", msg.Stream)
	assert.Equal(t, "evt-12345", msg.EventID)
	assert.Equal(t, "product", msg.AggregateType)
	assert.Equal(t, "prod-999", msg.AggregateID)
	assert.Equal(t, "product.changed", msg.EventType)
	assert.Equal(t, `{"name":"test product","price":"10.00"}`, string(msg.Payload))
	assert.True(t, now.Equal(msg.CreatedAt))
}

func TestDecodeMessage_ValidationFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		stream      string
		raw         goredis.XMessage
		expectedErr string
	}{
		{
			name: "nil values map",
			raw: goredis.XMessage{
				ID:     "1-0",
				Values: nil,
			},
			expectedErr: "missing message values",
		},
		{
			name: "missing event_id",
			raw: goredis.XMessage{
				ID: "1-0",
				Values: map[string]any{
					FieldCreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
				},
			},
			expectedErr: "missing 'event_id' field",
		},
		{
			name: "empty event_id string",
			raw: goredis.XMessage{
				ID: "1-0",
				Values: map[string]any{
					FieldEventID:   "   ",
					FieldCreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
				},
			},
			expectedErr: "empty or invalid 'event_id' field",
		},
		{
			name: "missing created_at",
			raw: goredis.XMessage{
				ID: "1-0",
				Values: map[string]any{
					FieldEventID: "evt-1",
				},
			},
			expectedErr: "missing 'created_at' field",
		},
		{
			name: "invalid created_at format",
			raw: goredis.XMessage{
				ID: "1-0",
				Values: map[string]any{
					FieldEventID:   "evt-1",
					FieldCreatedAt: "not-a-timestamp",
					FieldEventType: "product.changed",
				},
			},
			expectedErr: "invalid 'created_at' format",
		},
		{
			name: "missing event_type",
			raw: goredis.XMessage{
				ID: "1-0",
				Values: map[string]any{
					FieldEventID:   "evt-1",
					FieldCreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
				},
			},
			expectedErr: "missing 'event_type' field",
		},
		{
			name: "empty event_type",
			raw: goredis.XMessage{
				ID: "1-0",
				Values: map[string]any{
					FieldEventID:   "evt-1",
					FieldCreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
					FieldEventType: "   ",
				},
			},
			expectedErr: "empty or invalid 'event_type' field",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := DecodeMessage("test.stream", tt.raw)
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrCorruptMessage)
			assert.Contains(t, err.Error(), tt.expectedErr)
		})
	}
}
