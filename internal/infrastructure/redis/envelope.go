package redis

import (
	"errors"
	"fmt"
	"strings"
	"time"

	appMessaging "github.com/ayo6706/cross-border-ecommerce/internal/application/messaging"
	appOutbox "github.com/ayo6706/cross-border-ecommerce/internal/application/outbox"
	goredis "github.com/redis/go-redis/v9"
)

const (
	FieldEventID       = "event_id"
	FieldAggregateType = "aggregate_type"
	FieldAggregateID   = "aggregate_id"
	FieldEventType     = "event_type"
	FieldPayload       = "payload"
	FieldCreatedAt     = "created_at"
)

var (
	ErrCorruptMessage = errors.New("corrupt stream message")
)

// EncodeEvent converts an Outbox event into a Redis Streams field-value map.
func EncodeEvent(e appOutbox.Event) map[string]any {
	return map[string]any{
		FieldEventID:       e.ID,
		FieldAggregateType: e.AggregateType,
		FieldAggregateID:   e.AggregateID,
		FieldEventType:     e.EventType,
		FieldPayload:       e.Payload,
		FieldCreatedAt:     e.CreatedAt.UTC().Format(time.RFC3339Nano),
	}
}

// DecodeMessage parses a raw Redis Streams XMessage into an application Message.
// It strictly validates mandatory fields (event_id, valid RFC3339 timestamp).
func DecodeMessage(stream string, raw goredis.XMessage) (appMessaging.Message, error) {
	if raw.Values == nil {
		return appMessaging.Message{}, fmt.Errorf("%w: missing message values for stream ID '%s'", ErrCorruptMessage, raw.ID)
	}

	eventIDVal, ok := raw.Values[FieldEventID]
	if !ok {
		return appMessaging.Message{}, fmt.Errorf("%w: missing '%s' field for stream ID '%s'", ErrCorruptMessage, FieldEventID, raw.ID)
	}
	eventID, ok := getString(eventIDVal)
	if !ok || strings.TrimSpace(eventID) == "" {
		return appMessaging.Message{}, fmt.Errorf("%w: empty or invalid '%s' field for stream ID '%s'", ErrCorruptMessage, FieldEventID, raw.ID)
	}

	createdAtVal, ok := raw.Values[FieldCreatedAt]
	if !ok {
		return appMessaging.Message{}, fmt.Errorf("%w: missing '%s' field for stream ID '%s'", ErrCorruptMessage, FieldCreatedAt, raw.ID)
	}
	createdAtStr, ok := getString(createdAtVal)
	if !ok || strings.TrimSpace(createdAtStr) == "" {
		return appMessaging.Message{}, fmt.Errorf("%w: empty '%s' field for stream ID '%s'", ErrCorruptMessage, FieldCreatedAt, raw.ID)
	}

	createdAt, err := time.Parse(time.RFC3339Nano, createdAtStr)
	if err != nil {
		return appMessaging.Message{}, fmt.Errorf("%w: invalid '%s' format '%s' for stream ID '%s': %w", ErrCorruptMessage, FieldCreatedAt, createdAtStr, raw.ID, err)
	}

	eventTypeVal, ok := raw.Values[FieldEventType]
	if !ok {
		return appMessaging.Message{}, fmt.Errorf("%w: missing '%s' field for stream ID '%s'", ErrCorruptMessage, FieldEventType, raw.ID)
	}
	eventType, ok := getString(eventTypeVal)
	if !ok || strings.TrimSpace(eventType) == "" {
		return appMessaging.Message{}, fmt.Errorf("%w: empty or invalid '%s' field for stream ID '%s'", ErrCorruptMessage, FieldEventType, raw.ID)
	}

	aggType, _ := getString(raw.Values[FieldAggregateType])
	aggID, _ := getString(raw.Values[FieldAggregateID])

	var payload []byte
	if rawPayload, ok := raw.Values[FieldPayload]; ok && rawPayload != nil {
		if b, ok := getBytes(rawPayload); ok {
			payload = b
		}
	}

	return appMessaging.Message{
		StreamID:      raw.ID,
		Stream:        stream,
		EventID:       eventID,
		AggregateType: aggType,
		AggregateID:   aggID,
		EventType:     eventType,
		Payload:       payload,
		CreatedAt:     createdAt.UTC(),
	}, nil
}

func getString(v any) (string, bool) {
	switch val := v.(type) {
	case string:
		return val, true
	case []byte:
		return string(val), true
	default:
		return "", false
	}
}

func getBytes(v any) ([]byte, bool) {
	switch val := v.(type) {
	case []byte:
		return val, true
	case string:
		return []byte(val), true
	default:
		return nil, false
	}
}
