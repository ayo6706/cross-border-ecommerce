package redis

import (
	"fmt"
	"strings"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/application/dlq"
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
	FieldTargetGroup   = "target_group"
	FieldCorrelationID = "correlation_id"
)

var (
	ErrCorruptMessage = dlq.ErrCorruptEnvelope
)

func EncodeEvent(e appOutbox.Event) map[string]any {
	eventID := e.EventID
	if eventID == "" {
		eventID = e.ID
	}
	m := map[string]any{
		FieldEventID:       eventID,
		FieldAggregateType: e.AggregateType,
		FieldAggregateID:   e.AggregateID,
		FieldEventType:     e.EventType,
		FieldPayload:       e.Payload,
		FieldCreatedAt:     e.CreatedAt.UTC().Format(time.RFC3339Nano),
	}
	if e.TargetGroup != "" {
		m[FieldTargetGroup] = e.TargetGroup
	}
	return m
}

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
	correlationID, _ := getString(raw.Values[FieldCorrelationID])

	var payload []byte
	if rawPayload, ok := raw.Values[FieldPayload]; ok && rawPayload != nil {
		if b, ok := getBytes(rawPayload); ok {
			payload = b
		}
	}

	targetGroup, _ := getString(raw.Values[FieldTargetGroup])

	return appMessaging.Message{
		StreamID:      raw.ID,
		Stream:        stream,
		EventID:       eventID,
		AggregateType: aggType,
		AggregateID:   aggID,
		CorrelationID: correlationID,
		EventType:     eventType,
		Payload:       payload,
		CreatedAt:     createdAt.UTC(),
		TargetGroup:   targetGroup,
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

func deadLetterRecord(raw goredis.XMessage) dlq.Message {
	field := func(name string) string {
		s, _ := getString(raw.Values[name])
		return s
	}

	rec := dlq.Message{
		StreamMessageID: raw.ID,
		EventType:       field(FieldEventType),
		AggregateType:   field(FieldAggregateType),
		AggregateID:     field(FieldAggregateID),
		CorrelationID:   field(FieldCorrelationID),
		Payload:         []byte{},
	}
	if eventID := field(FieldEventID); eventID != "" {
		rec.EventID = &eventID
	}
	if b, ok := getBytes(raw.Values[FieldPayload]); ok {
		rec.Payload = b
	}
	if createdAt, err := time.Parse(time.RFC3339Nano, field(FieldCreatedAt)); err == nil {
		createdAt = createdAt.UTC()
		rec.EventCreatedAt = &createdAt
	}
	return rec
}
