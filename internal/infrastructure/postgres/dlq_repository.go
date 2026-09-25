package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/ayo6706/cross-border-ecommerce/internal/application/dlq"
	"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/postgres/generated"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

var (
	_ dlq.Writer     = (*DLQRepository)(nil)
	_ dlq.Repository = (*DLQRepository)(nil)
)

const (
	dlqIdentityLen      = 128 // stream, consumer_group, consumer_name
	dlqStreamMsgIDLen   = 64
	dlqEventIDLen       = 256 // = idempotency_keys.key
	dlqEventTypeLen     = 64
	dlqAggregateTypeLen = 64
	dlqAggregateIDLen   = 128
	dlqCorrelationIDLen = 128
	dlqFailureClassLen  = 64
)

type DLQRepository struct {
	queries *generated.Queries
}

func NewDLQRepository(db generated.DBTX) (*DLQRepository, error) {
	if db == nil {
		return nil, errors.New("database connection cannot be nil")
	}
	return &DLQRepository{queries: generated.New(db)}, nil
}

func (r *DLQRepository) Insert(ctx context.Context, msg dlq.Message) error {
	if err := validateDLQIdentity(msg); err != nil {
		return err
	}
	attempts, err := toInt32(msg.Attempts)
	if err != nil {
		return fmt.Errorf("%w: invalid attempts count: %w", dlq.ErrInvalidDLQInput, err)
	}

	eventType, eventTypeIntact := storable(msg.EventType, dlqEventTypeLen)
	aggregateType, aggregateTypeIntact := storable(msg.AggregateType, dlqAggregateTypeLen)
	aggregateID, aggregateIDIntact := storable(msg.AggregateID, dlqAggregateIDLen)
	correlationID, _ := storable(msg.CorrelationID, dlqCorrelationIDLen)

	var eventID pgtype.Text
	eventIDIntact := false
	if msg.EventID != nil && strings.TrimSpace(*msg.EventID) != "" {
		eventID.String, eventIDIntact = storable(*msg.EventID, dlqEventIDLen)
		eventID.Valid = true
	}

	replayable := msg.FailureClass != dlq.ClassCorruptEnvelope &&
		eventIDIntact && eventTypeIntact && aggregateTypeIntact && aggregateIDIntact &&
		eventType != "" && json.Valid(msg.Payload)

	var eventCreatedAt pgtype.Timestamptz
	if msg.EventCreatedAt != nil {
		eventCreatedAt = pgtype.Timestamptz{Time: msg.EventCreatedAt.UTC(), Valid: true}
	}

	stack := sanitizeText(msg.Stack)
	err = r.queries.InsertDLQMessage(ctx, generated.InsertDLQMessageParams{
		Stream:          msg.Stream,
		ConsumerGroup:   msg.ConsumerGroup,
		StreamMessageID: msg.StreamMessageID,
		EventID:         eventID,
		EventType:       eventType,
		AggregateType:   aggregateType,
		AggregateID:     aggregateID,
		CorrelationID:   correlationID,
		Payload:         msg.Payload,
		FailureClass:    string(msg.FailureClass),
		LastError:       sanitizeText(msg.LastError),
		Stack:           pgtype.Text{String: stack, Valid: stack != ""},
		Attempts:        attempts,
		ConsumerName:    msg.ConsumerName,
		EventCreatedAt:  eventCreatedAt,
		FirstFailedAt:   pgtype.Timestamptz{Time: msg.FirstFailedAt.UTC(), Valid: true},
		Replayable:      replayable,
	})
	if err != nil {
		return fmt.Errorf("insert dlq message: %w", err)
	}
	return nil
}

func validateDLQIdentity(msg dlq.Message) error {
	for _, f := range []struct {
		name, value string
		max         int
	}{
		{"stream", msg.Stream, dlqIdentityLen},
		{"consumer group", msg.ConsumerGroup, dlqIdentityLen},
		{"consumer name", msg.ConsumerName, dlqIdentityLen},
		{"stream message id", msg.StreamMessageID, dlqStreamMsgIDLen},
		{"failure class", string(msg.FailureClass), dlqFailureClassLen},
	} {
		if strings.TrimSpace(f.value) == "" {
			return fmt.Errorf("%w: %s cannot be empty", dlq.ErrInvalidDLQInput, f.name)
		}
		if len(f.value) > f.max {
			return fmt.Errorf("%w: %s must be at most %d bytes, got %d", dlq.ErrInvalidDLQInput, f.name, f.max, len(f.value))
		}
	}
	if msg.Attempts < 1 {
		return fmt.Errorf("%w: attempts must be >= 1, got %d", dlq.ErrInvalidDLQInput, msg.Attempts)
	}
	if msg.FirstFailedAt.IsZero() {
		return fmt.Errorf("%w: first failed at is required", dlq.ErrInvalidDLQInput)
	}
	return nil
}

// sanitizeText makes s acceptable to a PostgreSQL TEXT column: valid UTF-8 and no NUL bytes.
func sanitizeText(s string) string {
	return strings.ReplaceAll(strings.ToValidUTF8(s, string(utf8.RuneError)), "\x00", "")
}

// storable returns s sanitized and cut to maxRunes, and whether it came through unchanged.
func storable(s string, maxRunes int) (string, bool) {
	clean := sanitizeText(s)
	if runes := []rune(clean); len(runes) > maxRunes {
		clean = string(runes[:maxRunes])
	}
	return clean, clean == s
}

func (r *DLQRepository) GetReplaySource(ctx context.Context, id string) (dlq.ReplaySource, error) {
	idUUID, err := parseUUID(id)
	if err != nil {
		return dlq.ReplaySource{}, fmt.Errorf("%w: invalid uuid %q: %w", dlq.ErrInvalidDLQID, id, err)
	}
	row, err := r.queries.GetDLQReplaySource(ctx, idUUID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return dlq.ReplaySource{}, dlq.ErrDLQNotFound
		}
		return dlq.ReplaySource{}, fmt.Errorf("get dlq replay source: %w", err)
	}
	return dlq.ReplaySource{
		ConsumerGroup: row.ConsumerGroup,
		EventID:       row.EventID.String,
		EventType:     row.EventType,
		AggregateType: row.AggregateType,
		AggregateID:   row.AggregateID,
		Payload:       row.Payload,
		Replayable:    row.Replayable,
	}, nil
}

// MarkReplayed moves the message from DEAD to REPLAYED with a guarded update, so of two concurrent
// replays only one changes the row.
func (r *DLQRepository) MarkReplayed(ctx context.Context, id, replayOutboxID string) (bool, error) {
	idUUID, err := parseUUID(id)
	if err != nil {
		return false, fmt.Errorf("%w: invalid uuid %q: %w", dlq.ErrInvalidDLQID, id, err)
	}
	outboxUUID, err := parseUUID(replayOutboxID)
	if err != nil {
		return false, fmt.Errorf("invalid replay outbox id: %w", err)
	}
	affected, err := r.queries.MarkDLQMessageReplayed(ctx, generated.MarkDLQMessageReplayedParams{
		ReplayOutboxID: outboxUUID,
		ID:             idUUID,
	})
	if err != nil {
		return false, fmt.Errorf("mark dlq message replayed: %w", err)
	}
	return affected == 1, nil
}
