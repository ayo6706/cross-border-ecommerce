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
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	_ dlq.Writer   = (*DLQRepository)(nil)
	_ dlq.Replayer = (*DLQRepository)(nil)
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
	pool    *pgxpool.Pool
	queries *generated.Queries
}

func NewDLQRepository(pool *pgxpool.Pool) (*DLQRepository, error) {
	if pool == nil {
		return nil, errors.New("database connection pool cannot be nil")
	}
	return &DLQRepository{
		pool:    pool,
		queries: generated.New(pool),
	}, nil
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

func (r *DLQRepository) Replay(ctx context.Context, id string) (string, error) {
	idUUID, err := parseUUID(id)
	if err != nil {
		return "", fmt.Errorf("%w: invalid uuid %q: %w", dlq.ErrInvalidDLQID, id, err)
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return "", fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	qtx := r.queries.WithTx(tx)

	row, err := qtx.GetDLQReplaySource(ctx, idUUID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", dlq.ErrDLQNotFound
		}
		return "", fmt.Errorf("query dlq message for replay: %w", err)
	}
	if !row.Replayable {
		return "", dlq.ErrNotReplayable
	}

	outboxID, err := uuid.NewString()
	if err != nil {
		return "", fmt.Errorf("generate replay outbox id: %w", err)
	}
	outboxUUID, err := parseUUID(outboxID)
	if err != nil {
		return "", fmt.Errorf("parse replay outbox uuid: %w", err)
	}

	err = qtx.CreateReplayOutboxEvent(ctx, generated.CreateReplayOutboxEventParams{
		ID:              outboxUUID,
		AggregateType:   row.AggregateType,
		AggregateID:     row.AggregateID,
		EventType:       row.EventType,
		Payload:         row.Payload,
		ReplayOfEventID: row.EventID,
		TargetGroup:     pgtype.Text{String: row.ConsumerGroup, Valid: true},
	})
	if err != nil {
		return "", fmt.Errorf("create replay outbox event: %w", err)
	}

	affected, err := qtx.MarkDLQMessageReplayed(ctx, generated.MarkDLQMessageReplayedParams{
		ReplayOutboxID: outboxUUID,
		ID:             idUUID,
	})
	if err != nil {
		return "", fmt.Errorf("mark dlq message replayed: %w", err)
	}
	if affected == 0 {
		return "", dlq.ErrAlreadyReplayed
	}

	if err := tx.Commit(ctx); err != nil {
		return "", fmt.Errorf("commit replay transaction: %w", err)
	}
	return outboxID, nil
}
