package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/postgres/generated"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/uuid"
	"github.com/jackc/pgx/v5"
)

type OutboxRepository struct {
	queries *generated.Queries
}

func NewOutboxRepository(db generated.DBTX) (*OutboxRepository, error) {
	if db == nil {
		return nil, errors.New("database connection cannot be nil")
	}
	return &OutboxRepository{
		queries: generated.New(db),
	}, nil
}

func (r *OutboxRepository) WithTx(tx pgx.Tx) *OutboxRepository {
	return &OutboxRepository{
		queries: r.queries.WithTx(tx),
	}
}

func (r *OutboxRepository) CreateEvent(
	ctx context.Context,
	aggregateType string,
	aggregateID string,
	eventType string,
	payload []byte,
) error {
	if strings.TrimSpace(aggregateType) == "" {
		return errors.New("aggregate type cannot be empty")
	}
	if strings.TrimSpace(aggregateID) == "" {
		return errors.New("aggregate id cannot be empty")
	}
	if strings.TrimSpace(eventType) == "" {
		return errors.New("event type cannot be empty")
	}
	if len(payload) == 0 {
		return errors.New("outbox event payload cannot be empty")
	}

	eventID, err := uuid.NewString()
	if err != nil {
		return fmt.Errorf("generate outbox event id: %w", err)
	}
	eventUUID, err := parseUUID(eventID)
	if err != nil {
		return fmt.Errorf("parse outbox event id: %w", err)
	}

	_, err = r.queries.CreateOutboxEvent(ctx, generated.CreateOutboxEventParams{
		ID:            eventUUID,
		AggregateType: strings.TrimSpace(aggregateType),
		AggregateID:   strings.TrimSpace(aggregateID),
		EventType:     strings.TrimSpace(eventType),
		Payload:       payload,
		Status:        "PENDING",
		RetryCount:    0,
		CreatedAt:     requiredTimestamptz(time.Now().UTC()),
	})
	if err != nil {
		return fmt.Errorf("create outbox event: %w", err)
	}

	return nil
}
