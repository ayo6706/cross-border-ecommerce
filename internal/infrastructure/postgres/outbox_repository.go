package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	appDLQ "github.com/ayo6706/cross-border-ecommerce/internal/application/dlq"
	appOutbox "github.com/ayo6706/cross-border-ecommerce/internal/application/outbox"
	appProduct "github.com/ayo6706/cross-border-ecommerce/internal/application/product"
	domainProduct "github.com/ayo6706/cross-border-ecommerce/internal/domain/product"
	"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/postgres/generated"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

var (
	_ appOutbox.Store         = (*OutboxRepository)(nil)
	_ appDLQ.OutboxWriter     = (*OutboxRepository)(nil)
	_ appProduct.OutboxWriter = (*OutboxRepository)(nil)
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

	err = r.queries.CreateOutboxEvent(ctx, generated.CreateOutboxEventParams{
		ID:            eventUUID,
		AggregateType: strings.TrimSpace(aggregateType),
		AggregateID:   strings.TrimSpace(aggregateID),
		EventType:     strings.TrimSpace(eventType),
		Payload:       payload,
	})
	if err != nil {
		return fmt.Errorf("create outbox event: %w", err)
	}

	return nil
}

// productChangedPayload is the published product.changed contract. Consumers decode this
// shape, so a field rename is a breaking change.
type productChangedPayload struct {
	ProductID     string   `json:"product_id"`
	VersionID     string   `json:"version_id"`
	VersionNumber int      `json:"version_number"`
	Fingerprint   string   `json:"fingerprint"`
	ChangeType    string   `json:"change_type"`
	ChangedFields []string `json:"changed_fields"`
}

// CreateProductChangedEvents writes one outbox row per event in a single COPY, inside the
// caller's transaction, so the events commit or roll back with the versions they describe.
func (r *OutboxRepository) CreateProductChangedEvents(ctx context.Context, events []domainProduct.ProductChanged) error {
	if len(events) == 0 {
		return nil
	}
	params := make([]generated.CopyOutboxEventsParams, len(events))
	for i, e := range events {
		id, err := newUUID()
		if err != nil {
			return fmt.Errorf("generate outbox event id: %w", err)
		}
		payload, err := json.Marshal(productChangedPayload{
			ProductID:     string(e.ProductID),
			VersionID:     e.VersionID,
			VersionNumber: e.VersionNumber,
			Fingerprint:   e.Fingerprint,
			ChangeType:    string(e.ChangeType),
			ChangedFields: e.ChangedFields,
		})
		if err != nil {
			return fmt.Errorf("marshal product.changed payload: %w", err)
		}
		params[i] = generated.CopyOutboxEventsParams{
			ID:            id,
			AggregateType: domainProduct.AggregateTypeProduct,
			AggregateID:   string(e.ProductID),
			EventType:     domainProduct.EventTypeProductChanged,
			Payload:       payload,
		}
	}
	if _, err := r.queries.CopyOutboxEvents(ctx, params); err != nil {
		return fmt.Errorf("copy outbox events: %w", err)
	}
	return nil
}

// CreateReplayEvent writes an outbox event that re-publishes a dead-lettered message to one consumer
// group under its original event_id.
func (r *OutboxRepository) CreateReplayEvent(ctx context.Context, e appDLQ.ReplayEvent) error {
	id, err := parseUUID(e.ID)
	if err != nil {
		return fmt.Errorf("invalid replay outbox id: %w", err)
	}
	if strings.TrimSpace(e.ReplayOfEventID) == "" {
		return errors.New("replay event id cannot be empty")
	}
	if strings.TrimSpace(e.TargetGroup) == "" {
		return errors.New("replay target group cannot be empty")
	}
	if strings.TrimSpace(e.EventType) == "" {
		return errors.New("event type cannot be empty")
	}
	if len(e.Payload) == 0 {
		return errors.New("outbox event payload cannot be empty")
	}

	err = r.queries.CreateReplayOutboxEvent(ctx, generated.CreateReplayOutboxEventParams{
		ID:              id,
		AggregateType:   e.AggregateType,
		AggregateID:     e.AggregateID,
		EventType:       e.EventType,
		Payload:         e.Payload,
		ReplayOfEventID: pgtype.Text{String: e.ReplayOfEventID, Valid: true},
		TargetGroup:     pgtype.Text{String: e.TargetGroup, Valid: true},
	})
	if err != nil {
		return fmt.Errorf("create replay outbox event: %w", err)
	}
	return nil
}

func (r *OutboxRepository) ClaimBatch(
	ctx context.Context,
	claimToken string,
	limit int,
	lease time.Duration,
) ([]appOutbox.Event, error) {
	tokenUUID, err := parseUUID(claimToken)
	if err != nil {
		return nil, fmt.Errorf("invalid claim token: %w", err)
	}
	if limit <= 0 {
		return nil, errors.New("limit must be positive")
	}
	limitInt32, err := toInt32(limit)
	if err != nil {
		return nil, fmt.Errorf("invalid limit: %w", err)
	}
	if lease <= 0 {
		return nil, errors.New("lease duration must be positive")
	}

	leaseInterval := pgtype.Interval{
		Microseconds: lease.Microseconds(),
		Valid:        true,
	}

	rows, err := r.queries.ClaimOutboxBatch(ctx, generated.ClaimOutboxBatchParams{
		ClaimToken:    tokenUUID,
		LeaseDuration: leaseInterval,
		BatchSize:     limitInt32,
	})
	if err != nil {
		return nil, fmt.Errorf("claim outbox batch: %w", err)
	}

	events := make([]appOutbox.Event, len(rows))
	for i, row := range rows {
		events[i] = appOutbox.Event{
			ID:            uuidToString(row.ID),
			EventID:       row.EventID,
			AggregateType: row.AggregateType,
			AggregateID:   row.AggregateID,
			EventType:     row.EventType,
			Payload:       row.Payload,
			RetryCount:    int(row.RetryCount),
			CreatedAt:     row.CreatedAt.Time.UTC(),
			TargetGroup:   row.TargetGroup,
		}
	}

	return events, nil
}

func (r *OutboxRepository) MarkPublished(
	ctx context.Context,
	claimToken string,
	ids []string,
) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}

	tokenUUID, err := parseUUID(claimToken)
	if err != nil {
		return 0, fmt.Errorf("invalid claim token: %w", err)
	}

	uuids := make([]pgtype.UUID, len(ids))
	for i, id := range ids {
		u, err := parseUUID(id)
		if err != nil {
			return 0, fmt.Errorf("invalid event id %q: %w", id, err)
		}
		uuids[i] = u
	}

	affected, err := r.queries.MarkOutboxPublished(ctx, generated.MarkOutboxPublishedParams{
		Ids:        uuids,
		ClaimToken: tokenUUID,
	})
	if err != nil {
		return 0, fmt.Errorf("mark outbox published: %w", err)
	}

	return affected, nil
}

func (r *OutboxRepository) RecordFailure(
	ctx context.Context,
	claimToken, id, cause string,
	maxAttempts int,
	backoff time.Duration,
) error {
	tokenUUID, err := parseUUID(claimToken)
	if err != nil {
		return fmt.Errorf("invalid claim token: %w", err)
	}
	eventUUID, err := parseUUID(id)
	if err != nil {
		return fmt.Errorf("invalid event id: %w", err)
	}
	maxAttemptsInt32, err := toInt32(maxAttempts)
	if err != nil {
		return fmt.Errorf("invalid max attempts: %w", err)
	}

	backoffInterval := pgtype.Interval{
		Microseconds: backoff.Microseconds(),
		Valid:        true,
	}

	_, err = r.queries.RecordOutboxPublishFailure(ctx, generated.RecordOutboxPublishFailureParams{
		LastError: pgtype.Text{
			String: cause,
			Valid:  cause != "",
		},
		Backoff:     backoffInterval,
		MaxAttempts: maxAttemptsInt32,
		ID:          eventUUID,
		ClaimToken:  tokenUUID,
	})
	if err != nil {
		return fmt.Errorf("record outbox publish failure: %w", err)
	}

	return nil
}

func (r *OutboxRepository) Release(
	ctx context.Context,
	claimToken string,
	ids []string,
) error {
	if len(ids) == 0 {
		return nil
	}

	tokenUUID, err := parseUUID(claimToken)
	if err != nil {
		return fmt.Errorf("invalid claim token: %w", err)
	}

	uuids := make([]pgtype.UUID, len(ids))
	for i, id := range ids {
		u, err := parseUUID(id)
		if err != nil {
			return fmt.Errorf("invalid event id %q: %w", id, err)
		}
		uuids[i] = u
	}

	_, err = r.queries.ReleaseOutboxClaims(ctx, generated.ReleaseOutboxClaimsParams{
		Ids:        uuids,
		ClaimToken: tokenUUID,
	})
	if err != nil {
		return fmt.Errorf("release outbox claims: %w", err)
	}

	return nil
}
