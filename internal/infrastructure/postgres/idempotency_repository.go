package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	appIdempotency "github.com/ayo6706/cross-border-ecommerce/internal/application/idempotency"
	"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/postgres/generated"
	"github.com/jackc/pgx/v5"
)

var _ appIdempotency.Store = (*IdempotencyRepository)(nil)

type IdempotencyRepository struct {
	queries *generated.Queries
}

func NewIdempotencyRepository(db generated.DBTX) (*IdempotencyRepository, error) {
	if db == nil {
		return nil, errors.New("database connection cannot be nil")
	}
	return &IdempotencyRepository{
		queries: generated.New(db),
	}, nil
}

func (r *IdempotencyRepository) WithTx(tx pgx.Tx) *IdempotencyRepository {
	return &IdempotencyRepository{
		queries: r.queries.WithTx(tx),
	}
}

//nolint:funlen // legacy baseline 2026-09-26: fix in ENG-025
func (r *IdempotencyRepository) ClaimKey(
	ctx context.Context,
	scope, key string,
	payloadHash []byte,
	token string,
	ttl time.Duration,
) (appIdempotency.Claim, error) {
	if strings.TrimSpace(scope) == "" {
		return appIdempotency.Claim{}, appIdempotency.ErrInvalidScope
	}
	if strings.TrimSpace(key) == "" {
		return appIdempotency.Claim{}, appIdempotency.ErrInvalidKey
	}
	if len(payloadHash) != 32 {
		return appIdempotency.Claim{}, appIdempotency.ErrInvalidPayloadHash
	}
	if ttl <= 0 {
		return appIdempotency.Claim{}, appIdempotency.ErrInvalidLeaseTTL
	}

	leaseUUID, err := parseUUID(token)
	if err != nil {
		return appIdempotency.Claim{}, fmt.Errorf("parse lease token uuid: %w", err)
	}

	claimRow, err := r.queries.ClaimIdempotencyKey(ctx, generated.ClaimIdempotencyKeyParams{
		Scope:           scope,
		Key:             key,
		PayloadHash:     payloadHash,
		LeaseToken:      leaseUUID,
		LeaseDurationMs: ttl.Milliseconds(),
	})
	if err == nil {
		return appIdempotency.Claim{
			Scope:      claimRow.Scope,
			Key:        claimRow.Key,
			Status:     appIdempotency.ClaimAcquired,
			LeaseToken: uuidToString(claimRow.LeaseToken),
			ExpiresAt:  claimRow.LeaseExpiresAt.Time,
			Attempts:   int(claimRow.Attempts),
		}, nil
	}

	if !errors.Is(err, pgx.ErrNoRows) {
		return appIdempotency.Claim{}, fmt.Errorf("claim idempotency key query: %w", err)
	}

	// Conflict occurred and WHERE clause prevented update: inspect existing row
	existing, err := r.queries.GetIdempotencyKey(ctx, generated.GetIdempotencyKeyParams{
		Scope: scope,
		Key:   key,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return appIdempotency.Claim{}, appIdempotency.ErrKeyInProgress
		}
		return appIdempotency.Claim{}, fmt.Errorf("inspect existing idempotency key: %w", err)
	}

	if !bytes.Equal(existing.PayloadHash, payloadHash) {
		return appIdempotency.Claim{}, appIdempotency.ErrPayloadMismatch
	}

	if existing.Status == "COMPLETED" {
		return appIdempotency.Claim{
			Scope:  existing.Scope,
			Key:    existing.Key,
			Status: appIdempotency.ClaimAlreadyCompleted,
		}, nil
	}

	// Status is IN_PROGRESS: actively held by another worker
	return appIdempotency.Claim{}, appIdempotency.ErrKeyInProgress
}

func (r *IdempotencyRepository) CompleteKey(ctx context.Context, scope, key, token string) error {
	if strings.TrimSpace(scope) == "" {
		return appIdempotency.ErrInvalidScope
	}
	if strings.TrimSpace(key) == "" {
		return appIdempotency.ErrInvalidKey
	}
	leaseUUID, err := parseUUID(token)
	if err != nil {
		return fmt.Errorf("parse lease token uuid: %w", err)
	}

	rowsAffected, err := r.queries.CompleteIdempotencyKey(ctx, generated.CompleteIdempotencyKeyParams{
		Scope:      scope,
		Key:        key,
		LeaseToken: leaseUUID,
	})
	if err != nil {
		return fmt.Errorf("complete idempotency key query: %w", err)
	}
	if rowsAffected == 0 {
		return appIdempotency.ErrLeaseLost
	}
	return nil
}

func (r *IdempotencyRepository) ReleaseKey(ctx context.Context, scope, key, token string) error {
	if strings.TrimSpace(scope) == "" {
		return appIdempotency.ErrInvalidScope
	}
	if strings.TrimSpace(key) == "" {
		return appIdempotency.ErrInvalidKey
	}
	leaseUUID, err := parseUUID(token)
	if err != nil {
		return fmt.Errorf("parse lease token uuid: %w", err)
	}

	_, err = r.queries.ReleaseIdempotencyKey(ctx, generated.ReleaseIdempotencyKeyParams{
		Scope:      scope,
		Key:        key,
		LeaseToken: leaseUUID,
	})
	if err != nil {
		return fmt.Errorf("release idempotency key query: %w", err)
	}
	return nil
}

func (r *IdempotencyRepository) GetKey(ctx context.Context, scope, key string) (appIdempotency.Record, error) {
	if strings.TrimSpace(scope) == "" {
		return appIdempotency.Record{}, appIdempotency.ErrInvalidScope
	}
	if strings.TrimSpace(key) == "" {
		return appIdempotency.Record{}, appIdempotency.ErrInvalidKey
	}

	row, err := r.queries.GetIdempotencyKey(ctx, generated.GetIdempotencyKeyParams{
		Scope: scope,
		Key:   key,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return appIdempotency.Record{}, appIdempotency.ErrKeyNotFound
		}
		return appIdempotency.Record{}, err
	}

	var leaseToken *string
	if row.LeaseToken.Valid {
		s := uuidToString(row.LeaseToken)
		leaseToken = &s
	}

	var leaseExpiresAt *time.Time
	if row.LeaseExpiresAt.Valid {
		t := row.LeaseExpiresAt.Time
		leaseExpiresAt = &t
	}

	var completedAt *time.Time
	if row.CompletedAt.Valid {
		t := row.CompletedAt.Time
		completedAt = &t
	}

	return appIdempotency.Record{
		Scope:          row.Scope,
		Key:            row.Key,
		Status:         row.Status,
		PayloadHash:    row.PayloadHash,
		LeaseToken:     leaseToken,
		LeaseExpiresAt: leaseExpiresAt,
		Attempts:       int(row.Attempts),
		CreatedAt:      row.CreatedAt.Time,
		CompletedAt:    completedAt,
	}, nil
}
