package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
	"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/postgres/generated"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

var _ ingestion.RunProcessingRepository = (*RunProcessingRepository)(nil)

type RunProcessingRepository struct {
	queries *generated.Queries
}

func NewRunProcessingRepository(db generated.DBTX) (*RunProcessingRepository, error) {
	if db == nil {
		return nil, errors.New("database connection cannot be nil")
	}
	return &RunProcessingRepository{
		queries: generated.New(db),
	}, nil
}

func (r *RunProcessingRepository) WithTx(tx pgx.Tx) *RunProcessingRepository {
	return &RunProcessingRepository{
		queries: r.queries.WithTx(tx),
	}
}

func (r *RunProcessingRepository) SeedPending(ctx context.Context) error {
	return r.queries.SeedPendingRunProcessing(ctx)
}

func (r *RunProcessingRepository) ClaimNext(ctx context.Context, claimToken string, leaseDuration time.Duration) (*ingestion.RunProcessing, error) {
	tokenUUID, err := parseUUID(claimToken)
	if err != nil {
		return nil, fmt.Errorf("invalid claim token uuid: %w", err)
	}

	leaseInterval := pgtype.Interval{
		Microseconds: leaseDuration.Microseconds(),
		Valid:        true,
	}

	row, err := r.queries.ClaimNextRunProcessing(ctx, generated.ClaimNextRunProcessingParams{
		ClaimToken:    tokenUUID,
		LeaseDuration: leaseInterval,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil // No runs available to claim
		}
		return nil, fmt.Errorf("claim next run processing: %w", err)
	}

	return toDomainRunProcessing(row), nil
}

func (r *RunProcessingRepository) ClaimSpecific(ctx context.Context, runID string, claimToken string, leaseDuration time.Duration) (*ingestion.RunProcessing, error) {
	rUUID, err := parseUUID(runID)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid run id: %w", ingestion.ErrInvalidRunID, err)
	}
	tokenUUID, err := parseUUID(claimToken)
	if err != nil {
		return nil, fmt.Errorf("invalid claim token uuid: %w", err)
	}

	leaseInterval := pgtype.Interval{
		Microseconds: leaseDuration.Microseconds(),
		Valid:        true,
	}

	row, err := r.queries.ClaimSpecificRunProcessing(ctx, generated.ClaimSpecificRunProcessingParams{
		RunID:         rUUID,
		ClaimToken:    tokenUUID,
		LeaseDuration: leaseInterval,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("cannot claim run %s: run not found or active lease held", runID)
		}
		return nil, fmt.Errorf("claim specific run processing: %w", err)
	}

	return toDomainRunProcessing(row), nil
}

func (r *RunProcessingRepository) GetByID(ctx context.Context, runID string) (*ingestion.RunProcessing, error) {
	rUUID, err := parseUUID(runID)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid run id: %w", ingestion.ErrInvalidRunID, err)
	}

	row, err := r.queries.GetRunProcessingByID(ctx, rUUID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ingestion.ErrRunProcessingNotFound
		}
		return nil, fmt.Errorf("get run processing by id: %w", err)
	}

	return toDomainRunProcessing(row), nil
}

func (r *RunProcessingRepository) EnsureExists(ctx context.Context, runID string) (*ingestion.RunProcessing, error) {
	rUUID, err := parseUUID(runID)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid run id: %w", ingestion.ErrInvalidRunID, err)
	}

	row, err := r.queries.EnsureRunProcessingExists(ctx, rUUID)
	if err != nil {
		return nil, fmt.Errorf("ensure run processing exists: %w", err)
	}

	return toDomainRunProcessing(row), nil
}

func (r *RunProcessingRepository) UpdateProgress(
	ctx context.Context,
	runID string,
	claimToken string,
	seen, newRecs, changed, unchanged, failed int,
	cursorID *string,
	leaseDuration time.Duration,
) (*ingestion.RunProcessing, error) {
	rUUID, err := parseUUID(runID)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid run id: %w", ingestion.ErrInvalidRunID, err)
	}
	tokenUUID, err := parseUUID(claimToken)
	if err != nil {
		return nil, fmt.Errorf("invalid claim token uuid: %w", err)
	}

	var cUUID pgtype.UUID
	if cursorID != nil && strings.TrimSpace(*cursorID) != "" {
		cUUID, err = parseUUID(*cursorID)
		if err != nil {
			return nil, fmt.Errorf("invalid cursor uuid: %w", err)
		}
	}

	counters, err := toRunCounters(seen, newRecs, changed, unchanged, failed)
	if err != nil {
		return nil, err
	}

	leaseInterval := pgtype.Interval{
		Microseconds: leaseDuration.Microseconds(),
		Valid:        true,
	}

	row, err := r.queries.UpdateRunProcessingProgress(ctx, generated.UpdateRunProcessingProgressParams{
		RunID:         rUUID,
		ClaimToken:    tokenUUID,
		CursorID:      cUUID,
		SeenInc:       counters.seen,
		NewInc:        counters.newRecords,
		ChangedInc:    counters.changed,
		UnchangedInc:  counters.unchanged,
		FailedInc:     counters.failed,
		LeaseDuration: leaseInterval,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ingestion.ErrLeaseLost
		}
		return nil, fmt.Errorf("update run processing progress: %w", err)
	}

	return toDomainRunProcessing(row), nil
}

func (r *RunProcessingRepository) Release(ctx context.Context, runID string, claimToken string) error {
	rUUID, err := parseUUID(runID)
	if err != nil {
		return fmt.Errorf("%w: invalid run id: %w", ingestion.ErrInvalidRunID, err)
	}
	tokenUUID, err := parseUUID(claimToken)
	if err != nil {
		return fmt.Errorf("invalid claim token uuid: %w", err)
	}

	_, err = r.queries.ReleaseRunProcessingClaim(ctx, generated.ReleaseRunProcessingClaimParams{
		RunID:      rUUID,
		ClaimToken: tokenUUID,
	})
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("release run processing claim: %w", err)
	}
	return nil
}

func (r *RunProcessingRepository) Complete(ctx context.Context, runID string, claimToken string) error {
	rUUID, err := parseUUID(runID)
	if err != nil {
		return fmt.Errorf("%w: invalid run id: %w", ingestion.ErrInvalidRunID, err)
	}
	tokenUUID, err := parseUUID(claimToken)
	if err != nil {
		return fmt.Errorf("invalid claim token uuid: %w", err)
	}

	_, err = r.queries.CompleteRunProcessing(ctx, generated.CompleteRunProcessingParams{
		RunID:      rUUID,
		ClaimToken: tokenUUID,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ingestion.ErrLeaseLost
		}
		return fmt.Errorf("complete run processing: %w", err)
	}
	return nil
}

func (r *RunProcessingRepository) Fail(ctx context.Context, runID string, claimToken string, errSummary string) error {
	rUUID, err := parseUUID(runID)
	if err != nil {
		return fmt.Errorf("%w: invalid run id: %w", ingestion.ErrInvalidRunID, err)
	}

	var tokenUUID pgtype.UUID
	if strings.TrimSpace(claimToken) != "" {
		tUUID, parseErr := parseUUID(claimToken)
		if parseErr != nil {
			return fmt.Errorf("invalid claim token uuid: %w", parseErr)
		}
		tokenUUID = tUUID
	}

	_, err = r.queries.FailRunProcessing(ctx, generated.FailRunProcessingParams{
		RunID:        rUUID,
		ClaimToken:   tokenUUID,
		ErrorSummary: strings.TrimSpace(errSummary),
	})
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("fail run processing: %w", err)
	}
	return nil
}

func (r *RunProcessingRepository) ResetFromStart(ctx context.Context, runID string) (*ingestion.RunProcessing, error) {
	rUUID, err := parseUUID(runID)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid run id: %w", ingestion.ErrInvalidRunID, err)
	}

	row, err := r.queries.ResetRunProcessingFromStart(ctx, rUUID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("cannot reset run %s: active lease held by running worker", runID)
		}
		return nil, fmt.Errorf("reset run processing: %w", err)
	}

	return toDomainRunProcessing(row), nil
}

func toDomainRunProcessing(row generated.IngestionRunProcessing) *ingestion.RunProcessing {
	var claimToken *string
	if row.ClaimToken.Valid {
		v := uuidToString(row.ClaimToken)
		claimToken = &v
	}

	var cursorID *string
	if row.CursorRawRecordID.Valid {
		v := uuidToString(row.CursorRawRecordID)
		cursorID = &v
	}

	return &ingestion.RunProcessing{
		RunID:             uuidToString(row.RunID),
		Status:            ingestion.ProcessingStatus(row.Status),
		ClaimToken:        claimToken,
		LeaseExpiresAt:    fromTimestamptz(row.LeaseExpiresAt),
		CursorRawRecordID: cursorID,
		RecordsSeen:       int(row.RecordsSeen),
		RecordsNew:        int(row.RecordsNew),
		RecordsChanged:    int(row.RecordsChanged),
		RecordsUnchanged:  int(row.RecordsUnchanged),
		RecordsFailed:     int(row.RecordsFailed),
		ErrorSummary:      row.ErrorSummary,
		StartedAt:         fromTimestamptz(row.StartedAt),
		CompletedAt:       fromTimestamptz(row.CompletedAt),
		CreatedAt:         row.CreatedAt.Time.UTC(),
		UpdatedAt:         row.UpdatedAt.Time.UTC(),
	}
}
