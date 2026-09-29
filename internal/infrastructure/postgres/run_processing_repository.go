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

func (r *RunProcessingRepository) ClaimNext(ctx context.Context, claimToken string, leaseDuration time.Duration) (*ingestion.RunProcessing, error) {
	tokenUUID, err := parseUUID(claimToken)
	if err != nil {
		return nil, fmt.Errorf("invalid claim token uuid: %w", err)
	}

	row, err := r.queries.ClaimNextRunProcessing(ctx, generated.ClaimNextRunProcessingParams{
		ClaimToken:    tokenUUID,
		LeaseDuration: toInterval(leaseDuration),
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil // No runs available to claim
		}
		return nil, fmt.Errorf("claim next run processing: %w", err)
	}

	return toDomainRunProcessing(&row), nil
}

func (r *RunProcessingRepository) ClaimSpecific(ctx context.Context, runID, claimToken string, leaseDuration time.Duration) (*ingestion.RunProcessing, error) {
	rUUID, err := parseUUID(runID)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid run id: %w", ingestion.ErrInvalidRunID, err)
	}
	tokenUUID, err := parseUUID(claimToken)
	if err != nil {
		return nil, fmt.Errorf("invalid claim token uuid: %w", err)
	}

	row, err := r.queries.ClaimSpecificRunProcessing(ctx, generated.ClaimSpecificRunProcessingParams{
		RunID:         rUUID,
		ClaimToken:    tokenUUID,
		LeaseDuration: toInterval(leaseDuration),
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, r.guardFailure(ctx, rUUID, ingestion.ErrLeaseHeld)
		}
		return nil, fmt.Errorf("claim specific run processing: %w", err)
	}

	return toDomainRunProcessing(&row), nil
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

	return toDomainRunProcessing(&row), nil
}

func (r *RunProcessingRepository) UpdateProgress(
	ctx context.Context,
	runID string,
	claimToken string,
	metrics ingestion.BatchMetrics,
	cursorID *string,
	leaseDuration time.Duration,
) (*ingestion.RunProcessing, error) {
	params, err := parseUpdateProgressParams(runID, claimToken, metrics, cursorID, leaseDuration)
	if err != nil {
		return nil, fmt.Errorf("update run processing progress: %w", err)
	}

	row, err := r.queries.UpdateRunProcessingProgress(ctx, params)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ingestion.ErrLeaseLost
		}
		return nil, fmt.Errorf("update run processing progress: %w", err)
	}

	return toDomainRunProcessing(&row), nil
}

func parseUpdateProgressParams(
	runID, claimToken string,
	metrics ingestion.BatchMetrics,
	cursorID *string,
	leaseDuration time.Duration,
) (generated.UpdateRunProcessingProgressParams, error) {
	rUUID, err := parseUUID(runID)
	if err != nil {
		return generated.UpdateRunProcessingProgressParams{}, fmt.Errorf("%w: invalid run id: %w", ingestion.ErrInvalidRunID, err)
	}
	tokenUUID, err := parseUUID(claimToken)
	if err != nil {
		return generated.UpdateRunProcessingProgressParams{}, fmt.Errorf("invalid claim token uuid: %w", err)
	}

	cUUID, err := parseOptionalUUID(cursorID)
	if err != nil {
		return generated.UpdateRunProcessingProgressParams{}, fmt.Errorf("invalid cursor uuid: %w", err)
	}

	counters, err := toRunCounters(metrics)
	if err != nil {
		return generated.UpdateRunProcessingProgressParams{}, err
	}

	return generated.UpdateRunProcessingProgressParams{
		RunID:         rUUID,
		ClaimToken:    tokenUUID,
		CursorID:      cUUID,
		SeenInc:       counters.seen,
		NewInc:        counters.newRecords,
		ChangedInc:    counters.changed,
		UnchangedInc:  counters.unchanged,
		FailedInc:     counters.failed,
		LeaseDuration: toInterval(leaseDuration),
	}, nil
}

func (r *RunProcessingRepository) Release(ctx context.Context, runID, claimToken string) error {
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

func (r *RunProcessingRepository) Complete(
	ctx context.Context, runID, claimToken string,
) (*ingestion.RunProcessing, error) {
	rUUID, err := parseUUID(runID)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid run id: %w", ingestion.ErrInvalidRunID, err)
	}
	tokenUUID, err := parseUUID(claimToken)
	if err != nil {
		return nil, fmt.Errorf("invalid claim token uuid: %w", err)
	}

	row, err := r.queries.CompleteRunProcessing(ctx, generated.CompleteRunProcessingParams{
		RunID:      rUUID,
		ClaimToken: tokenUUID,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ingestion.ErrLeaseLost
		}
		return nil, fmt.Errorf("complete run processing: %w", err)
	}
	return toDomainRunProcessing(&row), nil
}

func (r *RunProcessingRepository) Fail(ctx context.Context, runID, claimToken, errSummary string) error {
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
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return r.guardFailure(ctx, rUUID, ingestion.ErrLeaseLost)
		}
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
			return nil, r.guardFailure(ctx, rUUID, ingestion.ErrLeaseHeld)
		}
		return nil, fmt.Errorf("reset run processing: %w", err)
	}

	return toDomainRunProcessing(&row), nil
}

// guardFailure explains a guarded write that matched no row: either the run has no processing
// state, or it exists and the guard (claim token, lease) refused the write.
func (r *RunProcessingRepository) guardFailure(ctx context.Context, rUUID pgtype.UUID, whenExists error) error {
	_, err := r.queries.GetRunProcessingByID(ctx, rUUID)
	switch {
	case err == nil:
		return whenExists
	case errors.Is(err, pgx.ErrNoRows):
		return ingestion.ErrRunProcessingNotFound
	default:
		return fmt.Errorf("probe run processing %s: %w", uuidToString(rUUID), err)
	}
}

func toDomainRunProcessing(row *generated.IngestionRunProcessing) *ingestion.RunProcessing {
	return &ingestion.RunProcessing{
		RunID:             uuidToString(row.RunID),
		Status:            ingestion.ProcessingStatus(row.Status),
		ClaimToken:        uuidPtr(row.ClaimToken),
		LeaseExpiresAt:    fromTimestamptz(row.LeaseExpiresAt),
		CursorRawRecordID: uuidPtr(row.CursorRawRecordID),
		BatchMetrics: ingestion.BatchMetrics{
			Seen:      int(row.RecordsSeen),
			New:       int(row.RecordsNew),
			Changed:   int(row.RecordsChanged),
			Unchanged: int(row.RecordsUnchanged),
			Failed:    int(row.RecordsFailed),
		},
		ErrorSummary: row.ErrorSummary,
		StartedAt:    fromTimestamptz(row.StartedAt),
		CompletedAt:  fromTimestamptz(row.CompletedAt),
		CreatedAt:    row.CreatedAt.Time.UTC(),
		UpdatedAt:    row.UpdatedAt.Time.UTC(),
	}
}
