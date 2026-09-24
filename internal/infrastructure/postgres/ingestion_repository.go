package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/source"
	"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/postgres/generated"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var _ ingestion.Repository = (*IngestionRepository)(nil)

type IngestionRepository struct {
	queries *generated.Queries
}

func NewIngestionRepository(db generated.DBTX) (*IngestionRepository, error) {
	if db == nil {
		return nil, errors.New("database connection cannot be nil")
	}
	return &IngestionRepository{
		queries: generated.New(db),
	}, nil
}

func (r *IngestionRepository) WithTx(tx pgx.Tx) *IngestionRepository {
	return &IngestionRepository{
		queries: r.queries.WithTx(tx),
	}
}

func (r *IngestionRepository) CreateRun(ctx context.Context, run *ingestion.IngestionRun) error {
	if run == nil {
		return ingestion.ErrInvalidRunState
	}

	uuidVal, err := parseUUID(run.ID)
	if err != nil {
		return fmt.Errorf("%w: %w", ingestion.ErrInvalidRunID, err)
	}

	counters, err := toRunCounters(run.RecordsSeen, run.RecordsNew, run.RecordsChanged, run.RecordsUnchanged, run.RecordsFailed)
	if err != nil {
		return fmt.Errorf("%w: %w", ingestion.ErrInvalidRunState, err)
	}

	row, err := r.queries.CreateIngestionRun(ctx, generated.CreateIngestionRunParams{
		ID:               uuidVal,
		SourceID:         string(run.SourceID),
		Status:           string(run.Status),
		Checkpoint:       run.Checkpoint,
		RecordsSeen:      counters.seen,
		RecordsNew:       counters.newRecords,
		RecordsChanged:   counters.changed,
		RecordsUnchanged: counters.unchanged,
		RecordsFailed:    counters.failed,
		ErrorSummary:     run.ErrorSummary,
		StartedAt:        toTimestamptz(run.StartedAt),
		CompletedAt:      toTimestamptz(run.CompletedAt),
		CreatedAt:        requiredTimestamptz(run.CreatedAt),
		UpdatedAt:        requiredTimestamptz(run.UpdatedAt),
	})
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return fmt.Errorf("%w: %w", ingestion.ErrRunAlreadyActive, err)
		}
		return fmt.Errorf("create ingestion run: %w", err)
	}

	run.ID = uuidToString(row.ID)
	run.CreatedAt = row.CreatedAt.Time.UTC()
	run.UpdatedAt = row.UpdatedAt.Time.UTC()
	return nil
}

func (r *IngestionRepository) FindRunByID(ctx context.Context, id string) (*ingestion.IngestionRun, error) {
	uuidVal, err := parseUUID(id)
	if err != nil {
		return nil, ingestion.ErrRunNotFound
	}

	row, err := r.queries.GetIngestionRunByID(ctx, uuidVal)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ingestion.ErrRunNotFound
		}
		return nil, fmt.Errorf("get ingestion run by id: %w", err)
	}

	return toDomainIngestionRun(row), nil
}

func (r *IngestionRepository) UpdateProgress(ctx context.Context, id string, metrics ingestion.BatchMetrics, checkpoint string, updatedAt time.Time) error {
	uuidVal, err := parseUUID(id)
	if err != nil {
		return ingestion.ErrRunNotFound
	}

	if updatedAt.IsZero() {
		updatedAt = time.Now().UTC()
	}

	counters, err := toRunCounters(metrics.Seen, metrics.New, metrics.Changed, metrics.Unchanged, metrics.Failed)
	if err != nil {
		return fmt.Errorf("convert run progress counters: %w", err)
	}

	_, err = r.queries.UpdateIngestionRunProgress(ctx, generated.UpdateIngestionRunProgressParams{
		SeenIncrement:      counters.seen,
		NewIncrement:       counters.newRecords,
		ChangedIncrement:   counters.changed,
		UnchangedIncrement: counters.unchanged,
		FailedIncrement:    counters.failed,
		Checkpoint:         checkpoint,
		UpdatedAt:          requiredTimestamptz(updatedAt),
		ID:                 uuidVal,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			if _, getErr := r.queries.GetIngestionRunByID(ctx, uuidVal); getErr != nil {
				if errors.Is(getErr, pgx.ErrNoRows) {
					return ingestion.ErrRunNotFound
				}
			}
			return ingestion.ErrInvalidRunState
		}
		return fmt.Errorf("update ingestion run progress: %w", err)
	}

	return nil
}

func (r *IngestionRepository) UpdateStatus(ctx context.Context, run *ingestion.IngestionRun, from ingestion.RunStatus) error {
	if run == nil {
		return ingestion.ErrInvalidRunState
	}
	uuidVal, err := parseUUID(run.ID)
	if err != nil {
		return ingestion.ErrRunNotFound
	}

	_, err = r.queries.UpdateIngestionRunStatus(ctx, generated.UpdateIngestionRunStatusParams{
		Status:         string(run.Status),
		ErrorSummary:   run.ErrorSummary,
		Checkpoint:     run.Checkpoint,
		CompletedAt:    toTimestamptz(run.CompletedAt),
		UpdatedAt:      requiredTimestamptz(run.UpdatedAt),
		ID:             uuidVal,
		ExpectedStatus: string(from),
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			if _, getErr := r.queries.GetIngestionRunByID(ctx, uuidVal); getErr != nil {
				if errors.Is(getErr, pgx.ErrNoRows) {
					return ingestion.ErrRunNotFound
				}
			}
			return ingestion.ErrInvalidTransition
		}
		return fmt.Errorf("update ingestion run status: %w", err)
	}

	return nil
}

func (r *IngestionRepository) ListRunsBySource(ctx context.Context, sourceID source.ID, limit int) ([]*ingestion.IngestionRun, error) {
	if strings.TrimSpace(string(sourceID)) == "" {
		return nil, ingestion.ErrInvalidSourceID
	}
	rows, err := r.queries.ListIngestionRunsBySource(ctx, generated.ListIngestionRunsBySourceParams{
		SourceID: string(sourceID),
		Limit:    listLimit(limit, 20),
	})
	if err != nil {
		return nil, fmt.Errorf("list ingestion runs by source: %w", err)
	}

	result := make([]*ingestion.IngestionRun, 0, len(rows))
	for _, row := range rows {
		result = append(result, toDomainIngestionRun(row))
	}

	return result, nil
}

func (r *IngestionRepository) FindLatestRunBySource(ctx context.Context, sourceID source.ID) (*ingestion.IngestionRun, error) {
	if strings.TrimSpace(string(sourceID)) == "" {
		return nil, ingestion.ErrInvalidSourceID
	}

	row, err := r.queries.GetLatestIngestionRunBySource(ctx, string(sourceID))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ingestion.ErrRunNotFound
		}
		return nil, fmt.Errorf("get latest ingestion run: %w", err)
	}

	return toDomainIngestionRun(row), nil
}

func toDomainIngestionRun(row generated.IngestionRun) *ingestion.IngestionRun {
	return &ingestion.IngestionRun{
		ID:               uuidToString(row.ID),
		SourceID:         source.ID(row.SourceID),
		Status:           ingestion.RunStatus(row.Status),
		Checkpoint:       row.Checkpoint,
		RecordsSeen:      int(row.RecordsSeen),
		RecordsNew:       int(row.RecordsNew),
		RecordsChanged:   int(row.RecordsChanged),
		RecordsUnchanged: int(row.RecordsUnchanged),
		RecordsFailed:    int(row.RecordsFailed),
		ErrorSummary:     row.ErrorSummary,
		StartedAt:        fromTimestamptz(row.StartedAt),
		CompletedAt:      fromTimestamptz(row.CompletedAt),
		CreatedAt:        row.CreatedAt.Time.UTC(),
		UpdatedAt:        row.UpdatedAt.Time.UTC(),
	}
}
