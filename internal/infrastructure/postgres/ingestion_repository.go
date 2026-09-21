package postgres

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/source"
	"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/postgres/generated"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

var _ ingestion.Repository = (*IngestionRepository)(nil)

type IngestionRepository struct {
	db      generated.DBTX
	queries *generated.Queries
}

func NewIngestionRepository(db generated.DBTX) (*IngestionRepository, error) {
	if db == nil {
		return nil, errors.New("database connection cannot be nil")
	}
	return &IngestionRepository{
		db:      db,
		queries: generated.New(db),
	}, nil
}

func (r *IngestionRepository) WithTx(tx pgx.Tx) *IngestionRepository {
	return &IngestionRepository{
		db:      tx,
		queries: r.queries.WithTx(tx),
	}
}

func (r *IngestionRepository) CreateRun(ctx context.Context, run *ingestion.IngestionRun) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if run == nil {
		return ingestion.ErrInvalidRunState
	}

	var uuidVal pgtype.UUID
	var err error
	if strings.TrimSpace(run.ID) == "" {
		uuidVal, err = newUUID()
		if err != nil {
			return fmt.Errorf("generate run uuid: %w", err)
		}
		run.ID = uuidToString(uuidVal)
	} else {
		uuidVal, err = parseUUID(run.ID)
		if err != nil {
			return fmt.Errorf("%w: %v", ingestion.ErrInvalidRunID, err)
		}
	}

	now := time.Now().UTC()
	createdAt := run.CreatedAt
	if createdAt.IsZero() {
		createdAt = now
	}
	updatedAt := run.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = now
	}

	var startedAt pgtype.Timestamptz
	if run.StartedAt != nil && !run.StartedAt.IsZero() {
		startedAt = pgtype.Timestamptz{Time: run.StartedAt.UTC(), Valid: true}
	}

	var completedAt pgtype.Timestamptz
	if run.CompletedAt != nil && !run.CompletedAt.IsZero() {
		completedAt = pgtype.Timestamptz{Time: run.CompletedAt.UTC(), Valid: true}
	}

	row, err := r.queries.CreateIngestionRun(ctx, generated.CreateIngestionRunParams{
		ID:               uuidVal,
		SourceID:         string(run.SourceID),
		Status:           string(run.Status),
		Checkpoint:       run.Checkpoint,
		RecordsSeen:      safeInt32(run.RecordsSeen),
		RecordsNew:       safeInt32(run.RecordsNew),
		RecordsChanged:   safeInt32(run.RecordsChanged),
		RecordsUnchanged: safeInt32(run.RecordsUnchanged),
		RecordsFailed:    safeInt32(run.RecordsFailed),
		ErrorSummary:     run.ErrorSummary,
		StartedAt:        startedAt,
		CompletedAt:      completedAt,
		CreatedAt:        pgtype.Timestamptz{Time: createdAt, Valid: true},
		UpdatedAt:        pgtype.Timestamptz{Time: updatedAt, Valid: true},
	})
	if err != nil {
		return fmt.Errorf("create ingestion run: %w", err)
	}

	run.ID = uuidToString(row.ID)
	run.CreatedAt = row.CreatedAt.Time.UTC()
	run.UpdatedAt = row.UpdatedAt.Time.UTC()
	return nil
}

func (r *IngestionRepository) FindRunByID(ctx context.Context, id string) (*ingestion.IngestionRun, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

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
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	uuidVal, err := parseUUID(id)
	if err != nil {
		return ingestion.ErrRunNotFound
	}

	if updatedAt.IsZero() {
		updatedAt = time.Now().UTC()
	}

	_, err = r.queries.UpdateIngestionRunProgress(ctx, generated.UpdateIngestionRunProgressParams{
		SeenIncrement:      safeInt32(metrics.Seen),
		NewIncrement:       safeInt32(metrics.New),
		ChangedIncrement:   safeInt32(metrics.Changed),
		UnchangedIncrement: safeInt32(metrics.Unchanged),
		FailedIncrement:    safeInt32(metrics.Failed),
		Checkpoint:         checkpoint,
		UpdatedAt:          pgtype.Timestamptz{Time: updatedAt.UTC(), Valid: true},
		ID:                 uuidVal,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ingestion.ErrRunNotFound
		}
		return fmt.Errorf("update ingestion run progress: %w", err)
	}

	return nil
}

func (r *IngestionRepository) UpdateStatus(ctx context.Context, id string, status ingestion.RunStatus, errorSummary string, checkpoint string, completedAt time.Time, updatedAt time.Time) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	uuidVal, err := parseUUID(id)
	if err != nil {
		return ingestion.ErrRunNotFound
	}

	if updatedAt.IsZero() {
		updatedAt = time.Now().UTC()
	}

	var compAt pgtype.Timestamptz
	if !completedAt.IsZero() {
		compAt = pgtype.Timestamptz{Time: completedAt.UTC(), Valid: true}
	}

	_, err = r.queries.UpdateIngestionRunStatus(ctx, generated.UpdateIngestionRunStatusParams{
		Status:       string(status),
		ErrorSummary: errorSummary,
		Checkpoint:   checkpoint,
		CompletedAt:  compAt,
		UpdatedAt:    pgtype.Timestamptz{Time: updatedAt.UTC(), Valid: true},
		ID:           uuidVal,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ingestion.ErrRunNotFound
		}
		return fmt.Errorf("update ingestion run status: %w", err)
	}

	return nil
}

func (r *IngestionRepository) ListRunsBySource(ctx context.Context, sourceID source.ID, limit int) ([]*ingestion.IngestionRun, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if strings.TrimSpace(string(sourceID)) == "" {
		return nil, ingestion.ErrInvalidSourceID
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 1000 {
		limit = 1000
	}

	rows, err := r.queries.ListIngestionRunsBySource(ctx, generated.ListIngestionRunsBySourceParams{
		SourceID: string(sourceID),
		Limit:    int32(limit),
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
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

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
	var startedAt *time.Time
	if row.StartedAt.Valid {
		t := row.StartedAt.Time.UTC()
		startedAt = &t
	}

	var completedAt *time.Time
	if row.CompletedAt.Valid {
		t := row.CompletedAt.Time.UTC()
		completedAt = &t
	}

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
		StartedAt:        startedAt,
		CompletedAt:      completedAt,
		CreatedAt:        row.CreatedAt.Time.UTC(),
		UpdatedAt:        row.UpdatedAt.Time.UTC(),
	}
}

func safeInt32(n int) int32 {
	if n > math.MaxInt32 {
		return math.MaxInt32
	}
	if n < math.MinInt32 {
		return math.MinInt32
	}
	return int32(n)
}
