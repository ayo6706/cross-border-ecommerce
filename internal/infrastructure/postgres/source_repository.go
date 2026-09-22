package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/source"
	"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/postgres/generated"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

var _ source.Repository = (*SourceRepository)(nil)

type SourceRepository struct {
	db      generated.DBTX
	queries *generated.Queries
}

func NewSourceRepository(db generated.DBTX) (*SourceRepository, error) {
	if db == nil {
		return nil, errors.New("database connection cannot be nil")
	}
	return &SourceRepository{
		db:      db,
		queries: generated.New(db),
	}, nil
}

func (r *SourceRepository) WithTx(tx pgx.Tx) *SourceRepository {
	return &SourceRepository{
		db:      tx,
		queries: r.queries.WithTx(tx),
	}
}

func (r *SourceRepository) FindByID(ctx context.Context, id source.ID) (*source.Source, error) {
	trimmed := strings.TrimSpace(string(id))
	if trimmed == "" {
		return nil, source.ErrSourceNotFound
	}

	row, err := r.queries.GetSourceByID(ctx, trimmed)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, source.ErrSourceNotFound
		}
		return nil, fmt.Errorf("find source by id: %w", err)
	}

	return toDomainSource(row)
}

func (r *SourceRepository) FindActive(ctx context.Context) ([]*source.Source, error) {
	rows, err := r.queries.ListActiveSources(ctx)
	if err != nil {
		return nil, fmt.Errorf("list active sources: %w", err)
	}

	result := make([]*source.Source, 0, len(rows))
	for _, row := range rows {
		src, err := toDomainSource(row)
		if err != nil {
			return nil, fmt.Errorf("convert source model: %w", err)
		}
		result = append(result, src)
	}

	return result, nil
}

func (r *SourceRepository) List(ctx context.Context) ([]*source.Source, error) {
	rows, err := r.queries.ListSources(ctx)
	if err != nil {
		return nil, fmt.Errorf("list sources: %w", err)
	}

	result := make([]*source.Source, 0, len(rows))
	for _, row := range rows {
		src, err := toDomainSource(row)
		if err != nil {
			return nil, fmt.Errorf("convert source model: %w", err)
		}
		result = append(result, src)
	}

	return result, nil
}

func (r *SourceRepository) Delete(ctx context.Context, id source.ID) error {
	trimmed := strings.TrimSpace(string(id))
	if trimmed == "" {
		return source.ErrSourceNotFound
	}

	if err := r.queries.DeleteSource(ctx, trimmed); err != nil {
		return fmt.Errorf("delete source: %w", err)
	}

	return nil
}

func (r *SourceRepository) Save(ctx context.Context, s *source.Source) error {
	if s == nil || strings.TrimSpace(string(s.ID)) == "" {
		return source.ErrInvalidSourceState
	}

	configBytes := []byte("{}")
	if len(s.Config) > 0 {
		b, err := json.Marshal(s.Config)
		if err != nil {
			return fmt.Errorf("%w: marshal config: %w", source.ErrInvalidSourceState, err)
		}
		configBytes = b
	}

	now := time.Now().UTC()
	createdAt := s.CreatedAt
	if createdAt.IsZero() {
		createdAt = now
	}
	updatedAt := now

	saved, err := r.queries.UpsertSource(ctx, generated.UpsertSourceParams{
		ID:        string(s.ID),
		Name:      s.Name,
		Type:      string(s.Type),
		Config:    configBytes,
		RateLimit: int32(s.RateLimit),
		Enabled:   s.Enabled,
		CreatedAt: pgtype.Timestamptz{Time: createdAt, Valid: true},
		UpdatedAt: pgtype.Timestamptz{Time: updatedAt, Valid: true},
	})
	if err != nil {
		return fmt.Errorf("upsert source: %w", err)
	}

	s.CreatedAt = saved.CreatedAt.Time.UTC()
	s.UpdatedAt = saved.UpdatedAt.Time.UTC()
	return nil
}

func toDomainSource(row generated.Source) (*source.Source, error) {
	config := make(map[string]any)
	if len(row.Config) > 0 {
		if err := json.Unmarshal(row.Config, &config); err != nil {
			return nil, fmt.Errorf("unmarshal source config: %w", err)
		}
	}

	return &source.Source{
		ID:        source.ID(row.ID),
		Name:      row.Name,
		Type:      source.Type(row.Type),
		Config:    config,
		RateLimit: int(row.RateLimit),
		Enabled:   row.Enabled,
		CreatedAt: row.CreatedAt.Time.UTC(),
		UpdatedAt: row.UpdatedAt.Time.UTC(),
	}, nil
}
