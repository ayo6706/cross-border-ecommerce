package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/product"
	"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/postgres/generated"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

var _ product.Repository = (*ProductRepository)(nil)

type ProductRepository struct {
	db      generated.DBTX
	queries *generated.Queries
}

func NewProductRepository(db generated.DBTX) (*ProductRepository, error) {
	if db == nil {
		return nil, errors.New("database connection cannot be nil")
	}
	return &ProductRepository{
		db:      db,
		queries: generated.New(db),
	}, nil
}

func (r *ProductRepository) WithTx(tx pgx.Tx) *ProductRepository {
	return &ProductRepository{
		db:      tx,
		queries: r.queries.WithTx(tx),
	}
}

func (r *ProductRepository) FindByID(ctx context.Context, id product.ID) (*product.Product, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	uid, err := parseUUID(string(id))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid product id: %v", product.ErrProductNotFound, err)
	}

	row, err := r.queries.GetProductByID(ctx, uid)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, product.ErrProductNotFound
		}
		return nil, fmt.Errorf("find product by id: %w", err)
	}

	return toDomainProduct(row), nil
}

func (r *ProductRepository) FindByFingerprint(ctx context.Context, fingerprint string) (*product.Product, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	trimmed := strings.TrimSpace(fingerprint)
	if trimmed == "" {
		return nil, product.ErrProductNotFound
	}

	row, err := r.queries.GetProductByFingerprint(ctx, trimmed)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, product.ErrProductNotFound
		}
		return nil, fmt.Errorf("find product by fingerprint: %w", err)
	}

	return toDomainProduct(row), nil
}

func (r *ProductRepository) Save(ctx context.Context, p *product.Product) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if p == nil {
		return product.ErrInvalidProductState
	}

	if err := p.Validate(); err != nil {
		return fmt.Errorf("validate product: %w", err)
	}

	var idUUID pgtype.UUID
	if strings.TrimSpace(string(p.ID)) == "" {
		generatedUUID, err := newUUID()
		if err != nil {
			return fmt.Errorf("generate product id: %w", err)
		}
		idUUID = generatedUUID
	} else {
		parsed, err := parseUUID(string(p.ID))
		if err != nil {
			return fmt.Errorf("%w: invalid uuid: %v", product.ErrInvalidProductState, err)
		}
		idUUID = parsed
	}

	var versionUUID pgtype.UUID
	if p.CurrentVersionID != nil && strings.TrimSpace(*p.CurrentVersionID) != "" {
		parsed, err := parseUUID(*p.CurrentVersionID)
		if err != nil {
			return fmt.Errorf("%w: invalid current version uuid: %v", product.ErrInvalidProductState, err)
		}
		versionUUID = parsed
	}

	now := time.Now().UTC()
	createdAt := p.CreatedAt
	if createdAt.IsZero() {
		createdAt = now
	}
	updatedAt := now

	saved, err := r.queries.UpsertProduct(ctx, generated.UpsertProductParams{
		ID:                 idUUID,
		CanonicalName:      p.CanonicalName,
		Description:        p.Description,
		Brand:              p.Brand,
		OriginCountry:      p.OriginCountry,
		Status:             string(p.Status),
		CurrentVersionID:   versionUUID,
		CurrentFingerprint: p.CurrentFingerprint,
		CreatedAt:          pgtype.Timestamptz{Time: createdAt, Valid: true},
		UpdatedAt:          pgtype.Timestamptz{Time: updatedAt, Valid: true},
	})
	if err != nil {
		return fmt.Errorf("upsert product: %w", err)
	}

	p.ID = product.ID(uuidToString(saved.ID))
	p.CreatedAt = saved.CreatedAt.Time.UTC()
	p.UpdatedAt = saved.UpdatedAt.Time.UTC()
	return nil
}

func (r *ProductRepository) List(ctx context.Context, params product.ListParams) ([]*product.Product, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	limit := params.Limit
	if limit <= 0 {
		limit = 50
	} else if limit > 1000 {
		limit = 1000
	}

	if (params.LastCreatedAt != nil && params.LastID == nil) || (params.LastCreatedAt == nil && params.LastID != nil) {
		return nil, errors.New("invalid cursor: both LastCreatedAt and LastID must be specified together")
	}

	var rows []generated.Product
	var err error

	if params.LastCreatedAt != nil && params.LastID != nil {
		cursorTime, parseErr := time.Parse(time.RFC3339Nano, *params.LastCreatedAt)
		if parseErr != nil {
			cursorTime, parseErr = time.Parse(time.RFC3339, *params.LastCreatedAt)
		}
		if parseErr != nil {
			return nil, fmt.Errorf("invalid cursor timestamp format: %w", parseErr)
		}

		cursorID, parseErr := parseUUID(string(*params.LastID))
		if parseErr != nil {
			return nil, fmt.Errorf("invalid cursor id format: %w", parseErr)
		}

		rows, err = r.queries.ListProductsAfterCursor(ctx, generated.ListProductsAfterCursorParams{
			Limit:           int32(limit),
			CursorCreatedAt: pgtype.Timestamptz{Time: cursorTime, Valid: true},
			CursorID:        cursorID,
		})
	} else {
		rows, err = r.queries.ListProductsFirstPage(ctx, int32(limit))
	}

	if err != nil {
		return nil, fmt.Errorf("list products: %w", err)
	}

	result := make([]*product.Product, 0, len(rows))
	for _, row := range rows {
		result = append(result, toDomainProduct(row))
	}

	return result, nil
}

func toDomainProduct(row generated.Product) *product.Product {
	var currentVersionID *string
	if row.CurrentVersionID.Valid {
		v := uuidToString(row.CurrentVersionID)
		currentVersionID = &v
	}

	return &product.Product{
		ID:                 product.ID(uuidToString(row.ID)),
		CanonicalName:      row.CanonicalName,
		Description:        row.Description,
		Brand:              row.Brand,
		OriginCountry:      row.OriginCountry,
		Status:             product.Status(row.Status),
		CurrentVersionID:   currentVersionID,
		CurrentFingerprint: row.CurrentFingerprint,
		CreatedAt:          row.CreatedAt.Time.UTC(),
		UpdatedAt:          row.UpdatedAt.Time.UTC(),
	}
}
