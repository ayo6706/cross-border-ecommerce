package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/product"
	"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/postgres/generated"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

var _ product.Repository = (*ProductRepository)(nil)

type ProductRepository struct {
	queries *generated.Queries
}

func NewProductRepository(db generated.DBTX) (*ProductRepository, error) {
	if db == nil {
		return nil, errors.New("database connection cannot be nil")
	}
	return &ProductRepository{
		queries: generated.New(db),
	}, nil
}

func (r *ProductRepository) FindByID(ctx context.Context, id product.ID) (*product.Product, error) {
	uid, err := parseUUID(string(id))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid product id: %w", product.ErrProductNotFound, err)
	}

	row, err := r.queries.GetProductByID(ctx, uid)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, product.ErrProductNotFound
		}
		return nil, fmt.Errorf("find product by id: %w", err)
	}

	return toDomainProduct(&row), nil
}

// List reads one row past the page so Next is set only when another page exists.
func (r *ProductRepository) List(ctx context.Context, params product.ListParams) (product.Page, error) {
	if err := params.Validate(); err != nil {
		return product.Page{}, err
	}
	rows, err := r.listRows(ctx, params)
	if err != nil {
		return product.Page{}, err
	}

	var page product.Page
	if len(rows) > params.Limit {
		rows = rows[:params.Limit]
		last := rows[len(rows)-1]
		page.Next = &product.Cursor{
			CreatedAt: last.CreatedAt.Time.UTC(),
			ID:        product.ID(uuidToString(last.ID)),
		}
	}
	page.Items = make([]*product.Product, 0, len(rows))
	for i := range rows {
		page.Items = append(page.Items, toDomainProduct(&rows[i]))
	}
	return page, nil
}

func (r *ProductRepository) listRows(ctx context.Context, params product.ListParams) ([]generated.Product, error) {
	limit, err := toInt32(params.Limit + 1)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", product.ErrInvalidListParams, err)
	}
	if params.After == nil {
		rows, err := r.queries.ListProductsFirstPage(ctx, limit)
		if err != nil {
			return nil, fmt.Errorf("list first product page: %w", err)
		}
		return rows, nil
	}

	cursorID, err := parseUUID(string(params.After.ID))
	if err != nil {
		return nil, fmt.Errorf("%w: cursor id: %w", product.ErrInvalidListParams, err)
	}
	rows, err := r.queries.ListProductsAfterCursor(ctx, generated.ListProductsAfterCursorParams{
		Limit:           limit,
		CursorCreatedAt: pgtype.Timestamptz{Time: params.After.CreatedAt.UTC(), Valid: true},
		CursorID:        cursorID,
	})
	if err != nil {
		return nil, fmt.Errorf("list products after cursor: %w", err)
	}
	return rows, nil
}

//nolint:funlen // legacy baseline 2026-09-26: fix in ENG-044
func (r *ProductRepository) FindSnapshotsByIdentities(ctx context.Context, identities []product.IdentityRef) (map[string]*product.Snapshot, error) {
	if len(identities) == 0 {
		return make(map[string]*product.Snapshot), nil
	}

	sourceIDs := make([]string, len(identities))
	externalIDs := make([]string, len(identities))
	for i, id := range identities {
		sourceIDs[i] = id.SourceID
		externalIDs[i] = id.ExternalProductID
	}

	rows, err := r.queries.GetProductWithSourceByIdentities(ctx, generated.GetProductWithSourceByIdentitiesParams{
		SourceIds:          sourceIDs,
		ExternalProductIds: externalIDs,
	})
	if err != nil {
		return nil, fmt.Errorf("find snapshots by identities: %w", err)
	}

	results := make(map[string]*product.Snapshot, len(rows))
	for i := range rows {
		row := &rows[i]
		var currentVersionID *string
		if row.CurrentVersionID.Valid {
			v := uuidToString(row.CurrentVersionID)
			currentVersionID = &v
		}

		var storedVersion *product.ProductVersion
		if row.VersionID.Valid {
			var attrs map[string]string
			if len(row.VersionAttributes) > 0 {
				if err := json.Unmarshal(row.VersionAttributes, &attrs); err != nil {
					return nil, fmt.Errorf("unmarshal version attributes for version %s: %w", uuidToString(row.VersionID), err)
				}
			}
			var runID *string
			if row.VersionIngestionRunID.Valid {
				rID := uuidToString(row.VersionIngestionRunID)
				runID = &rID
			}

			storedVersion = &product.ProductVersion{
				ID:             uuidToString(row.VersionID),
				ProductID:      product.ID(uuidToString(row.ProductID)),
				VersionNumber:  int(row.VersionNumber.Int32),
				Fingerprint:    row.VersionFingerprint.String,
				CanonicalName:  row.VersionCanonicalName.String,
				Description:    row.VersionDescription.String,
				Brand:          row.VersionBrand.String,
				OriginCountry:  row.VersionOriginCountry.String,
				Attributes:     attrs,
				IngestionRunID: runID,
				CreatedAt:      row.VersionCreatedAt.Time.UTC(),
			}
		}

		snap := &product.Snapshot{
			ProductSourceID:      uuidToString(row.ProductSourceID),
			ProductID:            product.ID(uuidToString(row.ProductID)),
			CurrentVersionID:     currentVersionID,
			CurrentFingerprint:   row.CurrentFingerprint,
			LastSourceUpdatedAt:  fromTimestamptz(row.LastSourceUpdatedAt),
			LastReceivedAt:       row.LastReceivedAt.Time.UTC(),
			StoredCurrentVersion: storedVersion,
		}
		key := product.IdentityKey(row.SourceID, row.ExternalProductID)
		results[key] = snap
	}

	return results, nil
}

func mapPostgresError(err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if pgErr.Code == "23505" {
			if pgErr.ConstraintName == "uq_product_sources_source_external" {
				return fmt.Errorf("%w: %w", product.ErrIdentityConflict, err)
			}
			if pgErr.ConstraintName == "uq_product_versions_product_version" {
				return fmt.Errorf("%w: %w", product.ErrVersionConflict, err)
			}
		}
		if pgErr.Code == "40P01" || pgErr.Code == "40001" {
			return fmt.Errorf("%w: %w", product.ErrDeadlockConflict, err)
		}
	}
	return err
}

func toDomainProduct(row *generated.Product) *product.Product {
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
