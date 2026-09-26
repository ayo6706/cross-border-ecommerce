package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

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

func (r *ProductRepository) WithTx(tx pgx.Tx) *ProductRepository {
	return &ProductRepository{
		queries: r.queries.WithTx(tx),
	}
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

	return toDomainProduct(row), nil
}

func (r *ProductRepository) Save(ctx context.Context, p *product.Product) error {
	if p == nil {
		return product.ErrInvalidProductState
	}

	if err := p.Validate(); err != nil {
		return fmt.Errorf("validate product: %w", err)
	}

	if strings.TrimSpace(string(p.ID)) == "" {
		return fmt.Errorf("%w: product id cannot be empty", product.ErrInvalidProductState)
	}

	idUUID, err := parseUUID(string(p.ID))
	if err != nil {
		return fmt.Errorf("%w: invalid uuid: %w", product.ErrInvalidProductState, err)
	}

	var versionUUID pgtype.UUID
	if p.CurrentVersionID != nil && strings.TrimSpace(*p.CurrentVersionID) != "" {
		parsed, err := parseUUID(*p.CurrentVersionID)
		if err != nil {
			return fmt.Errorf("%w: invalid current version uuid: %w", product.ErrInvalidProductState, err)
		}
		versionUUID = parsed
	}

	saved, err := r.queries.UpsertProduct(ctx, generated.UpsertProductParams{
		ID:                 idUUID,
		CanonicalName:      p.CanonicalName,
		Description:        p.Description,
		Brand:              p.Brand,
		OriginCountry:      p.OriginCountry,
		Status:             string(p.Status),
		CurrentVersionID:   versionUUID,
		CurrentFingerprint: p.CurrentFingerprint,
		CreatedAt:          requiredTimestamptz(p.CreatedAt),
		UpdatedAt:          requiredTimestamptz(p.UpdatedAt),
	})
	if err != nil {
		return mapPostgresError(fmt.Errorf("upsert product: %w", err))
	}

	p.ID = product.ID(uuidToString(saved.ID))
	p.CreatedAt = saved.CreatedAt.Time.UTC()
	p.UpdatedAt = saved.UpdatedAt.Time.UTC()
	return nil
}

func (r *ProductRepository) List(ctx context.Context, params product.ListParams) ([]*product.Product, error) {
	limit := listLimit(params.Limit, 50)

	if (params.LastCreatedAt != nil && params.LastID == nil) || (params.LastCreatedAt == nil && params.LastID != nil) {
		return nil, errors.New("invalid cursor: both LastCreatedAt and LastID must be specified together")
	}

	var rows []generated.Product
	var err error

	if params.LastCreatedAt != nil && params.LastID != nil {
		cursorID, parseErr := parseUUID(string(*params.LastID))
		if parseErr != nil {
			return nil, fmt.Errorf("invalid cursor id format: %w", parseErr)
		}

		rows, err = r.queries.ListProductsAfterCursor(ctx, generated.ListProductsAfterCursorParams{
			Limit:           limit,
			CursorCreatedAt: toTimestamptz(params.LastCreatedAt),
			CursorID:        cursorID,
		})
	} else {
		rows, err = r.queries.ListProductsFirstPage(ctx, limit)
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

func (r *ProductRepository) FindSnapshotsByIdentities(ctx context.Context, identities []product.IdentityRef) (map[string]*product.Snapshot, error) {
	if len(identities) == 0 {
		return make(map[string]*product.Snapshot), nil
	}

	sourceIDs := make([]string, len(identities))
	externalIDs := make([]string, len(identities))
	for i, id := range identities {
		sourceIDs[i] = strings.TrimSpace(id.SourceID)
		externalIDs[i] = strings.TrimSpace(id.ExternalProductID)
	}

	rows, err := r.queries.GetProductWithSourceByIdentities(ctx, generated.GetProductWithSourceByIdentitiesParams{
		SourceIds:          sourceIDs,
		ExternalProductIds: externalIDs,
	})
	if err != nil {
		return nil, fmt.Errorf("find snapshots by identities: %w", err)
	}

	results := make(map[string]*product.Snapshot, len(rows))
	for _, row := range rows {
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
