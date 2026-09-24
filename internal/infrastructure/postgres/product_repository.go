package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

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

func (r *ProductRepository) FindSnapshotByIdentity(ctx context.Context, sourceID string, externalProductID string) (*product.Snapshot, error) {
	row, err := r.queries.GetProductWithSourceByIdentity(ctx, generated.GetProductWithSourceByIdentityParams{
		SourceID:          strings.TrimSpace(sourceID),
		ExternalProductID: strings.TrimSpace(externalProductID),
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil // Nil snapshot indicates new identity
		}
		return nil, fmt.Errorf("find snapshot by identity: %w", err)
	}

	var currentVersionID *string
	if row.CurrentVersionID.Valid {
		v := uuidToString(row.CurrentVersionID)
		currentVersionID = &v
	}

	var storedVersion *product.ProductVersion
	if row.VersionID.Valid {
		var attrs map[string]string
		if len(row.VersionAttributes) > 0 {
			_ = json.Unmarshal(row.VersionAttributes, &attrs)
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

	return &product.Snapshot{
		ProductSourceID:      uuidToString(row.ProductSourceID),
		ProductID:            product.ID(uuidToString(row.ProductID)),
		CurrentVersionID:     currentVersionID,
		CurrentFingerprint:   row.CurrentFingerprint,
		LastSourceUpdatedAt:  fromTimestamptz(row.LastSourceUpdatedAt),
		LastReceivedAt:       row.LastReceivedAt.Time.UTC(),
		StoredCurrentVersion: storedVersion,
	}, nil
}

func (r *ProductRepository) CreateProductWithSource(ctx context.Context, p *product.Product, ps *product.ProductSource) error {
	if p == nil || ps == nil {
		return product.ErrInvalidProductState
	}

	prodUUID, err := parseUUID(string(p.ID))
	if err != nil {
		return fmt.Errorf("%w: invalid product id: %w", product.ErrInvalidProductState, err)
	}

	var versionUUID pgtype.UUID
	if p.CurrentVersionID != nil && strings.TrimSpace(*p.CurrentVersionID) != "" {
		versionUUID, err = parseUUID(*p.CurrentVersionID)
		if err != nil {
			return fmt.Errorf("%w: invalid current version id: %w", product.ErrInvalidProductState, err)
		}
	}

	psUUID, err := parseUUID(ps.ID)
	if err != nil {
		return fmt.Errorf("%w: invalid product source id: %w", product.ErrInvalidProductState, err)
	}

	// 1. Insert product
	savedProd, err := r.queries.UpsertProduct(ctx, generated.UpsertProductParams{
		ID:                 prodUUID,
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
		return mapPostgresError(fmt.Errorf("insert product: %w", err))
	}
	p.CreatedAt = savedProd.CreatedAt.Time.UTC()
	p.UpdatedAt = savedProd.UpdatedAt.Time.UTC()

	// 2. Insert product source
	savedSource, err := r.queries.CreateProductSource(ctx, generated.CreateProductSourceParams{
		ID:                  psUUID,
		ProductID:           prodUUID,
		SourceID:            string(ps.SourceID),
		ExternalProductID:   ps.ExternalProductID,
		FirstSeenAt:         requiredTimestamptz(ps.FirstSeenAt),
		LastChangedAt:       requiredTimestamptz(ps.LastChangedAt),
		LastSourceUpdatedAt: toTimestamptz(ps.LastSourceUpdatedAt),
		LastReceivedAt:      requiredTimestamptz(ps.LastReceivedAt),
	})
	if err != nil {
		return mapPostgresError(fmt.Errorf("create product source: %w", err))
	}

	ps.ID = uuidToString(savedSource.ID)
	ps.FirstSeenAt = savedSource.FirstSeenAt.Time.UTC()
	ps.LastChangedAt = savedSource.LastChangedAt.Time.UTC()
	ps.LastReceivedAt = savedSource.LastReceivedAt.Time.UTC()
	return nil
}

func (r *ProductRepository) CreateVersion(ctx context.Context, pv *product.ProductVersion) error {
	if pv == nil {
		return product.ErrInvalidProductState
	}

	vUUID, err := parseUUID(pv.ID)
	if err != nil {
		return fmt.Errorf("%w: invalid version id: %w", product.ErrInvalidProductState, err)
	}
	pUUID, err := parseUUID(string(pv.ProductID))
	if err != nil {
		return fmt.Errorf("%w: invalid product id: %w", product.ErrInvalidProductState, err)
	}

	var runUUID pgtype.UUID
	if pv.IngestionRunID != nil && strings.TrimSpace(*pv.IngestionRunID) != "" {
		runUUID, err = parseUUID(*pv.IngestionRunID)
		if err != nil {
			return fmt.Errorf("%w: invalid ingestion run id: %w", product.ErrInvalidProductState, err)
		}
	}

	attrBytes, err := json.Marshal(pv.Attributes)
	if err != nil {
		return fmt.Errorf("marshal version attributes: %w", err)
	}

	saved, err := r.queries.CreateProductVersionWithRun(ctx, generated.CreateProductVersionWithRunParams{
		ID:             vUUID,
		ProductID:      pUUID,
		VersionNumber:  int32(pv.VersionNumber),
		Fingerprint:    pv.Fingerprint,
		CanonicalName:  pv.CanonicalName,
		Description:    pv.Description,
		Brand:          pv.Brand,
		OriginCountry:  pv.OriginCountry,
		Attributes:     attrBytes,
		IngestionRunID: runUUID,
		CreatedAt:      requiredTimestamptz(pv.CreatedAt),
	})
	if err != nil {
		return mapPostgresError(fmt.Errorf("create product version: %w", err))
	}

	pv.ID = uuidToString(saved.ID)
	pv.CreatedAt = saved.CreatedAt.Time.UTC()
	return nil
}

func (r *ProductRepository) GuardedUpdateVersion(ctx context.Context, p *product.Product, expectedVersionID *string) error {
	if p == nil || p.CurrentVersionID == nil {
		return product.ErrInvalidProductState
	}

	pUUID, err := parseUUID(string(p.ID))
	if err != nil {
		return fmt.Errorf("%w: invalid product id: %w", product.ErrInvalidProductState, err)
	}
	toVersionUUID, err := parseUUID(*p.CurrentVersionID)
	if err != nil {
		return fmt.Errorf("%w: invalid to_version_id: %w", product.ErrInvalidProductState, err)
	}

	var expUUID pgtype.UUID
	if expectedVersionID != nil && strings.TrimSpace(*expectedVersionID) != "" {
		expUUID, err = parseUUID(*expectedVersionID)
		if err != nil {
			return fmt.Errorf("%w: invalid expected_version_id: %w", product.ErrInvalidProductState, err)
		}
	}

	saved, err := r.queries.GuardedUpdateProductVersion(ctx, generated.GuardedUpdateProductVersionParams{
		ID:                 pUUID,
		ToVersionID:        toVersionUUID,
		CurrentFingerprint: p.CurrentFingerprint,
		CanonicalName:      p.CanonicalName,
		Description:        p.Description,
		Brand:              p.Brand,
		OriginCountry:      p.OriginCountry,
		UpdatedAt:          requiredTimestamptz(p.UpdatedAt),
		ExpectedVersionID:  expUUID,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return product.ErrVersionConflict
		}
		return mapPostgresError(fmt.Errorf("guarded update product version: %w", err))
	}

	p.UpdatedAt = saved.UpdatedAt.Time.UTC()
	return nil
}

func (r *ProductRepository) GuardedUpdateFingerprintOnly(
	ctx context.Context,
	productID product.ID,
	expectedVersionID *string,
	newFingerprint string,
	updatedAt time.Time,
) error {
	pUUID, err := parseUUID(string(productID))
	if err != nil {
		return fmt.Errorf("%w: invalid product id: %w", product.ErrInvalidProductState, err)
	}

	var expUUID pgtype.UUID
	if expectedVersionID != nil && strings.TrimSpace(*expectedVersionID) != "" {
		expUUID, err = parseUUID(*expectedVersionID)
		if err != nil {
			return fmt.Errorf("%w: invalid expected_version_id: %w", product.ErrInvalidProductState, err)
		}
	}

	_, err = r.queries.GuardedUpdateProductFingerprintOnly(ctx, generated.GuardedUpdateProductFingerprintOnlyParams{
		ID:                 pUUID,
		CurrentFingerprint: strings.TrimSpace(newFingerprint),
		UpdatedAt:          requiredTimestamptz(updatedAt),
		ExpectedVersionID:  expUUID,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return product.ErrVersionConflict
		}
		return mapPostgresError(fmt.Errorf("guarded update product fingerprint only: %w", err))
	}
	return nil
}

func (r *ProductRepository) CreateChange(ctx context.Context, pc *product.ProductChange) error {
	if pc == nil {
		return product.ErrInvalidProductState
	}

	cUUID, err := parseUUID(pc.ID)
	if err != nil {
		return fmt.Errorf("%w: invalid change id: %w", product.ErrInvalidProductState, err)
	}
	pUUID, err := parseUUID(string(pc.ProductID))
	if err != nil {
		return fmt.Errorf("%w: invalid product id: %w", product.ErrInvalidProductState, err)
	}
	toVUUID, err := parseUUID(pc.ToVersionID)
	if err != nil {
		return fmt.Errorf("%w: invalid to_version_id: %w", product.ErrInvalidProductState, err)
	}

	var fromVUUID pgtype.UUID
	if pc.FromVersionID != nil && strings.TrimSpace(*pc.FromVersionID) != "" {
		fromVUUID, err = parseUUID(*pc.FromVersionID)
		if err != nil {
			return fmt.Errorf("%w: invalid from_version_id: %w", product.ErrInvalidProductState, err)
		}
	}

	var runUUID pgtype.UUID
	if pc.IngestionRunID != nil && strings.TrimSpace(*pc.IngestionRunID) != "" {
		runUUID, err = parseUUID(*pc.IngestionRunID)
		if err != nil {
			return fmt.Errorf("%w: invalid ingestion run id: %w", product.ErrInvalidProductState, err)
		}
	}

	var rawUUID pgtype.UUID
	if pc.RawRecordID != nil && strings.TrimSpace(*pc.RawRecordID) != "" {
		rawUUID, err = parseUUID(*pc.RawRecordID)
		if err != nil {
			return fmt.Errorf("%w: invalid raw record id: %w", product.ErrInvalidProductState, err)
		}
	}

	fieldBytes, err := json.Marshal(pc.ChangedFields)
	if err != nil {
		return fmt.Errorf("marshal changed fields: %w", err)
	}

	saved, err := r.queries.CreateProductChange(ctx, generated.CreateProductChangeParams{
		ID:             cUUID,
		ProductID:      pUUID,
		FromVersionID:  fromVUUID,
		ToVersionID:    toVUUID,
		ChangeType:     string(pc.ChangeType),
		ChangedFields:  fieldBytes,
		IngestionRunID: runUUID,
		RawRecordID:    rawUUID,
		DetectedAt:     requiredTimestamptz(pc.DetectedAt),
	})
	if err != nil {
		return mapPostgresError(fmt.Errorf("create product change: %w", err))
	}

	pc.ID = uuidToString(saved.ID)
	pc.DetectedAt = saved.DetectedAt.Time.UTC()
	return nil
}

func (r *ProductRepository) UpdateSourceWatermark(
	ctx context.Context,
	psID string,
	sourceUpdatedAt *time.Time,
	receivedAt time.Time,
) error {
	psUUID, err := parseUUID(psID)
	if err != nil {
		return fmt.Errorf("%w: invalid product source id: %w", product.ErrInvalidProductState, err)
	}

	_, err = r.queries.UpdateProductSourceWatermark(ctx, generated.UpdateProductSourceWatermarkParams{
		ID:              psUUID,
		SourceUpdatedAt: toTimestamptz(sourceUpdatedAt),
		ReceivedAt:      requiredTimestamptz(receivedAt),
	})
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return mapPostgresError(fmt.Errorf("update product source watermark: %w", err))
	}
	return nil
}

func (r *ProductRepository) UpdateSourceOnChanged(
	ctx context.Context,
	psID string,
	lastChangedAt time.Time,
	sourceUpdatedAt *time.Time,
	receivedAt time.Time,
) error {
	psUUID, err := parseUUID(psID)
	if err != nil {
		return fmt.Errorf("%w: invalid product source id: %w", product.ErrInvalidProductState, err)
	}

	_, err = r.queries.UpdateProductSourceOnChanged(ctx, generated.UpdateProductSourceOnChangedParams{
		ID:              psUUID,
		LastChangedAt:   requiredTimestamptz(lastChangedAt),
		SourceUpdatedAt: toTimestamptz(sourceUpdatedAt),
		ReceivedAt:      requiredTimestamptz(receivedAt),
	})
	if err != nil {
		return mapPostgresError(fmt.Errorf("update product source on changed: %w", err))
	}
	return nil
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
