package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/product"
	"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/postgres/generated"
	"github.com/jackc/pgx/v5/pgtype"
)

func (r *ProductRepository) ApplyBatch(ctx context.Context, plan *product.BatchPlan) error {
	if plan == nil {
		return fmt.Errorf("%w: nil batch plan", product.ErrInvalidProductState)
	}
	if err := copyRows(ctx, "products", plan.ProductsToInsert, copyProductParams, r.queries.CopyProducts); err != nil {
		return err
	}
	if err := copyRows(ctx, "product sources", plan.ProductSourcesToInsert, copyProductSourceParams, r.queries.CopyProductSources); err != nil {
		return err
	}
	if err := copyRows(ctx, "product versions", plan.ProductVersionsToInsert, copyProductVersionParams, r.queries.CopyProductVersions); err != nil {
		return err
	}
	if err := r.updateProductVersions(ctx, plan.ProductsToUpdate); err != nil {
		return err
	}
	if err := r.updateProductFingerprints(ctx, plan.ProductsToUpdateFingerprint); err != nil {
		return err
	}
	if err := r.updateSourcesOnChanged(ctx, plan.SourcesToUpdateChanged); err != nil {
		return err
	}
	if err := r.updateSourceWatermarks(ctx, plan.SourcesToUpdateWatermark); err != nil {
		return err
	}
	return copyRows(ctx, "product changes", plan.ProductChangesToInsert, copyProductChangeParams, r.queries.CopyProductChanges)
}

func copyRows[T, P any](
	ctx context.Context,
	table string,
	rows []T,
	toParams func(T) (P, error),
	copyFn func(context.Context, []P) (int64, error),
) error {
	if len(rows) == 0 {
		return nil
	}
	params := make([]P, len(rows))
	for i, row := range rows {
		p, err := toParams(row)
		if err != nil {
			return err
		}
		params[i] = p
	}
	if _, err := copyFn(ctx, params); err != nil {
		return mapPostgresError(fmt.Errorf("copy %s: %w", table, err))
	}
	return nil
}

func (r *ProductRepository) updateProductVersions(ctx context.Context, updates []product.ProductGuardedUpdate) error {
	if len(updates) == 0 {
		return nil
	}
	var arg generated.BatchGuardedUpdateProductVersionParams
	var ids planIDs
	for _, u := range sortedByKey(updates, func(u product.ProductGuardedUpdate) string { return string(u.ProductID) }) {
		arg.Ids = append(arg.Ids, ids.required("product id", string(u.ProductID)))
		arg.ToVersionIds = append(arg.ToVersionIds, ids.required("to_version_id", u.ToVersionID))
		arg.ExpectedVersionIds = append(arg.ExpectedVersionIds, ids.optional("expected_version_id", u.ExpectedVersionID))
		arg.CurrentFingerprints = append(arg.CurrentFingerprints, u.CurrentFingerprint)
		arg.CanonicalNames = append(arg.CanonicalNames, u.CanonicalName)
		arg.Descriptions = append(arg.Descriptions, u.Description)
		arg.Brands = append(arg.Brands, u.Brand)
		arg.OriginCountries = append(arg.OriginCountries, u.OriginCountry)
		arg.UpdatedAts = append(arg.UpdatedAts, requiredTimestamptz(u.UpdatedAt))
	}
	if ids.err != nil {
		return ids.err
	}
	updated, err := r.queries.BatchGuardedUpdateProductVersion(ctx, arg)
	if err != nil {
		return mapPostgresError(fmt.Errorf("batch guarded update product version: %w", err))
	}
	return requireAllUpdated(len(updated), len(updates))
}

func (r *ProductRepository) updateProductFingerprints(ctx context.Context, updates []product.ProductFingerprintUpdate) error {
	if len(updates) == 0 {
		return nil
	}
	var arg generated.BatchGuardedUpdateProductFingerprintOnlyParams
	var ids planIDs
	for _, u := range sortedByKey(updates, func(u product.ProductFingerprintUpdate) string { return string(u.ProductID) }) {
		arg.Ids = append(arg.Ids, ids.required("product id", string(u.ProductID)))
		arg.ExpectedVersionIds = append(arg.ExpectedVersionIds, ids.optional("expected_version_id", u.ExpectedVersionID))
		arg.CurrentFingerprints = append(arg.CurrentFingerprints, u.CurrentFingerprint)
		arg.UpdatedAts = append(arg.UpdatedAts, requiredTimestamptz(u.UpdatedAt))
	}
	if ids.err != nil {
		return ids.err
	}
	updated, err := r.queries.BatchGuardedUpdateProductFingerprintOnly(ctx, arg)
	if err != nil {
		return mapPostgresError(fmt.Errorf("batch guarded update product fingerprint: %w", err))
	}
	return requireAllUpdated(len(updated), len(updates))
}

func (r *ProductRepository) updateSourcesOnChanged(ctx context.Context, updates []product.SourceChangedUpdate) error {
	if len(updates) == 0 {
		return nil
	}
	var arg generated.BatchUpdateProductSourceOnChangedParams
	var ids planIDs
	for _, u := range sortedByKey(updates, func(u product.SourceChangedUpdate) string { return u.ProductSourceID }) {
		arg.Ids = append(arg.Ids, ids.required("product source id", u.ProductSourceID))
		arg.LastChangedAts = append(arg.LastChangedAts, requiredTimestamptz(u.LastChangedAt))
		arg.SourceUpdatedAts = append(arg.SourceUpdatedAts, toTimestamptz(u.SourceUpdatedAt))
		arg.ReceivedAts = append(arg.ReceivedAts, requiredTimestamptz(u.ReceivedAt))
	}
	if ids.err != nil {
		return ids.err
	}
	if _, err := r.queries.BatchUpdateProductSourceOnChanged(ctx, arg); err != nil {
		return mapPostgresError(fmt.Errorf("batch update product source on changed: %w", err))
	}
	return nil
}

// updateSourceWatermarks ignores the affected-row count on purpose: the query skips
// rows whose watermark is already at or past the incoming value.
func (r *ProductRepository) updateSourceWatermarks(ctx context.Context, updates []product.SourceWatermarkUpdate) error {
	if len(updates) == 0 {
		return nil
	}
	var arg generated.BatchUpdateProductSourceWatermarksParams
	var ids planIDs
	for _, u := range sortedByKey(updates, func(u product.SourceWatermarkUpdate) string { return u.ProductSourceID }) {
		arg.Ids = append(arg.Ids, ids.required("product source id", u.ProductSourceID))
		arg.SourceUpdatedAts = append(arg.SourceUpdatedAts, toTimestamptz(u.SourceUpdatedAt))
		arg.ReceivedAts = append(arg.ReceivedAts, requiredTimestamptz(u.ReceivedAt))
	}
	if ids.err != nil {
		return ids.err
	}
	if _, err := r.queries.BatchUpdateProductSourceWatermarks(ctx, arg); err != nil {
		return mapPostgresError(fmt.Errorf("batch update product source watermarks: %w", err))
	}
	return nil
}

func copyProductParams(p *product.Product) (generated.CopyProductsParams, error) {
	var ids planIDs
	params := generated.CopyProductsParams{
		ID:                 ids.required("product id", string(p.ID)),
		CurrentVersionID:   ids.optional("current version id", p.CurrentVersionID),
		CanonicalName:      p.CanonicalName,
		Description:        p.Description,
		Brand:              p.Brand,
		OriginCountry:      p.OriginCountry,
		Status:             string(p.Status),
		CurrentFingerprint: p.CurrentFingerprint,
		CreatedAt:          requiredTimestamptz(p.CreatedAt),
		UpdatedAt:          requiredTimestamptz(p.UpdatedAt),
	}
	return params, ids.err
}

func copyProductSourceParams(ps *product.ProductSource) (generated.CopyProductSourcesParams, error) {
	var ids planIDs
	params := generated.CopyProductSourcesParams{
		ID:                  ids.required("product source id", ps.ID),
		ProductID:           ids.required("product id", string(ps.ProductID)),
		SourceID:            ps.SourceID,
		ExternalProductID:   ps.ExternalProductID,
		FirstSeenAt:         requiredTimestamptz(ps.FirstSeenAt),
		LastChangedAt:       requiredTimestamptz(ps.LastChangedAt),
		LastSourceUpdatedAt: toTimestamptz(ps.LastSourceUpdatedAt),
		LastReceivedAt:      requiredTimestamptz(ps.LastReceivedAt),
	}
	return params, ids.err
}

func copyProductVersionParams(pv *product.ProductVersion) (generated.CopyProductVersionsParams, error) {
	versionNumber, err := toInt32(pv.VersionNumber)
	if err != nil {
		return generated.CopyProductVersionsParams{}, fmt.Errorf("%w: version number: %w", product.ErrInvalidProductState, err)
	}
	attributes, err := json.Marshal(pv.Attributes)
	if err != nil {
		return generated.CopyProductVersionsParams{}, fmt.Errorf("marshal version attributes: %w", err)
	}
	var ids planIDs
	params := generated.CopyProductVersionsParams{
		ID:             ids.required("version id", pv.ID),
		ProductID:      ids.required("product id", string(pv.ProductID)),
		IngestionRunID: ids.optional("ingestion run id", pv.IngestionRunID),
		VersionNumber:  versionNumber,
		Fingerprint:    pv.Fingerprint,
		CanonicalName:  pv.CanonicalName,
		Description:    pv.Description,
		Brand:          pv.Brand,
		OriginCountry:  pv.OriginCountry,
		Attributes:     attributes,
		CreatedAt:      requiredTimestamptz(pv.CreatedAt),
	}
	return params, ids.err
}

func copyProductChangeParams(pc *product.ProductChange) (generated.CopyProductChangesParams, error) {
	changedFields, err := json.Marshal(pc.ChangedFields)
	if err != nil {
		return generated.CopyProductChangesParams{}, fmt.Errorf("marshal changed fields: %w", err)
	}
	var ids planIDs
	params := generated.CopyProductChangesParams{
		ID:             ids.required("change id", pc.ID),
		ProductID:      ids.required("product id", string(pc.ProductID)),
		ToVersionID:    ids.required("to_version_id", pc.ToVersionID),
		FromVersionID:  ids.optional("from_version_id", pc.FromVersionID),
		IngestionRunID: ids.optional("ingestion run id", pc.IngestionRunID),
		RawRecordID:    ids.optional("raw record id", pc.RawRecordID),
		ChangeType:     string(pc.ChangeType),
		ChangedFields:  changedFields,
		DetectedAt:     requiredTimestamptz(pc.DetectedAt),
	}
	return params, ids.err
}

type planIDs struct {
	err error
}

func (p *planIDs) required(field, s string) pgtype.UUID {
	if p.err != nil {
		return pgtype.UUID{}
	}
	u, err := parseUUID(s)
	if err != nil {
		p.err = fmt.Errorf("%w: invalid %s: %w", product.ErrInvalidProductState, field, err)
	}
	return u
}

// optional maps a nil or blank ID to SQL NULL.
func (p *planIDs) optional(field string, s *string) pgtype.UUID {
	if s == nil || strings.TrimSpace(*s) == "" {
		return pgtype.UUID{}
	}
	return p.required(field, *s)
}

// sortedByKey returns a sorted copy, leaving the caller's plan untouched.
func sortedByKey[T any](rows []T, key func(T) string) []T {
	sorted := slices.Clone(rows)
	slices.SortFunc(sorted, func(a, b T) int { return strings.Compare(key(a), key(b)) })
	return sorted
}

func requireAllUpdated(updated, want int) error {
	if updated != want {
		return fmt.Errorf("%w: %d of %d products had moved past the expected version",
			product.ErrVersionConflict, want-updated, want)
	}
	return nil
}
