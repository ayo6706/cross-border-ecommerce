package postgres

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	appRegulatory "github.com/ayo6706/cross-border-ecommerce/internal/application/regulatory"
	domain "github.com/ayo6706/cross-border-ecommerce/internal/domain/regulatory"
	"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/postgres/generated"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/shopspring/decimal"
)

var (
	_ appRegulatory.DatasetRepository = (*RegulatoryRepository)(nil)
	_ appRegulatory.RuleWriter        = (*RegulatoryRepository)(nil)
	_ appRegulatory.TariffRepository  = (*RegulatoryRepository)(nil)
)

// Constraints of migrations 000022/000023 that map to domain errors.
var regulatoryConstraintErrors = map[string]error{
	"uq_regulatory_datasets_version":                   domain.ErrDuplicateVersion,
	"uq_regulatory_datasets_active":                    domain.ErrConcurrentActivation,
	"chk_regulatory_datasets_review_before_activation": domain.ErrReviewRequired,
	"chk_regulatory_datasets_reviewer_not_loader":      domain.ErrReviewerIsLoader,
}

type RegulatoryRepository struct {
	queries *generated.Queries
}

func NewRegulatoryRepository(db generated.DBTX) (*RegulatoryRepository, error) {
	if db == nil {
		return nil, errors.New("regulatory repository needs a database handle")
	}
	return &RegulatoryRepository{queries: generated.New(db)}, nil
}

func (r *RegulatoryRepository) Create(ctx context.Context, d *domain.Dataset) error {
	id, err := parseUUID(d.ID)
	if err != nil {
		return fmt.Errorf("invalid dataset id: %w", err)
	}
	fetchedAt, err := requiredTimestamptz(d.FetchedAt)
	if err != nil {
		return fmt.Errorf("dataset fetched_at: %w", err)
	}
	err = r.queries.CreateRegulatoryDataset(ctx, generated.CreateRegulatoryDatasetParams{
		ID:             id,
		Jurisdiction:   d.Jurisdiction,
		Category:       string(d.Category),
		Source:         d.Source,
		Version:        d.Version,
		FetchedAt:      fetchedAt,
		ContentSha256:  d.ContentSHA256[:],
		Licence:        d.Licence,
		Attribution:    d.Attribution,
		Status:         string(d.Status),
		LoadedBy:       d.LoadedBy,
		RequiresReview: d.RequiresReview,
	})
	if err != nil {
		return fmt.Errorf("create dataset: %w", mapRegulatoryError(err))
	}
	return nil
}

func (r *RegulatoryRepository) GetForUpdate(ctx context.Context, id string) (*domain.Dataset, error) {
	uid, err := parseUUID(id)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", domain.ErrDatasetNotFound, id)
	}
	row, err := r.queries.GetRegulatoryDatasetForUpdate(ctx, uid)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: %s", domain.ErrDatasetNotFound, id)
	}
	if err != nil {
		return nil, fmt.Errorf("get dataset %s: %w", id, err)
	}
	return toDomainDataset(&row)
}

func (r *RegulatoryRepository) ActiveForUpdate(
	ctx context.Context, jurisdiction string, category domain.Category, source string,
) (*domain.Dataset, bool, error) {
	row, err := r.queries.GetActiveRegulatoryDatasetForUpdate(ctx, generated.GetActiveRegulatoryDatasetForUpdateParams{
		Jurisdiction: jurisdiction, Category: string(category), Source: source,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("get active dataset: %w", err)
	}
	d, err := toDomainDataset(&row)
	return d, err == nil, err
}

func (r *RegulatoryRepository) SaveLifecycle(ctx context.Context, d *domain.Dataset, expected domain.Status) error {
	id, err := parseUUID(d.ID)
	if err != nil {
		return fmt.Errorf("invalid dataset id: %w", err)
	}
	params := generated.UpdateRegulatoryDatasetLifecycleParams{
		ID:             id,
		ExpectedStatus: string(expected),
		Status:         string(d.Status),
		ActivatedAt:    toTimestamptz(d.ActivatedAt),
		SupersededAt:   toTimestamptz(d.SupersededAt),
		RejectedReason: optionalText(d.RejectedReason),
	}
	if d.Review != nil {
		params.ReviewedBy = optionalText(d.Review.By)
		params.ReviewNote = optionalText(d.Review.Note)
		params.ReviewedAt = toTimestamptz(&d.Review.At)
	}
	n, err := r.queries.UpdateRegulatoryDatasetLifecycle(ctx, params)
	if err != nil {
		return fmt.Errorf("save dataset %s: %w", d.ID, mapRegulatoryError(err))
	}
	if n == 0 {
		return fmt.Errorf("%w: dataset %s is no longer %s", domain.ErrInvalidTransition, d.ID, expected)
	}
	return nil
}

func (r *RegulatoryRepository) ActiveAt(
	ctx context.Context, jurisdiction string, category domain.Category, at time.Time,
) ([]*domain.Dataset, error) {
	ts, err := requiredTimestamptz(at)
	if err != nil {
		return nil, fmt.Errorf("coverage time: %w", err)
	}
	rows, err := r.queries.ListRegulatoryDatasetsActiveAt(ctx, generated.ListRegulatoryDatasetsActiveAtParams{
		Jurisdiction: jurisdiction, Category: string(category), At: ts,
	})
	if err != nil {
		return nil, fmt.Errorf("list active datasets: %w", err)
	}
	out := make([]*domain.Dataset, 0, len(rows))
	for i := range rows {
		d, err := toDomainDataset(&rows[i])
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, nil
}

// InsertCurated writes every rule of a curated file in one statement. Rule constraints (code and
// country formats, overlapping validity, frozen dataset) surface as ErrInvalidRule.
func (r *RegulatoryRepository) InsertCurated(ctx context.Context, datasetID string, rules domain.CuratedRules) error {
	id, err := parseUUID(datasetID)
	if err != nil {
		return fmt.Errorf("invalid dataset id: %w", err)
	}
	category, err := rules.Category()
	if err != nil {
		return err
	}
	switch category {
	case domain.CategoryImportRestriction:
		err = r.queries.InsertImportRestrictions(ctx, restrictionParams(id, rules.Restrictions))
	case domain.CategoryPermit:
		err = r.queries.InsertPermitRequirements(ctx, permitParams(id, rules.Permits))
	default:
		return fmt.Errorf("%w: no curated table for %s", domain.ErrInvalidRule, category)
	}
	if err != nil {
		return fmt.Errorf("insert curated rules: %w", mapRuleError(err))
	}
	return nil
}

func (r *RegulatoryRepository) TariffCandidates(
	ctx context.Context, datasetIDs []string, q domain.TariffQuery,
) ([]domain.TariffMeasure, error) {
	ids, err := parseUUIDs(datasetIDs)
	if err != nil {
		return nil, fmt.Errorf("invalid dataset id: %w", err)
	}
	onDate := q.TransactionDate()
	rows, err := r.queries.ListTariffCandidates(ctx, generated.ListTariffCandidatesParams{
		DatasetIds: ids,
		HsCodes:    q.HSPrefixes(),
		Origin:     q.Origin(),
		OnDate:     toDate(&onDate),
	})
	if err != nil {
		return nil, fmt.Errorf("list tariff candidates: %w", err)
	}
	out := make([]domain.TariffMeasure, 0, len(rows))
	for i := range rows {
		m, err := toTariffMeasure(&rows[i])
		if err != nil {
			return nil, fmt.Errorf("tariff rule %s: %w", uuidToString(rows[i].ID), err)
		}
		out = append(out, m)
	}
	return out, nil
}

func toTariffMeasure(row *generated.ListTariffCandidatesRow) (domain.TariffMeasure, error) {
	adValorem, err := toDecimal(row.AdValoremPercent)
	if err != nil {
		return domain.TariffMeasure{}, fmt.Errorf("ad_valorem_percent: %w", err)
	}
	specificAmount, err := toDecimal(row.SpecificAmount)
	if err != nil {
		return domain.TariffMeasure{}, fmt.Errorf("specific_amount: %w", err)
	}
	validity, err := domain.NewValidity(row.EffectiveFrom.Time, fromDate(row.EffectiveTo))
	if err != nil {
		return domain.TariffMeasure{}, err
	}
	m := domain.TariffMeasure{
		ID:              uuidToString(row.ID),
		DatasetID:       uuidToString(row.DatasetID),
		HSCode:          row.HsCode,
		OriginCountry:   row.OriginCountry,
		MeasureType:     domain.MeasureType(row.MeasureType),
		MeasureCode:     row.MeasureCode.String,
		RateType:        domain.RateType(row.RateType),
		AdValorem:       adValorem,
		RateExpression:  row.RateExpression,
		SourceReference: row.SourceReference,
		Validity:        validity,
	}
	if specificAmount != nil {
		m.Specific = &domain.SpecificDuty{
			Amount:   *specificAmount,
			Currency: row.SpecificCurrency.String,
			Unit:     row.SpecificUnit.String,
		}
	}
	return m, nil
}

// toDecimal converts a NUMERIC exactly; NULL is nil. NaN and infinities satisfy the rate columns'
// ">= 0" CHECKs, so they reach here and are refused as a corrupt rule rather than read as a rate.
func toDecimal(n pgtype.Numeric) (*decimal.Decimal, error) {
	if !n.Valid {
		return nil, nil
	}
	if n.NaN || n.InfinityModifier != pgtype.Finite {
		return nil, fmt.Errorf("%w: not a finite number", domain.ErrInvalidRule)
	}
	d := decimal.NewFromBigInt(n.Int, n.Exp)
	return &d, nil
}

func restrictionParams(id pgtype.UUID, rules []domain.ImportRestriction) generated.InsertImportRestrictionsParams {
	p := generated.InsertImportRestrictionsParams{DatasetID: id}
	for i := range rules {
		rule := &rules[i]
		p.HsCodes = append(p.HsCodes, rule.HSCode)
		p.OriginCountries = append(p.OriginCountries, rule.OriginCountry)
		p.Descriptions = append(p.Descriptions, rule.Description)
		p.EffectiveFroms = append(p.EffectiveFroms, toDate(&rule.Validity.From))
		p.EffectiveTos = append(p.EffectiveTos, toDate(rule.Validity.To))
		p.SourceReferences = append(p.SourceReferences, rule.SourceReference)
	}
	return p
}

func permitParams(id pgtype.UUID, rules []domain.PermitRequirement) generated.InsertPermitRequirementsParams {
	p := generated.InsertPermitRequirementsParams{DatasetID: id}
	for i := range rules {
		rule := &rules[i]
		p.HsCodes = append(p.HsCodes, rule.HSCode)
		p.OriginCountries = append(p.OriginCountries, rule.OriginCountry)
		p.PermitCodes = append(p.PermitCodes, rule.PermitCode)
		p.IssuingAgencies = append(p.IssuingAgencies, rule.IssuingAgency)
		p.DocumentTypes = append(p.DocumentTypes, rule.DocumentType)
		p.EffectiveFroms = append(p.EffectiveFroms, toDate(&rule.Validity.From))
		p.EffectiveTos = append(p.EffectiveTos, toDate(rule.Validity.To))
		p.SourceReferences = append(p.SourceReferences, rule.SourceReference)
	}
	return p
}

func toDomainDataset(row *generated.RegulatoryDataset) (*domain.Dataset, error) {
	if len(row.ContentSha256) != 32 {
		return nil, fmt.Errorf("dataset %s: content hash is %d bytes", uuidToString(row.ID), len(row.ContentSha256))
	}
	d := &domain.Dataset{
		ID:             uuidToString(row.ID),
		Jurisdiction:   row.Jurisdiction,
		Category:       domain.Category(row.Category),
		Source:         row.Source,
		Version:        row.Version,
		FetchedAt:      row.FetchedAt.Time.UTC(),
		Licence:        row.Licence,
		Attribution:    row.Attribution,
		LoadedBy:       row.LoadedBy,
		RequiresReview: row.RequiresReview,
		Status:         domain.Status(row.Status),
		ActivatedAt:    fromTimestamptz(row.ActivatedAt),
		SupersededAt:   fromTimestamptz(row.SupersededAt),
		RejectedReason: row.RejectedReason.String,
	}
	copy(d.ContentSHA256[:], row.ContentSha256)
	if row.ReviewedAt.Valid {
		d.Review = &domain.Review{By: row.ReviewedBy.String, Note: row.ReviewNote.String, At: row.ReviewedAt.Time.UTC()}
	}
	return d, nil
}

func mapRegulatoryError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if mapped, ok := regulatoryConstraintErrors[pgErr.ConstraintName]; ok {
			return fmt.Errorf("%w: %w", mapped, err)
		}
	}
	return err
}

// ruleErrorCodes are the SQLSTATEs a bad file causes: a CHECK or domain format, an overlap, a
// duplicate, or rules added to a dataset that is no longer LOADED. Others are code bugs.
var ruleErrorCodes = []string{"23514", "23P01", "23505", "55000"}

// mapRuleError classifies the file's errors as invalid rules: the file, not the code, has to change.
func mapRuleError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && slices.Contains(ruleErrorCodes, pgErr.Code) {
		return fmt.Errorf("%w: %w", domain.ErrInvalidRule, err)
	}
	return err
}

func optionalText(s string) pgtype.Text {
	return pgtype.Text{String: s, Valid: s != ""}
}

// toDate sends a calendar date (midnight UTC); nil is SQL NULL, an open end. pgx sends the date
// of the time's own location, so the caller passes a UTC calendar date.
func toDate(t *time.Time) pgtype.Date {
	if t == nil {
		return pgtype.Date{}
	}
	return pgtype.Date{Time: *t, Valid: true}
}

func fromDate(d pgtype.Date) *time.Time {
	if !d.Valid {
		return nil
	}
	return &d.Time
}
