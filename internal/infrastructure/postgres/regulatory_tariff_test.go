package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	appRegulatory "github.com/ayo6706/cross-border-ecommerce/internal/application/regulatory"
	domain "github.com/ayo6706/cross-border-ecommerce/internal/domain/regulatory"
	"github.com/jackc/pgx/v5/pgxpool"
)

// loadTariffDataset inserts a LOADED tariff dataset with its rules. Tariff datasets have no load
// path yet, so the rows are SQL: each is a tariff_rates VALUES tuple whose dataset_id is $1.
func loadTariffDataset(
	ctx context.Context, t *testing.T, pool *pgxpool.Pool,
	jurisdiction, source, version string, fetchedAt time.Time, rows ...string,
) string {
	t.Helper()
	var id string
	err := pool.QueryRow(ctx, insertDataset+`($1, 'TARIFF', $2, $3::text, $4, sha256(convert_to($3::text, 'UTF8')), 'Public domain',
		'fixture', 'LOADED', 'loader', false) RETURNING id`, jurisdiction, source, version, fetchedAt).Scan(&id)
	if err != nil {
		t.Fatalf("insert %s %s dataset: %v", source, version, err)
	}
	if _, err := pool.Exec(ctx, insertTariff+strings.Join(rows, ", "), id); err != nil {
		t.Fatalf("insert %s %s rules: %v", source, version, err)
	}
	return id
}

// tariffDataset loads a tariff dataset and activates it through the service.
func tariffDataset(
	ctx context.Context, t *testing.T, svc *appRegulatory.Service, pool *pgxpool.Pool,
	jurisdiction, source, version string, fetchedAt time.Time, rows ...string,
) *domain.Dataset {
	t.Helper()
	id := loadTariffDataset(ctx, t, pool, jurisdiction, source, version, fetchedAt, rows...)
	d, err := svc.Activate(ctx, id)
	if err != nil {
		t.Fatalf("activate %s %s: %v", source, version, err)
	}
	return d
}

func adValorem(hs, origin, code, percent, from, to string) string {
	measureType, measureCode := "MFN", "NULL"
	if code != "" {
		measureType, measureCode = "ADDITIONAL_DUTY", "'"+code+"'"
	}
	return fmt.Sprintf("($1, '%s', '%s', '%s', %s, 'AD_VALOREM', %s, NULL, NULL, NULL, '%s%%', '%s', %s, 'ref')",
		hs, origin, measureType, measureCode, percent, strings.Trim(percent, "'"), from, to)
}

func resolveTariff(
	ctx context.Context, svc *appRegulatory.Service, destination, origin, hs string, transactionAt, evaluatedAt time.Time,
) (*domain.TariffResolution, error) {
	q, err := domain.NewTariffQuery(domain.TariffQueryParams{
		Destination:   destination,
		Origin:        origin,
		HSCode:        hs,
		TransactionAt: transactionAt,
		EvaluatedAt:   evaluatedAt,
	})
	if err != nil {
		return nil, err
	}
	return svc.ResolveTariff(ctx, q)
}

// measureKeys names each applied measure hs/origin/code, and checks it came from a dataset the
// resolution reports.
func measureKeys(t *testing.T, r *domain.TariffResolution) []string {
	t.Helper()
	var keys []string
	for _, m := range r.Measures {
		if !slices.ContainsFunc(r.Datasets, func(d *domain.Dataset) bool { return d.ID == m.DatasetID }) {
			t.Errorf("measure %s is from dataset %s, which the resolution does not report", m.ID, m.DatasetID)
		}
		keys = append(keys, m.HSCode+"/"+m.OriginCountry+"/"+m.MeasureCode)
	}
	return keys
}

func utc(t *testing.T, rfc3339 string) time.Time {
	t.Helper()
	at, err := time.Parse(time.RFC3339, rfc3339)
	if err != nil {
		t.Fatalf("parse %q: %v", rfc3339, err)
	}
	return at
}

// seedTariffs activates tariff datasets for US (two sources), NG, EU and GB, all fetched now.
func seedTariffs(ctx context.Context, t *testing.T, svc *appRegulatory.Service, pool *pgxpool.Pool) {
	t.Helper()
	now := time.Now()
	tariffDataset(ctx, t, svc, pool, "US", "usitc_hts", "2026-09", now,
		adValorem("85287200", "*", "", "5", "2020-01-01", "NULL"),
		adValorem("8528", "KP", "", "35", "2020-01-01", "NULL"),
		adValorem("6109100012", "*", "", "16.5", "2020-01-01", "NULL"),
		// Replaces MFN only once the origin qualifies, which the tariff resolver does not decide.
		`($1, '6109100012', 'VN', 'PREFERENTIAL', 'US_VN_FTA', 'AD_VALOREM', 0, NULL, NULL, NULL, '0%',
			'2020-01-01', NULL, 'fixture agreement')`,
		`($1, '2204210000', '*', 'MFN', NULL, 'SPECIFIC', NULL, 0.063, 'USD', 'LITRE', '6.3¢/liter',
			'2020-01-01', NULL, 'HTSUS 2204.21')`)
	tariffDataset(ctx, t, svc, pool, "US", "us_chapter99", "2026-09", now,
		adValorem("6109100012", "CN", "US_SEC_301", "7.5", "2020-02-14", "NULL"))
	tariffDataset(ctx, t, svc, pool, "NG", "ng_cet", "2026-09", now,
		adValorem("8517130000", "*", "", "10", "2022-01-01", "'2026-07-01'"),
		adValorem("8517130000", "*", "", "15", "2026-07-01", "NULL"),
		adValorem("8517130000", "*", "NG_NAC_LEVY", "0.5", "2022-01-01", "NULL"))
	tariffDataset(ctx, t, svc, pool, "EU", "xi_tariff", "2026-09", now,
		adValorem("6109100010", "*", "", "12", "2020-01-01", "NULL"))
	tariffDataset(ctx, t, svc, pool, "GB", "uk_tariff", "2026-09", now,
		`($1, '1806900000', '*', 'MFN', NULL, 'UNSUPPORTED_MEASURE', NULL, NULL, NULL, NULL,
			'8.00 % + EA MAX 18.70 % +ADSZ', '2021-01-01', NULL, 'UK Global Tariff 1806900000')`)
}

func TestRegulatoryService_ResolveTariff_Live(t *testing.T) {
	svc, pool := newRegulatoryService(t, 24*time.Hour)
	ctx := context.Background()
	seedTariffs(ctx, t, svc, pool)
	evaluatedAt := time.Now()
	newYork := time.FixedZone("EDT", -4*3600)

	cases := map[string]struct {
		destination, origin, hs string
		transactionAt           time.Time
		wantMeasures            []string
		wantPercent             string
	}{
		// Evaluated today against a dataset activated today: the transaction date selects the rule.
		"8-digit rule on a past date": {"US", "CN", "8528.72.00", utc(t, "2026-03-15T10:00:00Z"),
			[]string{"85287200/*/"}, "5"},
		"10-digit code falls back to the 8-digit rule": {"US", "CN", "8528.72.00.10", utc(t, "2026-03-15T10:00:00Z"),
			[]string{"85287200/*/"}, "5"},
		"the origin's heading rule beats the any-origin rule": {"US", "KP", "8528720000", utc(t, "2026-03-15T10:00:00Z"),
			[]string{"8528/KP/"}, "35"},
		"an overlay from a second source stacks": {"US", "CN", "6109.10.00.12", utc(t, "2026-03-15T10:00:00Z"),
			[]string{"6109100012/*/", "6109100012/CN/US_SEC_301"}, "24"},
		"no overlay and no preferential rate for VN": {"US", "VN", "6109.10.00.12", utc(t, "2026-03-15T10:00:00Z"),
			[]string{"6109100012/*/"}, "16.5"},
		"a specific rate adds no ad valorem": {"US", "FR", "2204.21.00.00", utc(t, "2026-03-15T10:00:00Z"),
			[]string{"2204210000/*/"}, "0"},
		"the day before a rate change": {"NG", "CN", "8517130000", utc(t, "2026-06-30T23:59:59Z"),
			[]string{"8517130000/*/", "8517130000/*/NG_NAC_LEVY"}, "10.5"},
		"the day of a rate change": {"NG", "CN", "8517130000", utc(t, "2026-07-01T00:00:00Z"),
			[]string{"8517130000/*/", "8517130000/*/NG_NAC_LEVY"}, "15.5"},
		"an evening in New York is the next UTC day": {"NG", "CN", "8517130000",
			time.Date(2026, 6, 30, 22, 30, 0, 0, newYork),
			[]string{"8517130000/*/", "8517130000/*/NG_NAC_LEVY"}, "15.5"},
		"a member state imports under the EU dataset": {"DE", "CN", "6109100010", utc(t, "2026-03-15T10:00:00Z"),
			[]string{"6109100010/*/"}, "12"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			r, err := resolveTariff(ctx, svc, tc.destination, tc.origin, tc.hs, tc.transactionAt, evaluatedAt)
			if err != nil {
				t.Fatalf("resolve: %v", err)
			}
			if got := measureKeys(t, r); !slices.Equal(got, tc.wantMeasures) {
				t.Fatalf("measures = %v, want %v", got, tc.wantMeasures)
			}
			if got := r.AdValoremPercent(); got.String() != tc.wantPercent {
				t.Fatalf("ad valorem = %s%%, want %s%%", got, tc.wantPercent)
			}
		})
	}

	t.Run("a specific rate keeps its amount, currency and unit exactly", func(t *testing.T) {
		r, err := resolveTariff(ctx, svc, "US", "FR", "2204210000", utc(t, "2026-03-15T10:00:00Z"), evaluatedAt)
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		s := r.Measures[0].Specific
		if s == nil || s.Amount.String() != "0.063" || s.Currency != "USD" || s.Unit != "LITRE" {
			t.Fatalf("specific duty = %+v, want 0.063 USD per LITRE", s)
		}
	})
}

func TestRegulatoryService_ResolveTariffHolds_Live(t *testing.T) {
	svc, pool := newRegulatoryService(t, 24*time.Hour)
	ctx := context.Background()
	seedTariffs(ctx, t, svc, pool)
	tariffDataset(ctx, t, svc, pool, "CA", "cbsa_tariff", "2026-09", time.Now().Add(-25*time.Hour),
		adValorem("6109", "*", "", "18", "2020-01-01", "NULL"))
	evaluatedAt := time.Now()

	cases := map[string]struct {
		destination, origin, hs string
		transactionAt           time.Time
		want                    error
		reason                  string
	}{
		"no tariff dataset for the jurisdiction": {"JP", "CN", "6109100012", utc(t, "2026-03-15T10:00:00Z"),
			domain.ErrNoCoverage, domain.ReasonNoCoverage},
		"the only dataset is past its SLA": {"CA", "CN", "6109100012", utc(t, "2026-03-15T10:00:00Z"),
			domain.ErrStaleCoverage, domain.ReasonStaleData},
		"a 6-digit code where only national lines exist": {"US", "CN", "6109.10", utc(t, "2026-03-15T10:00:00Z"),
			domain.ErrNoTariffRate, domain.ReasonNoTariffRate},
		"a date before the first rule took effect": {"NG", "CN", "8517130000", utc(t, "2021-12-31T12:00:00Z"),
			domain.ErrNoTariffRate, domain.ReasonNoTariffRate},
		"an unsupported rate": {"GB", "CN", "1806900000", utc(t, "2026-03-15T10:00:00Z"),
			domain.ErrUnsupportedMeasure, domain.ReasonUnsupportedMeasure},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			r, err := resolveTariff(ctx, svc, tc.destination, tc.origin, tc.hs, tc.transactionAt, evaluatedAt)
			if !errors.Is(err, tc.want) || r != nil {
				t.Fatalf("want %v and no resolution, got %v, %v", tc.want, r, err)
			}
			if reason, _ := domain.HoldReason(err); reason != tc.reason {
				t.Fatalf("hold reason = %q, want %q", reason, tc.reason)
			}
		})
	}
}

// The evaluation time selects the dataset version: before v2's activation, v1's rate; after, v2's.
// v3 is loaded but never activated, so its rate is never used.
func TestRegulatoryService_ResolveTariffUsesVersionInForceAtEvaluation_Live(t *testing.T) {
	svc, pool := newRegulatoryService(t, 24*time.Hour)
	ctx := context.Background()
	rule := func(percent string) string { return adValorem("85287200", "*", "", percent, "2020-01-01", "NULL") }
	v1 := tariffDataset(ctx, t, svc, pool, "US", "usitc_hts", "v1", time.Now(), rule("5"))
	v2 := tariffDataset(ctx, t, svc, pool, "US", "usitc_hts", "v2", time.Now(), rule("6"))
	loadTariffDataset(ctx, t, pool, "US", "usitc_hts", "v3", time.Now(), rule("9"))

	cases := map[string]struct {
		evaluatedAt time.Time
		wantDataset string
		wantPercent string
	}{
		"just before v2's activation": {v2.ActivatedAt.Add(-time.Microsecond), v1.ID, "5"},
		"after v2's activation":       {time.Now(), v2.ID, "6"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			r, err := resolveTariff(ctx, svc, "US", "CN", "85287200", utc(t, "2026-03-15T10:00:00Z"), tc.evaluatedAt)
			if err != nil {
				t.Fatalf("resolve: %v", err)
			}
			if got := r.Measures[0].DatasetID; got != tc.wantDataset || r.AdValoremPercent().String() != tc.wantPercent {
				t.Fatalf("resolved %s%% from %s, want %s%% from %s", r.AdValoremPercent(), got, tc.wantPercent, tc.wantDataset)
			}
		})
	}
}

// NaN and Infinity pass the rate columns' ">= 0" CHECKs. A rule holding one fails the resolution as
// a corrupt rule; it is neither a rate nor a HOLD reason.
func TestRegulatoryService_ResolveTariffRefusesNonFiniteRates_Live(t *testing.T) {
	svc, pool := newRegulatoryService(t, 24*time.Hour)
	ctx := context.Background()
	cases := []struct{ rate, jurisdiction string }{{"'NaN'", "US"}, {"'Infinity'", "CA"}}
	for _, tc := range cases {
		t.Run(tc.rate, func(t *testing.T) {
			tariffDataset(ctx, t, svc, pool, tc.jurisdiction, "tariff", tc.rate, time.Now(),
				adValorem("85287200", "*", "", tc.rate, "2020-01-01", "NULL"))
			r, err := resolveTariff(ctx, svc, tc.jurisdiction, "CN", "85287200", utc(t, "2026-03-15T10:00:00Z"), time.Now())
			if !errors.Is(err, domain.ErrInvalidRule) || r != nil {
				t.Fatalf("want ErrInvalidRule and no resolution, got %v, %v", r, err)
			}
			if reason, ok := domain.HoldReason(err); ok {
				t.Fatalf("a corrupt rate became HOLD %s: %v", reason, err)
			}
		})
	}
}
