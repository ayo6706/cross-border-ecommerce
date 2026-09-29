package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/testsupport"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Dataset ids from testdata/regulatory_seed.sql.
const (
	ngTariffDataset      = "00000000-0000-4000-8000-000000000001"
	ngRestrictionDataset = "00000000-0000-4000-8000-000000000011"
	usPermitDataset      = "00000000-0000-4000-8000-000000000021"
	usSanctionsDataset   = "00000000-0000-4000-8000-000000000031"
	usExportDataset      = "00000000-0000-4000-8000-000000000041"
	gbAgreementDataset   = "00000000-0000-4000-8000-000000000051"
)

const insertTariff = `INSERT INTO tariff_rates (dataset_id, hs_code, origin_country, measure_type,
	measure_code, rate_type, ad_valorem_percent, specific_amount, specific_currency, specific_unit,
	rate_expression, effective_from, effective_to, source_reference) VALUES `

const insertSanction = `INSERT INTO sanctions_list (dataset_id, list_entry_id, entity_type, primary_name,
	program, effective_from, effective_to, source_reference) VALUES `

// withRegulatorySeed runs fn after loading the regulatory fixtures, inside a transaction that is
// always rolled back, so no fixture row reaches the shared test database.
func withRegulatorySeed(t *testing.T, pool *pgxpool.Pool, fn func(ctx context.Context, tx pgx.Tx)) {
	t.Helper()
	seed, err := os.ReadFile("testdata/regulatory_seed.sql")
	if err != nil {
		t.Fatalf("read regulatory seed: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() {
		if err := tx.Rollback(context.WithoutCancel(ctx)); err != nil {
			t.Errorf("rollback: %v", err)
		}
	}()
	if _, err := tx.Exec(ctx, string(seed)); err != nil {
		t.Fatalf("load regulatory seed: %v", err)
	}
	fn(ctx, tx)
}

func requirePgError(t *testing.T, err error, code, constraint string) {
	t.Helper()
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("want SQLSTATE %s on %s, got %v", code, constraint, err)
	}
	if pgErr.Code != code || pgErr.ConstraintName != constraint {
		t.Fatalf("got SQLSTATE %s on %q, want %s on %q (%s)",
			pgErr.Code, pgErr.ConstraintName, code, constraint, pgErr.Message)
	}
}

func TestRegulatorySeed_Live(t *testing.T) {
	pool := testsupport.LiveDB(t)

	withRegulatorySeed(t, pool, func(ctx context.Context, tx pgx.Tx) {
		rows, err := tx.Query(ctx, `SELECT DISTINCT jurisdiction FROM regulatory_datasets ORDER BY 1`)
		if err != nil {
			t.Fatalf("list jurisdictions: %v", err)
		}
		jurisdictions, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			t.Fatalf("list jurisdictions: %v", err)
		}
		if want := []string{"EU", "GB", "NG", "US"}; !slices.Equal(jurisdictions, want) {
			t.Fatalf("seeded jurisdictions = %v, want %v", jurisdictions, want)
		}

		for _, table := range []string{"tariff_rates", "import_restrictions", "permit_requirements",
			"sanctions_list", "export_controls", "preferential_agreements"} {
			var n int
			if err := tx.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil {
				t.Fatalf("count %s: %v", table, err)
			}
			if n == 0 {
				t.Errorf("seed has no %s rows", table)
			}
		}
	})
}

// NG raises the smartphone duty from 10% to 15% on 2026-07-01. The query uses the effective-dating
// predicate from compliance-rules.md §1.
func TestRegulatorySeed_ResolvesRateInEffectOnTransactionDate_Live(t *testing.T) {
	pool := testsupport.LiveDB(t)

	const rateOn = `SELECT ad_valorem_percent::text FROM tariff_rates
		WHERE dataset_id = $1 AND hs_code = '8517130000' AND origin_country = '*' AND measure_type = 'MFN'
		  AND effective_from <= $2 AND (effective_to IS NULL OR effective_to > $2)`
	cases := map[string]string{
		"2022-01-01": "10.0000", // first day of the old rate (inclusive start)
		"2026-06-30": "10.0000",
		"2026-07-01": "15.0000", // old rate's end is exclusive
		"2030-01-01": "15.0000", // open-ended
	}
	withRegulatorySeed(t, pool, func(ctx context.Context, tx pgx.Tx) {
		for day, want := range cases {
			date, err := time.Parse(time.DateOnly, day)
			if err != nil {
				t.Fatalf("parse %s: %v", day, err)
			}
			rows, err := tx.Query(ctx, rateOn, ngTariffDataset, date)
			if err != nil {
				t.Fatalf("rate on %s: %v", day, err)
			}
			got, err := pgx.CollectRows(rows, pgx.RowTo[string])
			if err != nil {
				t.Fatalf("rate on %s: %v", day, err)
			}
			if len(got) != 1 || got[0] != want {
				t.Errorf("rate on %s = %v, want exactly [%s]", day, got, want)
			}
		}
	})
}

// A new dataset version repeats the rules it still contains; the same rule key with the same
// validity is allowed across versions, and each version keeps its own rate.
func TestRegulatorySchema_SameRuleInNewDatasetVersion_Live(t *testing.T) {
	pool := testsupport.LiveDB(t)

	withRegulatorySeed(t, pool, func(ctx context.Context, tx pgx.Tx) {
		var next string
		err := tx.QueryRow(ctx, `INSERT INTO regulatory_datasets
			(jurisdiction, category, source, version, fetched_at, content_sha256, licence, attribution)
			VALUES ('NG', 'TARIFF', 'ng_cet', 'fixture-2026-08', '2026-08-01T09:00:00Z',
			        sha256('ng_cet v2'), 'Public sector information', 'Nigeria Customs Service')
			RETURNING id::text`).Scan(&next)
		if err != nil {
			t.Fatalf("insert second dataset version: %v", err)
		}
		_, err = tx.Exec(ctx, insertTariff+`($1, '8517130000', '*', 'MFN', NULL, 'AD_VALOREM', 20.0000,
			NULL, NULL, NULL, '20%', '2026-07-01', NULL, 'Fiscal Policy Measures 2026 (amended)')`, next)
		if err != nil {
			t.Fatalf("same rule key and validity in a new dataset version must be accepted: %v", err)
		}

		const rate = `SELECT ad_valorem_percent::text FROM tariff_rates WHERE dataset_id = $1
			AND hs_code = '8517130000' AND measure_type = 'MFN' AND effective_from = '2026-07-01'`
		for dataset, want := range map[string]string{ngTariffDataset: "15.0000", next: "20.0000"} {
			var got string
			if err := tx.QueryRow(ctx, rate, dataset).Scan(&got); err != nil {
				t.Fatalf("rate in dataset %s: %v", dataset, err)
			}
			if got != want {
				t.Errorf("rate in dataset %s = %s, want %s", dataset, got, want)
			}
		}
	})
}

// A published rate is stored with every decimal it has; nothing rounds it on the way in.
func TestRegulatorySchema_StoresRatesExactly_Live(t *testing.T) {
	pool := testsupport.LiveDB(t)

	withRegulatorySeed(t, pool, func(ctx context.Context, tx pgx.Tx) {
		var amount string
		err := tx.QueryRow(ctx, insertTariff+`($1, '0201100000', '*', 'MFN', NULL, 'SPECIFIC', NULL,
			0.00176, 'USD', 'KG', '0.176¢/kg', '2026-01-01', NULL, 'test')
			RETURNING specific_amount::text`, ngTariffDataset).Scan(&amount)
		if err != nil {
			t.Fatalf("insert five-decimal rate: %v", err)
		}
		if amount != "0.00176" {
			t.Fatalf("stored specific_amount = %s, want 0.00176", amount)
		}
	})
}

// Every case either has a key no seeded rule has, or does not overlap the seeded rule's range.
func TestRegulatorySchema_AcceptsDistinctRules_Live(t *testing.T) {
	pool := testsupport.LiveDB(t)

	cases := map[string]struct {
		sql     string
		dataset string
	}{
		"range ending where the next begins": {insertTariff + `($1, '8517130000', '*', 'MFN', NULL,
			'AD_VALOREM', 9, NULL, NULL, NULL, '9%', '2020-01-01', '2022-01-01', 'test')`, ngTariffDataset},
		"same code under another origin": {insertTariff + `($1, '8517130000', 'CN', 'MFN', NULL,
			'AD_VALOREM', 12, NULL, NULL, NULL, '12%', '2026-06-01', NULL, 'test')`, ngTariffDataset},
		"additional duty under a second programme": {insertTariff + `($1, '8517130000', '*',
			'ADDITIONAL_DUTY', 'NG_ETLS', 'AD_VALOREM', 0.5, NULL, NULL, NULL, '0.5%', '2022-01-01', NULL,
			'test')`, ngTariffDataset},
		"relisting after a delisting": {insertSanction + `($1, 'FIXTURE-OFAC-0002', 'INDIVIDUAL',
			'FIXTURE PERSON ONE', 'RUSSIA-EO14024', '2025-11-20', NULL, 'test')`, usSanctionsDataset},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			withRegulatorySeed(t, pool, func(ctx context.Context, tx pgx.Tx) {
				if _, err := tx.Exec(ctx, tc.sql, tc.dataset); err != nil {
					t.Fatalf("want the row accepted, got %v", err)
				}
			})
		})
	}
}

func TestRegulatorySchema_RejectsInvalidRules_Live(t *testing.T) {
	pool := testsupport.LiveDB(t)

	const (
		checkViolation     = "23514"
		fkViolation        = "23503"
		uniqueViolation    = "23505"
		exclusionViolation = "23P01"
	)
	// A tariff row in dataset $1. measureCode, pct and to are raw SQL (quote a string, pass NULL
	// bare); the other arguments are quoted here.
	tariff := func(hsCode, origin, measureType, measureCode, rateType, pct, from, to, sourceRef string) string {
		return fmt.Sprintf("%s($1, '%s', '%s', '%s', %s, '%s', %s, NULL, NULL, NULL, 'x', '%s', %s, '%s')",
			insertTariff, hsCode, origin, measureType, measureCode, rateType, pct, from, to, sourceRef)
	}
	// The seed already holds NG 8517130000 MFN 10% [2022-01-01, 2026-07-01) and 15% from 2026-07-01,
	// and the NG_NAC_LEVY additional duty from 2022-01-01.
	cases := []struct {
		name, sql      string
		args           []any
		code, violated string
	}{
		{"tariff overlapping a seeded rule",
			tariff("8517130000", "*", "MFN", "NULL", "AD_VALOREM", "12", "2026-06-01", "'2026-08-01'", "t"),
			[]any{ngTariffDataset}, exclusionViolation, "ex_tariff_rates_no_overlap"},
		{"open-ended tariff overlapping an open-ended rule",
			tariff("8517130000", "*", "MFN", "NULL", "AD_VALOREM", "12", "2030-01-01", "NULL", "t"),
			[]any{ngTariffDataset}, exclusionViolation, "ex_tariff_rates_no_overlap"},
		{"additional duty overlapping the same programme",
			tariff("8517130000", "*", "ADDITIONAL_DUTY", "'NG_NAC_LEVY'", "AD_VALOREM", "1", "2025-01-01", "NULL", "t"),
			[]any{ngTariffDataset}, exclusionViolation, "ex_tariff_rates_no_overlap"},
		{"empty validity range",
			tariff("0101210000", "*", "MFN", "NULL", "AD_VALOREM", "5", "2026-01-01", "'2026-01-01'", "t"),
			[]any{ngTariffDataset}, checkViolation, "chk_tariff_rates_effective_range"},
		{"range ending before it starts",
			tariff("0101210000", "*", "MFN", "NULL", "AD_VALOREM", "5", "2026-02-01", "'2026-01-01'", "t"),
			[]any{ngTariffDataset}, checkViolation, "chk_tariff_rates_effective_range"},
		{"infinity instead of NULL for an open end",
			tariff("0101210000", "*", "MFN", "NULL", "AD_VALOREM", "5", "2026-01-01", "'infinity'", "t"),
			[]any{ngTariffDataset}, checkViolation, "chk_regulatory_date_finite"},
		{"odd-length HS code",
			tariff("85171", "*", "MFN", "NULL", "AD_VALOREM", "5", "2026-01-01", "NULL", "t"),
			[]any{ngTariffDataset}, checkViolation, "chk_hs_code_format"},
		{"lowercase origin country",
			tariff("0101210000", "cn", "MFN", "NULL", "AD_VALOREM", "5", "2026-01-01", "NULL", "t"),
			[]any{ngTariffDataset}, checkViolation, "chk_country_code_or_any_format"},
		{"tariff row in a sanctions dataset",
			tariff("0101210000", "*", "MFN", "NULL", "AD_VALOREM", "5", "2026-01-01", "NULL", "t"),
			[]any{usSanctionsDataset}, fkViolation, "fk_tariff_rates_dataset"},
		{"tariff row with no dataset",
			tariff("0101210000", "*", "MFN", "NULL", "AD_VALOREM", "5", "2026-01-01", "NULL", "t"),
			[]any{"00000000-0000-4000-8000-0000000000ff"}, fkViolation, "fk_tariff_rates_dataset"},
		{"ad valorem rate without a percentage",
			tariff("0101210000", "*", "MFN", "NULL", "AD_VALOREM", "NULL", "2026-01-01", "NULL", "t"),
			[]any{ngTariffDataset}, checkViolation, "chk_tariff_rates_rate_components"},
		{"unsupported measure carrying a guessed rate",
			tariff("0101210000", "*", "MFN", "NULL", "UNSUPPORTED_MEASURE", "5", "2026-01-01", "NULL", "t"),
			[]any{ngTariffDataset}, checkViolation, "chk_tariff_rates_rate_components"},
		{"compound rate without a currency", insertTariff + `($1, '0101210000', '*', 'MFN', NULL, 'COMPOUND', 5,
			10, NULL, 'KG', '5% + 10/kg', '2026-01-01', NULL, 't')`,
			[]any{ngTariffDataset}, checkViolation, "chk_tariff_rates_rate_components"},
		{"MFN rate naming a programme",
			tariff("0101210000", "*", "MFN", "'US_SEC_301'", "AD_VALOREM", "5", "2026-01-01", "NULL", "t"),
			[]any{ngTariffDataset}, checkViolation, "chk_tariff_rates_measure_code"},
		{"additional duty without a programme",
			tariff("0101210000", "*", "ADDITIONAL_DUTY", "NULL", "AD_VALOREM", "5", "2026-01-01", "NULL", "t"),
			[]any{ngTariffDataset}, checkViolation, "chk_tariff_rates_measure_code"},
		{"preferential rate for any origin",
			tariff("0101210000", "*", "PREFERENTIAL", "'ECOWAS_TLS'", "AD_VALOREM", "0", "2026-01-01", "NULL", "t"),
			[]any{ngTariffDataset}, checkViolation, "chk_tariff_rates_preferential_origin"},
		{"rule without a source reference",
			tariff("0101210000", "*", "MFN", "NULL", "AD_VALOREM", "5", "2026-01-01", "NULL", " "),
			[]any{ngTariffDataset}, checkViolation, "tariff_rates_source_reference_check"},
		{"duplicate dataset version", `INSERT INTO regulatory_datasets (jurisdiction, category, source,
			version, fetched_at, content_sha256, licence, attribution) VALUES ('NG', 'TARIFF', 'ng_cet',
			'fixture-2026-01', NOW(), sha256('other'), 'Public sector information', 'Nigeria Customs Service')`,
			nil, uniqueViolation, "uq_regulatory_datasets_version"},
		{"dataset hash that is not SHA-256", `INSERT INTO regulatory_datasets (jurisdiction, category, source,
			version, fetched_at, content_sha256, licence, attribution) VALUES ('NG', 'TARIFF', 'ng_cet',
			'fixture-2026-09', NOW(), decode(md5('x'), 'hex'), 'Public sector information', 'Nigeria Customs')`,
			nil, checkViolation, "regulatory_datasets_content_sha256_check"},
		{"dataset without a licence", `UPDATE regulatory_datasets SET licence = '' WHERE id = $1`,
			[]any{ngTariffDataset}, checkViolation, "regulatory_datasets_licence_check"},
		{"deleting a dataset that rules reference", `DELETE FROM regulatory_datasets WHERE id = $1`,
			[]any{ngTariffDataset}, fkViolation, "fk_tariff_rates_dataset"},
		{"overlapping import restriction", `INSERT INTO import_restrictions (dataset_id, hs_code,
			origin_country, description, effective_from, effective_to, source_reference) VALUES
			($1, '6309', '*', 'Used clothing', '2024-01-01', NULL, 't')`,
			[]any{ngRestrictionDataset}, exclusionViolation, "ex_import_restrictions_no_overlap"},
		{"overlapping permit requirement", `INSERT INTO permit_requirements (dataset_id, hs_code,
			origin_country, permit_code, issuing_agency, document_type, effective_from, effective_to,
			source_reference) VALUES ($1, '080450', '*', 'USDA-PPQ-587', 'USDA APHIS', 'Plant import permit',
			'2021-01-01', '2022-01-01', 't')`,
			[]any{usPermitDataset}, exclusionViolation, "ex_permit_requirements_no_overlap"},
		{"overlapping sanctions designation", insertSanction + `($1, 'FIXTURE-OFAC-0001', 'ENTITY',
			'FIXTURE EXPORT TRADING LLC', 'SDGT', '2025-01-01', NULL, 't')`,
			[]any{usSanctionsDataset}, exclusionViolation, "ex_sanctions_list_no_overlap"},
		{"overlapping export control", `INSERT INTO export_controls (dataset_id, hs_code,
			destination_country, control_code, licence_type, effective_from, effective_to, source_reference)
			VALUES ($1, '852691', 'CN', '7A994', 'BIS_LICENCE', '2024-01-01', NULL, 't')`,
			[]any{usExportDataset}, exclusionViolation, "ex_export_controls_no_overlap"},
		{"overlapping agreement partner", `INSERT INTO preferential_agreements (dataset_id, agreement_code,
			partner_country, proof_of_origin, effective_from, effective_to, source_reference) VALUES
			($1, 'UK_EU_TCA', 'DE', 'Importer''s knowledge', '2022-01-01', NULL, 't')`,
			[]any{gbAgreementDataset}, exclusionViolation, "ex_preferential_agreements_no_overlap"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			withRegulatorySeed(t, pool, func(ctx context.Context, tx pgx.Tx) {
				_, err := tx.Exec(ctx, tc.sql, tc.args...)
				requirePgError(t, err, tc.code, tc.violated)
			})
		})
	}
}
