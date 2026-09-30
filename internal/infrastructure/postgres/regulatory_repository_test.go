package postgres_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	appRegulatory "github.com/ayo6706/cross-border-ecommerce/internal/application/regulatory"
	domain "github.com/ayo6706/cross-border-ecommerce/internal/domain/regulatory"
	"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/postgres"
	"github.com/ayo6706/cross-border-ecommerce/internal/testsupport"
	"github.com/jackc/pgx/v5/pgxpool"
)

const regulatoryTables = "regulatory_datasets, tariff_rates, import_restrictions, permit_requirements, " +
	"sanctions_list, export_controls, preferential_agreements, outbox_events"

func newRegulatoryService(t *testing.T, sla time.Duration) (*appRegulatory.Service, *pgxpool.Pool) {
	t.Helper()
	pool := testsupport.LiveDB(t)
	testsupport.Truncate(t, pool, regulatoryTables)
	tx, err := postgres.NewRegulatoryTxManager(pool)
	if err != nil {
		t.Fatalf("tx manager: %v", err)
	}
	repo, err := postgres.NewRegulatoryRepository(pool)
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	slaByCategory := map[domain.Category]time.Duration{}
	for _, c := range domain.Categories() {
		slaByCategory[c] = sla
	}
	slas, err := domain.NewSLAs(slaByCategory)
	if err != nil {
		t.Fatalf("slas: %v", err)
	}
	svc, err := appRegulatory.NewService(tx, repo, repo, slas)
	if err != nil {
		t.Fatalf("service: %v", err)
	}
	return svc, pool
}

// restrictionLoad is a one-rule NG curated restriction file; version and fetchedAt vary per test.
func restrictionLoad(version string, fetchedAt time.Time) appRegulatory.CuratedLoad {
	validity, _ := domain.NewValidity(time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC), nil)
	return appRegulatory.CuratedLoad{
		Dataset: domain.NewDatasetParams{
			Jurisdiction: "NG", Category: domain.CategoryImportRestriction, Source: "ng_prohibition_list",
			Version: version, FetchedAt: fetchedAt, ContentSHA256: sha256.Sum256([]byte(version)),
			Licence: "Public sector information", Attribution: "Nigeria Customs Service", LoadedBy: "alice",
		},
		Rules: domain.CuratedRules{Restrictions: []domain.ImportRestriction{{
			HSCode: "6309", OriginCountry: "*", Description: "Used clothing",
			SourceReference: "NCS import prohibition list, item 13", Validity: validity,
		}}},
	}
}

// loadReviewed loads and signs off a restriction dataset, ready to activate.
func loadReviewed(ctx context.Context, t *testing.T, svc *appRegulatory.Service, version string) *domain.Dataset {
	t.Helper()
	d, err := svc.LoadCurated(ctx, restrictionLoad(version, time.Now()))
	if err != nil {
		t.Fatalf("load %s: %v", version, err)
	}
	if _, err := svc.Review(ctx, d.ID, "bob", "checked against the published list"); err != nil {
		t.Fatalf("review %s: %v", version, err)
	}
	return d
}

func datasetStatus(ctx context.Context, t *testing.T, pool *pgxpool.Pool, id string) string {
	t.Helper()
	var status string
	if err := pool.QueryRow(ctx, "SELECT status FROM regulatory_datasets WHERE id = $1", id).Scan(&status); err != nil {
		t.Fatalf("read status of %s: %v", id, err)
	}
	return status
}

func TestRegulatoryService_ActivateSupersedesPreviousAndWritesEvent_Live(t *testing.T) {
	svc, pool := newRegulatoryService(t, 24*time.Hour)
	ctx := context.Background()
	v1 := loadReviewed(ctx, t, svc, "2026-08")
	v2 := loadReviewed(ctx, t, svc, "2026-09")

	if _, err := svc.Activate(ctx, v1.ID); err != nil {
		t.Fatalf("activate v1: %v", err)
	}
	if _, err := svc.Activate(ctx, v2.ID); err != nil {
		t.Fatalf("activate v2: %v", err)
	}

	if got := datasetStatus(ctx, t, pool, v1.ID); got != "SUPERSEDED" {
		t.Errorf("v1 status = %s, want SUPERSEDED", got)
	}
	if got := datasetStatus(ctx, t, pool, v2.ID); got != "ACTIVE" {
		t.Errorf("v2 status = %s, want ACTIVE", got)
	}
	rows, err := pool.Query(ctx, `SELECT aggregate_id, payload FROM outbox_events
		WHERE event_type = 'regulatory.dataset_activated' ORDER BY created_at, aggregate_id`)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	defer rows.Close()
	var previous []*string
	for rows.Next() {
		var aggregate string
		var payload []byte
		if err := rows.Scan(&aggregate, &payload); err != nil {
			t.Fatalf("scan event: %v", err)
		}
		var p struct {
			DatasetID         string  `json:"dataset_id"`
			PreviousDatasetID *string `json:"previous_dataset_id"`
		}
		if err := json.Unmarshal(payload, &p); err != nil {
			t.Fatalf("decode payload: %v", err)
		}
		if p.DatasetID != aggregate {
			t.Errorf("payload dataset %s != aggregate %s", p.DatasetID, aggregate)
		}
		previous = append(previous, p.PreviousDatasetID)
	}
	if len(previous) != 2 || previous[0] != nil || previous[1] == nil || *previous[1] != v1.ID {
		t.Fatalf("events' previous ids = %v, want [nil, %s]", previous, v1.ID)
	}
}

func TestRegulatoryService_CoverageResolvesVersionActiveAtT_Live(t *testing.T) {
	svc, _ := newRegulatoryService(t, 24*time.Hour)
	ctx := context.Background()
	activate := func(version string) *domain.Dataset {
		d, err := svc.Activate(ctx, loadReviewed(ctx, t, svc, version).ID)
		if err != nil {
			t.Fatalf("activate %s: %v", version, err)
		}
		return d
	}
	v1 := activate("2026-08")
	v2 := activate("2026-09")

	// The returned activation times equal the stored ones (the service clock is microsecond-precise).
	cases := map[string]struct {
		at      time.Time
		want    string
		wantErr error
	}{
		"just before v1's activation":                 {v1.ActivatedAt.Add(-time.Microsecond), "", domain.ErrNoCoverage},
		"exactly v1's activation":                     {*v1.ActivatedAt, v1.ID, nil},
		"just before v2 supersedes v1":                {v2.ActivatedAt.Add(-time.Microsecond), v1.ID, nil},
		"exactly v2's activation (v1's supersession)": {*v2.ActivatedAt, v2.ID, nil},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			active, err := svc.Coverage(ctx, "ng", domain.CategoryImportRestriction, tc.at)
			if !errors.Is(err, tc.wantErr) || (tc.wantErr == nil && err != nil) {
				t.Fatalf("want %v, got %v", tc.wantErr, err)
			}
			var ids []string
			for _, d := range active {
				ids = append(ids, d.ID)
			}
			if want := []string{tc.want}; tc.want == "" && len(ids) != 0 || tc.want != "" && !slices.Equal(ids, want) {
				t.Fatalf("active = %v, want [%s]", ids, tc.want)
			}
		})
	}
}

func TestRegulatoryService_StaleCoverage_Live(t *testing.T) {
	svc, _ := newRegulatoryService(t, 7*24*time.Hour)
	ctx := context.Background()
	d, err := svc.LoadCurated(ctx, restrictionLoad("2026-08", time.Now().Add(-8*24*time.Hour)))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if _, err := svc.Review(ctx, d.ID, "bob", "ok"); err != nil {
		t.Fatalf("review: %v", err)
	}
	if _, err := svc.Activate(ctx, d.ID); err != nil {
		t.Fatalf("activate: %v", err)
	}
	_, err = svc.Coverage(ctx, "NG", domain.CategoryImportRestriction, time.Now())
	if reason, _ := domain.HoldReason(err); !errors.Is(err, domain.ErrStaleCoverage) || reason != domain.ReasonStaleData {
		t.Fatalf("want ErrStaleCoverage / %s, got %v (%q)", domain.ReasonStaleData, err, reason)
	}
}

func TestRegulatoryService_RefusedTransitionsChangeNothing_Live(t *testing.T) {
	svc, pool := newRegulatoryService(t, 24*time.Hour)
	ctx := context.Background()

	rejected := loadReviewed(ctx, t, svc, "rejected")
	if _, err := svc.Reject(ctx, rejected.ID, "partial upstream file"); err != nil {
		t.Fatalf("reject: %v", err)
	}
	superseded := loadReviewed(ctx, t, svc, "superseded")
	current := loadReviewed(ctx, t, svc, "current")
	for _, id := range []string{superseded.ID, current.ID} {
		if _, err := svc.Activate(ctx, id); err != nil {
			t.Fatalf("activate %s: %v", id, err)
		}
	}
	unreviewed, err := svc.LoadCurated(ctx, restrictionLoad("unreviewed", time.Now()))
	if err != nil {
		t.Fatalf("load unreviewed: %v", err)
	}

	cases := map[string]struct {
		id   string
		want error
	}{
		"activate REJECTED":       {rejected.ID, domain.ErrInvalidTransition},
		"activate SUPERSEDED":     {superseded.ID, domain.ErrInvalidTransition},
		"activate unreviewed":     {unreviewed.ID, domain.ErrReviewRequired},
		"activate unknown":        {"00000000-0000-4000-8000-0000000000ff", domain.ErrDatasetNotFound},
		"activate malformed id":   {"not-a-uuid", domain.ErrDatasetNotFound},
		"activate current ACTIVE": {current.ID, domain.ErrInvalidTransition},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := svc.Activate(ctx, tc.id); !errors.Is(err, tc.want) {
				t.Fatalf("want %v, got %v", tc.want, err)
			}
		})
	}
	for id, want := range map[string]string{rejected.ID: "REJECTED", superseded.ID: "SUPERSEDED",
		current.ID: "ACTIVE", unreviewed.ID: "LOADED"} {
		if got := datasetStatus(ctx, t, pool, id); got != want {
			t.Errorf("dataset %s status = %s, want %s", id, got, want)
		}
	}
	var events int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM outbox_events").Scan(&events); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if events != 2 {
		t.Fatalf("outbox events = %d, want 2 (only the two successful activations)", events)
	}
}

func TestRegulatoryService_LoadCuratedIsAllOrNothing_Live(t *testing.T) {
	svc, pool := newRegulatoryService(t, 24*time.Hour)
	ctx := context.Background()

	overlapping := restrictionLoad("overlap", time.Now())
	overlapping.Rules.Restrictions = append(overlapping.Rules.Restrictions, overlapping.Rules.Restrictions[0])
	badCode := restrictionLoad("bad-code", time.Now())
	badCode.Rules.Restrictions[0].HSCode = "63.09"

	for name, load := range map[string]appRegulatory.CuratedLoad{"overlapping rows": overlapping, "malformed HS code": badCode} {
		t.Run(name, func(t *testing.T) {
			if _, err := svc.LoadCurated(ctx, load); !errors.Is(err, domain.ErrInvalidRule) {
				t.Fatalf("want ErrInvalidRule, got %v", err)
			}
		})
	}
	var datasets int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM regulatory_datasets").Scan(&datasets); err != nil {
		t.Fatalf("count datasets: %v", err)
	}
	if datasets != 0 {
		t.Fatalf("a failed load left %d dataset rows", datasets)
	}

	if _, err := svc.LoadCurated(ctx, restrictionLoad("dup", time.Now())); err != nil {
		t.Fatalf("load: %v", err)
	}
	if _, err := svc.LoadCurated(ctx, restrictionLoad("dup", time.Now())); !errors.Is(err, domain.ErrDuplicateVersion) {
		t.Fatalf("second load of a version: want ErrDuplicateVersion, got %v", err)
	}
}

// Two transactions activate different versions of one key without seeing each other's write. The
// partial unique index decides, whichever order the statements reach the server in.
func TestRegulatoryRepository_ConcurrentActivationLosesToUniqueIndex_Live(t *testing.T) {
	svc, pool := newRegulatoryService(t, 24*time.Hour)
	ctx := context.Background()
	v1 := loadReviewed(ctx, t, svc, "2026-08")
	v2 := loadReviewed(ctx, t, svc, "2026-09")

	activateInTx := func(id string) (commit func() error, save func() error) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
		repo, err := postgres.NewRegulatoryRepository(tx)
		if err != nil {
			t.Fatalf("repository: %v", err)
		}
		d, err := repo.GetForUpdate(ctx, id)
		if err != nil {
			t.Fatalf("get %s: %v", id, err)
		}
		if err := d.Activate(time.Now()); err != nil {
			t.Fatalf("activate %s: %v", id, err)
		}
		return func() error { return tx.Commit(ctx) }, func() error { return repo.SaveLifecycle(ctx, d, domain.StatusLoaded) }
	}
	commit1, save1 := activateInTx(v1.ID)
	_, save2 := activateInTx(v2.ID)

	if err := save1(); err != nil {
		t.Fatalf("first activation: %v", err)
	}
	second := make(chan error, 1)
	go func() { second <- save2() }() // may block on the index entry until the first transaction ends
	if err := commit1(); err != nil {
		t.Fatalf("commit first activation: %v", err)
	}
	if err := <-second; !errors.Is(err, domain.ErrConcurrentActivation) {
		t.Fatalf("second activation: want ErrConcurrentActivation, got %v", err)
	}
}

// The entity refuses these transitions first; these writes bypass it to prove the database
// refuses them on its own.
func TestRegulatoryRepository_DatabaseEnforcesReviewRules_Live(t *testing.T) {
	svc, pool := newRegulatoryService(t, 24*time.Hour)
	ctx := context.Background()
	repo, err := postgres.NewRegulatoryRepository(pool)
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	now := time.Now().UTC()
	cases := map[string]struct {
		bypass func(d *domain.Dataset)
		want   error
	}{
		"activated without a review": {func(d *domain.Dataset) {
			d.Status, d.ActivatedAt = domain.StatusActive, &now
		}, domain.ErrReviewRequired},
		"reviewed by the loader": {func(d *domain.Dataset) {
			d.Review = &domain.Review{By: " ALICE ", Note: "self", At: now}
		}, domain.ErrReviewerIsLoader},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			d, err := svc.LoadCurated(ctx, restrictionLoad(name, now))
			if err != nil {
				t.Fatalf("load: %v", err)
			}
			tc.bypass(d)
			if err := repo.SaveLifecycle(ctx, d, domain.StatusLoaded); !errors.Is(err, tc.want) {
				t.Fatalf("want %v, got %v", tc.want, err)
			}
		})
	}
}

func TestRegulatoryRules_FrozenOnceDatasetLeavesLoaded_Live(t *testing.T) {
	svc, pool := newRegulatoryService(t, 24*time.Hour)
	ctx := context.Background()
	d := loadReviewed(ctx, t, svc, "2026-09")
	const editWhileLoaded = `UPDATE import_restrictions SET description = 'Used clothing and shoes' WHERE dataset_id = $1`
	if _, err := pool.Exec(ctx, editWhileLoaded, d.ID); err != nil {
		t.Fatalf("rules of a LOADED dataset must be editable: %v", err)
	}
	if _, err := svc.Activate(ctx, d.ID); err != nil {
		t.Fatalf("activate: %v", err)
	}

	for name, sql := range map[string]string{
		"insert": `INSERT INTO import_restrictions (dataset_id, hs_code, origin_country, description,
			effective_from, source_reference) VALUES ($1, '6310', '*', 'Rags', '2019-01-01', 't')`,
		"update": `UPDATE import_restrictions SET description = 'changed' WHERE dataset_id = $1`,
		"delete": `DELETE FROM import_restrictions WHERE dataset_id = $1`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := pool.Exec(ctx, sql, d.ID)
			requirePgError(t, err, "55000", "")
		})
	}
}

func TestRegulatoryRepository_InsertCuratedRefusesEmptyRules_Live(t *testing.T) {
	svc, pool := newRegulatoryService(t, 24*time.Hour)
	ctx := context.Background()
	d, err := svc.LoadCurated(ctx, restrictionLoad("2026-09", time.Now()))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	repo, err := postgres.NewRegulatoryRepository(pool)
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	if err := repo.InsertCurated(ctx, d.ID, domain.CuratedRules{}); !errors.Is(err, domain.ErrInvalidRule) {
		t.Fatalf("empty rules: want ErrInvalidRule, got %v", err)
	}
}

// The activation event is written after the supersede and the activation; when it fails, both
// roll back with it.
func TestRegulatoryService_ActivationRollsBackWhenTheEventFails_Live(t *testing.T) {
	svc, pool := newRegulatoryService(t, 24*time.Hour)
	ctx := context.Background()
	v1 := loadReviewed(ctx, t, svc, "2026-08")
	v2 := loadReviewed(ctx, t, svc, "2026-09")
	if _, err := svc.Activate(ctx, v1.ID); err != nil {
		t.Fatalf("activate v1: %v", err)
	}

	const failEvents = `CREATE OR REPLACE FUNCTION test_fail_dataset_event() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'injected outbox failure' USING ERRCODE = 'XX999'; END $$;
		DROP TRIGGER IF EXISTS test_fail_dataset_event ON outbox_events;
		CREATE TRIGGER test_fail_dataset_event BEFORE INSERT ON outbox_events
		FOR EACH ROW WHEN (NEW.event_type = 'regulatory.dataset_activated')
		EXECUTE FUNCTION test_fail_dataset_event();`
	if _, err := pool.Exec(ctx, failEvents); err != nil {
		t.Fatalf("install fault: %v", err)
	}
	t.Cleanup(func() {
		const drop = `DROP TRIGGER IF EXISTS test_fail_dataset_event ON outbox_events;
			DROP FUNCTION IF EXISTS test_fail_dataset_event();`
		if _, err := pool.Exec(context.Background(), drop); err != nil {
			t.Errorf("remove fault: %v", err)
		}
	})

	_, err := svc.Activate(ctx, v2.ID)
	requirePgError(t, err, "XX999", "")
	for id, want := range map[string]string{v1.ID: "ACTIVE", v2.ID: "LOADED"} {
		if got := datasetStatus(ctx, t, pool, id); got != want {
			t.Errorf("dataset %s status = %s, want %s", id, got, want)
		}
	}
}
