//go:build perf

// Package performance checks the query plans of the statements production code sends, on a
// dataset seeded at representative scale (seed.sql). It is a separate job, not a verify gate:
// plan shape is only meaningful at scale, and timings are recorded, never asserted.
//
//	make perf    # TEST_DATABASE_URL must point at a throwaway database: every table is truncated
package performance

import (
	"context"
	_ "embed"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"

	appOutbox "github.com/ayo6706/cross-border-ecommerce/internal/application/outbox"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/regulatory"
	"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/postgres"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/config"
	"github.com/ayo6706/cross-border-ecommerce/internal/testsupport"
	"github.com/ayo6706/cross-border-ecommerce/internal/wiring"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

//go:embed seed.sql
var seedSQL string

const (
	allTables = "product_changes, product_sources, product_versions, products, raw_records, " +
		"ingestion_run_processing, ingestion_runs, outbox_events, dlq_messages, idempotency_keys, sources, " +
		"regulatory_datasets, tariff_rates, import_restrictions, permit_requirements, sanctions_list, " +
		"export_controls, preferential_agreements"
	seedTimeout = 10 * time.Minute
	flowTimeout = 5 * time.Minute
)

// largeTables are the tables seed.sql fills at scale; every read of them must use an index.
var largeTables = map[string]bool{
	"ingestion_runs": true, "ingestion_run_processing": true, "raw_records": true, "products": true,
	"product_versions": true, "product_sources": true, "product_changes": true, "outbox_events": true,
	"tariff_rates": true,
}

type planEnv struct {
	admin    *pgxpool.Pool // untraced: seeds and runs EXPLAIN
	traced   *pgxpool.Pool
	recorder *statementRecorder
	cfg      *config.Config
	logger   *slog.Logger
}

type flow struct {
	name string
	run  func(t *testing.T, ctx context.Context)
}

func TestQueryPlans(t *testing.T) {
	env := setupPlanEnv(t)
	t.Log("| flow | statement | table access (index) | exec ms | planning ms | shared hit | shared read |")
	t.Log("|---|---|---|---|---|---|---|")
	// The flows share the seeded state and run in this order: the worker step processes the run
	// that ingestion_complete_run finished, so it cannot be selected alone with -run.
	for _, f := range env.flows() {
		t.Run(f.name, func(t *testing.T) { env.profile(t, f) })
	}
}

func setupPlanEnv(t *testing.T) *planEnv {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("TEST_DATABASE_URL is required: the query-plan suite does not skip")
	}
	admin := testsupport.LiveDB(t)
	testsupport.Truncate(t, admin, allTables)
	seed(t, admin)

	cfg, err := config.LoadFromLookup(func(key string) string {
		if key == "DATABASE_URL" {
			return dsn
		}
		return ""
	})
	require.NoError(t, err, "production defaults must load")

	recorder := &statementRecorder{}
	return &planEnv{
		admin:    admin,
		traced:   tracedPool(t, dsn, recorder),
		recorder: recorder,
		cfg:      cfg,
		logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func (e *planEnv) flows() []flow {
	return []flow{
		{"ingestion_complete_run", func(t *testing.T, ctx context.Context) { completeRun(t, ctx, e.traced) }},
		{"worker_process_next", func(t *testing.T, ctx context.Context) { processNext(t, ctx, e.traced, e.cfg) }},
		{"outbox_relay_run_once", func(t *testing.T, ctx context.Context) { relayOnce(t, ctx, e.traced, e.cfg, e.logger) }},
		{"api_list_and_get_products", func(t *testing.T, _ context.Context) { browseProducts(t, e.traced, e.logger) }},
		{"regulatory_resolve_tariff", func(t *testing.T, ctx context.Context) { resolveTariff(t, ctx, e.traced) }},
	}
}

// profile runs f, then checks the custom plan (EXPLAIN ANALYZE with the recorded arguments) and
// the generic plan of every statement it sent.
func (e *planEnv) profile(t *testing.T, f flow) {
	ctx, cancel := context.WithTimeout(context.Background(), flowTimeout)
	defer cancel()
	e.recorder.take()
	f.run(t, ctx)
	statements := e.recorder.take()
	require.NotEmpty(t, statements, "the flow sent no statement")
	for _, s := range statements {
		name := firstLine(s.sql)
		plan, err := explain(ctx, e.admin, s)
		require.NoError(t, err, name)
		requireIndexAccess(t, name, plan, largeTables)
		generic, err := explainGeneric(ctx, e.admin, s)
		require.NoError(t, err, name+" (generic plan)")
		requireIndexAccess(t, name+" (generic plan)", generic, largeTables)
		t.Logf("| %s | %s | %s | %.2f | %.2f | %d | %d |", f.name, name, describeAccesses(plan),
			plan.ExecutionTime, plan.PlanningTime, plan.Plan.SharedHit, plan.Plan.SharedRead)
	}
}

func seed(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), seedTimeout)
	defer cancel()
	start := time.Now()
	_, err := pool.Exec(ctx, seedSQL)
	require.NoError(t, err, "seed dataset")
	_, err = pool.Exec(ctx, "VACUUM ANALYZE")
	require.NoError(t, err, "vacuum analyze")
	t.Logf("seeded in %s", time.Since(start).Round(time.Second))
}

// tracedPool is a second pool on the same database whose statements the recorder captures.
func tracedPool(t *testing.T, dsn string, recorder *statementRecorder) *pgxpool.Pool {
	t.Helper()
	poolCfg, err := pgxpool.ParseConfig(dsn)
	require.NoError(t, err)
	poolCfg.ConnConfig.Tracer = recorder
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	require.NoError(t, pool.Ping(ctx))
	return pool
}

// completeRun finishes the seeded RUNNING run the way the ingestion service does, which queues it
// for processing.
func completeRun(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	runs, err := postgres.NewIngestionRepository(pool)
	require.NoError(t, err)
	run, err := runs.FindLatestRunBySource(ctx, "src-perf")
	require.NoError(t, err)
	require.NoError(t, run.Complete("", time.Now().UTC()))
	require.NoError(t, runs.UpdateStatus(ctx, run, ingestion.StatusRunning))
}

// processNext is one worker loop step with the worker's configuration.
func processNext(t *testing.T, ctx context.Context, pool *pgxpool.Pool, cfg *config.Config) {
	processor, err := wiring.RunProcessor(pool)
	require.NoError(t, err)
	opts, err := wiring.ProcessRunOptions(cfg.RunProcessing, false)
	require.NoError(t, err)

	result, err := processor.ProcessNext(ctx, opts)
	require.NoError(t, err)
	require.NotNil(t, result, "completeRun queued the seeded run")
	require.Equal(t, 10000, result.Seen)
	require.Equal(t, 5000, result.New)
	require.Equal(t, 5000, result.Changed)
}

type acceptAll struct{}

func (acceptAll) Publish(context.Context, appOutbox.Event) error { return nil }

// relayOnce claims and publishes one outbox batch with the worker's relay configuration.
func relayOnce(t *testing.T, ctx context.Context, pool *pgxpool.Pool, cfg *config.Config, logger *slog.Logger) {
	repo, err := postgres.NewOutboxRepository(pool)
	require.NoError(t, err)
	relay, err := appOutbox.NewRelay(repo, acceptAll{}, appOutbox.RelayConfig{
		BatchSize:    cfg.Outbox.BatchSize,
		PollInterval: cfg.Outbox.PollInterval,
		Lease:        cfg.Outbox.Lease,
		BaseBackoff:  cfg.Outbox.BaseBackoff,
		MaxBackoff:   cfg.Outbox.MaxBackoff,
		MaxAttempts:  cfg.Outbox.MaxAttempts,
	}, logger)
	require.NoError(t, err)
	claimed, published, err := relay.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, cfg.Outbox.BatchSize, claimed)
	require.Equal(t, claimed, published)
}

// browseProducts reads the first two catalogue pages and one product through the API router.
func browseProducts(t *testing.T, pool *pgxpool.Pool, logger *slog.Logger) {
	handler, err := wiring.APIHandler(logger, pool)
	require.NoError(t, err)

	var page struct {
		Items      []struct{ ID string } `json:"items"`
		NextCursor *string               `json:"next_cursor"`
	}
	getJSON(t, handler, "/v1/products?limit=50", &page)
	require.Len(t, page.Items, 50)
	require.NotNil(t, page.NextCursor)
	getJSON(t, handler, "/v1/products?limit=50&cursor="+url.QueryEscape(*page.NextCursor), &page)
	require.Len(t, page.Items, 50)

	var product struct{ ID string }
	getJSON(t, handler, "/v1/products/"+page.Items[0].ID, &product)
	require.Equal(t, page.Items[0].ID, product.ID)
}

// resolveTariff resolves a seeded line under the active usitc_hts version and the overlay; the nine
// superseded versions hold the same line.
func resolveTariff(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	slaByCategory := map[regulatory.Category]time.Duration{}
	for _, c := range regulatory.Categories() {
		slaByCategory[c] = 24 * time.Hour
	}
	slas, err := regulatory.NewSLAs(slaByCategory)
	require.NoError(t, err)
	svc, err := wiring.RegulatoryService(pool, slas)
	require.NoError(t, err)
	q, err := regulatory.NewTariffQuery(regulatory.TariffQueryParams{
		Destination:   "US",
		Origin:        "CN",
		HSCode:        "8400000028",
		TransactionAt: time.Date(2026, 3, 15, 10, 0, 0, 0, time.UTC),
		EvaluatedAt:   time.Now(),
	})
	require.NoError(t, err)

	r, err := svc.ResolveTariff(ctx, q)
	require.NoError(t, err)
	require.Len(t, r.Measures, 2, "MFN and the Section 301 overlay")
	require.Equal(t, "11.5", r.AdValoremPercent().String(), "4% MFN + 7.5% overlay")
}

func getJSON(t *testing.T, handler http.Handler, target string, into any) {
	t.Helper()
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, http.NoBody))
	require.Equal(t, http.StatusOK, rec.Code, "GET %s: %s", target, rec.Body.String())
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), into))
}
