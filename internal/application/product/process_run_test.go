package product_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	appIngestion "github.com/ayo6706/cross-border-ecommerce/internal/application/ingestion"
	appProduct "github.com/ayo6706/cross-border-ecommerce/internal/application/product"
	domainIngestion "github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
	domainProduct "github.com/ayo6706/cross-border-ecommerce/internal/domain/product"
	domainSource "github.com/ayo6706/cross-border-ecommerce/internal/domain/source"
	"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/postgres"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/uuid"
	"github.com/ayo6706/cross-border-ecommerce/migrations"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type feedBatchAdapter struct {
	batches []domainIngestion.FetchResult
	idx     int
}

func (a *feedBatchAdapter) Fetch(ctx context.Context, req domainIngestion.FetchRequest) (domainIngestion.FetchResult, error) {
	if a.idx >= len(a.batches) {
		return domainIngestion.FetchResult{HasMore: false}, nil
	}
	res := a.batches[a.idx]
	a.idx++
	return res, nil
}

type cancellingTxRunner struct {
	inner      appProduct.TxRunner
	cancelFunc context.CancelFunc
	target     int
	mu         sync.Mutex
	count      int
}

func (c *cancellingTxRunner) WithinTx(ctx context.Context, fn func(repos appProduct.TxRepos) error) error {
	err := c.inner.WithinTx(ctx, fn)
	if err == nil {
		c.mu.Lock()
		c.count++
		if c.count == c.target {
			c.cancelFunc()
		}
		c.mu.Unlock()
	}
	return err
}

type testEnv struct {
	pool           *pgxpool.Pool
	processor      *appProduct.RunProcessor
	runRepo        domainIngestion.Repository
	sourceRepo     domainSource.Repository
	rawRepo        domainIngestion.RawRecordRepository
	processingRepo domainIngestion.RunProcessingRepository
	productRepo    domainProduct.Repository
	txRunner       appProduct.TxRunner
	ingestTxRunner appIngestion.TxRunner
}

const truncateLiveTables = "TRUNCATE sources, ingestion_runs, raw_records, products, product_versions, " +
	"product_sources, product_changes, outbox_events, ingestion_run_processing CASCADE"

// setupLiveTestEnv shares TEST_DATABASE_URL with internal/infrastructure/postgres, whose
// cleanup truncates the same tables; integration runs must use `go test -p 1`.
func setupLiveTestEnv(t *testing.T) *testEnv {
	t.Helper()

	connStr := os.Getenv("TEST_DATABASE_URL")
	if connStr == "" {
		t.Skip("skipping live database test: TEST_DATABASE_URL not set")
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := postgres.NewPool(ctx, connStr,
		postgres.WithConnectTimeout(3*time.Second),
		postgres.WithMaxConns(10),
		postgres.WithMinConns(2),
	)
	if err != nil {
		t.Skipf("skipping live database test: unable to connect to %s: %v", connStr, err)
		return nil
	}

	t.Cleanup(func() {
		cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanCancel()
		if _, err := pool.Exec(cleanCtx, truncateLiveTables); err != nil {
			t.Errorf("truncate live tables on cleanup: %v", err)
		}
		pool.Close()
	})

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("expected successful pool ping: %v", err)
	}

	migrator, err := postgres.NewMigrator(pool, migrations.FS)
	if err != nil {
		t.Fatalf("failed to create migrator: %v", err)
	}
	if err := migrator.Up(ctx); err != nil {
		t.Fatalf("failed to apply migrations: %v", err)
	}
	// Clear leftovers from an aborted earlier run; the tests assert global row counts.
	if _, err := pool.Exec(ctx, truncateLiveTables); err != nil {
		t.Fatalf("truncate live tables on setup: %v", err)
	}

	runRepo, err := postgres.NewIngestionRepository(pool)
	require.NoError(t, err)
	sourceRepo, err := postgres.NewSourceRepository(pool)
	require.NoError(t, err)
	rawRepo, err := postgres.NewRawRecordRepository(pool)
	require.NoError(t, err)
	processingRepo, err := postgres.NewRunProcessingRepository(pool)
	require.NoError(t, err)
	productRepo, err := postgres.NewProductRepository(pool)
	require.NoError(t, err)
	txRunner, err := postgres.NewProductTxManager(pool)
	require.NoError(t, err)
	ingestTxRunner, err := postgres.NewTxManager(pool)
	require.NoError(t, err)

	processor, err := appProduct.NewRunProcessor(runRepo, sourceRepo, rawRepo, processingRepo, txRunner)
	require.NoError(t, err)

	return &testEnv{
		pool:           pool,
		processor:      processor,
		runRepo:        runRepo,
		sourceRepo:     sourceRepo,
		rawRepo:        rawRepo,
		processingRepo: processingRepo,
		productRepo:    productRepo,
		txRunner:       txRunner,
		ingestTxRunner: ingestTxRunner,
	}
}

func defaultFieldMapping() *domainProduct.FieldMapping {
	return &domainProduct.FieldMapping{
		NamePath:          "title",
		DescriptionPath:   "body",
		BrandPath:         "vendor",
		OriginCountryPath: "country_code",
		AttributePaths: map[string]string{
			"color": "details.color",
			"size":  "details.size",
		},
	}
}

func createTestSource(t *testing.T, ctx context.Context, env *testEnv, sourceID string) *domainSource.Source {
	t.Helper()
	fm := defaultFieldMapping()
	src, err := domainSource.NewSource(
		domainSource.ID(sourceID),
		"Test Supplier",
		domainSource.TypeAPI,
		map[string]any{
			"base_url": "https://api.supplier.com/v1",
			"field_mapping": map[string]any{
				"name_path":           fm.NamePath,
				"description_path":    fm.DescriptionPath,
				"brand_path":          fm.BrandPath,
				"origin_country_path": fm.OriginCountryPath,
				"attribute_paths": map[string]any{
					"color": "details.color",
					"size":  "details.size",
				},
			},
		},
		100,
	)
	require.NoError(t, err)
	require.NoError(t, env.sourceRepo.Save(ctx, src))
	return src
}

func createTestRun(t *testing.T, ctx context.Context, env *testEnv, sourceID string) *domainIngestion.IngestionRun {
	t.Helper()
	run, err := domainIngestion.NewRun("", domainSource.ID(sourceID), "")
	require.NoError(t, err)
	require.NoError(t, env.runRepo.CreateRun(ctx, run))
	require.NoError(t, run.Start(time.Now().UTC()))
	require.NoError(t, env.runRepo.UpdateStatus(ctx, run, domainIngestion.StatusPending))
	require.NoError(t, run.Complete("", time.Now().UTC()))
	require.NoError(t, env.runRepo.UpdateStatus(ctx, run, domainIngestion.StatusRunning))
	return run
}

func TestProcessRunRecords_NewProduct(t *testing.T) {
	env := setupLiveTestEnv(t)
	ctx := context.Background()

	sourceID := "src-a1-test"
	createTestSource(t, ctx, env, sourceID)
	run := createTestRun(t, ctx, env, sourceID)

	payload := []byte(`{
		"title": "Sony Bravia 65 Inch 4K TV",
		"body": "Stunning 4K HDR Smart Google TV",
		"vendor": "Sony",
		"country_code": "JP",
		"details": {
			"color": "Black",
			"size": "65"
		}
	}`)

	now := time.Now().UTC()
	record, err := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
		SourceID:          domainSource.ID(sourceID),
		ExternalProductID: "ext-prod-101",
		Payload:           payload,
		SourceUpdatedAt:   &now,
		IngestionRunID:    run.ID,
		ReceivedAt:        now,
	})
	require.NoError(t, err)
	require.NoError(t, env.rawRepo.Save(ctx, record))

	// Execute ProcessRun
	result, err := env.processor.ProcessRun(ctx, run.ID, appProduct.ProcessRunOptions{})
	require.NoError(t, err)

	assert.Equal(t, 1, result.RecordsSeen)
	assert.Equal(t, 1, result.RecordsNew)
	assert.Equal(t, 0, result.RecordsChanged)
	assert.Equal(t, 0, result.RecordsUnchanged)
	assert.Equal(t, 0, result.RecordsFailed)

	// Verify Snapshot / Identity
	snap, err := env.productRepo.FindSnapshotByIdentity(ctx, sourceID, "ext-prod-101")
	require.NoError(t, err)
	require.NotNil(t, snap)
	assert.NotEmpty(t, snap.ProductID)
	require.NotNil(t, snap.CurrentVersionID)
	assert.NotEmpty(t, snap.CurrentFingerprint)
	require.NotNil(t, snap.StoredCurrentVersion)
	assert.Equal(t, 1, snap.StoredCurrentVersion.VersionNumber)
	assert.Equal(t, "Sony Bravia 65 Inch 4K TV", snap.StoredCurrentVersion.CanonicalName)
	assert.Equal(t, "JP", snap.StoredCurrentVersion.OriginCountry)

	// Verify Outbox Event created
	var count int
	err = env.pool.QueryRow(ctx, "SELECT count(*) FROM outbox_events WHERE aggregate_type = 'product' AND aggregate_id = $1", string(snap.ProductID)).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Verify Product Change created
	var changeType string
	err = env.pool.QueryRow(ctx, "SELECT change_type FROM product_changes WHERE product_id = $1", string(snap.ProductID)).Scan(&changeType)
	require.NoError(t, err)
	assert.Equal(t, "NEW", changeType)
}

func TestProcessRunRecords_RejectsNonTerminalRun(t *testing.T) {
	env := setupLiveTestEnv(t)
	ctx := context.Background()

	sourceID := "src-running-reject"
	createTestSource(t, ctx, env, sourceID)

	run, err := domainIngestion.NewRun("", domainSource.ID(sourceID), "")
	require.NoError(t, err)
	require.NoError(t, env.runRepo.CreateRun(ctx, run))
	require.NoError(t, run.Start(time.Now().UTC()))
	require.NoError(t, env.runRepo.UpdateStatus(ctx, run, domainIngestion.StatusPending))

	_, err = env.processor.ProcessRun(ctx, run.ID, appProduct.ProcessRunOptions{})
	require.ErrorIs(t, err, domainIngestion.ErrInvalidRunState)

	var processingRows int
	require.NoError(t, env.pool.QueryRow(ctx, "SELECT count(*) FROM ingestion_run_processing WHERE run_id = $1", run.ID).Scan(&processingRows))
	assert.Equal(t, 0, processingRows, "a non-terminal run must not be claimed")
}

func TestProcessRunRecords_ChangedProduct(t *testing.T) {
	env := setupLiveTestEnv(t)
	ctx := context.Background()

	sourceID := "src-a2-test"
	createTestSource(t, ctx, env, sourceID)

	// Run 1: Initial creation
	run1 := createTestRun(t, ctx, env, sourceID)
	t1 := time.Date(2026, 9, 24, 10, 0, 0, 0, time.UTC)
	payload1 := []byte(`{
		"title": "Sony WH-1000XM5 Headphones",
		"body": "Wireless Noise Cancelling Headphones",
		"vendor": "Sony",
		"country_code": "JP",
		"details": {"color": "Black"}
	}`)
	rec1, err := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
		SourceID:          domainSource.ID(sourceID),
		ExternalProductID: "ext-headphones-1",
		Payload:           payload1,
		SourceUpdatedAt:   &t1,
		IngestionRunID:    run1.ID,
		ReceivedAt:        t1,
	})
	require.NoError(t, err)
	require.NoError(t, env.rawRepo.Save(ctx, rec1))

	_, err = env.processor.ProcessRun(ctx, run1.ID, appProduct.ProcessRunOptions{})
	require.NoError(t, err)

	// Run 2: Changed description and color
	run2 := createTestRun(t, ctx, env, sourceID)
	t2 := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	payload2 := []byte(`{
		"title": "Sony WH-1000XM5 Headphones",
		"body": "Premium Wireless Noise Cancelling Headphones with AI Audio",
		"vendor": "Sony",
		"country_code": "JP",
		"details": {"color": "Silver"}
	}`)
	rec2, err := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
		SourceID:          domainSource.ID(sourceID),
		ExternalProductID: "ext-headphones-1",
		Payload:           payload2,
		SourceUpdatedAt:   &t2,
		IngestionRunID:    run2.ID,
		ReceivedAt:        t2,
	})
	require.NoError(t, err)
	require.NoError(t, env.rawRepo.Save(ctx, rec2))

	res2, err := env.processor.ProcessRun(ctx, run2.ID, appProduct.ProcessRunOptions{})
	require.NoError(t, err)

	assert.Equal(t, 1, res2.RecordsSeen)
	assert.Equal(t, 0, res2.RecordsNew)
	assert.Equal(t, 1, res2.RecordsChanged)
	assert.Equal(t, 0, res2.RecordsUnchanged)

	// Check Snapshot version is now 2
	snap, err := env.productRepo.FindSnapshotByIdentity(ctx, sourceID, "ext-headphones-1")
	require.NoError(t, err)
	require.NotNil(t, snap.StoredCurrentVersion)
	assert.Equal(t, 2, snap.StoredCurrentVersion.VersionNumber)

	// Check Product Changes table
	var count int
	err = env.pool.QueryRow(ctx, "SELECT count(*) FROM product_changes WHERE product_id = $1", string(snap.ProductID)).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 2, count) // 1 NEW + 1 CHANGED

	var changedFieldsJSON []byte
	err = env.pool.QueryRow(ctx, "SELECT changed_fields FROM product_changes WHERE product_id = $1 AND change_type = 'CHANGED'", string(snap.ProductID)).Scan(&changedFieldsJSON)
	require.NoError(t, err)
	var diffs []string
	require.NoError(t, json.Unmarshal(changedFieldsJSON, &diffs))
	assert.ElementsMatch(t, []string{"description", "attributes"}, diffs)
}

func TestProcessRunRecords_Unchanged(t *testing.T) {
	env := setupLiveTestEnv(t)
	ctx := context.Background()

	sourceID := "src-a3-test"
	createTestSource(t, ctx, env, sourceID)

	run1 := createTestRun(t, ctx, env, sourceID)
	t1 := time.Date(2026, 9, 24, 10, 0, 0, 0, time.UTC)
	payload := []byte(`{
		"title": "Bose QuietComfort Ultra",
		"body": "Spatial Audio Headphones",
		"vendor": "Bose",
		"country_code": "US",
		"details": {"color": "Black"}
	}`)
	rec1, err := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
		SourceID:          domainSource.ID(sourceID),
		ExternalProductID: "ext-bose-1",
		Payload:           payload,
		SourceUpdatedAt:   &t1,
		IngestionRunID:    run1.ID,
		ReceivedAt:        t1,
	})
	require.NoError(t, err)
	require.NoError(t, env.rawRepo.Save(ctx, rec1))

	_, err = env.processor.ProcessRun(ctx, run1.ID, appProduct.ProcessRunOptions{})
	require.NoError(t, err)

	// Capture row counts
	var prodCount1, verCount1, changeCount1, outboxCount1 int
	require.NoError(t, env.pool.QueryRow(ctx, "SELECT count(*) FROM products").Scan(&prodCount1))
	require.NoError(t, env.pool.QueryRow(ctx, "SELECT count(*) FROM product_versions").Scan(&verCount1))
	require.NoError(t, env.pool.QueryRow(ctx, "SELECT count(*) FROM product_changes").Scan(&changeCount1))
	require.NoError(t, env.pool.QueryRow(ctx, "SELECT count(*) FROM outbox_events").Scan(&outboxCount1))

	// Run 2: Exact same payload with newer timestamp
	run2 := createTestRun(t, ctx, env, sourceID)
	t2 := time.Date(2026, 9, 24, 14, 0, 0, 0, time.UTC)
	rec2, err := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
		SourceID:          domainSource.ID(sourceID),
		ExternalProductID: "ext-bose-1",
		Payload:           payload, // identical payload
		SourceUpdatedAt:   &t2,
		IngestionRunID:    run2.ID,
		ReceivedAt:        t2,
	})
	require.NoError(t, err)
	require.NoError(t, env.rawRepo.Save(ctx, rec2))

	res2, err := env.processor.ProcessRun(ctx, run2.ID, appProduct.ProcessRunOptions{})
	require.NoError(t, err)

	assert.Equal(t, 1, res2.RecordsSeen)
	assert.Equal(t, 0, res2.RecordsNew)
	assert.Equal(t, 0, res2.RecordsChanged)
	assert.Equal(t, 1, res2.RecordsUnchanged)

	// Assert zero writes to products, versions, changes, outbox
	var prodCount2, verCount2, changeCount2, outboxCount2 int
	require.NoError(t, env.pool.QueryRow(ctx, "SELECT count(*) FROM products").Scan(&prodCount2))
	require.NoError(t, env.pool.QueryRow(ctx, "SELECT count(*) FROM product_versions").Scan(&verCount2))
	require.NoError(t, env.pool.QueryRow(ctx, "SELECT count(*) FROM product_changes").Scan(&changeCount2))
	require.NoError(t, env.pool.QueryRow(ctx, "SELECT count(*) FROM outbox_events").Scan(&outboxCount2))

	assert.Equal(t, prodCount1, prodCount2, "products count must not change")
	assert.Equal(t, verCount1, verCount2, "versions count must not change")
	assert.Equal(t, changeCount1, changeCount2, "changes count must not change")
	assert.Equal(t, outboxCount1, outboxCount2, "outbox count must not change")

	// Watermark should advance
	snap, err := env.productRepo.FindSnapshotByIdentity(ctx, sourceID, "ext-bose-1")
	require.NoError(t, err)
	require.NotNil(t, snap.LastSourceUpdatedAt)
	assert.Equal(t, t2.Unix(), snap.LastSourceUpdatedAt.Unix())
}

func TestProcessRunRecords_ReplayIsIdempotent(t *testing.T) {
	env := setupLiveTestEnv(t)
	ctx := context.Background()

	sourceID := "src-f1-replay"
	createTestSource(t, ctx, env, sourceID)
	run := createTestRun(t, ctx, env, sourceID)

	payload := []byte(`{"title": "Product Replay Test", "vendor": "BrandX"}`)
	now := time.Now().UTC()
	rec, err := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
		SourceID:          domainSource.ID(sourceID),
		ExternalProductID: "ext-replay-1",
		Payload:           payload,
		SourceUpdatedAt:   &now,
		IngestionRunID:    run.ID,
		ReceivedAt:        now,
	})
	require.NoError(t, err)
	require.NoError(t, env.rawRepo.Save(ctx, rec))

	// Initial execution
	res1, err := env.processor.ProcessRun(ctx, run.ID, appProduct.ProcessRunOptions{})
	require.NoError(t, err)
	assert.Equal(t, 1, res1.RecordsNew)

	// Replay 1 without --from-start: cursor is at end, no additional work
	resReplay1, err := env.processor.ProcessRun(ctx, run.ID, appProduct.ProcessRunOptions{FromStart: false})
	require.NoError(t, err)
	assert.Equal(t, 1, resReplay1.RecordsNew)

	// Replay 2 with --from-start: resets counters and walks from start, resolves as unchanged
	resReplay2, err := env.processor.ProcessRun(ctx, run.ID, appProduct.ProcessRunOptions{FromStart: true})
	require.NoError(t, err)
	assert.Equal(t, 1, resReplay2.RecordsSeen)
	assert.Equal(t, 0, resReplay2.RecordsNew)
	assert.Equal(t, 1, resReplay2.RecordsUnchanged)

	// Ensure no duplicate versions
	var verCount int
	require.NoError(t, env.pool.QueryRow(ctx, "SELECT count(*) FROM product_versions").Scan(&verCount))
	assert.Equal(t, 1, verCount)
}

func TestProcessRunRecords_ConcurrentSameIdentity(t *testing.T) {
	env := setupLiveTestEnv(t)
	ctx := context.Background()

	sourceID := "src-f2-concurrent"
	createTestSource(t, ctx, env, sourceID)

	// Create two separate runs for the same source with overlapping product identities
	run1 := createTestRun(t, ctx, env, sourceID)
	run2 := createTestRun(t, ctx, env, sourceID)

	now := time.Now().UTC()
	payload := []byte(`{"title": "Concurrent Product", "vendor": "RaceBrand"}`)

	rec1, err := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
		SourceID:          domainSource.ID(sourceID),
		ExternalProductID: "ext-concurrent-1",
		Payload:           payload,
		SourceUpdatedAt:   &now,
		IngestionRunID:    run1.ID,
		ReceivedAt:        now,
	})
	require.NoError(t, err)
	require.NoError(t, env.rawRepo.Save(ctx, rec1))

	rec2, err := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
		SourceID:          domainSource.ID(sourceID),
		ExternalProductID: "ext-concurrent-1",
		Payload:           payload,
		SourceUpdatedAt:   &now,
		IngestionRunID:    run2.ID,
		ReceivedAt:        now,
	})
	require.NoError(t, err)
	require.NoError(t, env.rawRepo.Save(ctx, rec2))

	var wg sync.WaitGroup
	errs := make([]error, 2)

	wg.Add(2)
	go func() {
		defer wg.Done()
		_, errs[0] = env.processor.ProcessRun(ctx, run1.ID, appProduct.ProcessRunOptions{})
	}()
	go func() {
		defer wg.Done()
		_, errs[1] = env.processor.ProcessRun(ctx, run2.ID, appProduct.ProcessRunOptions{})
	}()
	wg.Wait()

	require.NoError(t, errs[0])
	require.NoError(t, errs[1])

	// Exactly 1 Product and 1 Version must exist
	var prodCount, verCount int
	require.NoError(t, env.pool.QueryRow(ctx, "SELECT count(*) FROM products").Scan(&prodCount))
	require.NoError(t, env.pool.QueryRow(ctx, "SELECT count(*) FROM product_versions").Scan(&verCount))
	assert.Equal(t, 1, prodCount)
	assert.Equal(t, 1, verCount)
}

func TestProcessRunRecords_OutOfOrder(t *testing.T) {
	env := setupLiveTestEnv(t)
	ctx := context.Background()

	sourceID := "src-f3-order"
	createTestSource(t, ctx, env, sourceID)

	// Run 1 has newer timestamp T2
	t2 := time.Date(2026, 9, 24, 15, 0, 0, 0, time.UTC)
	run1 := createTestRun(t, ctx, env, sourceID)
	rec1, err := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
		SourceID:          domainSource.ID(sourceID),
		ExternalProductID: "ext-order-1",
		Payload:           []byte(`{"title": "Newer Title T2", "vendor": "BrandX"}`),
		SourceUpdatedAt:   &t2,
		IngestionRunID:    run1.ID,
		ReceivedAt:        t2,
	})
	require.NoError(t, err)
	require.NoError(t, env.rawRepo.Save(ctx, rec1))
	_, err = env.processor.ProcessRun(ctx, run1.ID, appProduct.ProcessRunOptions{})
	require.NoError(t, err)

	// Run 2 processes an older record T1 (where T1 < T2)
	t1 := time.Date(2026, 9, 24, 10, 0, 0, 0, time.UTC)
	run2 := createTestRun(t, ctx, env, sourceID)
	rec2, err := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
		SourceID:          domainSource.ID(sourceID),
		ExternalProductID: "ext-order-1",
		Payload:           []byte(`{"title": "Older Title T1", "vendor": "BrandX"}`),
		SourceUpdatedAt:   &t1,
		IngestionRunID:    run2.ID,
		ReceivedAt:        t2.Add(time.Minute),
	})
	require.NoError(t, err)
	require.NoError(t, env.rawRepo.Save(ctx, rec2))

	res2, err := env.processor.ProcessRun(ctx, run2.ID, appProduct.ProcessRunOptions{})
	require.NoError(t, err)

	assert.Equal(t, 1, res2.RecordsUnchanged, "stale record must be counted as unchanged")
	assert.Equal(t, 0, res2.RecordsChanged)

	// Verify catalog did not regress
	snap, err := env.productRepo.FindSnapshotByIdentity(ctx, sourceID, "ext-order-1")
	require.NoError(t, err)
	assert.Equal(t, "Newer Title T2", snap.StoredCurrentVersion.CanonicalName)
}

func TestProcessRunRecords_BadRecordCounted(t *testing.T) {
	env := setupLiveTestEnv(t)
	ctx := context.Background()

	sourceID := "src-f4-bad"
	createTestSource(t, ctx, env, sourceID)
	run := createTestRun(t, ctx, env, sourceID)

	now := time.Now().UTC()
	// Record 1: Valid
	r1, _ := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
		SourceID:          domainSource.ID(sourceID),
		ExternalProductID: "ext-1",
		Payload:           []byte(`{"title": "Valid Product 1"}`),
		IngestionRunID:    run.ID,
		ReceivedAt:        now,
	})
	// Record 2: Missing title -> Normalization failure
	r2, _ := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
		SourceID:          domainSource.ID(sourceID),
		ExternalProductID: "ext-2",
		Payload:           []byte(`{"body": "No title here"}`),
		IngestionRunID:    run.ID,
		ReceivedAt:        now,
	})
	// Record 3: Valid
	r3, _ := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
		SourceID:          domainSource.ID(sourceID),
		ExternalProductID: "ext-3",
		Payload:           []byte(`{"title": "Valid Product 3"}`),
		IngestionRunID:    run.ID,
		ReceivedAt:        now,
	})
	require.NoError(t, env.rawRepo.SaveBatch(ctx, []*domainIngestion.RawRecord{r1, r2, r3}))

	budget := domainIngestion.ErrorBudget{MaxErrorRate: 0.50, MinSampleRows: 2}
	result, err := env.processor.ProcessRun(ctx, run.ID, appProduct.ProcessRunOptions{
		ErrorBudget: budget,
	})
	require.NoError(t, err)

	assert.Equal(t, 3, result.RecordsSeen)
	assert.Equal(t, 2, result.RecordsNew)
	assert.Equal(t, 1, result.RecordsFailed)
}

func TestProcessRunRecords_ErrorBudgetExceeded_FailsLoudly(t *testing.T) {
	env := setupLiveTestEnv(t)
	ctx := context.Background()

	sourceID := "src-f4-budget-fail"
	createTestSource(t, ctx, env, sourceID)
	run := createTestRun(t, ctx, env, sourceID)

	now := time.Now().UTC()
	var records []*domainIngestion.RawRecord
	for i := 0; i < 200; i++ {
		r, _ := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
			SourceID:          domainSource.ID(sourceID),
			ExternalProductID: fmt.Sprintf("ext-bad-%d", i),
			Payload:           []byte(fmt.Sprintf(`{"invalid_no_title": %d}`, i)),
			IngestionRunID:    run.ID,
			ReceivedAt:        now,
		})
		records = append(records, r)
	}
	require.NoError(t, env.rawRepo.SaveBatch(ctx, records))

	// Neither caller passes an error budget -> DefaultErrorBudget (5% after 100) applies
	_, err := env.processor.ProcessRun(ctx, run.ID, appProduct.ProcessRunOptions{
		BatchSize: 50,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "error budget exceeded")

	// Verify run processing state is FAILED and counters reflect the failing record
	rp, err := env.processingRepo.GetByID(ctx, run.ID)
	require.NoError(t, err)
	assert.Equal(t, domainIngestion.ProcessingFailed, rp.Status)
	assert.Equal(t, 100, rp.RecordsSeen)
	assert.Equal(t, 100, rp.RecordsFailed)
}

func TestProcessRunRecords_PriceOnlyChange(t *testing.T) {
	env := setupLiveTestEnv(t)
	ctx := context.Background()

	sourceID := "src-f5-price"
	createTestSource(t, ctx, env, sourceID)

	run1 := createTestRun(t, ctx, env, sourceID)
	t1 := time.Date(2026, 9, 24, 10, 0, 0, 0, time.UTC)
	payload1 := []byte(`{
		"title": "Nike Air Max 90",
		"vendor": "Nike",
		"price": "129.99",
		"currency": "USD"
	}`)
	rec1, _ := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
		SourceID:          domainSource.ID(sourceID),
		ExternalProductID: "ext-nike-1",
		Payload:           payload1,
		SourceUpdatedAt:   &t1,
		IngestionRunID:    run1.ID,
		ReceivedAt:        t1,
	})
	require.NoError(t, env.rawRepo.Save(ctx, rec1))
	_, err := env.processor.ProcessRun(ctx, run1.ID, appProduct.ProcessRunOptions{})
	require.NoError(t, err)

	// Run 2: Price changes from 129.99 to 99.99 (gotcha #1: price is not in canonical fingerprint)
	run2 := createTestRun(t, ctx, env, sourceID)
	t2 := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	payload2 := []byte(`{
		"title": "Nike Air Max 90",
		"vendor": "Nike",
		"price": "99.99",
		"currency": "USD"
	}`)
	rec2, _ := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
		SourceID:          domainSource.ID(sourceID),
		ExternalProductID: "ext-nike-1",
		Payload:           payload2,
		SourceUpdatedAt:   &t2,
		IngestionRunID:    run2.ID,
		ReceivedAt:        t2,
	})
	require.NoError(t, env.rawRepo.Save(ctx, rec2))

	res2, err := env.processor.ProcessRun(ctx, run2.ID, appProduct.ProcessRunOptions{})
	require.NoError(t, err)

	assert.Equal(t, 1, res2.RecordsUnchanged, "price change must not create new version")
	assert.Equal(t, 0, res2.RecordsChanged)

	var verCount int
	require.NoError(t, env.pool.QueryRow(ctx, "SELECT count(*) FROM product_versions").Scan(&verCount))
	assert.Equal(t, 1, verCount)
}

func TestProcessRunRecords_CancelAndResume(t *testing.T) {
	env := setupLiveTestEnv(t)
	ctx := context.Background()

	sourceID := "src-f6-cancel"
	createTestSource(t, ctx, env, sourceID)
	run := createTestRun(t, ctx, env, sourceID)

	now := time.Now().UTC()
	var records []*domainIngestion.RawRecord
	for i := 0; i < 10; i++ {
		r, _ := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
			SourceID:          domainSource.ID(sourceID),
			ExternalProductID: fmt.Sprintf("ext-resume-%d", i),
			Payload:           []byte(fmt.Sprintf(`{"title": "Product %d"}`, i)),
			IngestionRunID:    run.ID,
			ReceivedAt:        now,
		})
		records = append(records, r)
	}
	require.NoError(t, env.rawRepo.SaveBatch(ctx, records))

	cancelCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Decorate TxRunner to cancel the context deterministically after exactly 3 committed records
	cancellingRunner := &cancellingTxRunner{
		inner:      env.txRunner,
		cancelFunc: cancel,
		target:     3,
	}
	cancellingProcessor, err := appProduct.NewRunProcessor(env.runRepo, env.sourceRepo, env.rawRepo, env.processingRepo, cancellingRunner)
	require.NoError(t, err)

	_, err = cancellingProcessor.ProcessRun(cancelCtx, run.ID, appProduct.ProcessRunOptions{BatchSize: 1})
	require.Error(t, err)

	// Processing status should be released to PENDING
	rp, err := env.processingRepo.GetByID(ctx, run.ID)
	require.NoError(t, err)
	assert.Equal(t, domainIngestion.ProcessingPending, rp.Status)
	assert.Equal(t, 3, rp.RecordsSeen)

	// Check committed products exist in database
	var prodCountMid int
	require.NoError(t, env.pool.QueryRow(ctx, "SELECT count(*) FROM products").Scan(&prodCountMid))
	assert.Equal(t, 3, prodCountMid)

	// Resume on live context using the standard processor
	res, err := env.processor.ProcessRun(ctx, run.ID, appProduct.ProcessRunOptions{BatchSize: 2})
	require.NoError(t, err)
	assert.Equal(t, 10, res.RecordsSeen)
	assert.Equal(t, 10, res.RecordsNew)

	// Verify exactly 10 products and 10 versions in DB without duplicates
	var totalProds, totalVers int
	require.NoError(t, env.pool.QueryRow(ctx, "SELECT count(*) FROM products").Scan(&totalProds))
	require.NoError(t, env.pool.QueryRow(ctx, "SELECT count(*) FROM product_versions").Scan(&totalVers))
	assert.Equal(t, 10, totalProds)
	assert.Equal(t, 10, totalVers)
}

func TestWorker_ClaimNext_Then_ProcessRun(t *testing.T) {
	env := setupLiveTestEnv(t)
	ctx := context.Background()

	sourceID := "src-worker-flow"
	createTestSource(t, ctx, env, sourceID)
	run := createTestRun(t, ctx, env, sourceID)

	now := time.Now().UTC()
	var records []*domainIngestion.RawRecord
	for i := 0; i < 5; i++ {
		r, _ := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
			SourceID:          domainSource.ID(sourceID),
			ExternalProductID: fmt.Sprintf("ext-worker-%d", i),
			Payload:           []byte(fmt.Sprintf(`{"title": "Worker Product %d"}`, i)),
			IngestionRunID:    run.ID,
			ReceivedAt:        now,
		})
		records = append(records, r)
	}
	require.NoError(t, env.rawRepo.SaveBatch(ctx, records))

	_, err := env.processingRepo.EnsureExists(ctx, run.ID)
	require.NoError(t, err)

	// Worker step 1: Claim next available pending run
	claimToken, err := uuid.NewString()
	require.NoError(t, err)
	claimed, err := env.processingRepo.ClaimNext(ctx, claimToken, 30*time.Second)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, run.ID, claimed.RunID)
	assert.Equal(t, domainIngestion.ProcessingRunning, claimed.Status)

	// Worker step 2: ProcessRun using the claimed token
	res, err := env.processor.ProcessRun(ctx, claimed.RunID, appProduct.ProcessRunOptions{
		ClaimToken:    claimToken,
		LeaseDuration: 30 * time.Second,
		BatchSize:     50,
	})
	require.NoError(t, err)
	assert.Equal(t, 5, res.RecordsSeen)
	assert.Equal(t, 5, res.RecordsNew)

	// Verify run state is now COMPLETED
	finalRp, err := env.processingRepo.GetByID(ctx, run.ID)
	require.NoError(t, err)
	assert.Equal(t, domainIngestion.ProcessingCompleted, finalRp.Status)
}

func TestProcessRunRecords_FingerprintVersionMismatch(t *testing.T) {
	env := setupLiveTestEnv(t)
	ctx := context.Background()

	sourceID := "src-d4-mismatch"
	createTestSource(t, ctx, env, sourceID)
	run1 := createTestRun(t, ctx, env, sourceID)

	now := time.Now().UTC()
	payload := []byte(`{"title": "Legacy Formatted Item", "vendor": "LegacyCo"}`)
	rec1, _ := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
		SourceID:          domainSource.ID(sourceID),
		ExternalProductID: "ext-legacy-1",
		Payload:           payload,
		IngestionRunID:    run1.ID,
		ReceivedAt:        now,
	})
	require.NoError(t, env.rawRepo.Save(ctx, rec1))
	_, err := env.processor.ProcessRun(ctx, run1.ID, appProduct.ProcessRunOptions{})
	require.NoError(t, err)

	// Manually corrupt the current_fingerprint to a legacy "v0:..." format
	snap, err := env.productRepo.FindSnapshotByIdentity(ctx, sourceID, "ext-legacy-1")
	require.NoError(t, err)
	_, err = env.pool.Exec(ctx, "UPDATE products SET current_fingerprint = 'v0:legacy_hash_format_12345' WHERE id = $1", string(snap.ProductID))
	require.NoError(t, err)

	// Run 2 processes incoming record with identical data
	run2 := createTestRun(t, ctx, env, sourceID)
	rec2, _ := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
		SourceID:          domainSource.ID(sourceID),
		ExternalProductID: "ext-legacy-1",
		Payload:           payload,
		IngestionRunID:    run2.ID,
		ReceivedAt:        now.Add(time.Minute),
	})
	require.NoError(t, env.rawRepo.Save(ctx, rec2))

	res2, err := env.processor.ProcessRun(ctx, run2.ID, appProduct.ProcessRunOptions{})
	require.NoError(t, err)

	assert.Equal(t, 1, res2.RecordsUnchanged)
	assert.Equal(t, 0, res2.RecordsChanged)

	// Verify fingerprint was upgraded to v1 without creating a new version or change record
	updatedSnap, err := env.productRepo.FindSnapshotByIdentity(ctx, sourceID, "ext-legacy-1")
	require.NoError(t, err)
	assert.True(t, updatedSnap.CurrentFingerprint != "v0:legacy_hash_format_12345")
	assert.Equal(t, 1, updatedSnap.StoredCurrentVersion.VersionNumber)

	var verCount, changeCount, outboxCount int
	require.NoError(t, env.pool.QueryRow(ctx, "SELECT count(*) FROM product_versions WHERE product_id = $1", string(snap.ProductID)).Scan(&verCount))
	require.NoError(t, env.pool.QueryRow(ctx, "SELECT count(*) FROM product_changes WHERE product_id = $1", string(snap.ProductID)).Scan(&changeCount))
	require.NoError(t, env.pool.QueryRow(ctx, "SELECT count(*) FROM outbox_events WHERE aggregate_id = $1", string(snap.ProductID)).Scan(&outboxCount))

	assert.Equal(t, 1, verCount)
	assert.Equal(t, 1, changeCount)
	assert.Equal(t, 1, outboxCount)
}

func TestE2E_IngestTwice_OnlyChangedVersioned(t *testing.T) {
	env := setupLiveTestEnv(t)
	ctx := context.Background()

	sourceID := "src-a4-e2e"
	createTestSource(t, ctx, env, sourceID)

	runService, err := appIngestion.NewService(env.runRepo, env.sourceRepo)
	require.NoError(t, err)
	coordinator, err := appIngestion.NewSyncCoordinator(runService, env.ingestTxRunner)
	require.NoError(t, err)

	// Feed 1: 100 products ingested via SyncCoordinator
	t1 := time.Date(2026, 9, 24, 8, 0, 0, 0, time.UTC)
	var records1 []*domainIngestion.RawRecord
	for i := 0; i < 100; i++ {
		p := []byte(fmt.Sprintf(`{"title": "Product Number %d", "vendor": "BrandAll", "details": {"color": "Blue"}}`, i))
		r, err := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
			SourceID:          domainSource.ID(sourceID),
			ExternalProductID: fmt.Sprintf("ext-e2e-%d", i),
			Payload:           p,
			SourceUpdatedAt:   &t1,
			ReceivedAt:        t1,
		})
		require.NoError(t, err)
		records1 = append(records1, r)
	}

	adapter1 := &feedBatchAdapter{
		batches: []domainIngestion.FetchResult{
			{Records: records1[:50], NextCheckpoint: "page-2", HasMore: true},
			{Records: records1[50:], NextCheckpoint: "page-final", HasMore: false},
		},
	}

	run1, err := coordinator.SyncSource(ctx, appIngestion.SyncParams{
		SourceID: domainSource.ID(sourceID),
		Adapter:  adapter1,
	})
	require.NoError(t, err)
	require.Equal(t, domainIngestion.StatusCompleted, run1.Status)

	res1, err := env.processor.ProcessRun(ctx, run1.ID, appProduct.ProcessRunOptions{BatchSize: 25})
	require.NoError(t, err)
	assert.Equal(t, 100, res1.RecordsNew)

	// Feed 2: 100 products where 95 are identical and 5 (indices 0..4) have changed title/color
	t2 := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	var records2 []*domainIngestion.RawRecord
	for i := 0; i < 100; i++ {
		var p []byte
		if i < 5 {
			p = []byte(fmt.Sprintf(`{"title": "Product Number %d Modified", "vendor": "BrandAll", "details": {"color": "Red"}}`, i))
		} else {
			p = []byte(fmt.Sprintf(`{"title": "Product Number %d", "vendor": "BrandAll", "details": {"color": "Blue"}}`, i))
		}
		r, err := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
			SourceID:          domainSource.ID(sourceID),
			ExternalProductID: fmt.Sprintf("ext-e2e-%d", i),
			Payload:           p,
			SourceUpdatedAt:   &t2,
			ReceivedAt:        t2,
		})
		require.NoError(t, err)
		records2 = append(records2, r)
	}

	adapter2 := &feedBatchAdapter{
		batches: []domainIngestion.FetchResult{
			{Records: records2[:50], NextCheckpoint: "page-2", HasMore: true},
			{Records: records2[50:], NextCheckpoint: "page-final", HasMore: false},
		},
	}

	run2, err := coordinator.SyncSource(ctx, appIngestion.SyncParams{
		SourceID: domainSource.ID(sourceID),
		Adapter:  adapter2,
	})
	require.NoError(t, err)
	require.Equal(t, domainIngestion.StatusCompleted, run2.Status)

	res2, err := env.processor.ProcessRun(ctx, run2.ID, appProduct.ProcessRunOptions{BatchSize: 25})
	require.NoError(t, err)

	assert.Equal(t, 100, res2.RecordsSeen)
	assert.Equal(t, 0, res2.RecordsNew)
	assert.Equal(t, 5, res2.RecordsChanged)
	assert.Equal(t, 95, res2.RecordsUnchanged)

	// Verify exactly 105 total versions exist in database (100 from run1 + 5 from run2)
	var totalVersions int
	require.NoError(t, env.pool.QueryRow(ctx, "SELECT count(*) FROM product_versions").Scan(&totalVersions))
	assert.Equal(t, 105, totalVersions)

	// Verify exactly 105 outbox events exist
	var totalOutbox int
	require.NoError(t, env.pool.QueryRow(ctx, "SELECT count(*) FROM outbox_events").Scan(&totalOutbox))
	assert.Equal(t, 105, totalOutbox)
}
