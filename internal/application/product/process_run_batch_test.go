package product_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	appProduct "github.com/ayo6706/cross-border-ecommerce/internal/application/product"
	domainIngestion "github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
	domainProduct "github.com/ayo6706/cross-border-ecommerce/internal/domain/product"
	domainSource "github.com/ayo6706/cross-border-ecommerce/internal/domain/source"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProcessRunBatch_A1_5000Records(t *testing.T) {
	env := setupLiveTestEnv(t)
	ctx := context.Background()

	sourceID := "src-batch-5000"
	createTestSource(t, ctx, env, sourceID)
	run := createTestRun(t, ctx, env, sourceID)

	const totalRecords = 5000
	const pageSize = 500

	now := time.Now().UTC()
	rawRecords := make([]*domainIngestion.RawRecord, totalRecords)
	for i := 0; i < totalRecords; i++ {
		payload := []byte(fmt.Sprintf(`{
			"title": "Batch Product %d",
			"body": "Description for batch item %d",
			"vendor": "Brand %d",
			"country_code": "US",
			"details": {
				"color": "Color %d",
				"size": "M"
			}
		}`, i, i, i%10, i%5))

		rec, err := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
			SourceID:          domainSource.ID(sourceID),
			ExternalProductID: fmt.Sprintf("ext-batch-%05d", i),
			Payload:           payload,
			SourceUpdatedAt:   &now,
			IngestionRunID:    run.ID,
			ReceivedAt:        now,
		})
		require.NoError(t, err)
		rawRecords[i] = rec
	}

	// Bulk insert all 5000 raw records via pgx.CopyFrom
	require.NoError(t, env.rawRepo.SaveBatch(ctx, rawRecords))

	// Process run with batch size 500 (10 page transactions)
	result, err := env.processor.ProcessRun(ctx, run.ID, appProduct.ProcessRunOptions{
		BatchSize:     pageSize,
		LeaseDuration: 30 * time.Second,
	})
	require.NoError(t, err)

	assert.Equal(t, totalRecords, result.RecordsSeen)
	assert.Equal(t, totalRecords, result.RecordsNew)
	assert.Equal(t, 0, result.RecordsChanged)
	assert.Equal(t, 0, result.RecordsUnchanged)
	assert.Equal(t, 0, result.RecordsFailed)

	// Verify table row counts in PostgreSQL
	var prodCount, psCount, pvCount, pcCount, outboxCount int
	require.NoError(t, env.pool.QueryRow(ctx, "SELECT count(*) FROM products").Scan(&prodCount))
	require.NoError(t, env.pool.QueryRow(ctx, "SELECT count(*) FROM product_sources WHERE source_id = $1", sourceID).Scan(&psCount))
	require.NoError(t, env.pool.QueryRow(ctx, "SELECT count(*) FROM product_versions WHERE ingestion_run_id = $1", run.ID).Scan(&pvCount))
	require.NoError(t, env.pool.QueryRow(ctx, "SELECT count(*) FROM product_changes WHERE ingestion_run_id = $1", run.ID).Scan(&pcCount))
	require.NoError(t, env.pool.QueryRow(ctx, "SELECT count(*) FROM outbox_events WHERE aggregate_type = 'product'").Scan(&outboxCount))

	assert.Equal(t, totalRecords, prodCount)
	assert.Equal(t, totalRecords, psCount)
	assert.Equal(t, totalRecords, pvCount)
	assert.Equal(t, totalRecords, pcCount)
	assert.Equal(t, totalRecords, outboxCount)
}

func TestProcessRunBatch_A2_MixedPage(t *testing.T) {
	env := setupLiveTestEnv(t)
	ctx := context.Background()

	sourceID := "src-mixed-page"
	createTestSource(t, ctx, env, sourceID)
	run1 := createTestRun(t, ctx, env, sourceID)

	t0 := time.Now().UTC().Add(-10 * time.Minute)
	t1 := time.Now().UTC()

	// Seed 2 existing products
	p1Payload := []byte(`{"title": "Existing Product 1", "body": "B1", "vendor": "V1", "country_code": "US"}`)
	p2Payload := []byte(`{"title": "Existing Product 2", "body": "B2", "vendor": "V2", "country_code": "US"}`)

	rec1, err := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
		SourceID:          domainSource.ID(sourceID),
		ExternalProductID: "ext-mixed-1",
		Payload:           p1Payload,
		SourceUpdatedAt:   &t0,
		IngestionRunID:    run1.ID,
		ReceivedAt:        t0,
	})
	require.NoError(t, err)

	rec2, err := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
		SourceID:          domainSource.ID(sourceID),
		ExternalProductID: "ext-mixed-2",
		Payload:           p2Payload,
		SourceUpdatedAt:   &t0,
		IngestionRunID:    run1.ID,
		ReceivedAt:        t0,
	})
	require.NoError(t, err)

	require.NoError(t, env.rawRepo.SaveBatch(ctx, []*domainIngestion.RawRecord{rec1, rec2}))
	res1, err := env.processor.ProcessRun(ctx, run1.ID, appProduct.ProcessRunOptions{
		BatchSize:     500,
		LeaseDuration: 30 * time.Second,
	})
	require.NoError(t, err)
	assert.Equal(t, 2, res1.RecordsNew)

	// Now run 2 with mixed records:
	// 1: ext-mixed-1 is CHANGED
	// 2: ext-mixed-2 is UNCHANGED
	// 3: ext-mixed-3 is NEW
	// 4: ext-mixed-1 replay with older timestamp is STALE
	run2 := createTestRun(t, ctx, env, sourceID)
	p1ChangedPayload := []byte(`{"title": "Existing Product 1 - Updated", "body": "B1", "vendor": "V1", "country_code": "US"}`)
	p3NewPayload := []byte(`{"title": "Brand New Product 3", "body": "B3", "vendor": "V3", "country_code": "CA"}`)
	p1StalePayload := []byte(`{"title": "Existing Product 1", "body": "B1", "vendor": "V1", "country_code": "US"}`)
	tStale := t0.Add(-5 * time.Minute)

	r1Changed, err := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
		SourceID:          domainSource.ID(sourceID),
		ExternalProductID: "ext-mixed-1",
		Payload:           p1ChangedPayload,
		SourceUpdatedAt:   &t1,
		IngestionRunID:    run2.ID,
		ReceivedAt:        t1,
	})
	require.NoError(t, err)

	r2Unchanged, err := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
		SourceID:          domainSource.ID(sourceID),
		ExternalProductID: "ext-mixed-2",
		Payload:           p2Payload,
		SourceUpdatedAt:   &t1,
		IngestionRunID:    run2.ID,
		ReceivedAt:        t1,
	})
	require.NoError(t, err)

	r3New, err := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
		SourceID:          domainSource.ID(sourceID),
		ExternalProductID: "ext-mixed-3",
		Payload:           p3NewPayload,
		SourceUpdatedAt:   &t1,
		IngestionRunID:    run2.ID,
		ReceivedAt:        t1,
	})
	require.NoError(t, err)

	r1Stale, err := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
		SourceID:          domainSource.ID(sourceID),
		ExternalProductID: "ext-mixed-1",
		Payload:           p1StalePayload,
		SourceUpdatedAt:   &tStale,
		IngestionRunID:    run2.ID,
		ReceivedAt:        tStale,
	})
	require.NoError(t, err)

	require.NoError(t, env.rawRepo.SaveBatch(ctx, []*domainIngestion.RawRecord{r1Changed, r2Unchanged, r3New, r1Stale}))

	res2, err := env.processor.ProcessRun(ctx, run2.ID, appProduct.ProcessRunOptions{
		BatchSize:     50,
		LeaseDuration: 30 * time.Second,
	})
	require.NoError(t, err)

	assert.Equal(t, 4, res2.RecordsSeen)
	assert.Equal(t, 1, res2.RecordsNew)       // ext-mixed-3
	assert.Equal(t, 1, res2.RecordsChanged)   // ext-mixed-1
	assert.Equal(t, 2, res2.RecordsUnchanged) // ext-mixed-2 (unchanged) + ext-mixed-1 stale
	assert.Equal(t, 0, res2.RecordsFailed)

	// Verify ext-mixed-1 is at version 2
	snap1 := findTestSnapshot(t, ctx, env, sourceID, "ext-mixed-1")
	require.NotNil(t, snap1)
	require.NotNil(t, snap1.StoredCurrentVersion)
	assert.Equal(t, 2, snap1.StoredCurrentVersion.VersionNumber)
	assert.Equal(t, "Existing Product 1 - Updated", snap1.StoredCurrentVersion.CanonicalName)
}

func TestProcessRunBatch_A3_SameIdentityTwiceInPage(t *testing.T) {
	env := setupLiveTestEnv(t)
	ctx := context.Background()

	sourceID := "src-identity-twice"
	createTestSource(t, ctx, env, sourceID)
	run := createTestRun(t, ctx, env, sourceID)

	t1 := time.Now().UTC().Add(-2 * time.Hour)
	t2 := time.Now().UTC().Add(-1 * time.Hour)

	// Product arrives as NEW (v1) and then CHANGED (v2) in the same ingestion run page
	payloadV1 := []byte(`{"title": "Initial Title", "body": "Desc", "vendor": "Brand", "country_code": "US"}`)
	payloadV2 := []byte(`{"title": "Updated Title", "body": "Desc", "vendor": "Brand", "country_code": "US"}`)

	r1, err := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
		SourceID:          domainSource.ID(sourceID),
		ExternalProductID: "ext-twice-1",
		Payload:           payloadV1,
		SourceUpdatedAt:   &t1,
		IngestionRunID:    run.ID,
		ReceivedAt:        t1,
	})
	require.NoError(t, err)

	r2, err := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
		SourceID:          domainSource.ID(sourceID),
		ExternalProductID: "ext-twice-1",
		Payload:           payloadV2,
		SourceUpdatedAt:   &t2,
		IngestionRunID:    run.ID,
		ReceivedAt:        t2,
	})
	require.NoError(t, err)

	require.NoError(t, env.rawRepo.SaveBatch(ctx, []*domainIngestion.RawRecord{r1, r2}))

	res, err := env.processor.ProcessRun(ctx, run.ID, appProduct.ProcessRunOptions{
		BatchSize:     50,
		LeaseDuration: 30 * time.Second,
	})
	require.NoError(t, err)

	assert.Equal(t, 2, res.RecordsSeen)
	assert.Equal(t, 1, res.RecordsNew)
	assert.Equal(t, 1, res.RecordsChanged)

	// Verify only 1 product in DB pointing to v2
	snap := findTestSnapshot(t, ctx, env, sourceID, "ext-twice-1")
	require.NotNil(t, snap)
	require.NotNil(t, snap.StoredCurrentVersion)
	assert.Equal(t, 2, snap.StoredCurrentVersion.VersionNumber)
	assert.Equal(t, "Updated Title", snap.StoredCurrentVersion.CanonicalName)

	// Verify 2 versions in product_versions
	var versionsCount int
	require.NoError(t, env.pool.QueryRow(ctx, "SELECT count(*) FROM product_versions WHERE product_id = $1", string(snap.ProductID)).Scan(&versionsCount))
	assert.Equal(t, 2, versionsCount)

	// Verify 2 changes in product_changes (NEW and CHANGED)
	var changesCount int
	require.NoError(t, env.pool.QueryRow(ctx, "SELECT count(*) FROM product_changes WHERE product_id = $1", string(snap.ProductID)).Scan(&changesCount))
	assert.Equal(t, 2, changesCount)

	// Verify 2 outbox events
	var outboxCount int
	require.NoError(t, env.pool.QueryRow(ctx, "SELECT count(*) FROM outbox_events WHERE aggregate_id = $1", string(snap.ProductID)).Scan(&outboxCount))
	assert.Equal(t, 2, outboxCount)

	// The published product.changed contract: one event per version, in version order.
	rows, err := env.pool.Query(ctx, `
		SELECT aggregate_type, event_type, payload FROM outbox_events
		WHERE aggregate_id = $1 ORDER BY (payload->>'version_number')::int`, string(snap.ProductID))
	require.NoError(t, err)
	defer rows.Close()
	type changedPayload struct {
		ProductID     string   `json:"product_id"`
		VersionID     string   `json:"version_id"`
		VersionNumber int      `json:"version_number"`
		Fingerprint   string   `json:"fingerprint"`
		ChangeType    string   `json:"change_type"`
		ChangedFields []string `json:"changed_fields"`
	}
	var payloads []changedPayload
	for rows.Next() {
		var aggregateType, eventType string
		var raw []byte
		require.NoError(t, rows.Scan(&aggregateType, &eventType, &raw))
		assert.Equal(t, "product", aggregateType)
		assert.Equal(t, "product.changed", eventType)
		var p changedPayload
		require.NoError(t, json.Unmarshal(raw, &p))
		payloads = append(payloads, p)
	}
	require.NoError(t, rows.Err())
	require.Len(t, payloads, 2)
	assert.Equal(t, changedPayload{
		ProductID: string(snap.ProductID), VersionID: payloads[0].VersionID, VersionNumber: 1,
		Fingerprint: payloads[0].Fingerprint, ChangeType: "NEW", ChangedFields: []string{},
	}, payloads[0])
	assert.Equal(t, changedPayload{
		ProductID: string(snap.ProductID), VersionID: *snap.CurrentVersionID, VersionNumber: 2,
		Fingerprint: snap.CurrentFingerprint, ChangeType: "CHANGED", ChangedFields: []string{"canonical_name"},
	}, payloads[1])
}

// F1: A raw record causing database-level constraint error during COPY aborts page 2,
// while page 1 stays committed, cursor stays at page 1 checkpoint, and run fails.
func TestProcessRunBatch_F1_BadRowAbortsCopyRollsBack(t *testing.T) {
	env := setupLiveTestEnv(t)
	ctx := context.Background()

	sourceID := "src-f1-bad-copy"
	createTestSource(t, ctx, env, sourceID)
	run := createTestRun(t, ctx, env, sourceID)

	now := time.Now().UTC()

	// 10 valid records sorted by ID so keyset pagination processes allRecords[:5] as page 1 and allRecords[5:] as page 2
	allRecords := make([]*domainIngestion.RawRecord, 10)
	for i := 0; i < 10; i++ {
		r, err := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
			SourceID:          domainSource.ID(sourceID),
			ExternalProductID: fmt.Sprintf("ext-f1-%d", i),
			Payload:           []byte(fmt.Sprintf(`{"title": "F1 Item %d", "country_code": "US"}`, i)),
			IngestionRunID:    run.ID,
			ReceivedAt:        now,
		})
		require.NoError(t, err)
		allRecords[i] = r
	}
	sort.Slice(allRecords, func(i, j int) bool {
		return allRecords[i].ID < allRecords[j].ID
	})
	require.NoError(t, env.rawRepo.SaveBatch(ctx, allRecords))

	page1 := allRecords[:5]
	page2 := allRecords[5:]

	// Page fail runner simulates a database error (e.g. string truncation 22001) during COPY on page 2
	failingRunner := &failOnPageTxRunner{
		inner:    env.txRunner,
		failPage: 2,
	}

	proc, err := appProduct.NewRunProcessor(env.runRepo, env.sourceRepo, env.rawRepo, env.processingRepo, failingRunner)
	require.NoError(t, err)

	// Run processor with batch size 5
	_, err = proc.ProcessRun(ctx, run.ID, appProduct.ProcessRunOptions{
		BatchSize:     5,
		LeaseDuration: 30 * time.Second,
	})
	require.Error(t, err, "page 2 must fail on database constraint violation during COPY")

	// Page 1 products (5 rows) must remain committed
	var prodCount int
	require.NoError(t, env.pool.QueryRow(ctx, "SELECT count(*) FROM product_sources WHERE source_id = $1", sourceID).Scan(&prodCount))
	assert.Equal(t, 5, prodCount, "page 1 products must remain committed")

	// Every table page 2 wrote to inside its transaction holds only page 1's rows.
	for _, q := range []string{
		"SELECT count(*) FROM products",
		"SELECT count(*) FROM product_versions",
		"SELECT count(*) FROM product_changes",
		"SELECT count(*) FROM outbox_events WHERE aggregate_type = 'product'",
	} {
		var n int
		require.NoError(t, env.pool.QueryRow(ctx, q).Scan(&n))
		assert.Equal(t, 5, n, "page 2 rows must be rolled back: %s", q)
	}

	// Page 1 products must exist in database
	for _, rec := range page1 {
		snap := findTestSnapshot(t, ctx, env, sourceID, rec.ExternalProductID)
		assert.NotNil(t, snap, "page 1 item must exist in database")
	}

	// Page 2 products (0 rows) must be completely rolled back
	for _, rec := range page2 {
		snap := findTestSnapshot(t, ctx, env, sourceID, rec.ExternalProductID)
		assert.Nil(t, snap, "page 2 items must not exist in database")
	}

	// Cursor must remain at the last record of page 1
	rp, err := env.processingRepo.GetByID(ctx, run.ID)
	require.NoError(t, err)
	assert.Equal(t, domainIngestion.ProcessingFailed, rp.Status)
	assert.Equal(t, 5, rp.RecordsSeen)
	assert.Equal(t, page1[4].ID, *rp.CursorRawRecordID)
}

type failOnPageTxRunner struct {
	inner     appProduct.TxRunner
	failPage  int
	pageCount int
	mu        sync.Mutex
}

func (p *failOnPageTxRunner) WithinTx(ctx context.Context, fn func(repos appProduct.TxRepos) error) error {
	p.mu.Lock()
	p.pageCount++
	page := p.pageCount
	p.mu.Unlock()

	return p.inner.WithinTx(ctx, func(repos appProduct.TxRepos) error {
		// Run the real page first so its COPY rows exist inside the transaction; the
		// injected error must then roll all of them back.
		if err := fn(repos); err != nil {
			return err
		}
		if page == p.failPage {
			return &pgconn.PgError{
				Code:    "22001",
				Message: "value too long for type character varying(512)",
			}
		}
		return nil
	})
}

// hookProductRepo intercepts FindSnapshotsByIdentities to inject concurrent modifications immediately after snapshots are read.
type hookProductRepo struct {
	domainProduct.Repository
	onAfterSnapshot func()
}

func (h *hookProductRepo) FindSnapshotsByIdentities(ctx context.Context, refs []domainProduct.IdentityRef) (map[string]*domainProduct.Snapshot, error) {
	snaps, err := h.Repository.FindSnapshotsByIdentities(ctx, refs)
	if err != nil {
		return nil, err
	}
	if h.onAfterSnapshot != nil {
		h.onAfterSnapshot()
	}
	return snaps, nil
}

// hookTxRunner intercepts transaction execution to hook snapshot reads or transaction starts.
type hookTxRunner struct {
	inner           appProduct.TxRunner
	onAfterSnapshot func()
	onFirstTx       func()
	txAttempts      int
	mu              sync.Mutex
}

func (h *hookTxRunner) WithinTx(ctx context.Context, fn func(repos appProduct.TxRepos) error) error {
	h.mu.Lock()
	h.txAttempts++
	attempt := h.txAttempts
	h.mu.Unlock()

	return h.inner.WithinTx(ctx, func(repos appProduct.TxRepos) error {
		if attempt == 1 && h.onFirstTx != nil {
			h.onFirstTx()
		}
		wrappedProducts := &hookProductRepo{
			Repository: repos.Products,
			onAfterSnapshot: func() {
				if attempt == 1 && h.onAfterSnapshot != nil {
					h.onAfterSnapshot()
				}
			},
		}
		return fn(appProduct.TxRepos{
			Products:      wrappedProducts,
			RunProcessing: repos.RunProcessing,
			Outbox:        repos.Outbox,
		})
	})
}

// F2: Concurrent worker creates same identity after snapshot read;
// page retry catches ErrIdentityConflict, re-reads snapshots, and completes cleanly.
func TestProcessRunBatch_F2_IdentityRaceRetriesAndSucceeds(t *testing.T) {
	env := setupLiveTestEnv(t)
	ctx := context.Background()

	sourceID := "src-f2-race"
	createTestSource(t, ctx, env, sourceID)
	run := createTestRun(t, ctx, env, sourceID)

	now := time.Now().UTC()
	rec, err := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
		SourceID:          domainSource.ID(sourceID),
		ExternalProductID: "ext-race-1",
		Payload:           []byte(`{"title": "Race Product", "country_code": "US"}`),
		IngestionRunID:    run.ID,
		ReceivedAt:        now,
	})
	require.NoError(t, err)
	require.NoError(t, env.rawRepo.SaveBatch(ctx, []*domainIngestion.RawRecord{rec}))

	// Hook runner: AFTER snapshot read on attempt 1, another worker commits the same identity
	raceRunner := &hookTxRunner{
		inner: env.txRunner,
		onAfterSnapshot: func() {
			// Another worker commits the same product identity inside its own transaction
			prodID, _ := uuid.NewString()
			verID, _ := uuid.NewString()
			psID, _ := uuid.NewString()
			changeID, _ := uuid.NewString()

			plan := &domainProduct.BatchPlan{
				ProductsToInsert: []*domainProduct.Product{
					{
						ID:                 domainProduct.ID(prodID),
						CanonicalName:      "Concurrent Created Product",
						Status:             domainProduct.StatusDraft,
						CurrentVersionID:   &verID,
						CurrentFingerprint: "fp-concurrent",
						CreatedAt:          now,
						UpdatedAt:          now,
					},
				},
				ProductSourcesToInsert: []*domainProduct.ProductSource{
					{
						ID:                psID,
						ProductID:         domainProduct.ID(prodID),
						SourceID:          sourceID,
						ExternalProductID: "ext-race-1",
						FirstSeenAt:       now,
						LastChangedAt:     now,
						LastReceivedAt:    now,
					},
				},
				ProductVersionsToInsert: []*domainProduct.ProductVersion{
					{
						ID:            verID,
						ProductID:     domainProduct.ID(prodID),
						VersionNumber: 1,
						Fingerprint:   "fp-concurrent",
						CanonicalName: "Concurrent Created Product",
						CreatedAt:     now,
					},
				},
				ProductChangesToInsert: []*domainProduct.ProductChange{
					{
						ID:          changeID,
						ProductID:   domainProduct.ID(prodID),
						ToVersionID: verID,
						ChangeType:  domainProduct.ChangeTypeNew,
						DetectedAt:  now,
					},
				},
			}
			_ = env.txRunner.WithinTx(context.Background(), func(repos appProduct.TxRepos) error {
				return repos.Products.ApplyBatch(context.Background(), plan)
			})
		},
	}

	proc, err := appProduct.NewRunProcessor(env.runRepo, env.sourceRepo, env.rawRepo, env.processingRepo, raceRunner)
	require.NoError(t, err)

	res, err := proc.ProcessRun(ctx, run.ID, appProduct.ProcessRunOptions{
		BatchSize:     50,
		LeaseDuration: 30 * time.Second,
	})
	require.NoError(t, err)

	// ProcessRun retried and succeeded (1 record changed or unchanged)
	assert.Equal(t, 1, res.RecordsSeen)
	assert.Equal(t, 2, raceRunner.txAttempts, "must have retried on identity conflict")

	// Exactly 1 product source in database
	var count int
	require.NoError(t, env.pool.QueryRow(ctx, "SELECT count(*) FROM product_sources WHERE source_id = $1 AND external_product_id = 'ext-race-1'", sourceID).Scan(&count))
	assert.Equal(t, 1, count)
}

// F3: Another worker moves current_version_id after snapshot read;
// guarded update returns fewer rows than expected -> ErrVersionConflict -> retried and succeeds.
func TestProcessRunBatch_F3_VersionConflictRetriesAndSucceeds(t *testing.T) {
	env := setupLiveTestEnv(t)
	ctx := context.Background()

	sourceID := "src-f3-version-race"
	createTestSource(t, ctx, env, sourceID)
	run1 := createTestRun(t, ctx, env, sourceID)

	t0 := time.Now().UTC().Add(-10 * time.Minute)
	t1 := time.Now().UTC()

	// Seed version 1
	rec1, err := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
		SourceID:          domainSource.ID(sourceID),
		ExternalProductID: "ext-ver-race",
		Payload:           []byte(`{"title": "Version 1", "country_code": "US"}`),
		SourceUpdatedAt:   &t0,
		IngestionRunID:    run1.ID,
		ReceivedAt:        t0,
	})
	require.NoError(t, err)
	require.NoError(t, env.rawRepo.SaveBatch(ctx, []*domainIngestion.RawRecord{rec1}))
	_, err = env.processor.ProcessRun(ctx, run1.ID, appProduct.ProcessRunOptions{BatchSize: 50, LeaseDuration: 30 * time.Second})
	require.NoError(t, err)

	snap1 := findTestSnapshot(t, ctx, env, sourceID, "ext-ver-race")
	require.NotNil(t, snap1)

	// Run 2: tries to update to Version 2
	run2 := createTestRun(t, ctx, env, sourceID)
	rec2, err := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
		SourceID:          domainSource.ID(sourceID),
		ExternalProductID: "ext-ver-race",
		Payload:           []byte(`{"title": "Version 2 by Worker", "country_code": "US"}`),
		SourceUpdatedAt:   &t1,
		IngestionRunID:    run2.ID,
		ReceivedAt:        t1,
	})
	require.NoError(t, err)
	require.NoError(t, env.rawRepo.SaveBatch(ctx, []*domainIngestion.RawRecord{rec2}))

	raceRunner := &hookTxRunner{
		inner: env.txRunner,
		onAfterSnapshot: func() {
			// Another worker commits Version 100 on the same product
			v100ID, _ := uuid.NewString()
			now := time.Now().UTC()

			plan := &domainProduct.BatchPlan{
				ProductVersionsToInsert: []*domainProduct.ProductVersion{
					{
						ID:            v100ID,
						ProductID:     snap1.ProductID,
						VersionNumber: 100,
						Fingerprint:   "fp-v100",
						CanonicalName: "Version 100 by Concurrent Worker",
						CreatedAt:     now,
					},
				},
				ProductsToUpdate: []domainProduct.ProductGuardedUpdate{
					{
						ProductID:          snap1.ProductID,
						ExpectedVersionID:  snap1.CurrentVersionID,
						ToVersionID:        v100ID,
						CurrentFingerprint: "fp-v100",
						CanonicalName:      "Version 100 by Concurrent Worker",
						UpdatedAt:          now,
					},
				},
			}
			_ = env.productRepo.ApplyBatch(context.Background(), plan)
		},
	}

	proc, err := appProduct.NewRunProcessor(env.runRepo, env.sourceRepo, env.rawRepo, env.processingRepo, raceRunner)
	require.NoError(t, err)

	res, err := proc.ProcessRun(ctx, run2.ID, appProduct.ProcessRunOptions{
		BatchSize:     50,
		LeaseDuration: 30 * time.Second,
	})
	require.NoError(t, err)

	assert.Equal(t, 1, res.RecordsSeen)
	assert.Equal(t, 2, raceRunner.txAttempts, "must have retried on version conflict")

	// Product version in DB must now be version 101 (sequenced after v100)
	snapFinal := findTestSnapshot(t, ctx, env, sourceID, "ext-ver-race")
	require.NotNil(t, snapFinal)
	require.NotNil(t, snapFinal.StoredCurrentVersion)
	assert.Equal(t, 101, snapFinal.StoredCurrentVersion.VersionNumber)
	assert.Equal(t, "Version 2 by Worker", snapFinal.StoredCurrentVersion.CanonicalName)
}

func TestProcessRunBatch_F4_ContextCancelledMidBatch(t *testing.T) {
	env := setupLiveTestEnv(t)

	sourceID := "src-cancel-batch"
	createTestSource(t, context.Background(), env, sourceID)
	run := createTestRun(t, context.Background(), env, sourceID)

	now := time.Now().UTC()
	// Insert 20 raw records
	records := make([]*domainIngestion.RawRecord, 20)
	for i := 0; i < 20; i++ {
		payload := []byte(fmt.Sprintf(`{"title": "Prod %d", "body": "B", "vendor": "V", "country_code": "US"}`, i))
		rec, err := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
			SourceID:          domainSource.ID(sourceID),
			ExternalProductID: fmt.Sprintf("ext-cancel-%d", i),
			Payload:           payload,
			SourceUpdatedAt:   &now,
			IngestionRunID:    run.ID,
			ReceivedAt:        now,
		})
		require.NoError(t, err)
		records[i] = rec
	}
	require.NoError(t, env.rawRepo.SaveBatch(context.Background(), records))

	// Run processor with a context that cancels after page 1 commits
	ctx, cancel := context.WithCancel(context.Background())
	canceller := &cancellingTxRunner{
		inner:      env.txRunner,
		cancelFunc: cancel,
		target:     1, // Cancel right after first page commits
	}

	proc, err := appProduct.NewRunProcessor(env.runRepo, env.sourceRepo, env.rawRepo, env.processingRepo, canceller)
	require.NoError(t, err)

	_, err = proc.ProcessRun(ctx, run.ID, appProduct.ProcessRunOptions{
		BatchSize:     10,
		LeaseDuration: 30 * time.Second,
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)

	// Verify lease was released cleanly
	rp, err := env.processingRepo.GetByID(context.Background(), run.ID)
	require.NoError(t, err)
	assert.Equal(t, domainIngestion.ProcessingPending, rp.Status, "lease must be released on context cancellation")
	assert.Equal(t, 10, rp.RecordsSeen, "page 1 must be committed and checkpointed")

	// Resume run with new processor and clean context
	res2, err := env.processor.ProcessRun(context.Background(), run.ID, appProduct.ProcessRunOptions{
		BatchSize:     10,
		LeaseDuration: 30 * time.Second,
	})
	require.NoError(t, err)
	assert.Equal(t, 20, res2.RecordsSeen)
	assert.Equal(t, 20, res2.RecordsNew)

	// Verify total 20 products
	var prodCount int
	require.NoError(t, env.pool.QueryRow(context.Background(), "SELECT count(*) FROM product_sources WHERE source_id = $1", sourceID).Scan(&prodCount))
	assert.Equal(t, 20, prodCount)
}

// F5: Lease stolen during page processing causes UpdateProgress to fail with ErrLeaseLost,
// rolling back the entire page transaction and leaving 0 partial rows.
func TestProcessRunBatch_F5_LeaseLost_RollsBack(t *testing.T) {
	env := setupLiveTestEnv(t)
	ctx := context.Background()

	sourceID := "src-lease-lost"
	createTestSource(t, ctx, env, sourceID)
	run := createTestRun(t, ctx, env, sourceID)

	now := time.Now().UTC()
	rec, err := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
		SourceID:          domainSource.ID(sourceID),
		ExternalProductID: "ext-lease-1",
		Payload:           []byte(`{"title": "Prod 1", "body": "B", "vendor": "V", "country_code": "US"}`),
		SourceUpdatedAt:   &now,
		IngestionRunID:    run.ID,
		ReceivedAt:        now,
	})
	require.NoError(t, err)
	require.NoError(t, env.rawRepo.SaveBatch(ctx, []*domainIngestion.RawRecord{rec}))

	tokenA, err := uuid.NewString()
	require.NoError(t, err)
	tokenB, err := uuid.NewString()
	require.NoError(t, err)

	// Runner that steals the lease inside the transaction before UpdateProgress completes
	leaseStealingRunner := &hookTxRunner{
		inner: env.txRunner,
		onFirstTx: func() {
			// Steal claim with token B by overwriting claim_token in DB
			_, execErr := env.pool.Exec(context.Background(), "UPDATE ingestion_run_processing SET claim_token = $1 WHERE run_id = $2", tokenB, run.ID)
			require.NoError(t, execErr)
		},
	}

	proc, err := appProduct.NewRunProcessor(env.runRepo, env.sourceRepo, env.rawRepo, env.processingRepo, leaseStealingRunner)
	require.NoError(t, err)

	_, err = proc.ProcessRun(ctx, run.ID, appProduct.ProcessRunOptions{
		ClaimToken:    tokenA,
		BatchSize:     50,
		LeaseDuration: 30 * time.Second,
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, domainIngestion.ErrLeaseLost)

	// Entire page transaction rolled back: 0 products in DB
	var prodCount int
	require.NoError(t, env.pool.QueryRow(ctx, "SELECT count(*) FROM product_sources WHERE source_id = $1", sourceID).Scan(&prodCount))
	assert.Equal(t, 0, prodCount, "transaction must roll back when lease is lost")
}

func TestProcessRunBatch_F6_NormalizationFailuresInBatch(t *testing.T) {
	env := setupLiveTestEnv(t)
	ctx := context.Background()

	sourceID := "src-norm-failures"
	createTestSource(t, ctx, env, sourceID)
	run := createTestRun(t, ctx, env, sourceID)

	now := time.Now().UTC()
	validPayload := []byte(`{"title": "Valid Product", "body": "Desc", "vendor": "Brand", "country_code": "US"}`)
	invalidPayload := []byte(`{"invalid_json": true}`) // Missing required "title"

	r1, err := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
		SourceID:          domainSource.ID(sourceID),
		ExternalProductID: "ext-valid-1",
		Payload:           validPayload,
		SourceUpdatedAt:   &now,
		IngestionRunID:    run.ID,
		ReceivedAt:        now,
	})
	require.NoError(t, err)

	r2, err := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
		SourceID:          domainSource.ID(sourceID),
		ExternalProductID: "ext-invalid-2",
		Payload:           invalidPayload,
		SourceUpdatedAt:   &now,
		IngestionRunID:    run.ID,
		ReceivedAt:        now,
	})
	require.NoError(t, err)

	require.NoError(t, env.rawRepo.SaveBatch(ctx, []*domainIngestion.RawRecord{r1, r2}))

	// Budget of 60% allows 1 failure out of 2
	res, err := env.processor.ProcessRun(ctx, run.ID, appProduct.ProcessRunOptions{
		BatchSize:     50,
		LeaseDuration: 30 * time.Second,
		ErrorBudget: domainIngestion.ErrorBudget{
			MaxErrorRate:  0.6,
			MinSampleRows: 1,
		},
	})
	require.NoError(t, err)

	assert.Equal(t, 2, res.RecordsSeen)
	assert.Equal(t, 1, res.RecordsNew)
	assert.Equal(t, 1, res.RecordsFailed)

	// Valid product exists
	snap := findTestSnapshot(t, ctx, env, sourceID, "ext-valid-1")
	assert.NotNil(t, snap)

	// Invalid product does not exist
	snapInv := findTestSnapshot(t, ctx, env, sourceID, "ext-invalid-2")
	assert.Nil(t, snapInv)
}

func TestProcessRunBatch_F7_RerunWithFromStart(t *testing.T) {
	env := setupLiveTestEnv(t)
	ctx := context.Background()

	sourceID := "src-from-start"
	createTestSource(t, ctx, env, sourceID)
	run := createTestRun(t, ctx, env, sourceID)

	now := time.Now().UTC()
	records := make([]*domainIngestion.RawRecord, 100)
	for i := 0; i < 100; i++ {
		payload := []byte(fmt.Sprintf(`{"title": "Product %d", "body": "B", "vendor": "V", "country_code": "US"}`, i))
		rec, err := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
			SourceID:          domainSource.ID(sourceID),
			ExternalProductID: fmt.Sprintf("ext-fs-%d", i),
			Payload:           payload,
			SourceUpdatedAt:   &now,
			IngestionRunID:    run.ID,
			ReceivedAt:        now,
		})
		require.NoError(t, err)
		records[i] = rec
	}
	require.NoError(t, env.rawRepo.SaveBatch(ctx, records))

	// Initial run
	res1, err := env.processor.ProcessRun(ctx, run.ID, appProduct.ProcessRunOptions{
		BatchSize:     50,
		LeaseDuration: 30 * time.Second,
	})
	require.NoError(t, err)
	assert.Equal(t, 100, res1.RecordsNew)

	// Second run with FromStart = true (Scenario A unchanged catalog replay)
	res2, err := env.processor.ProcessRun(ctx, run.ID, appProduct.ProcessRunOptions{
		BatchSize:     50,
		LeaseDuration: 30 * time.Second,
		FromStart:     true,
	})
	require.NoError(t, err)

	assert.Equal(t, 100, res2.RecordsSeen)
	assert.Equal(t, 0, res2.RecordsNew)
	assert.Equal(t, 0, res2.RecordsChanged)
	assert.Equal(t, 100, res2.RecordsUnchanged)
	assert.Equal(t, 0, res2.RecordsFailed)

	// Still exactly 100 products and 100 versions (0 new versions created during rerun)
	var prodCount, pvCount, outboxCount int
	require.NoError(t, env.pool.QueryRow(ctx, "SELECT count(*) FROM products").Scan(&prodCount))
	require.NoError(t, env.pool.QueryRow(ctx, "SELECT count(*) FROM product_versions WHERE ingestion_run_id = $1", run.ID).Scan(&pvCount))
	require.NoError(t, env.pool.QueryRow(ctx, "SELECT count(*) FROM outbox_events WHERE aggregate_type = 'product'").Scan(&outboxCount))

	assert.Equal(t, 100, prodCount)
	assert.Equal(t, 100, pvCount)
	assert.Equal(t, 100, outboxCount)
}

// Deadlock resilience: Two concurrent page transactions updating the same 10 products
// execute concurrently. Deterministic sorting and deadlock retry prevent permanent 40P01 failures.
func TestProcessRunBatch_ConcurrentRunsSameSource(t *testing.T) {
	env := setupLiveTestEnv(t)
	ctx := context.Background()

	sourceID := "src-deadlock-resilience"
	createTestSource(t, ctx, env, sourceID)

	t0 := time.Now().UTC().Add(-1 * time.Hour)
	now := time.Now().UTC()

	// 1. Seed 10 products
	seedRun := createTestRun(t, ctx, env, sourceID)
	seedRecords := make([]*domainIngestion.RawRecord, 10)
	for i := 0; i < 10; i++ {
		r, err := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
			SourceID:          domainSource.ID(sourceID),
			ExternalProductID: fmt.Sprintf("ext-dl-%02d", i),
			Payload:           []byte(fmt.Sprintf(`{"title": "Initial DL %d", "country_code": "US"}`, i)),
			SourceUpdatedAt:   &t0,
			IngestionRunID:    seedRun.ID,
			ReceivedAt:        t0,
		})
		require.NoError(t, err)
		seedRecords[i] = r
	}
	require.NoError(t, env.rawRepo.SaveBatch(ctx, seedRecords))
	_, err := env.processor.ProcessRun(ctx, seedRun.ID, appProduct.ProcessRunOptions{BatchSize: 50, LeaseDuration: 30 * time.Second})
	require.NoError(t, err)

	// 2. Create two runs that update all 10 products in different raw-record orders
	runA := createTestRun(t, ctx, env, sourceID)
	runB := createTestRun(t, ctx, env, sourceID)

	recordsA := make([]*domainIngestion.RawRecord, 10)
	recordsB := make([]*domainIngestion.RawRecord, 10)

	for i := 0; i < 10; i++ {
		rA, err := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
			SourceID:          domainSource.ID(sourceID),
			ExternalProductID: fmt.Sprintf("ext-dl-%02d", i),
			Payload:           []byte(fmt.Sprintf(`{"title": "Updated DL %d by A", "country_code": "US"}`, i)),
			SourceUpdatedAt:   &now,
			IngestionRunID:    runA.ID,
			ReceivedAt:        now,
		})
		require.NoError(t, err)
		recordsA[i] = rA

		// Run B has reverse order
		rB, err := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
			SourceID:          domainSource.ID(sourceID),
			ExternalProductID: fmt.Sprintf("ext-dl-%02d", 9-i),
			Payload:           []byte(fmt.Sprintf(`{"title": "Updated DL %d by B", "country_code": "US"}`, 9-i)),
			SourceUpdatedAt:   &now,
			IngestionRunID:    runB.ID,
			ReceivedAt:        now,
		})
		require.NoError(t, err)
		recordsB[i] = rB
	}

	require.NoError(t, env.rawRepo.SaveBatch(ctx, recordsA))
	require.NoError(t, env.rawRepo.SaveBatch(ctx, recordsB))

	var wg sync.WaitGroup
	errs := make([]error, 2)

	wg.Add(2)
	go func() {
		defer wg.Done()
		_, errs[0] = env.processor.ProcessRun(ctx, runA.ID, appProduct.ProcessRunOptions{BatchSize: 10, LeaseDuration: 30 * time.Second})
	}()
	go func() {
		defer wg.Done()
		_, errs[1] = env.processor.ProcessRun(ctx, runB.ID, appProduct.ProcessRunOptions{BatchSize: 10, LeaseDuration: 30 * time.Second})
	}()
	wg.Wait()

	require.NoError(t, errs[0], "run A must succeed without unhandled deadlock")
	require.NoError(t, errs[1], "run B must succeed without unhandled deadlock")

	// Verify all 10 products exist in database and have exactly 1 source each
	var psCount int
	require.NoError(t, env.pool.QueryRow(ctx, "SELECT count(*) FROM product_sources WHERE source_id = $1", sourceID).Scan(&psCount))
	assert.Equal(t, 10, psCount)
}

// Two page transactions bumping the same product_sources in opposite order must not deadlock:
// ApplyBatch sorts every multi-row UPDATE by primary key so all writers lock rows in one order.
// Unsorted, this loop deadlocked (40P01) in 18 of 20 rounds.
func TestApplyBatch_OppositeOrderUpdatesDoNotDeadlock(t *testing.T) {
	env := setupLiveTestEnv(t)
	ctx := context.Background()

	sourceID := "src-apply-lock-order"
	createTestSource(t, ctx, env, sourceID)
	run := createTestRun(t, ctx, env, sourceID)

	const n = 2000
	seededAt := time.Now().UTC().Add(-time.Hour)
	records := make([]*domainIngestion.RawRecord, n)
	for i := range records {
		rec, err := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
			SourceID:          domainSource.ID(sourceID),
			ExternalProductID: fmt.Sprintf("ext-lock-%05d", i),
			Payload:           fmt.Appendf(nil, `{"title": "Lock %d", "country_code": "US"}`, i),
			SourceUpdatedAt:   &seededAt,
			IngestionRunID:    run.ID,
			ReceivedAt:        seededAt,
		})
		require.NoError(t, err)
		records[i] = rec
	}
	require.NoError(t, env.rawRepo.SaveBatch(ctx, records))
	_, err := env.processor.ProcessRun(ctx, run.ID, appProduct.ProcessRunOptions{BatchSize: 500, LeaseDuration: 30 * time.Second})
	require.NoError(t, err)

	rows, err := env.pool.Query(ctx, "SELECT id::text FROM product_sources WHERE source_id = $1 ORDER BY id", sourceID)
	require.NoError(t, err)
	var ascending []string
	for rows.Next() {
		var id string
		require.NoError(t, rows.Scan(&id))
		ascending = append(ascending, id)
	}
	require.NoError(t, rows.Err())
	require.Len(t, ascending, n)
	descending := make([]string, n)
	for i, id := range ascending {
		descending[n-1-i] = id
	}

	watermarkPlan := func(ids []string, at time.Time) *domainProduct.BatchPlan {
		plan := &domainProduct.BatchPlan{}
		for _, id := range ids {
			plan.SourcesToUpdateWatermark = append(plan.SourcesToUpdateWatermark,
				domainProduct.SourceWatermarkUpdate{ProductSourceID: id, SourceUpdatedAt: &at, ReceivedAt: at})
		}
		return plan
	}

	for round := 0; round < 20; round++ {
		at := time.Now().UTC().Add(time.Duration(round) * time.Second)
		var wg sync.WaitGroup
		errs := make([]error, 2)
		for w, ids := range [][]string{ascending, descending} {
			wg.Add(1)
			go func(w int, plan *domainProduct.BatchPlan) {
				defer wg.Done()
				errs[w] = env.txRunner.WithinTx(ctx, func(repos appProduct.TxRepos) error {
					return repos.Products.ApplyBatch(ctx, plan)
				})
			}(w, watermarkPlan(ids, at))
		}
		wg.Wait()
		require.NoError(t, errs[0], "round %d ascending writer", round)
		require.NoError(t, errs[1], "round %d descending writer", round)
	}
}
