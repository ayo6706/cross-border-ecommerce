package ingestion_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	appingestion "github.com/ayo6706/cross-border-ecommerce/internal/application/ingestion"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/source"
)

type memoryTxRunner struct {
	runs       ingestion.Repository
	rawRecords ingestion.RawRecordRepository
}

func (m *memoryTxRunner) WithinTx(ctx context.Context, fn func(repos appingestion.TxRepos) error) error {
	return fn(appingestion.TxRepos{
		Runs:       m.runs,
		RawRecords: m.rawRecords,
	})
}

type fakeAdapter struct {
	mu      sync.Mutex
	batches []ingestion.FetchResult
	errs    []error
	callIdx int
}

func (f *fakeAdapter) Fetch(ctx context.Context, req ingestion.FetchRequest) (ingestion.FetchResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.callIdx < len(f.errs) && f.errs[f.callIdx] != nil {
		err := f.errs[f.callIdx]
		f.callIdx++
		return ingestion.FetchResult{}, err
	}

	if f.callIdx < len(f.batches) {
		res := f.batches[f.callIdx]
		f.callIdx++
		return res, nil
	}

	return ingestion.FetchResult{HasMore: false}, nil
}

func TestSyncCoordinator_SyncSource(t *testing.T) {
	ctx := context.Background()

	setup := func(t *testing.T) (*appingestion.SyncCoordinator, *memoryRunRepo, *memoryRawRecordRepo, *memorySourceRepo, source.ID) {
		runRepo := newMemoryRunRepo()
		rawRepo := newMemoryRawRecordRepo()
		sourceRepo := newMemorySourceRepo()

		srcID := source.ID("src-sync-test")
		src, _ := source.NewSource(srcID, "Sync Test Source", source.TypeAPI, map[string]any{"base_url": "https://api.example.com"}, 100)
		_ = sourceRepo.Save(ctx, src)

		runService, err := appingestion.NewService(runRepo, sourceRepo)
		if err != nil {
			t.Fatalf("failed to create run service: %v", err)
		}

		txRunner := &memoryTxRunner{runs: runRepo, rawRecords: rawRepo}
		coordinator, err := appingestion.NewSyncCoordinator(runService, txRunner)
		if err != nil {
			t.Fatalf("failed to create sync coordinator: %v", err)
		}

		return coordinator, runRepo, rawRepo, sourceRepo, srcID
	}

	t.Run("FullSyncSuccess", func(t *testing.T) {
		coordinator, runRepo, rawRepo, _, srcID := setup(t)

		rec1, _ := ingestion.NewRawRecord(ingestion.RawRecordParams{SourceID: srcID, ExternalProductID: "P-1", Payload: []byte(`{"name":"item1"}`), SourceVersion: "v1"})
		rec2, _ := ingestion.NewRawRecord(ingestion.RawRecordParams{SourceID: srcID, ExternalProductID: "P-2", Payload: []byte(`{"name":"item2"}`), SourceVersion: "v1"})
		rec3, _ := ingestion.NewRawRecord(ingestion.RawRecordParams{SourceID: srcID, ExternalProductID: "P-3", Payload: []byte(`{"name":"item3"}`), SourceVersion: "v1"})

		adapter := &fakeAdapter{
			batches: []ingestion.FetchResult{
				{
					Records:        []*ingestion.RawRecord{rec1, rec2},
					NextCheckpoint: "cp-page-2",
					HasMore:        true,
				},
				{
					Records:        []*ingestion.RawRecord{rec3},
					NextCheckpoint: "cp-page-final",
					HasMore:        false,
				},
			},
		}

		run, err := coordinator.SyncSource(ctx, appingestion.SyncParams{
			SourceID:          srcID,
			Adapter:           adapter,
			InitialCheckpoint: "cp-start",
			BatchSize:         2,
		})
		if err != nil {
			t.Fatalf("unexpected sync error: %v", err)
		}

		if run.Status != ingestion.StatusCompleted {
			t.Errorf("expected status COMPLETED, got %s", run.Status)
		}
		if run.RecordsSeen != 3 {
			t.Errorf("expected 3 records seen, got %d", run.RecordsSeen)
		}
		if run.Checkpoint != "cp-page-final" {
			t.Errorf("expected final checkpoint cp-page-final, got %s", run.Checkpoint)
		}

		// Verify records stored in raw repo
		if len(rawRepo.records) != 3 {
			t.Errorf("expected 3 raw records stored in repo, got %d", len(rawRepo.records))
		}
		for _, r := range rawRepo.records {
			if r.IngestionRunID != run.ID {
				t.Errorf("expected record stamped with runID %s, got %s", run.ID, r.IngestionRunID)
			}
		}

		// Verify database run state
		dbRun, _ := runRepo.FindRunByID(ctx, run.ID)
		if dbRun.Status != ingestion.StatusCompleted {
			t.Errorf("expected db run status COMPLETED, got %s", dbRun.Status)
		}
	})

	t.Run("PartialFailureAdvancesCheckpointToLastSuccessfulBatch", func(t *testing.T) {
		coordinator, runRepo, rawRepo, _, srcID := setup(t)

		rec1, _ := ingestion.NewRawRecord(ingestion.RawRecordParams{SourceID: srcID, ExternalProductID: "P-1", Payload: []byte(`{"name":"item1"}`), SourceVersion: "v1"})

		adapter := &fakeAdapter{
			batches: []ingestion.FetchResult{
				{
					Records:        []*ingestion.RawRecord{rec1},
					NextCheckpoint: "cp-batch-1-ok",
					HasMore:        true,
				},
			},
			errs: []error{
				nil,
				errors.New("upstream connection reset"),
			},
		}

		run, err := coordinator.SyncSource(ctx, appingestion.SyncParams{
			SourceID:  srcID,
			Adapter:   adapter,
			BatchSize: 1,
		})
		if err == nil {
			t.Fatal("expected error on failed second batch fetch")
		}

		// The run must be marked FAILED
		dbRun, _ := runRepo.FindRunByID(ctx, run.ID)
		if dbRun.Status != ingestion.StatusFailed {
			t.Errorf("expected run status FAILED, got %s", dbRun.Status)
		}

		// Checkpoint must reflect exactly the last successfully committed batch (cp-batch-1-ok)
		if dbRun.Checkpoint != "cp-batch-1-ok" {
			t.Errorf("expected checkpoint to be cp-batch-1-ok, got %s", dbRun.Checkpoint)
		}
		if dbRun.RecordsSeen != 1 {
			t.Errorf("expected 1 record seen from batch 1, got %d", dbRun.RecordsSeen)
		}

		// Exactly 1 record saved
		if len(rawRepo.records) != 1 {
			t.Errorf("expected 1 raw record saved, got %d", len(rawRepo.records))
		}
	})

	t.Run("ContextCancellationMarksRunCancelled", func(t *testing.T) {
		coordinator, runRepo, _, _, srcID := setup(t)

		cancelCtx, cancel := context.WithCancel(ctx)
		cancel() // cancel immediately

		adapter := &fakeAdapter{}
		run, err := coordinator.SyncSource(cancelCtx, appingestion.SyncParams{
			SourceID: srcID,
			Adapter:  adapter,
		})
		if err == nil {
			t.Fatal("expected context cancellation error")
		}

		dbRun, _ := runRepo.FindRunByID(ctx, run.ID)
		if dbRun.Status != ingestion.StatusCancelled {
			t.Errorf("expected status CANCELLED, got %s", dbRun.Status)
		}
	})

	t.Run("ProgressGuardViolationFailsRun", func(t *testing.T) {
		coordinator, runRepo, _, _, srcID := setup(t)

		rec, _ := ingestion.NewRawRecord(ingestion.RawRecordParams{SourceID: srcID, ExternalProductID: "P-1", Payload: []byte(`{"name":"item1"}`), SourceVersion: "v1"})
		adapter := &fakeAdapter{
			batches: []ingestion.FetchResult{
				{
					Records:        []*ingestion.RawRecord{rec},
					NextCheckpoint: "cp-same",
					HasMore:        true,
				},
			},
		}

		run, err := coordinator.SyncSource(ctx, appingestion.SyncParams{
			SourceID:          srcID,
			Adapter:           adapter,
			InitialCheckpoint: "cp-same",
		})
		if err == nil {
			t.Fatal("expected ErrSourceContractViolation on same checkpoint with HasMore=true")
		}
		if !errors.Is(err, ingestion.ErrSourceContractViolation) {
			t.Errorf("expected ErrSourceContractViolation, got %v", err)
		}

		dbRun, _ := runRepo.FindRunByID(ctx, run.ID)
		if dbRun.Status != ingestion.StatusFailed {
			t.Errorf("expected status FAILED, got %s", dbRun.Status)
		}
	})

	t.Run("SkippedRowsWithinBudgetMarkRunPartial", func(t *testing.T) {
		coordinator, runRepo, rawRepo, _, srcID := setup(t)

		adapter := &fakeAdapter{
			batches: []ingestion.FetchResult{
				{Records: newTestRecords(t, srcID, 99), Failed: 1, NextCheckpoint: "cp-end"},
			},
		}

		run, err := coordinator.SyncSource(ctx, appingestion.SyncParams{SourceID: srcID, Adapter: adapter})
		if err != nil {
			t.Fatalf("unexpected sync error: %v", err)
		}
		if run.Status != ingestion.StatusPartial {
			t.Errorf("expected status PARTIAL, got %s", run.Status)
		}
		if run.RecordsSeen != 100 || run.RecordsFailed != 1 {
			t.Errorf("expected 100 seen and 1 failed, got %d seen and %d failed", run.RecordsSeen, run.RecordsFailed)
		}
		if len(rawRepo.records) != 99 {
			t.Errorf("expected 99 raw records saved, got %d", len(rawRepo.records))
		}

		dbRun, _ := runRepo.FindRunByID(ctx, run.ID)
		if dbRun.Status != ingestion.StatusPartial {
			t.Errorf("expected db run status PARTIAL, got %s", dbRun.Status)
		}
	})

	t.Run("ErrorBudgetIsEnforcedOverRunTotals", func(t *testing.T) {
		coordinator, runRepo, _, _, srcID := setup(t)

		// Batch 1 alone is clean; batch 2 pushes the run total to 7/15 failed.
		adapter := &fakeAdapter{
			batches: []ingestion.FetchResult{
				{Records: newTestRecords(t, srcID, 5), NextCheckpoint: "cp-2", HasMore: true},
				{Records: newTestRecords(t, srcID, 3), Failed: 7, NextCheckpoint: "cp-3", HasMore: true},
			},
		}

		run, err := coordinator.SyncSource(ctx, appingestion.SyncParams{
			SourceID:    srcID,
			Adapter:     adapter,
			ErrorBudget: ingestion.ErrorBudget{MaxErrorRate: 0.10, MinSampleRows: 10},
		})
		if !errors.Is(err, ingestion.ErrErrorBudgetExceeded) {
			t.Fatalf("expected ErrErrorBudgetExceeded, got %v", err)
		}

		dbRun, _ := runRepo.FindRunByID(ctx, run.ID)
		if dbRun.Status != ingestion.StatusFailed {
			t.Errorf("expected status FAILED, got %s", dbRun.Status)
		}
		if dbRun.RecordsSeen != 15 || dbRun.RecordsFailed != 7 {
			t.Errorf("expected progress of the failing batch to be recorded (15 seen, 7 failed), got %d seen, %d failed",
				dbRun.RecordsSeen, dbRun.RecordsFailed)
		}
	})

	t.Run("EmptyBatchStillAdvancesCheckpoint", func(t *testing.T) {
		coordinator, runRepo, _, _, srcID := setup(t)

		adapter := &fakeAdapter{
			batches: []ingestion.FetchResult{
				{NextCheckpoint: "cp-after-empty-page", HasMore: true},
			},
			errs: []error{nil, errors.New("upstream connection reset")},
		}

		run, err := coordinator.SyncSource(ctx, appingestion.SyncParams{SourceID: srcID, Adapter: adapter})
		if err == nil {
			t.Fatal("expected fetch error on second batch")
		}

		dbRun, _ := runRepo.FindRunByID(ctx, run.ID)
		if dbRun.Checkpoint != "cp-after-empty-page" {
			t.Errorf("expected checkpoint from empty page to be persisted, got %q", dbRun.Checkpoint)
		}
	})
}

func newTestRecords(t *testing.T, srcID source.ID, n int) []*ingestion.RawRecord {
	t.Helper()

	records := make([]*ingestion.RawRecord, 0, n)
	for i := range n {
		rec, err := ingestion.NewRawRecord(ingestion.RawRecordParams{
			SourceID:          srcID,
			ExternalProductID: fmt.Sprintf("P-%d", i),
			Payload:           []byte(`{"name":"item"}`),
		})
		if err != nil {
			t.Fatalf("failed to build test record: %v", err)
		}
		records = append(records, rec)
	}
	return records
}
