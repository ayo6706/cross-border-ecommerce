package ingestion_test

import (
	"errors"
	"testing"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
)

func TestIngestionRun_NewRun(t *testing.T) {
	t.Parallel()

	run, err := ingestion.NewRun("run-001", "src-01", "offset-0")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if run.ID != "run-001" {
		t.Fatalf("expected ID run-001, got %s", run.ID)
	}
	if run.SourceID != "src-01" {
		t.Fatalf("expected SourceID src-01, got %s", run.SourceID)
	}
	if run.Status != ingestion.StatusPending {
		t.Fatalf("expected status PENDING, got %s", run.Status)
	}
	if run.Checkpoint != "offset-0" {
		t.Fatalf("expected checkpoint offset-0, got %s", run.Checkpoint)
	}

	// Empty ID generates UUID
	autoRun, err := ingestion.NewRun("", "src-01", "")
	if err != nil {
		t.Fatalf("unexpected error on empty ID: %v", err)
	}
	if autoRun.ID == "" {
		t.Fatalf("expected non-empty auto-generated run ID")
	}

	// Direct validation on empty ID
	emptyRun := &ingestion.IngestionRun{ID: "", SourceID: "src-01", Status: ingestion.StatusPending}
	if !errors.Is(emptyRun.Validate(), ingestion.ErrInvalidRunID) {
		t.Fatalf("expected ErrInvalidRunID on empty ID validation, got %v", emptyRun.Validate())
	}

	// Invalid SourceID
	_, err = ingestion.NewRun("run-002", "", "")
	if !errors.Is(err, ingestion.ErrInvalidSourceID) {
		t.Fatalf("expected ErrInvalidSourceID, got %v", err)
	}
}

func TestIngestionRun_LifecycleSuccess(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	run, err := ingestion.NewRun("run-100", "src-01", "chk-0")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Start run
	if err := run.Start(now); err != nil {
		t.Fatalf("unexpected error starting run: %v", err)
	}
	if run.Status != ingestion.StatusRunning {
		t.Fatalf("expected status RUNNING, got %s", run.Status)
	}
	if run.StartedAt == nil {
		t.Fatalf("expected StartedAt to be set")
	}

	// Cannot start again
	if err := run.Start(now); !errors.Is(err, ingestion.ErrInvalidTransition) {
		t.Fatalf("expected ErrInvalidTransition on double start, got %v", err)
	}

	// Record batch 1
	batch1 := ingestion.BatchMetrics{
		Seen:      100,
		New:       20,
		Changed:   10,
		Unchanged: 70,
		Failed:    0,
	}
	if err := run.RecordBatch(batch1, "chk-100", now.Add(time.Second)); err != nil {
		t.Fatalf("unexpected error recording batch 1: %v", err)
	}
	if run.RecordsSeen != 100 || run.RecordsNew != 20 || run.RecordsChanged != 10 || run.RecordsUnchanged != 70 {
		t.Fatalf("unexpected metrics after batch 1: %+v", run)
	}
	if run.Checkpoint != "chk-100" {
		t.Fatalf("expected checkpoint chk-100, got %s", run.Checkpoint)
	}

	// Record batch 2
	batch2 := ingestion.BatchMetrics{
		Seen:      50,
		New:       10,
		Changed:   5,
		Unchanged: 35,
		Failed:    0,
	}
	if err := run.RecordBatch(batch2, "chk-150", now.Add(2*time.Second)); err != nil {
		t.Fatalf("unexpected error recording batch 2: %v", err)
	}
	if run.RecordsSeen != 150 || run.RecordsNew != 30 || run.RecordsChanged != 15 || run.RecordsUnchanged != 105 {
		t.Fatalf("unexpected metrics after batch 2: %+v", run)
	}

	// Complete run without failures -> COMPLETED
	if err := run.Complete("chk-150", now.Add(3*time.Second)); err != nil {
		t.Fatalf("unexpected error completing run: %v", err)
	}
	if run.Status != ingestion.StatusCompleted {
		t.Fatalf("expected COMPLETED status, got %s", run.Status)
	}
	if run.CompletedAt == nil {
		t.Fatalf("expected CompletedAt to be set")
	}

	// Cannot record batch or complete after completion
	if err := run.RecordBatch(batch1, "", now); !errors.Is(err, ingestion.ErrInvalidRunState) {
		t.Fatalf("expected ErrInvalidRunState, got %v", err)
	}
	if err := run.Complete("", now); !errors.Is(err, ingestion.ErrInvalidTransition) {
		t.Fatalf("expected ErrInvalidTransition, got %v", err)
	}
}

func TestIngestionRun_PartialAndFailures(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	// Scenario 1: Partial completion when some records fail
	run, _ := ingestion.NewRun("run-200", "src-01", "")
	_ = run.Start(now)
	err := run.RecordBatch(ingestion.BatchMetrics{
		Seen:   50,
		Failed: 5,
	}, "chk-50", now)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := run.Complete("chk-50", now); err != nil {
		t.Fatalf("unexpected error completing run: %v", err)
	}
	if run.Status != ingestion.StatusPartial {
		t.Fatalf("expected PARTIAL status when records failed, got %s", run.Status)
	}

	// Scenario 2: Fatal failure
	run2, _ := ingestion.NewRun("run-201", "src-01", "")
	_ = run2.Start(now)
	if err := run2.Fail("supplier api network timeout 504", now); err != nil {
		t.Fatalf("unexpected error failing run: %v", err)
	}
	if run2.Status != ingestion.StatusFailed {
		t.Fatalf("expected FAILED status, got %s", run2.Status)
	}
	if run2.ErrorSummary != "supplier api network timeout 504" {
		t.Fatalf("expected error summary to match, got %s", run2.ErrorSummary)
	}

	// Scenario 3: Cancel run
	run3, _ := ingestion.NewRun("run-202", "src-01", "")
	_ = run3.Start(now)
	if err := run3.Cancel("manual operator cancellation", now); err != nil {
		t.Fatalf("unexpected error cancelling run: %v", err)
	}
	if run3.Status != ingestion.StatusCancelled {
		t.Fatalf("expected CANCELLED status, got %s", run3.Status)
	}

	// Scenario 4: Negative metric rejection
	run4, _ := ingestion.NewRun("run-203", "src-01", "")
	_ = run4.Start(now)
	negErr := run4.RecordBatch(ingestion.BatchMetrics{Seen: -1}, "", now)
	if !errors.Is(negErr, ingestion.ErrNegativeMetric) {
		t.Fatalf("expected ErrNegativeMetric, got %v", negErr)
	}
}
