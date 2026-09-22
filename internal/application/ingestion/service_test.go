package ingestion_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	appingestion "github.com/ayo6706/cross-border-ecommerce/internal/application/ingestion"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/source"
)

type memoryRunRepo struct {
	mu   sync.RWMutex
	runs map[string]*ingestion.IngestionRun
}

func newMemoryRunRepo() *memoryRunRepo {
	return &memoryRunRepo{
		runs: make(map[string]*ingestion.IngestionRun),
	}
}

func (m *memoryRunRepo) CreateRun(_ context.Context, run *ingestion.IngestionRun) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if run.ID == "" {
		run.ID = fmt.Sprintf("run-%d", len(m.runs)+1)
	}
	cp := *run
	m.runs[run.ID] = &cp
	return nil
}

func (m *memoryRunRepo) FindRunByID(_ context.Context, id string) (*ingestion.IngestionRun, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	r, ok := m.runs[id]
	if !ok {
		return nil, ingestion.ErrRunNotFound
	}
	cp := *r
	return &cp, nil
}

func (m *memoryRunRepo) UpdateProgress(_ context.Context, id string, metrics ingestion.BatchMetrics, checkpoint string, updatedAt time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	r, ok := m.runs[id]
	if !ok {
		return ingestion.ErrRunNotFound
	}

	r.RecordsSeen += metrics.Seen
	r.RecordsNew += metrics.New
	r.RecordsChanged += metrics.Changed
	r.RecordsUnchanged += metrics.Unchanged
	r.RecordsFailed += metrics.Failed
	if checkpoint != "" {
		r.Checkpoint = checkpoint
	}
	r.UpdatedAt = updatedAt
	return nil
}

func (m *memoryRunRepo) UpdateStatus(_ context.Context, id string, status ingestion.RunStatus, errorSummary string, checkpoint string, completedAt time.Time, updatedAt time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	r, ok := m.runs[id]
	if !ok {
		return ingestion.ErrRunNotFound
	}

	r.Status = status
	r.ErrorSummary = errorSummary
	if checkpoint != "" {
		r.Checkpoint = checkpoint
	}
	r.CompletedAt = &completedAt
	r.UpdatedAt = updatedAt
	return nil
}

func (m *memoryRunRepo) ListRunsBySource(_ context.Context, sourceID source.ID, limit int) ([]*ingestion.IngestionRun, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	res := make([]*ingestion.IngestionRun, 0, len(m.runs))
	for _, r := range m.runs {
		if r.SourceID == sourceID {
			cp := *r
			res = append(res, &cp)
		}
	}

	slices.SortFunc(res, func(a, b *ingestion.IngestionRun) int {
		return b.CreatedAt.Compare(a.CreatedAt)
	})

	if limit > 0 && len(res) > limit {
		res = res[:limit]
	}
	return res, nil
}

func (m *memoryRunRepo) FindLatestRunBySource(_ context.Context, sourceID source.ID) (*ingestion.IngestionRun, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var latest *ingestion.IngestionRun
	for _, r := range m.runs {
		if r.SourceID == sourceID {
			if latest == nil || r.CreatedAt.After(latest.CreatedAt) {
				cp := *r
				latest = &cp
			}
		}
	}

	if latest == nil {
		return nil, ingestion.ErrRunNotFound
	}
	return latest, nil
}

type memorySourceRepo struct {
	mu      sync.RWMutex
	sources map[source.ID]*source.Source
}

func (m *memorySourceRepo) FindByID(_ context.Context, id source.ID) (*source.Source, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.sources[id]
	if !ok {
		return nil, source.ErrSourceNotFound
	}
	cp := *s
	return &cp, nil
}

func (m *memorySourceRepo) FindActive(_ context.Context) ([]*source.Source, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	res := make([]*source.Source, 0, len(m.sources))
	for _, s := range m.sources {
		if s.Enabled {
			cp := *s
			res = append(res, &cp)
		}
	}
	return res, nil
}

func (m *memorySourceRepo) List(_ context.Context) ([]*source.Source, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	res := make([]*source.Source, 0, len(m.sources))
	for _, s := range m.sources {
		cp := *s
		res = append(res, &cp)
	}
	return res, nil
}

func (m *memorySourceRepo) Save(_ context.Context, s *source.Source) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := *s
	m.sources[s.ID] = &cp
	return nil
}

func (m *memorySourceRepo) Delete(_ context.Context, id source.ID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sources, id)
	return nil
}

func TestIngestionService_StartRun(t *testing.T) {
	t.Parallel()

	runRepo := newMemoryRunRepo()
	sourceRepo := &memorySourceRepo{
		sources: map[source.ID]*source.Source{
			"active-src": {
				ID:      "active-src",
				Name:    "Active Supplier",
				Type:    source.TypeAPI,
				Enabled: true,
			},
			"disabled-src": {
				ID:      "disabled-src",
				Name:    "Disabled Supplier",
				Type:    source.TypeFeed,
				Enabled: false,
			},
		},
	}

	svc, err := appingestion.NewService(runRepo, sourceRepo)
	if err != nil {
		t.Fatalf("failed to create ingestion service: %v", err)
	}

	ctx := context.Background()

	// 1. Success on active source -> creates run with status RUNNING
	run, err := svc.StartRun(ctx, "active-src", "initial-page-0")
	if err != nil {
		t.Fatalf("unexpected error starting run: %v", err)
	}
	if run.Status != ingestion.StatusRunning {
		t.Fatalf("expected status RUNNING, got %s", run.Status)
	}
	if run.Checkpoint != "initial-page-0" {
		t.Fatalf("expected checkpoint initial-page-0, got %s", run.Checkpoint)
	}

	// 2. Error on inactive/disabled source
	_, err = svc.StartRun(ctx, "disabled-src", "")
	if !errors.Is(err, ingestion.ErrInactiveSource) {
		t.Fatalf("expected ErrInactiveSource, got %v", err)
	}

	// 3. Error on non-existent source
	_, err = svc.StartRun(ctx, "non-existent", "")
	if !errors.Is(err, source.ErrSourceNotFound) {
		t.Fatalf("expected ErrSourceNotFound, got %v", err)
	}
}

func TestIngestionService_RecordBatchAndComplete(t *testing.T) {
	t.Parallel()

	runRepo := newMemoryRunRepo()
	sourceRepo := &memorySourceRepo{
		sources: map[source.ID]*source.Source{
			"src-1": {ID: "src-1", Enabled: true, Type: source.TypeAPI},
		},
	}

	svc, _ := appingestion.NewService(runRepo, sourceRepo)
	ctx := context.Background()

	run, err := svc.StartRun(ctx, "src-1", "")
	if err != nil {
		t.Fatalf("failed to start run: %v", err)
	}

	// Record batch
	err = svc.RecordBatch(ctx, run.ID, ingestion.BatchMetrics{
		Seen:      1000,
		New:       100,
		Changed:   50,
		Unchanged: 850,
		Failed:    0,
	}, "offset-1000")
	if err != nil {
		t.Fatalf("failed to record batch: %v", err)
	}

	updated, err := svc.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("failed to get run: %v", err)
	}
	if updated.RecordsSeen != 1000 || updated.Checkpoint != "offset-1000" {
		t.Fatalf("unexpected updated metrics: %+v", updated)
	}

	// Complete run
	if err := svc.CompleteRun(ctx, run.ID, "offset-1000"); err != nil {
		t.Fatalf("failed to complete run: %v", err)
	}

	completed, _ := svc.GetRun(ctx, run.ID)
	if completed.Status != ingestion.StatusCompleted {
		t.Fatalf("expected COMPLETED status, got %s", completed.Status)
	}
}

func TestIngestionService_ResumeRun(t *testing.T) {
	t.Parallel()

	runRepo := newMemoryRunRepo()
	sourceRepo := &memorySourceRepo{
		sources: map[source.ID]*source.Source{
			"src-1": {ID: "src-1", Enabled: true, Type: source.TypeAPI},
		},
	}

	svc, _ := appingestion.NewService(runRepo, sourceRepo)
	ctx := context.Background()

	run1, _ := svc.StartRun(ctx, "src-1", "cursor-0")
	_ = svc.RecordBatch(ctx, run1.ID, ingestion.BatchMetrics{Seen: 500, Failed: 10}, "cursor-500")
	_ = svc.FailRun(ctx, run1.ID, "network blip")

	// Resume from run1
	resumedRun, err := svc.ResumeRun(ctx, run1.ID)
	if err != nil {
		t.Fatalf("failed to resume run: %v", err)
	}
	if resumedRun.Checkpoint != "cursor-500" {
		t.Fatalf("expected resumed run to inherit checkpoint cursor-500, got %s", resumedRun.Checkpoint)
	}
	if resumedRun.Status != ingestion.StatusRunning {
		t.Fatalf("expected resumed run to be RUNNING, got %s", resumedRun.Status)
	}
}
