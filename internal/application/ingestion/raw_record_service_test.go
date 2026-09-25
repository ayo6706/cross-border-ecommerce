package ingestion_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	appingestion "github.com/ayo6706/cross-border-ecommerce/internal/application/ingestion"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/source"
)

type memoryRawRecordRepo struct {
	mu      sync.RWMutex
	records map[string]*ingestion.RawRecord
}

func newMemoryRawRecordRepo() *memoryRawRecordRepo {
	return &memoryRawRecordRepo{
		records: make(map[string]*ingestion.RawRecord),
	}
}

func newMemorySourceRepo() *memorySourceRepo {
	return &memorySourceRepo{
		sources: make(map[source.ID]*source.Source),
	}
}

func (m *memoryRawRecordRepo) Save(_ context.Context, record *ingestion.RawRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if record == nil {
		return ingestion.ErrInvalidRecordState
	}
	if record.ID == "" {
		record.ID = fmt.Sprintf("raw-%d", len(m.records)+1)
	}
	cp := *record
	m.records[record.ID] = &cp
	return nil
}

func (m *memoryRawRecordRepo) SaveBatch(_ context.Context, records []*ingestion.RawRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, record := range records {
		if record == nil {
			return ingestion.ErrInvalidRecordState
		}
		if record.ID == "" {
			record.ID = fmt.Sprintf("raw-%d", len(m.records)+1)
		}
		cp := *record
		m.records[record.ID] = &cp
	}
	return nil
}

func (m *memoryRawRecordRepo) FindByID(_ context.Context, id string) (*ingestion.RawRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	r, ok := m.records[id]
	if !ok {
		return nil, ingestion.ErrRecordNotFound
	}
	cp := *r
	return &cp, nil
}

func (m *memoryRawRecordRepo) FindLatestBySourceAndExternalID(
	_ context.Context,
	sourceID source.ID,
	externalProductID string,
) (*ingestion.RawRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var matches []*ingestion.RawRecord
	for _, r := range m.records {
		if r.SourceID == sourceID && r.ExternalProductID == externalProductID {
			cp := *r
			matches = append(matches, &cp)
		}
	}
	if len(matches) == 0 {
		return nil, ingestion.ErrRecordNotFound
	}

	slices.SortFunc(matches, func(a, b *ingestion.RawRecord) int {
		if a.ReceivedAt.Equal(b.ReceivedAt) {
			return 0
		}
		if a.ReceivedAt.After(b.ReceivedAt) {
			return -1
		}
		return 1
	})

	return matches[0], nil
}

func (m *memoryRawRecordRepo) ListBySourceAndExternalID(
	_ context.Context,
	sourceID source.ID,
	externalProductID string,
	limit int,
) ([]*ingestion.RawRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var matches []*ingestion.RawRecord
	for _, r := range m.records {
		if r.SourceID == sourceID && r.ExternalProductID == externalProductID {
			cp := *r
			matches = append(matches, &cp)
		}
	}

	slices.SortFunc(matches, func(a, b *ingestion.RawRecord) int {
		if a.ReceivedAt.Equal(b.ReceivedAt) {
			return 0
		}
		if a.ReceivedAt.After(b.ReceivedAt) {
			return -1
		}
		return 1
	})

	if limit > 0 && len(matches) > limit {
		matches = matches[:limit]
	}
	return matches, nil
}

func (m *memoryRawRecordRepo) ListByRunID(
	_ context.Context,
	runID string,
	limit int,
) ([]*ingestion.RawRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var matches []*ingestion.RawRecord
	for _, r := range m.records {
		if r.IngestionRunID == runID {
			cp := *r
			matches = append(matches, &cp)
		}
	}

	slices.SortFunc(matches, func(a, b *ingestion.RawRecord) int {
		if a.ReceivedAt.Equal(b.ReceivedAt) {
			return 0
		}
		if a.ReceivedAt.After(b.ReceivedAt) {
			return -1
		}
		return 1
	})

	if limit > 0 && len(matches) > limit {
		matches = matches[:limit]
	}
	return matches, nil
}

func (m *memoryRawRecordRepo) ListKeysetByRunID(
	_ context.Context,
	runID string,
	cursorID *string,
	limit int,
) ([]*ingestion.RawRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var matches []*ingestion.RawRecord
	for _, r := range m.records {
		if r.IngestionRunID == runID {
			if cursorID != nil && strings.TrimSpace(*cursorID) != "" && r.ID <= *cursorID {
				continue
			}
			cp := *r
			matches = append(matches, &cp)
		}
	}

	slices.SortFunc(matches, func(a, b *ingestion.RawRecord) int {
		return strings.Compare(a.ID, b.ID)
	})

	if limit > 0 && len(matches) > limit {
		matches = matches[:limit]
	}
	return matches, nil
}

func TestRawRecordService_Constructor(t *testing.T) {
	rawRepo := newMemoryRawRecordRepo()
	sourceRepo := newMemorySourceRepo()
	runRepo := newMemoryRunRepo()

	t.Run("NilRawRepo", func(t *testing.T) {
		svc, err := appingestion.NewRawRecordService(nil, sourceRepo, runRepo)
		if err == nil || svc != nil {
			t.Fatal("expected error with nil raw record repo")
		}
	})

	t.Run("NilSourceRepo", func(t *testing.T) {
		svc, err := appingestion.NewRawRecordService(rawRepo, nil, runRepo)
		if err == nil || svc != nil {
			t.Fatal("expected error with nil source repo")
		}
	})

	t.Run("NilRunRepo", func(t *testing.T) {
		svc, err := appingestion.NewRawRecordService(rawRepo, sourceRepo, nil)
		if err == nil || svc != nil {
			t.Fatal("expected error with nil run repo")
		}
	})

	t.Run("ValidConstruct", func(t *testing.T) {
		svc, err := appingestion.NewRawRecordService(rawRepo, sourceRepo, runRepo)
		if err != nil || svc == nil {
			t.Fatalf("unexpected error creating service: %v", err)
		}
	})
}

func TestRawRecordService_StoreRawRecord(t *testing.T) {
	rawRepo := newMemoryRawRecordRepo()
	sourceRepo := newMemorySourceRepo()
	runRepo := newMemoryRunRepo()

	apiCfg := map[string]any{"base_url": "https://api.example.com"}
	activeSrc, _ := source.NewSource("src-active", "Active Supplier", source.TypeAPI, apiCfg, 100)
	_ = sourceRepo.Save(context.Background(), activeSrc)

	disabledSrc, _ := source.NewSource("src-disabled", "Disabled Supplier", source.TypeAPI, apiCfg, 100)
	disabledSrc.Enabled = false
	_ = sourceRepo.Save(context.Background(), disabledSrc)

	run, _ := ingestion.NewRun("run-100", "src-active", "")
	_ = run.Start(time.Now())
	_ = runRepo.CreateRun(context.Background(), run)

	svc, err := appingestion.NewRawRecordService(rawRepo, sourceRepo, runRepo)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		rec, err := svc.StoreRawRecord(ctx, appingestion.StoreRawRecordParams{
			SourceID:          "src-active",
			ExternalProductID: "PROD-1",
			Payload:           []byte(`{"title": "Watch", "price": 199.99}`),
			SourceVersion:     "v1",
			ETag:              "etag-1",
			IngestionRunID:    "run-100",
		})
		if err != nil {
			t.Fatalf("expected success, got: %v", err)
		}
		if rec.ID == "" {
			t.Error("expected non-empty record ID")
		}
		if rec.SourceID != "src-active" {
			t.Errorf("expected source src-active, got %s", rec.SourceID)
		}
	})

	t.Run("InactiveSource", func(t *testing.T) {
		_, err := svc.StoreRawRecord(ctx, appingestion.StoreRawRecordParams{
			SourceID:          "src-disabled",
			ExternalProductID: "PROD-2",
			Payload:           []byte(`{"title": "Pen"}`),
		})
		if !errors.Is(err, ingestion.ErrInactiveSource) {
			t.Fatalf("expected ErrInactiveSource, got: %v", err)
		}
	})

	t.Run("EmptySourceID", func(t *testing.T) {
		_, err := svc.StoreRawRecord(ctx, appingestion.StoreRawRecordParams{
			SourceID:          "",
			ExternalProductID: "PROD-3",
			Payload:           []byte(`{"title": "Book"}`),
		})
		if !errors.Is(err, ingestion.ErrInvalidSourceID) {
			t.Fatalf("expected ErrInvalidSourceID, got: %v", err)
		}
	})

	t.Run("InvalidPayloadJSON", func(t *testing.T) {
		_, err := svc.StoreRawRecord(ctx, appingestion.StoreRawRecordParams{
			SourceID:          "src-active",
			ExternalProductID: "PROD-4",
			Payload:           []byte(`invalid-json`),
		})
		if err == nil {
			t.Fatal("expected error on invalid JSON payload")
		}
	})

	t.Run("NonExistentRun", func(t *testing.T) {
		_, err := svc.StoreRawRecord(ctx, appingestion.StoreRawRecordParams{
			SourceID:          "src-active",
			ExternalProductID: "PROD-5",
			Payload:           []byte(`{"title": "Phone"}`),
			IngestionRunID:    "run-does-not-exist",
		})
		if err == nil {
			t.Fatal("expected error when ingestion run does not exist")
		}
	})
}

func TestRawRecordService_StoreRawRecordBatch(t *testing.T) {
	rawRepo := newMemoryRawRecordRepo()
	sourceRepo := newMemorySourceRepo()
	runRepo := newMemoryRunRepo()

	apiCfg := map[string]any{"base_url": "https://api.example.com"}
	activeSrc, _ := source.NewSource("src-active", "Active Supplier", source.TypeAPI, apiCfg, 100)
	_ = sourceRepo.Save(context.Background(), activeSrc)

	run, _ := ingestion.NewRun("run-200", "src-active", "")
	_ = run.Start(time.Now())
	_ = runRepo.CreateRun(context.Background(), run)

	svc, err := appingestion.NewRawRecordService(rawRepo, sourceRepo, runRepo)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	ctx := context.Background()

	t.Run("EmptyBatchReturnsEmpty", func(t *testing.T) {
		records, err := svc.StoreRawRecordBatch(ctx, "src-active", "run-200", nil)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if len(records) != 0 {
			t.Fatalf("expected 0 records, got: %d", len(records))
		}
	})

	t.Run("SuccessBatch", func(t *testing.T) {
		r1, _ := ingestion.NewRawRecord(ingestion.RawRecordParams{SourceID: "src-active", ExternalProductID: "SKU-B1", Payload: []byte(`{"name": "Item 1"}`), SourceVersion: "v1"})
		r2, _ := ingestion.NewRawRecord(ingestion.RawRecordParams{SourceID: "src-active", ExternalProductID: "SKU-B2", Payload: []byte(`{"name": "Item 2"}`), SourceVersion: "v1"})

		records, err := svc.StoreRawRecordBatch(ctx, "src-active", "run-200", []*ingestion.RawRecord{r1, r2})
		if err != nil {
			t.Fatalf("expected success, got: %v", err)
		}
		if len(records) != 2 {
			t.Fatalf("expected 2 records, got: %d", len(records))
		}
		for _, r := range records {
			if r.IngestionRunID != "run-200" {
				t.Errorf("expected run ID run-200, got: %s", r.IngestionRunID)
			}
		}
	})

	t.Run("InvalidItemInBatchFailsFast", func(t *testing.T) {
		r1, _ := ingestion.NewRawRecord(ingestion.RawRecordParams{SourceID: "src-active", ExternalProductID: "SKU-GOOD", Payload: []byte(`{"name": "Good"}`)})
		r2 := &ingestion.RawRecord{
			ExternalProductID: "", // invalid
			Payload:           []byte(`{"name": "Bad"}`),
		}

		_, err := svc.StoreRawRecordBatch(ctx, "src-active", "run-200", []*ingestion.RawRecord{r1, r2})
		if err == nil {
			t.Fatal("expected error on invalid item in batch")
		}
	})
}

func TestRawRecordService_Queries(t *testing.T) {
	rawRepo := newMemoryRawRecordRepo()
	sourceRepo := newMemorySourceRepo()
	runRepo := newMemoryRunRepo()

	apiCfg := map[string]any{"base_url": "https://api.example.com"}
	activeSrc, _ := source.NewSource("src-query", "Query Supplier", source.TypeAPI, apiCfg, 100)
	_ = sourceRepo.Save(context.Background(), activeSrc)

	svc, err := appingestion.NewRawRecordService(rawRepo, sourceRepo, runRepo)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	ctx := context.Background()

	// Seed 2 records
	rec1, err := svc.StoreRawRecord(ctx, appingestion.StoreRawRecordParams{
		SourceID:          "src-query",
		ExternalProductID: "SKU-QUERY-1",
		Payload:           []byte(`{"version": 1}`),
		SourceVersion:     "v1",
		ReceivedAt:        time.Now().Add(-1 * time.Hour),
	})
	if err != nil {
		t.Fatalf("seed rec1: %v", err)
	}

	rec2, err := svc.StoreRawRecord(ctx, appingestion.StoreRawRecordParams{
		SourceID:          "src-query",
		ExternalProductID: "SKU-QUERY-1",
		Payload:           []byte(`{"version": 2}`),
		SourceVersion:     "v2",
		ReceivedAt:        time.Now(),
	})
	if err != nil {
		t.Fatalf("seed rec2: %v", err)
	}

	t.Run("GetRecordByID", func(t *testing.T) {
		found, err := svc.GetRecordByID(ctx, rec1.ID)
		if err != nil {
			t.Fatalf("get by id: %v", err)
		}
		if found.ID != rec1.ID {
			t.Errorf("expected ID %s, got %s", rec1.ID, found.ID)
		}

		_, err = svc.GetRecordByID(ctx, "")
		if !errors.Is(err, ingestion.ErrInvalidRecordID) {
			t.Fatalf("expected ErrInvalidRecordID on empty ID, got: %v", err)
		}
	})

	t.Run("GetLatestRecord", func(t *testing.T) {
		latest, err := svc.GetLatestRecord(ctx, "src-query", "SKU-QUERY-1")
		if err != nil {
			t.Fatalf("get latest: %v", err)
		}
		if latest.ID != rec2.ID {
			t.Fatalf("expected latest rec2 %s, got: %s", rec2.ID, latest.ID)
		}

		_, err = svc.GetLatestRecord(ctx, "", "SKU-QUERY-1")
		if !errors.Is(err, ingestion.ErrInvalidSourceID) {
			t.Fatalf("expected ErrInvalidSourceID, got: %v", err)
		}

		_, err = svc.GetLatestRecord(ctx, "src-query", "")
		if !errors.Is(err, ingestion.ErrInvalidExternalProductID) {
			t.Fatalf("expected ErrInvalidExternalProductID, got: %v", err)
		}
	})

	t.Run("ListRecords", func(t *testing.T) {
		list, err := svc.ListRecords(ctx, "src-query", "SKU-QUERY-1", 10)
		if err != nil {
			t.Fatalf("list records: %v", err)
		}
		if len(list) != 2 {
			t.Fatalf("expected 2 records, got: %d", len(list))
		}
		// First should be the newest
		if list[0].SourceVersion != "v2" {
			t.Errorf("expected newest first, got version %s", list[0].SourceVersion)
		}
	})
}
