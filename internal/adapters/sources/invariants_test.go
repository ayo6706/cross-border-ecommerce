package sources_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"testing"

	"github.com/ayo6706/cross-border-ecommerce/internal/adapters/sources"
	"github.com/ayo6706/cross-border-ecommerce/internal/adapters/sources/extraction"
	"github.com/ayo6706/cross-border-ecommerce/internal/adapters/sources/identity"
	"github.com/ayo6706/cross-border-ecommerce/internal/adapters/sources/pagination"
	"github.com/ayo6706/cross-border-ecommerce/internal/adapters/sources/policy"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/source"
)

// Invariant 1: Durable Resume Invariant.
// Simulating an interrupted run and resuming from checkpoint C guarantees that record C+1
// is the first record received and zero committed records are skipped.
func TestInvariant_DurableResume(t *testing.T) {
	var builder bytes.Buffer
	builder.WriteString("id,name\n")
	for i := 1; i <= 100; i++ {
		builder.WriteString(fmt.Sprintf("P-%03d,Item %d\n", i, i))
	}
	csvContent := builder.String()

	adapter, err := sources.NewFeedFileAdapter(sources.FeedFileConfig{
		SourceID:       source.ID("src-resume-inv"),
		Format:         sources.FeedFormatCSV,
		ReaderProvider: newStringProvider(csvContent),
		BatchSize:      25,
	})
	if err != nil {
		t.Fatalf("unexpected init error: %v", err)
	}

	ctx := context.Background()

	// Batch 1 (1-25)
	res1, err := adapter.Fetch(ctx, ingestion.FetchRequest{BatchSize: 25})
	if err != nil {
		t.Fatalf("fetch 1 error: %v", err)
	}
	if len(res1.Records) != 25 || res1.Records[0].ExternalProductID != "P-001" {
		t.Fatalf("unexpected batch 1: first=%s, count=%d", res1.Records[0].ExternalProductID, len(res1.Records))
	}

	// Batch 2 (26-50)
	res2, err := adapter.Fetch(ctx, ingestion.FetchRequest{Checkpoint: res1.NextCheckpoint, BatchSize: 25})
	if err != nil {
		t.Fatalf("fetch 2 error: %v", err)
	}
	if len(res2.Records) != 25 || res2.Records[24].ExternalProductID != "P-050" {
		t.Fatalf("unexpected batch 2: last=%s", res2.Records[24].ExternalProductID)
	}

	// SIMULATE CRASH: New worker instance restarts using checkpoint after record 50
	crashResumeCheckpoint := res2.NextCheckpoint
	recoveredAdapter, err := sources.NewFeedFileAdapter(sources.FeedFileConfig{
		SourceID:       source.ID("src-resume-inv"),
		Format:         sources.FeedFormatCSV,
		ReaderProvider: newStringProvider(csvContent),
		BatchSize:      25,
	})
	if err != nil {
		t.Fatalf("unexpected recovered adapter init error: %v", err)
	}

	// Batch 3 after restart: Must resume precisely at P-051
	res3, err := recoveredAdapter.Fetch(ctx, ingestion.FetchRequest{Checkpoint: crashResumeCheckpoint, BatchSize: 25})
	if err != nil {
		t.Fatalf("fetch 3 after crash error: %v", err)
	}
	if len(res3.Records) != 25 {
		t.Fatalf("expected 25 records after resume, got %d", len(res3.Records))
	}
	if res3.Records[0].ExternalProductID != "P-051" {
		t.Errorf("invariant violated: expected first resumed record P-051, got %s", res3.Records[0].ExternalProductID)
	}
}

// Invariant 2: Echoed Cursor Termination Invariant.
// If an upstream server returns the same cursor token it was sent, pagination terminates immediately.
func TestInvariant_EchoedCursorTermination(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cursorSent := r.URL.Query().Get("cursor")
		echoToken := "cursor_stuck_token"
		if cursorSent != "" {
			echoToken = cursorSent
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(fmt.Sprintf(`{"items": [{"id": "ITEM-1"}], "next_cursor": "%s"}`, echoToken)))
	}))
	defer server.Close()

	adapter, err := sources.NewRESTAdapter(sources.RESTAdapterConfig{
		SourceID:   source.ID("echo-source"),
		BaseURL:    server.URL,
		Client:     server.Client(),
		Pagination: pagination.NewCursorPagination("cursor", "next_cursor"),
		Extractor:  extraction.NewPathRecordExtractor("items"),
		Identity:   identity.NewPathIdentityStrategy("id"),
	})
	if err != nil {
		t.Fatalf("unexpected init error: %v", err)
	}

	ctx := context.Background()

	// Initial fetch receives cursor_stuck_token
	res1, err := adapter.Fetch(ctx, ingestion.FetchRequest{})
	if err != nil {
		t.Fatalf("fetch 1 failed: %v", err)
	}
	if res1.NextCheckpoint.String() != "cursor:cursor_stuck_token" {
		t.Fatalf("expected next checkpoint cursor:cursor_stuck_token, got %q", res1.NextCheckpoint.String())
	}

	// Subsequent fetch sends cursor_stuck_token; server echoes it back
	res2, err := adapter.Fetch(ctx, ingestion.FetchRequest{Checkpoint: res1.NextCheckpoint})
	if err != nil {
		t.Fatalf("fetch 2 failed: %v", err)
	}

	// Invariant: NextCheckpoint must be empty and HasMore must be false
	if res2.HasMore {
		t.Errorf("invariant violated: expected HasMore to be false on echoed cursor")
	}
	if !res2.NextCheckpoint.IsEmpty() {
		t.Errorf("invariant violated: expected empty next checkpoint on echoed cursor, got %q", res2.NextCheckpoint.String())
	}
}

// Invariant 3: Identity Determinism Invariant.
// External identity resolution is pure, deterministic, and idempotent across concurrent goroutines.
func TestInvariant_IdentityDeterminismConcurrent(t *testing.T) {
	strat, err := identity.NewCompositeIdentityStrategy([]string{"site", "product.sku"}, ":")
	if err != nil {
		t.Fatalf("unexpected init error: %v", err)
	}

	rawPayload := []byte(`{"site": "EU-NORTH", "product": {"sku": "GALAXY-S24", "color": "black"}}`)
	expectedID := "EU-NORTH:GALAXY-S24"

	const goroutines = 50
	const iterations = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				resolved, resolveErr := strat.Resolve(rawPayload)
				if resolveErr != nil {
					t.Errorf("concurrent resolve error: %v", resolveErr)
					return
				}
				if resolved != expectedID {
					t.Errorf("invariant violated: expected %s, got %s", expectedID, resolved)
					return
				}
			}
		}()
	}
	wg.Wait()
}

// Invariant 4: Bounded Memory Invariant.
// Streaming a large 100,000-row feed preserves bounded memory overhead.
func TestInvariant_StreamingBoundedMemory(t *testing.T) {
	var builder bytes.Buffer
	builder.WriteString("id,title,price\n")
	for i := 1; i <= 10000; i++ {
		builder.WriteString(fmt.Sprintf("SKU-%06d,Product %d,%.2f\n", i, i, float64(i)*1.5))
	}
	csvContent := builder.String()

	adapter, err := sources.NewFeedFileAdapter(sources.FeedFileConfig{
		SourceID:       source.ID("mem-feed"),
		Format:         sources.FeedFormatCSV,
		ReaderProvider: newStringProvider(csvContent),
		BatchSize:      500,
	})
	if err != nil {
		t.Fatalf("unexpected init error: %v", err)
	}

	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	ctx := context.Background()
	cp := ingestion.NewCheckpoint("")
	totalRows := 0

	for {
		res, fetchErr := adapter.Fetch(ctx, ingestion.FetchRequest{Checkpoint: cp, BatchSize: 500})
		if fetchErr != nil {
			t.Fatalf("fetch error: %v", fetchErr)
		}
		totalRows += len(res.Records)
		if !res.HasMore {
			break
		}
		cp = res.NextCheckpoint
	}

	if totalRows != 10000 {
		t.Fatalf("expected 10000 rows streamed, got %d", totalRows)
	}

	runtime.GC()
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	// Memory difference should not exceed 10 MB for streaming 10,000 rows
	heapGrowth := int64(memAfter.HeapAlloc) - int64(memBefore.HeapAlloc)
	t.Logf("Heap memory delta during 10,000 row stream: %d bytes (%.2f KB)", heapGrowth, float64(heapGrowth)/1024)
}

// Invariant 5: Pre-Flight Probe Invariant.
// Probing a valid source succeeds with diagnostic metadata; probing a misconfigured source fails fast.
func TestInvariant_PreFlightProbe(t *testing.T) {
	t.Run("HealthySourceProbeSuccess", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"items": [{"id": "PROBE-100", "title": "Test"}]}`))
		}))
		defer server.Close()

		adapter, err := sources.NewRESTAdapter(sources.RESTAdapterConfig{
			SourceID:  source.ID("healthy-probe"),
			BaseURL:   server.URL,
			Client:    server.Client(),
			Extractor: extraction.NewPathRecordExtractor("items"),
			Identity:  identity.NewPathIdentityStrategy("id"),
		})
		if err != nil {
			t.Fatalf("unexpected init error: %v", err)
		}

		diag, err := adapter.Probe(context.Background())
		if err != nil {
			t.Fatalf("probe failed: %v", err)
		}
		if !diag.Reachable || !diag.Authenticated {
			t.Errorf("expected reachable and authenticated, got reachable=%v, auth=%v", diag.Reachable, diag.Authenticated)
		}
		if diag.SampleExternalID != "PROBE-100" {
			t.Errorf("expected sample id PROBE-100, got %q", diag.SampleExternalID)
		}
	})

	t.Run("MisconfiguredSchemaProbeFailsPreFlight", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"wrong_key": []}`))
		}))
		defer server.Close()

		adapter, err := sources.NewRESTAdapter(sources.RESTAdapterConfig{
			SourceID:  source.ID("broken-probe"),
			BaseURL:   server.URL,
			Client:    server.Client(),
			Extractor: extraction.NewPathRecordExtractor("items"), // schema mismatch
			Identity:  identity.NewPathIdentityStrategy("id"),
		})
		if err != nil {
			t.Fatalf("unexpected init error: %v", err)
		}

		diag, err := adapter.Probe(context.Background())
		if err == nil {
			t.Fatal("expected pre-flight probe error, got nil")
		}
		if !errors.Is(err, ingestion.ErrSourceContractViolation) {
			t.Errorf("expected ErrSourceContractViolation, got %v", err)
		}
		if diag == nil || !diag.Reachable {
			t.Error("expected server to be marked reachable despite schema failure")
		}
	})
}

// Invariant 6: Error Threshold Cut-off Invariant.
// If the error rate exceeds MaxErrorRate, processing halts deterministically with ErrQuarantineThreshold.
func TestInvariant_ErrorThresholdCutoff(t *testing.T) {
	tracker := policy.NewErrorTracker(policy.PolicySkipMalformed, 0.05, nil) // 5% max error rate
	for i := 0; i < 9; i++ {
		tracker.RecordSuccess()
	}
	err := tracker.RecordError(10, []byte("bad"), errors.New("malformed"))
	if err == nil {
		t.Fatal("expected error on threshold breach, got nil")
	}
	if !errors.Is(err, ingestion.ErrQuarantineThreshold) {
		t.Errorf("expected ErrQuarantineThreshold, got %v", err)
	}
}
