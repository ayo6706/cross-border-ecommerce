package sources_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/ayo6706/cross-border-ecommerce/internal/adapters/sources"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/source"
)

type stringReaderCloser struct {
	*bytes.Reader
}

func (s *stringReaderCloser) Close() error {
	return nil
}

type errorReaderCloser struct {
	readErr error
}

func (e *errorReaderCloser) Read(p []byte) (n int, err error) {
	return 0, e.readErr
}

func (e *errorReaderCloser) Close() error {
	return nil
}

func newStringProvider(content string) func() (io.ReadCloser, error) {
	return func() (io.ReadCloser, error) {
		return &stringReaderCloser{Reader: bytes.NewReader([]byte(content))}, nil
	}
}

func TestFeedFileAdapter_CSVStreaming(t *testing.T) {
	csvData := `id,name,price,brand
P1,Smartphone,799.99,Acme
P2,Laptop,1299.50,Dell
P3,Headphones,149.00,Sony
P4,Smartwatch,249.99,Apple
P5,Camera,899.00,Canon
`

	t.Run("BatchedStreamingWithByteOffsetCheckpoint", func(t *testing.T) {
		adapter, err := sources.NewFeedFileAdapter(sources.FeedFileConfig{
			SourceID:       source.ID("src-csv-1"),
			Format:         sources.FeedFormatCSV,
			ReaderProvider: newStringProvider(csvData),
			BatchSize:      2,
		})
		if err != nil {
			t.Fatalf("unexpected init error: %v", err)
		}

		ctx := context.Background()

		// Batch 1: should return P1, P2
		records1, cp1, err := adapter.FetchRecords(ctx, "")
		if err != nil {
			t.Fatalf("unexpected error fetching batch 1: %v", err)
		}
		if len(records1) != 2 {
			t.Fatalf("expected 2 records in batch 1, got %d", len(records1))
		}
		if records1[0].ExternalProductID != "P1" || records1[1].ExternalProductID != "P2" {
			t.Errorf("batch 1 records mismatch: %v, %v", records1[0].ExternalProductID, records1[1].ExternalProductID)
		}
		if cp1 == "" || !strings.HasPrefix(cp1, "offset:") {
			t.Fatalf("expected valid byte offset checkpoint, got %q", cp1)
		}

		// Batch 2: should return P3, P4
		records2, cp2, err := adapter.FetchRecords(ctx, cp1)
		if err != nil {
			t.Fatalf("unexpected error fetching batch 2: %v", err)
		}
		if len(records2) != 2 {
			t.Fatalf("expected 2 records in batch 2, got %d", len(records2))
		}
		if records2[0].ExternalProductID != "P3" || records2[1].ExternalProductID != "P4" {
			t.Errorf("batch 2 records mismatch: %v, %v", records2[0].ExternalProductID, records2[1].ExternalProductID)
		}
		if cp2 == "" {
			t.Fatal("expected non-empty checkpoint after batch 2")
		}

		// Batch 3: should return P5 and empty next checkpoint (EOF)
		records3, cp3, err := adapter.FetchRecords(ctx, cp2)
		if err != nil {
			t.Fatalf("unexpected error fetching batch 3: %v", err)
		}
		if len(records3) != 1 || records3[0].ExternalProductID != "P5" {
			t.Fatalf("expected 1 record P5 in batch 3, got %v", records3)
		}
		if cp3 != "" {
			t.Errorf("expected empty checkpoint at EOF, got %q", cp3)
		}
	})

	t.Run("SkipMalformedRowsInCSV", func(t *testing.T) {
		corruptedCSV := `id,name,price
P10,Item 10,10.0
,Item 11,11.0
P12,Item 12,12.0
`
		var reportedErrors int
		adapter, err := sources.NewFeedFileAdapter(sources.FeedFileConfig{
			SourceID:       source.ID("src-csv-malformed"),
			Format:         sources.FeedFormatCSV,
			ReaderProvider: newStringProvider(corruptedCSV),
			BatchSize:      10,
			SkipMalformed:  true,
			OnRowError: func(rowNumber int, rawRow []byte, err error) {
				reportedErrors++
			},
		})
		if err != nil {
			t.Fatalf("unexpected init error: %v", err)
		}

		records, cp, err := adapter.FetchRecords(context.Background(), "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Only P10 and P12 should be returned (P11 row has empty id)
		if len(records) != 2 {
			t.Fatalf("expected 2 valid records, got %d", len(records))
		}
		if records[0].ExternalProductID != "P10" || records[1].ExternalProductID != "P12" {
			t.Errorf("records mismatch: %v, %v", records[0].ExternalProductID, records[1].ExternalProductID)
		}
		if reportedErrors == 0 {
			t.Error("expected malformed row callback to be invoked")
		}
		if cp != "" {
			t.Errorf("expected empty checkpoint at EOF, got %q", cp)
		}
	})

	t.Run("QuotedNewlinesWithinSingleRecord", func(t *testing.T) {
		csvWithNewlines := "id,name,description\nP1,Widget,\"Line 1\nLine 2\"\nP2,Gadget,Simple\n"
		adapter, err := sources.NewFeedFileAdapter(sources.FeedFileConfig{
			SourceID:       source.ID("src-csv-newlines"),
			Format:         sources.FeedFormatCSV,
			ReaderProvider: newStringProvider(csvWithNewlines),
			BatchSize:      1,
		})
		if err != nil {
			t.Fatalf("unexpected init error: %v", err)
		}

		ctx := context.Background()

		// Batch 1: Should read P1 with the multiline description
		records1, cp1, err := adapter.FetchRecords(ctx, "")
		if err != nil {
			t.Fatalf("unexpected error batch 1: %v", err)
		}
		if len(records1) != 1 || records1[0].ExternalProductID != "P1" {
			t.Fatalf("expected 1 record P1, got %v", records1)
		}
		if !strings.Contains(string(records1[0].Payload), "Line 1\\nLine 2") && !strings.Contains(string(records1[0].Payload), "Line 1\nLine 2") {
			t.Errorf("expected payload to contain multiline text, got %s", string(records1[0].Payload))
		}
		if cp1 == "" {
			t.Fatal("expected non-empty checkpoint after batch 1")
		}

		// Batch 2: Should resume and read P2
		records2, cp2, err := adapter.FetchRecords(ctx, cp1)
		if err != nil {
			t.Fatalf("unexpected error batch 2: %v", err)
		}
		if len(records2) != 1 || records2[0].ExternalProductID != "P2" {
			t.Fatalf("expected 1 record P2, got %v", records2)
		}
		if cp2 != "" {
			t.Errorf("expected empty checkpoint at EOF, got %q", cp2)
		}
	})

	t.Run("CaseInsensitiveIDField", func(t *testing.T) {
		csvData := "sku,title\nSKU-001,Gadget\n"
		adapter, err := sources.NewFeedFileAdapter(sources.FeedFileConfig{
			SourceID:       source.ID("src-case-id"),
			Format:         sources.FeedFormatCSV,
			ReaderProvider: newStringProvider(csvData),
			IDField:        "SKU", // Uppercase in config, lowercase in header
		})
		if err != nil {
			t.Fatalf("unexpected init error: %v", err)
		}

		records, _, err := adapter.FetchRecords(context.Background(), "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(records) != 1 || records[0].ExternalProductID != "SKU-001" {
			t.Fatalf("expected SKU-001, got %v", records)
		}
	})

	t.Run("TerminalReaderErrorNotSkipped", func(t *testing.T) {
		ioErr := errors.New("simulated disk i/o error")
		adapter, err := sources.NewFeedFileAdapter(sources.FeedFileConfig{
			SourceID: source.ID("src-csv-err"),
			Format:   sources.FeedFormatCSV,
			ReaderProvider: func() (io.ReadCloser, error) {
				return &errorReaderCloser{readErr: ioErr}, nil
			},
			SkipMalformed: true,
		})
		if err != nil {
			t.Fatalf("unexpected init error: %v", err)
		}

		_, _, err = adapter.FetchRecords(context.Background(), "")
		if err == nil {
			t.Fatal("expected error on terminal reader error even when SkipMalformed is true, got nil")
		}
		if !errors.Is(err, ioErr) {
			t.Fatalf("expected error to wrap %v, got %v", ioErr, err)
		}
	})

	t.Run("CompositeIdentityInCSV", func(t *testing.T) {
		csvData := "warehouse,part_no,qty\nWH-US,PART-99,10\n"
		adapter, err := sources.NewFeedFileAdapter(sources.FeedFileConfig{
			SourceID:       source.ID("src-csv-comp"),
			Format:         sources.FeedFormatCSV,
			ReaderProvider: newStringProvider(csvData),
			CompositeIDs:   []string{"warehouse", "part_no"},
			CompositeSep:   ":",
		})
		if err != nil {
			t.Fatalf("unexpected init error: %v", err)
		}

		records, _, err := adapter.FetchRecords(context.Background(), "")
		if err != nil {
			t.Fatalf("unexpected fetch error: %v", err)
		}
		if len(records) != 1 || records[0].ExternalProductID != "WH-US:PART-99" {
			t.Fatalf("expected composite id WH-US:PART-99, got %v", records)
		}
	})
}

func TestFeedFileAdapter_NDJSONStreaming(t *testing.T) {
	ndjsonData := `{"id":"J1","title":"Monitor","price":299.99}
{"id":"J2","title":"Keyboard","price":89.99}
{"id":"J3","title":"Mouse","price":49.99}
`

	t.Run("NDJSONBatchedStreaming", func(t *testing.T) {
		adapter, err := sources.NewFeedFileAdapter(sources.FeedFileConfig{
			SourceID:       source.ID("src-ndjson-1"),
			Format:         sources.FeedFormatNDJSON,
			ReaderProvider: newStringProvider(ndjsonData),
			BatchSize:      2,
		})
		if err != nil {
			t.Fatalf("unexpected init error: %v", err)
		}

		ctx := context.Background()

		// Batch 1
		records1, cp1, err := adapter.FetchRecords(ctx, "")
		if err != nil {
			t.Fatalf("unexpected error batch 1: %v", err)
		}
		if len(records1) != 2 {
			t.Fatalf("expected 2 records, got %d", len(records1))
		}
		if records1[0].ExternalProductID != "J1" || records1[1].ExternalProductID != "J2" {
			t.Errorf("batch 1 mismatch: %v, %v", records1[0].ExternalProductID, records1[1].ExternalProductID)
		}

		// Batch 2
		records2, cp2, err := adapter.FetchRecords(ctx, cp1)
		if err != nil {
			t.Fatalf("unexpected error batch 2: %v", err)
		}
		if len(records2) != 1 || records2[0].ExternalProductID != "J3" {
			t.Fatalf("expected J3, got %v", records2)
		}
		if cp2 != "" {
			t.Errorf("expected empty checkpoint at EOF, got %q", cp2)
		}
	})

	t.Run("NDJSONMalformedLineHandling", func(t *testing.T) {
		badNDJSON := `{"id":"GOOD-1","name":"Ok"}
{not-json-line}
{"id":"GOOD-2","name":"Ok"}
`
		// Case 1: Fail-fast when SkipMalformed is false
		failAdapter, _ := sources.NewFeedFileAdapter(sources.FeedFileConfig{
			SourceID:       source.ID("src-fail"),
			Format:         sources.FeedFormatNDJSON,
			ReaderProvider: newStringProvider(badNDJSON),
			SkipMalformed:  false,
		})
		_, _, err := failAdapter.FetchRecords(context.Background(), "")
		if err == nil {
			t.Fatal("expected error on malformed line when SkipMalformed is false, got nil")
		}
		if !errors.Is(err, ingestion.ErrMalformedRecord) {
			t.Errorf("expected ErrMalformedRecord, got %v", err)
		}

		// Case 2: Skip when SkipMalformed is true
		var errorReported bool
		skipAdapter, _ := sources.NewFeedFileAdapter(sources.FeedFileConfig{
			SourceID:       source.ID("src-skip"),
			Format:         sources.FeedFormatNDJSON,
			ReaderProvider: newStringProvider(badNDJSON),
			SkipMalformed:  true,
			OnRowError: func(rowNumber int, rawRow []byte, err error) {
				errorReported = true
			},
		})
		records, _, err := skipAdapter.FetchRecords(context.Background(), "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(records) != 2 {
			t.Fatalf("expected 2 records, got %d", len(records))
		}
		if !errorReported {
			t.Error("expected error callback to be fired for malformed line")
		}
	})

	t.Run("LargeFeedStreamingMemorySimulation", func(t *testing.T) {
		var builder strings.Builder
		for i := 1; i <= 1000; i++ {
			builder.WriteString(fmt.Sprintf(`{"id":"ITEM-%04d","title":"Product %d","price":%d}`+"\n", i, i, i*10))
		}
		largeFeed := builder.String()

		adapter, err := sources.NewFeedFileAdapter(sources.FeedFileConfig{
			SourceID:       source.ID("src-large"),
			Format:         sources.FeedFormatNDJSON,
			ReaderProvider: newStringProvider(largeFeed),
			BatchSize:      250,
		})
		if err != nil {
			t.Fatalf("unexpected init error: %v", err)
		}

		totalSeen := 0
		checkpoint := ""
		ctx := context.Background()

		for {
			batch, nextCp, fetchErr := adapter.FetchRecords(ctx, checkpoint)
			if fetchErr != nil {
				t.Fatalf("unexpected error at checkpoint %s: %v", checkpoint, fetchErr)
			}
			totalSeen += len(batch)
			checkpoint = nextCp
			if checkpoint == "" {
				break
			}
		}

		if totalSeen != 1000 {
			t.Errorf("expected 1000 records processed across batches, got %d", totalSeen)
		}
	})

	t.Run("LargeNumericIDPreservation", func(t *testing.T) {
		ndjsonData := "{\"id\":987654321012345678,\"title\":\"Large ID Product\"}\n"
		adapter, err := sources.NewFeedFileAdapter(sources.FeedFileConfig{
			SourceID:       source.ID("src-ndjson-num"),
			Format:         sources.FeedFormatNDJSON,
			ReaderProvider: newStringProvider(ndjsonData),
		})
		if err != nil {
			t.Fatalf("unexpected init error: %v", err)
		}

		records, _, err := adapter.FetchRecords(context.Background(), "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(records) != 1 || records[0].ExternalProductID != "987654321012345678" {
			t.Fatalf("expected 987654321012345678, got %v", records)
		}
	})

	t.Run("NonEOFReaderErrorReturned", func(t *testing.T) {
		ioErr := errors.New("simulated network read failure")
		adapter, err := sources.NewFeedFileAdapter(sources.FeedFileConfig{
			SourceID: source.ID("src-ndjson-err"),
			Format:   sources.FeedFormatNDJSON,
			ReaderProvider: func() (io.ReadCloser, error) {
				return &errorReaderCloser{readErr: ioErr}, nil
			},
			SkipMalformed: true,
		})
		if err != nil {
			t.Fatalf("unexpected init error: %v", err)
		}

		_, _, err = adapter.FetchRecords(context.Background(), "")
		if err == nil {
			t.Fatal("expected error on non-EOF reader error, got nil")
		}
		if !errors.Is(err, ioErr) {
			t.Fatalf("expected error to wrap %v, got %v", ioErr, err)
		}
	})

	t.Run("CompositeIdentityInNDJSON", func(t *testing.T) {
		ndjsonData := "{\"site\":\"UK\",\"item_code\":\"SKU-555\"}\n"
		adapter, err := sources.NewFeedFileAdapter(sources.FeedFileConfig{
			SourceID:       source.ID("src-ndjson-comp"),
			Format:         sources.FeedFormatNDJSON,
			ReaderProvider: newStringProvider(ndjsonData),
			CompositeIDs:   []string{"site", "item_code"},
			CompositeSep:   "#",
		})
		if err != nil {
			t.Fatalf("unexpected init error: %v", err)
		}

		records, _, err := adapter.FetchRecords(context.Background(), "")
		if err != nil {
			t.Fatalf("unexpected fetch error: %v", err)
		}
		if len(records) != 1 || records[0].ExternalProductID != "UK#SKU-555" {
			t.Fatalf("expected composite id UK#SKU-555, got %v", records)
		}
	})
}
