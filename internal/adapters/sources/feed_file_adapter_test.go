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
	"github.com/ayo6706/cross-border-ecommerce/internal/adapters/sources/policy"
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
		res1, err := adapter.Fetch(ctx, ingestion.FetchRequest{BatchSize: 2})
		if err != nil {
			t.Fatalf("unexpected error fetching batch 1: %v", err)
		}
		if len(res1.Records) != 2 {
			t.Fatalf("expected 2 records in batch 1, got %d", len(res1.Records))
		}
		if res1.Records[0].ExternalProductID != "P1" || res1.Records[1].ExternalProductID != "P2" {
			t.Errorf("batch 1 records mismatch: %v, %v", res1.Records[0].ExternalProductID, res1.Records[1].ExternalProductID)
		}
		if res1.NextCheckpoint == "" || !strings.HasPrefix(res1.NextCheckpoint, "offset:") {
			t.Fatalf("expected valid byte offset checkpoint, got %q", res1.NextCheckpoint)
		}

		// Batch 2: should return P3, P4
		res2, err := adapter.Fetch(ctx, ingestion.FetchRequest{Checkpoint: res1.NextCheckpoint, BatchSize: 2})
		if err != nil {
			t.Fatalf("unexpected error fetching batch 2: %v", err)
		}
		if len(res2.Records) != 2 {
			t.Fatalf("expected 2 records in batch 2, got %d", len(res2.Records))
		}
		if res2.Records[0].ExternalProductID != "P3" || res2.Records[1].ExternalProductID != "P4" {
			t.Errorf("batch 2 records mismatch: %v, %v", res2.Records[0].ExternalProductID, res2.Records[1].ExternalProductID)
		}
		if res2.NextCheckpoint == "" {
			t.Fatal("expected non-empty checkpoint after batch 2")
		}

		// Batch 3: should return P5 and empty next checkpoint (EOF)
		res3, err := adapter.Fetch(ctx, ingestion.FetchRequest{Checkpoint: res2.NextCheckpoint, BatchSize: 2})
		if err != nil {
			t.Fatalf("unexpected error fetching batch 3: %v", err)
		}
		if len(res3.Records) != 1 || res3.Records[0].ExternalProductID != "P5" {
			t.Fatalf("expected 1 record P5 in batch 3, got %v", res3.Records)
		}
		if res3.NextCheckpoint != "" {
			t.Errorf("expected empty checkpoint at EOF, got %q", res3.NextCheckpoint)
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
			ErrorPolicy: policy.ErrorPolicy{
				Policy: policy.PolicySkipMalformed,
				OnRowError: func(rowNumber int, rawRow []byte, err error) {
					reportedErrors++
				},
			},
		})
		if err != nil {
			t.Fatalf("unexpected init error: %v", err)
		}

		res, err := adapter.Fetch(context.Background(), ingestion.FetchRequest{BatchSize: 10})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(res.Records) != 2 {
			t.Fatalf("expected 2 valid records, got %d", len(res.Records))
		}
		if res.Records[0].ExternalProductID != "P10" || res.Records[1].ExternalProductID != "P12" {
			t.Errorf("records mismatch: %v, %v", res.Records[0].ExternalProductID, res.Records[1].ExternalProductID)
		}
		if reportedErrors == 0 {
			t.Error("expected malformed row callback to be invoked")
		}
		if res.NextCheckpoint != "" {
			t.Errorf("expected empty checkpoint at EOF, got %q", res.NextCheckpoint)
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
		res1, err := adapter.Fetch(ctx, ingestion.FetchRequest{BatchSize: 1})
		if err != nil {
			t.Fatalf("unexpected error batch 1: %v", err)
		}
		if len(res1.Records) != 1 || res1.Records[0].ExternalProductID != "P1" {
			t.Fatalf("expected 1 record P1, got %v", res1.Records)
		}
		if !strings.Contains(string(res1.Records[0].Payload), "Line 1\\nLine 2") && !strings.Contains(string(res1.Records[0].Payload), "Line 1\nLine 2") {
			t.Errorf("expected payload to contain multiline text, got %s", string(res1.Records[0].Payload))
		}
		if res1.NextCheckpoint == "" {
			t.Fatal("expected non-empty checkpoint after batch 1")
		}

		// Batch 2: Should resume and read P2
		res2, err := adapter.Fetch(ctx, ingestion.FetchRequest{Checkpoint: res1.NextCheckpoint, BatchSize: 1})
		if err != nil {
			t.Fatalf("unexpected error batch 2: %v", err)
		}
		if len(res2.Records) != 1 || res2.Records[0].ExternalProductID != "P2" {
			t.Fatalf("expected 1 record P2, got %v", res2.Records)
		}
		if res2.NextCheckpoint != "" {
			t.Errorf("expected empty checkpoint at EOF, got %q", res2.NextCheckpoint)
		}
	})

	t.Run("CaseInsensitiveIDField", func(t *testing.T) {
		csvData := "sku,title\nSKU-001,Gadget\n"
		adapter, err := sources.NewFeedFileAdapter(sources.FeedFileConfig{
			SourceID:       source.ID("src-case-id"),
			Format:         sources.FeedFormatCSV,
			ReaderProvider: newStringProvider(csvData),
			IDField:        "SKU",
		})
		if err != nil {
			t.Fatalf("unexpected init error: %v", err)
		}

		res, err := adapter.Fetch(context.Background(), ingestion.FetchRequest{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(res.Records) != 1 || res.Records[0].ExternalProductID != "SKU-001" {
			t.Fatalf("expected SKU-001, got %v", res.Records)
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
			ErrorPolicy: policy.ErrorPolicy{Policy: policy.PolicySkipMalformed},
		})
		if err != nil {
			t.Fatalf("unexpected init error: %v", err)
		}

		_, err = adapter.Fetch(context.Background(), ingestion.FetchRequest{})
		if err == nil {
			t.Fatal("expected error on terminal reader error even under the skip-malformed policy, got nil")
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

		res, err := adapter.Fetch(context.Background(), ingestion.FetchRequest{})
		if err != nil {
			t.Fatalf("unexpected fetch error: %v", err)
		}
		if len(res.Records) != 1 || res.Records[0].ExternalProductID != "WH-US:PART-99" {
			t.Fatalf("expected composite id WH-US:PART-99, got %v", res.Records)
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
		res1, err := adapter.Fetch(ctx, ingestion.FetchRequest{BatchSize: 2})
		if err != nil {
			t.Fatalf("unexpected error batch 1: %v", err)
		}
		if len(res1.Records) != 2 {
			t.Fatalf("expected 2 records, got %d", len(res1.Records))
		}
		if res1.Records[0].ExternalProductID != "J1" || res1.Records[1].ExternalProductID != "J2" {
			t.Errorf("batch 1 mismatch: %v, %v", res1.Records[0].ExternalProductID, res1.Records[1].ExternalProductID)
		}

		// Batch 2
		res2, err := adapter.Fetch(ctx, ingestion.FetchRequest{Checkpoint: res1.NextCheckpoint, BatchSize: 2})
		if err != nil {
			t.Fatalf("unexpected error batch 2: %v", err)
		}
		if len(res2.Records) != 1 || res2.Records[0].ExternalProductID != "J3" {
			t.Fatalf("expected J3, got %v", res2.Records)
		}
		if res2.NextCheckpoint != "" {
			t.Errorf("expected empty checkpoint at EOF, got %q", res2.NextCheckpoint)
		}
	})

	t.Run("NDJSONMalformedLineHandling", func(t *testing.T) {
		badNDJSON := `{"id":"GOOD-1","name":"Ok"}
{not-json-line}
{"id":"GOOD-2","name":"Ok"}
`
		// Case 1: Fail-fast under the default fail-fast policy
		failAdapter, _ := sources.NewFeedFileAdapter(sources.FeedFileConfig{
			SourceID:       source.ID("src-fail"),
			Format:         sources.FeedFormatNDJSON,
			ReaderProvider: newStringProvider(badNDJSON),
		})
		_, err := failAdapter.Fetch(context.Background(), ingestion.FetchRequest{})
		if err == nil {
			t.Fatal("expected error on malformed line under the default fail-fast policy, got nil")
		}
		if !errors.Is(err, ingestion.ErrMalformedRecord) {
			t.Errorf("expected ErrMalformedRecord, got %v", err)
		}

		// Case 2: Skip under the skip-malformed policy
		var errorReported bool
		skipAdapter, _ := sources.NewFeedFileAdapter(sources.FeedFileConfig{
			SourceID:       source.ID("src-skip"),
			Format:         sources.FeedFormatNDJSON,
			ReaderProvider: newStringProvider(badNDJSON),
			ErrorPolicy: policy.ErrorPolicy{
				Policy: policy.PolicySkipMalformed,
				OnRowError: func(rowNumber int, rawRow []byte, err error) {
					errorReported = true
				},
			},
		})
		res, err := skipAdapter.Fetch(context.Background(), ingestion.FetchRequest{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(res.Records) != 2 {
			t.Fatalf("expected 2 records, got %d", len(res.Records))
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
			res, fetchErr := adapter.Fetch(ctx, ingestion.FetchRequest{Checkpoint: checkpoint, BatchSize: 250})
			if fetchErr != nil {
				t.Fatalf("unexpected error at checkpoint %s: %v", checkpoint, fetchErr)
			}
			totalSeen += len(res.Records)
			checkpoint = res.NextCheckpoint
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

		res, err := adapter.Fetch(context.Background(), ingestion.FetchRequest{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(res.Records) != 1 || res.Records[0].ExternalProductID != "987654321012345678" {
			t.Fatalf("expected 987654321012345678, got %v", res.Records)
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
			ErrorPolicy: policy.ErrorPolicy{Policy: policy.PolicySkipMalformed},
		})
		if err != nil {
			t.Fatalf("unexpected init error: %v", err)
		}

		_, err = adapter.Fetch(context.Background(), ingestion.FetchRequest{})
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

		res, err := adapter.Fetch(context.Background(), ingestion.FetchRequest{})
		if err != nil {
			t.Fatalf("unexpected fetch error: %v", err)
		}
		if len(res.Records) != 1 || res.Records[0].ExternalProductID != "UK#SKU-555" {
			t.Fatalf("expected composite id UK#SKU-555, got %v", res.Records)
		}
	})

	t.Run("NDJSONErrorThresholdCalculatesAccurateRatio", func(t *testing.T) {
		var b strings.Builder
		for i := 1; i <= 990; i++ {
			b.WriteString(fmt.Sprintf(`{"id":"ITEM-%d"}`+"\n", i))
		}
		for i := 1; i <= 10; i++ {
			b.WriteString("{bad-json-row}\n")
		}

		adapter, err := sources.NewFeedFileAdapter(sources.FeedFileConfig{
			SourceID:       source.ID("src-ratio"),
			Format:         sources.FeedFormatNDJSON,
			ReaderProvider: newStringProvider(b.String()),
			ErrorPolicy:    policy.ErrorPolicy{Policy: policy.PolicySkipMalformed},
			BatchSize:      2000,
		})
		if err != nil {
			t.Fatalf("unexpected init error: %v", err)
		}

		res, err := adapter.Fetch(context.Background(), ingestion.FetchRequest{})
		if err != nil {
			t.Fatalf("expected 10 errors out of 1000 rows (1.0%%) to pass 5%% threshold, got error: %v", err)
		}
		if len(res.Records) != 990 {
			t.Fatalf("expected 990 valid records, got %d", len(res.Records))
		}
	})

	t.Run("NDJSONPreservesCaseSensitiveIDField", func(t *testing.T) {
		ndjson := `{"SKU":"MY-UPPERCASE-SKU","name":"Product"}` + "\n"
		adapter, err := sources.NewFeedFileAdapter(sources.FeedFileConfig{
			SourceID:       source.ID("src-case-sens"),
			Format:         sources.FeedFormatNDJSON,
			ReaderProvider: newStringProvider(ndjson),
			IDField:        "SKU",
		})
		if err != nil {
			t.Fatalf("unexpected init error: %v", err)
		}

		res, err := adapter.Fetch(context.Background(), ingestion.FetchRequest{})
		if err != nil {
			t.Fatalf("unexpected fetch error: %v", err)
		}
		if len(res.Records) != 1 || res.Records[0].ExternalProductID != "MY-UPPERCASE-SKU" {
			t.Fatalf("expected SKU MY-UPPERCASE-SKU, got %v", res.Records)
		}
	})

	t.Run("NonSeekableCSVStreaming", func(t *testing.T) {
		csvData := "id,name,price\nP1,Item1,10.0\nP2,Item2,20.0\nP3,Item3,30.0\nP4,Item4,40.0\nP5,Item5,50.0\n"
		adapter, err := sources.NewFeedFileAdapter(sources.FeedFileConfig{
			SourceID: source.ID("src-csv-nonseekable"),
			Format:   sources.FeedFormatCSV,
			ReaderProvider: func() (io.ReadCloser, error) {
				return &nonSeekableReaderCloser{r: strings.NewReader(csvData)}, nil
			},
			BatchSize: 2,
		})
		if err != nil {
			t.Fatalf("unexpected init error: %v", err)
		}

		ctx := context.Background()

		// Batch 1: should get 2 records
		res1, err := adapter.Fetch(ctx, ingestion.FetchRequest{BatchSize: 2})
		if err != nil {
			t.Fatalf("unexpected error batch 1: %v", err)
		}
		if len(res1.Records) != 2 {
			t.Fatalf("expected 2 records in batch 1, got %d", len(res1.Records))
		}
		if res1.Records[0].ExternalProductID != "P1" || res1.Records[1].ExternalProductID != "P2" {
			t.Errorf("batch 1 records mismatch: %v, %v", res1.Records[0].ExternalProductID, res1.Records[1].ExternalProductID)
		}

		// Batch 2: resume from checkpoint, should get P3, P4
		res2, err := adapter.Fetch(ctx, ingestion.FetchRequest{Checkpoint: res1.NextCheckpoint, BatchSize: 2})
		if err != nil {
			t.Fatalf("unexpected error batch 2: %v", err)
		}
		if len(res2.Records) != 2 {
			t.Fatalf("expected 2 records in batch 2, got %d", len(res2.Records))
		}
		if res2.Records[0].ExternalProductID != "P3" || res2.Records[1].ExternalProductID != "P4" {
			t.Errorf("batch 2 records mismatch: %v, %v", res2.Records[0].ExternalProductID, res2.Records[1].ExternalProductID)
		}

		// Batch 3: resume from checkpoint, should get P5
		res3, err := adapter.Fetch(ctx, ingestion.FetchRequest{Checkpoint: res2.NextCheckpoint, BatchSize: 2})
		if err != nil {
			t.Fatalf("unexpected error batch 3: %v", err)
		}
		if len(res3.Records) != 1 || res3.Records[0].ExternalProductID != "P5" {
			t.Fatalf("expected 1 record P5 in batch 3, got %v", res3.Records)
		}
		if res3.NextCheckpoint != "" {
			t.Errorf("expected empty checkpoint at EOF, got %q", res3.NextCheckpoint)
		}
	})
}

type nonSeekableReaderCloser struct {
	r io.Reader
}

func (n *nonSeekableReaderCloser) Read(p []byte) (int, error) {
	return n.r.Read(p)
}

func (n *nonSeekableReaderCloser) Close() error {
	return nil
}

func TestFeedFileAdapter_NDJSON_CRLFAndOversized(t *testing.T) {
	t.Run("CRLFLineEndingsResumeExactCheckpoint", func(t *testing.T) {
		crlfNDJSON := "{\"id\":\"A1\"}\r\n{\"id\":\"A2\"}\r\n{\"id\":\"A3\"}\r\n{\"id\":\"A4\"}\r\n{\"id\":\"A5\"}\r\n"
		adapter, err := sources.NewFeedFileAdapter(sources.FeedFileConfig{
			SourceID:       source.ID("src-crlf"),
			Format:         sources.FeedFormatNDJSON,
			ReaderProvider: newStringProvider(crlfNDJSON),
			BatchSize:      2,
		})
		if err != nil {
			t.Fatalf("unexpected init error: %v", err)
		}

		ctx := context.Background()

		// Batch 1: A1, A2
		res1, err := adapter.Fetch(ctx, ingestion.FetchRequest{BatchSize: 2})
		if err != nil {
			t.Fatalf("unexpected error batch 1: %v", err)
		}
		if len(res1.Records) != 2 {
			t.Fatalf("expected 2 records in batch 1, got %d", len(res1.Records))
		}
		if res1.Records[0].ExternalProductID != "A1" || res1.Records[1].ExternalProductID != "A2" {
			t.Errorf("batch 1 mismatch: %v, %v", res1.Records[0].ExternalProductID, res1.Records[1].ExternalProductID)
		}

		// Batch 2: A3, A4 (must not fail with JSON parse error from misaligned byte offset)
		res2, err := adapter.Fetch(ctx, ingestion.FetchRequest{Checkpoint: res1.NextCheckpoint, BatchSize: 2})
		if err != nil {
			t.Fatalf("unexpected error batch 2 (CRLF checkpoint drift): %v", err)
		}
		if len(res2.Records) != 2 {
			t.Fatalf("expected 2 records in batch 2, got %d", len(res2.Records))
		}
		if res2.Records[0].ExternalProductID != "A3" || res2.Records[1].ExternalProductID != "A4" {
			t.Errorf("batch 2 mismatch: %v, %v", res2.Records[0].ExternalProductID, res2.Records[1].ExternalProductID)
		}

		// Batch 3: A5
		res3, err := adapter.Fetch(ctx, ingestion.FetchRequest{Checkpoint: res2.NextCheckpoint, BatchSize: 2})
		if err != nil {
			t.Fatalf("unexpected error batch 3: %v", err)
		}
		if len(res3.Records) != 1 || res3.Records[0].ExternalProductID != "A5" {
			t.Fatalf("expected A5 in batch 3, got %v", res3.Records)
		}
	})

	t.Run("OversizedLineSkippedWithSkipMalformed", func(t *testing.T) {
		oversized := strings.Repeat("a", 2*1024*1024)
		ndjson := fmt.Sprintf("{\"id\":\"PRE\"}\n{\"id\":\"OVERSIZED\",\"data\":\"%s\"}\n{\"id\":\"POST\"}\n", oversized)

		var reportedErrors int
		adapter, err := sources.NewFeedFileAdapter(sources.FeedFileConfig{
			SourceID:       source.ID("src-oversized-skip"),
			Format:         sources.FeedFormatNDJSON,
			ReaderProvider: newStringProvider(ndjson),
			ErrorPolicy: policy.ErrorPolicy{
				Policy: policy.PolicySkipMalformed,
				OnRowError: func(rowNumber int, rawRow []byte, err error) {
					reportedErrors++
				},
			},
		})
		if err != nil {
			t.Fatalf("unexpected init error: %v", err)
		}

		res, err := adapter.Fetch(context.Background(), ingestion.FetchRequest{BatchSize: 10})
		if err != nil {
			t.Fatalf("expected oversized line to be skipped under the skip-malformed policy, got err: %v", err)
		}

		if len(res.Records) != 2 {
			t.Fatalf("expected 2 valid records (PRE and POST), got %d", len(res.Records))
		}
		if res.Records[0].ExternalProductID != "PRE" || res.Records[1].ExternalProductID != "POST" {
			t.Errorf("expected PRE and POST, got %v, %v", res.Records[0].ExternalProductID, res.Records[1].ExternalProductID)
		}
		if reportedErrors != 1 {
			t.Errorf("expected 1 reported error callback for oversized line, got %d", reportedErrors)
		}
	})
}
