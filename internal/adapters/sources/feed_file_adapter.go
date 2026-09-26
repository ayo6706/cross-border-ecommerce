package sources

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/adapters/sources/identity"
	"github.com/ayo6706/cross-border-ecommerce/internal/adapters/sources/policy"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/source"
)

const (
	maxNDJSONLineBytes = 1024 * 1024 // 1 MiB max line length
)

type FeedFormat string

const (
	FeedFormatCSV    FeedFormat = "CSV"
	FeedFormatNDJSON FeedFormat = "NDJSON"
)

type FeedFileConfig struct {
	SourceID       source.ID
	Format         FeedFormat
	FilePath       string
	ReaderProvider func() (io.ReadCloser, error)
	BatchSize      int
	IDField        string
	CompositeIDs   []string
	CompositeSep   string
	Identity       identity.IdentityStrategy
	ErrorPolicy    policy.ErrorPolicy
	SourceVersion  string
}

type FeedFileAdapter struct {
	sourceID       source.ID
	format         FeedFormat
	readerProvider func() (io.ReadCloser, error)
	batchSize      int
	identity       identity.IdentityStrategy
	errorPolicy    policy.ErrorPolicy
	sourceVersion  string
}

// feedBatch is the result of streaming one batch from a feed.
type feedBatch struct {
	records []*ingestion.RawRecord
	failed  int
	next    string
}

//nolint:funlen // legacy baseline 2026-09-26: fix in ENG-049
func NewFeedFileAdapter(cfg FeedFileConfig) (*FeedFileAdapter, error) {
	if strings.TrimSpace(string(cfg.SourceID)) == "" {
		return nil, ingestion.ErrInvalidSourceID
	}

	switch cfg.Format {
	case FeedFormatCSV, FeedFormatNDJSON:
	default:
		return nil, fmt.Errorf("unsupported feed format: %s", cfg.Format)
	}

	if strings.TrimSpace(cfg.FilePath) == "" && cfg.ReaderProvider == nil {
		return nil, errors.New("either file path or reader provider is required")
	}

	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 500
	}

	idField := strings.TrimSpace(cfg.IDField)
	if cfg.Format == FeedFormatCSV {
		idField = strings.ToLower(idField)
	}
	if idField == "" {
		idField = "id"
	}

	provider := cfg.ReaderProvider
	if provider == nil {
		filePath := strings.TrimSpace(cfg.FilePath)
		provider = func() (io.ReadCloser, error) {
			file, err := os.Open(filePath)
			if err != nil {
				return nil, fmt.Errorf("open feed file %s: %w", filePath, err)
			}
			return file, nil
		}
	}

	idStrat := cfg.Identity
	if idStrat == nil && len(cfg.CompositeIDs) > 0 {
		sep := cfg.CompositeSep
		if sep == "" {
			sep = ":"
		}
		comp, compErr := identity.NewCompositeIdentityStrategy(cfg.CompositeIDs, sep)
		if compErr != nil {
			return nil, compErr
		}
		idStrat = comp
	} else if idStrat == nil {
		idStrat = identity.NewPathIdentityStrategy(idField)
	}

	return &FeedFileAdapter{
		sourceID:       cfg.SourceID,
		format:         cfg.Format,
		readerProvider: provider,
		batchSize:      batchSize,
		identity:       idStrat,
		errorPolicy:    cfg.ErrorPolicy,
		sourceVersion:  strings.TrimSpace(cfg.SourceVersion),
	}, nil
}

// Fetch executes a single batch read against the feed file from the given checkpoint.
func (a *FeedFileAdapter) Fetch(ctx context.Context, req ingestion.FetchRequest) (ingestion.FetchResult, error) {
	if a == nil {
		return ingestion.FetchResult{}, ingestion.ErrAdapterUnavailable
	}

	select {
	case <-ctx.Done():
		return ingestion.FetchResult{}, ctx.Err()
	default:
	}

	limit := a.batchSize
	if req.BatchSize > 0 {
		limit = req.BatchSize
	}

	startOffset, err := ingestion.ParseByteOffsetCheckpoint(req.Checkpoint)
	if err != nil {
		return ingestion.FetchResult{}, fmt.Errorf("parse checkpoint: %w", err)
	}

	rc, err := a.readerProvider()
	if err != nil {
		return ingestion.FetchResult{}, fmt.Errorf("%w: %w", ingestion.ErrAdapterUnavailable, err)
	}
	defer rc.Close() //nolint:errcheck // read-only source: a close error cannot invalidate the decoded batch

	stream := a.streamNDJSON
	if a.format == FeedFormatCSV {
		stream = a.streamCSV
	}

	batch, err := stream(ctx, rc, startOffset, limit)
	if err != nil {
		return ingestion.FetchResult{}, err
	}

	return ingestion.FetchResult{
		Records:        batch.records,
		Failed:         batch.failed,
		NextCheckpoint: batch.next,
		HasMore:        batch.next != "",
	}, nil
}

// Probe executes a pre-flight dry-run diagnostic query to verify feed accessibility and sample row schema.
func (a *FeedFileAdapter) Probe(ctx context.Context) (*ingestion.SourceProbeResult, error) {
	return probeWithFetch(ctx, a)
}

var _ ingestion.ProbingAdapter = (*FeedFileAdapter)(nil)

var errLineTooLong = errors.New("line exceeds maximum length")

//nolint:funlen,gocognit // legacy baseline 2026-09-26: fix in ENG-049
func (a *FeedFileAdapter) streamNDJSON(ctx context.Context, rc io.ReadCloser, startOffset int64, limit int) (feedBatch, error) {
	if seeker, ok := rc.(io.Seeker); ok {
		if _, err := seeker.Seek(startOffset, io.SeekStart); err != nil {
			return feedBatch{}, fmt.Errorf("seek to offset %d: %w", startOffset, err)
		}
	} else if startOffset > 0 {
		if _, err := io.CopyN(io.Discard, rc, startOffset); err != nil && !errors.Is(err, io.EOF) {
			return feedBatch{}, fmt.Errorf("skip to offset %d: %w", startOffset, err)
		}
	}

	reader := bufio.NewReader(rc)
	currentOffset := startOffset
	records := make([]*ingestion.RawRecord, 0, limit)
	rowNum := 0
	now := time.Now().UTC()
	failed := 0

	for len(records) < limit {
		select {
		case <-ctx.Done():
			return feedBatch{}, ctx.Err()
		default:
		}

		lineBytes, bytesConsumed, err := readBoundedLine(reader, maxNDJSONLineBytes)
		if bytesConsumed == 0 && errors.Is(err, io.EOF) {
			return feedBatch{records: records, failed: failed}, nil
		}

		rowNum++
		currentOffset += int64(bytesConsumed)

		if errors.Is(err, errLineTooLong) {
			if policyErr := a.errorPolicy.HandleRowError(rowNum, nil, fmt.Errorf("%w: line %d exceeds maximum length of %d bytes", ingestion.ErrMalformedRecord, rowNum, maxNDJSONLineBytes)); policyErr != nil {
				return feedBatch{}, policyErr
			}
			failed++
			continue
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return feedBatch{}, fmt.Errorf("read ndjson line %d: %w", rowNum, err)
		}

		trimmed := bytes.TrimSpace(lineBytes)
		if len(trimmed) == 0 {
			if errors.Is(err, io.EOF) {
				return feedBatch{records: records, failed: failed}, nil
			}
			continue
		}

		var rowMap map[string]any
		dec := json.NewDecoder(bytes.NewReader(trimmed))
		dec.UseNumber()
		if jsonErr := dec.Decode(&rowMap); jsonErr != nil {
			if policyErr := a.errorPolicy.HandleRowError(rowNum, trimmed, fmt.Errorf("%w: line %d: %w", ingestion.ErrMalformedRecord, rowNum, jsonErr)); policyErr != nil {
				return feedBatch{}, policyErr
			}
			failed++
			if errors.Is(err, io.EOF) {
				return feedBatch{records: records, failed: failed}, nil
			}
			continue
		}

		extID, idErr := a.identity.Resolve(trimmed)
		if idErr != nil {
			if policyErr := a.errorPolicy.HandleRowError(rowNum, trimmed, idErr); policyErr != nil {
				return feedBatch{}, policyErr
			}
			failed++
			if errors.Is(err, io.EOF) {
				return feedBatch{records: records, failed: failed}, nil
			}
			continue
		}

		srcVer, srcUpAt := extractRecordMapMetadata(rowMap)
		if srcVer == "" {
			srcVer = a.sourceVersion
		}

		rec, errRec := ingestion.NewRawRecord(ingestion.RawRecordParams{
			SourceID:          a.sourceID,
			ExternalProductID: extID,
			Payload:           trimmed,
			PayloadRaw:        trimmed,
			SourceVersion:     srcVer,
			SourceUpdatedAt:   srcUpAt,
			ReceivedAt:        now,
		})
		if errRec != nil {
			if policyErr := a.errorPolicy.HandleRowError(rowNum, trimmed, errRec); policyErr != nil {
				return feedBatch{}, policyErr
			}
			failed++
			if errors.Is(err, io.EOF) {
				return feedBatch{records: records, failed: failed}, nil
			}
			continue
		}

		records = append(records, rec)

		if errors.Is(err, io.EOF) {
			return feedBatch{records: records, failed: failed}, nil
		}
	}

	nextCheckpoint := ingestion.FormatByteOffsetCheckpoint(currentOffset)
	return feedBatch{records: records, failed: failed, next: nextCheckpoint}, nil
}

func readBoundedLine(r *bufio.Reader, maxBytes int) (line []byte, n int, err error) {
	var buf bytes.Buffer
	totalConsumed := 0
	oversized := false

	for {
		chunk, readErr := r.ReadSlice('\n')
		chunkLen := len(chunk)
		totalConsumed += chunkLen

		if !oversized {
			if buf.Len()+chunkLen > maxBytes {
				oversized = true
			} else {
				buf.Write(chunk)
			}
		}

		if readErr != nil {
			if errors.Is(readErr, bufio.ErrBufferFull) {
				continue
			}
			if oversized {
				return nil, totalConsumed, errLineTooLong
			}
			return buf.Bytes(), totalConsumed, readErr
		}

		if oversized {
			return nil, totalConsumed, errLineTooLong
		}
		return buf.Bytes(), totalConsumed, nil
	}
}

//nolint:funlen,gocognit // legacy baseline 2026-09-26: fix in ENG-049
func (a *FeedFileAdapter) streamCSV(ctx context.Context, rc io.ReadCloser, startOffset int64, limit int) (feedBatch, error) {
	var header []string
	var headerBytesLen int64
	var csvReader *csv.Reader
	var actualOffset int64

	if seeker, ok := rc.(io.Seeker); ok {
		if _, err := seeker.Seek(0, io.SeekStart); err != nil {
			return feedBatch{}, fmt.Errorf("seek to start for csv header: %w", err)
		}
		reader := bufio.NewReader(rc)
		headerBytes, err := reader.ReadBytes('\n')
		if len(headerBytes) == 0 && err != nil {
			return feedBatch{}, fmt.Errorf("empty csv feed: %w", err)
		}
		csvR := csv.NewReader(bytes.NewReader(bytes.TrimSpace(headerBytes)))
		fields, err := csvR.Read()
		if err != nil {
			return feedBatch{}, fmt.Errorf("parse csv header: %w", err)
		}
		header = make([]string, 0, len(fields))
		for _, f := range fields {
			header = append(header, strings.ToLower(strings.TrimSpace(f)))
		}
		headerBytesLen = int64(len(headerBytes))

		actualStartOffset := startOffset
		if actualStartOffset == 0 {
			actualStartOffset = headerBytesLen
		}
		if _, err := seeker.Seek(actualStartOffset, io.SeekStart); err != nil {
			return feedBatch{}, fmt.Errorf("seek csv to offset %d: %w", actualStartOffset, err)
		}
		actualOffset = actualStartOffset
		csvReader = csv.NewReader(rc)
	} else {
		br := bufio.NewReader(rc)
		headerBytes, err := br.ReadBytes('\n')
		if len(headerBytes) == 0 && err != nil {
			return feedBatch{}, fmt.Errorf("empty csv feed: %w", err)
		}
		csvR := csv.NewReader(bytes.NewReader(bytes.TrimSpace(headerBytes)))
		fields, err := csvR.Read()
		if err != nil {
			return feedBatch{}, fmt.Errorf("parse csv header: %w", err)
		}
		header = make([]string, 0, len(fields))
		for _, f := range fields {
			header = append(header, strings.ToLower(strings.TrimSpace(f)))
		}
		headerBytesLen = int64(len(headerBytes))

		if startOffset > headerBytesLen {
			skipBytes := startOffset - headerBytesLen
			if _, err := io.CopyN(io.Discard, br, skipBytes); err != nil && !errors.Is(err, io.EOF) {
				return feedBatch{}, fmt.Errorf("skip csv to offset %d: %w", startOffset, err)
			}
			actualOffset = startOffset
		} else {
			actualOffset = headerBytesLen
		}
		csvReader = csv.NewReader(br)
	}

	csvReader.FieldsPerRecord = -1
	csvReader.ReuseRecord = false

	records := make([]*ingestion.RawRecord, 0, limit)
	currentOffset := actualOffset
	rowNum := 0
	now := time.Now().UTC()
	failed := 0

	for len(records) < limit {
		select {
		case <-ctx.Done():
			return feedBatch{}, ctx.Err()
		default:
		}

		rowFields, csvErr := csvReader.Read()
		if errors.Is(csvErr, io.EOF) {
			return feedBatch{records: records, failed: failed}, nil
		}

		rowNum++
		currentOffset = actualOffset + csvReader.InputOffset()

		if csvErr != nil {
			var parseErr *csv.ParseError
			if errors.As(csvErr, &parseErr) {
				if policyErr := a.errorPolicy.HandleRowError(rowNum, nil, fmt.Errorf("%w: csv row %d: %w", ingestion.ErrMalformedRecord, rowNum, csvErr)); policyErr != nil {
					return feedBatch{}, policyErr
				}
				failed++
				continue
			}
			return feedBatch{}, fmt.Errorf("read csv row %d: %w", rowNum, csvErr)
		}
		if len(rowFields) == 0 {
			continue
		}

		rowMap := make(map[string]any, len(header))
		for i, colName := range header {
			if i < len(rowFields) {
				rowMap[colName] = rowFields[i]
			} else {
				rowMap[colName] = ""
			}
		}

		payloadJSON, errJSON := json.Marshal(rowMap)
		if errJSON != nil {
			if policyErr := a.errorPolicy.HandleRowError(rowNum, nil, errJSON); policyErr != nil {
				return feedBatch{}, policyErr
			}
			failed++
			continue
		}

		extID, idErr := a.identity.Resolve(payloadJSON)
		if idErr != nil {
			if policyErr := a.errorPolicy.HandleRowError(rowNum, payloadJSON, idErr); policyErr != nil {
				return feedBatch{}, policyErr
			}
			failed++
			continue
		}

		srcVer, srcUpAt := extractRecordMapMetadata(rowMap)
		if srcVer == "" {
			srcVer = a.sourceVersion
		}

		rec, errRec := ingestion.NewRawRecord(ingestion.RawRecordParams{
			SourceID:          a.sourceID,
			ExternalProductID: extID,
			Payload:           payloadJSON,
			PayloadRaw:        payloadJSON,
			SourceVersion:     srcVer,
			SourceUpdatedAt:   srcUpAt,
			ReceivedAt:        now,
		})
		if errRec != nil {
			if policyErr := a.errorPolicy.HandleRowError(rowNum, nil, errRec); policyErr != nil {
				return feedBatch{}, policyErr
			}
			failed++
			continue
		}

		records = append(records, rec)
	}

	if len(records) > 0 {
		_, nextErr := csvReader.Read()
		if errors.Is(nextErr, io.EOF) {
			return feedBatch{records: records, failed: failed}, nil
		}
	}

	nextCheckpoint := ingestion.FormatByteOffsetCheckpoint(currentOffset)
	return feedBatch{records: records, failed: failed, next: nextCheckpoint}, nil
}

func extractRecordMapMetadata(m map[string]any) (string, *time.Time) {
	srcVersion := ""
	for _, field := range []string{"version", "v", "etag", "revision"} {
		if str, ok := m[field].(string); ok && strings.TrimSpace(str) != "" {
			srcVersion = strings.TrimSpace(str)
			break
		}
	}

	var srcUpdatedAt *time.Time
	for _, field := range []string{"updated_at", "source_updated_at", "modified_at"} {
		if str, ok := m[field].(string); ok {
			if parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(str)); err == nil {
				utc := parsed.UTC()
				srcUpdatedAt = &utc
				break
			}
		}
	}

	return srcVersion, srcUpdatedAt
}
