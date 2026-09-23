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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/adapters/sources/identity"
	"github.com/ayo6706/cross-border-ecommerce/internal/adapters/sources/policy"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/source"
)

type FeedFormat string

const (
	FeedFormatCSV    FeedFormat = "CSV"
	FeedFormatNDJSON FeedFormat = "NDJSON"
)

type RowErrorHandler func(rowNumber int, rawRow []byte, err error)

type FeedFileConfig struct {
	SourceID        source.ID
	Format          FeedFormat
	FilePath        string
	ReaderProvider  func() (io.ReadCloser, error)
	BatchSize       int
	IDField         string
	CompositeIDs    []string
	CompositeSep    string
	Identity        identity.IdentityStrategy
	ErrorTracker    *policy.ErrorTracker
	SkipMalformed   bool
	OnRowError      RowErrorHandler
	SourceVersion   string
}

type FeedFileAdapter struct {
	sourceID        source.ID
	format          FeedFormat
	filePath        string
	readerProvider  func() (io.ReadCloser, error)
	batchSize       int
	idField         string
	compositeIDs    []string
	compositeSep    string
	identity        identity.IdentityStrategy
	errorTracker    *policy.ErrorTracker
	skipMalformed   bool
	onRowError      RowErrorHandler
	sourceVersion   string
	headerMu        sync.RWMutex
	cachedHeader    []string
	cachedHeaderLen int64
}

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

	idField := strings.ToLower(strings.TrimSpace(cfg.IDField))
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

	tracker := cfg.ErrorTracker
	if tracker == nil {
		polType := policy.PolicyFailFast
		if cfg.SkipMalformed {
			polType = policy.PolicySkipMalformed
		}
		tracker = policy.NewErrorTracker(polType, 0.05, policy.RowErrorCallback(cfg.OnRowError))
	}

	return &FeedFileAdapter{
		sourceID:        cfg.SourceID,
		format:          cfg.Format,
		filePath:        strings.TrimSpace(cfg.FilePath),
		readerProvider:  provider,
		batchSize:       batchSize,
		idField:         idField,
		compositeIDs:    cfg.CompositeIDs,
		compositeSep:    cfg.CompositeSep,
		identity:        idStrat,
		errorTracker:    tracker,
		skipMalformed:   cfg.SkipMalformed,
		onRowError:      cfg.OnRowError,
		sourceVersion:   strings.TrimSpace(cfg.SourceVersion),
	}, nil
}

func (a *FeedFileAdapter) FetchRecords(ctx context.Context, checkpoint string) ([]*ingestion.RawRecord, string, error) {
	return a.fetchRecordsBounded(ctx, checkpoint, a.batchSize)
}

func (a *FeedFileAdapter) fetchRecordsBounded(ctx context.Context, checkpoint string, limit int) ([]*ingestion.RawRecord, string, error) {
	if a == nil {
		return nil, "", ingestion.ErrAdapterUnavailable
	}

	select {
	case <-ctx.Done():
		return nil, "", ctx.Err()
	default:
	}

	startOffset, err := ingestion.ParseByteOffsetCheckpoint(checkpoint)
	if err != nil {
		return nil, "", fmt.Errorf("parse checkpoint: %w", err)
	}

	rc, err := a.readerProvider()
	if err != nil {
		return nil, "", fmt.Errorf("%w: %w", ingestion.ErrAdapterUnavailable, err)
	}
	defer rc.Close()

	if a.format == FeedFormatCSV {
		return a.streamCSV(ctx, rc, startOffset, limit)
	}
	return a.streamNDJSON(ctx, rc, startOffset, limit)
}

func (a *FeedFileAdapter) streamNDJSON(ctx context.Context, rc io.ReadCloser, startOffset int64, limit int) ([]*ingestion.RawRecord, string, error) {
	if seeker, ok := rc.(io.Seeker); ok {
		if _, err := seeker.Seek(startOffset, io.SeekStart); err != nil {
			return nil, "", fmt.Errorf("seek to offset %d: %w", startOffset, err)
		}
	} else if startOffset > 0 {
		if _, err := io.CopyN(io.Discard, rc, startOffset); err != nil && !errors.Is(err, io.EOF) {
			return nil, "", fmt.Errorf("skip to offset %d: %w", startOffset, err)
		}
	}

	reader := bufio.NewReader(rc)
	currentOffset := startOffset
	records := make([]*ingestion.RawRecord, 0, limit)
	rowNum := 0
	now := time.Now().UTC()

	for len(records) < limit {
		select {
		case <-ctx.Done():
			return nil, "", ctx.Err()
		default:
		}

		lineBytes, err := reader.ReadBytes('\n')
		lineLen := int64(len(lineBytes))
		if lineLen == 0 && errors.Is(err, io.EOF) {
			return records, "", nil
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, "", fmt.Errorf("read ndjson line %d: %w", rowNum+1, err)
		}

		rowNum++
		currentOffset += lineLen

		trimmed := bytes.TrimSpace(lineBytes)
		if len(trimmed) == 0 {
			if errors.Is(err, io.EOF) {
				return records, "", nil
			}
			continue
		}

		var rowMap map[string]any
		dec := json.NewDecoder(bytes.NewReader(trimmed))
		dec.UseNumber()
		if jsonErr := dec.Decode(&rowMap); jsonErr != nil {
			if a.skipMalformed {
				if a.onRowError != nil {
					a.onRowError(rowNum, trimmed, jsonErr)
				}
				if errors.Is(err, io.EOF) {
					return records, "", nil
				}
				continue
			}
			return nil, "", fmt.Errorf("%w: line %d: %w", ingestion.ErrMalformedRecord, rowNum, jsonErr)
		}

		extID, idErr := a.identity.Resolve(trimmed)
		if idErr != nil {
			recErr := a.errorTracker.RecordError(rowNum, trimmed, idErr)
			if recErr != nil {
				return nil, "", recErr
			}
			if errors.Is(err, io.EOF) {
				return records, "", nil
			}
			continue
		}

		_, srcVer, srcUpAt := extractRecordMapMetadata(rowMap, a.idField)

		if srcVer == "" {
			srcVer = a.sourceVersion
		}

		rec, errRec := ingestion.NewRawRecord(
			"",
			a.sourceID,
			extID,
			trimmed,
			srcVer,
			"",
			srcUpAt,
			"",
			now,
		)
		if errRec != nil {
			if a.skipMalformed {
				if a.onRowError != nil {
					a.onRowError(rowNum, trimmed, errRec)
				}
				if errors.Is(err, io.EOF) {
					return records, "", nil
				}
				continue
			}
			return nil, "", fmt.Errorf("create raw record line %d: %w", rowNum, errRec)
		}

		records = append(records, rec)

		if errors.Is(err, io.EOF) {
			return records, "", nil
		}
	}

	nextCheckpoint := ingestion.FormatByteOffsetCheckpoint(currentOffset)
	return records, nextCheckpoint, nil
}

func (a *FeedFileAdapter) streamCSV(ctx context.Context, rc io.ReadCloser, startOffset int64, limit int) ([]*ingestion.RawRecord, string, error) {
	header, headerBytesLen, err := a.readCSVHeader()
	if err != nil {
		return nil, "", fmt.Errorf("read csv header: %w", err)
	}

	actualStartOffset := startOffset
	if actualStartOffset == 0 {
		actualStartOffset = headerBytesLen
	}

	if seeker, ok := rc.(io.Seeker); ok {
		if _, err := seeker.Seek(actualStartOffset, io.SeekStart); err != nil {
			return nil, "", fmt.Errorf("seek csv to offset %d: %w", actualStartOffset, err)
		}
	} else if actualStartOffset > 0 {
		if _, err := io.CopyN(io.Discard, rc, actualStartOffset); err != nil && !errors.Is(err, io.EOF) {
			return nil, "", fmt.Errorf("skip csv to offset %d: %w", actualStartOffset, err)
		}
	}

	cr := &countingReader{r: rc}
	csvReader := csv.NewReader(cr)
	csvReader.FieldsPerRecord = -1
	csvReader.ReuseRecord = false

	records := make([]*ingestion.RawRecord, 0, limit)
	currentOffset := actualStartOffset
	rowNum := 0
	now := time.Now().UTC()

	for len(records) < limit {
		select {
		case <-ctx.Done():
			return nil, "", ctx.Err()
		default:
		}

		rowFields, csvErr := csvReader.Read()
		if errors.Is(csvErr, io.EOF) {
			return records, "", nil
		}

		rowNum++
		currentOffset = actualStartOffset + csvReader.InputOffset()

		if csvErr != nil {
			var parseErr *csv.ParseError
			if errors.As(csvErr, &parseErr) {
				if a.skipMalformed {
					if a.onRowError != nil {
						a.onRowError(rowNum, nil, csvErr)
					}
					continue
				}
				return nil, "", fmt.Errorf("%w: csv row %d: %w", ingestion.ErrMalformedRecord, rowNum, csvErr)
			}
			return nil, "", fmt.Errorf("read csv row %d: %w", rowNum, csvErr)
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
			recErr := a.errorTracker.RecordError(rowNum, nil, errJSON)
			if recErr != nil {
				return nil, "", recErr
			}
			continue
		}

		extID, idErr := a.identity.Resolve(payloadJSON)
		if idErr != nil {
			recErr := a.errorTracker.RecordError(rowNum, payloadJSON, idErr)
			if recErr != nil {
				return nil, "", recErr
			}
			continue
		}

		_, srcVer, srcUpAt := extractRecordMapMetadata(rowMap, a.idField)

		if srcVer == "" {
			srcVer = a.sourceVersion
		}

		rec, errRec := ingestion.NewRawRecord(
			"",
			a.sourceID,
			extID,
			payloadJSON,
			srcVer,
			"",
			srcUpAt,
			"",
			now,
		)
		if errRec != nil {
			if a.skipMalformed {
				if a.onRowError != nil {
					a.onRowError(rowNum, nil, errRec)
				}
				continue
			}
			return nil, "", fmt.Errorf("create raw record for csv row %d: %w", rowNum, errRec)
		}

		records = append(records, rec)
	}

	if len(records) > 0 {
		_, nextErr := csvReader.Read()
		if errors.Is(nextErr, io.EOF) {
			return records, "", nil
		}
	}

	nextCheckpoint := ingestion.FormatByteOffsetCheckpoint(currentOffset)
	return records, nextCheckpoint, nil
}

type countingReader struct {
	r     io.Reader
	bytes int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.bytes += int64(n)
	return n, err
}

func (a *FeedFileAdapter) readCSVHeader() ([]string, int64, error) {
	a.headerMu.RLock()
	if len(a.cachedHeader) > 0 {
		headerCopy := make([]string, len(a.cachedHeader))
		copy(headerCopy, a.cachedHeader)
		headerLen := a.cachedHeaderLen
		a.headerMu.RUnlock()
		return headerCopy, headerLen, nil
	}
	a.headerMu.RUnlock()

	a.headerMu.Lock()
	defer a.headerMu.Unlock()

	if len(a.cachedHeader) > 0 {
		headerCopy := make([]string, len(a.cachedHeader))
		copy(headerCopy, a.cachedHeader)
		return headerCopy, a.cachedHeaderLen, nil
	}

	rc, err := a.readerProvider()
	if err != nil {
		return nil, 0, err
	}
	defer rc.Close()

	reader := bufio.NewReader(rc)
	headerBytes, err := reader.ReadBytes('\n')
	if len(headerBytes) == 0 && err != nil {
		return nil, 0, fmt.Errorf("empty csv feed: %w", err)
	}

	csvReader := csv.NewReader(bytes.NewReader(bytes.TrimSpace(headerBytes)))
	headerFields, err := csvReader.Read()
	if err != nil {
		return nil, 0, fmt.Errorf("parse csv header: %w", err)
	}

	cleanedHeader := make([]string, 0, len(headerFields))
	for _, f := range headerFields {
		cleanedHeader = append(cleanedHeader, strings.ToLower(strings.TrimSpace(f)))
	}

	a.cachedHeader = cleanedHeader
	a.cachedHeaderLen = int64(len(headerBytes))

	headerCopy := make([]string, len(cleanedHeader))
	copy(headerCopy, cleanedHeader)
	return headerCopy, a.cachedHeaderLen, nil
}

// Fetch provides the expressive, typed FetchRequest/FetchResult contract.
func (a *FeedFileAdapter) Fetch(ctx context.Context, req ingestion.FetchRequest) (ingestion.FetchResult, error) {
	limit := a.batchSize
	if req.BatchSize > 0 {
		limit = req.BatchSize
	}
	records, nextCPStr, err := a.fetchRecordsBounded(ctx, req.Checkpoint.String(), limit)
	if err != nil {
		return ingestion.FetchResult{}, err
	}
	nextCP := ingestion.NewCheckpoint(nextCPStr)
	return ingestion.FetchResult{
		Records:        records,
		NextCheckpoint: nextCP,
		HasMore:        !nextCP.IsEmpty(),
	}, nil
}

// Probe executes a pre-flight dry-run diagnostic query to verify feed accessibility and sample row schema.
func (a *FeedFileAdapter) Probe(ctx context.Context) (*ingestion.SourceProbeResult, error) {
	return probeWithFetch(ctx, a)
}

var _ ingestion.ProbingAdapter = (*FeedFileAdapter)(nil)

func extractRecordMapMetadata(m map[string]any, preferredID string) (string, string, *time.Time) {
	extID := ""
	idCandidates := []string{preferredID, "id", "sku", "product_id", "item_id", "code"}
	for _, field := range idCandidates {
		switch v := m[field].(type) {
		case string:
			if s := strings.TrimSpace(v); s != "" {
				extID = s
			}
		case json.Number:
			if intVal, err := v.Int64(); err == nil {
				extID = strconv.FormatInt(intVal, 10)
			}
		}
		if extID != "" {
			break
		}
	}

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

	return extID, srcVersion, srcUpdatedAt
}
