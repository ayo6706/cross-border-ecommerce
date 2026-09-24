package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/source"
	"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/postgres/generated"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

var _ ingestion.RawRecordRepository = (*RawRecordRepository)(nil)

type copyFromDB interface {
	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
}

type RawRecordRepository struct {
	db      generated.DBTX
	queries *generated.Queries
}

func NewRawRecordRepository(db generated.DBTX) (*RawRecordRepository, error) {
	if db == nil {
		return nil, errors.New("database connection cannot be nil")
	}
	return &RawRecordRepository{
		db:      db,
		queries: generated.New(db),
	}, nil
}

func (r *RawRecordRepository) WithTx(tx pgx.Tx) *RawRecordRepository {
	return &RawRecordRepository{
		db:      tx,
		queries: r.queries.WithTx(tx),
	}
}

func (r *RawRecordRepository) Save(ctx context.Context, record *ingestion.RawRecord) error {
	params, err := toCreateParams(record)
	if err != nil {
		return err
	}

	row, err := r.queries.CreateRawRecord(ctx, params)
	if err != nil {
		return fmt.Errorf("create raw record: %w", err)
	}

	record.ID = uuidToString(row.ID)
	return nil
}

func (r *RawRecordRepository) SaveBatch(ctx context.Context, records []*ingestion.RawRecord) error {
	if len(records) == 0 {
		return nil
	}

	paramsList := make([]generated.CreateRawRecordParams, 0, len(records))
	for _, record := range records {
		params, err := toCreateParams(record)
		if err != nil {
			return err
		}
		paramsList = append(paramsList, params)
	}

	if cf, ok := r.db.(copyFromDB); ok {
		rows := make([][]any, 0, len(paramsList))
		for _, p := range paramsList {
			rows = append(rows, []any{
				p.ID,
				p.SourceID,
				p.ExternalProductID,
				p.Payload,
				p.PayloadRaw,
				p.PayloadSha256,
				p.SourceVersion,
				p.Etag,
				p.SourceUpdatedAt,
				p.IngestionRunID,
				p.ReceivedAt,
			})
		}

		_, err := cf.CopyFrom(
			ctx,
			pgx.Identifier{"raw_records"},
			[]string{
				"id",
				"source_id",
				"external_product_id",
				"payload",
				"payload_raw",
				"payload_sha256",
				"source_version",
				"etag",
				"source_updated_at",
				"ingestion_run_id",
				"received_at",
			},
			pgx.CopyFromRows(rows),
		)
		if err != nil {
			return fmt.Errorf("copy from raw records: %w", err)
		}
		return nil
	}

	for i, params := range paramsList {
		_, err := r.queries.CreateRawRecord(ctx, params)
		if err != nil {
			return fmt.Errorf("fallback insert raw record %s: %w", records[i].ID, err)
		}
	}

	return nil
}

func toCreateParams(record *ingestion.RawRecord) (generated.CreateRawRecordParams, error) {
	if record == nil {
		return generated.CreateRawRecordParams{}, ingestion.ErrInvalidRecordState
	}

	if err := record.Validate(); err != nil {
		return generated.CreateRawRecordParams{}, err
	}

	uuidVal, err := parseUUID(record.ID)
	if err != nil {
		return generated.CreateRawRecordParams{}, fmt.Errorf("%w: %v", ingestion.ErrInvalidRecordID, err)
	}

	var runUUID pgtype.UUID
	if trimmedRunID := strings.TrimSpace(record.IngestionRunID); trimmedRunID != "" {
		runUUID, err = parseUUID(trimmedRunID)
		if err != nil {
			return generated.CreateRawRecordParams{}, fmt.Errorf("%w: %v", ingestion.ErrInvalidRunID, err)
		}
	}

	rawBytes := record.PayloadRaw
	if len(rawBytes) == 0 {
		rawBytes = record.Payload
	}

	return generated.CreateRawRecordParams{
		ID:                uuidVal,
		SourceID:          string(record.SourceID),
		ExternalProductID: record.ExternalProductID,
		Payload:           record.Payload,
		PayloadRaw:        rawBytes,
		PayloadSha256:     record.PayloadSHA256,
		SourceVersion:     record.SourceVersion,
		Etag:              record.ETag,
		SourceUpdatedAt:   toTimestamptz(record.SourceUpdatedAt),
		IngestionRunID:    runUUID,
		ReceivedAt:        requiredTimestamptz(record.ReceivedAt),
	}, nil
}

func (r *RawRecordRepository) FindByID(ctx context.Context, id string) (*ingestion.RawRecord, error) {
	uuidVal, err := parseUUID(id)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ingestion.ErrInvalidRecordID, err)
	}

	row, err := r.queries.GetRawRecordByID(ctx, uuidVal)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ingestion.ErrRecordNotFound
		}
		return nil, fmt.Errorf("find raw record by id: %w", err)
	}

	return toDomainRawRecord(row), nil
}

func (r *RawRecordRepository) FindLatestBySourceAndExternalID(
	ctx context.Context,
	sourceID source.ID,
	externalProductID string,
) (*ingestion.RawRecord, error) {
	if strings.TrimSpace(string(sourceID)) == "" {
		return nil, ingestion.ErrInvalidSourceID
	}
	if strings.TrimSpace(externalProductID) == "" {
		return nil, ingestion.ErrInvalidExternalProductID
	}

	row, err := r.queries.GetLatestRawRecordBySourceAndExternalID(ctx, generated.GetLatestRawRecordBySourceAndExternalIDParams{
		SourceID:          string(sourceID),
		ExternalProductID: strings.TrimSpace(externalProductID),
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ingestion.ErrRecordNotFound
		}
		return nil, fmt.Errorf("find latest raw record: %w", err)
	}

	return toDomainRawRecord(row), nil
}

func (r *RawRecordRepository) ListBySourceAndExternalID(
	ctx context.Context,
	sourceID source.ID,
	externalProductID string,
	limit int,
) ([]*ingestion.RawRecord, error) {
	if strings.TrimSpace(string(sourceID)) == "" {
		return nil, ingestion.ErrInvalidSourceID
	}
	if strings.TrimSpace(externalProductID) == "" {
		return nil, ingestion.ErrInvalidExternalProductID
	}

	rows, err := r.queries.ListRawRecordsBySourceAndExternalID(ctx, generated.ListRawRecordsBySourceAndExternalIDParams{
		SourceID:          string(sourceID),
		ExternalProductID: strings.TrimSpace(externalProductID),
		Limit:             listLimit(limit, 50),
	})
	if err != nil {
		return nil, fmt.Errorf("list raw records by source and external id: %w", err)
	}

	records := make([]*ingestion.RawRecord, 0, len(rows))
	for _, row := range rows {
		records = append(records, toDomainRawRecord(row))
	}
	return records, nil
}

func (r *RawRecordRepository) ListByRunID(
	ctx context.Context,
	runID string,
	limit int,
) ([]*ingestion.RawRecord, error) {
	runUUID, err := parseUUID(runID)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ingestion.ErrInvalidRunID, err)
	}

	rows, err := r.queries.ListRawRecordsByRunID(ctx, generated.ListRawRecordsByRunIDParams{
		IngestionRunID: runUUID,
		Limit:          listLimit(limit, 50),
	})
	if err != nil {
		return nil, fmt.Errorf("list raw records by run id: %w", err)
	}

	records := make([]*ingestion.RawRecord, 0, len(rows))
	for _, row := range rows {
		records = append(records, toDomainRawRecord(row))
	}
	return records, nil
}

func toDomainRawRecord(row generated.RawRecord) *ingestion.RawRecord {
	return &ingestion.RawRecord{
		ID:                uuidToString(row.ID),
		SourceID:          source.ID(row.SourceID),
		ExternalProductID: row.ExternalProductID,
		Payload:           row.Payload,
		PayloadRaw:        row.PayloadRaw,
		PayloadSHA256:     row.PayloadSha256,
		SourceVersion:     row.SourceVersion,
		ETag:              row.Etag,
		SourceUpdatedAt:   fromTimestamptz(row.SourceUpdatedAt),
		IngestionRunID:    uuidToString(row.IngestionRunID),
		ReceivedAt:        row.ReceivedAt.Time.UTC(),
	}
}
