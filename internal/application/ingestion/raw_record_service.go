package ingestion

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/source"
)

type RawRecordService struct {
	rawRepo    ingestion.RawRecordRepository
	sourceRepo source.Repository
	runRepo    ingestion.Repository
}

func NewRawRecordService(
	rawRepo ingestion.RawRecordRepository,
	sourceRepo source.Repository,
	runRepo ingestion.Repository,
) (*RawRecordService, error) {
	if rawRepo == nil {
		return nil, errors.New("raw record repository is required")
	}
	if sourceRepo == nil {
		return nil, errors.New("source repository is required")
	}
	if runRepo == nil {
		return nil, errors.New("ingestion repository is required")
	}
	return &RawRecordService{
		rawRepo:    rawRepo,
		sourceRepo: sourceRepo,
		runRepo:    runRepo,
	}, nil
}

type StoreRawRecordParams struct {
	ID                string
	SourceID          source.ID
	ExternalProductID string
	Payload           []byte
	SourceVersion     string
	ETag              string
	SourceUpdatedAt   *time.Time
	IngestionRunID    string
	ReceivedAt        time.Time
}

func (s *RawRecordService) StoreRawRecord(
	ctx context.Context,
	params StoreRawRecordParams,
) (*ingestion.RawRecord, error) {
	if strings.TrimSpace(string(params.SourceID)) == "" {
		return nil, ingestion.ErrInvalidSourceID
	}

	src, err := s.sourceRepo.FindByID(ctx, params.SourceID)
	if err != nil {
		return nil, fmt.Errorf("verify source for raw record: %w", err)
	}
	if !src.Enabled {
		return nil, ingestion.ErrInactiveSource
	}

	trimmedRunID := strings.TrimSpace(params.IngestionRunID)
	if trimmedRunID != "" {
		if _, err := s.runRepo.FindRunByID(ctx, trimmedRunID); err != nil {
			return nil, fmt.Errorf("verify ingestion run for raw record: %w", err)
		}
	}

	record, err := ingestion.NewRawRecord(ingestion.RawRecordParams{
		ID:                params.ID,
		SourceID:          params.SourceID,
		ExternalProductID: params.ExternalProductID,
		Payload:           params.Payload,
		SourceVersion:     params.SourceVersion,
		ETag:              params.ETag,
		SourceUpdatedAt:   params.SourceUpdatedAt,
		IngestionRunID:    trimmedRunID,
		ReceivedAt:        params.ReceivedAt,
	})
	if err != nil {
		return nil, fmt.Errorf("initialize raw record entity: %w", err)
	}

	if err := s.rawRepo.Save(ctx, record); err != nil {
		return nil, fmt.Errorf("persist raw record: %w", err)
	}

	return record, nil
}

func (s *RawRecordService) StoreRawRecordBatch(
	ctx context.Context,
	sourceID source.ID,
	runID string,
	records []*ingestion.RawRecord,
) ([]*ingestion.RawRecord, error) {
	if strings.TrimSpace(string(sourceID)) == "" {
		return nil, ingestion.ErrInvalidSourceID
	}

	if len(records) == 0 {
		return []*ingestion.RawRecord{}, nil
	}

	src, err := s.sourceRepo.FindByID(ctx, sourceID)
	if err != nil {
		return nil, fmt.Errorf("verify source for batch: %w", err)
	}
	if !src.Enabled {
		return nil, ingestion.ErrInactiveSource
	}

	trimmedRunID := strings.TrimSpace(runID)
	if trimmedRunID != "" {
		if _, err := s.runRepo.FindRunByID(ctx, trimmedRunID); err != nil {
			return nil, fmt.Errorf("verify ingestion run for batch: %w", err)
		}
	}

	for _, rec := range records {
		rec.SourceID = sourceID
		rec.IngestionRunID = trimmedRunID
		if err := rec.Validate(); err != nil {
			return nil, fmt.Errorf("validate batch record: %w", err)
		}
	}

	if err := s.rawRepo.SaveBatch(ctx, records); err != nil {
		return nil, fmt.Errorf("persist raw record batch: %w", err)
	}

	return records, nil
}

func (s *RawRecordService) GetRecordByID(ctx context.Context, id string) (*ingestion.RawRecord, error) {
	if strings.TrimSpace(id) == "" {
		return nil, ingestion.ErrInvalidRecordID
	}

	record, err := s.rawRepo.FindByID(ctx, strings.TrimSpace(id))
	if err != nil {
		return nil, fmt.Errorf("get raw record by id: %w", err)
	}

	return record, nil
}

func (s *RawRecordService) GetLatestRecord(
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

	record, err := s.rawRepo.FindLatestBySourceAndExternalID(
		ctx,
		sourceID,
		strings.TrimSpace(externalProductID),
	)
	if err != nil {
		return nil, fmt.Errorf("get latest raw record: %w", err)
	}

	return record, nil
}

func (s *RawRecordService) ListRecords(
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

	records, err := s.rawRepo.ListBySourceAndExternalID(
		ctx,
		sourceID,
		strings.TrimSpace(externalProductID),
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list raw records: %w", err)
	}

	return records, nil
}

func (s *RawRecordService) ListRecordsByRun(
	ctx context.Context,
	runID string,
	limit int,
) ([]*ingestion.RawRecord, error) {
	if strings.TrimSpace(runID) == "" {
		return nil, ingestion.ErrInvalidRunID
	}

	records, err := s.rawRepo.ListByRunID(ctx, strings.TrimSpace(runID), limit)
	if err != nil {
		return nil, fmt.Errorf("list raw records by run: %w", err)
	}

	return records, nil
}
