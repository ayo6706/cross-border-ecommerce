package ingestion

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/source"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/uuid"
)

type RawRecord struct {
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

func NewRawRecord(
	id string,
	sourceID source.ID,
	externalProductID string,
	payload []byte,
	sourceVersion string,
	etag string,
	sourceUpdatedAt *time.Time,
	ingestionRunID string,
	receivedAt time.Time,
) (*RawRecord, error) {
	trimmedID := strings.TrimSpace(id)
	if trimmedID == "" {
		generatedID, err := uuid.NewString()
		if err != nil {
			return nil, fmt.Errorf("generate raw record id: %w", err)
		}
		trimmedID = generatedID
	}

	recordTime := receivedAt.UTC()
	if recordTime.IsZero() {
		recordTime = time.Now().UTC()
	}

	var srcUpdatedAt *time.Time
	if sourceUpdatedAt != nil && !sourceUpdatedAt.IsZero() {
		t := sourceUpdatedAt.UTC()
		srcUpdatedAt = &t
	}

	record := &RawRecord{
		ID:                trimmedID,
		SourceID:          sourceID,
		ExternalProductID: strings.TrimSpace(externalProductID),
		Payload:           payload,
		SourceVersion:     strings.TrimSpace(sourceVersion),
		ETag:              strings.TrimSpace(etag),
		SourceUpdatedAt:   srcUpdatedAt,
		IngestionRunID:    strings.TrimSpace(ingestionRunID),
		ReceivedAt:        recordTime,
	}

	if err := record.Validate(); err != nil {
		return nil, err
	}

	return record, nil
}

func (r *RawRecord) Validate() error {
	if r == nil {
		return ErrInvalidRecordState
	}
	if strings.TrimSpace(r.ID) == "" {
		return ErrInvalidRecordID
	}
	if strings.TrimSpace(string(r.SourceID)) == "" {
		return ErrInvalidSourceID
	}
	if strings.TrimSpace(r.ExternalProductID) == "" {
		return ErrInvalidExternalProductID
	}
	if len(r.Payload) == 0 {
		return ErrEmptyPayload
	}
	if !json.Valid(r.Payload) {
		return ErrInvalidPayloadJSON
	}
	return nil
}
