package ingestion

import (
	"crypto/sha256"
	"encoding/hex"
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
	PayloadRaw        []byte
	PayloadSHA256     string
	SourceVersion     string
	ETag              string
	SourceUpdatedAt   *time.Time
	IngestionRunID    string
	ReceivedAt        time.Time
}

type RawRecordParams struct {
	ID                string
	SourceID          source.ID
	ExternalProductID string
	Payload           []byte
	PayloadRaw        []byte
	PayloadSHA256     string
	SourceVersion     string
	ETag              string
	SourceUpdatedAt   *time.Time
	IngestionRunID    string
	ReceivedAt        time.Time
}

func NewRawRecord(params RawRecordParams) (*RawRecord, error) {
	trimmedID := strings.TrimSpace(params.ID)
	if trimmedID == "" {
		generatedID, err := uuid.NewString()
		if err != nil {
			return nil, fmt.Errorf("generate raw record id: %w", err)
		}
		trimmedID = generatedID
	}

	recordTime := params.ReceivedAt.UTC()
	if recordTime.IsZero() {
		recordTime = time.Now().UTC()
	}

	var srcUpdatedAt *time.Time
	if params.SourceUpdatedAt != nil && !params.SourceUpdatedAt.IsZero() {
		t := params.SourceUpdatedAt.UTC()
		srcUpdatedAt = &t
	}

	rawBytes := params.PayloadRaw
	if len(rawBytes) == 0 && len(params.Payload) > 0 {
		rawBytes = params.Payload
	}

	sha := strings.TrimSpace(params.PayloadSHA256)
	if sha == "" && len(rawBytes) > 0 {
		sum := sha256.Sum256(rawBytes)
		sha = hex.EncodeToString(sum[:])
	}

	record := &RawRecord{
		ID:                trimmedID,
		SourceID:          params.SourceID,
		ExternalProductID: strings.TrimSpace(params.ExternalProductID),
		Payload:           params.Payload,
		PayloadRaw:        rawBytes,
		PayloadSHA256:     sha,
		SourceVersion:     strings.TrimSpace(params.SourceVersion),
		ETag:              strings.TrimSpace(params.ETag),
		SourceUpdatedAt:   srcUpdatedAt,
		IngestionRunID:    strings.TrimSpace(params.IngestionRunID),
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
