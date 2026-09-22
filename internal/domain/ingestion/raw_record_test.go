package ingestion_test

import (
	"errors"
	"testing"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/source"
)

func TestNewRawRecord(t *testing.T) {
	validPayload := []byte(`{"sku": "ABC-123", "price": 49.99}`)
	now := time.Now().UTC()

	t.Run("ValidRecord_WithExplicitID", func(t *testing.T) {
		rec, err := ingestion.NewRawRecord(
			"c56a4180-65aa-42ec-a945-5fd21dec0538",
			source.ID("supplier-a"),
			"PROD-999",
			validPayload,
			"v1.0",
			"etag-xyz",
			&now,
			"b88e6e5a-73eb-46f9-b873-61f22e70b741",
			now,
		)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if rec.ID != "c56a4180-65aa-42ec-a945-5fd21dec0538" {
			t.Errorf("expected ID 'c56a4180-65aa-42ec-a945-5fd21dec0538', got: %s", rec.ID)
		}
		if rec.SourceID != "supplier-a" {
			t.Errorf("expected SourceID 'supplier-a', got: %s", rec.SourceID)
		}
		if rec.ExternalProductID != "PROD-999" {
			t.Errorf("expected ExternalProductID 'PROD-999', got: %s", rec.ExternalProductID)
		}
		if rec.SourceVersion != "v1.0" {
			t.Errorf("expected SourceVersion 'v1.0', got: %s", rec.SourceVersion)
		}
		if rec.ETag != "etag-xyz" {
			t.Errorf("expected ETag 'etag-xyz', got: %s", rec.ETag)
		}
		if rec.SourceUpdatedAt == nil || !rec.SourceUpdatedAt.Equal(now) {
			t.Errorf("expected SourceUpdatedAt %v, got: %v", now, rec.SourceUpdatedAt)
		}
		if rec.IngestionRunID != "b88e6e5a-73eb-46f9-b873-61f22e70b741" {
			t.Errorf("expected IngestionRunID 'b88e6e5a-73eb-46f9-b873-61f22e70b741', got: %s", rec.IngestionRunID)
		}
		if !rec.ReceivedAt.Equal(now) {
			t.Errorf("expected ReceivedAt %v, got: %v", now, rec.ReceivedAt)
		}
	})

	t.Run("ValidRecord_GeneratesIDAndDefaultReceivedAt", func(t *testing.T) {
		rec, err := ingestion.NewRawRecord(
			"",
			source.ID("supplier-b"),
			"SKU-456",
			validPayload,
			"",
			"",
			nil,
			"",
			time.Time{},
		)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if rec.ID == "" {
			t.Errorf("expected auto-generated UUID, got empty string")
		}
		if rec.ReceivedAt.IsZero() {
			t.Errorf("expected non-zero default ReceivedAt")
		}
		if rec.SourceUpdatedAt != nil {
			t.Errorf("expected nil SourceUpdatedAt, got: %v", rec.SourceUpdatedAt)
		}
	})

	t.Run("Validation_TableDrivenFailures", func(t *testing.T) {
		tests := []struct {
			name        string
			id          string
			sourceID    source.ID
			externalID  string
			payload     []byte
			expectedErr error
		}{
			{
				name:        "EmptySourceID",
				id:          "c56a4180-65aa-42ec-a945-5fd21dec0538",
				sourceID:    "",
				externalID:  "SKU-1",
				payload:     validPayload,
				expectedErr: ingestion.ErrInvalidSourceID,
			},
			{
				name:        "WhitespaceSourceID",
				id:          "c56a4180-65aa-42ec-a945-5fd21dec0538",
				sourceID:    "   ",
				externalID:  "SKU-1",
				payload:     validPayload,
				expectedErr: ingestion.ErrInvalidSourceID,
			},
			{
				name:        "EmptyExternalProductID",
				id:          "c56a4180-65aa-42ec-a945-5fd21dec0538",
				sourceID:    "src-1",
				externalID:  "",
				payload:     validPayload,
				expectedErr: ingestion.ErrInvalidExternalProductID,
			},
			{
				name:        "WhitespaceExternalProductID",
				id:          "c56a4180-65aa-42ec-a945-5fd21dec0538",
				sourceID:    "src-1",
				externalID:  "   ",
				payload:     validPayload,
				expectedErr: ingestion.ErrInvalidExternalProductID,
			},
			{
				name:        "NilPayload",
				id:          "c56a4180-65aa-42ec-a945-5fd21dec0538",
				sourceID:    "src-1",
				externalID:  "SKU-1",
				payload:     nil,
				expectedErr: ingestion.ErrEmptyPayload,
			},
			{
				name:        "EmptyPayloadBytes",
				id:          "c56a4180-65aa-42ec-a945-5fd21dec0538",
				sourceID:    "src-1",
				externalID:  "SKU-1",
				payload:     []byte{},
				expectedErr: ingestion.ErrEmptyPayload,
			},
			{
				name:        "InvalidJSONPayload",
				id:          "c56a4180-65aa-42ec-a945-5fd21dec0538",
				sourceID:    "src-1",
				externalID:  "SKU-1",
				payload:     []byte(`{invalid-json`),
				expectedErr: ingestion.ErrInvalidPayloadJSON,
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				_, err := ingestion.NewRawRecord(
					tc.id,
					tc.sourceID,
					tc.externalID,
					tc.payload,
					"",
					"",
					nil,
					"",
					time.Now(),
				)
				if !errors.Is(err, tc.expectedErr) {
					t.Fatalf("expected error %v, got %v", tc.expectedErr, err)
				}
			})
		}
	})
}

func TestRawRecord_Validate_NilReceiver(t *testing.T) {
	var rec *ingestion.RawRecord
	err := rec.Validate()
	if !errors.Is(err, ingestion.ErrInvalidRecordState) {
		t.Fatalf("expected ErrInvalidRecordState on nil receiver, got: %v", err)
	}
}

func TestRawRecord_Validate_EmptyID(t *testing.T) {
	rec := &ingestion.RawRecord{
		ID:                "   ",
		SourceID:          "source-1",
		ExternalProductID: "SKU-1",
		Payload:           []byte(`{"a": 1}`),
	}
	err := rec.Validate()
	if !errors.Is(err, ingestion.ErrInvalidRecordID) {
		t.Fatalf("expected ErrInvalidRecordID, got: %v", err)
	}
}
