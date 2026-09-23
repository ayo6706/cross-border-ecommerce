package extraction_test

import (
	"errors"
	"testing"

	"github.com/ayo6706/cross-border-ecommerce/internal/adapters/sources/extraction"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
)

func TestPathRecordExtractor(t *testing.T) {
	t.Run("RootArrayExtraction", func(t *testing.T) {
		jsonArray := `[{"id":"P1","name":"Item 1"},{"id":"P2","name":"Item 2"}]`
		extractor := extraction.NewPathRecordExtractor("")

		records, err := extractor.ExtractRecords([]byte(jsonArray))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(records) != 2 {
			t.Fatalf("expected 2 records, got %d", len(records))
		}
	})

	t.Run("RootArrayRejectsNonObjectItems", func(t *testing.T) {
		badArray := `[{"id":"P1"}, "string-item", {"id":"P3"}]`
		extractor := extraction.NewPathRecordExtractor("")

		_, err := extractor.ExtractRecords([]byte(badArray))
		if err == nil {
			t.Fatal("expected error for non-object element, got nil")
		}
		if !errors.Is(err, ingestion.ErrRecordNotObject) {
			t.Errorf("expected ErrRecordNotObject, got %v", err)
		}
	})

	t.Run("RootArrayExpectedFoundObject", func(t *testing.T) {
		jsonObject := `{"products":[{"id":"P1"}]}`
		extractor := extraction.NewPathRecordExtractor("")

		_, err := extractor.ExtractRecords([]byte(jsonObject))
		if err == nil {
			t.Fatal("expected error when root is object but array was expected, got nil")
		}
		if !errors.Is(err, ingestion.ErrPathWrongType) {
			t.Errorf("expected ErrPathWrongType, got %v", err)
		}
	})

	t.Run("NestedPathExtraction", func(t *testing.T) {
		payload := `{
			"code": 200,
			"payload": {
				"catalog": [
					{"sku": "SKU-001", "title": "Keyboard"},
					{"sku": "SKU-002", "title": "Mouse"}
				]
			}
		}`
		extractor := extraction.NewPathRecordExtractor("payload.catalog")

		records, err := extractor.ExtractRecords([]byte(payload))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(records) != 2 {
			t.Fatalf("expected 2 records, got %d", len(records))
		}
	})

	t.Run("ShopifyAndAmazonFormats", func(t *testing.T) {
		shopifyPayload := `{"products": [{"id": 632910392, "title": "Snowboard"}]}`
		shopifyExtractor := extraction.NewPathRecordExtractor("products")
		shopifyRecords, err := shopifyExtractor.ExtractRecords([]byte(shopifyPayload))
		if err != nil {
			t.Fatalf("shopify extraction failed: %v", err)
		}
		if len(shopifyRecords) != 1 {
			t.Fatalf("expected 1 shopify record, got %d", len(shopifyRecords))
		}

		amazonPayload := `{"items": [{"asin": "B07N4M94X4", "summaries": [{"itemName": "TV"}]}]}`
		amazonExtractor := extraction.NewPathRecordExtractor("items")
		amazonRecords, err := amazonExtractor.ExtractRecords([]byte(amazonPayload))
		if err != nil {
			t.Fatalf("amazon extraction failed: %v", err)
		}
		if len(amazonRecords) != 1 {
			t.Fatalf("expected 1 amazon record, got %d", len(amazonRecords))
		}
	})

	t.Run("PathNotFoundReturnsErrPathNotFound", func(t *testing.T) {
		payload := `{"data": {"items": []}}`
		extractor := extraction.NewPathRecordExtractor("payload.catalog")

		_, err := extractor.ExtractRecords([]byte(payload))
		if err == nil {
			t.Fatal("expected error for missing path, got nil")
		}
		if !errors.Is(err, ingestion.ErrPathNotFound) {
			t.Errorf("expected ErrPathNotFound, got %v", err)
		}
	})

	t.Run("PathResolvedToWrongType", func(t *testing.T) {
		payload := `{"payload": {"catalog": "not-an-array"}}`
		extractor := extraction.NewPathRecordExtractor("payload.catalog")

		_, err := extractor.ExtractRecords([]byte(payload))
		if err == nil {
			t.Fatal("expected error when path is not an array, got nil")
		}
		if !errors.Is(err, ingestion.ErrPathWrongType) {
			t.Errorf("expected ErrPathWrongType, got %v", err)
		}
	})

	t.Run("RecordInArrayNotObject", func(t *testing.T) {
		payload := `{"items": [{"id": "1"}, 12345]}`
		extractor := extraction.NewPathRecordExtractor("items")

		_, err := extractor.ExtractRecords([]byte(payload))
		if err == nil {
			t.Fatal("expected error when array item is not an object, got nil")
		}
		if !errors.Is(err, ingestion.ErrRecordNotObject) {
			t.Errorf("expected ErrRecordNotObject, got %v", err)
		}
	})
}
