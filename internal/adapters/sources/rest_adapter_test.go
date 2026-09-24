package sources_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/adapters/sources"
	"github.com/ayo6706/cross-border-ecommerce/internal/adapters/sources/auth"
	"github.com/ayo6706/cross-border-ecommerce/internal/adapters/sources/extraction"
	"github.com/ayo6706/cross-border-ecommerce/internal/adapters/sources/identity"
	"github.com/ayo6706/cross-border-ecommerce/internal/adapters/sources/pagination"
	"github.com/ayo6706/cross-border-ecommerce/internal/adapters/sources/policy"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/source"
)

func TestRESTAdapter_RealWorldSuppliers(t *testing.T) {
	t.Run("ShopifyRESTProductCatalog", func(t *testing.T) {
		shopifyResp := `{
			"products": [
				{
					"id": 632910392,
					"title": "Burton Custom Freestyle Snowboard",
					"vendor": "Burton",
					"variants": [
						{"id": 808950810, "sku": "BURTON-SNOW-123", "price": "499.99"}
					]
				},
				{
					"id": 632910393,
					"title": "Burton Bindings",
					"vendor": "Burton",
					"variants": [
						{"id": 808950811, "sku": "BURTON-BIND-456", "price": "199.99"}
					]
				}
			]
		}`

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer shpat_test123" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Link", `<https://example.com/products.json?page_info=next_page_token>; rel="next"`)
			_, _ = w.Write([]byte(shopifyResp))
		}))
		defer server.Close()

		adapter, err := sources.NewRESTAdapter(sources.RESTAdapterConfig{
			SourceID:   source.ID("shopify-us"),
			BaseURL:    server.URL,
			Client:     server.Client(),
			Auth:       auth.NewBearerAuth("shpat_test123"),
			Pagination: pagination.NewLinkHeaderPagination(),
			Extractor:  extraction.NewPathRecordExtractor("products"),
			Identity:   identity.NewPathIdentityStrategy("variants[0].sku"),
		})
		if err != nil {
			t.Fatalf("unexpected init error: %v", err)
		}

		res, err := adapter.Fetch(context.Background(), ingestion.FetchRequest{})
		if err != nil {
			t.Fatalf("fetch records failed: %v", err)
		}

		if len(res.Records) != 2 {
			t.Fatalf("expected 2 records, got %d", len(res.Records))
		}
		if res.Records[0].ExternalProductID != "BURTON-SNOW-123" {
			t.Errorf("expected BURTON-SNOW-123, got %q", res.Records[0].ExternalProductID)
		}
		if res.Records[1].ExternalProductID != "BURTON-BIND-456" {
			t.Errorf("expected BURTON-BIND-456, got %q", res.Records[1].ExternalProductID)
		}
		if res.NextCheckpoint != "https://example.com/products.json?page_info=next_page_token" {
			t.Errorf("expected next link checkpoint, got %q", res.NextCheckpoint)
		}
	})

	t.Run("AmazonSPAPICatalogItems", func(t *testing.T) {
		amazonResp := `{
			"numberOfResults": 1050,
			"pagination": {
				"nextToken": "amzn.nextToken.12345"
			},
			"items": [
				{
					"asin": "B07N4M94X4",
					"summaries": [{"brandName": "Samsung", "itemName": "OLED TV"}]
				}
			]
		}`

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("x-amz-access-token") != "amzn_access_token_abc" {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(amazonResp))
		}))
		defer server.Close()

		adapter, err := sources.NewRESTAdapter(sources.RESTAdapterConfig{
			SourceID:   source.ID("amazon-sp-eu"),
			BaseURL:    server.URL,
			Client:     server.Client(),
			Auth:       auth.NewAPIKeyHeaderAuth("amzn_access_token_abc", "x-amz-access-token"),
			Pagination: pagination.NewCursorPagination("nextToken", "pagination.nextToken"),
			Extractor:  extraction.NewPathRecordExtractor("items"),
			Identity:   identity.NewPathIdentityStrategy("asin"),
		})
		if err != nil {
			t.Fatalf("unexpected init error: %v", err)
		}

		res, err := adapter.Fetch(context.Background(), ingestion.FetchRequest{})
		if err != nil {
			t.Fatalf("fetch failed: %v", err)
		}
		if len(res.Records) != 1 || res.Records[0].ExternalProductID != "B07N4M94X4" {
			t.Fatalf("expected asin B07N4M94X4, got %v", res.Records)
		}
		if res.NextCheckpoint != "cursor:amzn.nextToken.12345" {
			t.Errorf("expected cursor:amzn.nextToken.12345, got %q", res.NextCheckpoint)
		}
	})

	t.Run("WarehouseERPWithCompositeIdentity", func(t *testing.T) {
		erpResp := `{
			"code": 200,
			"payload": {
				"catalog": [
					{"warehouse_id": "WH-DE", "part_number": "PART-100", "qty": 45},
					{"warehouse_id": "WH-DE", "part_number": "PART-200", "qty": 12}
				],
				"paging": {"scroll_id": "scroll_abc_999"}
			}
		}`

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(erpResp))
		}))
		defer server.Close()

		compId, _ := identity.NewCompositeIdentityStrategy([]string{"warehouse_id", "part_number"}, ":")
		adapter, err := sources.NewRESTAdapter(sources.RESTAdapterConfig{
			SourceID:   source.ID("sap-erp-de"),
			BaseURL:    server.URL,
			Client:     server.Client(),
			Pagination: pagination.NewCursorPagination("scroll_id", "payload.paging.scroll_id"),
			Extractor:  extraction.NewPathRecordExtractor("payload.catalog"),
			Identity:   compId,
		})
		if err != nil {
			t.Fatalf("unexpected init error: %v", err)
		}

		res, err := adapter.Fetch(context.Background(), ingestion.FetchRequest{})
		if err != nil {
			t.Fatalf("fetch failed: %v", err)
		}
		if len(res.Records) != 2 {
			t.Fatalf("expected 2 records, got %d", len(res.Records))
		}
		if res.Records[0].ExternalProductID != "WH-DE:PART-100" {
			t.Errorf("expected WH-DE:PART-100, got %q", res.Records[0].ExternalProductID)
		}
		if res.Records[1].ExternalProductID != "WH-DE:PART-200" {
			t.Errorf("expected WH-DE:PART-200, got %q", res.Records[1].ExternalProductID)
		}
		if res.NextCheckpoint != "cursor:scroll_abc_999" {
			t.Errorf("expected cursor:scroll_abc_999, got %q", res.NextCheckpoint)
		}
	})

	t.Run("SourceContractViolationOnMissingPath", func(t *testing.T) {
		changedSchemaResp := `{"products": [{"id": "P1"}]}`

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(changedSchemaResp))
		}))
		defer server.Close()

		adapter, err := sources.NewRESTAdapter(sources.RESTAdapterConfig{
			SourceID:  source.ID("fragile-supplier"),
			BaseURL:   server.URL,
			Client:    server.Client(),
			Extractor: extraction.NewPathRecordExtractor("items"), // expects "items"
			Identity:  identity.NewPathIdentityStrategy("id"),
		})
		if err != nil {
			t.Fatalf("unexpected init error: %v", err)
		}

		_, err = adapter.Fetch(context.Background(), ingestion.FetchRequest{})
		if err == nil {
			t.Fatal("expected error on contract violation, got nil")
		}
		if !errors.Is(err, ingestion.ErrSourceContractViolation) {
			t.Errorf("expected ErrSourceContractViolation, got %v", err)
		}
		if !errors.Is(err, ingestion.ErrPathNotFound) {
			t.Errorf("expected underlying ErrPathNotFound, got %v", err)
		}
	})

	t.Run("SkippedRowsAreCountedNotFailed", func(t *testing.T) {
		corruptedItemsResp := `{
			"items": [
				{"id": "VALID-1"},
				{"bad_key": "NO_ID"},
				{"id": "VALID-2"},
				{"bad_key": "NO_ID"},
				{"id": "VALID-3"},
				{"bad_key": "NO_ID"},
				{"id": "VALID-4"},
				{"bad_key": "NO_ID"},
				{"id": "VALID-5"},
				{"bad_key": "NO_ID"},
				{"bad_key": "NO_ID"},
				{"bad_key": "NO_ID"}
			]
		}`

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(corruptedItemsResp))
		}))
		defer server.Close()

		adapter, err := sources.NewRESTAdapter(sources.RESTAdapterConfig{
			SourceID:    source.ID("corrupted-source"),
			BaseURL:     server.URL,
			Client:      server.Client(),
			Extractor:   extraction.NewPathRecordExtractor("items"),
			Identity:    identity.NewPathIdentityStrategy("id"),
			ErrorPolicy: policy.ErrorPolicy{Policy: policy.PolicySkipMalformed},
		})
		if err != nil {
			t.Fatalf("unexpected init error: %v", err)
		}

		// The adapter only applies the row policy; the run-level error budget is
		// enforced by the sync coordinator over run totals.
		res, err := adapter.Fetch(context.Background(), ingestion.FetchRequest{})
		if err != nil {
			t.Fatalf("expected skipped rows not to fail the fetch, got %v", err)
		}
		if len(res.Records) != 5 || res.Failed != 7 {
			t.Fatalf("expected 5 records and 7 skipped rows, got %d records and %d skipped", len(res.Records), res.Failed)
		}
	})
}

func TestRESTAdapter_ErrorHandlingAndRateLimiting(t *testing.T) {
	t.Run("RateLimitHTTP429ReturnsErrRateLimitExceeded", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusTooManyRequests)
		}))
		defer ts.Close()

		adapter, err := sources.NewRESTAdapter(sources.RESTAdapterConfig{
			SourceID: source.ID("src-429"),
			BaseURL:  ts.URL,
			Client:   ts.Client(),
		})
		if err != nil {
			t.Fatalf("unexpected init error: %v", err)
		}

		_, err = adapter.Fetch(context.Background(), ingestion.FetchRequest{})
		if !errors.Is(err, ingestion.ErrRateLimitExceeded) {
			t.Errorf("expected ErrRateLimitExceeded, got %v", err)
		}
	})

	t.Run("Server500ReturnsErrAdapterUnavailable", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer ts.Close()

		adapter, err := sources.NewRESTAdapter(sources.RESTAdapterConfig{
			SourceID: source.ID("src-500"),
			BaseURL:  ts.URL,
			Client:   ts.Client(),
		})
		if err != nil {
			t.Fatalf("unexpected init error: %v", err)
		}

		_, err = adapter.Fetch(context.Background(), ingestion.FetchRequest{})
		if !errors.Is(err, ingestion.ErrAdapterUnavailable) {
			t.Errorf("expected ErrAdapterUnavailable, got %v", err)
		}
	})

	t.Run("AuthenticationFailedReturnsErrAuthenticationFailed", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
		}))
		defer ts.Close()

		adapter, err := sources.NewRESTAdapter(sources.RESTAdapterConfig{
			SourceID: source.ID("src-401"),
			BaseURL:  ts.URL,
			Client:   ts.Client(),
		})
		if err != nil {
			t.Fatalf("unexpected init error: %v", err)
		}

		_, err = adapter.Fetch(context.Background(), ingestion.FetchRequest{})
		if !errors.Is(err, ingestion.ErrAuthenticationFailed) {
			t.Errorf("expected ErrAuthenticationFailed, got %v", err)
		}
	})

	t.Run("OversizedResponseBodyReturnsErrAdapterUnavailable", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(bytes.Repeat([]byte("a"), 200))
		}))
		defer ts.Close()

		adapter, err := sources.NewRESTAdapter(sources.RESTAdapterConfig{
			SourceID:     source.ID("src-oversized"),
			BaseURL:      ts.URL,
			Client:       ts.Client(),
			MaxBodyBytes: 100, // Small limit
		})
		if err != nil {
			t.Fatalf("unexpected init error: %v", err)
		}

		_, err = adapter.Fetch(context.Background(), ingestion.FetchRequest{})
		if !errors.Is(err, ingestion.ErrAdapterUnavailable) {
			t.Errorf("expected ErrAdapterUnavailable on oversized body, got %v", err)
		}
	})

	t.Run("ClientRateLimiterEnforcement", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"items": [{"id": "1"}]}`))
		}))
		defer ts.Close()

		limiter, err := sources.NewTokenBucketLimiter(10, 1)
		if err != nil {
			t.Fatalf("unexpected limiter error: %v", err)
		}

		adapter, err := sources.NewRESTAdapter(sources.RESTAdapterConfig{
			SourceID:    source.ID("src-throttled"),
			BaseURL:     ts.URL,
			Client:      ts.Client(),
			RateLimiter: limiter,
			Extractor:   extraction.NewPathRecordExtractor("items"),
			Identity:    identity.NewPathIdentityStrategy("id"),
		})
		if err != nil {
			t.Fatalf("unexpected init error: %v", err)
		}

		ctx := context.Background()
		start := time.Now()
		for range 3 {
			if _, err := adapter.Fetch(ctx, ingestion.FetchRequest{}); err != nil {
				t.Fatalf("fetch error: %v", err)
			}
		}
		duration := time.Since(start)
		if duration < 150*time.Millisecond {
			t.Errorf("expected rate limiter to pace requests across at least 150ms, took %v", duration)
		}
	})

	t.Run("InvalidConstructorConfigs", func(t *testing.T) {
		_, err := sources.NewRESTAdapter(sources.RESTAdapterConfig{
			SourceID: "",
			BaseURL:  "https://example.com",
		})
		if !errors.Is(err, ingestion.ErrInvalidSourceID) {
			t.Errorf("expected ErrInvalidSourceID, got %v", err)
		}

		_, err = sources.NewRESTAdapter(sources.RESTAdapterConfig{
			SourceID: "valid-id",
			BaseURL:  "",
		})
		if !errors.Is(err, ingestion.ErrSourceContractViolation) {
			t.Errorf("expected ErrSourceContractViolation, got %v", err)
		}
	})

	t.Run("PagePaginationDoesNotHaltEarlyOnBadItem", func(t *testing.T) {
		respJSON := `{"items": [{"id": "ITEM-1"}, {"name": "No ID"}, {"id": "ITEM-3"}]}`
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(respJSON))
		}))
		defer server.Close()

		adapter, err := sources.NewRESTAdapter(sources.RESTAdapterConfig{
			SourceID:    source.ID("page-src"),
			BaseURL:     server.URL,
			Client:      server.Client(),
			Pagination:  pagination.NewPagePagination("page", "limit", 3),
			Extractor:   extraction.NewPathRecordExtractor("items"),
			Identity:    identity.NewPathIdentityStrategy("id"),
			ErrorPolicy: policy.ErrorPolicy{Policy: policy.PolicySkipMalformed},
		})
		if err != nil {
			t.Fatalf("unexpected init error: %v", err)
		}

		res, err := adapter.Fetch(context.Background(), ingestion.FetchRequest{})
		if err != nil {
			t.Fatalf("unexpected fetch error: %v", err)
		}
		if len(res.Records) != 2 {
			t.Fatalf("expected 2 valid records, got %d", len(res.Records))
		}
		if res.NextCheckpoint != "2" {
			t.Fatalf("expected next checkpoint 2, got %q", res.NextCheckpoint)
		}
	})

	t.Run("ContextCancellationPreserved", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(200 * time.Millisecond)
		}))
		defer server.Close()

		adapter, err := sources.NewRESTAdapter(sources.RESTAdapterConfig{
			SourceID: source.ID("timeout-src"),
			BaseURL:  server.URL,
			Client:   server.Client(),
		})
		if err != nil {
			t.Fatalf("unexpected init error: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()

		_, err = adapter.Fetch(ctx, ingestion.FetchRequest{})
		if err == nil {
			t.Fatal("expected error on cancelled context, got nil")
		}
		if errors.Is(err, ingestion.ErrAdapterUnavailable) {
			t.Errorf("cancelled context should NOT be classified as ErrAdapterUnavailable: %v", err)
		}
		if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			t.Errorf("expected context cancellation error, got %v", err)
		}
	})

	t.Run("HTTPClientTimeoutClassifiedAsUnavailable", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(200 * time.Millisecond)
		}))
		defer server.Close()

		adapter, err := sources.NewRESTAdapter(sources.RESTAdapterConfig{
			SourceID: source.ID("client-timeout-src"),
			BaseURL:  server.URL,
			Client: &http.Client{
				Timeout: 30 * time.Millisecond,
			},
		})
		if err != nil {
			t.Fatalf("unexpected init error: %v", err)
		}

		res, err := adapter.Fetch(context.Background(), ingestion.FetchRequest{})
		if err == nil {
			t.Fatal("expected error on client timeout, got nil")
		}
		if !errors.Is(err, ingestion.ErrAdapterUnavailable) {
			t.Errorf("expected ErrAdapterUnavailable on client timeout, got: %v", err)
		}
		if len(res.Records) != 0 || res.NextCheckpoint != "" {
			t.Errorf("expected empty records and nextCP, got %d records and %q", len(res.Records), res.NextCheckpoint)
		}
	})
}
