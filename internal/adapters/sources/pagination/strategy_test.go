package pagination_test

import (
	"net/http"
	"testing"

	"github.com/ayo6706/cross-border-ecommerce/internal/adapters/sources/pagination"
)

func TestCursorPagination(t *testing.T) {
	strategy := pagination.NewCursorPagination("after", "pagination.nextToken")

	t.Run("ApplyPaginationSetsQueryParam", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "https://api.example.com/items", nil)
		err := strategy.ApplyPagination(req, "cursor:tok_abc123")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if req.URL.Query().Get("after") != "tok_abc123" {
			t.Errorf("expected query param after=tok_abc123, got %s", req.URL.RawQuery)
		}
	})

	t.Run("ExtractNextCheckpointExtractsToken", func(t *testing.T) {
		body := []byte(`{"pagination": {"nextToken": "tok_xyz789"}}`)
		cp, err := strategy.ExtractNextCheckpoint(nil, body, "", 10)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cp != "cursor:tok_xyz789" {
			t.Errorf("expected cursor:tok_xyz789, got %q", cp)
		}
	})

	t.Run("EchoedCursorTerminatesStream", func(t *testing.T) {
		body := []byte(`{"pagination": {"nextToken": "tok_same"}}`)
		cp, err := strategy.ExtractNextCheckpoint(nil, body, "cursor:tok_same", 10)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cp != "" {
			t.Errorf("expected empty checkpoint on echoed cursor, got %q", cp)
		}
	})
}

func TestPagePagination(t *testing.T) {
	strategy := pagination.NewPagePagination("page", "limit", 25)

	t.Run("ApplyPaginationInitialPage", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "https://api.example.com/items", nil)
		err := strategy.ApplyPagination(req, "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if req.URL.Query().Get("page") != "1" || req.URL.Query().Get("limit") != "25" {
			t.Errorf("unexpected query: %s", req.URL.RawQuery)
		}
	})

	t.Run("AdvancesWhenFullBatch", func(t *testing.T) {
		cp, err := strategy.ExtractNextCheckpoint(nil, []byte("{}"), "1", 25)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cp != "2" {
			t.Errorf("expected page 2, got %q", cp)
		}
	})

	t.Run("TerminatesWhenPartialBatch", func(t *testing.T) {
		cp, err := strategy.ExtractNextCheckpoint(nil, []byte("{}"), "1", 10)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cp != "" {
			t.Errorf("expected empty checkpoint for partial batch, got %q", cp)
		}
	})
}

func TestLinkHeaderPagination(t *testing.T) {
	strategy := pagination.NewLinkHeaderPagination()

	resp := &http.Response{
		Header: http.Header{
			"Link": []string{`<https://api.shopify.com/admin/products.json?page_info=hij123>; rel="next"`},
		},
	}

	cp, err := strategy.ExtractNextCheckpoint(resp, nil, "", 50)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cp != "https://api.shopify.com/admin/products.json?page_info=hij123" {
		t.Errorf("unexpected checkpoint: %q", cp)
	}
}
