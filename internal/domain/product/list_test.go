package product_test

import (
	"errors"
	"testing"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/product"
)

func TestListParams_Validate(t *testing.T) {
	t.Parallel()

	at := time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)
	tests := []struct {
		name    string
		params  product.ListParams
		wantErr bool
	}{
		{"first_page", product.ListParams{Limit: 1}, false},
		{"max_limit", product.ListParams{Limit: product.MaxListLimit}, false},
		{"with_cursor", product.ListParams{Limit: 10, After: &product.Cursor{CreatedAt: at, ID: "p-1"}}, false},
		{"zero_limit", product.ListParams{Limit: 0}, true},
		{"negative_limit", product.ListParams{Limit: -1}, true},
		{"limit_over_max", product.ListParams{Limit: product.MaxListLimit + 1}, true},
		{"cursor_without_time", product.ListParams{Limit: 10, After: &product.Cursor{ID: "p-1"}}, true},
		{"cursor_without_id", product.ListParams{Limit: 10, After: &product.Cursor{CreatedAt: at, ID: " "}}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.params.Validate()
			if tc.wantErr != (err != nil) {
				t.Fatalf("Validate() = %v, wantErr %v", err, tc.wantErr)
			}
			if err != nil && !errors.Is(err, product.ErrInvalidListParams) {
				t.Fatalf("Validate() = %v, want ErrInvalidListParams", err)
			}
		})
	}
}
