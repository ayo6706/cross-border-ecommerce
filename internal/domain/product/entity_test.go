package product_test

import (
	"errors"
	"testing"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/product"
)

func TestProduct_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		product   *product.Product
		expectErr error
	}{
		{
			name:      "nil product receiver returns ErrInvalidProductState",
			product:   nil,
			expectErr: product.ErrInvalidProductState,
		},
		{
			name: "empty canonical name returns ErrEmptyCanonicalName",
			product: &product.Product{
				CanonicalName: "",
			},
			expectErr: product.ErrEmptyCanonicalName,
		},
		{
			name: "whitespace-only canonical name returns ErrEmptyCanonicalName",
			product: &product.Product{
				CanonicalName: "   \t\n ",
			},
			expectErr: product.ErrEmptyCanonicalName,
		},
		{
			name: "valid product returns nil",
			product: &product.Product{
				ID:            "prod-001",
				CanonicalName: "Sony WH-1000XM5",
			},
			expectErr: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.product.Validate()
			if tc.expectErr != nil {
				if !errors.Is(err, tc.expectErr) {
					t.Fatalf("expected error %v, got %v", tc.expectErr, err)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestNewProduct(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		id            product.ID
		canonicalName string
		description   string
		brand         string
		originCountry string
		expectErr     error
		verify        func(t *testing.T, p *product.Product)
	}{
		{
			name:          "valid product construction",
			id:            "prod-001",
			canonicalName: "  Mechanical Keyboard  ",
			description:   "  Keychron Q1 Pro  ",
			brand:         "  Keychron  ",
			originCountry: "cn",
			expectErr:     nil,
			verify: func(t *testing.T, p *product.Product) {
				if p.ID != "prod-001" {
					t.Errorf("expected ID 'prod-001', got %q", p.ID)
				}
				if p.CanonicalName != "Mechanical Keyboard" {
					t.Errorf("expected trimmed CanonicalName, got %q", p.CanonicalName)
				}
				if p.Description != "Keychron Q1 Pro" {
					t.Errorf("expected trimmed Description, got %q", p.Description)
				}
				if p.Brand != "Keychron" {
					t.Errorf("expected trimmed Brand, got %q", p.Brand)
				}
				if p.OriginCountry != "CN" {
					t.Errorf("expected uppercase OriginCountry 'CN', got %q", p.OriginCountry)
				}
				if p.Status != product.StatusDraft {
					t.Errorf("expected default StatusDraft, got %q", p.Status)
				}
				if p.CreatedAt.IsZero() || p.UpdatedAt.IsZero() {
					t.Error("expected non-zero timestamps")
				}
			},
		},
		{
			name:          "empty canonical name returns validation error",
			id:            "prod-002",
			canonicalName: "   ",
			description:   "Desc",
			brand:         "Brand",
			originCountry: "US",
			expectErr:     product.ErrEmptyCanonicalName,
			verify:        nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			p, err := product.NewProduct(tc.id, tc.canonicalName, tc.description, tc.brand, tc.originCountry)
			if tc.expectErr != nil {
				if !errors.Is(err, tc.expectErr) {
					t.Fatalf("expected error %v, got %v", tc.expectErr, err)
				}
				if p != nil {
					t.Fatalf("expected nil product on error, got %v", p)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if p == nil {
				t.Fatal("expected non-nil product")
			}
			if tc.verify != nil {
				tc.verify(t, p)
			}
		})
	}
}
