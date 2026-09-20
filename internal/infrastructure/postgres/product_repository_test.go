package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/product"
	"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/postgres"
)

func TestProductRepository_CRUD(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo := postgres.NewProductRepository()

	p := &product.Product{
		ID:                 product.ID("prod-test-01"),
		CanonicalName:      "Logitech MX Master 3S",
		Description:        "Wireless Performance Mouse",
		Brand:              "Logitech",
		OriginCountry:      "CH",
		Status:             product.StatusActive,
		CurrentFingerprint: "sha256-abc123mockfingerprint",
		CreatedAt:          time.Now().UTC(),
		UpdatedAt:          time.Now().UTC(),
	}

	// 1. Save
	if err := repo.Save(ctx, p); err != nil {
		t.Fatalf("failed to save product: %v", err)
	}

	// 2. Find by ID
	found, err := repo.FindByID(ctx, p.ID)
	if err != nil {
		t.Fatalf("failed to find product by id: %v", err)
	}
	if found.CanonicalName != p.CanonicalName {
		t.Errorf("expected %q, got %q", p.CanonicalName, found.CanonicalName)
	}

	// 3. Find by Fingerprint
	byFp, err := repo.FindByFingerprint(ctx, p.CurrentFingerprint)
	if err != nil {
		t.Fatalf("failed to find product by fingerprint: %v", err)
	}
	if byFp.ID != p.ID {
		t.Errorf("expected id %v, got %v", p.ID, byFp.ID)
	}

	// 4. List
	list, err := repo.List(ctx, product.ListParams{Limit: 10})
	if err != nil {
		t.Fatalf("failed to list products: %v", err)
	}
	if len(list) != 1 {
		t.Errorf("expected 1 product in list, got %d", len(list))
	}

	// 5. Not found checks
	_, err = repo.FindByID(ctx, product.ID("unknown-id"))
	if !errors.Is(err, product.ErrProductNotFound) {
		t.Errorf("expected ErrProductNotFound, got %v", err)
	}
}
