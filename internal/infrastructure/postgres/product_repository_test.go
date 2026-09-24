package postgres_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/product"
	"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/postgres"
	"github.com/ayo6706/cross-border-ecommerce/migrations"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestProductRepository_ConstructorValidation(t *testing.T) {
	t.Parallel()

	repo, err := postgres.NewProductRepository(nil)
	if err == nil {
		t.Fatal("expected error when db is nil, got nil")
	}
	if repo != nil {
		t.Fatalf("expected nil repo, got %v", repo)
	}
}

func setupLiveProductDB(t *testing.T) (*pgxpool.Pool, *postgres.ProductRepository) {
	t.Helper()

	connStr := os.Getenv("TEST_DATABASE_URL")
	if connStr == "" {
		t.Skip("skipping live database test: TEST_DATABASE_URL not set")
		return nil, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := postgres.NewPool(ctx, connStr,
		postgres.WithConnectTimeout(3*time.Second),
		postgres.WithMaxConns(5),
		postgres.WithMinConns(1),
	)
	if err != nil {
		t.Skipf("skipping live database test: unable to connect to %s: %v", connStr, err)
		return nil, nil
	}
	t.Cleanup(func() {
		cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanCancel()
		_, _ = pool.Exec(cleanCtx, "TRUNCATE products, product_versions, product_sources, product_changes, ingestion_run_processing CASCADE")
		pool.Close()
	})

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("expected successful pool ping: %v", err)
	}

	migrator, err := postgres.NewMigrator(pool, migrations.FS)
	if err != nil {
		t.Fatalf("failed to create migrator: %v", err)
	}
	if err := migrator.Up(ctx); err != nil {
		t.Fatalf("failed to apply migrations: %v", err)
	}

	repo, err := postgres.NewProductRepository(pool)
	if err != nil {
		t.Fatalf("failed to create product repository: %v", err)
	}

	return pool, repo
}

func TestProductRepository_LiveIntegration(t *testing.T) {
	pool, repo := setupLiveProductDB(t)
	ctx := context.Background()

	t.Run("Validation_NilOrInvalid", func(t *testing.T) {
		if err := repo.Save(ctx, nil); !errors.Is(err, product.ErrInvalidProductState) {
			t.Fatalf("expected ErrInvalidProductState on nil product, got %v", err)
		}

		invalidProduct := &product.Product{
			CanonicalName: "   ",
		}
		if err := repo.Save(ctx, invalidProduct); err == nil {
			t.Fatal("expected error on empty canonical name, got nil")
		}
	})

	t.Run("Save_And_FindByID", func(t *testing.T) {
		now := time.Now().UTC().Truncate(time.Microsecond)
		p1 := newTestProduct(t, "Logitech MX Master 3S", "Logitech", "CH", "sha256-logitech-mx3s-test")
		p1.Description = "Wireless Performance Mouse"
		p1.Status = product.StatusActive
		p1.CreatedAt = now
		p1.UpdatedAt = now

		if err := repo.Save(ctx, p1); err != nil {
			t.Fatalf("failed to save product: %v", err)
		}

		found, err := repo.FindByID(ctx, p1.ID)
		if err != nil {
			t.Fatalf("failed to find product by id: %v", err)
		}
		if found.CanonicalName != p1.CanonicalName {
			t.Errorf("expected canonical name %q, got %q", p1.CanonicalName, found.CanonicalName)
		}
		if found.Status != product.StatusActive {
			t.Errorf("expected status %v, got %v", product.StatusActive, found.Status)
		}

		// Update existing product
		p1.Brand = "Logitech International"
		if err := repo.Save(ctx, p1); err != nil {
			t.Fatalf("failed to update product: %v", err)
		}
		updated, err := repo.FindByID(ctx, p1.ID)
		if err != nil {
			t.Fatalf("failed to find updated product: %v", err)
		}
		if updated.Brand != "Logitech International" {
			t.Errorf("expected updated brand 'Logitech International', got %q", updated.Brand)
		}
	})

	t.Run("Find_NotFound", func(t *testing.T) {
		_, err := repo.FindByID(ctx, product.ID("00000000-0000-0000-0000-000000000000"))
		if !errors.Is(err, product.ErrProductNotFound) {
			t.Errorf("expected ErrProductNotFound, got %v", err)
		}
	})

	t.Run("Keyset_Pagination", func(t *testing.T) {
		now := time.Now().UTC()
		p2 := newTestProduct(t, "Apple Magic Keyboard", "Apple", "US", "sha256-apple-keyboard-test")
		p2.CreatedAt = now.Add(1 * time.Second)
		p3 := newTestProduct(t, "Dell UltraSharp 27", "Dell", "US", "sha256-dell-monitor-test")
		p3.CreatedAt = now.Add(2 * time.Second)
		if err := repo.Save(ctx, p2); err != nil {
			t.Fatalf("failed to save p2: %v", err)
		}
		if err := repo.Save(ctx, p3); err != nil {
			t.Fatalf("failed to save p3: %v", err)
		}

		page1, err := repo.List(ctx, product.ListParams{Limit: 2})
		if err != nil {
			t.Fatalf("failed to fetch page 1: %v", err)
		}
		if len(page1) != 2 {
			t.Fatalf("expected 2 items on page 1, got %d", len(page1))
		}

		lastItem := page1[1]
		page2, err := repo.List(ctx, product.ListParams{
			Limit:         2,
			LastCreatedAt: &lastItem.CreatedAt,
			LastID:        &lastItem.ID,
		})
		if err != nil {
			t.Fatalf("failed to fetch page 2: %v", err)
		}
		if len(page2) < 1 {
			t.Fatalf("expected at least 1 item on page 2, got %d", len(page2))
		}
		if page2[0].ID == lastItem.ID {
			t.Errorf("page 2 first item must not equal page 1 last item: %v", page2[0].ID)
		}

		// Verify partial cursor parameter rejection
		_, err = repo.List(ctx, product.ListParams{
			Limit:         2,
			LastCreatedAt: &lastItem.CreatedAt,
			LastID:        nil,
		})
		if err == nil {
			t.Error("expected error when LastCreatedAt is provided without LastID")
		}
		_, err = repo.List(ctx, product.ListParams{
			Limit:         2,
			LastCreatedAt: nil,
			LastID:        &lastItem.ID,
		})
		if err == nil {
			t.Error("expected error when LastID is provided without LastCreatedAt")
		}
	})

	t.Run("Transaction_Rollback", func(t *testing.T) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("failed to start transaction: %v", err)
		}
		defer func() { _ = tx.Rollback(ctx) }()

		txRepo := repo.WithTx(tx)
		pTx := newTestProduct(t, "Sony WH-1000XM5 in Tx", "Sony", "", "sha256-sony-tx-test")
		if err := txRepo.Save(ctx, pTx); err != nil {
			t.Fatalf("failed to save in transaction: %v", err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatalf("failed to rollback tx: %v", err)
		}

		_, err = repo.FindByID(ctx, pTx.ID)
		if !errors.Is(err, product.ErrProductNotFound) {
			t.Errorf("expected ErrProductNotFound for rolled back record, got %v", err)
		}
	})
}

func newTestProduct(t *testing.T, name, brand, origin, fingerprint string) *product.Product {
	t.Helper()

	p, err := product.NewProduct("", name, "", brand, origin)
	if err != nil {
		t.Fatalf("failed to build test product: %v", err)
	}
	p.CurrentFingerprint = fingerprint
	return p
}
