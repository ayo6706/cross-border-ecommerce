package postgres_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/source"
	"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/postgres"
	"github.com/ayo6706/cross-border-ecommerce/migrations"
)

func TestSourceRepository_ConstructorValidation(t *testing.T) {
	t.Parallel()

	repo, err := postgres.NewSourceRepository(nil)
	if err == nil {
		t.Fatal("expected error when db is nil, got nil")
	}
	if repo != nil {
		t.Fatalf("expected nil repo, got %v", repo)
	}
}

func TestSourceRepository_LiveIntegration(t *testing.T) {
	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		connStr = "postgres://postgres:postgres@127.0.0.1:5433/crossborder_test?sslmode=disable"
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
		return
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("expected successful pool ping, got %v", err)
	}

	migrator, err := postgres.NewMigrator(pool, migrations.FS)
	if err != nil {
		t.Fatalf("failed to create migrator: %v", err)
	}
	if err := migrator.Up(ctx); err != nil {
		t.Fatalf("failed to apply migrations: %v", err)
	}

	repo, err := postgres.NewSourceRepository(pool)
	if err != nil {
		t.Fatalf("failed to create source repository: %v", err)
	}

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupCtx, "DELETE FROM sources WHERE id IN ($1, $2, $3)",
			"shopify-us-live-test", "amazon-de-disabled-test", "source-in-tx")
	})

	// 1. Validation error on nil or empty ID
	if err := repo.Save(ctx, nil); !errors.Is(err, source.ErrInvalidSourceState) {
		t.Fatalf("expected ErrInvalidSourceState on nil source, got %v", err)
	}
	if err := repo.Save(ctx, &source.Source{ID: ""}); !errors.Is(err, source.ErrInvalidSourceState) {
		t.Fatalf("expected ErrInvalidSourceState on empty ID, got %v", err)
	}

	// 2. Save active source with JSONB configuration
	s1 := &source.Source{
		ID:        source.ID("shopify-us-live-test"),
		Name:      "Shopify US Merchant Store",
		Type:      source.TypeAPI,
		Config:    map[string]any{"store_domain": "us-merchant.myshopify.com", "sync_interval_sec": float64(300)},
		RateLimit: 50,
		Enabled:   true,
	}
	if err := repo.Save(ctx, s1); err != nil {
		t.Fatalf("failed to save source: %v", err)
	}

	// 3. FindByID
	found, err := repo.FindByID(ctx, s1.ID)
	if err != nil {
		t.Fatalf("failed to find source by id: %v", err)
	}
	if found.Name != s1.Name {
		t.Errorf("expected name %q, got %q", s1.Name, found.Name)
	}
	if found.Config["store_domain"] != "us-merchant.myshopify.com" {
		t.Errorf("expected store_domain in config, got %v", found.Config)
	}

	// 4. Save disabled source
	s2 := &source.Source{
		ID:        source.ID("amazon-de-disabled-test"),
		Name:      "Amazon DE Vendor",
		Type:      source.TypeFeed,
		Config:    map[string]any{"region": "eu-central-1"},
		RateLimit: 20,
		Enabled:   false,
	}
	if err := repo.Save(ctx, s2); err != nil {
		t.Fatalf("failed to save disabled source: %v", err)
	}

	// 5. FindActive
	activeList, err := repo.FindActive(ctx)
	if err != nil {
		t.Fatalf("failed to list active sources: %v", err)
	}
	var foundS1, foundS2 bool
	for _, s := range activeList {
		if s.ID == s1.ID {
			foundS1 = true
		}
		if s.ID == s2.ID {
			foundS2 = true
		}
	}
	if !foundS1 {
		t.Error("expected active source s1 to be in FindActive list")
	}
	if foundS2 {
		t.Error("expected disabled source s2 to NOT be in FindActive list")
	}

	// 6. Not found check
	_, err = repo.FindByID(ctx, source.ID("non-existent-source-id"))
	if !errors.Is(err, source.ErrSourceNotFound) {
		t.Errorf("expected ErrSourceNotFound, got %v", err)
	}

	// 7. Transaction support
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin tx: %v", err)
	}
	txRepo := repo.WithTx(tx)
	sTx := &source.Source{
		ID:      source.ID("source-in-tx"),
		Name:    "Source in Tx",
		Type:    source.TypeScraper,
		Enabled: true,
	}
	if err := txRepo.Save(ctx, sTx); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("failed to save source in tx: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("failed to rollback tx: %v", err)
	}

	_, err = repo.FindByID(ctx, sTx.ID)
	if !errors.Is(err, source.ErrSourceNotFound) {
		t.Errorf("expected ErrSourceNotFound after tx rollback, got %v", err)
	}
}
