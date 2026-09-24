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
	"github.com/jackc/pgx/v5/pgxpool"
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

func setupLiveSourceDB(t *testing.T) (*pgxpool.Pool, *postgres.SourceRepository) {
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
	t.Cleanup(pool.Close)

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

	repo, err := postgres.NewSourceRepository(pool)
	if err != nil {
		t.Fatalf("failed to create source repository: %v", err)
	}

	return pool, repo
}

func TestSourceRepository_LiveIntegration(t *testing.T) {
	pool, repo := setupLiveSourceDB(t)
	ctx := context.Background()

	t.Run("Validation_NilOrEmptyID", func(t *testing.T) {
		if err := repo.Save(ctx, nil); !errors.Is(err, source.ErrInvalidSourceState) {
			t.Fatalf("expected ErrInvalidSourceState on nil source, got %v", err)
		}
		if err := repo.Save(ctx, &source.Source{ID: ""}); !errors.Is(err, source.ErrInvalidSourceState) {
			t.Fatalf("expected ErrInvalidSourceState on empty ID, got %v", err)
		}
	})

	t.Run("Save_And_FindByID", func(t *testing.T) {
		s := &source.Source{
			ID:                 source.ID("shopify-us-live-test"),
			Name:               "Shopify US Merchant Store",
			Type:               source.TypeAPI,
			Config:             map[string]any{"store_domain": "us-merchant.myshopify.com", "sync_interval_sec": float64(300)},
			RateLimitPerSecond: 50,
			Enabled:            true,
		}
		t.Cleanup(func() {
			cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cleanCancel()
			_ = repo.Delete(cleanCtx, s.ID)
		})

		if err := repo.Save(ctx, s); err != nil {
			t.Fatalf("failed to save source: %v", err)
		}

		found, err := repo.FindByID(ctx, s.ID)
		if err != nil {
			t.Fatalf("failed to find source by id: %v", err)
		}
		if found.Name != s.Name {
			t.Errorf("expected name %q, got %q", s.Name, found.Name)
		}
		if found.Config["store_domain"] != "us-merchant.myshopify.com" {
			t.Errorf("expected store_domain in config, got %v", found.Config)
		}
	})

	t.Run("FindActive_FiltersCorrectly", func(t *testing.T) {
		sActive := &source.Source{
			ID:                 source.ID("active-test-source"),
			Name:               "Active Source",
			Type:               source.TypeAPI,
			RateLimitPerSecond: 20,
			Enabled:            true,
		}
		sDisabled := &source.Source{
			ID:                 source.ID("disabled-test-source"),
			Name:               "Disabled Source",
			Type:               source.TypeFeed,
			RateLimitPerSecond: 10,
			Enabled:            false,
		}

		t.Cleanup(func() {
			cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cleanCancel()
			_ = repo.Delete(cleanCtx, sActive.ID)
			_ = repo.Delete(cleanCtx, sDisabled.ID)
		})

		_ = repo.Save(ctx, sActive)
		_ = repo.Save(ctx, sDisabled)

		activeList, err := repo.FindActive(ctx)
		if err != nil {
			t.Fatalf("failed to list active sources: %v", err)
		}

		var foundActive, foundDisabled bool
		for _, s := range activeList {
			if s.ID == sActive.ID {
				foundActive = true
			}
			if s.ID == sDisabled.ID {
				foundDisabled = true
			}
		}
		if !foundActive {
			t.Error("expected active source to be in FindActive list")
		}
		if foundDisabled {
			t.Error("expected disabled source to NOT be in FindActive list")
		}
	})

	t.Run("List_And_Delete", func(t *testing.T) {
		s := &source.Source{
			ID:                 source.ID("delete-test-source"),
			Name:               "To Delete",
			Type:               source.TypeFile,
			RateLimitPerSecond: 30,
			Enabled:            true,
		}
		_ = repo.Save(ctx, s)

		allList, err := repo.List(ctx)
		if err != nil {
			t.Fatalf("failed to list sources: %v", err)
		}
		var found bool
		for _, item := range allList {
			if item.ID == s.ID {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected created source to be in List()")
		}

		if err := repo.Delete(ctx, s.ID); err != nil {
			t.Fatalf("failed to delete source: %v", err)
		}

		_, err = repo.FindByID(ctx, s.ID)
		if !errors.Is(err, source.ErrSourceNotFound) {
			t.Fatalf("expected ErrSourceNotFound after delete, got %v", err)
		}
	})

	t.Run("FindByID_NotFound", func(t *testing.T) {
		_, err := repo.FindByID(ctx, source.ID("non-existent-source-id"))
		if !errors.Is(err, source.ErrSourceNotFound) {
			t.Errorf("expected ErrSourceNotFound, got %v", err)
		}
	})

	t.Run("Transaction_Rollback", func(t *testing.T) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("failed to begin tx: %v", err)
		}
		defer func() { _ = tx.Rollback(ctx) }()

		txRepo := repo.WithTx(tx)
		sTx := &source.Source{
			ID:      source.ID("source-in-tx"),
			Name:    "Source in Tx",
			Type:    source.TypeScraper,
			Enabled: true,
		}
		if err := txRepo.Save(ctx, sTx); err != nil {
			t.Fatalf("failed to save source in tx: %v", err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatalf("failed to rollback tx: %v", err)
		}

		_, err = repo.FindByID(ctx, sTx.ID)
		if !errors.Is(err, source.ErrSourceNotFound) {
			t.Errorf("expected ErrSourceNotFound after tx rollback, got %v", err)
		}
	})
}
