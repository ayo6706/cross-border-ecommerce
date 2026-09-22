package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/source"
	"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/postgres"
	"github.com/ayo6706/cross-border-ecommerce/migrations"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestIngestionRepository_ConstructorValidation(t *testing.T) {
	t.Parallel()

	repo, err := postgres.NewIngestionRepository(nil)
	if err == nil {
		t.Fatal("expected error when db is nil, got nil")
	}
	if repo != nil {
		t.Fatalf("expected nil repo, got %v", repo)
	}
}

func setupLiveIngestionDB(t *testing.T) (*pgxpool.Pool, *postgres.IngestionRepository, source.ID) {
	t.Helper()

	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		connStr = "postgres://postgres:postgres@127.0.0.1:5433/crossborder_test?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool, err := postgres.NewPool(ctx, connStr,
		postgres.WithConnectTimeout(3*time.Second),
		postgres.WithMaxConns(5),
		postgres.WithMinConns(1),
	)
	if err != nil {
		t.Skipf("skipping live database test: unable to connect to %s: %v", connStr, err)
		return nil, nil, ""
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

	sourceRepo, err := postgres.NewSourceRepository(pool)
	if err != nil {
		t.Fatalf("failed to create source repo: %v", err)
	}

	ingestionRepo, err := postgres.NewIngestionRepository(pool)
	if err != nil {
		t.Fatalf("failed to create ingestion repo: %v", err)
	}

	testSourceID := source.ID(fmt.Sprintf("src-ingest-%d", time.Now().UnixNano()))
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_ = sourceRepo.Delete(cleanupCtx, testSourceID)
	})

	err = sourceRepo.Save(ctx, &source.Source{
		ID:        testSourceID,
		Name:      "Ingestion Run Test Source",
		Type:      source.TypeAPI,
		RateLimit: 100,
		Enabled:   true,
	})
	if err != nil {
		t.Fatalf("failed to seed source: %v", err)
	}

	return pool, ingestionRepo, testSourceID
}

func TestIngestionRepository_LiveIntegration(t *testing.T) {
	pool, repo, sourceID := setupLiveIngestionDB(t)
	ctx := context.Background()

	t.Run("Validation_NilRun", func(t *testing.T) {
		err := repo.CreateRun(ctx, nil)
		if !errors.Is(err, ingestion.ErrInvalidRunState) {
			t.Fatalf("expected ErrInvalidRunState on nil run, got %v", err)
		}
	})

	t.Run("Create_And_FindByID", func(t *testing.T) {
		now := time.Now().UTC()
		run, err := ingestion.NewRun("", sourceID, "cursor-init")
		if err != nil {
			t.Fatalf("failed to create domain run: %v", err)
		}
		if err := run.Start(now); err != nil {
			t.Fatalf("failed to start domain run: %v", err)
		}

		if err := repo.CreateRun(ctx, run); err != nil {
			t.Fatalf("failed to persist ingestion run: %v", err)
		}
		if run.ID == "" {
			t.Fatalf("expected non-empty run ID after insert")
		}

		found, err := repo.FindRunByID(ctx, run.ID)
		if err != nil {
			t.Fatalf("failed to find run by ID: %v", err)
		}
		if found.SourceID != sourceID {
			t.Errorf("expected source ID %s, got %s", sourceID, found.SourceID)
		}
		if found.Status != ingestion.StatusRunning {
			t.Errorf("expected status RUNNING, got %s", found.Status)
		}
		if found.Checkpoint != "cursor-init" {
			t.Errorf("expected checkpoint cursor-init, got %s", found.Checkpoint)
		}
	})

	t.Run("UpdateProgress_AtomicIncrements", func(t *testing.T) {
		now := time.Now().UTC()
		run, err := ingestion.NewRun("", sourceID, "")
		if err != nil {
			t.Fatalf("failed to initialize run: %v", err)
		}
		_ = run.Start(now)
		if err := repo.CreateRun(ctx, run); err != nil {
			t.Fatalf("failed to create run: %v", err)
		}

		batch1 := ingestion.BatchMetrics{
			Seen:      500,
			New:       100,
			Changed:   50,
			Unchanged: 350,
			Failed:    0,
		}
		if err := repo.UpdateProgress(ctx, run.ID, batch1, "cursor-500", now.Add(time.Second)); err != nil {
			t.Fatalf("failed to update run progress batch 1: %v", err)
		}

		batch2 := ingestion.BatchMetrics{
			Seen:      200,
			New:       20,
			Changed:   10,
			Unchanged: 170,
			Failed:    0,
		}
		if err := repo.UpdateProgress(ctx, run.ID, batch2, "cursor-700", now.Add(2*time.Second)); err != nil {
			t.Fatalf("failed to update run progress batch 2: %v", err)
		}

		found, err := repo.FindRunByID(ctx, run.ID)
		if err != nil {
			t.Fatalf("failed to find run after batches: %v", err)
		}
		if found.RecordsSeen != 700 || found.RecordsNew != 120 || found.RecordsChanged != 60 || found.RecordsUnchanged != 520 {
			t.Fatalf("expected atomic sum 700/120/60/520, got %+v", found)
		}
		if found.Checkpoint != "cursor-700" {
			t.Fatalf("expected checkpoint cursor-700, got %s", found.Checkpoint)
		}
	})

	t.Run("UpdateStatus_Completed", func(t *testing.T) {
		now := time.Now().UTC()
		run, err := ingestion.NewRun("", sourceID, "")
		if err != nil {
			t.Fatalf("failed to initialize run: %v", err)
		}
		_ = run.Start(now)
		if err := repo.CreateRun(ctx, run); err != nil {
			t.Fatalf("failed to create run: %v", err)
		}

		completedAt := now.Add(3 * time.Second)
		if err := repo.UpdateStatus(ctx, run.ID, ingestion.StatusCompleted, "", "cursor-done", completedAt, completedAt); err != nil {
			t.Fatalf("failed to update status to COMPLETED: %v", err)
		}

		completedRun, err := repo.FindRunByID(ctx, run.ID)
		if err != nil {
			t.Fatalf("failed to find completed run: %v", err)
		}
		if completedRun.Status != ingestion.StatusCompleted {
			t.Fatalf("expected status COMPLETED, got %s", completedRun.Status)
		}
		if completedRun.CompletedAt == nil {
			t.Fatalf("expected non-nil completed_at")
		}
	})

	t.Run("ListAndLatestRuns", func(t *testing.T) {
		now := time.Now().UTC()
		runA, _ := ingestion.NewRun("", sourceID, "cursor-A")
		runA.CreatedAt = now.Add(-10 * time.Second)
		_ = runA.Start(runA.CreatedAt)
		_ = repo.CreateRun(ctx, runA)

		runB, _ := ingestion.NewRun("", sourceID, "cursor-B")
		runB.CreatedAt = now
		_ = runB.Start(runB.CreatedAt)
		_ = repo.CreateRun(ctx, runB)

		latest, err := repo.FindLatestRunBySource(ctx, sourceID)
		if err != nil {
			t.Fatalf("failed to find latest run: %v", err)
		}
		if latest.ID != runB.ID {
			t.Fatalf("expected latest run to be runB (%s), got %s", runB.ID, latest.ID)
		}

		runs, err := repo.ListRunsBySource(ctx, sourceID, 10)
		if err != nil {
			t.Fatalf("failed to list runs: %v", err)
		}
		if len(runs) < 2 {
			t.Fatalf("expected at least 2 runs, got %d", len(runs))
		}
		if runs[0].ID != runB.ID {
			t.Errorf("expected runs[0] to be newest runB (%s), got %s", runB.ID, runs[0].ID)
		}
	})

	t.Run("FindRunByID_NotFound", func(t *testing.T) {
		_, err := repo.FindRunByID(ctx, "00000000-0000-0000-0000-000000000000")
		if !errors.Is(err, ingestion.ErrRunNotFound) {
			t.Fatalf("expected ErrRunNotFound for non-existent UUID, got %v", err)
		}
	})

	t.Run("Transaction_Rollback", func(t *testing.T) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("failed to begin tx: %v", err)
		}
		defer func() { _ = tx.Rollback(ctx) }()

		txRepo := repo.WithTx(tx)
		runTx, _ := ingestion.NewRun("", sourceID, "cursor-tx")
		if err := txRepo.CreateRun(ctx, runTx); err != nil {
			t.Fatalf("failed to create run in tx: %v", err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatalf("failed to rollback tx: %v", err)
		}

		_, err = repo.FindRunByID(ctx, runTx.ID)
		if !errors.Is(err, ingestion.ErrRunNotFound) {
			t.Fatalf("expected ErrRunNotFound after tx rollback, got %v", err)
		}
	})
}
