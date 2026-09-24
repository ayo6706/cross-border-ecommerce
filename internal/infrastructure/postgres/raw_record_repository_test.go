package postgres_test

import (
	"bytes"
	"context"
	"encoding/json"
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

func TestRawRecordRepository_ConstructorValidation(t *testing.T) {
	t.Parallel()

	repo, err := postgres.NewRawRecordRepository(nil)
	if repo != nil {
		t.Fatalf("expected nil repo on nil pool, got %v", repo)
	}
	if err == nil {
		t.Fatal("expected error on nil pool, got nil")
	}
}

func setupLiveRawRecordDB(t *testing.T) (*pgxpool.Pool, *postgres.RawRecordRepository, source.ID, string) {
	t.Helper()
	connStr := os.Getenv("TEST_DATABASE_URL")
	if connStr == "" {
		t.Skip("skipping live database test: TEST_DATABASE_URL not set")
		return nil, nil, "", ""
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
		return nil, nil, "", ""
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
		t.Fatalf("failed to run migrations: %v", err)
	}

	srcRepo, err := postgres.NewSourceRepository(pool)
	if err != nil {
		t.Fatalf("failed to create source repo: %v", err)
	}

	sourceID := source.ID(fmt.Sprintf("src-test-raw-%d", time.Now().UnixNano()))
	testSource, err := source.NewSource(
		sourceID,
		"Test Live Source",
		source.TypeAPI,
		map[string]any{"base_url": "https://api.example.com"},
		100,
	)
	if err != nil {
		t.Fatalf("failed to create source entity: %v", err)
	}
	if err := srcRepo.Save(ctx, testSource); err != nil {
		t.Fatalf("failed to seed source: %v", err)
	}

	ingestionRepo, err := postgres.NewIngestionRepository(pool)
	if err != nil {
		t.Fatalf("failed to create ingestion repo: %v", err)
	}

	run, err := ingestion.NewRun("", sourceID, "")
	if err != nil {
		t.Fatalf("failed to create run entity: %v", err)
	}
	if err := ingestionRepo.CreateRun(ctx, run); err != nil {
		t.Fatalf("failed to seed run: %v", err)
	}

	repo, err := postgres.NewRawRecordRepository(pool)
	if err != nil {
		t.Fatalf("failed to create raw record repo: %v", err)
	}

	return pool, repo, sourceID, run.ID
}

func TestRawRecordRepository_LiveIntegration(t *testing.T) {
	pool, repo, testSourceID, runID := setupLiveRawRecordDB(t)
	if pool == nil {
		return
	}
	ctx := context.Background()

	t.Run("Validation_NilRecord", func(t *testing.T) {
		err := repo.Save(ctx, nil)
		if !errors.Is(err, ingestion.ErrInvalidRecordState) {
			t.Fatalf("expected ErrInvalidRecordState, got: %v", err)
		}
	})

	t.Run("Save_And_FindByID", func(t *testing.T) {
		now := time.Now().UTC().Truncate(time.Microsecond)
		payload := []byte(`{"title": "Organic Green Tea", "price": 12.50}`)
		rec, err := ingestion.NewRawRecord(ingestion.RawRecordParams{
			SourceID:          testSourceID,
			ExternalProductID: "SKU-TEA-001",
			Payload:           payload,
			SourceVersion:     "v1.2",
			ETag:              "etag-tea-1",
			SourceUpdatedAt:   &now,
			IngestionRunID:    runID,
			ReceivedAt:        now,
		})
		if err != nil {
			t.Fatalf("failed to create raw record entity: %v", err)
		}

		if err := repo.Save(ctx, rec); err != nil {
			t.Fatalf("failed to save raw record: %v", err)
		}

		found, err := repo.FindByID(ctx, rec.ID)
		if err != nil {
			t.Fatalf("failed to find raw record by id: %v", err)
		}
		if found.ID != rec.ID {
			t.Errorf("expected ID %s, got: %s", rec.ID, found.ID)
		}
		if found.SourceID != testSourceID {
			t.Errorf("expected source %s, got: %s", testSourceID, found.SourceID)
		}
		if found.ExternalProductID != "SKU-TEA-001" {
			t.Errorf("expected SKU-TEA-001, got: %s", found.ExternalProductID)
		}
		if !bytes.Equal(found.PayloadRaw, payload) {
			t.Errorf("expected payload_raw %s, got: %s", string(payload), string(found.PayloadRaw))
		}
		var expectedJSON, actualJSON map[string]any
		if err := json.Unmarshal(payload, &expectedJSON); err != nil {
			t.Fatalf("failed to unmarshal expected payload: %v", err)
		}
		if err := json.Unmarshal(found.Payload, &actualJSON); err != nil {
			t.Fatalf("failed to unmarshal actual payload: %v", err)
		}
		if expectedJSON["title"] != actualJSON["title"] {
			t.Errorf("expected title %v, got %v", expectedJSON["title"], actualJSON["title"])
		}
		if found.SourceVersion != "v1.2" {
			t.Errorf("expected version v1.2, got: %s", found.SourceVersion)
		}
		if found.ETag != "etag-tea-1" {
			t.Errorf("expected ETag etag-tea-1, got: %s", found.ETag)
		}
		if found.SourceUpdatedAt == nil || !found.SourceUpdatedAt.Equal(now) {
			t.Errorf("expected source updated at %v, got: %v", now, found.SourceUpdatedAt)
		}
		if found.IngestionRunID != runID {
			t.Errorf("expected run ID %s, got: %s", runID, found.IngestionRunID)
		}
		if !found.ReceivedAt.Equal(now) {
			t.Errorf("expected received at %v, got: %v", now, found.ReceivedAt)
		}
	})

	t.Run("SaveBatch_CopyFrom", func(t *testing.T) {
		now := time.Now().UTC().Truncate(time.Microsecond)
		records := make([]*ingestion.RawRecord, 0, 5)
		for i := 1; i <= 5; i++ {
			sku := fmt.Sprintf("SKU-BATCH-%03d", i)
			payload := []byte(fmt.Sprintf(`{"sku": "%s", "item_num": %d}`, sku, i))
			rec, err := ingestion.NewRawRecord(ingestion.RawRecordParams{
				SourceID:          testSourceID,
				ExternalProductID: sku,
				Payload:           payload,
				SourceVersion:     "v2.0",
				ETag:              fmt.Sprintf("etag-b-%d", i),
				SourceUpdatedAt:   &now,
				IngestionRunID:    runID,
				ReceivedAt:        now,
			})
			if err != nil {
				t.Fatalf("failed to initialize batch item %d: %v", i, err)
			}
			records = append(records, rec)
		}

		if err := repo.SaveBatch(ctx, records); err != nil {
			t.Fatalf("failed to execute SaveBatch: %v", err)
		}

		for _, rec := range records {
			found, err := repo.FindByID(ctx, rec.ID)
			if err != nil {
				t.Fatalf("failed to retrieve batch record %s: %v", rec.ID, err)
			}
			if found.ExternalProductID != rec.ExternalProductID {
				t.Errorf("expected SKU %s, got: %s", rec.ExternalProductID, found.ExternalProductID)
			}
		}
	})

	t.Run("FindLatestBySourceAndExternalID", func(t *testing.T) {
		sku := "SKU-VERSIONED-01"
		baseTime := time.Now().UTC().Truncate(time.Microsecond)

		recOld, err := ingestion.NewRawRecord(ingestion.RawRecordParams{
			SourceID:          testSourceID,
			ExternalProductID: sku,
			Payload:           []byte(`{"version": 1}`),
			SourceVersion:     "v1",
			ETag:              "etag-1",
			IngestionRunID:    runID,
			ReceivedAt:        baseTime.Add(-1 * time.Hour),
		})
		if err != nil {
			t.Fatalf("create old record: %v", err)
		}
		if err := repo.Save(ctx, recOld); err != nil {
			t.Fatalf("save old record: %v", err)
		}

		recNew, err := ingestion.NewRawRecord(ingestion.RawRecordParams{
			SourceID:          testSourceID,
			ExternalProductID: sku,
			Payload:           []byte(`{"version": 2}`),
			SourceVersion:     "v2",
			ETag:              "etag-2",
			IngestionRunID:    runID,
			ReceivedAt:        baseTime,
		})
		if err != nil {
			t.Fatalf("create new record: %v", err)
		}
		if err := repo.Save(ctx, recNew); err != nil {
			t.Fatalf("save new record: %v", err)
		}

		latest, err := repo.FindLatestBySourceAndExternalID(ctx, testSourceID, sku)
		if err != nil {
			t.Fatalf("find latest record: %v", err)
		}
		if latest.ID != recNew.ID {
			t.Fatalf("expected latest ID %s, got: %s", recNew.ID, latest.ID)
		}
		if latest.SourceVersion != "v2" {
			t.Errorf("expected version v2, got: %s", latest.SourceVersion)
		}
	})

	t.Run("ListBySourceAndExternalID", func(t *testing.T) {
		sku := "SKU-VERSIONED-01"
		list, err := repo.ListBySourceAndExternalID(ctx, testSourceID, sku, 10)
		if err != nil {
			t.Fatalf("list by source and external ID: %v", err)
		}
		if len(list) < 2 {
			t.Fatalf("expected at least 2 versions, got %d", len(list))
		}
		if list[0].SourceVersion != "v2" {
			t.Errorf("expected first item to be newest v2, got: %s", list[0].SourceVersion)
		}
	})

	t.Run("ListByRunID", func(t *testing.T) {
		list, err := repo.ListByRunID(ctx, runID, 20)
		if err != nil {
			t.Fatalf("list by run ID: %v", err)
		}
		if len(list) == 0 {
			t.Fatalf("expected non-empty list for run %s", runID)
		}
	})

	t.Run("FindByID_NotFound", func(t *testing.T) {
		found, err := repo.FindByID(ctx, "00000000-0000-0000-0000-000000000000")
		if !errors.Is(err, ingestion.ErrRecordNotFound) {
			t.Fatalf("expected ErrRecordNotFound, got: %v", err)
		}
		if found != nil {
			t.Fatalf("expected nil record, got: %v", found)
		}
	})

	t.Run("Transaction_Rollback", func(t *testing.T) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("failed to start tx: %v", err)
		}
		defer func() { _ = tx.Rollback(ctx) }()

		txRepo := repo.WithTx(tx)
		rec, err := ingestion.NewRawRecord(ingestion.RawRecordParams{
			SourceID:          testSourceID,
			ExternalProductID: "SKU-ROLLBACK",
			Payload:           []byte(`{"temp": true}`),
			IngestionRunID:    runID,
			ReceivedAt:        time.Now(),
		})
		if err != nil {
			t.Fatalf("create record: %v", err)
		}

		if err := txRepo.Save(ctx, rec); err != nil {
			t.Fatalf("save in tx: %v", err)
		}

		if err := tx.Rollback(ctx); err != nil {
			t.Fatalf("rollback: %v", err)
		}

		_, err = repo.FindByID(ctx, rec.ID)
		if !errors.Is(err, ingestion.ErrRecordNotFound) {
			t.Fatalf("expected ErrRecordNotFound after rollback, got: %v", err)
		}
	})
}
