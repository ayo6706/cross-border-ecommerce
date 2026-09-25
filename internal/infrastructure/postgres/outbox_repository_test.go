package postgres_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/postgres"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/uuid"
	"github.com/ayo6706/cross-border-ecommerce/migrations"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOutboxRepository_ConstructorValidation(t *testing.T) {
	t.Parallel()

	repo, err := postgres.NewOutboxRepository(nil)
	assert.Error(t, err)
	assert.Nil(t, repo)
}

func setupLiveOutboxDB(t *testing.T) (*pgxpool.Pool, *postgres.OutboxRepository) {
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
		postgres.WithMaxConns(20),
		postgres.WithMinConns(2),
	)
	if err != nil {
		t.Skipf("skipping live database test: unable to connect to %s: %v", connStr, err)
		return nil, nil
	}
	t.Cleanup(func() {
		cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanCancel()
		_, _ = pool.Exec(cleanCtx, "TRUNCATE outbox_events CASCADE")
		pool.Close()
	})

	migrator, err := postgres.NewMigrator(pool, migrations.FS)
	if err != nil {
		t.Fatalf("failed to initialize migrator: %v", err)
	}
	if err := migrator.Up(ctx); err != nil {
		t.Fatalf("migration failed: %v", err)
	}

	repo, err := postgres.NewOutboxRepository(pool)
	if err != nil {
		t.Fatalf("failed to create outbox repository: %v", err)
	}

	return pool, repo
}

func TestOutboxStore_Live(t *testing.T) {
	pool, repo := setupLiveOutboxDB(t)
	if pool == nil || repo == nil {
		return
	}

	ctx := context.Background()

	// I1: Claim, mark and release SQL; a stale token affects 0 rows
	t.Run("claim_guard", func(t *testing.T) {
		_, err := pool.Exec(ctx, "TRUNCATE outbox_events CASCADE")
		require.NoError(t, err)

		err = repo.CreateEvent(ctx, "product", "prod-1", "product.changed", []byte(`{"v":1}`))
		require.NoError(t, err)
		err = repo.CreateEvent(ctx, "product", "prod-2", "product.changed", []byte(`{"v":2}`))
		require.NoError(t, err)

		tokenA, err := uuid.NewString()
		require.NoError(t, err)

		events, err := repo.ClaimBatch(ctx, tokenA, 10, 10*time.Second)
		require.NoError(t, err)
		require.Len(t, events, 2)

		staleToken, err := uuid.NewString()
		require.NoError(t, err)

		// Stale token mark affects 0 rows
		affected, err := repo.MarkPublished(ctx, staleToken, []string{events[0].ID, events[1].ID})
		require.NoError(t, err)
		assert.Equal(t, int64(0), affected)

		// Valid token mark affects 2 rows
		affected, err = repo.MarkPublished(ctx, tokenA, []string{events[0].ID, events[1].ID})
		require.NoError(t, err)
		assert.Equal(t, int64(2), affected)

		// Release with tokenA now affects 0 rows (since already PROCESSED)
		err = repo.Release(ctx, tokenA, []string{events[0].ID, events[1].ID})
		require.NoError(t, err)
	})

	// I2: 4 goroutines claiming 500 rows concurrently: disjoint sets that together cover all rows
	t.Run("skip_locked_concurrency", func(t *testing.T) {
		_, err := pool.Exec(ctx, "TRUNCATE outbox_events CASCADE")
		require.NoError(t, err)

		totalRows := 500
		for i := 0; i < totalRows; i++ {
			err = repo.CreateEvent(ctx, "product", fmt.Sprintf("prod-%d", i), "product.changed", []byte(`{"v":1}`))
			require.NoError(t, err)
		}

		concurrency := 4
		var wg sync.WaitGroup
		mu := sync.Mutex{}
		claimedByWorker := make([][]string, concurrency)

		for w := 0; w < concurrency; w++ {
			w := w
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					token, tErr := uuid.NewString()
					if tErr != nil {
						return
					}
					batch, cErr := repo.ClaimBatch(ctx, token, 25, 30*time.Second)
					if cErr != nil || len(batch) == 0 {
						break
					}
					ids := make([]string, len(batch))
					for i, e := range batch {
						ids[i] = e.ID
					}
					mu.Lock()
					claimedByWorker[w] = append(claimedByWorker[w], ids...)
					mu.Unlock()
				}
			}()
		}

		wg.Wait()

		allClaimed := make(map[string]bool)
		for w := 0; w < concurrency; w++ {
			for _, id := range claimedByWorker[w] {
				assert.False(t, allClaimed[id], "ID %s was claimed by multiple workers (contention bug)", id)
				allClaimed[id] = true
			}
		}

		assert.Len(t, allClaimed, totalRows, "Total claimed rows should equal 500")
	})

	// I3: Expired claim can be reclaimed; old token's mark updates 0 rows
	t.Run("lease_expiry", func(t *testing.T) {
		_, err := pool.Exec(ctx, "TRUNCATE outbox_events CASCADE")
		require.NoError(t, err)

		err = repo.CreateEvent(ctx, "product", "prod-lease", "product.changed", []byte(`{"v":1}`))
		require.NoError(t, err)

		tokenA, err := uuid.NewString()
		require.NoError(t, err)

		events, err := repo.ClaimBatch(ctx, tokenA, 10, 100*time.Millisecond)
		require.NoError(t, err)
		require.Len(t, events, 1)

		// Wait for lease to expire
		time.Sleep(250 * time.Millisecond)

		tokenB, err := uuid.NewString()
		require.NoError(t, err)

		// Reclaimed by tokenB
		eventsB, err := repo.ClaimBatch(ctx, tokenB, 10, 30*time.Second)
		require.NoError(t, err)
		require.Len(t, eventsB, 1)
		assert.Equal(t, events[0].ID, eventsB[0].ID)

		// Old tokenA mark updates 0 rows
		affected, err := repo.MarkPublished(ctx, tokenA, []string{events[0].ID})
		require.NoError(t, err)
		assert.Equal(t, int64(0), affected)

		// TokenB mark updates 1 row
		affected, err = repo.MarkPublished(ctx, tokenB, []string{eventsB[0].ID})
		require.NoError(t, err)
		assert.Equal(t, int64(1), affected)
	})

	// I4: Max attempts reached: FAILED and never claimed again; bad status is rejected by CHECK (SQLSTATE 23514)
	t.Run("poison_terminal", func(t *testing.T) {
		_, err := pool.Exec(ctx, "TRUNCATE outbox_events CASCADE")
		require.NoError(t, err)

		err = repo.CreateEvent(ctx, "product", "prod-poison", "product.changed", []byte(`{"v":1}`))
		require.NoError(t, err)

		maxAttempts := 3

		for attempt := 1; attempt <= maxAttempts; attempt++ {
			token, tErr := uuid.NewString()
			require.NoError(t, tErr)

			events, cErr := repo.ClaimBatch(ctx, token, 10, 30*time.Second)
			require.NoError(t, cErr)
			require.Len(t, events, 1, "Attempt %d should claim event", attempt)

			rErr := repo.RecordFailure(ctx, token, events[0].ID, "simulated poison payload", maxAttempts, 10*time.Millisecond)
			require.NoError(t, rErr)

			// Wait backoff
			time.Sleep(20 * time.Millisecond)
		}

		// After maxAttempts, event is FAILED and cannot be claimed
		tokenAfter, err := uuid.NewString()
		require.NoError(t, err)
		eventsAfter, err := repo.ClaimBatch(ctx, tokenAfter, 10, 30*time.Second)
		require.NoError(t, err)
		assert.Empty(t, eventsAfter, "FAILED event should never be claimed again")

		// Verify CHECK constraint rejects bad status with SQLSTATE 23514
		badID, err := uuid.NewString()
		require.NoError(t, err)
		_, err = pool.Exec(ctx, "INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, payload, status) VALUES ($1, 'product', '1', 'product.changed', '{}', 'INVALID_STATUS')", badID)
		require.Error(t, err)
		var pgErr *pgconn.PgError
		require.ErrorAs(t, err, &pgErr)
		assert.Equal(t, "23514", pgErr.Code, "Expected check constraint violation code 23514")
	})
}
