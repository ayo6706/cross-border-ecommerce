package postgres_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	appIdempotency "github.com/ayo6706/cross-border-ecommerce/internal/application/idempotency"
	"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/postgres"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/uuid"
	"github.com/ayo6706/cross-border-ecommerce/migrations"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIdempotencyRepository_ConstructorValidation(t *testing.T) {
	t.Parallel()

	repo, err := postgres.NewIdempotencyRepository(nil)
	assert.Error(t, err)
	assert.Nil(t, repo)
}

func setupLiveIdempotencyDB(t *testing.T) (*pgxpool.Pool, *postgres.IdempotencyRepository) {
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
		_, _ = pool.Exec(cleanCtx, "TRUNCATE idempotency_keys CASCADE")
		pool.Close()
	})

	migrator, err := postgres.NewMigrator(pool, migrations.FS)
	if err != nil {
		t.Fatalf("failed to initialize migrator: %v", err)
	}
	if err := migrator.Up(ctx); err != nil {
		t.Fatalf("failed to run migrations: %v", err)
	}

	repo, err := postgres.NewIdempotencyRepository(pool)
	if err != nil {
		t.Fatalf("failed to initialize idempotency repository: %v", err)
	}

	return pool, repo
}

func TestIdempotency_Live(t *testing.T) {
	pool, repo := setupLiveIdempotencyDB(t)
	ctx := context.Background()

	t.Run("first_delivery_completes", func(t *testing.T) {
		scope := "consumer-group-1"
		eventID := "evt-" + mustUUID(t)
		hash := sha256.Sum256([]byte(`{"item":"test"}`))
		token := mustUUID(t)

		// 1. Claim new key
		claim, err := repo.ClaimKey(ctx, scope, eventID, hash[:], token, 10*time.Second)
		require.NoError(t, err)
		assert.Equal(t, appIdempotency.ClaimAcquired, claim.Status)
		assert.Equal(t, scope, claim.Scope)
		assert.Equal(t, eventID, claim.Key)
		assert.Equal(t, token, claim.LeaseToken)
		assert.Equal(t, 1, claim.Attempts)

		// 2. Complete key
		err = repo.CompleteKey(ctx, scope, eventID, token)
		require.NoError(t, err)

		// 3. Inspect record
		rec, err := repo.GetKey(ctx, scope, eventID)
		require.NoError(t, err)
		assert.Equal(t, "COMPLETED", rec.Status)
		assert.NotNil(t, rec.CompletedAt)
		assert.Nil(t, rec.LeaseToken)
		assert.Nil(t, rec.LeaseExpiresAt)
		assert.Equal(t, 1, rec.Attempts)
	})

	t.Run("redelivery_skipped", func(t *testing.T) {
		scope := "consumer-group-1"
		eventID := "evt-" + mustUUID(t)
		hash := sha256.Sum256([]byte(`{"item":"redelivery"}`))
		token1 := mustUUID(t)

		// Claim and complete
		claim1, err := repo.ClaimKey(ctx, scope, eventID, hash[:], token1, 10*time.Second)
		require.NoError(t, err)
		require.Equal(t, appIdempotency.ClaimAcquired, claim1.Status)

		err = repo.CompleteKey(ctx, scope, eventID, token1)
		require.NoError(t, err)

		// Redelivery attempt with new token
		token2 := mustUUID(t)
		claim2, err := repo.ClaimKey(ctx, scope, eventID, hash[:], token2, 10*time.Second)
		require.NoError(t, err)
		assert.Equal(t, appIdempotency.ClaimAlreadyCompleted, claim2.Status)
		assert.Equal(t, scope, claim2.Scope)
		assert.Equal(t, eventID, claim2.Key)
	})

	t.Run("fingerprint_revert_not_skipped", func(t *testing.T) {
		scope := "consumer-group-1"
		// A -> B -> A scenario with distinct event IDs
		eventA1 := "evt-A1-" + mustUUID(t)
		eventB := "evt-B-" + mustUUID(t)
		eventA2 := "evt-A2-" + mustUUID(t)

		hashA := sha256.Sum256([]byte(`{"fingerprint":"fp-A"}`))
		hashB := sha256.Sum256([]byte(`{"fingerprint":"fp-B"}`))

		// Deliver A1
		claimA1, err := repo.ClaimKey(ctx, scope, eventA1, hashA[:], mustUUID(t), 10*time.Second)
		require.NoError(t, err)
		assert.Equal(t, appIdempotency.ClaimAcquired, claimA1.Status)
		require.NoError(t, repo.CompleteKey(ctx, scope, eventA1, claimA1.LeaseToken))

		// Deliver B
		claimB, err := repo.ClaimKey(ctx, scope, eventB, hashB[:], mustUUID(t), 10*time.Second)
		require.NoError(t, err)
		assert.Equal(t, appIdempotency.ClaimAcquired, claimB.Status)
		require.NoError(t, repo.CompleteKey(ctx, scope, eventB, claimB.LeaseToken))

		// Deliver A2 (revert to same fingerprint payload, but new event_id)
		claimA2, err := repo.ClaimKey(ctx, scope, eventA2, hashA[:], mustUUID(t), 10*time.Second)
		require.NoError(t, err)
		assert.Equal(t, appIdempotency.ClaimAcquired, claimA2.Status, "reverted fingerprint with new event_id must not be skipped")
		require.NoError(t, repo.CompleteKey(ctx, scope, eventA2, claimA2.LeaseToken))
	})

	t.Run("concurrent_claim_single_winner", func(t *testing.T) {
		scope := "consumer-group-concurrent"
		eventID := "evt-concurrent-" + mustUUID(t)
		hash := sha256.Sum256([]byte(`{"concurrent":true}`))

		const numWorkers = 10
		var acquiredCount int32
		var inProgressCount int32

		var wg sync.WaitGroup
		wg.Add(numWorkers)

		startSignal := make(chan struct{})

		for i := 0; i < numWorkers; i++ {
			go func() {
				defer wg.Done()
				token := mustUUID(t)
				<-startSignal

				claim, err := repo.ClaimKey(ctx, scope, eventID, hash[:], token, 10*time.Second)
				if err == nil && claim.Status == appIdempotency.ClaimAcquired {
					atomic.AddInt32(&acquiredCount, 1)
				} else if errors.Is(err, appIdempotency.ErrKeyInProgress) {
					atomic.AddInt32(&inProgressCount, 1)
				}
			}()
		}

		close(startSignal)
		wg.Wait()

		assert.Equal(t, int32(1), atomic.LoadInt32(&acquiredCount), "exactly one worker must acquire the lease")
		assert.Equal(t, int32(numWorkers-1), atomic.LoadInt32(&inProgressCount), "all other workers must receive ErrKeyInProgress")
	})

	t.Run("expired_lease_reclaimable", func(t *testing.T) {
		scope := "consumer-group-1"
		eventID := "evt-expire-" + mustUUID(t)
		hash := sha256.Sum256([]byte(`{"expire":true}`))
		token1 := mustUUID(t)

		// Short TTL: 50ms
		claim1, err := repo.ClaimKey(ctx, scope, eventID, hash[:], token1, 50*time.Millisecond)
		require.NoError(t, err)
		assert.Equal(t, appIdempotency.ClaimAcquired, claim1.Status)
		assert.Equal(t, 1, claim1.Attempts)

		// Wait for lease to expire
		time.Sleep(80 * time.Millisecond)

		// Reclaim by Worker 2
		token2 := mustUUID(t)
		claim2, err := repo.ClaimKey(ctx, scope, eventID, hash[:], token2, 10*time.Second)
		require.NoError(t, err)
		assert.Equal(t, appIdempotency.ClaimAcquired, claim2.Status)
		assert.Equal(t, token2, claim2.LeaseToken)
		assert.Equal(t, 2, claim2.Attempts, "attempts count must increment on reclaim")

		require.NoError(t, repo.CompleteKey(ctx, scope, eventID, token2))
	})

	t.Run("crash_after_commit_before_ack", func(t *testing.T) {
		scope := "consumer-group-1"
		eventID := "evt-tx-" + mustUUID(t)
		hash := sha256.Sum256([]byte(`{"tx":true}`))
		token := mustUUID(t)

		claim, err := repo.ClaimKey(ctx, scope, eventID, hash[:], token, 10*time.Second)
		require.NoError(t, err)
		assert.Equal(t, appIdempotency.ClaimAcquired, claim.Status)

		// Simulate atomic database transaction with business writes and CompleteKey
		tx, err := pool.Begin(ctx)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback(ctx) }()

		txRepo := repo.WithTx(tx)
		err = txRepo.CompleteKey(ctx, scope, eventID, token)
		require.NoError(t, err)

		// Commit transaction
		err = tx.Commit(ctx)
		require.NoError(t, err)

		// Simulate worker crash before ACK: message redelivered from stream
		tokenRedeliver := mustUUID(t)
		claimRedeliver, err := repo.ClaimKey(ctx, scope, eventID, hash[:], tokenRedeliver, 10*time.Second)
		require.NoError(t, err)
		assert.Equal(t, appIdempotency.ClaimAlreadyCompleted, claimRedeliver.Status)
	})

	t.Run("stale_holder_cannot_complete", func(t *testing.T) {
		scope := "consumer-group-1"
		eventID := "evt-stale-" + mustUUID(t)
		hash := sha256.Sum256([]byte(`{"stale":true}`))

		// Worker A claims with 50ms TTL
		tokenA := mustUUID(t)
		claimA, err := repo.ClaimKey(ctx, scope, eventID, hash[:], tokenA, 50*time.Millisecond)
		require.NoError(t, err)
		assert.Equal(t, 1, claimA.Attempts)

		// Wait for Worker A lease to expire
		time.Sleep(80 * time.Millisecond)

		// Worker B claims the expired lease
		tokenB := mustUUID(t)
		claimB, err := repo.ClaimKey(ctx, scope, eventID, hash[:], tokenB, 10*time.Second)
		require.NoError(t, err)
		assert.Equal(t, 2, claimB.Attempts)

		// Worker A tries to complete in a transaction
		txA, err := pool.Begin(ctx)
		require.NoError(t, err)
		defer func() { _ = txA.Rollback(ctx) }()

		txRepoA := repo.WithTx(txA)
		err = txRepoA.CompleteKey(ctx, scope, eventID, tokenA)
		assert.ErrorIs(t, err, appIdempotency.ErrLeaseLost, "Worker A must fail with ErrLeaseLost")
		_ = txA.Rollback(ctx)

		// Worker B completes successfully
		err = repo.CompleteKey(ctx, scope, eventID, tokenB)
		require.NoError(t, err)
	})

	t.Run("payload_mismatch_fails_loudly", func(t *testing.T) {
		scope := "consumer-group-1"
		eventID := "evt-mismatch-" + mustUUID(t)
		hashOriginal := sha256.Sum256([]byte(`{"original":true}`))
		hashTampered := sha256.Sum256([]byte(`{"tampered":true}`))

		// Claim key with original payload
		token1 := mustUUID(t)
		claim, err := repo.ClaimKey(ctx, scope, eventID, hashOriginal[:], token1, 10*time.Second)
		require.NoError(t, err)
		assert.Equal(t, appIdempotency.ClaimAcquired, claim.Status)

		// Attempt to claim same key with tampered payload
		token2 := mustUUID(t)
		_, err = repo.ClaimKey(ctx, scope, eventID, hashTampered[:], token2, 10*time.Second)
		assert.ErrorIs(t, err, appIdempotency.ErrPayloadMismatch, "differing payload hash must return ErrPayloadMismatch")

		// Complete original key
		require.NoError(t, repo.CompleteKey(ctx, scope, eventID, token1))

		// Attempt to claim completed key with tampered payload
		_, err = repo.ClaimKey(ctx, scope, eventID, hashTampered[:], token2, 10*time.Second)
		assert.ErrorIs(t, err, appIdempotency.ErrPayloadMismatch, "completed key must also reject differing payload hash")
	})

	t.Run("handler_error_releases", func(t *testing.T) {
		scope := "consumer-group-1"
		eventID := "evt-release-" + mustUUID(t)
		hash := sha256.Sum256([]byte(`{"release":true}`))

		// Claim with long TTL: 60s
		token1 := mustUUID(t)
		claim1, err := repo.ClaimKey(ctx, scope, eventID, hash[:], token1, 60*time.Second)
		require.NoError(t, err)
		assert.Equal(t, 1, claim1.Attempts)

		// Release key immediately (simulating handler error)
		err = repo.ReleaseKey(ctx, scope, eventID, token1)
		require.NoError(t, err)

		// Next delivery claims immediately without waiting 60s
		token2 := mustUUID(t)
		claim2, err := repo.ClaimKey(ctx, scope, eventID, hash[:], token2, 10*time.Second)
		require.NoError(t, err)
		assert.Equal(t, appIdempotency.ClaimAcquired, claim2.Status)
		assert.Equal(t, 2, claim2.Attempts)

		require.NoError(t, repo.CompleteKey(ctx, scope, eventID, token2))
	})

	t.Run("database_integrity_constraints", func(t *testing.T) {
		scope := "consumer-group-constraints"

		// 1. Cannot insert COMPLETED with completed_at NULL
		_, err := pool.Exec(ctx, `
			INSERT INTO idempotency_keys (scope, key, status, payload_hash, attempts, created_at, completed_at)
			VALUES ($1, $2, 'COMPLETED', $3, 1, NOW(), NULL)
		`, scope, "key-bad-1", make([]byte, 32))
		var pgErr *pgconn.PgError
		require.ErrorAs(t, err, &pgErr)
		assert.Equal(t, "23514", pgErr.Code) // check_violation

		// 2. Cannot insert IN_PROGRESS with lease_token NULL
		_, err = pool.Exec(ctx, `
			INSERT INTO idempotency_keys (scope, key, status, payload_hash, lease_token, lease_expires_at, attempts, created_at)
			VALUES ($1, $2, 'IN_PROGRESS', $3, NULL, NOW() + interval '10s', 1, NOW())
		`, scope, "key-bad-2", make([]byte, 32))
		require.ErrorAs(t, err, &pgErr)
		assert.Equal(t, "23514", pgErr.Code)

		// 3. Cannot insert payload_hash of wrong length (<> 32 bytes)
		_, err = pool.Exec(ctx, `
			INSERT INTO idempotency_keys (scope, key, status, payload_hash, lease_token, lease_expires_at, attempts, created_at)
			VALUES ($1, $2, 'IN_PROGRESS', $3, gen_random_uuid(), NOW() + interval '10s', 1, NOW())
		`, scope, "key-bad-3", []byte("too-short"))
		require.ErrorAs(t, err, &pgErr)
		assert.Equal(t, "23514", pgErr.Code)

		// 4. Cannot insert attempts < 1
		_, err = pool.Exec(ctx, `
			INSERT INTO idempotency_keys (scope, key, status, payload_hash, lease_token, lease_expires_at, attempts, created_at)
			VALUES ($1, $2, 'IN_PROGRESS', $3, gen_random_uuid(), NOW() + interval '10s', 0, NOW())
		`, scope, "key-bad-4", make([]byte, 32))
		require.ErrorAs(t, err, &pgErr)
		assert.Equal(t, "23514", pgErr.Code)
	})

	t.Run("get_key_not_found_returns_ErrKeyNotFound", func(t *testing.T) {
		_, err := repo.GetKey(ctx, "nonexistent-scope", "nonexistent-key")
		require.ErrorIs(t, err, appIdempotency.ErrKeyNotFound)
	})

	t.Run("db_clock_lease_expiry", func(t *testing.T) {
		scope := "consumer-group-db-clock"
		eventID := "evt-" + mustUUID(t)
		hash := sha256.Sum256([]byte("db-clock-test"))
		token := mustUUID(t)

		var dbNow time.Time
		err := pool.QueryRow(ctx, "SELECT NOW()").Scan(&dbNow)
		require.NoError(t, err)

		claim, err := repo.ClaimKey(ctx, scope, eventID, hash[:], token, 15*time.Second)
		require.NoError(t, err)

		// Assert expiration is computed from DB clock (within 1 second of dbNow + 15s)
		expectedExp := dbNow.Add(15 * time.Second)
		diff := claim.ExpiresAt.Sub(expectedExp)
		if diff < 0 {
			diff = -diff
		}
		assert.Less(t, diff, 2*time.Second, "lease expiration must be computed from PostgreSQL clock")
	})
}

func mustUUID(t *testing.T) string {
	t.Helper()
	s, err := uuid.NewString()
	require.NoError(t, err)
	return s
}
