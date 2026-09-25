package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	appMessaging "github.com/ayo6706/cross-border-ecommerce/internal/application/messaging"
	appOutbox "github.com/ayo6706/cross-border-ecommerce/internal/application/outbox"
	appProduct "github.com/ayo6706/cross-border-ecommerce/internal/application/product"
	domainIngestion "github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
	domainProduct "github.com/ayo6706/cross-border-ecommerce/internal/domain/product"
	domainSource "github.com/ayo6706/cross-border-ecommerce/internal/domain/source"
	"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/postgres"
	infraRedis "github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/redis"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/uuid"
	"github.com/ayo6706/cross-border-ecommerce/migrations"
	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

type failMarkStore struct {
	postgres.OutboxRepository
}

func (f *failMarkStore) MarkPublished(ctx context.Context, claimToken string, ids []string) (int64, error) {
	return 0, errors.New("simulated database crash before mark published")
}

func setupLiveRelayEnv(t *testing.T) (*pgxpool.Pool, *postgres.OutboxRepository, *goredis.Client, *infraRedis.Publisher) {
	t.Helper()

	pgURL := os.Getenv("TEST_DATABASE_URL")
	if pgURL == "" {
		t.Skip("skipping live relay test: TEST_DATABASE_URL not set")
		return nil, nil, nil, nil
	}
	redisURL := os.Getenv("TEST_REDIS_URL")
	if redisURL == "" {
		t.Skip("skipping live relay test: TEST_REDIS_URL not set")
		return nil, nil, nil, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := postgres.NewPool(ctx, pgURL,
		postgres.WithConnectTimeout(3*time.Second),
		postgres.WithMaxConns(20),
		postgres.WithMinConns(2),
	)
	if err != nil {
		t.Skipf("skipping live relay test: unable to connect to Postgres %s: %v", pgURL, err)
		return nil, nil, nil, nil
	}

	opt, err := goredis.ParseURL(redisURL)
	if err != nil {
		pool.Close()
		t.Skipf("skipping live relay test: invalid TEST_REDIS_URL %s: %v", redisURL, err)
		return nil, nil, nil, nil
	}
	rClient := goredis.NewClient(opt)
	if err := rClient.Ping(ctx).Err(); err != nil {
		pool.Close()
		_ = rClient.Close()
		t.Skipf("skipping live relay test: unable to ping Redis %s: %v", redisURL, err)
		return nil, nil, nil, nil
	}

	t.Cleanup(func() {
		cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanCancel()
		_, _ = pool.Exec(cleanCtx, "TRUNCATE products, product_versions, product_sources, product_changes, raw_records, ingestion_runs, ingestion_run_processing, sources, outbox_events CASCADE")
		_ = rClient.FlushDB(cleanCtx).Err()
		_ = rClient.Close()
		pool.Close()
	})

	migrator, err := postgres.NewMigrator(pool, migrations.FS)
	if err != nil {
		t.Fatalf("failed to initialize migrator: %v", err)
	}
	if err := migrator.Up(ctx); err != nil {
		t.Fatalf("migration failed: %v", err)
	}

	outboxRepo, err := postgres.NewOutboxRepository(pool)
	if err != nil {
		t.Fatalf("failed to create outbox repository: %v", err)
	}
	pub := infraRedis.NewPublisherFromClient(rClient)

	return pool, outboxRepo, rClient, pub
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestOutboxRelay_Live(t *testing.T) {
	pool, outboxRepo, rClient, pub := setupLiveRelayEnv(t)
	if pool == nil || outboxRepo == nil {
		return
	}

	ctx := context.Background()

	// E1: ProcessRun -> outbox row -> RunOnce -> Redis XRANGE product.changed shows event_id = outbox id; row PROCESSED
	t.Run("end_to_end", func(t *testing.T) {
		_, err := pool.Exec(ctx, "TRUNCATE products, product_versions, product_sources, product_changes, raw_records, ingestion_runs, ingestion_run_processing, sources, outbox_events CASCADE")
		require.NoError(t, err)
		_ = rClient.FlushDB(ctx).Err()

		sourceRepo, err := postgres.NewSourceRepository(pool)
		require.NoError(t, err)
		src, err := domainSource.NewSource(
			domainSource.ID("src-e1"),
			"Supplier E1",
			domainSource.TypeAPI,
			map[string]any{
				"base_url": "https://api.supplier.com/v1",
				"field_mapping": map[string]any{
					"name_path":           "title",
					"description_path":    "body",
					"brand_path":          "vendor",
					"origin_country_path": "country_code",
					"attribute_paths": map[string]any{
						"color": "details.color",
					},
				},
			},
			100,
		)
		require.NoError(t, err)
		err = sourceRepo.Save(ctx, src)
		require.NoError(t, err)

		runRepo, err := postgres.NewIngestionRepository(pool)
		require.NoError(t, err)
		run, err := domainIngestion.NewRun("", domainSource.ID("src-e1"), "")
		require.NoError(t, err)
		err = runRepo.CreateRun(ctx, run)
		require.NoError(t, err)
		now := time.Now().UTC()
		require.NoError(t, run.Start(now))
		require.NoError(t, runRepo.UpdateStatus(ctx, run, domainIngestion.StatusPending))
		require.NoError(t, run.Complete("", now))
		require.NoError(t, runRepo.UpdateStatus(ctx, run, domainIngestion.StatusRunning))

		rawRepo, err := postgres.NewRawRecordRepository(pool)
		require.NoError(t, err)
		rawRecord, err := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
			SourceID:          domainSource.ID("src-e1"),
			ExternalProductID: "SKU-E1",
			Payload:           []byte(`{"title":"Product E1","body":"Desc E1","vendor":"Brand E1","country_code":"US","details":{"color":"blue"}}`),
			SourceUpdatedAt:   &now,
			IngestionRunID:    run.ID,
			ReceivedAt:        now,
		})
		require.NoError(t, err)
		err = rawRepo.SaveBatch(ctx, []*domainIngestion.RawRecord{rawRecord})
		require.NoError(t, err)

		procRepo, err := postgres.NewRunProcessingRepository(pool)
		require.NoError(t, err)
		err = procRepo.SeedPending(ctx)
		require.NoError(t, err)

		txRunner, err := postgres.NewProductTxManager(pool)
		require.NoError(t, err)

		processor, err := appProduct.NewRunProcessor(runRepo, sourceRepo, rawRepo, procRepo, txRunner)
		require.NoError(t, err)

		claimToken, err := uuid.NewString()
		require.NoError(t, err)
		claimed, err := procRepo.ClaimNext(ctx, claimToken, 30*time.Second)
		require.NoError(t, err)
		require.NotNil(t, claimed)

		result, err := processor.ProcessRun(ctx, claimed.RunID, appProduct.ProcessRunOptions{
			ClaimToken:    claimToken,
			LeaseDuration: 30 * time.Second,
			BatchSize:     10,
		})
		require.NoError(t, err)
		assert.Equal(t, 1, result.RecordsNew)

		// Check outbox row created in DB
		var eventID, status string
		err = pool.QueryRow(ctx, "SELECT id, status FROM outbox_events WHERE event_type = $1", domainProduct.EventTypeProductChanged).Scan(&eventID, &status)
		require.NoError(t, err)
		assert.Equal(t, "PENDING", status)

		// Run relay RunOnce
		cfg := appOutbox.RelayConfig{
			BatchSize:    10,
			PollInterval: 100 * time.Millisecond,
			Lease:        30 * time.Second,
			BaseBackoff:  1 * time.Second,
			MaxBackoff:   5 * time.Minute,
			MaxAttempts:  5,
		}
		relay, err := appOutbox.NewRelay(outboxRepo, pub, cfg, testLogger())
		require.NoError(t, err)

		claimedCount, published, err := relay.RunOnce(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, claimedCount)
		assert.Equal(t, 1, published)

		// Check outbox row is now PROCESSED
		var processedStatus string
		var processedAt *time.Time
		err = pool.QueryRow(ctx, "SELECT status, processed_at FROM outbox_events WHERE id = $1", eventID).Scan(&processedStatus, &processedAt)
		require.NoError(t, err)
		assert.Equal(t, "PROCESSED", processedStatus)
		assert.NotNil(t, processedAt)

		// Check Redis Stream product.changed
		entries, err := rClient.XRange(ctx, domainProduct.EventTypeProductChanged, "-", "+").Result()
		require.NoError(t, err)
		require.Len(t, entries, 1)
		assert.Equal(t, eventID, entries[0].Values["event_id"])
		assert.Equal(t, domainProduct.AggregateTypeProduct, entries[0].Values["aggregate_type"])
		assert.Equal(t, domainProduct.EventTypeProductChanged, entries[0].Values["event_type"])
	})

	// E2: Redis unreachable (127.0.0.1:1): rows PENDING, retry_count 0, claims released
	t.Run("broker_down", func(t *testing.T) {
		_, err := pool.Exec(ctx, "TRUNCATE outbox_events CASCADE")
		require.NoError(t, err)

		err = outboxRepo.CreateEvent(ctx, "product", "prod-bdown", "product.changed", []byte(`{"v":1}`))
		require.NoError(t, err)

		deadClient := goredis.NewClient(&goredis.Options{
			Addr:        "127.0.0.1:54321",
			DialTimeout: 100 * time.Millisecond,
			ReadTimeout: 100 * time.Millisecond,
		})
		defer deadClient.Close()
		deadPub := infraRedis.NewPublisherFromClient(deadClient)

		cfg := appOutbox.RelayConfig{
			BatchSize:    10,
			PollInterval: 100 * time.Millisecond,
			Lease:        30 * time.Second,
			BaseBackoff:  1 * time.Second,
			MaxBackoff:   5 * time.Minute,
			MaxAttempts:  5,
		}
		relay, err := appOutbox.NewRelay(outboxRepo, deadPub, cfg, testLogger())
		require.NoError(t, err)

		claimedCount, published, err := relay.RunOnce(ctx)
		assert.Equal(t, 1, claimedCount)
		assert.Equal(t, 0, published)
		require.Error(t, err)
		assert.ErrorIs(t, err, appMessaging.ErrBrokerUnavailable)

		// Check row status: still PENDING, retry_count is 0, claim_token is NULL
		var status string
		var retryCount int
		var claimToken *string
		err = pool.QueryRow(ctx, "SELECT status, retry_count, claim_token FROM outbox_events WHERE aggregate_id = 'prod-bdown'").Scan(&status, &retryCount, &claimToken)
		require.NoError(t, err)
		assert.Equal(t, "PENDING", status)
		assert.Equal(t, 0, retryCount, "Broker outage must not increment retry_count")
		assert.Nil(t, claimToken, "Claim token must be released")
	})

	// E3: Publish succeeds, mark fails (store wrapper), claim expires, relay runs again: XLEN is 2, both entries share the same event_id
	t.Run("crash_before_mark", func(t *testing.T) {
		_, err := pool.Exec(ctx, "TRUNCATE outbox_events CASCADE")
		require.NoError(t, err)
		_ = rClient.FlushDB(ctx).Err()

		err = outboxRepo.CreateEvent(ctx, "product", "prod-crash", "product.changed", []byte(`{"v":1}`))
		require.NoError(t, err)

		brokenStore := &failMarkStore{OutboxRepository: *outboxRepo}
		cfg := appOutbox.RelayConfig{
			BatchSize:    10,
			PollInterval: 100 * time.Millisecond,
			Lease:        100 * time.Millisecond,
			BaseBackoff:  1 * time.Second,
			MaxBackoff:   5 * time.Minute,
			MaxAttempts:  5,
		}
		failingRelay, err := appOutbox.NewRelay(brokenStore, pub, cfg, testLogger())
		require.NoError(t, err)

		// First run: publish succeeds to Redis, but MarkPublished fails
		_, _, err = failingRelay.RunOnce(ctx)
		require.Error(t, err)

		// Wait for lease to expire
		time.Sleep(200 * time.Millisecond)

		// Second run with normal store: publishes again and marks PROCESSED
		normalRelay, err := appOutbox.NewRelay(outboxRepo, pub, cfg, testLogger())
		require.NoError(t, err)

		claimedCount, published, err := normalRelay.RunOnce(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, claimedCount)
		assert.Equal(t, 1, published)

		// Check Redis stream length is 2 (at-least-once duplicate delivery)
		entries, err := rClient.XRange(ctx, "product.changed", "-", "+").Result()
		require.NoError(t, err)
		require.Len(t, entries, 2)
		assert.Equal(t, entries[0].Values["event_id"], entries[1].Values["event_id"], "Both messages share the same outbox event_id")
	})

	// E4: A row committed late with an older created_at is still published after newer rows (no high-water-mark gaps)
	t.Run("late_commit", func(t *testing.T) {
		_, err := pool.Exec(ctx, "TRUNCATE outbox_events CASCADE")
		require.NoError(t, err)
		_ = rClient.FlushDB(ctx).Err()

		cfg := appOutbox.RelayConfig{
			BatchSize:    10,
			PollInterval: 100 * time.Millisecond,
			Lease:        30 * time.Second,
			BaseBackoff:  1 * time.Second,
			MaxBackoff:   5 * time.Minute,
			MaxAttempts:  5,
		}
		relay, err := appOutbox.NewRelay(outboxRepo, pub, cfg, testLogger())
		require.NoError(t, err)

		// 1. Insert newer row 1 (created_at = NOW())
		id1, err := uuid.NewString()
		require.NoError(t, err)
		_, err = pool.Exec(ctx, "INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, payload, created_at, available_at) VALUES ($1, 'product', 'p1', 'product.changed', '{}', NOW(), NOW())", id1)
		require.NoError(t, err)

		// 2. Publish newer row first
		claimedCount, published, err := relay.RunOnce(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, claimedCount)
		assert.Equal(t, 1, published)

		var status1 string
		err = pool.QueryRow(ctx, "SELECT status FROM outbox_events WHERE id = $1", id1).Scan(&status1)
		require.NoError(t, err)
		assert.Equal(t, "PROCESSED", status1)

		// 3. Insert row 2 with older timestamp (simulating a long-running transaction that started earlier and just committed)
		id2, err := uuid.NewString()
		require.NoError(t, err)
		_, err = pool.Exec(ctx, "INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, payload, created_at, available_at) VALUES ($1, 'product', 'p2', 'product.changed', '{}', NOW() - INTERVAL '10 minutes', NOW() - INTERVAL '10 minutes')", id2)
		require.NoError(t, err)

		// 4. Run relay again and verify older row is discovered and published
		claimedCount, published, err = relay.RunOnce(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, claimedCount)
		assert.Equal(t, 1, published)

		var status2 string
		err = pool.QueryRow(ctx, "SELECT status FROM outbox_events WHERE id = $1", id2).Scan(&status2)
		require.NoError(t, err)
		assert.Equal(t, "PROCESSED", status2)

		// 5. Verify 0 pending rows remaining
		var pendingCount int
		err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM outbox_events WHERE status = 'PENDING'").Scan(&pendingCount)
		require.NoError(t, err)
		assert.Equal(t, 0, pendingCount)

		// 6. Verify Redis Stream contains both events
		entries, err := rClient.XRange(ctx, "product.changed", "-", "+").Result()
		require.NoError(t, err)
		require.Len(t, entries, 2)
		assert.Equal(t, id1, entries[0].Values["event_id"])
		assert.Equal(t, id2, entries[1].Values["event_id"])
	})

	// E5: ProcessRun -> Outbox -> Relay -> Redis Streams -> Two independent Consumer Groups (fan-out vertical slice)
	t.Run("fan_out_consumer_groups", func(t *testing.T) {
		_, err := pool.Exec(ctx, "TRUNCATE products, product_versions, product_sources, product_changes, raw_records, ingestion_runs, ingestion_run_processing, sources, outbox_events CASCADE")
		require.NoError(t, err)
		_ = rClient.FlushDB(ctx).Err()

		sourceRepo, err := postgres.NewSourceRepository(pool)
		require.NoError(t, err)
		src, err := domainSource.NewSource(
			domainSource.ID("src-fanout"),
			"Supplier Fanout",
			domainSource.TypeAPI,
			map[string]any{
				"base_url": "https://api.supplier.com/v1",
				"field_mapping": map[string]any{
					"name_path":           "title",
					"description_path":    "body",
					"brand_path":          "vendor",
					"origin_country_path": "country_code",
				},
			},
			100,
		)
		require.NoError(t, err)
		err = sourceRepo.Save(ctx, src)
		require.NoError(t, err)

		runRepo, err := postgres.NewIngestionRepository(pool)
		require.NoError(t, err)
		run, err := domainIngestion.NewRun("", domainSource.ID("src-fanout"), "")
		require.NoError(t, err)
		err = runRepo.CreateRun(ctx, run)
		require.NoError(t, err)
		now := time.Now().UTC()
		require.NoError(t, run.Start(now))
		require.NoError(t, runRepo.UpdateStatus(ctx, run, domainIngestion.StatusPending))
		require.NoError(t, run.Complete("", now))
		require.NoError(t, runRepo.UpdateStatus(ctx, run, domainIngestion.StatusRunning))

		rawRepo, err := postgres.NewRawRecordRepository(pool)
		require.NoError(t, err)

		const totalProducts = 3
		records := make([]*domainIngestion.RawRecord, totalProducts)
		for i := 0; i < totalProducts; i++ {
			rec, err := domainIngestion.NewRawRecord(domainIngestion.RawRecordParams{
				SourceID:          domainSource.ID("src-fanout"),
				ExternalProductID: fmt.Sprintf("SKU-FANOUT-%d", i),
				Payload:           []byte(fmt.Sprintf(`{"title":"Fanout Product %d","body":"Desc %d","vendor":"Brand %d","country_code":"US"}`, i, i, i)),
				SourceUpdatedAt:   &now,
				IngestionRunID:    run.ID,
				ReceivedAt:        now,
			})
			require.NoError(t, err)
			records[i] = rec
		}
		err = rawRepo.SaveBatch(ctx, records)
		require.NoError(t, err)

		procRepo, err := postgres.NewRunProcessingRepository(pool)
		require.NoError(t, err)
		err = procRepo.SeedPending(ctx)
		require.NoError(t, err)

		txRunner, err := postgres.NewProductTxManager(pool)
		require.NoError(t, err)

		processor, err := appProduct.NewRunProcessor(runRepo, sourceRepo, rawRepo, procRepo, txRunner)
		require.NoError(t, err)

		// 1. Process Run -> Ingests products and generates outbox rows atomically
		claimToken, err := uuid.NewString()
		require.NoError(t, err)
		result, err := processor.ProcessRun(ctx, run.ID, appProduct.ProcessRunOptions{
			ClaimToken:    claimToken,
			LeaseDuration: 30 * time.Second,
			BatchSize:     50,
		})
		require.NoError(t, err)
		assert.Equal(t, totalProducts, result.RecordsNew)

		// 2. Run Outbox Relay -> Publishes batch to Redis Stream 'product.changed' and marks outbox rows PROCESSED
		relayCfg := appOutbox.RelayConfig{
			BatchSize:    10,
			PollInterval: 100 * time.Millisecond,
			Lease:        30 * time.Second,
			BaseBackoff:  100 * time.Millisecond,
			MaxBackoff:   5 * time.Second,
			MaxAttempts:  5,
		}
		relay, err := appOutbox.NewRelay(outboxRepo, pub, relayCfg, testLogger())
		require.NoError(t, err)

		claimed, published, err := relay.RunOnce(ctx)
		require.NoError(t, err)
		assert.Equal(t, totalProducts, claimed)
		assert.Equal(t, totalProducts, published)

		// 3. Two independent consumer groups: 'compliance-evaluators' and 'ai-enrichers'
		group1 := "compliance-evaluators"
		group2 := "ai-enrichers"

		dlqRepo, err := postgres.NewDLQRepository(pool)
		require.NoError(t, err)

		c1Cfg := infraRedis.ConsumerConfig{
			Stream:           "product.changed",
			Group:            group1,
			ConsumerName:     "c1-compliance",
			BatchSize:        10,
			BlockDuration:    500 * time.Millisecond,
			ClaimMinIdle:     1 * time.Second,
			ClaimInterval:    500 * time.Millisecond,
			ClaimBatchSize:   10,
			BaseBackoff:      10 * time.Millisecond,
			MaxBackoff:       100 * time.Millisecond,
			Concurrency:      2,
			QueueSize:        10,
			HandlerTimeout:   500 * time.Millisecond,
			DrainTimeout:     1 * time.Second,
			RetryMaxAttempts: 1,
			RetryBaseBackoff: 10 * time.Millisecond,
			RetryMaxBackoff:  20 * time.Millisecond,
			DLQStore:         dlqRepo,
		}
		c1, err := infraRedis.NewConsumer(rClient, c1Cfg, testLogger())
		require.NoError(t, err)

		c2Cfg := infraRedis.ConsumerConfig{
			Stream:           "product.changed",
			Group:            group2,
			ConsumerName:     "c2-ai",
			BatchSize:        10,
			BlockDuration:    500 * time.Millisecond,
			ClaimMinIdle:     1 * time.Second,
			ClaimInterval:    500 * time.Millisecond,
			ClaimBatchSize:   10,
			BaseBackoff:      10 * time.Millisecond,
			MaxBackoff:       100 * time.Millisecond,
			Concurrency:      2,
			QueueSize:        10,
			HandlerTimeout:   500 * time.Millisecond,
			DrainTimeout:     1 * time.Second,
			RetryMaxAttempts: 1,
			RetryBaseBackoff: 10 * time.Millisecond,
			RetryMaxBackoff:  20 * time.Millisecond,
			DLQStore:         dlqRepo,
		}
		c2, err := infraRedis.NewConsumer(rClient, c2Cfg, testLogger())
		require.NoError(t, err)

		var g1Count, g2Count atomic.Int32
		consumeCtx, cancelConsume := context.WithTimeout(ctx, 5*time.Second)
		defer cancelConsume()

		g, gCtx := errgroup.WithContext(consumeCtx)

		g.Go(func() error {
			return c1.Run(gCtx, func(ctx context.Context, msg appMessaging.Message) error {
				assert.Equal(t, "product.changed", msg.EventType)
				assert.Equal(t, "product", msg.AggregateType)
				if g1Count.Add(1) == totalProducts && g2Count.Load() == totalProducts {
					cancelConsume()
				}
				return nil
			})
		})

		g.Go(func() error {
			return c2.Run(gCtx, func(ctx context.Context, msg appMessaging.Message) error {
				assert.Equal(t, "product.changed", msg.EventType)
				assert.Equal(t, "product", msg.AggregateType)
				if g2Count.Add(1) == totalProducts && g1Count.Load() == totalProducts {
					cancelConsume()
				}
				return nil
			})
		})

		_ = g.Wait()

		assert.Equal(t, int32(totalProducts), g1Count.Load(), "compliance consumer group should receive all 3 events")
		assert.Equal(t, int32(totalProducts), g2Count.Load(), "ai consumer group should receive all 3 events")

		// 4. Verify 0 pending messages remain in either group
		pend1, err := rClient.XPending(ctx, "product.changed", group1).Result()
		require.NoError(t, err)
		assert.Equal(t, int64(0), pend1.Count)

		pend2, err := rClient.XPending(ctx, "product.changed", group2).Result()
		require.NoError(t, err)
		assert.Equal(t, int64(0), pend2.Count)
	})
}
