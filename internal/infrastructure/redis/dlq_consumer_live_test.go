package redis_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	httpapi "github.com/ayo6706/cross-border-ecommerce/internal/adapters/httpapi"
	appDLQ "github.com/ayo6706/cross-border-ecommerce/internal/application/dlq"
	appIdempotency "github.com/ayo6706/cross-border-ecommerce/internal/application/idempotency"
	appMessaging "github.com/ayo6706/cross-border-ecommerce/internal/application/messaging"
	appOutbox "github.com/ayo6706/cross-border-ecommerce/internal/application/outbox"
	infraPostgres "github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/postgres"
	infraRedis "github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/redis"
	platformUUID "github.com/ayo6706/cross-border-ecommerce/internal/platform/uuid"
	"github.com/ayo6706/cross-border-ecommerce/migrations"
	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func setupLivePostgresForDLQ(t *testing.T) (*pgxpool.Pool, *infraPostgres.DLQRepository, *infraPostgres.OutboxRepository) {
	t.Helper()

	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("skipping live database test: TEST_DATABASE_URL not set")
		return nil, nil, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := infraPostgres.NewPool(ctx, dbURL,
		infraPostgres.WithConnectTimeout(3*time.Second),
		infraPostgres.WithMaxConns(20),
		infraPostgres.WithMinConns(2),
	)
	if err != nil {
		t.Skipf("skipping live database test: unable to connect to %s: %v", dbURL, err)
		return nil, nil, nil
	}

	t.Cleanup(func() {
		cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanCancel()
		_, _ = pool.Exec(cleanCtx, "TRUNCATE dlq_messages, outbox_events CASCADE")
		pool.Close()
	})

	migrator, err := infraPostgres.NewMigrator(pool, migrations.FS)
	require.NoError(t, err)
	require.NoError(t, migrator.Up(ctx))

	dlqRepo, err := infraPostgres.NewDLQRepository(pool)
	require.NoError(t, err)

	outboxRepo, err := infraPostgres.NewOutboxRepository(pool)
	require.NoError(t, err)

	return pool, dlqRepo, outboxRepo
}

func TestConsumer_ENG014_Live(t *testing.T) {
	client := getTestRedisClient(t)
	defer client.Close()

	pool, dlqRepo, outboxRepo := setupLivePostgresForDLQ(t)
	_ = pool

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// A1: Transient failure succeeds on attempt 3 within jittered backoff bounds
	t.Run("A1_transient_error_retries_and_succeeds", func(t *testing.T) {
		stream := uniqueTestStream("a1")
		group := "a1-group"
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		require.NoError(t, infraRedis.EnsureGroup(ctx, client, stream, group))

		pub := infraRedis.NewPublisherFromClient(client)
		evtID, _ := platformUUID.NewString()
		require.NoError(t, pub.Publish(ctx, appOutbox.Event{
			ID:        evtID,
			EventType: stream,
			Payload:   []byte(`{"attempt":"test"}`),
			CreatedAt: time.Now().UTC(),
		}))

		var attempts atomic.Int32
		done := make(chan struct{})

		cfg := infraRedis.ConsumerConfig{
			Stream:           stream,
			Group:            group,
			ConsumerName:     "c-a1",
			BatchSize:        10,
			BlockDuration:    200 * time.Millisecond,
			ClaimMinIdle:     3 * time.Second,
			ClaimInterval:    500 * time.Millisecond,
			ClaimBatchSize:   10,
			BaseBackoff:      50 * time.Millisecond,
			MaxBackoff:       100 * time.Millisecond,
			Concurrency:      2,
			QueueSize:        2,
			HandlerTimeout:   200 * time.Millisecond,
			DrainTimeout:     1 * time.Second,
			RetryMaxAttempts: 4,
			RetryBaseBackoff: 10 * time.Millisecond,
			RetryMaxBackoff:  30 * time.Millisecond,
			DLQStore:         dlqRepo,
		}
		consumer, err := infraRedis.NewConsumer(client, cfg, logger)
		require.NoError(t, err)

		go func() {
			_ = consumer.Run(ctx, func(ctx context.Context, msg appMessaging.Message) error {
				cnt := attempts.Add(1)
				if cnt < 3 {
					return errors.New("simulated transient network failure")
				}
				close(done)
				return nil
			})
		}()

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for retry success")
		}

		assert.Equal(t, int32(3), attempts.Load())

		// Acknowledged on stream; PEL is empty
		require.Eventually(t, func() bool {
			pend, err := client.XPending(ctx, stream, group).Result()
			return err == nil && pend.Count == 0
		}, 3*time.Second, 50*time.Millisecond)

		// Zero rows in DLQ
		var dlqCount int
		err = pool.QueryRow(ctx, "SELECT count(*) FROM dlq_messages WHERE stream = $1", stream).Scan(&dlqCount)
		require.NoError(t, err)
		assert.Equal(t, 0, dlqCount)
	})

	// A2: Transient retries run out -> written to dlq_messages with attempts=3, acknowledged on Redis
	t.Run("A2_transient_exhaustion_dead_lettered_and_acked", func(t *testing.T) {
		stream := uniqueTestStream("a2")
		group := "a2-group"
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		require.NoError(t, infraRedis.EnsureGroup(ctx, client, stream, group))

		pub := infraRedis.NewPublisherFromClient(client)
		evtID, _ := platformUUID.NewString()
		payload := []byte(`{"order_id":"fail-forever"}`)
		require.NoError(t, pub.Publish(ctx, appOutbox.Event{
			ID:            evtID,
			EventType:     stream,
			AggregateType: "order",
			AggregateID:   "ord-123",
			Payload:       payload,
			CreatedAt:     time.Now().UTC(),
		}))

		var attempts atomic.Int32
		attemptsDone := make(chan struct{})

		cfg := infraRedis.ConsumerConfig{
			Stream:           stream,
			Group:            group,
			ConsumerName:     "c-a2",
			BatchSize:        10,
			BlockDuration:    200 * time.Millisecond,
			ClaimMinIdle:     3 * time.Second,
			ClaimInterval:    500 * time.Millisecond,
			ClaimBatchSize:   10,
			BaseBackoff:      50 * time.Millisecond,
			MaxBackoff:       100 * time.Millisecond,
			Concurrency:      2,
			QueueSize:        2,
			HandlerTimeout:   100 * time.Millisecond,
			DrainTimeout:     1 * time.Second,
			RetryMaxAttempts: 3,
			RetryBaseBackoff: 10 * time.Millisecond,
			RetryMaxBackoff:  20 * time.Millisecond,
			DLQStore:         dlqRepo,
		}
		consumer, err := infraRedis.NewConsumer(client, cfg, logger)
		require.NoError(t, err)

		cCtx, cCancel := context.WithCancel(ctx)
		defer cCancel()

		go func() {
			_ = consumer.Run(cCtx, func(ctx context.Context, msg appMessaging.Message) error {
				if attempts.Add(1) == 3 {
					close(attemptsDone)
				}
				return errors.New("downstream database deadlock")
			})
		}()

		select {
		case <-attemptsDone:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for 3 retry attempts")
		}

		// Message acknowledged in Redis stream; PEL is empty
		require.Eventually(t, func() bool {
			pend, err := client.XPending(ctx, stream, group).Result()
			return err == nil && pend.Count == 0
		}, 4*time.Second, 50*time.Millisecond)

		assert.Equal(t, int32(3), attempts.Load(), "should have run exactly RetryMaxAttempts times")

		// Verified row inserted into dlq_messages
		var (
			dID           string
			dFailureClass string
			dAttempts     int
			dStatus       string
			dPayload      []byte
		)
		err = pool.QueryRow(ctx, `
			SELECT id, failure_class, attempts, status, payload
			FROM dlq_messages
			WHERE stream = $1 AND consumer_group = $2 AND event_id = $3
		`, stream, group, evtID).Scan(&dID, &dFailureClass, &dAttempts, &dStatus, &dPayload)
		require.NoError(t, err)
		assert.Equal(t, "TRANSIENT_EXHAUSTED", dFailureClass)
		assert.Equal(t, 3, dAttempts)
		assert.Equal(t, "DEAD", dStatus)
		assert.Equal(t, payload, dPayload)
	})

	// A3: Permanent error routed to DLQ immediately after 1 attempt
	t.Run("A3_permanent_error_immediate_dlq", func(t *testing.T) {
		stream := uniqueTestStream("a3")
		group := "a3-group"
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		require.NoError(t, infraRedis.EnsureGroup(ctx, client, stream, group))

		pub := infraRedis.NewPublisherFromClient(client)
		evtID, _ := platformUUID.NewString()
		payload := []byte(`{"invalid":"contract"}`)
		require.NoError(t, pub.Publish(ctx, appOutbox.Event{
			ID:        evtID,
			EventType: stream,
			Payload:   payload,
			CreatedAt: time.Now().UTC(),
		}))

		var attempts atomic.Int32
		attemptDone := make(chan struct{})

		cfg := infraRedis.ConsumerConfig{
			Stream:           stream,
			Group:            group,
			ConsumerName:     "c-a3",
			BatchSize:        10,
			BlockDuration:    200 * time.Millisecond,
			ClaimMinIdle:     3 * time.Second,
			ClaimInterval:    500 * time.Millisecond,
			ClaimBatchSize:   10,
			BaseBackoff:      50 * time.Millisecond,
			MaxBackoff:       100 * time.Millisecond,
			Concurrency:      2,
			QueueSize:        2,
			HandlerTimeout:   100 * time.Millisecond,
			DrainTimeout:     1 * time.Second,
			RetryMaxAttempts: 5,
			RetryBaseBackoff: 10 * time.Millisecond,
			RetryMaxBackoff:  20 * time.Millisecond,
			DLQStore:         dlqRepo,
		}
		consumer, err := infraRedis.NewConsumer(client, cfg, logger)
		require.NoError(t, err)

		cCtx, cCancel := context.WithCancel(ctx)
		defer cCancel()

		go func() {
			_ = consumer.Run(cCtx, func(ctx context.Context, msg appMessaging.Message) error {
				attempts.Add(1)
				close(attemptDone)
				return fmt.Errorf("%w: payload violates schema", appDLQ.ErrPermanent)
			})
		}()

		select {
		case <-attemptDone:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for permanent error handler")
		}

		// Acknowledged on stream; PEL is empty
		require.Eventually(t, func() bool {
			pend, err := client.XPending(ctx, stream, group).Result()
			return err == nil && pend.Count == 0
		}, 3*time.Second, 50*time.Millisecond)

		assert.Equal(t, int32(1), attempts.Load(), "permanent error must dead-letter after attempt 1")

		// Verify row in dlq_messages
		var dFailureClass string
		err = pool.QueryRow(ctx, "SELECT failure_class FROM dlq_messages WHERE stream = $1 AND event_id = $2", stream, evtID).Scan(&dFailureClass)
		require.NoError(t, err)
		assert.Equal(t, "PERMANENT", dFailureClass)
	})

	// F1: DLQ insert failure leaves message unacknowledged and pending in PEL
	t.Run("F1_dlq_insert_failure_leaves_pending", func(t *testing.T) {
		stream := uniqueTestStream("f1")
		group := "f1-group"
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		require.NoError(t, infraRedis.EnsureGroup(ctx, client, stream, group))

		pub := infraRedis.NewPublisherFromClient(client)
		evtID, _ := platformUUID.NewString()
		require.NoError(t, pub.Publish(ctx, appOutbox.Event{
			ID:        evtID,
			EventType: stream,
			Payload:   []byte(`{"f1":true}`),
			CreatedAt: time.Now().UTC(),
		}))

		failingDLQ := &failingDLQStore{err: errors.New("postgres connection refused")}

		cfg := infraRedis.ConsumerConfig{
			Stream:           stream,
			Group:            group,
			ConsumerName:     "c-f1",
			BatchSize:        10,
			BlockDuration:    200 * time.Millisecond,
			ClaimMinIdle:     1 * time.Second,
			ClaimInterval:    500 * time.Millisecond,
			ClaimBatchSize:   10,
			BaseBackoff:      50 * time.Millisecond,
			MaxBackoff:       100 * time.Millisecond,
			Concurrency:      1,
			QueueSize:        1,
			HandlerTimeout:   100 * time.Millisecond,
			DrainTimeout:     1 * time.Second,
			RetryMaxAttempts: 1,
			RetryBaseBackoff: 10 * time.Millisecond,
			RetryMaxBackoff:  20 * time.Millisecond,
			DLQStore:         failingDLQ,
		}
		consumer, err := infraRedis.NewConsumer(client, cfg, logger)
		require.NoError(t, err)

		handlerRan := make(chan struct{})
		cCtx, cCancel := context.WithCancel(ctx)
		defer cCancel()

		go func() {
			_ = consumer.Run(cCtx, func(ctx context.Context, msg appMessaging.Message) error {
				close(handlerRan)
				return fmt.Errorf("%w: fatal error", appDLQ.ErrPermanent)
			})
		}()

		select {
		case <-handlerRan:
		case <-time.After(3 * time.Second):
			t.Fatal("handler never executed")
		}

		// Allow consumer time to attempt DLQ insert and fail
		time.Sleep(300 * time.Millisecond)

		// Must remain pending in PEL
		pend, err := client.XPending(ctx, stream, group).Result()
		require.NoError(t, err)
		assert.Equal(t, int64(1), pend.Count, "DLQ insert failure must leave message unacknowledged in PEL")
	})

	// F2: Crash between DLQ insert and ACK: redelivery re-inserts (ON CONFLICT DO NOTHING) and ACKs
	t.Run("F2_crash_between_insert_and_ack_idempotent_recovery", func(t *testing.T) {
		stream := uniqueTestStream("f2")
		group := "f2-group"
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		require.NoError(t, infraRedis.EnsureGroup(ctx, client, stream, group))

		pub := infraRedis.NewPublisherFromClient(client)
		evtID, _ := platformUUID.NewString()
		require.NoError(t, pub.Publish(ctx, appOutbox.Event{
			ID:        evtID,
			EventType: stream,
			Payload:   []byte(`{"f2":true}`),
			CreatedAt: time.Now().UTC(),
		}))

		// Simulate pre-existing DLQ row inserted by a previous dead worker before crash
		rawRead, err := client.XReadGroup(ctx, &goredis.XReadGroupArgs{
			Group:    group,
			Consumer: "c-pre",
			Streams:  []string{stream, ">"},
			Count:    1,
		}).Result()
		require.NoError(t, err)
		require.Len(t, rawRead[0].Messages, 1)
		streamMsgID := rawRead[0].Messages[0].ID

		err = dlqRepo.Insert(ctx, appDLQ.Message{
			Stream:          stream,
			ConsumerGroup:   group,
			StreamMessageID: streamMsgID,
			EventID:         &evtID,
			EventType:       stream,
			Payload:         []byte(`{"f2":true}`),
			FailureClass:    "PERMANENT",
			LastError:       "crash before ack",
			Attempts:        1,
			ConsumerName:    "c-pre",
			FirstFailedAt:   time.Now(),
		})
		require.NoError(t, err)

		// Now new recovering consumer starts, claims the idle pending message and encounters permanent error again
		cfg := infraRedis.ConsumerConfig{
			Stream:           stream,
			Group:            group,
			ConsumerName:     "c-f2-recovering",
			BatchSize:        10,
			BlockDuration:    100 * time.Millisecond,
			ClaimMinIdle:     200 * time.Millisecond,
			ClaimInterval:    50 * time.Millisecond,
			ClaimBatchSize:   10,
			BaseBackoff:      50 * time.Millisecond,
			MaxBackoff:       100 * time.Millisecond,
			Concurrency:      1,
			QueueSize:        1,
			HandlerTimeout:   100 * time.Millisecond,
			DrainTimeout:     1 * time.Second,
			RetryMaxAttempts: 1,
			RetryBaseBackoff: 10 * time.Millisecond,
			RetryMaxBackoff:  20 * time.Millisecond,
			DLQStore:         dlqRepo,
		}
		consumer, err := infraRedis.NewConsumer(client, cfg, logger)
		require.NoError(t, err)

		cCtx, cCancel := context.WithCancel(ctx)
		defer cCancel()

		go func() {
			_ = consumer.Run(cCtx, func(ctx context.Context, msg appMessaging.Message) error {
				return fmt.Errorf("%w: permanent error on redelivery", appDLQ.ErrPermanent)
			})
		}()

		// Recovering consumer re-inserts into DLQ (idempotent ON CONFLICT DO NOTHING) and ACKs!
		require.Eventually(t, func() bool {
			pend, err := client.XPending(ctx, stream, group).Result()
			return err == nil && pend.Count == 0
		}, 3*time.Second, 50*time.Millisecond)

		// Exactly 1 row in dlq_messages
		var count int
		err = pool.QueryRow(ctx, "SELECT count(*) FROM dlq_messages WHERE stream = $1 AND stream_message_id = $2", stream, streamMsgID).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	// F3: Shutdown during backoff sleep returns promptly, does not write DLQ, leaves message pending in PEL
	t.Run("F3_shutdown_during_retry_sleep_leaves_pending", func(t *testing.T) {
		stream := uniqueTestStream("f3")
		group := "f3-group"
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		require.NoError(t, infraRedis.EnsureGroup(ctx, client, stream, group))

		pub := infraRedis.NewPublisherFromClient(client)
		evtID, _ := platformUUID.NewString()
		require.NoError(t, pub.Publish(ctx, appOutbox.Event{
			ID:        evtID,
			EventType: stream,
			Payload:   []byte(`{"f3":true}`),
			CreatedAt: time.Now().UTC(),
		}))

		cfg := infraRedis.ConsumerConfig{
			Stream:           stream,
			Group:            group,
			ConsumerName:     "c-f3",
			BatchSize:        10,
			BlockDuration:    100 * time.Millisecond,
			ClaimMinIdle:     15 * time.Second,
			ClaimInterval:    1 * time.Second,
			ClaimBatchSize:   10,
			BaseBackoff:      50 * time.Millisecond,
			MaxBackoff:       100 * time.Millisecond,
			Concurrency:      1,
			QueueSize:        1,
			HandlerTimeout:   10 * time.Millisecond,
			DrainTimeout:     1 * time.Second,
			RetryMaxAttempts: 10,
			RetryBaseBackoff: 500 * time.Millisecond,
			RetryMaxBackoff:  1 * time.Second,
			DLQStore:         dlqRepo,
		}
		consumer, err := infraRedis.NewConsumer(client, cfg, logger)
		require.NoError(t, err)

		handlerAttempted := make(chan struct{})
		cCtx, cCancel := context.WithCancel(ctx)

		runDone := make(chan error, 1)
		go func() {
			runDone <- consumer.Run(cCtx, func(ctx context.Context, msg appMessaging.Message) error {
				select {
				case <-handlerAttempted:
				default:
					close(handlerAttempted)
				}
				return errors.New("transient error to trigger backoff sleep")
			})
		}()

		select {
		case <-handlerAttempted:
		case <-time.After(3 * time.Second):
			t.Fatal("handler never executed")
		}

		// Ensure consumer is now sleeping in backoff timer
		time.Sleep(50 * time.Millisecond)

		// Cancel consumer context during sleep
		startTime := time.Now()
		cCancel()

		select {
		case err := <-runDone:
			assert.ErrorIs(t, err, context.Canceled)
		case <-time.After(1 * time.Second):
			t.Fatal("consumer did not abort backoff sleep promptly upon cancellation")
		}
		assert.Less(t, time.Since(startTime), 1*time.Second, "shutdown during backoff should return immediately")

		// Message was not ACKed; remains in PEL
		pend, err := client.XPending(context.Background(), stream, group).Result()
		require.NoError(t, err)
		assert.Equal(t, int64(1), pend.Count, "unacknowledged message must remain in PEL after cancellation")

		// Zero DLQ rows
		var count int
		err = pool.QueryRow(context.Background(), "SELECT count(*) FROM dlq_messages WHERE stream = $1", stream).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 0, count, "cancelled message must not be dead-lettered")
	})

	// A5: End-to-end Replay via HTTP API -> Outbox -> Relay -> Redis Consumer Group
	t.Run("A5_end_to_end_replay_route_outbox_relay", func(t *testing.T) {
		stream := uniqueTestStream("a5")
		targetGroup := "target-workers"
		otherGroup := "other-workers"
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		require.NoError(t, infraRedis.EnsureGroup(ctx, client, stream, targetGroup))
		require.NoError(t, infraRedis.EnsureGroup(ctx, client, stream, otherGroup))

		pub := infraRedis.NewPublisherFromClient(client)
		originalEventID, _ := platformUUID.NewString()
		payload := []byte(`{"replay_test":"payload_content"}`)

		// 1. Publish and let target group fail permanently to populate dlq_messages
		require.NoError(t, pub.Publish(ctx, appOutbox.Event{
			ID:            originalEventID,
			EventType:     stream,
			AggregateType: "test_agg",
			AggregateID:   "agg-123",
			Payload:       payload,
			CreatedAt:     time.Now().UTC(),
		}))

		cInitialCfg := infraRedis.ConsumerConfig{
			Stream:           stream,
			Group:            targetGroup,
			ConsumerName:     "c-initial",
			BatchSize:        10,
			BlockDuration:    100 * time.Millisecond,
			ClaimMinIdle:     3 * time.Second,
			ClaimInterval:    500 * time.Millisecond,
			ClaimBatchSize:   10,
			BaseBackoff:      50 * time.Millisecond,
			MaxBackoff:       100 * time.Millisecond,
			Concurrency:      1,
			QueueSize:        1,
			HandlerTimeout:   100 * time.Millisecond,
			DrainTimeout:     1 * time.Second,
			RetryMaxAttempts: 1,
			RetryBaseBackoff: 10 * time.Millisecond,
			RetryMaxBackoff:  20 * time.Millisecond,
			DLQStore:         dlqRepo,
		}
		cInitial, err := infraRedis.NewConsumer(client, cInitialCfg, logger)
		require.NoError(t, err)

		cInitCtx, cInitCancel := context.WithCancel(ctx)
		defer cInitCancel()

		initialFailed := make(chan struct{})
		go func() {
			_ = cInitial.Run(cInitCtx, func(ctx context.Context, msg appMessaging.Message) error {
				close(initialFailed)
				return fmt.Errorf("%w: initial permanent failure", appDLQ.ErrPermanent)
			})
		}()

		select {
		case <-initialFailed:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for cInitial to process message")
		}

		// Target group dead-letters the message and ACKs
		require.Eventually(t, func() bool {
			pend, err := client.XPending(ctx, stream, targetGroup).Result()
			return err == nil && pend.Count == 0
		}, 3*time.Second, 50*time.Millisecond)
		cInitCancel()

		// otherGroup reads and ACKs the original event normally
		otherInitRead, err := client.XReadGroup(ctx, &goredis.XReadGroupArgs{
			Group:    otherGroup,
			Consumer: "c-other-init",
			Streams:  []string{stream, ">"},
			Count:    1,
		}).Result()
		require.NoError(t, err)
		require.Len(t, otherInitRead[0].Messages, 1)
		require.NoError(t, client.XAck(ctx, stream, otherGroup, otherInitRead[0].Messages[0].ID).Err())

		// Get the created DLQ message ID
		var dlqID string
		err = pool.QueryRow(ctx, `
			SELECT id FROM dlq_messages
			WHERE stream = $1 AND consumer_group = $2 AND event_id = $3
		`, stream, targetGroup, originalEventID).Scan(&dlqID)
		require.NoError(t, err)

		// 2. Invoke HTTP API: POST /v1/dlq/{id}/replay
		router := httpapi.NewRouter(httpapi.RouterConfig{
			DB:       pool,
			DLQStore: dlqRepo,
		})

		req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/v1/dlq/%s/replay", dlqID), nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		require.Equal(t, http.StatusAccepted, rec.Code)
		var replayResp struct {
			ReplayOutboxID string `json:"replay_outbox_id"`
			Status         string `json:"status"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &replayResp))
		assert.NotEmpty(t, replayResp.ReplayOutboxID)
		assert.Equal(t, "REPLAYED", replayResp.Status)

		// Verify dlq_messages is now REPLAYED in Postgres
		var dlqStatus string
		err = pool.QueryRow(ctx, "SELECT status FROM dlq_messages WHERE id = $1", dlqID).Scan(&dlqStatus)
		require.NoError(t, err)
		assert.Equal(t, "REPLAYED", dlqStatus)

		// 3. Outbox Relay runs, claims the replay outbox event, and publishes to Redis stream
		relayCfg := appOutbox.RelayConfig{
			BatchSize:    10,
			PollInterval: 100 * time.Millisecond,
			Lease:        30 * time.Second,
			MaxAttempts:  5,
			BaseBackoff:  10 * time.Millisecond,
			MaxBackoff:   100 * time.Millisecond,
		}
		relay, err := appOutbox.NewRelay(outboxRepo, pub, relayCfg, logger)
		require.NoError(t, err)

		claimed, published, err := relay.RunOnce(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, claimed)
		assert.Equal(t, 1, published)

		// 4. Start Target and Non-Target consumers to verify selective dispatch
		var targetProcessed atomic.Int32
		var otherProcessed atomic.Int32

		targetCfg := infraRedis.ConsumerConfig{
			Stream:           stream,
			Group:            targetGroup,
			ConsumerName:     "c-target-replay",
			BatchSize:        10,
			BlockDuration:    100 * time.Millisecond,
			ClaimMinIdle:     3 * time.Second,
			ClaimInterval:    500 * time.Millisecond,
			ClaimBatchSize:   10,
			BaseBackoff:      50 * time.Millisecond,
			MaxBackoff:       100 * time.Millisecond,
			Concurrency:      1,
			QueueSize:        1,
			HandlerTimeout:   500 * time.Millisecond,
			DrainTimeout:     1 * time.Second,
			RetryMaxAttempts: 1,
			RetryBaseBackoff: 10 * time.Millisecond,
			RetryMaxBackoff:  20 * time.Millisecond,
			DLQStore:         dlqRepo,
		}
		cTarget, err := infraRedis.NewConsumer(client, targetCfg, logger)
		require.NoError(t, err)

		otherCfg := infraRedis.ConsumerConfig{
			Stream:           stream,
			Group:            otherGroup,
			ConsumerName:     "c-other-replay",
			BatchSize:        10,
			BlockDuration:    100 * time.Millisecond,
			ClaimMinIdle:     3 * time.Second,
			ClaimInterval:    500 * time.Millisecond,
			ClaimBatchSize:   10,
			BaseBackoff:      50 * time.Millisecond,
			MaxBackoff:       100 * time.Millisecond,
			Concurrency:      1,
			QueueSize:        1,
			HandlerTimeout:   500 * time.Millisecond,
			DrainTimeout:     1 * time.Second,
			RetryMaxAttempts: 1,
			RetryBaseBackoff: 10 * time.Millisecond,
			RetryMaxBackoff:  20 * time.Millisecond,
			DLQStore:         dlqRepo,
		}
		cOther, err := infraRedis.NewConsumer(client, otherCfg, logger)
		require.NoError(t, err)

		cRunCtx, cRunCancel := context.WithCancel(ctx)
		defer cRunCancel()

		g, gCtx := errgroup.WithContext(cRunCtx)

		// Target group: runs handler and succeeds
		g.Go(func() error {
			return cTarget.Run(gCtx, func(ctx context.Context, msg appMessaging.Message) error {
				assert.Equal(t, originalEventID, msg.EventID, "replayed event must retain original event_id")
				assert.Equal(t, targetGroup, msg.TargetGroup, "target group must match")
				targetProcessed.Add(1)
				return nil
			})
		})

		// Other group: skipped by consumer.processMessage because target_group != c.cfg.Group!
		g.Go(func() error {
			return cOther.Run(gCtx, func(ctx context.Context, msg appMessaging.Message) error {
				otherProcessed.Add(1)
				return nil
			})
		})

		// Wait until target group processes the replayed event
		require.Eventually(t, func() bool {
			return targetProcessed.Load() == 1
		}, 4*time.Second, 50*time.Millisecond)

		// Both groups acknowledge the message and empty their PELs
		require.Eventually(t, func() bool {
			pTarget, err1 := client.XPending(ctx, stream, targetGroup).Result()
			pOther, err2 := client.XPending(ctx, stream, otherGroup).Result()
			return err1 == nil && err2 == nil && pTarget.Count == 0 && pOther.Count == 0
		}, 4*time.Second, 50*time.Millisecond)

		cRunCancel()
		_ = g.Wait()

		assert.Equal(t, int32(1), targetProcessed.Load(), "target group must have executed handler")
		assert.Equal(t, int32(0), otherProcessed.Load(), "non-target group must NOT have executed handler (skipped and ACKed)")

		// 5. Duplicate replay of already replayed message returns 409 Conflict
		reqDup := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/v1/dlq/%s/replay", dlqID), nil)
		recDup := httptest.NewRecorder()
		router.ServeHTTP(recDup, reqDup)
		assert.Equal(t, http.StatusConflict, recDup.Code)
	})

	// F6: Replay of an event the group already completed: idempotency guard skips it, preventing duplicate effects
	t.Run("F6_idempotent_replay_guard_prevents_duplicate_effects", func(t *testing.T) {
		stream := uniqueTestStream("f6")
		group := "f6-group"
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		require.NoError(t, infraRedis.EnsureGroup(ctx, client, stream, group))

		idempotencyRepo, err := infraPostgres.NewIdempotencyRepository(pool)
		require.NoError(t, err)

		guard, err := appIdempotency.NewGuard(idempotencyRepo, 10*time.Second, logger)
		require.NoError(t, err)

		evtID, _ := platformUUID.NewString()
		payload := []byte(`{"order_id":"replay-guard-test"}`)

		// 1. Mark event as already COMPLETED in postgres idempotency_keys
		token, _ := platformUUID.NewString()
		hasher := sha256.Sum256(payload)
		claim, err := idempotencyRepo.ClaimKey(ctx, group, evtID, hasher[:], token, 10*time.Second)
		require.NoError(t, err)

		tx, err := pool.Begin(ctx)
		require.NoError(t, err)
		txRepo := idempotencyRepo.WithTx(tx)
		claimCtx := appIdempotency.WithClaim(ctx, &claim)
		require.NoError(t, appIdempotency.CompleteInTx(claimCtx, txRepo))
		require.NoError(t, tx.Commit(ctx))

		// 2. Publish replayed message to Redis stream
		pub := infraRedis.NewPublisherFromClient(client)
		require.NoError(t, pub.Publish(ctx, appOutbox.Event{
			ID:          evtID,
			EventType:   stream,
			TargetGroup: group,
			Payload:     payload,
			CreatedAt:   time.Now().UTC(),
		}))

		var sideEffects atomic.Int32
		rawHandler := func(ctx context.Context, msg appMessaging.Message) error {
			sideEffects.Add(1)
			return nil
		}

		wrappedHandler, err := guard.Wrap(group, rawHandler)
		require.NoError(t, err)

		cfg := infraRedis.ConsumerConfig{
			Stream:           stream,
			Group:            group,
			ConsumerName:     "c-f6",
			BatchSize:        10,
			BlockDuration:    100 * time.Millisecond,
			ClaimMinIdle:     3 * time.Second,
			ClaimInterval:    500 * time.Millisecond,
			ClaimBatchSize:   10,
			BaseBackoff:      50 * time.Millisecond,
			MaxBackoff:       100 * time.Millisecond,
			Concurrency:      1,
			QueueSize:        1,
			HandlerTimeout:   500 * time.Millisecond,
			DrainTimeout:     1 * time.Second,
			RetryMaxAttempts: 1,
			RetryBaseBackoff: 10 * time.Millisecond,
			RetryMaxBackoff:  20 * time.Millisecond,
			DLQStore:         dlqRepo,
		}
		consumer, err := infraRedis.NewConsumer(client, cfg, logger)
		require.NoError(t, err)

		cCtx, cCancel := context.WithCancel(ctx)
		defer cCancel()

		go func() {
			_ = consumer.Run(cCtx, wrappedHandler)
		}()

		// Message is acknowledged by guard and PEL becomes empty
		require.Eventually(t, func() bool {
			pend, err := client.XPending(ctx, stream, group).Result()
			return err == nil && pend.Count == 0
		}, 3*time.Second, 50*time.Millisecond)

		cCancel()

		// Side effects was NEVER incremented because idempotency guard intercepted the already COMPLETED key!
		assert.Equal(t, int32(0), sideEffects.Load(), "handler must be skipped when idempotency key is COMPLETED")
	})

	t.Run("Regression_Bug1_Corrupt_UTF8_NUL_Oversized_Isolated_To_Live_Postgres_DLQ", func(t *testing.T) {
		stream := uniqueTestStream("corrupt-utf8")
		group := "corrupt-utf8-group"
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		require.NoError(t, infraRedis.EnsureGroup(ctx, client, stream, group))

		// Publish raw message with invalid UTF-8 bytes, missing created_at, oversized headers
		xmsgID, err := client.XAdd(ctx, &goredis.XAddArgs{
			Stream: stream,
			Values: map[string]any{
				"event_id":       "evt-corrupt-1",
				"event_type":     "\xff\xfe" + string(make([]byte, 200)), // invalid UTF-8 and > 64 chars
				"aggregate_type": "Aggregate\x00WithNUL",
				"aggregate_id":   "AggID\xff\xfe",
				"correlation_id": "corr-\x00-123",
				"payload":        []byte("\xff\xfe\x00\x01binary garbage"),
			},
		}).Result()
		require.NoError(t, err)

		cfg := infraRedis.ConsumerConfig{
			Stream:           stream,
			Group:            group,
			ConsumerName:     "c-corrupt-utf8",
			BatchSize:        10,
			BlockDuration:    100 * time.Millisecond,
			ClaimMinIdle:     3 * time.Second,
			ClaimInterval:    500 * time.Millisecond,
			ClaimBatchSize:   10,
			BaseBackoff:      50 * time.Millisecond,
			MaxBackoff:       100 * time.Millisecond,
			Concurrency:      1,
			QueueSize:        1,
			HandlerTimeout:   500 * time.Millisecond,
			DrainTimeout:     1 * time.Second,
			RetryMaxAttempts: 1,
			RetryBaseBackoff: 10 * time.Millisecond,
			RetryMaxBackoff:  20 * time.Millisecond,
			DLQStore:         dlqRepo, // REAL PostgreSQL repository!
		}
		consumer, err := infraRedis.NewConsumer(client, cfg, logger)
		require.NoError(t, err)

		cCtx, cCancel := context.WithCancel(ctx)
		defer cCancel()

		go func() {
			_ = consumer.Run(cCtx, func(ctx context.Context, msg appMessaging.Message) error {
				return nil
			})
		}()

		// The corrupt envelope must be saved to PostgreSQL dlq_messages and acknowledged in Redis
		require.Eventually(t, func() bool {
			var count int
			err = pool.QueryRow(ctx, "SELECT count(*) FROM dlq_messages WHERE stream = $1 AND consumer_group = $2 AND stream_message_id = $3",
				stream, group, xmsgID).Scan(&count)
			return err == nil && count == 1
		}, 5*time.Second, 50*time.Millisecond, "corrupt envelope must be safely persisted into live PostgreSQL dlq_messages")

		require.Eventually(t, func() bool {
			pend, err := client.XPending(ctx, stream, group).Result()
			return err == nil && pend.Count == 0
		}, 5*time.Second, 50*time.Millisecond, "corrupt envelope must be acknowledged and cleared from PEL")

		cCancel()
	})

	t.Run("Regression_Bug2_NonUUID_EventID_Preserved_And_Replayed", func(t *testing.T) {
		stream := uniqueTestStream("nonuuid")
		group := "nonuuid-group"
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		require.NoError(t, infraRedis.EnsureGroup(ctx, client, stream, group))

		pub := infraRedis.NewPublisherFromClient(client)
		nonUUIDEventID := "order-42-custom-string"
		payload := []byte(`{"item":"book","qty":1}`)

		err := pub.Publish(ctx, appOutbox.Event{
			ID:            nonUUIDEventID,
			AggregateType: "Order",
			AggregateID:   "order-42",
			EventType:     stream,
			Payload:       payload,
			CreatedAt:     time.Now().UTC(),
		})
		require.NoError(t, err)

		cfg := infraRedis.ConsumerConfig{
			Stream:           stream,
			Group:            group,
			ConsumerName:     "c-nonuuid",
			BatchSize:        10,
			BlockDuration:    100 * time.Millisecond,
			ClaimMinIdle:     3 * time.Second,
			ClaimInterval:    500 * time.Millisecond,
			ClaimBatchSize:   10,
			BaseBackoff:      50 * time.Millisecond,
			MaxBackoff:       100 * time.Millisecond,
			Concurrency:      1,
			QueueSize:        1,
			HandlerTimeout:   500 * time.Millisecond,
			DrainTimeout:     1 * time.Second,
			RetryMaxAttempts: 1,
			RetryBaseBackoff: 10 * time.Millisecond,
			RetryMaxBackoff:  20 * time.Millisecond,
			DLQStore:         dlqRepo,
		}
		consumer, err := infraRedis.NewConsumer(client, cfg, logger)
		require.NoError(t, err)

		cCtx, cCancel := context.WithCancel(ctx)
		defer cCancel()

		handlerCalled := make(chan struct{})
		go func() {
			_ = consumer.Run(cCtx, func(ctx context.Context, msg appMessaging.Message) error {
				close(handlerCalled)
				return appDLQ.ErrPermanent
			})
		}()

		select {
		case <-handlerCalled:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for handler invocation")
		}

		// Wait for message to be dead-lettered and acknowledged
		require.Eventually(t, func() bool {
			pend, err := client.XPending(ctx, stream, group).Result()
			return err == nil && pend.Count == 0
		}, 5*time.Second, 50*time.Millisecond)

		require.Eventually(t, func() bool {
			var count int
			err := pool.QueryRow(ctx, "SELECT count(*) FROM dlq_messages WHERE stream = $1 AND consumer_group = $2",
				stream, group).Scan(&count)
			return err == nil && count == 1
		}, 5*time.Second, 50*time.Millisecond)

		cCancel()

		// Verify event_id is preserved in PostgreSQL dlq_messages
		var dlqID string
		var storedEventID string
		err = pool.QueryRow(ctx, "SELECT id::text, event_id FROM dlq_messages WHERE stream = $1 AND consumer_group = $2",
			stream, group).Scan(&dlqID, &storedEventID)
		require.NoError(t, err)
		assert.Equal(t, nonUUIDEventID, storedEventID)

		// Replay via HTTP router
		router := httpapi.NewRouter(httpapi.RouterConfig{
			Logger:   logger,
			DB:       pool,
			DLQStore: dlqRepo,
		})

		req := httptest.NewRequest(http.MethodPost, "/v1/dlq/"+dlqID+"/replay", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusAccepted, rec.Code)

		var resp map[string]any
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		replayOutboxID := resp["replay_outbox_id"].(string)

		// Run outbox relay to publish replay to Redis
		relay, err := appOutbox.NewRelay(outboxRepo, infraRedis.NewPublisherFromClient(client), appOutbox.RelayConfig{
			BatchSize:    10,
			PollInterval: 10 * time.Millisecond,
			Lease:        2 * time.Second,
			BaseBackoff:  10 * time.Millisecond,
			MaxBackoff:   50 * time.Millisecond,
			MaxAttempts:  3,
		}, logger)
		require.NoError(t, err)

		rCtx, rCancel := context.WithTimeout(ctx, 3*time.Second)
		defer rCancel()
		_, _, err = relay.RunOnce(rCtx)
		require.NoError(t, err)

		// Verify outbox row has replay_of_event_id set to "order-42-custom-string"
		var replayOf string
		err = pool.QueryRow(ctx, "SELECT replay_of_event_id FROM outbox_events WHERE id = $1", replayOutboxID).Scan(&replayOf)
		require.NoError(t, err)
		assert.Equal(t, nonUUIDEventID, replayOf)
	})
}

type failingDLQStore struct {
	err error
}

func (f *failingDLQStore) Insert(ctx context.Context, msg appDLQ.Message) error {
	return f.err
}
