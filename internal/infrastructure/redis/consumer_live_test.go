package redis_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	appDLQ "github.com/ayo6706/cross-border-ecommerce/internal/application/dlq"
	appIdempotency "github.com/ayo6706/cross-border-ecommerce/internal/application/idempotency"
	appMessaging "github.com/ayo6706/cross-border-ecommerce/internal/application/messaging"
	appOutbox "github.com/ayo6706/cross-border-ecommerce/internal/application/outbox"
	infraPostgres "github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/postgres"
	infraRedis "github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/redis"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/uuid"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/worker"
	"github.com/ayo6706/cross-border-ecommerce/migrations"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

type memoryLogHandler struct {
	mu   sync.Mutex
	logs []string
}

func (m *memoryLogHandler) Enabled(context.Context, slog.Level) bool { return true }
func (m *memoryLogHandler) Handle(_ context.Context, r slog.Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logs = append(m.logs, r.Message)
	return nil
}
func (m *memoryLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler { return m }
func (m *memoryLogHandler) WithGroup(name string) slog.Handler       { return m }
func (m *memoryLogHandler) HasMessage(substr string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, l := range m.logs {
		if strings.Contains(l, substr) {
			return true
		}
	}
	return false
}

func (m *memoryLogHandler) CountMessage(substr string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	count := 0
	for _, l := range m.logs {
		if strings.Contains(l, substr) {
			count++
		}
	}
	return count
}

type tcpProxy struct {
	targetAddr string
	listener   net.Listener
	closed     atomic.Bool
	conns      []net.Conn
	mu         sync.Mutex
}

func newTCPProxy(t *testing.T, targetAddr string) *tcpProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	p := &tcpProxy{
		targetAddr: targetAddr,
		listener:   ln,
	}

	go func() {
		for {
			clientConn, err := ln.Accept()
			if err != nil {
				return
			}
			if p.closed.Load() {
				_ = clientConn.Close()
				continue
			}
			targetConn, err := net.DialTimeout("tcp", p.targetAddr, 2*time.Second)
			if err != nil {
				_ = clientConn.Close()
				continue
			}
			p.mu.Lock()
			p.conns = append(p.conns, clientConn, targetConn)
			p.mu.Unlock()

			go func(c1, c2 net.Conn) {
				defer c1.Close()
				defer c2.Close()
				_, _ = io.Copy(c1, c2)
			}(clientConn, targetConn)

			go func(c1, c2 net.Conn) {
				defer c1.Close()
				defer c2.Close()
				_, _ = io.Copy(c2, c1)
			}(clientConn, targetConn)
		}
	}()

	return p
}

func (p *tcpProxy) Addr() string {
	return p.listener.Addr().String()
}

func (p *tcpProxy) Break() {
	p.closed.Store(true)
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		_ = c.Close()
	}
	p.conns = nil
}

func (p *tcpProxy) Resume() {
	p.closed.Store(false)
}

func (p *tcpProxy) Close() {
	p.closed.Store(true)
	_ = p.listener.Close()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		_ = c.Close()
	}
}

func newProxiedClient(t *testing.T, baseClient *goredis.Client, proxy *tcpProxy) *goredis.Client {
	t.Helper()
	opt := *baseClient.Options()
	opt.Addr = proxy.Addr()
	opt.DialTimeout = 1 * time.Second
	opt.ReadTimeout = 5 * time.Second
	opt.WriteTimeout = 1 * time.Second
	opt.MaxRetries = 0
	return goredis.NewClient(&opt)
}

func getTestRedisClient(t *testing.T) *goredis.Client {
	t.Helper()
	redisURL := os.Getenv("TEST_REDIS_URL")
	if redisURL == "" {
		t.Skip("skipping live redis test: TEST_REDIS_URL not set")
	}

	opt, err := goredis.ParseURL(redisURL)
	if err != nil {
		t.Skipf("skipping live redis test: invalid TEST_REDIS_URL: %v", err)
	}

	client := goredis.NewClient(opt)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("skipping live redis test: ping failed: %v", err)
	}

	return client
}

func uniqueTestStream(prefix string) string {
	u, _ := uuid.NewString()
	if len(u) > 8 {
		u = u[:8]
	}
	return fmt.Sprintf("test.%s.%s", prefix, u)
}

func TestConsumer_Live(t *testing.T) {
	client := getTestRedisClient(t)
	defer client.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Run("ensure_group", func(t *testing.T) {
		stream := uniqueTestStream("ensure")
		group := "group-1"
		ctx := context.Background()

		err := infraRedis.EnsureGroup(ctx, client, stream, group)
		require.NoError(t, err)

		err = infraRedis.EnsureGroup(ctx, client, stream, group)
		require.NoError(t, err)

		pub := infraRedis.NewPublisherFromClient(client)
		evtID, _ := uuid.NewString()
		err = pub.Publish(ctx, appOutbox.Event{
			ID:        evtID,
			EventType: stream,
			Payload:   []byte(`{"key":"value"}`),
			CreatedAt: time.Now().UTC(),
		})
		require.NoError(t, err)

		group2 := "group-2"
		err = infraRedis.EnsureGroup(ctx, client, stream, group2)
		require.NoError(t, err)

		readRes, err := client.XReadGroup(ctx, &goredis.XReadGroupArgs{
			Group:    group2,
			Consumer: "c-test",
			Streams:  []string{stream, ">"},
			Count:    10,
		}).Result()
		require.NoError(t, err)
		require.Len(t, readRes, 1)
		require.NotEmpty(t, readRes[0].Messages)
		assert.Equal(t, evtID, readRes[0].Messages[0].Values[infraRedis.FieldEventID])
	})

	t.Run("fan_out_consumer_groups", func(t *testing.T) {
		stream := uniqueTestStream("fanout")
		group1 := "compliance-evaluators"
		group2 := "ai-enrichers"

		pub := infraRedis.NewPublisherFromClient(client)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		const numEvents = 5
		for i := 0; i < numEvents; i++ {
			evtID, _ := uuid.NewString()
			err := pub.Publish(ctx, appOutbox.Event{
				ID:        evtID,
				EventType: stream,
				Payload:   []byte(fmt.Sprintf(`{"index":%d}`, i)),
				CreatedAt: time.Now().UTC(),
			})
			require.NoError(t, err)
		}

		var g1Count, g2Count atomic.Int32
		cfg1 := infraRedis.ConsumerConfig{
			Stream:           stream,
			Group:            group1,
			ConsumerName:     "c1-fanout",
			BatchSize:        10,
			BlockDuration:    500 * time.Millisecond,
			ClaimMinIdle:     1 * time.Second,
			ClaimInterval:    500 * time.Millisecond,
			ClaimBatchSize:   10,
			BaseBackoff:      100 * time.Millisecond,
			MaxBackoff:       1 * time.Second,
			Concurrency:      5,
			QueueSize:        10,
			HandlerTimeout:   500 * time.Millisecond,
			DrainTimeout:     1 * time.Second,
			RetryMaxAttempts: 1,
			RetryBaseBackoff: 10 * time.Millisecond,
			RetryMaxBackoff:  20 * time.Millisecond,
			DLQStore:         &infraRedis.MemoryDLQStore{},
		}
		c1, err := infraRedis.NewConsumer(client, cfg1, logger)
		require.NoError(t, err)

		cfg2 := infraRedis.ConsumerConfig{
			Stream:           stream,
			Group:            group2,
			ConsumerName:     "c2-fanout",
			BatchSize:        10,
			BlockDuration:    500 * time.Millisecond,
			ClaimMinIdle:     1 * time.Second,
			ClaimInterval:    500 * time.Millisecond,
			ClaimBatchSize:   10,
			BaseBackoff:      100 * time.Millisecond,
			MaxBackoff:       1 * time.Second,
			Concurrency:      5,
			QueueSize:        10,
			HandlerTimeout:   500 * time.Millisecond,
			DrainTimeout:     1 * time.Second,
			RetryMaxAttempts: 1,
			RetryBaseBackoff: 10 * time.Millisecond,
			RetryMaxBackoff:  20 * time.Millisecond,
			DLQStore:         &infraRedis.MemoryDLQStore{},
		}
		c2, err := infraRedis.NewConsumer(client, cfg2, logger)
		require.NoError(t, err)

		g, gCtx := errgroup.WithContext(ctx)

		g.Go(func() error {
			return c1.Run(gCtx, func(ctx context.Context, msg appMessaging.Message) error {
				if g1Count.Add(1) == numEvents {
					if g2Count.Load() == numEvents {
						cancel()
					}
				}
				return nil
			})
		})

		g.Go(func() error {
			return c2.Run(gCtx, func(ctx context.Context, msg appMessaging.Message) error {
				if g2Count.Add(1) == numEvents {
					if g1Count.Load() == numEvents {
						cancel()
					}
				}
				return nil
			})
		})

		_ = g.Wait()

		assert.Equal(t, int32(numEvents), g1Count.Load(), "group 1 should receive all events")
		assert.Equal(t, int32(numEvents), g2Count.Load(), "group 2 should receive all events")
	})

	t.Run("crash_before_ack_reclaimed", func(t *testing.T) {
		stream := uniqueTestStream("crash")
		group := "recovery-group"
		ctx := context.Background()

		err := infraRedis.EnsureGroup(ctx, client, stream, group)
		require.NoError(t, err)

		pub := infraRedis.NewPublisherFromClient(client)
		evtID, _ := uuid.NewString()
		err = pub.Publish(ctx, appOutbox.Event{
			ID:        evtID,
			EventType: stream,
			Payload:   []byte(`{"crash":"test"}`),
			CreatedAt: time.Now().UTC(),
		})
		require.NoError(t, err)

		readRes, err := client.XReadGroup(ctx, &goredis.XReadGroupArgs{
			Group:    group,
			Consumer: "c1-crashed",
			Streams:  []string{stream, ">"},
			Count:    1,
		}).Result()
		require.NoError(t, err)
		require.Len(t, readRes[0].Messages, 1)
		msgID := readRes[0].Messages[0].ID

		pend, err := client.XPending(ctx, stream, group).Result()
		require.NoError(t, err)
		assert.Equal(t, int64(1), pend.Count)

		time.Sleep(300 * time.Millisecond)

		var reclaimed atomic.Bool
		reclaimDone := make(chan struct{})

		c2Ctx, c2Cancel := context.WithTimeout(ctx, 3*time.Second)
		defer c2Cancel()

		cfg2 := infraRedis.ConsumerConfig{
			Stream:           stream,
			Group:            group,
			ConsumerName:     "c2-recovering",
			BatchSize:        10,
			BlockDuration:    200 * time.Millisecond,
			ClaimMinIdle:     200 * time.Millisecond,
			ClaimInterval:    100 * time.Millisecond,
			ClaimBatchSize:   10,
			BaseBackoff:      100 * time.Millisecond,
			MaxBackoff:       1 * time.Second,
			Concurrency:      5,
			QueueSize:        10,
			HandlerTimeout:   100 * time.Millisecond,
			DrainTimeout:     1 * time.Second,
			RetryMaxAttempts: 1,
			RetryBaseBackoff: 10 * time.Millisecond,
			RetryMaxBackoff:  20 * time.Millisecond,
			DLQStore:         &infraRedis.MemoryDLQStore{},
		}
		c2, err := infraRedis.NewConsumer(client, cfg2, logger)
		require.NoError(t, err)

		go func() {
			_ = c2.Run(c2Ctx, func(ctx context.Context, msg appMessaging.Message) error {
				if msg.StreamID == msgID && msg.EventID == evtID {
					reclaimed.Store(true)
					close(reclaimDone)
					c2Cancel()
				}
				return nil
			})
		}()

		select {
		case <-reclaimDone:
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for pending message to be reclaimed")
		}

		assert.True(t, reclaimed.Load())

		require.Eventually(t, func() bool {
			pendAfter, err := client.XPending(ctx, stream, group).Result()
			return err == nil && pendAfter.Count == 0
		}, 2*time.Second, 50*time.Millisecond, "expected 0 pending messages after reclaim and ack")
	})

	t.Run("handler_error_redelivered", func(t *testing.T) {
		stream := uniqueTestStream("handlererr")
		group := "error-group"
		ctx := context.Background()

		pub := infraRedis.NewPublisherFromClient(client)
		evtID, _ := uuid.NewString()
		err := pub.Publish(ctx, appOutbox.Event{
			ID:        evtID,
			EventType: stream,
			Payload:   []byte(`{"status":"fail-first"}`),
			CreatedAt: time.Now().UTC(),
		})
		require.NoError(t, err)

		var attempts atomic.Int32
		done := make(chan struct{})
		cCtx, cCancel := context.WithTimeout(ctx, 4*time.Second)
		defer cCancel()

		cfg := infraRedis.ConsumerConfig{
			Stream:           stream,
			Group:            group,
			ConsumerName:     "c-handler-err",
			BatchSize:        10,
			BlockDuration:    200 * time.Millisecond,
			ClaimMinIdle:     1 * time.Second,
			ClaimInterval:    100 * time.Millisecond,
			ClaimBatchSize:   10,
			BaseBackoff:      100 * time.Millisecond,
			MaxBackoff:       1 * time.Second,
			Concurrency:      5,
			QueueSize:        10,
			HandlerTimeout:   100 * time.Millisecond,
			DrainTimeout:     1 * time.Second,
			RetryMaxAttempts: 3,
			RetryBaseBackoff: 10 * time.Millisecond,
			RetryMaxBackoff:  20 * time.Millisecond,
			DLQStore:         &infraRedis.MemoryDLQStore{},
		}
		consumer, err := infraRedis.NewConsumer(client, cfg, logger)
		require.NoError(t, err)

		go func() {
			_ = consumer.Run(cCtx, func(ctx context.Context, msg appMessaging.Message) error {
				count := attempts.Add(1)
				if count == 1 {
					return errors.New("transient handler error")
				}
				close(done)
				cCancel()
				return nil
			})
		}()

		select {
		case <-done:
		case <-time.After(4 * time.Second):
			t.Fatal("timed out waiting for handler error message redelivery")
		}

		assert.GreaterOrEqual(t, attempts.Load(), int32(2))
	})

	t.Run("broker_down_backoff_resume", func(t *testing.T) {
		targetAddr := client.Options().Addr
		proxy := newTCPProxy(t, targetAddr)
		defer proxy.Close()

		proxyClient := newProxiedClient(t, client, proxy)
		defer proxyClient.Close()

		stream := uniqueTestStream("brokerdown")
		group := "outage-group"

		cfg := infraRedis.ConsumerConfig{
			Stream:           stream,
			Group:            group,
			ConsumerName:     "c-broker-down",
			BatchSize:        10,
			BlockDuration:    200 * time.Millisecond,
			ClaimMinIdle:     1 * time.Second,
			ClaimInterval:    500 * time.Millisecond,
			ClaimBatchSize:   10,
			BaseBackoff:      100 * time.Millisecond,
			MaxBackoff:       500 * time.Millisecond,
			Concurrency:      5,
			QueueSize:        10,
			HandlerTimeout:   500 * time.Millisecond,
			DrainTimeout:     1 * time.Second,
			RetryMaxAttempts: 1,
			RetryBaseBackoff: 10 * time.Millisecond,
			RetryMaxBackoff:  20 * time.Millisecond,
			DLQStore:         &infraRedis.MemoryDLQStore{},
		}
		consumer, err := infraRedis.NewConsumer(proxyClient, cfg, logger)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()

		var received atomic.Bool
		done := make(chan struct{})

		go func() {
			_ = consumer.Run(ctx, func(ctx context.Context, msg appMessaging.Message) error {
				if msg.EventID == "evt-recovered" {
					received.Store(true)
					close(done)
					cancel()
				}
				return nil
			})
		}()

		time.Sleep(300 * time.Millisecond)

		proxy.Break()
		time.Sleep(600 * time.Millisecond)

		proxy.Resume()
		time.Sleep(200 * time.Millisecond)

		pub := infraRedis.NewPublisherFromClient(client)
		err = pub.Publish(context.Background(), appOutbox.Event{
			ID:        "evt-recovered",
			EventType: stream,
			Payload:   []byte(`{"recovered":true}`),
			CreatedAt: time.Now().UTC(),
		})
		require.NoError(t, err)

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("consumer failed to back off and recover after broker connectivity restored")
		}

		assert.True(t, received.Load())
	})

	t.Run("startup_broker_down_backoff_resume", func(t *testing.T) {
		targetAddr := client.Options().Addr
		proxy := newTCPProxy(t, targetAddr)
		defer proxy.Close()

		proxy.Break()

		proxyClient := newProxiedClient(t, client, proxy)
		defer proxyClient.Close()

		stream := uniqueTestStream("startupdown")
		group := "startup-group"

		cfg := infraRedis.ConsumerConfig{
			Stream:           stream,
			Group:            group,
			ConsumerName:     "c-startup-down",
			BatchSize:        10,
			BlockDuration:    200 * time.Millisecond,
			ClaimMinIdle:     1 * time.Second,
			ClaimInterval:    500 * time.Millisecond,
			ClaimBatchSize:   10,
			BaseBackoff:      100 * time.Millisecond,
			MaxBackoff:       500 * time.Millisecond,
			Concurrency:      5,
			QueueSize:        10,
			HandlerTimeout:   500 * time.Millisecond,
			DrainTimeout:     1 * time.Second,
			RetryMaxAttempts: 1,
			RetryBaseBackoff: 10 * time.Millisecond,
			RetryMaxBackoff:  20 * time.Millisecond,
			DLQStore:         &infraRedis.MemoryDLQStore{},
		}
		consumer, err := infraRedis.NewConsumer(proxyClient, cfg, logger)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
		defer cancel()

		var running atomic.Bool
		done := make(chan struct{})

		go func() {
			_ = consumer.Run(ctx, func(ctx context.Context, msg appMessaging.Message) error {
				if msg.EventID == "evt-startup-ok" {
					running.Store(true)
					close(done)
					cancel()
				}
				return nil
			})
		}()

		time.Sleep(400 * time.Millisecond)

		proxy.Resume()
		time.Sleep(200 * time.Millisecond)

		pub := infraRedis.NewPublisherFromClient(client)
		err = pub.Publish(context.Background(), appOutbox.Event{
			ID:        "evt-startup-ok",
			EventType: stream,
			Payload:   []byte(`{"startup":true}`),
			CreatedAt: time.Now().UTC(),
		})
		require.NoError(t, err)

		select {
		case <-done:
		case <-time.After(4 * time.Second):
			t.Fatal("consumer failed to back off and start once initial broker connection restored")
		}

		assert.True(t, running.Load())
	})

	t.Run("cancel", func(t *testing.T) {
		stream := uniqueTestStream("cancel")
		group := "cancel-group"
		ctx, cancel := context.WithCancel(context.Background())

		err := infraRedis.EnsureGroup(ctx, client, stream, group)
		require.NoError(t, err)

		pub := infraRedis.NewPublisherFromClient(client)
		evtID, _ := uuid.NewString()
		err = pub.Publish(ctx, appOutbox.Event{
			ID:        evtID,
			EventType: stream,
			Payload:   []byte(`{"cancel":"test"}`),
			CreatedAt: time.Now().UTC(),
		})
		require.NoError(t, err)

		cfg := infraRedis.ConsumerConfig{
			Stream:           stream,
			Group:            group,
			ConsumerName:     "c-cancel",
			BatchSize:        10,
			BlockDuration:    2 * time.Second,
			ClaimMinIdle:     1 * time.Second,
			ClaimInterval:    1 * time.Second,
			ClaimBatchSize:   10,
			BaseBackoff:      100 * time.Millisecond,
			MaxBackoff:       1 * time.Second,
			Concurrency:      5,
			QueueSize:        10,
			HandlerTimeout:   500 * time.Millisecond,
			DrainTimeout:     1 * time.Second,
			RetryMaxAttempts: 1,
			RetryBaseBackoff: 10 * time.Millisecond,
			RetryMaxBackoff:  20 * time.Millisecond,
			DLQStore:         &infraRedis.MemoryDLQStore{},
		}
		consumer, err := infraRedis.NewConsumer(client, cfg, logger)
		require.NoError(t, err)

		runErrCh := make(chan error, 1)
		handlerEntered := make(chan struct{})

		go func() {
			runErrCh <- consumer.Run(ctx, func(handlerCtx context.Context, msg appMessaging.Message) error {
				close(handlerEntered)
				<-handlerCtx.Done()
				return handlerCtx.Err()
			})
		}()

		select {
		case <-handlerEntered:
		case <-time.After(3 * time.Second):
			t.Fatal("handler never entered")
		}

		cancel()

		select {
		case err := <-runErrCh:
			assert.ErrorIs(t, err, context.Canceled)
		case <-time.After(3 * time.Second):
			t.Fatal("consumer did not stop promptly upon context cancellation")
		}

		// The in-flight message was cancelled mid-execution and not acknowledged; must remain in PEL
		pend, err := client.XPending(context.Background(), stream, group).Result()
		require.NoError(t, err)
		assert.Equal(t, int64(1), pend.Count, "in-flight message cancelled mid-execution must remain in PEL")
	})

	t.Run("nogroup_recreated", func(t *testing.T) {
		stream := uniqueTestStream("nogroup")
		group := "transient-group"
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		pub := infraRedis.NewPublisherFromClient(client)
		cfg := infraRedis.ConsumerConfig{
			Stream:           stream,
			Group:            group,
			ConsumerName:     "c-nogroup",
			BatchSize:        10,
			BlockDuration:    200 * time.Millisecond,
			ClaimMinIdle:     500 * time.Millisecond,
			ClaimInterval:    200 * time.Millisecond,
			ClaimBatchSize:   10,
			BaseBackoff:      100 * time.Millisecond,
			MaxBackoff:       1 * time.Second,
			Concurrency:      5,
			QueueSize:        10,
			HandlerTimeout:   200 * time.Millisecond,
			DrainTimeout:     1 * time.Second,
			RetryMaxAttempts: 1,
			RetryBaseBackoff: 10 * time.Millisecond,
			RetryMaxBackoff:  20 * time.Millisecond,
			DLQStore:         &infraRedis.MemoryDLQStore{},
		}
		consumer, err := infraRedis.NewConsumer(client, cfg, logger)
		require.NoError(t, err)

		var receivedEvt1, receivedEvt2 atomic.Bool
		done := make(chan struct{})

		go func() {
			_ = consumer.Run(ctx, func(ctx context.Context, msg appMessaging.Message) error {
				if msg.EventID == "evt-nogroup-1" {
					receivedEvt1.Store(true)
				}
				if msg.EventID == "evt-nogroup-2" {
					receivedEvt2.Store(true)
					close(done)
					cancel()
				}
				return nil
			})
		}()

		err = pub.Publish(ctx, appOutbox.Event{
			ID:        "evt-nogroup-1",
			EventType: stream,
			Payload:   []byte(`{"item":1}`),
			CreatedAt: time.Now().UTC(),
		})
		require.NoError(t, err)

		time.Sleep(300 * time.Millisecond)

		_ = client.XGroupDestroy(ctx, stream, group).Err()

		err = pub.Publish(ctx, appOutbox.Event{
			ID:        "evt-nogroup-2",
			EventType: stream,
			Payload:   []byte(`{"item":2}`),
			CreatedAt: time.Now().UTC(),
		})
		require.NoError(t, err)

		select {
		case <-done:
		case <-time.After(4 * time.Second):
			t.Fatal("consumer failed to recreate group and receive event 2")
		}

		assert.True(t, receivedEvt1.Load(), "should have received evt-nogroup-1")
		assert.True(t, receivedEvt2.Load(), "should have received evt-nogroup-2")
	})

	t.Run("corrupt_message_logged", func(t *testing.T) {
		stream := uniqueTestStream("corrupt")
		group := "corrupt-group"
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()

		err := infraRedis.EnsureGroup(ctx, client, stream, group)
		require.NoError(t, err)

		corruptMsgID, err := client.XAdd(ctx, &goredis.XAddArgs{
			Stream: stream,
			Values: map[string]any{
				"corrupt_field": "no event_id",
			},
		}).Result()
		require.NoError(t, err)

		pub := infraRedis.NewPublisherFromClient(client)
		validID, _ := uuid.NewString()
		err = pub.Publish(ctx, appOutbox.Event{
			ID:        validID,
			EventType: stream,
			Payload:   []byte(`{"valid":true}`),
			CreatedAt: time.Now().UTC(),
		})
		require.NoError(t, err)

		var receivedValid atomic.Bool
		done := make(chan struct{})

		mockDLQ := &infraRedis.MemoryDLQStore{}

		cfg := infraRedis.ConsumerConfig{
			Stream:           stream,
			Group:            group,
			ConsumerName:     "c-corrupt",
			BatchSize:        10,
			BlockDuration:    200 * time.Millisecond,
			ClaimMinIdle:     1 * time.Second,
			ClaimInterval:    500 * time.Millisecond,
			ClaimBatchSize:   10,
			BaseBackoff:      100 * time.Millisecond,
			MaxBackoff:       1 * time.Second,
			Concurrency:      5,
			QueueSize:        10,
			HandlerTimeout:   500 * time.Millisecond,
			DrainTimeout:     1 * time.Second,
			RetryMaxAttempts: 1,
			RetryBaseBackoff: 10 * time.Millisecond,
			RetryMaxBackoff:  20 * time.Millisecond,
			DLQStore:         mockDLQ,
		}
		consumer, err := infraRedis.NewConsumer(client, cfg, logger)
		require.NoError(t, err)

		go func() {
			_ = consumer.Run(ctx, func(ctx context.Context, msg appMessaging.Message) error {
				if msg.EventID == validID {
					receivedValid.Store(true)
					close(done)
					cancel()
				}
				return nil
			})
		}()

		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("consumer failed to process valid message after encountering corrupt message")
		}

		assert.True(t, receivedValid.Load())

		// A4: corrupt message is dead-lettered and acknowledged, not left in PEL
		require.Eventually(t, func() bool {
			pend, err := client.XPending(context.Background(), stream, group).Result()
			return err == nil && pend.Count == 0
		}, 3*time.Second, 50*time.Millisecond)

		dead := mockDLQ.Messages()
		require.Len(t, dead, 1)
		assert.Equal(t, appDLQ.ClassCorruptEnvelope, dead[0].FailureClass)
		assert.Equal(t, corruptMsgID, dead[0].StreamMessageID)
	})

	t.Run("ack_failure_redelivered", func(t *testing.T) {
		stream := uniqueTestStream("ackfail")
		group := "ackfail-group"
		ctx := context.Background()

		err := infraRedis.EnsureGroup(ctx, client, stream, group)
		require.NoError(t, err)

		pub := infraRedis.NewPublisherFromClient(client)
		evtID, _ := uuid.NewString()
		err = pub.Publish(ctx, appOutbox.Event{
			ID:        evtID,
			EventType: stream,
			Payload:   []byte(`{"ack":"fail"}`),
			CreatedAt: time.Now().UTC(),
		})
		require.NoError(t, err)

		proxy := newTCPProxy(t, client.Options().Addr)
		defer proxy.Close()
		proxyClient := newProxiedClient(t, client, proxy)
		defer proxyClient.Close()

		memLog := &memoryLogHandler{}
		testLogger := slog.New(memLog)

		cfg := infraRedis.ConsumerConfig{
			Stream:           stream,
			Group:            group,
			ConsumerName:     "c-ack-fail",
			BatchSize:        10,
			BlockDuration:    100 * time.Millisecond,
			ClaimMinIdle:     150 * time.Millisecond,
			ClaimInterval:    50 * time.Millisecond,
			ClaimBatchSize:   10,
			BaseBackoff:      50 * time.Millisecond,
			MaxBackoff:       200 * time.Millisecond,
			Concurrency:      5,
			QueueSize:        10,
			HandlerTimeout:   50 * time.Millisecond,
			DrainTimeout:     1 * time.Second,
			RetryMaxAttempts: 1,
			RetryBaseBackoff: 10 * time.Millisecond,
			RetryMaxBackoff:  20 * time.Millisecond,
			DLQStore:         &infraRedis.MemoryDLQStore{},
		}
		consumer, err := infraRedis.NewConsumer(proxyClient, cfg, testLogger)
		require.NoError(t, err)

		var attempts atomic.Int32
		done := make(chan struct{})
		cCtx, cCancel := context.WithTimeout(ctx, 4*time.Second)
		defer cCancel()

		go func() {
			_ = consumer.Run(cCtx, func(ctx context.Context, msg appMessaging.Message) error {
				if msg.EventID == evtID {
					count := attempts.Add(1)
					if count == 1 {
						// Handler succeeds, but break proxy connection before XAck executes
						proxy.Break()
						return nil
					}
					close(done)
					cCancel()
				}
				return nil
			})
		}()

		require.Eventually(t, func() bool {
			return memLog.HasMessage("failed to acknowledge processed stream message")
		}, 3*time.Second, 20*time.Millisecond)

		proxy.Resume()

		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for message to be redelivered after XACK failure")
		}

		assert.GreaterOrEqual(t, attempts.Load(), int32(2), "expected handler to be re-invoked on claim redelivery")
		assert.True(t, memLog.HasMessage("failed to acknowledge processed stream message"), "expected ERROR log for failed XACK")

		require.Eventually(t, func() bool {
			pend, err := client.XPending(ctx, stream, group).Result()
			return err == nil && pend.Count == 0
		}, 2*time.Second, 50*time.Millisecond)
	})

	t.Run("handler_panic_recovered", func(t *testing.T) {
		stream := uniqueTestStream("panic")
		group := "panic-group"
		ctx := context.Background()

		pub := infraRedis.NewPublisherFromClient(client)
		evtID, _ := uuid.NewString()
		err := pub.Publish(ctx, appOutbox.Event{
			ID:        evtID,
			EventType: stream,
			Payload:   []byte(`{"panic":"test"}`),
			CreatedAt: time.Now().UTC(),
		})
		require.NoError(t, err)

		var attempts atomic.Int32
		panicked := make(chan struct{})
		cCtx, cCancel := context.WithTimeout(ctx, 4*time.Second)
		defer cCancel()

		mockDLQ := &infraRedis.MemoryDLQStore{}

		cfg := infraRedis.ConsumerConfig{
			Stream:           stream,
			Group:            group,
			ConsumerName:     "c-panic",
			BatchSize:        10,
			BlockDuration:    200 * time.Millisecond,
			ClaimMinIdle:     1 * time.Second,
			ClaimInterval:    100 * time.Millisecond,
			ClaimBatchSize:   10,
			BaseBackoff:      100 * time.Millisecond,
			MaxBackoff:       1 * time.Second,
			Concurrency:      5,
			QueueSize:        10,
			HandlerTimeout:   100 * time.Millisecond,
			DrainTimeout:     1 * time.Second,
			RetryMaxAttempts: 3,
			RetryBaseBackoff: 10 * time.Millisecond,
			RetryMaxBackoff:  20 * time.Millisecond,
			DLQStore:         mockDLQ,
		}
		consumer, err := infraRedis.NewConsumer(client, cfg, logger)
		require.NoError(t, err)

		go func() {
			_ = consumer.Run(cCtx, func(ctx context.Context, msg appMessaging.Message) error {
				attempts.Add(1)
				close(panicked)
				panic("deliberate test panic in handler")
			})
		}()

		select {
		case <-panicked:
		case <-time.After(4 * time.Second):
			t.Fatal("timed out waiting for handler to execute and panic")
		}

		// A3: handler panic is immediately dead-lettered with stack trace and ACKed (never retried)
		require.Eventually(t, func() bool {
			pend, err := client.XPending(ctx, stream, group).Result()
			return err == nil && pend.Count == 0
		}, 3*time.Second, 50*time.Millisecond)

		assert.Equal(t, int32(1), attempts.Load(), "handler panic must not be retried")
		dead := mockDLQ.Messages()
		require.Len(t, dead, 1)
		assert.Equal(t, appDLQ.ClassHandlerPanic, dead[0].FailureClass)
		assert.NotEmpty(t, dead[0].Stack)
	})

	t.Run("trimmed_pending_logged", func(t *testing.T) {
		stream := uniqueTestStream("trimmed")
		group := "trim-group"
		ctx := context.Background()

		err := infraRedis.EnsureGroup(ctx, client, stream, group)
		require.NoError(t, err)

		pub := infraRedis.NewPublisherFromClient(client)
		evtID, _ := uuid.NewString()
		err = pub.Publish(ctx, appOutbox.Event{
			ID:        evtID,
			EventType: stream,
			Payload:   []byte(`{"trim":"test"}`),
			CreatedAt: time.Now().UTC(),
		})
		require.NoError(t, err)

		readRes, err := client.XReadGroup(ctx, &goredis.XReadGroupArgs{
			Group:    group,
			Consumer: "c-old",
			Streams:  []string{stream, ">"},
			Count:    1,
		}).Result()
		require.NoError(t, err)
		require.NotEmpty(t, readRes[0].Messages)
		msgID := readRes[0].Messages[0].ID

		time.Sleep(50 * time.Millisecond)
		cutoffMs := time.Now().Add(10 * time.Second).UnixMilli()
		_ = client.XTrimMinID(ctx, stream, fmt.Sprintf("%d-0", cutoffMs)).Err()

		memLog := &memoryLogHandler{}
		testLogger := slog.New(memLog)

		cfg := infraRedis.ConsumerConfig{
			Stream:           stream,
			Group:            group,
			ConsumerName:     "c-reclaimer",
			BatchSize:        10,
			BlockDuration:    200 * time.Millisecond,
			ClaimMinIdle:     10 * time.Millisecond,
			ClaimInterval:    50 * time.Millisecond,
			ClaimBatchSize:   10,
			BaseBackoff:      100 * time.Millisecond,
			MaxBackoff:       1 * time.Second,
			Concurrency:      5,
			QueueSize:        10,
			HandlerTimeout:   5 * time.Millisecond,
			DrainTimeout:     1 * time.Second,
			RetryMaxAttempts: 1,
			RetryBaseBackoff: 10 * time.Millisecond,
			RetryMaxBackoff:  20 * time.Millisecond,
			DLQStore:         &infraRedis.MemoryDLQStore{},
		}
		consumer, err := infraRedis.NewConsumer(client, cfg, testLogger)
		require.NoError(t, err)

		cCtx, cCancel := context.WithTimeout(ctx, 400*time.Millisecond)
		defer cCancel()
		_ = consumer.Run(cCtx, func(ctx context.Context, msg appMessaging.Message) error {
			return nil
		})

		r, err := client.XRange(ctx, stream, msgID, msgID).Result()
		require.NoError(t, err)
		assert.Empty(t, r)

		assert.True(t, memLog.HasMessage("unacknowledged message was trimmed by stream retention policy"), "expected ERROR log for trimmed pending message ID")
	})

	t.Run("slow_group_lag_loss_logged", func(t *testing.T) {
		ctx := context.Background()

		// Part 1: Newly attached group on an already-trimmed stream must NOT trigger false loss alarm
		streamTrimmed := uniqueTestStream("alreadytrimmed")
		pub := infraRedis.NewPublisherFromClient(client)
		for i := 0; i < 20; i++ {
			err := pub.Publish(ctx, appOutbox.Event{
				ID:        fmt.Sprintf("evt-early-%d", i),
				EventType: streamTrimmed,
				Payload:   []byte(`{}`),
				CreatedAt: time.Now().UTC(),
			})
			require.NoError(t, err)
		}
		time.Sleep(50 * time.Millisecond)
		_ = client.XTrimMinID(ctx, streamTrimmed, fmt.Sprintf("%d-0", time.Now().Add(10*time.Second).UnixMilli())).Err()

		// Create new group on the already-trimmed stream
		groupNew := "new-group-on-trimmed"
		err := infraRedis.EnsureGroup(ctx, client, streamTrimmed, groupNew)
		require.NoError(t, err)

		memLogClean := &memoryLogHandler{}
		cfgClean := infraRedis.ConsumerConfig{
			Stream:           streamTrimmed,
			Group:            groupNew,
			ConsumerName:     "c-clean",
			BatchSize:        10,
			BlockDuration:    50 * time.Millisecond,
			ClaimMinIdle:     50 * time.Millisecond,
			ClaimInterval:    50 * time.Millisecond,
			ClaimBatchSize:   10,
			BaseBackoff:      50 * time.Millisecond,
			MaxBackoff:       100 * time.Millisecond,
			Concurrency:      5,
			QueueSize:        10,
			HandlerTimeout:   20 * time.Millisecond,
			DrainTimeout:     1 * time.Second,
			RetryMaxAttempts: 1,
			RetryBaseBackoff: 10 * time.Millisecond,
			RetryMaxBackoff:  20 * time.Millisecond,
			DLQStore:         &infraRedis.MemoryDLQStore{},
		}
		cClean, err := infraRedis.NewConsumer(client, cfgClean, slog.New(memLogClean))
		require.NoError(t, err)

		cleanCtx, cleanCancel := context.WithTimeout(ctx, 200*time.Millisecond)
		defer cleanCancel()
		_ = cClean.Run(cleanCtx, func(ctx context.Context, msg appMessaging.Message) error {
			return nil
		})
		assert.False(t, memLogClean.HasMessage("consumer group lag exceeded stream retention"), "newly attached group on already-trimmed stream must not false alarm")

		// Part 2: Real lag loss: slow group falls behind retention window
		stream := uniqueTestStream("lagloss")
		group := "slow-group"

		err = infraRedis.EnsureGroup(ctx, client, stream, group)
		require.NoError(t, err)

		// Publish 30 events
		for i := 0; i < 30; i++ {
			err := pub.Publish(ctx, appOutbox.Event{
				ID:        fmt.Sprintf("evt-lag-%d", i),
				EventType: stream,
				Payload:   []byte(`{}`),
				CreatedAt: time.Now().UTC(),
			})
			require.NoError(t, err)
		}

		readRes, err := client.XReadGroup(ctx, &goredis.XReadGroupArgs{
			Group:    group,
			Consumer: "c-slow",
			Streams:  []string{stream, ">"},
			Count:    5,
		}).Result()
		require.NoError(t, err)
		for _, m := range readRes[0].Messages {
			_ = client.XAck(ctx, stream, group, m.ID).Err()
		}

		time.Sleep(50 * time.Millisecond)
		cutoffMs := time.Now().Add(10 * time.Second).UnixMilli()
		_ = client.XTrimMinID(ctx, stream, fmt.Sprintf("%d-0", cutoffMs)).Err()

		memLog := &memoryLogHandler{}
		testLogger := slog.New(memLog)

		cfg := infraRedis.ConsumerConfig{
			Stream:           stream,
			Group:            group,
			ConsumerName:     "c-slow-monitor",
			BatchSize:        10,
			BlockDuration:    50 * time.Millisecond,
			ClaimMinIdle:     50 * time.Millisecond,
			ClaimInterval:    50 * time.Millisecond,
			ClaimBatchSize:   10,
			BaseBackoff:      50 * time.Millisecond,
			MaxBackoff:       100 * time.Millisecond,
			Concurrency:      5,
			QueueSize:        10,
			HandlerTimeout:   20 * time.Millisecond,
			DrainTimeout:     1 * time.Second,
			RetryMaxAttempts: 1,
			RetryBaseBackoff: 10 * time.Millisecond,
			RetryMaxBackoff:  20 * time.Millisecond,
			DLQStore:         &infraRedis.MemoryDLQStore{},
		}
		consumer, err := infraRedis.NewConsumer(client, cfg, testLogger)
		require.NoError(t, err)

		cCtx, cCancel := context.WithTimeout(ctx, 300*time.Millisecond)
		defer cCancel()
		_ = consumer.Run(cCtx, func(ctx context.Context, msg appMessaging.Message) error {
			return nil
		})

		assert.True(t, memLog.HasMessage("consumer group lag exceeded stream retention"), "expected ERROR log for unread entries lost to retention trimming")

		// Part 3: Loss -> Catch-up -> Second loss sequence
		streamMulti := uniqueTestStream("multiloss")
		groupMulti := "multi-loss-group"

		err = infraRedis.EnsureGroup(ctx, client, streamMulti, groupMulti)
		require.NoError(t, err)

		for i := 0; i < 30; i++ {
			err := pub.Publish(ctx, appOutbox.Event{
				ID:        fmt.Sprintf("evt-m1-%d", i),
				EventType: streamMulti,
				Payload:   []byte(`{}`),
				CreatedAt: time.Now().UTC(),
			})
			require.NoError(t, err)
		}
		readRes1, err := client.XReadGroup(ctx, &goredis.XReadGroupArgs{
			Group:    groupMulti,
			Consumer: "c-multi",
			Streams:  []string{streamMulti, ">"},
			Count:    5,
		}).Result()
		require.NoError(t, err)
		for _, m := range readRes1[0].Messages {
			_ = client.XAck(ctx, streamMulti, groupMulti, m.ID).Err()
		}
		time.Sleep(50 * time.Millisecond)
		_ = client.XTrimMinID(ctx, streamMulti, fmt.Sprintf("%d-0", time.Now().Add(10*time.Second).UnixMilli())).Err()

		memLogMulti := &memoryLogHandler{}
		cfgMulti := infraRedis.ConsumerConfig{
			Stream:           streamMulti,
			Group:            groupMulti,
			ConsumerName:     "c-multi-monitor",
			BatchSize:        10,
			BlockDuration:    50 * time.Millisecond,
			ClaimMinIdle:     50 * time.Millisecond,
			ClaimInterval:    50 * time.Millisecond,
			ClaimBatchSize:   10,
			BaseBackoff:      50 * time.Millisecond,
			MaxBackoff:       100 * time.Millisecond,
			Concurrency:      5,
			QueueSize:        10,
			HandlerTimeout:   20 * time.Millisecond,
			DrainTimeout:     1 * time.Second,
			RetryMaxAttempts: 1,
			RetryBaseBackoff: 10 * time.Millisecond,
			RetryMaxBackoff:  20 * time.Millisecond,
			DLQStore:         &infraRedis.MemoryDLQStore{},
		}
		cMulti, err := infraRedis.NewConsumer(client, cfgMulti, slog.New(memLogMulti))
		require.NoError(t, err)

		ctxM1, cancelM1 := context.WithTimeout(ctx, 200*time.Millisecond)
		defer cancelM1()
		_ = cMulti.Run(ctxM1, func(ctx context.Context, msg appMessaging.Message) error {
			return nil
		})
		assert.Equal(t, 1, memLogMulti.CountMessage("consumer group lag exceeded stream retention"), "first loss must be logged")

		for i := 0; i < 10; i++ {
			err := pub.Publish(ctx, appOutbox.Event{
				ID:        fmt.Sprintf("evt-m2-%d", i),
				EventType: streamMulti,
				Payload:   []byte(`{}`),
				CreatedAt: time.Now().UTC(),
			})
			require.NoError(t, err)
		}
		ctxCatchUp, cancelCatchUp := context.WithTimeout(ctx, 200*time.Millisecond)
		defer cancelCatchUp()
		_ = cMulti.Run(ctxCatchUp, func(ctx context.Context, msg appMessaging.Message) error {
			return nil
		})
		assert.Equal(t, 1, memLogMulti.CountMessage("consumer group lag exceeded stream retention"))

		// Second loss (15) is kept smaller than first loss (25) to verify reset after catching up.
		for i := 0; i < 20; i++ {
			err := pub.Publish(ctx, appOutbox.Event{
				ID:        fmt.Sprintf("evt-m3-%d", i),
				EventType: streamMulti,
				Payload:   []byte(`{}`),
				CreatedAt: time.Now().UTC(),
			})
			require.NoError(t, err)
		}
		readRes3, err := client.XReadGroup(ctx, &goredis.XReadGroupArgs{
			Group:    groupMulti,
			Consumer: "c-multi-manual",
			Streams:  []string{streamMulti, ">"},
			Count:    5,
		}).Result()
		require.NoError(t, err)
		for _, m := range readRes3[0].Messages {
			_ = client.XAck(ctx, streamMulti, groupMulti, m.ID).Err()
		}
		time.Sleep(50 * time.Millisecond)
		_ = client.XTrimMinID(ctx, streamMulti, fmt.Sprintf("%d-0", time.Now().Add(10*time.Second).UnixMilli())).Err()

		ctxM2, cancelM2 := context.WithTimeout(ctx, 200*time.Millisecond)
		defer cancelM2()
		_ = cMulti.Run(ctxM2, func(ctx context.Context, msg appMessaging.Message) error {
			return nil
		})
		assert.Equal(t, 2, memLogMulti.CountMessage("consumer group lag exceeded stream retention"), "second loss after catch-up must be logged")
	})

	t.Run("backpressure_bounds_reads", func(t *testing.T) {
		stream := uniqueTestStream("backpressure")
		group := "bp-group"
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		err := infraRedis.EnsureGroup(ctx, client, stream, group)
		require.NoError(t, err)

		pub := infraRedis.NewPublisherFromClient(client)
		const totalEvents = 20
		for i := 0; i < totalEvents; i++ {
			err := pub.Publish(ctx, appOutbox.Event{
				ID:        fmt.Sprintf("evt-bp-%d", i),
				EventType: stream,
				Payload:   []byte(`{"bp":true}`),
				CreatedAt: time.Now().UTC(),
			})
			require.NoError(t, err)
		}

		// Concurrency = 2, QueueSize = 2, BatchSize = 2
		// Max in memory at once = 2 + 2 + 2 = 6
		cfg := infraRedis.ConsumerConfig{
			Stream:           stream,
			Group:            group,
			ConsumerName:     "c-backpressure",
			BatchSize:        2,
			BlockDuration:    100 * time.Millisecond,
			ClaimMinIdle:     5 * time.Second,
			ClaimInterval:    5 * time.Second,
			ClaimBatchSize:   2,
			BaseBackoff:      50 * time.Millisecond,
			MaxBackoff:       100 * time.Millisecond,
			Concurrency:      2,
			QueueSize:        2,
			HandlerTimeout:   4 * time.Second,
			DrainTimeout:     2 * time.Second,
			RetryMaxAttempts: 1,
			RetryBaseBackoff: 10 * time.Millisecond,
			RetryMaxBackoff:  20 * time.Millisecond,
			DLQStore:         &infraRedis.MemoryDLQStore{},
		}
		consumer, err := infraRedis.NewConsumer(client, cfg, logger)
		require.NoError(t, err)

		handlerBlocked := make(chan struct{})
		var handlerBlockedOnce sync.Once
		releaseHandlers := make(chan struct{})
		var activeHandlers, processed atomic.Int32

		go func() {
			_ = consumer.Run(ctx, func(handlerCtx context.Context, msg appMessaging.Message) error {
				count := activeHandlers.Add(1)
				// After release, the count can reach 2 again; a second close would panic
				// and leave that message pending past the assertion window.
				if count == 2 {
					handlerBlockedOnce.Do(func() { close(handlerBlocked) })
				}
				<-releaseHandlers
				activeHandlers.Add(-1)
				processed.Add(1)
				return nil
			})
		}()

		select {
		case <-handlerBlocked:
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for worker pool concurrency to fill")
		}

		// Let the consumer attempt to read further while workers are blocked
		time.Sleep(300 * time.Millisecond)

		// Check PEL in Redis: the 2 running workers have read messages, and at most batch + queue messages were read
		pend, err := client.XPending(ctx, stream, group).Result()
		require.NoError(t, err)
		assert.LessOrEqual(t, pend.Count, int64(6), "backpressure must bound in-flight reads; remaining events must stay unread in Redis")

		// Release workers so all remaining events are processed and ACKed
		close(releaseHandlers)

		require.Eventually(t, func() bool {
			return processed.Load() == int32(totalEvents)
		}, 5*time.Second, 50*time.Millisecond, "all events must eventually be processed after backpressure is relieved")

		require.Eventually(t, func() bool {
			p, err := client.XPending(ctx, stream, group).Result()
			return err == nil && p.Count == 0
		}, 3*time.Second, 50*time.Millisecond, "all events must be acknowledged")
	})

	t.Run("no_self_reclaim_in_flight", func(t *testing.T) {
		stream := uniqueTestStream("noselfreclaim")
		group := "self-reclaim-group"
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err := infraRedis.EnsureGroup(ctx, client, stream, group)
		require.NoError(t, err)

		pub := infraRedis.NewPublisherFromClient(client)
		const numEvents = 5
		for i := 0; i < numEvents; i++ {
			err = pub.Publish(ctx, appOutbox.Event{
				ID:        fmt.Sprintf("evt-self-%d", i),
				EventType: stream,
				Payload:   []byte(`{"self_reclaim":true}`),
				CreatedAt: time.Now().UTC(),
			})
			require.NoError(t, err)
		}

		// Concurrency=1, QueueSize=10: messages 1..4 wait in queue while message 0 runs.
		// ClaimMinIdle (200ms) > HandlerTimeout (150ms).
		// Each task runs for 80ms; total sequential queue drain takes ~400ms.
		// After 200ms, queued messages exceed ClaimMinIdle and trigger claim sweeps every 30ms.
		cfg := infraRedis.ConsumerConfig{
			Stream:           stream,
			Group:            group,
			ConsumerName:     "c-self-reclaim",
			BatchSize:        10,
			BlockDuration:    50 * time.Millisecond,
			ClaimMinIdle:     200 * time.Millisecond,
			ClaimInterval:    30 * time.Millisecond,
			ClaimBatchSize:   10,
			BaseBackoff:      50 * time.Millisecond,
			MaxBackoff:       100 * time.Millisecond,
			Concurrency:      1,
			QueueSize:        10,
			HandlerTimeout:   150 * time.Millisecond,
			DrainTimeout:     2 * time.Second,
			RetryMaxAttempts: 1,
			RetryBaseBackoff: 10 * time.Millisecond,
			RetryMaxBackoff:  20 * time.Millisecond,
			DLQStore:         &infraRedis.MemoryDLQStore{},
		}
		consumer, err := infraRedis.NewConsumer(client, cfg, logger)
		require.NoError(t, err)

		var runsPerEvent sync.Map
		var totalRuns atomic.Int32
		allDone := make(chan struct{})

		go func() {
			_ = consumer.Run(ctx, func(hCtx context.Context, msg appMessaging.Message) error {
				countVal, _ := runsPerEvent.LoadOrStore(msg.EventID, new(atomic.Int32))
				countVal.(*atomic.Int32).Add(1)

				// Each task sleeps 80ms; by message 3, messages 3 and 4 have waited > 200ms in queue.
				time.Sleep(80 * time.Millisecond)

				if totalRuns.Add(1) == int32(numEvents) {
					close(allDone)
				}
				return nil
			})
		}()

		select {
		case <-allDone:
		case <-time.After(4 * time.Second):
			t.Fatal("timed out waiting for all messages to process")
		}

		// Give time for any erroneous duplicate claims to manifest
		time.Sleep(200 * time.Millisecond)

		assert.Equal(t, int32(numEvents), totalRuns.Load(), "every message must be processed exactly once; in-flight guard must prevent self-reclaim")
		for i := 0; i < numEvents; i++ {
			evtID := fmt.Sprintf("evt-self-%d", i)
			countVal, ok := runsPerEvent.Load(evtID)
			require.True(t, ok, "event %s must have run", evtID)
			assert.Equal(t, int32(1), countVal.(*atomic.Int32).Load(), "event %s must run exactly once", evtID)
		}
	})

	t.Run("handler_timeout_leaves_pending", func(t *testing.T) {
		stream := uniqueTestStream("handlertimeout")
		group := "ht-group"
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err := infraRedis.EnsureGroup(ctx, client, stream, group)
		require.NoError(t, err)

		pub := infraRedis.NewPublisherFromClient(client)
		evtID, _ := uuid.NewString()
		err = pub.Publish(ctx, appOutbox.Event{
			ID:        evtID,
			EventType: stream,
			Payload:   []byte(`{"timeout":true}`),
			CreatedAt: time.Now().UTC(),
		})
		require.NoError(t, err)

		cfg := infraRedis.ConsumerConfig{
			Stream:           stream,
			Group:            group,
			ConsumerName:     "c-timeout",
			BatchSize:        10,
			BlockDuration:    50 * time.Millisecond,
			ClaimMinIdle:     3 * time.Second,
			ClaimInterval:    500 * time.Millisecond,
			ClaimBatchSize:   10,
			BaseBackoff:      50 * time.Millisecond,
			MaxBackoff:       100 * time.Millisecond,
			Concurrency:      1,
			QueueSize:        1,
			HandlerTimeout:   100 * time.Millisecond,
			DrainTimeout:     1 * time.Second,
			RetryMaxAttempts: 3,
			RetryBaseBackoff: 500 * time.Millisecond,
			RetryMaxBackoff:  1 * time.Second,
			DLQStore:         &infraRedis.MemoryDLQStore{},
		}
		consumer, err := infraRedis.NewConsumer(client, cfg, logger)
		require.NoError(t, err)

		handlerTimedOut := make(chan error, 1)
		go func() {
			_ = consumer.Run(ctx, func(hCtx context.Context, msg appMessaging.Message) error {
				<-hCtx.Done()
				select {
				case handlerTimedOut <- hCtx.Err():
				default:
				}
				return hCtx.Err()
			})
		}()

		select {
		case err := <-handlerTimedOut:
			assert.ErrorIs(t, err, context.DeadlineExceeded)
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for handler timeout")
		}

		// Timed-out message was not ACKed; must stay pending in PEL
		pend, err := client.XPending(context.Background(), stream, group).Result()
		require.NoError(t, err)
		assert.Equal(t, int64(1), pend.Count, "timed-out message must remain in PEL")
		cancel()
	})

	t.Run("graceful_drain_acks", func(t *testing.T) {
		stream := uniqueTestStream("drainack")
		group := "drainack-group"
		ctx := context.Background()

		err := infraRedis.EnsureGroup(ctx, client, stream, group)
		require.NoError(t, err)

		pub := infraRedis.NewPublisherFromClient(client)
		const numEvents = 5
		for i := 0; i < numEvents; i++ {
			err := pub.Publish(ctx, appOutbox.Event{
				ID:        fmt.Sprintf("evt-drain-%d", i),
				EventType: stream,
				Payload:   []byte(`{"drain":true}`),
				CreatedAt: time.Now().UTC(),
			})
			require.NoError(t, err)
		}

		cfg := infraRedis.ConsumerConfig{
			Stream:           stream,
			Group:            group,
			ConsumerName:     "c-drain",
			BatchSize:        10,
			BlockDuration:    100 * time.Millisecond,
			ClaimMinIdle:     5 * time.Second,
			ClaimInterval:    5 * time.Second,
			ClaimBatchSize:   10,
			BaseBackoff:      50 * time.Millisecond,
			MaxBackoff:       100 * time.Millisecond,
			Concurrency:      2,
			QueueSize:        5,
			HandlerTimeout:   2 * time.Second,
			DrainTimeout:     3 * time.Second,
			RetryMaxAttempts: 1,
			RetryBaseBackoff: 10 * time.Millisecond,
			RetryMaxBackoff:  20 * time.Millisecond,
			DLQStore:         &infraRedis.MemoryDLQStore{},
		}
		consumer, err := infraRedis.NewConsumer(client, cfg, logger)
		require.NoError(t, err)

		cCtx, cCancel := context.WithCancel(ctx)
		var processed atomic.Int32
		runErr := make(chan error, 1)

		go func() {
			runErr <- consumer.Run(cCtx, func(hCtx context.Context, msg appMessaging.Message) error {
				time.Sleep(50 * time.Millisecond)
				if processed.Add(1) == 2 {
					cCancel() // Cancel consumer context while tasks are in flight and in queue
				}
				return nil
			})
		}()

		select {
		case err := <-runErr:
			assert.ErrorIs(t, err, context.Canceled)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for consumer to stop")
		}

		assert.Equal(t, int32(numEvents), processed.Load(), "all dispatched/queued tasks should finish draining")

		// All drained messages should be acknowledged
		pend, err := client.XPending(ctx, stream, group).Result()
		require.NoError(t, err)
		assert.Equal(t, int64(0), pend.Count, "all drained messages must be acknowledged")
	})

	t.Run("drain_timeout_leaves_pending", func(t *testing.T) {
		stream := uniqueTestStream("draintimeout")
		group := "draintimeout-group"
		ctx := context.Background()

		err := infraRedis.EnsureGroup(ctx, client, stream, group)
		require.NoError(t, err)

		pub := infraRedis.NewPublisherFromClient(client)
		err = pub.Publish(ctx, appOutbox.Event{
			ID:        "evt-slow-drain",
			EventType: stream,
			Payload:   []byte(`{"slow":true}`),
			CreatedAt: time.Now().UTC(),
		})
		require.NoError(t, err)

		cfg := infraRedis.ConsumerConfig{
			Stream:           stream,
			Group:            group,
			ConsumerName:     "c-drain-timeout",
			BatchSize:        10,
			BlockDuration:    50 * time.Millisecond,
			ClaimMinIdle:     6 * time.Second,
			ClaimInterval:    5 * time.Second,
			ClaimBatchSize:   10,
			BaseBackoff:      50 * time.Millisecond,
			MaxBackoff:       100 * time.Millisecond,
			Concurrency:      1,
			QueueSize:        1,
			HandlerTimeout:   5 * time.Second,
			DrainTimeout:     50 * time.Millisecond,
			RetryMaxAttempts: 1,
			RetryBaseBackoff: 10 * time.Millisecond,
			RetryMaxBackoff:  20 * time.Millisecond,
			DLQStore:         &infraRedis.MemoryDLQStore{},
		}
		consumer, err := infraRedis.NewConsumer(client, cfg, logger)
		require.NoError(t, err)

		cCtx, cCancel := context.WithCancel(ctx)
		runErr := make(chan error, 1)
		handlerEntered := make(chan struct{})

		go func() {
			runErr <- consumer.Run(cCtx, func(hCtx context.Context, msg appMessaging.Message) error {
				close(handlerEntered)
				<-hCtx.Done()
				return hCtx.Err()
			})
		}()

		select {
		case <-handlerEntered:
		case <-time.After(3 * time.Second):
			t.Fatal("handler never entered")
		}

		cCancel()

		select {
		case err := <-runErr:
			assert.ErrorIs(t, err, worker.ErrDrainTimeout)
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for consumer shutdown")
		}

		pend, err := client.XPending(ctx, stream, group).Result()
		require.NoError(t, err)
		assert.Equal(t, int64(1), pend.Count, "un-drained message must remain pending")
	})

	t.Run("idempotent_cross_consumer", func(t *testing.T) {
		pgConnStr := os.Getenv("TEST_DATABASE_URL")
		if pgConnStr == "" {
			t.Skip("skipping postgres integration part of test: TEST_DATABASE_URL not set")
		}

		ctx := context.Background()

		pgCtx, pgCancel := context.WithTimeout(ctx, 10*time.Second)
		defer pgCancel()

		pgPool, err := infraPostgres.NewPool(pgCtx, pgConnStr,
			infraPostgres.WithConnectTimeout(3*time.Second),
			infraPostgres.WithMaxConns(10),
			infraPostgres.WithMinConns(2),
		)
		require.NoError(t, err)
		defer pgPool.Close()

		migrator, err := infraPostgres.NewMigrator(pgPool, migrations.FS)
		require.NoError(t, err)
		require.NoError(t, migrator.Up(pgCtx))

		idempotencyRepo, err := infraPostgres.NewIdempotencyRepository(pgPool)
		require.NoError(t, err)

		_, err = pgPool.Exec(pgCtx, `
			CREATE TABLE IF NOT EXISTS test_e2e_effects (
				id TEXT PRIMARY KEY,
				event_id TEXT NOT NULL,
				consumer TEXT NOT NULL,
				created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
			);
			TRUNCATE test_e2e_effects;
		`)
		require.NoError(t, err)
		t.Cleanup(func() {
			cCtx, cCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cCancel()
			_, _ = pgPool.Exec(cCtx, "DROP TABLE IF EXISTS test_e2e_effects")
		})

		stream := uniqueTestStream("idempe2e")
		group := "e2e-consumer-group"
		eventID, err := uuid.NewString()
		require.NoError(t, err)
		eventID = "evt-e2e-" + eventID

		guard, err := appIdempotency.NewGuard(idempotencyRepo, 10*time.Second, slog.Default())
		require.NoError(t, err)

		var executionCount int32
		c1InHandler := make(chan struct{})
		var c1Signaled sync.Once
		c1AllowCommit := make(chan struct{})

		// The first invocation deliberately overruns HandlerTimeout (80ms): it ignores ctx while
		// blocked and writes on a detached context. This simulates a handler that outlives its
		// timeout (or waited in the queue) past ClaimMinIdle, the case where another consumer
		// XAUTOCLAIMs a message that is still running. Well-behaved handlers honour ctx.
		rawHandler := func(ctx context.Context, msg appMessaging.Message) error {
			atomic.AddInt32(&executionCount, 1)

			c1Signaled.Do(func() {
				close(c1InHandler)
				<-c1AllowCommit
			})

			dbCtx, dbCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer dbCancel()

			tx, err := pgPool.Begin(dbCtx)
			if err != nil {
				return err
			}
			defer func() { _ = tx.Rollback(dbCtx) }()

			_, err = tx.Exec(dbCtx,
				"INSERT INTO test_e2e_effects (id, event_id, consumer) VALUES ($1, $2, $3)",
				"effect-"+msg.EventID, msg.EventID, "worker-1",
			)
			if err != nil {
				return err
			}

			txRepo := idempotencyRepo.WithTx(tx)
			if err := appIdempotency.CompleteInTx(dbCtx, txRepo); err != nil {
				return err
			}

			return tx.Commit(dbCtx)

		}

		wrappedHandler, err := guard.Wrap(group, rawHandler)
		require.NoError(t, err)

		// 1. Publish event to stream
		pub := infraRedis.NewPublisherFromClient(client)
		payload := []byte(`{"order_id":"123","amount":"99.99"}`)
		err = pub.Publish(ctx, appOutbox.Event{
			ID:        eventID,
			EventType: stream,
			Payload:   payload,
			CreatedAt: time.Now().UTC(),
		})
		require.NoError(t, err)

		// 2. Start Consumer 1: processes the event and pauses in rawHandler before commit
		cfg1 := infraRedis.ConsumerConfig{
			Stream:           stream,
			Group:            group,
			ConsumerName:     "consumer-1",
			BatchSize:        10,
			BlockDuration:    100 * time.Millisecond,
			ClaimMinIdle:     500 * time.Millisecond,
			ClaimInterval:    10 * time.Second,
			ClaimBatchSize:   10,
			BaseBackoff:      50 * time.Millisecond,
			MaxBackoff:       200 * time.Millisecond,
			HandlerTimeout:   300 * time.Millisecond,
			Concurrency:      2,
			QueueSize:        2,
			DrainTimeout:     2 * time.Second,
			RetryMaxAttempts: 1,
			RetryBaseBackoff: 10 * time.Millisecond,
			RetryMaxBackoff:  20 * time.Millisecond,
			DLQStore:         &infraRedis.MemoryDLQStore{},
		}
		c1, err := infraRedis.NewConsumer(client, cfg1, slog.Default())
		require.NoError(t, err)

		c1Ctx, c1Cancel := context.WithCancel(ctx)
		defer c1Cancel()

		c1Done := make(chan struct{})
		go func() {
			defer close(c1Done)
			_ = c1.Run(c1Ctx, func(ctx context.Context, msg appMessaging.Message) error {
				hErr := wrappedHandler(ctx, msg)
				if hErr == nil {
					c1Cancel()
				}
				return hErr
			})
		}()

		// Wait until consumer-1 enters handler and acquires Postgres idempotency lease
		select {
		case <-c1InHandler:
		case <-time.After(5 * time.Second):
			t.Fatal("consumer-1 timed out entering handler")
		}

		// Verify idempotency key is IN_PROGRESS in Postgres
		rec, err := idempotencyRepo.GetKey(ctx, group, eventID)
		require.NoError(t, err)
		assert.Equal(t, "IN_PROGRESS", rec.Status)

		// Allow idle time to exceed consumer-2's ClaimMinIdle (200ms)
		time.Sleep(250 * time.Millisecond)

		// 3. Start Consumer 2: claims the message via XAUTOCLAIM while consumer-1 is still processing!
		c2ConflictSeen := make(chan struct{}, 1)
		cfg2 := infraRedis.ConsumerConfig{
			Stream:           stream,
			Group:            group,
			ConsumerName:     "consumer-2",
			BatchSize:        10,
			BlockDuration:    50 * time.Millisecond,
			ClaimMinIdle:     200 * time.Millisecond,
			ClaimInterval:    30 * time.Millisecond,
			ClaimBatchSize:   10,
			BaseBackoff:      50 * time.Millisecond,
			MaxBackoff:       200 * time.Millisecond,
			HandlerTimeout:   150 * time.Millisecond,
			Concurrency:      2,
			QueueSize:        2,
			DrainTimeout:     2 * time.Second,
			RetryMaxAttempts: 1,
			RetryBaseBackoff: 10 * time.Millisecond,
			RetryMaxBackoff:  20 * time.Millisecond,
			DLQStore:         &infraRedis.MemoryDLQStore{},
		}
		c2, err := infraRedis.NewConsumer(client, cfg2, slog.Default())
		require.NoError(t, err)

		c2Ctx, c2Cancel := context.WithCancel(ctx)
		defer c2Cancel()

		c2Done := make(chan struct{})
		go func() {
			defer close(c2Done)
			_ = c2.Run(c2Ctx, func(ctx context.Context, msg appMessaging.Message) error {
				hErr := wrappedHandler(ctx, msg)
				if errors.Is(hErr, appIdempotency.ErrKeyInProgress) {
					select {
					case c2ConflictSeen <- struct{}{}:
					default:
					}
				}
				if hErr == nil {
					c2Cancel()
				}
				return hErr
			})
		}()

		// Assert consumer-2 attempted to process the autoclaimed message and got ErrKeyInProgress!
		select {
		case <-c2ConflictSeen:
		case <-time.After(5 * time.Second):
			t.Fatal("consumer-2 timed out waiting for concurrent claim collision (ErrKeyInProgress)")
		}

		// 4. Now release consumer-1 to complete its transaction and commit to Postgres
		close(c1AllowCommit)

		select {
		case <-c1Done:
		case <-time.After(5 * time.Second):
			t.Fatal("consumer-1 timed out after unblocking commit")
		}

		assert.Equal(t, int32(1), atomic.LoadInt32(&executionCount), "handler must be executed once by consumer-1")

		// 5. Force redelivery of identical event to stream (simulating outbox republishing or broker redelivery)
		err = pub.Publish(ctx, appOutbox.Event{
			ID:        eventID,
			EventType: stream,
			Payload:   payload,
			CreatedAt: time.Now().UTC(),
		})
		require.NoError(t, err)

		// Consumer-2 will read the redelivery; this time key is COMPLETED so it skips and ACKs
		select {
		case <-c2Done:
		case <-time.After(5 * time.Second):
			t.Fatal("consumer-2 timed out finishing after consumer-1 completed")
		}

		// Verify execution count remains exactly 1!
		assert.Equal(t, int32(1), atomic.LoadInt32(&executionCount), "handler must NOT run again on redelivery / autoclaim")

		// Exactly one effect row in PostgreSQL
		var effectRows int
		err = pgPool.QueryRow(ctx, "SELECT COUNT(*) FROM test_e2e_effects WHERE event_id = $1", eventID).Scan(&effectRows)
		require.NoError(t, err)
		assert.Equal(t, 1, effectRows, "exactly one effect row must be committed in DB")

		// Stream message was acknowledged (no pending messages in PEL)
		pend, err := client.XPending(ctx, stream, group).Result()
		require.NoError(t, err)
		assert.Equal(t, int64(0), pend.Count, "autoclaimed message must be ACKed after skip")
	})

}

func TestPublisher_Live(t *testing.T) {
	client := getTestRedisClient(t)
	defer client.Close()

	ctx := context.Background()

	t.Run("retention_minid", func(t *testing.T) {
		stream := uniqueTestStream("retention")

		pubNoRetention := infraRedis.NewPublisherFromClient(client)
		for i := 0; i < 250; i++ {
			err := pubNoRetention.Publish(ctx, appOutbox.Event{
				ID:        fmt.Sprintf("evt-old-%d", i),
				EventType: stream,
				Payload:   []byte(`{"old":true}`),
				CreatedAt: time.Now().UTC(),
			})
			require.NoError(t, err)
		}

		lenBefore, err := client.XLen(ctx, stream).Result()
		require.NoError(t, err)
		assert.Equal(t, int64(250), lenBefore)

		time.Sleep(150 * time.Millisecond)

		pubWithRetention := infraRedis.NewPublisherFromClient(client, infraRedis.WithRetention(100*time.Millisecond))
		err = pubWithRetention.Publish(ctx, appOutbox.Event{
			ID:        "evt-new",
			EventType: stream,
			Payload:   []byte(`{"new":true}`),
			CreatedAt: time.Now().UTC(),
		})
		require.NoError(t, err)

		lenAfter, err := client.XLen(ctx, stream).Result()
		require.NoError(t, err)
		assert.Less(t, lenAfter, int64(251), "expected publisher retention trimming to reduce stream length")
		assert.LessOrEqual(t, lenAfter, int64(150), "expected publisher approximate trimming to evict older macro-nodes")
	})
}
