package redis

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	appMessaging "github.com/ayo6706/cross-border-ecommerce/internal/application/messaging"
	appOutbox "github.com/ayo6706/cross-border-ecommerce/internal/application/outbox"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/uuid"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/worker"
	goredis "github.com/redis/go-redis/v9"
)

var (
	ErrInvalidConsumerConfig = errors.New("invalid consumer configuration")
	ErrNilConsumerClient     = errors.New("redis client cannot be nil")
	ErrNilConsumerLogger     = errors.New("logger cannot be nil")
	ErrNilHandler            = errors.New("message handler cannot be nil")
)

type ConsumerConfig struct {
	Stream         string
	Group          string
	ConsumerName   string
	BatchSize      int
	BlockDuration  time.Duration
	ClaimMinIdle   time.Duration
	ClaimInterval  time.Duration
	ClaimBatchSize int
	BaseBackoff    time.Duration
	MaxBackoff     time.Duration
	Concurrency    int
	QueueSize      int
	HandlerTimeout time.Duration
	DrainTimeout   time.Duration
}

func GenerateConsumerName(prefix string) (string, error) {
	if strings.TrimSpace(prefix) == "" {
		h, err := os.Hostname()
		if err != nil || strings.TrimSpace(h) == "" {
			prefix = "worker"
		} else {
			prefix = h
		}
	}
	u, err := uuid.NewString()
	if err != nil {
		return "", fmt.Errorf("generate consumer UUID: %w", err)
	}
	if len(u) > 8 {
		u = u[:8]
	}
	return fmt.Sprintf("%s-%s", prefix, u), nil
}

func (c ConsumerConfig) Validate() error {
	if strings.TrimSpace(c.Stream) == "" {
		return fmt.Errorf("%w: stream name cannot be empty", ErrInvalidConsumerConfig)
	}
	if strings.TrimSpace(c.Group) == "" {
		return fmt.Errorf("%w: consumer group cannot be empty", ErrInvalidConsumerConfig)
	}
	if strings.TrimSpace(c.ConsumerName) == "" {
		return fmt.Errorf("%w: consumer name cannot be empty", ErrInvalidConsumerConfig)
	}
	if c.BatchSize <= 0 || c.BatchSize > 1000 {
		return fmt.Errorf("%w: batch size must be between 1 and 1000, got %d", ErrInvalidConsumerConfig, c.BatchSize)
	}
	if c.BlockDuration <= 0 {
		return fmt.Errorf("%w: block duration must be strictly positive", ErrInvalidConsumerConfig)
	}
	if c.ClaimMinIdle <= 0 {
		return fmt.Errorf("%w: claim min idle must be strictly positive", ErrInvalidConsumerConfig)
	}
	if c.ClaimInterval <= 0 {
		return fmt.Errorf("%w: claim interval must be strictly positive", ErrInvalidConsumerConfig)
	}
	if c.ClaimBatchSize <= 0 || c.ClaimBatchSize > 1000 {
		return fmt.Errorf("%w: claim batch size must be between 1 and 1000, got %d", ErrInvalidConsumerConfig, c.ClaimBatchSize)
	}
	if c.BaseBackoff <= 0 {
		return fmt.Errorf("%w: base backoff must be strictly positive", ErrInvalidConsumerConfig)
	}
	if c.MaxBackoff < c.BaseBackoff {
		return fmt.Errorf("%w: max backoff cannot be less than base backoff", ErrInvalidConsumerConfig)
	}
	if c.Concurrency <= 0 {
		return fmt.Errorf("%w: concurrency must be strictly positive, got %d", ErrInvalidConsumerConfig, c.Concurrency)
	}
	if c.QueueSize < 0 {
		return fmt.Errorf("%w: queue size cannot be negative, got %d", ErrInvalidConsumerConfig, c.QueueSize)
	}
	if c.HandlerTimeout <= 0 {
		return fmt.Errorf("%w: handler timeout must be strictly positive", ErrInvalidConsumerConfig)
	}
	if c.DrainTimeout <= 0 {
		return fmt.Errorf("%w: drain timeout must be strictly positive", ErrInvalidConsumerConfig)
	}
	if c.ClaimMinIdle <= c.HandlerTimeout {
		return fmt.Errorf("%w: claim min idle (%v) must be strictly greater than handler timeout (%v)",
			ErrInvalidConsumerConfig, c.ClaimMinIdle, c.HandlerTimeout)
	}
	return nil
}

// EnsureGroup creates a consumer group starting at "0" with MKSTREAM.
// If the stream already has trimmed entries, it initializes EntriesRead to the trimmed
// count so existing history trimming is not reported as unread loss for the new group.
// If the group already exists (BUSYGROUP error), it returns nil harmlessly.
func EnsureGroup(ctx context.Context, client *goredis.Client, stream, group string) error {
	if client == nil {
		return fmt.Errorf("%w: redis client is nil", appMessaging.ErrBrokerUnavailable)
	}
	if strings.TrimSpace(stream) == "" {
		return errors.New("stream name cannot be empty")
	}
	if strings.TrimSpace(group) == "" {
		return errors.New("consumer group cannot be empty")
	}

	// Note: We query the stream's trimmed entries count and seed ENTRIESREAD so that
	// a newly created group on an already-trimmed stream is not falsely detected as
	// having lost unread entries. If stream trimming occurs between XInfoStream and
	// XGroupCreate, trimmedCount may slightly underestimate trimmed entries.
	var trimmedCount int64
	streamInfo, err := client.XInfoStream(ctx, stream).Result()
	if err == nil && streamInfo != nil {
		trimmedCount = streamInfo.EntriesAdded - streamInfo.Length
	}

	var createErr error
	if trimmedCount > 0 {
		createErr = client.Do(ctx, "XGROUP", "CREATE", stream, group, "0", "MKSTREAM", "ENTRIESREAD", trimmedCount).Err()
	} else {
		createErr = client.XGroupCreateMkStream(ctx, stream, group, "0").Err()
	}

	if createErr == nil {
		return nil
	}

	if strings.Contains(strings.ToUpper(createErr.Error()), "BUSYGROUP") {
		return nil
	}

	return ClassifyRedisError(createErr, ctx.Err())
}

type Consumer struct {
	client           *goredis.Client
	cfg              ConsumerConfig
	logger           *slog.Logger
	claimStart       string
	lastReportedLost int64
	inFlightMu       sync.Mutex
	inFlight         map[string]struct{}
}

func NewConsumer(client *goredis.Client, cfg ConsumerConfig, logger *slog.Logger) (*Consumer, error) {
	if client == nil {
		return nil, ErrNilConsumerClient
	}
	if logger == nil {
		return nil, ErrNilConsumerLogger
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &Consumer{
		client:     client,
		cfg:        cfg,
		logger:     logger,
		claimStart: "0-0",
		inFlight:   make(map[string]struct{}),
	}, nil
}

func (c *Consumer) checkLagLoss(ctx context.Context) {
	streamInfo, err := c.client.XInfoStream(ctx, c.cfg.Stream).Result()
	if err != nil {
		if !errors.Is(err, goredis.Nil) && !strings.Contains(strings.ToUpper(err.Error()), "NOSUCHKEY") {
			c.logger.Error("failed to query stream info for lag loss audit", slog.String("stream", c.cfg.Stream), slog.Any("error", err))
		}
		return
	}
	groups, err := c.client.XInfoGroups(ctx, c.cfg.Stream).Result()
	if err != nil {
		c.logger.Error("failed to query consumer groups for lag loss audit", slog.String("stream", c.cfg.Stream), slog.Any("error", err))
		return
	}
	trimmedCount := streamInfo.EntriesAdded - streamInfo.Length
	for _, g := range groups {
		if g.Name == c.cfg.Group {
			var lost int64
			if g.EntriesRead >= 0 && trimmedCount > g.EntriesRead {
				lost = trimmedCount - g.EntriesRead
			}
			if lost > c.lastReportedLost {
				c.logger.Error("consumer group lag exceeded stream retention; unread entries permanently lost",
					slog.String("stream", c.cfg.Stream),
					slog.String("group", c.cfg.Group),
					slog.Int64("lost_entries", lost),
					slog.Int64("trimmed_entries", trimmedCount),
					slog.Int64("entries_read", g.EntriesRead),
				)
			}
			c.lastReportedLost = lost
			break
		}
	}
}

func (c *Consumer) dispatchMessage(ctx context.Context, pool *worker.Pool, handler appMessaging.Handler, rawMsg goredis.XMessage) error {
	c.inFlightMu.Lock()
	if _, exists := c.inFlight[rawMsg.ID]; exists {
		c.inFlightMu.Unlock()
		return nil
	}
	c.inFlight[rawMsg.ID] = struct{}{}
	c.inFlightMu.Unlock()

	msgToProcess := rawMsg
	err := pool.Dispatch(ctx, func(taskCtx context.Context) {
		defer func() {
			c.inFlightMu.Lock()
			delete(c.inFlight, msgToProcess.ID)
			c.inFlightMu.Unlock()
		}()
		c.processMessage(taskCtx, handler, msgToProcess)
	})
	if err != nil {
		c.inFlightMu.Lock()
		delete(c.inFlight, msgToProcess.ID)
		c.inFlightMu.Unlock()
		return err
	}
	return nil
}

func (c *Consumer) claimPending(ctx context.Context, pool *worker.Pool, handler appMessaging.Handler) error {
	c.checkLagLoss(ctx)

	msgs, nextStart, deleted, err := c.client.XAutoClaimWithDeleted(ctx, &goredis.XAutoClaimArgs{
		Stream:   c.cfg.Stream,
		Group:    c.cfg.Group,
		Consumer: c.cfg.ConsumerName,
		MinIdle:  c.cfg.ClaimMinIdle,
		Start:    c.claimStart,
		Count:    int64(c.cfg.ClaimBatchSize),
	}).Result()

	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return nil
		}
		return err
	}

	if len(deleted) > 0 {
		for _, delID := range deleted {
			c.logger.Error("unacknowledged message was trimmed by stream retention policy",
				slog.String("stream", c.cfg.Stream),
				slog.String("group", c.cfg.Group),
				slog.String("stream_id", delID),
			)
		}
	}

	if nextStart != "" {
		c.claimStart = nextStart
	} else {
		c.claimStart = "0-0"
	}

	for _, rawMsg := range msgs {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := c.dispatchMessage(ctx, pool, handler, rawMsg); err != nil {
			return err
		}
	}

	return nil
}

func (c *Consumer) runHandler(ctx context.Context, handler appMessaging.Handler, msg appMessaging.Message) (err error) {
	defer func() {
		if r := recover(); r != nil {
			stack := debug.Stack()
			c.logger.Error("message handler panicked",
				slog.String("event_id", msg.EventID),
				slog.String("stream_id", msg.StreamID),
				slog.Any("panic", r),
				slog.String("stack", string(stack)),
			)
			err = fmt.Errorf("handler panic: %v", r)
		}
	}()
	return handler(ctx, msg)
}

func (c *Consumer) processMessage(taskCtx context.Context, handler appMessaging.Handler, rawMsg goredis.XMessage) {
	msg, err := DecodeMessage(c.cfg.Stream, rawMsg)
	if err != nil {
		c.logger.Error("failed to decode stream message envelope",
			slog.String("stream_id", rawMsg.ID),
			slog.String("stream", c.cfg.Stream),
			slog.Any("error", err),
		)
		return
	}

	handlerCtx, handlerCancel := context.WithTimeout(taskCtx, c.cfg.HandlerTimeout)
	defer handlerCancel()

	if handlerErr := c.runHandler(handlerCtx, handler, msg); handlerErr != nil {
		c.logger.Error("message handler returned error; leaving unacknowledged for redelivery",
			slog.String("event_id", msg.EventID),
			slog.String("stream_id", msg.StreamID),
			slog.String("stream", c.cfg.Stream),
			slog.String("group", c.cfg.Group),
			slog.Any("error", handlerErr),
		)
		return
	}

	ackCtx, ackCancel := context.WithTimeout(context.WithoutCancel(taskCtx), 5*time.Second)
	defer ackCancel()

	if ackErr := c.client.XAck(ackCtx, c.cfg.Stream, c.cfg.Group, rawMsg.ID).Err(); ackErr != nil {
		c.logger.Error("failed to acknowledge processed stream message",
			slog.String("event_id", msg.EventID),
			slog.String("stream_id", msg.StreamID),
			slog.String("stream", c.cfg.Stream),
			slog.String("group", c.cfg.Group),
			slog.Any("error", ackErr),
		)
	}
}

func (c *Consumer) handleStreamError(ctx context.Context, err error, consecutiveErrors *int) {
	if err == nil || errors.Is(err, goredis.Nil) {
		return
	}

	errMsg := strings.ToUpper(err.Error())
	if strings.Contains(errMsg, "NOGROUP") {
		c.logger.Error("consumer group missing; recreating group starting at 0 (replays retained stream history)",
			slog.String("stream", c.cfg.Stream),
			slog.String("group", c.cfg.Group),
			slog.Any("error", err),
		)
		if ensureErr := EnsureGroup(ctx, c.client, c.cfg.Stream, c.cfg.Group); ensureErr != nil {
			*consecutiveErrors++
			bo := appOutbox.Backoff(*consecutiveErrors-1, c.cfg.BaseBackoff, c.cfg.MaxBackoff)
			c.logger.Error("failed to recreate consumer group, backing off",
				slog.Int("consecutive_errors", *consecutiveErrors),
				slog.Duration("backoff", bo),
				slog.Any("error", ensureErr),
			)
			timer := time.NewTimer(bo)
			select {
			case <-ctx.Done():
				timer.Stop()
			case <-timer.C:
			}
		}
		return
	}

	classified := ClassifyRedisError(err, ctx.Err())
	*consecutiveErrors++
	bo := appOutbox.Backoff(*consecutiveErrors-1, c.cfg.BaseBackoff, c.cfg.MaxBackoff)
	c.logger.Error("stream consumer error, backing off",
		slog.Int("consecutive_errors", *consecutiveErrors),
		slog.Duration("backoff", bo),
		slog.Any("error", classified),
	)

	timer := time.NewTimer(bo)
	select {
	case <-ctx.Done():
		timer.Stop()
	case <-timer.C:
	}
}

func (c *Consumer) ensureGroupWithBackoff(ctx context.Context) error {
	consecutiveErrors := 0
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err := EnsureGroup(ctx, c.client, c.cfg.Stream, c.cfg.Group)
		if err == nil {
			return nil
		}
		if !errors.Is(err, appMessaging.ErrBrokerUnavailable) {
			return err
		}
		consecutiveErrors++
		bo := appOutbox.Backoff(consecutiveErrors-1, c.cfg.BaseBackoff, c.cfg.MaxBackoff)
		c.logger.Error("broker unavailable while ensuring consumer group, backing off",
			slog.String("stream", c.cfg.Stream),
			slog.String("group", c.cfg.Group),
			slog.Int("consecutive_errors", consecutiveErrors),
			slog.Duration("backoff", bo),
			slog.Any("error", err),
		)
		timer := time.NewTimer(bo)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (c *Consumer) Run(ctx context.Context, handler appMessaging.Handler) (returnErr error) {
	if handler == nil {
		return ErrNilHandler
	}

	if err := c.ensureGroupWithBackoff(ctx); err != nil {
		return err
	}

	pool, err := worker.NewPool(ctx, c.cfg.Concurrency, c.cfg.QueueSize, c.logger)
	if err != nil {
		return fmt.Errorf("create worker pool: %w", err)
	}
	defer func() {
		shutdownErr := pool.Shutdown(c.cfg.DrainTimeout)
		if shutdownErr != nil {
			if returnErr == nil {
				returnErr = shutdownErr
			} else {
				returnErr = errors.Join(returnErr, shutdownErr)
			}
		}
	}()

	claimTicker := time.NewTicker(c.cfg.ClaimInterval)
	defer claimTicker.Stop()

	// Initial sweep for abandoned messages from previous consumer crashes
	if err := c.claimPending(ctx, pool, handler); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		c.logger.Warn("initial claim sweep encountered error", slog.Any("error", err))
	}

	consecutiveErrors := 0

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		select {
		case <-claimTicker.C:
			if err := c.claimPending(ctx, pool, handler); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				c.handleStreamError(ctx, err, &consecutiveErrors)
			}
		default:
		}

		streams, err := c.client.XReadGroup(ctx, &goredis.XReadGroupArgs{
			Group:    c.cfg.Group,
			Consumer: c.cfg.ConsumerName,
			Streams:  []string{c.cfg.Stream, ">"},
			Count:    int64(c.cfg.BatchSize),
			Block:    c.cfg.BlockDuration,
		}).Result()

		if err != nil {
			if errors.Is(err, goredis.Nil) {
				continue
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			c.handleStreamError(ctx, err, &consecutiveErrors)
			continue
		}

		consecutiveErrors = 0

		for _, stream := range streams {
			for _, rawMsg := range stream.Messages {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if err := c.dispatchMessage(ctx, pool, handler, rawMsg); err != nil {
					if ctx.Err() != nil {
						return ctx.Err()
					}
					return err
				}
			}
		}
	}
}
