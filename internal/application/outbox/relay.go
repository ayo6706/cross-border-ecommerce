package outbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	appMessaging "github.com/ayo6706/cross-border-ecommerce/internal/application/messaging"
	platformBackoff "github.com/ayo6706/cross-border-ecommerce/internal/platform/backoff"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/cleanup"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/uuid"
)

// maxBatchSize bounds the events one claim statement locks and one RunOnce publishes.
const maxBatchSize = 1000

var (
	ErrInvalidConfig = errors.New("invalid relay configuration")
	ErrNilStore      = errors.New("store cannot be nil")
	ErrNilPublisher  = errors.New("publisher cannot be nil")
	ErrNilLogger     = errors.New("logger cannot be nil")
	ErrClaimLost     = errors.New("outbox event claim lost")
)

type Event struct {
	ID            string
	EventID       string
	AggregateType string
	AggregateID   string
	EventType     string
	Payload       []byte
	RetryCount    int
	CreatedAt     time.Time
	TargetGroup   string
}

type Publisher interface {
	Publish(ctx context.Context, e Event) error
}

type Store interface {
	ClaimBatch(ctx context.Context, claimToken string, limit int, lease time.Duration) ([]Event, error)
	MarkPublished(ctx context.Context, claimToken string, ids []string) (int64, error)
	RecordFailure(ctx context.Context, claimToken, id, cause string, maxAttempts int, backoff time.Duration) error
	Release(ctx context.Context, claimToken string, ids []string) error
}

type RelayConfig struct {
	BatchSize    int
	PollInterval time.Duration
	Lease        time.Duration
	BaseBackoff  time.Duration
	MaxBackoff   time.Duration
	MaxAttempts  int
}

func (c RelayConfig) Validate() error {
	if c.BatchSize <= 0 || c.BatchSize > maxBatchSize {
		return fmt.Errorf("%w: batch size must be between 1 and %d, got %d", ErrInvalidConfig, maxBatchSize, c.BatchSize)
	}
	if c.PollInterval <= 0 {
		return fmt.Errorf("%w: poll interval must be strictly positive", ErrInvalidConfig)
	}
	if c.Lease <= 0 {
		return fmt.Errorf("%w: lease must be strictly positive", ErrInvalidConfig)
	}
	if c.BaseBackoff <= 0 {
		return fmt.Errorf("%w: base backoff must be strictly positive", ErrInvalidConfig)
	}
	if c.MaxBackoff < c.BaseBackoff {
		return fmt.Errorf("%w: max backoff cannot be less than base backoff", ErrInvalidConfig)
	}
	if c.MaxAttempts < 1 {
		return fmt.Errorf("%w: max attempts must be at least 1, got %d", ErrInvalidConfig, c.MaxAttempts)
	}
	return nil
}

type Relay struct {
	store  Store
	pub    Publisher
	cfg    RelayConfig
	logger *slog.Logger
}

func NewRelay(store Store, pub Publisher, cfg RelayConfig, logger *slog.Logger) (*Relay, error) {
	if store == nil {
		return nil, ErrNilStore
	}
	if pub == nil {
		return nil, ErrNilPublisher
	}
	if logger == nil {
		return nil, ErrNilLogger
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &Relay{
		store:  store,
		pub:    pub,
		cfg:    cfg,
		logger: logger,
	}, nil
}

// settleOnCancel marks what reached the broker and releases the unprocessed claims so another
// relay can take them now instead of after the lease. It uses a detached context because the
// caller's is already cancelled, and attempts both writes even if one fails.
func (r *Relay) settleOnCancel(ctx context.Context, claimToken string, publishedIDs, unprocessedIDs []string) error {
	settleCtx, settleCancel := cleanup.Context(ctx)
	defer settleCancel()

	var markErr, relErr error
	if len(publishedIDs) > 0 {
		if _, err := r.store.MarkPublished(settleCtx, claimToken, publishedIDs); err != nil {
			r.logger.Error("failed to mark published events on context cancellation", slog.Any("error", err))
			markErr = fmt.Errorf("mark published on cancel: %w", err)
		}
	}
	if len(unprocessedIDs) > 0 {
		if err := r.store.Release(settleCtx, claimToken, unprocessedIDs); err != nil {
			r.logger.Error("failed to release outbox claims on context cancellation", slog.Any("error", err))
			relErr = fmt.Errorf("release claims on cancel: %w", err)
		}
	}
	return errors.Join(markErr, relErr)
}

func eventIDs(events []Event) []string {
	ids := make([]string, len(events))
	for i := range events {
		ids[i] = events[i].ID
	}
	return ids
}

//nolint:funlen,gocognit // legacy baseline 2026-09-26: fix in ENG-051
func (r *Relay) RunOnce(ctx context.Context) (claimed, published int, err error) {
	claimToken, err := uuid.NewString()
	if err != nil {
		return 0, 0, fmt.Errorf("generate claim token: %w", err)
	}

	events, err := r.store.ClaimBatch(ctx, claimToken, r.cfg.BatchSize, r.cfg.Lease)
	if err != nil {
		return 0, 0, fmt.Errorf("claim outbox batch: %w", err)
	}
	if len(events) == 0 {
		return 0, 0, nil
	}

	publishedIDs := make([]string, 0, len(events))
	var errs []error

	for i := 0; i < len(events); i++ {
		if ctx.Err() != nil {
			settleErr := r.settleOnCancel(ctx, claimToken, publishedIDs, eventIDs(events[i:]))
			return len(events), len(publishedIDs), errors.Join(errors.Join(errs...), ctx.Err(), settleErr)
		}

		event := events[i]
		pubErr := r.pub.Publish(ctx, event)
		if pubErr == nil {
			publishedIDs = append(publishedIDs, event.ID)
			continue
		}

		if errors.Is(pubErr, appMessaging.ErrBrokerUnavailable) {
			var markErr error
			if len(publishedIDs) > 0 {
				if _, err := r.store.MarkPublished(ctx, claimToken, publishedIDs); err != nil {
					markErr = fmt.Errorf("mark published before release: %w", err)
					r.logger.Error("failed to mark published events before broker backoff", slog.Any("error", err))
				}
			}

			var relErr error
			if err := r.store.Release(ctx, claimToken, eventIDs(events[i:])); err != nil {
				relErr = fmt.Errorf("release claims on broker outage: %w", err)
				r.logger.Error("failed to release outbox claims after broker outage", slog.Any("error", err))
			}

			return len(events), len(publishedIDs), errors.Join(
				errors.Join(errs...),
				fmt.Errorf("publish event %s: %w", event.ID, pubErr),
				markErr,
				relErr,
			)
		}

		if ctx.Err() != nil {
			settleErr := r.settleOnCancel(ctx, claimToken, publishedIDs, eventIDs(events[i:]))
			return len(events), len(publishedIDs), errors.Join(errors.Join(errs...), ctx.Err(), settleErr)
		}

		bo := platformBackoff.Exponential(event.RetryCount, r.cfg.BaseBackoff, r.cfg.MaxBackoff)
		if recErr := r.store.RecordFailure(ctx, claimToken, event.ID, pubErr.Error(), r.cfg.MaxAttempts, bo); recErr != nil {
			r.logger.Error("failed to record outbox event failure",
				slog.String("event_id", event.ID),
				slog.Any("error", recErr),
			)
			errs = append(errs, fmt.Errorf("record failure for event %s: %w", event.ID, recErr))
		}
	}

	if len(publishedIDs) > 0 {
		affected, markErr := r.store.MarkPublished(ctx, claimToken, publishedIDs)
		if markErr != nil {
			errs = append(errs, fmt.Errorf("mark outbox events published: %w", markErr))
		} else if affected < int64(len(publishedIDs)) {
			r.logger.Warn("outbox claim lease expired before marking published; potential duplicate delivery",
				slog.Int64("affected", affected),
				slog.Int("expected", len(publishedIDs)),
			)
		}
	}

	minCreatedAt := events[0].CreatedAt
	for i := 1; i < len(events); i++ {
		if events[i].CreatedAt.Before(minCreatedAt) {
			minCreatedAt = events[i].CreatedAt
		}
	}
	oldestAge := time.Since(minCreatedAt)
	r.logger.Info("relayed outbox batch",
		slog.Int("claimed", len(events)),
		slog.Int("published", len(publishedIDs)),
		slog.Int("failed", len(events)-len(publishedIDs)),
		slog.Duration("oldest_age", oldestAge),
	)

	return len(events), len(publishedIDs), errors.Join(errs...)
}

// Run relays batches until ctx is cancelled. A cancellation that interrupts a batch is a normal
// shutdown and returns nil, but a real failure in that batch (e.g. claims that could not be
// released) is returned, never swallowed.
func (r *Relay) Run(ctx context.Context) error {
	consecutiveFailures := 0

	for ctx.Err() == nil {
		claimed, _, err := r.RunOnce(ctx)
		if err != nil && ctx.Err() != nil {
			return withoutCancellation(err)
		}
		if err != nil {
			consecutiveFailures++
			r.waitAfterFailure(ctx, consecutiveFailures, err)
			continue
		}

		consecutiveFailures = 0
		if claimed < r.cfg.BatchSize {
			sleep(ctx, r.cfg.PollInterval)
		}
	}
	return nil
}

func (r *Relay) waitAfterFailure(ctx context.Context, consecutiveFailures int, err error) {
	bo := platformBackoff.Exponential(consecutiveFailures-1, r.cfg.BaseBackoff, r.cfg.MaxBackoff)
	r.logger.Error("outbox relay batch failed, backing off",
		slog.Int("consecutive_failures", consecutiveFailures),
		slog.Duration("backoff", bo),
		slog.Any("error", err),
	)
	sleep(ctx, bo)
}

// sleep waits for d or until ctx is done, whichever comes first.
func sleep(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

// withoutCancellation returns nil when err is only cancellation, and err otherwise.
func withoutCancellation(err error) error {
	if isOnlyContextCanceled(err) {
		return nil
	}
	return err
}

// isOnlyContextCanceled reports whether every leaf of err's tree is context.Canceled. It walks
// both single (%w) and joined wraps, so a real error wrapped around a join is still seen.
func isOnlyContextCanceled(err error) bool {
	switch e := err.(type) {
	case nil:
		return false
	case interface{ Unwrap() []error }:
		for _, inner := range e.Unwrap() {
			if !isOnlyContextCanceled(inner) {
				return false
			}
		}
		return true
	case interface{ Unwrap() error }:
		if inner := e.Unwrap(); inner != nil {
			return isOnlyContextCanceled(inner)
		}
	}
	return errors.Is(err, context.Canceled)
}
