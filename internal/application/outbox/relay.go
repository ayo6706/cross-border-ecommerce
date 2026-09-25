package outbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	appMessaging "github.com/ayo6706/cross-border-ecommerce/internal/application/messaging"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/uuid"
)

var (
	ErrInvalidConfig = errors.New("invalid relay configuration")
	ErrNilStore      = errors.New("store cannot be nil")
	ErrNilPublisher  = errors.New("publisher cannot be nil")
	ErrNilLogger     = errors.New("logger cannot be nil")
)

type Event struct {
	ID            string
	AggregateType string
	AggregateID   string
	EventType     string
	Payload       []byte
	RetryCount    int
	CreatedAt     time.Time
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
	if c.BatchSize <= 0 || c.BatchSize > 1000 {
		return fmt.Errorf("%w: batch size must be between 1 and 1000, got %d", ErrInvalidConfig, c.BatchSize)
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

func Backoff(attempt int, base, max time.Duration) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	if attempt >= 62 {
		return max
	}
	multiplier := time.Duration(1) << attempt
	res := base * multiplier
	if res <= 0 || res > max || res/multiplier != base { // overflow guard
		return max
	}
	return res
}

// settleOnCancel marks what reached the broker and releases the unprocessed claims so another
// relay can take them now instead of after the lease. It uses a detached context because the
// caller's is already cancelled, and attempts both writes even if one fails.
func (r *Relay) settleOnCancel(ctx context.Context, claimToken string, publishedIDs, unprocessedIDs []string) error {
	settleCtx, settleCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
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
	for i, e := range events {
		ids[i] = e.ID
	}
	return ids
}

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
			allErrs := append(errs, ctx.Err(), settleErr)
			return len(events), len(publishedIDs), errors.Join(allErrs...)
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

			allErrs := append(errs, fmt.Errorf("publish event %s: %w", event.ID, pubErr), markErr, relErr)
			return len(events), len(publishedIDs), errors.Join(allErrs...)
		}

		if ctx.Err() != nil {
			settleErr := r.settleOnCancel(ctx, claimToken, publishedIDs, eventIDs(events[i:]))
			allErrs := append(errs, ctx.Err(), settleErr)
			return len(events), len(publishedIDs), errors.Join(allErrs...)
		}

		bo := Backoff(event.RetryCount, r.cfg.BaseBackoff, r.cfg.MaxBackoff)
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
	for _, e := range events[1:] {
		if e.CreatedAt.Before(minCreatedAt) {
			minCreatedAt = e.CreatedAt
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

func (r *Relay) Run(ctx context.Context) error {
	consecutiveFailures := 0

	for {
		if ctx.Err() != nil {
			return nil
		}

		claimed, _, err := r.RunOnce(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			consecutiveFailures++
			bo := Backoff(consecutiveFailures-1, r.cfg.BaseBackoff, r.cfg.MaxBackoff)
			r.logger.Error("outbox relay batch failed, backing off",
				slog.Int("consecutive_failures", consecutiveFailures),
				slog.Duration("backoff", bo),
				slog.Any("error", err),
			)

			timer := time.NewTimer(bo)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil
			case <-timer.C:
			}
			continue
		}

		consecutiveFailures = 0

		if claimed == r.cfg.BatchSize {
			continue
		}

		timer := time.NewTimer(r.cfg.PollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}
