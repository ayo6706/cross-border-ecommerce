package idempotency

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/application/messaging"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/uuid"
)

type claimContextKey struct{}

// WithClaim injects the Claim pointer into context so transactional operations can complete it.
func WithClaim(ctx context.Context, claim *Claim) context.Context {
	return context.WithValue(ctx, claimContextKey{}, claim)
}

// FromContext retrieves the active Claim pointer from context, if present.
func FromContext(ctx context.Context) (*Claim, bool) {
	c, ok := ctx.Value(claimContextKey{}).(*Claim)
	return c, ok && c != nil
}

// CompleteInTx is a helper for transactional handlers to mark the active claim completed within their transaction.
func CompleteInTx(ctx context.Context, completer Completer) error {
	if completer == nil {
		return errors.New("idempotency completer cannot be nil")
	}
	claim, ok := FromContext(ctx)
	if !ok {
		return errors.New("no idempotency claim found in context")
	}
	if err := completer.CompleteKey(ctx, claim.Scope, claim.Key, claim.LeaseToken); err != nil {
		return err
	}
	claim.MarkCompleted()
	return nil
}

// Guard wraps messaging handlers with database-backed idempotency protection.
type Guard struct {
	store    Store
	leaseTTL time.Duration
	logger   *slog.Logger
}

func NewGuard(store Store, leaseTTL time.Duration, logger *slog.Logger) (*Guard, error) {
	if store == nil {
		return nil, errors.New("idempotency store cannot be nil")
	}
	if leaseTTL <= 0 {
		return nil, fmt.Errorf("%w: lease TTL must be strictly positive", ErrInvalidLeaseTTL)
	}
	if logger == nil {
		return nil, ErrNilLogger
	}
	return &Guard{
		store:    store,
		leaseTTL: leaseTTL,
		logger:   logger,
	}, nil
}

// Wrap wraps a messaging.Handler with idempotency enforcement.
//
//nolint:funlen,gocognit // legacy baseline 2026-09-26: fix in ENG-025
func (g *Guard) Wrap(scope string, next messaging.Handler) (messaging.Handler, error) {
	if strings.TrimSpace(scope) == "" {
		return nil, ErrInvalidScope
	}
	if next == nil {
		return nil, errors.New("downstream handler cannot be nil")
	}

	return func(ctx context.Context, msg messaging.Message) error {
		if strings.TrimSpace(msg.EventID) == "" {
			return ErrInvalidKey
		}

		hash := sha256.Sum256(msg.Payload)
		payloadHash := hash[:]
		token, err := uuid.NewString()
		if err != nil {
			return fmt.Errorf("generate lease token: %w", err)
		}

		claim, err := g.store.ClaimKey(ctx, scope, msg.EventID, payloadHash, token, g.leaseTTL)
		if err != nil {
			if errors.Is(err, ErrPayloadMismatch) {
				g.logger.ErrorContext(ctx, "idempotency payload hash mismatch",
					"scope", scope,
					"key", msg.EventID,
					"stream", msg.Stream,
				)
				return err
			}
			if errors.Is(err, ErrKeyInProgress) {
				return err
			}
			return fmt.Errorf("claim idempotency key: %w", err)
		}

		if claim.Status == ClaimAlreadyCompleted {
			g.logger.DebugContext(ctx, "idempotency key already completed, skipping handler",
				"scope", scope,
				"key", msg.EventID,
			)
			return nil
		}

		claimPtr := &claim
		ctxWithClaim := WithClaim(ctx, claimPtr)

		defer func() {
			if r := recover(); r != nil {
				g.release(ctx, scope, msg.EventID, token, "handler panic")
				panic(r)
			}
		}()

		handleErr := next(ctxWithClaim, msg)
		if handleErr != nil {
			g.release(ctx, scope, msg.EventID, token, "handler error")
			return handleErr
		}

		if !claimPtr.IsCompleted() {
			g.release(ctx, scope, msg.EventID, token, "handler returned without CompleteInTx")
			return ErrNotCompletedInTx
		}

		return nil
	}, nil
}

// release expires the lease early so the next delivery can claim it without waiting out the TTL.
// It runs on a detached context because the handler's context may already be cancelled.
func (g *Guard) release(ctx context.Context, scope, key, token, reason string) {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := g.store.ReleaseKey(releaseCtx, scope, key, token); err != nil {
		g.logger.WarnContext(ctx, "failed to release idempotency lease; retry waits for lease expiry",
			"scope", scope,
			"key", key,
			"reason", reason,
			"error", err,
		)
	}
}
