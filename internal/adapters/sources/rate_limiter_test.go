package sources_test

import (
	"context"
	"testing"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/adapters/sources"
)

func TestTokenBucketLimiter_BurstAndThrottle(t *testing.T) {
	t.Run("BurstAllowedInstantly", func(t *testing.T) {
		limiter, err := sources.NewTokenBucketLimiter(10, 5)
		if err != nil {
			t.Fatalf("unexpected error creating limiter: %v", err)
		}

		ctx := context.Background()
		start := time.Now()
		for range 5 {
			if err := limiter.Wait(ctx); err != nil {
				t.Fatalf("unexpected error during burst: %v", err)
			}
		}

		duration := time.Since(start)
		if duration > 100*time.Millisecond {
			t.Errorf("expected burst to be nearly instant, took %v", duration)
		}
	})

	t.Run("ContextCancellationAbortsWait", func(t *testing.T) {
		limiter, err := sources.NewTokenBucketLimiter(1, 1)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		ctx := context.Background()
		// Exhaust burst token
		if err := limiter.Wait(ctx); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Next call should wait 1 second; cancel after 50ms
		timedCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		err = limiter.Wait(timedCtx)
		if err == nil {
			t.Fatal("expected context deadline error, got nil")
		}
	})

	t.Run("NilLimiter", func(t *testing.T) {
		var nilLimiter *sources.TokenBucketLimiter
		if err := nilLimiter.Wait(context.Background()); err != nil {
			t.Errorf("expected nil limiter to return nil, got %v", err)
		}
	})

	t.Run("ContextCancellationRestoresToken", func(t *testing.T) {
		// Rate = 2 tokens/sec (1 token per 500ms), capacity = 2
		limiter, err := sources.NewTokenBucketLimiter(2, 2)
		if err != nil {
			t.Fatalf("unexpected error creating limiter: %v", err)
		}

		ctx := context.Background()
		// Exhaust both burst tokens
		if err := limiter.Wait(ctx); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if err := limiter.Wait(ctx); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Next call needs to wait 500ms for 1 token. Cancel it after 20ms.
		timedCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
		defer cancel()

		if err := limiter.Wait(timedCtx); err == nil {
			t.Fatal("expected context deadline error, got nil")
		}

		// Because token was restored upon cancellation, the deficit is 1 token (500ms)
		// rather than 2 tokens (1000ms).
		// Wait 520ms to allow exactly 1 token to refill into the bucket.
		time.Sleep(520 * time.Millisecond)

		start := time.Now()
		if err := limiter.Wait(ctx); err != nil {
			t.Fatalf("unexpected error after cancellation: %v", err)
		}
		elapsed := time.Since(start)
		if elapsed > 100*time.Millisecond {
			t.Errorf("expected immediate acquisition (<100ms) after refill of restored token, took %v", elapsed)
		}
	})

	t.Run("InvalidConstructorArgs", func(t *testing.T) {
		_, err := sources.NewTokenBucketLimiter(0, 10)
		if err == nil {
			t.Fatal("expected error with 0 rate, got nil")
		}

		_, err = sources.NewTokenBucketLimiter(-5, 10)
		if err == nil {
			t.Fatal("expected error with negative rate, got nil")
		}
	})

	t.Run("ConcurrentWaitersStaggerDelays", func(t *testing.T) {
		// 10 tokens/sec = 1 token every 100ms, burst capacity = 1
		limiter, err := sources.NewTokenBucketLimiter(10, 1)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		ctx := context.Background()
		// Exhaust the 1 burst token immediately
		if err := limiter.Wait(ctx); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Now launch 2 concurrent waiters
		start := time.Now()
		times := make([]time.Duration, 2)
		errCh := make(chan error, 2)

		for i := range 2 {
			idx := i
			go func() {
				if err := limiter.Wait(ctx); err != nil {
					errCh <- err
					return
				}
				times[idx] = time.Since(start)
				errCh <- nil
			}()
		}

		for range 2 {
			if err := <-errCh; err != nil {
				t.Fatalf("waiter error: %v", err)
			}
		}

		// First waiter should finish after ~100ms, second waiter should finish after ~200ms
		// Verify there is a meaningful stagger between them (at least 50ms apart)
		diff := times[1] - times[0]
		if diff < 0 {
			diff = -diff
		}
		if diff < 40*time.Millisecond {
			t.Errorf("expected concurrent waiters to be staggered, but diff was %v (times: %v, %v)", diff, times[0], times[1])
		}
	})
}
