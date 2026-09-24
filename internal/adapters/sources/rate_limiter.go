package sources

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"
)

type RateLimiter interface {
	Wait(ctx context.Context) error
}

type TokenBucketLimiter struct {
	mu           sync.Mutex
	rate         float64
	capacity     float64
	tokens       float64
	lastRefillAt time.Time
}

func NewTokenBucketLimiter(ratePerSecond int, burst int) (*TokenBucketLimiter, error) {
	if ratePerSecond <= 0 {
		return nil, errors.New("rate per second must be positive")
	}
	if burst <= 0 {
		burst = ratePerSecond
	}

	now := time.Now().UTC()
	return &TokenBucketLimiter{
		rate:         float64(ratePerSecond),
		capacity:     float64(burst),
		tokens:       float64(burst),
		lastRefillAt: now,
	}, nil
}

func (l *TokenBucketLimiter) Wait(ctx context.Context) error {
	if l == nil {
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	sleepDuration := l.reserve(time.Now().UTC())
	if sleepDuration <= 0 {
		return nil
	}

	timer := time.NewTimer(sleepDuration)
	select {
	case <-ctx.Done():
		if !timer.Stop() {
			<-timer.C
		}
		l.mu.Lock()
		l.tokens = math.Min(l.capacity, l.tokens+1.0)
		l.mu.Unlock()
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (l *TokenBucketLimiter) reserve(now time.Time) time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()

	elapsed := now.Sub(l.lastRefillAt).Seconds()
	if elapsed > 0 {
		l.tokens = math.Min(l.capacity, l.tokens+(elapsed*l.rate))
		l.lastRefillAt = now
	}

	l.tokens -= 1.0
	if l.tokens >= 0 {
		return 0
	}

	missing := -l.tokens
	secondsToWait := missing / l.rate
	return time.Duration(secondsToWait * float64(time.Second))
}

var _ RateLimiter = (*TokenBucketLimiter)(nil)
