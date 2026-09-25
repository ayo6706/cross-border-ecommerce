package backoff_test

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/platform/backoff"
)

func TestExponential(t *testing.T) {
	base := 100 * time.Millisecond
	max := 1 * time.Second

	tests := []struct {
		name     string
		attempt  int
		expected time.Duration
	}{
		{"attempt_negative", -1, 100 * time.Millisecond},
		{"attempt_0", 0, 100 * time.Millisecond},
		{"attempt_1", 1, 200 * time.Millisecond},
		{"attempt_2", 2, 400 * time.Millisecond},
		{"attempt_3", 3, 800 * time.Millisecond},
		{"attempt_4_capped", 4, 1000 * time.Millisecond},
		{"attempt_overflow", 100, 1000 * time.Millisecond},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := backoff.Exponential(tc.attempt, base, max)
			if got != tc.expected {
				t.Fatalf("attempt %d: expected %v, got %v", tc.attempt, tc.expected, got)
			}
		})
	}
}

func TestExponential_ZeroValues(t *testing.T) {
	if got := backoff.Exponential(0, 0, time.Second); got != 0 {
		t.Fatalf("expected 0 for base 0, got %v", got)
	}
	if got := backoff.Exponential(0, time.Second, 0); got != 0 {
		t.Fatalf("expected 0 for max 0, got %v", got)
	}
}

func TestFullJitter_Bounds(t *testing.T) {
	base := 100 * time.Millisecond
	max := 1 * time.Second

	for attempt := 0; attempt < 5; attempt++ {
		ceiling := backoff.Exponential(attempt, base, max)
		for i := 0; i < 100; i++ {
			got := backoff.FullJitter(attempt, base, max)
			if got < 0 || got > ceiling {
				t.Fatalf("attempt %d: jitter value %v outside [0, %v]", attempt, got, ceiling)
			}
		}
	}
}

func TestValidateRetryPolicy(t *testing.T) {
	const (
		base    = 200 * time.Millisecond
		max     = 2 * time.Second
		timeout = 5 * time.Second
		idle    = 60 * time.Second
	)
	require := func(t *testing.T, err error, wantErr bool) {
		t.Helper()
		if wantErr && !errors.Is(err, backoff.ErrInvalidRetryPolicy) {
			t.Fatalf("expected ErrInvalidRetryPolicy, got %v", err)
		}
		if !wantErr && err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	tests := []struct {
		name                             string
		attempts                         int
		base, max, timeout, claimMinIdle time.Duration
		wantErr                          bool
	}{
		{"defaults fit: 5*5s + 4*2s = 33s < 60s", 5, base, max, timeout, idle, false},
		{"window equal to claim min idle", 2, base, max, timeout, 12 * time.Second, true},
		{"zero attempts", 0, base, max, timeout, idle, true},
		{"attempts above bound", backoff.MaxRetryAttempts + 1, base, max, timeout, idle, true},
		{"attempts that would overflow", 1 << 31, base, max, timeout, idle, true},
		{"zero base backoff", 3, 0, max, timeout, idle, true},
		{"max below base", 3, base, base / 2, timeout, idle, true},
		{"zero handler timeout", 3, base, max, 0, idle, true},
		{"window overflows int64", backoff.MaxRetryAttempts, base, time.Duration(math.MaxInt64 / 50), timeout, idle, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require(t, backoff.ValidateRetryPolicy(tc.attempts, tc.base, tc.max, tc.timeout, tc.claimMinIdle), tc.wantErr)
		})
	}
}
