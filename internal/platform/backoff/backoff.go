package backoff

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"time"
)

// MaxRetryAttempts bounds in-process retries so the retry window arithmetic cannot overflow.
const MaxRetryAttempts = 100

var ErrInvalidRetryPolicy = errors.New("invalid retry policy")

// Exponential returns min(maxBackoff, base * 2^attempt).
func Exponential(attempt int, base, maxBackoff time.Duration) time.Duration {
	if base <= 0 {
		return 0
	}
	if maxBackoff <= 0 {
		return 0
	}
	if attempt < 0 {
		attempt = 0
	}
	if attempt >= 62 {
		return maxBackoff
	}
	multiplier := time.Duration(1) << attempt
	res := base * multiplier
	if res <= 0 || res > maxBackoff || res/multiplier != base {
		return maxBackoff
	}
	return res
}

// FullJitter returns a random delay in [0, min(maxBackoff, base * 2^attempt)].
func FullJitter(attempt int, base, maxBackoff time.Duration) time.Duration {
	ceiling := Exponential(attempt, base, maxBackoff)
	if ceiling <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(ceiling) + 1))
}

// ValidateRetryPolicy ensures that the worst-case retry duration is strictly
// less than claimMinIdle so an in-flight message is not prematurely reclaimed.
func ValidateRetryPolicy(attempts int, baseBackoff, maxBackoff, handlerTimeout, claimMinIdle time.Duration) error {
	if attempts < 1 || attempts > MaxRetryAttempts {
		return fmt.Errorf("%w: attempts must be between 1 and %d, got %d", ErrInvalidRetryPolicy, MaxRetryAttempts, attempts)
	}
	if baseBackoff <= 0 {
		return fmt.Errorf("%w: base backoff must be strictly positive", ErrInvalidRetryPolicy)
	}
	if maxBackoff < baseBackoff {
		return fmt.Errorf("%w: max backoff cannot be less than base backoff", ErrInvalidRetryPolicy)
	}
	if handlerTimeout <= 0 {
		return fmt.Errorf("%w: handler timeout must be strictly positive", ErrInvalidRetryPolicy)
	}
	window, err := maxRetryWindow(attempts, handlerTimeout, maxBackoff)
	if err != nil {
		return err
	}
	if window >= claimMinIdle {
		return fmt.Errorf("%w: worst-case retry window (%v) must be strictly less than claim min idle (%v)",
			ErrInvalidRetryPolicy, window, claimMinIdle)
	}
	return nil
}

func maxRetryWindow(attempts int, handlerTimeout, maxBackoff time.Duration) (time.Duration, error) {
	n := time.Duration(attempts)
	if handlerTimeout > math.MaxInt64/n {
		return 0, fmt.Errorf("%w: retry window overflows", ErrInvalidRetryPolicy)
	}
	window := n * handlerTimeout
	if sleeps := n - 1; sleeps > 0 && maxBackoff > 0 {
		if maxBackoff > (math.MaxInt64-window)/sleeps {
			return 0, fmt.Errorf("%w: retry window overflows", ErrInvalidRetryPolicy)
		}
		window += sleeps * maxBackoff
	}
	return window, nil
}
