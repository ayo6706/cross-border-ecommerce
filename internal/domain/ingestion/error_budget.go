package ingestion

import (
	"errors"
	"fmt"
)

var ErrInvalidErrorBudget = errors.New("error budget must have max error rate in (0, 1) and positive min sample rows")

// ErrorBudget bounds the share of rows a run may skip before the run is failed.
// It is evaluated over run totals, not per batch, so a cluster of bad rows in
// one page does not fail a run whose overall error rate is acceptable.
type ErrorBudget struct {
	MaxErrorRate  float64
	MinSampleRows int
}

// Validate checks that configuration values are strictly positive and within bounds.
func (b ErrorBudget) Validate() error {
	if b.MaxErrorRate <= 0 || b.MaxErrorRate >= 1.0 || b.MinSampleRows <= 0 {
		return ErrInvalidErrorBudget
	}
	return nil
}

// Check returns ErrErrorBudgetExceeded when failed/seen exceeds MaxErrorRate.
func (b ErrorBudget) Check(seen, failed int) error {
	if seen < b.MinSampleRows {
		return nil
	}

	rate := float64(failed) / float64(seen)
	if rate > b.MaxErrorRate {
		return fmt.Errorf("%w: %d of %d rows failed (%.1f%% > %.1f%%)",
			ErrErrorBudgetExceeded, failed, seen, rate*100, b.MaxErrorRate*100)
	}
	return nil
}
