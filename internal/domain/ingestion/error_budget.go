package ingestion

import "fmt"

// ErrorBudget bounds the share of rows a run may skip before the run is failed.
// It is evaluated over run totals, not per batch, so a cluster of bad rows in
// one page does not fail a run whose overall error rate is acceptable.
type ErrorBudget struct {
	MaxErrorRate  float64
	MinSampleRows int
}

// DefaultErrorBudget allows up to 5% skipped rows once at least 100 rows have been seen.
var DefaultErrorBudget = ErrorBudget{MaxErrorRate: 0.05, MinSampleRows: 100}

// OrDefault returns DefaultErrorBudget if b is the zero value (or has non-positive configuration).
func (b ErrorBudget) OrDefault() ErrorBudget {
	if b.MaxErrorRate <= 0 && b.MinSampleRows <= 0 {
		return DefaultErrorBudget
	}
	return b
}

// Check returns ErrErrorBudgetExceeded when failed/seen exceeds MaxErrorRate.
// A zero MaxErrorRate disables the budget.
func (b ErrorBudget) Check(seen, failed int) error {
	if b.MaxErrorRate <= 0 || seen == 0 || seen < b.MinSampleRows {
		return nil
	}

	rate := float64(failed) / float64(seen)
	if rate > b.MaxErrorRate {
		return fmt.Errorf("%w: %d of %d rows failed (%.1f%% > %.1f%%)",
			ErrErrorBudgetExceeded, failed, seen, rate*100, b.MaxErrorRate*100)
	}
	return nil
}
