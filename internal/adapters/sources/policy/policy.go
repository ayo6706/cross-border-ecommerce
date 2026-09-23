package policy

import (
	"fmt"
	"sync"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
)

type PolicyType string

const (
	PolicyFailFast      PolicyType = "FAIL_FAST"
	PolicySkipMalformed PolicyType = "SKIP_MALFORMED"
	PolicyQuarantine    PolicyType = "QUARANTINE"
)

type RowErrorCallback func(rowNumber int, rawRow []byte, err error)

// ErrorTracker tracks row processing and evaluates error policies and quarantine thresholds.
type ErrorTracker struct {
	mu             sync.Mutex
	Policy         PolicyType
	MaxErrorRate   float64 // e.g. 0.05 (5%) or 0.005 (0.5%)
	MinSampleRows  int     // minimum rows processed before enforcing threshold
	TotalRows      int
	ErrorCount     int
	OnRowErrorFunc RowErrorCallback
}

func NewErrorTracker(policy PolicyType, maxErrorRate float64, onRowError RowErrorCallback) *ErrorTracker {
	p := policy
	if p == "" {
		p = PolicyFailFast
	}
	rate := maxErrorRate
	if rate <= 0 {
		rate = 0.05 // default 5%
	}
	return &ErrorTracker{
		Policy:         p,
		MaxErrorRate:   rate,
		MinSampleRows:  10,
		OnRowErrorFunc: onRowError,
	}
}

func (t *ErrorTracker) RecordSuccess() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.TotalRows++
}

// RecordError records a row-level processing failure and evaluates the policy.
// Returns an error if the policy dictates terminating the run or the quarantine threshold was breached.
func (t *ErrorTracker) RecordError(rowNumber int, rawRow []byte, err error) error {
	if t == nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	t.TotalRows++
	t.ErrorCount++

	if t.OnRowErrorFunc != nil {
		t.OnRowErrorFunc(rowNumber, rawRow, err)
	}

	if t.Policy == PolicyFailFast {
		return err
	}

	// For Skip or Quarantine policies, check error threshold
	if t.TotalRows >= t.MinSampleRows && t.MaxErrorRate > 0 {
		rate := float64(t.ErrorCount) / float64(t.TotalRows)
		if rate > t.MaxErrorRate {
			return fmt.Errorf("%w: error rate %.1f%% exceeded max threshold %.1f%% (%d/%d errors): %v",
				ingestion.ErrQuarantineThreshold, rate*100, t.MaxErrorRate*100, t.ErrorCount, t.TotalRows, err)
		}
	}

	return nil
}

func (t *ErrorTracker) Stats() (total int, errors int, rate float64) {
	if t == nil {
		return 0, 0, 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.TotalRows == 0 {
		return 0, 0, 0
	}
	return t.TotalRows, t.ErrorCount, float64(t.ErrorCount) / float64(t.TotalRows)
}
