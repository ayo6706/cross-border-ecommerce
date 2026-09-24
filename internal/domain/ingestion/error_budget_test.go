package ingestion_test

import (
	"errors"
	"testing"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
)

func TestErrorBudget_Check(t *testing.T) {
	t.Parallel()

	budget := ingestion.ErrorBudget{MaxErrorRate: 0.10, MinSampleRows: 10}

	tests := []struct {
		name         string
		budget       ingestion.ErrorBudget
		seen, failed int
		wantExceeded bool
	}{
		{name: "under rate", budget: budget, seen: 100, failed: 10},
		{name: "over rate", budget: budget, seen: 100, failed: 11, wantExceeded: true},
		{name: "below min sample", budget: budget, seen: 9, failed: 9},
		{name: "at min sample over rate", budget: budget, seen: 10, failed: 2, wantExceeded: true},
		{name: "no rows", budget: budget},
		{name: "disabled budget", budget: ingestion.ErrorBudget{}, seen: 100, failed: 100},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.budget.Check(tc.seen, tc.failed)
			if got := errors.Is(err, ingestion.ErrErrorBudgetExceeded); got != tc.wantExceeded {
				t.Fatalf("Check(%d, %d) exceeded=%v, want %v (err: %v)", tc.seen, tc.failed, got, tc.wantExceeded, err)
			}
		})
	}
}
