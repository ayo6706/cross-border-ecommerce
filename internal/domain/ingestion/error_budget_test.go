package ingestion_test

import (
	"errors"
	"testing"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
)

func TestErrorBudget_Validation(t *testing.T) {
	t.Parallel()

	validCases := []struct {
		rate float64
		rows int
	}{
		{0.05, 100},
		{0.10, 10},
		{0.01, 1},
	}
	for _, tc := range validCases {
		b, err := ingestion.NewErrorBudget(tc.rate, tc.rows)
		if err != nil {
			t.Errorf("expected valid budget for (%f, %d), got %v", tc.rate, tc.rows, err)
		}
		if err := b.Validate(); err != nil {
			t.Errorf("expected Validate() to pass for (%f, %d), got %v", tc.rate, tc.rows, err)
		}
	}

	invalidCases := []struct {
		name string
		rate float64
		rows int
	}{
		{"zero rate", 0, 100},
		{"negative rate", -0.05, 100},
		{"rate equal to 1", 1.0, 100},
		{"rate greater than 1", 1.5, 100},
		{"zero sample rows", 0.05, 0},
		{"negative sample rows", 0.05, -10},
	}
	for _, tc := range invalidCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ingestion.NewErrorBudget(tc.rate, tc.rows)
			if !errors.Is(err, ingestion.ErrInvalidErrorBudget) {
				t.Errorf("expected ErrInvalidErrorBudget, got %v", err)
			}
			b := ingestion.ErrorBudget{MaxErrorRate: tc.rate, MinSampleRows: tc.rows}
			if !errors.Is(b.Validate(), ingestion.ErrInvalidErrorBudget) {
				t.Errorf("expected Validate() to return ErrInvalidErrorBudget, got %v", b.Validate())
			}
		})
	}
}

func TestErrorBudget_Check(t *testing.T) {
	t.Parallel()

	budget, err := ingestion.NewErrorBudget(0.10, 10)
	if err != nil {
		t.Fatalf("unexpected error creating budget: %v", err)
	}

	tests := []struct {
		name         string
		seen, failed int
		wantExceeded bool
	}{
		{name: "under rate", seen: 100, failed: 10},
		{name: "over rate", seen: 100, failed: 11, wantExceeded: true},
		{name: "below min sample", seen: 9, failed: 9},
		{name: "at min sample over rate", seen: 10, failed: 2, wantExceeded: true},
		{name: "no rows", seen: 0, failed: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := budget.Check(tc.seen, tc.failed)
			if got := errors.Is(err, ingestion.ErrErrorBudgetExceeded); got != tc.wantExceeded {
				t.Fatalf("Check(%d, %d) exceeded=%v, want %v (err: %v)", tc.seen, tc.failed, got, tc.wantExceeded, err)
			}
		})
	}
}
