package policy_test

import (
	"errors"
	"testing"

	"github.com/ayo6706/cross-border-ecommerce/internal/adapters/sources/policy"
)

func TestErrorPolicy_HandleRowError(t *testing.T) {
	t.Parallel()

	rowErr := errors.New("malformed row")

	t.Run("FailFastReturnsError", func(t *testing.T) {
		p := policy.ErrorPolicy{Policy: policy.PolicyFailFast}
		if err := p.HandleRowError(1, []byte("bad"), rowErr); !errors.Is(err, rowErr) {
			t.Fatalf("expected %v, got %v", rowErr, err)
		}
	})

	t.Run("ZeroValueIsFailFast", func(t *testing.T) {
		var p policy.ErrorPolicy
		if err := p.HandleRowError(1, nil, rowErr); !errors.Is(err, rowErr) {
			t.Fatalf("expected zero-value policy to fail fast, got %v", err)
		}
	})

	t.Run("SkipMalformedSkipsAndReports", func(t *testing.T) {
		var reportedRow int
		p := policy.ErrorPolicy{
			Policy: policy.PolicySkipMalformed,
			OnRowError: func(row int, _ []byte, _ error) {
				reportedRow = row
			},
		}
		if err := p.HandleRowError(7, []byte("bad"), rowErr); err != nil {
			t.Fatalf("expected row to be skipped, got %v", err)
		}
		if reportedRow != 7 {
			t.Fatalf("expected callback for row 7, got %d", reportedRow)
		}
	})
}
