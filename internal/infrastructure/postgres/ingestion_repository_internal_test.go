package postgres

import (
	"math"
	"testing"
)

func TestSafeInt32(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    int
		expected int32
	}{
		{
			name:     "zero",
			input:    0,
			expected: 0,
		},
		{
			name:     "positive within bounds",
			input:    42,
			expected: 42,
		},
		{
			name:     "negative within bounds",
			input:    -42,
			expected: -42,
		},
		{
			name:     "max int32",
			input:    math.MaxInt32,
			expected: math.MaxInt32,
		},
		{
			name:     "min int32",
			input:    math.MinInt32,
			expected: math.MinInt32,
		},
		{
			name:     "overflow capped to MaxInt32",
			input:    math.MaxInt32 + 1000,
			expected: math.MaxInt32,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			res := safeInt32(tc.input)
			if res != tc.expected {
				t.Errorf("safeInt32(%d) = %d, expected %d", tc.input, res, tc.expected)
			}
		})
	}
}
