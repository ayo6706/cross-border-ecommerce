package postgres

import (
	"math"
	"testing"
)

func TestToInt32(t *testing.T) {
	t.Parallel()

	if got, err := toInt32(42); err != nil || got != 42 {
		t.Fatalf("toInt32(42) = %d, %v; want 42, nil", got, err)
	}
	if _, err := toInt32(math.MaxInt32 + 1); err == nil {
		t.Fatal("expected overflow error above MaxInt32")
	}
	if _, err := toInt32(math.MinInt32 - 1); err == nil {
		t.Fatal("expected overflow error below MinInt32")
	}
}

func TestListLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		limit int
		want  int32
	}{
		{name: "zero uses default", limit: 0, want: 50},
		{name: "negative uses default", limit: -5, want: 50},
		{name: "within range", limit: 200, want: 200},
		{name: "capped at max", limit: 1_000_000, want: maxListLimit},
		{name: "beyond int32 capped", limit: math.MaxInt32 + 10, want: maxListLimit},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := listLimit(tc.limit, 50); got != tc.want {
				t.Fatalf("listLimit(%d, 50) = %d, want %d", tc.limit, got, tc.want)
			}
		})
	}
}

func TestToRunCounters_RejectsOverflow(t *testing.T) {
	t.Parallel()

	if _, err := toRunCounters(1, 2, 3, 4, math.MaxInt32+1); err == nil {
		t.Fatal("expected overflow error for failed counter")
	}
	c, err := toRunCounters(1, 2, 3, 4, 5)
	if err != nil || c.seen != 1 || c.failed != 5 {
		t.Fatalf("unexpected counters %+v, err %v", c, err)
	}
}
