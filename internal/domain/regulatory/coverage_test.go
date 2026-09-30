package regulatory_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/regulatory"
)

func allSLAs(d time.Duration) map[regulatory.Category]time.Duration {
	m := map[regulatory.Category]time.Duration{}
	for _, c := range regulatory.Categories() {
		m[c] = d
	}
	return m
}

func TestNewSLAs_RequiresEveryCategory(t *testing.T) {
	m := allSLAs(time.Hour)
	delete(m, regulatory.CategorySanctions)
	m[regulatory.CategoryTariff] = 0
	_, err := regulatory.NewSLAs(m)
	if !errors.Is(err, regulatory.ErrInvalidSLA) {
		t.Fatalf("want ErrInvalidSLA, got %v", err)
	}
	for _, name := range []string{"SANCTIONS", "TARIFF"} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("error %q does not name %s", err, name)
		}
	}
}

func TestSLAs_Check(t *testing.T) {
	slas, err := regulatory.NewSLAs(allSLAs(24 * time.Hour))
	if err != nil {
		t.Fatalf("slas: %v", err)
	}
	at := t0.Add(48 * time.Hour)
	fresh := &regulatory.Dataset{ID: "fresh", FetchedAt: at.Add(-24 * time.Hour)} // exactly the SLA: still fresh
	stale := &regulatory.Dataset{ID: "stale", FetchedAt: at.Add(-24*time.Hour - time.Second)}

	cases := map[string]struct {
		active []*regulatory.Dataset
		want   error
		reason string
	}{
		"no active dataset":        {nil, regulatory.ErrNoCoverage, regulatory.ReasonNoCoverage},
		"fresh dataset":            {[]*regulatory.Dataset{fresh}, nil, ""},
		"one of two sources stale": {[]*regulatory.Dataset{fresh, stale}, regulatory.ErrStaleCoverage, regulatory.ReasonStaleData},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			err := slas.Check(regulatory.CategoryTariff, tc.active, at)
			if !errors.Is(err, tc.want) || (tc.want == nil && err != nil) {
				t.Fatalf("want %v, got %v", tc.want, err)
			}
			if reason, _ := regulatory.HoldReason(err); reason != tc.reason {
				t.Fatalf("hold reason = %q, want %q", reason, tc.reason)
			}
		})
	}
}
