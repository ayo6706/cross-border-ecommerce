package regulatory

import (
	"errors"
	"fmt"
	"maps"
	"time"
)

// HOLD reason codes a decision reports when it cannot rely on regulatory data.
const (
	ReasonNoCoverage         = "NO_REGULATORY_COVERAGE"
	ReasonStaleData          = "STALE_REGULATORY_DATA"
	ReasonNoTariffRate       = "NO_TARIFF_RATE"
	ReasonAmbiguousTariff    = "AMBIGUOUS_TARIFF"
	ReasonUnsupportedMeasure = "UNSUPPORTED_MEASURE"
)

// HoldReason maps a coverage or rule-resolution error to its reason code; ok is false for any
// other error.
func HoldReason(err error) (reason string, ok bool) {
	switch {
	case errors.Is(err, ErrNoCoverage):
		return ReasonNoCoverage, true
	case errors.Is(err, ErrStaleCoverage):
		return ReasonStaleData, true
	case errors.Is(err, ErrNoTariffRate):
		return ReasonNoTariffRate, true
	case errors.Is(err, ErrAmbiguousTariff):
		return ReasonAmbiguousTariff, true
	case errors.Is(err, ErrUnsupportedMeasure):
		return ReasonUnsupportedMeasure, true
	default:
		return "", false
	}
}

// SLAs is the maximum age of fetched data, per category. Every category has one.
type SLAs map[Category]time.Duration

func NewSLAs(m map[Category]time.Duration) (SLAs, error) {
	var errs []error
	for _, c := range Categories() {
		errs = append(errs, ValidateSLA(c, m[c]))
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return SLAs(maps.Clone(m)), nil
}

func ValidateSLA(c Category, d time.Duration) error {
	if d <= 0 {
		return fmt.Errorf("%w: %s must be greater than zero", ErrInvalidSLA, c)
	}
	return nil
}

// Check reports whether the datasets active at `at` cover a category: at least one is active, and
// none was fetched more than the category's SLA before `at`.
func (s SLAs) Check(category Category, active []*Dataset, at time.Time) error {
	if len(active) == 0 {
		return fmt.Errorf("%w for %s at %s", ErrNoCoverage, category, at.UTC().Format(time.RFC3339))
	}
	for _, d := range active {
		if age := at.Sub(d.FetchedAt); age > s[category] {
			return fmt.Errorf("%w: dataset %s (%s %s) fetched %s before %s, SLA %s", ErrStaleCoverage,
				d.ID, d.Source, d.Version, age.Round(time.Minute), at.UTC().Format(time.RFC3339), s[category])
		}
	}
	return nil
}
