package regulatory

import (
	"fmt"
	"strings"
	"time"
)

// Validity is a half-open date range [From, To); a nil To is open-ended. Both are calendar dates:
// midnight UTC of the date the caller's time falls on, in the caller's location.
type Validity struct {
	From time.Time
	To   *time.Time
}

func NewValidity(from time.Time, to *time.Time) (Validity, error) {
	if from.IsZero() {
		return Validity{}, fmt.Errorf("%w: effective_from is required", ErrInvalidRule)
	}
	from = calendarDate(from)
	if to != nil {
		end := calendarDate(*to)
		to = &end
	}
	if to != nil && !to.After(from) {
		return Validity{}, fmt.Errorf("%w: effective_to %s is not after effective_from %s",
			ErrInvalidRule, to.Format(time.DateOnly), from.Format(time.DateOnly))
	}
	return Validity{From: from, To: to}, nil
}

// ImportRestriction prohibits importing goods under an HS code (prefix) from an origin ('*' = any).
type ImportRestriction struct {
	HSCode          string
	OriginCountry   string
	Description     string
	SourceReference string
	Validity        Validity
}

// PermitRequirement allows goods only with the named permit.
type PermitRequirement struct {
	HSCode          string
	OriginCountry   string
	PermitCode      string
	IssuingAgency   string
	DocumentType    string
	SourceReference string
	Validity        Validity
}

// CuratedRules are the rows of one curated file: one kind of rule, matching its dataset's category.
type CuratedRules struct {
	Restrictions []ImportRestriction
	Permits      []PermitRequirement
}

// Category is the one category the rules belong to. No rules, or rules of two kinds, is an error.
func (r CuratedRules) Category() (Category, error) {
	switch {
	case len(r.Restrictions) > 0 && len(r.Permits) == 0:
		return CategoryImportRestriction, nil
	case len(r.Permits) > 0 && len(r.Restrictions) == 0:
		return CategoryPermit, nil
	case len(r.Restrictions) == 0 && len(r.Permits) == 0:
		return "", fmt.Errorf("%w: no rules", ErrInvalidRule)
	default:
		return "", fmt.Errorf("%w: restrictions and permits in one file", ErrInvalidRule)
	}
}

// Validate checks the rules fit a dataset of the category and that no row misses a field. Code and
// country formats and overlapping validity are enforced by the database.
func (r CuratedRules) Validate(category Category) error {
	got, err := r.Category()
	if err != nil {
		return err
	}
	if got != category {
		return fmt.Errorf("%w: %s rules in a %s dataset", ErrInvalidRule, got, category)
	}
	for i := range r.Restrictions {
		rule := &r.Restrictions[i]
		err := requireFields(i, rule.HSCode, rule.OriginCountry, rule.Description, rule.SourceReference)
		if err != nil {
			return err
		}
	}
	for i := range r.Permits {
		rule := &r.Permits[i]
		if err := requireFields(i, rule.HSCode, rule.OriginCountry, rule.PermitCode, rule.IssuingAgency,
			rule.DocumentType, rule.SourceReference); err != nil {
			return err
		}
	}
	return nil
}

func requireFields(row int, values ...string) error {
	for _, v := range values {
		if strings.TrimSpace(v) == "" {
			return fmt.Errorf("%w: rule %d has an empty required field", ErrInvalidRule, row+1)
		}
	}
	return nil
}

func calendarDate(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}
