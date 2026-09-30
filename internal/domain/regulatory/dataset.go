// Package regulatory models versioned regulatory datasets: an immutable load of rules that affects
// decisions only between its activation and its supersession.
package regulatory

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/platform/uuid"
)

type Category string

const (
	CategoryTariff                Category = "TARIFF"
	CategoryImportRestriction     Category = "IMPORT_RESTRICTION"
	CategoryPermit                Category = "PERMIT"
	CategorySanctions             Category = "SANCTIONS"
	CategoryExportControl         Category = "EXPORT_CONTROL"
	CategoryPreferentialAgreement Category = "PREFERENTIAL_AGREEMENT"
)

// Categories matches the category CHECK of regulatory_datasets (migration 000022).
func Categories() []Category {
	return []Category{CategoryTariff, CategoryImportRestriction, CategoryPermit,
		CategorySanctions, CategoryExportControl, CategoryPreferentialAgreement}
}

func ParseCategory(s string) (Category, error) {
	c := Category(strings.ToUpper(strings.TrimSpace(s)))
	if !slices.Contains(Categories(), c) {
		return "", fmt.Errorf("%w: unknown category %q", ErrInvalidDataset, s)
	}
	return c, nil
}

// Status values match the status CHECK of regulatory_datasets (migration 000023).
type Status string

const (
	StatusLoaded     Status = "LOADED"
	StatusActive     Status = "ACTIVE"
	StatusSuperseded Status = "SUPERSEDED"
	StatusRejected   Status = "REJECTED"
)

// jurisdictionPattern matches the country_code domain of migration 000022.
var jurisdictionPattern = regexp.MustCompile(`^[A-Z]{2}$`)

// ParseJurisdiction normalizes an ISO 3166 alpha-2 code (EU included); the UK is GB.
func ParseJurisdiction(s string) (string, error) {
	j := strings.ToUpper(strings.TrimSpace(s))
	if !jurisdictionPattern.MatchString(j) {
		return "", fmt.Errorf("%w: jurisdiction %q is not an alpha-2 code", ErrInvalidDataset, s)
	}
	return j, nil
}

type Review struct {
	By   string
	Note string
	At   time.Time
}

type Dataset struct {
	ID             string
	Jurisdiction   string
	Category       Category
	Source         string
	Version        string
	FetchedAt      time.Time
	ContentSHA256  [32]byte
	Licence        string
	Attribution    string
	LoadedBy       string
	RequiresReview bool
	Status         Status
	Review         *Review
	ActivatedAt    *time.Time
	SupersededAt   *time.Time
	RejectedReason string
}

type NewDatasetParams struct {
	Jurisdiction   string
	Category       Category
	Source         string
	Version        string
	FetchedAt      time.Time
	ContentSHA256  [32]byte
	Licence        string
	Attribution    string
	LoadedBy       string
	RequiresReview bool
}

// NewDataset returns a LOADED dataset with a new id.
func NewDataset(p NewDatasetParams) (*Dataset, error) {
	jurisdiction, err := ParseJurisdiction(p.Jurisdiction)
	if err != nil {
		return nil, err
	}
	category, err := ParseCategory(string(p.Category))
	if err != nil {
		return nil, err
	}
	d := &Dataset{
		Jurisdiction:   jurisdiction,
		Category:       category,
		Source:         strings.TrimSpace(p.Source),
		Version:        strings.TrimSpace(p.Version),
		FetchedAt:      p.FetchedAt.UTC(),
		ContentSHA256:  p.ContentSHA256,
		Licence:        strings.TrimSpace(p.Licence),
		Attribution:    strings.TrimSpace(p.Attribution),
		LoadedBy:       strings.TrimSpace(p.LoadedBy),
		RequiresReview: p.RequiresReview,
		Status:         StatusLoaded,
	}
	if err := d.validateProvenance(p.FetchedAt); err != nil {
		return nil, err
	}
	if d.ID, err = uuid.NewString(); err != nil {
		return nil, fmt.Errorf("generate dataset id: %w", err)
	}
	return d, nil
}

func (d *Dataset) validateProvenance(fetchedAt time.Time) error {
	required := [][2]string{{"source", d.Source}, {"version", d.Version}, {"licence", d.Licence},
		{"attribution", d.Attribution}, {"loaded by", d.LoadedBy}}
	for _, field := range required {
		if field[1] == "" {
			return fmt.Errorf("%w: %s is required", ErrInvalidDataset, field[0])
		}
	}
	if fetchedAt.IsZero() {
		return fmt.Errorf("%w: fetch time is required", ErrInvalidDataset)
	}
	if d.ContentSHA256 == [32]byte{} {
		return fmt.Errorf("%w: content hash is required", ErrInvalidDataset)
	}
	return nil
}

// RecordReview signs off a LOADED dataset. The reviewer must be someone other than the loader.
func (d *Dataset) RecordReview(by, note string, now time.Time) error {
	if d.Status != StatusLoaded || d.Review != nil {
		return fmt.Errorf("%w: review of a %s dataset", ErrInvalidTransition, d.Status)
	}
	by, note = strings.TrimSpace(by), strings.TrimSpace(note)
	if by == "" || note == "" {
		return ErrInvalidReview
	}
	// Mirrors chk_regulatory_datasets_reviewer_not_loader; the database is the guarantee.
	if strings.EqualFold(by, d.LoadedBy) {
		return ErrReviewerIsLoader
	}
	d.Review = &Review{By: by, Note: note, At: now.UTC()}
	return nil
}

func (d *Dataset) Activate(now time.Time) error {
	if d.Status != StatusLoaded {
		return fmt.Errorf("%w: activate a %s dataset", ErrInvalidTransition, d.Status)
	}
	if d.RequiresReview && d.Review == nil {
		return ErrReviewRequired
	}
	at := now.UTC()
	d.Status = StatusActive
	d.ActivatedAt = &at
	return nil
}

func (d *Dataset) Supersede(now time.Time) error {
	if d.Status != StatusActive {
		return fmt.Errorf("%w: supersede a %s dataset", ErrInvalidTransition, d.Status)
	}
	at := now.UTC()
	if at.Before(*d.ActivatedAt) {
		return fmt.Errorf("%w: superseded at %s, before its activation at %s", ErrInvalidTransition,
			at.Format(time.RFC3339Nano), d.ActivatedAt.Format(time.RFC3339Nano))
	}
	d.Status = StatusSuperseded
	d.SupersededAt = &at
	return nil
}

// Reject marks a LOADED dataset as never to be activated.
func (d *Dataset) Reject(reason string) error {
	if d.Status != StatusLoaded {
		return fmt.Errorf("%w: reject a %s dataset", ErrInvalidTransition, d.Status)
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return fmt.Errorf("%w: a rejection needs a reason", ErrInvalidDataset)
	}
	d.Status = StatusRejected
	d.RejectedReason = reason
	return nil
}
