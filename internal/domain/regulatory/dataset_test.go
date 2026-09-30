package regulatory_test

import (
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/regulatory"
)

var t0 = time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)

func validParams() regulatory.NewDatasetParams {
	return regulatory.NewDatasetParams{
		Jurisdiction:   "ng",
		Category:       regulatory.CategoryImportRestriction,
		Source:         "ng_prohibition_list",
		Version:        "2026-09",
		FetchedAt:      t0,
		ContentSHA256:  sha256.Sum256([]byte("file")),
		Licence:        "Public sector information",
		Attribution:    "Nigeria Customs Service",
		LoadedBy:       "alice",
		RequiresReview: true,
	}
}

func newDataset(t *testing.T) *regulatory.Dataset {
	t.Helper()
	d, err := regulatory.NewDataset(validParams())
	if err != nil {
		t.Fatalf("new dataset: %v", err)
	}
	return d
}

func TestNewDataset_NormalizesAndStartsLoaded(t *testing.T) {
	d := newDataset(t)
	if d.ID == "" || d.Jurisdiction != "NG" || d.Status != regulatory.StatusLoaded {
		t.Fatalf("got id=%q jurisdiction=%q status=%s", d.ID, d.Jurisdiction, d.Status)
	}
}

func TestNewDataset_RejectsInvalidInput(t *testing.T) {
	cases := map[string]func(p *regulatory.NewDatasetParams){
		"jurisdiction not alpha-2": func(p *regulatory.NewDatasetParams) { p.Jurisdiction = "NGA" },
		"unknown category":         func(p *regulatory.NewDatasetParams) { p.Category = "VAT" },
		"blank source":             func(p *regulatory.NewDatasetParams) { p.Source = " " },
		"blank version":            func(p *regulatory.NewDatasetParams) { p.Version = "" },
		"no fetch time":            func(p *regulatory.NewDatasetParams) { p.FetchedAt = time.Time{} },
		"zero content hash":        func(p *regulatory.NewDatasetParams) { p.ContentSHA256 = [32]byte{} },
		"blank licence":            func(p *regulatory.NewDatasetParams) { p.Licence = "" },
		"blank attribution":        func(p *regulatory.NewDatasetParams) { p.Attribution = "" },
		"blank loader":             func(p *regulatory.NewDatasetParams) { p.LoadedBy = " " },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			p := validParams()
			mutate(&p)
			if _, err := regulatory.NewDataset(p); !errors.Is(err, regulatory.ErrInvalidDataset) {
				t.Fatalf("want ErrInvalidDataset, got %v", err)
			}
		})
	}
}

func TestDataset_ActivateRequiresReview(t *testing.T) {
	d := newDataset(t)
	if err := d.Activate(t0); !errors.Is(err, regulatory.ErrReviewRequired) {
		t.Fatalf("want ErrReviewRequired, got %v", err)
	}
	if d.Status != regulatory.StatusLoaded || d.ActivatedAt != nil {
		t.Fatalf("refused activation changed the dataset: status=%s activated=%v", d.Status, d.ActivatedAt)
	}

	if err := d.RecordReview("bob", "checked against the gazette", t0); err != nil {
		t.Fatalf("review: %v", err)
	}
	if err := d.Activate(t0.Add(time.Hour)); err != nil {
		t.Fatalf("activate after review: %v", err)
	}
	if d.Status != regulatory.StatusActive || !d.ActivatedAt.Equal(t0.Add(time.Hour)) {
		t.Fatalf("got status=%s activated=%v", d.Status, d.ActivatedAt)
	}
}

func TestDataset_ReviewerMustNotBeTheLoader(t *testing.T) {
	d := newDataset(t)
	if err := d.RecordReview(" ALICE ", "self review", t0); !errors.Is(err, regulatory.ErrReviewerIsLoader) {
		t.Fatalf("want ErrReviewerIsLoader, got %v", err)
	}
	if d.Review != nil {
		t.Fatal("refused review was recorded")
	}
}

func TestDataset_ReviewNeedsReviewerAndNote(t *testing.T) {
	for name, args := range map[string][2]string{"no reviewer": {" ", "note"}, "no note": {"bob", " "}} {
		t.Run(name, func(t *testing.T) {
			if err := newDataset(t).RecordReview(args[0], args[1], t0); !errors.Is(err, regulatory.ErrInvalidReview) {
				t.Fatalf("want ErrInvalidReview, got %v", err)
			}
		})
	}
}

func TestDataset_TransitionsOutOfTerminalStatesFail(t *testing.T) {
	reviewed := func(t *testing.T) *regulatory.Dataset {
		d := newDataset(t)
		if err := d.RecordReview("bob", "ok", t0); err != nil {
			t.Fatalf("review: %v", err)
		}
		return d
	}
	rejected := func(t *testing.T) *regulatory.Dataset {
		d := reviewed(t)
		if err := d.Reject("partial upstream file"); err != nil {
			t.Fatalf("reject: %v", err)
		}
		return d
	}
	superseded := func(t *testing.T) *regulatory.Dataset {
		d := reviewed(t)
		if err := d.Activate(t0); err != nil {
			t.Fatalf("activate: %v", err)
		}
		if err := d.Supersede(t0.Add(time.Hour)); err != nil {
			t.Fatalf("supersede: %v", err)
		}
		return d
	}
	for name, build := range map[string]func(*testing.T) *regulatory.Dataset{"REJECTED": rejected, "SUPERSEDED": superseded} {
		t.Run(name, func(t *testing.T) {
			d := build(t)
			before := *d
			for op, err := range map[string]error{
				"activate":  d.Activate(t0.Add(2 * time.Hour)),
				"supersede": d.Supersede(t0.Add(2 * time.Hour)),
				"reject":    d.Reject("again"),
				"review":    d.RecordReview("carol", "again", t0.Add(2*time.Hour)),
			} {
				if !errors.Is(err, regulatory.ErrInvalidTransition) {
					t.Errorf("%s: want ErrInvalidTransition, got %v", op, err)
				}
			}
			if d.Status != before.Status || d.ActivatedAt != before.ActivatedAt || d.SupersededAt != before.SupersededAt {
				t.Fatalf("refused transitions changed the dataset")
			}
		})
	}
}

func TestDataset_SupersedeBeforeActivationFails(t *testing.T) {
	d := newDataset(t)
	if err := d.RecordReview("bob", "ok", t0); err != nil {
		t.Fatalf("review: %v", err)
	}
	if err := d.Activate(t0); err != nil {
		t.Fatalf("activate: %v", err)
	}
	if err := d.Supersede(t0.Add(-time.Second)); !errors.Is(err, regulatory.ErrInvalidTransition) {
		t.Fatalf("want ErrInvalidTransition, got %v", err)
	}
}

func TestDataset_RejectNeedsReason(t *testing.T) {
	if err := newDataset(t).Reject(" "); !errors.Is(err, regulatory.ErrInvalidDataset) {
		t.Fatalf("want ErrInvalidDataset, got %v", err)
	}
}

func TestNewValidity_KeepsTheCallersCalendarDate(t *testing.T) {
	lagos := time.FixedZone("WAT", 3600)
	from := time.Date(2026, 7, 1, 0, 30, 0, 0, lagos) // 2026-06-30 23:30 UTC, but 1 July in Lagos
	v, err := regulatory.NewValidity(from, nil)
	if err != nil {
		t.Fatalf("validity: %v", err)
	}
	if want := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC); !v.From.Equal(want) || v.From.Location() != time.UTC {
		t.Fatalf("From = %v, want %v", v.From, want)
	}
}

func TestCuratedRules_CategoryAndValidate(t *testing.T) {
	validity, err := regulatory.NewValidity(t0, nil)
	if err != nil {
		t.Fatalf("validity: %v", err)
	}
	restriction := regulatory.ImportRestriction{HSCode: "6309", OriginCountry: "*", Description: "Used clothing",
		SourceReference: "ref", Validity: validity}
	permit := regulatory.PermitRequirement{HSCode: "080450", OriginCountry: "*", PermitCode: "P", IssuingAgency: "A",
		DocumentType: "D", SourceReference: "ref", Validity: validity}
	restrictions := regulatory.CuratedRules{Restrictions: []regulatory.ImportRestriction{restriction}}
	permits := regulatory.CuratedRules{Permits: []regulatory.PermitRequirement{permit}}
	blank := restriction
	blank.Description = " "

	cases := map[string]struct {
		rules    regulatory.CuratedRules
		category regulatory.Category
		wantErr  bool
	}{
		"restrictions in a restriction dataset": {restrictions, regulatory.CategoryImportRestriction, false},
		"permits in a permit dataset":           {permits, regulatory.CategoryPermit, false},
		"no rules":                              {regulatory.CuratedRules{}, regulatory.CategoryPermit, true},
		"both kinds": {regulatory.CuratedRules{Restrictions: restrictions.Restrictions, Permits: permits.Permits},
			regulatory.CategoryImportRestriction, true},
		"restrictions in a permit dataset": {restrictions, regulatory.CategoryPermit, true},
		"permits in a tariff dataset":      {permits, regulatory.CategoryTariff, true},
		"a rule with a blank field": {regulatory.CuratedRules{Restrictions: []regulatory.ImportRestriction{blank}},
			regulatory.CategoryImportRestriction, true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			err := tc.rules.Validate(tc.category)
			if tc.wantErr != (err != nil) || (err != nil && !errors.Is(err, regulatory.ErrInvalidRule)) {
				t.Fatalf("Validate(%s) = %v, wantErr %v", tc.category, err, tc.wantErr)
			}
		})
	}
}
