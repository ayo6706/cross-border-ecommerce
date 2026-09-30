// Package regulatory parses curated regulatory files into domain rules.
package regulatory

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	domain "github.com/ayo6706/cross-border-ecommerce/internal/domain/regulatory"
)

// Curated CSV headers, one per category, in column order. A file must match exactly.
var curatedHeaders = map[domain.Category][]string{
	domain.CategoryImportRestriction: {"hs_code", "origin_country", "description",
		"effective_from", "effective_to", "source_reference"},
	domain.CategoryPermit: {"hs_code", "origin_country", "permit_code", "issuing_agency", "document_type",
		"effective_from", "effective_to", "source_reference"},
}

// ParseCurated reads a curated CSV for the category. Dates are YYYY-MM-DD; an empty effective_to
// is open-ended. Any malformed line fails the whole file, naming the line.
func ParseCurated(category domain.Category, r io.Reader) (domain.CuratedRules, error) {
	header, ok := curatedHeaders[category]
	if !ok {
		return domain.CuratedRules{}, fmt.Errorf("%w: category %s has no curated file format",
			domain.ErrInvalidRule, category)
	}
	reader := csv.NewReader(r)
	reader.FieldsPerRecord = len(header)
	got, err := reader.Read()
	if err != nil {
		return domain.CuratedRules{}, fmt.Errorf("%w: read header: %w", domain.ErrInvalidRule, err)
	}
	if !slices.Equal(trimAll(got), header) {
		return domain.CuratedRules{}, fmt.Errorf("%w: header %v, want %v", domain.ErrInvalidRule, got, header)
	}

	var rules domain.CuratedRules
	for {
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			return rules, nil
		}
		if err != nil {
			return domain.CuratedRules{}, fmt.Errorf("%w: %w", domain.ErrInvalidRule, err)
		}
		line, _ := reader.FieldPos(0)
		if err := appendRule(&rules, category, trimAll(record)); err != nil {
			return domain.CuratedRules{}, fmt.Errorf("line %d: %w", line, err)
		}
	}
}

func appendRule(rules *domain.CuratedRules, category domain.Category, f []string) error {
	if category == domain.CategoryImportRestriction {
		validity, err := parseValidity(f[3], f[4])
		if err != nil {
			return err
		}
		rules.Restrictions = append(rules.Restrictions, domain.ImportRestriction{
			HSCode: f[0], OriginCountry: f[1], Description: f[2], SourceReference: f[5], Validity: validity,
		})
		return nil
	}
	validity, err := parseValidity(f[5], f[6])
	if err != nil {
		return err
	}
	rules.Permits = append(rules.Permits, domain.PermitRequirement{
		HSCode: f[0], OriginCountry: f[1], PermitCode: f[2], IssuingAgency: f[3], DocumentType: f[4],
		SourceReference: f[7], Validity: validity,
	})
	return nil
}

func parseValidity(from, to string) (domain.Validity, error) {
	start, err := time.Parse(time.DateOnly, from)
	if err != nil {
		return domain.Validity{}, fmt.Errorf("%w: effective_from: %w", domain.ErrInvalidRule, err)
	}
	var end *time.Time
	if to != "" {
		t, err := time.Parse(time.DateOnly, to)
		if err != nil {
			return domain.Validity{}, fmt.Errorf("%w: effective_to: %w", domain.ErrInvalidRule, err)
		}
		end = &t
	}
	return domain.NewValidity(start, end)
}

func trimAll(fields []string) []string {
	out := make([]string, len(fields))
	for i, f := range fields {
		out[i] = strings.TrimSpace(f)
	}
	return out
}
