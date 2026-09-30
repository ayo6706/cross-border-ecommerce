package regulatory_test

import (
	"errors"
	"strings"
	"testing"

	adapter "github.com/ayo6706/cross-border-ecommerce/internal/adapters/regulatory"
	domain "github.com/ayo6706/cross-border-ecommerce/internal/domain/regulatory"
)

const restrictionHeader = "hs_code,origin_country,description,effective_from,effective_to,source_reference\n"

func TestParseCurated_Restrictions(t *testing.T) {
	csv := restrictionHeader +
		"6309,*,Used clothing,2019-01-01,,NCS prohibition list item 13\r\n" +
		"\"0207\",BR,\"Frozen poultry, parts\",2019-01-01,2026-01-01,NCS item 2\n"
	rules, err := adapter.ParseCurated(domain.CategoryImportRestriction, strings.NewReader(csv))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(rules.Restrictions) != 2 || len(rules.Permits) != 0 {
		t.Fatalf("got %d restrictions, %d permits", len(rules.Restrictions), len(rules.Permits))
	}
	first, second := rules.Restrictions[0], rules.Restrictions[1]
	if first.Validity.To != nil || first.OriginCountry != "*" {
		t.Errorf("first = %+v, want open-ended for any origin", first)
	}
	if second.Description != "Frozen poultry, parts" || second.Validity.To == nil || second.Validity.To.Year() != 2026 {
		t.Errorf("second = %+v", second)
	}
}

func TestParseCurated_Permits(t *testing.T) {
	csv := "hs_code,origin_country,permit_code,issuing_agency,document_type,effective_from,effective_to,source_reference\n" +
		"080450,*,USDA-PPQ-587,USDA APHIS,Plant import permit,2020-01-01,,7 CFR 319.56\n"
	rules, err := adapter.ParseCurated(domain.CategoryPermit, strings.NewReader(csv))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(rules.Permits) != 1 || rules.Permits[0].PermitCode != "USDA-PPQ-587" {
		t.Fatalf("permits = %+v", rules.Permits)
	}
}

func TestParseCurated_RejectsMalformedFiles(t *testing.T) {
	cases := map[string]struct {
		category domain.Category
		csv      string
		wantLine string
	}{
		"empty file":     {domain.CategoryImportRestriction, "", ""},
		"missing column": {domain.CategoryImportRestriction, "hs_code,origin_country,description,effective_from,effective_to\n", ""},
		"permit header for a restriction file": {domain.CategoryImportRestriction,
			"hs_code,origin_country,permit_code,issuing_agency,document_type,effective_from,effective_to,source_reference\n", ""},
		"short row":               {domain.CategoryImportRestriction, restrictionHeader + "6309,*,Used clothing,2019-01-01,\n", ""},
		"bad date":                {domain.CategoryImportRestriction, restrictionHeader + "6309,*,x,01/01/2019,,ref\n", "line 2"},
		"end before start":        {domain.CategoryImportRestriction, restrictionHeader + "6309,*,x,2019-01-01,2018-01-01,ref\n", "line 2"},
		"category without format": {domain.CategoryTariff, restrictionHeader, ""},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := adapter.ParseCurated(tc.category, strings.NewReader(tc.csv))
			if !errors.Is(err, domain.ErrInvalidRule) {
				t.Fatalf("want ErrInvalidRule, got %v", err)
			}
			if !strings.Contains(err.Error(), tc.wantLine) {
				t.Fatalf("error %q does not name %q", err, tc.wantLine)
			}
		})
	}
}
