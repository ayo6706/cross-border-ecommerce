package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/regulatory"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/config"
	"github.com/ayo6706/cross-border-ecommerce/internal/testsupport"
)

func regloadEnv(databaseURL string) map[string]string {
	env := map[string]string{"DATABASE_URL": databaseURL}
	for _, c := range regulatory.Categories() {
		env[config.SLAVariable(c)] = "720h"
	}
	return env
}

// Each invocation is refused before configuration loads: the environment is empty, so reaching
// config would fail with "load config" instead.
func TestRun_RejectsBadInvocationsBeforeConnecting(t *testing.T) {
	malformed := filepath.Join(t.TempDir(), "bad_date.csv")
	badDate := "hs_code,origin_country,description,effective_from,effective_to,source_reference\n" +
		"6309,*,Used clothing,01/01/2019,,ref\n"
	if err := os.WriteFile(malformed, []byte(badDate), 0o600); err != nil {
		t.Fatalf("write csv: %v", err)
	}
	cases := map[string][]string{
		"no command":         nil,
		"unknown command":    {"activte", "--dataset", "x"},
		"missing flag":       {"activate"},
		"unknown flag":       {"activate", "--dataset", "x", "--force"},
		"unknown category":   {"coverage", "--jurisdiction", "NG", "--category", "VAT"},
		"malformed time":     {"coverage", "--jurisdiction", "NG", "--category", "TARIFF", "--at", "yesterday"},
		"missing load flags": {"load-curated", "--file", "x.csv"},
		"malformed csv": {"load-curated", "--file", malformed, "--jurisdiction", "NG",
			"--category", "IMPORT_RESTRICTION", "--source", "s", "--version", "v", "--fetched-at", "2026-09-29T10:00:00Z",
			"--licence", "l", "--attribution", "a", "--loaded-by", "alice"},
		"missing file": {"load-curated", "--file", filepath.Join(t.TempDir(), "absent.csv"), "--jurisdiction", "NG",
			"--category", "PERMIT", "--source", "s", "--version", "v", "--fetched-at", "2026-09-29T10:00:00Z",
			"--licence", "l", "--attribution", "a", "--loaded-by", "alice"},
	}
	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			err := run(context.Background(), args, func(string) string { return "" }, &bytes.Buffer{})
			if err == nil || strings.Contains(err.Error(), "load config") {
				t.Fatalf("want a usage error before loading config, got %v", err)
			}
			if name == "malformed csv" && !errors.Is(err, regulatory.ErrInvalidRule) {
				t.Fatalf("malformed csv: want ErrInvalidRule, got %v", err)
			}
		})
	}
}

func TestRun_MissingSLAFailsAtStartup(t *testing.T) {
	env := regloadEnv("postgres://user:pass@localhost:1/db")
	delete(env, config.SLAVariable(regulatory.CategorySanctions))
	err := run(context.Background(), []string{"activate", "--dataset", "x"}, func(k string) string { return env[k] }, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "REGULATORY_SLA_SANCTIONS") {
		t.Fatalf("want a config error naming REGULATORY_SLA_SANCTIONS, got %v", err)
	}
}

func TestRun_LoadReviewActivateCoverage_Live(t *testing.T) {
	pool := testsupport.LiveDB(t)
	testsupport.Truncate(t, pool, "regulatory_datasets, import_restrictions, permit_requirements, outbox_events")
	env := regloadEnv(os.Getenv("TEST_DATABASE_URL"))
	regload := func(args ...string) (string, error) {
		var out bytes.Buffer
		err := run(context.Background(), args, func(k string) string { return env[k] }, &out)
		return out.String(), err
	}

	file := filepath.Join(t.TempDir(), "ng_restrictions.csv")
	csv := "hs_code,origin_country,description,effective_from,effective_to,source_reference\n" +
		"6309,*,Used clothing,2019-01-01,,NCS import prohibition list item 13\n"
	if err := os.WriteFile(file, []byte(csv), 0o600); err != nil {
		t.Fatalf("write csv: %v", err)
	}
	loaded, err := regload("load-curated", "--file", file, "--jurisdiction", "ng", "--category", "import_restriction",
		"--source", "ng_prohibition_list", "--version", "2026-09", "--fetched-at", "2026-09-29T08:00:00Z",
		"--licence", "Public sector information", "--attribution", "Nigeria Customs Service", "--loaded-by", "alice")
	if err != nil {
		t.Fatalf("load-curated: %v", err)
	}
	id := strings.Fields(loaded)[0]
	if !strings.Contains(loaded, "LOADED\tNG\tIMPORT_RESTRICTION") {
		t.Fatalf("load-curated printed %q", loaded)
	}

	if _, err := regload("activate", "--dataset", id); !errors.Is(err, regulatory.ErrReviewRequired) {
		t.Fatalf("activate before review: want ErrReviewRequired, got %v", err)
	}
	if _, err := regload("review", "--dataset", id, "--reviewer", "bob", "--note", "matches the gazette"); err != nil {
		t.Fatalf("review: %v", err)
	}
	if out, err := regload("activate", "--dataset", id); err != nil || !strings.Contains(out, id+"\tACTIVE") {
		t.Fatalf("activate: %q, %v", out, err)
	}

	out, err := regload("coverage", "--jurisdiction", "NG", "--category", "IMPORT_RESTRICTION")
	if err != nil || !strings.Contains(out, id+"\tACTIVE") {
		t.Fatalf("coverage of the activated category: %q, %v", out, err)
	}
	out, err = regload("coverage", "--jurisdiction", "NG", "--category", "PERMIT")
	if !errors.Is(err, regulatory.ErrNoCoverage) || !strings.Contains(out, "HOLD NO_REGULATORY_COVERAGE") {
		t.Fatalf("coverage of an unloaded category: %q, %v", out, err)
	}
}
