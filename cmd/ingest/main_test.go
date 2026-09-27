package main

import (
	"errors"
	"testing"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/config"
)

// ProcessRun rejects options without a claim token or a valid error budget before touching
// the database, so the options cmd/ingest builds must carry both.
func TestProcessRunOptions_CarriesClaimTokenAndConfiguredBudget(t *testing.T) {
	cfg := config.IngestionConfig{ErrorBudgetMaxRate: 0.1, ErrorBudgetMinRows: 50}

	opts, err := processRunOptions(cfg, 30*time.Second, 500, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.ClaimToken == "" {
		t.Fatal("claim token must be generated")
	}
	if opts.ErrorBudget != (ingestion.ErrorBudget{MaxErrorRate: 0.1, MinSampleRows: 50}) {
		t.Fatalf("error budget must come from config, got %+v", opts.ErrorBudget)
	}
	if opts.LeaseDuration != 30*time.Second || opts.BatchSize != 500 || !opts.FromStart {
		t.Fatalf("flags not carried through: %+v", opts)
	}

	again, err := processRunOptions(cfg, 30*time.Second, 500, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if again.ClaimToken == opts.ClaimToken {
		t.Fatal("each invocation needs its own claim token")
	}
}

func TestProcessRunOptions_RejectsInvalidBudget(t *testing.T) {
	_, err := processRunOptions(config.IngestionConfig{}, 30*time.Second, 500, false)
	if !errors.Is(err, ingestion.ErrInvalidErrorBudget) {
		t.Fatalf("want ErrInvalidErrorBudget, got %v", err)
	}
}
