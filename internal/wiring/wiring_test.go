package wiring_test

import (
	"testing"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/config"
	"github.com/ayo6706/cross-border-ecommerce/internal/wiring"
)

// ProcessRun rejects options without a claim token or a valid error budget before touching the
// database, so the options the worker and cmd/ingest build must carry both.
func TestProcessRunOptions_CarriesConfigAndFreshClaimToken(t *testing.T) {
	cfg := config.RunProcessingConfig{
		PollInterval: time.Second,
		Lease:        45 * time.Second,
		BatchSize:    250,
		ErrorBudget:  ingestion.ErrorBudget{MaxErrorRate: 0.1, MinSampleRows: 50},
	}

	opts, err := wiring.ProcessRunOptions(cfg, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.ClaimToken == "" {
		t.Fatal("claim token must be generated")
	}
	if opts.LeaseDuration != cfg.Lease || opts.BatchSize != cfg.BatchSize || opts.ErrorBudget != cfg.ErrorBudget ||
		!opts.FromStart {
		t.Fatalf("configuration not carried through: %+v", opts)
	}

	again, err := wiring.ProcessRunOptions(cfg, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if again.ClaimToken == opts.ClaimToken {
		t.Fatal("each call needs its own claim token")
	}
}
