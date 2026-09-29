package main

import (
	"errors"
	"flag"
	"testing"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/config"
)

func workerDefaults() config.RunProcessingConfig {
	return config.RunProcessingConfig{
		PollInterval: 2 * time.Second,
		Lease:        30 * time.Second,
		BatchSize:    500,
		ErrorBudget:  ingestion.ErrorBudget{MaxErrorRate: 0.05, MinSampleRows: 100},
	}
}

func TestParseProcessFlags(t *testing.T) {
	t.Run("defaults_come_from_configuration", func(t *testing.T) {
		cfg := workerDefaults()
		got, err := parseProcessFlags([]string{"--run", " run-1 "}, &cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != (processArgs{runID: "run-1"}) || cfg != workerDefaults() {
			t.Fatalf("got %+v with %+v; want run-1 and the configured lease/batch", got, cfg)
		}
	})

	t.Run("flags_override_configuration", func(t *testing.T) {
		cfg := workerDefaults()
		got, err := parseProcessFlags([]string{"--run", "run-1", "--from-start", "--lease", "45s", "--batch-size", "250"}, &cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !got.fromStart || cfg.Lease != 45*time.Second || cfg.BatchSize != 250 {
			t.Fatalf("got %+v with %+v; want from-start, lease 45s, batch 250", got, cfg)
		}
	})

	rejected := map[string][]string{
		"missing run":            {"--from-start"},
		"lease without a unit":   {"--run", "run-1", "--lease", "30"},
		"zero batch size":        {"--run", "run-1", "--batch-size", "0"},
		"non-positive lease":     {"--run", "run-1", "--lease", "0s"},
		"unknown flag":           {"--run", "run-1", "--sideways"},
		"non-numeric batch size": {"--run", "run-1", "--batch-size", "many"},
	}
	for name, args := range rejected {
		t.Run(name, func(t *testing.T) {
			cfg := workerDefaults()
			if _, err := parseProcessFlags(args, &cfg); err == nil {
				t.Fatalf("parseProcessFlags(%v) = nil error; want a rejection before connecting", args)
			}
		})
	}

	t.Run("help_is_not_a_failure", func(t *testing.T) {
		cfg := workerDefaults()
		if _, err := parseProcessFlags([]string{"-h"}, &cfg); !errors.Is(err, flag.ErrHelp) {
			t.Fatalf("error = %v; want flag.ErrHelp, which main exits 0 on", err)
		}
	})
}
