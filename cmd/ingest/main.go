package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/postgres"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/config"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/logging"
	"github.com/ayo6706/cross-border-ecommerce/internal/wiring"
)

func main() {
	err := run(os.Args[1:])
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "ingest error: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: ingest <command> [options]\ncommands: process")
	}
	switch args[0] {
	case "process":
		return runProcess(args[1:])
	default:
		return fmt.Errorf("unknown command %q (expected 'process')", args[0])
	}
}

// runProcess processes one run now. Configuration loads first because it supplies the flag
// defaults, so even -h needs a valid environment (DATABASE_URL).
func runProcess(args []string) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	p, err := parseProcessFlags(args, &cfg.RunProcessing)
	if err != nil {
		return err
	}

	logger := logging.NewLogger(os.Stdout, cfg.Log)
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	return processRun(ctx, cfg, p.runID, p.fromStart, logger)
}

type processArgs struct {
	runID     string
	fromStart bool
}

// parseProcessFlags reads the process flags. --lease and --batch-size default to RUN_PROCESSING_*
// (the worker's values) and override cfg in place; the result is validated like the environment.
func parseProcessFlags(args []string, cfg *config.RunProcessingConfig) (processArgs, error) {
	fs := flag.NewFlagSet("process", flag.ContinueOnError)
	runID := fs.String("run", "", "Ingestion run UUID to process")
	fromStart := fs.Bool("from-start", false, "Reset processing cursor and counters to re-process from start")
	fs.IntVar(&cfg.BatchSize, "batch-size", cfg.BatchSize, "Raw records per page")
	fs.DurationVar(&cfg.Lease, "lease", cfg.Lease, "Claim lease, e.g. 45s")
	if err := fs.Parse(args); err != nil {
		return processArgs{}, fmt.Errorf("parse process flags: %w", err)
	}
	if strings.TrimSpace(*runID) == "" {
		return processArgs{}, errors.New("--run <id> is required")
	}
	if err := cfg.Validate(); err != nil {
		return processArgs{}, fmt.Errorf("validate process flags: %w", err)
	}
	return processArgs{runID: strings.TrimSpace(*runID), fromStart: *fromStart}, nil
}

func processRun(ctx context.Context, cfg *config.Config, runID string, fromStart bool, logger *slog.Logger) error {
	pool, err := postgres.NewPool(ctx, cfg.Database)
	if err != nil {
		return fmt.Errorf("connect to database: %w", err)
	}
	defer pool.Close()

	processor, err := wiring.RunProcessor(pool)
	if err != nil {
		return fmt.Errorf("wire run processor: %w", err)
	}
	opts, err := wiring.ProcessRunOptions(cfg.RunProcessing, fromStart)
	if err != nil {
		return fmt.Errorf("build run processing options: %w", err)
	}

	logger.Info("starting run processing", slog.String("run_id", runID), slog.Bool("from_start", fromStart))
	result, err := processor.ProcessRun(ctx, runID, opts)
	if err != nil {
		return fmt.Errorf("process run %s: %w", runID, err)
	}
	logger.Info("run processing completed successfully", slog.Any("result", result))
	return nil
}
