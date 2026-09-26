package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	appProduct "github.com/ayo6706/cross-border-ecommerce/internal/application/product"
	"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/postgres"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/config"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/logging"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "ingest error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) < 2 {
		return fmt.Errorf("usage: ingest <command> [options]\ncommands: process")
	}

	command := os.Args[1]
	switch command {
	case "process":
		return runProcess(os.Args[2:])
	default:
		return fmt.Errorf("unknown command %q (expected 'process')", command)
	}
}

//nolint:funlen // legacy baseline 2026-09-26: fix in ENG-018
func runProcess(args []string) error {
	fs := flag.NewFlagSet("process", flag.ExitOnError)
	runID := fs.String("run", "", "Ingestion run UUID to process")
	fromStart := fs.Bool("from-start", false, "Reset processing cursor and counters to re-process from start")
	batchSize := fs.Int("batch-size", 500, "Batch size for keyset pagination")
	leaseSec := fs.Int("lease", 30, "Lease duration in seconds")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if strings.TrimSpace(*runID) == "" {
		return fmt.Errorf("--run <id> is required")
	}

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	logger := logging.NewLogger(os.Stdout, logging.Options{
		Level:     cfg.Log.Level,
		Format:    cfg.Log.Format,
		AddSource: cfg.Log.AddSource,
	})
	slog.SetDefault(logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-shutdown
		logger.Info("cancellation signal received, stopping processing gracefully")
		cancel()
	}()

	pool, err := postgres.NewPool(
		ctx,
		cfg.Database.URL,
		postgres.WithMaxConns(cfg.Database.MaxConns),
		postgres.WithMinConns(cfg.Database.MinConns),
	)
	if err != nil {
		return fmt.Errorf("connect to database: %w", err)
	}
	defer pool.Close()

	runRepo, err := postgres.NewIngestionRepository(pool)
	if err != nil {
		return err
	}
	sourceRepo, err := postgres.NewSourceRepository(pool)
	if err != nil {
		return err
	}
	rawRepo, err := postgres.NewRawRecordRepository(pool)
	if err != nil {
		return err
	}
	processingRepo, err := postgres.NewRunProcessingRepository(pool)
	if err != nil {
		return err
	}
	txRunner, err := postgres.NewProductTxManager(pool)
	if err != nil {
		return err
	}

	processor, err := appProduct.NewRunProcessor(runRepo, sourceRepo, rawRepo, processingRepo, txRunner)
	if err != nil {
		return fmt.Errorf("initialize processor: %w", err)
	}

	logger.Info("starting run processing",
		slog.String("run_id", *runID),
		slog.Bool("from_start", *fromStart),
	)

	result, err := processor.ProcessRun(ctx, *runID, appProduct.ProcessRunOptions{
		LeaseDuration: time.Duration(*leaseSec) * time.Second,
		BatchSize:     *batchSize,
		FromStart:     *fromStart,
	})
	if err != nil {
		return fmt.Errorf("process run %s: %w", *runID, err)
	}

	logger.Info("run processing completed successfully",
		slog.String("run_id", result.RunID),
		slog.Int("records_seen", result.RecordsSeen),
		slog.Int("records_new", result.RecordsNew),
		slog.Int("records_changed", result.RecordsChanged),
		slog.Int("records_unchanged", result.RecordsUnchanged),
		slog.Int("records_failed", result.RecordsFailed),
	)

	return nil
}
