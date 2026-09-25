package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	appProduct "github.com/ayo6706/cross-border-ecommerce/internal/application/product"
	"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/postgres"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/config"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/logging"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/uuid"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "worker error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
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

	logger.Info("starting stream consumer and outbox worker daemon",
		slog.String("service", cfg.App.ServiceName),
		slog.String("environment", cfg.App.Environment),
	)

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

	var wg sync.WaitGroup

	// Background worker loop for claiming and processing ingestion runs
	wg.Add(1)
	go func() {
		defer wg.Done()
		pollTicker := time.NewTicker(2 * time.Second)
		defer pollTicker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-pollTicker.C:
				// 1. Seed any completed/partial runs that haven't been queued yet
				if err := processingRepo.SeedPending(ctx); err != nil {
					if ctx.Err() == nil {
						logger.Error("failed to seed pending run processing", slog.Any("error", err))
					}
					continue
				}

				// 2. Attempt to claim next available run
				claimToken, err := uuid.NewString()
				if err != nil {
					continue
				}

				claimed, err := processingRepo.ClaimNext(ctx, claimToken, 30*time.Second)
				if err != nil {
					if ctx.Err() == nil {
						logger.Error("failed to claim run processing", slog.Any("error", err))
					}
					continue
				}
				if claimed == nil {
					// No runs ready to process
					continue
				}

				logger.Info("claimed run processing job",
					slog.String("run_id", claimed.RunID),
					slog.String("claim_token", claimToken),
				)

				result, err := processor.ProcessRun(ctx, claimed.RunID, appProduct.ProcessRunOptions{
					ClaimToken:    claimToken,
					LeaseDuration: 30 * time.Second,
					BatchSize:     50,
				})
				if err != nil {
					if ctx.Err() == nil {
						logger.Error("run processing failed",
							slog.String("run_id", claimed.RunID),
							slog.Any("error", err),
						)
					}
				} else {
					logger.Info("completed run processing job",
						slog.String("run_id", result.RunID),
						slog.Int("seen", result.RecordsSeen),
						slog.Int("new", result.RecordsNew),
						slog.Int("changed", result.RecordsChanged),
						slog.Int("unchanged", result.RecordsUnchanged),
						slog.Int("failed", result.RecordsFailed),
					)
				}
			}
		}
	}()

	sig := <-shutdown
	logger.Info("shutdown signal received, stopping worker", slog.String("signal", sig.String()))
	cancel()

	wg.Wait()
	logger.Info("worker daemon stopped gracefully")
	return nil
}
