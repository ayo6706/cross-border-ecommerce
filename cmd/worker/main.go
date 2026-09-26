package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	appOutbox "github.com/ayo6706/cross-border-ecommerce/internal/application/outbox"
	appProduct "github.com/ayo6706/cross-border-ecommerce/internal/application/product"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
	"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/postgres"
	"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/redis"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/config"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/logging"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/uuid"
	"golang.org/x/sync/errgroup"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "worker error: %v\n", err)
		os.Exit(1)
	}
}

//nolint:funlen // legacy baseline 2026-09-26: fix in ENG-018
func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}

	if err := cfg.Redis.Validate(); err != nil {
		return fmt.Errorf("validate redis configuration: %w", err)
	}

	if err := cfg.Worker.ValidateAgainstDBPool(cfg.Database.MaxConns); err != nil {
		return fmt.Errorf("validate worker configuration: %w", err)
	}

	logger := logging.NewLogger(os.Stdout, logging.Options{
		Level:     cfg.Log.Level,
		Format:    cfg.Log.Format,
		AddSource: cfg.Log.AddSource,
	})
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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

	redisPub, err := redis.NewPublisher(ctx, cfg.Redis.URL, redis.WithRetention(cfg.Stream.Retention))
	if err != nil {
		return fmt.Errorf("connect to redis: %w", err)
	}
	defer func() {
		if err := redisPub.Close(); err != nil {
			logger.Error("close redis publisher", slog.Any("error", err))
		}
	}()

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
	outboxRepo, err := postgres.NewOutboxRepository(pool)
	if err != nil {
		return err
	}

	processor, err := appProduct.NewRunProcessor(runRepo, sourceRepo, rawRepo, processingRepo, txRunner)
	if err != nil {
		return fmt.Errorf("initialize processor: %w", err)
	}

	relay, err := appOutbox.NewRelay(outboxRepo, redisPub, appOutbox.RelayConfig{
		BatchSize:    cfg.Outbox.BatchSize,
		PollInterval: cfg.Outbox.PollInterval,
		Lease:        cfg.Outbox.Lease,
		BaseBackoff:  cfg.Outbox.BaseBackoff,
		MaxBackoff:   cfg.Outbox.MaxBackoff,
		MaxAttempts:  cfg.Outbox.MaxAttempts,
	}, logger)
	if err != nil {
		return fmt.Errorf("initialize outbox relay: %w", err)
	}

	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		runProcessingLoop(gCtx, processingRepo, processor, logger)
		return nil
	})

	g.Go(func() error {
		return relay.Run(gCtx)
	})

	if err := g.Wait(); err != nil && ctx.Err() == nil {
		return fmt.Errorf("worker group execution error: %w", err)
	}

	logger.Info("worker daemon stopped gracefully")
	return nil
}

//nolint:funlen,gocognit // legacy baseline 2026-09-26: fix in ENG-018
func runProcessingLoop(
	ctx context.Context,
	processingRepo ingestion.RunProcessingRepository,
	processor *appProduct.RunProcessor,
	logger *slog.Logger,
) {
	pollTicker := time.NewTicker(2 * time.Second)
	defer pollTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-pollTicker.C:
			if err := processingRepo.SeedPending(ctx); err != nil {
				if ctx.Err() == nil {
					logger.Error("failed to seed pending run processing", slog.Any("error", err))
				}
				continue
			}

			claimToken, err := uuid.NewString()
			if err != nil {
				logger.Error("failed to generate claim token for run processing", slog.Any("error", err))
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
				continue
			}

			logger.Info("claimed run processing job",
				slog.String("run_id", claimed.RunID),
				slog.String("claim_token", claimToken),
			)

			result, err := processor.ProcessRun(ctx, claimed.RunID, appProduct.ProcessRunOptions{
				ClaimToken:    claimToken,
				LeaseDuration: 30 * time.Second,
				BatchSize:     500,
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
}
