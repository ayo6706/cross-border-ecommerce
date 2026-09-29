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
	"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/postgres"
	"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/redis"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/config"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/logging"
	"github.com/ayo6706/cross-border-ecommerce/internal/wiring"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"
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
	if err := cfg.Redis.Validate(); err != nil {
		return fmt.Errorf("validate redis configuration: %w", err)
	}

	logger := logging.NewLogger(os.Stdout, cfg.Log)
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info("starting outbox worker and run processing daemon",
		slog.String("service", cfg.App.ServiceName),
		slog.String("environment", cfg.App.Environment),
	)

	pool, err := postgres.NewPool(ctx, cfg.Database)
	if err != nil {
		return fmt.Errorf("connect to database: %w", err)
	}
	defer pool.Close()

	publisher, err := redis.NewPublisher(ctx, cfg.Redis.URL, redis.WithRetention(cfg.Stream.Retention))
	if err != nil {
		return fmt.Errorf("connect to redis: %w", err)
	}
	defer func() {
		if err := publisher.Close(); err != nil {
			logger.Error("close redis publisher", slog.Any("error", err))
		}
	}()

	if err := serve(ctx, cfg, pool, publisher, logger); err != nil {
		return err
	}
	logger.Info("worker daemon stopped gracefully")
	return nil
}

// serve runs the run-processing loop and the outbox relay until ctx is cancelled or the relay fails.
func serve(ctx context.Context, cfg *config.Config, pool *pgxpool.Pool, publisher appOutbox.Publisher,
	logger *slog.Logger,
) error {
	processor, err := wiring.RunProcessor(pool)
	if err != nil {
		return fmt.Errorf("wire run processor: %w", err)
	}
	outboxRepo, err := postgres.NewOutboxRepository(pool)
	if err != nil {
		return fmt.Errorf("create outbox repository: %w", err)
	}
	relay, err := appOutbox.NewRelay(outboxRepo, publisher, appOutbox.RelayConfig{
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
		runProcessingLoop(gCtx, processor, cfg.RunProcessing, logger)
		return nil
	})
	g.Go(func() error {
		return relay.Run(gCtx)
	})
	if err := g.Wait(); err != nil {
		return fmt.Errorf("worker group execution error: %w", err)
	}
	return nil
}

// runProcessingLoop takes one processing step every poll interval until ctx is cancelled. A failed
// step is logged and retried on the next tick; the claimed run's lease lets another worker take over.
func runProcessingLoop(ctx context.Context, processor *appProduct.RunProcessor, cfg config.RunProcessingConfig,
	logger *slog.Logger,
) {
	ticker := time.NewTicker(cfg.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			processNext(ctx, processor, cfg, logger)
		}
	}
}

func processNext(ctx context.Context, processor *appProduct.RunProcessor, cfg config.RunProcessingConfig,
	logger *slog.Logger,
) {
	opts, err := wiring.ProcessRunOptions(cfg, false)
	if err != nil {
		logger.Error("build run processing options", slog.Any("error", err))
		return
	}
	result, err := processor.ProcessNext(ctx, opts)
	switch {
	case err != nil && ctx.Err() == nil:
		logger.Error("run processing step failed", slog.Any("error", err))
	case result != nil:
		logger.Info("completed run processing job", slog.Any("result", result))
	}
}
