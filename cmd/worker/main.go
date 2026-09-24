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

	"github.com/ayo6706/cross-border-ecommerce/internal/platform/config"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/logging"
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

	var wg sync.WaitGroup
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case t := <-ticker.C:
				logger.Info("worker daemon heartbeat", slog.Time("timestamp", t))
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
