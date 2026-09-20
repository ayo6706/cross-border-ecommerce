package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	logger.Info("starting stream consumer and outbox worker daemon")

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
				logger.Info("worker heartbeat", slog.Time("timestamp", t))
			}
		}
	}()

	sig := <-shutdown
	logger.Info("shutdown signal received, draining worker pool", slog.String("signal", sig.String()))
	cancel()

	wg.Wait()
	logger.Info("worker daemon stopped cleanly")
}
