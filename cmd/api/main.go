package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/postgres"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/config"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/logging"
	"github.com/ayo6706/cross-border-ecommerce/internal/wiring"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "api error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}

	logger := logging.NewLogger(os.Stdout, cfg.Log)
	slog.SetDefault(logger)

	logger.Info("starting api server",
		slog.String("service", cfg.App.ServiceName),
		slog.String("environment", cfg.App.Environment),
		slog.String("port", cfg.Server.Port),
		slog.String("database_url", cfg.Database.RedactedURL()),
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := postgres.NewPool(ctx, cfg.Database)
	if err != nil {
		return fmt.Errorf("connect to postgresql: %w", err)
	}
	defer pool.Close()
	logger.Info("connected to postgresql",
		slog.Int("max_conns", int(cfg.Database.MaxConns)),
		slog.Int("min_conns", int(cfg.Database.MinConns)),
		slog.Duration("max_conn_idle_time", cfg.Database.MaxConnIdleTime),
		slog.Duration("max_conn_lifetime", cfg.Database.MaxConnLifetime),
	)

	handler, err := wiring.APIHandler(logger, pool)
	if err != nil {
		return fmt.Errorf("wire api handler: %w", err)
	}
	srv := &http.Server{
		Addr:         ":" + cfg.Server.Port,
		Handler:      handler,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
		IdleTimeout:  cfg.Server.IdleTimeout,
	}
	return serve(ctx, srv, cfg.Server.ShutdownTimeout, logger)
}

// serve runs srv until it fails or ctx is cancelled by a signal, then drains it within shutdownTimeout.
func serve(ctx context.Context, srv *http.Server, shutdownTimeout time.Duration, logger *slog.Logger) error {
	serverErrors := make(chan error, 1)
	go func() {
		logger.Info("http server listening", slog.String("addr", srv.Addr))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrors <- err
		}
	}()

	select {
	case err := <-serverErrors:
		return fmt.Errorf("server fatal error: %w", err)
	case <-ctx.Done():
	}

	logger.Info("shutdown signal received, commencing graceful shutdown")
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return errors.Join(fmt.Errorf("graceful server shutdown failed: %w", err), srv.Close())
	}
	logger.Info("server exited gracefully")
	return nil
}
