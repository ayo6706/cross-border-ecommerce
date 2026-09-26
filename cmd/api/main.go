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

	"github.com/ayo6706/cross-border-ecommerce/internal/adapters/httpapi"
	appDLQ "github.com/ayo6706/cross-border-ecommerce/internal/application/dlq"
	"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/postgres"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/config"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/logging"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "api error: %v\n", err)
		os.Exit(1)
	}
}

//nolint:funlen // legacy baseline 2026-09-26: fix in ENG-018
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

	logger.Info("starting api server",
		slog.String("service", cfg.App.ServiceName),
		slog.String("environment", cfg.App.Environment),
		slog.String("port", cfg.Server.Port),
		slog.String("database_url", cfg.Database.RedactedURL()),
	)

	initCtx, cancelInit := context.WithTimeout(context.Background(), cfg.Database.ConnectTimeout+5*time.Second)
	defer cancelInit()

	dbPool, err := postgres.NewPool(
		initCtx,
		cfg.Database.URL,
		postgres.WithMaxConns(cfg.Database.MaxConns),
		postgres.WithMinConns(cfg.Database.MinConns),
		postgres.WithMaxConnIdleTime(cfg.Database.MaxConnIdleTime),
		postgres.WithMaxConnLifetime(cfg.Database.MaxConnLifetime),
		postgres.WithConnectTimeout(cfg.Database.ConnectTimeout),
	)
	if err != nil {
		return fmt.Errorf("connect to postgresql: %w", err)
	}
	defer dbPool.Close()

	logger.Info("connected to postgresql successfully",
		slog.Int("max_conns", int(cfg.Database.MaxConns)),
		slog.Int("min_conns", int(cfg.Database.MinConns)),
	)

	dlqTx, err := postgres.NewDLQTxManager(dbPool)
	if err != nil {
		return fmt.Errorf("create dlq transaction manager: %w", err)
	}
	dlqReplayer, err := appDLQ.NewReplayService(dlqTx)
	if err != nil {
		return fmt.Errorf("create dlq replay service: %w", err)
	}

	handler := httpapi.NewRouter(httpapi.RouterConfig{
		Logger:      logger,
		DB:          dbPool,
		DLQReplayer: dlqReplayer,
	})

	srv := &http.Server{
		Addr:         ":" + cfg.Server.Port,
		Handler:      handler,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
		IdleTimeout:  cfg.Server.IdleTimeout,
	}

	serverErrors := make(chan error, 1)
	go func() {
		logger.Info("http server listening", slog.String("addr", srv.Addr))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrors <- err
		}
	}()

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-serverErrors:
		return fmt.Errorf("server fatal error: %w", err)
	case sig := <-shutdown:
		logger.Info("shutdown signal received, commencing graceful shutdown", slog.String("signal", sig.String()))

		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
		defer cancelShutdown()

		if err := srv.Shutdown(shutdownCtx); err != nil {
			_ = srv.Close()
			return fmt.Errorf("graceful server shutdown failed: %w", err)
		}

		logger.Info("server exited gracefully")
	}

	return nil
}
