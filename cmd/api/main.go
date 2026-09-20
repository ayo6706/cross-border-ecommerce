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

	httpAdapter "github.com/ayo6706/cross-border-ecommerce/internal/adapters/http"
	"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/postgres"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/config"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/logging"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load configuration: %v\n", err)
		os.Exit(1)
	}

	logger := logging.NewLogger(cfg.Log)
	slog.SetDefault(logger)

	logger.Info("starting api server",
		slog.String("service", cfg.App.ServiceName),
		slog.String("environment", cfg.App.Environment),
		slog.String("port", cfg.Server.Port),
		slog.String("database_url", cfg.Database.RedactedURL()),
	)

	// Context for initialization
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
		logger.Warn("postgres connection failed during startup (readiness probe will report NOT_READY)",
			slog.String("error", err.Error()),
		)
	} else {
		logger.Info("connected to postgresql successfully",
			slog.Int("max_conns", int(cfg.Database.MaxConns)),
			slog.Int("min_conns", int(cfg.Database.MinConns)),
		)
		defer dbPool.Close()
	}

	var pinger httpAdapter.Pinger
	if dbPool != nil {
		pinger = dbPool
	}

	handler := httpAdapter.NewRouter(httpAdapter.RouterConfig{
		Logger: logger,
		DB:     pinger,
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
		logger.Error("server fatal runtime error", slog.String("error", err.Error()))
		os.Exit(1)
	case sig := <-shutdown:
		logger.Info("shutdown signal received, commencing graceful shutdown", slog.String("signal", sig.String()))

		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
		defer cancelShutdown()

		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.Error("graceful server shutdown failed, forcing close", slog.String("error", err.Error()))
			_ = srv.Close()
			os.Exit(1)
		}

		logger.Info("server exited gracefully")
	}
}
