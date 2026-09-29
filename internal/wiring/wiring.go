// Package wiring composes PostgreSQL adapters into the application services the binaries run.
// cmd/api, cmd/worker, cmd/ingest and the query-plan suite (tests/performance) share it, so the
// suite profiles the same composition production runs.
package wiring

import (
	"fmt"
	"log/slog"
	"net/http"

	"github.com/ayo6706/cross-border-ecommerce/internal/adapters/httpapi"
	appDLQ "github.com/ayo6706/cross-border-ecommerce/internal/application/dlq"
	appProduct "github.com/ayo6706/cross-border-ecommerce/internal/application/product"
	"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/postgres"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/config"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func RunProcessor(pool *pgxpool.Pool) (*appProduct.RunProcessor, error) {
	runRepo, err := postgres.NewIngestionRepository(pool)
	if err != nil {
		return nil, fmt.Errorf("create ingestion repository: %w", err)
	}
	sourceRepo, err := postgres.NewSourceRepository(pool)
	if err != nil {
		return nil, fmt.Errorf("create source repository: %w", err)
	}
	rawRepo, err := postgres.NewRawRecordRepository(pool)
	if err != nil {
		return nil, fmt.Errorf("create raw record repository: %w", err)
	}
	processingRepo, err := postgres.NewRunProcessingRepository(pool)
	if err != nil {
		return nil, fmt.Errorf("create run processing repository: %w", err)
	}
	txRunner, err := postgres.NewProductTxManager(pool)
	if err != nil {
		return nil, fmt.Errorf("create product transaction manager: %w", err)
	}
	processor, err := appProduct.NewRunProcessor(runRepo, sourceRepo, rawRepo, processingRepo, txRunner)
	if err != nil {
		return nil, fmt.Errorf("create run processor: %w", err)
	}
	return processor, nil
}

// ProcessRunOptions builds the options of one ProcessRun or ProcessNext call from configuration,
// with a fresh claim token: a token identifies a single claim and is never reused.
func ProcessRunOptions(cfg config.RunProcessingConfig, fromStart bool) (appProduct.ProcessRunOptions, error) {
	claimToken, err := uuid.NewString()
	if err != nil {
		return appProduct.ProcessRunOptions{}, fmt.Errorf("generate claim token: %w", err)
	}
	return appProduct.ProcessRunOptions{
		ClaimToken:    claimToken,
		LeaseDuration: cfg.Lease,
		BatchSize:     cfg.BatchSize,
		FromStart:     fromStart,
		ErrorBudget:   cfg.ErrorBudget,
	}, nil
}

func APIHandler(logger *slog.Logger, pool *pgxpool.Pool) (http.Handler, error) {
	dlqTx, err := postgres.NewDLQTxManager(pool)
	if err != nil {
		return nil, fmt.Errorf("create dlq transaction manager: %w", err)
	}
	dlqReplayer, err := appDLQ.NewReplayService(dlqTx)
	if err != nil {
		return nil, fmt.Errorf("create dlq replay service: %w", err)
	}
	productRepo, err := postgres.NewProductRepository(pool)
	if err != nil {
		return nil, fmt.Errorf("create product repository: %w", err)
	}
	products, err := appProduct.NewService(productRepo)
	if err != nil {
		return nil, fmt.Errorf("create product service: %w", err)
	}
	return httpapi.NewRouter(httpapi.RouterConfig{
		Logger:      logger,
		DB:          pool,
		DLQReplayer: dlqReplayer,
		Products:    products,
	}), nil
}
