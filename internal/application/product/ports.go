package product

import (
	"context"

	domainIngestion "github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
	domainProduct "github.com/ayo6706/cross-border-ecommerce/internal/domain/product"
)

type OutboxWriter interface {
	CreateProductChangedEvents(ctx context.Context, events []domainProduct.ProductChanged) error
}

type TxRepos struct {
	Products      domainProduct.Repository
	RunProcessing domainIngestion.RunProcessingRepository
	Outbox        OutboxWriter
}

type TxRunner interface {
	WithinTx(ctx context.Context, fn func(repos TxRepos) error) error
}
