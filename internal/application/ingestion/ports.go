package ingestion

import (
	"context"

	domainIngestion "github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
)

// TxRepos holds repository instances bound to an active database transaction.
type TxRepos struct {
	Runs       domainIngestion.Repository
	RawRecords domainIngestion.RawRecordRepository
}

// TxRunner coordinates transactional execution across multiple ingestion repositories.
type TxRunner interface {
	WithinTx(ctx context.Context, fn func(repos TxRepos) error) error
}
