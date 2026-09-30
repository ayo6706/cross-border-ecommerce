package regulatory

import (
	"context"
	"time"

	domain "github.com/ayo6706/cross-border-ecommerce/internal/domain/regulatory"
)

// DatasetRepository persists datasets. SaveLifecycle writes the status, review and lifecycle
// timestamps only if the stored status is still `expected` (ErrInvalidTransition otherwise).
type DatasetRepository interface {
	Create(ctx context.Context, d *domain.Dataset) error
	GetForUpdate(ctx context.Context, id string) (*domain.Dataset, error)
	// ActiveForUpdate returns the ACTIVE version of a key, locked, and false if there is none.
	ActiveForUpdate(
		ctx context.Context, jurisdiction string, category domain.Category, source string,
	) (*domain.Dataset, bool, error)
	SaveLifecycle(ctx context.Context, d *domain.Dataset, expected domain.Status) error
	ActiveAt(
		ctx context.Context, jurisdiction string, category domain.Category, at time.Time,
	) ([]*domain.Dataset, error)
}

type RuleWriter interface {
	InsertCurated(ctx context.Context, datasetID string, rules domain.CuratedRules) error
}

type OutboxWriter interface {
	CreateDatasetActivatedEvent(ctx context.Context, e domain.DatasetActivated) error
}

type TxRepos struct {
	Datasets DatasetRepository
	Rules    RuleWriter
	Outbox   OutboxWriter
}

type TxRunner interface {
	WithinTx(ctx context.Context, fn func(repos TxRepos) error) error
}
