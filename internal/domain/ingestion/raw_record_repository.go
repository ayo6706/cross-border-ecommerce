package ingestion

import (
	"context"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/source"
)

type RawRecordRepository interface {
	Save(ctx context.Context, record *RawRecord) error
	SaveBatch(ctx context.Context, records []*RawRecord) error
	FindByID(ctx context.Context, id string) (*RawRecord, error)
	FindLatestBySourceAndExternalID(ctx context.Context, sourceID source.ID, externalProductID string) (*RawRecord, error)
	ListBySourceAndExternalID(ctx context.Context, sourceID source.ID, externalProductID string, limit int) ([]*RawRecord, error)
	ListByRunID(ctx context.Context, runID string, limit int) ([]*RawRecord, error)
}
