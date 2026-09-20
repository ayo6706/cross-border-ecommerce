package ingestion

import (
	"context"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/source"
)

type Repository interface {
	CreateRun(ctx context.Context, run *IngestionRun) error
	FindRunByID(ctx context.Context, id string) (*IngestionRun, error)
	UpdateProgress(ctx context.Context, id string, metrics BatchMetrics, checkpoint string, updatedAt time.Time) error
	UpdateStatus(ctx context.Context, id string, status RunStatus, errorSummary string, checkpoint string, completedAt time.Time, updatedAt time.Time) error
	ListRunsBySource(ctx context.Context, sourceID source.ID, limit int) ([]*IngestionRun, error)
	FindLatestRunBySource(ctx context.Context, sourceID source.ID) (*IngestionRun, error)
}
