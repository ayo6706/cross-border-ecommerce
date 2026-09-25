package ingestion

import (
	"context"
	"time"
)

type RunProcessingRepository interface {
	SeedPending(ctx context.Context) error
	ClaimNext(ctx context.Context, claimToken string, leaseDuration time.Duration) (*RunProcessing, error)
	ClaimSpecific(ctx context.Context, runID string, claimToken string, leaseDuration time.Duration) (*RunProcessing, error)
	GetByID(ctx context.Context, runID string) (*RunProcessing, error)
	EnsureExists(ctx context.Context, runID string) (*RunProcessing, error)
	UpdateProgress(
		ctx context.Context,
		runID string,
		claimToken string,
		seen, newRecs, changed, unchanged, failed int,
		cursorID *string,
		leaseDuration time.Duration,
	) (*RunProcessing, error)
	Release(ctx context.Context, runID string, claimToken string) error
	Complete(ctx context.Context, runID string, claimToken string) error
	Fail(ctx context.Context, runID string, claimToken string, errSummary string) error
	ResetFromStart(ctx context.Context, runID string) (*RunProcessing, error)
}
