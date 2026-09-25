package ingestion

import (
	"context"
	"errors"
	"fmt"
	"time"

	domainIngestion "github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/source"
)

const runCleanupTimeout = 5 * time.Second

// SyncParams contains parameters required to execute an ingestion sync run.
// A zero ErrorBudget means domainIngestion.DefaultErrorBudget.
type SyncParams struct {
	SourceID          source.ID
	Adapter           domainIngestion.Adapter
	InitialCheckpoint string
	BatchSize         int
	ErrorBudget       domainIngestion.ErrorBudget
}

// SyncCoordinator orchestrates the end-to-end ingestion pipeline:
// adapter fetch -> atomic (save raw records + update run progress/checkpoint) transaction -> completion.
type SyncCoordinator struct {
	runService *Service
	txRunner   TxRunner
}

// NewSyncCoordinator creates a new SyncCoordinator.
func NewSyncCoordinator(runService *Service, txRunner TxRunner) (*SyncCoordinator, error) {
	if runService == nil {
		return nil, errors.New("run service is required")
	}
	if txRunner == nil {
		return nil, errors.New("tx runner is required")
	}
	return &SyncCoordinator{
		runService: runService,
		txRunner:   txRunner,
	}, nil
}

// SyncSource fetches batches from the adapter and persists each batch's records
// together with the run's progress and checkpoint in one transaction, so the
// checkpoint never advances past records that were not saved.
func (c *SyncCoordinator) SyncSource(ctx context.Context, params SyncParams) (*domainIngestion.IngestionRun, error) {
	if params.Adapter == nil {
		return nil, errors.New("source adapter is required")
	}

	budget := params.ErrorBudget.OrDefault()

	run, err := c.runService.StartRun(ctx, params.SourceID, params.InitialCheckpoint)
	if err != nil {
		return nil, fmt.Errorf("start ingestion run: %w", err)
	}

	var totalSeen, totalFailed int
	currentCP := run.Checkpoint
	for {
		if ctx.Err() != nil {
			return run, c.cancelRun(ctx, run.ID, "context cancelled during ingestion")
		}

		res, err := params.Adapter.Fetch(ctx, domainIngestion.FetchRequest{
			Checkpoint: currentCP,
			BatchSize:  params.BatchSize,
		})
		if err != nil {
			if ctx.Err() != nil {
				return run, c.cancelRun(ctx, run.ID, "context cancelled during adapter fetch")
			}
			return run, c.failRun(ctx, run.ID, fmt.Errorf("adapter fetch: %w", err))
		}

		if res.HasMore && res.NextCheckpoint == currentCP {
			return run, c.failRun(ctx, run.ID, fmt.Errorf(
				"%w: adapter returned hasMore=true with unadvanced checkpoint %q",
				domainIngestion.ErrSourceContractViolation, currentCP))
		}

		if err := c.persistBatch(ctx, run.ID, res); err != nil {
			if ctx.Err() != nil {
				return run, c.cancelRun(ctx, run.ID, "context cancelled during batch persistence")
			}
			return run, c.failRun(ctx, run.ID, fmt.Errorf("persist batch: %w", err))
		}

		totalSeen += len(res.Records) + res.Failed
		totalFailed += res.Failed
		if err := budget.Check(totalSeen, totalFailed); err != nil {
			return run, c.failRun(ctx, run.ID, err)
		}

		if res.NextCheckpoint != "" {
			currentCP = res.NextCheckpoint
		}
		if !res.HasMore || res.NextCheckpoint == "" {
			break
		}
	}

	if err := c.runService.CompleteRun(ctx, run.ID, currentCP); err != nil {
		if ctx.Err() != nil {
			return run, c.cancelRun(ctx, run.ID, "context cancelled during run completion")
		}
		return run, c.failRun(ctx, run.ID, fmt.Errorf("complete ingestion run: %w", err))
	}

	return c.runService.GetRun(ctx, run.ID)
}

// persistBatch saves the batch's records and advances run progress atomically.
// It runs even for batches without records so skipped rows and the new
// checkpoint are still recorded.
func (c *SyncCoordinator) persistBatch(ctx context.Context, runID string, res domainIngestion.FetchResult) error {
	now := time.Now().UTC()
	for _, rec := range res.Records {
		rec.IngestionRunID = runID
		if rec.ReceivedAt.IsZero() {
			rec.ReceivedAt = now
		}
	}

	metrics := domainIngestion.BatchMetrics{
		Seen:   len(res.Records) + res.Failed,
		Failed: res.Failed,
	}

	return c.txRunner.WithinTx(ctx, func(repos TxRepos) error {
		if err := repos.RawRecords.SaveBatch(ctx, res.Records); err != nil {
			return fmt.Errorf("save raw records: %w", err)
		}
		if err := repos.Runs.UpdateProgress(ctx, runID, metrics, res.NextCheckpoint, now); err != nil {
			return fmt.Errorf("update run progress: %w", err)
		}
		return nil
	})
}

// cancelRun marks the run CANCELLED on a context that survives the caller's
// cancellation, and returns the cancellation cause joined with any cleanup error.
func (c *SyncCoordinator) cancelRun(ctx context.Context, runID, reason string) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), runCleanupTimeout)
	defer cancel()

	cause := context.Cause(ctx)
	if cause == nil {
		cause = context.Canceled
	}
	if err := c.runService.CancelRun(cleanupCtx, runID, reason); err != nil {
		return errors.Join(cause, fmt.Errorf("mark run %s cancelled: %w", runID, err))
	}
	return cause
}

// failRun marks the run FAILED and returns runErr joined with any cleanup error.
func (c *SyncCoordinator) failRun(ctx context.Context, runID string, runErr error) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), runCleanupTimeout)
	defer cancel()

	if err := c.runService.FailRun(cleanupCtx, runID, runErr.Error()); err != nil {
		return errors.Join(runErr, fmt.Errorf("mark run %s failed: %w", runID, err))
	}
	return runErr
}
