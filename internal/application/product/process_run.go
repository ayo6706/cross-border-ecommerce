package product

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	domainIngestion "github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
	domainProduct "github.com/ayo6706/cross-border-ecommerce/internal/domain/product"
	domainSource "github.com/ayo6706/cross-border-ecommerce/internal/domain/source"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/uuid"
)

type ProcessRunOptions struct {
	ClaimToken    string
	LeaseDuration time.Duration
	BatchSize     int
	FromStart     bool
	ErrorBudget   domainIngestion.ErrorBudget
}

type ProcessRunResult struct {
	RunID            string
	RecordsSeen      int
	RecordsNew       int
	RecordsChanged   int
	RecordsUnchanged int
	RecordsFailed    int
}

type RunProcessor struct {
	runRepo        domainIngestion.Repository
	sourceRepo     domainSource.Repository
	rawRepo        domainIngestion.RawRecordRepository
	processingRepo domainIngestion.RunProcessingRepository
	txRunner       TxRunner
}

func NewRunProcessor(
	runRepo domainIngestion.Repository,
	sourceRepo domainSource.Repository,
	rawRepo domainIngestion.RawRecordRepository,
	processingRepo domainIngestion.RunProcessingRepository,
	txRunner TxRunner,
) (*RunProcessor, error) {
	if runRepo == nil {
		return nil, errors.New("run repository is required")
	}
	if sourceRepo == nil {
		return nil, errors.New("source repository is required")
	}
	if rawRepo == nil {
		return nil, errors.New("raw record repository is required")
	}
	if processingRepo == nil {
		return nil, errors.New("processing repository is required")
	}
	if txRunner == nil {
		return nil, errors.New("tx runner is required")
	}

	return &RunProcessor{
		runRepo:        runRepo,
		sourceRepo:     sourceRepo,
		rawRepo:        rawRepo,
		processingRepo: processingRepo,
		txRunner:       txRunner,
	}, nil
}

//nolint:funlen,gocognit // legacy baseline 2026-09-26: fix in ENG-044
func (p *RunProcessor) ProcessRun(ctx context.Context, runID string, opts ProcessRunOptions) (*ProcessRunResult, error) {
	trimmedRunID := strings.TrimSpace(runID)
	if trimmedRunID == "" {
		return nil, errors.New("run id cannot be empty")
	}

	claimToken := strings.TrimSpace(opts.ClaimToken)
	if claimToken == "" {
		generatedToken, err := uuid.NewString()
		if err != nil {
			return nil, fmt.Errorf("generate claim token: %w", err)
		}
		claimToken = generatedToken
	}

	leaseDuration := opts.LeaseDuration
	if leaseDuration <= 0 {
		return nil, errors.New("lease duration must be greater than zero")
	}

	batchSize := opts.BatchSize
	if batchSize <= 0 {
		return nil, errors.New("batch size must be greater than zero")
	}

	budget := opts.ErrorBudget.OrDefault()

	cleanupCtx := context.WithoutCancel(ctx)

	run, err := p.runRepo.FindRunByID(cleanupCtx, trimmedRunID)
	if err != nil {
		return nil, fmt.Errorf("find ingestion run: %w", err)
	}
	if run.Status != domainIngestion.StatusCompleted && run.Status != domainIngestion.StatusPartial {
		return nil, fmt.Errorf("%w: run %s has status %s, want %s or %s", domainIngestion.ErrInvalidRunState,
			trimmedRunID, run.Status, domainIngestion.StatusCompleted, domainIngestion.StatusPartial)
	}

	_, err = p.processingRepo.EnsureExists(cleanupCtx, trimmedRunID)
	if err != nil {
		return nil, fmt.Errorf("ensure run processing exists: %w", err)
	}

	if opts.FromStart {
		_, err = p.processingRepo.ResetFromStart(cleanupCtx, trimmedRunID)
		if err != nil {
			return nil, fmt.Errorf("reset run processing from start: %w", err)
		}
	}

	rp, err := p.processingRepo.ClaimSpecific(cleanupCtx, trimmedRunID, claimToken, leaseDuration)
	if err != nil {
		return nil, fmt.Errorf("claim run processing: %w", err)
	}

	src, err := p.sourceRepo.FindByID(cleanupCtx, run.SourceID)
	if err != nil {
		if ctx.Err() != nil {
			releaseErr := p.processingRepo.Release(cleanupCtx, trimmedRunID, claimToken)
			return nil, errors.Join(ctx.Err(), releaseErr)
		}
		failErr := p.processingRepo.Fail(cleanupCtx, trimmedRunID, claimToken, fmt.Sprintf("find source: %v", err))
		return nil, errors.Join(fmt.Errorf("find source for run: %w", err), failErr)
	}

	fieldMapping, err := src.GetFieldMapping()
	if err != nil {
		failErr := p.processingRepo.Fail(cleanupCtx, trimmedRunID, claimToken, fmt.Sprintf("invalid field mapping: %v", err))
		return nil, errors.Join(fmt.Errorf("%w: %w", domainProduct.ErrInvalidFieldMapping, err), failErr)
	}
	if fieldMapping == nil {
		failErr := p.processingRepo.Fail(cleanupCtx, trimmedRunID, claimToken, "missing field mapping on source configuration")
		return nil, errors.Join(domainProduct.ErrMissingFieldMapping, failErr)
	}

	cursorID := rp.CursorRawRecordID

	for {
		if err := ctx.Err(); err != nil {
			releaseErr := p.processingRepo.Release(cleanupCtx, trimmedRunID, claimToken)
			return nil, errors.Join(err, releaseErr)
		}

		records, err := p.rawRepo.ListKeysetByRunID(ctx, trimmedRunID, cursorID, batchSize)
		if err != nil {
			if ctx.Err() != nil {
				releaseErr := p.processingRepo.Release(cleanupCtx, trimmedRunID, claimToken)
				return nil, errors.Join(ctx.Err(), releaseErr)
			}
			failErr := p.processingRepo.Fail(cleanupCtx, trimmedRunID, claimToken, fmt.Sprintf("list raw records: %v", err))
			return nil, errors.Join(fmt.Errorf("list raw records: %w", err), failErr)
		}

		if len(records) == 0 {
			break
		}

		validRecords := make([]domainProduct.BatchIncomingRecord, 0, len(records))
		failedCount := 0

		for _, record := range records {
			normalized, normErr := domainProduct.Normalize(record.Payload, *fieldMapping)
			if normErr != nil {
				failedCount++
				continue
			}

			fingerprint := domainProduct.Fingerprint(normalized)
			validRecords = append(validRecords, domainProduct.BatchIncomingRecord{
				RawRecordID:       record.ID,
				SourceID:          string(record.SourceID),
				ExternalProductID: record.ExternalProductID,
				Normalized:        &normalized,
				Fingerprint:       fingerprint,
				SourceUpdatedAt:   record.SourceUpdatedAt,
				ReceivedAt:        record.ReceivedAt,
				IngestionRunID:    trimmedRunID,
			})
		}

		lastRecordID := records[len(records)-1].ID
		_, err = p.processPageWithRetry(
			ctx,
			validRecords,
			len(records),
			failedCount,
			lastRecordID,
			trimmedRunID,
			claimToken,
			leaseDuration,
			budget,
		)
		if err != nil {
			if ctx.Err() != nil {
				releaseErr := p.processingRepo.Release(cleanupCtx, trimmedRunID, claimToken)
				return nil, errors.Join(ctx.Err(), releaseErr)
			}
			if errors.Is(err, domainIngestion.ErrLeaseLost) {
				return nil, err
			}
			failErr := p.processingRepo.Fail(cleanupCtx, trimmedRunID, claimToken, err.Error())
			return nil, errors.Join(err, failErr)
		}

		cursorID = &lastRecordID
	}

	if err := p.processingRepo.Complete(cleanupCtx, trimmedRunID, claimToken); err != nil {
		return nil, fmt.Errorf("complete run processing: %w", err)
	}

	finalState, err := p.processingRepo.GetByID(cleanupCtx, trimmedRunID)
	if err != nil {
		return nil, fmt.Errorf("get final processing state: %w", err)
	}

	return &ProcessRunResult{
		RunID:            trimmedRunID,
		RecordsSeen:      finalState.RecordsSeen,
		RecordsNew:       finalState.RecordsNew,
		RecordsChanged:   finalState.RecordsChanged,
		RecordsUnchanged: finalState.RecordsUnchanged,
		RecordsFailed:    finalState.RecordsFailed,
	}, nil
}

//nolint:funlen,gocognit // legacy baseline 2026-09-26: fix in ENG-044
func (p *RunProcessor) processPageWithRetry(
	ctx context.Context,
	validRecords []domainProduct.BatchIncomingRecord,
	totalSeenInPage int,
	failedInPage int,
	lastRecordID string,
	runID string,
	claimToken string,
	leaseDuration time.Duration,
	errorBudget domainIngestion.ErrorBudget,
) (*domainIngestion.RunProcessing, error) {
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		var updatedRp *domainIngestion.RunProcessing
		err := p.txRunner.WithinTx(ctx, func(repos TxRepos) error {
			var plan *domainProduct.BatchPlan
			if len(validRecords) > 0 {
				seenIdentities := make(map[string]struct{})
				identities := make([]domainProduct.IdentityRef, 0, len(validRecords))
				for _, r := range validRecords {
					key := domainProduct.IdentityKey(r.SourceID, r.ExternalProductID)
					if _, ok := seenIdentities[key]; !ok {
						seenIdentities[key] = struct{}{}
						identities = append(identities, domainProduct.IdentityRef{
							SourceID:          r.SourceID,
							ExternalProductID: r.ExternalProductID,
						})
					}
				}

				snapshots, err := repos.Products.FindSnapshotsByIdentities(ctx, identities)
				if err != nil {
					return fmt.Errorf("find snapshots by identities: %w", err)
				}

				computedPlan, err := domainProduct.DecideBatch(snapshots, validRecords, time.Now().UTC())
				if err != nil {
					return fmt.Errorf("decide batch plan: %w", err)
				}
				plan = computedPlan

				if err := repos.Products.ApplyBatch(ctx, plan); err != nil {
					return err
				}

				if err := repos.Outbox.CreateProductChangedEvents(ctx, plan.Events); err != nil {
					return fmt.Errorf("create product changed events: %w", err)
				}
			}

			newCount := 0
			changedCount := 0
			unchangedCount := 0
			if plan != nil {
				newCount = plan.RecordsNew
				changedCount = plan.RecordsChanged
				unchangedCount = plan.RecordsUnchanged
			}

			rp, err := repos.RunProcessing.UpdateProgress(
				ctx,
				runID,
				claimToken,
				totalSeenInPage,
				newCount,
				changedCount,
				unchangedCount,
				failedInPage,
				&lastRecordID,
				leaseDuration,
			)
			if err != nil {
				return err
			}
			updatedRp = rp
			return nil
		})

		if err == nil {
			if updatedRp != nil {
				if budgetErr := errorBudget.Check(updatedRp.RecordsSeen, updatedRp.RecordsFailed); budgetErr != nil {
					return nil, budgetErr
				}
			}
			return updatedRp, nil
		}

		if errors.Is(err, domainIngestion.ErrLeaseLost) {
			return nil, err
		}
		if errors.Is(err, domainProduct.ErrIdentityConflict) ||
			errors.Is(err, domainProduct.ErrVersionConflict) ||
			errors.Is(err, domainProduct.ErrDeadlockConflict) {
			lastErr = err
			continue
		}
		return nil, err
	}
	return nil, fmt.Errorf("page processing retry exhausted: %w", lastErr)
}
