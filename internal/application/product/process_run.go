package product

import (
	"context"
	"encoding/json"
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
		leaseDuration = 30 * time.Second
	}

	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = 50
	}

	budget := opts.ErrorBudget.OrDefault()

	// Cleanup context ensuring status release/completion happens even if parent context cancels
	cleanupCtx := context.WithoutCancel(ctx)

	// 1. Verify ingestion run exists
	run, err := p.runRepo.FindRunByID(cleanupCtx, trimmedRunID)
	if err != nil {
		return nil, fmt.Errorf("find ingestion run: %w", err)
	}
	if run.Status != domainIngestion.StatusCompleted && run.Status != domainIngestion.StatusPartial {
		return nil, fmt.Errorf("%w: run %s has status %s, want %s or %s", domainIngestion.ErrInvalidRunState,
			trimmedRunID, run.Status, domainIngestion.StatusCompleted, domainIngestion.StatusPartial)
	}

	// 2. Ensure run processing state exists
	_, err = p.processingRepo.EnsureExists(cleanupCtx, trimmedRunID)
	if err != nil {
		return nil, fmt.Errorf("ensure run processing exists: %w", err)
	}

	// 3. Handle FromStart option
	if opts.FromStart {
		_, err = p.processingRepo.ResetFromStart(cleanupCtx, trimmedRunID)
		if err != nil {
			return nil, fmt.Errorf("reset run processing from start: %w", err)
		}
	}

	// 4. Claim the run processing lease
	rp, err := p.processingRepo.ClaimSpecific(cleanupCtx, trimmedRunID, claimToken, leaseDuration)
	if err != nil {
		return nil, fmt.Errorf("claim run processing: %w", err)
	}

	// 5. Load source configuration
	src, err := p.sourceRepo.FindByID(cleanupCtx, run.SourceID)
	if err != nil {
		if ctx.Err() != nil {
			releaseErr := p.processingRepo.Release(cleanupCtx, trimmedRunID, claimToken)
			return nil, errors.Join(ctx.Err(), releaseErr)
		}
		failErr := p.processingRepo.Fail(cleanupCtx, trimmedRunID, claimToken, fmt.Sprintf("find source: %v", err))
		return nil, errors.Join(fmt.Errorf("find source for run: %w", err), failErr)
	}

	// Rule 8: Fail loudly if FieldMapping is missing or invalid
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

	// 6. Iterate through raw records using keyset pagination
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
			// All records processed!
			break
		}

		for _, record := range records {
			if err := ctx.Err(); err != nil {
				releaseErr := p.processingRepo.Release(cleanupCtx, trimmedRunID, claimToken)
				return nil, errors.Join(err, releaseErr)
			}

			err := p.processRecordWithRetry(ctx, record, fieldMapping, trimmedRunID, claimToken, leaseDuration, budget)
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

			recordID := record.ID
			cursorID = &recordID
		}
	}

	// 7. Complete run processing
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

func (p *RunProcessor) processRecordWithRetry(
	ctx context.Context,
	record *domainIngestion.RawRecord,
	fieldMapping *domainProduct.FieldMapping,
	runID string,
	claimToken string,
	leaseDuration time.Duration,
	errorBudget domainIngestion.ErrorBudget,
) error {
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		var updatedRp *domainIngestion.RunProcessing
		err := p.txRunner.WithinTx(ctx, func(repos TxRepos) error {
			rp, txErr := p.processSingleRecord(ctx, repos, record, fieldMapping, runID, claimToken, leaseDuration)
			if txErr != nil {
				return txErr
			}
			updatedRp = rp
			return nil
		})
		if err == nil {
			if updatedRp != nil {
				if budgetErr := errorBudget.Check(updatedRp.RecordsSeen, updatedRp.RecordsFailed); budgetErr != nil {
					return budgetErr
				}
			}
			return nil
		}
		if errors.Is(err, domainIngestion.ErrLeaseLost) {
			return err
		}
		if errors.Is(err, domainProduct.ErrIdentityConflict) || errors.Is(err, domainProduct.ErrVersionConflict) {
			lastErr = err
			continue // Retry once on concurrent race conflict
		}
		return err
	}
	return fmt.Errorf("record processing retry exhausted: %w", lastErr)
}

func (p *RunProcessor) processSingleRecord(
	ctx context.Context,
	repos TxRepos,
	record *domainIngestion.RawRecord,
	fieldMapping *domainProduct.FieldMapping,
	runID string,
	claimToken string,
	leaseDuration time.Duration,
) (*domainIngestion.RunProcessing, error) {
	// 1. Normalization
	normalized, normErr := domainProduct.Normalize(record.Payload, *fieldMapping)
	if normErr != nil {
		// F4: Normalization failure
		rp, err := repos.RunProcessing.UpdateProgress(ctx, runID, claimToken, 1, 0, 0, 0, 1, &record.ID, leaseDuration)
		if err != nil {
			return nil, err
		}
		return rp, nil
	}

	fingerprint := domainProduct.Fingerprint(normalized)

	// 2. Identity lookup snapshot
	snapshot, err := repos.Products.FindSnapshotByIdentity(ctx, string(record.SourceID), record.ExternalProductID)
	if err != nil {
		return nil, fmt.Errorf("find snapshot by identity: %w", err)
	}

	incoming := domainProduct.IncomingRecord{
		Normalized:      &normalized,
		Fingerprint:     fingerprint,
		SourceUpdatedAt: record.SourceUpdatedAt,
		ReceivedAt:      record.ReceivedAt,
		RawRecordID:     record.ID,
	}

	// 3. Domain transition decision
	transition := domainProduct.DecideTransition(snapshot, incoming)

	switch transition.Type {
	case domainProduct.TransitionNew:
		if err := p.handleNewTransition(ctx, repos, record, &normalized, fingerprint, runID); err != nil {
			return nil, err
		}
		rp, err := repos.RunProcessing.UpdateProgress(ctx, runID, claimToken, 1, 1, 0, 0, 0, &record.ID, leaseDuration)
		return rp, err

	case domainProduct.TransitionChanged:
		if err := p.handleChangedTransition(ctx, repos, snapshot, record, &normalized, fingerprint, transition.ChangedFields, runID); err != nil {
			return nil, err
		}
		rp, err := repos.RunProcessing.UpdateProgress(ctx, runID, claimToken, 1, 0, 1, 0, 0, &record.ID, leaseDuration)
		return rp, err

	case domainProduct.TransitionUnchanged:
		if snapshot != nil {
			if err := repos.Products.UpdateSourceWatermark(ctx, snapshot.ProductSourceID, record.SourceUpdatedAt, record.ReceivedAt); err != nil {
				return nil, fmt.Errorf("update product source watermark: %w", err)
			}
		}
		rp, err := repos.RunProcessing.UpdateProgress(ctx, runID, claimToken, 1, 0, 0, 1, 0, &record.ID, leaseDuration)
		return rp, err

	case domainProduct.TransitionStale:
		// Older record received: no state mutations, count as unchanged
		rp, err := repos.RunProcessing.UpdateProgress(ctx, runID, claimToken, 1, 0, 0, 1, 0, &record.ID, leaseDuration)
		return rp, err

	case domainProduct.TransitionVersionMismatch:
		// Decision 4: Upgrade fingerprint only
		if snapshot != nil {
			if err := repos.Products.GuardedUpdateFingerprintOnly(ctx, snapshot.ProductID, snapshot.CurrentVersionID, fingerprint, time.Now().UTC()); err != nil {
				return nil, err
			}
			if err := repos.Products.UpdateSourceWatermark(ctx, snapshot.ProductSourceID, record.SourceUpdatedAt, record.ReceivedAt); err != nil {
				return nil, fmt.Errorf("update product source watermark: %w", err)
			}
		}
		rp, err := repos.RunProcessing.UpdateProgress(ctx, runID, claimToken, 1, 0, 0, 1, 0, &record.ID, leaseDuration)
		return rp, err

	default:
		return nil, fmt.Errorf("%w: unknown transition type %s", domainProduct.ErrInvalidTransition, transition.Type)
	}
}

func (p *RunProcessor) handleNewTransition(
	ctx context.Context,
	repos TxRepos,
	record *domainIngestion.RawRecord,
	normalized *domainProduct.NormalizedProduct,
	fingerprint string,
	runID string,
) error {
	prodID, err := uuid.NewString()
	if err != nil {
		return err
	}
	versionID, err := uuid.NewString()
	if err != nil {
		return err
	}
	changeID, err := uuid.NewString()
	if err != nil {
		return err
	}
	sourceID, err := uuid.NewString()
	if err != nil {
		return err
	}

	now := time.Now().UTC()

	// 1. Create Product
	prod := &domainProduct.Product{
		ID:                 domainProduct.ID(prodID),
		CanonicalName:      normalized.CanonicalName,
		Description:        normalized.Description,
		Brand:              normalized.Brand,
		OriginCountry:      normalized.OriginCountry,
		Status:             domainProduct.StatusDraft,
		CurrentVersionID:   &versionID,
		CurrentFingerprint: fingerprint,
		CreatedAt:          now,
		UpdatedAt:          now,
	}

	// 2. Create ProductSource
	ps := &domainProduct.ProductSource{
		ID:                  sourceID,
		ProductID:           prod.ID,
		SourceID:            string(record.SourceID),
		ExternalProductID:   record.ExternalProductID,
		FirstSeenAt:         now,
		LastChangedAt:       now,
		LastSourceUpdatedAt: record.SourceUpdatedAt,
		LastReceivedAt:      record.ReceivedAt,
	}

	if err := repos.Products.CreateProductWithSource(ctx, prod, ps); err != nil {
		return err
	}

	// 3. Create ProductVersion (v1)
	pv := &domainProduct.ProductVersion{
		ID:             versionID,
		ProductID:      prod.ID,
		VersionNumber:  1,
		Fingerprint:    fingerprint,
		CanonicalName:  normalized.CanonicalName,
		Description:    normalized.Description,
		Brand:          normalized.Brand,
		OriginCountry:  normalized.OriginCountry,
		Attributes:     normalized.Attributes,
		IngestionRunID: &runID,
		CreatedAt:      now,
	}
	if err := repos.Products.CreateVersion(ctx, pv); err != nil {
		return err
	}

	// 4. Create ProductChange (NEW)
	pc := &domainProduct.ProductChange{
		ID:             changeID,
		ProductID:      prod.ID,
		FromVersionID:  nil,
		ToVersionID:    versionID,
		ChangeType:     domainProduct.ChangeTypeNew,
		ChangedFields:  []string{},
		IngestionRunID: &runID,
		RawRecordID:    &record.ID,
		DetectedAt:     now,
	}
	if err := repos.Products.CreateChange(ctx, pc); err != nil {
		return err
	}

	// 5. Create Outbox Event
	eventPayload, err := json.Marshal(map[string]any{
		"product_id":     prodID,
		"version_id":     versionID,
		"version_number": 1,
		"fingerprint":    fingerprint,
		"change_type":    "NEW",
		"changed_fields": []string{},
	})
	if err != nil {
		return fmt.Errorf("marshal outbox event payload: %w", err)
	}

	if err := repos.Outbox.CreateEvent(ctx, domainProduct.AggregateTypeProduct, prodID, domainProduct.EventTypeProductChanged, eventPayload); err != nil {
		return fmt.Errorf("create outbox event: %w", err)
	}

	return nil
}

func (p *RunProcessor) handleChangedTransition(
	ctx context.Context,
	repos TxRepos,
	snapshot *domainProduct.Snapshot,
	record *domainIngestion.RawRecord,
	normalized *domainProduct.NormalizedProduct,
	fingerprint string,
	changedFields []string,
	runID string,
) error {
	if snapshot == nil {
		return domainProduct.ErrInvalidProductState
	}

	versionID, err := uuid.NewString()
	if err != nil {
		return err
	}
	changeID, err := uuid.NewString()
	if err != nil {
		return err
	}

	nextVersionNum := 1
	if snapshot.StoredCurrentVersion != nil {
		nextVersionNum = snapshot.StoredCurrentVersion.VersionNumber + 1
	}

	now := time.Now().UTC()

	// 1. Create ProductVersion (vN+1)
	pv := &domainProduct.ProductVersion{
		ID:             versionID,
		ProductID:      snapshot.ProductID,
		VersionNumber:  nextVersionNum,
		Fingerprint:    fingerprint,
		CanonicalName:  normalized.CanonicalName,
		Description:    normalized.Description,
		Brand:          normalized.Brand,
		OriginCountry:  normalized.OriginCountry,
		Attributes:     normalized.Attributes,
		IngestionRunID: &runID,
		CreatedAt:      now,
	}
	if err := repos.Products.CreateVersion(ctx, pv); err != nil {
		return err
	}

	// 2. Guarded CAS Update on Product
	updatedProduct := &domainProduct.Product{
		ID:                 snapshot.ProductID,
		CanonicalName:      normalized.CanonicalName,
		Description:        normalized.Description,
		Brand:              normalized.Brand,
		OriginCountry:      normalized.OriginCountry,
		CurrentVersionID:   &versionID,
		CurrentFingerprint: fingerprint,
		UpdatedAt:          now,
	}
	if err := repos.Products.GuardedUpdateVersion(ctx, updatedProduct, snapshot.CurrentVersionID); err != nil {
		return err
	}

	// 3. Update ProductSource timestamps and last_changed_at
	if err := repos.Products.UpdateSourceOnChanged(ctx, snapshot.ProductSourceID, now, record.SourceUpdatedAt, record.ReceivedAt); err != nil {
		return err
	}

	// 4. Create ProductChange (CHANGED)
	pc := &domainProduct.ProductChange{
		ID:             changeID,
		ProductID:      snapshot.ProductID,
		FromVersionID:  snapshot.CurrentVersionID,
		ToVersionID:    versionID,
		ChangeType:     domainProduct.ChangeTypeChanged,
		ChangedFields:  changedFields,
		IngestionRunID: &runID,
		RawRecordID:    &record.ID,
		DetectedAt:     now,
	}
	if err := repos.Products.CreateChange(ctx, pc); err != nil {
		return err
	}

	// 5. Create Outbox Event
	eventPayload, err := json.Marshal(map[string]any{
		"product_id":     string(snapshot.ProductID),
		"version_id":     versionID,
		"version_number": nextVersionNum,
		"fingerprint":    fingerprint,
		"change_type":    "CHANGED",
		"changed_fields": changedFields,
	})
	if err != nil {
		return fmt.Errorf("marshal outbox event payload: %w", err)
	}

	if err := repos.Outbox.CreateEvent(ctx, domainProduct.AggregateTypeProduct, string(snapshot.ProductID), domainProduct.EventTypeProductChanged, eventPayload); err != nil {
		return fmt.Errorf("create outbox event: %w", err)
	}

	return nil
}
