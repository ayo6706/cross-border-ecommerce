package ingestion

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/source"
)

type Service struct {
	runRepo    ingestion.Repository
	sourceRepo source.Repository
}

func NewService(runRepo ingestion.Repository, sourceRepo source.Repository) (*Service, error) {
	if runRepo == nil {
		return nil, errors.New("ingestion run repository is required")
	}
	if sourceRepo == nil {
		return nil, errors.New("source repository is required")
	}
	return &Service{
		runRepo:    runRepo,
		sourceRepo: sourceRepo,
	}, nil
}

func (s *Service) StartRun(ctx context.Context, sourceID source.ID, initialCheckpoint string) (*ingestion.IngestionRun, error) {
	if strings.TrimSpace(string(sourceID)) == "" {
		return nil, ingestion.ErrInvalidSourceID
	}

	src, err := s.sourceRepo.FindByID(ctx, sourceID)
	if err != nil {
		return nil, fmt.Errorf("verify source for run: %w", err)
	}
	if !src.Enabled {
		return nil, ingestion.ErrInactiveSource
	}

	latestRun, err := s.runRepo.FindLatestRunBySource(ctx, sourceID)
	if err != nil && !errors.Is(err, ingestion.ErrRunNotFound) {
		return nil, fmt.Errorf("check active run: %w", err)
	}
	if err == nil && latestRun != nil && (latestRun.Status == ingestion.StatusRunning || latestRun.Status == ingestion.StatusPending) {
		return nil, ingestion.ErrRunAlreadyActive
	}

	now := time.Now().UTC()
	run, err := ingestion.NewRun("", sourceID, initialCheckpoint)
	if err != nil {
		return nil, fmt.Errorf("initialize run entity: %w", err)
	}

	if err := run.Start(now); err != nil {
		return nil, fmt.Errorf("start run transition: %w", err)
	}

	if err := s.runRepo.CreateRun(ctx, run); err != nil {
		return nil, fmt.Errorf("persist started run: %w", err)
	}

	return run, nil
}

func (s *Service) RecordBatch(ctx context.Context, runID string, metrics ingestion.BatchMetrics, checkpoint string) error {
	if strings.TrimSpace(runID) == "" {
		return ingestion.ErrInvalidRunID
	}

	if metrics.Seen < 0 || metrics.New < 0 || metrics.Changed < 0 || metrics.Unchanged < 0 || metrics.Failed < 0 {
		return ingestion.ErrNegativeMetric
	}

	now := time.Now().UTC()
	if err := s.runRepo.UpdateProgress(ctx, runID, metrics, strings.TrimSpace(checkpoint), now); err != nil {
		return fmt.Errorf("update run progress: %w", err)
	}

	return nil
}

func (s *Service) CompleteRun(ctx context.Context, runID string, checkpoint string) error {
	return s.transitionRun(ctx, runID, "complete", func(run *ingestion.IngestionRun, now time.Time) error {
		return run.Complete(checkpoint, now)
	})
}

func (s *Service) FailRun(ctx context.Context, runID string, errorSummary string) error {
	return s.transitionRun(ctx, runID, "fail", func(run *ingestion.IngestionRun, now time.Time) error {
		return run.Fail(errorSummary, now)
	})
}

func (s *Service) CancelRun(ctx context.Context, runID string, reason string) error {
	return s.transitionRun(ctx, runID, "cancel", func(run *ingestion.IngestionRun, now time.Time) error {
		return run.Cancel(reason, now)
	})
}

func (s *Service) transitionRun(
	ctx context.Context,
	runID string,
	op string,
	apply func(run *ingestion.IngestionRun, now time.Time) error,
) error {
	run, err := s.runRepo.FindRunByID(ctx, runID)
	if err != nil {
		return fmt.Errorf("find run to %s: %w", op, err)
	}

	from := run.Status
	if err := apply(run, time.Now().UTC()); err != nil {
		return err
	}

	if err := s.runRepo.UpdateStatus(ctx, run, from); err != nil {
		return fmt.Errorf("%s run: %w", op, err)
	}
	return nil
}

func (s *Service) ResumeRun(ctx context.Context, previousRunID string) (*ingestion.IngestionRun, error) {
	prevRun, err := s.runRepo.FindRunByID(ctx, previousRunID)
	if err != nil {
		return nil, fmt.Errorf("find previous run for resume: %w", err)
	}

	if prevRun.Status != ingestion.StatusPartial && prevRun.Status != ingestion.StatusFailed {
		return nil, fmt.Errorf("%w: only PARTIAL or FAILED runs can be resumed (current status: %s)", ingestion.ErrInvalidTransition, prevRun.Status)
	}

	return s.StartRun(ctx, prevRun.SourceID, prevRun.Checkpoint)
}

func (s *Service) GetRun(ctx context.Context, runID string) (*ingestion.IngestionRun, error) {
	if strings.TrimSpace(runID) == "" {
		return nil, ingestion.ErrInvalidRunID
	}

	run, err := s.runRepo.FindRunByID(ctx, runID)
	if err != nil {
		return nil, fmt.Errorf("get run: %w", err)
	}

	return run, nil
}

func (s *Service) GetLatestRun(ctx context.Context, sourceID source.ID) (*ingestion.IngestionRun, error) {
	if strings.TrimSpace(string(sourceID)) == "" {
		return nil, ingestion.ErrInvalidSourceID
	}

	run, err := s.runRepo.FindLatestRunBySource(ctx, sourceID)
	if err != nil {
		return nil, fmt.Errorf("get latest run: %w", err)
	}

	return run, nil
}

func (s *Service) ListRunsBySource(ctx context.Context, sourceID source.ID, limit int) ([]*ingestion.IngestionRun, error) {
	if strings.TrimSpace(string(sourceID)) == "" {
		return nil, ingestion.ErrInvalidSourceID
	}

	runs, err := s.runRepo.ListRunsBySource(ctx, sourceID, limit)
	if err != nil {
		return nil, fmt.Errorf("list runs by source: %w", err)
	}

	return runs, nil
}
