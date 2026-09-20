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
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

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
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

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
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	run, err := s.runRepo.FindRunByID(ctx, runID)
	if err != nil {
		return fmt.Errorf("find run for completion: %w", err)
	}

	if run.Status != ingestion.StatusRunning {
		return ingestion.ErrInvalidTransition
	}

	now := time.Now().UTC()
	status := ingestion.StatusCompleted
	if run.RecordsFailed > 0 {
		status = ingestion.StatusPartial
	}

	if err := s.runRepo.UpdateStatus(ctx, runID, status, "", strings.TrimSpace(checkpoint), now, now); err != nil {
		return fmt.Errorf("complete run status: %w", err)
	}

	return nil
}

func (s *Service) FailRun(ctx context.Context, runID string, errorSummary string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	run, err := s.runRepo.FindRunByID(ctx, runID)
	if err != nil {
		return fmt.Errorf("find run for failure: %w", err)
	}

	if run.Status == ingestion.StatusCompleted || run.Status == ingestion.StatusCancelled {
		return ingestion.ErrInvalidTransition
	}

	now := time.Now().UTC()
	if err := s.runRepo.UpdateStatus(ctx, runID, ingestion.StatusFailed, strings.TrimSpace(errorSummary), "", now, now); err != nil {
		return fmt.Errorf("fail run status: %w", err)
	}

	return nil
}

func (s *Service) CancelRun(ctx context.Context, runID string, reason string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	run, err := s.runRepo.FindRunByID(ctx, runID)
	if err != nil {
		return fmt.Errorf("find run for cancellation: %w", err)
	}

	if run.Status == ingestion.StatusCompleted || run.Status == ingestion.StatusFailed {
		return ingestion.ErrInvalidTransition
	}

	now := time.Now().UTC()
	if err := s.runRepo.UpdateStatus(ctx, runID, ingestion.StatusCancelled, strings.TrimSpace(reason), "", now, now); err != nil {
		return fmt.Errorf("cancel run status: %w", err)
	}

	return nil
}

func (s *Service) ResumeRun(ctx context.Context, previousRunID string) (*ingestion.IngestionRun, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	prevRun, err := s.runRepo.FindRunByID(ctx, previousRunID)
	if err != nil {
		return nil, fmt.Errorf("find previous run for resume: %w", err)
	}

	return s.StartRun(ctx, prevRun.SourceID, prevRun.Checkpoint)
}

func (s *Service) GetRun(ctx context.Context, runID string) (*ingestion.IngestionRun, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

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
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

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
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if strings.TrimSpace(string(sourceID)) == "" {
		return nil, ingestion.ErrInvalidSourceID
	}

	runs, err := s.runRepo.ListRunsBySource(ctx, sourceID, limit)
	if err != nil {
		return nil, fmt.Errorf("list runs by source: %w", err)
	}

	return runs, nil
}
