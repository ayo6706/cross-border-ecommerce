package ingestion

import (
	"fmt"
	"strings"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/product"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/source"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/uuid"
)

type RunStatus string

const (
	StatusPending   RunStatus = "PENDING"
	StatusRunning   RunStatus = "RUNNING"
	StatusCompleted RunStatus = "COMPLETED"
	StatusPartial   RunStatus = "PARTIAL"
	StatusFailed    RunStatus = "FAILED"
	StatusCancelled RunStatus = "CANCELLED"
)

// BatchMetrics is declared once, in the product domain, which owns the outcome categories.
type BatchMetrics = product.BatchMetrics

type IngestionRun struct {
	ID         string
	SourceID   source.ID
	Status     RunStatus
	Checkpoint string
	BatchMetrics
	ErrorSummary string
	StartedAt    *time.Time
	CompletedAt  *time.Time
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

func NewRun(id string, sourceID source.ID, initialCheckpoint string) (*IngestionRun, error) {
	runID := strings.TrimSpace(id)
	if runID == "" {
		generatedID, err := uuid.NewString()
		if err != nil {
			return nil, fmt.Errorf("generate run id: %w", err)
		}
		runID = generatedID
	}

	now := time.Now().UTC()
	run := &IngestionRun{
		ID:         runID,
		SourceID:   sourceID,
		Status:     StatusPending,
		Checkpoint: strings.TrimSpace(initialCheckpoint),
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	if err := run.Validate(); err != nil {
		return nil, err
	}

	return run, nil
}

func (r *IngestionRun) Validate() error {
	if r == nil {
		return ErrInvalidRunState
	}
	if strings.TrimSpace(r.ID) == "" {
		return ErrInvalidRunID
	}
	if strings.TrimSpace(string(r.SourceID)) == "" {
		return ErrInvalidSourceID
	}
	switch r.Status {
	case StatusPending, StatusRunning, StatusCompleted, StatusPartial, StatusFailed, StatusCancelled:
	default:
		return ErrInvalidRunState
	}
	return nil
}

func (r *IngestionRun) Start(now time.Time) error {
	if r.Status != StatusPending {
		return ErrInvalidTransition
	}
	utcNow := now.UTC()
	r.Status = StatusRunning
	r.StartedAt = &utcNow
	r.UpdatedAt = utcNow
	return nil
}

func (r *IngestionRun) RecordBatch(metrics BatchMetrics, nextCheckpoint string, now time.Time) error {
	if r.Status != StatusRunning {
		return ErrInvalidRunState
	}
	if err := metrics.Validate(); err != nil {
		return err
	}

	r.Seen += metrics.Seen
	r.New += metrics.New
	r.Changed += metrics.Changed
	r.Unchanged += metrics.Unchanged
	r.Failed += metrics.Failed

	if trimmed := strings.TrimSpace(nextCheckpoint); trimmed != "" {
		r.Checkpoint = trimmed
	}

	r.UpdatedAt = now.UTC()
	return nil
}

func (r *IngestionRun) IsTerminal() bool {
	if r == nil {
		return false
	}
	switch r.Status {
	case StatusCompleted, StatusPartial, StatusFailed, StatusCancelled:
		return true
	default:
		return false
	}
}

func (r *IngestionRun) Complete(finalCheckpoint string, now time.Time) error {
	if r.Status != StatusRunning {
		return ErrInvalidTransition
	}

	if r.Failed > 0 {
		r.Status = StatusPartial
	} else {
		r.Status = StatusCompleted
	}

	if trimmed := strings.TrimSpace(finalCheckpoint); trimmed != "" {
		r.Checkpoint = trimmed
	}

	utcNow := now.UTC()
	r.CompletedAt = &utcNow
	r.UpdatedAt = utcNow
	return nil
}

func (r *IngestionRun) Fail(summary string, now time.Time) error {
	if r.IsTerminal() {
		return ErrInvalidTransition
	}

	r.Status = StatusFailed
	r.ErrorSummary = strings.TrimSpace(summary)
	utcNow := now.UTC()
	r.CompletedAt = &utcNow
	r.UpdatedAt = utcNow
	return nil
}

func (r *IngestionRun) Cancel(reason string, now time.Time) error {
	if r.IsTerminal() {
		return ErrInvalidTransition
	}

	r.Status = StatusCancelled
	r.ErrorSummary = strings.TrimSpace(reason)
	utcNow := now.UTC()
	r.CompletedAt = &utcNow
	r.UpdatedAt = utcNow
	return nil
}
