package ingestion

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

type ProcessingStatus string

const (
	ProcessingPending   ProcessingStatus = "PENDING"
	ProcessingRunning   ProcessingStatus = "RUNNING"
	ProcessingCompleted ProcessingStatus = "COMPLETED"
	ProcessingFailed    ProcessingStatus = "FAILED"
)

var (
	ErrLeaseLost             = errors.New("run processing lease lost")
	ErrRunProcessingNotFound = errors.New("run processing state not found")
)

type RunProcessing struct {
	RunID             string
	Status            ProcessingStatus
	ClaimToken        *string
	LeaseExpiresAt    *time.Time
	CursorRawRecordID *string
	RecordsSeen       int
	RecordsNew        int
	RecordsChanged    int
	RecordsUnchanged  int
	RecordsFailed     int
	ErrorSummary      string
	StartedAt         *time.Time
	CompletedAt       *time.Time
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

func NewRunProcessing(runID string, now time.Time) (*RunProcessing, error) {
	trimmedRunID := strings.TrimSpace(runID)
	if trimmedRunID == "" {
		return nil, errors.New("run id cannot be empty")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}

	return &RunProcessing{
		RunID:     trimmedRunID,
		Status:    ProcessingPending,
		CreatedAt: now.UTC(),
		UpdatedAt: now.UTC(),
	}, nil
}

func (rp *RunProcessing) Claim(token string, leaseDuration time.Duration, now time.Time) error {
	if rp == nil {
		return errors.New("run processing cannot be nil")
	}
	trimmedToken := strings.TrimSpace(token)
	if trimmedToken == "" {
		return errors.New("claim token cannot be empty")
	}
	if leaseDuration <= 0 {
		return errors.New("lease duration must be positive")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}

	// Can claim if PENDING, COMPLETED, FAILED, or RUNNING with expired lease
	if rp.Status == ProcessingRunning && rp.LeaseExpiresAt != nil && !rp.LeaseExpiresAt.Before(now) {
		return fmt.Errorf("cannot claim run processing: active lease held until %s", rp.LeaseExpiresAt.Format(time.RFC3339))
	}

	expiresAt := now.Add(leaseDuration).UTC()
	rp.Status = ProcessingRunning
	rp.ClaimToken = &trimmedToken
	rp.LeaseExpiresAt = &expiresAt
	if rp.StartedAt == nil {
		started := now.UTC()
		rp.StartedAt = &started
	}
	rp.UpdatedAt = now.UTC()
	return nil
}

func (rp *RunProcessing) RecordProgress(
	token string,
	seen, newRecs, changed, unchanged, failed int,
	cursorID *string,
	leaseDuration time.Duration,
	now time.Time,
) error {
	if rp == nil {
		return errors.New("run processing cannot be nil")
	}
	if rp.Status != ProcessingRunning {
		return fmt.Errorf("cannot record progress: run processing status is %s", rp.Status)
	}
	if rp.ClaimToken == nil || *rp.ClaimToken != strings.TrimSpace(token) {
		return errors.New("cannot record progress: claim token mismatch or lease lost")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}

	rp.RecordsSeen += seen
	rp.RecordsNew += newRecs
	rp.RecordsChanged += changed
	rp.RecordsUnchanged += unchanged
	rp.RecordsFailed += failed

	if cursorID != nil && strings.TrimSpace(*cursorID) != "" {
		v := strings.TrimSpace(*cursorID)
		rp.CursorRawRecordID = &v
	}

	if leaseDuration > 0 {
		expiresAt := now.Add(leaseDuration).UTC()
		rp.LeaseExpiresAt = &expiresAt
	}
	rp.UpdatedAt = now.UTC()
	return nil
}

func (rp *RunProcessing) Release(token string, now time.Time) error {
	if rp == nil {
		return errors.New("run processing cannot be nil")
	}
	if rp.ClaimToken != nil && *rp.ClaimToken != strings.TrimSpace(token) {
		return errors.New("cannot release claim: claim token mismatch")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}

	rp.Status = ProcessingPending
	rp.ClaimToken = nil
	rp.LeaseExpiresAt = nil
	rp.UpdatedAt = now.UTC()
	return nil
}

func (rp *RunProcessing) Complete(token string, now time.Time) error {
	if rp == nil {
		return errors.New("run processing cannot be nil")
	}
	if rp.Status != ProcessingRunning {
		return fmt.Errorf("cannot complete run processing: status is %s", rp.Status)
	}
	if rp.ClaimToken != nil && *rp.ClaimToken != strings.TrimSpace(token) {
		return errors.New("cannot complete run processing: claim token mismatch")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}

	completed := now.UTC()
	rp.Status = ProcessingCompleted
	rp.ClaimToken = nil
	rp.LeaseExpiresAt = nil
	rp.CompletedAt = &completed
	rp.UpdatedAt = now.UTC()
	return nil
}

func (rp *RunProcessing) Fail(token, errSummary string, now time.Time) error {
	if rp == nil {
		return errors.New("run processing cannot be nil")
	}
	if rp.ClaimToken != nil && strings.TrimSpace(token) != "" && *rp.ClaimToken != strings.TrimSpace(token) {
		return errors.New("cannot fail run processing: claim token mismatch")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}

	completed := now.UTC()
	rp.Status = ProcessingFailed
	rp.ErrorSummary = strings.TrimSpace(errSummary)
	rp.ClaimToken = nil
	rp.LeaseExpiresAt = nil
	rp.CompletedAt = &completed
	rp.UpdatedAt = now.UTC()
	return nil
}

func (rp *RunProcessing) Restart(now time.Time) error {
	if rp == nil {
		return errors.New("run processing cannot be nil")
	}
	if rp.Status == ProcessingRunning && rp.LeaseExpiresAt != nil && !rp.LeaseExpiresAt.Before(now) {
		return errors.New("cannot restart run processing: active lease held by running worker")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}

	rp.Status = ProcessingPending
	rp.ClaimToken = nil
	rp.LeaseExpiresAt = nil
	rp.CursorRawRecordID = nil
	rp.RecordsSeen = 0
	rp.RecordsNew = 0
	rp.RecordsChanged = 0
	rp.RecordsUnchanged = 0
	rp.RecordsFailed = 0
	rp.ErrorSummary = ""
	rp.StartedAt = nil
	rp.CompletedAt = nil
	rp.UpdatedAt = now.UTC()
	return nil
}
