package ingestion

import (
	"errors"
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
	ErrLeaseHeld             = errors.New("active run processing lease held by another worker")
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
