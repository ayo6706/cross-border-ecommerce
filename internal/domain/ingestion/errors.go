package ingestion

import "errors"

var (
	ErrRunNotFound         = errors.New("ingestion run not found")
	ErrInvalidRunState     = errors.New("invalid ingestion run state")
	ErrInvalidTransition    = errors.New("invalid ingestion run status transition")
	ErrNegativeMetric      = errors.New("metric counters cannot be negative")
	ErrInvalidRunID        = errors.New("run id cannot be empty")
	ErrInvalidSourceID     = errors.New("source id cannot be empty")
	ErrInactiveSource      = errors.New("cannot start ingestion run for inactive source")
)
