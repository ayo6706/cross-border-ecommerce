package ingestion

import "errors"

var (
	ErrRunNotFound       = errors.New("ingestion run not found")
	ErrInvalidRunState   = errors.New("invalid ingestion run state")
	ErrInvalidTransition = errors.New("invalid ingestion run status transition")
	ErrNegativeMetric    = errors.New("metric counters cannot be negative")
	ErrInvalidRunID      = errors.New("run id cannot be empty")
	ErrInvalidSourceID   = errors.New("source id cannot be empty")
	ErrInactiveSource    = errors.New("cannot start ingestion run for inactive source")

	ErrRecordNotFound           = errors.New("raw record not found")
	ErrInvalidRecordState       = errors.New("invalid raw record state")
	ErrInvalidRecordID          = errors.New("raw record id cannot be empty")
	ErrInvalidExternalProductID = errors.New("external product id cannot be empty")
	ErrEmptyPayload             = errors.New("raw payload cannot be empty")
	ErrInvalidPayloadJSON       = errors.New("raw payload must be valid json")

	ErrInvalidCheckpoint  = errors.New("invalid checkpoint format")
	ErrAdapterUnavailable = errors.New("source adapter unavailable")
	ErrRateLimitExceeded  = errors.New("source adapter rate limit exceeded")
	ErrMalformedRecord    = errors.New("malformed record in source feed")
)
