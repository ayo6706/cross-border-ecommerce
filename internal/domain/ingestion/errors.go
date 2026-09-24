package ingestion

import (
	"errors"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/source"
)

var (
	ErrRunNotFound               = errors.New("ingestion run not found")
	ErrInvalidRunState           = errors.New("invalid ingestion run state")
	ErrInvalidTransition         = errors.New("invalid ingestion run state transition")
	ErrInvalidRunID              = errors.New("run id cannot be empty")
	ErrInvalidSourceID           = source.ErrInvalidSourceID
	ErrNegativeMetric            = errors.New("metrics cannot be negative")
	ErrRecordNotFound            = errors.New("raw record not found")
	ErrInvalidRecordID           = errors.New("raw record id cannot be empty")
	ErrInvalidExternalProductID  = errors.New("external product id cannot be empty")
	ErrEmptyPayload              = errors.New("payload cannot be empty")
	ErrInvalidPayloadJSON        = errors.New("payload must be valid json")
	ErrInvalidRecordState        = errors.New("invalid raw record state")
	ErrAdapterUnavailable        = errors.New("source adapter unavailable")
	ErrAuthenticationFailed      = errors.New("authentication failed")
	ErrRateLimitExceeded         = errors.New("source rate limit exceeded")
	ErrSourceContractViolation   = errors.New("source contract violation")
	ErrMalformedRecord           = errors.New("malformed source record")
	ErrErrorBudgetExceeded       = errors.New("row error budget exceeded")
	ErrInvalidCheckpoint         = errors.New("invalid checkpoint format")
	ErrPathNotFound              = errors.New("configured extraction path not found in payload")
	ErrIdentityNotFound          = errors.New("unable to resolve external identity from record")
	ErrUnsupportedIdentityFormat = errors.New("unsupported identity value type")
	ErrRecordNotObject           = errors.New("record in items array is not a json object")
	ErrInactiveSource            = errors.New("source is disabled or inactive")
	ErrRunAlreadyActive          = errors.New("source already has an active ingestion run")
	ErrPathWrongType             = errors.New("extraction path points to unexpected data type")
)
