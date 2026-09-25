package dlq

import (
	"errors"

	"github.com/ayo6706/cross-border-ecommerce/internal/application/idempotency"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/source"
)

type Decision int

const (
	DecisionRetry Decision = iota
	DecisionDeadLetter
	DecisionLeavePending
)

type FailureClass string

const (
	ClassHandlerPanic      FailureClass = "HANDLER_PANIC"
	ClassCorruptEnvelope   FailureClass = "CORRUPT_ENVELOPE"
	ClassPayloadMismatch   FailureClass = "PAYLOAD_MISMATCH"
	ClassNotCompletedInTx  FailureClass = "NOT_COMPLETED_IN_TX"
	ClassContractViolation FailureClass = "CONTRACT_VIOLATION"
	ClassMalformedRecord   FailureClass = "MALFORMED_RECORD"
	ClassInvalidConfig     FailureClass = "INVALID_CONFIG"
	ClassPermanent         FailureClass = "PERMANENT"
	ClassRetriesExhausted  FailureClass = "TRANSIENT_EXHAUSTED"
)

var (
	ErrPermanent       = errors.New("permanent failure")
	ErrHandlerPanic    = errors.New("handler panic")
	ErrCorruptEnvelope = errors.New("corrupt stream envelope")
	ErrDLQNotFound     = errors.New("dlq message not found")
	ErrAlreadyReplayed = errors.New("dlq message already replayed")
	ErrNotReplayable   = errors.New("dlq message cannot be replayed")
)

// ErrPermanent must remain last so specific sentinels in a wrapped error chain match first.
var permanentFailures = []struct {
	err   error
	class FailureClass
}{
	{ErrHandlerPanic, ClassHandlerPanic},
	{ErrCorruptEnvelope, ClassCorruptEnvelope},
	{idempotency.ErrPayloadMismatch, ClassPayloadMismatch},
	{idempotency.ErrNotCompletedInTx, ClassNotCompletedInTx},
	{ingestion.ErrSourceContractViolation, ClassContractViolation},
	{ingestion.ErrMalformedRecord, ClassMalformedRecord},
	{source.ErrInvalidSourceConfig, ClassInvalidConfig},
	{ErrPermanent, ClassPermanent},
}

func Classify(err, parentCtxErr error) (Decision, FailureClass) {
	if parentCtxErr != nil {
		return DecisionLeavePending, ""
	}
	if errors.Is(err, idempotency.ErrKeyInProgress) || errors.Is(err, idempotency.ErrLeaseLost) {
		return DecisionLeavePending, ""
	}
	for _, p := range permanentFailures {
		if errors.Is(err, p.err) {
			return DecisionDeadLetter, p.class
		}
	}
	return DecisionRetry, ""
}
