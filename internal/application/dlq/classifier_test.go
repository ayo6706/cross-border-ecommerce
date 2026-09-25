package dlq_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/ayo6706/cross-border-ecommerce/internal/application/dlq"
	"github.com/ayo6706/cross-border-ecommerce/internal/application/idempotency"
	"github.com/ayo6706/cross-border-ecommerce/internal/application/messaging"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/source"
)

func TestClassify(t *testing.T) {
	tests := []struct {
		name         string
		err          error
		parentCtxErr error
		wantDecision dlq.Decision
	}{
		{
			name:         "nil error returns retry",
			err:          nil,
			parentCtxErr: nil,
			wantDecision: dlq.DecisionRetry,
		},
		{
			name:         "parent context canceled returns leave pending",
			err:          errors.New("something"),
			parentCtxErr: context.Canceled,
			wantDecision: dlq.DecisionLeavePending,
		},
		{
			name:         "parent context deadline exceeded returns leave pending",
			err:          errors.New("something"),
			parentCtxErr: context.DeadlineExceeded,
			wantDecision: dlq.DecisionLeavePending,
		},
		{
			name:         "err itself is context canceled but parent context alive returns retry",
			err:          fmt.Errorf("wrap: %w", context.Canceled),
			parentCtxErr: nil,
			wantDecision: dlq.DecisionRetry,
		},
		{
			name:         "err itself is context canceled and parent context canceled returns leave pending",
			err:          fmt.Errorf("wrap: %w", context.Canceled),
			parentCtxErr: context.Canceled,
			wantDecision: dlq.DecisionLeavePending,
		},
		{
			name:         "ErrKeyInProgress returns leave pending",
			err:          fmt.Errorf("wrap: %w", idempotency.ErrKeyInProgress),
			parentCtxErr: nil,
			wantDecision: dlq.DecisionLeavePending,
		},
		{
			name:         "ErrLeaseLost returns leave pending",
			err:          fmt.Errorf("wrap: %w", idempotency.ErrLeaseLost),
			parentCtxErr: nil,
			wantDecision: dlq.DecisionLeavePending,
		},
		{
			name:         "ErrPermanent returns dead letter",
			err:          fmt.Errorf("wrap: %w", dlq.ErrPermanent),
			parentCtxErr: nil,
			wantDecision: dlq.DecisionDeadLetter,
		},
		{
			name:         "ErrHandlerPanic returns dead letter",
			err:          fmt.Errorf("wrap: %w", dlq.ErrHandlerPanic),
			parentCtxErr: nil,
			wantDecision: dlq.DecisionDeadLetter,
		},
		{
			name:         "ErrCorruptEnvelope returns dead letter",
			err:          fmt.Errorf("wrap: %w", dlq.ErrCorruptEnvelope),
			parentCtxErr: nil,
			wantDecision: dlq.DecisionDeadLetter,
		},
		{
			name:         "ErrSourceContractViolation returns dead letter",
			err:          fmt.Errorf("wrap: %w", ingestion.ErrSourceContractViolation),
			parentCtxErr: nil,
			wantDecision: dlq.DecisionDeadLetter,
		},
		{
			name:         "ErrMalformedRecord returns dead letter",
			err:          fmt.Errorf("wrap: %w", ingestion.ErrMalformedRecord),
			parentCtxErr: nil,
			wantDecision: dlq.DecisionDeadLetter,
		},
		{
			name:         "ErrInvalidSourceConfig returns dead letter",
			err:          fmt.Errorf("wrap: %w", source.ErrInvalidSourceConfig),
			parentCtxErr: nil,
			wantDecision: dlq.DecisionDeadLetter,
		},
		{
			name:         "ErrPayloadMismatch returns dead letter",
			err:          fmt.Errorf("wrap: %w", idempotency.ErrPayloadMismatch),
			parentCtxErr: nil,
			wantDecision: dlq.DecisionDeadLetter,
		},
		{
			name:         "ErrNotCompletedInTx returns dead letter",
			err:          fmt.Errorf("wrap: %w", idempotency.ErrNotCompletedInTx),
			parentCtxErr: nil,
			wantDecision: dlq.DecisionDeadLetter,
		},
		{
			name:         "ErrAdapterUnavailable returns retry",
			err:          fmt.Errorf("wrap: %w", ingestion.ErrAdapterUnavailable),
			parentCtxErr: nil,
			wantDecision: dlq.DecisionRetry,
		},
		{
			name:         "ErrRateLimitExceeded returns retry",
			err:          fmt.Errorf("wrap: %w", ingestion.ErrRateLimitExceeded),
			parentCtxErr: nil,
			wantDecision: dlq.DecisionRetry,
		},
		{
			name:         "ErrBrokerUnavailable returns retry",
			err:          fmt.Errorf("wrap: %w", messaging.ErrBrokerUnavailable),
			parentCtxErr: nil,
			wantDecision: dlq.DecisionRetry,
		},
		{
			name:         "handler timeout context.DeadlineExceeded when parent context is alive returns retry",
			err:          fmt.Errorf("handler timeout: %w", context.DeadlineExceeded),
			parentCtxErr: nil,
			wantDecision: dlq.DecisionRetry,
		},
		{
			name:         "unknown error returns retry",
			err:          errors.New("unrecognized database or network glitch"),
			parentCtxErr: nil,
			wantDecision: dlq.DecisionRetry,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, _ := dlq.Classify(tc.err, tc.parentCtxErr)
			if got != tc.wantDecision {
				t.Fatalf("expected %v, got %v", tc.wantDecision, got)
			}
		})
	}
}

func TestClassify_FailureClass(t *testing.T) {
	tests := []struct {
		err  error
		want dlq.FailureClass
	}{
		{dlq.ErrHandlerPanic, dlq.ClassHandlerPanic},
		{dlq.ErrCorruptEnvelope, dlq.ClassCorruptEnvelope},
		{idempotency.ErrPayloadMismatch, dlq.ClassPayloadMismatch},
		{idempotency.ErrNotCompletedInTx, dlq.ClassNotCompletedInTx},
		{ingestion.ErrSourceContractViolation, dlq.ClassContractViolation},
		{ingestion.ErrMalformedRecord, dlq.ClassMalformedRecord},
		{source.ErrInvalidSourceConfig, dlq.ClassInvalidConfig},
		{dlq.ErrPermanent, dlq.ClassPermanent},
		// A specific sentinel wins over the generic ErrPermanent marker in the same chain.
		{fmt.Errorf("%w: %w", dlq.ErrPermanent, ingestion.ErrMalformedRecord), dlq.ClassMalformedRecord},
		// Retried and left-pending errors carry no class.
		{errors.New("unknown"), ""},
		{idempotency.ErrKeyInProgress, ""},
	}

	for _, tc := range tests {
		_, got := dlq.Classify(tc.err, nil)
		if got != tc.want {
			t.Errorf("err %v: got %q, want %q", tc.err, got, tc.want)
		}
	}
}
