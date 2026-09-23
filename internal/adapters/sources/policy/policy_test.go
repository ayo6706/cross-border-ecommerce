package policy_test

import (
	"errors"
	"testing"

	"github.com/ayo6706/cross-border-ecommerce/internal/adapters/sources/policy"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
)

func TestErrorTracker(t *testing.T) {
	t.Run("FailFastTerminatesImmediately", func(t *testing.T) {
		tracker := policy.NewErrorTracker(policy.PolicyFailFast, 0.1, nil)
		errTest := errors.New("malformed row")
		err := tracker.RecordError(1, []byte("bad-row"), errTest)
		if !errors.Is(err, errTest) {
			t.Fatalf("expected error %v, got %v", errTest, err)
		}
	})

	t.Run("SkipMalformedAllowsErrorsUnderThreshold", func(t *testing.T) {
		var callbackInvoked bool
		tracker := policy.NewErrorTracker(policy.PolicySkipMalformed, 0.20, func(row int, raw []byte, err error) {
			callbackInvoked = true
		})

		// 9 successes, 1 error (10% < 20%)
		for i := 0; i < 9; i++ {
			tracker.RecordSuccess()
		}
		err := tracker.RecordError(10, []byte("bad"), errors.New("syntax error"))
		if err != nil {
			t.Fatalf("expected error to be skipped under threshold, got %v", err)
		}
		if !callbackInvoked {
			t.Error("expected error callback to be called")
		}
	})

	t.Run("ExceedingThresholdReturnsErrQuarantineThreshold", func(t *testing.T) {
		// 10% threshold
		tracker := policy.NewErrorTracker(policy.PolicySkipMalformed, 0.10, nil)

		// 6 successes, 5 errors out of 11 (45% > 10%)
		for i := 0; i < 6; i++ {
			tracker.RecordSuccess()
		}
		var finalErr error
		for i := 7; i <= 11; i++ {
			finalErr = tracker.RecordError(i, []byte("bad"), errors.New("syntax error"))
		}
		if finalErr == nil {
			t.Fatal("expected error on threshold breach, got nil")
		}
		if !errors.Is(finalErr, ingestion.ErrQuarantineThreshold) {
			t.Errorf("expected ErrQuarantineThreshold, got %v", finalErr)
		}
	})
}
