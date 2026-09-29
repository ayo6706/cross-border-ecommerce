package cleanup_test

import (
	"context"
	"testing"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/platform/cleanup"
)

type key struct{}

func TestContext_OutlivesCancelledParentWithinTimeout(t *testing.T) {
	parent, cancel := context.WithCancel(context.WithValue(context.Background(), key{}, "v"))
	cancel()

	ctx, stop := cleanup.Context(parent)
	defer stop()

	if err := ctx.Err(); err != nil {
		t.Fatalf("cleanup context inherited the parent's cancellation: %v", err)
	}
	if ctx.Value(key{}) != "v" {
		t.Fatal("cleanup context lost the parent's values")
	}
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > cleanup.Timeout {
		t.Fatalf("deadline = %v (set: %v); want at most %v from now", deadline, ok, cleanup.Timeout)
	}
}
