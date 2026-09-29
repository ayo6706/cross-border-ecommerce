// Package cleanup bounds the writes that must still happen after the caller's context is
// cancelled: releasing a lease or claim, acknowledging a message, recording a run's final state.
package cleanup

import (
	"context"
	"time"
)

const Timeout = 5 * time.Second

// Context keeps ctx's values but not its cancellation, and expires after Timeout.
func Context(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), Timeout)
}
