package worker_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/platform/worker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newPool(t *testing.T, size, queueSize int) *worker.Pool {
	t.Helper()
	p, err := worker.NewPool(context.Background(), size, queueSize, discardLogger())
	require.NoError(t, err)
	return p
}

func TestNewPool_InvalidConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		size      int
		queueSize int
		logger    *slog.Logger
	}{
		{name: "zero size", size: 0, queueSize: 1, logger: discardLogger()},
		{name: "negative size", size: -1, queueSize: 1, logger: discardLogger()},
		{name: "negative queue", size: 1, queueSize: -1, logger: discardLogger()},
		{name: "nil logger", size: 1, queueSize: 1, logger: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p, err := worker.NewPool(context.Background(), tt.size, tt.queueSize, tt.logger)
			assert.Nil(t, p)
			assert.ErrorIs(t, err, worker.ErrInvalidPoolConfig)
		})
	}

	t.Run("nil context", func(t *testing.T) {
		t.Parallel()
		var nilCtx context.Context
		p, err := worker.NewPool(nilCtx, 1, 1, discardLogger())
		assert.Nil(t, p)
		assert.ErrorIs(t, err, worker.ErrInvalidPoolConfig)
	})
}

func TestPool_BoundedConcurrency(t *testing.T) {
	t.Parallel()

	const (
		size  = 20
		tasks = 50_000
	)
	p := newPool(t, size, 2*size)

	var current, peak, started, done atomic.Int64
	allStarted := make(chan struct{})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := 0; i < tasks; i++ {
		err := p.Dispatch(ctx, func(context.Context) {
			n := current.Add(1)
			for {
				old := peak.Load()
				if n <= old || peak.CompareAndSwap(old, n) {
					break
				}
			}
			// The first `size` tasks wait for each other, so the pool must reach full concurrency.
			if started.Add(1) == size {
				close(allStarted)
			}
			<-allStarted
			current.Add(-1)
			done.Add(1)
		})
		require.NoError(t, err)
	}

	require.NoError(t, p.Shutdown(30*time.Second))
	assert.Equal(t, int64(size), peak.Load(), "peak concurrency must equal pool size exactly")
	assert.Equal(t, int64(tasks), done.Load(), "every dispatched task must run exactly once")
}

func TestPool_StartsInDispatchOrder(t *testing.T) {
	t.Parallel()

	p := newPool(t, 1, 100)
	var mu sync.Mutex
	var order []int

	for i := 0; i < 100; i++ {
		require.NoError(t, p.Dispatch(context.Background(), func(context.Context) {
			mu.Lock()
			order = append(order, i)
			mu.Unlock()
		}))
	}
	require.NoError(t, p.Shutdown(5*time.Second))

	require.Len(t, order, 100)
	for i, v := range order {
		assert.Equal(t, i, v)
	}
}

func TestPool_DispatchBlocksWhenFull(t *testing.T) {
	t.Parallel()

	p := newPool(t, 1, 1)
	running := make(chan struct{})
	release := make(chan struct{})

	require.NoError(t, p.Dispatch(context.Background(), func(context.Context) {
		close(running)
		<-release
	}))
	<-running
	require.NoError(t, p.Dispatch(context.Background(), func(context.Context) {})) // fills the queue

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := p.Dispatch(ctx, func(context.Context) { t.Error("task must not be accepted when the queue is full") })
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	close(release)
	require.NoError(t, p.Shutdown(5*time.Second))
}

func TestPool_DispatchNilTask(t *testing.T) {
	t.Parallel()

	p := newPool(t, 1, 1)
	assert.ErrorIs(t, p.Dispatch(context.Background(), nil), worker.ErrNilTask)
	require.NoError(t, p.Shutdown(time.Second))
}

func TestPool_PanicRecovered(t *testing.T) {
	t.Parallel()

	var logBuf bytes.Buffer
	var logMu sync.Mutex
	logger := slog.New(slog.NewTextHandler(&lockedWriter{w: &logBuf, mu: &logMu}, nil))

	const size = 2
	p, err := worker.NewPool(context.Background(), size, 10, logger)
	require.NoError(t, err)

	for i := 0; i < size; i++ {
		require.NoError(t, p.Dispatch(context.Background(), func(context.Context) {
			panic("deliberate task panic")
		}))
	}

	// Both workers must still be alive: two tasks that wait for each other can only finish
	// if they run concurrently.
	var wg sync.WaitGroup
	wg.Add(size)
	finished := make(chan struct{})
	var count atomic.Int32
	for i := 0; i < size; i++ {
		require.NoError(t, p.Dispatch(context.Background(), func(context.Context) {
			wg.Done()
			wg.Wait()
			if count.Add(1) == size {
				close(finished)
			}
		}))
	}

	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("workers did not survive task panics")
	}
	require.NoError(t, p.Shutdown(5*time.Second))

	logMu.Lock()
	logs := logBuf.String()
	logMu.Unlock()
	assert.Contains(t, logs, "deliberate task panic")
	assert.Contains(t, logs, "runtime/debug.Stack", "panic log must include the stack trace")
}

func TestPool_ShutdownDrains(t *testing.T) {
	t.Parallel()

	const tasks = 7
	p := newPool(t, 2, 5)
	gate := make(chan struct{})
	var done, cancelled atomic.Int32

	for i := 0; i < tasks; i++ {
		require.NoError(t, p.Dispatch(context.Background(), func(ctx context.Context) {
			<-gate
			if ctx.Err() != nil {
				cancelled.Add(1)
			}
			done.Add(1)
		}))
	}

	shutdownErr := make(chan error, 1)
	go func() { shutdownErr <- p.Shutdown(5 * time.Second) }()
	close(gate)

	select {
	case err := <-shutdownErr:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("shutdown did not return")
	}
	assert.Equal(t, int32(tasks), done.Load(), "queued and running tasks must all complete")
	assert.Zero(t, cancelled.Load(), "task context must stay live while draining")
}

func TestPool_ShutdownDoesNotCancelOnParentCancel(t *testing.T) {
	t.Parallel()

	parent, cancelParent := context.WithCancel(context.Background())
	p, err := worker.NewPool(parent, 1, 1, discardLogger())
	require.NoError(t, err)

	running := make(chan struct{})
	release := make(chan struct{})
	var sawCancel atomic.Bool
	require.NoError(t, p.Dispatch(context.Background(), func(ctx context.Context) {
		close(running)
		<-release
		sawCancel.Store(ctx.Err() != nil)
	}))
	<-running
	cancelParent() // the shutdown signal must not cancel in-flight work; only the drain timeout does
	close(release)

	require.NoError(t, p.Shutdown(5*time.Second))
	assert.False(t, sawCancel.Load())
}

func TestPool_DrainTimeoutCancelsTasks(t *testing.T) {
	t.Parallel()

	p := newPool(t, 1, 1)
	running := make(chan struct{})
	observed := make(chan error, 1)

	require.NoError(t, p.Dispatch(context.Background(), func(ctx context.Context) {
		close(running)
		<-ctx.Done()
		observed <- ctx.Err()
	}))
	<-running

	err := p.Shutdown(50 * time.Millisecond)
	assert.ErrorIs(t, err, worker.ErrDrainTimeout)

	select {
	case taskErr := <-observed:
		assert.ErrorIs(t, taskErr, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("task context was not cancelled after the drain timeout")
	}
}

func TestPool_ShutdownDrainTimeoutDoesNotBlockOnUnresponsiveTask(t *testing.T) {
	t.Parallel()

	p := newPool(t, 1, 1)
	running := make(chan struct{})

	require.NoError(t, p.Dispatch(context.Background(), func(ctx context.Context) {
		close(running)
		time.Sleep(2 * time.Second)
	}))
	<-running

	start := time.Now()
	err := p.Shutdown(50 * time.Millisecond)
	elapsed := time.Since(start)

	assert.ErrorIs(t, err, worker.ErrDrainTimeout)
	assert.Less(t, elapsed, 500*time.Millisecond, "Shutdown must return promptly after drain timeout even if task ignores cancellation")
}

func TestPool_DispatchAfterShutdown(t *testing.T) {
	t.Parallel()

	p := newPool(t, 1, 1)
	require.NoError(t, p.Shutdown(time.Second))
	require.NoError(t, p.Shutdown(time.Second), "a second Shutdown must be safe")

	err := p.Dispatch(context.Background(), func(context.Context) { t.Error("task must not run after shutdown") })
	assert.ErrorIs(t, err, worker.ErrPoolClosed)
}

func TestPool_ConcurrentDispatchAndShutdown(t *testing.T) {
	t.Parallel()

	for round := 0; round < 50; round++ {
		p := newPool(t, 4, 4)
		var accepted, ran atomic.Int64
		var wg sync.WaitGroup

		for d := 0; d < 8; d++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					err := p.Dispatch(context.Background(), func(context.Context) { ran.Add(1) })
					if errors.Is(err, worker.ErrPoolClosed) {
						return
					}
					if !assert.NoError(t, err) {
						return
					}
					accepted.Add(1)
				}
			}()
		}

		require.Eventually(t, func() bool { return accepted.Load() > 0 }, 5*time.Second, time.Millisecond)
		require.NoError(t, p.Shutdown(5*time.Second))
		wg.Wait()
		assert.Equal(t, accepted.Load(), ran.Load(), "every accepted task must run; none may be dropped")
	}
}

func TestPool_NoGoroutineLeak(t *testing.T) {
	baseline := runtime.NumGoroutine()

	p := newPool(t, 50, 50)
	for i := 0; i < 1000; i++ {
		require.NoError(t, p.Dispatch(context.Background(), func(context.Context) {}))
	}
	require.NoError(t, p.Shutdown(5*time.Second))

	deadline := time.Now().Add(5 * time.Second)
	leaked := true
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= baseline {
			leaked = false
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	assert.False(t, leaked, "worker goroutines must exit after Shutdown")
}

func BenchmarkPool_Dispatch(b *testing.B) {
	p, err := worker.NewPool(context.Background(), 8, 64, discardLogger())
	require.NoError(b, err)
	task := func(context.Context) {}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := p.Dispatch(ctx, task); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	require.NoError(b, p.Shutdown(time.Minute))
}

type lockedWriter struct {
	w  io.Writer
	mu *sync.Mutex
}

func (l *lockedWriter) Write(b []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(b)
}
