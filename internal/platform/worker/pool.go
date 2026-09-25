package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"
)

var (
	ErrInvalidPoolConfig = errors.New("invalid worker pool configuration")
	ErrNilTask           = errors.New("task cannot be nil")
	ErrPoolClosed        = errors.New("worker pool is closed")
	ErrDrainTimeout      = errors.New("worker pool drain timeout exceeded")
)

// Task represents a unit of work to be executed by the worker pool.
type Task func(context.Context)

// Pool represents a fixed-size worker pool with a bounded task queue.
type Pool struct {
	tasks        chan Task
	closing      chan struct{}
	taskCtx      context.Context
	cancelTasks  context.CancelFunc
	logger       *slog.Logger
	mu           sync.RWMutex
	closed       bool
	dispatcherWg sync.WaitGroup
	workerWg     sync.WaitGroup
	shutdownOnce sync.Once
	shutdownErr  error
}

// NewPool creates and starts a new worker pool with size long-lived worker goroutines.
// The pool runs tasks on a context derived from context.WithoutCancel(ctx) so that
// shutdown of the caller does not prematurely cancel running tasks before the drain timeout.
func NewPool(ctx context.Context, size, queueSize int, logger *slog.Logger) (*Pool, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context cannot be nil", ErrInvalidPoolConfig)
	}
	if size <= 0 {
		return nil, fmt.Errorf("%w: pool size must be greater than 0, got %d", ErrInvalidPoolConfig, size)
	}
	if queueSize < 0 {
		return nil, fmt.Errorf("%w: queue size cannot be negative, got %d", ErrInvalidPoolConfig, queueSize)
	}
	if logger == nil {
		return nil, fmt.Errorf("%w: logger cannot be nil", ErrInvalidPoolConfig)
	}

	taskCtx, cancelTasks := context.WithCancel(context.WithoutCancel(ctx))

	p := &Pool{
		tasks:       make(chan Task, queueSize),
		closing:     make(chan struct{}),
		taskCtx:     taskCtx,
		cancelTasks: cancelTasks,
		logger:      logger,
	}

	p.workerWg.Add(size)
	for i := 0; i < size; i++ {
		go p.worker()
	}

	return p, nil
}

func (p *Pool) worker() {
	defer p.workerWg.Done()
	for task := range p.tasks {
		p.runTask(task)
	}
}

func (p *Pool) runTask(task Task) {
	defer func() {
		if r := recover(); r != nil {
			stack := debug.Stack()
			p.logger.Error("worker recovered from task panic",
				slog.Any("panic", r),
				slog.String("stack", string(stack)),
			)
		}
	}()
	task(p.taskCtx)
}

// Dispatch submits a task to the pool. It blocks if the queue is full, providing backpressure.
// Returns ctx.Err() if ctx is cancelled while waiting, ErrNilTask if task is nil,
// or ErrPoolClosed if the pool is closed or shutting down.
func (p *Pool) Dispatch(ctx context.Context, task Task) error {
	if task == nil {
		return ErrNilTask
	}

	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return ErrPoolClosed
	}
	p.dispatcherWg.Add(1)
	p.mu.RUnlock()
	defer p.dispatcherWg.Done()

	select {
	case <-p.closing:
		return ErrPoolClosed
	case <-ctx.Done():
		return ctx.Err()
	case p.tasks <- task:
		return nil
	}
}

// Shutdown stops accepting new tasks, drains queued and in-flight work up to drainTimeout,
// and cancels remaining in-flight tasks if drainTimeout expires.
// Tasks submitted to the pool must respect context cancellation to ensure timely termination.
// If the drain timeout expires, Shutdown returns ErrDrainTimeout without blocking on unresponsive tasks.
func (p *Pool) Shutdown(drainTimeout time.Duration) error {
	p.shutdownOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		close(p.closing)
		p.mu.Unlock()

		// Wait for all in-flight Dispatch calls to complete their select
		p.dispatcherWg.Wait()

		// Safe to close tasks channel now that no dispatcher can send
		close(p.tasks)

		done := make(chan struct{})
		go func() {
			p.workerWg.Wait()
			close(done)
		}()

		timer := time.NewTimer(drainTimeout)
		defer timer.Stop()
		defer p.cancelTasks()

		select {
		case <-done:
			p.shutdownErr = nil
		case <-timer.C:
			p.shutdownErr = ErrDrainTimeout
		}
	})

	return p.shutdownErr
}
