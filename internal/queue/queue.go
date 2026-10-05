// Package queue runs tasks on a pool of workers and tracks their status.
package queue

import (
	"context"
	"errors"
	"gqin/task-queue/internal/task"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"
)

// Errors returned by Submit.
var (
	ErrQueueFull   = errors.New("queue is full")
	ErrQueueClosed = errors.New("queue is closed")
)

// Queue is a buffered task queue processed by a fixed pool of workers.
type Queue struct {
	tasks    chan string
	store    *store
	registry *task.Registry
	wg       sync.WaitGroup
	cancel   context.CancelFunc
	closedMu sync.RWMutex
	closed   bool
	cfg      Config
	logger   *slog.Logger
}

// Config configures a Queue. Zero fields use their defaults.
type Config struct {
	// WorkerCount is how many tasks run at once. Defaults to 1.
	WorkerCount int
	// BufferSize is how many tasks can wait to run. Defaults to 100.
	BufferSize int
	// RetryBaseDelay is the delay before the first retry; it doubles on each
	// later retry. Each wait is randomized between 0 and the current delay.
	// Defaults to 100ms.
	RetryBaseDelay time.Duration
	// RetryMaxDelay caps the wait between retries. Defaults to 10s, and is
	// raised to RetryBaseDelay if lower.
	RetryMaxDelay time.Duration
	// Logger receives the queue's logs. Defaults to slog.Default().
	Logger *slog.Logger
}

// New returns a Queue that holds up to Config.BufferSize waiting tasks and runs
// Config.WorkerCount of them at a time. Call Start to begin processing.
func New(reg *task.Registry, cfg Config) *Queue {
	if cfg.WorkerCount <= 0 {
		cfg.WorkerCount = 1
	}

	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 100
	}

	if cfg.RetryBaseDelay <= 0 {
		cfg.RetryBaseDelay = 100 * time.Millisecond
	}

	if cfg.RetryMaxDelay <= 0 {
		cfg.RetryMaxDelay = 10 * time.Second
	}

	if cfg.RetryMaxDelay < cfg.RetryBaseDelay {
		cfg.RetryMaxDelay = cfg.RetryBaseDelay
	}

	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	return &Queue{
		tasks:    make(chan string, cfg.BufferSize),
		store:    newStore(),
		registry: reg,
		cfg:      cfg,
		logger:   cfg.Logger,
	}
}

// Start launches the workers. Cancelling ctx cancels running handlers.
// Call Start once.
func (q *Queue) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	q.cancel = cancel

	for i := 1; i <= q.cfg.WorkerCount; i++ {
		q.worker(ctx, i)
	}
	q.logger.Info("queue started", "workers", q.cfg.WorkerCount, "buffer_size", q.cfg.BufferSize)
}

// Submit enqueues a copy of t. It returns an error wrapping
// task.ErrUnknownType, ErrQueueFull, or ErrQueueClosed.
func (q *Queue) Submit(t *task.Task) error {
	_, err := q.registry.Get(t.Type)
	if err != nil {
		return err
	}

	q.closedMu.RLock()
	defer q.closedMu.RUnlock()

	if q.closed {
		return ErrQueueClosed
	}

	q.store.Add(t)

	select {
	case q.tasks <- t.ID:
		q.logger.Debug("task submitted", "task_id", t.ID, "task_type", t.Type)
		return nil
	default:
		q.store.Delete(t.ID)
		q.logger.Warn("queue full, task rejected", "task_id", t.ID, "task_type", t.Type)
		return ErrQueueFull
	}
}

// Shutdown stops accepting tasks and waits for queued ones to finish. If ctx
// expires first, it cancels running handlers, marks the rest failed, and
// returns ctx.Err() once all workers exit. Call Start first. Later calls
// return nil immediately.
func (q *Queue) Shutdown(ctx context.Context) error {
	// must always call context cancel function
	defer q.cancel()

	q.closedMu.Lock()
	// checks if closed is already true, making redundant Shutdown calls safe
	if q.closed {
		q.closedMu.Unlock()
		return nil
	}
	q.closed = true
	close(q.tasks)
	q.closedMu.Unlock()
	q.logger.Info("queue shutting down, draining tasks")

	done := make(chan struct{})
	go func() {
		q.wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		q.logger.Warn("shutdown deadline exceeded, cancelling running tasks")
		// explicit cancel function call to signal to running handlers to stop
		q.cancel()
		<-done
		q.logger.Info("queue stopped")
		return ctx.Err()
	case <-done:
		q.logger.Info("queue stopped")
		return nil
	}
}

// Get returns a copy of the task with the given id.
func (q *Queue) Get(id string) (task.Task, bool) {
	return q.store.Get(id)
}

// List returns copies of all tasks in submission order.
func (q *Queue) List() []task.Task {
	return q.store.List()
}

func (q *Queue) worker(ctx context.Context, wID int) {
	logger := q.logger.With("worker", wID)
	q.wg.Go(func() {
		for tID := range q.tasks {
			q.process(ctx, logger, tID)
		}
	})
}

func (q *Queue) finish(id string, err error) {
	q.store.Update(id, func(t *task.Task) {
		t.DoneAt = new(time.Now())
		if err != nil {
			t.Status = task.StatusFailed
			t.LastError = err.Error()
		} else {
			t.Status = task.StatusSucceeded
			t.LastError = ""
		}
	})
}

func (q *Queue) process(ctx context.Context, logger *slog.Logger, tID string) {
	t, ok := q.store.Get(tID)
	if !ok {
		logger.Error("task not found in store", "task_id", tID)
		return
	}
	logger = logger.With("task_id", t.ID, "task_type", t.Type)

	// if cancel triggered, drain channel
	if ctx.Err() != nil {
		q.finish(t.ID, ctx.Err())
		logger.Warn("task cancelled before starting", "err", ctx.Err())
		return
	}

	handler, err := q.registry.Get(t.Type)
	if err != nil {
		q.finish(t.ID, err)
		logger.Error("task failed", "err", err)
		return
	}

	q.store.Update(t.ID, func(t *task.Task) {
		t.StartedAt = new(time.Now())
	})

	// t is a copy, so track attempts locally and mirror them into the store.
	attempt := t.AttemptCount
	for {
		attempt++
		q.store.Update(t.ID, func(t *task.Task) {
			t.Status = task.StatusRunning
			t.AttemptCount = attempt
		})

		logger.Debug("attempt started", "attempt", attempt, "max_attempts", t.MaxAttempts)

		err := handler(ctx, t.Payload)
		if err == nil {
			q.finish(t.ID, nil)
			logger.Info("task succeeded", "attempt", attempt)
			return
		}
		if ctx.Err() != nil {
			q.finish(t.ID, ctx.Err())
			logger.Warn("task cancelled", "attempt", attempt, "err", err)
			return
		}
		if errors.Is(err, task.ErrPermanent) {
			q.finish(t.ID, err)
			logger.Error("task failed with permanent error", "attempt", attempt, "err", err)
			return
		}
		if attempt >= t.MaxAttempts {
			q.finish(t.ID, err)
			logger.Error("task failed after max attempts", "attempt", attempt, "err", err)
			return
		}

		q.store.Update(t.ID, func(t *task.Task) {
			t.Status = task.StatusRetrying
			t.LastError = err.Error()
		})

		wait := q.backoff(attempt)
		logger.Warn("attempt failed, retrying",
			"attempt", attempt, "max_attempts", t.MaxAttempts, "backoff", wait, "err", err)

		select {
		case <-time.After(wait):
		case <-ctx.Done():
			q.finish(t.ID, ctx.Err())
			logger.Warn("task cancelled during backoff", "attempt", attempt, "err", ctx.Err())
			return
		}
	}
}

// backoff returns the wait after the given failed attempt (1-based): the base
// delay doubled per attempt, capped at the max, with full jitter.
func (q *Queue) backoff(attempt int) time.Duration {
	d := q.cfg.RetryBaseDelay
	// Double by looping rather than shifting so large attempts can't overflow.
	for i := 1; i < attempt && d < q.cfg.RetryMaxDelay; i++ {
		d *= 2
	}
	return rand.N(min(d, q.cfg.RetryMaxDelay))
}
