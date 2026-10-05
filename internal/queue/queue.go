// Package queue runs tasks on a pool of workers and tracks their status.
package queue

import (
	"context"
	"errors"
	"fmt"
	"gqin/task-queue/internal/task"
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

	return &Queue{
		tasks:    make(chan string, cfg.BufferSize),
		store:    newStore(),
		registry: reg,
		cfg:      cfg,
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
		return nil
	default:
		q.store.Delete(t.ID)
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

	done := make(chan struct{})
	go func() {
		q.wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		// explicit cancel function call to signal to running handlers to stop
		q.cancel()
		<-done
		return ctx.Err()
	case <-done:
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
	q.wg.Go(func() {
		for tID := range q.tasks {
			q.process(ctx, wID, tID)
		}
	})
}

func (q *Queue) finish(id string, err error) {
	q.store.Update(id, func(t *task.Task) {
		t.DoneAt = new(time.Now())
		if err != nil {
			t.Status = task.StatusFailed
			t.Error = err.Error()
		} else {
			t.Status = task.StatusSucceeded
			t.Error = ""
		}
	})
}

func (q *Queue) process(ctx context.Context, wID int, tID string) {
	t, ok := q.store.Get(tID)
	if !ok {
		fmt.Printf("worker <%d>: error -> task <%s> does not exist in store\n", wID, tID)
		return
	}

	// if cancel triggered, drain channel
	if ctx.Err() != nil {
		q.finish(t.ID, ctx.Err())
		fmt.Printf("worker <%d>: error -> %v\n", wID, ctx.Err())
		return
	}

	handler, err := q.registry.Get(t.Type)
	if err != nil {
		q.finish(t.ID, err)
		fmt.Printf("worker <%d>: error -> %v\n", wID, err)
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

		err := handler(ctx, t.Payload)
		if err == nil {
			q.finish(t.ID, nil)
			return
		}
		if ctx.Err() != nil {
			q.finish(t.ID, ctx.Err())
			return
		}
		if errors.Is(err, task.ErrPermanent) {
			q.finish(t.ID, err)
			return
		}
		if attempt >= t.MaxAttempts {
			q.finish(t.ID, err)
			return
		}

		q.store.Update(t.ID, func(t *task.Task) {
			t.Status = task.StatusRetrying
			t.Error = err.Error()
		})

		select {
		case <-time.After(q.backoff(attempt)):
		case <-ctx.Done():
			q.finish(t.ID, ctx.Err())
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
