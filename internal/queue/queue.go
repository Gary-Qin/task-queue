// Package queue runs tasks on a pool of workers and tracks their status.
package queue

import (
	"context"
	"errors"
	"fmt"
	"gqin/task-queue/internal/task"
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
	tasks       chan string
	store       *store
	registry    *task.Registry
	workerCount int
	wg          sync.WaitGroup
	cancel      context.CancelFunc
	closedMu    sync.RWMutex
	closed      bool
}

// New returns a Queue that holds up to bufferSize waiting tasks and runs
// workerCount of them at a time. Call Start to begin processing.
func New(reg *task.Registry, workerCount, bufferSize int) *Queue {
	return &Queue{
		tasks:       make(chan string, bufferSize),
		store:       newStore(),
		registry:    reg,
		workerCount: workerCount,
	}
}

// Start launches the workers. Cancelling ctx cancels running handlers.
// Call Start once.
func (q *Queue) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	q.cancel = cancel

	for i := 1; i <= q.workerCount; i++ {
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

// List returns copies of all tasks, oldest first.
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
		t.Status = task.StatusRunning
		t.AttemptCount++
	})

	err = handler(ctx, t.Payload)

	q.finish(t.ID, err)
}
