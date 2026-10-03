package queue

import (
	"context"
	"errors"
	"fmt"
	"gqin/task-queue/internal/task"
	"sync"
	"time"
)

var ErrQueueFull = errors.New("queue is full")
var ErrQueueClosed = errors.New("queue is closed from shutdown")

type Queue struct {
	tasks       chan string
	store       *store
	registry    *task.Registry
	workerCount int
	wg          sync.WaitGroup
	cancel      context.CancelFunc
	closed      bool
	closedMu    sync.RWMutex
}

func New(reg *task.Registry, workerCount, bufferSize int) *Queue {
	return &Queue{
		tasks:       make(chan string, bufferSize),
		store:       newStore(),
		registry:    reg,
		workerCount: workerCount,
	}
}

func (q *Queue) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	q.cancel = cancel

	for i := 1; i <= q.workerCount; i++ {
		q.worker(ctx, i)
	}
}

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
		// explicit cancel function call to signal to workers to drain channel
		q.cancel()
		<-done
		return ctx.Err()
	case <-done:
		return nil
	}
}

func (q *Queue) Get(id string) (task.Task, bool) {
	return q.store.Get(id)
}

func (q *Queue) List() []task.Task {
	return q.store.List()
}

func (q *Queue) worker(ctx context.Context, id int) {
	q.wg.Go(func() {
		for tID := range q.tasks {
			// retrieve task t from store
			t, ok := q.store.Get(tID)
			if !ok {
				fmt.Printf("worker <%d>: error -> task <%s> does not exist in store\n", id, tID)
				continue
			}

			// if cancel triggered, drain channel
			if ctx.Err() != nil {
				q.store.Update(t.ID, func(t *task.Task) {
					t.DoneAt = new(time.Now())
					t.Status = task.StatusFailed
					t.Error = ctx.Err().Error()
				})
				fmt.Printf("worker <%d>: error -> %v\n", id, ctx.Err())
				continue
			}

			// retrieve handler for tasks of type t.Type
			handler, err := q.registry.Get(t.Type)
			if err != nil {
				q.store.Update(t.ID, func(t *task.Task) {
					t.DoneAt = new(time.Now())
					t.Status = task.StatusFailed
					t.Error = err.Error()
				})
				fmt.Printf("worker <%d>: error -> %v\n", id, err)
				continue
			}

			q.store.Update(t.ID, func(t *task.Task) {
				t.StartedAt = new(time.Now())
				t.Status = task.StatusRunning
				t.AttemptCount++
			})

			// execute handler on task
			err = handler(ctx, t.Payload)
			if err != nil {
				q.store.Update(t.ID, func(t *task.Task) {
					t.DoneAt = new(time.Now())
					t.Status = task.StatusFailed
					t.Error = err.Error()
				})
				// fmt.Printf("worker <%d>: task <%s> -> failed: %v\n", id, t.ID, err)
			} else {
				q.store.Update(t.ID, func(t *task.Task) {
					t.DoneAt = new(time.Now())
					t.Status = task.StatusSucceeded
					t.Error = ""
				})
				// fmt.Printf("worker <%d>: task <%s> -> succeeded\n", id, t.ID)
			}
		}
	})
}
