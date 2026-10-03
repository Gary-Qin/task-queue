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

type Queue struct {
	tasks       chan string
	store       *store
	registry    *task.Registry
	workerCount int
	wg          sync.WaitGroup
}

func New(reg *task.Registry, workerCount, bufferSize int) *Queue {
	return &Queue{
		tasks:       make(chan string, bufferSize),
		store:       newStore(),
		registry:    reg,
		workerCount: workerCount,
	}
}

func (q *Queue) Start() {
	for i := 1; i <= q.workerCount; i++ {
		q.worker(i)
	}
}

func (q *Queue) Submit(t *task.Task) error {
	_, err := q.registry.Get(t.Type)
	if err != nil {
		return err
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

func (q *Queue) Stop() {
	close(q.tasks)
	q.wg.Wait()
}

func (q *Queue) Get(id string) (task.Task, bool) {
	return q.store.Get(id)
}

func (q *Queue) List() []task.Task {
	return q.store.List()
}

func (q *Queue) worker(id int) {
	q.wg.Go(func() {
		for tID := range q.tasks {
			t, ok := q.store.Get(tID)
			if !ok {
				fmt.Printf("worker <%d>: task <%s> does not exist in store\n", id, tID)
				continue
			}

			handler, err := q.registry.Get(t.Type)
			if err != nil {
				q.store.Update(t.ID, func(t *task.Task) {
					t.Status = task.StatusFailed
					t.Error = err.Error()
					t.DoneAt = new(time.Now())
				})
				fmt.Printf("worker <%d>: error -> %v\n", id, err)
				continue
			}

			q.store.Update(t.ID, func(t *task.Task) {
				t.Status = task.StatusRunning
				t.StartedAt = new(time.Now())
				t.AttemptCount++
			})

			err = handler(context.Background(), t.Payload)
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
