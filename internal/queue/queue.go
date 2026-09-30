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
	tasks       chan *task.Task
	registry    *task.Registry
	workerCount int
	wg          sync.WaitGroup
}

func New(reg *task.Registry, workerCount, bufferSize int) *Queue {
	return &Queue{
		tasks:       make(chan *task.Task, bufferSize),
		registry:    reg,
		workerCount: workerCount,
	}
}

func (q *Queue) Start() {
	for i := 0; i < q.workerCount; i++ {
		q.worker(i + 1)
	}
}

func (q *Queue) worker(id int) {
	q.wg.Go(func() {
		for t := range q.tasks {
			handler, err := q.registry.Get(t.Type)
			if err != nil {
				t.Status = task.StatusFailed
				t.Error = err.Error()
				t.DoneAt = new(time.Now())
				fmt.Printf("worker <%d>: error -> %v\n", id, err)
				continue
			}

			t.Status = task.StatusRunning
			t.StartedAt = new(time.Now())
			t.AttemptCount++

			err = handler(context.Background(), t.Payload)
			t.DoneAt = new(time.Now())
			if err != nil {
				t.Status = task.StatusFailed
				t.Error = err.Error()
			} else {
				t.Status = task.StatusSucceeded
				t.Error = ""
			}
			fmt.Printf("worker <%d>: task <%s> -> %s\n", id, t.ID, t.Status)
		}
	})
}

func (q *Queue) Submit(t *task.Task) error {
	select {
	case q.tasks <- t:
		return nil
	default:
		return ErrQueueFull
	}

}

func (q *Queue) Stop() {
	close(q.tasks)
	q.wg.Wait()
}
