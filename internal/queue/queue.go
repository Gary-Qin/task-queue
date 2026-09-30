package queue

import (
	"fmt"
	"gqin/task-queue/internal/task"
	"sync"
)

type Pool struct {
	tasks       chan *task.Task
	registry    *task.Registry
	workerCount int
	wg          sync.WaitGroup
}

func NewPool(reg *task.Registry, workerCount, bufferSize int) *Pool {
	return &Pool{
		tasks: make(chan *task.Task, bufferSize),
		registry: reg,
		workerCount: workerCount,
	}
}

func (p *Pool) Start() {
	for i := 0; i < p.workerCount; i++ {
		p.wg.Go(func() {
			// do something
		})
		p.wg.Wait()
	}
}

func (p *Pool) Submit(t *task.Task) error {
	p.tasks <- t
	return nil
}

func (p *Pool) Stop() {
	close(p.tasks)
	for v := range p.tasks {
		// placeholder
		fmt.Print(v)
	}
}
