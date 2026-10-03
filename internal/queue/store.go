package queue

import (
	"gqin/task-queue/internal/task"
	"slices"
	"sync"
)

type store struct {
	tasks map[string]*task.Task
	mu    sync.RWMutex
}

func newStore() *store {
	return &store{tasks: make(map[string]*task.Task)}
}

func (s *store) Add(t *task.Task) {
	cp := *t

	s.mu.Lock()
	defer s.mu.Unlock()

	s.tasks[t.ID] = &cp
}

func (s *store) Delete(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.tasks, id)
}

func (s *store) Get(id string) (task.Task, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	t, ok := s.tasks[id]
	if ok {
		return *t, ok
	}
	return task.Task{}, false
}

func (s *store) Update(id string, modifier func(t *task.Task)) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	t, ok := s.tasks[id]
	if ok {
		modifier(t)
	}
	return ok
}

func (s *store) List() []task.Task {
	tSlice := []task.Task{}
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, t := range s.tasks {
		tSlice = append(tSlice, *t)
	}
	slices.SortFunc(tSlice, func(a, b task.Task) int {
		return a.CreatedAt.Compare(b.CreatedAt)
	})
	return tSlice
}
