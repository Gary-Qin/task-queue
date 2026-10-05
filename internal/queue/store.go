package queue

import (
	"cmp"
	"gqin/task-queue/internal/task"
	"slices"
	"sync"
)

// entry pairs a task with the order it was added in. CreatedAt can't be used
// for ordering because the clock may return the same time for tasks created
// back to back.
type entry struct {
	task *task.Task
	seq  uint64
}

type store struct {
	tasks   map[string]entry
	nextSeq uint64
	mu      sync.RWMutex
}

func newStore() *store {
	return &store{tasks: make(map[string]entry)}
}

func (s *store) Add(t *task.Task) {
	cp := *t

	s.mu.Lock()
	defer s.mu.Unlock()

	s.tasks[t.ID] = entry{task: &cp, seq: s.nextSeq}
	s.nextSeq++
}

func (s *store) Delete(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.tasks, id)
}

func (s *store) Get(id string) (task.Task, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	e, ok := s.tasks[id]
	if ok {
		return *e.task, true
	}
	return task.Task{}, false
}

func (s *store) Update(id string, modifier func(t *task.Task)) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.tasks[id]
	if ok {
		modifier(e.task)
	}
	return ok
}

// List returns copies of all tasks in the order they were added.
func (s *store) List() []task.Task {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entries := make([]entry, 0, len(s.tasks))
	for _, e := range s.tasks {
		entries = append(entries, e)
	}
	slices.SortFunc(entries, func(a, b entry) int {
		return cmp.Compare(a.seq, b.seq)
	})

	tSlice := make([]task.Task, 0, len(entries))
	for _, e := range entries {
		tSlice = append(tSlice, *e.task)
	}
	return tSlice
}
