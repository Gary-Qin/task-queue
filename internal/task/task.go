package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type Status string

const (
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
)

type Task struct {
	ID           string          `json:"id"`
	Type         string          `json:"type"`
	Payload      json.RawMessage `json:"payload"`
	Status       Status          `json:"status"`
	AttemptCount int             `json:"attemptCount"`
	MaxAttempts  int             `json:"maxAttempts"`
	Error        string          `json:"error,omitempty"`
	CreatedAt    time.Time       `json:"createdAt"`
	StartedAt    *time.Time      `json:"startedAt,omitempty"`
	DoneAt       *time.Time      `json:"doneAt,omitempty"`
}

func NewTask(taskType string, payload any, maxAttempts int) (*Task, error) {
	if maxAttempts <= 0 {
		return nil, errors.New("maxAttempts must be greater than 0")
	}
	jsonData, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	return &Task{
		ID:           uuid.New().String(),
		Type:         taskType,
		Payload:      jsonData,
		Status:       StatusPending,
		AttemptCount: 0,
		MaxAttempts:  maxAttempts,
		CreatedAt:    time.Now(),
	}, nil
}

func (t *Task) UnmarshalPayload(v any) error {
	return json.Unmarshal(t.Payload, v)
}

type Handler func(ctx context.Context, payload json.RawMessage) error

type Registry struct {
	handlers map[string]Handler
}

func NewTaskRegistry() *Registry {
	return &Registry{
		handlers: make(map[string]Handler),
	}
}

func (tr *Registry) Register(name string, h Handler) {
	tr.handlers[name] = h
}

func (tr *Registry) Get(name string) (Handler, error) {
	h, ok := tr.handlers[name]
	if !ok {
		return h, fmt.Errorf("task handler for %s does not exist in registry", name)
	}
	return h, nil
}
