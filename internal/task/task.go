package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

var ErrUnknownType = errors.New("unknown handler type")

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

func New(taskType string, payload any, maxAttempts int) (*Task, error) {
	if maxAttempts <= 0 {
		return nil, errors.New("maxAttempts must be greater than 0")
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	return &Task{
		ID:          uuid.New().String(),
		Type:        taskType,
		Payload:     data,
		Status:      StatusPending,
		MaxAttempts: maxAttempts,
		CreatedAt:   time.Now(),
	}, nil
}

type Handler func(ctx context.Context, payload json.RawMessage) error

type Registry struct {
	handlers map[string]Handler
}

func NewRegistry() *Registry {
	return &Registry{
		handlers: make(map[string]Handler),
	}
}

func (r *Registry) Register(name string, h Handler) {
	r.handlers[name] = h
}

func (r *Registry) Get(name string) (Handler, error) {
	h, ok := r.handlers[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownType, name)
	}
	return h, nil
}
