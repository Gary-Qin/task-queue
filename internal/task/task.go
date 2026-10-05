// Package task defines tasks and the handlers that run them.
package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

var (
	// ErrPermanent marks an error that retrying won't fix. See Permanent.
	ErrPermanent = errors.New("permanent error")
	// ErrUnknownType is returned when no handler is registered for a task type.
	ErrUnknownType = errors.New("unknown handler type")
	// ErrInvalidMaxAttempts is returned by New when maxAttempts is less than 1.
	ErrInvalidMaxAttempts = errors.New("maxAttempts must be greater than 0")
)

// Status is a task's position in its lifecycle:
// pending → running (⇄ retrying) → succeeded or failed.
type Status string

// Task statuses.
const (
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusRetrying  Status = "retrying"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
)

// Task is a unit of work. Payload holds the handler's JSON-encoded input.
type Task struct {
	ID           string          `json:"id"`
	Type         string          `json:"type"`
	Payload      json.RawMessage `json:"payload"`
	Status       Status          `json:"status"`
	AttemptCount int             `json:"attemptCount"`
	MaxAttempts  int             `json:"maxAttempts"`
	LastError    string          `json:"error,omitempty"`
	CreatedAt    time.Time       `json:"createdAt"`
	StartedAt    *time.Time      `json:"startedAt,omitempty"`
	DoneAt       *time.Time      `json:"doneAt,omitempty"`
}

// New returns a pending task with payload encoded as JSON.
// maxAttempts must be at least 1.
func New(taskType string, payload any, maxAttempts int) (*Task, error) {
	if maxAttempts <= 0 {
		return nil, ErrInvalidMaxAttempts
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

// Permanent wraps err so the queue fails the task without retrying.
// It returns nil if err is nil.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %w", ErrPermanent, err)
}

// Handler runs one type of task. It should return promptly once ctx is
// cancelled. A nil error means the task succeeded. A handler may be run
// more than once due to retries, so it should be idempotent. For errors
// that can't be fixed by retrying, handlers should return Permanent(err).
type Handler func(ctx context.Context, payload json.RawMessage) error

// Registry maps task types to handlers. It is not safe for concurrent use:
// register every handler before the queue starts.
type Registry struct {
	handlers map[string]Handler
}

// NewRegistry returns an empty Registry.
func NewRegistry() *Registry {
	return &Registry{
		handlers: make(map[string]Handler),
	}
}

// Register sets the handler for name, replacing any existing one.
func (r *Registry) Register(name string, h Handler) {
	r.handlers[name] = h
}

// Get returns the handler for name, or an error wrapping ErrUnknownType.
func (r *Registry) Get(name string) (Handler, error) {
	h, ok := r.handlers[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownType, name)
	}
	return h, nil
}
