package queue

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"gqin/task-queue/internal/task"
)

const testMaxAttempts = 5

// newTestQueue starts a single-worker queue with millisecond backoff that runs
// h for tasks of type "test".
func newTestQueue(t *testing.T, h task.Handler) *Queue {
	t.Helper()
	reg := task.NewRegistry()
	reg.Register("test", h)
	q := New(reg, Config{
		WorkerCount:    1,
		BufferSize:     10,
		RetryBaseDelay: time.Millisecond,
		RetryMaxDelay:  5 * time.Millisecond,
	})
	q.Start(context.Background())
	return q
}

func submitTestTask(t *testing.T, q *Queue) string {
	t.Helper()
	tk, err := task.New("test", nil, testMaxAttempts)
	if err != nil {
		t.Fatalf("task.New: %v", err)
	}
	if err := q.Submit(tk); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	return tk.ID
}

func TestRetry(t *testing.T) {
	errTransient := errors.New("transient")

	tests := []struct {
		name         string
		failures     int32 // calls that fail before the handler succeeds
		err          error // error returned by a failing call
		wantStatus   task.Status
		wantAttempts int
	}{
		{"succeeds after two failures", 2, errTransient, task.StatusSucceeded, 3},
		{"always fails", testMaxAttempts, errTransient, task.StatusFailed, testMaxAttempts},
		{"permanent error is not retried", testMaxAttempts, task.Permanent(errTransient), task.StatusFailed, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var calls atomic.Int32
			q := newTestQueue(t, func(ctx context.Context, payload json.RawMessage) error {
				if calls.Add(1) <= tt.failures {
					return tt.err
				}
				return nil
			})
			id := submitTestTask(t, q)

			// Shutdown waits for the worker to finish, so the result below is final.
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := q.Shutdown(ctx); err != nil {
				t.Fatalf("Shutdown: %v", err)
			}

			got, ok := q.Get(id)
			if !ok {
				t.Fatal("task missing from store")
			}
			if got.Status != tt.wantStatus {
				t.Errorf("Status: got %q, want %q (error: %q)", got.Status, tt.wantStatus, got.Error)
			}
			if got.AttemptCount != tt.wantAttempts {
				t.Errorf("AttemptCount: got %d, want %d", got.AttemptCount, tt.wantAttempts)
			}
			if n := int(calls.Load()); n != tt.wantAttempts {
				t.Errorf("handler calls: got %d, want %d", n, tt.wantAttempts)
			}
		})
	}
}

func TestShutdownInterruptsBackoff(t *testing.T) {
	reg := task.NewRegistry()
	reg.Register("test", func(ctx context.Context, payload json.RawMessage) error {
		return errors.New("transient")
	})
	// A long backoff that the test would notice if Shutdown waited it out.
	q := New(reg, Config{RetryBaseDelay: time.Hour, RetryMaxDelay: time.Hour})
	q.Start(context.Background())
	id := submitTestTask(t, q)

	// Wait for the first attempt to fail and the task to enter its backoff.
	deadline := time.Now().Add(5 * time.Second)
	for {
		got, _ := q.Get(id)
		if got.Status == task.StatusRetrying {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("task never entered retrying; status %q", got.Status)
		}
		time.Sleep(time.Millisecond)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := q.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Shutdown: got %v, want %v", err, context.DeadlineExceeded)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Shutdown took %v; it should not wait out the backoff", elapsed)
	}

	got, _ := q.Get(id)
	if got.Status != task.StatusFailed {
		t.Errorf("Status: got %q, want %q", got.Status, task.StatusFailed)
	}
	if got.AttemptCount != 1 {
		t.Errorf("AttemptCount: got %d, want 1", got.AttemptCount)
	}
}
