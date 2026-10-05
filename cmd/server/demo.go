package main

import (
	"context"
	"encoding/json"
	"errors"
	"gqin/task-queue/internal/task"
	"math/rand/v2"
	"time"
)

// registerDemoHandlers registers example task types for trying out the queue:
//   - "sleep" waits 5–20s, then succeeds.
//   - "flaky" waits 0.5–2s, then fails with a transient error half the time.
//   - "bad" always fails with a permanent error.
func registerDemoHandlers(r *task.Registry) {
	r.Register("sleep", func(ctx context.Context, payload json.RawMessage) error {
		return sleepBetween(ctx, 5*time.Second, 20*time.Second)
	})
	r.Register("flaky", func(ctx context.Context, payload json.RawMessage) error {
		if err := sleepBetween(ctx, 500*time.Millisecond, 2*time.Second); err != nil {
			return err
		}
		if rand.IntN(2) == 0 {
			return errors.New("transient")
		}
		return nil
	})
	r.Register("bad", func(ctx context.Context, payload json.RawMessage) error {
		return task.Permanent(errors.New("bad"))
	})
}

// sleepBetween waits a random duration in [lo, hi], or until ctx is cancelled.
func sleepBetween(ctx context.Context, lo, hi time.Duration) error {
	select {
	case <-time.After(lo + rand.N(hi-lo+1)):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
