package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"gqin/task-queue/internal/api"
	"gqin/task-queue/internal/queue"
	"gqin/task-queue/internal/task"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"text/tabwriter"
	"time"
)

func taskSnapshot(q *queue.Queue) map[task.Status]int {
	tasks := q.List()
	statusMap := make(map[task.Status]int)

	for _, t := range tasks {
		statusMap[t.Status]++
	}

	fmt.Printf("pending %2d | running %2d | retrying %2d | succeeded %2d | failed %2d\n", statusMap[task.StatusPending], statusMap[task.StatusRunning], statusMap[task.StatusRetrying], statusMap[task.StatusSucceeded], statusMap[task.StatusFailed])

	return statusMap
}

func main() {
	r := task.NewRegistry()
	q := queue.New(r, queue.Config{
		WorkerCount:    4,
		BufferSize:     20,
		RetryBaseDelay: 100 * time.Millisecond,
		RetryMaxDelay:  800 * time.Millisecond,
	})

	sleepHandler := func(ctx context.Context, payload json.RawMessage) error {
		minMs := 5000
		maxMs := 20000
		ms := minMs + rand.IntN(maxMs-minMs+1)
		select {
		case <-time.After(time.Duration(ms) * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	flakyHandler := func(ctx context.Context, payload json.RawMessage) error {
		minMs := 500
		maxMs := 2000
		ms := minMs + rand.IntN(maxMs-minMs+1)
		select {
		case <-time.After(time.Duration(ms) * time.Millisecond):
			flip := rand.IntN(2)
			if flip == 0 {
				return errors.New("transient")
			}
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	badHandler := func(ctx context.Context, payload json.RawMessage) error {
		return task.Permanent(errors.New("bad"))
	}
	r.Register("sleep", sleepHandler)
	r.Register("flaky", flakyHandler)
	r.Register("bad", badHandler)

	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	q.Start(context.Background())

	srv := &http.Server{Addr: ":8080", Handler: api.NewHandler(q), ReadHeaderTimeout: 5 * time.Second}
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- srv.ListenAndServe()
	}()
	fmt.Println("listening on", srv.Addr)

	var serverFailed bool
	select {
	case <-signalCtx.Done():
		stop()
		fmt.Println("shutting down")
	case err := <-serverErr:
		fmt.Println("server error:", err)
		serverFailed = true
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if !serverFailed {
		if err := srv.Shutdown(shutdownCtx); err != nil {
			fmt.Println("http shutdown:", err)
		}
	}
	if err := q.Shutdown(shutdownCtx); err != nil {
		fmt.Println("queue shutdown:", err)
	}

	fmt.Println("--- Final Task Snapshot ---")
	taskSnapshot(q)
	fmt.Println("--- Tasks ---")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "#\tTYPE\tSTATUS\tATTEMPTS\tERROR")
	for i, t := range q.List() {
		fmt.Fprintf(w, "%d\t%s\t%s\t%d\t%s\n", i+1, t.Type, t.Status, t.AttemptCount, t.Error)
	}
	w.Flush()

	if serverFailed {
		os.Exit(1)
	}
}
