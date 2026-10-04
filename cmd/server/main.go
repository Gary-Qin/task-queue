package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"gqin/task-queue/internal/queue"
	"gqin/task-queue/internal/task"
	"log"
	"math/rand/v2"
	"os"
	"os/signal"
	"text/tabwriter"
	"time"
)

type demoPayload struct {
	TaskNum int `json:"taskNum"`
}

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
		minMs := 500
		maxMs := 2000
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
	taskCount := 20
	tasksSubmitted := 0
	for i := range taskCount {
		var t *task.Task
		var err error
		switch rand.IntN(3) {
		case 0:
			t, err = task.New("flaky", demoPayload{TaskNum: i + 1}, 5)
		case 1:
			t, err = task.New("bad", demoPayload{TaskNum: i + 1}, 5)
		case 2:
			t, err = task.New("sleep", demoPayload{TaskNum: i + 1}, 5)
		}

		if err != nil {
			log.Fatal(err)
		}
		err = q.Submit(t)
		if err != nil {
			fmt.Printf("%v\n", err)
		} else {
			tasksSubmitted++
		}
	}

	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

loop:
	for {
		select {
		case <-signalCtx.Done():
			stop()
			fmt.Println("shutting down")
			break loop
		case <-ticker.C:
			statusMap := taskSnapshot(q)
			if statusMap[task.StatusSucceeded]+statusMap[task.StatusFailed] == tasksSubmitted {
				break loop
			}
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := q.Shutdown(shutdownCtx)
	if err != nil {
		fmt.Printf("%v\n", err)
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
}
