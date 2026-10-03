package main

import (
	"context"
	"encoding/json"
	"fmt"
	"gqin/task-queue/internal/queue"
	"gqin/task-queue/internal/task"
	"log"
	"math/rand/v2"
	"os"
	"os/signal"
	"time"
)

type TestStruct struct {
	TaskNum int `json:"taskNum"`
}

func taskSnapshot(q *queue.Queue) map[task.Status]int {
	tasks := q.List()
	statusMap := make(map[task.Status]int)
	
	for _, t := range tasks {
		statusMap[t.Status]++
	}

	fmt.Printf("pending: %v\trunning: %v\tsucceeded: %v\tfailed: %v\n", statusMap[task.StatusPending], statusMap[task.StatusRunning], statusMap[task.StatusSucceeded], statusMap[task.StatusFailed])

	return statusMap
}

func main() {
	workerCount := 4
	bufferSize := 20
	tr := task.NewRegistry()
	q := queue.New(tr, workerCount, bufferSize)

	minMs := 500
	maxMs := 2000
	sleepHandler := func(ctx context.Context, payload json.RawMessage) error {
		ms := minMs + rand.IntN(maxMs-minMs+1)
		select {
		case <-time.After(time.Duration(ms) * time.Millisecond):
			// fmt.Printf("%s\n", payload)
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	tr.Register("sleep", sleepHandler)

	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	q.Start(context.Background())
	taskCount := 20
	tasksSubmitted := 0
	for i := range taskCount {
		t, err := task.NewTask("sleep", TestStruct{TaskNum: i + 1}, 5)
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
}
