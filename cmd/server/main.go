package main

import (
	"context"
	"encoding/json"
	"fmt"
	"gqin/task-queue/internal/queue"
	"gqin/task-queue/internal/task"
	"log"
	"math/rand/v2"
	"time"
)

type TestStruct struct {
	TaskNum int `json:"taskNum"`
}

func main() {
	workerCount := 4
	bufferSize := 20
	tr := task.NewTaskRegistry()
	q := queue.New(tr, workerCount, bufferSize)

	minMs := 500
	maxMs := 2000
	sleepHandler := func(ctx context.Context, payload json.RawMessage) error {
		ms := minMs + rand.IntN(maxMs-minMs+1)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		// fmt.Printf("%s\n", payload)
		return nil
	}
	tr.Register("sleep", sleepHandler)

	q.Start()
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
	for {
		tasks := q.List()
		statusMap := make(map[task.Status]int)
		for _, t := range tasks {
			statusMap[t.Status]++
		}

		fmt.Printf("pending: %v\trunning: %v\tsucceeded: %v\tfailed: %v\n", statusMap[task.StatusPending], statusMap[task.StatusRunning], statusMap[task.StatusSucceeded], statusMap[task.StatusFailed])

		if statusMap[task.StatusSucceeded]+statusMap[task.StatusFailed] == tasksSubmitted {
			break
		}

		time.Sleep(250 * time.Millisecond)
	}
	q.Stop()
}
