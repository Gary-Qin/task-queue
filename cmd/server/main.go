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
	registry := task.NewTaskRegistry()
	workerCount := 4
	bufferSize := 20
	queue := queue.New(registry, workerCount, bufferSize)

	minMs := 500
	maxMs := 2000
	sleepHandler := func(ctx context.Context, payload json.RawMessage) error {
		ms := minMs + rand.IntN(maxMs-minMs+1)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		fmt.Printf("%s\n", payload)
		return nil
	}
	registry.Register("sleep", sleepHandler)

	taskCount := 20
	queue.Start()
	for i := range taskCount {
		t, err := task.NewTask("sleep", TestStruct{TaskNum: i + 1}, 5)
		if err != nil {
			log.Fatal(err)
		}
		err = queue.Submit(t)
		if err != nil {
			fmt.Printf("%v\n", err)
		}
	}
	queue.Stop()
}
