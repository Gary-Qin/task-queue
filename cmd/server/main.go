package main

import (
	"context"
	"errors"
	"flag"
	"gqin/task-queue/internal/api"
	"gqin/task-queue/internal/queue"
	"gqin/task-queue/internal/task"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"time"
)

func main() {
	cfg, err := parseConfig(os.Args[1:], os.Stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		os.Exit(2) // parseConfig has already printed the problem
	}

	logger := newLogger(cfg)
	slog.SetDefault(logger)

	r := task.NewRegistry()
	registerDemoHandlers(r)
	q := queue.New(r, queue.Config{
		WorkerCount:    cfg.workers,
		BufferSize:     cfg.bufferSize,
		RetryBaseDelay: cfg.retryBaseDelay,
		RetryMaxDelay:  cfg.retryMaxDelay,
		Logger:         logger,
	})

	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	q.Start(context.Background())

	srv := &http.Server{
		Addr:              cfg.addr,
		Handler:           api.NewHandler(q, logger),
		ReadHeaderTimeout: 5 * time.Second,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelError),
	}
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- srv.ListenAndServe()
	}()
	logger.Info("server listening", "addr", srv.Addr)

	var serverFailed bool
	select {
	case <-signalCtx.Done():
		stop()
		logger.Info("shutdown signal received", "timeout", cfg.shutdownTimeout)
	case err := <-serverErr:
		logger.Error("server failed", "err", err)
		serverFailed = true
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.shutdownTimeout)
	defer cancel()

	if !serverFailed {
		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.Error("http shutdown", "err", err)
		}
	}
	if err := q.Shutdown(shutdownCtx); err != nil {
		logger.Error("queue shutdown", "err", err)
	}
	logSummary(logger, q)

	if serverFailed {
		os.Exit(1)
	}
}

// logSummary logs how many tasks ended in each status.
func logSummary(logger *slog.Logger, q *queue.Queue) {
	counts := make(map[task.Status]int)
	for _, t := range q.List() {
		counts[t.Status]++
	}
	logger.Info("shutdown complete",
		"succeeded", counts[task.StatusSucceeded],
		"failed", counts[task.StatusFailed],
		"pending", counts[task.StatusPending],
		"running", counts[task.StatusRunning],
		"retrying", counts[task.StatusRetrying],
	)
}
