package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"
)

// config holds the server's command-line settings.
type config struct {
	addr            string
	workers         int
	bufferSize      int
	retryBaseDelay  time.Duration
	retryMaxDelay   time.Duration
	shutdownTimeout time.Duration
	logLevel        slog.Level
	logFormat       string
}

// parseConfig parses command-line flags from args. Usage and error messages
// are written to output. It returns flag.ErrHelp if -h or -help was given.
func parseConfig(args []string, output io.Writer) (config, error) {
	fs := flag.NewFlagSet("task-queue", flag.ContinueOnError)
	fs.SetOutput(output)

	var c config
	fs.StringVar(&c.addr, "addr", ":8080", "HTTP listen address")
	fs.IntVar(&c.workers, "workers", 4, "number of tasks to run at once")
	fs.IntVar(&c.bufferSize, "buffer", 100, "number of tasks that can wait to run")
	fs.DurationVar(&c.retryBaseDelay, "retry-base-delay", 100*time.Millisecond, "delay before the first retry; doubles on each later retry")
	fs.DurationVar(&c.retryMaxDelay, "retry-max-delay", 10*time.Second, "maximum delay between retries")
	fs.DurationVar(&c.shutdownTimeout, "shutdown-timeout", 30*time.Second, "time allowed for in-flight work to finish on shutdown")
	fs.TextVar(&c.logLevel, "log-level", slog.LevelInfo, "minimum log level: debug, info, warn, or error")
	fs.StringVar(&c.logFormat, "log-format", "text", "log format: text or json")

	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	if fs.NArg() > 0 {
		err := fmt.Errorf("unexpected arguments: %v", fs.Args())
		fmt.Fprintln(output, err)
		return config{}, err
	}
	if err := c.validate(); err != nil {
		fmt.Fprintln(output, err)
		return config{}, err
	}
	return c, nil
}

func (c config) validate() error {
	switch {
	case c.workers < 1:
		return errors.New("-workers must be at least 1")
	case c.bufferSize < 1:
		return errors.New("-buffer must be at least 1")
	case c.retryBaseDelay <= 0:
		return errors.New("-retry-base-delay must be positive")
	case c.retryMaxDelay < c.retryBaseDelay:
		return errors.New("-retry-max-delay must be at least -retry-base-delay")
	case c.shutdownTimeout <= 0:
		return errors.New("-shutdown-timeout must be positive")
	case c.logFormat != "text" && c.logFormat != "json":
		return fmt.Errorf("-log-format must be text or json, got %q", c.logFormat)
	}
	return nil
}

// newLogger returns a logger writing to stderr in the configured format and level.
func newLogger(c config) *slog.Logger {
	opts := &slog.HandlerOptions{Level: c.logLevel}
	if c.logFormat == "json" {
		return slog.New(slog.NewJSONHandler(os.Stderr, opts))
	}
	return slog.New(slog.NewTextHandler(os.Stderr, opts))
}
