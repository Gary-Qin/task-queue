package main

import (
	"errors"
	"flag"
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestParseConfigDefaults(t *testing.T) {
	c, err := parseConfig(nil, io.Discard)
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}

	want := config{
		addr:            ":8080",
		workers:         4,
		bufferSize:      100,
		retryBaseDelay:  100 * time.Millisecond,
		retryMaxDelay:   10 * time.Second,
		shutdownTimeout: 30 * time.Second,
		logLevel:        slog.LevelInfo,
		logFormat:       "text",
	}
	if c != want {
		t.Errorf("got %+v, want %+v", c, want)
	}
}

func TestParseConfigOverrides(t *testing.T) {
	c, err := parseConfig([]string{
		"-addr", ":9090",
		"-workers", "8",
		"-buffer", "50",
		"-retry-base-delay", "250ms",
		"-retry-max-delay", "5s",
		"-shutdown-timeout", "1m",
		"-log-level", "debug",
		"-log-format", "json",
	}, io.Discard)
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}

	want := config{
		addr:            ":9090",
		workers:         8,
		bufferSize:      50,
		retryBaseDelay:  250 * time.Millisecond,
		retryMaxDelay:   5 * time.Second,
		shutdownTimeout: time.Minute,
		logLevel:        slog.LevelDebug,
		logFormat:       "json",
	}
	if c != want {
		t.Errorf("got %+v, want %+v", c, want)
	}
}

func TestParseConfigInvalid(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{"zero workers", []string{"-workers", "0"}},
		{"zero buffer", []string{"-buffer", "0"}},
		{"max delay below base", []string{"-retry-base-delay", "1s", "-retry-max-delay", "500ms"}},
		{"zero shutdown timeout", []string{"-shutdown-timeout", "0s"}},
		{"unknown log format", []string{"-log-format", "xml"}},
		{"unknown log level", []string{"-log-level", "loud"}},
		{"malformed duration", []string{"-retry-base-delay", "soon"}},
		{"unknown flag", []string{"-nope"}},
		{"positional argument", []string{"extra"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := parseConfig(tt.args, io.Discard); err == nil {
				t.Error("expected an error, got nil")
			}
		})
	}
}

func TestParseConfigHelp(t *testing.T) {
	_, err := parseConfig([]string{"-h"}, io.Discard)
	if !errors.Is(err, flag.ErrHelp) {
		t.Errorf("got %v, want flag.ErrHelp", err)
	}
}
