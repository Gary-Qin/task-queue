package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"gqin/task-queue/internal/queue"
	"gqin/task-queue/internal/task"
	"log/slog"
	"net/http"
	"time"
)

type server struct {
	q      *queue.Queue
	logger *slog.Logger
}

type request struct {
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	MaxAttempts int             `json:"maxAttempts"`
}

// NewHandler returns the HTTP API for q. Every request is logged to logger;
// a nil logger uses slog.Default().
func NewHandler(q *queue.Queue, logger *slog.Logger) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	s := &server{q: q, logger: logger}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /tasks", s.handlePost)
	mux.HandleFunc("GET /tasks", s.handleGetAll)
	mux.HandleFunc("GET /tasks/{id}", s.handleGet)
	return s.logRequests(mux)
}

// statusRecorder captures the status code a handler writes so it can be logged.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

// Unwrap lets http.ResponseController reach the underlying ResponseWriter.
func (r *statusRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}

// logRequests logs the method, path, status, and duration of every request.
func (s *server) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		s.logger.Info("request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", rec.status,
			"duration", time.Since(start),
		)
	})
}

func (s *server) handlePost(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	var req request
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
			s.writeError(w, http.StatusRequestEntityTooLarge, err.Error())
			return
		}
		s.writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON: %s", err))
		return
	}

	t, err := task.New(req.Type, req.Payload, req.MaxAttempts)
	if err != nil {
		switch {
		case errors.Is(err, task.ErrInvalidMaxAttempts):
			s.writeError(w, http.StatusBadRequest, err.Error())
		default:
			s.logger.Error("creating task", "err", err)
			s.writeError(w, http.StatusInternalServerError, "internal error")
		}
		return
	}

	err = s.q.Submit(t)
	if err != nil {
		switch {
		case errors.Is(err, queue.ErrQueueFull), errors.Is(err, queue.ErrQueueClosed):
			s.writeError(w, http.StatusServiceUnavailable, err.Error())
		case errors.Is(err, task.ErrUnknownType):
			s.writeError(w, http.StatusBadRequest, err.Error())
		default:
			s.logger.Error("submitting task", "task_id", t.ID, "err", err)
			s.writeError(w, http.StatusInternalServerError, "internal error")
		}
		return
	}
	s.writeJSON(w, http.StatusAccepted, struct {
		ID string `json:"id"`
	}{ID: t.ID})
}

func (s *server) handleGetAll(w http.ResponseWriter, r *http.Request) {
	tasks := s.q.List()
	s.writeJSON(w, http.StatusOK, tasks)
}

func (s *server) handleGet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	t, ok := s.q.Get(id)
	if !ok {
		s.writeError(w, http.StatusNotFound, "task not found")
		return
	}
	s.writeJSON(w, http.StatusOK, t)
}

func (s *server) writeError(w http.ResponseWriter, status int, errorMsg string) {
	s.writeJSON(w, status, struct {
		Error string `json:"error"`
	}{Error: errorMsg})
}

func (s *server) writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	err := json.NewEncoder(w).Encode(body)
	if err != nil {
		s.logger.Error("writing response", "err", err)
	}
}
