package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"gqin/task-queue/internal/queue"
	"gqin/task-queue/internal/task"
	"log"
	"net/http"
)

type server struct {
	q *queue.Queue
}

type request struct {
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	MaxAttempts int             `json:"maxAttempts"`
}

func NewHandler(q *queue.Queue) http.Handler {
	s := &server{q: q}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /tasks", s.handlePost)
	mux.HandleFunc("GET /tasks", s.handleGetAll)
	mux.HandleFunc("GET /tasks/{id}", s.handleGet)
	return mux
}

func (s *server) handlePost(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	var req request
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
			writeError(w, http.StatusRequestEntityTooLarge, err.Error())
			return
		}
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON: %s", err))
		return
	}

	t, err := task.New(req.Type, req.Payload, req.MaxAttempts)
	if err != nil {
		switch {
		case errors.Is(err, task.ErrInvalidMaxAttempts):
			writeError(w, http.StatusBadRequest, err.Error())
		default:
			log.Printf("task.New: %v", err)
			writeError(w, http.StatusInternalServerError, "internal error")
		}
		return
	}

	err = s.q.Submit(t)
	if err != nil {
		switch {
		case errors.Is(err, queue.ErrQueueFull), errors.Is(err, queue.ErrQueueClosed):
			writeError(w, http.StatusServiceUnavailable, err.Error())
		case errors.Is(err, task.ErrUnknownType):
			writeError(w, http.StatusBadRequest, err.Error())
		default:
			log.Printf("queue.Submit: %v", err)
			writeError(w, http.StatusInternalServerError, "internal error")
		}
		return
	}
	writeJSON(w, http.StatusAccepted, struct {
		ID string `json:"id"`
	}{ID: t.ID})
}

func (s *server) handleGetAll(w http.ResponseWriter, r *http.Request) {
	tasks := s.q.List()
	writeJSON(w, http.StatusOK, tasks)
}

func (s *server) handleGet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	t, ok := s.q.Get(id)
	if !ok {
		writeError(w, http.StatusNotFound, "task not found")
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func writeError(w http.ResponseWriter, status int, errorMsg string) {
	writeJSON(w, status, struct {
		Error string `json:"error"`
	}{Error: errorMsg})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	err := json.NewEncoder(w).Encode(body)
	if err != nil {
		log.Printf("writeJSON: %v", err)
	}
}
