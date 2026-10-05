package api

import (
	"context"
	"encoding/json"
	"gqin/task-queue/internal/queue"
	"gqin/task-queue/internal/task"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// setup returns an API handler over a fresh queue with a "noop" task type.
// The queue is not started, so submitted tasks stay pending.
func setup(t *testing.T, cfg queue.Config) (http.Handler, *queue.Queue) {
	t.Helper()

	r := task.NewRegistry()
	r.Register("noop", func(ctx context.Context, payload json.RawMessage) error {
		return nil
	})
	logger := slog.New(slog.DiscardHandler)
	cfg.Logger = logger
	q := queue.New(r, cfg)
	return NewHandler(q, logger), q
}

// seedTask submits a task directly to q, bypassing the API, and returns its ID.
func seedTask(t *testing.T, q *queue.Queue) string {
	t.Helper()

	tk, err := task.New("noop", nil, 1)
	if err != nil {
		t.Fatalf("task.New: %v", err)
	}
	if err := q.Submit(tk); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	return tk.ID
}

// serve sends a request to h and returns the recorded response.
func serve(h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func assertStatus(t *testing.T, rec *httptest.ResponseRecorder, want int) {
	t.Helper()

	if rec.Code != want {
		t.Fatalf("status: got %d, want %d; body: %s", rec.Code, want, rec.Body.String())
	}
}

// assertErrorBody checks that the response is a JSON error mentioning want.
func assertErrorBody(t *testing.T, rec *httptest.ResponseRecorder, want string) {
	t.Helper()

	var body struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode error body: %v; body: %s", err, rec.Body.String())
	}
	if !strings.Contains(body.Error, want) {
		t.Errorf("error: got %q, want it to contain %q", body.Error, want)
	}
}

func TestValidSubmit(t *testing.T) {
	h, q := setup(t, queue.Config{})

	rec := serve(h, http.MethodPost, "/tasks", `{"type":"noop","payload":{"test":2},"maxAttempts":2}`)

	assertStatus(t, rec, http.StatusAccepted)
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type: got %q, want %q", ct, "application/json")
	}
	var body struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v; body: %s", err, rec.Body.String())
	}
	got, ok := q.Get(body.ID)
	if !ok {
		t.Fatalf("returned ID %q not found in queue", body.ID)
	}
	if got.Status != task.StatusPending {
		t.Errorf("Status: got %q, want %q", got.Status, task.StatusPending)
	}
}

func TestSubmitInvalidJSON(t *testing.T) {
	h, _ := setup(t, queue.Config{})

	rec := serve(h, http.MethodPost, "/tasks", `{"type":"noop",`)

	assertStatus(t, rec, http.StatusBadRequest)
	assertErrorBody(t, rec, "invalid JSON")
}

func TestSubmitUnknownType(t *testing.T) {
	h, _ := setup(t, queue.Config{})

	rec := serve(h, http.MethodPost, "/tasks", `{"type":"nope","maxAttempts":1}`)

	assertStatus(t, rec, http.StatusBadRequest)
	assertErrorBody(t, rec, `"nope"`)
}

func TestSubmitInvalidMaxAttempts(t *testing.T) {
	h, _ := setup(t, queue.Config{})

	rec := serve(h, http.MethodPost, "/tasks", `{"type":"noop","maxAttempts":0}`)

	assertStatus(t, rec, http.StatusBadRequest)
	assertErrorBody(t, rec, task.ErrInvalidMaxAttempts.Error())
}

func TestSubmitBodyTooLarge(t *testing.T) {
	h, _ := setup(t, queue.Config{})
	big := `{"type":"noop","payload":"` + strings.Repeat("a", 1<<20) + `"}`

	rec := serve(h, http.MethodPost, "/tasks", big)

	assertStatus(t, rec, http.StatusRequestEntityTooLarge)
}

func TestSubmitQueueFull(t *testing.T) {
	h, q := setup(t, queue.Config{BufferSize: 1})
	seedTask(t, q) // fills the only slot; no workers are running to drain it

	rec := serve(h, http.MethodPost, "/tasks", `{"type":"noop","maxAttempts":1}`)

	assertStatus(t, rec, http.StatusServiceUnavailable)
	assertErrorBody(t, rec, queue.ErrQueueFull.Error())
}

func TestSubmitQueueClosed(t *testing.T) {
	h, q := setup(t, queue.Config{})
	q.Start(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := q.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	rec := serve(h, http.MethodPost, "/tasks", `{"type":"noop","maxAttempts":1}`)

	assertStatus(t, rec, http.StatusServiceUnavailable)
	assertErrorBody(t, rec, queue.ErrQueueClosed.Error())
}

func TestGetTask(t *testing.T) {
	h, q := setup(t, queue.Config{})
	id := seedTask(t, q)

	rec := serve(h, http.MethodGet, "/tasks/"+id, "")

	assertStatus(t, rec, http.StatusOK)
	var got task.Task
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode body: %v; body: %s", err, rec.Body.String())
	}
	if got.ID != id {
		t.Errorf("ID: got %q, want %q", got.ID, id)
	}
	if got.Status != task.StatusPending {
		t.Errorf("Status: got %q, want %q", got.Status, task.StatusPending)
	}
}

func TestGetTaskNotFound(t *testing.T) {
	h, _ := setup(t, queue.Config{})

	rec := serve(h, http.MethodGet, "/tasks/does-not-exist", "")

	assertStatus(t, rec, http.StatusNotFound)
	assertErrorBody(t, rec, "not found")
}

func TestListTasks(t *testing.T) {
	h, q := setup(t, queue.Config{})
	first := seedTask(t, q)
	second := seedTask(t, q)

	rec := serve(h, http.MethodGet, "/tasks", "")

	assertStatus(t, rec, http.StatusOK)
	var got []task.Task
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode body: %v; body: %s", err, rec.Body.String())
	}
	if len(got) != 2 {
		t.Fatalf("len: got %d, want 2", len(got))
	}
	if got[0].ID != first || got[1].ID != second {
		t.Errorf("order: got [%s %s], want [%s %s] (submission order)", got[0].ID, got[1].ID, first, second)
	}
}

func TestListTasksEmpty(t *testing.T) {
	h, _ := setup(t, queue.Config{})

	rec := serve(h, http.MethodGet, "/tasks", "")

	assertStatus(t, rec, http.StatusOK)
	// An empty list must encode as [], not null.
	if body := strings.TrimSpace(rec.Body.String()); body != "[]" {
		t.Errorf("body: got %s, want []", body)
	}
}

func TestWrongMethod(t *testing.T) {
	h, _ := setup(t, queue.Config{})

	rec := serve(h, http.MethodDelete, "/tasks", "")

	assertStatus(t, rec, http.StatusMethodNotAllowed)
}
