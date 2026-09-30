package task

import (
	"context"
	"encoding/json"
	"testing"
)

type TestStruct struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func TestTaskJSONRoundTrip(t *testing.T) {
	original, err := NewTask("test", TestStruct{Name: "Bob", Age: 20}, 3)
	if err != nil {
		t.Fatalf("NewTask: %v", err)
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var got Task
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if got.ID != original.ID {
		t.Errorf("ID: got %q, want %q", got.ID, original.ID)
	}
	if got.Type != original.Type {
		t.Errorf("Type: got %q, want %q", got.Type, original.Type)
	}
	if got.Status != original.Status {
		t.Errorf("Status: got %q, want %q", got.Status, original.Status)
	}
	if got.MaxAttempts != original.MaxAttempts {
		t.Errorf("MaxAttempts: got %d, want %d", got.MaxAttempts, original.MaxAttempts)
	}

	if !got.CreatedAt.Equal(original.CreatedAt) {
		t.Errorf("CreatedAt: got %v, want %v", got.CreatedAt, original.CreatedAt)
	}
	if got.StartedAt != nil || got.DoneAt != nil {
		t.Errorf("expected StartedAt and DoneAt to be nil, got %v and %v", got.StartedAt, got.DoneAt)
	}
}

func TestRegistryGet(t *testing.T) {
	reg := NewTaskRegistry()
	called := false
	reg.Register("print", func(ctx context.Context, payload json.RawMessage) error {
		called = true
		return nil
	})

	h, err := reg.Get("print")
	if err != nil {
		t.Fatalf("Get(%q): unexpected error: %v", "print", err)
	}
	if err := h(context.Background(), nil); err != nil {
		t.Errorf("handler returned error: %v", err)
	}
	if !called {
		t.Error("expected the registered handler to be called")
	}
}

func TestRegistryGetUnknown(t *testing.T) {
	reg := NewTaskRegistry()

	h, err := reg.Get("nope")
	if err == nil {
		t.Fatal("expected an error for an unregistered type, got nil")
	}
	if h != nil {
		t.Error("expected a nil handler for an unregistered type")
	}
}
