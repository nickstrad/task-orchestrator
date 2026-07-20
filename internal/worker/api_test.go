package worker

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/nickstrad/task-orchestrator/internal/httpapi"
	"github.com/nickstrad/task-orchestrator/internal/store"
	"github.com/nickstrad/task-orchestrator/internal/task"
)

func newTestAPI(t *testing.T) *API {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	w := NewWorker("worker-test", 0, logger, store.InMemoryDb)
	api := NewAPI(&w, "localhost", 0, logger)
	api.initRouter()
	return &api
}

// decodeError reads the body as an ErrorResponse and asserts its Code matches
// the HTTP status — the mismatch httpapi.WriteError exists to prevent.
func decodeError(t *testing.T, rec *httptest.ResponseRecorder, wantStatus int) {
	t.Helper()
	if rec.Code != wantStatus {
		t.Errorf("status = %d, want %d", rec.Code, wantStatus)
	}
	var e httpapi.ErrorResponse
	if err := json.NewDecoder(rec.Body).Decode(&e); err != nil {
		t.Fatalf("decoding error body: %v", err)
	}
	if e.Code != wantStatus {
		t.Errorf("body code = %d, want %d (must match the status)", e.Code, wantStatus)
	}
}

func TestStartTaskHandlerRejectsGarbage(t *testing.T) {
	api := newTestAPI(t)
	req := httptest.NewRequest(http.MethodPost, "/tasks/", strings.NewReader("{not json"))
	rec := httptest.NewRecorder()

	api.Router.ServeHTTP(rec, req)

	decodeError(t, rec, http.StatusBadRequest)
}

func TestStopTaskHandlerUnknownTask(t *testing.T) {
	api := newTestAPI(t)
	req := httptest.NewRequest(http.MethodDelete, "/tasks/"+uuid.New().String()+"/", nil)
	rec := httptest.NewRecorder()

	api.Router.ServeHTTP(rec, req)

	decodeError(t, rec, http.StatusNotFound)
}

func TestStopTaskHandlerExistingTaskReturns204WithNoBody(t *testing.T) {
	api := newTestAPI(t)
	existing := task.Task{ID: uuid.New(), State: task.Running, ContainerID: "abc123"}
	// Seed through the state helper rather than Db directly: state.go owns every
	// access to Db, and a test that reaches around it is the first place that
	// convention rots.
	api.Worker.putTask(existing)

	req := httptest.NewRequest(http.MethodDelete, "/tasks/"+existing.ID.String()+"/", nil)
	rec := httptest.NewRecorder()

	api.Router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusNoContent)
	}
	if body := rec.Body.String(); body != "" {
		t.Errorf("body = %q, want empty (204 means no content)", body)
	}
	if api.Worker.Queue.Len() != 1 {
		t.Errorf("queue length = %d, want 1 stop task enqueued", api.Worker.Queue.Len())
	}
}
