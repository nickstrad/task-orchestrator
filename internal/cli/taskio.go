package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/nickstrad/task-orchestrator/internal/httpapi"
	"github.com/nickstrad/task-orchestrator/internal/task"
	"gopkg.in/yaml.v3"
)

// httpClient is the shared client for the one-shot task/stats commands. The
// timeout keeps a command from hanging forever against a manager or worker that
// is down or wedged mid-response — the bare http.Get/http.Post helpers and
// http.DefaultClient have no timeout at all.
var httpClient = &http.Client{Timeout: 30 * time.Second}

// target is the address a task command talks to — a manager, or a worker
// directly, since both expose the same /tasks endpoints.
type target struct {
	host string
	port int
}

func (t target) tasksURL() string {
	return fmt.Sprintf("http://%s:%d/tasks", t.host, t.port)
}

// loadTaskEvent reads a TaskEvent from a JSON or YAML file (or stdin when path
// is "-"). Both formats go through one path: YAML is a superset of JSON, so a
// generic YAML decode parses either, and re-encoding to JSON lets encoding/json
// match the tagless task structs case-insensitively — so a file can use natural
// keys like "healthCheck" instead of the mangled "healthcheck" a direct YAML
// decode into the struct would demand.
//
// Missing IDs are filled with fresh UUIDs; every other field is taken verbatim.
func loadTaskEvent(path string) (task.TaskEvent, error) {
	var (
		data []byte
		err  error
	)
	if path == "-" {
		data, err = io.ReadAll(os.Stdin)
	} else {
		data, err = os.ReadFile(path)
	}
	if err != nil {
		return task.TaskEvent{}, fmt.Errorf("reading task event: %w", err)
	}

	var generic any
	if err := yaml.Unmarshal(data, &generic); err != nil {
		return task.TaskEvent{}, fmt.Errorf("parsing task event (want JSON or YAML): %w", err)
	}
	jsonBytes, err := json.Marshal(generic)
	if err != nil {
		return task.TaskEvent{}, fmt.Errorf("normalizing task event: %w", err)
	}

	var te task.TaskEvent
	dec := json.NewDecoder(bytes.NewReader(jsonBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&te); err != nil {
		return task.TaskEvent{}, fmt.Errorf("decoding task event (check the field names): %w", err)
	}

	if te.ID == uuid.Nil {
		te.ID = uuid.New()
	}
	if te.Task.ID == uuid.Nil {
		te.Task.ID = uuid.New()
	}
	if te.Timestamp.IsZero() {
		te.Timestamp = time.Now()
	}
	return te, nil
}

// submitTaskEvent POSTs a TaskEvent to the target's /tasks endpoint. When
// dryRun is set it prints the exact JSON that would be sent and returns without
// making a request — handy for checking a file before a manager is up.
func submitTaskEvent(tgt target, te task.TaskEvent, dryRun bool, out io.Writer) error {
	body, err := json.MarshalIndent(te, "", "  ")
	if err != nil {
		return fmt.Errorf("encoding task event: %w", err)
	}
	if dryRun {
		fmt.Fprintln(out, string(body))
		return nil
	}

	resp, err := httpClient.Post(tgt.tasksURL(), "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("sending task to %s: %w", tgt.tasksURL(), err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("task rejected: %s", readAPIError(resp))
	}
	fmt.Fprintf(out, "task submitted: id=%s name=%s state=%s\n",
		te.Task.ID, te.Task.Name, te.Task.State)
	return nil
}

// readAPIError turns a non-2xx response into a one-line message, preferring the
// server's httpapi.ErrorResponse body over a bare status code.
func readAPIError(resp *http.Response) string {
	var apiErr httpapi.ErrorResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiErr); err == nil && apiErr.Message != "" {
		return fmt.Sprintf("%s (%d)", apiErr.Message, resp.StatusCode)
	}
	return resp.Status
}
