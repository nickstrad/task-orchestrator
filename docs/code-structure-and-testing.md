# Code Structure & Testing Conventions

How to split logic in this repo so it can be tested, and what to test once it is split.
If you are adding code, follow this.

One rule carries most of the weight:

**Separate the decision from the plumbing.** Pull the logic that computes a value or
picks an outcome out of the code that talks to HTTP, Docker, the clock, or a logger.
Test the logic directly; keep the plumbing thin enough to read.

---

## Three kinds of code

| Kind | What it does | Where it lives | How it is tested |
| --- | --- | --- | --- |
| **Pure functions** | Values in, values out. No receiver, no I/O, no clock, no logging, no globals. | `pure.go` in the package, or alongside the type they serve | Directly, table-driven. No fixtures, no fakes. |
| **Business logic** | Composes pure functions and mutates the package's own state (`TaskDb`, `w.Db`, the queue). | Methods on `Manager` / `Worker` | Construct the struct, call the method, assert on the state it owns. |
| **Framework plumbing** | HTTP handlers, Docker SDK calls, `time.After` loops, `slog` lines. | `api.go`, `docker.go`, the `*Tasks(done <-chan struct{})` loops | Sparingly — `httptest` at the boundary, or not at all. |

The point of the split is that most of what can actually be *wrong* lives in the first
two rows, and neither needs a Docker daemon or a live worker to exercise.

## Extracting a pure function

The signal is a block inside a method that computes something without touching the
receiver. It is buried, so nothing can test it, and it will drift.

`manager.checkTaskHealth` originally built its URL inline:

```go
worker := strings.Split(w, ":")
url := fmt.Sprintf("https://%s:%s%s", worker[0], hostPort, t.HealthCheck)
resp, err := http.Get(url)
```

Wrong scheme, and `worker[0]` is a worker *name*, not a host. Every health check failed,
which restarted every running task on a 60-second loop. The bug was invisible because
reaching it required a live worker, a running container, and a failing probe.

Pulled out, it is a function you can just call:

```go
// healthCheckURL builds the URL for a task's health check endpoint.
//
// The worker's address carries the worker's own port (e.g. http://localhost:3001),
// but the health check must reach the *container*, which Docker published on a
// different host port.
func healthCheckURL(workerAddr, hostPort, healthCheckPath string) (string, error) {
    parsed, err := neturl.Parse(workerAddr)
    if err != nil {
        return "", fmt.Errorf("parsing worker address %q: %w", workerAddr, err)
    }
    if parsed.Scheme == "" || parsed.Hostname() == "" {
        return "", fmt.Errorf("worker address %q has no scheme or host", workerAddr)
    }
    ...
}
```

Three things the extraction bought:

- **The edge cases became visible.** `url.Parse` accepts almost anything, including `""`,
  so a nil error never meant the address was usable. That guard is obvious in a function
  whose whole job is building a URL, and invisible in the middle of a health check.
- **The comment had somewhere to live.** Why the worker's port is dropped is the part
  that kept getting lost; on a named function it sits at the top.
- **The regression got pinned.** A test asserts the exact broken string
  (`http://localhost:3001:32768/health`) can never come back.

Good candidates, in rough priority:

1. **String and URL construction** — `healthCheckURL`, `workerTasksURL`.
2. **Struct-to-struct merges** — `mergeTaskUpdate`. A field-copy block silently forgetting
   a field is a classic bug; a test pins the list.
3. **Branching that picks an outcome** — `decideHealthAction`. Return an enum describing
   *what to do*, and let the caller do it. The `if`/`continue` chain becomes a `switch`,
   and every branch is one table row.
4. **Filters and predicates** over a slice or map.

Not everything belongs in `pure.go`. If a helper only makes sense next to one type, keep
it in that type's file. The file is a convenience, not a rule.

## Keep the error style

Pure functions have no `Op` of their own, so they return plain `fmt.Errorf` with `%w`.
The calling method wraps at the boundary, per
[`error-handling-and-logging.md`](error-handling-and-logging.md):

```go
url, err := healthCheckURL(workerAddr, hostPort, t.HealthCheck)
if err != nil {
    return E("manager.checkTaskHealth", fmt.Sprintf("building health check url for task %s", t.ID), err)
}
```

## What to test

Test **both** layers. The pure functions catch the arithmetic; the business logic catches
the wiring — a correct helper called with the wrong arguments still produces a bug.

### Pure functions

Table-driven, standard library only, no testify. Cover the happy path, each rejected
input, and any regression worth pinning:

```go
func TestHealthCheckURL(t *testing.T) {
    tests := []struct {
        name       string
        workerAddr string
        hostPort   string
        healthPath string
        want       string
        wantErr    bool
    }{
        {
            name:       "replaces the worker port with the container host port",
            workerAddr: "http://localhost:3001",
            hostPort:   "32768",
            healthPath: "/health",
            want:       "http://localhost:32768/health",
        },
        {
            name:       "address without a scheme is rejected",
            workerAddr: "localhost:3001",
            hostPort:   "32768",
            healthPath: "/health",
            wantErr:    true,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) { ... })
    }
}
```

Name each case after the behavior it describes, not `case1`/`case2` — the name is what
you read when it fails. Failure messages follow the standard Go form:
`got X, want Y`, with the inputs included.

Two habits worth keeping:

- **Pin real bugs by name.** `TestHealthCheckURLDoesNotDoubleThePort` asserts against the
  literal string the bug produced. A test named after the bug explains itself.
- **Assert what must *not* change.** `TestMergeTaskUpdatePreservesManagerOwnedFields`
  checks that a worker response cannot reset `RestartCount` — the worker doesn't track
  restarts and sends zero, so a careless merge would silently uncap the restart budget.
  For value-semantics helpers, also assert the arguments were not mutated.

### Business logic

Methods that compose pure functions and own state are testable without a network: build
the struct, seed its maps, call the method, assert on the state.

```go
m := &Manager{TaskDb: map[uuid.UUID]task.Task{id: persisted}, logger: slog.Default()}
m.someMethod(...)
if got := m.TaskDb[id].State; got != task.Running { ... }
```

When a method mixes decision and I/O, that is the signal to extract — `doHealthChecks`
became testable in exactly this way once `decideHealthAction` carried the branching.

### Framework plumbing

Test at the boundary or not at all. `internal/worker/api_test.go` uses `httptest` for
handlers; the `time.After` polling loops and Docker SDK calls are left to manual runs.
Do not build elaborate Docker fakes — if a bug needs one to reproduce, the logic it lives
in probably wants extracting instead.

## Running tests

```sh
go test ./...              # everything
go test ./internal/manager/ -v
gofmt -l .                 # must print nothing
```

`go build ./...` and `go vet ./...` should both be clean before you call a change done.
