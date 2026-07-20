# Error Handling & Logging Conventions

How every package in this repo is expected to report failures and emit logs. If you are
adding code, follow this.

Two rules carry most of the weight:

1. **`E` at the boundary with the outside world, `Wrap` everywhere above it.**
2. **One log per error** — the function that stops propagating an error logs it.

---

## Errors

Each package owns its error type, named after the package, in that package's `errors.go`:

| Package            | Type            |
| ------------------ | --------------- |
| `internal/task`    | `TaskError`     |
| `internal/worker`  | `WorkerError`   |
| `internal/manager` | `ManagerError`  |

They are deliberately near-identical — about 30 lines each, differing only in the name.
A shared `errkit` package would exist for one line (`debug.Stack()`), and the duplication
keeps each package self-contained and its errors self-identifying.

```go
type TaskError struct {
    Op      string // e.g. "task.Docker.Run" — package.Type.Method
    Message string // user-friendly, no %v of the cause and no trailing newline
    Err     error  // wrapped cause (nil at an origin with no cause)
    Stack   string // captured ONLY at the origin
}
```

`Error()` renders `op: message: cause`, and `Unwrap()` makes `errors.Is` / `errors.As`
work through the whole chain.

### `E` vs `Wrap`

- **`E(op, message, err)` — origin.** The cause is `nil`, or it came from outside this
  codebase: the Docker SDK, `net/http`, `encoding/json`. Captures the stack.
- **`Wrap(op, message, err)` — everything above.** The error already carries a stack, so
  this does not capture a second one.

```go
// origin: the Docker SDK is outside our code
return NewDockerResult(E("task.Docker.Run", fmt.Sprintf("pulling image %s", img), err), ...)

// above it: task.TaskError already has the stack
result.Error = Wrap("worker.StartTask", fmt.Sprintf("starting task %s", t.ID), result.Error)
```

Put the cause in `Err`, never formatted into `Message` — `Error()` appends it for you.

### Wrap vs log-and-replace at a boundary

- **Your caller can act on it** → wrap and return. (`worker.StartTask` → `RunTask`.)
- **You are a top-of-goroutine loop, or the error crossed an HTTP process boundary** →
  log the underlying error once, then continue or mint a fresh error of your own
  package's type.

The HTTP case matters: a worker's Go error never crosses the wire, so the manager only
ever sees an `httpapi.ErrorResponse` DTO. It logs what the DTO said and mints a fresh
`ManagerError` rather than pretending to wrap an error it does not have.

## Logging

Stdlib `log/slog` only. **No `log.Printf`, no `fmt.Println` for diagnostics** — those
carry no identity, which is the problem this replaced.

`main.go` builds the root logger once and hands child loggers down through constructors:

```go
logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
wLogger := logger.With("component", workerName, "workerID", workerNum, "addr", workerAddr)
w := worker.NewWorker(workerName, workerNum, wLogger)
```

- Components that own an identity (`worker`, `manager`) hold a `logger` field and log.
- APIs reuse their owner's logger with `.With("subcomponent", "api")`.
- **`internal/task` is a silent library** — it only *returns* errors, never logs. Leaf
  libraries return; components log. (`task` does still stream Docker image-pull and
  container output straight to stdout/stderr; that is passthrough, not logging.)

Accept a logger, don't reach for a global. `slog.Default()` and package-level
`slog.Info` are back doors that undo the identity threading.

### Attribute keys

`component`, `subcomponent`, `addr`, `worker` (name), `taskID` (uuid), `containerID`,
`err`, `state`, `image`, `url`, `code` (HTTP status).

`worker` is the routing key — it is what the manager's maps are keyed by and what
identifies a worker across components. `main.go` and a worker's own child logger also
carry `workerID` (int), but that is a process-local index from the spawn loop, not a
shared identity; don't join on it.

### Levels

| Level   | Use for                                                              |
| ------- | -------------------------------------------------------------------- |
| `Debug` | periodic loop chatter — polling, sleeping, "checking status of tasks" |
| `Info`  | lifecycle — listening, task added, container started/stopped          |
| `Warn`  | recoverable or retried — health check failed, worker requeued         |
| `Error` | failed and dropped or gave up                                        |

## HTTP responses

Use [`internal/httpapi`](../internal/httpapi/httpapi.go) — never hand-roll the
status-plus-JSON dance. `ErrorResponse` is a wire format shared by the worker's server
and the manager's client, so it belongs to neither package.

```go
httpapi.WriteError(w, http.StatusBadRequest, "error unmarshalling body")  // body Code == 400, always
httpapi.WriteJSON(w, http.StatusCreated, taskEvent.Task)
```

`WriteError` sets the body's `Code` from the status it just wrote, so the two cannot
disagree — a mismatch the old hand-rolled code shipped several of. A `204` must write no
body at all: `w.WriteHeader(http.StatusNoContent)` and nothing else.

## Known gaps

- `Stack` is captured at every origin but nothing reads it — no log site emits it and
  `Error()` omits it. Either surface it at `Debug` (a `LogValue()` on each error type) or
  drop the field; it currently costs a `debug.Stack()` per error for no payoff.
- `Wrap` is unused in `task` and `manager` (both are always at one end of the chain).
  Kept so all three packages present the same API.
