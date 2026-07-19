# Change summary — main (no worktree; user chose to work directly on main)

Plan: docs/error-handling-and-logging-plan.md — **deleted in this commit at the user's
request.** Its rules live on in `docs/error-handling-and-logging.md`; use that as the
spec when reviewing this change.

All changes are uncommitted in the working tree.

## What was implemented

**1. `internal/httpapi` (new)** — `ErrorResponse` DTO plus `WriteError`, which sets the
status and writes a body whose `Code` equals that status, so the two cannot disagree.
`WriteJSON` was added as the success-path twin (see `/simplify` below).

**2. `internal/task`** — new `errors.go` with `TaskError` + `E`/`Wrap`. Deleted the old
`TaskError`/`WrapError` (and its unused `Misc` map). `DockerResult.Error` is now a plain
`error`. All six `WrapError` sites became `E("task.Docker.Run|.Stop", ...)` with the cause
in `Err` rather than `%v`-formatted into the message. `Inspect` returns a `TaskError`
instead of a bare SDK error. `NewDocker` now returns `(*Docker, error)` instead of
discarding the `client.New` error. Both `log.Printf` calls are gone — `task` is a silent
library.

**3. `internal/worker`** — new `errors.go` with `WorkerError`. Deleted the broken
`WorkerError`/`WrapError` (it had no `Error()` method). `Worker` and `API` carry a
`*slog.Logger`; `NewWorker`/`NewAPI`/`NewStats` take one. `RunTask` uses `State.String()`
instead of `%d`. `StartTask`/`StopTask` wrap and return; `RunTasks` is the terminal
consumer and logs once. `api.go` uses `httpapi`, fixing three status/body mismatches
(400/400, 400/400, 404/404 — all previously claimed `Code: 500`), and the 204 now writes
no body. `stats.go` logs via the worker logger; the copy-pasted "mem stats" message on the
`load.Avg` branch is fixed.

**4. `internal/scheduler`, `internal/node`, `internal/queue`** — no changes, per the plan.

**5. `internal/manager`** — new `errors.go` with `ManagerError`. Deleted
`manager.ErrorResponse` and with it the `worker` import. `SelectWorker`/`checkTaskHealth`
return `ManagerError`s and no longer log before returning. `SendWork`/`restartTask` use
log-and-replace at the HTTP boundary. Three flow bugs the plan called out are fixed:
`updateTasks` no longer logs a nil `err` on non-200, one unknown task no longer aborts
updates for every remaining worker (`return` → `continue`), and the copy-pasted
"not running or failed" skip message now says restart-max-reached.

**6. `main.go`** — root logger + `SetDefault`, with `mainLogger`/`mLogger`/`wLogger`
children carrying `component`/`workerID`/`addr`. All `fmt.Printf` identity prefixes are
gone (which fixes the "listenining" typo). Dropped the line that printed `resp.Body`, a
reader. The dead `valueStream` mechanism is kept and commented, per the plan.

**Tests (new)**: `errors_test.go` in all three packages (`errors.Is` through the chain to
`io.EOF`, `errors.As` extraction, `E` populates `Stack` / `Wrap` does not) and
`internal/worker/api_test.go` (httptest: garbage POST → 400 status *and* `{"code":400}`
body; DELETE unknown → 404/404; DELETE existing → 204 with an empty body).

**Docs (requested mid-task, beyond the plan)**: added `docs/index.md` (index describing
every doc), `docs/error-handling-and-logging.md` (the conventions distilled from the
plan), and `docs/reference/` — the two `*-reference.md` docs moved there. Root `CLAUDE.md`
and `AGENTS.md` were already a symlink pair; the direction was flipped so `AGENTS.md` is
the real file, and it now points at `docs/`.

## What /simplify changed

Four review agents ran; these were applied:

- **`worker.StopTask` logged "container stopped and removed" at Info on the failure
  path**, then returned the error for `RunTasks` to log again — a false success line plus
  the exact double-log the plan set out to kill. Now returns early, matching `StartTask`.
- **Removed the `ErrNoTasks` sentinel and the three-arm switch in `RunTasks`.** The
  `errors.Is(ErrNoTasks)` branch was unreachable — `RunTask` is only called inside
  `if w.Queue.Len() != 0`. Collapsed to a plain `if result.Error != nil`. (I had added the
  sentinel to honour the plan's "log the empty queue at Debug"; the branch cannot fire, so
  it went.)
- **`NewDockerResult`'s `if err != nil { d.Error = err }` guard** is vestigial now that
  `Error` is a plain `error` — replaced with a direct struct literal.
- **Added `httpapi.WriteJSON`** and routed the five success paths through it. The
  status-plus-JSON sequence `WriteError` was extracted to own was still hand-rolled on
  every success branch, with bare `200`/`201` literals.

## Notes for review

**Not done — needs a decision.** Three of the four agents independently flagged that
**`Stack` is write-only**: `E()` captures `debug.Stack()` at all ~32 origins, but nothing
reads it. `Error()` omits it and no log site emits it, so the plan's headline feature
("stack captured once at origin, traceable end-to-end") is currently inert and costs an
allocation per error. The plan's optional `LogValue()` nicety would fix it. I did not
pick — surfacing stacks or dropping the field is a design call, and this is a learning
repo where that choice is the point. Recorded under "Known gaps" in the conventions doc.

**Deliberate deviations from the plan:**
- Error types are `TaskError`/`WorkerError`/`ManagerError`, not a bare `Error` in each
  package — you asked for the package name in the type name mid-implementation.
- `httpapi.WriteJSON` is not in the plan; it came out of the `/simplify` reuse pass.

**Left alone deliberately** (flagged by agents, out of scope for an error/logging plan —
all pre-existing):
- `task.NewDocker` builds a fresh Docker client per operation, including once per task per
  15s tick in `updateTasks`, and none are closed. Hoisting the client onto `Worker` is a
  real fix but an architectural change.
- `resp.Body` is never `Close`d anywhere in `manager.go`; `updateTasks`/`doHealthChecks`
  make sequential HTTP calls with no timeout.
- `SendWork` and `restartTask` duplicate the whole worker-POST protocol, and they resolve
  the worker address two different ways (`getWorkerUrl` vs `WorkerNameToAddress`).
- `task` still writes Docker passthrough output to stdout/stderr — the plan's verification
  step anticipates this.
- `Wrap` is unused in `task` and `manager` (each sits at one end of the chain). Kept for a
  uniform API across the three packages.

**Verification run:** `go build ./...`, `go vet ./...`, and `go test ./...` all pass.
`gofmt -l .` is clean. The plan's step-3 runtime check (`go run .` against a live Docker
daemon, confirming a healthy task reaches Running and `/healthfail` restarts) was **not
run** — it needs a Docker daemon and a live observation of the logs.
