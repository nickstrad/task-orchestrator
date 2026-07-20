# Documentation Index

`task-orchestrator` is a **learning project** — a small Kubernetes-like orchestrator that
schedules Docker containers across in-process workers. This index describes every
document here and what it is for. Start with the conventions, then the reference
material.

## Conventions — how the code works today

Follow these when writing code.

| Document | What it covers |
| --- | --- |
| [`error-handling-and-logging.md`](error-handling-and-logging.md) | **Read before writing any error or log line.** Per-package error types (`TaskError`/`WorkerError`/`ManagerError`), the `E`-at-the-boundary / `Wrap`-above-it rule, one-log-per-error, `slog` child loggers and attribute keys, and the `internal/httpapi` helpers for HTTP responses. |
| [`code-structure-and-testing.md`](code-structure-and-testing.md) | How to split pure functions and business logic out of framework plumbing, when a buried block is worth extracting, and what to test at each layer. Table-driven test conventions and the `go build` / `go vet` / `gofmt` checks a change is expected to pass. |
| [`concurrency-and-state.md`](concurrency-and-state.md) | **Read before touching either `state.go`, or the scheduler.** Why both packages put all mutable state behind helpers, the four helper categories and which package uses which, the `upsertTask` pattern for read-modify-writes with I/O in the middle, the `Locked` suffix convention, and how to race-test a new helper. Also covers scheduling: why `pickWorker` takes no lock, why per-index slice writes need no mutex when map writes do, and why a node is paired with its score. |

## Reference — where the project could go next

Forward-looking material in [`reference/`](reference/). These describe systems that **do
not exist in this repo**. They are thinking tools, not specs — don't implement from them
unless asked.

| Document | What it covers |
| --- | --- |
| [`reference/coding-agent-platform-refactor-reference.md`](reference/coding-agent-platform-refactor-reference.md) | A post-book sketch of turning this orchestrator into a small platform for running coding agents in isolated environments — accepting a task plus a repo reference, scheduling it onto a compatible worker, and where the current model would need to change. Contains no working code. |
| [`reference/cloud-coding-sandbox-environment-reference.md`](reference/cloud-coding-sandbox-environment-reference.md) | How a cloud coding-agent platform can build a usable, isolated dev environment given only a repo, a commit or branch, a task, and Kubernetes workers using Kata Containers. |
| [`reference/agentic-platform-scheduling-reference.md`](reference/agentic-platform-scheduling-reference.md) | Scheduling for an agent platform, in depth: resource vectors, feasibility filters vs. scoring plugins, spread-vs-pack, multidimensional fragmentation, headroom for bursty agents, warm-cache affinity, tenant fairness, reservations and reconciliation. Ends with a ten-milestone roadmap from round robin to a coding-agent thin slice, plus a reading order (Borg, Kubernetes, DRF, Omega, Mesos). Roadmap, not a spec. |

## Adding a document

Put it in `docs/` if it describes this codebase, or `docs/reference/` if it describes
something that does not exist yet — then add a row to the right table above. See
[`AGENTS.md`](../AGENTS.md) at the repo root for the working guidelines.
