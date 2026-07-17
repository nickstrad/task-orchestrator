# Refactoring the Orchestrator into a Coding-Agent Platform

## Purpose

This is a post-book reference for turning this Kubernetes-like learning project into a small platform for running coding agents in isolated environments.

It is not an implementation plan and intentionally contains no working code. The goal is to preserve the orchestration ideas learned in the book while showing where the model needs to change for agent workloads.

The target is deliberately modest:

- accept a coding task and repository reference;
- schedule it onto a compatible worker;
- create an isolated, disposable development environment;
- run a coding agent inside that environment;
- stream its output and observe its state;
- retain the useful result, such as a patch or branch;
- destroy the environment reliably.

This does not attempt to design a production multi-tenant service, a general Kubernetes replacement, or an agent framework that decides how an agent reasons.

## The central shift

The current system schedules a `Task`, starts one Docker container, observes it, and eventually removes it. A coding-agent platform still needs those orchestration primitives, but its main unit of work is better understood as three related resources:

1. **Agent run** — the user's requested coding job, its instructions, repository, outcome, and cancellation state.
2. **Sandbox** — the isolated environment in which the run executes.
3. **Process execution** — a command or interactive process inside a ready sandbox, such as repository setup, the agent itself, or a validation command.

A web server container can be modeled as “start and keep running.” A coding agent usually needs a richer sequence: prepare a workspace, inject narrowly scoped credentials, execute several commands, stream output, modify files, possibly pause for input, and export the result. Keeping these three lifecycles separate prevents the agent's state from being confused with the container's state.

## How the existing repository maps to the new model

| Current area | Concept it already teaches | Agent-platform direction |
| --- | --- | --- |
| `internal/manager` | Control plane, queue, desired work, worker assignment | Own agent runs and sandbox records; issue commands and reconcile observed state |
| `internal/scheduler` | Placement policy | Filter workers by capacity, runtime support, isolation profile, and workspace needs, then score candidates |
| `internal/worker` | Node agent and local execution loop | Reconcile sandboxes through a runtime interface and report status/capabilities |
| `internal/task/task.go` | Workload specification, events, and lifecycle | Split into runtime-neutral agent-run, sandbox, and process-execution models |
| `internal/task/docker.go` | Concrete container lifecycle | Move behind a sandbox runtime adapter |
| Manager and worker HTTP APIs | Control-plane-to-worker communication | Carry commands, stable IDs, desired state, and observed status without Docker-specific fields |

The existing manager → scheduler → worker shape is worth keeping. The pivot happens mostly at the workload model and at the worker's execution boundary.

## Docker, gVisor, and Kata are different layers

It is useful to separate the container manager from the isolation mechanism:

- **Docker Engine** can remain the API used by a worker to pull images and create, start, inspect, and delete containers.
- **The default Docker runtime (`runc`)** uses ordinary Linux container isolation and shares the host kernel.
- **gVisor (`runsc`)** is an OCI runtime with a userspace application kernel. It reduces direct exposure to the host kernel, but may have syscall compatibility or syscall-heavy performance costs.
- **Kata Containers** is OCI-compatible but realizes the OCI container inside a lightweight virtual machine, giving the workload a separate guest kernel through hardware virtualization. It generally has higher startup and resource overhead and stricter host requirements.

OCI compatibility is the common ground: the image, command, environment, mounts, and resource intent can retain standard container semantics even though the isolation implementation changes. Kata's current containerd integration is a shim v2 runtime; that shim translates container lifecycle requests into VM and in-guest-agent operations. The platform does not need to expose those VM details in its workload API.

Docker Engine can invoke alternative containerd shims or registered OCI runtimes. Therefore, a useful first architecture is one Docker-backed adapter with a runtime selection in the sandbox profile. The three initial profiles could be:

| Profile | Engine/runtime | Intended use | Main trade-off |
| --- | --- | --- | --- |
| `development` | Docker + `runc` | Trusted local experiments and fast feedback | Weakest boundary for untrusted code because the host kernel is shared |
| `sandboxed` | Docker + gVisor | Default coding-agent jobs if the toolchain is compatible | Stronger boundary with possible compatibility and I/O overhead |
| `vm-isolated` | Docker or containerd + Kata | Higher-risk, cross-tenant, or explicitly hardened jobs | Stronger VM boundary with more startup, memory, and operational cost |

The names above are platform policy names, not runtime binary names. A request should ask for an isolation outcome such as `sandboxed`, not know that the worker currently fulfills it with `runsc`.

Linux execution workers are the practical target. gVisor depends on Linux facilities, and Kata additionally needs usable hardware virtualization or a supported nested-virtualization setup. A macOS development machine may host the control plane or a Linux VM, but it is not equivalent to the eventual Linux worker environment.

## The main abstraction: `SandboxRuntime`

The worker should depend on a platform-owned interface rather than constructing `task.Docker` directly. Call the interface `SandboxRuntime`, `SandboxDriver`, or `Executor`; `SandboxRuntime` makes its purpose clearest.

The conceptual contract is:

| Operation | Responsibility |
| --- | --- |
| `Capabilities` | Report supported isolation, architectures, interactive execution, snapshots, and other features used during scheduling |
| `Prepare` | Make the requested image or root filesystem available; safe to repeat |
| `Create` | Create an isolated sandbox from a runtime-neutral specification and return a platform sandbox ID |
| `Start` | Start the sandbox and make it ready for process execution |
| `Exec` | Start a command inside an existing sandbox with stdin/stdout/stderr streaming and optional TTY support |
| `Inspect` | Return observed state, timestamps, exit information, and resource usage without exposing engine-specific response types |
| `Stop` | Request graceful termination, followed by forced termination after a deadline |
| `Delete` | Remove the sandbox and runtime-owned resources; safe to retry |
| `Logs` | Read sandbox or process output from a cursor so reconnecting clients do not require replaying everything |

This interface should describe lifecycle semantics, not mirror every Docker SDK method. Docker, direct containerd, a local fake, and a future remote sandbox service can all implement it.

Two details make the interface useful for coding agents:

- `Exec` is a first-class operation. Rebuilding a container for every command makes an agent session awkward and loses its evolving workspace state.
- Streaming and cancellation belong in the contract. Returning one final string is not sufficient for long-running or interactive agent sessions.

An execution handle can expose waiting, streaming, input, resize, and cancellation behavior without making the top-level runtime interface enormous. The exact Go method signatures are an implementation exercise.

### Runtime-neutral inputs and outputs

A `SandboxSpec` will likely need concepts such as:

- image reference and architecture;
- CPU, memory, disk, and process-count limits;
- isolation profile;
- non-root user and working directory;
- workspace attachment;
- environment variables and references to secrets;
- network policy;
- startup and overall deadlines;
- labels for ownership, run ID, and cleanup;
- read-only or writable filesystem areas.

It should not contain Moby port types, Docker container IDs, Docker host configuration, Kata annotations, or gVisor flags. Those belong inside adapters.

Likewise, return a platform-owned `SandboxID` and `SandboxStatus`. An adapter can persist the mapping from that ID to a Docker container ID, containerd task ID, or Kata VM/container identity.

### Capabilities, not runtime-name conditionals

A worker should advertise facts such as:

- available isolation profiles;
- CPU architecture and capacity;
- hardware virtualization availability;
- support for TTY and streaming exec;
- snapshot or suspend support, if later added;
- supported workspace attachment modes.

The scheduler filters against these capabilities. Avoid spreading checks such as “if runtime is Kata” through the manager, scheduler, and API. Runtime-specific behavior belongs in the adapter; platform policy belongs in the control plane.

## Separate the important lifecycles

The current `Task.State` combines scheduling and execution. The agent platform becomes easier to reason about if each resource has its own small state machine.

An agent run might move through:

`Queued → Assigned → Preparing → Running → Succeeded | Failed | Cancelled | TimedOut`

A sandbox might move through:

`Requested → Creating → Ready → Stopping → Stopped | Failed | Lost`

An individual process execution might move through:

`Pending → Running → Exited | Cancelled | TimedOut`

These are examples, not required names. The useful principle is that a failed validation command does not necessarily mean the sandbox failed, and deleting a sandbox should not erase the durable record of the agent run.

Store desired state separately from observed state. The manager expresses what should exist; the worker reports what actually exists. Reconciliation can then retry idempotent operations after a restart or network interruption.

## A basic coding-agent run

A small end-to-end flow can be thought of as follows:

1. The API accepts an agent request containing instructions, a repository reference, an image/toolchain, limits, and an isolation policy.
2. The manager stores the run and places it in the scheduling queue.
3. The scheduler selects a worker that supports the requested profile and has enough capacity.
4. The worker asks its `SandboxRuntime` to prepare, create, and start the sandbox.
5. A workspace component clones or restores the repository into the sandbox's writable workspace.
6. A credential component makes only the short-lived credentials required by that run available.
7. An agent driver builds the agent-specific invocation and starts it through `SandboxRuntime.Exec`.
8. Output and status events flow back to the manager; clients may reconnect using log cursors or event sequence numbers.
9. When the agent exits, optional validation commands run in the same workspace.
10. The platform exports a patch, commit, branch reference, or workspace snapshot before cleanup.
11. The worker stops and deletes the sandbox even when the run fails or is cancelled.

This flow suggests two supporting boundaries in addition to `SandboxRuntime`:

- **`AgentDriver`** translates an agent-independent request into the command, environment, and output interpretation needed by a specific coding agent. It should not create containers.
- **`WorkspaceProvider`** creates, attaches, snapshots, exports, and deletes workspaces. It should not know which coding agent edits them.

Keeping these separate lets the same agent run in different isolation profiles and lets different agents share the same sandbox machinery.

## Suggested refactor sequence after the book

This is a dependency order to consult during the exercise, not a prescription for file layout or implementation.

### 1. Preserve a baseline

Finish the book implementation and record the behavior that currently works: submit, schedule, start, inspect, stop, and remove a container. Keep one end-to-end example as a learning baseline.

### 2. Remove Docker concepts from the domain model

Replace leaked engine types and fields with platform concepts. In this repository, `network.PortSet`, `ContainerID`, and the direct construction of `task.Docker` show where the boundary currently leaks.

Do this before adding more runtimes. Otherwise each new backend will add another set of special cases to `Task` and `Worker`.

### 3. Introduce `SandboxRuntime` around current behavior

Wrap the existing Docker behavior in an adapter without changing scheduling or lifecycle behavior. The worker should receive the interface as a dependency. A small fake implementation can make worker state-machine exercises deterministic without starting containers.

At the end of this step, Docker is still the only real backend, but it is no longer part of the platform's domain language.

### 4. Add isolation profiles and worker capabilities

Carry an isolation profile from the request to the scheduler and worker. Have workers advertise only profiles that are actually configured and healthy. Reject or leave pending a run when no compatible worker exists rather than silently falling back to weaker isolation.

### 5. Split agent-run and sandbox state

Give the durable user request an identity independent from the disposable sandbox. Make commands and events idempotent enough that manager and worker restarts do not accidentally create duplicate sandboxes or lose terminal status.

### 6. Add process execution and streaming

Teach a ready sandbox to execute more than its image entrypoint. Model stdout and stderr, exit status, cancellation, timeouts, and optional stdin/TTY. Add bounded buffering and backpressure so an agent cannot exhaust manager memory by printing indefinitely.

### 7. Add workspace and result handling

Start with one writable workspace per run. Decide explicitly what survives sandbox deletion. A patch or pushed branch is usually a better durable result than retaining every stopped container.

Treat workspace size, file ownership, cleanup, and interrupted export as lifecycle concerns rather than incidental shell setup.

### 8. Add the first coding-agent driver

Choose one agent and one repository source. Keep its CLI arguments, authentication convention, and output parsing behind `AgentDriver`. The orchestration system should only understand common states and artifacts.

### 9. Run the same contract with gVisor

Configure a Linux worker with gVisor and map the `sandboxed` profile to it. Exercise the same runtime contract and a representative toolchain matrix. Expect to discover compatibility differences; they should appear as capability or validation results, not conditionals throughout the control plane.

### 10. Add Kata only when it teaches a new boundary

Kata is valuable when you want to study VM-backed isolation, stronger tenant boundaries, hardware virtualization, and OCI lifecycle translation through a containerd shim. It need not be part of the first pivot. If added, make it satisfy the same sandbox contract and add its host requirements to worker capabilities; do not replace the platform workload model with a VM-specific one.

### 11. Harden cleanup and recovery

Reconcile orphaned sandboxes, expire abandoned runs, account for disk and process limits, and make stop/delete safe to retry. A coding-agent platform that starts environments but cannot always reclaim them will eventually exhaust its workers.

## Security boundaries for coding agents

Coding agents routinely execute package scripts, compiler plugins, tests, and repository code. Treat the entire repository and everything it downloads as untrusted.

gVisor or Kata strengthens isolation, but neither one defines the complete security policy. At minimum, keep these concerns visible:

- never mount the Docker or containerd socket into an agent sandbox;
- do not mount arbitrary host paths or the orchestrator source tree;
- run as a non-root sandbox user where the toolchain permits it;
- drop Linux capabilities and avoid privileged containers;
- enforce CPU, memory, process, disk, and time limits;
- default network access to the narrowest policy compatible with the exercise;
- block cloud metadata and worker-management endpoints;
- use short-lived, run-scoped secrets and avoid placing long-lived tokens in images or logs;
- separate secret references stored by the manager from secret values delivered at execution time;
- make the base image read-only when practical and give the agent explicit writable locations;
- export results before deletion, then verify that workspaces, credentials, and runtime resources are removed;
- keep the worker host, runtime, kernel, and firmware patched;
- record who requested a run, which image and policy it used, and what artifacts it produced.

Network policy and secrets deserve special attention. An isolated agent that is allowed to read a powerful token and make arbitrary outbound requests can still exfiltrate that token without escaping its sandbox.

## Scheduling changes that matter for agents

Round-robin placement is a good starting exercise. Coding-agent scheduling later benefits from filtering and scoring on:

- requested isolation profile;
- available CPU, memory, disk, and process capacity;
- image architecture and cached images;
- workspace locality or snapshot availability;
- hardware virtualization for Kata;
- expected run duration and worker draining state;
- per-user or per-tenant concurrency limits.

Keep scheduling concerned with placement. Creating the sandbox remains the worker's responsibility, and interpreting agent output remains the agent driver's responsibility.

## Persistence and events

The in-memory maps and queues are appropriate while learning the basic mechanics. Once restart behavior becomes the lesson, persist at least:

- agent-run specification and desired state;
- assignment and sandbox identity mapping;
- observed state and terminal outcome;
- monotonically ordered events or log cursors;
- artifact references;
- cleanup deadline and last reconciliation time.

Events should be facts in the past tense, while commands express intent. For example, “start sandbox” is a command; “sandbox became ready” is an event. Do not rely on delivery exactly once. Give commands stable IDs and make handlers safe to repeat.

## Observability worth learning

Useful early measurements include:

- queue and scheduling latency;
- image preparation, sandbox startup, and workspace preparation time;
- agent execution duration and exit reason;
- CPU, memory, disk, and process usage per run;
- bytes of logs and artifacts;
- cancellation and timeout counts;
- sandbox creation failures by isolation profile;
- leaked or orphaned sandboxes found during reconciliation.

Correlate logs and events with agent-run ID, sandbox ID, execution ID, and worker ID. Keep runtime-native identifiers available for debugging but out of the public domain model.

## Validation matrix

Use the same behavioral contract for every enabled profile:

| Behavior | Docker/`runc` | gVisor | Kata |
| --- | --- | --- | --- |
| Prepare and start a basic image | Required | Required | Required if enabled |
| Execute multiple commands in one workspace | Required | Required | Required if enabled |
| Stream stdout and stderr | Required | Required | Required if enabled |
| Cancel and enforce a deadline | Required | Required | Required if enabled |
| Apply CPU, memory, process, and disk controls | Verify | Verify | Verify |
| Survive manager/worker retry without duplication | Verify | Verify | Verify |
| Export results before cleanup | Required | Required | Required if enabled |
| Remove all runtime resources | Required | Required | Required if enabled |
| Run representative Go, Git, shell, and package-manager operations | Baseline | Compatibility check | Compatibility check |

Also test negative cases: incompatible images, missing runtimes, failed image pulls, exhausted disk, lost workers, excessive output, cancellation during setup, failed artifact export, and repeated cleanup.

## Questions to revisit while refactoring

- Is a sandbox created per agent run, per repository, or reused across trusted runs?
- Which state is durable when a worker disappears?
- What is the result of a successful run: patch, commit, branch, snapshot, or several artifacts?
- Can users provide arbitrary images, or only platform-approved toolchain images?
- Which network destinations are required for source control, package registries, and model APIs?
- Does the agent receive model credentials directly, or call a platform proxy?
- What isolation profile is the minimum for untrusted repositories?
- What happens when an agent needs a syscall or filesystem feature unsupported by gVisor?
- Is weaker-runtime fallback forbidden, explicitly approved, or allowed only for trusted local work?
- How are stuck executions and orphaned sandboxes discovered and reclaimed?

Answering these questions gradually is part of the exercise; the first version does not need every feature.

## A reasonable first milestone

The smallest version that demonstrates the pivot is:

- one manager and one Linux worker;
- one coding-agent driver;
- one repository source;
- a Docker-backed `SandboxRuntime`;
- `development` and `sandboxed` profiles using `runc` and gVisor;
- a disposable workspace;
- streaming output, timeout, cancellation, patch export, and reliable deletion;
- no silent downgrade from gVisor to `runc`.

Kata, snapshots, warm pools, multi-tenancy, interactive approval, and distributed persistence can remain later learning topics.

## Further reading

- [Docker: Alternative container runtimes](https://docs.docker.com/engine/daemon/alternative-runtimes/) — how Docker Engine selects registered OCI runtimes and containerd shims, including gVisor and Kata.
- [gVisor: What is gVisor?](https://gvisor.dev/docs/) — the application-kernel model, OCI `runsc` runtime, and its isolation/performance trade-offs.
- [gVisor: Application compatibility](https://gvisor.dev/docs/user_guide/compatibility/) — why representative toolchain testing matters.
- [gVisor: Security model](https://gvisor.dev/docs/architecture_guide/security/) — the boundary gVisor does and does not provide.
- [Kata Containers architecture](https://github.com/kata-containers/kata-containers/blob/main/docs/design/architecture/README.md) — lightweight-VM isolation and the containerd shim v2 model.
- [Kata Containers documentation](https://katacontainers.io/docs/) — installation, limitations, and runtime integration guides.
