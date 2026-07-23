# task-orchestrator

A task orchestrator written in **Go**, in the spirit of
[Kubernetes](https://kubernetes.io/), [Nomad](https://www.nomadproject.io/), and
[Apache Mesos](https://mesos.apache.org/): a **manager** schedules tasks across a
pool of **worker** nodes, each worker runs its tasks as Docker containers and
reports health and stats back, and the manager reconciles the cluster toward the
desired state. A single `orchestrator` binary runs the manager, runs a worker,
and drives tasks from the command line.

## Architecture

```
             orchestrator task start -f task.json
                          │  (HTTP)
                          ▼
                 ┌──────────────────┐
                 │     Manager      │   control plane
                 │  ├ HTTP API      │   accepts tasks
                 │  ├ Scheduler     │   picks a worker (round-robin / marginal-cost)
                 │  ├ Health checks │   polls workers
                 │  └ Reconcile     │   drives tasks toward desired state
                 └──────────────────┘
                    │            │  (HTTP)
             ┌──────┘            └──────┐
             ▼                          ▼
      ┌────────────┐             ┌────────────┐
      │  Worker A  │             │  Worker B  │   run tasks, report stats
      │  HTTP API  │             │  HTTP API  │
      └────────────┘             └────────────┘
             │                          │
             ▼                          ▼
        Docker daemon             Docker daemon      containers run here
```

- **Manager** (`internal/manager`) — an HTTP server that accepts task events,
  schedules each task onto a worker, and runs background loops that poll worker
  health and reconcile task state.
- **Scheduler** (`internal/scheduler`) — pluggable strategies: `round-robin` and
  `marginal-cost` (scores workers by resource fit).
- **Worker** (`internal/worker`) — an HTTP server that runs assigned tasks as
  Docker containers and periodically reports memory/CPU/disk/task-count stats.
- **Task runtime** (`internal/task`) — starts, stops, and inspects containers
  through the Docker Engine API.
- **Store** (`internal/store`) — task state behind a generic store, either
  in-memory or a persistent [bbolt](https://github.com/etcd-io/bbolt) file.

The manager and worker both run in the foreground, log to stderr, and shut down
cleanly on Ctrl-C.

## Requirements

- **Go 1.26+** to build.
- **A running Docker daemon.** Workers run tasks as Docker containers via the
  Docker Engine API, so the daemon must be running and reachable (e.g. Docker
  Desktop, or `dockerd`) wherever a worker runs. Without it, workers start but
  cannot run tasks.

## Installation

```sh
# clone and build
git clone https://github.com/nickstrad/task-orchestrator.git
cd task-orchestrator
go build -o orchestrator ./cmd/orchestrator

# or install onto your PATH
go install github.com/nickstrad/task-orchestrator/cmd/orchestrator@latest
```

This produces an `orchestrator` binary. Run `orchestrator --help` (or
`orchestrator <command> --help`) for full flag documentation.

## Running it

Make sure the Docker daemon is running first. A typical local cluster is three
terminals:

```sh
# 1. a worker, listening on 5556
orchestrator worker --name worker-1 --port 5556

# 2. a manager, pointed at that worker
orchestrator manager --port 5555 --worker worker-1=http://localhost:5556

# 3. submit a task and watch it schedule
orchestrator task start -f examples/task-event.json
```

Add more workers by starting more `worker` processes and passing another
`--worker name=address` to the manager:

```sh
orchestrator manager --port 5555 \
  --worker worker-1=http://localhost:5556 \
  --worker worker-2=http://localhost:5557
```

Pass `--log-level debug|info|warn|error` to any command to control log
verbosity.

## CLI usage

The client subcommands are one-shot HTTP calls against a running manager (or a
worker directly). `--host`/`--port` point them at the target; `task start`,
`stop`, and `status` are also exposed as the top-level shortcuts `run`, `stop`,
and `status`.

```sh
# Submit a task from a JSON or YAML file (or stdin with -f -)
orchestrator task start -f examples/task-event.json
orchestrator task start -f examples/task-event.yaml --port 5555
cat task.json | orchestrator task start -f -
orchestrator task start -f examples/task-event.json --dry-run   # print, don't send

# List every task the manager knows about and its state
orchestrator task status --port 5555

# Stop a running task by id (printed when it was submitted)
orchestrator task stop 4b1e9f0c-2a1e-4c3b-9d5f-6a7b8c9d0e1f

# Read a worker's collected stats (target the worker, not the manager)
orchestrator stats --port 5556
```

A task event is a small JSON/YAML document. Field names are matched
case-insensitively and omitted IDs are filled with fresh UUIDs. States are
integers: `0=Pending 1=Scheduled 2=Running 3=Completed 4=Failed`.

```json
{
  "State": 2,
  "Task": {
    "Name": "echo-server",
    "State": 1,
    "Image": "timboring/echo-server:latest",
    "ExposedPorts": { "7777/tcp": {} },
    "HealthCheck": "/health"
  }
}
```

See [`examples/`](examples/) for ready-to-run task events.

### Scheduling and persistence

```sh
# Round-robin scheduling instead of the default marginal-cost
orchestrator manager --scheduler round-robin --worker worker-1=http://localhost:5556

# Keep manager/worker task state in a bbolt file across restarts
orchestrator manager --db-type PERSISTENT --worker worker-1=http://localhost:5556
orchestrator worker --name worker-1 --db-type PERSISTENT --fresh-start=false
```

## Built with

| Library | Role |
| --- | --- |
| [`moby/moby` client & api](https://github.com/moby/moby) | Docker Engine API — workers run tasks as containers |
| [`spf13/cobra`](https://github.com/spf13/cobra) | CLI command tree and flag parsing |
| [`go-chi/chi`](https://github.com/go-chi/chi) | HTTP routing for the manager and worker APIs |
| [`etcd-io/bbolt`](https://github.com/etcd-io/bbolt) | Embedded key/value store for the persistent backend |
| [`shirou/gopsutil`](https://github.com/shirou/gopsutil) | Worker host metrics (memory, CPU, disk) |
| [`google/uuid`](https://github.com/google/uuid) | Task and worker identifiers |
| [`gopkg.in/yaml.v3`](https://gopkg.in/yaml.v3) | YAML task-event parsing |

## Documentation

Developer documentation lives in [`docs/`](docs/) — start at
[`docs/index.md`](docs/index.md).
```
