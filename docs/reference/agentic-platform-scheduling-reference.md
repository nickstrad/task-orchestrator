# Scheduling for Agentic Execution Platforms

**Status: reference and project roadmap.**

This document is a guide for evolving a basic Go orchestrator into a scheduler for
background agents, coding agents, and isolated execution sandboxes.

It is not a production specification. It is a map of the scheduling concepts, papers,
algorithms, and project milestones that become useful after implementing basic task
placement.

The central use case is:

> A user submits an agent task, the platform creates or selects an isolated environment,
> the agent performs unpredictable work, and the scheduler must place that environment
> safely and efficiently.

The execution environment might be:

- A Docker container.
- A gVisor sandbox.
- A Kata Containers workload.
- A Firecracker microVM.
- A long-lived development environment.
- A short-lived task sandbox restored from a snapshot.

---

## Why scheduling agent workloads is different

Agent workloads resemble batch jobs, but they have several unusual properties.

They are often:

- Bursty.
- Difficult to predict.
- Long-running relative to normal API requests.
- Capable of spawning many subprocesses.
- Heavy on disk and network activity.
- Stateful because they clone repositories, install dependencies, and generate artifacts.
- Latency-sensitive during startup.
- Potentially untrusted.
- Expensive because they consume both compute and model tokens.
- Uneven, with some tasks completing in seconds and others running for hours.

A coding agent may begin as a small process and later run:

- A compiler.
- A test suite.
- A development server.
- A database.
- A browser.
- Several language servers.
- Multiple agent subprocesses.

For this reason, maximizing density is not always the best scheduling objective.
Preserving headroom and limiting correlated failures may be more important.

---

## The current scheduler shape

A useful scheduler interface separates placement into three stages:

```go
type Scheduler interface {
    SelectCandidateNodes(
        task task.Task,
        nodes []node.Node,
    ) []node.Node

    Score(
        task task.Task,
        nodes []node.Node,
    ) map[string]float64

    Pick(
        scores map[string]float64,
        candidates []node.Node,
    ) node.Node
}
```

These stages represent:

1. **Feasibility**

   Remove nodes that cannot or should not run the task.

2. **Scoring**

   Rank the remaining nodes according to scheduling policy.

3. **Selection**

   Pick a destination and begin the reservation or launch process.

This is a useful abstraction because new scheduling policies can usually be introduced
without changing task execution.

Round robin can remain one implementation while later policies add resource awareness,
affinity, fairness, cost, and fragmentation handling.

---

## The scheduling decision

The core question is not simply:

> Which node has the most free CPU?

It is:

> Which eligible execution environment can run this task while preserving safety,
> responsiveness, future capacity, and tenant fairness?

A production placement decision may consider:

- CPU.
- Memory.
- Local disk.
- Network bandwidth.
- GPU availability.
- Supported sandbox runtime.
- Architecture, such as ARM64 or x86-64.
- Required kernel or virtualization features.
- Repository or dependency cache availability.
- Snapshot availability.
- Tenant isolation.
- Region or data locality.
- Node health.
- Current node pressure.
- Task priority.
- Expected task duration.
- Estimated startup cost.
- Cost per unit of compute.
- Failure-domain distribution.

Not every project needs every dimension. The scheduler should start small and gain
dimensions only when observed behavior justifies them.

---

## Resource vectors

Round robin treats tasks and nodes as interchangeable. Real schedulers operate on
resource vectors.

```go
type Resources struct {
    CPUMillis        int64
    MemoryBytes      int64
    EphemeralDisk    int64
    NetworkMbps      int64
    GPUCount         int64
}
```

A node exposes capacity and current state:

```go
type NodeResources struct {
    Capacity    Resources
    Reserved    Resources
    Allocated   Resources
    Observed    Resources
}
```

A task provides a request and possibly a limit:

```go
type TaskResources struct {
    Requests Resources
    Limits   Resources
}
```

The distinction matters:

- **Capacity** is what the machine physically provides.
- **Reserved** is capacity withheld for the operating system, runtime, failover, or safety.
- **Allocated** is capacity promised to scheduled tasks.
- **Observed** is what workloads currently consume.
- **Request** is what the scheduler reserves for placement.
- **Limit** is the maximum the runtime allows a workload to consume.

Agent platforms should not schedule directly from observed usage alone. A quiet task
may suddenly compile a large project or start a browser.

---

## Feasibility filters

Feasibility determines whether a task can run on a node at all.

A basic resource filter is:

```go
func Fits(task TaskResources, node NodeResources) bool {
    available := node.Capacity.
        Sub(node.Reserved).
        Sub(node.Allocated)

    return task.Requests.LessThanOrEqual(available)
}
```

Real filters may include:

### Resource fit

Reject nodes without sufficient requested CPU, memory, disk, network, or GPU capacity.

### Runtime support

A task may require:

- Docker.
- gVisor.
- Kata Containers.
- Firecracker.
- KVM.
- A particular kernel feature.
- Nested virtualization.
- A specific CPU architecture.

### Security policy

A high-risk task may require stronger isolation than an ordinary trusted build.

For example:

```text
trusted internal task      → container
untrusted code execution   → gVisor or Kata
high-risk tenant workload  → microVM
```

### Locality requirements

A task may need:

- A repository mirror.
- A cached base image.
- A prebuilt workspace snapshot.
- A mounted volume.
- A regional secret or credential.
- Proximity to an artifact store.

### Health and pressure

Reject nodes that are:

- Not ready.
- Draining.
- Under memory pressure.
- Under disk pressure.
- Experiencing runtime failures.
- Behind on heartbeats.
- Scheduled for maintenance.

### Tenant isolation

The system may prohibit two tasks from the same tenant, repository, or security class
from sharing a host.

Feasibility should answer only whether placement is allowed. Preferences belong in
scoring.

---

## Scoring policies

After filtering, the scheduler ranks feasible nodes.

A useful design is to compose several scoring plugins:

```go
type ScorePlugin interface {
    Name() string
    Score(task Task, node Node) float64
    Weight() float64
}
```

The combined score can be:

```go
func CombinedScore(task Task, node Node, plugins []ScorePlugin) float64 {
    var total float64

    for _, plugin := range plugins {
        total += plugin.Score(task, node) * plugin.Weight()
    }

    return total
}
```

This allows the platform to add policies independently.

Example plugins:

- Resource balance.
- Headroom preservation.
- Best-fit packing.
- Repository cache affinity.
- Snapshot affinity.
- Tenant spreading.
- Zone spreading.
- Node reliability.
- Startup latency.
- Infrastructure cost.
- Carbon-aware placement.
- Task priority.

Scores should be normalized to comparable ranges before weights are applied.

---

## Spread versus pack

The first major policy tradeoff is whether to spread workloads or pack them.

### Spread or worst-fit behavior

Prefer the least-utilized node.

Benefits:

- Leaves headroom around active agents.
- Reduces the effect of workload spikes.
- Distributes noisy workloads.
- Reduces correlated task failure on one machine.
- Helps when resource estimates are unreliable.

Costs:

- Leaves every machine partially occupied.
- Increases active-host count.
- Can create resource fragmentation.
- May reduce the ability to place large future tasks.
- Can increase infrastructure cost.

Spread is attractive early in an agent platform because agent resource use is highly
unpredictable.

### Pack or best-fit behavior

Prefer the node that will have the least remaining capacity after placement.

Benefits:

- Consolidates workloads.
- Preserves empty machines for future large tasks.
- Can lower infrastructure cost.
- Makes scale-down easier.
- Can reduce fragmentation in some workload distributions.

Costs:

- Leaves less burst headroom.
- Increases noisy-neighbor risk.
- Amplifies the effect of a host failure.
- May overload a resource dimension that the model underestimates.

An agent scheduler will usually need a blend rather than a pure spread or pure pack
policy.

One practical approach is:

```text
normal pressure   → prefer spread and headroom
high cluster load → gradually increase packing weight
large task        → preserve or seek contiguous capacity
warm cache hit    → allow affinity to override a small resource-score difference
```

---

## Multidimensional fragmentation

A cluster can have enough aggregate free resources while being unable to place a task.

Example:

```text
Node A: 8 CPU free, 2 GB memory free
Node B: 2 CPU free, 16 GB memory free

Task:   4 CPU, 8 GB memory
```

The cluster has 10 free CPUs and 18 GB of free memory, but the task fits nowhere.

This is multidimensional fragmentation.

Agent platforms are especially vulnerable because task shapes vary considerably:

- Compilation-heavy tasks need CPU and memory.
- Browser tasks need memory and shared memory.
- Large repositories need disk.
- Integration tests may require CPU, memory, network, and several services.
- Local model inference may require large memory or GPU capacity.

Useful fragmentation metrics include:

- Percentage of tasks rejected despite sufficient aggregate capacity.
- Largest task shape that can currently fit.
- Free-capacity vector per node.
- Distribution of task request shapes.
- Capacity stranded in each dimension.
- Number of nodes that could fit common task profiles.
- Pending tasks grouped by resource bottleneck.

The goal is not to eliminate fragmentation. The goal is to understand and control it.

---

## E-PVM as a learning milestone

E-PVM is useful as an early multidimensional scoring exercise.

The broad idea is to:

1. Normalize resource usage by node capacity.
2. Represent utilization as a multidimensional vector.
3. Calculate the cost of the node before placement.
4. Calculate the cost after hypothetical placement.
5. Score the task by the marginal cost increase.

Conceptually:

```go
func Score(task Resources, node NodeResources) float64 {
    before := Normalize(node.Allocated, node.Capacity)
    after := Normalize(node.Allocated.Add(task), node.Capacity)

    return Cost(after) - Cost(before)
}
```

The exact cost function is less important than learning how different choices affect
behavior.

Possible cost functions include:

- Sum of normalized utilization.
- Sum of squared utilization.
- Maximum normalized utilization.
- Weighted combinations.
- Penalties when headroom falls below a threshold.

A squared cost function penalizes highly utilized dimensions more strongly:

```text
cost = cpu² + memory² + disk²
```

That tends to favor balanced utilization rather than placing a task on a node that is
already near exhaustion in one dimension.

Implementing this behind the existing `Score` method is a natural next step after round
robin.

---

## Headroom for unpredictable agents

Traditional schedulers often assume resource requests are reasonably accurate. Agent
tasks make this assumption weaker.

A task may request 2 GB and later start a compiler, browser, database, and test runner.

The scheduler can protect itself with explicit headroom policies.

### Static reserve

Reserve a fixed percentage of each node:

```text
schedulable CPU    = physical CPU × 0.85
schedulable memory = physical memory × 0.80
```

### Per-task burst reserve

Inflate each task request:

```text
effective memory request = declared request × 1.25
```

### Workload-class reserve

Use different multipliers:

```text
simple command task      → 1.1×
repository build         → 1.3×
browser automation       → 1.5×
unknown external project → 1.7×
```

### Pressure-sensitive scoring

Allow placement but sharply penalize nodes above thresholds:

```text
memory utilization > 70% → moderate penalty
memory utilization > 80% → large penalty
memory utilization > 90% → infeasible
```

### Historical estimation

Use previous runs to estimate future requests:

```text
repository + task class + runtime → expected resource envelope
```

Historical estimation should be advisory at first. Automatically shrinking requests
based on past low usage can create instability.

---

## Warm-state and cache affinity

Agent startup cost is often dominated by environment preparation rather than scheduling.

Examples:

- Cloning a large repository.
- Downloading dependencies.
- Pulling container images.
- Restoring a microVM snapshot.
- Starting language servers.
- Building indexes.
- Installing toolchains.
- Restoring package caches.

This means the cheapest node by resource score may not provide the fastest task start.

The scheduler can model warm-state affinity:

```go
type WarmState struct {
    RepositoryCached bool
    ImageCached      bool
    SnapshotCached   bool
    ToolchainCached  bool
}
```

A cache-affinity score may reward:

- Existing repository clone.
- Matching commit or nearby Git object database.
- Existing package-manager cache.
- Matching sandbox snapshot.
- Matching base image.
- Matching language toolchain.

Affinity should not override hard resource or security constraints.

A practical rule is:

> Prefer a warm node when its resource score is close enough to the best cold node.

For example:

```text
Select warm node when:
warm_score >= best_score - affinity_tolerance
```

This prevents a cache hit from placing work onto a dangerously loaded machine.

---

## Long-lived environments versus task sandboxes

The platform may schedule two related but distinct objects.

### Long-lived environment

A reusable workspace associated with a user, repository, or project.

Characteristics:

- Expensive to create.
- Contains cached dependencies and repository state.
- May remain paused or stopped.
- Benefits from snapshotting.
- May own persistent storage.
- Can execute multiple tasks over time.

### Task sandbox

An isolated execution attempt for one requested agent task.

Characteristics:

- Usually tied to one branch, worktree, or pull request.
- Has a shorter lifecycle.
- Should have explicit resource and time limits.
- May be cloned from a long-lived environment or snapshot.
- Should be disposable after artifacts are persisted.

A useful architecture is:

```text
environment scheduler
    decides where reusable workspace state lives

task scheduler
    decides where each isolated execution attempt runs
```

These can initially be one scheduler, but the distinction becomes useful as the system
gains snapshots, persistent volumes, or workspace reuse.

---

## Tenant fairness

Without fairness controls, one organization can consume every available sandbox slot.

The first useful fairness mechanism is usually simpler than a sophisticated cluster
algorithm.

Track limits such as:

```go
type TenantLimits struct {
    MaxConcurrentTasks int
    MaxCPUMillis       int64
    MaxMemoryBytes     int64
    MaxDailyRuntime    time.Duration
}
```

Admission can then reject or queue tasks that exceed tenant limits.

### Weighted fair queuing

Tenants receive scheduling opportunities according to configured weights.

Example:

```text
Tenant A weight: 4
Tenant B weight: 2
Tenant C weight: 1
```

This does not necessarily reserve fixed machines. It controls how queued tasks are
selected when capacity becomes available.

### Dominant Resource Fairness

Dominant Resource Fairness becomes relevant when tenants consume different resource
mixtures.

One tenant may dominate CPU while another dominates memory. DRF compares each tenant by
the largest share it consumes of any constrained resource.

For an early agent platform, DRF is a later milestone. Start with:

1. Per-tenant concurrent-task limits.
2. Per-tenant queueing.
3. Weighted round robin across tenant queues.
4. Resource-aware fairness only after actual contention appears.

---

## Priority and preemption

Not all agent tasks are equally important.

Potential priority classes:

```text
interactive user task
pull-request validation
background indexing
scheduled maintenance
speculative agent exploration
```

A simple model is:

```go
type PriorityClass int

const (
    PriorityBackground PriorityClass = iota
    PriorityNormal
    PriorityInteractive
    PriorityCritical
)
```

Priority can affect:

- Queue order.
- Scheduling weight.
- Maximum waiting time.
- Which tasks may be preempted.
- Resource guarantees.
- Timeout policy.

Preemption is dangerous because agent tasks may hold important state.

Before terminating a task, the platform may need to:

- Persist logs.
- Save patches.
- Upload artifacts.
- Create a checkpoint.
- Record agent state.
- Release repository locks.
- Mark the attempt as interrupted rather than failed.

Do not implement preemption until task lifecycle and cleanup are reliable.

---

## Queueing and backpressure

When no node can accept a task, the system needs an explicit queueing policy.

Useful task states include:

```text
submitted
queued
admitted
assigned
provisioning
running
checkpointing
completed
failed
cancelled
timed_out
preempted
```

A task should not remain ambiguously “pending.”

Queueing policies may include:

- FIFO.
- Priority FIFO.
- Weighted round robin across tenants.
- Shortest estimated task first.
- Deadline-aware ordering.
- Aging to prevent starvation.

Backpressure may occur at several levels:

- Reject new tasks immediately.
- Queue tasks up to a tenant limit.
- Limit queue size.
- Limit concurrent environment creation.
- Limit image pulls.
- Limit snapshot restores.
- Limit repository clones.
- Limit model-token expenditure.
- Slow admission when the control plane or storage system is unhealthy.

The scheduler should distinguish:

```text
cannot run now
cannot run anywhere
not allowed by quota
invalid task specification
platform unhealthy
```

These conditions require different user-facing errors and retry behavior.

---

## Reservations and race conditions

A scheduling decision is based on state that may change before launch completes.

Example:

1. Scheduler A sees 8 GB free.
2. Scheduler B sees the same 8 GB free.
3. Both schedule 6 GB tasks.
4. The node is overcommitted.

The scheduler therefore needs a reservation step.

A simplified flow:

```text
read state
→ filter
→ score
→ select
→ atomically reserve capacity
→ launch sandbox
→ confirm running
```

If launch fails, the reservation must be released.

An in-memory single-scheduler implementation can protect this with a mutex. A
distributed control plane may later need:

- Optimistic concurrency.
- Compare-and-swap updates.
- Versioned node records.
- Leases.
- Transactional state.
- Idempotent launch operations.
- Reconciliation after partial failures.

Omega is useful when studying shared-state schedulers with multiple scheduling loops,
but it should come after a correct single-scheduler implementation.

---

## Reconciliation

The scheduler makes desired-state decisions. Reality changes afterward.

Examples:

- A node disappears.
- A sandbox fails to start.
- A task exits without reporting completion.
- A reservation is left behind.
- Observed usage exceeds its limit.
- A node is drained.
- A microVM process survives after the control-plane request times out.
- A task finishes but its state remains `running`.

A reconciler periodically compares desired and observed state:

```text
desired task: running on node-4
observed task: absent
action: retry, fail, or reschedule
```

Scheduling and reconciliation should be separate concepts.

The scheduler decides placement. The reconciler repairs drift.

A small orchestrator should introduce reconciliation before attempting distributed or
parallel scheduling.

---

## Failure domains

Placing every task from one tenant or repository on one machine creates correlated risk.

Useful failure-domain labels include:

```go
type NodeLabels struct {
    Region string
    Zone   string
    Rack   string
    Host   string
    Pool   string
}
```

Anti-affinity policies may spread:

- Tasks belonging to the same organization.
- Replicated services.
- Critical control-plane components.
- Multiple attempts of the same task.
- Snapshot replicas.
- Repository cache replicas.

For an early single-region project, host-level spreading is enough. Zone-aware policies
become useful after the system truly spans zones.

---

## Runtime-aware scheduling

Different isolation technologies have different overhead and capabilities.

A node pool may support one or more runtimes:

```text
container pool
gVisor pool
Kata pool
Firecracker pool
GPU pool
ARM64 pool
x86-64 pool
```

A task specification can declare its needs:

```go
type IsolationLevel string

const (
    IsolationContainer IsolationLevel = "container"
    IsolationSandboxed IsolationLevel = "sandboxed"
    IsolationMicroVM   IsolationLevel = "microvm"
)
```

A policy layer maps task risk and requirements to a runtime:

```text
trusted first-party build → container
untrusted repository      → gVisor or Kata
high-risk execution       → Firecracker microVM
hardware-specific build   → matching architecture pool
```

The scheduler then filters nodes by runtime capability.

This keeps security policy separate from the scoring algorithm.

---

## Cost-aware scheduling

Agent tasks consume several kinds of cost:

- VM or bare-metal compute.
- Persistent and ephemeral storage.
- Network transfer.
- Snapshot storage.
- Container registry transfer.
- Model tokens.
- External API usage.
- Idle warm environments.

The scheduler can eventually include infrastructure cost:

```text
placement score =
    resource balance
  + warm-state benefit
  + reliability benefit
  - infrastructure cost
  - fragmentation penalty
```

Do not add cost optimization before resource and lifecycle correctness.

The first useful cost control is often:

- Idle timeout.
- Maximum task duration.
- Maximum concurrent tasks.
- Scale-down of empty workers.
- Reuse of safe cached artifacts.
- Explicit model-token budgets.

---

## Autoscaling interaction

The scheduler and autoscaler solve different problems.

The scheduler asks:

> Where should this task run?

The autoscaler asks:

> Does the cluster need more or fewer workers?

A task may be pending because:

- The cluster lacks total capacity.
- Capacity exists but is fragmented.
- The correct runtime pool has no capacity.
- A required architecture is unavailable.
- A tenant has exhausted its limit.
- Nodes are unhealthy.
- A large image or snapshot is still being prepared.

The autoscaler should not add generic workers for every pending task.

Useful signals include:

- Pending tasks grouped by required runtime.
- Pending tasks grouped by resource shape.
- Time spent pending.
- Number of tasks that cannot fit on any existing node.
- Number of tasks blocked only by quota.
- Number of tasks blocked by affinity requirements.
- Utilization of each worker pool.
- Scale-up and provisioning latency.

Eventually, the scheduler may simulate whether a hypothetical new node type could place
the pending workload.

---

## Observability for scheduling

A scheduler that cannot explain its decisions will be difficult to operate.

Each placement attempt should record:

```go
type SchedulingDecision struct {
    TaskID          string
    CandidateNodes  []string
    FilterFailures  map[string][]string
    Scores          map[string]map[string]float64
    SelectedNode    string
    DecisionTime    time.Duration
    Attempt         int
}
```

Useful metrics include:

- Scheduling latency.
- Queue wait time.
- Feasible-node count.
- Filter rejection count by reason.
- Score distribution.
- Reservation conflicts.
- Launch failures.
- Pending tasks by reason.
- Node utilization by resource.
- Task-request versus observed-usage error.
- Cache-affinity hit rate.
- Snapshot-restore hit rate.
- Tasks rejected by tenant quota.
- Unplaceable tasks despite aggregate capacity.
- Preemption count.
- Rescheduling count.

Useful traces include:

```text
task submitted
→ quota checked
→ scheduling started
→ nodes filtered
→ nodes scored
→ reservation created
→ sandbox provisioning started
→ agent process started
```

This is directly aligned with an observability-heavy platform engineering background and
would make the scheduler project more realistic.

---

## Suggested Go architecture

A practical package layout might be:

```text
internal/
├── scheduler/
│   ├── scheduler.go
│   ├── round_robin.go
│   ├── resource_score.go
│   ├── filters.go
│   ├── plugins.go
│   └── decision.go
├── queue/
│   ├── queue.go
│   ├── fifo.go
│   └── fair_queue.go
├── quota/
│   ├── quota.go
│   └── admission.go
├── reservation/
│   ├── reservation.go
│   └── store.go
├── reconciler/
│   └── reconciler.go
├── runtime/
│   ├── runtime.go
│   ├── docker.go
│   ├── gvisor.go
│   └── firecracker.go
├── node/
│   ├── node.go
│   └── heartbeat.go
└── task/
    ├── task.go
    └── state.go
```

Keep the interfaces small.

Example scheduler dependencies:

```go
type Scheduler struct {
    Filters []FilterPlugin
    Scores  []ScorePlugin
    Nodes   NodeStore
    Reserve ReservationStore
}
```

Do not begin with a generic plugin framework unless at least two real policies require
it. Plain Go interfaces and explicit composition are enough.

---

## Project milestones

### Milestone 1: resource-aware placement

Goal:

> Replace round robin with a scheduler that rejects nodes without enough CPU, memory,
> and disk, then selects among feasible nodes.

Implement:

- Resource request model.
- Node capacity model.
- Feasibility filters.
- Simple least-allocated score.
- Scheduling decision logs.
- Deterministic unit tests.

Do not add distributed state yet.

### Milestone 2: observe fragmentation

Goal:

> Demonstrate that sufficient aggregate capacity does not guarantee that a task can be
> placed.

Create a simulation that:

1. Adds nodes with fixed CPU and memory.
2. Schedules many differently shaped small tasks.
3. Attempts to place a large task.
4. Reports cluster-wide free resources.
5. Reports why the large task fits nowhere.

Compare:

- Round robin.
- Pure spread.
- Pure pack.
- Multidimensional scoring.

### Milestone 3: E-PVM-inspired scoring

Goal:

> Use marginal multidimensional cost rather than a single-resource heuristic.

Implement:

- Normalized resource vectors.
- Configurable resource weights.
- Marginal-cost scoring.
- Tests covering CPU-heavy and memory-heavy task mixes.
- Metrics for stranded capacity.

### Milestone 4: headroom policy

Goal:

> Keep agent workloads from exhausting nodes when actual usage exceeds requests.

Implement one or more:

- Static node reserve.
- Request inflation.
- Workload-class multipliers.
- Pressure thresholds.
- Hard memory feasibility limit.

Simulate agents that burst after launch.

### Milestone 5: queueing and tenant limits

Goal:

> Prevent one tenant from consuming the entire cluster.

Implement:

- Per-tenant concurrent-task limit.
- Per-tenant queues.
- Weighted round robin between tenant queues.
- Explicit pending reasons.
- Queue aging to reduce starvation.

Avoid DRF until this simpler model becomes inadequate.

### Milestone 6: runtime-aware placement

Goal:

> Schedule tasks according to their required isolation level.

Add:

- Runtime capability labels.
- Isolation requirements.
- Separate worker pools.
- Filters for Docker, gVisor, Kata, or Firecracker support.
- Tests proving incompatible nodes are rejected.

This is a natural point to refactor the book orchestrator toward agent-task execution.

### Milestone 7: warm-state affinity

Goal:

> Reduce startup latency without sacrificing resource safety.

Add scoring for:

- Cached repository.
- Cached container image.
- Matching workspace snapshot.
- Installed toolchain.
- Package cache.

Measure:

- Time from submission to agent start.
- Cache-hit rate.
- Resource-score tradeoff.
- Cold-start versus warm-start latency.

### Milestone 8: reservations and reconciliation

Goal:

> Make placement robust against concurrent decisions and failed launches.

Implement:

- Atomic reservation.
- Idempotent launch request.
- Reservation rollback.
- Node heartbeats.
- Reconciliation loop.
- Orphan sandbox cleanup.
- Retry policy.
- Task attempt records.

Complete this before parallel schedulers.

### Milestone 9: agent-task thin slice

Goal:

> Submit a coding task and produce a pull request using the scheduler.

Flow:

```text
API or Slack task
→ tenant admission
→ queue
→ scheduler
→ runtime selection
→ sandbox launch
→ repository checkout
→ agent execution
→ tests
→ patch persisted
→ pull request created
→ sandbox cleanup
```

The scheduler should expose enough decision data to explain:

- Why the task was queued.
- Why a runtime was chosen.
- Which nodes were rejected.
- Why the selected node won.
- Whether warm state was used.

### Milestone 10: multiple scheduling loops

Goal:

> Explore scheduler scalability only after a single control loop is correct.

Study and experiment with:

- Optimistic concurrency.
- Versioned cluster state.
- Reservation conflicts.
- Separate schedulers by workload class.
- Separate queues by runtime pool.
- Shared-state scheduling.

Omega belongs here, not near the beginning.

---

## Recommended reading order

### 1. Borg

**Large-scale cluster management at Google with Borg**

Focus on:

- Feasibility checks.
- Scoring.
- Resource requests.
- Admission.
- Priority.
- Preemption.
- Cell-level scheduling.
- Operational tradeoffs.

This is the most directly useful paper for the current scheduler interface.

### 2. Kubernetes scheduler documentation and source

Study:

- Filter plugins.
- Score plugins.
- Scheduling profiles.
- Node affinity.
- Pod topology spread.
- Taints and tolerations.
- Resource requests.
- Scheduling queues.
- Framework extension points.

The Kubernetes scheduler framework provides a modern example of how a pluggable
feasibility-and-scoring pipeline is organized.

Do not try to reproduce all of Kubernetes.

### 3. Dominant Resource Fairness

**Dominant Resource Fairness: Fair Allocation of Multiple Resource Types**

Read after implementing basic tenant queues.

Use it to understand:

- Multi-resource fairness.
- Dominant shares.
- Strategy resistance.
- Why simple per-resource quotas can behave poorly.

### 4. Omega

**Omega: Flexible, Scalable Schedulers for Large Compute Clusters**

Read after implementing:

- Reservations.
- Reconciliation.
- Versioned state.
- At least one correct single scheduler.

Its main value is shared-state scheduling and optimistic concurrency, not basic
placement math.

### 5. Mesos

**Mesos: A Platform for Fine-Grained Resource Sharing in the Data Center**

Useful for understanding:

- Two-level scheduling.
- Resource offers.
- Framework-specific schedulers.
- Separation between cluster resource management and workload policy.

This becomes relevant if different agent classes eventually need different schedulers.

### 6. Sparrow

**Sparrow: Distributed, Low Latency Scheduling**

Useful later for:

- Very high task-arrival rates.
- Low-latency distributed scheduling.
- Randomized sampling.
- Short-lived tasks.

It is less relevant while agent tasks are relatively expensive and long-running.

### 7. Firmament

**Firmament: Fast, Centralized Cluster Scheduling at Scale**

Useful for:

- Modeling placement as an optimization problem.
- Min-cost flow.
- Rich policy combinations.
- Understanding the gap between heuristic and optimization-based schedulers.

Treat this as an advanced milestone rather than an implementation target.

---

## Books and longer references

### Handbook of Scheduling: Algorithms, Models, and Performance Analysis  
**Joseph Y-T. Leung**

Use as a lookup reference rather than a cover-to-cover book.

Relevant when exploring:

- Online scheduling.
- Multiprocessor scheduling.
- Approximation algorithms.
- Deadline scheduling.
- Bin packing.
- Scheduling under constraints.

### Scheduling: Theory, Algorithms, and Systems  
**Michael Pinedo**

A strong general scheduling reference.

Most applicable topics:

- Machine scheduling models.
- Priority rules.
- Batch scheduling.
- Parallel-machine scheduling.
- Stochastic scheduling.
- Performance objectives and tradeoffs.

It is more mathematical than agent-platform-specific, so read selected chapters.

### Kubernetes Scheduling  
**Technical documentation, scheduler framework material, and source**

For the agent platform goal, Kubernetes scheduler internals are often more actionable
than a generic cloud-computing textbook.

Study the implementation selectively:

- Queue.
- Cache.
- Scheduling cycle.
- Binding cycle.
- Framework plugins.
- Extenders and profiles.
- Failure handling.

### Site Reliability Engineering  
**Google**

Relevant chapters and concepts:

- Borg.
- Load balancing.
- Handling overload.
- Cascading failures.
- Distributed periodic scheduling.
- Practical capacity and reliability tradeoffs.

### The Datacenter as a Computer  
**Luiz André Barroso, Urs Hölzle, and Parthasarathy Ranganathan**

Use for the system-level context surrounding scheduling:

- Warehouse-scale resource management.
- Power and cost.
- Failure behavior.
- Utilization.
- Service-level objectives.
- Cluster-level design.

This helps explain why scheduler decisions matter beyond algorithm elegance.

### Designing Data-Intensive Applications  
**Martin Kleppmann**

Already-completed concepts from this book remain relevant to:

- Scheduler state.
- Replication.
- Consistency.
- Transactions.
- Leases.
- Event logs.
- Reconciliation.
- Idempotency.

It is not a scheduling book, but it supports the control-plane implementation.

---

## Topics to defer

These are valuable, but they should not distract from the first working agent platform.

Defer until real requirements appear:

- Real-time deadline scheduling.
- Full constraint solvers.
- Min-cost-flow scheduling.
- Reinforcement-learning schedulers.
- Carbon-aware global placement.
- Multi-region active-active scheduling.
- Gang scheduling.
- GPU topology optimization.
- Live migration.
- Complex preemption trees.
- Fully general hierarchical quota systems.
- Kubernetes-scale scheduler plugin frameworks.

The early platform needs a correct, explainable scheduler more than a mathematically
optimal one.

---

## Recommended implementation sequence

Use this order:

```text
round robin
→ resource feasibility
→ least-allocated scoring
→ fragmentation simulation
→ multidimensional marginal-cost scoring
→ headroom policy
→ tenant queues and limits
→ runtime-aware placement
→ cache and snapshot affinity
→ atomic reservations
→ reconciliation
→ autoscaling signals
→ multiple scheduling loops
```

This sequence aligns the learning path with an actual agent-platform thin slice.

Each step introduces one new class of problem while preserving a working system.

---

## Definition of success for the first agent scheduler

The first meaningful scheduler does not need to rival Kubernetes or Borg.

It should be able to:

- Accept a task with CPU, memory, disk, and isolation requirements.
- Reject incompatible or unhealthy nodes.
- Explain every filter rejection.
- Score all feasible nodes.
- Preserve configurable node headroom.
- Reserve capacity atomically.
- Launch an isolated sandbox.
- Recover from a failed launch.
- Queue tasks when no capacity exists.
- Enforce a basic per-tenant concurrency limit.
- Emit metrics and traces for the entire scheduling decision.
- Reconcile orphaned or missing workloads.
- Run a coding agent that produces a persisted patch or pull request.

That is a strong project boundary for evolving a book orchestrator into the foundation
of a background-agent execution platform.
