# Concurrency & State Conventions

How `internal/manager` and `internal/worker` guard their mutable state. Both packages
put every read and write behind helpers in a `state.go`; the helpers are shaped
differently in each, and the difference is structural rather than stylistic.

Two rules carry most of the weight:

1. **Never hold the mutex across I/O.**
2. **Nothing outside `state.go` touches guarded state directly.**

---

## What each package guards

| Package | Mutex | Guarded state | Concurrent goroutines |
| --- | --- | --- | --- |
| `manager` | `Manager.mu` | `TaskDb`, `EventDb`, `WorkerTaskMap`, `TaskWorkerMap`, `Pending` | `UpdateTasks`, `ProcessTasks`, `DoHealthChecks`, HTTP API |
| `worker` | `Worker.mu` | `Db`, `Queue`, `Stats` | `RunTasks`, `UpdateTasks`, `CollectStats`, HTTP API |
| `scheduler` | `RoundRobinScheduler.mu` | `LastWorker` | whichever goroutine calls `SendWork` |

Fields set once at construction and never written again — `WorkerNodes`, `Scheduler`,
`httpClient`, `logger`, the loop intervals — are not guarded. A value only ever read
needs no lock. Making any of them mutable means bringing it under the mutex.

`MarginalCostScheduler` has no row because it has no mutable state: it holds a name and a
logger, and its parallel scoring writes only into per-goroutine slice slots. Concurrency is
not the same thing as shared state, and only the second one needs a mutex.

**A component guards its own mutable state.** `Manager.mu` does not cover the scheduler's
cursor, because `Scheduler.Score` does network I/O and rule 1 then forbids the manager
from holding its lock across the call. See "Scheduling takes no lock" below.

`Queue` needs the lock as much as the stores do: `internal/queue` wraps
`collections/queue`, a plain linked list with no synchronisation of its own.

## The four categories

Lock scope is bounded by I/O, not by logic. Every helper in either `state.go` is one of
these four shapes.

| # | Shape | Manager | Worker |
| --- | --- | --- | --- |
| 1 | Pure read | `snapshotTasks`, `LookupTask`, `workerForTask` | `lookupTask`, `listTasks`, `queueLen`, `SnapshotStats`, `taskCount` |
| 2 | Whole-value write the caller owns | `putTask`, `enqueueEvent` | `putTask`, `enqueueTask`, `setStats` |
| 3 | Read-modify-write, **no** I/O between | `assignTask`, `applyWorkerReport`, `beginRestart`, `retireTask`, `nextPendingEvent` | `dequeueTask` |
| 4 | Read-modify-write **with** I/O between | — | `upsertTask` |

### 1. Pure read

RLock, copy out, return. Reads return copies: handing back the live slice out of
`WorkerTaskMap` would let the caller read that memory after the lock is gone.

These are snapshots, not views. `doHealthChecks` and `updateTasks` both iterate their own
slice and call back into the package while looping, which ranging the live state would
not allow.

### 2. Whole-value write

Lock, write, unlock. Safe only when the caller owns the whole record and no earlier
version can conflict — in practice, a task the store has never seen:

```go
// RunTask, recording a task pulled off the queue for the first time
taskPersisted = taskQueued
w.putTask(taskQueued)
```

Anything writing back a value it *read* earlier is category 3 or 4, not this.

### 3. Read-modify-write with no I/O

**One helper, one lock — not a get followed by a set.** Locking each field access
individually still lets two goroutines interleave between the get and the set, which is
how check-then-act bugs survive a mutex.

This is the manager's default, because all its state is pure bookkeeping. `assignTask`
writes three maps together: a caller updating them one helper at a time would leave
windows where a task is routed to a worker that does not list it. `beginRestart` reads
`RestartCount` and writes `RestartCount+1` in one critical section, which is what keeps
two concurrent restarts from both seeing the same count and burning only one of the
budget.

The worker has exactly one: `dequeueTask`. `Dequeue` folds "is it empty" into its bool
return, which is what makes it expressible as a single locked call.

> Do not check `queueLen()` and then dequeue. Two goroutines can both pass the length
> check and race for the same task.

### 4. Read-modify-write with I/O in the middle

**The worker-only category, and the reason the manager's approach does not port over.**

Every worker state change is the consequence of a Docker call:

```
read the task  ->  start/stop/inspect a container  ->  write the result
```

Rule 1 forbids holding the lock across that middle step — a Docker image pull can take a
minute, and a lock spanning one stalls the API, the update loop, and the stats loop for
its whole duration. So the lock *cannot* cover the read and the write together, and
category 3 is not available.

Small per-access helpers are not enough either. They make each access atomic, which stops
the data race, but not the lost update:

```
updateTasks reads task X          {State: Running, ContainerID: abc}
updateTasks calls Inspect         ... two seconds of I/O ...
RunTask stops X, writes           {State: Completed, FinishTime: now}
updateTasks writes its copy back  {State: Running, FinishTime: zero}
```

Every access there is locked and `-race` reports nothing, but X is Running again with no
container behind it. The next pass marks it Failed and the manager restarts a task the
operator asked to stop.

The cause is that the write carries *every* field, not just the ones that changed.
`upsertTask` fixes it by moving the mutation inside the lock: it re-reads the task, hands
the callback a pointer to that fresh copy, and writes back what the callback touched.

```go
hostPorts := resp.Container.Container.NetworkSettings.Ports

w.upsertTask(t, func(p *task.Task) {
    if p.State != task.Running {
        return          // someone stopped it during the Inspect — leave it alone
    }
    p.HostPorts = hostPorts
    if exited {
        p.State = task.Failed
    }
})
```

The Docker call still happens outside the lock; only the read-apply-write is atomic.

Two constraints on the callback:

- **Touch only fields the caller owns.** `updateTasks` owns `HostPorts`; it does not own
  `State`, so it re-checks before writing one. `StartTask` owns `StartTime`,
  `ContainerID`, `HostPorts`, and `State` — it just created the container — but not
  `FinishTime`.
- **No I/O, and no calls back into the package.** The lock is held; either deadlocks.

`upsertTask` falls back to the caller's copy when the task is not in the store, which
makes it an upsert rather than an update: `RunTask` persists a new task and `StartTask`
immediately writes to it, and a failure of the first must not silently discard the second.

The manager's `applyWorkerReport` is the same idea in category-3 form — it merges a
worker's report into the persisted copy via the pure `mergeTaskUpdate` rather than
overwriting. The worker uses a callback instead because each call site owns a different
field set.

## The `Locked` suffix

`sync.RWMutex` is not reentrant. A helper that takes the lock again from inside
`upsertTask` — which already holds it for writing — deadlocks the whole worker. Shared
bodies therefore come in pairs:

```go
func (w *Worker) lookupTask(id uuid.UUID) (task.Task, bool) {
    w.mu.RLock()
    defer w.mu.RUnlock()
    return w.lookupTaskLocked(id)
}
```

**If a method has the `Locked` suffix, the caller already holds the lock. If it does not,
it takes one.** No exceptions — that is the only thing keeping the pairs readable.

## `RWMutex`, not `Mutex`

Most goroutines touching this state are readers: the API's GET handlers, `updateTasks`'
initial listing, the stats endpoint. A plain `Mutex` serialises them against each other
for no benefit. Write paths are unaffected.

## Scheduling takes no lock

`Manager.pickWorker` is the one state helper that acquires nothing, and that is
deliberate. `Scheduler.Score` does network I/O — `MarginalCostScheduler` fetches live
stats from every candidate node — so rule 1 applies with full force. Holding `m.mu` across
it would stall the HTTP API, the update fan-out, the health check loop, and every enqueue
for as long as the slowest node took to answer.

Nothing in `pickWorker` needs the manager's lock: `Scheduler` and `WorkerNodes` are
write-once, `SelectCandidateNodes` and `Pick` are pure, and the only genuinely mutable
scheduler state — `RoundRobinScheduler.LastWorker` — is guarded by the scheduler's own
mutex.

> Push the lock down to the state that needs it. A cursor that is only safe under its
> caller's lock forces that caller to hold the lock across whatever else the callee does —
> here, a network call. Guarding it where it lives removes the constraint entirely.

### Scoring fans out, and needs no lock to do it

`MarginalCostScheduler.Score` runs one goroutine per candidate. Each writes its own slot
in two pre-sized slices and nothing else:

```go
results := make([]ScoredNode, len(candidates))
scored := make([]bool, len(candidates))

for i, n := range candidates {
    wg.Go(func() {
        ...
        results[i] = ScoredNode{Node: n, Score: score}
        scored[i] = true
    })
}
```

**This is the one place a lock is genuinely unnecessary, and the reason is worth being
precise about.** Distinct slice indices are distinct memory addresses; the slice header is
never written and the backing array never reallocates, so the writes cannot interfere.

A map keyed by node name looks like the same "each goroutine owns its own key" argument
and is **not** safe. A Go map shares `count`, its bucket array, and its growth/rehash
across every key, so two goroutines inserting different keys still write shared state —
and the runtime detects it and calls `fatal error: concurrent map writes`, a throw that
`recover` cannot catch. Unique keys buy nothing; unique indices buy everything.

> A natural partition removes the need for a lock only when the writes land on disjoint
> memory *and* the container has no shared mutable metadata. Slices qualify. Maps do not.

The results are compacted into one `[]ScoredNode` after `wg.Wait()`, single-threaded, in
candidate order — so ties break the same way on every run rather than by goroutine
completion order.

Errors travel on a channel buffered to `len(candidates)`, so no goroutine can block on a
send. Nothing drains it until `wg.Wait()` returns, so a smaller buffer deadlocks: the
senders that do not fit park forever, and `Wait` never returns to start the drain.

### Partial failure, and why `ScoredNode` exists

`Score` tolerates unreachable nodes: they are logged and omitted, and the pass fails only
when *no* candidate could be scored. One dead worker must not stop the cluster scheduling.

That makes the result shorter than its input, which is why a node travels **with** its
score:

```go
type ScoredNode struct {
    Node  node.Node
    Score float64
}
```

The earlier shape was a `map[string]float64` keyed by name plus the candidate slice, joined
by `Pick`. It had a live bug. An unreachable node had no entry, a missing key reads as
`0.0`, and these scores are *costs* where lowest wins — so every node the scheduler failed
to reach beat every healthy one, and the task landed on the most broken worker available.

Pairing the two makes that state unrepresentable rather than merely documented: if a node
is in the slice it has a score, and if it has no score it is not in the slice. `Pick` takes
that slice directly, so there is no second list to keep in step and no absent-key case for
either scoring helper to define.

> When a value and its qualifier can drift apart, the bug is the join, not the join's
> callers. Store them together and the invalid combination stops existing.

## Reading through the store interface

`store.Store[T]` is generic, so reads come back already typed: `Get` returns a `task.Task`
and `List` returns a `[]task.Task`. There are no type assertions and no "unexpected value
in task db" branches — the compiler rejects a mismatched write at the call site instead.
This is the same reasoning as `internal/queue`; see the comment at the top of
`internal/queue/queue.go` for the panics the untyped version caused.

A missing key returns `store.ErrNotFound` alongside the **zero value** of `T`, not `nil`,
so a caller that ignores the error gets an empty struct rather than a panic. Check the
error anyway — it is the only thing distinguishing "absent" from "present and zero".

Values go in and come out **by value**. Go copies a struct on assignment, so nothing a
caller holds aliases what the store holds. A pointer-based store would have to copy
explicitly at both boundaries and would share state silently the moment one implementation
forgot.

`store.InMemory[T]` does no locking of its own. Both packages have invariants spanning
more than one store call, so they lock around it; a store that locked internally would
make them pay twice and still not make a read-modify-write atomic.

## State helpers mint their own op

Every helper in either `state.go` builds its own op string (`"worker.upsertTask"`,
`"manager.beginRestart"`). Callers do not pass one in. A caller-supplied op is an
unverifiable literal at every call site that goes stale silently when the caller is
renamed — and since most of these helpers log rather than return, the caller's op never
reached anyone anyway.

## Testing

Assertions are secondary here — the value is in what `go test -race` reports. A test that
does not run the helpers *concurrently* proves nothing, so each package has one that
drives every helper from goroutines standing in for the real loops
(`TestWorkerStateIsRaceFree`, and the manager's update fan-out test).

The same rule applies to the scheduler: every test in `marginalcost_test.go` uses several
nodes on purpose, because a single-candidate `Score` spawns one goroutine and exercises
none of the fan-out. `TestScoreIsRaceFreeAcrossManyNodes` is the one that would catch a
regression to a shared map or a shared `append`.

Lost updates need their own test, because `-race` cannot see them:
`TestUpsertTaskDoesNotResurrectAStoppedTask` pins the sequence above.

Two failure modes here are invisible to `-race` and need tests of their own:

- **Deadlock** shows up as the test binary's timeout, not a failure.
  `TestScoreDoesNotDeadlockWhenEveryNodeFails` runs more failing nodes than any plausible
  fixed buffer, so an under-sized error channel hangs instead of passing.
- **Choosing the wrong node** is a correct-looking program computing the wrong answer.
  `TestPickNeverChoosesAnUnreachableNode` covers the bug where an unscored node won on an
  absent zero cost.

When adding a helper, check it fails with the locking removed. A race test that passes
either way is not testing anything. `RoundRobinScheduler.mu` was verified this way:
commenting it out makes the manager's fan-out test report a data race on `LastWorker`.

## Known gaps

- `RunTask` reads a task, validates the state transition, then calls `StartTask` — a
  check-then-act spanning Docker I/O. Safe today only because `RunTasks` is a single
  loop; nothing in the type system says so. Making `RunTasks` a pool needs a per-task
  lock or a claimed flag in the store first.
- The manager commits an assignment before the HTTP send and compensates with
  `unassignTask` if the worker turns out to be unreachable. That window is deliberate —
  the alternative is holding the lock across the send — but it is a window.
- Round-robin's `Score` mutates `LastWorker` on a path named like a read. Anything else
  that gives a scheduler memory across calls has to guard it there too, or `pickWorker`
  goes back to needing a lock across network I/O.
