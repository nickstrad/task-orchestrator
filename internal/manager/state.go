package manager

import (
	"errors"
	"slices"

	"github.com/google/uuid"
	"github.com/nickstrad/task-orchestrator/internal/node"
	"github.com/nickstrad/task-orchestrator/internal/store"
	"github.com/nickstrad/task-orchestrator/internal/task"
)

// This file owns every read and write of the Manager's mutable state. Nothing
// outside it may touch TaskDb, EventDb, WorkerTaskMap, TaskWorkerMap, Pending,
// or Scheduler directly.
//
// See docs/concurrency-and-state.md for the four helper categories and the
// rules behind them. Almost everything here is category 3 — one helper, one
// lock, the whole read-modify-write inside it — because manager state is pure
// bookkeeping with no I/O between the read and the write. internal/worker
// cannot do that; the doc explains why.
//
// The fields set once by NewManager and never written again — WorkerNodes,
// httpClient, logger, and the intervals — are not guarded, because a value that
// is only ever read needs no lock. Keep it that way: making any of them mutable
// means bringing it under m.mu.

// taskLocked reads one task for a caller that already holds m.mu, wrapping the
// store's error with this package's op so the caller only has to log it.
//
// A missing key comes back with store.ErrNotFound intact for callers that treat
// it as an expected miss.
func (m *Manager) taskLocked(op string, id uuid.UUID) (task.Task, error) {
	t, err := m.TaskDb.Get(id.String())
	if err != nil {
		return task.Task{}, Wrap(op, "reading task "+id.String(), err)
	}
	return t, nil
}

// putTask stores a task, overwriting any existing copy. (Category 2.) The
// error is returned rather than logged: the caller decides whether a lost
// write is fatal to its operation.
func (m *Manager) putTask(t task.Task) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.TaskDb.Put(t.ID.String(), t); err != nil {
		return Wrap("manager.putTask", "storing task "+t.ID.String(), err)
	}
	return nil
}

// snapshotTasks returns every task the manager knows about. (Category 1.) It
// is a snapshot, not a view — doHealthChecks calls back into the manager while
// iterating it.
func (m *Manager) snapshotTasks() ([]task.Task, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tasks, err := m.TaskDb.List()
	if err != nil {
		return nil, Wrap("manager.snapshotTasks", "listing tasks", err)
	}
	return tasks, nil
}

// LookupTask returns the manager's copy of one task. (Category 1.) It is the
// read the API needs: assignmentFor also requires a worker assignment, so it
// reports "not found" for a task that exists but is not routed anywhere.
func (m *Manager) LookupTask(id uuid.UUID) (task.Task, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	t, err := m.taskLocked("manager.LookupTask", id)
	if errors.Is(err, store.ErrNotFound) {
		return task.Task{}, false
	}
	if err != nil {
		m.logger.Error("reading task db failed", "err", err, "taskID", id)
		return task.Task{}, false
	}
	return t, true
}

// workerForTask returns the worker a task is currently assigned to. (Category 1.)
func (m *Manager) workerForTask(id uuid.UUID) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	worker, ok := m.TaskWorkerMap[id]
	return worker, ok
}

// assignmentFor returns the worker a task is assigned to together with the
// manager's copy of that task. (Category 1.) Both under a single lock, so the
// two cannot describe different moments in time.
func (m *Manager) assignmentFor(id uuid.UUID) (string, task.Task, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	worker, ok := m.TaskWorkerMap[id]
	if !ok {
		return "", task.Task{}, false
	}

	// The routing map says this task exists but the store disagrees — the two
	// are written under the same lock, so that is a real inconsistency, not a
	// race. Log it here: the caller only gets a bool and cannot report it.
	t, err := m.taskLocked("manager.assignmentFor", id)
	if err != nil {
		m.logger.Error("task routed to a worker but missing from task db",
			"err", err, "taskID", id, "worker", worker)
		return "", task.Task{}, false
	}
	return worker, t, true
}

// enqueueEvent puts an event on the pending queue. (Category 2.)
func (m *Manager) enqueueEvent(te task.TaskEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.Pending.Enqueue(te)
}

// nextPendingEvent takes the next event off the pending queue. (Category 3:
// the emptiness check and the dequeue are one lock, so two callers cannot both
// see a non-empty queue and race for the same item.)
func (m *Manager) nextPendingEvent() (task.TaskEvent, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.Pending.Dequeue()
}

// assignTask records that a task has been handed to a worker. (Category 3.)
// The three maps are written together: a caller that updated them one helper
// at a time would leave windows where a task is routed to a worker that does
// not list it, or listed by a worker it is not routed to.
func (m *Manager) assignTask(workerName string, te task.TaskEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// EventDb is an audit log, not routing state. A failed write must not stop
	// the assignment itself, so this logs and carries on.
	if err := m.EventDb.Put(te.ID.String(), te); err != nil {
		m.logger.Error("storing task event failed",
			"err", Wrap("manager.assignTask", "storing event "+te.ID.String(), err),
			"taskID", te.Task.ID, "worker", workerName)
	}

	m.WorkerTaskMap[workerName] = append(m.WorkerTaskMap[workerName], te.Task.ID)
	m.TaskWorkerMap[te.Task.ID] = workerName
}

// unassignTask undoes assignTask. It is the compensating write for a send that
// never landed: the assignment is committed before the HTTP call, so a worker
// that turns out to be unreachable leaves a task routed somewhere it was never
// delivered. Without this, the requeued event comes back to a task that already
// has an assignment and is dropped as a duplicate.
//
// The event stays in EventDb. It is a log of what the manager was asked to do,
// not routing state, and the retry reuses the same event ID.
func (m *Manager) unassignTask(workerName string, taskID uuid.UUID) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.TaskWorkerMap, taskID)

	assigned, ok := m.WorkerTaskMap[workerName]
	if !ok {
		return
	}
	if idx := slices.Index(assigned, taskID); idx >= 0 {
		m.WorkerTaskMap[workerName] = slices.Delete(assigned, idx, idx+1)
	}
}

// applyWorkerReport merges a worker's view of a task into the manager's copy,
// reporting whether the manager had ever heard of it. (Category 3: the lookup
// and the merge share one lock, so a report cannot be merged into a task that
// another goroutine retired in between.)
func (m *Manager) applyWorkerReport(reported task.Task) (known bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	reportedID := reported.ID.String()

	persisted, err := m.taskLocked("manager.applyWorkerReport", reported.ID)
	if errors.Is(err, store.ErrNotFound) {
		// The one expected miss: a task the manager never knew about. The
		// caller logs it, so this stays quiet to keep it at one log per error.
		return false
	}
	if err != nil {
		m.logger.Error("reading task for worker report failed",
			"err", err, "taskID", reported.ID)
		return false
	}

	merged := mergeTaskUpdate(persisted, reported)
	if err := m.TaskDb.Put(reportedID, merged); err != nil {
		// Known, but the merge was lost. Returning false here would make the
		// caller log "task not found", which is the wrong diagnosis entirely.
		m.logger.Error("persisting worker report failed",
			"err", Wrap("manager.applyWorkerReport", "storing task "+reportedID, err),
			"taskID", reported.ID, "state", reported.State)
	}
	return true
}

// beginRestart marks a task as scheduled for another attempt and returns the
// updated copy for the caller to send. (Category 3: reading RestartCount and
// writing RestartCount+1 in one critical section is what keeps two concurrent
// restarts from both seeing the same count and burning only one of the budget.)
func (m *Manager) beginRestart(uid uuid.UUID) (task.Task, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	id := uid.String()

	t, err := m.taskLocked("manager.beginRestart", uid)
	if err != nil {
		m.logger.Error("cannot restart task: reading task db failed",
			"err", err, "taskID", uid)
		return task.Task{}, false
	}

	t.State = task.Scheduled
	t.RestartCount++

	// The bump must be durable before the caller sends the restart. If the
	// write is lost the count never advances, and the restart budget that
	// stops a crash-looping task from being restarted forever never runs out.
	if err := m.TaskDb.Put(id, t); err != nil {
		m.logger.Error("cannot restart task: persisting restart count failed",
			"err", Wrap("manager.beginRestart", "storing task "+id, err),
			"taskID", uid, "restartCount", t.RestartCount)
		return task.Task{}, false
	}
	return t, true
}

// retireTask closes out a task that has been stopped on its worker. (Category 3.)
//
// The task stays in TaskDb marked Completed rather than being deleted: the
// worker keeps its own record and will keep reporting the task on every poll,
// so a manager that had forgotten it would log "task not found in task db"
// forever. Completed is terminal in the state machine, so mergeTaskUpdate will
// not walk it back.
//
// The routing maps do drop it, which is what takes the task out of scheduling
// and out of the health check loop.
func (m *Manager) retireTask(worker string, taskUID uuid.UUID) {
	m.mu.Lock()
	defer m.mu.Unlock()

	taskID := taskUID.String()

	// A task missing from the store is not an error here: retireTask also runs
	// for tasks the manager already dropped. Only the routing cleanup below is
	// load-bearing, so every failure in this block logs and falls through.
	t, err := m.taskLocked("manager.retireTask", taskUID)
	switch {
	case errors.Is(err, store.ErrNotFound):
		m.logger.Debug("retiring a task that is not in the task db",
			"taskID", taskUID, "worker", worker)
	case err != nil:
		m.logger.Error("reading task for retirement failed",
			"err", err, "taskID", taskUID, "worker", worker)
	default:
		t.State = task.Completed
		if err := m.TaskDb.Put(taskID, t); err != nil {
			// The task stays at its old state, so the next worker poll will
			// keep reporting it and mergeTaskUpdate will keep accepting it.
			m.logger.Error("marking task completed failed",
				"err", Wrap("manager.retireTask", "storing task "+taskID, err),
				"taskID", taskUID, "worker", worker)
		}
	}

	delete(m.TaskWorkerMap, taskUID)

	assigned, ok := m.WorkerTaskMap[worker]
	if !ok {
		return
	}
	if idx := slices.Index(assigned, taskUID); idx >= 0 {
		m.WorkerTaskMap[worker] = slices.Delete(assigned, idx, idx+1)
	}
}

// pickWorker runs the scheduler. It takes NO lock, which is the point.
//
// Scheduler.Score does network I/O: MarginalCostScheduler fetches live stats
// from every candidate node, one HTTP round trip each. Holding m.mu across that
// would break rule 1 — a wedged node would stall the API, the update fan-out,
// the health check loop, and every enqueue for as long as its request took, up
// to the full stats timeout per node.
//
// Nothing here needs m.mu. Scheduler and WorkerNodes are both set once by
// NewManager and never written again, SelectCandidateNodes and Pick are pure,
// and the one piece of genuinely mutable scheduler state —
// RoundRobinScheduler.LastWorker — is guarded by the scheduler's own mutex.
// That is the right home for it: a cursor that is only safe under the caller's
// lock forces the caller to hold that lock across Score, which is exactly the
// I/O this avoids.
func (m *Manager) pickWorker(t task.Task) (node.Node, bool) {
	candidates := m.Scheduler.SelectCandidateNodes(t, m.WorkerNodes)
	if len(candidates) == 0 {
		return node.Node{}, false
	}

	// Score may drop nodes it could not reach, so what comes back is not
	// necessarily one entry per candidate. Pick takes that result directly —
	// each node carries its own score, so there is no way to offer Pick a node
	// that was never scored.
	scored, err := m.Scheduler.Score(t, candidates)
	if err != nil {
		return node.Node{}, false
	}
	if len(scored) == 0 {
		return node.Node{}, false
	}
	return m.Scheduler.Pick(scored), true
}
