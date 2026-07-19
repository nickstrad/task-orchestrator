package manager

import (
	"slices"

	"github.com/google/uuid"
	"github.com/nickstrad/task-orchestrator/internal/node"
	"github.com/nickstrad/task-orchestrator/internal/task"
)

// This file owns every read and write of the Manager's mutable state. Nothing
// outside it may touch TaskDb, EventDb, WorkerTaskMap, TaskWorkerMap, Pending,
// or Scheduler directly — main.go runs UpdateTasks, ProcessTasks, and
// DoHealthChecks as three concurrent goroutines, and all three reach that state.
//
// Three rules make the locking work, and all three are structural rather than
// something a call site has to remember:
//
//  1. Never hold m.mu across I/O. Every helper here is pure bookkeeping and
//     returns before its caller makes an HTTP request. A lock held across a
//     call to an unreachable worker would stall every other loop for the
//     length of the timeout.
//
//  2. A read-modify-write is one helper, not a get followed by a set. Locking
//     each field access individually still lets two goroutines interleave
//     between the get and the set, which is how check-then-act bugs survive a
//     mutex. assignTask, applyWorkerReport, beginRestart, and retireTask each
//     take the lock once and finish the whole transaction inside it.
//
//  3. Reads return copies. Handing a caller the live slice out of
//     WorkerTaskMap would let it read that memory after the lock is gone.
//
// The fields set once by NewManager and never written again — Workers,
// WorkerNameToAddress, WorkerNameToID, WorkerNodes, logger, and the intervals —
// are not guarded, because a value that is only ever read needs no lock. Keep
// it that way: making any of them mutable means bringing it under m.mu.

// taskByID returns the manager's copy of a task.
func (m *Manager) taskByID(id uuid.UUID) (task.Task, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	t, ok := m.TaskDb[id]
	return t, ok
}

// putTask stores a task, overwriting any existing copy.
func (m *Manager) putTask(t task.Task) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.TaskDb[t.ID] = t
}

// snapshotTasks returns every task the manager knows about. It is a snapshot,
// not a view: the caller iterates its own slice, so a concurrent write cannot
// corrupt the range and the caller may safely call back into the manager while
// looping (doHealthChecks does exactly that).
func (m *Manager) snapshotTasks() []task.Task {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tasks := make([]task.Task, 0, len(m.TaskDb))
	for _, t := range m.TaskDb {
		tasks = append(tasks, t)
	}
	return tasks
}

// workerForTask returns the worker a task is currently assigned to.
func (m *Manager) workerForTask(id uuid.UUID) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	worker, ok := m.TaskWorkerMap[id]
	return worker, ok
}

// assignmentFor returns the worker a task is assigned to together with the
// manager's copy of that task, read under a single lock so the two cannot
// describe different moments in time.
func (m *Manager) assignmentFor(id uuid.UUID) (string, task.Task, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	worker, ok := m.TaskWorkerMap[id]
	if !ok {
		return "", task.Task{}, false
	}
	return worker, m.TaskDb[id], true
}

// enqueueEvent puts an event on the pending queue.
func (m *Manager) enqueueEvent(te task.TaskEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.Pending.Enqueue(te)
}

// nextPendingEvent takes the next event off the pending queue. The emptiness
// check and the dequeue happen under one lock, so two callers cannot both see a
// non-empty queue and race for the same item.
func (m *Manager) nextPendingEvent() (task.TaskEvent, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.Pending.Dequeue()
}

// assignTask records that a task has been handed to a worker. The three maps
// are written together: a caller that updated them one helper at a time would
// leave windows where a task is routed to a worker that does not list it, or
// listed by a worker it is not routed to.
func (m *Manager) assignTask(workerName string, te task.TaskEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.EventDb[te.ID] = te
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
// reporting whether the manager had ever heard of it. The lookup and the merge
// share one lock, so a report cannot be merged into a task that another
// goroutine retired in between.
func (m *Manager) applyWorkerReport(reported task.Task) (known bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	persisted, ok := m.TaskDb[reported.ID]
	if !ok {
		return false
	}
	m.TaskDb[reported.ID] = mergeTaskUpdate(persisted, reported)
	return true
}

// beginRestart marks a task as scheduled for another attempt and returns the
// updated copy for the caller to send. Reading RestartCount and writing
// RestartCount+1 in one critical section is what keeps two concurrent restarts
// from both seeing the same count and burning only one of the budget.
func (m *Manager) beginRestart(id uuid.UUID) (task.Task, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	t, ok := m.TaskDb[id]
	if !ok {
		return task.Task{}, false
	}

	t.State = task.Scheduled
	t.RestartCount++
	m.TaskDb[id] = t
	return t, true
}

// retireTask closes out a task that has been stopped on its worker.
//
// The task stays in TaskDb marked Completed rather than being deleted: the
// worker keeps its own record and will keep reporting the task on every poll,
// so a manager that had forgotten it would log "task not found in task db"
// forever. Completed is terminal in the state machine, so mergeTaskUpdate will
// not walk it back.
//
// The routing maps do drop it, which is what takes the task out of scheduling
// and out of the health check loop.
func (m *Manager) retireTask(worker string, taskID uuid.UUID) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if t, ok := m.TaskDb[taskID]; ok {
		t.State = task.Completed
		m.TaskDb[taskID] = t
	}

	delete(m.TaskWorkerMap, taskID)

	assigned, ok := m.WorkerTaskMap[worker]
	if !ok {
		return
	}
	if idx := slices.Index(assigned, taskID); idx >= 0 {
		m.WorkerTaskMap[worker] = slices.Delete(assigned, idx, idx+1)
	}
}

// pickWorker runs the scheduler under the write lock.
//
// This needs Lock, not RLock: RoundRobinScheduler.Score advances LastWorker, so
// scheduling is a mutation wearing a read-shaped name. Under RLock several
// goroutines would hold the lock at once and all write that cursor.
func (m *Manager) pickWorker(t task.Task) (node.Node, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	candidates := m.Scheduler.SelectCandidateNodes(t, m.WorkerNodes)
	if len(candidates) == 0 {
		return node.Node{}, false
	}

	scores := m.Scheduler.Score(t, candidates)
	return m.Scheduler.Pick(scores, candidates), true
}
