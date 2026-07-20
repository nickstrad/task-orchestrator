package worker

import (
	"errors"

	"github.com/google/uuid"
	"github.com/nickstrad/task-orchestrator/internal/store"
	"github.com/nickstrad/task-orchestrator/internal/task"
)

// This file owns every read and write of the Worker's mutable state — Db,
// Queue, and Stats. Nothing outside it may touch them directly.
//
// See docs/concurrency-and-state.md for the four helper categories, why the
// manager's transaction style does not port over to this package, and the
// Locked-suffix convention. Each helper below names its category.
//
// Each helper mints its own op, the same way the manager's do. A caller-supplied
// op would be an unverifiable string literal at every call site — and since most
// of these log rather than return, the caller's op never reached anyone anyway.

// lookupTask reads one task out of the store. (Category 1: pure read.)
func (w *Worker) lookupTask(id uuid.UUID) (task.Task, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.lookupTaskLocked(id)
}

// lookupTaskLocked is lookupTask for callers that already hold the lock.
func (w *Worker) lookupTaskLocked(id uuid.UUID) (task.Task, bool) {
	t, err := w.Db.Get(id.String())
	if errors.Is(err, store.ErrNotFound) {
		return task.Task{}, false
	}
	if err != nil {
		w.logger.Error("reading task db failed",
			"err", Wrap("worker.lookupTask", "reading task "+id.String(), err), "taskID", id)
		return task.Task{}, false
	}
	return t, true
}

// listTasks returns every task the worker knows about. (Category 1.) It is a
// snapshot, not a view — updateTasks writes back while iterating it.
func (w *Worker) listTasks() ([]task.Task, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	tasks, err := w.Db.List()
	if err != nil {
		return nil, Wrap("worker.listTasks", "listing tasks", err)
	}
	return tasks, nil
}

// putTask writes a task, overwriting any existing copy. (Category 2.)
//
// Use this ONLY when the caller owns the whole record — currently just RunTask
// recording a task it has never seen before. Anything writing back a task it
// read before a Docker call must use upsertTask instead.
func (w *Worker) putTask(t task.Task) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.putTaskLocked(t)
}

func (w *Worker) putTaskLocked(t task.Task) {
	// Logged, not returned: every call site is mid-Docker-operation with an
	// error of its own to report, but a dropped write leaves the worker's
	// record disagreeing with the container it just started.
	if err := w.Db.Put(t.ID.String(), t); err != nil {
		w.logger.Error("persisting task failed",
			"err", Wrap("worker.putTask", "storing task "+t.ID.String(), err),
			"taskID", t.ID, "state", t.State)
	}
}

// upsertTask applies a change to the CURRENT stored task. (Category 4: a
// read-modify-write with I/O in the middle. See docs/concurrency-and-state.md
// — this is the pattern that whole-struct writes get wrong.)
//
// It re-reads the task under the lock, hands apply a pointer to that fresh
// copy, and writes back what apply touched. Two rules for apply: touch only
// fields the caller owns, and do no I/O — the lock is held.
//
// base is the fallback for a task not yet in the store.
func (w *Worker) upsertTask(base task.Task, apply func(t *task.Task)) {
	w.mu.Lock()
	defer w.mu.Unlock()

	t, ok := w.lookupTaskLocked(base.ID)
	if !ok {
		t = base
	}

	apply(&t)
	w.putTaskLocked(t)
}

// enqueueTask puts a task on the pending queue. (Category 2.) The queue needs
// guarding as much as the store does — collections/queue is a plain linked
// list with no synchronisation of its own.
func (w *Worker) enqueueTask(t task.Task) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.Queue.Enqueue(t)
}

// dequeueTask takes the next task off the queue. (Category 3.)
//
// Callers must not check queueLen and then dequeue: two goroutines can both
// pass the length check and race for the same task.
func (w *Worker) dequeueTask() (task.Task, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.Queue.Dequeue()
}

// queueLen reports the pending queue depth. (Category 1.) A status read, not
// a guard — see dequeueTask.
func (w *Worker) queueLen() int {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.Queue.Len()
}

// setStats replaces the stats snapshot. (Category 2: CollectStats builds the
// whole value from scratch each tick.)
func (w *Worker) setStats(s *Stats) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.Stats = s
}

// SnapshotStats returns the most recent stats, and false if none have been
// collected yet. (Category 1.)
//
// It copies rather than returning w.Stats: CollectStats replaces that pointer
// every 15 seconds, so a bare return races once the lock is gone.
func (w *Worker) SnapshotStats() (Stats, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.Stats == nil {
		return Stats{}, false
	}
	return *w.Stats, true
}

// taskCount reports how many tasks the worker is tracking. (Category 1.)
func (w *Worker) taskCount() int {
	w.mu.RLock()
	defer w.mu.RUnlock()

	n, err := w.Db.Count()
	if err != nil {
		w.logger.Error("counting tasks failed",
			"err", Wrap("worker.taskCount", "counting tasks", err))
		return 0
	}
	return n
}
