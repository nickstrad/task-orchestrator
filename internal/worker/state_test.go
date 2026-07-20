package worker

import (
	"log/slog"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/nickstrad/task-orchestrator/internal/store"
	"github.com/nickstrad/task-orchestrator/internal/task"
)

// TestWorkerStateIsRaceFree drives every state.go helper from concurrent
// goroutines, standing in for the four the worker really runs: RunTasks
// (dequeue + write), UpdateTasks (list + merge), CollectStats (stats write),
// and the API (reads).
//
// The assertions here are almost beside the point — the value is entirely in
// what `go test -race` reports. Without w.mu this fails on Db, on Queue (a
// plain linked list with no synchronisation of its own), and on Stats.
func TestWorkerStateIsRaceFree(t *testing.T) {
	w := NewWorker("worker-test", 0, slog.New(slog.DiscardHandler), store.InMemoryDb)

	ids := make([]uuid.UUID, 8)
	for i := range ids {
		ids[i] = uuid.New()
		w.putTask(task.Task{ID: ids[i], State: task.Running})
	}

	const rounds = 50
	var wg sync.WaitGroup

	wg.Go(func() { // stands in for the API's writes
		for range rounds {
			w.AddTask(task.Task{ID: uuid.New(), State: task.Scheduled})
		}
	})

	wg.Go(func() { // stands in for RunTasks
		for range rounds {
			if w.queueLen() != 0 {
				w.dequeueTask()
			}
		}
	})

	wg.Go(func() { // stands in for updateTasks' read-merge-write
		for range rounds {
			tasks, err := w.listTasks()
			if err != nil {
				t.Errorf("listTasks: %v", err)
				return
			}
			for _, tk := range tasks {
				w.upsertTask(tk, func(p *task.Task) {
					p.State = task.Running
				})
			}
		}
	})

	wg.Go(func() { // stands in for CollectStats
		for range rounds {
			w.setStats(&Stats{TaskCount: w.taskCount()})
		}
	})

	wg.Go(func() { // stands in for the API's reads
		for range rounds {
			w.SnapshotStats()
			for _, id := range ids {
				w.LookupTask(id)
			}
		}
	})

	wg.Wait()

	for _, id := range ids {
		if _, ok := w.LookupTask(id); !ok {
			t.Errorf("task %s went missing under concurrent access", id)
		}
	}
}

// TestUpsertTaskDoesNotResurrectAStoppedTask pins the lost update that
// upsertTask exists to prevent: updateTasks reads a Running task, its Inspect
// call takes long enough for StopTask to complete the task, and the write-back
// must not restore the stale State it read.
//
// A whole-struct putTask here would leave the task Running with no container,
// so the next pass marks it Failed and the manager restarts a task the
// operator asked to stop.
func TestUpsertTaskDoesNotResurrectAStoppedTask(t *testing.T) {
	w := NewWorker("worker-test", 0, slog.New(slog.DiscardHandler), store.InMemoryDb)

	id := uuid.New()
	w.putTask(task.Task{ID: id, State: task.Running, ContainerID: "abc"})

	// What updateTasks read before its Inspect call.
	stale, ok := w.LookupTask(id)
	if !ok {
		t.Fatal("seeded task not found")
	}

	// StopTask completes the task while that Inspect is in flight.
	w.upsertTask(stale, func(p *task.Task) { p.State = task.Completed })

	// updateTasks now writes back what it learned. It owns HostPorts; it does
	// not own State, and its copy of State is stale.
	w.upsertTask(stale, func(p *task.Task) {
		if p.State != task.Running {
			return
		}
		p.State = task.Failed
	})

	got, ok := w.LookupTask(id)
	if !ok {
		t.Fatal("task went missing")
	}
	if got.State != task.Completed {
		t.Errorf("State = %v, want %v — the stale update walked back the stop",
			got.State, task.Completed)
	}
}
