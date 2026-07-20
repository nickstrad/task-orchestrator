package worker

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/nickstrad/task-orchestrator/internal/queue"
	"github.com/nickstrad/task-orchestrator/internal/store"
	"github.com/nickstrad/task-orchestrator/internal/task"
)

type Worker struct {
	// mu guards Db, Queue, and Stats. Every access goes through a helper in
	// state.go; see docs/concurrency-and-state.md.
	mu sync.RWMutex

	Name   string
	ID     int
	Db     store.Store[task.Task]
	Queue  *queue.Queue[task.Task]
	Stats  *Stats
	logger *slog.Logger
}

func NewWorker(name string, id int, logger *slog.Logger, dbType string) Worker {

	dbs := store.GetDbs(dbType)

	return Worker{
		Name:   name,
		ID:     id,
		Db:     dbs.TaskDb,
		Queue:  queue.New[task.Task](),
		logger: logger,
	}
}

// LookupTask is lookupTask for callers outside this package — the API needs it
// to answer a stop request for a task the worker may not have.
func (w *Worker) LookupTask(id uuid.UUID) (task.Task, bool) {
	return w.lookupTask(id)
}

func (w *Worker) CollectStats(done <-chan struct{}) {
	for {
		select {
		case <-done:
			return
		default:
		}
		w.logger.Debug("collecting stats")
		stats := NewStats(w.logger)
		// TaskCount used to come from a Worker field that nothing ever
		// incremented, so this always reported zero. Asking the store means it
		// cannot drift out of step with reality again.
		stats.TaskCount = w.taskCount()
		w.setStats(stats)
		time.Sleep(time.Second * 15)
	}
}

func (w *Worker) RunTask() task.DockerResult {
	taskQueued, ok := w.dequeueTask()
	if !ok {
		return task.NewDockerResult(E("worker.RunTask", "no tasks in the queue", nil), "", "", "")
	}

	taskPersisted, exists := w.lookupTask(taskQueued.ID)

	if !exists {
		// Category 2: nothing else can hold a version of a task the store has
		// never seen, so a whole-value write is safe here.
		taskPersisted = taskQueued
		w.putTask(taskQueued)
	}

	stateMachine := task.NewStateMachine()

	if !stateMachine.IsValidTransition(taskPersisted.State, taskQueued.State) {
		msg := fmt.Sprintf("invalid state transition %s to %s", taskPersisted.State, taskQueued.State)
		return task.NewDockerResult(E("worker.RunTask", msg, nil), "", "", "")
	}

	var result task.DockerResult
	switch taskQueued.State {
	case task.Scheduled:
		result = w.StartTask(taskQueued)
	case task.Completed:
		result = w.StopTask(taskQueued)
	default:
		msg := fmt.Sprintf("invalid state %s", taskQueued.State)
		result.Error = E("worker.RunTask", msg, nil)
	}

	return result
}

func (w *Worker) StartTask(t task.Task) task.DockerResult {
	startTime := time.Now().UTC()
	hostPorts := t.HostPorts

	// Every write below follows a Docker call, so each is a category-4 merge
	// applying only the fields StartTask owns. See docs/concurrency-and-state.md.
	// The merge callback is the only writer of those fields — mutating the local
	// t as well would be a second copy of the same decision to keep in step.
	fail := func() {
		w.upsertTask(t, func(p *task.Task) {
			p.StartTime = startTime
			p.State = task.Failed
		})
	}

	config := task.NewConfig(t)
	dockerHandler, err := task.NewDocker(config)
	if err != nil {
		fail()
		return task.NewDockerResult(Wrap("worker.StartTask", "creating docker handler", err), "", "", "")
	}

	if t.ContainerID != "" {
		dockerResult := dockerHandler.Stop(t.ContainerID)
		if dockerResult.Error != nil {
			fail()
			return task.NewDockerResult(Wrap("worker.StartTask", fmt.Sprintf("stopping existing container %s for task %s", t.ContainerID, t.ID), dockerResult.Error), "", "", "")
		}
	}

	result := dockerHandler.Run()

	if result.Error != nil {
		fail()
		result.Error = Wrap("worker.StartTask", fmt.Sprintf("starting task %s", t.ID), result.Error)
		return result
	}

	if result.Result == "success" {
		inspectResult := dockerHandler.Inspect(result.ContainerId)
		if inspectResult.Error != nil {
			fail()
			result.Error = Wrap("worker.StartTask", fmt.Sprintf("inspecting container %s for task %s", result.ContainerId, t.ID), inspectResult.Error)
			return result
		}

		if inspectResult.Container.Container.NetworkSettings.Ports != nil {
			hostPorts = inspectResult.Container.Container.NetworkSettings.Ports
		}
	}

	w.upsertTask(t, func(p *task.Task) {
		p.StartTime = startTime
		p.ContainerID = result.ContainerId
		p.HostPorts = hostPorts
		p.State = task.Running
	})
	w.logger.Info("container started", "taskID", t.ID, "containerID", result.ContainerId, "image", t.Image)

	return result
}

func (w *Worker) StopTask(t task.Task) task.DockerResult {
	config := task.NewConfig(t)
	dockerHandler, err := task.NewDocker(config)
	if err != nil {
		return task.NewDockerResult(Wrap("worker.StopTask", "creating docker handler", err), "", "", "")
	}
	result := dockerHandler.Stop(t.ContainerID)

	if result.Error != nil {
		result.Error = Wrap("worker.StopTask", fmt.Sprintf("stopping task %s", t.ID), result.Error)
		return result
	}

	finishTime := time.Now().UTC()
	w.upsertTask(t, func(p *task.Task) {
		p.FinishTime = finishTime
		p.State = task.Completed
	})
	w.logger.Info("container stopped and removed", "taskID", t.ID, "containerID", t.ContainerID)

	return result
}

func (w *Worker) AddTask(t task.Task) {
	w.enqueueTask(t)
}

func (w *Worker) GetTasks() ([]task.Task, error) {
	return w.listTasks()
}

func (w *Worker) RunTasks(done <-chan struct{}) {
	for {
		select {
		case <-done:
			return
		case <-time.After(10 * time.Second):
		}
		if w.queueLen() != 0 {
			// Terminal consumer: nobody above us can act on this, so log once here.
			result := w.RunTask()
			if result.Error != nil {
				w.logger.Error("run task failed", "err", result.Error, "containerID", result.ContainerId)
			}
		} else {
			w.logger.Debug("no tasks to process currently")
		}
		w.logger.Debug("sleeping", "seconds", 10)
	}
}

func (w *Worker) InspectTask(t task.Task) task.DockerInspectResponse {
	config := task.NewConfig(t)
	d, err := task.NewDocker(config)
	if err != nil {
		return task.DockerInspectResponse{
			Error: Wrap("worker.InspectTask", "creating docker handler", err),
		}
	}
	return d.Inspect(t.ContainerID)
}

func (w *Worker) UpdateTasks() {
	for {
		w.logger.Debug("checking status of tasks")
		w.updateTasks()
		w.logger.Debug("task updates completed, sleeping", "seconds", 15)
		time.Sleep(15 * time.Second)
	}
}

func (w *Worker) updateTasks() {
	// Snapshot first. Ranging the store while writing back into it was safe on
	// a bare map only because the keys never changed; through the Store
	// interface there is no such guarantee, and List already hands back a slice
	// the loop owns.
	tasks, err := w.listTasks()
	if err != nil {
		// Top of a loop goroutine: nobody above can act on this, so it is
		// logged here and the pass is skipped. The next tick retries.
		w.logger.Error("skipping task update pass: listing tasks failed", "err", err)
		return
	}

	for _, t := range tasks {
		id := t.ID
		if t.State != task.Running {
			continue
		}

		// Both of these leave resp.Container nil, so we must not fall through
		// to the NetworkSettings dereference below.
		resp := w.InspectTask(t)
		if resp.Error != nil {
			w.logger.Warn("inspecting task failed", "err", resp.Error, "taskID", id)
			continue
		}
		if resp.Container == nil {
			w.logger.Warn("no container for running task", "taskID", id)
			w.upsertTask(t, func(p *task.Task) {
				p.State = task.Failed
			})
			continue
		}

		exited := resp.Container.Container.State.Status == "exited"
		if exited {
			w.logger.Warn("container in non-running state",
				"taskID", id, "state", resp.Container.Container.State.Status)
		}
		hostPorts := resp.Container.Container.NetworkSettings.Ports

		// The Inspect above is the I/O this merge exists for: the task may have
		// been stopped while it ran, and this pass must not walk that back.
		w.upsertTask(t, func(p *task.Task) {
			if p.State != task.Running {
				return
			}
			p.HostPorts = hostPorts
			if exited {
				p.State = task.Failed
			}
		})
	}
}
