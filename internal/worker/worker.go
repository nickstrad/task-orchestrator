package worker

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/nickstrad/task-orchestrator/internal/queue"
	"github.com/nickstrad/task-orchestrator/internal/task"
)

type Worker struct {
	Name      string
	ID        int
	Db        map[uuid.UUID]task.Task
	Queue     *queue.Queue[task.Task]
	TaskCount int
	Stats     *Stats
	logger    *slog.Logger
}

func NewWorker(name string, id int, logger *slog.Logger) Worker {
	return Worker{
		Name:   name,
		ID:     id,
		Db:     make(map[uuid.UUID]task.Task),
		Queue:  queue.New[task.Task](),
		logger: logger,
	}
}

func (w *Worker) CollectStats(done <-chan struct{}) {
	for {
		select {
		case <-done:
			return
		default:
		}
		w.logger.Debug("collecting stats")
		w.Stats = NewStats(w.logger)
		w.Stats.TaskCount = w.TaskCount
		time.Sleep(time.Second * 15)
	}
}

func (w *Worker) RunTask() task.DockerResult {
	taskQueued, ok := w.Queue.Dequeue()
	if !ok {
		return task.NewDockerResult(E("worker.RunTask", "no tasks in the queue", nil), "", "", "")
	}

	taskPersisted, exists := w.Db[taskQueued.ID]

	if !exists {
		taskPersisted = taskQueued
		w.Db[taskQueued.ID] = taskQueued
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
	t.StartTime = time.Now().UTC()
	config := task.NewConfig(t)
	dockerHandler, err := task.NewDocker(config)
	if err != nil {
		t.State = task.Failed
		w.Db[t.ID] = t
		return task.NewDockerResult(Wrap("worker.StartTask", "creating docker handler", err), "", "", "")
	}
	result := dockerHandler.Run()

	if result.Error != nil {
		t.State = task.Failed
		w.Db[t.ID] = t
		result.Error = Wrap("worker.StartTask", fmt.Sprintf("starting task %s", t.ID), result.Error)
		return result
	}

	t.ContainerID = result.ContainerId
	t.State = task.Running
	w.Db[t.ID] = t
	w.logger.Info("container started", "taskID", t.ID, "containerID", t.ContainerID, "image", t.Image)

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

	t.FinishTime = time.Now().UTC()
	t.State = task.Completed
	w.Db[t.ID] = t
	w.logger.Info("container stopped and removed", "taskID", t.ID, "containerID", t.ContainerID)

	return result
}

func (w *Worker) AddTask(t task.Task) {
	w.Queue.Enqueue(t)
}

func (w *Worker) GetTasks() []task.Task {
	tasks := make([]task.Task, 0, len(w.Db))
	for _, value := range w.Db {
		tasks = append(tasks, value)
	}

	return tasks
}

func (w *Worker) RunTasks(done <-chan struct{}) {
	for {
		select {
		case <-done:
			return
		case <-time.After(10 * time.Second):
		}
		if w.Queue.Len() != 0 {
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
	for id, t := range w.Db {
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
			t.State = task.Failed
			w.Db[id] = t
			continue
		}

		if resp.Container.Container.State.Status == "exited" {
			w.logger.Warn("container in non-running state",
				"taskID", id, "state", resp.Container.Container.State.Status)
			t.State = task.Failed
		}

		t.HostPorts = resp.Container.Container.NetworkSettings.Ports
		w.Db[id] = t
	}
}
