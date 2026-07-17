package worker

import (
	"errors"
	"fmt"
	"log"
	"runtime/debug"
	"time"

	"github.com/google/uuid"
	"github.com/nickstrad/task-orchestrator/internal/queue"
	"github.com/nickstrad/task-orchestrator/internal/task"
)

type WorkerError struct {
	Inner      error
	Message    string
	StackTrace string
	Misc       map[string]interface{}
}

type Worker struct {
	Name      string
	ID        int
	Db        map[uuid.UUID]task.Task
	Queue     *queue.Queue[task.Task]
	TaskCount int
	Stats     *Stats
}

func NewWorker(name string, id int) Worker {
	return Worker{
		Name:  name,
		ID:    id,
		Db:    make(map[uuid.UUID]task.Task),
		Queue: queue.New[task.Task](),
	}
}

func (w *Worker) CollectStats(done <-chan struct{}) {
	for {
		select {
		case <-done:
			return
		default:
		}
		log.Println("Collecting Stats")
		w.Stats = NewStats()
		w.Stats.TaskCount = w.TaskCount
		time.Sleep(time.Second * 15)
	}
}

func WrapError(err error, messagef string, msgArgs ...interface{}) *WorkerError {
	return &WorkerError{
		Inner:      err,
		Message:    fmt.Sprintf(messagef, msgArgs...),
		StackTrace: string(debug.Stack()),
		Misc:       make(map[string]interface{}),
	}
}

func (w *Worker) RunTask() task.DockerResult {
	taskQueued, ok := w.Queue.Dequeue()
	if !ok {
		errMsg := "No tasks in the queue."
		err := task.WrapError(errors.New(errMsg), "%s", errMsg)
		log.Println(err)
		return task.NewDockerResult(err, "", "", "")
	}

	taskPersisted, exists := w.Db[taskQueued.ID]

	if !exists {
		taskPersisted = taskQueued
		w.Db[taskQueued.ID] = taskQueued
	}

	stateMachine := task.NewStateMachine()

	if !stateMachine.IsValidTransition(taskPersisted.State, taskQueued.State) {
		errMsg := "invalid state transition %d to %d."
		err := task.WrapError(errors.New(errMsg), errMsg, taskPersisted.State, taskQueued.State)
		log.Println(err)
		return task.NewDockerResult(err, "", "", "")
	}

	var result task.DockerResult
	switch taskQueued.State {
	case task.Scheduled:
		result = w.StartTask(taskQueued)
	case task.Completed:
		result = w.StopTask(taskQueued)
	default:
		errMsg := "invalid state %d"
		result.Error = task.WrapError(errors.New(errMsg), errMsg, taskQueued.State)
	}

	return result
}

func (w *Worker) StartTask(t task.Task) task.DockerResult {
	t.StartTime = time.Now().UTC()
	config := task.NewConfig(t)
	dockerHandler := task.NewDocker(config)
	result := dockerHandler.Run()

	if result.Error != nil {
		log.Printf("Error starting container:%v: %v\n", t.ContainerID, result.Error.Message)
		t.State = task.Failed
		w.Db[t.ID] = t
		return result
	}

	t.ContainerID = result.ContainerId
	t.State = task.Running
	w.Db[t.ID] = t

	return result
}

func (w *Worker) StopTask(t task.Task) task.DockerResult {
	config := task.NewConfig(t)
	dockerHandler := task.NewDocker(config)
	result := dockerHandler.Stop(t.ContainerID)

	if result.Error != nil {
		log.Printf("Error stopping  container:%v: %v\n", t.ContainerID, result.Error.Message)
	}

	t.FinishTime = time.Now().UTC()
	t.State = task.Completed
	w.Db[t.ID] = t
	log.Printf("Stopped and removed container %v for task %v", t.ContainerID, t.ID)

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
			result := w.RunTask()
			if result.Error != nil {
				log.Printf("Error running task: %v\n", result.Error)
			}
		} else {
			log.Println("No tasks to process currently.")
		}
		log.Println("Sleeping for 10 seconds")
	}
}

func (w *Worker) InspectTask(t task.Task) task.DockerInspectResponse {
	config := task.NewConfig(t)
	d := task.NewDocker(config)
	return d.Inspect(t.ContainerID)
}

func (w *Worker) UpdateTasks() {
	for {
		log.Println("checking status of tasks")
		w.updateTasks()
		log.Println("Task updates completed")
		log.Println("sleeping for 15 seconds")
		time.Sleep(15 * time.Second)
	}
}

func (w *Worker) updateTasks() {
	for id, t := range w.Db {
		if t.State != task.Running {
			continue
		}

		resp := w.InspectTask(t)
		if resp.Error != nil {
			fmt.Printf("Error: %v\n", resp.Error)
		} else if resp.Container == nil {
			log.Printf("No container for running task %s\n", id)
			t.State = task.Failed
		} else if resp.Container.Container.State.Status == "exited" {
			log.Printf("container for task %s in non-running state %s", id, resp.Container.Container.State.Status)
			t.State = task.Failed
		}

		t.HostPorts = resp.Container.Container.NetworkSettings.Ports
		w.Db[id] = t
	}
}
