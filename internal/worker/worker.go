package worker

import (
	"errors"
	"fmt"
	"log"
	"runtime/debug"
	"time"

	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
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
	Queue     queue.Queue
	TaskCount int
	Stats     *Stats
}

func NewWorker(name string, id int) Worker {
	return Worker{
		Name:  name,
		ID:    id,
		Db:    make(map[uuid.UUID]task.Task),
		Queue: *queue.New(),
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
	t := w.Queue.Dequeue()
	if t == nil {
		errMsg := "No tasks in the queue."
		err := task.WrapError(errors.New(errMsg), "%s", errMsg)
		log.Println(err)
		return task.NewDockerResult(err, "", "", "")
	}

	taskQueued := t.(task.Task)

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
