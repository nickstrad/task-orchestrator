package worker

import (
	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
	"github.com/nickstrad/task-orchestrator/internal/task"
)

type Worker struct {
	Name      string
	Db        map[uuid.UUID]*task.Task
	Queue     queue.Queue
	TaskCount int
}

func (w *Worker) RunTask()      {}
func (w *Worker) StartTask()    {}
func (w *Worker) StopTask()     {}
func (w *Worker) CollectStats() {}
