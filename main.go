package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
	"github.com/nickstrad/task-orchestrator/internal/manager"
	"github.com/nickstrad/task-orchestrator/internal/node"
	"github.com/nickstrad/task-orchestrator/internal/task"
	"github.com/nickstrad/task-orchestrator/internal/worker"
)

func main() {

	worker := worker.NewWorker("test")
	m := manager.Manager{
		Pending: *queue.New(),
		TaskDb:  make(map[string][]*task.Task),
		EventDb: make(map[string][]*task.TaskEvent),
		Workers: []string{worker.Name},
	}

	fmt.Printf("manager: %v\n", m)
	m.SelectWorker()
	m.UpdateTasks()
	m.SendWork()

	n := node.Node{
		Name:   "Node-1",
		Ip:     "192.168.1.1",
		Cores:  4,
		Memory: 1024,
		Disk:   25,
		Role:   "worker",
	}
	fmt.Printf("node: %v\n", n)

	var wg sync.WaitGroup

	wg.Go(func() {
		stream := doWork(task.Task{
			ID:    uuid.New(),
			Name:  "test-container-1",
			State: task.Scheduled,
			Image: "strm/helloworld-http",
		}, "worker-1", 10)

		for v := range stream {
			if v.Error != nil {
				fmt.Printf("%v", v.Error)
				continue
			}
			fmt.Println(v.Update)
		}
	})

	wg.Go(func() {
		stream := doWork(task.Task{
			ID:    uuid.New(),
			Name:  "test-container-2",
			State: task.Scheduled,
			Image: "strm/helloworld-http",
		}, "worker-2", 10)

		for v := range stream {
			if v.Error != nil {
				fmt.Printf("%v", v.Error)
				continue
			}
			fmt.Println(v.Update)
		}
	})

	wg.Go(func() {
		stream := doWork(task.Task{
			ID:    uuid.New(),
			Name:  "test-container-3",
			State: task.Scheduled,
			Image: "strm/helloworld-http",
		}, "worker-3", 10)

		for v := range stream {
			if v.Error != nil {
				fmt.Printf("%v", v.Error)
				continue
			}
			fmt.Println(v.Update)
		}
	})

	wg.Wait()
}

type UpdateValue struct {
	Update string
	Error  error
}

func doWork(workerTask task.Task, workerName string, sleepTimeSecs int) <-chan UpdateValue {

	if sleepTimeSecs == 0 {
		sleepTimeSecs = 5
	}

	valueStream := make(chan UpdateValue)
	var wg sync.WaitGroup

	wg.Go(func() {
		worker := worker.NewWorker(workerName)

		valueStream <- UpdateValue{Update: fmt.Sprintf("%s: starting task", workerName)}
		worker.AddTask(workerTask)
		result := worker.RunTask()

		if result.Error != nil {
			valueStream <- UpdateValue{Error: fmt.Errorf(workerName+": ", result.Error)}
			return
		}

		workerTask.ContainerID = result.ContainerId
		valueStream <- UpdateValue{Update: fmt.Sprintf("%s: task %s is running in container %s\n", workerName, workerTask.ID, workerTask.ContainerID)}
		valueStream <- UpdateValue{Update: fmt.Sprintf("%s: Sleepy time\n", workerName)}
		time.Sleep(time.Second * time.Duration(sleepTimeSecs))

		valueStream <- UpdateValue{Update: fmt.Sprintf("%s: stopping task %s\n", workerName, workerTask.ID)}

		workerTask.State = task.Completed
		worker.AddTask(workerTask)
		result = worker.RunTask()

		if result.Error != nil {
			valueStream <- UpdateValue{Error: fmt.Errorf(workerName+": ", result.Error)}
		}

	})

	go func() {
		defer close(valueStream)
		wg.Wait()
	}()
	return valueStream

}
