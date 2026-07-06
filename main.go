package main

import (
	"fmt"
	"os"
	"time"

	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
	"github.com/moby/moby/client"
	"github.com/nickstrad/task-orchestrator/internal/manager"
	"github.com/nickstrad/task-orchestrator/internal/node"
	"github.com/nickstrad/task-orchestrator/internal/task"
	"github.com/nickstrad/task-orchestrator/internal/worker"
)

func main() {

	t := task.Task{
		ID:     uuid.New(),
		Name:   "Task-1",
		State:  task.Pending,
		Image:  "Image-1",
		Memory: 1024,
		Disk:   1,
	}
	te := task.TaskEvent{
		ID:        uuid.New(),
		State:     task.Pending,
		Timestamp: time.Now(),
		Task:      t,
	}

	fmt.Printf("task: %v\n", t)
	fmt.Printf("task event: %v\n", te)

	w := worker.Worker{
		Name:  "worker-1",
		Queue: *queue.New(),
		Db:    make(map[uuid.UUID]*task.Task),
	}
	fmt.Printf("worker: %v\n", w)
	w.CollectStats()
	w.RunTask()
	w.StartTask()
	w.StopTask()

	m := manager.Manager{
		Pending: *queue.New(),
		TaskDb:  make(map[string][]*task.Task),
		EventDb: make(map[string][]*task.TaskEvent),
		Workers: []string{w.Name},
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

	dockerCreateTask, createResult := createContainer()
	if createResult.Error != nil {
		fmt.Printf("%v", createResult.Error)
		os.Exit(1)
	}

	time.Sleep(time.Second * 10)
	deleteResult := stopContainer(dockerCreateTask, createResult.ContainerId)

	if deleteResult.Error != nil {
		fmt.Printf("%v", deleteResult.Error)
		os.Exit(1)
	}

	fmt.Printf("successfull stopped conatiner '%s'\n", deleteResult.ContainerId)
}

func createContainer() (*task.Docker, *task.DockerResult) {
	taskConfig := task.Config{
		Name:  "test-container-1",
		Image: "postgres:13",
		Env: []string{
			"POSTGRES_USER=task-orchestrator",
			"POSTGRES_PASSWORD=task-orchestrator",
		},
	}

	dockerClient, _ := client.New(client.FromEnv)

	dockerTask := task.Docker{
		Client: dockerClient,
		Config: taskConfig,
	}
	result := dockerTask.Run()
	if result.Error != nil {
		fmt.Printf("%v\n", result.Error.Message)
		return nil, result
	}

	fmt.Printf("Container %s is running with config %v\n", result.ContainerId, taskConfig)
	return &dockerTask, result
}

func stopContainer(dockerTask *task.Docker, id string) *task.DockerResult {

	result := dockerTask.Stop(id)
	if result.Error != nil {
		fmt.Printf("%v\n", result.Error.Message)
		return nil
	}

	fmt.Printf("Container %s has been stopped\n", result.ContainerId)
	return result
}
