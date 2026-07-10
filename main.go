package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/nickstrad/task-orchestrator/internal/manager"
	"github.com/nickstrad/task-orchestrator/internal/task"
	"github.com/nickstrad/task-orchestrator/internal/worker"
)

func main() {

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	var wg sync.WaitGroup
	done := make(chan struct{})

	wg.Go(func() {
		defer close(done)
		sig := <-sigChan
		fmt.Printf("\nReceived signal: %v. Cleaning up...\n", sig)

	})

	totalWorkers := 1
	workerStream := make(chan manager.WorkerMetadata)
	for i := range totalWorkers {
		wg.Add(1)
		go func(workerNum int) {
			defer wg.Done()
			stream, workerMetadata := doWork(done, workerNum)
			workerStream <- workerMetadata
			for v := range stream {
				select {
				case <-done:
					return
				default:
				}
				if v.Error != nil {
					fmt.Printf("%v", v.Error)
					continue
				}
				fmt.Println(v.Update)
			}
		}(i)
	}

	wg.Go(func() {
		workers := []manager.WorkerMetadata{}
		for range totalWorkers {
			select {
			case <-done:
				return
			default:
			}
			workers = append(workers, <-workerStream)
		}
		m := manager.NewManager(workers)

		for i := range totalWorkers {
			select {
			case <-done:
				return
			default:
			}
			t := task.Task{
				ID:    uuid.New(),
				Name:  fmt.Sprintf("test-container-%d", i),
				State: task.Scheduled,
				Image: "strm/helloworld-http",
			}
			te := task.TaskEvent{
				ID:    uuid.New(),
				State: task.Running,
				Task:  t,
			}
			m.AddTask(te)
			m.SendWork()
		}
		for {
			select {
			case <-done:
				return
			case <-time.After(time.Second * 10):
				m.UpdateTasks()

			}
		}
	})

	wg.Wait()
}

type UpdateValue struct {
	Update string
	Error  error
}

func doWork(done <-chan struct{}, workerNum int) (<-chan UpdateValue, manager.WorkerMetadata) {

	valueStream := make(chan UpdateValue)
	var wg sync.WaitGroup
	workerName := fmt.Sprintf("worker-%d", workerNum)
	host := "localhost"
	port := 3000 + workerNum

	fmt.Printf("%s listenining on %s:%d\n", workerName, host, port)
	w := worker.NewWorker(workerName, workerNum)
	go w.CollectStats(done)
	api := worker.NewAPI(&w, host, port)
	wg.Go(func() {
		api.Start()
	})

	wg.Go(func() {
		for {
			select {
			case <-done:
				api.Stop()
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
	})

	go func() {
		defer close(valueStream)
		wg.Wait()
	}()

	return valueStream, manager.WorkerMetadata{Name: workerName, ID: workerNum}

}
