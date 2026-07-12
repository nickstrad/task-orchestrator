package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
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
	totalWorkers := 1
	mHost := "localhost"
	mPort := 3000 + totalWorkers + 1
	mAddr := fmt.Sprintf("http://%s:%d", mHost, mPort)
	workerStream := make(chan manager.WorkerMetadata)
	var wg sync.WaitGroup
	done := make(chan struct{})

	wg.Go(func() {
		defer close(done)
		sig := <-sigChan
		fmt.Printf("\nmain process: Received signal: %v. Cleaning up...\n", sig)
	})

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
					fmt.Printf("worker-%d: %v\n", workerNum, v.Error)
					continue
				}
				fmt.Printf("worker-%d: %v", workerNum, v.Update)
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
		api := manager.NewAPI(mHost, mPort, *m)
		go api.Start(done)
		go api.Manager.UpdateTasks(done)
		go api.Manager.ProcessTasks(done)
		fmt.Printf("Manager listening on: %s\n", mAddr)
		defer api.Stop()
		<-done
	})

	wg.Go(func() {
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
			data, err := json.Marshal(te)

			if err != nil {
				log.Printf("main process: Unable to marshal task object: %v.\n", t)
				continue
			}

			fmt.Println("main process: sending task to manager!")
			resp, err := http.Post(fmt.Sprintf("%s/tasks", mAddr), "application/json", bytes.NewBuffer(data))
			if err != nil {
				fmt.Println(fmt.Errorf("main process: error sending task to manager %v", err))
				continue
			}

			fmt.Printf("main process: %v\n", resp.Body)
			d := json.NewDecoder(resp.Body)

			respTaskEvent := task.TaskEvent{}

			if err := d.Decode(&respTaskEvent); err != nil {
				fmt.Println(fmt.Errorf("%s: Error unmarshalling body: %v\n", mAddr, err))
				continue
			}

			fmt.Printf("main process: resp from manager: %v\n", respTaskEvent)
			fmt.Println("main process: sleeping for 3 seconds")
			time.Sleep(3 * time.Second)
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

	workerAddr := fmt.Sprintf("http://%s:%d", host, port)
	fmt.Printf("%s listenining on %s\n", workerName, workerAddr)
	w := worker.NewWorker(workerName, workerNum)
	go w.CollectStats(done)
	api := worker.NewAPI(&w, host, port)
	wg.Go(func() {
		fmt.Printf("%s: starting server \n", workerAddr)
		api.Start(done)
	})

	wg.Go(func() {
		fmt.Printf("%s: starting run tasks \n", workerAddr)
		w.RunTasks(done)
	})

	go func() {
		defer close(valueStream)
		wg.Wait()
		fmt.Printf("%s: cleaning up closing value stream \n", workerAddr)
	}()

	return valueStream, manager.WorkerMetadata{Name: workerName, ID: workerNum}

}
