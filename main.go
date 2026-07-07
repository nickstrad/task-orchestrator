package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

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

	for i := range 3 {
		wg.Add(1)
		go func(workerNum int) {
			defer wg.Done()
			stream := doWork(done, workerNum)

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

	wg.Wait()
}

type UpdateValue struct {
	Update string
	Error  error
}

func doWork(done <-chan struct{}, workerNum int) <-chan UpdateValue {

	valueStream := make(chan UpdateValue)
	var wg sync.WaitGroup
	workerName := fmt.Sprintf("worker-%d", workerNum)
	host := "localhost"
	port := 3000 + workerNum

	fmt.Printf("%s listenining on %s:%d\n", workerName, host, port)
	w := worker.NewWorker(workerName)
	api := worker.NewAPI(&w, host, port)

	runTasks := func() {
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
	}

	wg.Go(func() {
		api.Start()
	})

	wg.Go(runTasks)

	go func() {
		defer close(valueStream)
		wg.Wait()
	}()

	return valueStream

}
