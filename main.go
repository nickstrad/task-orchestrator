package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/nickstrad/task-orchestrator/internal/manager"
	"github.com/nickstrad/task-orchestrator/internal/scheduler"
	"github.com/nickstrad/task-orchestrator/internal/task"
	"github.com/nickstrad/task-orchestrator/internal/worker"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	slog.SetDefault(logger)
	mainLogger := logger.With("component", "main")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	totalWorkers := 2
	mHost := "localhost"
	mPort := 3000 + totalWorkers + 1
	mAddr := fmt.Sprintf("http://%s:%d", mHost, mPort)
	workerStream := make(chan manager.WorkerMetadata)
	var wg sync.WaitGroup
	done := make(chan struct{})

	wg.Go(func() {
		defer close(done)
		sig := <-sigChan
		mainLogger.Info("received signal, cleaning up", "signal", sig)
	})

	for i := range totalWorkers {
		wg.Add(1)
		go func(workerNum int) {
			defer wg.Done()
			stream, workerMetadata := doWork(done, workerNum, logger)
			workerStream <- workerMetadata
			for v := range stream {
				select {
				case <-done:
					return
				default:
				}
				if v.Error != nil {
					mainLogger.Error("worker update", "err", v.Error, "workerID", workerNum)
					continue
				}
				mainLogger.Info("worker update", "update", v.Update, "workerID", workerNum)
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
		mLogger := logger.With("component", "manager", "addr", mAddr)
		m := manager.NewManager(workers, scheduler.RoundRobin, mLogger)
		api := manager.NewAPI(mHost, mPort, *m, mLogger)
		go api.Start(done)
		go api.Manager.UpdateTasks(done)
		go api.Manager.DoHealthChecks(done)
		go api.Manager.ProcessTasks(done)
		mLogger.Info("listening")
		defer api.Stop()
		<-done
	})

	wg.Go(func() {
		tasks := []task.Task{
			{
				ID:          uuid.New(),
				Name:        "echo-healthy",
				State:       task.Scheduled,
				Image:       "timboring/echo-server:latest",
				HealthCheck: "/health",
			},
			{
				ID:          uuid.New(),
				Name:        "echo-healthfail",
				State:       task.Scheduled,
				Image:       "timboring/echo-server:latest",
				HealthCheck: "/healthfail",
			},
		}
		for _, t := range tasks {
			select {
			case <-done:
				return
			default:
			}
			te := task.TaskEvent{
				ID:    uuid.New(),
				State: task.Running,
				Task:  t,
			}
			data, err := json.Marshal(te)

			if err != nil {
				mainLogger.Error("unable to marshal task object", "err", err, "taskID", t.ID)
				continue
			}

			mainLogger.Info("sending task to manager", "taskID", t.ID)
			resp, err := http.Post(fmt.Sprintf("%s/tasks", mAddr), "application/json", bytes.NewBuffer(data))
			if err != nil {
				mainLogger.Error("sending task to manager failed", "err", err, "taskID", t.ID)
				continue
			}

			d := json.NewDecoder(resp.Body)

			respTaskEvent := task.TaskEvent{}

			if err := d.Decode(&respTaskEvent); err != nil {
				mainLogger.Error("unmarshalling response body failed", "err", err, "url", mAddr)
				continue
			}

			mainLogger.Info("response from manager",
				"taskID", respTaskEvent.Task.ID, "taskEventID", respTaskEvent.ID)
			mainLogger.Debug("sleeping", "seconds", 3)
			time.Sleep(3 * time.Second)
		}
	})

	wg.Wait()
}

type UpdateValue struct {
	Update string
	Error  error
}

func doWork(done <-chan struct{}, workerNum int, logger *slog.Logger) (<-chan UpdateValue, manager.WorkerMetadata) {

	// NOTE: nothing ever sends on valueStream — the UpdateValue plumbing is
	// currently dead. Kept as-is; candidate for removal in a later pass.
	valueStream := make(chan UpdateValue)
	var wg sync.WaitGroup
	workerName := fmt.Sprintf("worker-%d", workerNum)
	host := "localhost"
	port := 3000 + workerNum

	workerAddr := fmt.Sprintf("http://%s:%d", host, port)
	wLogger := logger.With("component", workerName, "workerID", workerNum, "addr", workerAddr)
	wLogger.Info("listening")
	w := worker.NewWorker(workerName, workerNum, wLogger)
	go w.CollectStats(done)
	api := worker.NewAPI(&w, host, port, wLogger)
	wg.Go(func() {
		wLogger.Info("starting server")
		api.Start(done)
	})

	wg.Go(func() {
		wLogger.Info("starting run tasks")
		w.RunTasks(done)
	})

	go func() {
		defer close(valueStream)
		wg.Wait()
		wLogger.Info("cleaning up, closing value stream")
	}()

	return valueStream, manager.WorkerMetadata{Name: workerName, ID: workerNum, Address: workerAddr}

}
