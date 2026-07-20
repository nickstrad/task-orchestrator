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
	"github.com/moby/moby/api/types/network"
	"github.com/nickstrad/task-orchestrator/internal/manager"
	"github.com/nickstrad/task-orchestrator/internal/scheduler"
	"github.com/nickstrad/task-orchestrator/internal/store"
	"github.com/nickstrad/task-orchestrator/internal/task"
	"github.com/nickstrad/task-orchestrator/internal/worker"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	slog.SetDefault(logger)
	mainLogger := logger.With("component", "main")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	totalWorkers := 4
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
		m := manager.NewManager(workers, scheduler.MarginalCost, mLogger, store.InMemoryDb)
		api := manager.NewAPI(mHost, mPort, m, mLogger)
		go api.Start(done)
		go api.Manager.UpdateTasks(done)
		go api.Manager.DoHealthChecks(done)
		go api.Manager.ProcessTasks(done)
		mLogger.Info("listening")
		defer api.Stop()
		<-done
	})

	wg.Go(func() {
		taskEvents := []task.TaskEvent{
			newEchoServerTaskEvent("echo-healthy", "/health"),
			newEchoServerTaskEvent("echo-healthfail", "/health"),
			newEchoServerTaskEvent("echo-healthfail", "/health"),
			newEchoServerTaskEvent("echo-healthfail", "/health"),
		}
		for _, te := range taskEvents {
			select {
			case <-done:
				return
			default:
			}
			taskID := te.Task.ID
			data, err := json.Marshal(te)

			if err != nil {
				mainLogger.Error("unable to marshal task object", "err", err, "taskID", taskID)
				continue
			}

			mainLogger.Info("sending task to manager", "taskID", taskID)
			resp, err := http.Post(fmt.Sprintf("%s/tasks", mAddr), "application/json", bytes.NewBuffer(data))
			if err != nil {
				mainLogger.Error("sending task to manager failed", "err", err, "taskID", taskID)
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

// The demo workload below is specific to timboring/echo-server. Everything the
// orchestrator itself needs is on task.Task; these helpers only exist so the
// image's quirks live in one place instead of being copied into each literal.
const (
	echoServerImage = "timboring/echo-server:latest"

	// The image declares no EXPOSE, so PublishAllPorts (see task.Docker.Run) has
	// nothing to publish unless the task declares the port itself. Without this
	// the container starts fine but gets no host port, so every health check
	// fails with "no host port found" and the manager restarts it on a loop.
	echoServerPort = "7777/tcp"
)

// newEchoServerTask builds a Scheduled echo-server task probing healthCheck.
func newEchoServerTask(name, healthCheck string) task.Task {
	return task.Task{
		ID:           uuid.New(),
		Name:         fmt.Sprintf("%s%v", name, uuid.New()),
		State:        task.Scheduled,
		Image:        echoServerImage,
		ExposedPorts: network.PortSet{network.MustParsePort(echoServerPort): struct{}{}},
		HealthCheck:  healthCheck,
	}
}

// newEchoServerTaskEvent wraps newEchoServerTask in the Running event the
// manager's /tasks endpoint expects.
func newEchoServerTaskEvent(name, healthCheck string) task.TaskEvent {
	return task.TaskEvent{
		ID:    uuid.New(),
		State: task.Running,
		Task:  newEchoServerTask(name, healthCheck),
	}
}

// stopWorkerTasks stops and removes every container the worker still owns.
// task.Docker.Stop does both, so this is all that's needed to leave the
// docker daemon clean after a Ctrl-C.
func stopWorkerTasks(w *worker.Worker, logger *slog.Logger) {
	tasks, err := w.GetTasks()
	if err != nil {
		// Shutdown path: log and move on. Failing to enumerate tasks leaves
		// containers running, which is worth saying out loud, but there is no
		// recovery to attempt with the process already on its way down.
		logger.Error("cannot stop worker tasks: listing tasks failed", "err", err)
		return
	}

	for _, t := range tasks {
		if t.ContainerID == "" || t.State == task.Completed {
			continue
		}
		logger.Info("stopping container on shutdown", "taskID", t.ID, "containerID", t.ContainerID)
		// StopTask expects the task to be heading for Completed; the queue
		// normally sets that, but we're bypassing the queue here.
		t.State = task.Completed
		if result := w.StopTask(t); result.Error != nil {
			logger.Error("stopping container on shutdown failed",
				"err", result.Error, "taskID", t.ID, "containerID", t.ContainerID)
		}
	}
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
	w := worker.NewWorker(workerName, workerNum, wLogger, store.InMemoryDb)
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
		// Wait for the API and the RunTasks loop to exit first — otherwise
		// RunTasks could start a fresh container behind our back while we're
		// tearing them down.
		wg.Wait()
		stopWorkerTasks(&w, wLogger)
		wLogger.Info("cleaning up, closing value stream")
	}()

	return valueStream, manager.WorkerMetadata{Name: workerName, Address: workerAddr}

}
