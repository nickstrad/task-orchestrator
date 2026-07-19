package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/nickstrad/task-orchestrator/internal/httpapi"
	"github.com/nickstrad/task-orchestrator/internal/task"
)

type API struct {
	Address string
	Port    int
	Worker  *Worker
	Router  *chi.Mux
	Server  *http.Server
	logger  *slog.Logger
}

func NewAPI(worker *Worker, host string, port int, logger *slog.Logger) API {
	return API{
		Address: host,
		Port:    port,
		Worker:  worker,
		logger:  logger.With("subcomponent", "api"),
	}
}

func (a *API) StartTaskHandler(w http.ResponseWriter, r *http.Request) {
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()

	taskEvent := task.TaskEvent{}
	if err := d.Decode(&taskEvent); err != nil {
		// The error crossed a process boundary: log it here, send the client a message.
		a.logger.Error("unmarshalling request body", "err", E("worker.API.StartTaskHandler", "decoding task event", err))
		httpapi.WriteError(w, http.StatusBadRequest, "error unmarshalling body")
		return
	}

	a.Worker.AddTask(taskEvent.Task)
	a.logger.Info("task added", "taskID", taskEvent.Task.ID)
	httpapi.WriteJSON(w, http.StatusCreated, taskEvent.Task)
}

func (a *API) GetTasksHandler(w http.ResponseWriter, r *http.Request) {
	httpapi.WriteJSON(w, http.StatusOK, a.Worker.GetTasks())
}

func (a *API) StopTaskHandler(w http.ResponseWriter, r *http.Request) {
	taskID := chi.URLParam(r, "taskID")
	if taskID == "" {
		a.logger.Warn("no taskID passed in request")
		httpapi.WriteError(w, http.StatusBadRequest, "no taskID passed in request")
		return
	}

	parsedTaskID, _ := uuid.Parse(taskID)

	taskToStop, ok := a.Worker.Db[parsedTaskID]
	if !ok {
		a.logger.Warn("task does not exist on worker", "taskID", taskID)
		httpapi.WriteError(w, http.StatusNotFound, "task does not exist on worker")
		return
	}

	taskCopy := taskToStop
	taskCopy.State = task.Completed
	a.Worker.AddTask(taskCopy)

	a.logger.Info("task added to stop container",
		"taskID", taskToStop.ID, "containerID", taskToStop.ContainerID)
	// 204 means no content — writing a body here would violate the status.
	w.WriteHeader(http.StatusNoContent)
}

func (a *API) GetStatsHandler(w http.ResponseWriter, r *http.Request) {
	httpapi.WriteJSON(w, http.StatusOK, a.Worker.Stats)
}

func (a *API) initRouter() {
	a.Router = chi.NewRouter()
	a.Router.Route("/tasks", func(r chi.Router) {
		r.Post("/", a.StartTaskHandler)
		r.Get("/", a.GetTasksHandler)
		r.Route("/{taskID}", func(r chi.Router) {
			r.Delete("/", a.StopTaskHandler)
		})
	})
	a.Router.Route("/stats", func(r chi.Router) {
		r.Get("/", a.GetStatsHandler)

	})
	server := &http.Server{
		Addr:    fmt.Sprintf("%s:%d", a.Address, a.Port),
		Handler: a.Router,
	}
	a.Server = server
}

func (a *API) Start(done <-chan struct{}) {
	a.initRouter()
	go a.Server.ListenAndServe()
	<-done
	a.Stop()
}

func (a *API) Stop() {
	if a.Server != nil {
		ctx := context.Background()
		a.Server.Shutdown(ctx)
	}
}
