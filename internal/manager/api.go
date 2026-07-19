package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/nickstrad/task-orchestrator/internal/httpapi"
	"github.com/nickstrad/task-orchestrator/internal/task"
)

type API struct {
	Address string
	Port    int
	Router  *chi.Mux
	Server  *http.Server
	Manager *Manager
	logger  *slog.Logger
}

func NewAPI(host string, port int, manager *Manager, logger *slog.Logger) API {
	return API{
		Address: host,
		Port:    port,
		Manager: manager,
		logger:  logger.With("subcomponent", "api"),
	}
}

func (a *API) StartTaskHandler(w http.ResponseWriter, r *http.Request) {
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()

	taskEvent := task.TaskEvent{}

	if err := d.Decode(&taskEvent); err != nil {
		a.logger.Error("unmarshalling request body",
			"err", E("manager.API.StartTaskHandler", "decoding task event", err))
		httpapi.WriteError(w, http.StatusBadRequest, "error unmarshalling body")
		return
	}

	a.Manager.AddTask(taskEvent)
	a.logger.Info("task added", "taskID", taskEvent.Task.ID)
	httpapi.WriteJSON(w, http.StatusCreated, taskEvent)
}

func (a *API) StopTaskHandler(w http.ResponseWriter, r *http.Request) {
	taskID := chi.URLParam(r, "taskID")
	if taskID == "" {
		a.logger.Warn("no taskID passed in request")
		httpapi.WriteError(w, http.StatusBadRequest, "no taskID passed in request")
		return
	}
	parsedTaskID, _ := uuid.Parse(taskID)

	t, exists := a.Manager.TaskDb[parsedTaskID]
	if !exists {
		a.logger.Warn("task does not exist on manager", "taskID", taskID)
		httpapi.WriteError(w, http.StatusNotFound, "task does not exist on manager")
		return
	}
	tCopy := t
	tCopy.State = task.Completed
	te := task.TaskEvent{
		ID:        uuid.New(),
		Task:      tCopy,
		State:     task.Completed,
		Timestamp: time.Now(),
	}

	a.Manager.AddTask(te)
	a.logger.Info("task scheduled for deletion", "taskID", t.ID, "taskEventID", te.ID)
	w.WriteHeader(http.StatusNoContent)
}

func (a *API) GetTasksHandler(w http.ResponseWriter, r *http.Request) {
	httpapi.WriteJSON(w, http.StatusOK, a.Manager.GetTasks())
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
