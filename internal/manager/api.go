package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/nickstrad/task-orchestrator/internal/task"
)

type API struct {
	Address string
	Port    int
	Router  *chi.Mux
	Server  *http.Server
	Manager Manager
}

func NewAPI(host string, port int, manager Manager) API {
	return API{
		Address: host,
		Port:    port,
		Manager: manager,
	}
}

func (a *API) StartTaskHandler(w http.ResponseWriter, r *http.Request) {
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	w.Header().Set("Content-Type", "application/json")

	taskEvent := task.TaskEvent{}

	if err := d.Decode(&taskEvent); err != nil {
		msg := fmt.Sprintf("Error unmarshalling body: %v\n", err)
		log.Println(msg)
		w.WriteHeader(400)
		e := ErrorResponse{
			Code:    400,
			Message: msg,
		}
		json.NewEncoder(w).Encode(e)
		return
	}

	a.Manager.AddTask(taskEvent)
	fmt.Printf("Added task: %s\n", taskEvent.Task.ID)
	w.WriteHeader(201)
	json.NewEncoder(w).Encode(taskEvent)
}

func (a *API) StopTaskHandler(w http.ResponseWriter, r *http.Request) {
	taskID := chi.URLParam(r, "taskID")
	if taskID == "" {
		msg := "No taskID passed in request"
		log.Println(msg)
		w.WriteHeader(400)
		e := ErrorResponse{
			Code:    400,
			Message: msg,
		}
		json.NewEncoder(w).Encode(e)
		return
	}
	parsedTaskID, _ := uuid.Parse(taskID)

	t, exists := a.Manager.TaskDb[parsedTaskID]
	if !exists {
		msg := "task does not exist on worker"
		log.Println(msg)
		e := ErrorResponse{
			Code:    404,
			Message: msg,
		}
		w.WriteHeader(404)
		json.NewEncoder(w).Encode(e)
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
	fmt.Printf("Scheduled task %s to be deleted with task event %s\n", t.ID, te.ID)
	w.WriteHeader(204)
}

func (a *API) GetTasksHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	json.NewEncoder(w).Encode(a.Manager.GetTasks())
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
