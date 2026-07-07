package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/nickstrad/task-orchestrator/internal/task"
)

type API struct {
	Address string
	Port    int
	Worker  *Worker
	Router  *chi.Mux
	Server  *http.Server
}

type ErrorResponse struct {
	Message string `json:"message"`
	Code    int    `json:"code"`
}

type Response struct {
	Message string `json:"message"`
}

func NewAPI(worker *Worker, host string, port int) API {
	return API{
		Address: host,
		Port:    port,
		Worker:  worker,
	}
}

func (a *API) StartTaskHandler(w http.ResponseWriter, r *http.Request) {
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()

	w.Header().Set("Content-Type", "application/json")

	taskEvent := task.TaskEvent{}
	if err := d.Decode(&taskEvent); err != nil {
		msg := fmt.Sprintf("Error unmarshalling bodh: %v\n", err)
		log.Println(msg)
		w.WriteHeader(400)
		e := ErrorResponse{
			Code:    500,
			Message: msg,
		}
		json.NewEncoder(w).Encode(e)
		return
	}

	a.Worker.AddTask(taskEvent.Task)
	log.Printf("Added task %v\n", taskEvent.Task.ID)
	w.WriteHeader(201)
	json.NewEncoder(w).Encode(taskEvent.Task)
}

func (a *API) GetTasksHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	json.NewEncoder(w).Encode(a.Worker.GetTasks())
}

func (a *API) StopTaskHandler(w http.ResponseWriter, r *http.Request) {
	taskID := chi.URLParam(r, "taskID")
	if taskID == "" {
		msg := "No taskID passed in request"
		log.Println(msg)
		w.WriteHeader(400)
		e := ErrorResponse{
			Code:    500,
			Message: msg,
		}
		json.NewEncoder(w).Encode(e)
		return
	}

	parsedTaskID, _ := uuid.Parse(taskID)

	_, ok := a.Worker.Db[parsedTaskID]
	if !ok {
		msg := "task does not exist on worker"
		log.Println(msg)
		e := ErrorResponse{
			Code:    500,
			Message: msg,
		}
		w.WriteHeader(404)
		json.NewEncoder(w).Encode(e)
		return
	}

	taskToStop := a.Worker.Db[parsedTaskID]
	taskCopy := *taskToStop
	taskCopy.State = task.Completed
	a.Worker.AddTask(taskCopy)

	msg := fmt.Sprintf("Added task %v to stop container %v\n", taskToStop.ID, taskToStop.ContainerID)
	log.Println(msg)
	w.WriteHeader(204)
	e := Response{
		Message: msg,
	}
	json.NewEncoder(w).Encode(e)

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

func (a *API) Start() {
	a.initRouter()
	a.Server.ListenAndServe()
}

func (a *API) Stop() {
	if a.Server != nil {
		ctx := context.Background()
		a.Server.Shutdown(ctx)
	}
}
