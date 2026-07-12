package manager

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
	"github.com/nickstrad/task-orchestrator/internal/task"
)

type Manager struct {
	LastWorker     int
	Pending        queue.Queue
	TaskDb         map[uuid.UUID]*task.Task
	EventDb        map[uuid.UUID]*task.TaskEvent
	Workers        []string
	WorkerTaskMap  map[string][]uuid.UUID
	TaskWorkerMap  map[uuid.UUID]string
	WorkerNameToID map[string]int
}
type ErrorResponse struct {
	Message string `json:"message"`
	Code    int    `json:"code"`
}

func getWorkerUrl(workerNum int) string {
	return fmt.Sprintf("http://localhost:%d", 3000+workerNum)
}

type WorkerMetadata struct {
	Name string
	ID   int
}

func NewManager(workers []WorkerMetadata) *Manager {
	workerTaskMap := make(map[string][]uuid.UUID)
	workerNameToID := make(map[string]int)
	workerNames := []string{}

	for _, w := range workers {
		workerTaskMap[w.Name] = []uuid.UUID{}
		workerNameToID[w.Name] = w.ID
		workerNames = append(workerNames, w.Name)

	}

	return &Manager{
		LastWorker:     0,
		Pending:        *queue.New(),
		TaskDb:         make(map[uuid.UUID]*task.Task),
		EventDb:        make(map[uuid.UUID]*task.TaskEvent),
		Workers:        workerNames,
		WorkerNameToID: workerNameToID,
		WorkerTaskMap:  workerTaskMap,
		TaskWorkerMap:  make(map[uuid.UUID]string),
	}
}

func (m *Manager) SelectWorker() string {
	if m.LastWorker == len(m.Workers)-1 {
		m.LastWorker = 0
	} else {
		m.LastWorker += 1
	}
	return m.Workers[m.LastWorker]
}

func (m *Manager) UpdateTasks(done <-chan struct{}) {
	for {
		select {
		case <-done:
			return
		case <-time.After(15 * time.Second):
			fmt.Println("updating tasks")
			m.updateTasks()
			fmt.Println("sleeping for 15 seconds")
		}
	}
}

func (m *Manager) ProcessTasks(done <-chan struct{}) {
	for {
		select {
		case <-done:
			return
		case <-time.After(10 * time.Second):
			fmt.Println("delegating tasks to workers")
			m.SendWork()
			fmt.Println("sleeping for 10 seconds")
		}
	}
}

func (m *Manager) updateTasks() {
	for _, worker := range m.Workers {
		log.Printf("Checking worker %v for task updates", worker)
		workerID := m.WorkerNameToID[worker]
		workerURL := getWorkerUrl(workerID)
		url := fmt.Sprintf("%s/tasks", workerURL)
		resp, err := http.Get(url)
		if err != nil {
			log.Printf("error connecting to %v:%v\n", worker, err)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			log.Printf("Error sending request: %v\n", err)
		}

		decoder := json.NewDecoder(resp.Body)
		var tasks []*task.Task
		err = decoder.Decode(&tasks)
		if err != nil {
			log.Printf("error unmarshalling tasks: %s\n", err.Error())
		}

		for _, t := range tasks {
			log.Printf("Attempting to update task %v\n", t.ID)
			_, ok := m.TaskDb[t.ID]
			if !ok {
				log.Printf("Task with ID %s not found from worker %s\n", t.ID, workerURL)
				return
			}

			if m.TaskDb[t.ID].State != t.State {
				m.TaskDb[t.ID].State = t.State
			}

			m.TaskDb[t.ID].StartTime = t.StartTime
			m.TaskDb[t.ID].FinishTime = t.FinishTime
			m.TaskDb[t.ID].ContainerID = t.ContainerID
		}
	}
}

func (m *Manager) SendWork() {
	if m.Pending.Len() == 0 {
		log.Println("No work in the queue ")
		return
	}
	worker := m.SelectWorker()
	taskEvent := m.Pending.Dequeue().(task.TaskEvent)
	t := taskEvent.Task
	log.Printf("Pulled %v off pending queue\n", t)

	m.EventDb[taskEvent.ID] = &taskEvent
	m.WorkerTaskMap[worker] = append(m.WorkerTaskMap[worker], t.ID)
	m.TaskWorkerMap[t.ID] = worker

	data, err := json.Marshal(taskEvent)

	if err != nil {
		log.Printf("Unable to marshal task object: %v.\n", t)
		return
	}

	workerID := m.WorkerNameToID[worker]
	url := fmt.Sprintf("%s/tasks", getWorkerUrl(workerID))
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))

	if err != nil {
		log.Printf("Error connecting to %v: %v\n", worker, err)
		m.Pending.Enqueue(taskEvent)
		return
	}

	decoder := json.NewDecoder(resp.Body)

	if resp.StatusCode != http.StatusCreated {
		e := ErrorResponse{}
		err := decoder.Decode(&e)
		if err != nil {
			fmt.Printf("Error decoding response: %s\n", e.Message)
			return
		}
		log.Printf("Response error (%d): %s", e.Code, e.Message)
		return
	}

	t = task.Task{}

	err = decoder.Decode(&t)

	if err != nil {
		fmt.Printf("error decoding response: %s\n", err.Error())
		return
	}

	m.TaskDb[t.ID] = &t
	log.Printf("%#v\n", t)
}

func (m *Manager) AddTask(taskEvent task.TaskEvent) {
	m.Pending.Enqueue(taskEvent)
}

func (m *Manager) GetTasks() []task.Task {
	tasks := []task.Task{}
	for _, task := range m.TaskDb {
		tasks = append(tasks, *task)
	}
	return tasks
}
