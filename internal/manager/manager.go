package manager

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/moby/moby/api/types/network"
	"github.com/nickstrad/task-orchestrator/internal/node"
	"github.com/nickstrad/task-orchestrator/internal/queue"
	"github.com/nickstrad/task-orchestrator/internal/scheduler"
	"github.com/nickstrad/task-orchestrator/internal/task"
	"github.com/nickstrad/task-orchestrator/internal/worker"
)

const (
	TaskRestartMax = 3
)

type Manager struct {
	LastWorker          int
	Pending             *queue.Queue[task.TaskEvent]
	TaskDb              map[uuid.UUID]task.Task
	EventDb             map[uuid.UUID]task.TaskEvent
	Workers             []string
	WorkerTaskMap       map[string][]uuid.UUID
	WorkerNameToAddress map[string]string
	TaskWorkerMap       map[uuid.UUID]string
	WorkerNameToID      map[string]int
	WorkerNodes         []node.Node
	Scheduler           scheduler.Scheduler
}
type ErrorResponse struct {
	Message string `json:"message"`
	Code    int    `json:"code"`
}

func getWorkerUrl(workerNum int) string {
	return fmt.Sprintf("http://localhost:%d", 3000+workerNum)
}

type WorkerMetadata struct {
	Name    string
	ID      int
	Address string
}

func NewManager(workers []WorkerMetadata, schedulerType string) *Manager {
	workerTaskMap := make(map[string][]uuid.UUID)
	workerNameToID := make(map[string]int)
	workerNameToAddress := make(map[string]string)
	workerNames := []string{}

	var nodes []node.Node
	for _, w := range workers {
		workerTaskMap[w.Name] = []uuid.UUID{}
		workerNameToID[w.Name] = w.ID
		workerNames = append(workerNames, w.Name)
		workerNameToAddress[w.Name] = w.Address
		nodes = append(nodes, node.NewNode(w.Name, w.Address, "worker"))
	}

	s := scheduler.GetScheduler(schedulerType)

	return &Manager{
		LastWorker:     0,
		Pending:        queue.New[task.TaskEvent](),
		TaskDb:         make(map[uuid.UUID]task.Task),
		EventDb:        make(map[uuid.UUID]task.TaskEvent),
		Workers:        workerNames,
		WorkerNameToID: workerNameToID,
		WorkerTaskMap:  workerTaskMap,
		TaskWorkerMap:  make(map[uuid.UUID]string),
		Scheduler:      s,
		WorkerNodes:    nodes,
	}
}

func (m *Manager) SelectWorker(t task.Task) (node.Node, error) {
	candidates := m.Scheduler.SelectCandidateNodes(t, m.WorkerNodes)
	if candidates == nil {
		msg := fmt.Sprintf("No available candidates match resource request for task %v", t.ID)
		err := errors.New(msg)
		return node.Node{}, err
	}
	scores := m.Scheduler.Score(t, candidates)
	selectedNode := m.Scheduler.Pick(scores, candidates)

	return selectedNode, nil
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
		var tasks []task.Task
		err = decoder.Decode(&tasks)
		if err != nil {
			log.Printf("error unmarshalling tasks: %s\n", err.Error())
		}

		for _, t := range tasks {
			log.Printf("Attempting to update task %v\n", t.ID)
			persisted, ok := m.TaskDb[t.ID]
			if !ok {
				log.Printf("Task with ID %s not found from worker %s\n", t.ID, workerURL)
				return
			}

			persisted.State = t.State
			persisted.StartTime = t.StartTime
			persisted.FinishTime = t.FinishTime
			persisted.ContainerID = t.ContainerID
			m.TaskDb[t.ID] = persisted
		}
	}
}

func (m *Manager) SendWork() {
	if m.Pending.Len() == 0 {
		log.Println("No work in the queue ")
		return
	}
	taskEvent, ok := m.Pending.Dequeue()

	if !ok {
		return
	}
	t := taskEvent.Task
	log.Printf("Pulled %v off pending queue\n", t)

	node, err := m.SelectWorker(taskEvent.Task)
	if err != nil {
		log.Printf("error selecting worker for task %s: %v\n", t.ID, err)
		return
	}

	workerName := node.Name
	m.EventDb[taskEvent.ID] = taskEvent
	m.WorkerTaskMap[workerName] = append(m.WorkerTaskMap[workerName], t.ID)
	m.TaskWorkerMap[t.ID] = workerName

	data, err := json.Marshal(taskEvent)

	if err != nil {
		log.Printf("Unable to marshal task object: %v.\n", t)
		return
	}

	workerID := m.WorkerNameToID[workerName]
	url := fmt.Sprintf("%s/tasks", getWorkerUrl(workerID))
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))

	if err != nil {
		log.Printf("Error connecting to %v: %v\n", workerName, err)
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

	newTask := task.Task{}

	err = decoder.Decode(&newTask)

	if err != nil {
		fmt.Printf("error decoding response: %s\n", err.Error())
		return
	}

	m.TaskDb[newTask.ID] = newTask
	log.Printf("%#v\n", newTask)
}

func (m *Manager) AddTask(taskEvent task.TaskEvent) {
	m.Pending.Enqueue(taskEvent)
}

func (m *Manager) GetTasks() []task.Task {
	tasks := []task.Task{}
	for _, task := range m.TaskDb {
		tasks = append(tasks, task)
	}
	return tasks
}

func (m *Manager) checkTaskHealth(t task.Task) error {
	log.Printf("Calling health check for task %s: %s\n", t.ID, t.HealthCheck)
	w := m.TaskWorkerMap[t.ID]
	hostPort, ok := getHostPort(t.HostPorts)
	if !ok {
		msg := fmt.Sprintf("no host port found for task %s", t.ID)
		log.Println(msg)
		return errors.New(msg)
	}

	worker := strings.Split(w, ":")
	url := fmt.Sprintf("https://%s:%s%s", worker[0], hostPort, t.HealthCheck)
	log.Printf("calling health check for tas %s:%s\n", t.ID, url)
	resp, err := http.Get(url)
	if err != nil {
		msg := fmt.Sprintf("error connecting to health check %s", url)
		log.Println(msg)
		return errors.New(msg)
	}

	if resp.StatusCode != http.StatusOK {
		msg := fmt.Sprintf("Error health check for task %s did not return 200\n", t.ID)
		log.Println(msg)
		return errors.New(msg)
	}

	log.Printf("Task %s health check response: %v\n", t.ID, resp.StatusCode)

	return nil
}

func (m *Manager) doHealthChecks() {
	for _, t := range m.GetTasks() {
		if t.State != task.Running && t.State != task.Failed {
			log.Printf("Skipping health check for task %s because its not running or failed.\n", t.ID)
			continue
		}

		if t.RestartCount >= TaskRestartMax {
			log.Printf("Skipping health check for task %s because its not running or failed.\n", t.ID)
			continue
		}

		if t.State == task.Running {
			err := m.checkTaskHealth(t)
			if err == nil {
				continue
			}
			m.restartTask(t)
			continue
		}

		if t.State == task.Failed {
			m.restartTask(t)
			continue
		}
	}
}

func (m *Manager) restartTask(t task.Task) {
	w := m.TaskWorkerMap[t.ID]
	wAddress := m.WorkerNameToAddress[w]
	t.State = task.Scheduled
	t.RestartCount++
	m.TaskDb[t.ID] = t

	te := task.TaskEvent{
		ID:        uuid.New(),
		State:     task.Running,
		Timestamp: time.Now(),
		Task:      t,
	}
	data, err := json.Marshal(&te)
	if err != nil {
		log.Printf("Unabl to marshal task object: %v\n", t)
		return
	}
	url := fmt.Sprintf("%s/tasks", wAddress)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		log.Printf("Error connecting to %v at %s: %v", w, wAddress, err)
		m.Pending.Enqueue(te)
		return
	}

	d := json.NewDecoder(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		e := worker.ErrorResponse{}
		err := d.Decode(&e)
		if err != nil {
			fmt.Printf("Error decoding response: %s\n", err.Error())
			return
		}
		log.Printf("Response error: (%d): %s\n", e.Code, e.Message)
		return
	}

	newTask := task.Task{}
	err = d.Decode(&newTask)
	if err != nil {
		fmt.Printf("Error decoding response: %s\n", err.Error())
		return
	}
	log.Printf("%#v\n", t)
}

func (m *Manager) DoHealthChecks(done <-chan struct{}) {
	work := func() {
		log.Println("performing task health check")
		m.doHealthChecks()
		log.Println("task health checks completed")
		log.Println("sleeping for 60 seconds")
	}

	select {
	case <-done:
	default:
		work()
	}

	for {
		select {
		case <-done:
			return
		case <-time.After(60 * time.Second):
			work()
		}
	}
}

func getHostPort(ports network.PortMap) (string, bool) {
	for k := range ports {
		return ports[k][0].HostPort, true
	}
	return "", false
}
