package manager

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/nickstrad/task-orchestrator/internal/httpapi"
	"github.com/nickstrad/task-orchestrator/internal/node"
	"github.com/nickstrad/task-orchestrator/internal/queue"
	"github.com/nickstrad/task-orchestrator/internal/scheduler"
	"github.com/nickstrad/task-orchestrator/internal/task"
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
	logger              *slog.Logger
}

type WorkerMetadata struct {
	Name    string
	ID      int
	Address string
}

func NewManager(workers []WorkerMetadata, schedulerType string, logger *slog.Logger) *Manager {
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
		LastWorker:          0,
		Pending:             queue.New[task.TaskEvent](),
		TaskDb:              make(map[uuid.UUID]task.Task),
		EventDb:             make(map[uuid.UUID]task.TaskEvent),
		Workers:             workerNames,
		WorkerNameToID:      workerNameToID,
		WorkerTaskMap:       workerTaskMap,
		WorkerNameToAddress: workerNameToAddress,
		TaskWorkerMap:       make(map[uuid.UUID]string),
		Scheduler:           s,
		WorkerNodes:         nodes,
		logger:              logger,
	}
}

func (m *Manager) SelectWorker(t task.Task) (node.Node, error) {
	candidates := m.Scheduler.SelectCandidateNodes(t, m.WorkerNodes)
	if candidates == nil {
		msg := fmt.Sprintf("no available candidates match resource request for task %s", t.ID)
		return node.Node{}, E("manager.SelectWorker", msg, nil)
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
			m.logger.Debug("updating tasks")
			m.updateTasks()
			m.logger.Debug("sleeping", "seconds", 15)
		}
	}
}

func (m *Manager) ProcessTasks(done <-chan struct{}) {
	for {
		select {
		case <-done:
			return
		case <-time.After(10 * time.Second):
			m.logger.Debug("delegating tasks to workers")
			m.SendWork()
			m.logger.Debug("sleeping", "seconds", 10)
		}
	}
}

func (m *Manager) updateTasks() {
	for _, workerName := range m.Workers {
		// One worker per call, so the response body is closed as soon as we are
		// done with it rather than piling up until every worker has been polled.
		m.updateTasksFromWorker(workerName)
	}
}

func (m *Manager) updateTasksFromWorker(workerName string) {
	workerID := m.WorkerNameToID[workerName]
	workerURL := m.WorkerNameToAddress[workerName]
	m.logger.Debug("checking worker for task updates", "workerID", workerID, "url", workerURL)
	url := workerTasksURL(workerURL)
	resp, err := http.Get(url)
	if err != nil {
		m.logger.Warn("connecting to worker failed",
			"err", E("manager.updateTasksFromWorker", "connecting to worker "+workerName, err),
			"workerID", workerID)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		m.logger.Warn("worker returned unexpected status",
			"workerID", workerID, "code", resp.StatusCode)
		return
	}

	decoder := json.NewDecoder(resp.Body)
	var tasks []task.Task
	if err := decoder.Decode(&tasks); err != nil {
		m.logger.Warn("unmarshalling tasks failed",
			"err", E("manager.updateTasksFromWorker", "decoding task list", err),
			"workerID", workerID)
		return
	}

	for _, t := range tasks {
		persisted, ok := m.TaskDb[t.ID]
		if !ok {
			// One unknown task must not abort updates for the remaining tasks.
			m.logger.Warn("task not found in task db", "taskID", t.ID, "workerID", workerID)
			continue
		}

		m.TaskDb[t.ID] = mergeTaskUpdate(persisted, t)
	}
}

func (m *Manager) SendWork() {
	if m.Pending.Len() == 0 {
		m.logger.Debug("no work in the queue")
		return
	}
	taskEvent, ok := m.Pending.Dequeue()

	if !ok {
		return
	}
	t := taskEvent.Task
	m.logger.Debug("pulled task off pending queue", "taskID", t.ID)

	node, err := m.SelectWorker(taskEvent.Task)
	if err != nil {
		m.logger.Error("selecting worker failed", "err", err, "taskID", t.ID)
		return
	}

	workerName := node.Name
	m.EventDb[taskEvent.ID] = taskEvent
	m.WorkerTaskMap[workerName] = append(m.WorkerTaskMap[workerName], t.ID)
	m.TaskWorkerMap[t.ID] = workerName

	data, err := json.Marshal(taskEvent)

	if err != nil {
		m.logger.Error("marshalling task event failed",
			"err", E("manager.SendWork", "marshalling task event", err), "taskID", t.ID)
		return
	}

	url := workerTasksURL(m.WorkerNameToAddress[workerName])
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))

	if err != nil {
		m.logger.Warn("worker unreachable, requeueing task",
			"err", E("manager.SendWork", "connecting to worker "+workerName, err), "taskID", t.ID)
		m.Pending.Enqueue(taskEvent)
		return
	}
	defer resp.Body.Close()

	decoder := json.NewDecoder(resp.Body)

	if resp.StatusCode != http.StatusCreated {
		// The worker's Go error never crosses the wire — all we get is a DTO.
		// Log what it said, then mint a fresh error of our own rather than
		// pretending to wrap one we do not have.
		e := httpapi.ErrorResponse{}
		if err := decoder.Decode(&e); err != nil {
			m.logger.Error("decoding worker error response failed",
				"err", E("manager.SendWork", "decoding error response", err), "taskID", t.ID)
			return
		}
		rejected := E("manager.SendWork",
			fmt.Sprintf("worker %s rejected task (%d): %s", workerName, e.Code, e.Message), nil)
		m.logger.Error("worker rejected task", "err", rejected, "taskID", t.ID, "code", e.Code)
		return
	}

	newTask := task.Task{}

	if err := decoder.Decode(&newTask); err != nil {
		m.logger.Error("decoding worker response failed",
			"err", E("manager.SendWork", "decoding created task", err), "taskID", t.ID)
		return
	}

	m.TaskDb[newTask.ID] = newTask
	m.logger.Info("task sent to worker", "taskID", newTask.ID, "state", newTask.State)
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
	workerName, exists := m.TaskWorkerMap[t.ID]

	if !exists {
		return E("manager.checkTaskHealth", fmt.Sprintf("no worker found for task %s", t.ID), nil)
	}
	hostPort, ok := getHostPort(t.HostPorts)
	if !ok {
		return E("manager.checkTaskHealth", fmt.Sprintf("no host port found for task %s", t.ID), nil)
	}

	workerAddr, exists := m.WorkerNameToAddress[workerName]

	if !exists {
		return E("manager.checkTaskHealth", fmt.Sprintf("no worker address found for worker %s for %s", workerName, t.ID), nil)
	}

	url, err := healthCheckURL(workerAddr, hostPort, t.HealthCheck)
	if err != nil {
		return E("manager.checkTaskHealth", fmt.Sprintf("building health check url for task %s", t.ID), err)
	}
	m.logger.Debug("calling health check", "taskID", t.ID, "url", url)
	resp, err := http.Get(url)
	if err != nil {
		return E("manager.checkTaskHealth", "connecting to health check "+url, err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return E("manager.checkTaskHealth",
			fmt.Sprintf("health check for task %s did not return 200", t.ID), nil)
	}

	m.logger.Debug("health check passed", "taskID", t.ID, "code", resp.StatusCode)

	return nil
}

func (m *Manager) doHealthChecks() {
	for _, t := range m.GetTasks() {
		switch decideHealthAction(t) {
		case healthSkipNotEligible:
			m.logger.Debug("skipping health check: task not running or failed",
				"taskID", t.ID, "state", t.State)

		case healthSkipRestartMax:
			m.logger.Debug("skipping health check: restart max reached",
				"taskID", t.ID, "restartCount", t.RestartCount)

		case healthActionCheck:
			// Terminal consumer for checkTaskHealth's error: log it once here.
			err := m.checkTaskHealth(t)
			if err == nil {
				continue
			}
			m.logger.Warn("health check failed, restarting task", "err", err, "taskID", t.ID)
			m.restartTask(t)

		case healthActionRestart:
			m.restartTask(t)
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
		m.logger.Error("marshalling task event failed",
			"err", E("manager.restartTask", "marshalling task event", err), "taskID", t.ID)
		return
	}
	url := workerTasksURL(wAddress)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		m.logger.Warn("worker unreachable, requeueing task",
			"err", E("manager.restartTask", "connecting to worker "+w, err),
			"taskID", t.ID, "addr", wAddress)
		m.Pending.Enqueue(te)
		return
	}

	defer resp.Body.Close()

	d := json.NewDecoder(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		e := httpapi.ErrorResponse{}
		if err := d.Decode(&e); err != nil {
			m.logger.Error("decoding worker error response failed",
				"err", E("manager.restartTask", "decoding error response", err), "taskID", t.ID)
			return
		}
		rejected := E("manager.restartTask",
			fmt.Sprintf("worker %s rejected restart (%d): %s", w, e.Code, e.Message), nil)
		m.logger.Error("worker rejected restart", "err", rejected, "taskID", t.ID, "code", e.Code)
		return
	}

	newTask := task.Task{}
	if err := d.Decode(&newTask); err != nil {
		m.logger.Error("decoding worker response failed",
			"err", E("manager.restartTask", "decoding restarted task", err), "taskID", t.ID)
		return
	}
	m.logger.Info("task restarted", "taskID", newTask.ID, "restartCount", t.RestartCount)
}

func (m *Manager) DoHealthChecks(done <-chan struct{}) {
	work := func() {
		m.logger.Debug("performing task health checks")
		m.doHealthChecks()
		m.logger.Debug("task health checks completed, sleeping", "seconds", 60)
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
