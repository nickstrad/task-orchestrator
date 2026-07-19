package manager

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
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

	// Default cadences for the background loops. They are fields on Manager
	// rather than literals inside the loops so a test can drive a loop in
	// milliseconds instead of sitting through real minutes.
	DefaultUpdateInterval      = 15 * time.Second
	DefaultProcessInterval     = 10 * time.Second
	DefaultHealthCheckInterval = 60 * time.Second
)

type Manager struct {
	mu                  sync.RWMutex
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

	// How often each background loop wakes. Defaulted by NewManager; a test
	// overrides them to keep the loops sub-second.
	updateInterval      time.Duration
	processInterval     time.Duration
	healthCheckInterval time.Duration
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

	s := scheduler.GetScheduler(schedulerType, logger)

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
		updateInterval:      DefaultUpdateInterval,
		processInterval:     DefaultProcessInterval,
		healthCheckInterval: DefaultHealthCheckInterval,
	}
}

func (m *Manager) SelectWorker(t task.Task) (node.Node, error) {
	selected, ok := m.pickWorker(t)
	if !ok {
		msg := fmt.Sprintf("no available candidates match resource request for task %s", t.ID)
		return node.Node{}, E("manager.SelectWorker", msg, nil)
	}
	return selected, nil
}

func (m *Manager) UpdateTasks(done <-chan struct{}) {
	for {
		select {
		case <-done:
			return
		case <-time.After(m.updateInterval):
			m.logger.Debug("updating tasks")
			m.updateTasks()
			m.logger.Debug("sleeping", "interval", m.updateInterval)
		}
	}
}

func (m *Manager) ProcessTasks(done <-chan struct{}) {
	for {
		select {
		case <-done:
			return
		case <-time.After(m.processInterval):
			m.logger.Debug("delegating tasks to workers")
			m.SendWork()
			m.logger.Debug("sleeping", "interval", m.processInterval)
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
	// Set once by NewManager and never written again, so no lock is needed.
	workerID := m.WorkerNameToID[workerName]
	workerURL := m.WorkerNameToAddress[workerName]

	m.logger.Debug("checking worker for task updates", "workerID", workerID, "url", workerURL)
	url := WorkerTasksURL(workerURL)
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
		// One unknown task must not abort updates for the remaining tasks.
		if !m.applyWorkerReport(t) {
			m.logger.Warn("task not found in task db", "taskID", t.ID, "workerID", workerID)
		}
	}
}

func (m *Manager) SendWork() {
	taskEvent, ok := m.nextPendingEvent()
	if !ok {
		m.logger.Debug("no work in the queue")
		return
	}
	t := taskEvent.Task
	m.logger.Debug("pulled task off pending queue", "taskID", t.ID)

	taskWorker, persistedTask, ok := m.assignmentFor(t.ID)
	if ok {
		m.logger.Debug("task already exists in task worker map", "taskID", t.ID)
		stateMachine := task.NewStateMachine()
		if t.State == task.Completed && stateMachine.IsValidTransition(persistedTask.State, t.State) {
			m.stopTask(taskWorker, t.ID)
			return
		}
		m.logger.Warn("ignoring invalid transition to completed",
			"taskID", t.ID, "state", persistedTask.State, "requestedState", t.State)
		return
	}

	node, err := m.SelectWorker(taskEvent.Task)
	if err != nil {
		m.logger.Error("selecting worker failed", "err", err, "taskID", t.ID)
		return
	}

	workerName := node.Name
	data, err := json.Marshal(taskEvent)
	if err != nil {
		m.logger.Error("marshalling task event failed",
			"err", E("manager.SendWork", "marshalling task event", err), "taskID", t.ID)
		return
	}

	m.assignTask(workerName, taskEvent)

	url := WorkerTasksURL(m.WorkerNameToAddress[workerName])
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))

	if err != nil {
		// The assignment was committed before the send, so it has to be rolled
		// back: a requeued task that is still routed to a worker looks like a
		// duplicate on the next pass and gets dropped.
		m.unassignTask(workerName, t.ID)
		m.logger.Warn("worker unreachable, requeueing task",
			"err", E("manager.SendWork", "connecting to worker "+workerName, err), "taskID", t.ID)
		m.enqueueEvent(taskEvent)
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

	m.putTask(newTask)
	m.logger.Info("task sent to worker", "taskID", newTask.ID, "state", newTask.State)
}

func (m *Manager) AddTask(taskEvent task.TaskEvent) {
	m.enqueueEvent(taskEvent)
}

func (m *Manager) GetTasks() []task.Task {
	return m.snapshotTasks()
}

func (m *Manager) checkTaskHealth(t task.Task) error {
	workerName, exists := m.workerForTask(t.ID)

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
	w, exists := m.workerForTask(t.ID)
	if !exists {
		m.logger.Error("restarting task failed",
			"err", E("manager.restartTask", fmt.Sprintf("no worker assigned to task %s", t.ID), nil),
			"taskID", t.ID)
		return
	}
	wAddress := m.WorkerNameToAddress[w]

	t, ok := m.beginRestart(t.ID)
	if !ok {
		m.logger.Error("restarting task failed",
			"err", E("manager.restartTask", fmt.Sprintf("task %s is not in the task db", t.ID), nil),
			"taskID", t.ID)
		return
	}

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
	url := WorkerTasksURL(wAddress)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		m.logger.Warn("worker unreachable, requeueing task",
			"err", E("manager.restartTask", "connecting to worker "+w, err),
			"taskID", t.ID, "addr", wAddress)
		m.enqueueEvent(te)
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

func (m *Manager) stopTask(worker string, taskID uuid.UUID) {
	workerAddr, exists := m.WorkerNameToAddress[worker]
	if !exists {
		m.logger.Error("stopping task failed",
			"err", E("manager.stopTask", fmt.Sprintf("no worker address found for worker %s", worker), nil),
			"taskID", taskID)
		return
	}

	url := fmt.Sprintf("%s/%s", WorkerTasksURL(workerAddr), taskID.String())
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		m.logger.Error("building stop request failed",
			"err", E("manager.stopTask", fmt.Sprintf("building delete request for task %s", taskID), err),
			"taskID", taskID, "addr", workerAddr)
		return
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		m.logger.Error("worker unreachable, task not stopped",
			"err", E("manager.stopTask", "connecting to worker "+worker, err),
			"taskID", taskID, "addr", workerAddr)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		// The worker's Go error never crosses the wire — all we get is a DTO.
		// Log what it said, then mint a fresh error of our own rather than
		// pretending to wrap one we do not have.
		e := httpapi.ErrorResponse{}
		if err := json.NewDecoder(resp.Body).Decode(&e); err != nil {
			m.logger.Error("decoding worker error response failed",
				"err", E("manager.stopTask", "decoding error response", err),
				"taskID", taskID, "code", resp.StatusCode)
			return
		}
		rejected := E("manager.stopTask",
			fmt.Sprintf("worker %s rejected stop (%d): %s", worker, e.Code, e.Message), nil)
		m.logger.Error("worker rejected stop", "err", rejected, "taskID", taskID, "code", e.Code)
		return
	}

	m.retireTask(worker, taskID)
	m.logger.Info("task stopped on worker", "taskID", taskID, "addr", workerAddr)
}

func (m *Manager) DoHealthChecks(done <-chan struct{}) {
	work := func() {
		m.logger.Debug("performing task health checks")
		m.doHealthChecks()
		m.logger.Debug("task health checks completed, sleeping", "interval", m.healthCheckInterval)
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
		case <-time.After(m.healthCheckInterval):
			work()
		}
	}
}
