package manager

import (
	"bytes"
	"encoding/json"
	"errors"
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
	"github.com/nickstrad/task-orchestrator/internal/store"
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

	// WorkerRequestTimeout caps every request the manager makes to a worker.
	//
	// Without it these calls use http.DefaultClient, whose Timeout is zero: a
	// worker that accepts the connection and then never answers blocks the
	// calling loop forever, not just for that worker. The update loop polls
	// workers one at a time, so a single wedged worker stops task updates for
	// all of them, permanently.
	WorkerRequestTimeout = 30 * time.Second
)

type Manager struct {
	mu            sync.RWMutex
	Pending       *queue.Queue[task.TaskEvent]
	TaskDb        store.Store[task.Task]
	EventDb       store.Store[task.TaskEvent]
	WorkerTaskMap map[string][]uuid.UUID
	TaskWorkerMap map[uuid.UUID]string
	WorkerNodes   []node.Node
	Scheduler     scheduler.Scheduler
	logger        *slog.Logger

	// httpClient is every outbound call to a worker. It exists to carry
	// WorkerRequestTimeout; http.DefaultClient has none.
	httpClient *http.Client

	// How often each background loop wakes. Defaulted by NewManager; a test
	// overrides them to keep the loops sub-second.
	updateInterval      time.Duration
	processInterval     time.Duration
	healthCheckInterval time.Duration
}

type WorkerMetadata struct {
	Name    string
	Address string
}

func NewManager(workers []WorkerMetadata, schedulerType string, logger *slog.Logger, dbType string) *Manager {
	workerTaskMap := make(map[string][]uuid.UUID)

	nodes := make([]node.Node, 0, len(workers))
	for _, w := range workers {
		workerTaskMap[w.Name] = []uuid.UUID{}
		nodes = append(nodes, node.NewNode(w.Name, w.Address, "worker", logger))
	}

	s := scheduler.GetScheduler(schedulerType, logger)

	dbs := store.GetDbs(dbType)
	return &Manager{
		Pending:             queue.New[task.TaskEvent](),
		TaskDb:              dbs.TaskDb,
		EventDb:             dbs.TaskEventDb,
		WorkerTaskMap:       workerTaskMap,
		TaskWorkerMap:       make(map[uuid.UUID]string),
		Scheduler:           s,
		WorkerNodes:         nodes,
		httpClient:          &http.Client{Timeout: WorkerRequestTimeout},
		logger:              logger,
		updateInterval:      DefaultUpdateInterval,
		processInterval:     DefaultProcessInterval,
		healthCheckInterval: DefaultHealthCheckInterval,
	}
}

// workerAddress returns the API address of a worker by name. WorkerNodes is set
// once by NewManager and never written again, so this needs no lock.
//
// The scan is linear rather than a name-keyed index: the worker set is small and
// fixed, every caller is about to make an HTTP request anyway, and an index
// would be a second copy of WorkerNodes to keep in step.
func (m *Manager) workerAddress(name string) (string, bool) {
	for i := range m.WorkerNodes {
		if m.WorkerNodes[i].Name == name {
			return m.WorkerNodes[i].API, true
		}
	}
	return "", false
}

// requireWorkerAddress is workerAddress for callers that cannot continue
// without one. The error is minted here rather than at each call site because
// this is the layer that knows what failed to resolve; the caller supplies its
// own op and decides whether to log or return.
func (m *Manager) requireWorkerAddress(op, name string) (string, error) {
	addr, ok := m.workerAddress(name)
	if !ok {
		return "", E(op, fmt.Sprintf("no worker address found for worker %s", name), nil)
	}
	return addr, nil
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
	var wg sync.WaitGroup
	// Set once by NewManager and never written again, so no lock is needed.
	for _, n := range m.WorkerNodes {
		wg.Go(func() {
			m.updateTasksFromWorker(n.Name)
		})
	}
	wg.Wait()
}

func (m *Manager) updateTasksFromWorker(workerName string) {
	workerURL, exists := m.workerAddress(workerName)
	if !exists {
		m.logger.Warn("skipping task update for unknown worker", "worker", workerName)
		return
	}

	m.logger.Debug("checking worker for task updates", "worker", workerName, "url", workerURL)
	url := WorkerTasksURL(workerURL)
	resp, err := m.httpClient.Get(url)
	if err != nil {
		m.logger.Warn("connecting to worker failed",
			"err", E("manager.updateTasksFromWorker", "connecting to worker "+workerName, err),
			"worker", workerName)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		m.logger.Warn("worker returned unexpected status",
			"worker", workerName, "code", resp.StatusCode)
		return
	}

	decoder := json.NewDecoder(resp.Body)
	var tasks []task.Task
	if err := decoder.Decode(&tasks); err != nil {
		m.logger.Warn("unmarshalling tasks failed",
			"err", E("manager.updateTasksFromWorker", "decoding task list", err),
			"worker", workerName)
		return
	}

	for _, t := range tasks {
		// One unknown task must not abort updates for the remaining tasks.
		if !m.applyWorkerReport(t) {
			m.logger.Warn("task not found in task db", "taskID", t.ID, "worker", workerName)
		}
	}
}

// errWorkerUnreachable marks a send that never reached the worker. It is the
// one failure the callers treat differently: the request did not land, so the
// event is safe to put back on the queue. Every other failure means the worker
// answered and said no, and requeueing would just repeat it.
var errWorkerUnreachable = errors.New("worker unreachable")

// postTaskEvent sends te to a worker and returns the task the worker created.
//
// SendWork and restartTask both do this, and both got it slightly differently
// wrong to read; the op string is a parameter so each keeps its own identity in
// the error chain.
func (m *Manager) postTaskEvent(op, workerName, workerAddr string, te task.TaskEvent) (task.Task, error) {
	data, err := json.Marshal(te)
	if err != nil {
		return task.Task{}, E(op, "marshalling task event", err)
	}

	url := WorkerTasksURL(workerAddr)
	resp, err := m.httpClient.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		return task.Task{}, E(op, "connecting to worker "+workerName,
			errors.Join(errWorkerUnreachable, err))
	}
	defer resp.Body.Close()

	decoder := json.NewDecoder(resp.Body)

	if resp.StatusCode != http.StatusCreated {
		// The worker's Go error never crosses the wire — all we get is a DTO.
		// Read what it said, then mint a fresh error of our own rather than
		// pretending to wrap one we do not have.
		e := httpapi.ErrorResponse{}
		if err := decoder.Decode(&e); err != nil {
			return task.Task{}, E(op, "decoding error response", err)
		}
		return task.Task{}, E(op,
			fmt.Sprintf("worker %s rejected task (%d): %s", workerName, e.Code, e.Message), nil)
	}

	created := task.Task{}
	if err := decoder.Decode(&created); err != nil {
		return task.Task{}, E(op, "decoding created task", err)
	}
	return created, nil
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

	selected, err := m.SelectWorker(taskEvent.Task)
	if err != nil {
		m.logger.Error("selecting worker failed", "err", err, "taskID", t.ID)
		return
	}

	workerName := selected.Name
	m.assignTask(workerName, taskEvent)

	// The scheduler already handed us the node, so the address comes straight
	// off it rather than through another lookup by name.
	newTask, err := m.postTaskEvent("manager.SendWork", workerName, selected.API, taskEvent)
	if err != nil {
		if errors.Is(err, errWorkerUnreachable) {
			// The assignment was committed before the send, so it has to be
			// rolled back: a requeued task that is still routed to a worker
			// looks like a duplicate on the next pass and gets dropped.
			m.unassignTask(workerName, t.ID)
			m.logger.Warn("worker unreachable, requeueing task", "err", err, "taskID", t.ID)
			m.enqueueEvent(taskEvent)
			return
		}
		m.logger.Error("sending task to worker failed", "err", err, "taskID", t.ID)
		return
	}

	// The worker has the task either way. If this write is lost the manager
	// forgets a task that is running, so every later worker report for it is
	// rejected as unknown — worth an Error, but there is nothing to roll back.
	if err := m.putTask(newTask); err != nil {
		m.logger.Error("task sent but not recorded in task db",
			"err", err, "taskID", newTask.ID, "worker", workerName)
		return
	}
	m.logger.Info("task sent to worker", "taskID", newTask.ID, "state", newTask.State)
}

func (m *Manager) AddTask(taskEvent task.TaskEvent) {
	m.enqueueEvent(taskEvent)
}

func (m *Manager) GetTasks() ([]task.Task, error) {
	tasks, err := m.snapshotTasks()
	if err != nil {
		return nil, Wrap("manager.GetTasks", "snapshotting tasks", err)
	}
	return tasks, nil
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

	workerAddr, err := m.requireWorkerAddress("manager.checkTaskHealth", workerName)
	if err != nil {
		return err
	}

	url, err := healthCheckURL(workerAddr, hostPort, t.HealthCheck)
	if err != nil {
		return E("manager.checkTaskHealth", fmt.Sprintf("building health check url for task %s", t.ID), err)
	}
	m.logger.Debug("calling health check", "taskID", t.ID, "url", url)
	resp, err := m.httpClient.Get(url)
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
	tasks, err := m.GetTasks()
	if err != nil {
		// Top of a loop goroutine: nobody above can act on this, so it is
		// logged here and the pass is skipped. The next tick retries.
		m.logger.Error("skipping health check pass: listing tasks failed", "err", err)
		return
	}

	for _, t := range tasks {
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
	wAddress, err := m.requireWorkerAddress("manager.restartTask", w)
	if err != nil {
		m.logger.Error("restarting task failed", "err", err, "taskID", t.ID)
		return
	}

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
	newTask, err := m.postTaskEvent("manager.restartTask", w, wAddress, te)
	if err != nil {
		if errors.Is(err, errWorkerUnreachable) {
			m.logger.Warn("worker unreachable, requeueing task",
				"err", err, "taskID", t.ID, "addr", wAddress)
			m.enqueueEvent(te)
			return
		}
		m.logger.Error("restarting task on worker failed", "err", err, "taskID", t.ID)
		return
	}
	m.logger.Info("task restarted", "taskID", newTask.ID, "restartCount", t.RestartCount)
}

func (m *Manager) stopTask(worker string, taskID uuid.UUID) {
	workerAddr, err := m.requireWorkerAddress("manager.stopTask", worker)
	if err != nil {
		m.logger.Error("stopping task failed", "err", err, "taskID", taskID)
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

	resp, err := m.httpClient.Do(req)
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
