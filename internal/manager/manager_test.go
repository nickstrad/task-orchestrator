package manager

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nickstrad/task-orchestrator/internal/httpapi"
	"github.com/nickstrad/task-orchestrator/internal/scheduler"
	"github.com/nickstrad/task-orchestrator/internal/task"
)

// These tests stand a fake worker up on an httptest.Server and point the
// manager at it through the WorkerMetadata address NewManager already takes.
// Nothing is mocked: the manager builds real URLs, speaks real
// JSON, and reads real status codes, so a wrong path or a wrong verb fails here
// rather than in a live run.

const testWorker = "w1"

// newTestManager returns a manager whose only worker is reachable at addr.
func newTestManager(t *testing.T, addr string) *Manager {
	t.Helper()
	return NewManager(
		[]WorkerMetadata{{Name: testWorker, Address: addr}},
		scheduler.RoundRobin,
		slog.New(slog.DiscardHandler),
	)
}

// deadWorkerAddr returns an address nothing is listening on, for the
// worker-unreachable paths.
func deadWorkerAddr(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	addr := srv.URL
	srv.Close()
	return addr
}

func TestUpdateTasksFromWorkerPreservesRestartCount(t *testing.T) {
	id := uuid.New()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		httpapi.WriteJSON(w, http.StatusOK, []task.Task{{ID: id, State: task.Running, ContainerID: "abc"}})
	}))
	defer srv.Close()

	m := newTestManager(t, srv.URL)
	m.TaskDb[id] = task.Task{ID: id, State: task.Scheduled, RestartCount: 2}

	m.updateTasksFromWorker(testWorker)

	got := m.TaskDb[id]
	if got.State != task.Running {
		t.Errorf("State = %v, want %v", got.State, task.Running)
	}
	if got.ContainerID != "abc" {
		t.Errorf("ContainerID = %q, want %q", got.ContainerID, "abc")
	}
	// The worker never sees RestartCount and sends the zero value.
	if got.RestartCount != 2 {
		t.Errorf("RestartCount = %d, want 2", got.RestartCount)
	}
}

func TestUpdateTasksFromWorkerRequestsTheTasksEndpoint(t *testing.T) {
	var gotMethod, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		httpapi.WriteJSON(w, http.StatusOK, []task.Task{})
	}))
	defer srv.Close()

	newTestManager(t, srv.URL).updateTasksFromWorker(testWorker)

	if gotMethod != http.MethodGet || gotPath != "/tasks" {
		t.Errorf("worker received %s %s, want GET /tasks", gotMethod, gotPath)
	}
}

func TestUpdateTasksFromWorkerIgnoresNon200(t *testing.T) {
	id := uuid.New()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		httpapi.WriteError(w, http.StatusInternalServerError, "boom")
	}))
	defer srv.Close()

	m := newTestManager(t, srv.URL)
	m.TaskDb[id] = task.Task{ID: id, State: task.Running}

	m.updateTasksFromWorker(testWorker)

	if got := m.TaskDb[id].State; got != task.Running {
		t.Errorf("State = %v, want it left at %v", got, task.Running)
	}
}

// TestUpdateTasksFromWorkerSkipsUnknownTasks pins that one task the manager has
// never heard of does not abort the updates for the tasks it has.
func TestUpdateTasksFromWorkerSkipsUnknownTasks(t *testing.T) {
	known, unknown := uuid.New(), uuid.New()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		httpapi.WriteJSON(w, http.StatusOK, []task.Task{
			{ID: unknown, State: task.Running},
			{ID: known, State: task.Running},
		})
	}))
	defer srv.Close()

	m := newTestManager(t, srv.URL)
	m.TaskDb[known] = task.Task{ID: known, State: task.Scheduled}

	m.updateTasksFromWorker(testWorker)

	if got := m.TaskDb[known].State; got != task.Running {
		t.Errorf("known task State = %v, want %v", got, task.Running)
	}
	if _, ok := m.TaskDb[unknown]; ok {
		t.Error("unknown task was added to TaskDb")
	}
}

// TestUpdateTasksPollsWorkersConcurrently pins the fan-out in updateTasks.
//
// Every handler blocks until all of them have been entered, so the test can
// only get past the barrier if the workers are being polled at the same time.
// A sequential loop parks on the first worker and never reaches the second,
// which trips the deadline below rather than passing slowly.
//
// The other half of its job is what it drags in under -race: several
// applyWorkerReport calls writing into TaskDb at once. That is the pairing the
// fan-out created, and the assertion that fails if a write is ever moved out
// from under m.mu.
func TestUpdateTasksPollsWorkersConcurrently(t *testing.T) {
	const numWorkers = 3

	// Buffered so a handler can report arrival and still be released on the
	// failure path below without deadlocking.
	arrived := make(chan struct{}, numWorkers)
	release := make(chan struct{})

	workers := make([]WorkerMetadata, 0, numWorkers)
	taskIDs := make([]uuid.UUID, 0, numWorkers)
	for i := range numWorkers {
		id := uuid.New()
		taskIDs = append(taskIDs, id)

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			arrived <- struct{}{}
			<-release
			httpapi.WriteJSON(w, http.StatusOK,
				[]task.Task{{ID: id, State: task.Running, ContainerID: "abc"}})
		}))
		defer srv.Close()

		workers = append(workers, WorkerMetadata{
			Name:    fmt.Sprintf("w%d", i),
			Address: srv.URL,
		})
	}

	m := NewManager(workers, scheduler.RoundRobin, slog.New(slog.DiscardHandler))
	for _, id := range taskIDs {
		m.TaskDb[id] = task.Task{ID: id, State: task.Scheduled}
	}

	finished := make(chan struct{})
	go func() {
		m.updateTasks()
		close(finished)
	}()

	// Every worker must be mid-request before any of them is allowed to answer.
	for i := range numWorkers {
		select {
		case <-arrived:
		case <-time.After(2 * time.Second):
			close(release) // unblock whoever is waiting, so Close does not hang
			t.Fatalf("only %d of %d workers were polled before the deadline — "+
				"updateTasks is not fanning out", i, numWorkers)
		}
	}
	close(release)

	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("updateTasks did not return after every worker answered")
	}

	for i, id := range taskIDs {
		if got := m.TaskDb[id].State; got != task.Running {
			t.Errorf("task reported by w%d has State = %v, want %v", i, got, task.Running)
		}
	}
}

func TestSendWorkPostsTheEventAndRecordsTheAssignment(t *testing.T) {
	id := uuid.New()
	var gotMethod, gotPath string
	var gotEvent task.TaskEvent
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		json.NewDecoder(r.Body).Decode(&gotEvent)
		httpapi.WriteJSON(w, http.StatusCreated, task.Task{ID: id, State: task.Scheduled})
	}))
	defer srv.Close()

	m := newTestManager(t, srv.URL)
	m.AddTask(task.TaskEvent{ID: uuid.New(), State: task.Running, Task: task.Task{ID: id, State: task.Scheduled}})

	m.SendWork()

	if gotMethod != http.MethodPost || gotPath != "/tasks" {
		t.Errorf("worker received %s %s, want POST /tasks", gotMethod, gotPath)
	}
	if gotEvent.Task.ID != id {
		t.Errorf("worker received task %v, want %v", gotEvent.Task.ID, id)
	}
	if got := m.TaskWorkerMap[id]; got != testWorker {
		t.Errorf("TaskWorkerMap[%v] = %q, want %q", id, got, testWorker)
	}
	if got := m.WorkerTaskMap[testWorker]; len(got) != 1 || got[0] != id {
		t.Errorf("WorkerTaskMap[%q] = %v, want [%v]", testWorker, got, id)
	}
	if _, ok := m.TaskDb[id]; !ok {
		t.Error("task was not persisted to TaskDb")
	}
}

// TestSendWorkRequeuesWhenTheWorkerIsUnreachable is the recoverable case: the
// task must go back on the queue rather than being dropped.
func TestSendWorkRequeuesWhenTheWorkerIsUnreachable(t *testing.T) {
	id := uuid.New()
	m := newTestManager(t, deadWorkerAddr(t))
	m.AddTask(task.TaskEvent{ID: uuid.New(), Task: task.Task{ID: id, State: task.Scheduled}})

	m.SendWork()

	if got := m.Pending.Len(); got != 1 {
		t.Errorf("Pending.Len() = %d, want 1 (the task should be requeued)", got)
	}
	// The send never landed, so the assignment must have been rolled back.
	if worker, ok := m.workerForTask(id); ok {
		t.Errorf("task is still routed to %q after a failed send", worker)
	}
	if got := m.WorkerTaskMap[testWorker]; len(got) != 0 {
		t.Errorf("WorkerTaskMap[%q] = %v, want empty after a failed send", testWorker, got)
	}
}

// TestSendWorkRetriesAfterTheWorkerComesBack is the second round the requeue
// exists for, and the reason the assignment has to be rolled back. With the
// assignment left behind, the requeued event finds a task that already has a
// worker, falls into the already-assigned branch, and is dropped as an invalid
// transition to completed — so the task would never be sent at all.
func TestSendWorkRetriesAfterTheWorkerComesBack(t *testing.T) {
	id := uuid.New()

	var posts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		posts.Add(1)
		httpapi.WriteJSON(w, http.StatusCreated, task.Task{ID: id, State: task.Scheduled})
	}))
	defer srv.Close()

	// Round one: the worker is down.
	m := newTestManager(t, deadWorkerAddr(t))
	m.AddTask(task.TaskEvent{ID: uuid.New(), Task: task.Task{ID: id, State: task.Scheduled}})
	m.SendWork()

	if got := m.Pending.Len(); got != 1 {
		t.Fatalf("setup: Pending.Len() = %d, want 1", got)
	}

	// Round two: same manager, worker now answering. WorkerNodes is normally
	// write-once, so this reaches into it directly rather than through a
	// setter the production code has no use for. Safe only because the write
	// sits between two synchronous SendWork calls on this goroutine.
	m.WorkerNodes[0].API = srv.URL
	m.SendWork()

	if got := posts.Load(); got != 1 {
		t.Errorf("worker received %d posts, want 1 — the retry never reached it", got)
	}
	if got := m.Pending.Len(); got != 0 {
		t.Errorf("Pending.Len() = %d, want 0", got)
	}
	if got, ok := m.workerForTask(id); !ok || got != testWorker {
		t.Errorf("TaskWorkerMap[%v] = %q (present=%v), want %q", id, got, ok, testWorker)
	}
	if got := m.WorkerTaskMap[testWorker]; len(got) != 1 || got[0] != id {
		t.Errorf("WorkerTaskMap[%q] = %v, want [%v]", testWorker, got, id)
	}
}

// TestSendWorkDropsARejectedTask is the opposite case: the worker answered, and
// said no. Requeueing would spin on a task it will keep rejecting.
func TestSendWorkDropsARejectedTask(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		httpapi.WriteError(w, http.StatusBadRequest, "error unmarshalling body")
	}))
	defer srv.Close()

	m := newTestManager(t, srv.URL)
	m.AddTask(task.TaskEvent{ID: uuid.New(), Task: task.Task{ID: uuid.New(), State: task.Scheduled}})

	m.SendWork()

	if got := m.Pending.Len(); got != 0 {
		t.Errorf("Pending.Len() = %d, want 0 (a rejected task should not requeue)", got)
	}
}

func TestStopTaskSendsDeleteAndRetiresTheTask(t *testing.T) {
	id := uuid.New()
	var gotMethod, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	m := newTestManager(t, srv.URL)
	m.TaskDb[id] = task.Task{ID: id, State: task.Running}
	m.TaskWorkerMap[id] = testWorker
	m.WorkerTaskMap[testWorker] = []uuid.UUID{id}

	m.stopTask(testWorker, id)

	if want := "/tasks/" + id.String(); gotMethod != http.MethodDelete || gotPath != want {
		t.Errorf("worker received %s %s, want DELETE %s", gotMethod, gotPath, want)
	}
	if got := m.TaskDb[id].State; got != task.Completed {
		t.Errorf("State = %v, want %v", got, task.Completed)
	}
	if _, ok := m.TaskWorkerMap[id]; ok {
		t.Error("TaskWorkerMap still routes the stopped task")
	}
	if got := m.WorkerTaskMap[testWorker]; len(got) != 0 {
		t.Errorf("WorkerTaskMap[%q] = %v, want empty", testWorker, got)
	}
}

// TestStopTaskDoesNotRetireOnRejection: if the worker refused the stop, the
// task is still running over there, so the manager must keep routing to it.
func TestStopTaskDoesNotRetireOnRejection(t *testing.T) {
	id := uuid.New()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		httpapi.WriteError(w, http.StatusNotFound, "task does not exist on worker")
	}))
	defer srv.Close()

	m := newTestManager(t, srv.URL)
	m.TaskDb[id] = task.Task{ID: id, State: task.Running}
	m.TaskWorkerMap[id] = testWorker
	m.WorkerTaskMap[testWorker] = []uuid.UUID{id}

	m.stopTask(testWorker, id)

	if got := m.TaskDb[id].State; got != task.Running {
		t.Errorf("State = %v, want it left at %v", got, task.Running)
	}
	if got := m.TaskWorkerMap[id]; got != testWorker {
		t.Errorf("TaskWorkerMap[%v] = %q, want it left at %q", id, got, testWorker)
	}
}

// TestRetiredTaskIsNotHealthChecked is the end-to-end version of the stop /
// restart interaction: once a task has been stopped, the health check loop must
// leave it alone instead of restarting what was just completed.
func TestRetiredTaskIsNotHealthChecked(t *testing.T) {
	id := uuid.New()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	m := newTestManager(t, srv.URL)
	m.TaskDb[id] = task.Task{ID: id, State: task.Running}
	m.TaskWorkerMap[id] = testWorker
	m.WorkerTaskMap[testWorker] = []uuid.UUID{id}

	m.stopTask(testWorker, id)

	if got := decideHealthAction(m.TaskDb[id]); got != healthSkipNotEligible {
		t.Errorf("decideHealthAction after stop = %v, want healthSkipNotEligible", got)
	}
}

// TestStaleRunningReportCannotResurrectARetiredTask covers the race the
// mergeTaskUpdate guard exists for: the worker's Db still says Running until
// its queue drains, so the very next poll reports Running for a task the
// manager has already retired.
func TestStaleRunningReportCannotResurrectARetiredTask(t *testing.T) {
	id := uuid.New()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		// The stop was accepted but not yet executed, so the worker still
		// believes the task is running.
		httpapi.WriteJSON(w, http.StatusOK, []task.Task{{ID: id, State: task.Running}})
	}))
	defer srv.Close()

	m := newTestManager(t, srv.URL)
	m.TaskDb[id] = task.Task{ID: id, State: task.Running}
	m.TaskWorkerMap[id] = testWorker
	m.WorkerTaskMap[testWorker] = []uuid.UUID{id}

	m.stopTask(testWorker, id)
	m.updateTasksFromWorker(testWorker)

	if got := m.TaskDb[id].State; got != task.Completed {
		t.Errorf("State = %v, want %v — a stale report walked the task back", got, task.Completed)
	}
	if got := decideHealthAction(m.TaskDb[id]); got != healthSkipNotEligible {
		t.Errorf("decideHealthAction = %v, want healthSkipNotEligible", got)
	}
}

// The loop tests below exist because the loops own two things nothing else
// covers: that they actually call their body on each tick, and that they stop
// promptly when done closes. They run at millisecond intervals via the
// unexported interval fields, so they cost no real wall clock.

func TestUpdateTasksLoopPollsUntilDone(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		httpapi.WriteJSON(w, http.StatusOK, []task.Task{})
	}))
	defer srv.Close()

	m := newTestManager(t, srv.URL)
	m.updateInterval = time.Millisecond

	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		m.UpdateTasks(done)
		close(finished)
	}()

	// Let a handful of ticks land, then stop the loop.
	time.Sleep(50 * time.Millisecond)
	close(done)

	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("UpdateTasks did not return after done was closed")
	}

	if got := calls.Load(); got < 2 {
		t.Errorf("worker polled %d times, want at least 2", got)
	}
}

func TestProcessTasksLoopSendsWorkUntilDone(t *testing.T) {
	var posts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		posts.Add(1)
		httpapi.WriteJSON(w, http.StatusCreated, task.Task{ID: uuid.New(), State: task.Scheduled})
	}))
	defer srv.Close()

	m := newTestManager(t, srv.URL)
	m.processInterval = time.Millisecond
	for range 3 {
		m.AddTask(task.TaskEvent{ID: uuid.New(), Task: task.Task{ID: uuid.New(), State: task.Scheduled}})
	}

	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		m.ProcessTasks(done)
		close(finished)
	}()

	time.Sleep(50 * time.Millisecond)
	close(done)

	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("ProcessTasks did not return after done was closed")
	}

	// SendWork dequeues one task per tick, so all three should have gone out.
	if got := posts.Load(); got != 3 {
		t.Errorf("worker received %d task posts, want 3", got)
	}
	if got := m.Pending.Len(); got != 0 {
		t.Errorf("Pending.Len() = %d, want 0", got)
	}
}

// TestDoHealthChecksRunsBeforeItsFirstSleep pins the deliberate asymmetry in
// DoHealthChecks: unlike the other two loops it runs its body once up front
// rather than waiting a full interval, so a task that is already failed is not
// left broken for the first minute of a run.
func TestDoHealthChecksRunsBeforeItsFirstSleep(t *testing.T) {
	id := uuid.New()
	var restarts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		restarts.Add(1)
		httpapi.WriteJSON(w, http.StatusCreated, task.Task{ID: id, State: task.Scheduled})
	}))
	defer srv.Close()

	m := newTestManager(t, srv.URL)
	// An interval long enough that a second tick cannot be what restarts it.
	m.healthCheckInterval = time.Hour
	m.TaskDb[id] = task.Task{ID: id, State: task.Failed}
	m.TaskWorkerMap[id] = testWorker

	done := make(chan struct{})
	go m.DoHealthChecks(done)
	defer close(done)

	deadline := time.After(time.Second)
	for restarts.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("DoHealthChecks did not restart the failed task before its first sleep")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	if got := m.TaskDb[id].RestartCount; got != 1 {
		t.Errorf("RestartCount = %d, want 1", got)
	}
}

// TestDoHealthChecksStopsWhenDoneIsAlreadyClosed guards the pre-loop select:
// a manager told to stop before it starts must not run a health check pass.
func TestDoHealthChecksStopsWhenDoneIsAlreadyClosed(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		httpapi.WriteJSON(w, http.StatusCreated, task.Task{ID: uuid.New()})
	}))
	defer srv.Close()

	m := newTestManager(t, srv.URL)
	m.healthCheckInterval = time.Millisecond
	m.TaskDb[uuid.New()] = task.Task{ID: uuid.New(), State: task.Failed}

	done := make(chan struct{})
	close(done)

	finished := make(chan struct{})
	go func() {
		m.DoHealthChecks(done)
		close(finished)
	}()

	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("DoHealthChecks did not return when done was already closed")
	}

	if got := calls.Load(); got != 0 {
		t.Errorf("worker was contacted %d times, want 0", got)
	}
}

// TestConcurrentLoopsAreRaceFree runs the three background loops together the
// way main.go does, against a worker that answers every endpoint. Its value is
// entirely in what `go test -race` reports: any unguarded access to TaskDb,
// EventDb, the routing maps, the pending queue, or the scheduler's cursor
// surfaces here and nowhere else in the suite.
func TestConcurrentLoopsAreRaceFree(t *testing.T) {
	ids := make([]uuid.UUID, 8)
	for i := range ids {
		ids[i] = uuid.New()
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodGet && r.URL.Path == "/tasks":
			reported := make([]task.Task, 0, len(ids))
			for _, id := range ids {
				reported = append(reported, task.Task{ID: id, State: task.Running})
			}
			httpapi.WriteJSON(w, http.StatusOK, reported)
		case r.Method == http.MethodPost:
			httpapi.WriteJSON(w, http.StatusCreated, task.Task{ID: uuid.New(), State: task.Scheduled})
		default:
			// Health checks land here.
			httpapi.WriteJSON(w, http.StatusOK, struct{}{})
		}
	}))
	defer srv.Close()

	m := newTestManager(t, srv.URL)
	m.updateInterval = time.Millisecond
	m.processInterval = time.Millisecond
	m.healthCheckInterval = time.Millisecond

	// Seed both a backlog to schedule and tasks already assigned, so every loop
	// has real work and they contend on the same maps.
	for _, id := range ids {
		m.TaskDb[id] = task.Task{ID: id, State: task.Running}
		m.TaskWorkerMap[id] = testWorker
		m.WorkerTaskMap[testWorker] = append(m.WorkerTaskMap[testWorker], id)
		m.AddTask(task.TaskEvent{ID: uuid.New(), Task: task.Task{ID: uuid.New(), State: task.Scheduled}})
	}

	done := make(chan struct{})
	var wg sync.WaitGroup
	for _, loop := range []func(<-chan struct{}){m.UpdateTasks, m.ProcessTasks, m.DoHealthChecks} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			loop(done)
		}()
	}

	// Hammer the read paths from a fourth goroutine, standing in for the HTTP
	// API serving GET /tasks while the loops mutate state underneath it.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
				m.GetTasks()
			}
		}
	}()

	time.Sleep(200 * time.Millisecond)
	close(done)

	finished := make(chan struct{})
	go func() {
		wg.Wait()
		close(finished)
	}()

	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("loops did not shut down after done was closed")
	}
}

// TestConcurrentSendWorkDispatchesEachTaskOnce pins that the dequeue and the
// assignment are one transaction: with the emptiness check and the dequeue in
// separate critical sections, two callers can both observe a non-empty queue
// and race for the same event.
func TestConcurrentSendWorkDispatchesEachTaskOnce(t *testing.T) {
	const taskCount = 50

	var mu sync.Mutex
	seen := map[uuid.UUID]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var te task.TaskEvent
		json.NewDecoder(r.Body).Decode(&te)
		mu.Lock()
		seen[te.Task.ID]++
		mu.Unlock()
		httpapi.WriteJSON(w, http.StatusCreated, te.Task)
	}))
	defer srv.Close()

	m := newTestManager(t, srv.URL)
	for range taskCount {
		m.AddTask(task.TaskEvent{ID: uuid.New(), Task: task.Task{ID: uuid.New(), State: task.Scheduled}})
	}

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range taskCount {
				m.SendWork()
			}
		}()
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != taskCount {
		t.Errorf("dispatched %d distinct tasks, want %d", len(seen), taskCount)
	}
	for id, count := range seen {
		if count != 1 {
			t.Errorf("task %v was dispatched %d times, want exactly 1", id, count)
		}
	}
}
