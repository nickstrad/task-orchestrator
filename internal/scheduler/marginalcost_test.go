package scheduler

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/shirou/gopsutil/v4/mem"

	"github.com/nickstrad/task-orchestrator/internal/node"
	"github.com/nickstrad/task-orchestrator/internal/task"
	"github.com/nickstrad/task-orchestrator/internal/worker"
)

// Score fans out one goroutine per candidate, so every test here uses several
// nodes on purpose. A single-node test exercises none of the concurrency and
// would pass with the locking removed — see docs/concurrency-and-state.md.

func newTestMarginalCost() *MarginalCostScheduler {
	return &MarginalCostScheduler{
		Name:   "test",
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

// statsServer stands in for a worker's /stats endpoint.
func statsServer(t *testing.T, s worker.Stats) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(s)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// failingServer stands in for a worker that is up but broken.
func failingServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]any{"message": "boom", "code": 500})
	}))
	t.Cleanup(srv.Close)
	return srv
}

// scoreOf finds a node's score in a result slice. Score returns only the nodes
// it could reach, so "absent" is the normal way a failed node shows up.
func scoreOf(scored []ScoredNode, name string) (float64, bool) {
	for _, s := range scored {
		if s.Node.Name == name {
			return s.Score, true
		}
	}
	return 0, false
}

func healthyStats(taskCount int) worker.Stats {
	return worker.Stats{
		MemStats:           &mem.VirtualMemoryStat{Total: 16_000_000, Available: 8_000_000},
		CPUUsagePercentage: 25,
		TaskCount:          taskCount,
	}
}

// TestScoreIsRaceFreeAcrossManyNodes is the test the parallel Score exists for.
// Each goroutine writes its own slot in a pre-sized slice — distinct indices are
// distinct memory, so no lock is needed — but a regression to a shared map or a
// shared append would show up here under -race.
func TestScoreIsRaceFreeAcrossManyNodes(t *testing.T) {
	const nodeCount = 8

	candidates := make([]node.Node, 0, nodeCount)
	for i := range nodeCount {
		srv := statsServer(t, healthyStats(i))
		candidates = append(candidates, node.NewNode(
			"worker-"+string(rune('a'+i)), srv.URL, "worker", nil))
	}

	e := newTestMarginalCost()
	scored, err := e.Score(task.Task{Memory: 1000}, candidates)
	if err != nil {
		t.Fatalf("Score returned an error for all-healthy nodes: %v", err)
	}

	if len(scored) != nodeCount {
		t.Errorf("scored %d nodes, want %d — a lost write would drop one",
			len(scored), nodeCount)
	}
	// Order must follow the input so ties break the same way every run.
	for i, n := range candidates {
		if scored[i].Node.Name != n.Name {
			t.Errorf("scored[%d] is %q, want %q — result order must follow the input",
				i, scored[i].Node.Name, n.Name)
		}
	}
}

// TestScoreSucceedsWhenOnlySomeCandidatesFail is the availability contract: one
// reachable node is enough to place a task. Refusing to schedule because some
// OTHER worker is down would let a single bad node stall the whole cluster.
func TestScoreSucceedsWhenOnlySomeCandidatesFail(t *testing.T) {
	const failing = 3

	candidates := make([]node.Node, 0, failing+1)
	for i := range failing {
		srv := failingServer(t)
		candidates = append(candidates, node.NewNode(
			"bad-"+string(rune('a'+i)), srv.URL, "worker", nil))
	}
	healthy := statsServer(t, healthyStats(0))
	candidates = append(candidates, node.NewNode("good", healthy.URL, "worker", nil))

	e := newTestMarginalCost()
	scored, err := e.Score(task.Task{Memory: 1000}, candidates)

	if err != nil {
		t.Fatalf("Score failed despite one healthy candidate: %v", err)
	}
	if len(scored) != 1 {
		t.Errorf("scored %d nodes, want 1 — only the healthy node can be scored", len(scored))
	}
	if _, ok := scoreOf(scored, "good"); !ok {
		t.Errorf("scored = %v, want an entry for the healthy node", scored)
	}
	// A node that failed must not appear at all. Under the old map API a stub
	// zero entry would have made it the cheapest — and so the winner.
	for i := range failing {
		name := "bad-" + string(rune('a'+i))
		if _, ok := scoreOf(scored, name); ok {
			t.Errorf("failed node %s has a score entry", name)
		}
	}
}

// TestScoreFailsOnlyWhenEveryCandidateFails is the other half of that contract,
// and pins the error-channel drain: with nothing to offer, Score reports every
// failure rather than whichever goroutine happened to finish first.
func TestScoreFailsOnlyWhenEveryCandidateFails(t *testing.T) {
	const failing = 3

	candidates := make([]node.Node, 0, failing)
	for i := range failing {
		srv := failingServer(t)
		candidates = append(candidates, node.NewNode(
			"bad-"+string(rune('a'+i)), srv.URL, "worker", nil))
	}

	e := newTestMarginalCost()
	scored, err := e.Score(task.Task{Memory: 1000}, candidates)

	if err == nil {
		t.Fatal("Score returned nil error with no reachable candidate")
	}
	if scored != nil {
		t.Errorf("scored = %v, want nil", scored)
	}

	// errors.Join concatenates with newlines, so every failing node should be
	// named in the message.
	for i := range failing {
		name := "bad-" + string(rune('a'+i))
		if !strings.Contains(err.Error(), name) {
			t.Errorf("joined error does not mention %s: %v", name, err)
		}
	}
}

// TestScoreContactsEveryNodeEvenWhenSomeFail pins the difference between this
// and the sequential version it replaced.
//
// Sequentially, the first failure did `return nil, err` out of Score and the
// remaining nodes were never contacted. Now each node runs in its own goroutine
// and the early `return` in the closure ends only THAT goroutine, so a failure
// no longer short-circuits the loop: every node is always asked.
func TestScoreContactsEveryNodeEvenWhenSomeFail(t *testing.T) {
	var mu sync.Mutex
	contacted := map[string]bool{}

	// record wraps a handler so the test can see which nodes were actually hit.
	record := func(name string, h http.HandlerFunc) *httptest.Server {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			contacted[name] = true
			mu.Unlock()
			h(w, r)
		}))
		t.Cleanup(srv.Close)
		return srv
	}

	fail := func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]any{"message": "boom", "code": 500})
	}
	ok := func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(healthyStats(1))
	}

	// The FIRST node fails. Sequentially that ended the pass here.
	candidates := []node.Node{
		node.NewNode("first-fails", record("first-fails", fail).URL, "worker", nil),
		node.NewNode("second-ok", record("second-ok", ok).URL, "worker", nil),
		node.NewNode("third-ok", record("third-ok", ok).URL, "worker", nil),
	}

	e := newTestMarginalCost()
	if _, err := e.Score(task.Task{Memory: 1000}, candidates); err != nil {
		t.Fatalf("Score failed despite two healthy candidates: %v", err)
	}

	for _, name := range []string{"first-fails", "second-ok", "third-ok"} {
		if !contacted[name] {
			t.Errorf("node %s was never contacted — a failure short-circuited "+
				"the fan-out, which the closure's return should not do", name)
		}
	}
}

// TestPickNeverChoosesAnUnreachableNode is the end-to-end guard on partial
// scoring. It is the regression test for a bug the old API allowed: Score
// returned a score map plus the caller's candidate list, Pick re-joined them by
// name, and a node Score could not reach had no entry — which read as 0.0, the
// cheapest cost there is, so the dead node won every placement.
//
// Under ScoredNode that state cannot be built. Pick is handed exactly what
// Score produced, and an unreachable node is simply not in it. The old failure
// mode is now a compile error rather than a runtime one: there is no overload
// of Pick that accepts the full candidate list.
func TestPickNeverChoosesAnUnreachableNode(t *testing.T) {
	dead := failingServer(t)
	healthy := statsServer(t, healthyStats(3)) // loaded, so its cost is well above zero

	candidates := []node.Node{
		node.NewNode("dead", dead.URL, "worker", nil),
		node.NewNode("healthy", healthy.URL, "worker", nil),
	}

	e := newTestMarginalCost()
	scored, err := e.Score(task.Task{Memory: 1000}, candidates)
	if err != nil {
		t.Fatalf("Score failed despite a healthy candidate: %v", err)
	}

	if len(scored) != 1 || scored[0].Node.Name != "healthy" {
		t.Fatalf("scored = %v, want just the healthy node", scored)
	}
	if score := scored[0].Score; score <= 0 {
		t.Errorf("healthy node scored %v; a cost at or below zero would tie or "+
			"beat the absent-entry value the old API produced", score)
	}

	if picked := e.Pick(scored); picked.Name != "healthy" {
		t.Fatalf("Pick chose %q, want \"healthy\"", picked.Name)
	}
}

// TestScoreDoesNotDeadlockWhenEveryNodeFails is the regression test for the
// channel buffer. With a fixed-size buffer smaller than the candidate count,
// the goroutines that do not fit block forever on the send, wg.Wait never
// returns, and the drain below never starts.
func TestScoreDoesNotDeadlockWhenEveryNodeFails(t *testing.T) {
	const nodeCount = 16

	candidates := make([]node.Node, 0, nodeCount)
	for i := range nodeCount {
		srv := failingServer(t)
		candidates = append(candidates, node.NewNode(
			"bad-"+string(rune('a'+i)), srv.URL, "worker", nil))
	}

	e := newTestMarginalCost()
	// A deadlock surfaces as the test binary's panic timeout rather than a
	// failure here; the assertion is that this line is reached at all.
	if _, err := e.Score(task.Task{Memory: 1000}, candidates); err == nil {
		t.Fatal("Score returned nil error despite every node failing")
	}
}

// TestScoreWithNoCandidates guards the len(candidates) buffer against the
// zero case — make(chan, 0) is unbuffered, but with no goroutines nothing
// ever sends.
func TestScoreWithNoCandidates(t *testing.T) {
	e := newTestMarginalCost()
	scored, err := e.Score(task.Task{Memory: 1000}, nil)
	if err != nil {
		t.Fatalf("Score with no candidates returned an error: %v", err)
	}
	if len(scored) != 0 {
		t.Errorf("scored = %v, want empty", scored)
	}
}
