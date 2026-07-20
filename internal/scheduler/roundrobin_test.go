package scheduler

import (
	"testing"

	"github.com/nickstrad/task-orchestrator/internal/node"
	"github.com/nickstrad/task-orchestrator/internal/task"
)

// schedule runs the full select -> score -> pick path the way Manager.SelectWorker
// does, and returns the name of the node the scheduler landed on.
func schedule(s Scheduler, t task.Task, nodes []node.Node) string {
	candidates := s.SelectCandidateNodes(t, nodes)
	scored, _ := s.Score(t, candidates)
	return s.Pick(scored).Name
}

func TestRoundRobinSchedulerSelectCandidateNodesReturnsEveryNode(t *testing.T) {
	r := &RoundRobinScheduler{}
	nodes := nodesNamed("a", "b", "c")

	got := r.SelectCandidateNodes(task.Task{}, nodes)

	if len(got) != len(nodes) {
		t.Fatalf("SelectCandidateNodes returned %d nodes, want %d", len(got), len(nodes))
	}
	for idx, n := range got {
		if n.Name != nodes[idx].Name {
			t.Errorf("candidate %d = %q, want %q", idx, n.Name, nodes[idx].Name)
		}
	}
}

// TestRoundRobinSchedulerCyclesThroughEveryNode is the property that gives the
// scheduler its name: consecutive tasks land on consecutive nodes, and the
// sequence wraps.
func TestRoundRobinSchedulerCyclesThroughEveryNode(t *testing.T) {
	r := &RoundRobinScheduler{}
	nodes := nodesNamed("a", "b", "c")

	want := []string{"b", "c", "a", "b", "c", "a"}
	for i, wantName := range want {
		if got := schedule(r, task.Task{}, nodes); got != wantName {
			t.Errorf("schedule #%d = %q, want %q", i, got, wantName)
		}
	}
}

func TestRoundRobinSchedulerSingleNodeAlwaysWins(t *testing.T) {
	r := &RoundRobinScheduler{}
	nodes := nodesNamed("only")

	for i := range 3 {
		if got := schedule(r, task.Task{}, nodes); got != "only" {
			t.Errorf("schedule #%d = %q, want %q", i, got, "only")
		}
	}
}

// TestRoundRobinSchedulerKeepsSchedulingAfterNodesAreRemoved pins the bug the
// old cursor had: LastWorker only reset when it happened to equal len(nodes)-1,
// so shrinking the node set left it permanently past the end. From then on no
// node was ever scored 1 and Pick fell back to the first candidate forever —
// every task piled onto one worker.
func TestRoundRobinSchedulerKeepsSchedulingAfterNodesAreRemoved(t *testing.T) {
	r := &RoundRobinScheduler{}

	five := nodesNamed("a", "b", "c", "d", "e")
	for range 4 {
		schedule(r, task.Task{}, five)
	}
	if r.LastWorker != 4 {
		t.Fatalf("setup: LastWorker = %d, want 4", r.LastWorker)
	}

	// The cluster shrinks to two nodes while the cursor still points at index 4.
	two := nodesNamed("a", "b")

	seen := map[string]int{}
	for range 6 {
		seen[schedule(r, task.Task{}, two)]++
	}

	if seen["a"] != 3 || seen["b"] != 3 {
		t.Errorf("after shrinking the node set, distribution = %v, want a:3 b:3", seen)
	}
}

func TestRoundRobinSchedulerScoreGivesExactlyOneWinner(t *testing.T) {
	r := &RoundRobinScheduler{}
	nodes := nodesNamed("a", "b", "c")

	scored, _ := r.Score(task.Task{}, nodes)

	winners := 0
	for _, s := range scored {
		switch s.Score {
		case 1:
			winners++
		case 0:
		default:
			t.Errorf("score for %q = %v, want 0 or 1", s.Node.Name, s.Score)
		}
	}
	if winners != 1 {
		t.Errorf("got %d nodes scored 1, want exactly 1 (scored %v)", winners, scored)
	}
}

// TestRoundRobinSchedulerHandlesNoNodes covers the empty case end to end. The
// old Score advanced LastWorker past the end of an empty slice and returned an
// empty map, and Pick then indexed candidates[0] and panicked.
func TestRoundRobinSchedulerHandlesNoNodes(t *testing.T) {
	r := &RoundRobinScheduler{}

	scored, _ := r.Score(task.Task{}, nil)
	if len(scored) != 0 {
		t.Errorf("Score with no nodes = %v, want nothing scored", scored)
	}
	if r.LastWorker != 0 {
		t.Errorf("Score with no nodes moved the cursor to %d, want it left at 0", r.LastWorker)
	}

	if got := r.Pick(scored); got.Name != "" {
		t.Errorf("Pick with nothing scored = %q, want the zero Node", got.Name)
	}
}

// TestRoundRobinSchedulerResumesAfterAnEmptyRound checks the cursor is not
// disturbed by a round with no nodes: scheduling picks up where it left off.
func TestRoundRobinSchedulerResumesAfterAnEmptyRound(t *testing.T) {
	r := &RoundRobinScheduler{}
	nodes := nodesNamed("a", "b", "c")

	if got := schedule(r, task.Task{}, nodes); got != "b" {
		t.Fatalf("setup: first schedule = %q, want %q", got, "b")
	}

	r.Score(task.Task{}, nil)

	if got := schedule(r, task.Task{}, nodes); got != "c" {
		t.Errorf("schedule after an empty round = %q, want %q", got, "c")
	}
}

func TestGetSchedulerReturnsRoundRobin(t *testing.T) {
	tests := []struct {
		name          string
		schedulerType string
	}{
		{name: "the round-robin type", schedulerType: RoundRobin},
		{name: "an unknown type falls back to round-robin", schedulerType: "nonsense"},
		{name: "an empty type falls back to round-robin", schedulerType: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetScheduler(tt.schedulerType, nil)
			if _, ok := got.(*RoundRobinScheduler); !ok {
				t.Errorf("GetScheduler(%q) = %T, want *RoundRobinScheduler", tt.schedulerType, got)
			}
		})
	}
}
