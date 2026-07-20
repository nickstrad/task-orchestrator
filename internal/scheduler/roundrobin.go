package scheduler

import (
	"sync"

	"github.com/nickstrad/task-orchestrator/internal/node"
	"github.com/nickstrad/task-orchestrator/internal/task"
)

type RoundRobinScheduler struct {
	Name string

	// mu guards LastWorker. Score is a mutation wearing a read-shaped name, and
	// the manager calls it from several goroutines, so the cursor is guarded
	// here rather than by the caller's lock.
	//
	// This is what lets Manager.pickWorker score without holding its own mutex:
	// a scheduler whose state is only safe under a caller's lock forces that
	// lock to be held across Score, and Score does network I/O for the
	// marginal-cost scheduler. See docs/concurrency-and-state.md.
	mu sync.Mutex
	// LastWorker is the index of the node picked on the previous Score call.
	// It is the scheduler's only state; everything else is derived from it.
	LastWorker int
}

func (r *RoundRobinScheduler) SelectCandidateNodes(t task.Task, nodes []node.Node) []node.Node {
	return nodes
}

// Score advances the round-robin cursor and hands the next node the only
// non-zero score.
// Round-robin reaches no network, so nothing can fail to score: every node it
// is given comes back scored.
func (r *RoundRobinScheduler) Score(t task.Task, nodes []node.Node) ([]ScoredNode, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	next := nextWorkerIndex(r.LastWorker, len(nodes))
	if next < 0 {
		// No nodes to score; leave the cursor where it is so the next call
		// with a real node set resumes from it.
		return nil, nil
	}

	r.LastWorker = next
	return scoreNodes(nodes, next), nil
}

// Pick returns the highest-scoring entry. With nothing scored there is nothing
// to pick, so it returns the zero Node; callers are expected to have rejected an
// empty candidate set before scoring.
func (r *RoundRobinScheduler) Pick(scored []ScoredNode) node.Node {
	picked, _ := highestScoringNode(scored)
	return picked
}
