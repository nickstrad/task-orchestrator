package scheduler

import (
	"github.com/nickstrad/task-orchestrator/internal/node"
	"github.com/nickstrad/task-orchestrator/internal/task"
)

type RoundRobinScheduler struct {
	Name string
	// LastWorker is the index of the node picked on the previous Score call.
	// It is the scheduler's only state; everything else is derived from it.
	LastWorker int
}

func (r *RoundRobinScheduler) SelectCandidateNodes(t task.Task, nodes []node.Node) []node.Node {
	return nodes
}

// Score advances the round-robin cursor and hands the next node the only
// non-zero score.
func (r *RoundRobinScheduler) Score(t task.Task, nodes []node.Node) (map[string]float64, error) {
	next := nextWorkerIndex(r.LastWorker, len(nodes))
	if next < 0 {
		// No nodes to score; leave the cursor where it is so the next call
		// with a real node set resumes from it.
		return map[string]float64{}, nil
	}

	r.LastWorker = next
	return scoreNodes(nodes, next), nil
}

// Pick returns the highest-scoring candidate. With no candidates there is
// nothing to pick, so it returns the zero Node; callers are expected to have
// rejected an empty candidate set before scoring.
func (r *RoundRobinScheduler) Pick(scores map[string]float64, candidates []node.Node) node.Node {
	picked, _ := highestScoringNode(scores, candidates)
	return picked
}
