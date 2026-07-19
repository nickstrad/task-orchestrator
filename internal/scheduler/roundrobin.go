package scheduler

import (
	"github.com/nickstrad/task-orchestrator/internal/node"
	"github.com/nickstrad/task-orchestrator/internal/task"
)

type RoundRobinScheduler struct {
	Name       string
	LastWorker int
}

func (r *RoundRobinScheduler) SelectCandidateNodes(t task.Task, nodes []node.Node) []node.Node {
	return nodes
}

func (r *RoundRobinScheduler) Score(t task.Task, nodes []node.Node) map[string]float64 {
	scores := make(map[string]float64)
	var newLastWorker int

	if r.LastWorker == len(nodes)-1 {
		r.LastWorker = 0
		newLastWorker = 0
	} else {
		newLastWorker = r.LastWorker + 1
		r.LastWorker += 1
	}

	for idx, node := range nodes {
		scores[node.Name] = 0.0
		if idx == newLastWorker {
			scores[node.Name] += 1
		}
	}

	return scores
}

func (r *RoundRobinScheduler) Pick(scores map[string]float64, candidates []node.Node) node.Node {
	maxNode := candidates[0]
	for _, node := range candidates {
		if scores[node.Name] > scores[maxNode.Name] {
			maxNode = node
		}
	}

	return maxNode
}
