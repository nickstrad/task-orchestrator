package scheduler

import (
	"github.com/nickstrad/task-orchestrator/internal/node"
	"github.com/nickstrad/task-orchestrator/internal/task"
)

const (
	RoundRobin = "round-robin"
)

type Scheduler interface {
	SelectCandidateNodes(t task.Task, nodes []node.Node) []node.Node
	Score(t task.Task, nodes []node.Node) map[string]float64
	Pick(scores map[string]float64, candidates []node.Node) node.Node
}

func GetScheduler(schedulerType string) Scheduler {
	m := make(map[string]Scheduler)
	m[RoundRobin] = &RoundRobinScheduler{}

	if s, exists := m[schedulerType]; exists {
		return s
	}

	return &RoundRobinScheduler{}
}
