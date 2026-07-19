package scheduler

import (
	"log/slog"

	"github.com/nickstrad/task-orchestrator/internal/node"
	"github.com/nickstrad/task-orchestrator/internal/task"
)

const (
	RoundRobin   = "round-robin"
	MarginalCost = "marginal-cost"
)

type Scheduler interface {
	SelectCandidateNodes(t task.Task, nodes []node.Node) []node.Node
	Score(t task.Task, nodes []node.Node) (map[string]float64, error)
	Pick(scores map[string]float64, candidates []node.Node) node.Node
}

func GetScheduler(schedulerType string, logger *slog.Logger) Scheduler {
	m := make(map[string]Scheduler)
	m[RoundRobin] = &RoundRobinScheduler{}

	m[MarginalCost] = &MarginalCostScheduler{
		Name: "",
	}
	if logger != nil {
		m[MarginalCost] = &MarginalCostScheduler{
			Name:   "",
			logger: logger.With("subcomponent", "scheduler", "scheduler", "marginalCostScheduler"),
		}
	} else {
		m[MarginalCost] = &MarginalCostScheduler{
			Name: "",
		}
	}
	if s, exists := m[schedulerType]; exists {
		return s
	}

	return &RoundRobinScheduler{}
}
