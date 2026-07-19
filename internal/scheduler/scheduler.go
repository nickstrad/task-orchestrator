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

// GetScheduler returns the scheduler named by schedulerType, falling back to
// round-robin for any name it does not recognise.
//
// A nil logger is accepted and discarded rather than rejected: callers that do
// not care about scheduler logs (tests, mostly) should not have to build one,
// and a nil *slog.Logger stored on a scheduler would panic at its first use
// rather than here, which is much harder to place.
func GetScheduler(schedulerType string, logger *slog.Logger) Scheduler {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	switch schedulerType {
	case MarginalCost:
		return &MarginalCostScheduler{
			Name:   MarginalCost,
			logger: logger.With("subcomponent", "scheduler", "scheduler", MarginalCost),
		}
	default:
		return &RoundRobinScheduler{Name: RoundRobin}
	}
}
