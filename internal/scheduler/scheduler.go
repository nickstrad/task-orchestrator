package scheduler

import (
	"fmt"
	"log/slog"

	"github.com/nickstrad/task-orchestrator/internal/node"
	"github.com/nickstrad/task-orchestrator/internal/task"
)

const (
	RoundRobin   = "round-robin"
	MarginalCost = "marginal-cost"
)

// ScoredNode is a node together with what it scored. Keeping the two in one
// value is what makes "a candidate with no score" unrepresentable.
//
// The pair used to be a map[string]float64 keyed by name plus the candidate
// slice, which meant Pick had to re-join them and decide what an absent key
// meant. It read as zero — the cheapest cost — so any node the scheduler failed
// to reach silently beat every healthy one. There is no absent key to interpret
// here: if a node is in the slice it has a score, and if it has no score it is
// not in the slice.
type ScoredNode struct {
	Node  node.Node
	Score float64
}

type Scheduler interface {
	SelectCandidateNodes(t task.Task, nodes []node.Node) []node.Node
	// Score returns one ScoredNode per node it could score. A scheduler may
	// drop nodes it could not reach, so the result is not necessarily the same
	// length as its input — and because a node travels with its score, there is
	// no way to hand Pick a node that has none.
	//
	// Order follows the input, so ties break deterministically.
	Score(t task.Task, nodes []node.Node) ([]ScoredNode, error)

	// Pick chooses a winner from what Score produced. It cannot be given an
	// unscored node, so it has no missing-value case to define.
	Pick(scored []ScoredNode) node.Node
}

// GetScheduler returns the scheduler named by schedulerType, or an error naming
// the valid values for anything it does not recognise. It used to fall back to
// round-robin for an unknown name, which silently ran a strategy the caller
// never asked for; erroring here protects every caller, not just the ones that
// happened to validate the flag themselves.
//
// A nil logger is accepted and discarded rather than rejected: callers that do
// not care about scheduler logs (tests, mostly) should not have to build one,
// and a nil *slog.Logger stored on a scheduler would panic at its first use
// rather than here, which is much harder to place.
func GetScheduler(schedulerType string, logger *slog.Logger) (Scheduler, error) {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	switch schedulerType {
	case MarginalCost:
		return &MarginalCostScheduler{
			Name:   MarginalCost,
			logger: logger.With("subcomponent", "scheduler", "scheduler", MarginalCost),
		}, nil
	case RoundRobin:
		return &RoundRobinScheduler{Name: RoundRobin}, nil
	default:
		return nil, fmt.Errorf("unsupported scheduler type %q: want %s or %s",
			schedulerType, RoundRobin, MarginalCost)
	}
}
