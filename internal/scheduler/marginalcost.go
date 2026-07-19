package scheduler

import (
	"log/slog"

	"github.com/nickstrad/task-orchestrator/internal/node"
	"github.com/nickstrad/task-orchestrator/internal/task"
	"github.com/nickstrad/task-orchestrator/internal/worker"
)

// MarginalCostScheduler places a task on whichever node it makes least
// expensive, where cost is an exponential function of each resource's load.
//
// It descends from E-PVM but is not it: the published algorithm needs a per
// resource request for the task being placed, and task.Task only carries
// memory and disk. Memory and job count are scored as true marginal costs;
// CPU enters as a load penalty. See nodeScore in pure.go for the formula and
// what would have to change to make CPU marginal too.
type MarginalCostScheduler struct {
	Name string
	// logger is unexported like every other component's, so nothing outside
	// the package can swap it out mid-run.
	logger *slog.Logger
}

func (e *MarginalCostScheduler) SelectCandidateNodes(t task.Task, nodes []node.Node) []node.Node {
	candidates := []node.Node{}
	for _, n := range nodes {
		if !hasDiskRoom(t.Disk, n.Disk, n.DiskAllocated) {
			continue
		}
		candidates = append(candidates, n)
	}

	return candidates
}

func (e *MarginalCostScheduler) Score(t task.Task, candidates []node.Node) (map[string]float64, error) {
	op := "scheduler.MarginalCostScheduler.Score"

	scores := make(map[string]float64)
	for _, n := range candidates {
		// One fetch per node: cpu, memory and task count all come from the
		// same snapshot, so the terms below describe a single moment in time.
		stats, err := n.GetStats()
		if err != nil {
			return nil, Wrap(op, "scoring node "+n.Name, err)
		}

		load := nodeLoad{
			// n.MemoryAllocated is the manager's own bookkeeping of what it
			// has placed but the worker may not have started yet. Nothing
			// writes it today, so this is currently just the live usage.
			MemUsed:   float64(stats.MemUsedKb()) + float64(n.MemoryAllocated),
			MemTotal:  float64(stats.MemTotalKb()),
			CPULoad:   e.cpuLoad(n, stats),
			TaskCount: float64(stats.TaskCount),
		}

		score, err := nodeScore(load, float64(t.Memory))
		if err != nil {
			return nil, E(op, "scoring node "+n.Name, err)
		}
		scores[n.Name] = score
	}

	return scores, nil
}

// Pick returns the cheapest candidate. Scores here are costs, so this is the
// opposite of the round-robin scheduler's highest-wins rule.
func (e *MarginalCostScheduler) Pick(scores map[string]float64, candidates []node.Node) node.Node {
	picked, _ := lowestScoringNode(scores, candidates)
	return picked
}

// cpuLoad reads CPU load out of an already-fetched snapshot. It takes stats
// rather than fetching its own so that Score pays for one round trip per node
// instead of two, and so the cpu and memory terms describe the same instant.
//
// This wrapper exists to hold the logging: normalizeCPUUsage decides whether
// the reading is usable, and this decides what an unusable one means.
func (e *MarginalCostScheduler) cpuLoad(n node.Node, stats worker.Stats) float64 {
	load, ok := normalizeCPUUsage(stats.CPUUsage())
	if !ok {
		// Treat unknown as fully busy: the node sinks to the bottom of the
		// scoring but stays eligible, so a stats hiccup does not make an
		// otherwise healthy node unschedulable.
		e.logger.Warn("node reported unknown cpu usage, treating as fully busy",
			"node", n.Name, "usage", stats.CPUUsage())
		return 1
	}
	return load
}
