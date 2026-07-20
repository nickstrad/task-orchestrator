package scheduler

import (
	"errors"
	"log/slog"
	"sync"

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

func (e *MarginalCostScheduler) Score(t task.Task, candidates []node.Node) ([]ScoredNode, error) {
	op := "scheduler.MarginalCostScheduler.Score"

	// One slot per candidate, each written only by that candidate's goroutine.
	// This is the natural partition a map could not provide: distinct slice
	// indices are distinct memory, the slice header is never written, and
	// nothing reallocates — so these writes need no lock. A map shares its
	// count, its buckets and its growth between all keys, which is why the
	// earlier map version did.
	//
	// results[i] is meaningful only when scored[i] is true.
	results := make([]ScoredNode, len(candidates))
	scored := make([]bool, len(candidates))

	var wg sync.WaitGroup
	// One slot per candidate, so no goroutine can ever block on a send. Nothing
	// drains this channel until wg.Wait() returns, so a buffer smaller than the
	// number of possible sends deadlocks: the senders that do not fit park
	// forever, and Wait never returns to start the drain.
	errsStream := make(chan error, len(candidates))
	for i, n := range candidates {
		wg.Go(func() {
			// One fetch per node: cpu, memory and task count all come from the
			// same snapshot, so the terms below describe a single moment in time.
			stats, err := n.GetStats()
			if err != nil {
				errsStream <- Wrap(op, "scoring node "+n.Name, err)
				return
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
				errsStream <- E(op, "scoring node "+n.Name, err)
				return
			}
			results[i] = ScoredNode{Node: n, Score: score}
			scored[i] = true
		})
	}

	wg.Wait()
	close(errsStream)
	errs := make([]error, 0, len(candidates))
	for err := range errsStream {
		errs = append(errs, err)
	}

	// Compacted in candidate order, so ties break the same way every run rather
	// than by goroutine completion order.
	out := make([]ScoredNode, 0, len(candidates))
	for i := range candidates {
		if scored[i] {
			out = append(out, results[i])
		}
	}

	// A partial result is still a usable one: refusing to place a task because
	// some OTHER node is unreachable takes the whole cluster down with one bad
	// worker. Score fails only when it has nothing at all to offer.
	if len(out) == 0 && len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	if len(errs) > 0 {
		// Terminal consumer for these: the caller is getting a usable result and
		// will never see them, so they are logged once here or not at all. The
		// task still lands on the best of whatever answered.
		e.logger.Warn("scoring skipped unreachable candidates",
			"err", errors.Join(errs...),
			"failed", len(errs), "scored", len(out))
	}

	return out, nil
}

// Pick returns the cheapest entry. Scores here are costs, so this is the
// opposite of the round-robin scheduler's highest-wins rule.
func (e *MarginalCostScheduler) Pick(scored []ScoredNode) node.Node {
	picked, _ := lowestScoringNode(scored)
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
