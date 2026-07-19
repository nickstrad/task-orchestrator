package scheduler

import (
	"fmt"
	"math"

	"github.com/nickstrad/task-orchestrator/internal/node"
)

// This file holds the scheduler's pure functions: no receiver, no I/O, no
// logging, no clock. The round-robin cursor arithmetic and the score-to-winner
// lookup are values in, values out, so they can be tested without constructing
// a scheduler or threading LastWorker through by hand.

// nextWorkerIndex advances the round-robin cursor, returning the index of the
// node whose turn it is.
//
// The modulo is what makes the cursor safe: lastWorker is carried across calls
// while the node set can change between them, so a cursor left pointing past
// the end of a now-smaller set has to wrap rather than keep climbing. A
// negative lastWorker (an uninitialized or corrupted cursor) also lands back
// inside the range.
//
// Returns -1 when there are no nodes, since no index is valid.
func nextWorkerIndex(lastWorker, nodeCount int) int {
	if nodeCount <= 0 {
		return -1
	}

	next := (lastWorker + 1) % nodeCount
	if next < 0 {
		// Go's % keeps the sign of the dividend, so a negative cursor needs a
		// second wrap to become a usable index.
		next += nodeCount
	}
	return next
}

// scoreNodes gives the node at index selected a score of 1 and every other node
// a score of 0. Nodes not at selected are still present in the map: Pick reads
// scores by name, and a missing key would be indistinguishable from a zero.
func scoreNodes(nodes []node.Node, selected int) map[string]float64 {
	scores := make(map[string]float64, len(nodes))
	for idx, n := range nodes {
		if idx == selected {
			scores[n.Name] = 1.0
			continue
		}
		scores[n.Name] = 0.0
	}
	return scores
}

// highestScoringNode returns the candidate with the greatest score, and whether
// there was one at all. Ties go to the earliest candidate in the slice, so the
// result does not depend on map iteration order.
//
// A candidate missing from scores is treated as scoring zero, which is what the
// bare map lookup did before.
func highestScoringNode(scores map[string]float64, candidates []node.Node) (node.Node, bool) {
	if len(candidates) == 0 {
		return node.Node{}, false
	}

	best := candidates[0]
	for _, n := range candidates[1:] {
		if scores[n.Name] > scores[best.Name] {
			best = n
		}
	}
	return best, true
}

// lowestScoringNode returns the candidate with the smallest score, and whether
// there was one at all. Marginal-cost scoring is the inverse of round-robin's:
// the score is what placing the task *costs*, so the cheapest node wins.
//
// Ties go to the earliest candidate, and a candidate missing from scores is
// treated as scoring zero — the same conventions highestScoringNode uses.
func lowestScoringNode(scores map[string]float64, candidates []node.Node) (node.Node, bool) {
	if len(candidates) == 0 {
		return node.Node{}, false
	}

	best := candidates[0]
	for _, n := range candidates[1:] {
		if scores[n.Name] < scores[best.Name] {
			best = n
		}
	}
	return best, true
}

const (
	// LIEB is the Lieb square ice constant, the exponent base E-PVM uses to
	// turn a resource load into a cost. Any base above 1 makes the cost curve
	// convex — the same increment hurts more on a loaded node than an idle one
	// — and this is the value the published algorithm settled on.
	// https://en.wikipedia.org/wiki/Lieb%27s_square_ice_constant
	LIEB = 1.53960071783900203869

	// maxJobsPerNode is the task count at which the job-count term reaches a
	// full unit of load. It is a tuning knob, not a hard cap: a node with more
	// than this many tasks still scores, just expensively.
	maxJobsPerNode = 4.0
)

// hasDiskRoom reports whether a task's disk requirement fits in what the node
// has left unallocated. A task asking for exactly the remaining space fits.
func hasDiskRoom(taskDisk, nodeDisk, nodeDiskAllocated int) bool {
	return taskDisk <= nodeDisk-nodeDiskAllocated
}

// normalizeCPUUsage converts a gopsutil CPU reading into the 0-1 fraction the
// cost formula works in, reporting whether the reading was usable at all.
//
// worker.NewStats stores -1 when gopsutil could not read the CPU, and that
// sentinel must never reach the scoring math: a negative load makes a broken
// node look emptier than a genuinely idle one, so it would win every placement.
// The caller decides what to substitute; this only says the value is unusable.
func normalizeCPUUsage(percent float64) (float64, bool) {
	if percent < 0 {
		return 0, false
	}
	return percent / 100, true
}

// calculateLoad expresses usage as a fraction of capacity.
func calculateLoad(usage float64, capacity float64) float64 { return usage / capacity }

// powerCost turns a 0-1 resource load into its cost term. It is the whole of
// E-PVM in one line: cost grows exponentially with load, so the marginal cost
// of the next task rises the fuller a node gets.
func powerCost(load float64) float64 { return math.Pow(LIEB, load) }

// nodeLoad is one node's scoring input, already reduced to the units the cost
// formula works in: bytes for memory, a 0-1 fraction for CPU, a plain count for
// tasks. Keeping it a struct stops six same-typed float64 arguments from being
// silently transposable at the call site.
type nodeLoad struct {
	MemUsed   float64
	MemTotal  float64
	CPULoad   float64
	TaskCount float64
}

// nodeScore returns the marginal cost of placing a task of taskMemory bytes on
// a node with the given load. Lower is better — use lowestScoringNode to pick.
//
// This is E-PVM adapted to the stats a worker actually reports. Memory and job
// count are true marginal terms (cost after minus cost before). CPU cannot be:
// task.Task carries no CPU request, so there is no "after" figure to diff
// against, and current load stands in as a penalty that is 0 on an idle node
// and LIEB-1 on a pinned one. Give task.Task a Cpu field and the CPU term
// becomes marginal like the other two.
func nodeScore(l nodeLoad, taskMemory float64) (float64, error) {
	if l.MemTotal <= 0 {
		// Every memory term divides by this. Without the guard the node scores
		// NaN, which compares false against every other score and so neither
		// wins nor loses — a node that silently drops out of scheduling.
		return 0, fmt.Errorf("total memory must be positive, got %v", l.MemTotal)
	}

	memNow := calculateLoad(l.MemUsed, l.MemTotal)
	memNew := calculateLoad(l.MemUsed+taskMemory, l.MemTotal)

	memCost := powerCost(memNew) - powerCost(memNow)
	jobCost := powerCost((l.TaskCount+1)/maxJobsPerNode) - powerCost(l.TaskCount/maxJobsPerNode)
	cpuCost := powerCost(l.CPULoad) - 1

	return memCost + jobCost + cpuCost, nil
}
