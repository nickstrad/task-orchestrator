package scheduler

import "github.com/nickstrad/task-orchestrator/internal/node"

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
