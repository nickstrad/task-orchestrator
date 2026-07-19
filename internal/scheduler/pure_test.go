package scheduler

import (
	"testing"

	"github.com/nickstrad/task-orchestrator/internal/node"
)

func nodesNamed(names ...string) []node.Node {
	nodes := make([]node.Node, 0, len(names))
	for _, name := range names {
		nodes = append(nodes, node.Node{Name: name})
	}
	return nodes
}

func TestNextWorkerIndex(t *testing.T) {
	tests := []struct {
		name       string
		lastWorker int
		nodeCount  int
		want       int
	}{
		{
			name:       "advances to the next node",
			lastWorker: 0,
			nodeCount:  3,
			want:       1,
		},
		{
			name:       "wraps from the last node back to the first",
			lastWorker: 2,
			nodeCount:  3,
			want:       0,
		},
		{
			name:       "a single node always selects itself",
			lastWorker: 0,
			nodeCount:  1,
			want:       0,
		},
		{
			name:       "a cursor left past the end of a shrunken set wraps into range",
			lastWorker: 7,
			nodeCount:  3,
			want:       2,
		},
		{
			name:       "a cursor exactly one past the end wraps to the first node",
			lastWorker: 2,
			nodeCount:  2,
			want:       1,
		},
		{
			name:       "a negative cursor lands back inside the range",
			lastWorker: -4,
			nodeCount:  3,
			want:       0,
		},
		{
			name:       "no nodes yields no valid index",
			lastWorker: 0,
			nodeCount:  0,
			want:       -1,
		},
		{
			name:       "a negative node count yields no valid index",
			lastWorker: 0,
			nodeCount:  -1,
			want:       -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := nextWorkerIndex(tt.lastWorker, tt.nodeCount)
			if got != tt.want {
				t.Errorf("nextWorkerIndex(%d, %d) = %d, want %d",
					tt.lastWorker, tt.nodeCount, got, tt.want)
			}
		})
	}
}

// TestNextWorkerIndexAlwaysAddressesANode pins the bug the modulo fixes: the
// original cursor was incremented without wrapping, so once LastWorker ran past
// the end of the node set no index ever matched again and no node was scored.
func TestNextWorkerIndexAlwaysAddressesANode(t *testing.T) {
	const nodeCount = 3

	last := 0
	for range 20 {
		last = nextWorkerIndex(last, nodeCount)
		if last < 0 || last >= nodeCount {
			t.Fatalf("cursor escaped the node set: got %d, want 0..%d", last, nodeCount-1)
		}
	}
}

func TestScoreNodes(t *testing.T) {
	tests := []struct {
		name     string
		nodes    []node.Node
		selected int
		want     map[string]float64
	}{
		{
			name:     "selected node scores one and the rest score zero",
			nodes:    nodesNamed("a", "b", "c"),
			selected: 1,
			want:     map[string]float64{"a": 0, "b": 1, "c": 0},
		},
		{
			name:     "every node is present in the map even when unselected",
			nodes:    nodesNamed("a", "b"),
			selected: 0,
			want:     map[string]float64{"a": 1, "b": 0},
		},
		{
			name:     "an out-of-range selection scores nobody",
			nodes:    nodesNamed("a", "b"),
			selected: 5,
			want:     map[string]float64{"a": 0, "b": 0},
		},
		{
			name:     "no nodes yields an empty map",
			nodes:    nil,
			selected: 0,
			want:     map[string]float64{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := scoreNodes(tt.nodes, tt.selected)
			if len(got) != len(tt.want) {
				t.Fatalf("scoreNodes(%v, %d) = %v, want %v", tt.nodes, tt.selected, got, tt.want)
			}
			for name, wantScore := range tt.want {
				if got[name] != wantScore {
					t.Errorf("score for %q = %v, want %v (full map %v)", name, got[name], wantScore, got)
				}
			}
		})
	}
}

func TestHighestScoringNode(t *testing.T) {
	tests := []struct {
		name       string
		scores     map[string]float64
		candidates []node.Node
		wantName   string
		wantOK     bool
	}{
		{
			name:       "picks the only node with a non-zero score",
			scores:     map[string]float64{"a": 0, "b": 1, "c": 0},
			candidates: nodesNamed("a", "b", "c"),
			wantName:   "b",
			wantOK:     true,
		},
		{
			name:       "picks the winner when it is first in the slice",
			scores:     map[string]float64{"a": 1, "b": 0},
			candidates: nodesNamed("a", "b"),
			wantName:   "a",
			wantOK:     true,
		},
		{
			name:       "a tie goes to the earliest candidate",
			scores:     map[string]float64{"a": 1, "b": 1},
			candidates: nodesNamed("a", "b"),
			wantName:   "a",
			wantOK:     true,
		},
		{
			name:       "candidates missing from the scores are treated as zero",
			scores:     map[string]float64{"b": 1},
			candidates: nodesNamed("a", "b"),
			wantName:   "b",
			wantOK:     true,
		},
		{
			name:       "all scores zero still returns the first candidate",
			scores:     map[string]float64{"a": 0, "b": 0},
			candidates: nodesNamed("a", "b"),
			wantName:   "a",
			wantOK:     true,
		},
		{
			name:       "no candidates yields no node",
			scores:     map[string]float64{"a": 1},
			candidates: nil,
			wantOK:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := highestScoringNode(tt.scores, tt.candidates)
			if ok != tt.wantOK {
				t.Fatalf("highestScoringNode(...) ok = %v, want %v", ok, tt.wantOK)
			}
			if ok && got.Name != tt.wantName {
				t.Errorf("highestScoringNode(%v, %v) = %q, want %q",
					tt.scores, tt.candidates, got.Name, tt.wantName)
			}
		})
	}
}

// TestHighestScoringNodeDoesNotMutateCandidates guards the value semantics: the
// caller's slice is read-only here and must come back untouched.
func TestHighestScoringNodeDoesNotMutateCandidates(t *testing.T) {
	candidates := nodesNamed("a", "b", "c")
	scores := map[string]float64{"a": 0, "b": 1, "c": 0}

	highestScoringNode(scores, candidates)

	for idx, want := range []string{"a", "b", "c"} {
		if candidates[idx].Name != want {
			t.Errorf("candidates[%d].Name = %q, want %q", idx, candidates[idx].Name, want)
		}
	}
}
