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

func TestLowestScoringNode(t *testing.T) {
	tests := []struct {
		name       string
		scores     map[string]float64
		candidates []node.Node
		wantName   string
		wantOK     bool
	}{
		{
			name:       "picks the cheapest node",
			scores:     map[string]float64{"a": 0.9, "b": 0.2, "c": 0.5},
			candidates: nodesNamed("a", "b", "c"),
			wantName:   "b",
			wantOK:     true,
		},
		{
			name:       "picks the winner when it is first in the slice",
			scores:     map[string]float64{"a": 0.1, "b": 0.8},
			candidates: nodesNamed("a", "b"),
			wantName:   "a",
			wantOK:     true,
		},
		{
			name:       "a tie goes to the earliest candidate",
			scores:     map[string]float64{"a": 0.5, "b": 0.5},
			candidates: nodesNamed("a", "b"),
			wantName:   "a",
			wantOK:     true,
		},
		{
			name:       "candidates missing from the scores are treated as zero",
			scores:     map[string]float64{"a": 0.5},
			candidates: nodesNamed("a", "b"),
			wantName:   "b",
			wantOK:     true,
		},
		{
			name:       "negative scores are still comparable",
			scores:     map[string]float64{"a": -0.1, "b": 0.4},
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
			got, ok := lowestScoringNode(tt.scores, tt.candidates)
			if ok != tt.wantOK {
				t.Fatalf("lowestScoringNode(...) ok = %v, want %v", ok, tt.wantOK)
			}
			if ok && got.Name != tt.wantName {
				t.Errorf("lowestScoringNode(%v, %v) = %q, want %q",
					tt.scores, tt.candidates, got.Name, tt.wantName)
			}
		})
	}
}

// TestLowestScoringNodeIsTheInverseOfHighest pins the distinction the two
// schedulers depend on: round-robin scores a winner high, marginal cost scores
// a winner low. Reusing the wrong helper picks the worst node every time.
func TestLowestScoringNodeIsTheInverseOfHighest(t *testing.T) {
	scores := map[string]float64{"a": 0.1, "b": 0.9}
	candidates := nodesNamed("a", "b")

	low, _ := lowestScoringNode(scores, candidates)
	high, _ := highestScoringNode(scores, candidates)

	if low.Name == high.Name {
		t.Fatalf("lowest and highest both picked %q, want opposite nodes", low.Name)
	}
	if low.Name != "a" || high.Name != "b" {
		t.Errorf("lowest = %q, highest = %q, want %q and %q", low.Name, high.Name, "a", "b")
	}
}

func TestHasDiskRoom(t *testing.T) {
	tests := []struct {
		name              string
		taskDisk          int
		nodeDisk          int
		nodeDiskAllocated int
		want              bool
	}{
		{
			name:     "a task smaller than the free space fits",
			taskDisk: 5, nodeDisk: 10, nodeDiskAllocated: 0,
			want: true,
		},
		{
			name:     "a task exactly the size of the free space fits",
			taskDisk: 10, nodeDisk: 10, nodeDiskAllocated: 0,
			want: true,
		},
		{
			name:     "a task larger than the free space does not fit",
			taskDisk: 11, nodeDisk: 10, nodeDiskAllocated: 0,
			want: false,
		},
		{
			name:     "already-allocated disk reduces what fits",
			taskDisk: 5, nodeDisk: 10, nodeDiskAllocated: 8,
			want: false,
		},
		{
			name:     "a task needing nothing fits on a full node",
			taskDisk: 0, nodeDisk: 10, nodeDiskAllocated: 10,
			want: true,
		},
		{
			name:     "an over-allocated node has negative room",
			taskDisk: 1, nodeDisk: 10, nodeDiskAllocated: 12,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := hasDiskRoom(tt.taskDisk, tt.nodeDisk, tt.nodeDiskAllocated)
			if got != tt.want {
				t.Errorf("hasDiskRoom(%d, %d, %d) = %v, want %v",
					tt.taskDisk, tt.nodeDisk, tt.nodeDiskAllocated, got, tt.want)
			}
		})
	}
}

// TestHasDiskRoomAcceptsNodesThatFit pins the inverted condition the original
// filter shipped: it skipped every node with enough room and kept only the ones
// without, so the candidate set was exactly backwards.
func TestHasDiskRoomAcceptsNodesThatFit(t *testing.T) {
	if !hasDiskRoom(1, 100, 0) {
		t.Error("a 1-unit task was rejected by a node with 100 units free")
	}
	if hasDiskRoom(100, 1, 0) {
		t.Error("a 100-unit task was accepted by a node with 1 unit free")
	}
}

func TestNormalizeCPUUsage(t *testing.T) {
	tests := []struct {
		name    string
		percent float64
		want    float64
		wantOK  bool
	}{
		{name: "an idle cpu is zero load", percent: 0, want: 0, wantOK: true},
		{name: "a half-busy cpu is half load", percent: 50, want: 0.5, wantOK: true},
		{name: "a pinned cpu is full load", percent: 100, want: 1, wantOK: true},
		{name: "the -1 sentinel is unusable", percent: -1, wantOK: false},
		{name: "any negative reading is unusable", percent: -0.001, wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := normalizeCPUUsage(tt.percent)
			if ok != tt.wantOK {
				t.Fatalf("normalizeCPUUsage(%v) ok = %v, want %v", tt.percent, ok, tt.wantOK)
			}
			if ok && got != tt.want {
				t.Errorf("normalizeCPUUsage(%v) = %v, want %v", tt.percent, got, tt.want)
			}
		})
	}
}

// TestNormalizeCPUUsageNeverReturnsNegativeLoad pins why the sentinel check
// exists: a negative load makes a broken node the cheapest one in the scoring,
// so it would win every placement decision.
func TestNormalizeCPUUsageNeverReturnsNegativeLoad(t *testing.T) {
	for _, percent := range []float64{-1, -50, -100} {
		if got, ok := normalizeCPUUsage(percent); ok || got < 0 {
			t.Errorf("normalizeCPUUsage(%v) = (%v, %v), want (>=0, false)", percent, got, ok)
		}
	}
}

func TestNodeScoreRejectsUnusableCapacity(t *testing.T) {
	tests := []struct {
		name     string
		memTotal float64
	}{
		{name: "zero total memory", memTotal: 0},
		{name: "negative total memory", memTotal: -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := nodeScore(nodeLoad{MemTotal: tt.memTotal}, 100)
			if err == nil {
				t.Fatalf("nodeScore with MemTotal %v = nil error, want an error", tt.memTotal)
			}
		})
	}
}

// TestNodeScorePrefersTheEmptierNode is the property that actually matters: of
// two nodes differing in one resource, the less loaded one must cost less.
func TestNodeScorePrefersTheEmptierNode(t *testing.T) {
	const total = 1000.0

	tests := []struct {
		name          string
		cheap, costly nodeLoad
	}{
		{
			name:   "less memory used",
			cheap:  nodeLoad{MemUsed: 100, MemTotal: total},
			costly: nodeLoad{MemUsed: 800, MemTotal: total},
		},
		{
			name:   "fewer running tasks",
			cheap:  nodeLoad{MemUsed: 100, MemTotal: total, TaskCount: 1},
			costly: nodeLoad{MemUsed: 100, MemTotal: total, TaskCount: 6},
		},
		{
			name:   "lower cpu load",
			cheap:  nodeLoad{MemUsed: 100, MemTotal: total, CPULoad: 0.1},
			costly: nodeLoad{MemUsed: 100, MemTotal: total, CPULoad: 0.9},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cheap, err := nodeScore(tt.cheap, 100)
			if err != nil {
				t.Fatalf("scoring cheap node: %v", err)
			}
			costly, err := nodeScore(tt.costly, 100)
			if err != nil {
				t.Fatalf("scoring costly node: %v", err)
			}
			if cheap >= costly {
				t.Errorf("cheap node scored %v, costly node scored %v, want cheap < costly",
					cheap, costly)
			}
		})
	}
}

// TestNodeScoreCPUTermIsNotCancelledOut pins a bug from the book's formula: it
// added and subtracted the same math.Pow(LIEB, cpuLoad) term, so CPU load had
// no effect on placement at all.
func TestNodeScoreCPUTermIsNotCancelledOut(t *testing.T) {
	base := nodeLoad{MemUsed: 100, MemTotal: 1000}

	idle, err := nodeScore(base, 100)
	if err != nil {
		t.Fatalf("scoring idle node: %v", err)
	}

	busy := base
	busy.CPULoad = 1
	pinned, err := nodeScore(busy, 100)
	if err != nil {
		t.Fatalf("scoring pinned node: %v", err)
	}

	if idle == pinned {
		t.Fatalf("idle and pinned nodes both scored %v; the cpu term is cancelling out", idle)
	}
	if want := LIEB - 1; pinned-idle < want-1e-9 || pinned-idle > want+1e-9 {
		t.Errorf("cpu penalty at full load = %v, want %v", pinned-idle, want)
	}
}

// TestNodeScoreIdleNodeCostsOnlyItsMarginalTerms checks the zero point: with no
// cpu load and no memory in use, the only cost is what this task itself adds.
func TestNodeScoreIdleNodeCostsOnlyItsMarginalTerms(t *testing.T) {
	got, err := nodeScore(nodeLoad{MemUsed: 0, MemTotal: 1000, CPULoad: 0, TaskCount: 0}, 0)
	if err != nil {
		t.Fatalf("scoring empty node: %v", err)
	}

	// A zero-memory task on an idle node adds no memory load, so the whole
	// score is the job-count term for going from 0 to 1 task.
	want := powerCost(1/maxJobsPerNode) - powerCost(0)
	if got < want-1e-9 || got > want+1e-9 {
		t.Errorf("nodeScore of an idle node with a zero-size task = %v, want %v", got, want)
	}
}

// TestNodeScoreGrowsFasterOnALoadedNode is the convexity that makes the
// exponent worth having: the same task costs more on a busy node than an idle
// one. A linear cost function would score these two increments identically.
func TestNodeScoreGrowsFasterOnALoadedNode(t *testing.T) {
	const total, taskMem = 1000.0, 100.0

	empty, err := nodeScore(nodeLoad{MemUsed: 0, MemTotal: total}, taskMem)
	if err != nil {
		t.Fatalf("scoring empty node: %v", err)
	}
	loaded, err := nodeScore(nodeLoad{MemUsed: 800, MemTotal: total}, taskMem)
	if err != nil {
		t.Fatalf("scoring loaded node: %v", err)
	}

	if loaded <= empty {
		t.Errorf("the same task cost %v on an 80%%-full node and %v on an empty one, want the loaded node to cost more",
			loaded, empty)
	}
}
