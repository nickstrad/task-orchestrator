package node

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/nickstrad/task-orchestrator/internal/httpapi"
	"github.com/nickstrad/task-orchestrator/internal/worker"
)

type Node struct {
	Name            string
	API             string
	Cores           int
	Memory          int
	MemoryAllocated int
	Disk            int
	DiskAllocated   int
	Role            string
	TaskCount       int
	logger          *slog.Logger
}

// statsTimeout caps a single stats fetch. A node that has stopped answering
// must not stall the scheduler's scoring pass.
const statsTimeout = 30 * time.Second

func NewNode(name, addr, role string, logger *slog.Logger) Node {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return Node{
		Name:   name,
		API:    addr,
		Role:   role,
		logger: logger.With("component", "node", "node", name, "addr", addr),
	}
}

// GetStats fetches a node's live stats. It returns errors without logging
// them — the caller decides whether to drop the node or give up, and logs
// that once.
func (n *Node) GetStats() (worker.Stats, error) {
	op := "node.Node.GetStats"

	url := n.statsURL()
	n.logger.Debug("fetching node stats", "url", url)

	ctx, cancel := context.WithTimeout(context.Background(), statsTimeout)
	defer cancel()

	resp, err := httpapi.HTTPWithRetry(ctx, httpapi.Get, url)
	if err != nil {
		// resp is nil on a transport error, so this must return before the
		// deferred Close below is ever set up.
		return worker.Stats{}, E(op, "connecting to node "+n.Name, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// The worker's Go error never crosses the wire — all that arrives is
		// the httpapi DTO, so read what it said and mint our own error rather
		// than pretend to wrap one we do not have.
		var body httpapi.ErrorResponse
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			body.Message = "unreadable error body"
		}
		return worker.Stats{}, E(op, fmt.Sprintf("node %s returned %d: %s", n.Name, resp.StatusCode, body.Message), nil)
	}

	var stats worker.Stats
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		return worker.Stats{}, E(op, "decoding stats from node "+n.Name, err)
	}

	return stats, nil
}

// statsURL builds the URL of a node's stats endpoint.
func (n *Node) statsURL() string {
	return fmt.Sprintf("%s/stats", n.API)
}
